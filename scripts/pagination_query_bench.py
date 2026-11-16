#!/usr/bin/env python3
"""Benchmark pagination on isolated local mydb and MySQL 8.0 instances.

Requires PyMySQL and cryptography. All generated data, logs, and raw results stay under /tmp/mydb.
Existing servers are never used. Run with a freshly built release mydb binary.
"""

import argparse
import csv
import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import random
import socket
import statistics
import subprocess
import time

import pymysql

from like_query_bench import connect, dump, execute, wait_ready


ROOT = Path(__file__).resolve().parents[1]
DDL = """CREATE TABLE items (
 id BIGINT NOT NULL PRIMARY KEY,
 tenant_id INT NOT NULL,
 created_at DATETIME NOT NULL,
 score INT NOT NULL,
 price DECIMAL(12,2) NOT NULL,
 status INT NOT NULL,
 nullable_score INT NULL,
 tag VARCHAR(64) COLLATE utf8mb4_bin NOT NULL,
 payload VARCHAR(512) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"""
INDEXES = {
    "idx_created": "created_at,id",
    "idx_tenant_created": "tenant_id,created_at,id",
    "idx_score": "score,id",
    "idx_nullable": "nullable_score,id",
    "idx_tag": "tag,id",
}


def generate(path, count):
    base = datetime.datetime(2025, 1, 1)
    times = [(base + datetime.timedelta(seconds=i)).strftime("%Y-%m-%d %H:%M:%S")
             for i in range(200_000)]
    digest = hashlib.sha256()
    with path.open("wb") as stream:
        for i in range(1, count + 1):
            x = i * 104729 % count
            token = hashlib.blake2b(str(i).encode(), digest_size=16).hexdigest()
            price = x * 37 % 10_000_003
            fields = [str(i), str(x % 100), times[x % 200_000],
                      str(x * 37 % 10007), f"{price // 100}.{price % 100:02d}",
                      str(x // 100 % 4), r"\N" if i % 10 == 0 else str(x % 1000),
                      f"{'数据' if x % 2 else 'item'}-{x % 1000:04d}", token * 8]
            line = ("\t".join(fields) + "\n").encode()
            stream.write(line)
            digest.update(line)
    return digest.hexdigest()


def normalized(rows):
    return tuple(tuple(None if value is None else str(value) for value in row) for row in rows)


def fingerprint(rows):
    return hashlib.sha256(json.dumps(normalized(rows), ensure_ascii=False,
                                     separators=(",", ":")).encode()).hexdigest()


def make_cases(count, oracle):
    result = []

    def add(name, family, sql, **extra):
        result.append(dict(name=name, family=family, sql=sql, **extra))

    offsets = sorted(set([0, min(100, count // 100), count // 100, count // 10, count * 9 // 10]))
    for family, fields, order in [
        ("pk_cover", "id", "id"),
        ("pk_wide", "id,payload", "id"),
        ("pk_desc", "id,payload", "id DESC"),
        ("created_cover", "created_at,id", "created_at,id"),
        ("created_wide", "id,created_at,payload", "created_at,id"),
        ("score_cover", "score,id", "score,id"),
    ]:
        for offset in offsets:
            add(f"{family}.o{offset}", family,
                f"SELECT {fields} FROM items ORDER BY {order} LIMIT 20 OFFSET {offset}", offset=offset)
    for family, fields, order in [
        ("filesort_decimal", "id,price", "price,id"),
        ("filesort_expression", "id,score+1", "score+1,id"),
        ("filesort_string", "id,tag", "LOWER(tag),id"),
        ("mixed_direction", "score,id", "score DESC,id ASC"),
    ]:
        for offset in [0, count // 10, count * 9 // 10]:
            add(f"{family}.o{offset}", family,
                f"SELECT {fields} FROM items ORDER BY {order} LIMIT 20 OFFSET {offset}", offset=offset)
    tenant_count = execute(oracle, "SELECT COUNT(*) FROM items WHERE tenant_id=7")[0][0]
    for family, fields, order in [
        ("tenant_cover", "created_at,id", "created_at,id"),
        ("tenant_wide_desc", "id,created_at,payload", "created_at DESC,id DESC"),
    ]:
        for offset in sorted(set([0, tenant_count // 2, tenant_count * 9 // 10])):
            add(f"{family}.o{offset}", family,
                f"SELECT {fields} FROM items WHERE tenant_id=7 ORDER BY {order} LIMIT 20 OFFSET {offset}", offset=offset)
    for family, fields, order in [
        ("nullable", "nullable_score,id", "nullable_score,id"),
        ("unicode_tag", "tag,id", "tag,id"),
    ]:
        for offset in [0, count * 9 // 10]:
            add(f"{family}.o{offset}", family,
                f"SELECT {fields} FROM items ORDER BY {order} LIMIT 20 OFFSET {offset}", offset=offset)
    for family, predicate in [("filter_unindexed", "status=2"),
                              ("filter_indexed", "score BETWEEN 2000 AND 6000")]:
        for offset in [0, count // 10]:
            add(f"{family}.o{offset}", family,
                f"SELECT id,created_at,payload FROM items WHERE {predicate} ORDER BY created_at,id LIMIT 20 OFFSET {offset}", offset=offset)
    for offset in [count // 100, count // 10, count * 9 // 10]:
        for direction in ["ASC", "DESC"]:
            anchor = execute(oracle, f"SELECT id FROM items ORDER BY id {direction} LIMIT 1 OFFSET {offset - 1}")[0][0]
            op = ">" if direction == "ASC" else "<"
            add(f"seek_pk_{direction.lower()}.o{offset}", "seek_pk",
                f"SELECT id,payload FROM items WHERE id{op}{anchor} ORDER BY id {direction} LIMIT 20",
                offset=offset, equivalent=f"pk_{'wide' if direction == 'ASC' else 'desc'}.o{offset}")
        created, item_id = execute(oracle, f"SELECT created_at,id FROM items ORDER BY created_at,id LIMIT 1 OFFSET {offset - 1}")[0]
        literal = oracle.escape(created)
        for kind, predicate in [
            ("or", f"created_at>{literal} OR (created_at={literal} AND id>{item_id})"),
            ("tuple", f"(created_at,id)>({literal},{item_id})"),
        ]:
            add(f"seek_created_{kind}.o{offset}", f"seek_created_{kind}",
                f"SELECT id,created_at,payload FROM items WHERE {predicate} ORDER BY created_at,id LIMIT 20",
                offset=offset, equivalent=f"created_wide.o{offset}")
    tenant_offset = tenant_count * 9 // 10
    created, item_id = execute(oracle, f"SELECT created_at,id FROM items WHERE tenant_id=7 ORDER BY created_at,id LIMIT 1 OFFSET {max(tenant_offset - 1, 0)}")[0]
    literal = oracle.escape(created)
    add("seek_tenant", "seek_tenant",
        f"SELECT created_at,id FROM items WHERE tenant_id=7 AND (created_at>{literal} OR (created_at={literal} AND id>{item_id})) ORDER BY created_at,id LIMIT 20",
        equivalent=f"tenant_cover.o{tenant_offset}")
    for offset in [0, count // 10, count * 9 // 10]:
        add(f"join_direct.o{offset}", "join_direct",
            f"SELECT i.id,i.created_at,i.payload,t.name FROM items i JOIN tenants t ON i.tenant_id=t.id ORDER BY i.created_at,i.id LIMIT 20 OFFSET {offset}", offset=offset)
        add(f"join_deferred.o{offset}", "join_deferred",
            f"SELECT i.id,i.created_at,i.payload,t.name FROM (SELECT id,created_at FROM items ORDER BY created_at,id LIMIT 20 OFFSET {offset}) p JOIN items i ON i.id=p.id JOIN tenants t ON i.tenant_id=t.id ORDER BY p.created_at,p.id",
            offset=offset, equivalent=f"join_direct.o{offset}")
    for offset in [0, count * 9 // 10]:
        add(f"window_rownum.o{offset}", "window_rownum",
            f"SELECT id,created_at,rn FROM (SELECT id,created_at,ROW_NUMBER() OVER (ORDER BY created_at,id) AS rn FROM items) p WHERE rn>{offset} AND rn<={offset + 20} ORDER BY rn", offset=offset)
        add(f"window_total.o{offset}", "window_total",
            f"SELECT id,created_at,COUNT(*) OVER () AS total FROM items ORDER BY created_at,id LIMIT 20 OFFSET {offset}", offset=offset)
    add("aggregate_page", "aggregate", "SELECT tenant_id,COUNT(*) AS cnt,SUM(price) AS total FROM items GROUP BY tenant_id ORDER BY total DESC,tenant_id LIMIT 20 OFFSET 40")
    add("distinct_page", "distinct", "SELECT DISTINCT tag FROM items ORDER BY tag LIMIT 20 OFFSET 500")
    add("union_page", "union", f"SELECT id,created_at FROM items WHERE id<={count // 2} UNION ALL SELECT id,created_at FROM items WHERE id>{count // 2} ORDER BY created_at,id LIMIT 20 OFFSET {count * 9 // 10}")
    for size in [1, 100, 1000]:
        for family, fields, order in [("page_size_pk", "id,payload", "id"),
                                     ("page_size_sort", "id,price", "price,id")]:
            add(f"{family}.n{size}", family,
                f"SELECT {fields} FROM items ORDER BY {order} LIMIT {size} OFFSET {count // 10}", page_size=size)
    add("limit_zero", "boundary", "SELECT id,payload FROM items ORDER BY price,id LIMIT 0")
    add("beyond_end", "boundary", f"SELECT id FROM items ORDER BY id LIMIT 20 OFFSET {count + 100}")
    return result


def measure(case, connections, samples, warmups, seed):
    case["engines"] = {}
    outputs = {}
    for name, connection in connections.items():
        metrics = case["engines"][name] = dict(samples_ms=[])
        try:
            metrics["explain"] = execute(connection, "EXPLAIN " + case["sql"])
            rows = execute(connection, case["sql"])
            outputs[name] = normalized(rows)
            metrics.update(row_count=len(rows), sha256=fingerprint(rows), preview=normalized(rows[:2]))
        except pymysql.MySQLError as error:
            metrics["error"] = str(error)
    case["correct"] = len(outputs) == 2 and outputs["mysql"] == outputs["mydb"]
    if not case["correct"]:
        return
    for _ in range(warmups):
        for connection in connections.values():
            execute(connection, case["sql"])
    rng = random.Random(seed)
    for _ in range(samples):
        order = list(connections)
        rng.shuffle(order)
        for name in order:
            start = time.perf_counter_ns()
            rows = execute(connections[name], case["sql"])
            elapsed = (time.perf_counter_ns() - start) / 1e6
            if normalized(rows) != outputs[name]:
                raise RuntimeError(f"{case['name']}: unstable result on {name}")
            case["engines"][name]["samples_ms"].append(elapsed)
    for metrics in case["engines"].values():
        times = metrics["samples_ms"]
        metrics.update(median_ms=statistics.median(times), min_ms=min(times), max_ms=max(times),
                       p95_ms=sorted(times)[math.ceil(.95 * len(times)) - 1])
    case["mysql_over_mydb"] = case["engines"]["mysql"]["median_ms"] / case["engines"]["mydb"]["median_ms"]


def write_csv(directory, cases):
    with (directory / "summary.csv").open("w") as stream:
        writer = csv.writer(stream)
        writer.writerow(["case", "family", "correct", "mysql_median_ms", "mydb_median_ms", "mysql_over_mydb"])
        for case in cases:
            writer.writerow([case["name"], case["family"], case.get("correct"),
                             case.get("engines", {}).get("mysql", {}).get("median_ms"),
                             case.get("engines", {}).get("mydb", {}).get("median_ms"), case.get("mysql_over_mydb")])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--samples", type=int, default=9)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--mysql-home", type=Path, default=Path.home() / ".local/opt/mysql-8.0.46")
    parser.add_argument("--mydb-binary", type=Path, default=ROOT / "target/release/mydb")
    parser.add_argument("--mysql-port", type=int, default=33465)
    parser.add_argument("--mydb-port", type=int, default=48465)
    args = parser.parse_args()
    directory = args.directory.resolve()
    if not directory.is_relative_to(Path("/tmp/mydb").resolve()):
        parser.error("--directory must be under /tmp/mydb")
    if args.rows < 10_000 or math.gcd(args.rows, 104729) != 1 or args.samples < 3 or args.warmups < 1:
        parser.error("require rows >= 10000 coprime to 104729, samples >= 3, warmups >= 1")
    if args.mysql_port == args.mydb_port:
        parser.error("MySQL and mydb ports must differ")
    for port in [args.mysql_port, args.mydb_port]:
        with socket.socket() as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            probe.bind(("127.0.0.1", port))
    directory.mkdir(parents=True, exist_ok=False)
    data = directory / "items.tsv"
    result = dict(arguments=vars(args), started=datetime.datetime.now().astimezone().isoformat(),
                  platform=platform.platform(), git_head=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                  git_status=subprocess.check_output(["git", "status", "--short"], cwd=ROOT, text=True),
                  binary_sha256=hashlib.sha256(args.mydb_binary.read_bytes()).hexdigest(),
                  dataset_sha256=generate(data, args.rows), ddl=DDL, indexes=INDEXES,
                  configuration=dict(buffer_pool_bytes_each=1073741824, mydb_memory_target="2G",
                                     concurrency=1, protocol="PyMySQL text protocol/TCP, persistent, complete fetch",
                                     cache="warm data and plan caches; no result cache", isolation="READ-COMMITTED",
                                     parallelism="engine defaults", query_timeout_ms=120000), servers={}, cases=[])
    dump(directory / "results.json", result)
    mysql_data = directory / "mysql-data"
    with (directory / "mysql-initialize.log").open("w") as log:
        subprocess.run([str(args.mysql_home / "bin/mysqld"), "--no-defaults", "--initialize-insecure",
                        f"--basedir={args.mysql_home}", f"--datadir={mysql_data}"], check=True, stdout=log, stderr=subprocess.STDOUT)
    commands = {
        "mysql": [str(args.mysql_home / "bin/mysqld"), "--no-defaults", f"--basedir={args.mysql_home}",
                  f"--datadir={mysql_data}", f"--port={args.mysql_port}", "--bind-address=127.0.0.1",
                  f"--socket={directory / 'mysql.sock'}", f"--pid-file={directory / 'mysql.pid'}",
                  "--mysqlx=OFF", "--local-infile=ON", "--innodb-buffer-pool-size=1G",
                  "--character-set-server=utf8mb4", "--collation-server=utf8mb4_bin", "--skip-log-bin", "--skip-ssl"],
        "mydb": [str(args.mydb_binary), "--host", "127.0.0.1", "--port", str(args.mydb_port),
                 "--data-dir", str(directory / "mydb-data"), "--ssl-mode", "disabled"],
    }
    result["commands"] = commands
    environment = {key: value for key, value in os.environ.items() if not key.startswith("MYDB_")}
    environment.update(MYDB_BUFFER_POOL_PAGES="65536", MYDB_MEMORY_TARGET="2G", MYDB_SLOW_QUERY_LOG="OFF")
    processes, logs, connections = [], [], {}
    try:
        for name, port, password in [("mysql", args.mysql_port, ""), ("mydb", args.mydb_port, "root")]:
            log_path = directory / f"{name}.log"
            log = log_path.open("w")
            logs.append(log)
            process = subprocess.Popen(commands[name], stdout=log, stderr=subprocess.STDOUT,
                                       env=environment if name == "mydb" else os.environ)
            processes.append(process)
            wait_ready(process, port, password, log_path)
            connection = connect(port, password)
            connections[name] = connection
            execute(connection, "CREATE DATABASE pagination_bench CHARACTER SET utf8mb4 COLLATE utf8mb4_bin")
            execute(connection, "USE pagination_bench")
            execute(connection, DDL)
            start = time.perf_counter()
            execute(connection, f"LOAD DATA LOCAL INFILE {connection.escape(str(data))} INTO TABLE items CHARACTER SET utf8mb4")
            warnings = execute(connection, "SHOW WARNINGS")
            if warnings:
                raise RuntimeError(f"{name}: LOAD DATA warnings: {warnings[:5]}")
            load_seconds = time.perf_counter() - start
            print(f"{name}: loaded {args.rows} rows in {load_seconds:.2f}s", flush=True)
            start = time.perf_counter()
            for index, columns in INDEXES.items():
                execute(connection, f"CREATE INDEX {index} ON items ({columns})")
            index_seconds = time.perf_counter() - start
            execute(connection, "CREATE TABLE tenants (id INT NOT NULL PRIMARY KEY, name VARCHAR(32) NOT NULL) ENGINE=InnoDB")
            with connection.cursor() as cursor:
                cursor.executemany("INSERT INTO tenants VALUES (%s,%s)", [(i, f"tenant-{i:03d}") for i in range(100)])
            execute(connection, "ANALYZE TABLE items,tenants")
            execute(connection, "SET SESSION max_execution_time=120000")
            result["servers"][name] = dict(version=execute(connection, "SELECT VERSION()"), load_seconds=load_seconds,
                                           index_seconds=index_seconds, variables=execute(connection, "SHOW VARIABLES"),
                                           fingerprint=execute(connection, "SELECT COUNT(*),SUM(id),SUM(price),SUM(score),SUM(OCTET_LENGTH(payload)),COUNT(nullable_score) FROM items"),
                                           table_status=execute(connection, "SHOW TABLE STATUS LIKE 'items'"),
                                           status_before=execute(connection, "SHOW GLOBAL STATUS"))
            dump(directory / "results.json", result)
        if normalized(result["servers"]["mysql"]["fingerprint"]) != normalized(result["servers"]["mydb"]["fingerprint"]):
            raise RuntimeError("dataset aggregate mismatch")
        result["cases"] = make_cases(args.rows, connections["mysql"])
        dump(directory / "results.json", result)
        order = list(result["cases"])
        random.Random(20261115).shuffle(order)
        for number, case in enumerate(order):
            start = time.perf_counter()
            measure(case, connections, args.samples, args.warmups, 20261115 + number)
            if case["correct"]:
                print(f"[{number + 1}/{len(order)}] {case['name']}: mysql={case['engines']['mysql']['median_ms']:.3f}ms mydb={case['engines']['mydb']['median_ms']:.3f}ms mysql/mydb={case['mysql_over_mydb']:.3f} ({time.perf_counter() - start:.1f}s)", flush=True)
            else:
                print(f"[{number + 1}/{len(order)}] {case['name']}: MISMATCH/ERROR {case['engines']}", flush=True)
            dump(directory / "results.json", result)
            write_csv(directory, result["cases"])
        by_name = {case["name"]: case for case in result["cases"]}
        for case in result["cases"]:
            if "equivalent" in case:
                other = by_name[case["equivalent"]]
                complete = all("sha256" in candidate["engines"][name]
                               for candidate in [case, other] for name in connections)
                case["equivalent_correct"] = (
                    all(case["engines"][name]["sha256"] == other["engines"][name]["sha256"]
                        for name in connections) if complete else None
                )
        for name, connection in connections.items():
            result["servers"][name]["status_after"] = execute(connection, "SHOW GLOBAL STATUS")
        result["completed"] = datetime.datetime.now().astimezone().isoformat()
        dump(directory / "results.json", result)
        write_csv(directory, result["cases"])
    finally:
        for connection in connections.values():
            connection.close()
        for process in reversed(processes):
            process.terminate()
        for process in reversed(processes):
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for log in logs:
            log.close()


if __name__ == "__main__":
    main()
