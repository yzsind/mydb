#!/usr/bin/env python3
"""Compare LIKE queries on isolated local mydb and MySQL 8.0 instances.

Requires PyMySQL and cryptography. Generated data and diagnostics stay under /tmp/mydb.
The runner never connects to existing database instances.
"""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import statistics
import subprocess
import time

import pymysql


ROOT = Path(__file__).resolve().parents[1]
INDEXES = "idx_ci,idx_bin,idx_unicode"
DDL = """CREATE TABLE texts (
 id BIGINT NOT NULL PRIMARY KEY,
 ascii_ci VARCHAR(128) COLLATE utf8mb4_0900_ai_ci,
 ascii_bin VARCHAR(128) COLLATE utf8mb4_bin,
 unicode_ci VARCHAR(128) COLLATE utf8mb4_0900_ai_ci
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""


def execute(connection, sql):
    with connection.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchall() if cursor.description else ()


def dump(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n")


def connect(port, password, database=None):
    connection = pymysql.connect(
        host="127.0.0.1", port=port, user="root", password=password,
        database=database, charset="utf8mb4", autocommit=True,
        local_infile=True, ssl_disabled=True, connect_timeout=2,
        read_timeout=600, write_timeout=600,
    )
    execute(connection, "SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci")
    execute(connection, "SET SESSION transaction_isolation='READ-COMMITTED'")
    execute(connection, "SET SESSION sql_mode='NO_ENGINE_SUBSTITUTION'")
    return connection


def wait_ready(process, port, password, log):
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"server exited: {log}")
        try:
            connection = connect(port, password)
            connection.close()
            return
        except (OSError, pymysql.MySQLError):
            time.sleep(0.2)
    raise TimeoutError(f"server did not start: {log}")


def generate(path, rows):
    words = ["测试", "数据库", "北京", "上海", "🙂数据", "Café", "cafe", "CAFÉ", "Résumé", "naïve"]
    digest = hashlib.sha256()
    with path.open("wb") as stream:
        for i in range(1, rows + 1):
            x = i * 104729 % rows
            token = hashlib.blake2b(str(i).encode(), digest_size=16).hexdigest()
            group = f"grp{x % 1000:03d}"
            if i % 17 == 0:
                group = group.upper()
            marker = "lit%_" if i % 101 == 0 else "plain"
            text = f"{group}/{token}/{marker}/tail{x // 1000 % 20:02d}"
            unicode_text = f"{words[x % 10]}/{token}/结束"
            values = [text, text, unicode_text] if i % 97 else [r"\N"] * 3
            line = (str(i) + "\t" + "\t".join(values) + "\n").encode()
            stream.write(line)
            digest.update(line)
    return digest.hexdigest()


def cases():
    result = []
    predicates = [
        ("prefix_rare", "LIKE 'grp007%'"),
        ("prefix_broad", "LIKE 'grp0%'"),
        ("literal", "LIKE 'grp007/no-such-value'"),
        ("contains", "LIKE '%abc%'"),
        ("suffix", "LIKE '%tail03'"),
        ("underscore", "LIKE 'grp0_7/%'"),
        ("multi", "LIKE '%a%b%c%'"),
        ("not_contains", "NOT LIKE '%abc%'"),
        ("escaped", "LIKE '%lit!%!_/%' ESCAPE '!'"),
    ]
    for col in ["ascii_ci", "ascii_bin"]:
        for name, predicate in predicates:
            result.append(dict(name=f"{col}.{name}", predicate=f"{col} {predicate}", access="auto"))
        for name, predicate in predicates:
            if name in {"contains", "suffix", "multi"}:
                result.append(dict(name=f"{col}.{name}.heap", predicate=f"{col} {predicate}", access="heap"))
    for name, predicate in [
        ("prefix", "LIKE '测试%'"),
        ("contains", "LIKE '%数据%'"),
        ("accent", "LIKE 'cafe%'"),
        ("underscore", "LIKE '__/%'"),
    ]:
        result.append(dict(name=f"unicode_ci.{name}", predicate=f"unicode_ci {predicate}", access="auto"))
    for col in ["ascii_ci", "ascii_bin"]:
        for name in ["contains", "multi"]:
            result.append(dict(name=f"{col}.{name}.heap.serial", predicate=f"{col} LIKE '%{'abc' if name == 'contains' else 'a%b%c'}%'", access="heap", serial=True))
    return result


def sql_for(case, fingerprint=False):
    if "aggregate_sql" in case:
        return case["aggregate_sql"]
    hint = " /*+ NO_PARALLEL */" if case.get("serial") else ""
    fields = "COUNT(*), COALESCE(SUM(id),0), COALESCE(MIN(id),0), COALESCE(MAX(id),0)" if fingerprint else "COUNT(*)"
    access = f" IGNORE INDEX ({INDEXES})" if case["access"] == "heap" else ""
    return f"SELECT{hint} {fields} FROM texts{access} WHERE {case['predicate']}"


def main(case_factory=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--samples", type=int, default=9)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--mysql-home", type=Path, default=Path.home() / ".local/opt/mysql-8.0.46")
    parser.add_argument("--mydb-binary", type=Path, default=ROOT / "target/release/mydb")
    parser.add_argument("--mysql-port", type=int, default=33461)
    parser.add_argument("--mydb-port", type=int, default=48461)
    args = parser.parse_args()
    directory = args.directory.resolve()
    if not directory.is_relative_to(Path("/tmp/mydb").resolve()):
        parser.error("--directory must be under /tmp/mydb")
    if args.rows < 1 or args.samples < 3 or args.warmups < 1:
        parser.error("require positive rows/warmups and at least three samples")
    directory.mkdir(parents=True, exist_ok=False)
    dataset = directory / "texts.tsv"
    result = dict(arguments=vars(args), rows=args.rows, dataset_sha256=generate(dataset, args.rows),
                  platform=platform.platform(), started=time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                  git_head=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                  git_status=subprocess.check_output(["git", "status", "--short"], cwd=ROOT, text=True),
                  binary_sha256=hashlib.sha256(args.mydb_binary.read_bytes()).hexdigest(),
                  configuration={"buffer_pool_bytes_each": 1073741824, "mydb_memory_target": "2G",
                                 "mydb_parallel_degree": "unset (default)", "protocol": "text/TCP, persistent connections",
                                 "isolation": "READ-COMMITTED", "concurrency": 1},
                  servers={}, cases=(case_factory or cases)())
    dump(directory / "results.json", result)
    mysql_data = directory / "mysql-data"
    with (directory / "mysql-initialize.log").open("w") as initialize_log:
        subprocess.run([str(args.mysql_home / "bin/mysqld"), "--no-defaults", "--initialize-insecure",
                        f"--basedir={args.mysql_home}", f"--datadir={mysql_data}"], check=True,
                       stdout=initialize_log, stderr=subprocess.STDOUT)
    processes, logs, connections = [], [], {}
    try:
        commands = {
            "mysql": [str(args.mysql_home / "bin/mysqld"), "--no-defaults", f"--basedir={args.mysql_home}",
                      f"--datadir={mysql_data}", f"--port={args.mysql_port}", "--bind-address=127.0.0.1",
                      f"--socket={directory / 'mysql.sock'}", f"--pid-file={directory / 'mysql.pid'}",
                      "--mysqlx=OFF", "--local-infile=ON", "--innodb-buffer-pool-size=1G",
                      "--character-set-server=utf8mb4", "--collation-server=utf8mb4_0900_ai_ci",
                      "--skip-log-bin", "--skip-ssl"],
            "mydb": [str(args.mydb_binary), "--host", "127.0.0.1", "--port", str(args.mydb_port),
                     "--data-dir", str(directory / "mydb-data"), "--ssl-mode", "disabled"],
        }
        environment = {key: value for key, value in os.environ.items() if not key.startswith("MYDB_")}
        environment.update(MYDB_BUFFER_POOL_PAGES="65536", MYDB_MEMORY_TARGET="2G", MYDB_SLOW_QUERY_LOG="OFF")
        result["commands"] = commands
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
            execute(connection, "CREATE DATABASE like_bench CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")
            execute(connection, "USE like_bench")
            execute(connection, DDL)
            start = time.perf_counter()
            execute(connection, f"LOAD DATA LOCAL INFILE {connection.escape(str(dataset))} INTO TABLE texts CHARACTER SET utf8mb4")
            if execute(connection, "SHOW WARNINGS"):
                raise RuntimeError(f"{name}: LOAD DATA warnings")
            load_s = time.perf_counter() - start
            print(f"{name}: loaded {args.rows} rows in {load_s:.2f}s", flush=True)
            for col, idx in [("ascii_ci", "idx_ci"), ("ascii_bin", "idx_bin"), ("unicode_ci", "idx_unicode")]:
                execute(connection, f"CREATE INDEX {idx} ON texts ({col})")
            analyze = execute(connection, "ANALYZE TABLE texts")
            result["servers"][name] = dict(version=execute(connection, "SELECT VERSION()"), load_s=load_s,
                                           analyze=analyze, variables=execute(connection, "SHOW VARIABLES"),
                                           status_before=execute(connection, "SHOW GLOBAL STATUS"),
                                           table_status=execute(connection, "SHOW TABLE STATUS LIKE 'texts'"))
            dump(directory / "results.json", result)
        for case in result["cases"]:
            case.update(sql=sql_for(case), fingerprint_sql=sql_for(case, True), engines={})
            for name, connection in connections.items():
                fingerprint = tuple(str(value) for value in execute(connection, case["fingerprint_sql"])[0])
                case["engines"][name] = dict(fingerprint=fingerprint, explain=execute(connection, "EXPLAIN " + case["sql"]), samples_ms=[])
            case["correct"] = case["engines"]["mysql"]["fingerprint"] == case["engines"]["mydb"]["fingerprint"]
            print(f"verify {case['name']}: {case['correct']}, mysql={case['engines']['mysql']['fingerprint']}, mydb={case['engines']['mydb']['fingerprint']}", flush=True)
            for _ in range(args.warmups):
                for connection in connections.values():
                    execute(connection, case["sql"])
            for repeat in range(args.samples):
                order = ["mysql", "mydb"] if repeat % 2 == 0 else ["mydb", "mysql"]
                for name in order:
                    start = time.perf_counter_ns()
                    rows = execute(connections[name], case["sql"])
                    elapsed = (time.perf_counter_ns() - start) / 1e6
                    if str(rows[0][0]) != case["engines"][name]["fingerprint"][0]:
                        raise RuntimeError("timed count disagrees with fingerprint")
                    case["engines"][name]["samples_ms"].append(elapsed)
            for metrics in case["engines"].values():
                samples = metrics["samples_ms"]
                metrics.update(median_ms=statistics.median(samples), min_ms=min(samples),
                               p95_ms=sorted(samples)[math.ceil(.95 * len(samples)) - 1])
            case["mysql_over_mydb"] = case["engines"]["mysql"]["median_ms"] / case["engines"]["mydb"]["median_ms"] if case["correct"] else None
            print(f"  mysql={case['engines']['mysql']['median_ms']:.3f}ms mydb={case['engines']['mydb']['median_ms']:.3f}ms ratio={case['mysql_over_mydb']}", flush=True)
            dump(directory / "results.json", result)
        for name, connection in connections.items():
            result["servers"][name]["status_after"] = execute(connection, "SHOW GLOBAL STATUS")
        result["completed"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
        dump(directory / "results.json", result)
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
