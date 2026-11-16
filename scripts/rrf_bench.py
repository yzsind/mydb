#!/usr/bin/env python3
"""Reproducible RRF correctness, latency, throughput and operator profiling.

Requires PyMySQL[rsa]. See docs-internal/rrf-performance.md for the workload
format, external datasets, reproducible before/after runs and limitations.
No embeddings are generated during measured queries. All paths default to /tmp/mydb.
"""

import argparse
import csv
import dataclasses
import hashlib
import importlib.metadata
import json
import math
import multiprocessing as mp
import os
from pathlib import Path
import platform
import queue
import random
import re
import statistics
import subprocess
import time
import traceback
import uuid


def execute(connection, sql, params=None):
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        return cursor.fetchall() if cursor.description else ()


def connect(settings, database=None):
    import pymysql
    return pymysql.connect(**settings, database=database, autocommit=True,
                           charset="utf8mb4", ssl_disabled=True,
                           connect_timeout=15, read_timeout=300, write_timeout=300)


def digest(rows):
    return hashlib.sha256(repr(rows).encode()).hexdigest()


def percentile(values, fraction):
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)]


def metrics(samples, elapsed):
    if not samples:
        raise RuntimeError("no successful measured queries")
    return dict(operations=len(samples), elapsed_seconds=elapsed,
                qps=len(samples) / elapsed, mean_ms=statistics.mean(samples),
                p50_ms=statistics.median(samples), p95_ms=percentile(samples, .95),
                p99_ms=percentile(samples, .99), max_ms=max(samples))


def oracle(branches, weights, constant, limit=None, offset=0):
    """Independent exact fusion for scalar keys using binary text collations."""
    state = {}
    for rows, weight in zip(branches, weights):
        if not weight:
            continue
        seen = set()
        for rank, identity in enumerate(rows, 1):
            identity = tuple(identity)
            if identity in seen:
                continue
            seen.add(identity)
            score, best, matches = state.get(identity, (0.0, rank, 0))
            state[identity] = (score + weight / (constant + rank), min(best, rank), matches + 1)
    result = [(*key, *value) for key, value in state.items()]
    result.sort(key=lambda row: (-row[-3], row[-2], row[:-3]))
    return result[offset:None if limit is None else offset + limit]


def validate_rows(actual, expected, width):
    if len(actual) != len(expected):
        raise AssertionError(f"row count: {len(actual)} != {len(expected)}")
    for row, ref in zip(actual, expected):
        if (tuple(row[:width]) != tuple(ref[:width]) or
                tuple(row[width + 1:width + 3]) != tuple(ref[width + 1:width + 3]) or
                not math.isclose(float(row[width]), ref[width], rel_tol=1e-12, abs_tol=0)):
            raise AssertionError(f"fusion mismatch: {row[:width + 3]} != {ref}")


def verify(connection, case):
    actual = execute(connection, case["sql"])
    result = dict(result_rows=len(actual), result_sha256=digest(actual))
    if "branches" in case:
        branches = [execute(connection, sql) if w else ()
                    for sql, w in zip(case["branches"], case["weights"])]
        expected = oracle(branches, case["weights"], case["constant"],
                          case["limit"], case.get("offset", 0))
        validate_rows(actual, expected, case["width"])
        if case.get("details"):
            for row in actual:
                details = json.loads(row[-1])
                if len(details) != len(branches):
                    raise AssertionError("explanation branch count")
                score = sum(float(item["contribution"]) for item in details)
                if not math.isclose(score, float(row[case["width"]]), rel_tol=1e-12):
                    raise AssertionError("explanation contributions")
        result.update(oracle="independent Python RRF over actual ordered child lists",
                      input_rows=[len(rows) for rows in branches])
    else:
        result["oracle"] = "stable result digest only; no independent query oracle"
    result["explain"] = execute(connection, "EXPLAIN FORMAT=TREE " + case["sql"])
    result["analyze"] = execute(connection, "EXPLAIN ANALYZE FORMAT=JSON " + case["sql"])
    return result


def worker(settings, database, case, duration, requests, warmup, ready, start, results, prepared=False):
    connection = None
    try:
        connection = connect(settings, database)
        sql = case["sql"]
        if prepared:
            execute(connection, "PREPARE rrf_bench_statement FROM %s", (sql,))
            sql = "EXECUTE rrf_bench_statement"
        expected_digest = case.get("expected_digest")
        for _ in range(warmup):
            observed = digest(execute(connection, sql))
            if expected_digest is not None and observed != expected_digest:
                raise AssertionError("worker result differs from independent preflight")
            expected_digest = observed
        ready.put(None)
        if not start.wait(120):
            raise RuntimeError("worker start timeout")
        deadline = time.perf_counter() + duration
        samples, errors = [], []
        while (len(samples) < requests if requests else time.perf_counter() < deadline):
            before = time.perf_counter_ns()
            try:
                rows = execute(connection, sql)
                elapsed = (time.perf_counter_ns() - before) / 1e6
                if expected_digest is not None and digest(rows) != expected_digest:
                    raise AssertionError("query result changed during read-only load")
                samples.append(elapsed)
            except Exception:
                errors.append(traceback.format_exc())
                break
        results.put(dict(samples_ms=samples, errors=errors, finished=time.perf_counter()))
    except Exception:
        error = traceback.format_exc()
        ready.put(error)
        results.put(dict(samples_ms=[], errors=[error], finished=time.perf_counter()))
    finally:
        if connection is not None:
            connection.close()


def process_resources(pid):
    if pid is None:
        return None
    fields = subprocess.check_output(["ps", "-p", str(pid), "-o", "rss=,time="], text=True).split()
    days, clock = fields[1].split("-", 1) if "-" in fields[1] else ("0", fields[1])
    parts = clock.split(":")
    cpu_seconds = int(days) * 86400 + sum(float(part) * 60 ** i for i, part in enumerate(reversed(parts)))
    return dict(rss_kib=int(fields[0]), cpu_seconds=cpu_seconds)


def benchmark(settings, database, case, concurrency, args):
    # Processes isolate PyMySQL's Python decoding from the parent interpreter GIL.
    context = mp.get_context("spawn")
    ready, results, start = context.Queue(), context.Queue(), context.Event()
    processes = [context.Process(target=worker, args=(settings, database, case, args.duration,
                 args.requests, args.warmup, ready, start, results, args.sql_prepare)) for _ in range(concurrency)]
    received = []
    try:
        for process in processes:
            process.start()
        for _ in processes:
            error = ready.get(timeout=120)
            if error:
                raise RuntimeError(error)
        resources = []
        initial = process_resources(args.server_pid)
        if initial:
            resources.append(initial)
        began = time.perf_counter()
        start.set()
        last_resource = began
        while len(received) < concurrency:
            try:
                received.append(results.get(timeout=.1))
            except queue.Empty:
                if any(p.exitcode not in (None, 0) for p in processes):
                    raise RuntimeError("load worker exited unexpectedly")
                if time.perf_counter() - began > max(300, args.duration + 300):
                    raise RuntimeError("load worker timed out")
            if args.server_pid and time.perf_counter() - last_resource >= .5:
                resources.append(process_resources(args.server_pid))
                last_resource = time.perf_counter()
        elapsed = max(result["finished"] for result in received) - began
        samples = [value for result in received for value in result["samples_ms"]]
        errors = [error for result in received for error in result["errors"]]
        if errors:
            raise RuntimeError("load errors: " + "\n".join(errors))
        result = dict(name=case["name"], concurrency=concurrency, errors=errors,
                      **metrics(samples, elapsed), samples_ms=samples,
                      worker_operations=[len(result["samples_ms"]) for result in received])
        if initial:
            final = process_resources(args.server_pid)
            resources.append(final)
            result["server"] = dict(peak_sampled_rss_mib=max(r["rss_kib"] for r in resources) / 1024,
                                    cpu_seconds=final["cpu_seconds"] - initial["cpu_seconds"])
            result["server"]["cpu_percent"] = result["server"]["cpu_seconds"] / elapsed * 100
        return result
    finally:
        for process in processes:
            if process.pid is not None:
                process.join(timeout=2)
                if process.is_alive():
                    process.terminate()
                    process.join()
        ready.close()
        results.close()


@dataclasses.dataclass
class CaseBuilder:
    table: str
    rows: int
    args: object

    def fusion(self, name, branches, keys, weights=None, details=False, limit=None, offset=0):
        weights = weights or [1.0] * len(branches)
        limit = self.args.topk if limit is None else limit
        constant = self.args.rank_constant
        options = (f" WITH(RANK_CONSTANT={constant},WEIGHTS=({','.join(map(str, weights))}),"
                   f"SCORE_DETAILS={'TRUE' if details else 'FALSE'})")
        projection = ",".join(keys + ["rrf_score", "best_rank", "match_count"] +
                              (["rrf_details"] if details else []))
        order = ",".join(["rrf_score DESC", "best_rank"] + keys)
        sql = (f"SELECT {projection} FROM ({' RRF '.join(f'({b})' for b in branches)}{options}) h "
               f"ORDER BY {order} LIMIT {limit} OFFSET {offset}")
        return dict(name=f"n{self.rows}/{name}", sql=sql, branches=branches, weights=weights,
                    width=len(keys), constant=constant, limit=limit, offset=offset, details=details)

    def cases(self):
        cases = []
        identities = {
            "int": ("id", ["id"]),
            "composite": ("tenant,id", ["tenant", "id"]),
            "text": ("skey", ["skey"]),
            "decimal": ("dkey", ["dkey"]),
            "binary": ("bkey", ["bkey"]),
            "datetime": ("tkey,id", ["tkey", "id"]),
            "float": ("fkey", ["fkey"]),
            "expression": ("CAST(id AS DECIMAL(20,2))+0.25 AS k", ["k"]),
        }
        if "relational" in self.args.scenarios:
            for count in self.args.branches:
                for candidates in self.args.candidates:
                    for kind in self.args.key_types:
                        projection, keys = identities[kind]
                        branches = [f"SELECT {projection} FROM {self.table} ORDER BY "
                                    f"{'a' if i % 2 == 0 else 'b'},id LIMIT {candidates}"
                                    for i in range(count)]
                        for details in self.args.details:
                            name = f"relational/b{count}/k{candidates}/{kind}/details{int(details)}"
                            cases.append(self.fusion(name, branches, keys, details=details))
        if "edge" in self.args.scenarios:
            k = max(self.args.candidates)
            ascending = f"SELECT id FROM {self.table} ORDER BY id LIMIT {k}"
            descending = f"SELECT id FROM {self.table} ORDER BY id DESC LIMIT {k}"
            duplicate = f"SELECT tenant FROM {self.table} ORDER BY id LIMIT {k}"
            for name, branches, keys, weights, limit, offset in [
                ("overlap", [ascending, ascending], ["id"], [1, 1], self.args.topk, 0),
                ("disjoint", [ascending, descending], ["id"], [1, 2], self.args.topk, 0),
                ("duplicates", [duplicate, duplicate], ["tenant"], [1, 2], self.args.topk, 0),
                ("disabled", [ascending, descending], ["id"], [1, 0], self.args.topk, 0),
                ("empty", [ascending.replace(f"LIMIT {k}", "LIMIT 0"), descending], ["id"], [1, 1], self.args.topk, 0),
                ("offset", [ascending, descending], ["id"], [1, 2], self.args.topk, 13),
                ("all", [ascending, descending], ["id"], [1, 1], self.rows * 2, 0),
            ]:
                cases.append(self.fusion(f"edge/{name}", branches, keys, weights, True, limit, offset))
        if "hybrid" in self.args.scenarios:
            for fraction in self.args.selectivities:
                condition = f"id < {int(self.rows * fraction)}"
                queries = getattr(self.args, "queries", None) or [dict(text="database engine",
                    vector=[.25 + (d % 7) / 10 for d in range(self.args.dim)])]
                for query_index, query in enumerate(queries):
                    from pymysql.converters import escape_string
                    suffix = f"/q{query_index}" if len(queries) > 1 else ""
                    vector = json.dumps(query["vector"], separators=(",", ":"))
                    k = max(self.args.candidates)
                    match = f"MATCH(body) AGAINST('{escape_string(query['text'])}')"
                    lexical = (f"SELECT id FROM {self.table} WHERE {condition} AND {match} "
                               f"ORDER BY {match} DESC,id LIMIT {k}")
                    hint = {"hnsw": "HNSW_EF(200)", "ivf": "IVF_NPROBE(8)", "exact": ""}[self.args.index]
                    distance = {"l2": "L2_DISTANCE", "cosine": "COSINE_DISTANCE",
                                "dot": "MAX_INNER_PRODUCT_DISTANCE"}[self.args.metric]
                    dense = (f"SELECT /*+ {hint} */ id FROM {self.table} WHERE {condition} "
                             f"ORDER BY {distance}(v,TO_VECTOR('{vector}')),id LIMIT {k}")
                    for branch_name, branch in [("bm25", lexical), ("dense", dense)]:
                        cases.append(dict(name=f"n{self.rows}/{branch_name}/filter{fraction}{suffix}", sql=branch))
                    for details in self.args.details:
                        cases.append(self.fusion(f"hybrid/filter{fraction}/details{int(details)}{suffix}",
                                                 [lexical, dense], ["id"], [1, 2], details))
        return cases


def input_rows(args, count):
    rng = random.Random(args.seed)
    if args.dataset:
        path = Path(args.dataset)
        with path.open(newline="") as stream:
            records = csv.DictReader(stream) if path.suffix.lower() == ".csv" else (json.loads(line) for line in stream if line.strip())
            for i, record in enumerate(records):
                if i >= count:
                    break
                vector = record.get(args.vector_field, [])
                if isinstance(vector, str):
                    vector = json.loads(vector)
                yield i, str(record.get(args.text_field, "")), vector
        return
    for i in range(count):
        topic = "database engine storage transaction" if i % 5 else "retrieval ranking vector search"
        if args.distribution == "multilingual":
            topic += " 数据库 检索 引擎 recherche datenbank"
        repeats = 1 + (min(20, int(rng.paretovariate(1.5))) if args.distribution == "skewed" else i % 4)
        yield i, (topic + " ") * repeats, ([rng.uniform(-1, 1) for _ in range(args.dim)]
                                           if "hybrid" in args.scenarios else [])


def prepare(connection, args):
    database = "rrf_perf_" + uuid.uuid4().hex[:12]
    execute(connection, f"CREATE DATABASE {database}")
    execute(connection, f"USE {database}")
    manifest = dict(version=1, database=database, cases=[], datasets=[], seed=args.seed,
                    parameters={k: v for k, v in vars(args).items() if k != "password"})
    # Persist ownership immediately, so failed preparation can be inspected/cleaned.
    save(args.manifest, manifest)
    for count in args.scales:
        table = f"docs_{count}"
        vector_column = f",v VECTOR({args.dim}) NOT NULL" if "hybrid" in args.scenarios else ""
        execute(connection, f"CREATE TABLE {table}(id BIGINT PRIMARY KEY,tenant INT NOT NULL,"
                "a BIGINT NOT NULL,b BIGINT NOT NULL,skey VARCHAR(96) COLLATE utf8mb4_0900_bin NOT NULL,"
                "dkey DECIMAL(20,2) NOT NULL,bkey VARBINARY(32) NOT NULL,tkey DATETIME NOT NULL,"
                f"fkey DOUBLE NOT NULL,body TEXT NOT NULL{vector_column})")
        began = time.perf_counter()
        batch, loaded = [], 0
        data_hash = hashlib.sha256()
        for i, body, vector in input_rows(args, count):
            if vector_column and (len(vector) != args.dim or any(not math.isfinite(float(v)) for v in vector)):
                raise ValueError(f"row {i}: invalid vector dimension or finite domain")
            row = (i, i % 32, i * 7919 % count, i * 104729 % count, f"key{i:020d}",
                   f"{i}.25", f"bin{i:020d}".encode(), "2026-01-01 12:34:56", i + .5, body)
            if vector_column:
                row += (json.dumps(vector, separators=(",", ":")),)
            data_hash.update(repr(row).encode())
            batch.append(row)
            if len(batch) >= args.batch_size:
                with connection.cursor() as cursor:
                    cursor.executemany(f"INSERT INTO {table} VALUES ({','.join(['%s'] * len(row))})", batch)
                loaded += len(batch)
                batch.clear()
        if batch:
            with connection.cursor() as cursor:
                cursor.executemany(f"INSERT INTO {table} VALUES ({','.join(['%s'] * len(batch[0]))})", batch)
            loaded += len(batch)
        if loaded != count:
            raise ValueError(f"dataset contains {loaded} rows, requested {count}")
        load_seconds = time.perf_counter() - began
        began = time.perf_counter()
        execute(connection, f"CREATE INDEX ia ON {table}(a,id)")
        execute(connection, f"CREATE INDEX ib ON {table}(b,id)")
        if vector_column:
            execute(connection, f"CREATE FULLTEXT INDEX ft ON {table}(body)")
            if args.index != "exact":
                options = "m=16,ef_construction=100" if args.index == "hnsw" else "nlist=32,kmeans_iters=5"
                execute(connection, f"CREATE INDEX vx ON {table}(v) USING {args.index.upper()} "
                                    f"WITH(metric={args.metric},{options})")
        execute(connection, f"ANALYZE TABLE {table}")
        manifest["datasets"].append(dict(table=table, rows=count, sha256=data_hash.hexdigest(),
                                         load_seconds=load_seconds, index_seconds=time.perf_counter() - began))
        manifest["cases"].extend(CaseBuilder(table, count, args).cases())
        save(args.manifest, manifest)
        print(f"prepared {table}: {count} rows, {len(manifest['cases'])} cases total", flush=True)
    return manifest


def save(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n")
    temporary.replace(path)


def save_summary(path, results):
    fields = ["name", "concurrency", "repeat", "operations", "elapsed_seconds", "qps",
              "mean_ms", "p50_ms", "p95_ms", "p99_ms", "max_ms"]
    with Path(path).with_suffix(".csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)


def save_comparison_summary(path, results):
    with Path(path).with_suffix(".csv").open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["name", "concurrency", "before_qps", "after_qps", "qps_change_percent",
                         "before_p50_ms", "after_p50_ms", "p50_change_percent",
                         "before_p99_ms", "after_p99_ms"])
        for result in results:
            before, after = result["before"], result["after"]
            writer.writerow([result["name"], result["concurrency"], before["qps"], after["qps"],
                             result["qps_change_percent"], before["p50_ms"], after["p50_ms"],
                             result["p50_change_percent"], before["p99_ms"], after["p99_ms"]])


def compare_reports(before, after):
    for report in (before, after):
        if report["status"] != "passed":
            raise ValueError("cannot compare an incomplete or failed run")
    fingerprints = lambda report: [(d["rows"], d["sha256"]) for d in report["manifest"].get("datasets", [])]
    if fingerprints(before) != fingerprints(after):
        raise ValueError("dataset fingerprints differ")
    queries = lambda report: {c["name"]: c["sql"] for c in report["manifest"]["cases"]}
    if queries(before) != queries(after):
        raise ValueError("workload SQL differs")
    result_hashes = lambda report: {name: value["result_sha256"]
        for name, value in report.get("validation", {}).items()}
    if result_hashes(before) != result_hashes(after):
        raise ValueError("validated query result fingerprints differ")
    for key in ("requests", "duration", "warmup", "sql_prepare"):
        default = False if key == "sql_prepare" else None
        if before.get("parameters", {}).get(key, default) != after.get("parameters", {}).get(key, default):
            raise ValueError(f"measurement parameter differs: {key}")
    def grouped(report):
        groups = {}
        for result in report["results"]:
            groups.setdefault((result["name"], result["concurrency"]), []).append(result)
        return groups
    left, right = grouped(before), grouped(after)
    if not left or not right:
        raise ValueError("reports contain no measured queries")
    if left.keys() != right.keys():
        raise ValueError("measured case/concurrency sets differ")
    result = []
    for (name, concurrency), baseline in left.items():
        current = right[(name, concurrency)]
        old = {key: statistics.median(r[key] for r in baseline) for key in ("qps", "p50_ms", "p99_ms")}
        new = {key: statistics.median(r[key] for r in current) for key in old}
        result.append(dict(name=name, concurrency=concurrency, before=old, after=new,
                           repeats=[len(baseline), len(current)],
                           qps_change_percent=(new["qps"] / old["qps"] - 1) * 100,
                           p50_change_percent=(new["p50_ms"] / old["p50_ms"] - 1) * 100))
    return dict(aggregation="median across repetitions; negative latency change is improvement", results=result)


def numbers(value, convert=int):
    return [convert(item) for item in value.split(",")]


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=("all", "prepare", "run"), default="all")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=4837)
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default=os.getenv("MYDB_PASSWORD", "root"))
    parser.add_argument("--scales", type=lambda s: numbers(s), default=[1000, 10000])
    parser.add_argument("--branches", type=lambda s: numbers(s), default=[2, 4, 8])
    parser.add_argument("--candidates", type=lambda s: numbers(s), default=[20, 200, 2000])
    parser.add_argument("--concurrency", type=lambda s: numbers(s), default=[1, 4, 8])
    parser.add_argument("--key-types", type=lambda s: s.split(","), default=["int", "composite", "text", "decimal", "binary", "datetime", "float", "expression"])
    parser.add_argument("--scenarios", type=lambda s: s.split(","), default=["relational", "edge", "hybrid"])
    parser.add_argument("--details", choices=("off", "on", "both"), default="both")
    parser.add_argument("--selectivities", type=lambda s: numbers(s, float), default=[1, .1, .01])
    parser.add_argument("--dim", type=int, default=64)
    parser.add_argument("--index", choices=("hnsw", "ivf", "exact"), default="hnsw")
    parser.add_argument("--metric", choices=("l2", "cosine", "dot"), default="l2")
    parser.add_argument("--topk", type=int, default=20)
    parser.add_argument("--rank-constant", type=int, default=60)
    parser.add_argument("--seed", type=int, default=20261026)
    parser.add_argument("--distribution", choices=("uniform", "skewed", "multilingual"), default="uniform")
    parser.add_argument("--dataset", help="JSONL or CSV corpus; embeddings must already exist")
    parser.add_argument("--text-field", default="text")
    parser.add_argument("--vector-field", default="vector")
    parser.add_argument("--queries", help="JSONL query objects with text and precomputed vector")
    parser.add_argument("--batch-size", type=int, default=250)
    parser.add_argument("--duration", type=float, default=3)
    parser.add_argument("--requests", type=int, default=0, help="fixed requests per worker; overrides duration")
    parser.add_argument("--warmup", type=int, default=3)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--sql-prepare", action="store_true", help="prepare once per connection, time SQL EXECUTE (not binary protocol)")
    parser.add_argument("--only", default=".", help="regular expression matching case names")
    parser.add_argument("--server-pid", type=int, help="sample local server RSS and cumulative CPU")
    parser.add_argument("--keep-data", action="store_true")
    parser.add_argument("--manifest", default="/tmp/mydb/rrf-perf/manifest.json")
    parser.add_argument("--output", default="/tmp/mydb/rrf-perf/results.json")
    parser.add_argument("--compare", nargs=2, metavar=("BEFORE", "AFTER"), help="compare completed reports with identical datasets and SQL")
    args = parser.parse_args()
    if (any(n < 1 for n in args.scales + args.concurrency + args.candidates) or
            any(not 2 <= n <= 8 for n in args.branches) or args.dim < 1 or args.batch_size < 1 or
            not math.isfinite(args.duration) or args.duration <= 0 or
            args.requests < 0 or args.warmup < 1 or args.repeats < 1 or
            (args.server_pid is not None and args.server_pid <= 0) or
            args.topk < 0 or not 0 <= args.rank_constant <= 4294967295 or
            any(not 0 <= n <= 1 for n in args.selectivities)):
        parser.error("invalid workload bounds")
    if set(args.scenarios) - {"relational", "edge", "hybrid"}:
        parser.error("unknown scenario")
    if set(args.key_types) - {"int", "composite", "text", "decimal", "binary", "datetime", "float", "expression"}:
        parser.error("unknown key type")
    if args.queries:
        args.queries = [json.loads(line) for line in Path(args.queries).read_text().splitlines() if line.strip()]
        if not args.queries or any(not isinstance(q.get("text"), str) or
                len(q.get("vector", [])) != args.dim or
                any(not math.isfinite(float(v)) for v in q["vector"]) for q in args.queries):
            parser.error("queries must contain text and finite vectors matching --dim")
    args.details = {"off": [False], "on": [True], "both": [False, True]}[args.details]
    return args


def main():
    args = arguments()
    if args.compare:
        result = compare_reports(*(json.loads(Path(path).read_text()) for path in args.compare))
        save(args.output, result)
        save_comparison_summary(args.output, result["results"])
        print(f"compared {len(result['results'])} case/concurrency groups: {args.output}")
        return
    settings = dict(host=args.host, port=args.port, user=args.user, password=args.password)
    report = dict(version=1, status="running", environment=dict(platform=platform.platform(),
                  python=platform.python_version(), pymysql=importlib.metadata.version("PyMySQL"),
                  client_cpus=os.cpu_count(), script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()),
                  parameters={k: v for k, v in vars(args).items() if k != "password"},
                  validation={}, results=[], measurement="closed-loop; persistent connections; end-to-end fetch/decode; no retries")
    connection, manifest = None, None
    try:
        connection = connect(settings)
        manifest = (prepare(connection, args) if args.phase != "run" else
                    json.loads(Path(args.manifest).read_text()))
        report["manifest"] = manifest
        execute(connection, "USE " + manifest["database"])
        report["server_version"] = execute(connection, "SELECT VERSION()")
        if args.phase != "prepare":
            selected = [case for case in manifest["cases"] if re.search(args.only, case["name"])]
            if not selected:
                raise ValueError("no cases selected")
            names = [case["name"] for case in selected]
            if len(names) != len(set(names)):
                raise ValueError("duplicate case names")
            for case in selected:
                report["validation"][case["name"]] = verify(connection, case)
                load_case = {**case, "expected_digest": report["validation"][case["name"]]["result_sha256"]}
                for concurrency in args.concurrency:
                    for repeat in range(args.repeats):
                        result = benchmark(settings, manifest["database"], load_case, concurrency, args)
                        result["repeat"] = repeat
                        report["results"].append(result)
                        save(args.output, report)
                        print(f"{case['name']} c={concurrency} r={repeat}: {result['qps']:.1f} QPS "
                              f"p50={result['p50_ms']:.3f} p99={result['p99_ms']:.3f} ms", flush=True)
        report["status"] = "passed"
    except BaseException:
        report["status"] = "failed"
        report["error"] = traceback.format_exc()
        raise
    finally:
        try:
            if connection is not None:
                try:
                    if args.phase == "all" and not args.keep_data and manifest:
                        execute(connection, "DROP DATABASE " + manifest["database"])
                finally:
                    connection.close()
        except Exception:
            report["status"] = "failed"
            report["cleanup_error"] = traceback.format_exc()
            raise
        finally:
            save(args.output, report)
            save_summary(args.output, report["results"])


if __name__ == "__main__":
    main()
