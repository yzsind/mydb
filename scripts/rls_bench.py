#!/usr/bin/env python3
"""Compare local RLS paths against the original binary and equivalent SQL.

Defaults are the requested seven 30-second interleaved rounds. Shorter runs
are marked diagnostic only. Requires the same Python dependencies as
test_native_readiness.py. All artifacts remain below /tmp/mydb/rls.
"""
import argparse
import hashlib
import json
from pathlib import Path
import statistics
import random
import subprocess
import tempfile
import time

import pymysql
from test_native_readiness import Servers, free_port, query


def seed(connection, count, protected, skew=False):
    # Keep heap, index and background-maintenance load identical. Only the
    # candidate's security metadata differs from the preserved baseline.
    tables = ["plain", "secured", "mapped", "contextual", "multi"]
    for table in tables:
        query(connection, f"CREATE TABLE {table}(id BIGINT PRIMARY KEY,tenant_id INT,v BIGINT,payload VARCHAR(40),KEY tenant_key(tenant_id,id))")
        with connection.cursor() as cursor:
            for first in range(1, count + 1, 2000):
                cursor.executemany(f"INSERT INTO {table} VALUES(%s,%s,%s,%s)",
                                   [(i, 42 if skew and i % 10 else i % 100, i * 3, f"value-{i}")
                                    for i in range(first, min(count + 1, first + 2000))])
    query(connection, "CREATE USER app IDENTIFIED BY 'secret'")
    query(connection, "GRANT SELECT,INSERT,UPDATE,DELETE ON test.* TO app")
    query(connection, "CREATE TABLE membership(tenant_id INT PRIMARY KEY)")
    query(connection, "INSERT INTO membership VALUES(42)")
    for suffix in ["plain", "secured"]:
        query(connection, f"CREATE TABLE parent_{suffix}(id INT PRIMARY KEY,tenant_id INT)")
        query(connection, f"CREATE TABLE child_{suffix}(id INT PRIMARY KEY,parent_id INT,tenant_id INT,"
                          f"FOREIGN KEY(parent_id) REFERENCES parent_{suffix}(id) ON DELETE CASCADE)")
        query(connection, f"INSERT INTO parent_{suffix} VALUES(42,42)")
        query(connection, f"INSERT INTO child_{suffix} VALUES(42,42,42),(43,42,42)")
    if protected:
        for sql in ["ALTER TABLE secured ENABLE ROW LEVEL SECURITY",
                    "CREATE POLICY tenant ON secured USING(tenant_id=42)",
                    "ALTER TABLE contextual ENABLE ROW LEVEL SECURITY",
                    "CREATE POLICY context_tenant ON contextual USING(tenant_id=CAST(RLS_CONTEXT('tenant_id') AS UNSIGNED))",
                    "ALTER TABLE multi ENABLE ROW LEVEL SECURITY",
                    "CREATE POLICY first_tenant ON multi USING(tenant_id=42)",
                    "CREATE POLICY second_tenant ON multi USING(tenant_id=43)",
                    "CREATE POLICY bounds ON multi AS RESTRICTIVE USING(v>=0)",
                    "GRANT RLS_CONTEXT_ADMIN ON *.* TO app",
                    "GRANT ROW_SECURITY_ADMIN ON *.* TO app",
                    "ALTER TABLE mapped ENABLE ROW LEVEL SECURITY",
                    "CREATE POLICY membership ON mapped USING(EXISTS(SELECT 1 FROM membership m WHERE m.tenant_id=mapped.tenant_id))"]:
            query(connection, sql)
        for table in ["parent_secured", "child_secured"]:
            query(connection, f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
            query(connection, f"CREATE POLICY tenant ON {table} USING(tenant_id=42)")


def run_query(connection, sql, transactional=False):
    if not transactional:
        return query(connection, sql)
    query(connection, "BEGIN")
    try:
        return query(connection, sql)
    finally:
        query(connection, "ROLLBACK")


def measure(connection, sql, seconds, expected, transactional=False):
    samples = []
    reservoir = random.Random(20261104)
    count = 0
    start = time.perf_counter()
    while True:
        tick = time.perf_counter()
        actual = run_query(connection, sql, transactional)
        elapsed = time.perf_counter() - tick
        assert actual == expected, (sql, actual, expected)
        count += 1
        if len(samples) < 10000:
            samples.append(elapsed * 1000)
        else:
            index = reservoir.randrange(count)
            if index < len(samples):
                samples[index] = elapsed * 1000
        if time.perf_counter() - start >= seconds:
            break
    duration = time.perf_counter() - start
    samples.sort()
    return {"operations": count, "seconds": duration, "qps": count / duration,
            "p50_ms": samples[int((len(samples) - 1) * .50)],
            "p95_ms": samples[int((len(samples) - 1) * .95)],
            "p99_ms": samples[int((len(samples) - 1) * .99)]}


def process_sample(processes):
    return subprocess.check_output(["ps", "-p", ",".join(str(p.pid) for p in processes),
                                    "-o", "pid=,rss=,time="], text=True)


def active_builds():
    names = subprocess.check_output(["ps", "-axo", "comm="], text=True).splitlines()
    builders = {"cargo", "cargo-nextest", "rustc", "cc", "clang", "clang++", "ld"}
    return sorted({Path(name.strip()).name for name in names} & builders)


def wait_for_builds():
    announced = False
    while builds := active_builds():
        if not announced:
            print("Waiting for local builds to finish:", builds, flush=True)
            announced = True
        time.sleep(1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--rows", type=int, choices=[100000, 1000000], default=100000)
    parser.add_argument("--rounds", type=int, default=7)
    parser.add_argument("--seconds", type=float, default=30)
    parser.add_argument("--case", action="append", help="Run a named workload; repeat to select several")
    parser.add_argument("--tenant-skew", action="store_true", help="Assign 90 percent of rows to tenant 42")
    parser.add_argument("--memory-target", default="4G", help="Identical memory target for each server")
    args = parser.parse_args()
    base = Path("/tmp/mydb/rls/performance")
    base.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="run-", dir=base))
    old_root, new_root = root / "before", root / "after"
    old_root.mkdir()
    new_root.mkdir()
    before, after = Servers(args.baseline.resolve(), old_root), Servers(args.candidate.resolve(), new_root)
    clients = []
    result = {"rows": args.rows, "rounds": args.rounds, "seconds": args.seconds,
              "baseline_sha256": hashlib.sha256(args.baseline.read_bytes()).hexdigest(),
              "candidate_sha256": hashlib.sha256(args.candidate.read_bytes()).hexdigest(),
              "cargo_toml_sha256": hashlib.sha256(Path("Cargo.toml").read_bytes()).hexdigest(),
              "acceptance_duration": args.rounds >= 7 and args.seconds >= 30,
              "tenant_skew": args.tenant_skew,
              "selected_cases": args.case,
              "memory_target": args.memory_target,
              "coverage": "single-connection reads, joins, parallel scan hints, transactional DML and cascade; specialized retrieval indexes are separate",
              "cases": {}}
    try:
        memory_options = {"MYDB_MEMORY_TARGET": args.memory_target}
        old_process, old = before.start("database", free_port(), memory_options)
        port = free_port()
        new_process, new = after.start("database", port, memory_options)
        seed(old, args.rows, False, args.tenant_skew)
        seed(new, args.rows, True, args.tenant_skew)
        app = pymysql.connect(host="127.0.0.1", port=port, user="app", password="secret",
                              database="test", autocommit=True, read_timeout=300)
        clients.append(app)
        query(app, "SET RLS CONTEXT 'tenant_id'='42'")
        cases = [
            ("disabled_point", (old, "SELECT payload FROM plain WHERE id=42"), (new, "SELECT payload FROM plain WHERE id=42"), .02),
            ("super_point", (new, "SELECT payload FROM plain WHERE id=42"), (new, "SELECT payload FROM secured WHERE id=42"), .02),
            ("simple_point", (app, "SELECT payload FROM plain WHERE id=42 AND tenant_id=42"), (app, "SELECT payload FROM secured WHERE id=42"), .05),
            ("simple_scan", (app, "SELECT COUNT(*),SUM(v) FROM plain WHERE tenant_id=42"), (app, "SELECT COUNT(*),SUM(v) FROM secured"), .05),
            ("context_scan", (app, "SELECT COUNT(*),SUM(v) FROM plain WHERE tenant_id=42"), (app, "SELECT COUNT(*),SUM(v) FROM contextual"), .05),
            ("subquery_scan", (app, "SELECT COUNT(*),SUM(v) FROM plain WHERE EXISTS(SELECT 1 FROM membership m WHERE m.tenant_id=plain.tenant_id)"), (app, "SELECT COUNT(*),SUM(v) FROM mapped"), .10),
            ("multi_scan", (app, "SELECT COUNT(*),SUM(v) FROM plain WHERE (tenant_id=42 OR tenant_id=43) AND v>=0"), (app, "SELECT COUNT(*),SUM(v) FROM multi"), .10),
            ("disabled_scan", (old, "SELECT COUNT(*),SUM(v) FROM plain"), (new, "SELECT COUNT(*),SUM(v) FROM plain"), .02),
            ("super_scan", (new, "SELECT COUNT(*),SUM(v) FROM plain"), (new, "SELECT COUNT(*),SUM(v) FROM secured"), .02),
            ("simple_range", (app, "SELECT id,v FROM plain WHERE tenant_id=42 AND id BETWEEN 100 AND 10000 ORDER BY id"), (app, "SELECT id,v FROM secured WHERE id BETWEEN 100 AND 10000 ORDER BY id"), .05),
            ("simple_join", (app, "SELECT COUNT(*),SUM(p.v) FROM plain p JOIN membership m ON m.tenant_id=p.tenant_id WHERE p.tenant_id=42"), (app, "SELECT COUNT(*),SUM(p.v) FROM secured p JOIN membership m ON m.tenant_id=p.tenant_id"), .05),
            ("simple_parallel", (app, "SELECT /*+ PARALLEL(4) */ COUNT(*),SUM(v) FROM plain IGNORE INDEX(tenant_key) WHERE tenant_id=42"), (app, "SELECT /*+ PARALLEL(4) */ COUNT(*),SUM(v) FROM secured IGNORE INDEX(tenant_key)"), .05),
            ("simple_update", (app, "UPDATE plain SET v=v+1 WHERE id=42 AND tenant_id=42"), (app, "UPDATE secured SET v=v+1 WHERE id=42"), .05),
            ("simple_update_batch", (app, "UPDATE plain SET v=v+1 WHERE tenant_id=42"), (app, "UPDATE secured SET v=v+1"), .05),
            ("simple_insert", (app, f"INSERT INTO plain VALUES({args.rows + 42},42,0,'insert')"), (app, f"INSERT INTO secured VALUES({args.rows + 42},42,0,'insert')"), .05),
            ("simple_delete", (app, "DELETE FROM plain WHERE id=42 AND tenant_id=42"), (app, "DELETE FROM secured WHERE id=42"), .05),
            ("cascade_delete", (app, "DELETE FROM parent_plain WHERE id=42 AND tenant_id=42"), (app, "DELETE FROM parent_secured WHERE id=42"), .05),
        ]
        unknown = set(args.case or ()) - {case[0] for case in cases}
        if unknown:
            raise ValueError(f"Unknown workloads: {sorted(unknown)}")
        result["complete_workload_matrix"] = not args.case or set(args.case) == {case[0] for case in cases}
        for name, control, secured, threshold in cases:
            if args.case and name not in args.case:
                continue
            transactional = name in {"simple_update", "simple_update_batch", "simple_insert", "simple_delete", "cascade_delete"}
            wait_for_builds()
            expected = run_query(*control, transactional)
            assert run_query(*secured, transactional) == expected, name
            for case in [control, secured]:
                measure(*case, min(3, args.seconds), expected, transactional)
            measurements = [[], []]
            plans = [query(connection, "EXPLAIN " + sql) for connection, sql in [control, secured]]
            analyzed_plans = [run_query(connection, "EXPLAIN ANALYZE " + sql, transactional)
                              for connection, sql in [control, secured]]
            for round_number in range(args.rounds):
                order = [0, 1] if round_number % 2 == 0 else [1, 0]
                for index in order:
                    wait_for_builds()
                    before_sample = process_sample([old_process, new_process])
                    measurement = measure(*[control, secured][index], args.seconds, expected, transactional)
                    measurement["process_before"] = before_sample
                    measurement["process_after"] = process_sample([old_process, new_process])
                    interference = active_builds()
                    if interference:
                        result.setdefault("rejected_measurements", []).append({"case": name,
                            "round": round_number, "variant": index, "builders": interference,
                            "measurement": measurement})
                        wait_for_builds()
                        measurement = measure(*[control, secured][index], args.seconds, expected, transactional)
                        assert not active_builds(), "Repeated compiler interference; rerun this workload"
                    measurements[index].append(measurement)
                    print(name, f"round {round_number + 1}/{args.rounds}",
                          "control" if index == 0 else "rls", round(measurement["qps"], 2), "qps", flush=True)
            reference_qps = statistics.median(m["qps"] for m in measurements[0])
            secured_qps = statistics.median(m["qps"] for m in measurements[1])
            overhead = reference_qps / secured_qps - 1
            result["cases"][name] = {"control": measurements[0], "rls": measurements[1],
                                     "plans": plans, "analyzed_plans": analyzed_plans,
                                     "transactional_rollback_per_operation": transactional,
                                     "latency_overhead_ratio": overhead,
                                     "threshold": threshold, "within_threshold": overhead <= threshold}
            (root / "results.json").write_text(json.dumps(result, indent=2, default=str) + "\n")
            print(name, "overhead", round(overhead * 100, 2), "%", flush=True)
        # Every DML measurement rolls back both business and cascading changes.
        for table in ["plain", "secured", "mapped", "contextual", "multi"]:
            assert query(new, f"SELECT COUNT(*),SUM(v) FROM {table}") == (
                (args.rows, 3 * args.rows * (args.rows + 1) // 2),)
        for suffix in ["plain", "secured"]:
            assert query(new, f"SELECT COUNT(*) FROM parent_{suffix}") == ((1,),)
            assert query(new, f"SELECT COUNT(*) FROM child_{suffix}") == ((2,),)
        result["process_samples"] = subprocess.check_output(["ps", "-p", f"{old_process.pid},{new_process.pid}", "-o", "pid=,rss=,time="], text=True)
        result["all_case_thresholds_passed"] = all(case["within_threshold"] for case in result["cases"].values())
        result["duration_and_case_gates_passed"] = result["acceptance_duration"] and result["all_case_thresholds_passed"]
    finally:
        for client in clients:
            client.close()
        before.close()
        after.close()
        (root / "results.json").write_text(json.dumps(result, indent=2, default=str) + "\n")
        print("Artifacts:", root, flush=True)
    if result["acceptance_duration"] and not result.get("all_case_thresholds_passed", False):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
