#!/usr/bin/env python3
"""Local, alternating five-round MySQL regression gate for the REST listener.

Requires the existing sql_bench executable and mysql CLI. All data and evidence
stay under /tmp/mydb. No compilation settings are changed.
"""
from __future__ import annotations
import argparse
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import socket
import statistics
import subprocess
import time
import threading


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--baseline", type=Path, required=True)
    p.add_argument("--candidate", type=Path, required=True)
    p.add_argument("--client", type=Path, required=True)
    p.add_argument("--output", type=Path, default=Path("/tmp/mydb/rest/performance"))
    p.add_argument("--rounds", type=int, default=5)
    p.add_argument("--mixed", action="store_true", help="Also measure concurrent REST traffic without applying the idle regression gate to it")
    p.add_argument("--mixed-only", action="store_true", help="Measure only concurrent REST/MySQL traffic")
    p.add_argument("--scale", type=int, default=1, help="Multiply iteration counts to reduce short-run noise")
    p.add_argument("--workload", choices=["short_select","point_read","update","transaction","wide_row","large_result"], help="Isolate one workload so baseline/candidate samples are adjacent")
    args = p.parse_args()
    if args.scale < 1 or args.rounds < 1:
        p.error("rounds and scale must be positive")
    out = args.output.resolve()
    if not out.is_relative_to(Path("/tmp/mydb").resolve()):
        p.error("output must be under /tmp/mydb")
    out.mkdir(parents=True, exist_ok=True)
    variants = {"baseline": args.baseline, "candidate_off": args.candidate, "candidate_idle": args.candidate}
    if args.mixed:
        variants["candidate_mixed"] = args.candidate
    if args.mixed_only:
        variants = {"candidate_mixed": args.candidate}
    cases = {
        "short_select": ("SELECT 1", 10000),
        "point_read": ("SELECT v FROM rest_bench WHERE id=1", 5000),
        "update": ("UPDATE rest_bench SET v=v+1 WHERE id=1", 2000),
        "transaction": ("BEGIN; UPDATE rest_bench SET v=v+1 WHERE id=1; COMMIT", 1000),
        "wide_row": ("SELECT b FROM rest_bench WHERE id=1", 2000),
        "large_result": ("SELECT * FROM rest_bench", 100),
    }
    if args.workload:
        cases = {args.workload: cases[args.workload]}
    results = []
    mixed_results = []
    for rnd in range(args.rounds):
        order = list(variants)
        order = order[rnd % len(order):] + order[:rnd % len(order)]
        for variant in order:
            sql_port, http_port = port(), port()
            data = out / f"data-{rnd}-{variant}-{time.time_ns()}"
            env = dict(os.environ, MYDB_DATA_DIR=str(data), MYDB_DURABILITY="memory", MYDB_BUFFER_POOL_PAGES="512")
            cmd = [str(variants[variant].resolve()), "--host", "127.0.0.1", "--port", str(sql_port)]
            if variant in ("candidate_idle", "candidate_mixed"):
                cmd += ["--rest-enabled", "ON", "--rest-port", str(http_port), "--rest-tls-mode", "disabled"]
            with open(out / f"server-{rnd}-{variant}.log", "wb") as log:
                proc = subprocess.Popen(cmd, env=env, stdout=log, stderr=log)
                stop_http = threading.Event()
                http_workers = []
                http_counts = [0, 0]
                http_error_samples = []
                http_lock = threading.Lock()
                http_started = None
                try:
                    for _ in range(600):
                        if proc.poll() is not None:
                            raise RuntimeError(f"{variant} startup failed")
                        try:
                            with socket.create_connection(("127.0.0.1", sql_port), .1):
                                break
                        except OSError:
                            time.sleep(.05)
                    prep = "CREATE TABLE rest_bench(id INT PRIMARY KEY,v BIGINT,b VARCHAR(1024)); INSERT INTO rest_bench VALUES " + ",".join(f"({i},0,REPEAT('x',1024))" for i in range(1,129))
                    subprocess.run(["mysql", "--host=127.0.0.1", f"--port={sql_port}", "--user=root", "--password=root", "--ssl-mode=REQUIRED", "test", "-e", prep], check=True, capture_output=True)
                    if variant == "candidate_mixed":
                        created = subprocess.run(["mysql", "--host=127.0.0.1", f"--port={sql_port}", "--user=root", "--password=root",
                                                  "--ssl-mode=REQUIRED", "--batch", "--raw", "--skip-column-names", "-e", "CREATE API KEY 'mixed'"],
                                                 check=True, capture_output=True, text=True)
                        token = created.stdout.strip().split("\t")[6]
                        def http_worker():
                            connection = http.client.HTTPConnection("127.0.0.1", http_port, timeout=3)
                            try:
                                while not stop_http.is_set():
                                    try:
                                        connection.request("POST", "/api/v1/sql", '{"sql":"SELECT 1"}',
                                                           {"Authorization": "Bearer " + token, "Content-Type": "application/json"})
                                        reply = connection.getresponse()
                                        packet = reply.read()
                                        with http_lock:
                                            http_counts[0] += 1
                                            http_counts[1] += int(reply.status != 200)
                                            if reply.status != 200 and len(http_error_samples) < 3:
                                                http_error_samples.append({"status":reply.status, "body":packet.decode("utf-8", "replace")[:2048]})
                                    except (OSError, http.client.HTTPException):
                                        with http_lock:
                                            http_counts[1] += 1
                                        connection.close()
                                        connection = http.client.HTTPConnection("127.0.0.1", http_port, timeout=3)
                            finally:
                                connection.close()
                        http_started = time.monotonic()
                        http_workers = [threading.Thread(target=http_worker) for _ in range(4)]
                        for worker in http_workers:
                            worker.start()
                    for name, (sql, count) in cases.items():
                        r = subprocess.run([str(args.client.resolve()), "--port", str(sql_port), "--sql", sql,
                                            "--threads", "8", "--iterations", str(count * args.scale), "--warmup", "100"],
                                           check=True, capture_output=True, text=True, timeout=180)
                        (out / f"{rnd}-{variant}-{name}.txt").write_text(r.stdout + r.stderr)
                        qps = float(re.search(r"QPS:\s+([\d.]+)", r.stdout)[1])
                        p99 = int(re.search(r"p99:\s+(\d+)", r.stdout)[1])
                        errors = int(re.search(r"errors:\s+(\d+)", r.stdout)[1])
                        if errors:
                            raise RuntimeError(f"{variant} {name}: SQL errors")
                        results.append(dict(round=rnd, variant=variant, workload=name, qps=qps, p99_us=p99))
                        print(rnd + 1, variant, name, qps, p99, flush=True)
                finally:
                    stop_http.set()
                    for worker in http_workers:
                        worker.join(timeout=5)
                    if http_started is not None:
                        mixed_results.append({"round": rnd, "requests": http_counts[0], "errors": http_counts[1],
                                              "qps": http_counts[0] / (time.monotonic() - http_started), "error_samples":http_error_samples})
                    if proc.poll() is None:
                        proc.terminate()
                    proc.wait(timeout=40)
    medians = {}
    for variant in variants:
        medians[variant] = {name: {key: statistics.median(r[key] for r in results if r["variant"] == variant and r["workload"] == name) for key in ("qps", "p99_us")} for name in cases}
    comparisons = {v: {name: {"qps_change_pct": (medians[v][name]["qps"] / medians["baseline"][name]["qps"] - 1) * 100,
                              "p99_change_pct": (medians[v][name]["p99_us"] / medians["baseline"][name]["p99_us"] - 1) * 100} for name in cases} for v in ("candidate_off", "candidate_idle") if v in medians and "baseline" in medians}
    unpaired_comparisons = comparisons
    paired_samples = {}
    if "baseline" in medians:
        by_sample = {(r["round"], r["variant"], r["workload"]): r for r in results}
        comparisons = {}
        for variant in ("candidate_off", "candidate_idle"):
            if variant not in medians:
                continue
            comparisons[variant] = {}
            paired_samples[variant] = {}
            for name in cases:
                paired = {key: [(by_sample[(rnd, variant, name)][key] / by_sample[(rnd, "baseline", name)][key] - 1) * 100
                                for rnd in range(args.rounds)] for key in ("qps", "p99_us")}
                paired_samples[variant][name] = paired
                comparisons[variant][name] = {"qps_change_pct": statistics.median(paired["qps"]),
                                              "p99_change_pct": statistics.median(paired["p99_us"])}
    report = {"rounds": args.rounds, "iteration_scale":args.scale, "mode": "memory", "threads": 8,
              "binaries": {k: {"path": str(v.resolve()), "sha256": hashlib.sha256(v.read_bytes()).hexdigest()} for k,v in variants.items()},
              "mixed_rest": mixed_results,
              "client_sha256": hashlib.sha256(args.client.read_bytes()).hexdigest(),
              "samples": results, "medians": medians, "comparisons": comparisons,
              "comparison_method": "median of same-round ratios", "paired_samples": paired_samples,
              "unpaired_comparisons": unpaired_comparisons,
              "gate_passed": bool(comparisons) and all(v["qps_change_pct"] >= -3 and v["p99_change_pct"] <= 5 for group in comparisons.values() for v in group.values())}
    (out / "results.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"comparisons": comparisons, "gate_passed": report["gate_passed"]}, indent=2))


if __name__ == "__main__":
    main()
