#!/usr/bin/env python3
"""Load a fixed-seed property graph and compare exact GQL and SQL paths."""

import argparse
import concurrent.futures
import json
import os
import statistics
import subprocess
import threading
import time
from pathlib import Path

import mysql.connector


MASK = (1 << 64) - 1


def mix(value):
    value = (value + 0x9E3779B97F4A7C15) & MASK
    value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & MASK
    value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & MASK
    return value ^ (value >> 31)


def batches(start, stop, length):
    for first in range(start, stop, length):
        yield range(first, min(first + length, stop))


def connect(args):
    return mysql.connector.connect(
        host="127.0.0.1",
        port=args.port,
        database=args.database,
        user=os.environ.get("MYSQL_USER", "root"),
        password=os.environ.get("MYSQL_PASSWORD", "root"),
        autocommit=True,
        use_pure=args.phase == "load",
    )


def load(args):
    with connect(args) as db, db.cursor() as cur:
        for statement in (
            "CREATE TABLE grb_node (id BIGINT PRIMARY KEY, k BIGINT NOT NULL UNIQUE)",
            "CREATE TABLE grb_edge (id BIGINT AUTO_INCREMENT PRIMARY KEY, src BIGINT NOT NULL, dst BIGINT NOT NULL, INDEX outgoing(src,dst), INDEX incoming(dst,src))",
            "CREATE GRAPH grb ANY PROPERTY GRAPH",
            "CREATE UNIQUE GRAPH INDEX external_key ON grb NODE LABEL N (k INT64)",
        ):
            cur.execute(statement)
    start = time.perf_counter()
    batch = args.batch
    timings = {}

    def job(worker):
        elapsed = {"gql": 0.0, "sql": 0.0}
        with connect(args) as db, db.cursor() as cur:
            cur.execute("SESSION SET GRAPH grb")
            for kind, count in (("nodes", args.nodes), ("edges", args.edges)):
                if kind != phase:
                    continue
                for numbers in batches(count * worker // args.load_workers,
                                       count * (worker + 1) // args.load_workers, batch):
                    if kind == "nodes":
                        gql = f"FOR key IN [{','.join(map(str, numbers))}] INSERT (:N {{k: key}})"
                        sql = "INSERT INTO grb_node VALUES " + ",".join(f"({key},{key})" for key in numbers)
                    else:
                        links = [(
                            mix(i) % (max(1, args.nodes // 50) if i % 8 == 0 else args.nodes),
                            mix(i + args.edges) % args.nodes,
                        ) for i in numbers]
                        records = ",".join(f"{{src:{src},dst:{dst}}}" for src, dst in links)
                        gql = (
                            f"FOR pair IN [{records}] MATCH (a:N {{k: pair.src}}), (b:N {{k: pair.dst}}) "
                            "INSERT (a)-[]->(b)"
                        )
                        sql = "INSERT INTO grb_edge (src,dst) VALUES " + ",".join(f"({src},{dst})" for src, dst in links)
                    for name, query in (("gql", gql), ("sql", sql)):
                        before = time.perf_counter()
                        cur.execute(query)
                        elapsed[name] += time.perf_counter() - before
                        assert cur.rowcount == len(numbers), (name, cur.rowcount, len(numbers))
        return elapsed

    for phase in ("nodes", "edges"):
        phase_start = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.load_workers) as pool:
            durations = list(pool.map(job, range(args.load_workers)))
        timings[phase] = {
            "wall_seconds": time.perf_counter() - phase_start,
            "aggregate_driver_seconds": {key: sum(d[key] for d in durations) for key in ("gql", "sql")},
        }
        print(json.dumps({"phase": phase, **timings[phase]}), flush=True)
    print(json.dumps({"load_seconds": time.perf_counter() - start, "nodes": args.nodes, "edges": args.edges}), flush=True)


def queries(hops):
    graph = (
        f"MATCH (source:N {{k:%s}})-[]->{{{hops}}}(target) "
        "RETURN CAST(target.k AS INT64) AS dst ORDER BY dst"
    )
    joins = ["JOIN grb_edge e1 ON e1.src=a.id"] + [
        f"JOIN grb_edge e{i} ON e{i}.src=e{i - 1}.dst" for i in range(2, hops + 1)
    ]
    sql = (
        f"SELECT b.k AS dst FROM grb_node a {' '.join(joins)} "
        f"JOIN grb_node b ON b.id=e{hops}.dst WHERE a.k=%s ORDER BY dst"
    )
    return (graph, sql)


def run_queries(args, workers, hops, hot, first_touch):
    graph, sql = queries(hops)
    count = max(1, args.nodes // 50) if hot else args.nodes

    def job(worker):
        samples = ([], [])
        with connect(args) as db, db.cursor() as ctl:
            ctl.execute("SESSION SET GRAPH grb")
            with db.cursor(prepared=True) as a, db.cursor(prepared=True) as b:
                for iteration in range(args.queries):
                    seed = mix((worker * args.queries + iteration) ^ 0x1BFAEE) % count
                    results = [None, None]
                    work = ((0, a, graph), (1, b, sql))
                    if (iteration + worker) % 2:
                        work = work[::-1]
                    for index, cursor, text in work:
                        start = time.perf_counter_ns()
                        cursor.execute(text, (seed,))
                        results[index] = cursor.fetchall()
                        samples[index].append((time.perf_counter_ns() - start) / 1e6)
                    if results[0] != results[1]:
                        raise AssertionError(f"{seed=} {hops=}: GQL and SQL differ")
        return samples

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        all_samples = list(pool.map(job, range(workers)))
    stats = []
    for index in range(2):
        values = sorted(value for worker in all_samples for value in worker[index])
        stats.append({"p50_ms": statistics.median(values), "p95_ms": values[(len(values) * 95 + 99) // 100 - 1]})
    print(json.dumps({
        "hops": hops, "workers": workers, "hot_seed": hot, "first_touch": first_touch,
        "gql": stats[0], "sql": stats[1], "gql_to_sql_p95": stats[0]["p95_ms"] / stats[1]["p95_ms"],
    }), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("load", "measure"))
    parser.add_argument("--port", type=int, default=4837)
    parser.add_argument("--database", default="test")
    parser.add_argument("--nodes", type=int, default=1_000_000)
    parser.add_argument("--edges", type=int, default=10_000_000)
    parser.add_argument("--batch", type=int, default=512)
    parser.add_argument("--load-workers", type=int, default=8)
    parser.add_argument("--queries", type=int, default=64)
    parser.add_argument("--data-dir", type=Path, help="Report index file sizes in this local instance")
    parser.add_argument("--server-pid", type=int, help="Sample server RSS every 250 ms")
    args = parser.parse_args()
    if min(args.nodes, args.edges, args.batch, args.load_workers, args.queries) < 1:
        parser.error("counts and batch size must be positive")
    done = threading.Event()
    rss_samples = []

    def sample_rss():
        while not done.is_set():
            rss = subprocess.run(
                ["ps", "-o", "rss=", "-p", str(args.server_pid)],
                check=False, capture_output=True, text=True,
            ).stdout.strip()
            if rss.isdigit():
                rss_samples.append(int(rss) * 1024)
            done.wait(0.25)

    sampler = None
    if args.server_pid:
        sampler = threading.Thread(target=sample_rss, daemon=True)
        sampler.start()
    try:
        if args.phase == "load":
            load(args)
        else:
            for trial in ("first-touch", "repeat"):
                for workers in (1, 16):
                    for hops in (1, 2, 3):
                        for hot in (False, True):
                            run_queries(args, workers, hops, hot, trial == "first-touch")
    finally:
        done.set()
        if sampler:
            sampler.join()
    if rss_samples:
        print(json.dumps({"sampled_max_server_rss_bytes": max(rss_samples)}))
    if args.data_dir:
        sizes = {}
        for file in (args.data_dir / "objects" / "indexes").glob("*/*/*.idx"):
            table_id = file.relative_to(args.data_dir / "objects" / "indexes").parts[0]
            sizes[table_id] = sizes.get(table_id, 0) + file.stat().st_size
        print(json.dumps({"index_file_bytes_by_object": sizes}, sort_keys=True))


if __name__ == "__main__":
    main()
