#!/usr/bin/env python3
"""Measure warm cosine HNSW queries against an explicitly selected loaded table.

Input is JSONL: {"id": query_id, "vector": [...], "neighbors": [id, ...]}.
This runner does not create, drop, or load database objects.
"""

import argparse
import json
import os
import re
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path


def connect(config):
    if config["engine"] == "mydb":
        import pymysql

        return pymysql.connect(
            host=config["host"],
            port=config["port"],
            user=config["user"],
            password=os.environ["VECTOR_BENCH_PASSWORD"],
            database=config["database"],
            ssl={"check_hostname": False},
            autocommit=True,
        )
    import psycopg

    return psycopg.connect(
        host=config["host"],
        port=config["port"],
        user=config["user"],
        password=os.environ["VECTOR_BENCH_PASSWORD"],
        dbname=config["database"],
        autocommit=True,
    )


def setup(cur, cfg):
    if cfg["engine"] == "mydb":
        cur.execute("SET SESSION mydb_hnsw_ef_search=%s", (cfg["ef"],))
        cur.execute(
            "SET SESSION mydb_hnsw_ef_dynamic_factor=%s", (cfg["dynamic_factor"],)
        )
        cur.execute("SET SESSION mydb_hnsw_rerank_mult=1")
    else:
        cur.execute("SELECT set_config('hnsw.ef_search', %s, false)", (str(cfg["ef"]),))


def statement(cfg):
    table, column = cfg["table"], cfg["column"]
    k = cfg["k"]
    if cfg["engine"] == "mydb":
        return f"SELECT id FROM {table} ORDER BY DISTANCE({column},STRING_TO_VECTOR(%s),'COSINE') LIMIT {k}"
    return f"SELECT id FROM {table} ORDER BY {column} <=> %s::vector LIMIT {k}"


def vector(q):
    return json.dumps(q["vector"], separators=(",", ":"))


def worker(payload):
    cfg, queries, index, start = payload
    latencies = []
    count = 0
    with connect(cfg) as c, c.cursor() as cur:
        setup(cur, cfg)
        sql = statement(cfg)
        for _ in range(3):
            cur.execute(sql, (vector(queries[index % len(queries)]),))
            cur.fetchall()
        while time.time() < start:
            time.sleep(0.001)
        begin = time.perf_counter()
        while time.perf_counter() - begin < cfg["duration"]:
            q = queries[(index + count) % len(queries)]
            t = time.perf_counter()
            cur.execute(sql, (vector(q),))
            cur.fetchall()
            latencies.append((time.perf_counter() - t) * 1000)
            count += 1
        return count, time.perf_counter() - begin, latencies


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--engine", choices=["mydb", "postgres"], required=True)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--user", required=True)
    p.add_argument("--database", required=True)
    p.add_argument("--table", default="vbench")
    p.add_argument("--column", default="emb")
    p.add_argument("--queries", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--k", type=int, default=100)
    p.add_argument("--ef", type=int, default=200)
    p.add_argument("--dynamic-factor", type=int, default=2)
    p.add_argument("--clients", type=int, default=16)
    p.add_argument("--duration", type=float, default=10)
    p.add_argument("--rounds", type=int, default=3)
    args = p.parse_args()
    cfg = vars(args).copy()
    for name in ["table", "column"]:
        if not re.fullmatch("[A-Za-z_][A-Za-z0-9_]*", cfg[name]):
            p.error("Use simple identifiers for table/column")
    if min(args.k, args.ef, args.clients, args.rounds) < 1 or args.duration < 1:
        p.error("Counts must be positive; duration must be at least one second")
    queries = [
        json.loads(line)
        for line in args.queries.read_text().splitlines()
        if line.strip()
    ]
    if not queries or any(
        len(q["neighbors"]) < args.k or not q["vector"] for q in queries
    ):
        p.error("Need vectors and at least k ground-truth IDs per query")
    with connect(cfg) as c, c.cursor() as cur:
        setup(cur, cfg)
        cur.execute("SELECT VERSION()")
        version = cur.fetchone()[0]
        cur.execute("EXPLAIN " + statement(cfg), (vector(queries[0]),))
        plan = [list(row) for row in cur.fetchall()]
        recall = []
        for q in queries:
            cur.execute(statement(cfg), (vector(q),))
            ids = {int(row[0]) for row in cur.fetchall()}
            recall.append(len(ids & set(q["neighbors"][: args.k])) / args.k)
    rounds = []
    for _ in range(args.rounds):
        with ProcessPoolExecutor(max_workers=args.clients) as pool:
            start = time.time() + 3
            runs = list(
                pool.map(
                    worker, [(cfg, queries, i, start) for i in range(args.clients)]
                )
            )
        samples = sorted(t for _, _, ts in runs for t in ts)
        percentile = lambda q, samples=samples: samples[
            min(len(samples) - 1, int((len(samples) - 1) * q))
        ]
        rounds.append(
            {
                "qps": sum(n for n, _, _ in runs)
                / max(elapsed for _, elapsed, _ in runs),
                "p50_ms": percentile(0.5),
                "p95_ms": percentile(0.95),
                "p99_ms": percentile(0.99),
            }
        )
    result = {
        "engine_version": version,
        "configuration": {
            k: str(v) if isinstance(v, Path) else v for k, v in cfg.items()
        },
        "plan": plan,
        "query_count": len(queries),
        "recall_at_k": sum(recall) / len(recall),
        "rounds": rounds,
        "scope": "Warm repeated-query corpus; candidate memo controlled in server environment. Existing table/index and loading precision must be recorded separately.",
    }
    args.output.write_text(json.dumps(result, indent=2, default=str) + "\n")
    print(json.dumps(rounds))


if __name__ == "__main__":
    main()
