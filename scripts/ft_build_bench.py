#!/usr/bin/env python3
"""Measure existing-row FULLTEXT builds with independent membership checks.

Temporary data and profiler output belong under /tmp/mydb. The caller starts
the server and controls its durability, memory and worker settings.
"""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import time

import pymysql
import ft_bench as base


def document(index):
    return base.make_doc(index) + " commonterm" + (" rareterm" if index % 1000 == 0 else "")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=base.positive_int, default=14837)
    parser.add_argument("--rows", type=base.positive_int, default=1000000)
    parser.add_argument("--builds", type=base.positive_int, default=3)
    parser.add_argument("--reuse", action="store_true")
    parser.add_argument("--unique-terms", action="store_true", help="add one distinct term to every document")
    parser.add_argument("--validation-memory-mb", type=base.positive_int,
                        help="temporarily raise query memory after each timed build for membership checks")
    parser.add_argument("--sample-pid", type=int)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    def make_document(index):
        return document(index) + (f" rowtoken{index:08x}" if args.unique_terms else "")
    result = {"args": vars(args) | {"output": str(args.output)}, "builds": [], "passed": False}
    connection = None
    try:
        connection = pymysql.connect(host="127.0.0.1", port=args.port, user="root", password="root",
                                     autocommit=True, read_timeout=1800, write_timeout=1800)
        with connection.cursor() as cursor:
            result["version"] = base.execute(cursor, "SELECT VERSION()")
            base.execute(cursor, "CREATE DATABASE IF NOT EXISTS ft_build_profile")
            base.execute(cursor, "USE ft_build_profile")
            if not args.reuse:
                base.execute(cursor, "DROP TABLE IF EXISTS docs")
                base.execute(cursor, "CREATE TABLE docs(id BIGINT PRIMARY KEY,body TEXT)")
                result["load"] = base.load_rows(cursor, "docs", args.rows, 1000, make_document)
            assert base.execute(cursor, "SELECT COUNT(*) FROM docs")[0][0] == args.rows
            samples = sorted({0, args.rows // 2, args.rows - 1})
            actual_samples = base.execute(cursor, "SELECT id,body FROM docs WHERE id IN (" +
                                          ",".join(map(str, samples)) + ") ORDER BY id")
            assert tuple(actual_samples) == tuple((index, make_document(index)) for index in samples), "corpus mismatch"
            expected = tuple(range(0, args.rows, 1000))
            build_memory_mb = int(base.execute(cursor, "SELECT @@GLOBAL.mydb_query_mem_limit_mb")[0][0])
            for iteration in range(args.builds):
                indexes = base.execute(cursor, "SHOW INDEX FROM docs")
                if any(row[2] == "ft" for row in indexes):
                    base.execute(cursor, "DROP INDEX ft ON docs")
                before = {row[1]: row[2] for row in base.execute(cursor, "SHOW ENGINE MYDB STATUS")
                          if row[1].startswith(("index_build_", "spill_bytes_", "bulk_index_"))}
                profiler = None
                if args.sample_pid and iteration == 0:
                    profiler = subprocess.Popen(["sample", str(args.sample_pid), "15", "1", "-file",
                                                 str(args.output.with_suffix(".sample.txt"))],
                                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                started = time.perf_counter()
                base.execute(cursor, "CREATE FULLTEXT INDEX ft ON docs(body)")
                seconds = time.perf_counter() - started
                after = {row[1]: row[2] for row in base.execute(cursor, "SHOW ENGINE MYDB STATUS")
                         if row[1].startswith(("index_build_", "spill_bytes_", "bulk_index_"))}
                try:
                    if args.validation_memory_mb:
                        base.execute(cursor, f"SET GLOBAL mydb_query_mem_limit_mb={args.validation_memory_mb}")
                    actual = tuple(row[0] for row in base.execute(cursor,
                        "SELECT id FROM docs WHERE MATCH(body) AGAINST('rareterm') ORDER BY id"))
                    assert actual == expected, "rare-term membership mismatch"
                    assert base.execute(cursor, "SELECT COUNT(*) FROM docs WHERE MATCH(body) AGAINST('commonterm')")[0][0] == args.rows
                finally:
                    if args.validation_memory_mb:
                        base.execute(cursor, f"SET GLOBAL mydb_query_mem_limit_mb={build_memory_mb}")
                item = {"seconds": seconds, "status_delta": {
                    key: int(value) - int(before.get(key, 0)) for key, value in after.items()},
                    "status_after": {key: int(value) for key, value in after.items()},
                    "membership_sha256": hashlib.sha256(json.dumps(actual).encode()).hexdigest()}
                result["builds"].append(item)
                print(json.dumps(item), flush=True)
                if profiler:
                    profiler.wait()
            result["passed"] = True
    finally:
        if connection is not None:
            connection.close()
        args.output.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
