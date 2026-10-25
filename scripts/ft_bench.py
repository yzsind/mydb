#!/usr/bin/env python3
"""Full-text performance baseline over the MySQL protocol (requires PyMySQL).

Validated dependency: pip install 'PyMySQL[rsa]==1.1.1'

Start mydb separately, then run:
    python3 scripts/ft_bench.py --host 127.0.0.1 --port 4837

Each run owns a uniquely named database, dropped unless --keep-data is set.
Results default to /tmp/mydb. Timings include protocol and client decoding;
they are not directly comparable to the former in-process Rust benchmark.
"""

import argparse
import importlib.metadata
import json
import math
import os
from pathlib import Path
import statistics
import sys
import time
import uuid


# Keep the original corpus, including the repeated "checkpoint" entry.
CORPUS_WORDS = (
    "database", "systems", "storage", "engine", "index", "btree", "hash",
    "sort", "join", "query", "planner", "execution", "transaction", "recovery",
    "wal", "buffer", "pool", "replication", "consensus", "protocol", "network",
    "concurrency", "isolation", "snapshot", "compaction", "cache", "column",
    "vector", "search", "similarity", "matching", "ranking", "score", "relevance",
    "distributed", "coordinator", "leader", "follower", "commit", "abort",
    "durability", "checkpoint", "checkpoint", "undo", "redo", "mvcc", "gc",
    "reorganize",
)


def make_doc(index):
    # Match wrapping usize arithmetic from the original 64-bit Rust harness.
    mask = (1 << 64) - 1
    return " ".join(
        CORPUS_WORDS[
            (((index * 2654435761) & mask) ^ ((word * 11400714819) & mask))
            % len(CORPUS_WORDS)
        ]
        for word in range(8 + index * 7 % 9)
    )


def query_cases(table):
    cases = (
        ("NL 'database engine'", "database engine", "id", False, " ORDER BY id"),
        ("BOOL '+database -mysql'", "+database -mysql", "id", True, ""),
        ('PHRASE "database engine"', '"database engine"', "id", True, ""),
        ("PREFIX 'data*'", "data*", "id", True, ""),
        ("PHRASE repeated", '"database database" "database engine"', "id", True, ""),
        ("PHRASE scored", '"database engine"',
         "id, MATCH(body) AGAINST('\"database engine\"' IN BOOLEAN MODE) AS score", True, ""),
        ("BOOL overlapping", '+database "database engine" data*', "id", True, ""),
        ("NL scored", "database engine",
         "id, MATCH(body) AGAINST('database engine') AS score", False, ""),
    )
    for label, search, projection, boolean, order in cases:
        modifier = " IN BOOLEAN MODE" if boolean else ""
        base = (f"SELECT {projection} FROM {table} "
                f"WHERE MATCH(body) AGAINST('{search}'{modifier})")
        yield label, base + order + " LIMIT 20", base + " ORDER BY id"


def execute(cursor, sql):
    cursor.execute(sql)
    return cursor.fetchall() if cursor.description else ()


def timed_execute(cursor, sql):
    started = time.perf_counter_ns()
    rows = execute(cursor, sql)
    return time.perf_counter_ns() - started, rows


def load_rows(cursor, table, rows, batch_size, document=make_doc):
    elapsed_ns = 0
    for start in range(0, rows, batch_size):
        # Generate and escape SQL before starting the insertion timer.
        values = ",".join(
            cursor.mogrify("(%s,%s)", (index, document(index)))
            for index in range(start, min(start + batch_size, rows))
        )
        duration, _ = timed_execute(cursor, f"INSERT INTO {table} VALUES {values}")
        elapsed_ns += duration
    return {"elapsed_ms": elapsed_ns / 1e6, "rows_per_second": rows * 1e9 / elapsed_ns}


def validate_table(cursor, table, rows):
    count = execute(cursor, f"SELECT COUNT(*) FROM {table}")[0][0]
    if count != rows:
        raise RuntimeError(f"{table}: expected {rows} rows, got {count}")
    ddl = execute(cursor, f"SHOW CREATE TABLE {table}")[0][1]
    if "FULLTEXT" not in ddl.upper():
        raise RuntimeError(f"{table}: FULLTEXT index missing from schema: {ddl}")
    actual = tuple(row[0] for row in execute(
        cursor, f"SELECT id FROM {table} WHERE MATCH(body) AGAINST('database') ORDER BY id"
    ))
    expected = tuple(index for index in range(rows) if "database" in make_doc(index).split())
    if actual != expected:
        raise RuntimeError(f"{table}: 'database' matches differ from the generated corpus")


def benchmark_queries(cursor, table, iters, reference):
    results = []
    for label, sql, full_sql in query_cases(table):
        # Warm every query independently, including its score projection.
        execute(cursor, sql)
        samples = []
        row_counts = []
        for _ in range(iters):
            elapsed, rows = timed_execute(cursor, sql)
            samples.append(elapsed / 1000)
            row_counts.append(len(rows))

        # Compare all matching IDs, since unordered LIMIT queries can return
        # different valid subsets for incremental and bulk-built indexes.
        full_rows = execute(cursor, full_sql)
        ids = tuple(row[0] for row in full_rows)
        if label in reference and ids != reference[label]:
            raise RuntimeError(f"{table}: {label} differs from the inline-index result")
        reference[label] = ids
        if any(count != min(20, len(ids)) for count in row_counts):
            raise RuntimeError(f"{table}: {label} returned inconsistent LIMIT row counts")
        if any(len(row) == 2 and not math.isfinite(float(row[1])) for row in full_rows):
            raise RuntimeError(f"{table}: {label} returned a non-finite score")
        result = {
            "label": label,
            "sql": sql,
            "median_us": statistics.median(samples),
            "p95_us": sorted(samples)[math.ceil(len(samples) * 0.95) - 1],
            "rows_per_query": row_counts[0],
            "total_matching_rows": len(ids),
            "samples_us": samples,
        }
        results.append(result)
        print(f"[ft_bench] {table} {label}: median {result['median_us']:.1f} us, "
              f"p95 {result['p95_us']:.1f} us ({result['rows_per_query']} rows/query)",
              flush=True)
    return results


def benchmark(connection, args, report):
    reference = {}
    with connection.cursor() as cursor:
        report["server_version"] = execute(cursor, "SELECT VERSION()")[0][0]
        for table, inline in (("docs_inline", True), ("docs_bulk", False)):
            result = {"table": table, "index_before_insert": inline}
            report["scenarios"].append(result)
            index = ", FULLTEXT KEY ft (body)" if inline else ""
            elapsed, _ = timed_execute(
                cursor, f"CREATE TABLE {table} (id INT PRIMARY KEY, body TEXT{index})"
            )
            result["create_table_ms"] = elapsed / 1e6
            print(f"[ft_bench] {table} CREATE TABLE: {elapsed / 1e6:.3f} ms", flush=True)
            result["insert"] = load_rows(cursor, table, args.rows, args.batch_size)
            print(f"[ft_bench] {table} INSERT {args.rows} rows: "
                  f"{result['insert']['elapsed_ms']:.3f} ms "
                  f"({result['insert']['rows_per_second']:.0f} rows/s)", flush=True)
            if not inline:
                count = execute(cursor, f"SELECT COUNT(*) FROM {table}")[0][0]
                if count != args.rows:
                    raise RuntimeError(f"{table}: expected {args.rows} rows before index creation")
                sql = f"CREATE FULLTEXT INDEX ft ON {table} (body)"
                elapsed, _ = timed_execute(cursor, sql)
                result["build_index"] = {
                    "sql": sql,
                    "elapsed_ms": elapsed / 1e6,
                    "rows_per_second": args.rows * 1e9 / elapsed,
                }
                print(f"[ft_bench] {table} CREATE FULLTEXT INDEX on {count} existing rows: "
                      f"{elapsed / 1e6:.3f} ms", flush=True)
            validate_table(cursor, table, args.rows)
            result["queries"] = benchmark_queries(cursor, table, args.iters, reference)
            result["validated"] = True


def positive_int(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def parse_args(configure=None, description=__doc__):
    parser = argparse.ArgumentParser(
        description=description, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=positive_int, default=4837)
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default=os.environ.get("MYSQL_PWD", "root"),
                        help="server password (default: MYSQL_PWD or root)")
    parser.add_argument("--rows", type=positive_int, default=os.environ.get("FT_N", "10000"),
                        help="number of documents (default: FT_N or 10000)")
    parser.add_argument("--iters", type=positive_int, default=os.environ.get("FT_ITERS", "21"),
                        help="timed samples per query (default: FT_ITERS or 21)")
    parser.add_argument("--batch-size", type=positive_int, default=1000,
                        help="rows per INSERT statement (default: 1000)")
    parser.add_argument("--timeout", type=positive_int, default=300,
                        help="socket read/write timeout in seconds (default: 300)")
    parser.add_argument("--output", type=Path, help="JSON report (default: /tmp/mydb/ft-bench-UUID.json)")
    parser.add_argument("--keep-data", action="store_true", help="retain the owned benchmark database")
    if configure is not None:
        configure(parser)
    return parser.parse_args()


def run(args, benchmark_fn=benchmark):
    run_id = uuid.uuid4().hex
    database = f"ft_bench_{run_id}"
    output = args.output or Path("/tmp/mydb") / f"ft-bench-{run_id}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "passed": False,
        "host": args.host,
        "port": args.port,
        "database": database,
        "rows": args.rows,
        "iters": args.iters,
        "batch_size": args.batch_size,
        "timing": "MySQL protocol round trip including full result decoding; SQL generation excluded",
        "warmups_per_query": 1,
        "scenarios": [],
        "database_retained": False,
    }
    connection = None
    created = False
    try:
        import pymysql

        report["driver_version"] = importlib.metadata.version("PyMySQL")
        connection = pymysql.connect(
            host=args.host, port=args.port, user=args.user, password=args.password,
            charset="utf8mb4", autocommit=True, connect_timeout=10,
            read_timeout=args.timeout, write_timeout=args.timeout,
        )
        with connection.cursor() as cursor:
            execute(cursor, f"CREATE DATABASE `{database}`")
            created = True
            report["database_retained"] = True
            execute(cursor, f"USE `{database}`")
        print(f"[ft_bench] rows={args.rows} iters={args.iters} database={database}", flush=True)
        benchmark_fn(connection, args, report)
        report["passed"] = True
    except (Exception, KeyboardInterrupt) as error:
        report["error"] = f"{type(error).__name__}: {error}"
        print(f"[ft_bench] FAILED: {report['error']}", file=sys.stderr)
    finally:
        if connection is not None:
            try:
                if created:
                    report["database_retained"] = True
                    if not args.keep_data:
                        with connection.cursor() as cursor:
                            execute(cursor, f"DROP DATABASE `{database}`")
                        report["database_retained"] = False
            except Exception as error:
                report["passed"] = False
                report["cleanup_error"] = f"{type(error).__name__}: {error}"
                print(f"[ft_bench] cleanup failed: {error}", file=sys.stderr)
            finally:
                connection.close()
        output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(f"[ft_bench] report: {output}", flush=True)
    return 0 if report["passed"] else 1


def main():
    return run(parse_args())


if __name__ == "__main__":
    sys.exit(main())
