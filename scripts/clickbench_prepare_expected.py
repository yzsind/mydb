#!/usr/bin/env python3
"""Download/reuse ClickBench data, load DuckDB, and cache all 43 oracle results."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time

from clickbench_check_results import context, read_expected, read_queries, run
from clickbench_results import split_top

ROOT = Path(__file__).resolve().parent.parent
DATA_URL = "https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz"
ARCHIVE_BYTES = 16298506510
ARCHIVE_SHA256 = "bd500f777ef0eb55dc9d4f0eb245870351cc44a44d72a56395048d5c15f09fe2"
REVISION = "4e2a463c533c5615a12ee50943e5ac4f4c771d43"
QUERIES_SHA256 = "0f2f3ca4598df5563acf5346f6397acfb8eaf59e838e6e76feef3aa581ad4059"


def sha256(path):
    result = hashlib.sha256()
    with path.open("rb") as src:
        for block in iter(lambda: src.read(8 * 1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


def duckdb_schema(mysql_schema):
    match = re.fullmatch(r"\s*CREATE TABLE hits\s*\((.*)\)\s*ENGINE\s*=\s*InnoDB"
                         r"(?:\s+DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin)?\s*;\s*",
                         mysql_schema, re.I | re.S)
    if not match:
        raise ValueError("unsupported ClickBench schema: expected CREATE TABLE hits (...) ENGINE=InnoDB")
    columns = []
    for field in split_top(match[1]):
        if re.fullmatch(r"PRIMARY KEY\s*\([\w,\s]+\)", field, re.I):
            continue  # The oracle needs data types, not the MySQL physical index.
        if not re.fullmatch(r"\w+\s+(BIGINT|SMALLINT|INTEGER|TEXT|TIMESTAMP|DATE|CHAR|VARCHAR\(\d+\))"
                            r"(?:\s+NOT NULL)?", field, re.I):
            raise ValueError(f"unsupported ClickBench column definition: {field}")
        columns.append(field)
    if not columns:
        raise ValueError("empty ClickBench schema")
    return "CREATE TABLE hits (\n" + ",\n".join(columns) + "\n);"


def prepare(args):
    started = time.monotonic()
    args.cache_dir.mkdir(parents=True, exist_ok=True)
    # Serialize preparations so an interrupted import is never mistaken for a
    # completed database and two writers cannot replace each other's files.
    import fcntl
    with (args.cache_dir / "prepare-duckdb.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return prepare_locked(args, started)


def prepare_locked(args, started):
    args.queries_file = args.cache_dir / "queries.sql"
    if not args.queries_file.is_file() or sha256(args.queries_file) != QUERIES_SHA256:
        temporary = args.queries_file.with_suffix(".sql.part")
        subprocess.run(["curl", "--fail", "--location", "--retry", "5", "--output", str(temporary),
                        f"https://raw.githubusercontent.com/ClickHouse/ClickBench/{REVISION}/mysql/queries.sql"], check=True)
        if sha256(temporary) != QUERIES_SHA256:
            raise ValueError("downloaded queries.sql checksum mismatch")
        os.replace(temporary, args.queries_file)
    queries = read_queries(args.queries_file)
    ctx = context(args, queries)
    missing = []
    for q, sql in enumerate(queries, 1):
        try:
            read_expected(args.expected_dir / f"q{q:02}.json", ctx, sql)
        except (OSError, ValueError, KeyError, TypeError):
            missing.append(q)
    if not missing:
        print(f"All 43 DuckDB result caches are valid: {args.expected_dir}", flush=True)
        return 0
    print(f"Preparing {len(missing)} missing/stale query caches", flush=True)
    if not shutil.which(args.duckdb_bin):
        raise ValueError(f"DuckDB CLI not found: {args.duckdb_bin}")
    args.duckdb_database.parent.mkdir(parents=True, exist_ok=True)
    manifest_path = Path(str(args.duckdb_database) + ".manifest.json")
    schema = duckdb_schema(args.schema_file.read_text())
    identity = dict(dataset_id=args.dataset_id, archive_sha256=args.archive_sha256,
                    archive_bytes=args.archive_bytes, row_count=args.row_count,
                    import_format='raw-tsv-null-NULL-v2',
                    schema_sha256=hashlib.sha256(schema.encode()).hexdigest())
    try:
        reusable = args.duckdb_database.is_file() and json.loads(manifest_path.read_text()) == identity
    except (OSError, ValueError):
        reusable = False
    if not reusable:
        if args.duckdb_database.exists():
            raise ValueError(f"existing database has no matching manifest: {args.duckdb_database}; "
                             "choose a new --duckdb-database path to rebuild without overwriting it")
        if not args.archive.is_file():
            args.archive.parent.mkdir(parents=True, exist_ok=True)
            part = Path(str(args.archive) + ".part")
            subprocess.run(["curl", "--fail", "--location", "--continue-at", "-", "--retry", "5",
                            "--output", str(part), args.data_url], check=True)
            if part.stat().st_size != args.archive_bytes or sha256(part) != args.archive_sha256:
                raise ValueError("downloaded ClickBench archive size/SHA256 mismatch")
            os.replace(part, args.archive)
        if args.archive.stat().st_size != args.archive_bytes:
            raise ValueError(f"archive byte size mismatch: {args.archive}")
        print(f"Verifying archive SHA256: {args.archive}", flush=True)
        if sha256(args.archive) != args.archive_sha256:
            raise ValueError("ClickBench archive SHA256 mismatch")
        temporary_db = Path(str(args.duckdb_database) + ".loading")
        # Only this script's unpublished staging database can be discarded.
        temporary_db.unlink(missing_ok=True)
        Path(str(temporary_db) + ".wal").unlink(missing_ok=True)
        settings = (f"SET threads={args.threads}; SET memory_limit={sql_string(args.memory_limit)}; "
                    "SET preserve_insertion_order=false;\n")
        copy = (f"COPY hits FROM {sql_string(args.archive.resolve())} "
                "(FORMAT CSV, DELIMITER '\t', HEADER false, QUOTE '', ESCAPE '', NULL 'NULL');")
        sql = settings + schema + "\n" + copy + "\nCHECKPOINT;\n"
        log = args.cache_dir / "duckdb-load.log"
        print(f"Loading compressed data into {args.duckdb_database} "
              f"(threads={args.threads}, memory_limit={args.memory_limit}); log: {log}", flush=True)
        load_started = time.monotonic()
        with log.open("wb") as output:
            subprocess.run([args.duckdb_bin, "-batch", "-bail", str(temporary_db)],
                           input=sql.encode(), stdout=output, stderr=subprocess.STDOUT, check=True)
        count = subprocess.run([args.duckdb_bin, "-readonly", "-csv", "-noheader", str(temporary_db),
                                "-c", "SELECT count(*) FROM hits"], capture_output=True, text=True, check=True)
        if int(count.stdout.strip()) != args.row_count:
            raise ValueError(f"loaded row count mismatch: {count.stdout.strip()}; expected {args.row_count}")
        os.replace(temporary_db, args.duckdb_database)
        manifest_tmp = Path(str(manifest_path) + ".part")
        manifest_tmp.write_text(json.dumps(identity, indent=2) + "\n")
        os.replace(manifest_tmp, manifest_path)
        print(f"Loaded {args.row_count} rows in {time.monotonic() - load_started:.1f}s", flush=True)
    else:
        print(f"Reusing DuckDB database: {args.duckdb_database}", flush=True)
    args.mode = "generate"
    args.queries = ",".join(map(str, range(1, 44)))
    status = run(args)
    if not status:
        print(f"Ready: all 43 result caches saved in {args.expected_dir}", flush=True)
        print(f"Preparation elapsed: {time.monotonic() - started:.1f}s", flush=True)
    return status


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    cache = Path(os.environ.get("CLICKBENCH_CACHE_DIR", "/tmp/mydb/clickbench-cache"))
    parser.add_argument("--cache-dir", type=Path, default=cache)
    parser.add_argument("--expected-dir", type=Path, default=os.environ.get("CLICKBENCH_EXPECTED_DIR"))
    parser.add_argument("--duckdb-database", type=Path, default=os.environ.get("CLICKBENCH_DUCKDB_DATABASE"))
    parser.add_argument("--duckdb-bin", default=os.environ.get("DUCKDB_BIN", "duckdb"))
    parser.add_argument("--schema-file", type=Path, default=ROOT / "scripts/clickbench/create.sql")
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--data-url", default=os.environ.get("CLICKBENCH_DATA_URL", DATA_URL))
    parser.add_argument("--archive-bytes", type=int, default=int(os.environ.get("CLICKBENCH_DATA_EXPECTED_BYTES", ARCHIVE_BYTES)))
    parser.add_argument("--archive-sha256", default=os.environ.get("CLICKBENCH_DATA_SHA256", ARCHIVE_SHA256))
    parser.add_argument("--dataset-id", default=os.environ.get("CLICKBENCH_DATASET_ID"))
    parser.add_argument("--row-count", type=int, default=99997497)
    parser.add_argument("--threads", type=int, default=int(os.environ.get("CLICKBENCH_DUCKDB_THREADS", "4")))
    parser.add_argument("--memory-limit", default=os.environ.get("CLICKBENCH_DUCKDB_MEMORY_LIMIT", "8GB"))
    parser.add_argument("--timeout", type=int, default=int(os.environ.get("CLICKBENCH_DUCKDB_TIMEOUT", "1800")))
    args = parser.parse_args()
    args.expected_dir = args.expected_dir or args.cache_dir / "expected-duckdb"
    args.duckdb_database = args.duckdb_database or args.cache_dir / "duckdb/hits-v2.duckdb"
    args.archive = args.archive or args.cache_dir / "hits.tsv.gz"
    args.dataset_id = args.dataset_id or f"{args.data_url}#bytes={args.archive_bytes}"
    if min(args.row_count, args.threads, args.timeout, args.archive_bytes) < 1:
        parser.error("row count, threads, timeout, and archive size must be positive")
    if not re.fullmatch(r"[0-9a-fA-F]{64}", args.archive_sha256):
        parser.error("archive SHA256 must contain 64 hex digits")
    args.archive_sha256 = args.archive_sha256.lower()
    try:
        return prepare(args)
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
