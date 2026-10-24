#!/usr/bin/env python3
"""Compare TPC-DS query output against externally-generated expected result sets.

Tab-separated cells are trimmed and numbers are normalized to integer form or
4 decimals (half-up). Rows are compared as a multiset: SQL does not define the
relative order of rows tied on every ORDER BY key. See
scripts/tpcds/expected/README.md for how to populate an expected set.
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path


try:
    from ._tabular_results import CanonicalResult, normalize_cell, query_numbers, write_csv
    from ._tabular_results import canonicalize as _canonicalize
except ImportError:
    from _tabular_results import CanonicalResult, normalize_cell, query_numbers, write_csv
    from _tabular_results import canonicalize as _canonicalize


def canonicalize(path: Path) -> CanonicalResult:
    return _canonicalize(path, sort_rows=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-dir", required=True, type=Path)
    parser.add_argument("--actual-dir", required=True, type=Path)
    parser.add_argument("--queries", default="1-99")
    parser.add_argument("--actual-pattern", default="q{query:02d}_result.txt")
    parser.add_argument("--output-csv", required=True, type=Path)
    parser.add_argument(
        "--query-status-csv", type=Path,
        help="Execution status CSV from the benchmark runner; only ok may pass",
    )
    args = parser.parse_args()

    execution_status = None
    if args.query_status_csv is not None:
        with args.query_status_csv.open(newline="") as stream:
            execution_status = {
                int(row["query_num"]): row["status"] for row in csv.DictReader(stream)
            }

    rows: list[dict[str, object]] = []
    failed = False

    for query in query_numbers(args.queries):
        expected_path = args.expected_dir / f"q{query:02d}.tsv"
        actual_path = args.actual_dir / args.actual_pattern.format(query=query)
        status = "ok"
        message = ""
        expected: CanonicalResult | None = None
        actual: CanonicalResult | None = None

        if execution_status is not None and execution_status.get(query) != "ok":
            status = "execution_failed"
            message = f"query execution status: {execution_status.get(query, 'not_run')}"
        elif not expected_path.is_file():
            status = "missing_expected"
            message = f"{expected_path} not found"
        elif not actual_path.is_file():
            status = "missing_actual"
            message = f"{actual_path} not found"
        else:
            expected = canonicalize(expected_path)
            actual = canonicalize(actual_path)
            if expected.rows != actual.rows:
                status = "mismatch"
                message = "canonical rows differ"

        if status != "ok":
            failed = True

        rows.append(
            {
                "query_num": query,
                "status": status,
                "expected_rows": len(expected.rows) if expected else "",
                "actual_rows": len(actual.rows) if actual else "",
                "expected_hash": expected.digest if expected else "",
                "actual_hash": actual.digest if actual else "",
                "message": message,
            }
        )

    write_csv(args.output_csv, rows)

    for row in rows:
        query = int(row["query_num"])
        status = row["status"]
        if status == "ok":
            print(f"  Q{query:<2}  PASS   rows={row['actual_rows']} hash={row['actual_hash']}")
        else:
            print(f"  Q{query:<2}  FAIL   {status}: {row['message']}", file=sys.stderr)

    return 2 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
