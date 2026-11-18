#!/usr/bin/env python3
"""Compare TPC-DS query output against externally-generated expected result sets.

TSV values and row order are exact by default. Explicit per-query comparison
contracts may normalize declared numeric columns and reorder only ORDER BY ties.
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path


try:
    from ._tabular_results import CanonicalResult, comparison_contracts, query_numbers, write_csv
    from ._tabular_results import canonicalize as _canonicalize
except ImportError:
    from _tabular_results import CanonicalResult, comparison_contracts, query_numbers, write_csv
    from _tabular_results import canonicalize as _canonicalize


def canonicalize(path: Path, *, contract: dict | None = None) -> CanonicalResult:
    return _canonicalize(path, contract=contract)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-dir", required=True, type=Path)
    parser.add_argument("--actual-dir", required=True, type=Path)
    parser.add_argument("--queries", type=lambda value: query_numbers(value, maximum=99), default="1-99")
    parser.add_argument("--actual-pattern", default="q{query:02d}_result.txt")
    parser.add_argument("--output-csv", required=True, type=Path)
    parser.add_argument(
        "--query-status-csv", type=Path,
        help="Execution status CSV from the benchmark runner; only ok may pass",
    )
    parser.add_argument("--comparison-manifest", type=Path,
                        help="JSON query contracts; defaults to expected-dir/comparison.json if present, otherwise exact ordered TSV")
    args = parser.parse_args()
    manifest = args.comparison_manifest
    if manifest is None and (args.expected_dir / 'comparison.json').is_file():
        manifest = args.expected_dir / 'comparison.json'
    try:
        contracts = comparison_contracts(manifest)
    except (OSError, ValueError) as error:
        parser.error(str(error))

    execution_status = None
    if args.query_status_csv is not None:
        with args.query_status_csv.open(newline="") as stream:
            execution_status = {
                int(row["query_num"]): row["status"] for row in csv.DictReader(stream)
            }

    rows: list[dict[str, object]] = []
    failed = False

    for query in args.queries:
        contract = contracts.get(str(query), {})
        extension = 'json' if contract.get('format') == 'typed-json' else 'tsv'
        expected_path = args.expected_dir / f"q{query:02d}.{extension}"
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
            try:
                expected = canonicalize(expected_path, contract=contract)
            except (OSError, ValueError) as error:
                status = "invalid_expected"
                message = str(error)
            if status == "ok":
                try:
                    actual = canonicalize(actual_path, contract=contract)
                except (OSError, ValueError) as error:
                    status = "invalid_actual"
                    message = str(error)
            if status == "ok":
                if expected.rows != actual.rows or expected.metadata != actual.metadata:
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
