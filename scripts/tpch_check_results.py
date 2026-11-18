#!/usr/bin/env python3
"""Compare TPC-H query output against checked-in canonical result sets."""

from __future__ import annotations

import argparse
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
    parser.add_argument("--queries", type=lambda value: query_numbers(value, maximum=26), default="1-22")
    parser.add_argument("--actual-pattern", default="q{query:02d}_result.txt")
    parser.add_argument("--output-csv", required=True, type=Path)
    parser.add_argument(
        "--allow-missing-expected",
        action="store_true",
        help="report queries without canonical fixtures as skipped instead of failing",
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

        if not expected_path.is_file():
            status = (
                "skipped_missing_expected"
                if args.allow_missing_expected
                else "missing_expected"
            )
            message = f"{expected_path} not found"
        elif not actual_path.is_file():
            status = "missing_actual"
            message = f"{actual_path} not found"
        else:
            expected = canonicalize(expected_path, contract=contracts.get(str(query)))
            actual = canonicalize(actual_path, contract=contracts.get(str(query)))
            if expected.rows != actual.rows or expected.metadata != actual.metadata:
                status = "mismatch"
                message = "canonical rows differ"

        if status not in {"ok", "skipped_missing_expected"}:
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
        elif status == "skipped_missing_expected":
            print(f"  Q{query:<2}  SKIP   no canonical expected result")
        else:
            print(f"  Q{query:<2}  FAIL   {status}: {row['message']}", file=sys.stderr)

    return 2 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
