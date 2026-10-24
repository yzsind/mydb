"""Shared tabular result normalization; callers select ordered/multiset semantics."""
from __future__ import annotations
import csv
import decimal
import hashlib
from dataclasses import dataclass
from pathlib import Path

@dataclass
class CanonicalResult:
    rows: list[tuple[str, ...]]
    digest: str


def normalize_cell(value: str) -> str:
    value = value.strip()
    if value.upper() == "NULL":
        return "NULL"
    try:
        num = decimal.Decimal(value)
    except decimal.InvalidOperation:
        return value
    if num == num.to_integral_value():
        return str(num.quantize(decimal.Decimal(1)))
    return format(
        num.quantize(decimal.Decimal("0.0001"), rounding=decimal.ROUND_HALF_UP).normalize(),
        "f",
    )


def canonicalize(path: Path, *, sort_rows: bool = False) -> CanonicalResult:
    rows: list[tuple[str, ...]] = []
    for line in path.read_text(errors="replace").splitlines():
        if not line.strip():
            continue
        rows.append(tuple(normalize_cell(cell) for cell in line.split("\t")))
    if sort_rows:
        rows.sort()
    payload = "\n".join("\t".join(row) for row in rows).encode()
    return CanonicalResult(rows, hashlib.sha256(payload).hexdigest())


def query_numbers(value: str) -> list[int]:
    numbers: list[int] = []
    for part in value.replace(",", " ").split():
        if "-" in part:
            start, end = part.split("-", 1)
            numbers.extend(range(int(start), int(end) + 1))
        else:
            numbers.append(int(part))
    return numbers


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    fields = [
        "query_num",
        "status",
        "expected_rows",
        "actual_rows",
        "expected_hash",
        "actual_hash",
        "message",
    ]
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

