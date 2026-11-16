#!/usr/bin/env python3
"""MySQL 8.0.46 vs mydb expression-semantics differential runner.

The fixed in-repository Rust/MTR tests remain the CI oracle. This optional
tool is for refreshing/reviewing that oracle against a locally available
MySQL 8.0.46 server. It compares success/error identity, result text, protocol
column type names, and SHOW WARNINGS output without modifying server data.

Example:
  scripts/mysql80_expression_diff.py \
    --mysql "mysql -h127.0.0.1 -P3306 -uroot -proot --ssl-mode=DISABLED" \
    --mydb "mysql -h127.0.0.1 -P3307 -uroot -proot --ssl-mode=DISABLED"
"""

from __future__ import annotations

import argparse
import json
import re
import shlex
import subprocess
import sys
from dataclasses import asdict, dataclass
from typing import Iterable


ERROR_RE = re.compile(r"ERROR\s+(\d+)\s+\(([0-9A-Z]{5})\)", re.IGNORECASE)
TYPE_RE = re.compile(r"^\s*Type:\s+(\S+)", re.MULTILINE)
WARNING_HEADER = "Level\tCode\tMessage"


@dataclass(frozen=True)
class Endpoint:
    name: str
    argv: list[str]


@dataclass(frozen=True)
class Case:
    name: str
    sql_mode: str
    sql: str


@dataclass(frozen=True)
class ColumnDefinition:
    name: str
    type: str
    length: int | None
    decimals: int | None
    flags: tuple[str, ...]
    charset: int | None
    collation: str | None
    database: str
    table: str
    org_table: str
    org_name: str


@dataclass(frozen=True)
class Outcome:
    status: str
    errno: int | None
    sqlstate: str | None
    result: list[str]
    columns: list[ColumnDefinition]
    warnings: list[str]
    materialized: list[str]


CASES = (
    Case(
        "between_right_recursion",
        "",
        "SELECT 5 BETWEEN 0 AND 10 BETWEEN 0 AND 1, "
        "(5 BETWEEN 0 AND 10) BETWEEN 0 AND 1",
    ),
    Case("like_comparison", "", "SELECT '10' LIKE '1%' = 1"),
    Case("regexp_comparison", "", "SELECT 'ab' REGEXP '^a' = 1"),
    Case("shift_xor", "", "SELECT 4 << 1 ^ 1"),
    Case("xor_bit_and", "", "SELECT 4 ^ 1 & 1"),
    Case("bit_and_shift", "", "SELECT 4 & 1 << 2"),
    Case("div_plus", "", "SELECT 8 DIV 2 + 1"),
    Case("div_left_associative", "", "SELECT 16 DIV 4 DIV 2"),
    Case("logical_xor_comparison", "", "SELECT 0 XOR 2 = 2"),
    Case("logical_xor_and", "", "SELECT 1 XOR 1 AND 0"),
    Case("bang_boundary", "", "SELECT !0 = 2, ~1 * 0, !!1, !!!1"),
    Case("pipes_default", "", "SELECT 2 * 3 || 4"),
    Case("pipes_concat", "PIPES_AS_CONCAT", "SELECT 2 * 3 || 4"),
    Case("pipes_ansi", "ANSI", "SELECT 2 * 3 || 4"),
    Case(
        "high_not_precedence",
        "HIGH_NOT_PRECEDENCE",
        "SELECT NOT 0 = 2, NOT 1 BETWEEN 0 AND 2",
    ),
    Case("assignment_scope", "", "SELECT @mydb_expr_diff := 0 OR 2"),
    Case("deprecated_tokens", "", "SELECT !1, !!1, !!!1, 1 && 1, 1 || 0"),
    Case("invalid_like_between", "", "SELECT 1 LIKE 1 BETWEEN 0 AND 1"),
    Case("invalid_in_between", "", "SELECT 1 IN (1) BETWEEN 0 AND 1"),
    Case("invalid_is_compare", "", "SELECT 1 IS TRUE = 1"),
    Case("connection_id_type", "", "SELECT CONNECTION_ID()"),
    Case(
        "signed_unsigned_control_type",
        "",
        "SELECT CASE WHEN 1 THEN CAST(1 AS SIGNED) ELSE CAST(1 AS UNSIGNED) END",
    ),
    Case(
        "temporal_control_type",
        "",
        "SELECT CASE WHEN 1 THEN CAST('2024-01-01' AS DATE) "
        "ELSE CAST('2024-01-01 00:00:00.123456' AS DATETIME(6)) END",
    ),
    Case("string_control_length", "", "SELECT IFNULL(1, 'test')"),
    Case(
        "binary_string_octet_semantics",
        "",
        "SELECT CHAR_LENGTH(_binary 0xC380FF61), "
        "HEX(REVERSE(_binary 0xC380FF61)), "
        "HEX(SUBSTRING(_binary 0xC380FF61, 2, 2)), "
        "HEX(LEFT(_binary 0xC380FF61, 2)), "
        "HEX(RIGHT(_binary 0xC380FF61, 2))",
    ),
    Case("warning_producing_constant", "", "SELECT 7 / 0, 7 / 0"),
)


def endpoint_arg(name: str, value: str) -> Endpoint:
    argv = shlex.split(value)
    if not argv:
        raise SystemExit(f"--{name} must not be empty")
    return Endpoint(name, argv)


def invoke(endpoint: Endpoint, sql: str, *, type_info: bool = False) -> subprocess.CompletedProcess[str]:
    options = ["--batch", "--raw"]
    if type_info:
        options.extend(["--column-type-info", "-vvv"])
    return subprocess.run(
        endpoint.argv + options + ["--execute", sql],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def quote_sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def error_identity(proc: subprocess.CompletedProcess[str]) -> tuple[int | None, str | None]:
    match = ERROR_RE.search(proc.stderr) or ERROR_RE.search(proc.stdout)
    if match is None:
        return None, None
    return int(match.group(1)), match.group(2).upper()


def parse_column_definitions(output: str) -> list[ColumnDefinition]:
    fields: list[ColumnDefinition] = []
    for block in re.split(r"(?=Field\s+\d+:)", output):
        if not block.startswith("Field "):
            continue
        values: dict[str, str] = {}
        first = block.splitlines()[0]
        name_match = re.match(r"Field\s+\d+:\s+`(.*)`", first)
        if name_match is None:
            continue
        for line in block.splitlines()[1:]:
            match = re.match(r"\s*([A-Za-z_]+):\s*(.*)$", line)
            if match:
                values[match.group(1).lower()] = match.group(2).strip().strip("`")
        collation_match = re.match(r"(.*)\s+\((\d+)\)$", values.get("collation", ""))
        fields.append(
            ColumnDefinition(
                name=name_match.group(1),
                type=values.get("type", ""),
                length=int(values["length"]) if values.get("length", "").isdigit() else None,
                decimals=(
                    int(values["decimals"])
                    if values.get("decimals", "").isdigit()
                    else None
                ),
                flags=tuple(sorted(values.get("flags", "").split())),
                charset=int(collation_match.group(2)) if collation_match else None,
                collation=collation_match.group(1) if collation_match else None,
                database=values.get("database", ""),
                table=values.get("table", ""),
                org_table=values.get("org_table", ""),
                org_name=values.get("org_name", ""),
            )
        )
    return fields


def run_case(endpoint: Endpoint, case: Case, *, materialize: bool) -> Outcome:
    prefix = f"SET SESSION sql_mode={quote_sql_string(case.sql_mode)};"
    proc = invoke(endpoint, f"{prefix} {case.sql}; SHOW WARNINGS")
    if proc.returncode != 0:
        errno, sqlstate = error_identity(proc)
        return Outcome("error", errno, sqlstate, [], [], [], [])

    lines = proc.stdout.rstrip("\n").splitlines()
    try:
        warning_at = lines.index(WARNING_HEADER)
    except ValueError:
        result = lines
        warnings: list[str] = []
    else:
        result = lines[:warning_at]
        warnings = lines[warning_at + 1 :]

    # A second, read-only execution asks mysql(1) for protocol field types.
    # Deprecated-syntax warnings from this probe are intentionally discarded.
    type_proc = invoke(endpoint, f"{prefix} {case.sql}", type_info=True)
    columns = parse_column_definitions(type_proc.stdout) if type_proc.returncode == 0 else []
    # Keep the legacy parser as a guard for older mysql clients whose verbose
    # output omits the Field blocks.
    if not columns and type_proc.returncode == 0:
        columns = [
            ColumnDefinition("", type_name, None, None, (), None, None, "", "", "", "")
            for type_name in TYPE_RE.findall(type_proc.stdout)
        ]
    materialized: list[str] = []
    if materialize and case.sql.lstrip().upper().startswith("SELECT "):
        table = "__mydb_expr_diff_" + re.sub(r"[^a-z0-9_]", "_", case.name.lower())[:40]
        ddl = invoke(
            endpoint,
            f"{prefix} DROP TEMPORARY TABLE IF EXISTS `{table}`; "
            f"CREATE TEMPORARY TABLE `{table}` AS {case.sql}; SHOW COLUMNS FROM `{table}`",
        )
        if ddl.returncode == 0:
            materialized = ddl.stdout.rstrip("\n").splitlines()
    return Outcome("ok", None, None, result, columns, warnings, materialized)


def mysql_version(endpoint: Endpoint) -> str:
    proc = invoke(endpoint, "SELECT VERSION()")
    if proc.returncode != 0:
        raise SystemExit(f"cannot query {endpoint.name}: {proc.stderr.strip()}")
    lines = proc.stdout.strip().splitlines()
    if len(lines) < 2:
        raise SystemExit(f"cannot parse {endpoint.name} VERSION(): {proc.stdout!r}")
    return lines[-1]


def main(argv: Iterable[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mysql", required=True, help="mysql CLI command for MySQL 8.0.46")
    parser.add_argument("--mydb", required=True, help="mysql CLI command for mydb")
    parser.add_argument("--json", action="store_true", help="emit machine-readable results")
    parser.add_argument(
        "--materialize",
        action="store_true",
        help="also compare CREATE TEMPORARY TABLE AS SELECT / SHOW COLUMNS",
    )
    parser.add_argument(
        "--allow-nonbaseline-oracle",
        action="store_true",
        help="allow an explicitly requested MySQL version other than 8.0.46",
    )
    args = parser.parse_args(list(argv))

    mysql = endpoint_arg("mysql", args.mysql)
    mydb = endpoint_arg("mydb", args.mydb)
    version = mysql_version(mysql)
    baseline = version.startswith("8.0.46")
    if not baseline and not args.allow_nonbaseline_oracle:
        raise SystemExit(f"expected MySQL 8.0.46 oracle, got {version}")
    if not baseline and not args.json:
        print(
            f"WARNING: observational comparison against MySQL {version}; "
            "the compatibility baseline remains 8.0.46"
        )

    report: list[dict[str, object]] = []
    failed = 0
    for case in CASES:
        expected = run_case(mysql, case, materialize=args.materialize)
        actual = run_case(mydb, case, materialize=args.materialize)
        equal = expected == actual
        failed += not equal
        report.append(
            {
                "name": case.name,
                "sql_mode": case.sql_mode,
                "sql": case.sql,
                "equal": equal,
                "mysql": asdict(expected),
                "mydb": asdict(actual),
            }
        )
        if not args.json:
            print(f"{'PASS' if equal else 'FAIL'} {case.name}")
            if not equal:
                print(f"  mysql: {expected}")
                print(f"  mydb:  {actual}")

    if args.json:
        json.dump(
            {
                "mysql_version": version,
                "baseline": baseline,
                "failed": failed,
                "cases": report,
            },
            sys.stdout,
            indent=2,
            sort_keys=True,
        )
        sys.stdout.write("\n")
    elif failed == 0:
        qualifier = "baseline" if baseline else "observational"
        print(
            f"MySQL {version} expression differential passed "
            f"({len(CASES)} cases, {qualifier})"
        )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
