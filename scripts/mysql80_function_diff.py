#!/usr/bin/env python3
"""Capture/check the per-function MySQL 8.0.46 compatibility fixtures."""

from __future__ import annotations

import argparse
import json
import math
import re
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "src/engine/executor/tests/function_compat/fixtures"
ERROR_RE = re.compile(r"ERROR\s+(\d+)\s+\(([0-9A-Z]{5})\)", re.I)
BEGIN_RESULT = "__MYDB_FUNCTION_RESULT_BEGIN__"
END_RESULT = "__MYDB_FUNCTION_RESULT_END__"
BEGIN_WARNINGS = "__MYDB_FUNCTION_WARNINGS_BEGIN__"
END_WARNINGS = "__MYDB_FUNCTION_WARNINGS_END__"
SESSION_SETUP = (
    "SET SESSION sql_mode=''",
    "SET SESSION time_zone='+00:00'",
    "SET SESSION lc_time_names='en_US'",
    "SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci",
    "SET timestamp=1788105600",
)


@dataclass(frozen=True)
class Endpoint:
    name: str
    argv: list[str]


def endpoint(name: str, command: str) -> Endpoint:
    argv = shlex.split(command)
    if not argv:
        raise SystemExit(f"--{name} command is empty")
    return Endpoint(name, argv)


def invoke(
    ep: Endpoint,
    sql: str,
    *,
    type_info: bool = False,
    column_names: bool = False,
) -> subprocess.CompletedProcess[str]:
    options = ["--batch", "--raw"]
    if not column_names:
        options.append("--skip-column-names")
    if type_info:
        options = ["--batch", "--raw", "--column-type-info", "-vvv"]
    return subprocess.run(
        ep.argv + options + ["--execute", sql],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def server_version(ep: Endpoint) -> str:
    proc = invoke(ep, "SELECT VERSION()")
    if proc.returncode != 0:
        raise SystemExit(f"cannot query {ep.name} version: {proc.stderr.strip()}")
    return proc.stdout.strip().splitlines()[-1]


def split_region(lines: list[str], begin: str, end: str) -> list[str]:
    try:
        left = lines.index(begin)
        right = lines.index(end, left + 1)
    except ValueError:
        return []
    return lines[left + 1 : right]


def parse_columns(output: str) -> list[dict[str, object]]:
    columns = []
    blocks = re.split(r"(?=^Field\s+\d+:)", output, flags=re.M)[1:]
    for block in blocks:
        values: dict[str, str] = {}
        lines = block.splitlines()
        first = lines[0]
        name = re.match(r"Field\s+\d+:\s+`(.*?)`", first)
        for line in lines[1:]:
            if not line.strip():
                break
            match = re.match(r"\s*([A-Za-z_]+):\s*(.*)$", line)
            if match:
                values[match.group(1).lower()] = match.group(2).strip().strip("`")
        columns.append(
            {
                "name": name.group(1) if name else "",
                "type": values.get("type", ""),
                "length": int(values["length"]) if values.get("length", "").isdigit() else None,
                "decimals": int(values["decimals"]) if values.get("decimals", "").isdigit() else None,
                "flags": sorted(values.get("flags", "").split()),
                "collation": values.get("collation", ""),
            }
        )
    return columns


def mysql_type_to_rust(type_name: str) -> str | None:
    normalized = type_name.upper().removeprefix("MYSQL_TYPE_")
    mapping = {
        "TINY": "Tiny",
        "SHORT": "Short",
        "INT24": "MediumInt",
        "LONG": "Long",
        "LONGLONG": "LongLong",
        "FLOAT": "Float",
        "DOUBLE": "Double",
        "DATE": "Date",
        "TIME": "Time",
        "DATETIME": "DateTime",
        "TIMESTAMP": "Timestamp",
        "JSON": "Json",
        "GEOMETRY": "Geometry",
        "VECTOR": "Vector",
        "BLOB": "Blob",
        "TINY_BLOB": "Blob",
        "MEDIUM_BLOB": "Blob",
        "LONG_BLOB": "Blob",
        "STRING": "VarString",
        "VAR_STRING": "VarString",
        "VARCHAR": "VarString",
        "NEWDECIMAL": "Decimal*",
        "DECIMAL": "Decimal*",
    }
    return mapping.get(normalized)


def capture_outcome(ep: Endpoint, case: dict[str, object]) -> dict[str, object]:
    setup = [*SESSION_SETUP, *case.get("setup", [])]
    cleanup = case.get("cleanup", [])
    sql = "; ".join(
        [
            *setup,
            f"SELECT '{BEGIN_RESULT}'",
            str(case["mysql_sql"] if ep.name == "mysql" and case.get("mysql_sql") else case["sql"]),
            "SHOW WARNINGS",
            f"SELECT '{END_RESULT}' AS marker",
            *cleanup,
        ]
    )
    proc = invoke(ep, sql, column_names=True)
    if proc.returncode != 0:
        match = ERROR_RE.search(proc.stderr) or ERROR_RE.search(proc.stdout)
        return {
            "status": "error",
            "errno": int(match.group(1)) if match else None,
            "sqlstate": match.group(2).upper() if match else None,
        }
    lines = proc.stdout.rstrip("\n").splitlines()
    try:
        marker_header = lines.index(BEGIN_RESULT)
        result_begin = lines.index(BEGIN_RESULT, marker_header + 1)
        end = lines.index(END_RESULT, result_begin + 1)
        try:
            warning_header = lines.index("Level\tCode\tMessage", result_begin + 1, end)
        except ValueError:
            warning_header = end - 1  # `marker` header emitted by the END SELECT.
        result_lines = lines[result_begin + 1 : warning_header]
        # The first line is the main SELECT's column header.
        if result_lines:
            result_lines = result_lines[1:]
        warning_lines = (
            lines[warning_header + 1 : end]
            if lines[warning_header] == "Level\tCode\tMessage"
            else []
        )
        # The END marker SELECT contributes its own column header.
        if warning_lines and warning_lines[-1] == "marker":
            warning_lines.pop()
    except ValueError:
        result_lines = []
        warning_lines = []
    rows = [line.split("\t") for line in result_lines]
    warning_codes = []
    for line in warning_lines:
        parts = line.split("\t", 2)
        if len(parts) >= 2 and parts[1].isdigit():
            warning_codes.append(int(parts[1]))

    columns: list[dict[str, object]] = []
    if "metadata" in case.get("tags", []):
        type_sql = "; ".join([*setup, str(case.get("mysql_sql") if ep.name == "mysql" and case.get("mysql_sql") else case["sql"])])
        type_proc = invoke(ep, type_sql, type_info=True)
        columns = parse_columns(type_proc.stdout) if type_proc.returncode == 0 else []
    outcome: dict[str, object] = {
        "status": "ok",
        "rows": rows,
        "warning_codes": warning_codes,
        "columns": columns,
    }
    mapped_types = [mysql_type_to_rust(str(column["type"])) for column in columns]
    if columns and all(mapped_types):
        outcome["column_types"] = mapped_types
    return outcome


def fixture_paths() -> list[Path]:
    return sorted(FIXTURE_DIR.glob("*.json"))


def comparable(outcome: dict[str, object]) -> dict[str, object]:
    return {
        key: outcome.get(key)
        for key in ("status", "rows", "errno", "sqlstate", "warning_codes", "columns")
        if key in outcome
    }


DYNAMIC_FUNCTIONS = {
    "CONNECTION_ID",
    "FOUND_ROWS",
    "ROW_COUNT",
    "PS_CURRENT_THREAD_ID",
    "PS_THREAD_ID",
    "UUID",
    "UUID_SHORT",
    "USER",
    "SESSION_USER",
    "SYSTEM_USER",
    "CURRENT_USER",
    "VERSION",
    "SYSDATE",
}
APPROXIMATE_FUNCTIONS = {
    "ACOS",
    "ASIN",
    "ATAN",
    "ATAN2",
    "COS",
    "COT",
    "DEGREES",
    "EXP",
    "LN",
    "LOG",
    "LOG10",
    "LOG2",
    "PI",
    "POW",
    "POWER",
    "RADIANS",
    "SIN",
    "SQRT",
    "TAN",
    "ST_DISTANCE_SPHERE",
    "ST_FRECHETDISTANCE",
    "ST_HAUSDORFFDISTANCE",
    "ST_LATFROMGEOHASH",
    "ST_LATITUDE",
    "ST_LONGFROMGEOHASH",
    "ST_LONGITUDE",
}


def dynamic_cell(function: str, value: str) -> bool:
    if function in {"CONNECTION_ID", "FOUND_ROWS", "PS_CURRENT_THREAD_ID", "PS_THREAD_ID", "UUID_SHORT"}:
        return value.isdigit()
    if function == "ROW_COUNT":
        return bool(re.fullmatch(r"-?\d+", value))
    if function in {"USER", "SESSION_USER", "SYSTEM_USER", "CURRENT_USER"}:
        return "@" in value
    if function == "VERSION":
        return value.startswith("8.0.46")
    if function == "UUID":
        return bool(re.fullmatch(r"(?i)[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", value))
    if function == "SYSDATE":
        return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{1,6})?", value))
    return False


def normalize_wkt(value: str) -> str:
    return value.replace("MULTIPOINT((", "MULTIPOINT(").replace("),(", ",").replace("))", ")").replace(" ", "")


def semantic_cell_equal(function: str, left: object, right: object) -> bool:
    if left == right:
        return True
    if not isinstance(left, str) or not isinstance(right, str):
        return False
    if function in DYNAMIC_FUNCTIONS and dynamic_cell(function, left) and dynamic_cell(function, right):
        return True
    if left.rstrip("0").rstrip(".") == right.rstrip("0").rstrip(".") and ("." in left or "." in right):
        return True
    if f"{left} 00:00:00" == right or f"{right} 00:00:00" == left:
        return True
    if re.sub(r"\.0+$", "", left) == re.sub(r"\.0+$", "", right):
        return True
    if left.startswith(("POINT(", "LINESTRING(", "POLYGON(", "MULTI", "GEOMETRYCOLLECTION(")) and right.startswith(("POINT(", "LINESTRING(", "POLYGON(", "MULTI", "GEOMETRYCOLLECTION(")):
        if normalize_wkt(left) == normalize_wkt(right):
            return True
        number = re.compile(r"-?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?")
        if number.sub("#", left) == number.sub("#", right):
            left_values = [float(value) for value in number.findall(left)]
            right_values = [float(value) for value in number.findall(right)]
            return len(left_values) == len(right_values) and all(
                math.isclose(a, b, rel_tol=1e-5, abs_tol=1e-9)
                for a, b in zip(left_values, right_values)
            )
    try:
        left_json = json.loads(left)
        right_json = json.loads(right)
    except (json.JSONDecodeError, TypeError):
        pass
    else:
        if left_json == right_json:
            return True
    if function in APPROXIMATE_FUNCTIONS:
        try:
            return math.isclose(float(left), float(right), rel_tol=1e-5, abs_tol=1e-9)
        except ValueError:
            return False
    return False


def outcomes_equal(function: str, case: dict[str, object], left: dict[str, object], right: dict[str, object]) -> bool:
    if left.get("status") != right.get("status"):
        return False
    if case.get("comparison") == "status_only":
        return True
    if left.get("status") == "error":
        return (left.get("errno"), left.get("sqlstate")) == (right.get("errno"), right.get("sqlstate"))
    left_rows = left.get("rows", [])
    right_rows = right.get("rows", [])
    if len(left_rows) != len(right_rows):
        return False
    for left_row, right_row in zip(left_rows, right_rows):
        if len(left_row) != len(right_row):
            return False
        if not all(semantic_cell_equal(function, a, b) for a, b in zip(left_row, right_row)):
            return False
    if left.get("warning_codes", []) != right.get("warning_codes", []):
        return False
    if (
        case.get("metadata_comparison", "exact") == "exact"
        and "columns" in left
        and "columns" in right
        and left["columns"] != right["columns"]
    ):
        return False
    return True


def capture(
    mysql: Endpoint,
    mydb: Endpoint,
    *,
    only_uncaptured: bool,
    selected: set[str],
) -> int:
    version = server_version(mysql)
    if not version.startswith("8.0.46"):
        raise SystemExit(f"expected MySQL 8.0.46 oracle, got {version}")
    failed = 0
    for path in fixture_paths():
        fixture = json.loads(path.read_text())
        fixture["mysql_version"] = "8.0.46"
        for function in fixture["functions"]:
            if selected and function["name"] not in selected:
                continue
            for case in function["cases"]:
                if only_uncaptured and case.get("expected", {}).get("status") != "uncaptured":
                    continue
                mysql_outcome = capture_outcome(mysql, case)
                mydb_outcome = capture_outcome(mydb, case)
                case["mysql_probe"] = mysql_outcome
                case["mydb_probe"] = mydb_outcome
                if case.get("comparison") == "offline_contract":
                    pass
                elif function["oracle_class"] in {"mysql80_community", "mysql80_mapped"} and case.get("comparison") != "contract":
                    case["expected"] = {
                        key: value
                        for key, value in mysql_outcome.items()
                        if key != "columns"
                    }
                    if comparable(mysql_outcome) != comparable(mydb_outcome):
                        failed += 1
                        print(f"DIFF {function['name']}/{case['id']}")
                        print(f"  mysql={comparable(mysql_outcome)}")
                        print(f"  mydb ={comparable(mydb_outcome)}")
                else:
                    case["expected"] = {
                        key: value for key, value in mydb_outcome.items() if key != "columns"
                    }
        path.write_text(json.dumps(fixture, indent=2, sort_keys=False) + "\n")
    print(f"captured {len(fixture_paths())} fixture files; live differences={failed}")
    return 1 if failed else 0


def check(mysql: Endpoint, mydb: Endpoint, *, selected: set[str]) -> int:
    version = server_version(mysql)
    if not version.startswith("8.0.46"):
        raise SystemExit(f"expected MySQL 8.0.46 oracle, got {version}")
    failed = 0
    checked = 0
    for path in fixture_paths():
        fixture = json.loads(path.read_text())
        if fixture.get("mysql_version") != "8.0.46":
            print(f"FAIL {path}: fixture version is not 8.0.46")
            failed += 1
            continue
        for function in fixture["functions"]:
            if selected and function["name"] not in selected:
                continue
            for case in function["cases"]:
                checked += 1
                mysql_outcome = capture_outcome(mysql, case)
                mydb_outcome = capture_outcome(mydb, case)
                expected = case["expected"]
                function_name = function["name"]
                oracle_equal = outcomes_equal(function_name, case, mysql_outcome, case.get("mysql_probe", {}))
                if case.get("comparison") == "offline_contract":
                    mydb_equal = True
                elif function["oracle_class"] in {"mysql80_community", "mysql80_mapped"} and case.get("comparison") != "contract":
                    mydb_equal = outcomes_equal(function_name, case, mydb_outcome, mysql_outcome)
                else:
                    mydb_equal = outcomes_equal(function_name, case, mydb_outcome, expected)
                if not oracle_equal or not mydb_equal:
                    failed += 1
                    print(f"FAIL {function['name']}/{case['id']}: oracle={oracle_equal} mydb={mydb_equal}")
    print(f"checked {checked} cases; failures={failed}")
    return 1 if failed else 0


def main() -> int:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--capture", action="store_true")
    mode.add_argument("--check", action="store_true")
    parser.add_argument("--mysql", required=True, help="mysql CLI command for MySQL 8.0.46")
    parser.add_argument("--mydb", required=True, help="mysql CLI command for mydb")
    parser.add_argument(
        "--only-uncaptured",
        action="store_true",
        help="with --capture, leave already captured cases untouched",
    )
    parser.add_argument(
        "--function",
        action="append",
        default=[],
        help="limit capture/check to one function name; may be repeated",
    )
    args = parser.parse_args()
    mysql = endpoint("mysql", args.mysql)
    mydb = endpoint("mydb", args.mydb)
    return (
        capture(
            mysql,
            mydb,
            only_uncaptured=args.only_uncaptured,
            selected={name.upper() for name in args.function},
        )
        if args.capture
        else check(
            mysql,
            mydb,
            selected={name.upper() for name in args.function},
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
