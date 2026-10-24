#!/usr/bin/env python3
"""Differential Table-DDL smoke suite for mydb vs MySQL 8.0.46.

The runner compares result/error shape, errno, SQLSTATE, warnings and explicit
metadata/data probes. It never uses implementation-specific catalog tables.
Run against disposable schemas: each endpoint's database is dropped/recreated.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys
from typing import Any, Iterable

@dataclasses.dataclass(frozen=True)
class Endpoint:
    host: str
    port: int
    user: str
    password: str


@dataclasses.dataclass(frozen=True)
class Outcome:
    ok: bool
    errno: int | None
    sqlstate: str | None
    rows: tuple[tuple[str, ...], ...]
    warnings: tuple[tuple[str, ...], ...]


@dataclasses.dataclass(frozen=True)
class Scenario:
    name: str
    statements: tuple[str, ...]
    probes: tuple[str, ...]


SCENARIOS = (
    Scenario(
        "instant_lock_none_1221",
        (
            "CREATE TABLE t(id INT PRIMARY KEY)",
            "ALTER TABLE t ALGORITHM=INSTANT, LOCK=NONE, ADD COLUMN v INT",
        ),
        ("SHOW CREATE TABLE t",),
    ),
    Scenario(
        "default_algorithm_lock_none",
        (
            "CREATE TABLE t(id INT PRIMARY KEY)",
            "ALTER TABLE t LOCK=NONE, ADD COLUMN v INT",
        ),
        ("SHOW CREATE TABLE t",),
    ),
    Scenario(
        "unknown_engine_1286",
        ("CREATE TABLE t(id INT) ENGINE=unknown_engine_xyz",),
        ("SHOW TABLES",),
    ),
    Scenario(
        "parenthesized_add",
        (
            "CREATE TABLE t(id INT PRIMARY KEY)",
            "ALTER TABLE t ADD (a INT DEFAULT (1+2), b VARCHAR(20) DEFAULT 'x,y')",
            "INSERT INTO t(id) VALUES(1)",
        ),
        ("SELECT * FROM t", "SHOW CREATE TABLE t"),
    ),
    Scenario(
        "charset_conversion_atomic",
        (
            "CREATE TABLE t(id INT PRIMARY KEY, v VARCHAR(20), KEY(v)) DEFAULT CHARSET=utf8mb4",
            "INSERT INTO t VALUES(1,'汉字')",
            "ALTER TABLE t CONVERT TO CHARACTER SET latin1",
        ),
        ("SELECT * FROM t", "SHOW CREATE TABLE t"),
    ),
    Scenario(
        "compound_alter_unique_preflight",
        (
            "CREATE TABLE t(id INT PRIMARY KEY, v INT, duplicate_key INT)",
            "INSERT INTO t VALUES(1,10,7),(2,20,7)",
            "ALTER TABLE t MODIFY v BIGINT, ADD UNIQUE INDEX ux(duplicate_key)",
        ),
        ("SHOW CREATE TABLE t", "SHOW INDEX FROM t"),
    ),
    Scenario(
        "drop_multi_atomic",
        (
            "CREATE TABLE a(id INT)",
            "DROP TABLE a, missing_table",
        ),
        ("SHOW TABLES",),
    ),
    Scenario(
        "temporary_create_is_not_rollbackable",
        (
            "BEGIN",
            "CREATE TEMPORARY TABLE tt(id INT)",
            "INSERT INTO tt VALUES(1)",
            "ROLLBACK",
        ),
        ("SELECT COUNT(*) FROM tt", "SHOW WARNINGS"),
    ),
    Scenario(
        "temporary_alter_implicitly_commits",
        (
            "CREATE TABLE committed(id INT PRIMARY KEY)",
            "CREATE TEMPORARY TABLE tt(id INT)",
            "BEGIN",
            "INSERT INTO committed VALUES(1)",
            "ALTER TABLE tt ADD COLUMN v INT",
            "ROLLBACK",
        ),
        ("SELECT COUNT(*) FROM committed", "SHOW CREATE TABLE tt"),
    ),
)


def stringify(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bytes):
        return "0x" + value.hex()
    return str(value)


def execute(cursor: Any, sql: str) -> Outcome:
    import pymysql

    try:
        cursor.execute(sql)
        rows = tuple(tuple(stringify(v) for v in row) for row in cursor.fetchall())
        cursor.execute("SHOW WARNINGS")
        warnings = tuple(tuple(stringify(v) for v in row) for row in cursor.fetchall())
        return Outcome(True, None, None, rows, warnings)
    except pymysql.MySQLError as err:
        errno = int(err.args[0]) if err.args else None
        sqlstate = getattr(err, "sqlstate", None)
        # PyMySQL does not consistently expose SQLSTATE. The server errno is
        # still compared, while protocol-focused suites cover the wire field.
        try:
            cursor.execute("SHOW WARNINGS")
            warnings = tuple(tuple(stringify(v) for v in row) for row in cursor.fetchall())
        except pymysql.MySQLError:
            warnings = ()
        return Outcome(False, errno, sqlstate, (), warnings)


def connect(endpoint: Endpoint, database: str | None = None) -> Any:
    import pymysql

    return pymysql.connect(
        host=endpoint.host,
        port=endpoint.port,
        user=endpoint.user,
        password=endpoint.password,
        database=database,
        autocommit=True,
        charset="utf8mb4",
    )


def reset(endpoint: Endpoint, database: str) -> None:
    with connect(endpoint) as conn, conn.cursor() as cursor:
        cursor.execute(f"DROP DATABASE IF EXISTS `{database}`")
        cursor.execute(f"CREATE DATABASE `{database}`")


def run(endpoint: Endpoint, database: str, scenario: Scenario) -> dict[str, Any]:
    reset(endpoint, database)
    outcomes: list[Outcome] = []
    probes: list[Outcome] = []
    with connect(endpoint, database) as conn, conn.cursor() as cursor:
        for sql in scenario.statements:
            outcomes.append(execute(cursor, sql))
        for sql in scenario.probes:
            probes.append(execute(cursor, sql))
    return {
        "statements": [dataclasses.asdict(item) for item in outcomes],
        "probes": [dataclasses.asdict(item) for item in probes],
    }


def comparable_outcome(outcome: dict[str, Any], sql: str) -> dict[str, Any]:
    """Remove fields that MySQL explicitly leaves estimate/diagnostic-specific.

    SHOW INDEX cardinality is an optimizer estimate, not a DDL semantic. Error
    text remains in the JSON report through ``exact_matches``; the pass/fail
    gate compares response kind, errno, SQLSTATE, warning level/code and all
    deterministic rows.
    """
    comparable = dict(outcome)
    rows = [list(row) for row in comparable["rows"]]
    if sql.lstrip().upper().startswith(("SHOW INDEX", "SHOW INDEXES", "SHOW KEYS")):
        for row in rows:
            if len(row) > 6:
                row[6] = "<cardinality>"
    comparable["rows"] = rows
    if not comparable["ok"]:
        comparable["warnings"] = [list(row[:2]) for row in comparable["warnings"]]
    return comparable


def comparable_run(result: dict[str, Any], scenario: Scenario) -> dict[str, Any]:
    return {
        "statements": [
            comparable_outcome(outcome, sql)
            for outcome, sql in zip(result["statements"], scenario.statements)
        ],
        "probes": [
            comparable_outcome(outcome, sql)
            for outcome, sql in zip(result["probes"], scenario.probes)
        ],
    }


def endpoint_from_args(args: argparse.Namespace, prefix: str) -> Endpoint:
    return Endpoint(
        getattr(args, f"{prefix}_host"),
        getattr(args, f"{prefix}_port"),
        getattr(args, f"{prefix}_user"),
        getattr(args, f"{prefix}_password"),
    )


def add_endpoint(parser: argparse.ArgumentParser, prefix: str, default_port: int) -> None:
    parser.add_argument(f"--{prefix}-host", default="127.0.0.1")
    parser.add_argument(f"--{prefix}-port", type=int, default=default_port)
    parser.add_argument(f"--{prefix}-user", default="root")
    parser.add_argument(f"--{prefix}-password", default="root" if prefix == "mydb" else "")


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    add_endpoint(parser, "mysql", 3306)
    add_endpoint(parser, "mydb", 3307)
    parser.add_argument("--database", default="mydb_table_ddl_diff")
    parser.add_argument("--json")
    args = parser.parse_args(argv)
    mysql = endpoint_from_args(args, "mysql")
    mydb = endpoint_from_args(args, "mydb")

    report: dict[str, Any] = {"scenarios": {}}
    failed = 0
    for scenario in SCENARIOS:
        oracle = run(mysql, args.database, scenario)
        actual = run(mydb, args.database, scenario)
        exact_matches = oracle == actual
        matches = comparable_run(oracle, scenario) == comparable_run(actual, scenario)
        failed += int(not matches)
        report["scenarios"][scenario.name] = {
            "matches": matches,
            "exact_matches": exact_matches,
            "mysql": oracle,
            "mydb": actual,
        }
        print(f"{'PASS' if matches else 'FAIL'} {scenario.name}")

    rendered = json.dumps(report, ensure_ascii=False, indent=2)
    if args.json:
        with open(args.json, "w", encoding="utf-8") as output:
            output.write(rendered)
            output.write("\n")
    elif failed:
        print(rendered)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
