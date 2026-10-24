#!/usr/bin/env python3
"""Scenario-based INFORMATION_SCHEMA data differential for MySQL 8.0.46.

The static fixture checks column shapes. This tool creates the same isolated
catalog objects on Oracle MySQL and mydb, compares deterministic metadata
projections, and then removes only its dedicated schema.
"""

from __future__ import annotations

import argparse
import json
import re
import shlex
import subprocess
import sys
from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class Endpoint:
    name: str
    argv: list[str]


def run(endpoint: Endpoint, sql: str, *, check: bool = True) -> list[list[str]]:
    proc = subprocess.run(
        endpoint.argv + ["--batch", "--raw", "--skip-column-names", "-e", sql],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if check and proc.returncode != 0:
        raise RuntimeError(f"{endpoint.name}: {sql}\n{proc.stderr.strip()}")
    return [line.split("\t") for line in proc.stdout.splitlines()]


def setup(endpoint: Endpoint, schema: str) -> None:
    statements = [
        f"DROP DATABASE IF EXISTS `{schema}`",
        f"CREATE DATABASE `{schema}` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci",
        f"CREATE TABLE `{schema}`.`parent` ("
        "id INT PRIMARY KEY, code VARCHAR(16) CHARACTER SET latin1 NOT NULL,"
        "amount DECIMAL(10,2) DEFAULT 0.00, UNIQUE KEY uk_code(code),"
        "CONSTRAINT chk_amount CHECK (amount >= 0))",
        f"CREATE TABLE `{schema}`.`child` ("
        "id INT PRIMARY KEY, parent_id INT, qty INT, total INT AS (qty * 2) VIRTUAL,"
        "KEY idx_parent(parent_id), CONSTRAINT fk_child_parent FOREIGN KEY(parent_id) "
        f"REFERENCES `{schema}`.`parent`(id) ON DELETE CASCADE)",
        f"CREATE TABLE `{schema}`.`parted` (id INT PRIMARY KEY, v VARCHAR(20)) "
        "PARTITION BY HASH(id) PARTITIONS 2",
        f"CREATE TABLE `{schema}`.`geo` (id INT PRIMARY KEY, g GEOMETRY NOT NULL SRID 4326)",
        f"INSERT INTO `{schema}`.`parent` VALUES (1,'a',10.25),(2,'b',20.50)",
        f"INSERT INTO `{schema}`.`child`(id,parent_id,qty) VALUES (10,1,5),(11,2,7)",
        f"INSERT INTO `{schema}`.`parted` VALUES (1,'one'),(2,'two'),(3,'three')",
        f"CREATE VIEW `{schema}`.`v_parent` AS SELECT id,code FROM `{schema}`.`parent`",
        f"CREATE PROCEDURE `{schema}`.`p_echo`(IN p INT) SELECT p",
        f"CREATE TRIGGER `{schema}`.`bi_parent` BEFORE INSERT ON `{schema}`.`parent` "
        "FOR EACH ROW SET NEW.amount=COALESCE(NEW.amount,0)",
        f"ANALYZE TABLE `{schema}`.`parent`",
        f"ANALYZE TABLE `{schema}`.`child`",
    ]
    for statement in statements:
        run(endpoint, statement)


def queries(schema: str) -> dict[str, str]:
    q = lambda projection, table, order: (
        f"SELECT {projection} FROM information_schema.{table} "
        f"WHERE TABLE_SCHEMA='{schema}' ORDER BY {order}"
    )
    return {
        "SCHEMATA": (
            "SELECT SCHEMA_NAME,DEFAULT_CHARACTER_SET_NAME,DEFAULT_COLLATION_NAME,DEFAULT_ENCRYPTION "
            f"FROM information_schema.SCHEMATA WHERE SCHEMA_NAME='{schema}'"
        ),
        "TABLES": q("TABLE_NAME,TABLE_TYPE,CREATE_OPTIONS,TABLE_COMMENT", "TABLES", "TABLE_NAME"),
        "COLUMNS": q(
            "TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,COLUMN_DEFAULT,IS_NULLABLE,DATA_TYPE,"
            "CHARACTER_MAXIMUM_LENGTH,CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,"
            "DATETIME_PRECISION,CHARACTER_SET_NAME,COLLATION_NAME,COLUMN_TYPE,COLUMN_KEY,EXTRA,SRS_ID",
            "COLUMNS",
            "TABLE_NAME,ORDINAL_POSITION",
        ),
        "STATISTICS": q(
            "TABLE_NAME,NON_UNIQUE,INDEX_NAME,SEQ_IN_INDEX,COLUMN_NAME,COLLATION,SUB_PART,NULLABLE,"
            "INDEX_TYPE,IS_VISIBLE,EXPRESSION",
            "STATISTICS",
            "TABLE_NAME,INDEX_NAME,SEQ_IN_INDEX",
        ),
        "TABLE_CONSTRAINTS": q(
            "TABLE_NAME,CONSTRAINT_NAME,CONSTRAINT_TYPE,ENFORCED",
            "TABLE_CONSTRAINTS",
            "TABLE_NAME,CONSTRAINT_NAME",
        ),
        "KEY_COLUMN_USAGE": q(
            "TABLE_NAME,CONSTRAINT_NAME,COLUMN_NAME,ORDINAL_POSITION,POSITION_IN_UNIQUE_CONSTRAINT,"
            "REFERENCED_TABLE_SCHEMA,REFERENCED_TABLE_NAME,REFERENCED_COLUMN_NAME",
            "KEY_COLUMN_USAGE",
            "TABLE_NAME,CONSTRAINT_NAME,ORDINAL_POSITION",
        ),
        "REFERENTIAL_CONSTRAINTS": (
            "SELECT TABLE_NAME,CONSTRAINT_NAME,UNIQUE_CONSTRAINT_NAME,MATCH_OPTION,UPDATE_RULE,DELETE_RULE,"
            "REFERENCED_TABLE_NAME FROM information_schema.REFERENTIAL_CONSTRAINTS "
            f"WHERE CONSTRAINT_SCHEMA='{schema}' ORDER BY TABLE_NAME,CONSTRAINT_NAME"
        ),
        "CHECK_CONSTRAINTS": (
            "SELECT CONSTRAINT_NAME,CHECK_CLAUSE FROM information_schema.CHECK_CONSTRAINTS "
            f"WHERE CONSTRAINT_SCHEMA='{schema}' ORDER BY CONSTRAINT_NAME"
        ),
        "PARTITIONS": q(
            "TABLE_NAME,PARTITION_NAME,PARTITION_ORDINAL_POSITION,PARTITION_METHOD,PARTITION_EXPRESSION,"
            "PARTITION_DESCRIPTION",
            "PARTITIONS",
            "TABLE_NAME,PARTITION_ORDINAL_POSITION",
        ),
        "VIEWS": q(
            "TABLE_NAME,CHECK_OPTION,IS_UPDATABLE,SECURITY_TYPE,CHARACTER_SET_CLIENT,COLLATION_CONNECTION",
            "VIEWS",
            "TABLE_NAME",
        ),
        "ROUTINES": (
            "SELECT ROUTINE_NAME,ROUTINE_TYPE,DATA_TYPE,ROUTINE_BODY,EXTERNAL_LANGUAGE,IS_DETERMINISTIC,"
            "SQL_DATA_ACCESS,SECURITY_TYPE FROM information_schema.ROUTINES "
            f"WHERE ROUTINE_SCHEMA='{schema}' ORDER BY ROUTINE_NAME"
        ),
        "PARAMETERS": (
            "SELECT SPECIFIC_NAME,ORDINAL_POSITION,PARAMETER_MODE,PARAMETER_NAME,DATA_TYPE,DTD_IDENTIFIER,"
            "ROUTINE_TYPE FROM information_schema.PARAMETERS "
            f"WHERE SPECIFIC_SCHEMA='{schema}' ORDER BY SPECIFIC_NAME,ORDINAL_POSITION"
        ),
        "TRIGGERS": (
            "SELECT TRIGGER_NAME,EVENT_MANIPULATION,EVENT_OBJECT_TABLE,ACTION_ORDER,ACTION_ORIENTATION,"
            "ACTION_TIMING FROM information_schema.TRIGGERS "
            f"WHERE TRIGGER_SCHEMA='{schema}' ORDER BY TRIGGER_NAME"
        ),
        "ST_GEOMETRY_COLUMNS": q(
            "TABLE_NAME,COLUMN_NAME,SRS_NAME,SRS_ID,GEOMETRY_TYPE_NAME",
            "ST_GEOMETRY_COLUMNS",
            "TABLE_NAME,COLUMN_NAME",
        ),
    }


def main(argv: Iterable[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mysql", required=True)
    parser.add_argument("--mydb", required=True)
    parser.add_argument("--schema", default="is_compat_probe")
    parser.add_argument("--output")
    args = parser.parse_args(list(argv))
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,63}", args.schema):
        raise SystemExit("--schema must be a simple MySQL identifier")
    endpoints = [Endpoint("mysql", shlex.split(args.mysql)), Endpoint("mydb", shlex.split(args.mydb))]
    if not run(endpoints[0], "SELECT VERSION()")[0][0].startswith("8.0.46"):
        raise SystemExit("reference endpoint must be Oracle MySQL 8.0.46")
    report: dict[str, object] = {"schema": args.schema, "objects": {}}
    try:
        for endpoint in endpoints:
            setup(endpoint, args.schema)
        for name, sql in queries(args.schema).items():
            mysql_rows = run(endpoints[0], sql)
            mydb_rows = run(endpoints[1], sql)
            report["objects"][name] = {
                "equal": mysql_rows == mydb_rows,
                "mysql": mysql_rows,
                "mydb": mydb_rows,
            }
    finally:
        for endpoint in endpoints:
            run(endpoint, f"DROP DATABASE IF EXISTS `{args.schema}`", check=False)
    rendered = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output:
        with open(args.output, "w", encoding="utf-8") as output:
            output.write(rendered)
    else:
        sys.stdout.write(rendered)
    return 0 if all(item["equal"] for item in report["objects"].values()) else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
