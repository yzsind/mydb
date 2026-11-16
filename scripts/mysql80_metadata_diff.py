#!/usr/bin/env python3
"""Read-only MySQL 8.0 metadata compatibility diff.

The script compares a reference MySQL server with a mydb server through the
`mysql` CLI. It intentionally performs only metadata reads: SHOW FULL TABLES,
DESCRIBE, SHOW CREATE, and selected information_schema projections.

Example:
  scripts/mysql80_metadata_diff.py \
    --mysql "mysql -h127.0.0.1 -P3306 -uroot -proot --batch --raw --skip-column-names" \
    --mydb "mysql -h127.0.0.1 -P3307 -uroot -proot --batch --raw --skip-column-names"
"""

from __future__ import annotations

import argparse
import collections
import json
import pathlib
import shlex
import subprocess
import sys
from dataclasses import dataclass
from typing import Iterable


SYSTEM_SCHEMAS = ("information_schema", "mysql", "performance_schema", "sys")
ALLOWED_PERFORMANCE_SCHEMA_EXTRAS = {
    "clone_progress",
    "clone_status",
    "component_scheduler_tasks",
    "firewall_group_allowlist",
    "firewall_groups",
    "firewall_membership",
    "ndb_sync_excluded_objects",
    "ndb_sync_pending_objects",
    "replication_group_communication_information",
    "replication_group_configuration_version",
    "replication_group_member_actions",
    "tp_thread_group_state",
    "tp_thread_group_stats",
    "tp_thread_state",
}


def allowed_information_schema_extras() -> set[str]:
    matrix = pathlib.Path(__file__).resolve().parents[1] / "docs/mysql80-metadata-compat-matrix.json"
    payload = json.loads(matrix.read_text(encoding="utf-8"))
    return {
        surface["object"].upper()
        for surface in payload["surfaces"]
        if surface["compatibility_class"] != "mysql80_community"
    }


@dataclass(frozen=True)
class Endpoint:
    name: str
    argv: list[str]


def run_sql(endpoint: Endpoint, sql: str) -> list[list[str]]:
    proc = subprocess.run(
        endpoint.argv + ["-e", sql],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"{endpoint.name} metadata query failed: {sql}\n{proc.stderr.strip()}"
        )
    rows: list[list[str]] = []
    for line in proc.stdout.splitlines():
        rows.append(line.split("\t"))
    return rows


def table_names(endpoint: Endpoint, schema: str) -> list[str]:
    rows = run_sql(endpoint, f"SHOW FULL TABLES FROM `{schema}`")
    return sorted(row[0] for row in rows if row and row[0] != "__ERROR__")


def describe(endpoint: Endpoint, schema: str, table: str) -> list[list[str]]:
    return run_sql(endpoint, f"DESCRIBE `{schema}`.`{table}`")


def information_schema_columns(
    endpoint: Endpoint, schema: str, tables: list[str]
) -> list[list[str]]:
    table_filter = ",".join(
        "'" + table.replace("'", "''") + "'" for table in tables
    ) or "''"
    return run_sql(
        endpoint,
        "SELECT TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,"
        "COLUMN_DEFAULT,IS_NULLABLE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,"
        "CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION,"
        "CHARACTER_SET_NAME,COLLATION_NAME,COLUMN_TYPE,COLUMN_KEY,EXTRA,PRIVILEGES,"
        "COLUMN_COMMENT,GENERATION_EXPRESSION,SRS_ID "
        "FROM information_schema.columns "
        f"WHERE TABLE_SCHEMA = '{schema}' "
        f"AND TABLE_NAME IN ({table_filter}) "
        "ORDER BY TABLE_SCHEMA,TABLE_NAME,ORDINAL_POSITION",
    )


def information_schema_tables(
    endpoint: Endpoint, schema: str, tables: list[str]
) -> list[list[str]]:
    table_filter = ",".join(
        "'" + table.replace("'", "''") + "'" for table in tables
    ) or "''"
    return run_sql(
        endpoint,
        "SELECT TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,TABLE_TYPE,ENGINE,VERSION,ROW_FORMAT,"
        "TABLE_ROWS,AVG_ROW_LENGTH,DATA_LENGTH,MAX_DATA_LENGTH,INDEX_LENGTH,DATA_FREE,"
        "AUTO_INCREMENT,TABLE_COLLATION,CHECKSUM,CREATE_OPTIONS,TABLE_COMMENT "
        "FROM information_schema.tables "
        f"WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME IN ({table_filter}) "
        "ORDER BY TABLE_SCHEMA,TABLE_NAME",
    )


def information_schema_statistics(
    endpoint: Endpoint, schema: str, tables: list[str]
) -> list[list[str]]:
    table_filter = ",".join(
        "'" + table.replace("'", "''") + "'" for table in tables
    ) or "''"
    return run_sql(
        endpoint,
        "SELECT TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,NON_UNIQUE,INDEX_SCHEMA,INDEX_NAME,"
        "SEQ_IN_INDEX,COLUMN_NAME,COLLATION,CARDINALITY,SUB_PART,PACKED,NULLABLE,INDEX_TYPE,"
        "COMMENT,INDEX_COMMENT,IS_VISIBLE,EXPRESSION FROM information_schema.statistics "
        f"WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME IN ({table_filter}) "
        "ORDER BY TABLE_SCHEMA,TABLE_NAME,INDEX_NAME,SEQ_IN_INDEX",
    )


def show_create(endpoint: Endpoint, schema: str, table: str) -> list[list[str]]:
    argv = [arg for arg in endpoint.argv if arg != "--raw"]
    return run_sql(
        Endpoint(name=endpoint.name, argv=argv),
        f"SHOW CREATE TABLE `{schema}`.`{table}`",
    )


def compare_rows(mysql_rows: list[list[str]], mydb_rows: list[list[str]]) -> dict[str, object]:
    if mysql_rows == mydb_rows:
        return {"equal": True}
    mysql_counter = collections.Counter(tuple(row) for row in mysql_rows)
    mydb_counter = collections.Counter(tuple(row) for row in mydb_rows)
    mysql_only = list((mysql_counter - mydb_counter).elements())
    mydb_only = list((mydb_counter - mysql_counter).elements())
    return {
        "equal": False,
        "mysql_count": len(mysql_rows),
        "mydb_count": len(mydb_rows),
        "mysql_only": [list(row) for row in mysql_only],
        "mydb_only": [list(row) for row in mydb_only],
    }


def compare_schema(mysql: Endpoint, mydb: Endpoint, schema: str) -> dict[str, object]:
    mysql_tables = table_names(mysql, schema)
    mydb_tables = table_names(mydb, schema)
    common = sorted(set(mysql_tables) & set(mydb_tables))
    extra = sorted(set(mydb_tables) - set(mysql_tables))
    unexpected_extra = extra
    if schema == "information_schema":
        unexpected_extra = [
            table
            for table in extra
            if table.upper() not in allowed_information_schema_extras()
        ]
    elif schema == "performance_schema":
        unexpected_extra = [
            table for table in extra if table.lower() not in ALLOWED_PERFORMANCE_SCHEMA_EXTRAS
        ]
    describe_diff = {}
    show_create_diff = {}
    for table in common:
        diff = compare_rows(describe(mysql, schema, table), describe(mydb, schema, table))
        if not diff["equal"]:
            describe_diff[table] = diff
        create_diff = compare_rows(
            show_create(mysql, schema, table), show_create(mydb, schema, table)
        )
        if not create_diff["equal"]:
            show_create_diff[table] = create_diff
    return {
        "missing_in_mydb": sorted(set(mysql_tables) - set(mydb_tables)),
        "extra_in_mydb": extra,
        "unexpected_extra_in_mydb": unexpected_extra,
        "describe_diff": describe_diff,
        "show_create_diff": show_create_diff,
        "information_schema_tables": compare_rows(
            information_schema_tables(mysql, schema, mysql_tables),
            information_schema_tables(mydb, schema, mysql_tables),
        ),
        "information_schema_columns": compare_rows(
            information_schema_columns(mysql, schema, mysql_tables),
            information_schema_columns(mydb, schema, mysql_tables),
        ),
        "information_schema_statistics": compare_rows(
            information_schema_statistics(mysql, schema, mysql_tables),
            information_schema_statistics(mydb, schema, mysql_tables),
        ),
    }


def parse_endpoint(name: str, value: str) -> Endpoint:
    argv = shlex.split(value)
    if not argv:
        raise SystemExit(f"--{name} must not be empty")
    return Endpoint(name=name, argv=argv)


def main(argv: Iterable[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mysql", required=True, help="mysql CLI command for reference MySQL")
    parser.add_argument("--mydb", required=True, help="mysql CLI command for mydb")
    parser.add_argument(
        "--schema",
        action="append",
        choices=SYSTEM_SCHEMAS,
        help="System schema to compare; defaults to all core system schemas",
    )
    parser.add_argument("--output", help="write JSON report to this path")
    args = parser.parse_args(list(argv))

    mysql = parse_endpoint("mysql", args.mysql)
    mydb = parse_endpoint("mydb", args.mydb)
    schemas = tuple(args.schema) if args.schema else SYSTEM_SCHEMAS
    result = {
        "schemas": {
            schema: compare_schema(mysql, mydb, schema)
            for schema in schemas
        }
    }
    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        with open(args.output, "w", encoding="utf-8") as output:
            output.write(rendered)
    else:
        sys.stdout.write(rendered)

    failed = any(
        report["missing_in_mydb"]
        or report["unexpected_extra_in_mydb"]
        or report["describe_diff"]
        or report["show_create_diff"]
        or not report["information_schema_tables"]["equal"]
        or not report["information_schema_columns"]["equal"]
        or not report["information_schema_statistics"]["equal"]
        for report in result["schemas"].values()
    )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
