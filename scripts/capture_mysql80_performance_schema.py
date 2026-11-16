#!/usr/bin/env python3
"""Capture the fixed Oracle MySQL 8.0.46 Performance Schema contract.

The output is a checked-in build fixture.  The command is read-only apart
from the optional ACL probe, which is deliberately not enabled here; DML
behaviour has its own differential test so a metadata refresh cannot alter
the reference server.
"""

from __future__ import annotations

import argparse
import json
import shlex
import subprocess
from typing import Iterable


def run(argv: list[str], sql: str) -> list[list[str | None]]:
    proc = subprocess.run(
        argv + ["-e", sql],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"reference query failed: {sql}\n{proc.stderr.strip()}")
    rows: list[list[str | None]] = []
    for line in proc.stdout.splitlines():
        rows.append([None if value == "NULL" else value for value in line.split("\t")])
    return rows


def run_escaped(argv: list[str], sql: str) -> list[list[str | None]]:
    """Run with mysql batch escaping so multiline fields remain one row."""
    escaped_argv = [arg for arg in argv if arg != "--raw"]
    return run(escaped_argv, sql)


def mysql_unescape(value: str) -> str:
    return (
        value.replace("\\\\", "\0")
        .replace("\\n", "\n")
        .replace("\\t", "\t")
        .replace("\\r", "\r")
        .replace("\0", "\\")
    )


def records(columns: list[str], rows: list[list[str | None]]) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    for row in rows:
        if len(row) != len(columns):
            raise RuntimeError(f"column count mismatch: expected {len(columns)}, got {row!r}")
        result.append(dict(zip(columns, row, strict=True)))
    return result


def main(argv: Iterable[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mysql",
        required=True,
        help="mysql CLI command including connection flags",
    )
    parser.add_argument("--output", required=True)
    args = parser.parse_args(list(argv))
    mysql = shlex.split(args.mysql) + ["--batch", "--raw", "--skip-column-names"]

    version = run(mysql, "SELECT VERSION()")[0][0]
    if version != "8.0.46":
        raise RuntimeError(f"expected Oracle MySQL 8.0.46, got {version!r}")

    table_columns = [
        "table_name",
        "table_type",
        "engine",
        "version",
        "row_format",
        "table_rows",
        "avg_row_length",
        "data_length",
        "max_data_length",
        "index_length",
        "data_free",
        "auto_increment",
        "table_collation",
        "checksum",
        "create_options",
        "table_comment",
    ]
    tables = records(
        table_columns,
        run(
            mysql,
            "SELECT TABLE_NAME,TABLE_TYPE,ENGINE,VERSION,ROW_FORMAT,TABLE_ROWS,"
            "AVG_ROW_LENGTH,DATA_LENGTH,MAX_DATA_LENGTH,INDEX_LENGTH,DATA_FREE,"
            "AUTO_INCREMENT,TABLE_COLLATION,CHECKSUM,CREATE_OPTIONS,TABLE_COMMENT "
            "FROM information_schema.tables WHERE table_schema='performance_schema' "
            "ORDER BY TABLE_NAME",
        ),
    )

    column_columns = [
        "table_name",
        "ordinal_position",
        "column_name",
        "column_default",
        "is_nullable",
        "data_type",
        "character_maximum_length",
        "character_octet_length",
        "numeric_precision",
        "numeric_scale",
        "datetime_precision",
        "character_set_name",
        "collation_name",
        "column_type",
        "column_key",
        "extra",
        "privileges",
        "column_comment",
        "generation_expression",
        "srs_id",
    ]
    columns = records(
        column_columns,
        run(
            mysql,
            "SELECT TABLE_NAME,ORDINAL_POSITION,COLUMN_NAME,COLUMN_DEFAULT,IS_NULLABLE,"
            "DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION,"
            "NUMERIC_SCALE,DATETIME_PRECISION,CHARACTER_SET_NAME,COLLATION_NAME,COLUMN_TYPE,"
            "COLUMN_KEY,EXTRA,PRIVILEGES,COLUMN_COMMENT,GENERATION_EXPRESSION,SRS_ID "
            "FROM information_schema.columns WHERE table_schema='performance_schema' "
            "ORDER BY TABLE_NAME,ORDINAL_POSITION",
        ),
    )

    index_columns = [
        "table_name",
        "index_name",
        "non_unique",
        "seq_in_index",
        "column_name",
        "collation",
        "cardinality",
        "sub_part",
        "packed",
        "nullable",
        "index_type",
        "comment",
        "index_comment",
        "is_visible",
        "expression",
    ]
    indexes = records(
        index_columns,
        run(
            mysql,
            "SELECT TABLE_NAME,INDEX_NAME,NON_UNIQUE,SEQ_IN_INDEX,COLUMN_NAME,COLLATION,"
            "CARDINALITY,SUB_PART,PACKED,NULLABLE,INDEX_TYPE,COMMENT,INDEX_COMMENT,"
            "IS_VISIBLE,EXPRESSION FROM information_schema.statistics "
            "WHERE table_schema='performance_schema' ORDER BY TABLE_NAME,INDEX_NAME,SEQ_IN_INDEX",
        ),
    )

    create_sql: dict[str, str] = {}
    for table in tables:
        name = str(table["table_name"])
        create_rows = run_escaped(mysql, f"SHOW CREATE TABLE performance_schema.`{name}`")
        create_sql[name] = mysql_unescape(str(create_rows[0][1]))

    setup: dict[str, list[dict[str, object]]] = {}
    for table in [
        "setup_actors",
        "setup_consumers",
        "setup_instruments",
        "setup_objects",
        "setup_threads",
    ]:
        names = [
            str(row[0])
            for row in run(
                mysql,
                "SELECT COLUMN_NAME FROM information_schema.columns "
                f"WHERE table_schema='performance_schema' AND table_name='{table}' "
                "ORDER BY ORDINAL_POSITION",
            )
        ]
        setup[table] = records(names, run(mysql, f"SELECT * FROM performance_schema.`{table}`"))

    sysvars = records(
        ["name", "value"],
        run(
            mysql,
            "SELECT VARIABLE_NAME,VARIABLE_VALUE FROM performance_schema.global_variables "
            "WHERE VARIABLE_NAME LIKE 'performance_schema%' ORDER BY VARIABLE_NAME",
        ),
    )
    error_catalog = records(
        ["error_number", "error_name", "sql_state"],
        run(
            mysql,
            "SELECT ERROR_NUMBER,ERROR_NAME,SQL_STATE FROM "
            "performance_schema.events_errors_summary_global_by_error "
            "ORDER BY ERROR_NUMBER",
        ),
    )

    payload = {
        "schema": "performance_schema",
        "edition": "Community",
        "oracle_mysql_version": version,
        "table_count": len(tables),
        "column_count": len(columns),
        "index_part_count": len(indexes),
        "tables": tables,
        "columns": columns,
        "indexes": indexes,
        "create_sql": create_sql,
        "setup_defaults": setup,
        "system_variables": sysvars,
        "error_catalog": error_catalog,
    }
    with open(args.output, "w", encoding="utf-8") as output:
        json.dump(payload, output, indent=2, ensure_ascii=False, sort_keys=True)
        output.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(__import__("sys").argv[1:]))
