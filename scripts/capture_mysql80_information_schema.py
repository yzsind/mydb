#!/usr/bin/env python3
"""Capture the Oracle MySQL 8.0.46 Community information_schema contract.

The generated fixture is deterministic and contains only metadata.  It is
checked in so normal builds and tests never require a running MySQL server.
Regeneration intentionally refuses any server version other than 8.0.46.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import shlex
import subprocess
import sys
from typing import Any


EXPECTED_VERSION = "8.0.46"
DEFAULT_OUTPUT = pathlib.Path(
    "docs/fixtures/mysql80-information-schema-8.0.46.json"
)


def mysql_scalar(argv: list[str], sql: str) -> str:
    proc = subprocess.run(
        argv + ["--batch", "--raw", "--skip-column-names", "-e", sql],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if proc.returncode != 0:
        raise SystemExit(proc.stderr.strip() or "mysql metadata query failed")
    return proc.stdout.rstrip("\n")


def mysql_json(argv: list[str], sql: str) -> Any:
    value = mysql_scalar(argv, sql)
    if not value:
        return []
    return json.loads(value)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mysql",
        required=True,
        help="mysql CLI command, including connection/authentication options",
    )
    parser.add_argument("--output", type=pathlib.Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--base-fixture",
        type=pathlib.Path,
        help="preserve an existing fixture's deterministic row order and merge DESCRIBE defaults",
    )
    args = parser.parse_args()
    mysql = shlex.split(args.mysql)
    if not mysql:
        raise SystemExit("--mysql must not be empty")

    version = mysql_scalar(mysql, "SELECT VERSION()")
    if not version.startswith(EXPECTED_VERSION):
        raise SystemExit(
            f"expected Oracle MySQL {EXPECTED_VERSION}, connected to {version!r}"
        )

    tables = mysql_json(
        mysql,
        """
        SELECT JSON_ARRAYAGG(entry)
        FROM (
          SELECT JSON_OBJECT(
            'name', TABLE_NAME,
            'table_type', TABLE_TYPE
          ) AS entry
          FROM information_schema.tables
          WHERE table_schema = 'information_schema'
          ORDER BY TABLE_NAME
        ) AS ordered_tables
        """,
    )
    columns = mysql_json(
        mysql,
        """
        SELECT JSON_ARRAYAGG(entry)
        FROM (
          SELECT JSON_OBJECT(
            'table_name', TABLE_NAME,
            'column_name', COLUMN_NAME,
            'ordinal_position', ORDINAL_POSITION,
            'column_default', COLUMN_DEFAULT,
            'is_nullable', IS_NULLABLE,
            'data_type', DATA_TYPE,
            'character_maximum_length', CHARACTER_MAXIMUM_LENGTH,
            'character_octet_length', CHARACTER_OCTET_LENGTH,
            'numeric_precision', NUMERIC_PRECISION,
            'numeric_scale', NUMERIC_SCALE,
            'datetime_precision', DATETIME_PRECISION,
            'character_set_name', CHARACTER_SET_NAME,
            'collation_name', COLLATION_NAME,
            'column_type', COLUMN_TYPE,
            'column_key', COLUMN_KEY,
            'extra', EXTRA,
            'privileges', PRIVILEGES,
            'column_comment', COLUMN_COMMENT,
            'generation_expression', GENERATION_EXPRESSION,
            'srs_id', SRS_ID
          ) AS entry
          FROM information_schema.columns
          WHERE table_schema = 'information_schema'
          ORDER BY TABLE_NAME, ORDINAL_POSITION
        ) AS ordered_columns
        """,
    )
    describe_defaults: dict[tuple[str, str], str | None] = {}
    for table in tables:
        table_name = table["name"]
        described = mysql_scalar(
            mysql,
            f"SHOW COLUMNS FROM information_schema.`{table_name.replace('`', '``')}`",
        )
        for line in described.splitlines():
            fields = line.split("\t")
            if len(fields) != 6:
                raise SystemExit(f"unexpected DESCRIBE row for {table_name}: {line!r}")
            describe_defaults[(table_name, fields[0])] = (
                None if fields[4] == "NULL" else fields[4]
            )
    for column in columns:
        column["describe_default"] = describe_defaults[
            (column["table_name"], column["column_name"])
        ]
    keywords = mysql_json(
        mysql,
        """
        SELECT JSON_ARRAYAGG(entry)
        FROM (
          SELECT JSON_OBJECT('word', WORD, 'reserved', RESERVED) AS entry
          FROM information_schema.keywords
          ORDER BY WORD
        ) AS ordered_keywords
        """,
    )
    payload = {
        "oracle_mysql_version": EXPECTED_VERSION,
        "oracle_mysql_build": version,
        "edition": "Community",
        "schema": "information_schema",
        "table_count": len(tables),
        "column_count": len(columns),
        "tables": tables,
        "columns": columns,
        "keywords": keywords,
    }
    if args.base_fixture is not None:
        base = json.loads(args.base_fixture.read_text(encoding="utf-8"))
        if base.get("oracle_mysql_version") != EXPECTED_VERSION:
            raise SystemExit("base fixture is not the expected MySQL 8.0.46 contract")
        for column in base["columns"]:
            column["describe_default"] = describe_defaults[
                (column["table_name"], column["column_name"])
            ]
        base["oracle_mysql_build"] = version
        payload = base
    if payload["table_count"] != 79 or payload["column_count"] != 733:
        raise SystemExit(
            "unexpected information_schema inventory: "
            f"{payload['table_count']} tables, {payload['column_count']} columns"
        )
    if len(keywords) != 751:
        raise SystemExit(f"unexpected KEYWORDS inventory: {len(keywords)} rows")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
