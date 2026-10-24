#!/usr/bin/env python3
"""Capture the Oracle MySQL 8.0.46 SHOW STATUS/PFS contract.

The committed fixture contains names, scope classification and PFS column
metadata only; volatile counter values are intentionally excluded.  Semantic
delta/reset behavior remains covered by executable differential scenarios.
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
DEFAULT_OUTPUT = pathlib.Path("docs/fixtures/mysql80-show-status-8.0.46.json")


def mysql_rows(argv: list[str], sql: str) -> list[list[str]]:
    proc = subprocess.run(
        argv + ["--batch", "--raw", "--skip-column-names", "-e", sql],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if proc.returncode != 0:
        raise SystemExit(proc.stderr.strip() or "mysql SHOW STATUS query failed")
    return [line.split("\t") for line in proc.stdout.splitlines() if line]


def scalar(argv: list[str], sql: str) -> str:
    rows = mysql_rows(argv, sql)
    if len(rows) != 1 or len(rows[0]) != 1:
        raise SystemExit(f"expected one scalar row for {sql!r}, got {rows!r}")
    return rows[0][0]


def status_names(argv: list[str], scope: str) -> list[str]:
    rows = mysql_rows(argv, f"SHOW {scope} STATUS")
    return [row[0] for row in rows]


def column_contract(argv: list[str]) -> list[dict[str, Any]]:
    rows = mysql_rows(
        argv,
        """
        SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_TYPE,
               IS_NULLABLE, COALESCE(CHARACTER_SET_NAME, 'NULL'),
               COALESCE(COLLATION_NAME, 'NULL')
        FROM information_schema.columns
        WHERE table_schema = 'performance_schema'
          AND table_name IN (
            'global_status', 'session_status', 'status_by_thread',
            'status_by_account', 'status_by_host', 'status_by_user'
          )
        ORDER BY TABLE_NAME, ORDINAL_POSITION
        """,
    )
    return [
        {
            "table": row[0],
            "name": row[1],
            "ordinal": int(row[2]),
            "column_type": row[3],
            "nullable": row[4],
            "charset": None if row[5] == "NULL" else row[5],
            "collation": None if row[6] == "NULL" else row[6],
        }
        for row in rows
    ]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mysql",
        required=True,
        help="mysql CLI command including connection/authentication options",
    )
    parser.add_argument("--output", type=pathlib.Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    mysql = shlex.split(args.mysql)
    if not mysql:
        raise SystemExit("--mysql must not be empty")

    build = scalar(mysql, "SELECT VERSION()")
    if not build.startswith(EXPECTED_VERSION):
        raise SystemExit(
            f"expected Oracle MySQL {EXPECTED_VERSION}, connected to {build!r}"
        )

    global_names = status_names(mysql, "GLOBAL")
    session_names = status_names(mysql, "SESSION")
    global_set = set(global_names)
    session_set = set(session_names)
    pfs_global = [
        row[0]
        for row in mysql_rows(
            mysql,
            "SELECT VARIABLE_NAME FROM performance_schema.global_status "
            "ORDER BY VARIABLE_NAME",
        )
    ]
    pfs_session = [
        row[0]
        for row in mysql_rows(
            mysql,
            "SELECT VARIABLE_NAME FROM performance_schema.session_status "
            "ORDER BY VARIABLE_NAME",
        )
    ]
    unexpected_com = {
        name
        for name in pfs_global + pfs_session
        if name.startswith("Com_") and name != "Com_stmt_reprepare"
    }
    if unexpected_com:
        raise SystemExit(
            "MySQL 8.0.46 PFS status tables unexpectedly contain command counters: "
            + ", ".join(sorted(unexpected_com))
        )

    payload = {
        "oracle_mysql_version": EXPECTED_VERSION,
        "oracle_mysql_build": build,
        "edition": "Community",
        "global_count": len(global_names),
        "session_count": len(session_names),
        "global_only": sorted(global_set - session_set),
        "session_only": sorted(session_set - global_set),
        "both": sorted(global_set & session_set),
        "global_names": global_names,
        "session_names": session_names,
        "pfs_global_names": pfs_global,
        "pfs_session_names": pfs_session,
        "pfs_columns": column_contract(mysql),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
