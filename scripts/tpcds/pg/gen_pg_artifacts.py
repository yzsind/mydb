#!/usr/bin/env python3
"""Generate PostgreSQL-dialect TPC-DS artifacts from the checked-in mydb suite.

The mydb suite (scripts/tpcds/{schema.sql,indexes.sql,queries/qNN.sql}) is the
single source of truth for *which* queries and *which* substitution parameters
the reference set must match (dsqgen -rngseed 1 -scale 1). This script derives
the PostgreSQL equivalents by applying only the minimal dialect transforms so the
PG reference answers correspond to exactly the SQL mydb runs:

  schema   : drop `CREATE DATABASE`/`USE` (PG has no IF NOT EXISTS db / USE);
             column order and types (char/varchar/decimal/date/time) are
             PG-compatible and kept verbatim so the .dat column order matches.
  indexes  : `ALTER TABLE t ADD INDEX name (cols), ...` -> `CREATE INDEX name ON t (cols);`
  queries  : `interval N day`            -> `interval 'N day'`   (PG needs the quotes)
             `group by ... with rollup`  -> `group by rollup(...)` (reverse mydb's form)
             `|| ` concat and the rest are standard SQL PG accepts natively.

Outputs (under scripts/tpcds/pg/):
  schema_pg.sql, indexes_pg.sql, queries/qNN.sql
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

SUITE = Path(__file__).resolve().parent.parent          # scripts/tpcds
PG = SUITE / "pg"                                        # scripts/tpcds/pg

INTERVAL_RE = re.compile(
    r"\binterval\s+(\d+)\s+(day|days|month|months|year|years|week|weeks)\b",
    re.IGNORECASE,
)
# Match `group by <cols> with rollup`, possibly spanning multiple lines. The
# negative lookahead forbids another `group by` inside <cols>, so when a query
# has an inner (rollup-less) group by followed by the outer rollup group by,
# this binds `with rollup` to its own (nearest preceding) group by, not the inner.
ROLLUP_RE = re.compile(
    r"group\s+by\s+((?:(?!\bgroup\s+by\b).)*?)\s+with\s+rollup\b",
    re.IGNORECASE | re.DOTALL,
)


def gen_schema() -> None:
    lines = (SUITE / "schema.sql").read_text().splitlines()
    out = ["-- AUTO-GENERATED from scripts/tpcds/schema.sql by gen_pg_artifacts.py.",
           "-- PostgreSQL dialect: CREATE DATABASE / USE stripped (run inside the target db)."]
    for ln in lines:
        s = ln.strip().lower()
        if s.startswith("create database") or s.startswith("use "):
            continue
        if ln.startswith("-- TPC-DS schema for mydb"):
            continue
        out.append(ln)
    (PG / "schema_pg.sql").write_text("\n".join(out).strip() + "\n")
    print("wrote schema_pg.sql")


def gen_indexes() -> None:
    text = (SUITE / "indexes.sql").read_text()
    text = re.sub(r"--[^\n]*", "", text)   # strip line comments (they precede most ALTERs)
    out = ["-- AUTO-GENERATED from scripts/tpcds/indexes.sql by gen_pg_artifacts.py.",
           "-- mydb `ALTER TABLE ... ADD INDEX` -> PostgreSQL `CREATE INDEX`.", ""]
    n = 0
    for stmt in text.split(";"):
        m = re.search(r"alter\s+table\s+(\w+)", stmt, re.IGNORECASE)
        if not m:
            continue
        table = m.group(1)
        for iname, cols in re.findall(r"add\s+index\s+(\w+)\s*\(([^)]*)\)", stmt, re.IGNORECASE):
            cols = ", ".join(c.strip() for c in cols.split(","))
            out.append(f"CREATE INDEX {iname} ON {table} ({cols});")
            n += 1
    (PG / "indexes_pg.sql").write_text("\n".join(out) + "\n")
    print(f"wrote indexes_pg.sql ({n} indexes)")


# `<grouping(a)+grouping(b)...> as lochierarchy`. PG resolves a SELECT alias in
# ORDER BY only as a standalone sort key, not inside an expression like
# `case when lochierarchy = 0 ...`; MySQL/mydb resolves it anywhere. Expand the
# in-expression uses to the alias's definition (semantically identical row value).
LOCH_DEF_RE = re.compile(
    r"(grouping\([^)]*\)(?:\s*\+\s*grouping\([^)]*\))*)\s+as\s+lochierarchy",
    re.IGNORECASE,
)


def transform_query(text: str, qname: str) -> str:
    text = INTERVAL_RE.sub(lambda m: f"interval '{m.group(1)} {m.group(2)}'", text)
    want = len(re.findall(r"\bwith\s+rollup\b", text, re.IGNORECASE))
    text, got = ROLLUP_RE.subn(lambda m: f"group by rollup({m.group(1)})", text)
    if got != want:
        raise SystemExit(
            f"{qname}: rewrote {got} rollup clause(s) but found {want} `with rollup` "
            f"token(s); the group-by/rollup binding needs manual handling.")
    m = LOCH_DEF_RE.search(text)
    if m:
        # Replace `lochierarchy` only where it is compared (inside an expression);
        # the standalone `order by lochierarchy desc` stays as an alias sort key.
        text = re.sub(r"\blochierarchy\b(?=\s*=)", f"({m.group(1)})", text, flags=re.IGNORECASE)
    return text


def gen_queries() -> None:
    qsrc = SUITE / "queries"
    qdst = PG / "queries"
    qdst.mkdir(exist_ok=True)
    files = sorted(qsrc.glob("q*.sql"))
    for f in files:
        text = f.read_text()
        new = transform_query(text, f.name)
        # mydb header -> PG header.
        new = new.replace("(adapted for mydb / MySQL 8.0 from tpcds-kit)",
                          "(adapted for PostgreSQL from the mydb suite)")
        (qdst / f.name).write_text(new)
    print(f"wrote {len(files)} queries to pg/queries/")


if __name__ == "__main__":
    gen_schema()
    gen_indexes()
    gen_queries()
    print("done.")
