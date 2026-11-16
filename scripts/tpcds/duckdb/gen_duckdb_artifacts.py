#!/usr/bin/env python3
"""Generate DuckDB-dialect TPC-DS artifacts from the checked-in mydb suite.

Mirror of scripts/tpcds/pg/gen_pg_artifacts.py, but for DuckDB. The mydb suite
(scripts/tpcds/{schema.sql,queries/qNN.sql}) is the single source of truth for
*which* queries and *which* substitution parameters the reference set must match
(dsqgen -rngseed 1 -scale 1). This derives the DuckDB equivalents with only the
minimal dialect transforms so the DuckDB reference answers correspond to exactly
the SQL mydb runs:

  schema   : drop `CREATE DATABASE`/`USE` (DuckDB has neither); column order and
             types (char/varchar/decimal/integer/date/time) are DuckDB-compatible
             and kept verbatim so the .dat column order matches. DuckDB treats
             CHAR(n) as VARCHAR (no padding) — harmless because the checker
             trims every cell before comparing.
  queries  : `group by ... with rollup`  -> `group by rollup(...)` (reverse mydb's
                                            MySQL-only form; DuckDB uses ROLLUP()).
             `lochierarchy` used *inside* an ORDER BY expression -> the alias's
                                            `grouping(a)+grouping(b)` definition
                                            (identical row value; avoids any
                                            alias-resolution divergence).
             `interval N day`             -> kept verbatim (DuckDB accepts it
                                            natively, unlike PG which needs quotes).
             `||` concat and the rest are standard SQL DuckDB accepts natively.

Outputs (under scripts/tpcds/duckdb/):
  schema_duckdb.sql, queries/qNN.sql
"""
from __future__ import annotations

import re
from pathlib import Path

SUITE = Path(__file__).resolve().parent.parent          # scripts/tpcds
DUCK = SUITE / "duckdb"                                  # scripts/tpcds/duckdb

# Match `group by <cols> with rollup`, possibly spanning multiple lines. The
# negative lookahead forbids another `group by` inside <cols>, so when a query
# has an inner (rollup-less) group by followed by the outer rollup group by,
# this binds `with rollup` to its own (nearest preceding) group by, not the inner.
ROLLUP_RE = re.compile(
    r"group\s+by\s+((?:(?!\bgroup\s+by\b).)*?)\s+with\s+rollup\b",
    re.IGNORECASE | re.DOTALL,
)

# `<grouping(a)+grouping(b)...> as lochierarchy`. Used as an ORDER BY sort key
# (fine as an alias) but also *inside* `case when lochierarchy = N ...`. Expand
# the in-expression uses to the alias's definition (identical row value).
LOCH_DEF_RE = re.compile(
    r"(grouping\([^)]*\)(?:\s*\+\s*grouping\([^)]*\))*)\s+as\s+lochierarchy",
    re.IGNORECASE,
)

# TPC-DS uses `returns` (q05/q77/…) and `at` (q90) as plain identifiers. DuckDB
# reserves both, so an *implicit* alias (`expr returns`, `(subquery) at`) is a
# parse error while `as returns` / a column reference happen to parse. Quoting
# every standalone occurrence as `"returns"`/`"at"` is uniformly valid (DuckDB
# matches quoted identifiers case-insensitively) and leaves `store_returns`,
# `catalog_returns`, etc. untouched (word boundary). MySQL/PG don't reserve them,
# so this is a pure DuckDB-dialect fix with no semantic effect.
RESERVED_IDENT_RE = re.compile(r"\b(returns|at)\b", re.IGNORECASE)
STRING_LIT_RE = re.compile(r"('(?:[^']|'')*')")


def quote_reserved_idents(text: str) -> str:
    # Split out single-quoted string literals (odd indices) so we never rewrite
    # inside a literal; only transform the SQL between them (even indices).
    parts = STRING_LIT_RE.split(text)
    for i in range(0, len(parts), 2):
        parts[i] = RESERVED_IDENT_RE.sub(lambda m: f'"{m.group(0).lower()}"', parts[i])
    return "".join(parts)


def gen_schema() -> None:
    lines = (SUITE / "schema.sql").read_text().splitlines()
    out = ["-- AUTO-GENERATED from scripts/tpcds/schema.sql by gen_duckdb_artifacts.py.",
           "-- DuckDB dialect: CREATE DATABASE / USE stripped (run inside the target db)."]
    for ln in lines:
        s = ln.strip().lower()
        if s.startswith("create database") or s.startswith("use "):
            continue
        if ln.startswith("-- TPC-DS schema for mydb"):
            continue
        out.append(ln)
    (DUCK / "schema_duckdb.sql").write_text("\n".join(out).strip() + "\n")
    print("wrote schema_duckdb.sql")


def transform_query(text: str, qname: str) -> str:
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
    text = quote_reserved_idents(text)
    return text


def gen_queries() -> None:
    qsrc = SUITE / "queries"
    qdst = DUCK / "queries"
    qdst.mkdir(exist_ok=True)
    files = sorted(qsrc.glob("q*.sql"))
    for f in files:
        text = f.read_text()
        new = transform_query(text, f.name)
        new = new.replace("(adapted for mydb / MySQL 8.0 from tpcds-kit)",
                          "(adapted for DuckDB from the mydb suite)")
        (qdst / f.name).write_text(new)
    print(f"wrote {len(files)} queries to duckdb/queries/")


if __name__ == "__main__":
    gen_schema()
    gen_queries()
    print("done.")
