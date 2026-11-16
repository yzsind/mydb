#!/usr/bin/env python3
"""Create parser seeds under /tmp/mydb; never mutate the audit observations."""
import argparse
import hashlib
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--output", type=Path, default=Path("/tmp/mydb/parser-fuzz"))
args = parser.parse_args()
root = Path(__file__).resolve().parents[2]
cases = json.loads((root / "tests/fuzz/parser-corpus-seeds.json").read_text())["cases"]
seeds = [(int(case.get("sql_mode_bits", 0)), case["sql"]) for case in cases]
seeds.append((0x284d, " " + "!" * 93 + "@("))
for sql in [
    "UPDATE t SET v=v+1 WHERE id=42", "UPDATE t SET v=?,n=n+? WHERE id=?",
    "SELECT()*2", "WITH()*2", "`a b`(1)+2", "db.`a b`(1)+2", "ROW()", "ROW(1)",
    "!a*2", "NOT a AND b", "(a+1)*2", "a BETWEEN 1 AND 3", "'a' 'b'",
    "0xABZZ", "0x", "SELECT 0xN'alias' FROM t", "SELECT 1+2 AS x ORDER BY x",
    "SELECT * FROM (SELECT * FROM t) d(a,b)",
    "SELECT * FROM t AS a JOIN t AS b ON a.id=b.id FOR UPDATE OF a FOR SHARE OF b NOWAIT",
    "INSERT INTO t VALUES(1) RETURNING WITH(OLD AS o) 1 AS __mydb_extension_0",
    "CREATE PROCEDURE p() BEGIN `a b`: LOOP LEAVE `a b`; END LOOP `a b`; END",
    "SELECT ?+? FROM t WHERE id=? LIMIT ?", "SELECT /*!800001+*/ 2",
]:
    for mode in [0, 1, 2, 4, 8, 16, 65535]:
        seeds.append((mode, sql))
for target in ["sql_parser", "sql_lex", "parser_roundtrip", "parser_fast_path_differential"]:
    corpus = args.output / "corpus" / target
    corpus.mkdir(parents=True, exist_ok=True)
    (args.output / "artifacts" / target).mkdir(parents=True, exist_ok=True)
    for mode, sql in seeds:
        payload = mode.to_bytes(2, "little") + sql.encode()
        (corpus / hashlib.sha256(payload).hexdigest()).write_bytes(payload)
print(f"Wrote {len(seeds)} seeds per target to {args.output}")
