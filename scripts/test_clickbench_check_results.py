import argparse
import contextlib
import csv
import io
import json
import os
from pathlib import Path
import shutil
import re
import subprocess
import tempfile
import unittest

from clickbench_check_results import (DuckDB, cell_equal, compare_rows, context,
                                     digest, duckdb_sql, mysql_rows, read_expected,
                                     run)


class ComparisonTests(unittest.TestCase):
    def entry(self, rows, types=None, sql="SELECT x FROM hits;"):
        return dict(rows=rows, types=types or ["VARCHAR"], sql=sql)

    def test_numeric_types_not_numeric_looking_strings(self):
        self.assertTrue(cell_equal("1513.4879349030107", "1513.4879", "DOUBLE"))
        self.assertFalse(cell_equal("1.0", "1.0002", "DOUBLE"))
        self.assertFalse(cell_equal("9007199254740993", "9007199254740992", "BIGINT"))
        self.assertFalse(cell_equal("001", "1", "VARCHAR"))
        self.assertFalse(cell_equal(" a ", "a", "VARCHAR"))
        self.assertTrue(cell_equal("12.3400", "12.34", "DECIMAL(10,4)"))
        self.assertFalse(cell_equal("nan", "nan", "DOUBLE"))
        self.assertFalse(cell_equal(None, "", "VARCHAR"))

    def test_escaped_cells_and_empty_rows(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "actual.tsv"
            path.write_text("a\\tb\\nc\\0d\\\\e\tNULL\t\n\ttext\t\\\\N\n")
            self.assertEqual(mysql_rows(path), [["a\tb\nc\0d\\e", None, ""], ["", "text", "\\N"]])
            path.write_text("\n")
            self.assertEqual(mysql_rows(path), [[""]])
            path.write_text("")
            self.assertEqual(mysql_rows(path), [])
            path.write_text("truncated")
            with self.assertRaisesRegex(ValueError, "incomplete"):
                mysql_rows(path)

    def test_unordered_multiset_keeps_duplicates(self):
        entry = self.entry([["a"], ["a"], ["b"]])
        self.assertTrue(compare_rows(entry, [["b"], ["a"], ["a"]])[0])
        self.assertFalse(compare_rows(entry, [["b"], ["a"], ["b"]])[0])
        self.assertFalse(compare_rows(entry, [["a"]])[0])
        self.assertFalse(compare_rows(entry, [["a", "x"]] * 3)[0])

    def test_tie_permutations_but_not_rank_changes(self):
        entry = self.entry([["a", "5"], ["b", "5"], ["c", "4"]], ["VARCHAR", "BIGINT"],
                           "SELECT x, COUNT(*) AS c FROM hits GROUP BY x ORDER BY c DESC LIMIT 3;")
        self.assertTrue(compare_rows(entry, [["b", "5"], ["a", "5"], ["c", "4"]])[0])
        self.assertFalse(compare_rows(entry, [["c", "4"], ["a", "5"], ["b", "5"]])[0])
        self.assertFalse(compare_rows(entry, [["a", "5"], ["d", "5"], ["c", "4"]])[0])

    def test_nontransitive_numeric_tolerance_uses_full_matching(self):
        entry = self.entry([["0.00000"], ["0.00008"]], ["DOUBLE"])
        self.assertTrue(compare_rows(entry, [["0.00004"], ["-0.00004"]])[0])

    def test_hidden_order_key_is_not_discarded(self):
        entry = self.entry([["a"], ["b"]], sql="SELECT x FROM hits ORDER BY EventTime LIMIT 2;")
        self.assertFalse(compare_rows(entry, [["b"], ["a"]])[0])
        self.assertTrue(compare_rows(entry, [["a"], ["b"]])[0])

    def test_null_literal_is_not_silently_accepted(self):
        self.assertFalse(compare_rows(self.entry([["NULL"]]), [[None]])[0])
        self.assertTrue(compare_rows(self.entry([[None]]), [[None]])[0])
        self.assertTrue(compare_rows(self.entry([]), [])[0])

    def test_mysql_hour_and_byte_length_adapter(self):
        sql = "SELECT DATE_FORMAT(EventTime, '%Y-%m-%d %H:00:00'), AVG(length(URL)) FROM hits;"
        self.assertEqual(duckdb_sql(sql),
                         "SELECT strftime(EventTime, '%Y-%m-%d %H:00:00'), AVG(strlen(URL)) FROM hits;")

    def test_regex_adapter_preserves_mysql_literal_semantics(self):
        sql = r"SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') FROM hits;"
        self.assertEqual(duckdb_sql(sql),
                         "SELECT REGEXP_REPLACE(Referer, '^https?://(?:www.)?([^/]+)/.*$', '1', 'g') FROM hits;")
        self.assertEqual(duckdb_sql(r"SELECT 'a\\b', 'a\nb', 'it''s', '\%';"),
                         "SELECT 'a\\b', 'a\nb', 'it''s', '\\%';")


class CacheTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.queries = ["SELECT COUNT(*) FROM hits;"] * 43
        self.args = argparse.Namespace(mode="check", expected_dir=self.root / "expected",
            actual_dir=self.root / "actual", queries_file=self.root / "queries.sql",
            schema_file=self.root / "create.sql", dataset_id="test-data-v1", row_count=3,
            queries="1", duckdb_database=None, duckdb_bin="duckdb", timeout=10,
            runs=2, allow_missing_expected=False, output_csv=self.root / "correctness.csv")
        self.args.schema_file.write_text("CREATE TABLE hits (x BIGINT);")
        self.args.queries_file.write_text("\n".join(self.queries))
        self.args.expected_dir.mkdir()
        self.args.actual_dir.mkdir()
        (self.args.actual_dir / "query-01.sql").write_text(self.queries[0] + "\n")
        self.timings(["ok", "ok"])
        for n in [1, 2]:
            (self.args.actual_dir / f"query-01-run-{n}.tsv").write_text("3\n")
        self.entry = dict(context=context(self.args, self.queries), sql=self.queries[0],
                          duckdb_sql=self.queries[0], duckdb_version="test", types=["BIGINT"], rows=[["3"]])
        self.path = self.args.expected_dir / "q01.json"

    def timings(self, statuses):
        (self.args.actual_dir / "query_times.csv").write_text(
            "query,run,elapsed_seconds,status\n" + "".join(
                f"1,{n},0.1,{status}\n" for n, status in enumerate(statuses, 1)))

    def write_cache(self):
        self.path.write_text(json.dumps(dict(self.entry, sha256=digest(self.entry))))

    def check(self):
        with contextlib.redirect_stdout(io.StringIO()):
            status = run(self.args)
        with self.args.output_csv.open() as src:
            return status, [r["status"] for r in csv.DictReader(src)]

    def test_all_runs_checked_and_no_oracle_needed_on_hit(self):
        self.write_cache()
        self.args.duckdb_database = self.root / "does-not-exist.duckdb"
        self.args.duckdb_bin = "/no/duckdb"
        self.assertEqual(self.check(), (0, ["ok", "ok"]))
        (self.args.actual_dir / "query-01-run-2.tsv").write_text("4\n")
        self.assertEqual(self.check(), (1, ["ok", "mismatch"]))

    def test_missing_cache_auto_vs_required(self):
        self.assertEqual(self.check(), (1, ["missing_expected"] * 2))
        self.args.allow_missing_expected = True
        self.assertEqual(self.check(), (0, ["skipped_missing_expected"] * 2))
        (self.args.actual_dir / "query-01-run-2.tsv").unlink()
        self.assertEqual(self.check(), (1, ["skipped_missing_expected", "invalid_actual"]))

    def test_corrupt_or_stale_cache_is_never_auto_pass(self):
        self.args.allow_missing_expected = True
        for mutate in [lambda: self.entry["context"].update(dataset_id="other"),
                       lambda: self.entry["context"].update(row_count=4),
                       lambda: self.entry["context"].update(schema_sha256="other"),
                       lambda: self.entry["context"].update(workload_sha256="other")]:
            with self.subTest(mutate=mutate):
                mutate()
                self.write_cache()
                self.assertEqual(self.check(), (1, ["invalid_expected"] * 2))
        self.path.write_text('{"sha256":"broken"}')
        self.assertEqual(self.check(), (1, ["invalid_expected"] * 2))
        self.path.write_text("not json")
        self.assertEqual(self.check(), (1, ["invalid_expected"] * 2))

    def test_failed_and_missing_runs_cannot_pass_with_matching_output(self):
        self.write_cache()
        self.timings(["timeout"])
        self.args.duckdb_database = self.root / "absent.duckdb"
        self.assertEqual(self.check(), (1, ["query_failed"] * 2))

    def test_missing_actual_and_wrong_sql(self):
        self.write_cache()
        (self.args.actual_dir / "query-01-run-2.tsv").unlink()
        self.assertEqual(self.check(), (1, ["ok", "invalid_actual"]))
        (self.args.actual_dir / "query-01.sql").write_text("SELECT 3;")
        self.assertEqual(self.check(), (1, ["invalid_actual"] * 2))

    def test_oracle_failure_is_not_missing_cache_skip(self):
        self.args.allow_missing_expected = True
        self.args.duckdb_database = self.root / "absent.duckdb"
        status, results = self.check()
        self.assertEqual(status, 1)
        self.assertEqual(results, ["invalid_expected"] * 2)
        self.assertFalse(self.path.exists())

    @unittest.skipUnless(shutil.which("duckdb"), "DuckDB CLI not installed")
    def test_real_duckdb_capture_and_cache_reuse(self):
        self.args.duckdb_database = self.root / "test.duckdb"
        subprocess.run(["duckdb", str(self.args.duckdb_database), "-c",
                        "CREATE TABLE hits AS SELECT * FROM range(3);"], check=True, capture_output=True)
        self.assertEqual(self.check(), (0, ["ok", "ok"]))
        cached = self.path.read_bytes()
        self.args.duckdb_bin = "/no/duckdb"
        self.assertEqual(self.check(), (0, ["ok", "ok"]))
        self.assertEqual(cached, self.path.read_bytes())

    @unittest.skipUnless(shutil.which("duckdb"), "DuckDB CLI not installed")
    def test_real_duckdb_preserves_special_strings_and_large_numbers(self):
        self.args.duckdb_database = self.root / "special.duckdb"
        subprocess.run(["duckdb", str(self.args.duckdb_database), "-c",
                        "CREATE TABLE hits AS SELECT * FROM range(3);"], check=True, capture_output=True)
        sql = "SELECT 'a' || chr(9) || chr(10) || chr(13) || 'Я' AS s, NULL AS n, 9223372036854775807::BIGINT AS i, '' AS e;"
        oracle = DuckDB(self.args)
        result = oracle.capture(self.path, context(self.args, self.queries), sql)
        self.assertEqual(result["rows"], [["a\t\n\rЯ", None, "9223372036854775807", ""]])
        self.assertEqual(result, read_expected(self.path, context(self.args, self.queries), sql))
        self.args.row_count = 4
        with self.assertRaisesRegex(ValueError, "has 3 rows"):
            DuckDB(self.args)

    @unittest.skipUnless(shutil.which("duckdb"), "DuckDB CLI not installed")
    def test_live_limit_checks_values_ranks_hidden_keys_and_duplicates(self):
        self.args.duckdb_database = self.root / 'ties.duckdb'
        subprocess.run(['duckdb', str(self.args.duckdb_database), '-c',
            "CREATE TABLE hits AS SELECT * FROM (VALUES ('a',1),('a',1),('b',1),('b',1),('c',2),('d',3)) t(x,n)"],
            check=True, capture_output=True)
        self.args.row_count = 6
        oracle = DuckDB(self.args)
        cases = [
            ('SELECT x, COUNT(*) AS c FROM hits GROUP BY x ORDER BY c DESC LIMIT 1',
             ['VARCHAR','BIGINT'], [['a','2']], [['b','2']], [['c','1']]),
            ('SELECT x, COUNT(*) AS c FROM hits GROUP BY x LIMIT 1',
             ['VARCHAR','BIGINT'], [['a','2']], [['c','1']], [['c','2']]),
            ('SELECT x FROM hits ORDER BY n LIMIT 2',
             ['VARCHAR'], [['a'],['a']], [['b'],['a']], [['c'],['a']]),
            ('SELECT x FROM hits ORDER BY n LIMIT 2 OFFSET 3',
             ['VARCHAR'], [['a'],['c']], [['b'],['c']], [['b'],['b']]),
            ('SELECT x FROM hits LIMIT 3',
             ['VARCHAR'], [['a'],['a'],['b']], [['a'],['b'],['c']], [['a'],['a'],['a']]),
            ('SELECT * FROM hits ORDER BY n DESC LIMIT 2',
             ['VARCHAR','INTEGER'], [['d','3'],['c','2']], [['d','3'],['c','2']], [['c','2'],['d','3']]),
            ('SELECT x, AVG(n) AS m FROM hits GROUP BY x ORDER BY 2 LIMIT 1',
             ['VARCHAR','DOUBLE'], [['a','1']], [['b','1.0000']], [['b','1.1']]),
        ]
        for sql, types, expected, good, bad in cases:
            with self.subTest(sql=sql):
                entry = dict(sql=sql, types=types, rows=expected)
                self.assertTrue(oracle.verify_limit(entry, good)[0])
                self.assertFalse(oracle.verify_limit(entry, bad)[0])
                fabricated = [list(row) for row in good]
                fabricated[0][0] = 'absent'
                self.assertFalse(oracle.verify_limit(entry, fabricated)[0])

    @unittest.skipUnless(shutil.which("duckdb"), "DuckDB CLI not installed")
    def test_all_43_workload_queries_on_small_duckdb(self):
        script_dir = Path(__file__).parent
        schema = (script_dir / "clickbench/create.sql").read_text()
        columns = re.findall(r"^\s+(\w+) (BIGINT|SMALLINT|INTEGER|TEXT|TIMESTAMP|Date|VARCHAR|CHAR)\b", schema, re.M)
        self.assertEqual(len(columns), 105)
        values = []
        for name, type_name in columns:
            value = {"TEXT": "'google example'", "VARCHAR": "'google example'", "CHAR": "'a'",
                     "TIMESTAMP": "'2013-07-14 12:30:00'",
                     "Date": "'2013-07-14'"}.get(type_name, "0")
            values.append(f"{value}::{type_name} AS {name}")
        self.args.duckdb_database = self.root / "full-schema.duckdb"
        subprocess.run(["duckdb", str(self.args.duckdb_database), "-c",
                        "CREATE TABLE hits AS SELECT " + ",".join(values) + " FROM range(3);"],
                       check=True, capture_output=True)
        self.args.mode = "generate"
        self.args.schema_file = script_dir / "clickbench/create.sql"
        self.args.queries_file = script_dir / "clickbench/queries"
        self.args.queries = ",".join(map(str, range(1, 44)))
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(run(self.args), 0)
        self.assertEqual(len(list(self.args.expected_dir.glob("q*.json"))), 43)
        q1 = json.loads((self.args.expected_dir / "q01.json").read_text())
        self.assertEqual(q1["rows"], [["3"]])

if __name__ == "__main__":
    unittest.main()
