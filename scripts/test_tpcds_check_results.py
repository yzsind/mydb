import sys
import csv
import subprocess
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from tpcds_check_results import canonicalize


class CanonicalizeTest(unittest.TestCase):
    def test_order_by_ties_compare_as_multisets_and_keep_duplicates(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            expected = root / "expected.tsv"
            actual = root / "actual.tsv"
            expected.write_text("a\t2.00000\na\t1\na\t1\n")
            actual.write_text("a\t1.0000\na\t1\na\t2\n")

            expected_result = canonicalize(expected)
            actual_result = canonicalize(actual)

            self.assertEqual(expected_result.rows, actual_result.rows)
            self.assertEqual(expected_result.digest, actual_result.digest)
            self.assertEqual(len(actual_result.rows), 3)


class ExecutionStatusTest(unittest.TestCase):
    def test_unsuccessful_execution_never_passes_matching_output(self):
        Path("/tmp/mydb").mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir="/tmp/mydb") as directory:
            root = Path(directory)
            expected = root / "expected"
            expected.mkdir()
            for q in range(1, 7):
                (expected / f"q{q:02d}.tsv").write_text("")
                (root / f"q{q:02d}_result.txt").write_text("")
            (root / "query_times.csv").write_text(
                "query_num,status,elapsed_ms\n1,ok,1\n2,fail,0\n3,crash,0\n4,skip,0\n6,fail,0\n"
            )
            # Partial output can even equal the expected nonempty result.
            (expected / "q06.tsv").write_text("1\n")
            (root / "q06_result.txt").write_text("1\n")
            result = subprocess.run([
                sys.executable, str(Path(__file__).with_name("tpcds_check_results.py")),
                "--expected-dir", str(expected), "--actual-dir", str(root),
                "--queries", "1-6", "--query-status-csv", str(root / "query_times.csv"),
                "--output-csv", str(root / "check.csv"),
            ], capture_output=True, text=True)
            self.assertEqual(result.returncode, 2)
            with (root / "check.csv").open() as stream:
                rows = list(csv.DictReader(stream))
            self.assertEqual([r["status"] for r in rows], ["ok"] + ["execution_failed"] * 5)
            self.assertIn("Q1 ", result.stdout)
            self.assertNotIn("PASS", result.stderr)


if __name__ == "__main__":
    unittest.main()
