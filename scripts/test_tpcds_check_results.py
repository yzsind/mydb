import sys
import csv
import json
import subprocess
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from tpcds_check_results import canonicalize
from _tabular_results import comparison_contracts


class CanonicalizeTest(unittest.TestCase):
    def setUp(self):
        Path('/tmp/mydb').mkdir(parents=True, exist_ok=True)

    def test_order_by_ties_compare_as_multisets_and_keep_duplicates(self):
        with tempfile.TemporaryDirectory(dir="/tmp/mydb") as directory:
            root = Path(directory)
            expected = root / "expected.tsv"
            actual = root / "actual.tsv"
            expected.write_text("a\t2.00000\na\t1\na\t1\n")
            actual.write_text("a\t1.0000\na\t1\na\t2\n")

            expected_result = canonicalize(expected, contract={"columns": ["text", "decimal"], "order_keys": [0]})
            actual_result = canonicalize(actual, contract={"columns": ["text", "decimal"], "order_keys": [0]})

            self.assertEqual(expected_result.rows, actual_result.rows)
            self.assertEqual(expected_result.digest, actual_result.digest)
            self.assertEqual(len(actual_result.rows), 3)

    def test_all_builtin_fixture_shapes_match_their_contracts(self):
        suite = Path(__file__).resolve().parent / 'tpcds'
        for oracle in ('expected-duckdb', 'expected'):
            for directory in sorted((suite / oracle).glob('sf-*')):
                manifest = directory / 'comparison.json'
                contracts = comparison_contracts(manifest if manifest.is_file() else None)
                for path in directory.glob('q*.tsv'):
                    with self.subTest(oracle=oracle, scale=directory.name, query=path.stem):
                        canonicalize(path, contract=contracts.get(str(int(path.stem[1:]))))


class ExecutionStatusTest(unittest.TestCase):
    def setUp(self):
        Path('/tmp/mydb').mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir='/tmp/mydb')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.expected = self.root / 'expected'
        self.expected.mkdir()

    def check(self, queries, *options):
        result = subprocess.run([
            sys.executable, str(Path(__file__).with_name('tpcds_check_results.py')),
            '--expected-dir', str(self.expected), '--actual-dir', str(self.root),
            '--queries', queries, '--output-csv', str(self.root / 'check.csv'), *options,
        ], capture_output=True, text=True)
        self.assertTrue((self.root / 'check.csv').is_file(), result.stderr)
        with (self.root / 'check.csv').open() as stream:
            return result, list(csv.DictReader(stream))

    def test_malformed_results_are_reported_and_later_queries_are_checked(self):
        for query in range(1, 6):
            (self.expected / f'q{query:02d}.tsv').write_text('a\tb\n')
            (self.root / f'q{query:02d}_result.txt').write_text('a\tb\n')
        (self.expected / 'q01.tsv').write_text('a\tb\nc\n')
        (self.root / 'q02_result.txt').write_text('a\tb\nc\n')
        (self.expected / 'q03.tsv').write_bytes(b'\xff\n')
        (self.root / 'q04_result.txt').write_bytes(b'\xff\n')
        result, rows = self.check('1-5')
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertNotIn('Traceback', result.stderr)
        self.assertEqual([row['status'] for row in rows], [
            'invalid_expected', 'invalid_actual', 'invalid_expected', 'invalid_actual', 'ok'])
        self.assertIn('inconsistent result width', rows[0]['message'])
        self.assertEqual(rows[1]['expected_rows'], '1')
        self.assertIn('Q5 ', result.stdout)

    def test_builtin_q14_contract_handles_both_result_sets_and_override_is_strict(self):
        fixtures = Path(__file__).parent / 'tpcds' / 'expected-duckdb' / 'sf-1'
        expected = fixtures / 'q14.tsv'
        contract = comparison_contracts(fixtures / 'comparison.json')['14']
        self.assertEqual(len(canonicalize(expected, contract=contract).rows), 200)
        (self.expected / 'q14.tsv').write_bytes(expected.read_bytes())
        (self.expected / 'comparison.json').write_text(json.dumps({'14': contract}))
        (self.root / 'q14_result.txt').write_bytes(expected.read_bytes())
        result, rows = self.check('14')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(rows[0]['actual_rows'], '200')
        (self.root / 'q14_result.txt').write_bytes(expected.read_bytes().replace(b'115019.61', b'115019.62', 1))
        result, rows = self.check('14')
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertEqual(rows[0]['status'], 'mismatch')
        (self.root / 'q14_result.txt').write_bytes(expected.read_bytes())
        override = self.root / 'override.json'
        override.write_text('{}')
        result, rows = self.check('14', '--comparison-manifest', str(override))
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertEqual(rows[0]['status'], 'invalid_expected')
        (self.root / 'q14_result.txt').write_bytes(b''.join(expected.read_bytes().splitlines(keepends=True)[:-1]))
        result, rows = self.check('14')
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertEqual(rows[0]['status'], 'invalid_actual')

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


RUNNER = Path(__file__).resolve().parent.parent / 'scripts-internal' / 'run_tpcds.sh'


@unittest.skipUnless(RUNNER.is_file(), 'optional private benchmark driver is absent')
class RunnerSummaryTest(unittest.TestCase):
    def test_checker_failures_never_report_zero_failures_or_reuse_stale_csv(self):
        Path('/tmp/mydb').mkdir(parents=True, exist_ok=True)
        source = RUNNER.read_text()
        start = source.index('  echo "[Phase 7] Checking query correctness..."')
        end = source.index('\n  echo "Total query time:', start)
        # Exercise the actual shell branch without starting a server or loading data.
        shell = '''
set -euo pipefail
RESULT_DIR="$1"; checker_status="$2"; report="$3"
EXPECTED_DIR="$RESULT_DIR"; CHECKER=unused; query_list="1 2 3"
TPCDS_CHECK_CORRECTNESS=1
check_ok_count=0; check_fail_count=0; check_skip_count=0
tpcds_check_mode_enabled() { return 0; }
python3() {
  if [[ -f "$report" ]]; then cp "$report" "$RESULT_DIR/correctness.csv"; fi
  return "$checker_status"
}
''' + source[start:end] + '''
printf 'COUNTS %s %s %s\\n' "$check_ok_count" "$check_fail_count" "$check_skip_count"
if [[ "$check_fail_count" -gt 0 ]]; then exit 2; fi
'''
        cases = [
            ('missing', 1, None, '0 3 0', 2),
            ('empty', 0, 'query_num,status\n', '0 3 0', 2),
            ('partial', 2, 'query_num,status\n1,ok\n', '0 3 0', 2),
            ('unexpected_exit', 1, 'query_num,status\n1,ok\n2,ok\n3,ok\n', '0 3 0', 2),
            ('mixed', 2, 'query_num,status\n1,ok\n2,invalid_expected\n3,ok\n', '2 1 0', 2),
            ('success', 0, 'query_num,status\n1,ok\n2,ok\n3,ok\n', '3 0 0', 0),
        ]
        with tempfile.TemporaryDirectory(dir='/tmp/mydb') as directory:
            root = Path(directory)
            for name, status, report, counts, returncode in cases:
                with self.subTest(case=name):
                    results = root / name
                    results.mkdir()
                    (results / 'correctness.csv').write_text('query_num,status\n1,ok\n2,ok\n3,ok\n')
                    report_path = root / f'{name}.csv'
                    if report is not None:
                        report_path.write_text(report)
                    result = subprocess.run(['bash', '-c', shell, 'runner-test', str(results),
                                             str(status), str(report_path)], capture_output=True, text=True)
                    self.assertEqual(result.returncode, returncode, result.stderr)
                    self.assertIn(f'COUNTS {counts}\n', result.stdout)


if __name__ == "__main__":
    unittest.main()
