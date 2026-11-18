"""Regression controls for historical TPC-H fixture comparison contracts."""
import csv
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from _tabular_results import canonicalize, comparison_contracts


ROOT = Path(__file__).resolve().parent


class TpchComparisonTests(unittest.TestCase):
    def setUp(self):
        Path('/tmp/mydb').mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir='/tmp/mydb')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.expected = self.root / 'expected'
        self.expected.mkdir()

    def check(self, expected, actual, contract=None, *, explicit=False):
        (self.expected / 'q01.tsv').write_text(expected)
        (self.root / 'q01_result.txt').write_text(actual)
        manifest = self.expected / 'comparison.json'
        if contract is not None:
            manifest.write_text(json.dumps({'1': contract}))
        command = [sys.executable, str(ROOT / 'tpch_check_results.py'),
                   '--expected-dir', str(self.expected), '--actual-dir', str(self.root),
                   '--queries', '1', '--output-csv', str(self.root / 'check.csv')]
        if explicit:
            override = self.root / 'override.json'
            override.write_text('{}')
            command += ['--comparison-manifest', str(override)]
        result = subprocess.run(command, capture_output=True, text=True)
        with (self.root / 'check.csv').open() as stream:
            rows = list(csv.DictReader(stream))
        return result, rows[0]

    def test_adjacent_contract_restores_declared_fixture_normalization(self):
        contract = {'columns': ['text', 'decimal', 'integer'],
                    'trim_text': True, 'decimal_places': 4}
        result, row = self.check('address\t25.522\t1478493\n',
                                 ' address \t25.522006\t1478493\n', contract)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(row['status'], 'ok')

    def test_no_contract_or_explicit_override_stays_strict(self):
        for contract, explicit in [(None, False),
                ({'columns': ['decimal'], 'decimal_places': 4}, True)]:
            with self.subTest(contract=contract):
                result, row = self.check('1\n', '1.0000\n', contract, explicit=explicit)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertEqual(row['status'], 'mismatch')

    def test_declared_precision_does_not_hide_changed_values_rows_or_order(self):
        contract = {'columns': ['text', 'decimal', 'integer'],
                    'trim_text': True, 'decimal_places': 4}
        expected = '001\t1.0001\t2\nb\t2\t3\n'
        for actual in ['001\t1.0002\t2\nb\t2\t3\n',
                       '1\t1.0001\t2\nb\t2\t3\n',
                       '001\t1.0001\t4\nb\t2\t3\n',
                       '001\t1.0001\t2\n', expected + expected,
                       'b\t2\t3\n001\t1.0001\t2\n']:
            with self.subTest(actual=actual):
                result, row = self.check(expected, actual, contract)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertEqual(row['status'], 'mismatch')

    def test_all_builtin_fixtures_have_explicit_valid_contracts(self):
        for directory in sorted((ROOT / 'tpch' / 'expected').glob('sf-*')):
            contracts = comparison_contracts(directory / 'comparison.json')
            for path in directory.glob('q*.tsv'):
                query = str(int(path.stem[1:]))
                with self.subTest(scale=directory.name, query=query):
                    self.assertIn(query, contracts)
                    canonicalize(path, contract=contracts[query])


if __name__ == '__main__':
    unittest.main()
