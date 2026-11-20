"""Negative controls for typed values, ordering and nonempty query selections."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from _tabular_results import canonicalize, normalize_cell, query_numbers, results_match


class TabularTests(unittest.TestCase):
    def setUp(self):
        Path('/tmp/mydb').mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir='/tmp/mydb')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def result(self, text, **options):
        path = self.root / 'rows.tsv'
        path.write_text(text, encoding='utf-8')
        return canonicalize(path, **options)

    def test_text_is_not_coerced_trimmed_or_case_folded(self):
        for left, right in [('001', '1'), (' x ', 'x'), ('null', 'NULL'),
                            ('1.00001', '1.00002')]:
            self.assertNotEqual(normalize_cell(left), normalize_cell(right))

    def test_empty_cells_and_rows_are_preserved(self):
        self.assertEqual(self.result('').rows, [])
        self.assertEqual(self.result('\n').rows, [('',)])
        self.assertEqual(self.result('\t\n').rows, [('', '')])

    def test_corrupt_utf8_and_inconsistent_width_fail(self):
        path = self.root / 'bad.tsv'
        path.write_bytes(b'\xff\n')
        with self.assertRaises(UnicodeError):
            canonicalize(path)
        with self.assertRaises(ValueError):
            self.result('a\tb\nc\n')

    def test_order_is_strict_by_default(self):
        self.assertNotEqual(self.result('1\ta\n2\tb\n').rows,
                            self.result('2\tb\n1\ta\n').rows)

    def test_multiple_tsv_result_sets_have_explicit_boundaries_and_widths(self):
        contract = {'result_sets': [
            {'row_count': 2, 'columns': ['text'], 'unordered': True},
            {'row_count': 0, 'columns': ['text', 'integer']},
            {'row_count': 1, 'columns': ['text', 'decimal']},
        ]}
        expected = self.result('b\na\nx\t1.00\n', contract=contract)
        self.assertEqual(expected.rows, [('a',), ('b',), ('x', '1')])
        self.assertEqual(expected, self.result('a\nb\nx\t1\n', contract=contract))
        for text in ['a\nb\nx\n', 'a\nb\n', 'a\nb\nx\t1\nextra\n']:
            with self.subTest(text=text), self.assertRaises(ValueError):
                self.result(text, contract=contract)
        # Ordering permissions apply within each set, never across boundaries.
        same_width = {'result_sets': [
            {'row_count': 1, 'columns': ['text'], 'unordered': True},
            {'row_count': 1, 'columns': ['text'], 'unordered': True},
        ]}
        self.assertNotEqual(self.result('a\nb\n', contract=same_width),
                            self.result('b\na\n', contract=same_width))

    def test_only_explicit_ties_can_be_reordered(self):
        contract = {'columns': ['text', 'decimal'], 'order_keys': [0]}
        left = self.result('a\t2.00000\na\t1\na\t1\nb\t3\n', contract=contract)
        right = self.result('a\t1.0\na\t2\na\t1\nb\t3.0\n', contract=contract)
        self.assertEqual(left, right)
        self.assertNotEqual(left.rows, self.result('b\t3\na\t1\na\t2\na\t1\n', contract=contract).rows)
        self.assertNotEqual(left.rows, self.result('a\t1\na\t2\nb\t3\n', contract=contract).rows)

    def test_explicit_numbers_are_exact_without_global_decimal_precision_loss(self):
        contract = {'columns': ['decimal']}
        self.assertEqual(self.result('123456789012345678901234567890.1000\n', contract=contract),
                         self.result('123456789012345678901234567890.1\n', contract=contract))
        self.assertNotEqual(self.result('1.00001\n', contract=contract),
                            self.result('1.00002\n', contract=contract))
        for value in ['NaN', 'Infinity', ' 1', 'abc']:
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.result(value + '\n', contract=contract)

    def test_float_tolerance_is_relative_and_preserves_other_values_and_row_order(self):
        contract = {'columns': ['text', 'decimal', 'integer', 'float'],
                    'float_relative_tolerance': 1e-14, 'null_marker': 'NULL'}
        expected = self.result('001\t26.00\t3\t39.645413178425954\n'
                               '002\t2.0\t4\t1e-20\n', contract=contract)
        first = '001\t26.0\t3\t39.6454131784259567945497\n'
        second = '002\t2.00\t4\t1.000000000000001e-20\n'
        actual = self.result(first + second, contract=contract)
        self.assertTrue(results_match(expected, actual, contract))
        self.assertNotEqual(expected.digest, actual.digest)

        for changed in (first.replace('001', '1') + second,
                        first.replace('26.0', '26.01') + second,
                        first.replace('\t3\t', '\t2\t') + second,
                        first + second.replace('1.000000000000001e-20', '1.01e-20'),
                        first + second.replace('1.000000000000001e-20', '0'),
                        first + second.replace('1.000000000000001e-20', 'NULL'),
                        second + first, first):
            with self.subTest(changed=changed):
                self.assertFalse(results_match(expected, self.result(changed, contract=contract), contract))

    def test_lossy_fixture_precision_and_text_trimming_require_explicit_options(self):
        contract = {'columns': ['text', 'decimal', 'integer'],
                    'trim_text': True, 'decimal_places': 4}
        self.assertEqual(self.result(' 001 \t-1.23445\t12\n', contract=contract),
                         self.result('001\t-1.2345\t12\n', contract=contract))
        self.assertNotEqual(self.result('001\t1.0001\t12\n', contract=contract),
                            self.result('001\t1.0002\t12\n', contract=contract))
        contract = {'columns': ['decimal'], 'decimal_places': 4}
        self.assertEqual(self.result('123456789012345678901234567890.99995\n', contract=contract).rows,
                         [('123456789012345678901234567891',)])
        self.assertEqual(self.result('-0.00001\n', contract=contract).rows, [('0',)])
        with self.assertRaises(ValueError):
            self.result('001\t1\t12.00001\n', contract={
                'columns': ['text', 'decimal', 'integer'], 'decimal_places': 4})

    def test_null_requires_an_explicit_marker_and_does_not_equal_text(self):
        contract = {'columns': ['text'], 'null_marker': '\\N'}
        self.assertNotEqual(self.result('\\N\n', contract=contract), self.result('NULL\n', contract=contract))

    def test_explicit_unordered_results_preserve_multiplicity(self):
        contract = {'unordered': True}
        self.assertEqual(self.result('b\na\na\n', contract=contract),
                         self.result('a\nb\na\n', contract=contract))
        self.assertNotEqual(self.result('a\na\n', contract=contract), self.result('a\n', contract=contract))

    def test_invalid_contracts_fail(self):
        for contract in [{'columns': ['unknown']}, {'order_keys': [5]},
                         {'columns': ['text', 'decimal']}, {'unordered': True, 'order_keys': [0]},
                         {'order_keys': [-1]}, {'unordered': 'yes'}, {'typo': True},
                         {'trim_text': 'yes'}, {'decimal_places': 4},
                         {'columns': ['decimal'], 'decimal_places': True},
                         {'columns': ['decimal'], 'decimal_places': -1},
                         {'columns': ['decimal'], 'decimal_places': 1.5},
                         {'format': 'typed-json', 'trim_text': True},
                         {'format': 'typed-json', 'decimal_places': 4},
                         {'columns': ['decimal'], 'float_relative_tolerance': 1e-14},
                         {'columns': ['float'], 'float_relative_tolerance': True},
                         {'columns': ['float'], 'float_relative_tolerance': 0},
                         {'columns': ['float'], 'float_relative_tolerance': float('nan')},
                         {'columns': ['float'], 'float_relative_tolerance': 1e-14, 'order_keys': [0]},
                         {'result_sets': []}, {'result_sets': 'invalid'},
                         {'result_sets': [None]},
                         {'result_sets': [{'columns': ['text']}]},
                         {'result_sets': [{'row_count': 1}]},
                         {'result_sets': [{'row_count': 0, 'columns': None}]},
                         {'result_sets': [{'row_count': -1, 'columns': ['text']}]},
                         {'result_sets': [{'row_count': True, 'columns': ['text']}]},
                         {'result_sets': [{'row_count': 1, 'format': 'typed-json'}]},
                         {'result_sets': [{'row_count': 0, 'result_sets': []}]},
                         {'result_sets': [{'row_count': 0, 'columns': ['text']}], 'unordered': True}]:
            with self.subTest(contract=contract), self.assertRaises(ValueError):
                self.result('1\n', contract=contract)

    def test_typed_capture_preserves_null_binary_decimal_and_column_metadata(self):
        path = self.root / 'typed.json'
        data = {'columns': ['value'], 'types': [253], 'rows': [[None], ['NULL'], ['001'],
                [{'bytes': 'AAkK/w=='}], [{'decimal': '123456789012345678901234567890.100'}]]}
        path.write_text(json.dumps(data))
        result = canonicalize(path, contract={'format': 'typed-json'})
        self.assertNotEqual(result.rows[0], result.rows[1])
        self.assertEqual(result.rows[-1][0], {'decimal': '123456789012345678901234567890.1'})
        data['types'] = [8]
        path.write_text(json.dumps(data))
        self.assertNotEqual(result, canonicalize(path, contract={'format': 'typed-json'}))
        data['rows'] = [[{'bytes': '#invalid'}]]
        path.write_text(json.dumps(data))
        with self.assertRaises(ValueError):
            canonicalize(path, contract={'format': 'typed-json'})

    def test_typed_capture_cli_checks_null_versus_literal_null(self):
        manifest = self.root / 'contract.json'
        manifest.write_text(json.dumps({'1': {'format': 'typed-json'}}))
        data = {'columns': ['value'], 'types': [253], 'rows': [[None]]}
        (self.root / 'q01.json').write_text(json.dumps(data))
        data['rows'] = [['NULL']]
        (self.root / 'q01_result.txt').write_text(json.dumps(data))
        command = [sys.executable, str(Path(__file__).with_name('tpcds_check_results.py')),
            '--expected-dir', str(self.root), '--actual-dir', str(self.root), '--queries', '1',
            '--output-csv', str(self.root / 'result.csv'), '--comparison-manifest', str(manifest)]
        result = subprocess.run(command, capture_output=True, text=True)
        self.assertEqual(result.returncode, 2, result.stderr)

    def test_invalid_query_selections_fail(self):
        for selection in ['', ' ', '22-1', '0', '-1', '1-2-3', '1,1', '2-3,3']:
            with self.subTest(selection=selection), self.assertRaises(ValueError):
                query_numbers(selection)
        self.assertEqual(query_numbers('1-3, 5'), [1, 2, 3, 5])

    def test_both_clis_reject_empty_reversed_and_out_of_range_selections(self):
        for script, limit in [('tpch_check_results.py', 26), ('tpcds_check_results.py', 99)]:
            for selection in ['', '22-1', str(limit + 1)]:
                result = subprocess.run([sys.executable, str(Path(__file__).with_name(script)),
                    '--expected-dir', str(self.root), '--actual-dir', str(self.root),
                    '--queries', selection, '--output-csv', str(self.root / 'check.csv')],
                    capture_output=True, text=True)
                self.assertNotEqual(result.returncode, 0, (script, selection))

    def test_cli_uses_per_query_contract(self):
        (self.root / 'q01.tsv').write_text('a\t2\na\t1\n')
        (self.root / 'q01_result.txt').write_text('a\t1.0\na\t2.0\n')
        manifest = self.root / 'contract.json'
        manifest.write_text(json.dumps({'1': {'columns': ['text', 'decimal'], 'order_keys': [0]}}))
        command = [sys.executable, str(Path(__file__).with_name('tpcds_check_results.py')),
            '--expected-dir', str(self.root), '--actual-dir', str(self.root), '--queries', '1',
            '--output-csv', str(self.root / 'check.csv')]
        self.assertNotEqual(subprocess.run(command, capture_output=True).returncode, 0)
        result = subprocess.run(command + ['--comparison-manifest', str(manifest)], capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == '__main__':
    unittest.main()
