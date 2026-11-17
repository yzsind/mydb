"""Oracle comparisons preserve integer magnitude, precision and string semantics."""
import unittest

from mysql80_function_diff import outcomes_equal, semantic_cell_equal


def outcome(value, kind):
    return {'status': 'ok', 'rows': [[value]], 'warning_codes': [], 'columns': [{'type': kind}]}


class SemanticComparisonTests(unittest.TestCase):
    def test_trailing_integer_zero_is_not_a_fractional_zero(self):
        self.assertFalse(semantic_cell_equal('ABS', '10', '1.0'))
        self.assertFalse(outcomes_equal('ABS', {'metadata_comparison': 'relaxed'}, outcome('10', 'LONGLONG'), outcome('1.0', 'NEWDECIMAL')))

    def test_decimal_spelling_is_normalized_only_for_numeric_columns(self):
        case = {'metadata_comparison': 'relaxed'}
        self.assertTrue(outcomes_equal('ABS', case, outcome('10', 'LONGLONG'), outcome('10.000', 'NEWDECIMAL')))
        self.assertTrue(outcomes_equal('ABS', case, outcome('1e2', 'DOUBLE'), outcome('100.0', 'DOUBLE')))
        for left, right in [('10', '1.0'), ('100', '10.0'), ('1.00001', '1.00002'),
                            ('123456789012345678901234567890.1', '123456789012345678901234567890.2')]:
            self.assertFalse(outcomes_equal('ABS', case, outcome(left, 'NEWDECIMAL'), outcome(right, 'NEWDECIMAL')))

    def test_numeric_looking_and_json_looking_strings_remain_exact(self):
        for left, right in [('1', '1.0'), ('1.00', '1.0'), ('true', '1'),
                            ('2026-01-01', '2026-01-01 00:00:00'), ('a0', 'a.0')]:
            self.assertFalse(outcomes_equal('CONCAT', {}, outcome(left, 'VAR_STRING'), outcome(right, 'VAR_STRING')))
        self.assertFalse(semantic_cell_equal('CONCAT', '1', '1.0'))

    def test_float_tolerance_is_finite_and_reserved_for_approximate_functions(self):
        self.assertTrue(outcomes_equal('SQRT', {}, outcome('1.000001', 'DOUBLE'), outcome('1.0', 'DOUBLE')))
        for left, right in [('1e999', '2e999'), ('NaN', 'nan'), ('Infinity', 'inf'),
                            ('NaN', 'NaN'), ('Infinity', 'Infinity'), ('1e999', '1e999')]:
            self.assertFalse(outcomes_equal('SQRT', {}, outcome(left, 'DOUBLE'), outcome(right, 'DOUBLE')))
        self.assertFalse(outcomes_equal('ABS', {}, outcome('1.000001', 'DOUBLE'), outcome('1.0', 'DOUBLE')))
        self.assertTrue(outcomes_equal('SQRT', {}, outcome('NULL', 'DOUBLE'), outcome('NULL', 'DOUBLE')))

    def test_json_equality_keeps_boolean_distinct_from_number_and_exact_decimal(self):
        self.assertTrue(outcomes_equal('JSON_EXTRACT', {}, outcome('{"a":1.0}', 'JSON'), outcome('{"a":1}', 'JSON')))
        self.assertFalse(outcomes_equal('JSON_EXTRACT', {}, outcome('true', 'JSON'), outcome('1', 'JSON')))
        self.assertFalse(outcomes_equal('JSON_EXTRACT', {}, outcome('1.00000000000000000001', 'JSON'), outcome('1.00000000000000000002', 'JSON')))


if __name__ == '__main__':
    unittest.main()
