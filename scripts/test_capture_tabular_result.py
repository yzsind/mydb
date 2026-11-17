"""Round-trip typed captures and reject partial/overwritten capture evidence."""
import datetime
from decimal import Decimal
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

from _tabular_results import canonicalize
from capture_tabular_result import capture


class CaptureTests(unittest.TestCase):
    def setUp(self):
        Path('/tmp/mydb').mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir='/tmp/mydb')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.sql = self.root / 'query.sql'; self.sql.write_text('SELECT * FROM t')

    def connection(self, rows, fail=False):
        class Cursor:
            description = [('n', 8), ('null_value', 253), ('s', 253), ('d', 246),
                           ('b', 252), ('dt', 12), ('time', 11)]
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def execute(self, sql): pass
            def fetchmany(self, count):
                if fail: raise RuntimeError('read interrupted')
                current, rows[:] = rows[:count], rows[count:]
                return current
        return SimpleNamespace(cursor=lambda *args: Cursor())

    def test_roundtrip_preserves_null_strings_numbers_binary_and_temporal_values(self):
        output = self.root / 'q01.json'
        rows = [(1, None, 'NULL', Decimal('1.0000100'), b'\x00\t\n\xff',
                 datetime.datetime(2026, 11, 17, 1, 2, 3, 123456), datetime.timedelta(seconds=-5))]
        capture(self.connection(rows), self.sql, output)
        data = json.loads(output.read_text())
        self.assertEqual(data['rows'][0][1:3], [None, 'NULL'])
        self.assertEqual(data['rows'][0][4], {'bytes': 'AAkK/w=='})
        result = canonicalize(output, contract={'format': 'typed-json'})
        self.assertEqual(result.rows[0][3], {'decimal': '1.00001'})
        data['sql_sha256'] = '0' * 64
        output.write_text(json.dumps(data))
        self.assertNotEqual(result, canonicalize(output, contract={'format': 'typed-json'}))

    def test_failed_capture_removes_partial_output_and_never_replaces_existing_evidence(self):
        output = self.root / 'q01.json'
        with self.assertRaisesRegex(RuntimeError, 'interrupted'):
            capture(self.connection([], fail=True), self.sql, output)
        self.assertFalse(output.exists())
        output.write_text('existing evidence')
        with self.assertRaises(FileExistsError):
            capture(self.connection([]), self.sql, output)
        self.assertEqual(output.read_text(), 'existing evidence')


if __name__ == '__main__':
    unittest.main()
