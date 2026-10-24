import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import unittest

from clickbench_prepare_expected import duckdb_schema, sha256, sql_string

ROOT = Path(__file__).resolve().parent.parent


class PrepareTests(unittest.TestCase):
    def test_schema_preserves_types_and_rejects_unknown_definitions(self):
        schema = 'CREATE TABLE hits (x BIGINT NOT NULL, s VARCHAR(255), PRIMARY KEY (x)) ENGINE=InnoDB;'
        converted = duckdb_schema(schema)
        self.assertIn('x BIGINT NOT NULL', converted)
        self.assertIn('s VARCHAR(255)', converted)
        self.assertNotIn('PRIMARY KEY', converted)
        for invalid in [schema.replace('BIGINT', 'JSON'), schema + 'DROP TABLE other;',
                        'CREATE TABLE other (x BIGINT) ENGINE=InnoDB;']:
            with self.assertRaises(ValueError):
                duckdb_schema(invalid)
        self.assertEqual(sql_string("/tmp/a'b"), "'/tmp/a''b'")

    @unittest.skipUnless(shutil.which('duckdb'), 'DuckDB CLI not installed')
    def test_prepare_resume_cache_hit_and_failed_import(self):
        with tempfile.TemporaryDirectory(prefix='clickbench-prepare-test-') as name:
            cache = Path(name)
            source = ROOT / 'scripts/clickbench'
            (cache / 'queries.sql').write_bytes((source / 'queries').read_bytes().rstrip(b'\n') + b'\n')
            columns = re.findall(r'^\s+(\w+) (BIGINT|SMALLINT|INTEGER|TEXT|TIMESTAMP|Date|VARCHAR|CHAR)\b',
                                 (source / 'create.sql').read_text(), re.M)
            raw_text = r'google \f \n \N \\ trail '
            values = [{'TEXT': raw_text, 'VARCHAR': 'google example', 'CHAR': 'a',
                       'TIMESTAMP': '2013-07-14 12:30:00', 'Date': '2013-07-14'}.get(t, '0')
                      for _, t in columns]
            archive = cache / 'hits.tsv.gz'
            with gzip.open(archive, 'wb') as out:
                out.write(('\t'.join(values) + '\n').encode() * 3)
            self.assertEqual(sha256(archive), hashlib.sha256(archive.read_bytes()).hexdigest())
            command = [str(ROOT / 'scripts/prepare_clickbench_expected.sh'), '--cache-dir', name,
                       '--archive-bytes', str(archive.stat().st_size), '--archive-sha256', sha256(archive),
                       '--row-count', '3', '--dataset-id', 'test-dataset', '--threads', '1', '--memory-limit', '256MB']
            env = {k: v for k, v in os.environ.items() if not k.startswith('CLICKBENCH_') and k != 'DUCKDB_BIN'}
            result = subprocess.run(command, capture_output=True, text=True, env=env)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            expected = cache / 'expected-duckdb'
            self.assertEqual(len(list(expected.glob('q*.json'))), 43)
            # Match LOAD DATA ESCAPED BY '': no backslash interpretation,
            # no trimming, and only a whole-field NULL is null.
            self.assertEqual(json.loads((expected / 'q24.json').read_text())['rows'][0][2], raw_text)
            database = cache / 'duckdb/hits-v2.duckdb'
            modified = database.stat().st_mtime_ns
            # Complete cache is reusable without the DuckDB executable or source archive.
            archive.rename(cache / 'archive.saved')
            result = subprocess.run(command + ['--duckdb-bin', '/absent/duckdb'],
                                    capture_output=True, text=True, env=env)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('All 43 DuckDB result caches are valid', result.stdout)
            # A missing query resumes from the completed database, without reloading data.
            (expected / 'q01.json').unlink()
            result = subprocess.run(command, capture_output=True, text=True, env=env)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn('Reusing DuckDB database', result.stdout)
            self.assertEqual(database.stat().st_mtime_ns, modified)
            # A stale schema/database must not be overwritten.
            (expected / 'q01.json').unlink()
            result = subprocess.run(command + ['--row-count', '4'], capture_output=True, text=True, env=env)
            self.assertEqual(result.returncode, 1)
            self.assertIn('existing database has no matching manifest', result.stderr)
            self.assertEqual(database.stat().st_mtime_ns, modified)
            # A failed row-count check never publishes a complete database.
            (cache / 'archive.saved').rename(archive)
            other = cache / 'other.duckdb'
            result = subprocess.run(command + ['--row-count', '4', '--duckdb-database', str(other)],
                                    capture_output=True, text=True, env=env)
            self.assertEqual(result.returncode, 1)
            self.assertIn('loaded row count mismatch', result.stderr)
            self.assertFalse(other.exists())
            self.assertFalse(Path(str(other) + '.manifest.json').exists())
            # Refuse a corrupt archive before importing.
            result = subprocess.run(command + ['--duckdb-database', str(other), '--archive-sha256', '0' * 64],
                                    capture_output=True, text=True, env=env)
            self.assertEqual(result.returncode, 1)
            self.assertIn('archive SHA256 mismatch', result.stderr)
            # The lock prevents concurrent preparations in the same cache.
            with (cache / 'prepare-duckdb.lock').open('w') as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                result = subprocess.run(command, capture_output=True, text=True, env=env)
                self.assertEqual(result.returncode, 1)


if __name__ == '__main__':
    unittest.main()
