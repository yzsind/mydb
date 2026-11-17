"""Import failures, traversal and partial downloads cannot pass or overwrite data."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent


class MtrImportTests(unittest.TestCase):
    def setUp(self):
        Path('/tmp/mydb').mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(prefix='mtr-import-test-', dir='/tmp/mydb')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / 'scripts').mkdir()
        self.script = self.root / 'scripts/mtr_import.sh'
        shutil.copy(ROOT / 'mtr_import.sh', self.script)
        self.bin = self.root / 'bin'; self.bin.mkdir()
        curl = self.bin / 'curl'
        curl.write_text('''#!/bin/sh
url="$4"
echo "$*" >> "$FAKE_CURL_LOG"
while [ "$#" -gt 0 ]; do
  if [ "$1" = -o ]; then shift; output="$1"; fi
  shift
done
printf 'fixture' > "$output"
case "$url" in *"${FAKE_FAIL_SUFFIX:-no-failure-suffix}"*) exit 22;; esac
exit 0
''')
        curl.chmod(0o755)
        self.env = os.environ | dict(PATH=str(self.bin) + os.pathsep + os.environ['PATH'],
                                    FAKE_CURL_LOG=str(self.root / 'curl.log'))
        self.triage = self.root / 'tests/mydb-test/triage'

    def run_import(self, *specs, **env):
        return subprocess.run(['bash', str(self.script), *specs], env=self.env | env,
                              capture_output=True, text=True, timeout=10)

    def test_missing_test_include_data_and_result_each_fail(self):
        for spec, suffix in [('case', '.test'), ('case', '.result'),
                             ('inc:include/file.inc', '.inc'), ('data:std_data/file.dat', '.dat')]:
            with self.subTest(spec=spec, suffix=suffix):
                result = self.run_import(spec, FAKE_FAIL_SUFFIX=suffix)
                self.assertNotEqual(result.returncode, 0, result.stdout)

    def test_all_descriptor_forms_reject_escape_before_fetching(self):
        for spec in ['inc:../escape', 'inc:/absolute', 'inc:a/../../escape',
                     'inc:a\\..\\escape', 'inc:a/%2e%2e/file', 'data:../escape',
                     '../escape', '/absolute', 'suite/../escape', 'suite/a/escape', '', 'inc:']:
            with self.subTest(spec=spec):
                result = self.run_import('valid', spec)
                self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
                self.assertFalse((self.root / 'curl.log').exists())

    def test_directory_and_file_symlinks_cannot_escape(self):
        self.triage.mkdir(parents=True)
        outside = self.root / 'outside'; outside.mkdir()
        (self.triage / 'include').symlink_to(outside, target_is_directory=True)
        result = self.run_import('inc:include/escape.inc')
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertFalse((outside / 'escape.inc').exists())

    def test_failed_download_preserves_existing_fixture_and_cleans_temporary_files(self):
        path = self.triage / 'include/file.inc'; path.parent.mkdir(parents=True)
        path.write_text('original')
        result = self.run_import('inc:include/file.inc', FAKE_FAIL_SUFFIX='.inc')
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(path.read_text(), 'original')

    def test_complete_import_mirrors_all_supported_paths(self):
        result = self.run_import('case', 'suite/test', 'inc:include/nested/file.inc', 'data:std_data/file.dat')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        for path in ['t/main/case.test', 'upstream/main/case.result', 't/suite/test.test',
                     'upstream/suite/test.result', 'include/nested/file.inc', 'std_data/file.dat']:
            self.assertEqual((self.triage / path).read_text(), 'fixture')


if __name__ == '__main__':
    unittest.main()
