#!/usr/bin/env python3
"""Reject empty TSan selections and preserve sanitizer execution failures."""
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class TSanRunnerTests(unittest.TestCase):
    def setUp(self):
        Path('/tmp/mydb').mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(prefix='tsan-runner-test-', dir='/tmp/mydb')
        self.addCleanup(self.temp.cleanup)
        self.folder = Path(self.temp.name)
        self.bin = self.folder / 'bin'
        self.bin.mkdir()
        self.calls = self.folder / 'calls.jsonl'
        self.executable('rustc', 'print("host: x86_64-unknown-linux-gnu")\n')
        self.executable('cargo', '''import json, os, sys
args = sys.argv[1:]
tail = args[args.index('--') + 1:]
name = tail[0]
listing = '--list' in tail
with open(os.environ['TSAN_FAKE_CALLS'], 'a') as log:
    log.write(json.dumps({'args': args, 'filter': name, 'listing': listing,
                          'flags': os.environ['RUSTFLAGS'],
                          'options': os.environ['TSAN_OPTIONS'],
                          'tmp': os.environ['TMPDIR']}) + '\\n')
mode = os.environ['TSAN_FAKE_MODE']
if listing:
    if name == 'missing':
        print('0 tests, 0 benchmarks')
    else:
        print(f'\u001b[32mfixture::{name}: test\u001b[0m\\n\\n1 test, 0 benchmarks')
elif mode == 'sanitizer-failure':
    print('WARNING: ThreadSanitizer: data race', file=sys.stderr)
    sys.exit(66)
elif mode == 'ignored-only' or name == 'missing':
    print('test result: ok. 0 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out')
else:
    print(f'test fixture::{name} ... ok')
    print('\u001b[32mtest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\u001b[0m')
''')

    def executable(self, name, body):
        path = self.bin / name
        path.write_text(f'#!{sys.executable}\n' + body)
        path.chmod(0o755)

    def run_script(self, filters, mode='pass'):
        env = dict(os.environ, PATH=str(self.bin) + os.pathsep + os.environ['PATH'],
                   TSAN_FAKE_CALLS=str(self.calls), TSAN_FAKE_MODE=mode,
                   RUSTFLAGS='--cfg fixture', TSAN_OPTIONS='halt_on_error=1')
        result = subprocess.run(['bash', str(ROOT / 'scripts/run_tsan_btree.sh'), *filters],
                                env=env, text=True, capture_output=True, timeout=20)
        output = re.search(r'^output\s+: (.+)$', result.stdout, re.MULTILINE)
        if output:
            self.addCleanup(shutil.rmtree, output[1], ignore_errors=True)
        calls = ([json.loads(line) for line in self.calls.read_text().splitlines()]
                 if self.calls.exists() else [])
        return result, calls

    def test_empty_filter_is_rejected(self):
        result, calls = self.run_script([''])
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('filter must not be empty', result.stderr)
        self.assertEqual(calls, [])

    def test_missing_filter_fails_before_any_test_execution(self):
        result, calls = self.run_script(['first', 'missing'])
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("filter 'missing' matches no tests", result.stderr)
        self.assertTrue(all(call['listing'] for call in calls))

    def test_ignored_only_selection_is_not_success(self):
        result, _ = self.run_script(['first'], 'ignored-only')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('executed no successful tests', result.stderr)

    def test_sanitizer_failure_retains_its_exit_status(self):
        result, _ = self.run_script(['first'], 'sanitizer-failure')
        self.assertEqual(result.returncode, 66)
        self.assertIn('ThreadSanitizer: data race', result.stdout)

    def test_selected_groups_all_execute_with_sanitizer(self):
        result, calls = self.run_script(['first', 'second'])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual([(call['filter'], call['listing']) for call in calls],
                         [('first', True), ('second', True),
                          ('first', False), ('second', False)])
        for call in calls:
            self.assertIn('--locked', call['args'])
            self.assertEqual(call['flags'], '--cfg fixture -Zsanitizer=thread')
            self.assertEqual(call['options'], 'halt_on_error=1')
            self.assertTrue(Path(call['tmp']).is_relative_to('/tmp/mydb'))


if __name__ == '__main__':
    unittest.main()
