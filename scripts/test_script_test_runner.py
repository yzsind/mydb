"""Keep new offline regressions automatic and integration imports isolated."""
import contextlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import run_script_tests as runner


class ScriptRunnerTests(unittest.TestCase):
    def setUp(self):
        Path('/tmp/mydb').mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir='/tmp/mydb')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_discovers_new_unittest_modules_without_importing_integration_drivers(self):
        (self.root / 'test_new_unit.py').write_text('import unittest\nclass Checks(unittest.TestCase):\n pass\n')
        (self.root / 'test_live.py').write_text('raise RuntimeError("integration must not be imported")\n')
        (self.root / 'test_alias.py').write_text('from unittest import TestCase as Case\nclass Checks(Case):\n pass\n')
        (self.root / 'test_async.py').write_text('import unittest\nclass Checks(unittest.IsolatedAsyncioTestCase):\n pass\n')
        self.assertEqual([p.name for p in runner.unit_modules(self.root)],
                         ['test_alias.py', 'test_async.py', 'test_new_unit.py'])
        with self.assertRaises(ValueError):
            runner.unit_modules(self.root / 'missing')

    def test_zero_or_all_skipped_tests_cannot_pass(self):
        empty = unittest.TestResult()
        self.assertFalse(runner.passed(empty))
        empty.testsRun = 1
        empty.skipped = [('control', 'not executed')]
        self.assertFalse(runner.passed(empty))
        empty.testsRun = 2
        self.assertTrue(runner.passed(empty))

    def test_failing_and_import_error_modules_fail_the_runner_and_record_evidence(self):
        for name, source in [('test_new_failure', 'import unittest\nclass Checks(unittest.TestCase):\n'
                              ' def test_failure(self): self.fail("negative control")\n'),
                             ('test_new_import_error', 'import missing_script_test_dependency\n'
                              'import unittest\nclass Checks(unittest.TestCase):\n pass\n')]:
            path = self.root / (name + '.py'); path.write_text(source)
            def evidence(modules, result):
                return dict(tests=result.testsRun, failures=len(result.failures), errors=len(result.errors))
            with patch.object(runner, 'ROOT', self.root), \
                 patch.object(runner, 'unit_modules', return_value=[path]), \
                 patch.object(runner, 'evidence', side_effect=evidence), \
                 patch.object(sys, 'argv', ['tests', '--output', str(self.root / 'results.json')]), \
                 contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(runner.main(), 1)
            data = json.loads((self.root / 'results.json').read_text())
            self.assertGreater(data['failures'] + data['errors'], 0)
            sys.modules.pop(name, None)


if __name__ == '__main__':
    unittest.main()
