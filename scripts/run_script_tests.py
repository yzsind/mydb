#!/usr/bin/env python3
"""Run every offline unittest module without importing integration entry points."""
import argparse
import ast
import hashlib
import importlib.metadata
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent


def unit_modules(directory=ROOT):
    modules = []
    for path in sorted(directory.glob('test_*.py')):
        tree = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
        case_names = {'TestCase', 'IsolatedAsyncioTestCase'}
        for node in tree.body:
            if isinstance(node, ast.ImportFrom) and node.module == 'unittest':
                case_names.update(alias.asname or alias.name for alias in node.names
                                  if alias.name in ('TestCase', 'IsolatedAsyncioTestCase'))
        # Integration drivers also use test_*.py and some parse argv at import
        # time. Only modules declaring a unittest case are safe to load here.
        if any(isinstance(node, ast.ClassDef) and any(
                (isinstance(base, ast.Attribute) and base.attr in ('TestCase', 'IsolatedAsyncioTestCase')) or
                (isinstance(base, ast.Name) and base.id in case_names) for base in node.bases)
                for node in tree.body):
            modules.append(path)
    if not modules:
        raise ValueError('no offline unittest modules found')
    return modules


def passed(result):
    return result.wasSuccessful() and result.testsRun > len(result.skipped)


def git_commit(directory):
    try:
        top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], cwd=directory,
                                      text=True, stderr=subprocess.DEVNULL).strip()
        if Path(top).resolve() != directory.resolve():
            return None
        return subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=directory,
                                       text=True, stderr=subprocess.DEVNULL).strip()
    except (OSError, subprocess.CalledProcessError):
        # Exported public sources have no Git metadata; per-file hashes still
        # bind the results to the exact scripts that executed.
        return None


def evidence(modules, result):
    sources = [path for path in ROOT.rglob('*') if path.is_file()
               and path.suffix in ('.py', '.sh', '.ps1', '.txt', '.json', '.env')
               and '__pycache__' not in path.parts]
    return dict(python=sys.version, platform=platform.platform(),
        commit=git_commit(ROOT.parent),
        dependencies={entry.metadata['Name']: entry.version for entry in importlib.metadata.distributions()},
        sources={str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources},
        modules=[path.name for path in modules], tests=result.testsRun,
        failures=len(result.failures), errors=len(result.errors), skipped=len(result.skipped),
        status='passed' if passed(result) else 'failed')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=Path('/tmp/mydb/script-tests.json'))
    args = parser.parse_args()
    if not args.output.resolve().is_relative_to(Path('/tmp/mydb').resolve()):
        parser.error('--output must be below /tmp/mydb')
    Path('/tmp/mydb').mkdir(parents=True, exist_ok=True)
    os.environ['TMPDIR'] = '/tmp/mydb'
    os.environ['PYTHONDONTWRITEBYTECODE'] = '1'
    tempfile.tempdir = '/tmp/mydb'
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(ROOT))
    from safety_evidence import write_json
    args.output.parent.mkdir(parents=True, exist_ok=True)
    write_json(args.output, dict(status='incomplete', python=sys.version))
    modules = unit_modules()
    suite = unittest.TestSuite()
    loader = unittest.TestLoader()
    for path in modules:
        try:
            module = __import__(path.stem)
            tests = loader.loadTestsFromModule(module)
        except Exception:
            # Let unittest represent import failures as errors and keep running
            # the remaining modules so the evidence covers the entire suite.
            tests = loader.loadTestsFromName(path.stem)
        suite.addTests(tests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    write_json(args.output, evidence(modules, result))
    return 0 if passed(result) else 1


if __name__ == '__main__':
    raise SystemExit(main())
