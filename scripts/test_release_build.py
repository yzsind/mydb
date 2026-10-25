#!/usr/bin/env python3
"""Validate immutable build inputs without hashing machine-local artifacts."""
from pathlib import Path
import json
import os
import shlex
import tempfile
import tarfile
import tomllib
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import build_linux_glibc217 as builder
from build_linux_glibc217 import source_hash


class SourceFingerprintTests(unittest.TestCase):
    def setUp(self):
        Path('/tmp/mydb').mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir='/tmp/mydb')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        for name in ('Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml', 'build.rs',
                     'src/lib.rs', 'assets/mysql80/fixture.json', 'tests/r/core.result',
                     '.config/nextest.toml'):
            self.write(name, 'original')

    def write(self, name, content):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)

    def test_generated_data_does_not_change_default_build_identity(self):
        original = source_hash(self.root)
        for name in ('src/.DS_Store', 'tests/fuzz/corpus/generated',
                     'tests/fuzz/artifacts/crash', 'tests/fuzz/target/binary',
                     'tests/mydb-test/triage/upstream/imported', 'tests/__pycache__/cache'):
            self.write(name, 'generated')
        self.assertEqual(source_hash(self.root), original)

    def test_owned_source_and_fixture_mutations_change_identity(self):
        original = source_hash(self.root)
        self.write('tests/r/core.result', 'new expected rows')
        self.assertNotEqual(source_hash(self.root), original)
        self.write('tests/r/core.result', 'original')
        self.write('src/lib.rs', 'different implementation')
        self.assertNotEqual(source_hash(self.root), original)

    def test_appledouble_source_is_rejected(self):
        self.write('src/._lib.rs', 'not a Rust source file')
        with self.assertRaises(SystemExit):
            source_hash(self.root)

    def test_vendored_dependency_sources_are_part_of_build_identity(self):
        original = source_hash(self.root)
        self.write('vendor/mysql/src/lib.rs', 'patched TLS parser')
        self.assertNotEqual(source_hash(self.root), original)


class RuntimeSetupTests(unittest.TestCase):
    def test_pinned_runner_satisfies_repository_version_requirement(self):
        config = tomllib.loads((builder.ROOT / '.config/nextest.toml').read_text())
        required = config['nextest-version']['required']
        self.assertGreaterEqual(tuple(map(int, builder.NEXT.split('.'))),
                                tuple(map(int, required.split('.'))))

    def test_adapter_preserves_other_commands_and_quotes_executable(self):
        config = (builder.ROOT / '.config/nextest.toml').read_text()
        config += '\n[scripts.setup.other]\ncommand = "echo leave this alone"\n'
        for binary in [Path("/tmp/mydb/path with spaces/it's a seed"),
                       Path('/tmp/mydb/path\\with\\backslashes/seed')]:
            runtime = tomllib.loads(builder.runtime_nextest_config(config, binary))
            self.assertEqual(shlex.split(runtime['scripts']['setup']['mydb-test-seed']['command']),
                             [str(binary)])
            expected = tomllib.loads(config)
            expected['scripts']['setup']['mydb-test-seed']['command'] = runtime['scripts']['setup']['mydb-test-seed']['command']
            self.assertEqual(runtime, expected)

    def test_adapter_rejects_unknown_or_malformed_setup(self):
        config = (builder.ROOT / '.config/nextest.toml').read_text()
        for invalid in [config.replace('--ignored --exact', '--exact'),
                        config.replace('test_seed_setup::prepare_nextest_seed', 'other_test'),
                        config.replace('[scripts.setup.mydb-test-seed]', '[scripts.setup.other]'),
                        config + '\n[scripts.setup.mydb-test-seed]\n',
                        config.replace('command = "cargo test', 'command = ["cargo test')]:
            with self.subTest(config=invalid), self.assertRaises(SystemExit):
                builder.runtime_nextest_config(invalid, Path('/tmp/mydb/seed'))

    def test_current_setup_can_be_archived_without_cargo_at_runtime(self):
        with tempfile.TemporaryDirectory(dir='/tmp/mydb') as temporary:
            root = Path(temporary)
            source, output = root / 'source', root / 'output'
            (source / '.config').mkdir(parents=True)
            output.mkdir()
            config = (builder.ROOT / '.config/nextest.toml').read_text()
            (source / '.config/nextest.toml').write_text(config)
            (source / 'Cargo.toml').write_text('[package]\nversion = "1.0.0"\n')
            (source / 'Cargo.lock').write_text('version = 4\n')
            (source / 'LICENSE').write_text('fixture license\n')
            (source / 'assets').mkdir()
            (source / 'assets/mydb-architecture.svg').write_text('<svg/>\n')
            artifacts = output / 'bin'
            artifacts.mkdir()
            for binary in ['mydb', 'mydb-backup', 'mydb-test-seed']:
                (artifacts / binary).write_bytes(b'fixture binary')
            (output / 'abi.json').write_text('{}\n')
            (output / 'tests.tar.zst').write_bytes(b'fixture test archive')
            runner = root / 'cargo-nextest'
            runner.write_bytes(b'fixture runner')
            args = SimpleNamespace(source=source, output=output, offline=True,
                                   test_archive=True, test_no_incremental=True, image=builder.IMAGE)
            def check_output(command, **kwargs):
                return {('getconf', 'GNU_LIBC_VERSION'): 'glibc 2.17\n',
                        ('rustc', '-vV'): f'release: {builder.RUST}\nhost: x86_64-unknown-linux-gnu\n',
                        ('gcc', '--version'): 'fixture compiler\n'}[tuple(command)]
            with patch.dict(os.environ, {'CARGO_HOME': str(root / 'cargo')}, clear=True), \
                 patch.object(builder.subprocess, 'check_output', side_effect=check_output), \
                 patch.object(builder.shutil, 'which', return_value=str(runner)), \
                 patch.object(builder.shutil, 'copy2'), patch.object(builder, 'call') as call, \
                 patch.object(builder, 'ensure_openssl_build_tools', return_value='fixture Perl IPC::Cmd'), \
                 patch.object(builder, 'build_release_notices'), \
                 patch.object(builder, 'source_hash', return_value='fixture-fingerprint'):
                builder.inside(args)
            with tarfile.open(output / 'mydb-1.0.0-x86-linux.tar.gz') as package:
                self.assertIn('assets/mydb-architecture.svg', package.getnames())
            release = next(c for c in call.call_args_list if c.args[0][:2] == ['cargo', 'build'])
            archive = next(c for c in call.call_args_list if c.args[0][0] == 'cargo-nextest')
            self.assertNotIn('env', release.kwargs)
            self.assertEqual(archive.kwargs['env']['CARGO_INCREMENTAL'], '0')
            runtime = tomllib.loads((output / 'nextest-runtime.toml').read_text())
            original = tomllib.loads(config)
            expected = str(artifacts / 'mydb-test-seed')
            self.assertEqual(runtime['scripts']['setup']['mydb-test-seed']['command'], expected)
            original['scripts']['setup']['mydb-test-seed']['command'] = expected
            self.assertEqual(runtime, original)
            manifest = json.loads((output / 'build-manifest.json').read_text())
            self.assertEqual(manifest['test_build'], {'profile': 'test', 'incremental_override': '0'})
            self.assertIn('test_artifacts', manifest,
                          'archive and runtime setup must be bound to the successful build')
            builder.verify_test_artifacts(output, manifest, runner)
            for path in [output / 'tests.tar.zst', output / 'nextest-runtime.toml', runner]:
                original = path.read_bytes()
                path.write_bytes(b'stale or modified artifact')
                with self.subTest(artifact=path.name), self.assertRaises(SystemExit):
                    builder.verify_test_artifacts(output, manifest, runner)
                path.write_bytes(original)
            with self.assertRaises(SystemExit):
                builder.verify_test_artifacts(output, {}, runner)


if __name__ == '__main__':
    unittest.main()
