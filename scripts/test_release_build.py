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

    def test_client_sources_and_lockfile_change_identity(self):
        original = source_hash(self.root)
        self.write('clients/mydb-cli/Cargo.lock', 'version = 4')
        changed = source_hash(self.root)
        self.assertNotEqual(changed, original)
        self.write('clients/mydb-cli/src/main.rs', 'fn main() {}')
        self.assertNotEqual(source_hash(self.root), changed)


class LicenseCliTests(unittest.TestCase):
    def test_additional_client_graph_and_lockfile_are_accepted(self):
        import sys
        import release_licenses
        Path('/tmp/mydb').mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir='/tmp/mydb') as temporary:
            root = Path(temporary)
            server = root / 'server.json'
            client = root / 'client.json'
            server.write_text('{"kind":"server"}')
            client.write_text('{"kind":"client"}')
            lock = root / 'client.lock'
            output = root / 'licenses'
            argv = ['release_licenses.py', '--source', str(root), '--output', str(output),
                    '--metadata', str(server), '--extra-metadata', str(client),
                    '--extra-lockfile', str(lock)]
            with patch.object(sys, 'argv', argv), patch.object(release_licenses, 'collect_third_party') as collect:
                release_licenses.main()
            self.assertEqual(collect.call_args.args, (root, output, {'kind': 'server'}))
            self.assertEqual(collect.call_args.kwargs,
                             {'extra_metadata': [{'kind': 'client'}], 'extra_lockfiles': [lock]})


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
            (source / 'clients/mydb-cli').mkdir(parents=True)
            (source / 'clients/mydb-cli/Cargo.lock').write_text('version = 4\n')
            (source / 'assets').mkdir()
            (source / 'assets/mydb-architecture.svg').write_text('<svg/>\n')
            artifacts = output / 'bin'
            artifacts.mkdir()
            target = output / 'target/release'
            target.mkdir(parents=True)
            for binary in ['mydb', 'mydb-backup', 'mydb-test-seed', 'mydb-cli']:
                (target / binary).write_bytes(b'fixture binary')
            (artifacts / 'third-party').mkdir()
            (artifacts / 'third-party/packages.json').write_text('[]\n')
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
                 patch.object(builder, 'call') as call, \
                 patch.object(builder, 'ensure_openssl_build_tools', return_value='fixture Perl IPC::Cmd'), \
                 patch.object(builder, 'build_release_notices'), \
                 patch.object(builder, 'source_hash', return_value='fixture-fingerprint'):
                builder.inside(args)
            with tarfile.open(output / 'mydb-1.0.0-linux-x86_64.tar.gz') as package:
                self.assertIn('mydb-1.0.0-linux-x86_64/mydb-cli', package.getnames())
                files = {m.name.removeprefix('mydb-1.0.0-linux-x86_64/') for m in package.getmembers()}
                self.assertEqual(files, {'mydb', 'mydb-backup', 'mydb-cli', 'README.md', 'LICENSE',
                                         'third-party/packages.json'})
                manifest = json.loads((output / 'mydb-1.0.0-linux-x86_64.build-manifest.json').read_text())
                self.assertEqual(set(manifest['files']), files)
            for name in ['mydb-1.0.0-linux-x86_64.abi.json', 'mydb-1.0.0.Cargo.lock',
                         'mydb-1.0.0-cli.Cargo.lock']:
                self.assertTrue((output / name).is_file(), name)
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


class PackageLayoutTests(unittest.TestCase):
    def test_slim_packages_and_external_manifests(self):
        import hashlib
        import sys
        import verify_release_package
        from release_archive import package_prefix, runtime_files, write_archive, write_runtime_readme
        with tempfile.TemporaryDirectory(dir='/tmp/mydb') as temporary:
            root = Path(temporary)
            for target, suffix in [('aarch64-apple-darwin', '.tar.gz'),
                                   ('x86_64-unknown-linux-gnu', '.tar.gz'),
                                   ('x86_64-pc-windows-msvc', '.zip')]:
                with self.subTest(target=target):
                    prefix = package_prefix('1.0.0', target)
                    payload = root / prefix
                    payload.mkdir()
                    windows = 'windows' in target
                    binary_suffix = '.exe' if windows else ''
                    for name in ['mydb', 'mydb-backup', 'mydb-cli']:
                        (payload / (name + binary_suffix)).write_bytes(b'fixture binary')
                    (payload / 'LICENSE').write_text('project license')
                    (payload / 'third-party').mkdir()
                    (payload / 'third-party/packages.json').write_text('[]')
                    for name in ['Cargo.lock', 'build-manifest.json', 'abi.json']:
                        (payload / name).write_text('developer-only file')
                    (payload / 'docs').mkdir()
                    (payload / 'docs/configuration.md').write_text('full documentation')
                    write_runtime_readme(payload, '1.0.0', windows=windows)
                    contents = runtime_files(payload, windows=windows)
                    self.assertEqual(len(contents), 6)
                    self.assertIn('https://github.com/yzsind/mydb/blob/v1.0.0/docs/',
                                  (payload / 'README.md').read_text())
                    lock_hashes = {}
                    for name, package, key in [('mydb-1.0.0.Cargo.lock', 'mydb', 'cargo_lock_sha256'),
                                               ('mydb-1.0.0-cli.Cargo.lock', 'mydb-cli', 'client_cargo_lock_sha256')]:
                        path = root / name
                        path.write_text(f'version = 4\n[[package]]\nname = "{package}"\nversion = "1.0.0"\n')
                        lock_hashes[key] = hashlib.sha256(path.read_bytes()).hexdigest()
                    manifest = {'schema_version': 2, 'version': '1.0.0', 'host': target,
                                'source_sha256': 'fixture', **lock_hashes,
                                'files': {n: hashlib.sha256(p.read_bytes()).hexdigest() for n, p in contents.items()}}
                    manifest_path = root / (prefix + '.build-manifest.json')
                    manifest_path.write_text(json.dumps(manifest))
                    archive = root / (prefix + suffix)
                    write_archive(archive, prefix, contents)
                    output = root / (prefix + '-check')
                    with patch.object(sys, 'argv', ['verify_release_package.py', '--files-only',
                                      '--package', str(archive), '--output', str(output)]), patch('builtins.print'):
                        verify_release_package.main()
                    result = json.loads((output / 'result.json').read_text())
                    self.assertEqual(result['execution'], 'not-run-files-only')
                    # Even a matching manifest must not permit documentation to leak back into the runtime archive.
                    contents['docs/configuration.md'] = payload / 'docs/configuration.md'
                    manifest['files']['docs/configuration.md'] = hashlib.sha256(contents['docs/configuration.md'].read_bytes()).hexdigest()
                    manifest_path.write_text(json.dumps(manifest))
                    write_archive(archive, prefix, contents)
                    with patch.object(sys, 'argv', ['verify_release_package.py', '--files-only',
                                      '--package', str(archive), '--output', str(root / (prefix + '-bad'))]):
                        with self.assertRaisesRegex(AssertionError, 'runtime files'):
                            verify_release_package.main()

    def test_platform_archives_extract_into_the_named_directory(self):
        import zipfile
        from release_archive import package_prefix, validate_archive_paths, write_archive
        Path('/tmp/mydb').mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir='/tmp/mydb') as temporary:
            root = Path(temporary)
            binary = root / 'binary'
            binary.write_bytes(b'executable payload')
            binary.chmod(0o755)
            for target, platform, suffix in [
                ('aarch64-apple-darwin', 'macos-arm64', '.tar.gz'),
                ('x86_64-unknown-linux-gnu', 'linux-x86_64', '.tar.gz'),
                ('x86_64-pc-windows-msvc', 'windows-x86_64', '.zip'),
            ]:
                with self.subTest(target=target):
                    prefix = package_prefix('1.0.0', target)
                    self.assertEqual(prefix, f'mydb-1.0.0-{platform}')
                    archive = root / (prefix + suffix)
                    write_archive(archive, prefix, {'mydb': binary, 'docs/README.md': binary})
                    extracted = root / platform
                    if suffix == '.zip':
                        with zipfile.ZipFile(archive) as bundle:
                            validate_archive_paths(bundle.namelist(), prefix)
                            bundle.extractall(extracted)
                    else:
                        with tarfile.open(archive) as bundle:
                            validate_archive_paths(bundle.getnames(), prefix)
                            bundle.extractall(extracted, filter='data')
                        self.assertEqual((extracted / prefix / 'mydb').stat().st_mode & 0o777, 0o755)
                    self.assertEqual([p.name for p in extracted.iterdir()], [prefix])
                    self.assertEqual((extracted / prefix / 'mydb').read_bytes(), binary.read_bytes())
                    self.assertEqual((extracted / prefix / 'docs/README.md').read_bytes(), binary.read_bytes())

    def test_flat_and_escaping_entries_are_rejected(self):
        from release_archive import validate_archive_paths
        for names in [[], ['mydb'], ['other/mydb'], ['pkg/../mydb'], ['/pkg/mydb'], ['pkg\\mydb']]:
            with self.subTest(names=names), self.assertRaises(ValueError):
                validate_archive_paths(names, 'pkg')


class PackagePreflightTests(unittest.TestCase):
    def test_missing_license_stops_before_compilation(self):
        import sys
        import package_release
        Path('/tmp/mydb').mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir='/tmp/mydb') as temporary:
            def command(*args, **kwargs):
                if args[0] == 'rustc':
                    return 'host: x86_64-pc-windows-msvc\n'
                return '{}'
            with patch.object(sys, 'argv', ['package_release.py', '--output', temporary + '/package']), \
                 patch.object(package_release, 'source_hash', return_value='source'), \
                 patch.object(package_release, 'build_provenance', return_value=('head', '')), \
                 patch.object(package_release, 'command', side_effect=command), \
                 patch.object(package_release, 'collect_third_party', side_effect=SystemExit('missing license')), \
                 patch.object(package_release.subprocess, 'run') as build:
                with self.assertRaisesRegex(SystemExit, 'missing license'):
                    package_release.main()
                build.assert_not_called()


if __name__ == '__main__':
    unittest.main()
