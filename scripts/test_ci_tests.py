#!/usr/bin/env python3
"""Validate archive setup identity, path remapping, and preserved test policy."""
import copy
from pathlib import Path
import shlex
import tempfile
import tomllib
import unittest

from build_linux_glibc217 import ROOT, runtime_nextest_config
from run_ci_tests import archived_seed_binary


class ArchivedSetupTests(unittest.TestCase):
    def setUp(self):
        Path('/tmp/mydb').mkdir(exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir='/tmp/mydb')
        self.addCleanup(self.temporary.cleanup)
        self.extraction = Path(self.temporary.name).resolve()
        self.binary = self.extraction / 'target/debug/deps/mydb-feature-hash'
        self.binary.parent.mkdir(parents=True)
        self.binary.touch()
        self.metadata = {
            'rust-build-meta': {'target-directory': '/original/target'},
            'rust-binaries': {
                'mydb': {'binary-name': 'mydb', 'kind': 'lib',
                         'binary-path': '/original/target/debug/deps/mydb-feature-hash'},
                'mydb::bin/mydb': {'binary-name': 'mydb', 'kind': 'bin',
                                  'binary-path': '/original/target/debug/deps/mydb-bin-hash'},
            },
        }

    def test_selects_archived_library_instead_of_server_or_stale_binaries(self):
        stale = self.binary.with_name('mydb-other-feature-hash')
        stale.touch()
        self.assertEqual(archived_seed_binary(self.metadata, self.extraction), self.binary)
        # New Cargo versions can put intermediates outside target-directory.
        self.metadata['rust-build-meta']['build-directory'] = '/original/build'
        self.metadata['rust-binaries']['mydb']['binary-path'] = (
            '/original/build/debug/deps/mydb-feature-hash')
        self.assertEqual(archived_seed_binary(self.metadata, self.extraction), self.binary)

    def test_rejects_missing_ambiguous_and_escaping_binaries(self):
        variants = []
        no_library = copy.deepcopy(self.metadata)
        del no_library['rust-binaries']['mydb']
        variants.append(no_library)
        ambiguous = copy.deepcopy(self.metadata)
        ambiguous['rust-binaries']['duplicate'] = ambiguous['rust-binaries']['mydb'].copy()
        variants.append(ambiguous)
        for path in ['/elsewhere/mydb', '/original/target/../../outside',
                     '/original/target/debug/deps/missing']:
            metadata = copy.deepcopy(self.metadata)
            metadata['rust-binaries']['mydb']['binary-path'] = path
            variants.append(metadata)
        for metadata in variants:
            with self.subTest(metadata=metadata), self.assertRaises(ValueError):
                archived_seed_binary(metadata, self.extraction)

    def test_setup_executes_matching_harness_and_preserves_all_test_policy(self):
        config = (ROOT / '.config/nextest.toml').read_text()
        binary = Path("/tmp/mydb/path with spaces/it's a test binary")
        rewritten = tomllib.loads(runtime_nextest_config(config, binary, test_harness=True))
        command = rewritten['scripts']['setup']['mydb-test-seed']['command']
        self.assertEqual(shlex.split(command), [str(binary), '--ignored', '--exact',
                                               'test_seed_setup::prepare_nextest_seed'])
        expected = tomllib.loads(config)
        expected['scripts']['setup']['mydb-test-seed']['command'] = command
        self.assertEqual(rewritten, expected)


if __name__ == '__main__':
    unittest.main()
