#!/usr/bin/env python3
"""Guard backup acceptance WAL version, commit evidence, and integrity checks."""
from pathlib import Path
import struct
import tempfile
import unittest
from unittest.mock import patch
import zlib

import test_backup_recovery as recovery


def frame(lsn, txn, kind, payload, source_time):
    header = struct.pack('<IIQQQHHQ', 0x4d57414c, len(payload), lsn, 0,
                         txn, kind, 0, source_time)
    return header + struct.pack('<I', zlib.crc32(header + payload)) + payload


class SourceWalTests(unittest.TestCase):
    def setUp(self):
        Path('/tmp/mydb').mkdir(exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir='/tmp/mydb')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.wal = self.root / 'system/wal/wal-000000.log'
        self.wal.parent.mkdir(parents=True)
        self.version = recovery.wal_format_version()

    def write_wal(self, records, version=None):
        header = struct.pack('<8sIQQQ', b'MYDBWAL1',
                             self.version if version is None else version, 0, 0, 0)
        self.wal.write_bytes(header + struct.pack('<I', zlib.crc32(header)) + records)

    def test_version_follows_canonical_source(self):
        source = self.root / 'src/storage/format_versions.rs'
        source.parent.mkdir(parents=True)
        source.write_text('pub const WAL_VERSION: u32 = 71;\n')
        with patch.object(recovery, 'ROOT', self.root):
            self.assertEqual(recovery.wal_format_version(), 71)

    def test_missing_version_fails_explicitly(self):
        source = self.root / 'src/storage/format_versions.rs'
        source.parent.mkdir(parents=True)
        source.write_text('// No canonical version.\n')
        with patch.object(recovery, 'ROOT', self.root), \
                self.assertRaisesRegex(ValueError, 'cannot read WAL_VERSION'):
            recovery.wal_format_version()

    def test_current_version_preserves_commit_and_source_time(self):
        self.write_wal(frame(101, 17, 2, struct.pack('<QQ', 9, 501), 501))
        self.assertEqual(recovery.source_commits_after(self.root, 100), {17: (101, 501)})

    def test_other_versions_are_rejected(self):
        for version in [self.version - 1, self.version + 1]:
            with self.subTest(version=version):
                self.write_wal(b'', version)
                with self.assertRaisesRegex(ValueError,
                                            f'WAL version {version}; expected {self.version}'):
                    recovery.source_commits_after(self.root, 100)

    def test_batched_commit_keeps_logical_lsn_span(self):
        commit = struct.pack('<QQ', 10, 601)
        batch = struct.pack('<IHIHI', 2, 1, 0, 2, len(commit)) + commit
        self.write_wal(frame(102, 18, 24, batch, 601))
        self.assertEqual(recovery.source_commits_after(self.root, 100), {18: (102, 601)})

    def test_corrupt_frame_is_rejected(self):
        record = bytearray(frame(101, 17, 2, struct.pack('<QQ', 9, 501), 501))
        record[-1] ^= 1
        self.write_wal(record)
        with self.assertRaises(AssertionError):
            recovery.source_commits_after(self.root, 100)

    def test_missing_suffix_record_is_rejected(self):
        self.write_wal(frame(102, 17, 2, struct.pack('<QQ', 9, 501), 501))
        with self.assertRaises(AssertionError):
            recovery.source_commits_after(self.root, 100)


if __name__ == '__main__':
    unittest.main()
