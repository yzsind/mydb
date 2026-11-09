#!/usr/bin/env python3
"""Guard backup acceptance WAL evidence, integrity, and writer admission."""
from pathlib import Path
import struct
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import zlib

import test_backup_recovery as recovery


class CaptureAdmissionTests(unittest.TestCase):
    def test_measurement_retries_capture_barriers_and_lock_timeouts(self):
        release = threading.Event()
        self.addCleanup(release.set)
        errors = [
            recovery.mysql.connector.Error(errno=1290, msg=message)
            for message in [
                'new writes are paused while physical backup drains active writers; retry',
                'write blocked by capture admission barrier; retry after physical backup capture',
            ]
        ] + [recovery.mysql.connector.Error(errno=1205, msg='lock wait timeout')]

        def operation():
            if not release.wait(5):
                raise TimeoutError('writer never resumed')
            return 'backup complete'

        def query(*args):
            if errors:
                raise errors.pop(0)
            release.set()

        with patch.object(recovery, 'sql', query):
            metric, value = recovery.measure_while(None, operation)
        self.assertEqual(value, 'backup complete')
        self.assertEqual(metric['admission_retries'], 3)
        self.assertGreaterEqual(metric['transactions'], 1)

    def test_unrelated_write_errors_propagate_after_worker_cleanup(self):
        for errno, message in [
            (1290, 'server is read-only'),
            (1290, 'pending WAL budget exhausted'),
            (1064, 'syntax error'),
        ]:
            with self.subTest(errno=errno, message=message):
                release = threading.Event()
                finished = threading.Event()
                self.addCleanup(release.set)
                error = recovery.mysql.connector.Error(errno=errno, msg=message)

                def operation():
                    release.wait(5)
                    time.sleep(.01)
                    finished.set()

                def query(*args):
                    release.set()
                    raise error

                with patch.object(recovery, 'sql', query), \
                        self.assertRaises(recovery.mysql.connector.Error) as caught:
                    recovery.measure_while(None, operation)
                self.assertIs(caught.exception, error)
                self.assertTrue(finished.is_set(), 'background operation must finish before its connection closes')

    def test_backup_failure_is_not_hidden_by_admission_retries(self):
        started = threading.Event()

        def operation():
            started.wait(5)
            raise RuntimeError('backup failed')

        def query(*args):
            started.set()
            raise recovery.mysql.connector.Error(
                errno=1290,
                msg='write blocked by capture admission barrier; retry after physical backup capture',
            )

        with patch.object(recovery, 'sql', query), \
                self.assertRaisesRegex(AssertionError, 'backup failed'):
            recovery.measure_while(None, operation)

    barriers = [
        'new writes are paused while physical backup drains active writers; retry',
        'write blocked by capture admission barrier; retry after physical backup capture',
    ]

    def test_concurrent_measurement_retries_temporary_admission_errors(self):
        for code, message in [(1205, 'capture admission wait timed out'),
                              *[(1290, message) for message in self.barriers]]:
            with self.subTest(code=code, message=message):
                release = threading.Event()

                def operation():
                    if not release.wait(timeout=5):
                        raise TimeoutError('concurrent write did not resume')
                    return 'backup completed'

                calls = []

                def write(connection, statement):
                    calls.append(statement)
                    if len(calls) == 1:
                        raise recovery.mysql.connector.DatabaseError(message, errno=code)
                    release.set()

                try:
                    with patch.object(recovery, 'sql', side_effect=write):
                        metrics, result = recovery.measure_while(None, operation)
                finally:
                    release.set()
                self.assertEqual(result, 'backup completed')
                self.assertEqual(metrics['admission_retries'], 1)
                self.assertGreaterEqual(metrics['transactions'], 1)
                self.assertEqual(calls[0], calls[1])

    def test_unrelated_write_errors_propagate_after_operation_finishes(self):
        for code, message in [(1290, 'server is running with --read-only'),
                              (1290, 'unrelated capture failure'),
                              (1064, self.barriers[1])]:
            with self.subTest(code=code, message=message):
                release = threading.Event()
                finished = threading.Event()
                error = recovery.mysql.connector.DatabaseError(message, errno=code)

                def operation():
                    if not release.wait(timeout=5):
                        raise TimeoutError('write error was not observed')
                    finished.set()

                def write(connection, statement):
                    release.set()
                    raise error

                with patch.object(recovery, 'sql', side_effect=write), \
                        self.assertRaises(recovery.mysql.connector.Error) as caught:
                    recovery.measure_while(None, operation)
                self.assertIs(caught.exception, error)
                self.assertTrue(finished.is_set())


def frame(lsn, txn, kind, payload, source_time):
    header = struct.pack('<IIQQQHHQ', 0x4d57414c, len(payload), lsn, 0,
                         txn, kind, 0, source_time)
    return header + struct.pack('<I', zlib.crc32(header + payload)) + payload


def compressed_batch(count, payload, source_time):
    # A valid literal-only raw LZ4 block, without a third-party dependency.
    length = len(payload)
    block = bytes([min(length, 15) << 4])
    if length >= 15:
        extra = length - 15
        block += bytes([255]) * (extra // 255) + bytes([extra % 255])
    return struct.pack('<IIQ', count | (1 << 31), length, source_time) + block + payload


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
        header = struct.pack('<8sIQQQ16sII', b'MYDBWAL1',
                             self.version if version is None else version,
                             0, 0, 0, bytes(16), 64, 0)
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

    def test_compressed_commit_keeps_logical_span_and_stamped_time(self):
        commit = struct.pack('<QQ', 10, 0)
        batch = struct.pack('<IHIHI', 2, 1, 0, 2, len(commit)) + commit
        payload = compressed_batch(2, batch, 601)
        self.write_wal(frame(102, 18, 24, payload, 601))
        self.assertEqual(recovery.source_commits_after(self.root, 100), {18: (102, 601)})

    def test_compressed_system_batch_is_not_commit_evidence(self):
        batch = struct.pack('<IHI', 1, 6, 0)
        self.write_wal(frame(101, 0, 24, compressed_batch(1, batch, 0), 601))
        self.assertEqual(recovery.source_commits_after(self.root, 100), {})

    def test_invalid_compressed_envelopes_are_rejected(self):
        for count, raw_size, source_time in [(0, 10, 601), (258, 10, 601),
                                             (1, 65537, 601), (1, 10, 602)]:
            with self.subTest(count=count, raw_size=raw_size, source_time=source_time):
                payload = struct.pack('<IIQ', count | (1 << 31), raw_size, 601) + b'\0'
                self.write_wal(frame(101, 18, 24, payload, source_time))
                with self.assertRaises(AssertionError):
                    recovery.source_commits_after(self.root, 100)

    def test_corrupt_segment_header_is_rejected(self):
        self.write_wal(frame(101, 17, 2, struct.pack('<QQ', 9, 501), 501))
        contents = bytearray(self.wal.read_bytes())
        contents[36] ^= 1
        self.wal.write_bytes(contents)
        with self.assertRaises(AssertionError):
            recovery.source_commits_after(self.root, 100)

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
