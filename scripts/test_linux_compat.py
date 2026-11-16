#!/usr/bin/env python3
"""Validate runtime-matrix commands without launching Docker or a database."""
import unittest
from pathlib import Path
import tempfile
from types import SimpleNamespace
from unittest.mock import patch

import verify_linux_compat as compat


class TransportTests(unittest.TestCase):
    def test_plaintext_probe_cannot_silently_negotiate_tls(self):
        with patch.object(compat.pymysql, 'connect') as connect:
            compat.connect(4837, tls=False)
        self.assertIs(connect.call_args.kwargs.get('ssl_disabled'), True)

    def test_tls_probe_explicitly_enables_tls(self):
        with patch.object(compat.pymysql, 'connect') as connect:
            compat.connect(4837, tls=True)
        self.assertIs(connect.call_args.kwargs.get('ssl_disabled'), False)
        self.assertIsNotNone(connect.call_args.kwargs['ssl'])


class BackupJobTests(unittest.TestCase):
    def test_unrelated_completed_backup_does_not_finish_current_request(self):
        other = (1, 'FULL', 'COMPLETED', 'DONE', 'old-base')
        running = (2, 'FULL', 'RUNNING', 'COPY', None)
        completed = (2, 'FULL', 'COMPLETED', 'DONE', 'current-base')
        with patch.object(compat, 'query', side_effect=[((2,),), (other, running),
                                                      (other, completed)]), \
             patch.object(compat.time, 'sleep') as sleep:
            self.assertEqual(compat.backup_base(4837, '/tmp/mydb/repository'), completed)
        sleep.assert_called_once()

    def test_failed_or_missing_backup_is_not_reported_as_complete(self):
        for jobs in [(), ((2, 'FULL', 'FAILED', 'COPY', None),),
                     ((2, 'FULL', 'COMPLETED', 'DONE', None),)]:
            with self.subTest(jobs=jobs), \
                 patch.object(compat, 'query', side_effect=[((2,),), jobs]), \
                 self.assertRaises(RuntimeError):
                compat.backup_base(4837, '/tmp/mydb/repository')


class SnapshotStartupTests(unittest.TestCase):
    def setUp(self):
        Path('/tmp/mydb').mkdir(parents=True, exist_ok=True)

    def test_native_volume_snapshot_mount_is_read_only(self):
        with tempfile.TemporaryDirectory(dir='/tmp/mydb') as temporary:
            args = SimpleNamespace(output=Path(temporary), binary=Path('/tmp/mydb/server'),
                                   backup_binary=Path('/tmp/mydb/backup'), cli_binary=Path('/tmp/mydb/client'), storage_mode='volume')
            with patch.object(compat, 'checked') as checked:
                case = compat.Case(args, 'volume', 'fixture-image')
                with patch.object(case, 'ready'):
                    case.start('snapshot', 4837, repository='repository', base='pinned-base')
            command = checked.call_args.args[0]
            self.assertIn('/tmp/mydb/client:/opt/mydb/mydb-cli:ro', command)
            self.assertIn(f'{case.volume}:/tmp/mydb', command)
            self.assertIn(f'type=volume,src={case.volume},dst=/tmp/mydb/repository,readonly,volume-subpath=repository', command)
            self.assertEqual(case.result['storage_mode'], 'volume')

    def test_repository_start_has_readonly_mount_and_separate_runtime(self):
        with tempfile.TemporaryDirectory(dir='/tmp/mydb') as temporary:
            args = SimpleNamespace(output=Path(temporary), binary=Path('/tmp/mydb/server'),
                                   backup_binary=Path('/tmp/mydb/backup'), cli_binary=Path('/tmp/mydb/client'))
            case = compat.Case(args, 'startup', 'fixture-image')
            with patch.object(compat, 'checked') as checked, patch.object(case, 'ready'):
                case.start('snapshot', 4837, repository='repository', base='pinned-base')
            command = checked.call_args.args[0]
            self.assertNotIn('--data-dir', command)
            self.assertFalse(any(str(arg).startswith('MYDB_DATA_DIR=') for arg in command))
            self.assertIn(f'{case.storage}/repository:/tmp/mydb/repository:ro', command)
            self.assertEqual(command[command.index('--base') + 1], 'pinned-base')
            self.assertEqual(command[command.index('--runtime-dir') + 1],
                             '/tmp/mydb/snapshot-runtime')


if __name__ == '__main__':
    unittest.main()
