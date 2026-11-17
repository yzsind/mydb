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

    def test_restore_checks_full_vector_values_without_unsupported_wire_decoding(self):
        Path('/tmp/mydb').mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir='/tmp/mydb') as temporary:
            args = SimpleNamespace(output=Path(temporary), backup_binary=Path('/tmp/mydb/backup'))
            case = compat.Case(args, 'vector-check', 'fixture-image')
            def query(port, sql):
                if sql == 'SELECT * FROM vectors ORDER BY id':
                    raise ValueError('unsupported VECTOR driver decoding')
                if 'VECTOR_TO_STRING' in sql:
                    return ((1, '[1,2,3]'),)
                return ()
            def compare(port, sql, expected):
                if 'VECTOR_TO_STRING' in sql:
                    self.assertEqual(expected, ((1, '[1,2,3]'),))
                    raise AssertionError('full vector values differ: [1,2,4]')
            with patch.object(compat, 'query', side_effect=query), \
                 patch.object(compat, 'expect', side_effect=compare), \
                 patch.object(compat, 'backup_base', return_value=(1,'FULL','COMPLETED','DONE','base')), \
                 patch.object(compat, 'checked'), patch.object(case, 'start', return_value='snapshot'):
                with self.assertRaisesRegex(AssertionError, 'full vector values differ'):
                    case.backup('source')


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




class ArchivedContainerLifecycleTests(unittest.TestCase):
    def test_archived_test_container_is_retained_until_strict_cleanup(self):
        import subprocess
        Path('/tmp/mydb').mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir='/tmp/mydb') as temporary:
            root = Path(temporary)
            args = SimpleNamespace(output=root, build=root/'build', nextest=root/'nextest',
                                   test_archive=root/'tests.tar.zst')
            case = compat.Case(args, 'lifecycle', 'fixture-image')
            case.result['passed'] = True
            existing = set()
            def docker(command, **kwargs):
                command = list(map(str, command))
                if command[:2] == ['docker', 'run'] and '--name' in command:
                    name = command[command.index('--name') + 1]
                    existing.add(name)
                    if '--rm' in command:
                        existing.remove(name)
                elif command[:2] in (['docker', 'stop'], ['docker', 'logs'], ['docker', 'rm']):
                    name = command[-1]
                    if name not in existing:
                        return subprocess.CompletedProcess(command, 1, '', 'No such container')
                    if command[1] == 'rm':
                        existing.remove(name)
                return subprocess.CompletedProcess(command, 0, '', '')
            with patch.object(compat.subprocess, 'run', side_effect=docker):
                case.nextest()
                case.close()
            self.assertTrue(case.result['passed'], case.result.get('cleanup_errors'))
            self.assertTrue(case.result['checks']['full_nextest'])
            self.assertEqual(existing, set())

    def test_unexpected_missing_owned_container_remains_a_cleanup_failure(self):
        import subprocess
        with tempfile.TemporaryDirectory(dir='/tmp/mydb') as temporary:
            case = compat.Case(SimpleNamespace(output=Path(temporary)), 'missing', 'fixture-image')
            case.result['passed'] = True
            case.names.append('owned-server')
            with patch.object(compat.subprocess, 'run', return_value=subprocess.CompletedProcess([], 1, '', 'No such container')):
                case.close()
            self.assertFalse(case.result['passed'])
            self.assertTrue(case.result['cleanup_errors'])


if __name__ == '__main__':
    unittest.main()
