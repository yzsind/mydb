"""Executable negative controls for shell oracles and Python acceptance gates."""
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parent


class GateTests(unittest.TestCase):
    def setUp(self):
        Path('/tmp/mydb').mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir='/tmp/mydb')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def executable(self, name, source):
        path = self.root / name
        path.write_text(source)
        path.chmod(0o755)
        return path

    def test_postgres_errors_and_missing_timing_cannot_pass_or_replace_golden(self):
        self.executable('psql', '#!/bin/sh\ncase "$PG_FAKE" in\n'
            'fail) echo "query error" >&2; exit 1;;\n'
            'ok) echo "Time: 1.250 ms";;\n'
            'missing) exit 0;;\nesac\n')
        expected = self.root / 'expected'; expected.mkdir()
        (expected / 'q01.tsv').write_text('old canonical\n')
        for mode, run_pass, wanted in [('fail', 'both', 1), ('missing', 'perf', 1),
                                       ('ok', 'perf', 0)]:
            result = subprocess.run(['bash', str(ROOT / 'tpcds/pg/run_pg_expected.sh')],
                env=os.environ | dict(PGBIN=str(self.root), PG_FAKE=mode, TPCDS_PG_PASS=run_pass,
                TPCDS_QUERIES='1', TPCDS_PG_EXPECTED_DIR=str(expected),
                TPCDS_PG_RESULT_DIR=str(self.root / mode)), capture_output=True, text=True)
            self.assertEqual(result.returncode, wanted, result.stderr)
            self.assertEqual((expected / 'q01.tsv').read_text(), 'old canonical\n')
            csv = (self.root / mode / 'pg_query_times.csv').read_text()
            self.assertIn('1,ok,1.250' if wanted == 0 else '1,fail,-1', csv)

    def test_postgres_invalid_pass_and_empty_queries_fail_before_running(self):
        for settings in [dict(TPCDS_PG_PASS='typo'), dict(TPCDS_QUERIES=''),
                         dict(TPCDS_QUERIES='100'), dict(TPCDS_QUERIES='1 1')]:
            result = subprocess.run(['bash', str(ROOT / 'tpcds/pg/run_pg_expected.sh')],
                env=os.environ | dict(TPCDS_PG_RESULT_DIR=str(self.root / 'results'),
                TPCDS_PG_EXPECTED_DIR=str(self.root / 'expected')) | settings,
                capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)

    def test_rest_regression_gate_returns_failure_and_passing_controls_succeed(self):
        import bench_rest_mysql as bench
        binary = self.root / 'baseline'; candidate = self.root / 'candidate'; client = self.root / 'client'
        for path in [binary, candidate, client]:
            path.write_bytes(b'fixture')
        for regression in [False, True]:
            active = {}
            def popen(command, **kwargs):
                active['baseline'] = Path(command[0]) == binary.resolve()
                process = Mock(); process.poll.return_value = None; process.wait.return_value = 0
                return process
            def run(command, **kwargs):
                qps, p99 = (1, 1000) if regression and not active['baseline'] else (100, 10)
                return SimpleNamespace(stdout=f'QPS: {qps}\np99: {p99}\nerrors: 0\n', stderr='')
            args = ['bench', '--baseline', str(binary), '--candidate', str(candidate),
                    '--client', str(client), '--output', str(self.root / str(regression)),
                    '--rounds', '1', '--workload', 'short_select']
            with patch.object(sys, 'argv', args), patch.object(bench.subprocess, 'Popen', side_effect=popen), \
                 patch.object(bench.subprocess, 'run', side_effect=run), \
                 patch.object(bench.socket, 'create_connection'), contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(bench.main(), int(regression))
            self.assertEqual(json.loads((self.root / str(regression) / 'results.json').read_text())['gate_passed'],
                             not regression)

    def test_snapshot_auto_position_is_rejected_before_source_mutation(self):
        import setup_mysql_to_mydb_replica as setup
        with patch.object(sys, 'argv', ['setup', '--snapshot', '--auto-position']), \
             patch.object(setup.pymysql, 'connect') as connect, contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as error:
                setup.main()
            self.assertEqual(error.exception.code, 2)
            connect.assert_not_called()

    def test_snapshot_streams_hex_blob_dump_and_accepts_both_coordinate_spellings(self):
        import setup_mysql_to_mydb_replica as setup
        args = SimpleNamespace(work_dir=self.root, databases='db', source_host='127.0.0.1',
                               source_port=3306, mysqldump=Path('compatible-mysqldump'))
        for prefix, command in [('SOURCE', 'CHANGE REPLICATION SOURCE TO'), ('MASTER', 'CHANGE MASTER TO')]:
            dump = f"-- {command} {prefix}_LOG_FILE='bin.000012', {prefix}_LOG_POS=345;\nINSERT INTO t VALUES(0xFF);\n"
            def execute(argv, **kwargs):
                self.assertIn('--hex-blob', argv)
                self.assertIn('--skip-add-locks', argv)
                self.assertFalse(any('--password' in value for value in argv))
                self.assertEqual(kwargs['env']['MYSQL_PWD'], 'secret')
                kwargs['stdout'].write(dump.encode())
                return SimpleNamespace(returncode=0, stderr='')
            with patch.object(setup.subprocess, 'run', side_effect=execute), \
                 patch.object(setup, 'restore_filtered_dump') as restore:
                self.assertEqual(setup.run_snapshot(args, {'user': 'root', 'password': 'secret'}, '', 0),
                                 ('bin.000012', 345))
                self.assertIn('0xFF', restore.call_args.args[1].read_text())

    def test_snapshot_restores_session_semantics_and_quoted_data_instead_of_dropping_them(self):
        import setup_mysql_to_mydb_replica as setup
        dump = self.root / 'snapshot.sql'
        dump.write_text("/*!40103 SET TIME_ZONE='+00:00' */;\n"
                        "/*!40101 SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;\n"
                        "INSERT INTO t VALUES('line;\n/* literal */');\n")
        connection = Mock()
        connection.cursor.return_value.__enter__ = Mock(return_value=Mock())
        connection.cursor.return_value.__exit__ = Mock(return_value=False)
        with patch.object(setup, 'connect_mydb', return_value=connection), \
             contextlib.redirect_stdout(io.StringIO()):
            setup.restore_filtered_dump(SimpleNamespace(), dump)
        commands = [call.args[0] for call in connection.cursor.return_value.__enter__.return_value.execute.call_args_list]
        self.assertEqual(len(commands), 3)
        self.assertIn("SET TIME_ZONE='+00:00'", commands[0])
        self.assertIn('NO_AUTO_VALUE_ON_ZERO', commands[1])
        self.assertIn('line;\n/* literal */', commands[2])
        connection.close.assert_called_once()

    def test_snapshot_file_position_is_used_verbatim(self):
        import setup_mysql_to_mydb_replica as setup
        args = SimpleNamespace(source_host='127.0.0.1', source_port=3306, replica_server_id=123,
                               auto_position=False, databases=None)
        with patch.object(setup, 'connect_mydb'), patch.object(setup, 'query') as query, \
             contextlib.redirect_stdout(io.StringIO()):
            setup.configure_replication(args, 'repl', 'secret', 'bin.000012', 345, 'ON')
            sql = query.call_args.args[1]
            self.assertIn("SOURCE_LOG_FILE='bin.000012', SOURCE_LOG_POS=345", sql)
            self.assertNotIn('AUTO_POSITION=1', sql)

    def test_release_checks_survive_python_optimization(self):
        from release_archive import write_archive, package_prefix
        prefix = package_prefix('1.0.0', 'x86_64-unknown-linux-gnu')
        payload = self.root / 'payload'; payload.mkdir(); (payload / 'third-party').mkdir()
        contents = {}
        for name in ['mydb', 'mydb-cli', 'mydb-backup', 'README.md', 'LICENSE', 'third-party/packages.json']:
            path = payload / name; path.write_text('fixture'); contents[name] = path
        manifest = dict(schema_version=2, version='1.0.0', host='x86_64-unknown-linux-gnu',
            source_sha256='source', files={name: hashlib.sha256(path.read_bytes()).hexdigest()
                                         for name, path in contents.items()})
        for package, suffix, key in [('mydb', '.Cargo.lock', 'cargo_lock_sha256'),
                                    ('mydb-cli', '-cli.Cargo.lock', 'client_cargo_lock_sha256')]:
            path = self.root / ('mydb-1.0.0' + suffix)
            path.write_text(f'[[package]]\nname="{package}"\nversion="1.0.0"\n')
            manifest[key] = hashlib.sha256(path.read_bytes()).hexdigest()
        archive = self.root / (prefix + '.tar.gz'); write_archive(archive, prefix, contents)
        for optimized in [False, True]:
            for corrupt in [False, True]:
                data = json.loads(json.dumps(manifest))
                if corrupt:
                    data['files']['mydb'] = 'wrong-checksum'
                (self.root / (prefix + '.build-manifest.json')).write_text(json.dumps(data))
                command = [sys.executable] + (['-O'] if optimized else [])
                result = subprocess.run(command + [str(ROOT / 'verify_release_package.py'),
                    '--package', str(archive), '--files-only', '--output',
                    str(self.root / f'check-{optimized}-{corrupt}')], capture_output=True, text=True)
                self.assertEqual(result.returncode == 0, not corrupt, result.stderr)

    def fake_docker(self):
        self.executable('docker', '#!' + sys.executable + '\n' + r'''
import json, os, pathlib, sys
args = sys.argv[1:]
root = pathlib.Path(os.environ['FAKE_ROOT'])
with (root / 'docker.jsonl').open('a') as log:
    log.write(json.dumps(args) + '\n')
mode = os.environ.get('FAKE_MODE', 'ok')
if args[0] == 'run':
    if mode == 'conflict': sys.exit(125)
    label = args[args.index('--label') + 1].split('=', 1)[1]
    (root / 'owner').write_text(label)
    print('owned-container-id')
elif args[0] == 'inspect':
    print('someone-else' if mode == 'foreign' else (root / 'owner').read_text())
elif args[0] == 'exec':
    if '-e' in args:
        sys.exit(1 if mode == 'not-ready' else 0)
    if '-i' not in args: sys.exit(99)
    (root / 'stdin.sql').write_text(sys.stdin.read())
    if mode == 'sql-error': sys.exit(1)
''')
        self.executable('sleep', '#!/bin/sh\nexit 0\n')

    def test_docker_sql_stdin_ownership_and_failure_cleanup(self):
        self.fake_docker()
        for mode in ['ok', 'foreign', 'sql-error', 'not-ready', 'conflict']:
            folder = self.root / mode; folder.mkdir()
            result = subprocess.run(['bash', str(ROOT / 'run_mysql80_rollup_oracle.sh')],
                env=os.environ | dict(PATH=str(self.root) + os.pathsep + os.environ['PATH'],
                FAKE_ROOT=str(folder), FAKE_MODE=mode, MYSQL80_CONTAINER='preexisting-name',
                MYSQL_READY_ATTEMPTS='1'), capture_output=True, text=True)
            self.assertEqual(result.returncode == 0, mode in ('ok', 'foreign'), result.stderr)
            calls = [json.loads(line) for line in (folder / 'docker.jsonl').read_text().splitlines()]
            removals = [args for args in calls if args[0] == 'rm']
            self.assertEqual(bool(removals), mode in ('ok', 'sql-error', 'not-ready'))
            self.assertTrue(all(args[-1] == 'owned-container-id' for args in removals))
            if mode == 'ok':
                self.assertIn('WITH ROLLUP', (folder / 'stdin.sql').read_text())
                probes = [args for args in calls if args[0] == 'exec' and '-e' in args]
                self.assertTrue(probes)
                self.assertTrue(all('--protocol=TCP' in args and '--host=127.0.0.1' in args
                                    and '--port=3306' in args for args in probes))

    def test_custom_data_directories_are_never_deleted(self):
        folder = self.root / 'custom'; folder.mkdir()
        script = 'set -euo pipefail\nsource "$1"\nmydb_test_data_init\nmydb_test_data_cleanup\n'
        result = subprocess.run(['bash', '-c', script, 'test', str(ROOT / '_docker_test.sh')],
            env=os.environ | dict(MYDB_DATA_DIR=str(folder)), capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(folder.is_dir())
        (folder / 'sentinel').write_text('owned by caller')
        result = subprocess.run(['bash', '-c', script, 'test', str(ROOT / '_docker_test.sh')],
            env=os.environ | dict(MYDB_DATA_DIR=str(folder)), capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertTrue((folder / 'sentinel').is_file())


if __name__ == '__main__':
    unittest.main()
