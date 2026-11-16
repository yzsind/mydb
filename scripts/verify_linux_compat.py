#!/usr/bin/env python3
"""Verify the same GNU/Linux release in isolated Linux Docker environments.

Requires Linux Docker and PyMySQL with cryptography. Every failed check fails the
matrix. Output directories must be new; databases are retained as evidence.
"""
import argparse
from decimal import Decimal
import hashlib
import json
import math
import os
from pathlib import Path
import re
import socket
import subprocess
import time
import uuid

import pymysql
from protocol_helpers import prepared
from build_linux_glibc217 import source_hash, verify_test_artifacts

ROOT = Path(__file__).resolve().parent.parent
IMAGES = {
    'centos7': 'quay.io/centos/centos@sha256:e4ca2ed0202e76be184e75fb26d14bf974193579039d5573fb2348664deef76e',
    'centos8': 'quay.io/centos/centos@sha256:96a58892e20a62e264ccf432da7007871a427103804d82cb5029df5499f1d82f',
    'stream8': 'quay.io/centos/centos@sha256:20da069d4f8126c4517ee563e6e723d4cbe79ff62f6c4597f753478af91a09a3',
    'centos9': 'quay.io/centos/centos@sha256:33346f5fe280763eb78e85f968ad52bc2b2013369de85403f67e802db98ed243',
    'stream10': 'quay.io/centos/centos@sha256:e664412637fcadb7d9eef7da6cc48c29cd69fb977d27c871ec3c54d95c91ace9',
}


def run(args, timeout=60):
    return subprocess.run(list(map(str, args)), capture_output=True, text=True, timeout=timeout)


def checked(args, timeout=60):
    result = run(args, timeout)
    if result.returncode:
        raise RuntimeError(f'{args}: exit {result.returncode}\n{result.stdout}{result.stderr}')
    return result.stdout.strip()


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def connect(port, tls=False):
    kwargs = {'ssl': {'check_hostname': False, 'verify_mode': False}} if tls else {}
    return pymysql.connect(host='127.0.0.1', port=port, user=os.environ.get('MYDB_TEST_USER','root'),
                           password=os.environ.get('MYDB_TEST_PASSWORD','root'),
                           database='test', charset='utf8mb4', autocommit=True,
                           connect_timeout=3, read_timeout=60, ssl_disabled=not tls, **kwargs)


def query(port, sql):
    with connect(port) as connection, connection.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchall()


def expect(port, sql, rows):
    actual = query(port, sql)
    if actual != rows:
        raise AssertionError(f'{sql}: expected {rows!r}, got {actual!r}')


def backup_base(port, repository):
    request = query(port, f"BACKUP DATABASE TO 'file://{repository}'")
    job_id = request[0][0]
    for _ in range(360):
        jobs = query(port, 'SHOW MYDB BACKUP JOBS')
        job = next((row for row in jobs if row[0] == job_id), None)
        if job is None:
            raise RuntimeError(f'backup job {job_id} disappeared: {jobs!r}')
        if job[2] == 'COMPLETED':
            if not job[4]:
                raise RuntimeError(f'backup completed without a base: {job!r}')
            return job
        if job[2] != 'RUNNING':
            raise RuntimeError('backup failed: ' + repr(job))
        time.sleep(0.25)
    raise RuntimeError('backup timed out: ' + repr(job))


class Case:
    def __init__(self, args, label, image):
        self.args, self.image = args, image
        self.folder = args.output / label
        self.folder.mkdir()
        self.storage = self.folder / 'storage'
        self.storage.mkdir()
        self.prefix = 'mydb-compat-' + uuid.uuid4().hex[:12]
        self.names = []
        self.port = free_port()
        self.result = {'image': image, 'checks': {}, 'passed': False}
        self.volume = None
        if getattr(args, 'storage_mode', 'bind') == 'volume':
            self.volume = self.prefix + '-data'
            checked(['docker', 'volume', 'create', '--label', 'mydb.local-release=true', self.volume])
        self.result['storage_mode'] = 'volume' if self.volume else 'bind'

    def storage_mount(self):
        return ['-v', f'{self.volume or self.storage}:/tmp/mydb']

    def start(self, suffix, port, data='data', repository=None, base=None):
        name = self.prefix + '-' + suffix
        self.names.append(name)
        mounts = self.storage_mount()
        if repository:
            if self.volume:
                mounts.extend(['--mount', f'type=volume,src={self.volume},dst=/tmp/mydb/{repository},readonly,volume-subpath={repository}'])
            else:
                mounts.extend(['-v', f'{self.storage / repository}:/tmp/mydb/{repository}:ro'])
            startup = ['--backup-repository', f'/tmp/mydb/{repository}', '--base', base,
                       '--runtime-dir', f'/tmp/mydb/{suffix}-runtime']
        else:
            startup = ['--data-dir', f'/tmp/mydb/{data}']
        configured_timezone = getattr(self.args, 'timezone', None)
        timezone = ['-e', f'TZ={configured_timezone}'] if configured_timezone else []
        mode = getattr(self.args, 'network_mode', 'auto')
        if mode == 'auto':
            mode = 'publish' if os.sys.platform == 'darwin' else 'host'
        network = ['-p', f'127.0.0.1:{port}:{port}'] if mode == 'publish' else ['--network', 'host']
        bind = '0.0.0.0' if mode == 'publish' else '127.0.0.1'
        reserve = getattr(self.args, 'temp_disk_reserve_pct', None)
        budget = ['-e', f'MYDB_TEMP_DISK_RESERVE_PCT={reserve}'] if reserve is not None else []
        self.result['runtime_config'] = {'network': mode, 'temp_disk_reserve_pct': reserve,
                                         'timezone': configured_timezone}
        checked(['docker', 'run', '-d', '--name', name, *network, '--platform', 'linux/amd64',
                 '--user', '10001:10001', '--cpus', '2', '--memory', '1g',
                 '-v', f'{self.args.binary}:/opt/mydb/mydb:ro',
                 '-v', f'{self.args.backup_binary}:/opt/mydb/mydb-backup:ro',
                 '-v', f'{self.args.cli_binary}:/opt/mydb/mydb-cli:ro',
                 *mounts, *timezone, *budget,
                 '-e', 'MYDB_ROOT_PASSWORD=root',
                 '-e', 'MYDB_MEMORY_TARGET=256M', '-e', 'MYDB_DURABILITY=strict',
                 '-e', 'TMPDIR=/tmp/mydb', self.image, '/opt/mydb/mydb',
                 '--host', bind, '--port', str(port), *startup])
        if getattr(self.args, 'bootstrap_client_image', None):
            self.bootstrap_loopback(name, port)
        self.ready(name, port)
        return name

    def bootstrap_loopback(self, name, port):
        # Publishing a Docker port does not turn the bridge peer into a
        # loopback user. Keep root local and explicitly grant a test account.
        cli=['docker','run','--rm','-i','--pull','never','--network','container:'+name,
             '-e','MYSQL_PWD=root','--entrypoint','mysql',self.args.bootstrap_client_image,
             '-h127.0.0.1',f'-P{port}','-uroot','--ssl-mode=REQUIRED','--batch',
             '--skip-column-names','--connect-timeout=3']
        deadline=time.monotonic()+60
        while time.monotonic()<deadline:
            result=subprocess.run(cli,input="SELECT COUNT(*) FROM mysql.user WHERE User='release_matrix' AND Host='%';",
                                  capture_output=True,text=True,timeout=15)
            if result.returncode==0:break
            time.sleep(.2)
        else:raise RuntimeError('loopback account bootstrap: '+result.stderr)
        if result.stdout.strip()=='0':
            sql="CREATE USER 'release_matrix'@'%' IDENTIFIED BY 'matrix-password'; GRANT ALL PRIVILEGES ON *.* TO 'release_matrix'@'%' WITH GRANT OPTION; GRANT BACKUP_ADMIN,SYSTEM_VARIABLES_ADMIN ON *.* TO 'release_matrix'@'%';"
            created=subprocess.run(cli,input=sql,capture_output=True,text=True,timeout=30)
            if created.returncode:raise RuntimeError(created.stderr)
        else:assert result.stdout.strip()=='1',result.stdout
        self.result['checks']['local_root_and_explicit_remote_account']=True

    def ready(self, name, port):
        last = None
        for _ in range(120):
            try:
                expect(port, 'SELECT 1', ((1,),))
                return
            except Exception as error:
                last = error
                state = checked(['docker', 'inspect', '-f', '{{.State.Running}}', name])
                if state != 'true':
                    raise RuntimeError(checked(['docker', 'logs', name]) + '\n' + str(error))
                time.sleep(0.25)
        raise RuntimeError('readiness timeout: ' + str(last))

    def execute(self):
        info = json.loads(checked(['docker', 'image', 'inspect', self.image]))[0]
        self.result.update(image_id=info['Id'], image_digests=info['RepoDigests'],
                           architecture=info['Architecture'], image_os=info['Os'])
        loader = run(['docker', 'run', '--rm', '--platform', 'linux/amd64',
                      '-v', f'{self.args.binary}:/opt/mydb/mydb:ro', self.image,
                      'sh', '-ec',
                      'for release in /etc/os-release /etc/system-release /etc/neokylin-release /etc/redhat-release; do '
                      'if [ -r "$release" ]; then cat "$release"; break; fi; done; '
                      '(getconf GNU_LIBC_VERSION || ldd --version) 2>&1 || true; '
                      'ldd /opt/mydb/mydb 2>&1 || true; /opt/mydb/mydb --help'])
        (self.folder / 'loader.log').write_text(loader.stdout + loader.stderr)
        if loader.returncode:
            raise RuntimeError(loader.stdout + loader.stderr)
        self.result['checks']['loader'] = True
        checked(['docker', 'run', '--rm', *self.storage_mount(), self.image,
                 'sh', '-ec', 'chown 10001:10001 /tmp/mydb; chmod 0755 /tmp/mydb'])
        name = self.start('server', self.port)
        cli_output = checked(['docker', 'exec', name, '/opt/mydb/mydb-cli', '--no-config',
                              '-h127.0.0.1', f'-P{self.port}', '-uroot', '-proot', 'test', '-N',
                              '-e', 'SELECT 42'])
        if cli_output.strip() != '42':
            raise AssertionError('native CLI returned unexpected rows: ' + repr(cli_output))
        self.result['checks']['mydb_cli_tls_query'] = True
        self.result['version'] = repr(query(self.port, 'SELECT VERSION(),@@version_compile_machine'))
        query(self.port, 'CREATE TABLE compat(id BIGINT PRIMARY KEY,n DECIMAL(12,2),txt VARCHAR(100),j JSON,b VARBINARY(20),dt DATETIME(6));')
        query(self.port, 'CREATE INDEX idx_n ON compat(n)')
        query(self.port, "INSERT INTO compat VALUES(1,123.45,'中文数据库','{\"n\":7}',X'00FF','2026-10-12 12:34:56.123456'),(2,-8.25,'second','[1,2]',X'0102',NULL)")
        expect(self.port, "SELECT n,txt,JSON_EXTRACT(j,'$.n'),HEX(b),DATE_FORMAT(dt,'%Y-%m-%d %H:%i:%s.%f') FROM compat WHERE id=1",
               ((Decimal('123.45'), '中文数据库', '7', '00FF', '2026-10-12 12:34:56.123456'),))
        expect(self.port, 'SELECT COUNT(*),SUM(n) FROM compat', ((2, Decimal('115.20')),))
        expect(self.port, 'SELECT id FROM compat FORCE INDEX(idx_n) WHERE n>0', ((1,),))
        expect(self.port, 'SELECT a.id,b.id FROM compat a JOIN compat b ON a.id=b.id ORDER BY a.id', ((1,1),(2,2)))
        expect(self.port, 'SELECT id,ROW_NUMBER() OVER(ORDER BY id) FROM compat ORDER BY id', ((1,1),(2,2)))
        with connect(self.port) as connection, connection.cursor() as cursor:
            cursor.execute('BEGIN')
            cursor.execute('UPDATE compat SET n=999 WHERE id=1')
            cursor.execute('INSERT INTO compat(id,n) VALUES(999,999)')
            cursor.execute('ROLLBACK')
        expect(self.port, 'SELECT n FROM compat WHERE id=1', ((Decimal('123.45'),),))
        expect(self.port, 'SELECT COUNT(*) FROM compat WHERE id=999', ((0,),))
        self.result['checks']['types_indexes_join_window_transactions'] = True
        numeric = query(self.port, 'SELECT LN(2),LOG2(8),EXP(1),POW(2,10),SQRT(25)')[0]
        expected = (math.log(2),3.0,math.exp(1),1024.0,5.0)
        for actual, value in zip(numeric, expected):
            if not math.isclose(float(actual), value, rel_tol=1e-14, abs_tol=1e-14):
                raise AssertionError((numeric, expected))
        expect(self.port, "SELECT ST_Distance(ST_GeomFromText('POINT(0 0)'),ST_GeomFromText('POINT(3 4)'))", ((5.0,),))
        expect(self.port, "SELECT ST_AsText(ST_LineInterpolatePoint(ST_GeomFromText('LINESTRING(0 0,3 4)'),0.5))", (('POINT(1.5 2)',),))
        self.result['checks']['math_and_gis'] = True
        expect(self.port, "SELECT CAST(CONVERT_TZ('2024-01-15 12:00:00','+00:00','Asia/Shanghai') AS CHAR),"
               "CAST(CONVERT_TZ('2024-07-15 12:00:00','+00:00','Europe/Berlin') AS CHAR),"
               "CAST(CONVERT_TZ('2024-01-15 12:00:00','+00:00','America/New_York') AS CHAR)",
               (('2024-01-15 20:00:00', '2024-07-15 14:00:00', '2024-01-15 07:00:00'),))
        self.result['checks']['named_timezones'] = True
        query(self.port, 'CREATE TABLE vectors(id INT PRIMARY KEY,v VECTOR(3) NOT NULL)')
        query(self.port, "INSERT INTO vectors VALUES(1,'[0,0,0]'),(2,'[1,0,0]'),(3,'[3,4,0]')")
        query(self.port, 'CREATE INDEX idx_v ON vectors(v) USING HNSW')
        expect(self.port, "SELECT id FROM vectors ORDER BY DISTANCE(v,'[0,0,0]') LIMIT 3", ((1,),(2,),(3,)))
        query(self.port, 'CREATE TABLE documents(id INT PRIMARY KEY,body TEXT,FULLTEXT KEY ft(body))')
        query(self.port, "INSERT INTO documents VALUES(1,'database systems database tuning'),(2,'database design'),(3,'distributed consensus protocols')")
        expect(self.port, "SELECT id FROM documents WHERE MATCH(body) AGAINST('database') ORDER BY id", ((1,),(2,)))
        self.result['checks']['hnsw_and_fulltext'] = True
        with connect(self.port) as connection:
            wire = prepared(connection, 'SELECT CAST(1.25 AS DECIMAL(10,2)),CAST(7 AS UNSIGNED)')
            if wire['values'] != ['1.25',7]:
                raise AssertionError(wire)
        self.result['checks']['binary_prepared_protocol'] = True
        with connect(self.port, tls=True) as connection, connection.cursor() as cursor:
            cursor.execute("SHOW SESSION STATUS LIKE 'Ssl_cipher'")
            cipher = cursor.fetchone()[1]
            if not cipher:
                raise AssertionError('TLS was not negotiated')
        self.result['tls_cipher'] = cipher
        self.result['checks']['tls_and_authentication'] = True
        for script in ('verify_expression_protocol.py', 'verify_json_protocol.py'):
            command = [self.args.python, ROOT / 'scripts' / script, '--port', self.port]
            if script == 'verify_expression_protocol.py':
                command += ['--output', self.folder / 'expression-protocol.json']
            result = run(command, 180)
            (self.folder / (script + '.log')).write_text(result.stdout + result.stderr)
            if result.returncode:
                raise RuntimeError(script + ': ' + result.stdout + result.stderr)
        self.result['checks']['expression_json_protocol_regressions'] = True
        checked(['docker', 'stop', '-t', '60', name], 75)
        if checked(['docker', 'inspect', '-f', '{{.State.ExitCode}}', name]) != '0':
            raise AssertionError('graceful stop failed')
        checked(['docker', 'start', name])
        self.ready(name, self.port)
        expect(self.port, 'SELECT id,n FROM compat ORDER BY id', ((1,Decimal('123.45')),(2,Decimal('-8.25'))))
        expect(self.port, "SELECT id FROM vectors ORDER BY DISTANCE(v,'[0,0,0]') LIMIT 3", ((1,),(2,),(3,)))
        self.result['checks']['graceful_restart'] = True
        query(self.port, 'INSERT INTO compat(id,n) VALUES(3,77.77)')
        connection = connect(self.port)
        try:
            with connection.cursor() as cursor:
                cursor.execute('BEGIN')
                cursor.execute('INSERT INTO compat(id,n) VALUES(4,88.88)')
            checked(['docker', 'kill', '--signal', 'KILL', name])
        finally:
            connection.close()
        checked(['docker', 'start', name])
        self.ready(name, self.port)
        expect(self.port, 'SELECT id,n FROM compat ORDER BY id',
               ((1,Decimal('123.45')),(2,Decimal('-8.25')),(3,Decimal('77.77'))))
        query(self.port, 'UPDATE compat SET n=123.50 WHERE id=1')
        expect(self.port, 'SELECT n FROM compat WHERE id=1', ((Decimal('123.50'),),))
        self.result['checks']['crash_recovery_committed_and_uncommitted'] = True
        clone = self.backup(name)
        if self.args.performance:
            workloads = ('point_select,point_select_prep,oltp_read_write,update_index,'
                         'insert_only,range_scan,seq_scan,join_hash,math_scan,spatial_scan,'
                         'fulltext_search,vector_search')
            measured = run([self.args.python, ROOT / 'scripts-internal/perf_regression.py', 'run',
                            '--attach', f'127.0.0.1:{self.port}', '--binary', self.args.binary,
                            '--workloads', workloads, '--rows', '20000', '--scan-rows', '10000',
                            '--threads', '4', '--duration', '3', '--rounds', '2', '--warmup', '1',
                            '--label', self.prefix], 600)
            (self.folder / 'performance.log').write_text(measured.stdout + measured.stderr)
            if measured.returncode:
                raise RuntimeError('performance workload execution failed: ' + measured.stderr)
            paths = [line.split('result saved: ', 1)[1] for line in measured.stdout.splitlines()
                     if line.startswith('result saved: ')]
            if len(paths) != 1:
                raise RuntimeError('performance harness produced no unique result')
            self.result['performance'] = json.loads(Path(paths[0]).read_text())
            self.result['checks']['performance_workloads'] = True
        for process in (name, clone):
            checked(['docker', 'stop', '-t', '60', process], 75)
        if self.args.test_archive:
            self.nextest()
        self.result['passed'] = True

    def backup(self, source):
        # Compare full values and index queries against the captured source,
        # rather than treating equal row counts as proof of a valid restore.
        statements = ['SELECT * FROM compat ORDER BY id',
                      'SELECT * FROM vectors ORDER BY id',
                      'SELECT * FROM documents ORDER BY id',
                      'SELECT id FROM compat FORCE INDEX(idx_n) WHERE n>0 ORDER BY id',
                      "SELECT id FROM vectors ORDER BY DISTANCE(v,'[0,0,0]') LIMIT 3",
                      "SELECT id FROM documents WHERE MATCH(body) AGAINST('database') ORDER BY id"]
        expected = {sql: query(self.port, sql) for sql in statements}
        repository = '/tmp/mydb/snapshot'
        job = backup_base(self.port, repository)
        self.result['backup_job'] = repr(job)
        base = job[4]
        cli = ['docker', 'exec', source, '/opt/mydb/mydb-backup']
        checked([*cli, 'verify', '--repository', repository, '--base', base], 120)
        clone_port = free_port()
        clone = self.start('snapshot', clone_port, repository='snapshot', base=base)
        for sql, rows in expected.items():
            expect(clone_port, sql, rows)
        expect(clone_port, 'SELECT CAST(@@global.read_only AS UNSIGNED),CAST(@@global.super_read_only AS UNSIGNED)', ((1,1),))
        try:
            query(clone_port, 'INSERT INTO compat(id,n) VALUES(99,99)')
        except pymysql.Error as error:
            if error.args[0] != 1290:
                raise
        else:
            raise AssertionError('snapshot accepted a write')
        checked(['docker', 'stop', '-t', '60', clone], 75)
        logs = run(['docker', 'logs', clone])
        (self.folder / (clone + '.log')).write_text(logs.stdout + logs.stderr)
        checked(['docker', 'rm', clone])
        self.names.remove(clone)
        checked([*cli, 'restore', '--repository', repository, '--base', base,
                 '--data-dir', '/tmp/mydb/restored'], 120)
        checked([*cli, 'promote', '--data-dir', '/tmp/mydb/restored'], 120)
        clone = self.start('promoted', clone_port, data='restored')
        for sql, rows in expected.items():
            expect(clone_port, sql, rows)
        query(clone_port, 'INSERT INTO compat(id,n) VALUES(99,99)')
        expect(clone_port, 'SELECT id,n FROM compat WHERE id=99', ((99,Decimal('99.00')),))
        self.result['checks']['native_backup_verify_readonly_promote'] = True
        return clone

    def nextest(self):
        work = self.folder / 'test-data'
        work.mkdir()
        test_mount = ['-v', f'{work}:/tmp/mydb']
        if self.volume:
            checked(['docker', 'run', '--rm', *self.storage_mount(), self.image,
                     'sh', '-ec', 'mkdir -p /tmp/mydb/test-data'])
            test_mount = ['--mount', f'type=volume,src={self.volume},dst=/tmp/mydb,volume-subpath=test-data']
        checked(['docker', 'run', '--rm', *test_mount, self.image,
                 'sh', '-ec', 'chown 10001:10001 /tmp/mydb; chmod 0755 /tmp/mydb'])
        name = self.prefix + '-tests'
        self.names.append(name)
        threads = getattr(self.args, 'test_threads', 8)
        memory = getattr(self.args, 'test_memory', '32g')
        self.result['archived_test_config'] = {'threads': threads, 'memory': memory}
        command = ['docker', 'run', '--rm', '--name', name, '--platform', 'linux/amd64', '--user', '10001:10001',
                   '--cpus', str(min(threads, os.cpu_count() or 2)), '--memory', memory, *test_mount,
                   '-v', f'{self.args.build}:{self.args.build}:ro', '-v', f'{ROOT}:{ROOT}:ro',
                   '-v', f'{self.args.nextest}:/opt/cargo-nextest:ro', '-w', ROOT,
                   '-e', 'TMPDIR=/tmp/mydb', self.image, '/opt/cargo-nextest', 'nextest',
                   'run', '--archive-file', self.args.test_archive, '--workspace-remap', ROOT,
                   '--config-file', self.args.build / 'nextest-runtime.toml',
                   '--test-threads', str(threads), '--no-fail-fast']
        configured_timezone = getattr(self.args, 'timezone', None)
        if configured_timezone:
            command[2:2] = ['-e', f'TZ={configured_timezone}']
        reserve = getattr(self.args, 'temp_disk_reserve_pct', None)
        if reserve is not None:
            command[2:2] = ['-e', f'MYDB_TEMP_DISK_RESERVE_PCT={reserve}']
        with (self.folder / 'nextest.log').open('w') as log:
            print('Running full nextest suite in ' + self.image, flush=True)
            result = subprocess.run(list(map(str, command)), stdout=log, stderr=subprocess.STDOUT)
        if result.returncode:
            raise RuntimeError('nextest failed; see ' + str(self.folder / 'nextest.log'))
        self.result['checks']['full_nextest'] = True

    def close(self):
        def cleanup(args, timeout=60):
            try:
                result = run(args, timeout)
            except (OSError, subprocess.TimeoutExpired) as error:
                result = subprocess.CompletedProcess(args, 1, '', str(error))
            if result.returncode:
                self.result.setdefault('cleanup_errors', []).append(result.stderr)
                self.result['passed'] = False
            return result

        exported = not self.volume
        for name in self.names:
            cleanup(['docker', 'stop', '-t', '60', name], 75)
        for name in self.names:
            result = cleanup(['docker', 'logs', name])
            (self.folder / (name + '.log')).write_text(result.stdout + result.stderr)
            if not exported:
                # Copy after shutdown so the retained evidence is a stable
                # image of the Linux filesystem, not a shared-folder capture.
                copied = cleanup(['docker', 'cp', name + ':/tmp/mydb/.', self.storage], 180)
                if copied.returncode == 0:
                    exported = True
            cleanup(['docker', 'rm', name])
        if self.volume and exported:
            removed = cleanup(['docker', 'volume', 'rm', self.volume])
            if removed.returncode:
                self.result['retained_volume'] = self.volume
        elif self.volume:
            self.result['retained_volume'] = self.volume
        (self.folder / 'result.json').write_text(json.dumps(self.result, indent=2) + '\n')


def main():
    global ROOT
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build', type=Path, required=True, help='build_linux_glibc217.py output')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--source', type=Path, default=ROOT, help='immutable build input; its fingerprint must match the manifest')
    parser.add_argument('--timezone', help='explicit TZ for server and archived tests; defaults to the image environment')
    parser.add_argument('--network-mode', choices=['auto', 'host', 'publish'], default='auto',
                        help='auto publishes loopback ports on Docker Desktop/macOS')
    parser.add_argument('--storage-mode', choices=['bind', 'volume'], default='bind',
                        help='volume uses an owned Linux filesystem and exports evidence before cleanup')
    parser.add_argument('--temp-disk-reserve-pct', type=int,
                        help='explicit local test reserve (0..90); product default is unchanged')
    parser.add_argument('--bootstrap-client-image',
                        help='immutable local MySQL client image for loopback account initialization under published ports')
    parser.add_argument('--images', help='comma-separated matrix labels; defaults to all entries')
    parser.add_argument('--matrix', type=Path, help='JSON object mapping safe labels to digest-pinned references or immutable local image IDs')
    parser.add_argument('--python', default=os.sys.executable)
    parser.add_argument('--full-tests', action='store_true')
    parser.add_argument('--test-threads', type=int, default=8,
                        help='archived-test concurrency; reduce for a small local Docker VM')
    parser.add_argument('--test-memory', default='32g', help='archived-test container memory limit')
    parser.add_argument('--performance', action='store_true',
                        help='execute twelve bounded workloads using local scripts-internal/ tools')
    parser.add_argument('--nextest', type=Path, help='static nextest executable, defaults to BUILD/tools/cargo-nextest')
    args = parser.parse_args()
    if args.test_threads < 1:
        parser.error('--test-threads must be positive')
    if not re.fullmatch(r'[1-9][0-9]*(?:[bBkKmMgG])?', args.test_memory):
        parser.error('--test-memory must be a positive Docker memory limit')
    if args.temp_disk_reserve_pct is not None and not 0 <= args.temp_disk_reserve_pct <= 90:
        parser.error('--temp-disk-reserve-pct must be between 0 and 90')
    if args.bootstrap_client_image:
        if not re.fullmatch(r'(?:[^\s@]+@)?sha256:[0-9a-f]{64}',args.bootstrap_client_image):
            parser.error('--bootstrap-client-image must be immutable')
        os.environ['MYDB_TEST_USER']='release_matrix'
        os.environ['MYDB_TEST_PASSWORD']='matrix-password'
    ROOT = args.source.resolve()
    if args.performance and not (ROOT / 'scripts-internal/perf_regression.py').is_file():
        parser.error('--performance requires local scripts-internal/ tools; '
                     'omit it to run the public runtime compatibility checks')
    images = json.loads(args.matrix.read_text()) if args.matrix else IMAGES
    if not isinstance(images, dict) or not images:
        raise SystemExit('matrix must be a nonempty JSON object')
    for label, image in images.items():
        if not isinstance(label, str) or not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_.-]*', label):
            raise SystemExit('unsafe matrix label: ' + repr(label))
        if not isinstance(image, str) or not re.fullmatch(r'(?:[^\s@]+@)?sha256:[0-9a-f]{64}', image):
            raise SystemExit('matrix image must use a SHA256 digest or local image ID: ' + repr(image))
    labels = args.images.split(',') if args.images else list(images)
    if len(labels) != len(set(labels)) or any(label not in images for label in labels):
        raise SystemExit('matrix labels must be unique and present in the matrix')
    args.build = args.build.resolve()
    args.output = args.output.resolve()
    args.binary = args.build / 'bin/mydb'
    args.backup_binary = args.build / 'bin/mydb-backup'
    args.cli_binary = args.build / 'bin/mydb-cli'
    args.test_archive = args.build / 'tests.tar.zst' if args.full_tests else None
    args.nextest = (args.nextest or args.build / 'tools/cargo-nextest').resolve()
    manifest = json.loads((args.build / 'build-manifest.json').read_text())
    if source_hash(ROOT) != manifest['source_sha256']:
        raise SystemExit('verification source differs from the immutable build input')
    for name, digest in manifest['artifacts'].items():
        if hashlib.sha256((args.build / 'bin' / name).read_bytes()).hexdigest() != digest:
            raise SystemExit('artifact differs from the successful build manifest: ' + name)
    if args.full_tests:
        verify_test_artifacts(args.build, manifest, args.nextest)
    checked([os.sys.executable, ROOT / 'scripts/check_elf_abi.py', args.binary, args.backup_binary, args.cli_binary])
    args.output.mkdir(parents=True, exist_ok=False)
    results = {}
    for label in labels:
        case = Case(args, label, images[label])
        print('Verifying ' + label, flush=True)
        try:
            case.execute()
        except Exception as error:
            case.result['error'] = str(error)
            print(label + ': FAIL: ' + str(error), flush=True)
        finally:
            case.close()
        results[label] = case.result
        report = {'binary_sha256': hashlib.sha256(args.binary.read_bytes()).hexdigest(), 'results': results,
                  'passed': all(row['passed'] for row in results.values())}
        (args.output / 'results.json').write_text(json.dumps(report, indent=2) + '\n')
        print(label + ': ' + ('PASS' if case.result['passed'] else 'FAIL'), flush=True)
    return 0 if all(row['passed'] for row in results.values()) else 1


if __name__ == '__main__':
    raise SystemExit(main())
