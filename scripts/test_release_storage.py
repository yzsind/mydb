#!/usr/bin/env python3
"""Local storage failure gates against an explicitly selected server binary."""
import argparse
from datetime import date, timedelta
from decimal import Decimal
import hashlib
import importlib.metadata
import json
from pathlib import Path
import time

import pymysql

from test_native_readiness import Servers, free_port, query


class StorageServers(Servers):
    client_timeout = 120

    def connect(self, port):
        # This is a functional recovery gate, not a 30-second debug-build SLA.
        connection = pymysql.connect(host='127.0.0.1', port=port, user='root', password='root',
                                     database='test', autocommit=True,
                                     read_timeout=self.client_timeout, write_timeout=self.client_timeout)
        self.connections.append(connection)
        return connection


def binary_sha256(binary):
    with binary.open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def result_identity(binary):
    return {'passed': False, 'binary_sha256': binary_sha256(binary),
            'script_sha256': binary_sha256(Path(__file__).resolve()),
            'client_timeout_seconds': StorageServers.client_timeout,
            'driver_version': importlib.metadata.version('pymysql')}


def start(servers, extra=None):
    port = free_port()
    process, connection = servers.start('server', port, {
        'MYDB_CHECKPOINT_INTERVAL_MS': '3600000',
        'MYDB_UNDO_RETENTION_MS': '3600000',
        **(extra or {}),
    })
    assert Path(query(connection, 'SELECT @@datadir')[0][0]).resolve() == (servers.root / 'server').resolve()
    return process, connection, port


def missing_undo(binary, root):
    servers = StorageServers(binary, root)
    result = result_identity(binary)
    renamed = []
    try:
        process, admin, port = start(servers)
        query(admin, 'CREATE TABLE durable_rows(id INT PRIMARY KEY,v BIGINT,amount DECIMAL(20,4),payload LONGBLOB,UNIQUE KEY uv(v))')
        with admin.cursor() as cursor:
            cursor.executemany('INSERT INTO durable_rows VALUES(%s,%s,%s,%s)',
                               [(i, i, f'{i}.1250', bytes([i]) * 20000 if i % 17 == 0 else None)
                                for i in range(64)])
        expected = query(admin, 'SELECT * FROM durable_rows ORDER BY id')
        query(admin, 'ALTER SYSTEM CHECKPOINT')
        writer = servers.connect(port)
        query(writer, 'BEGIN')
        query(writer, 'UPDATE durable_rows SET v=v+10000')
        # Hide only this owned process's segment paths; the containing
        # directory remains accessible. Restore every identity before cleanup.
        paths = sorted((root / 'server' / 'system' / 'undo').rglob('*.seg'))
        assert paths, 'no undo segments were generated'
        result['hidden_segments'] = len(paths)
        try:
            for path in paths:
                hidden = path.with_suffix('.hidden')
                path.rename(hidden)
                renamed.append((hidden, path))
            assert process.poll() is None
            try:
                query(admin, 'ALTER SYSTEM CHECKPOINT')
            except pymysql.MySQLError as error:
                result['checkpoint_error'] = list(error.args)
                assert error.args[0] == 1030, error
            else:
                raise AssertionError('checkpoint accepted missing live undo segments')
        finally:
            for hidden, path in reversed(renamed):
                hidden.rename(path)
            renamed.clear()
        query(admin, 'ALTER SYSTEM CHECKPOINT')
        query(writer, 'ROLLBACK')
        assert query(admin, 'SELECT * FROM durable_rows ORDER BY id') == expected
        assert query(admin, 'SELECT id,v FROM durable_rows FORCE INDEX(uv) ORDER BY v') == tuple((i, i) for i in range(64))
        query(admin, 'ALTER SYSTEM CHECKPOINT')
        result.update(retry_and_rollback=True, complete_values_and_unique_index=True, passed=True)
    except Exception as error:
        result['error'] = repr(error)
        raise
    finally:
        for hidden, path in reversed(renamed):
            hidden.rename(path)
        (root / 'results.json').write_text(json.dumps(result, indent=2, default=str) + '\n')
        servers.close()


def crash_recovery(binary, root, prepared=False):
    servers = StorageServers(binary, root)
    result = {**result_identity(binary), 'prepared': prepared, 'rows': 5000, 'wal_txn_buffer': False}
    try:
        process, admin, port = start(servers, {'MYDB_WAL_TXN_BUFFER': '0'})
        query(admin, 'CREATE TABLE durable_rows(id INT PRIMARY KEY,v BIGINT,amount DECIMAL(20,4),payload LONGBLOB,day DATE,UNIQUE KEY uv(v),KEY kad(amount,day))')
        expected = tuple((i, i, Decimal(f'{i}.1250'), bytes([i % 256]) * 20000 if i % 257 == 0 else None,
                          date(2024, 1, 1) + timedelta(days=i % 31)) for i in range(5000))
        with admin.cursor() as cursor:
            cursor.executemany('INSERT INTO durable_rows VALUES(%s,%s,%s,%s,%s)', expected)
        assert query(admin, 'SELECT * FROM durable_rows ORDER BY id') == expected
        writer = servers.connect(port)
        query(writer, 'BEGIN')
        query(writer, 'UPDATE durable_rows SET v=v+100000')
        query(writer, "UPDATE durable_rows SET payload=REPEAT('p',24000) WHERE payload IS NOT NULL")
        query(writer, 'SAVEPOINT kept_prefix')
        query(writer, 'UPDATE durable_rows SET v=v+200000,amount=amount+7')
        query(writer, 'DELETE FROM durable_rows WHERE id%500=0')
        query(writer, "INSERT INTO durable_rows VALUES(9000,9000,9.1250,REPEAT('x',25000),'2024-01-01')")
        query(writer, 'ROLLBACK TO SAVEPOINT kept_prefix')
        query(writer, 'COMMIT')
        expected = tuple((i, v + 100000, amount, b'p' * 24000 if payload is not None else None, day)
                         for i, v, amount, payload, day in expected)
        result['confirmed_commit_sha256'] = hashlib.sha256(repr(expected).encode()).hexdigest()
        assert query(admin, 'SELECT * FROM durable_rows ORDER BY id') == expected
        query(writer, "XA START 'storage_gate'" if prepared else 'BEGIN')
        query(writer, "UPDATE durable_rows SET v=v+300000,amount=amount+11,payload=REPEAT('z',25000)")
        if prepared:
            query(writer, "XA END 'storage_gate'")
            query(writer, "XA PREPARE 'storage_gate'")
        else:
            query(writer, 'SAVEPOINT loser_prefix')
            query(writer, 'UPDATE durable_rows SET v=v+10000000 WHERE id%2=0')
        # Make uncommitted heap/index/LOB effects reach files so restart must
        # actually recover them, rather than just discard an unflushed buffer.
        query(admin, 'ALTER SYSTEM CHECKPOINT')
        result['sigkills'] = []
        for restart in range(2):
            result['sigkills'].append(process.pid)
            process.kill()
            process.wait(timeout=30)
            process, admin, port = start(servers, {'MYDB_WAL_TXN_BUFFER': '0'})
            assert query(admin, 'SELECT * FROM durable_rows ORDER BY id') == expected
            assert query(admin, 'SELECT id,v FROM durable_rows FORCE INDEX(uv) ORDER BY v') == tuple((i, i + 100000) for i in range(5000))
            assert query(admin, "SELECT id FROM durable_rows FORCE INDEX(kad) WHERE amount=123.1250 AND day='2024-01-31'") == ((123,),)
            if prepared and restart == 0:
                assert len(query(admin, 'XA RECOVER')) == 1
                began = time.monotonic()
                query(admin, "XA ROLLBACK 'storage_gate'")
                result['xa_rollback_seconds'] = time.monotonic() - began
            assert not query(admin, 'XA RECOVER')
            query(admin, 'ALTER SYSTEM CHECKPOINT')
        query(admin, "INSERT INTO durable_rows VALUES(6000,6000,6.1250,X'00FF','2024-02-01')")
        assert query(admin, 'SELECT id,v,payload FROM durable_rows WHERE id=6000') == ((6000,6000,b'\x00\xff'),)
        result.update(repeated_recovery=True, complete_values_and_indexes=True, writes_after_recovery=True, passed=True)
    except Exception as error:
        result['error'] = repr(error)
        raise
    finally:
        (root / 'results.json').write_text(json.dumps(result, indent=2, default=str) + '\n')
        servers.close()


def io_failures(binary, root, library):
    servers = StorageServers(binary, root)
    marker = root / 'armed-fault'
    extra = {'DYLD_INSERT_LIBRARIES': str(library),
             'MYDB_STORAGE_GATE_ROOT': str((root / 'server/system/undo').resolve()),
             'MYDB_STORAGE_GATE_MARKER': str(marker),
             'MYDB_UNDO_PURGE_INTERVAL_MS': '3600000'}
    result = {**result_identity(binary),
              'fault_library_sha256': binary_sha256(library), 'cases': []}
    try:
        process, admin, port = start(servers, extra)
        query(admin, 'CREATE TABLE durable_rows(id INT PRIMARY KEY,v BIGINT,UNIQUE KEY uv(v))')
        with admin.cursor() as cursor:
            cursor.executemany('INSERT INTO durable_rows VALUES(%s,%s)', [(i,i) for i in range(64)])
        query(admin, 'ALTER SYSTEM CHECKPOINT')
        writer = servers.connect(port)
        for mode in ['fsync-eio','fsync-enospc','write-enospc','write-short']:
            query(writer, 'BEGIN')
            query(writer, 'UPDATE durable_rows SET v=v+10000')
            marker.write_text(mode)
            error_code = None
            error_detail = None
            try:
                query(admin, 'ALTER SYSTEM CHECKPOINT')
            except pymysql.MySQLError as error:
                error_code = error.args[0]
                error_detail = list(error.args)
            assert not marker.exists(), (mode, 'fault was not exercised')
            if mode == 'write-short':
                assert error_code is None, error_detail
            else:
                assert error_code == 1030, (mode, 'checkpoint swallowed injected I/O error', error_detail)
            query(admin, 'ALTER SYSTEM CHECKPOINT')
            query(writer, 'ROLLBACK')
            assert query(admin, 'SELECT id,v FROM durable_rows ORDER BY id') == tuple((i,i) for i in range(64))
            assert query(admin, 'SELECT id,v FROM durable_rows FORCE INDEX(uv) ORDER BY v') == tuple((i,i) for i in range(64))
            query(admin, 'ALTER SYSTEM CHECKPOINT')
            result['cases'].append({'mode': mode, 'error_code': error_code,
                                    'error_detail': error_detail, 'retry_passed': True})
        query(admin, 'UPDATE durable_rows SET v=v+100000')
        process.kill()
        process.wait(timeout=30)
        process, admin, port = start(servers, extra)
        assert query(admin, 'SELECT id,v FROM durable_rows ORDER BY id') == tuple((i,i+100000) for i in range(64))
        result.update(acknowledged_write_survives_sigkill=True, passed=True)
    except Exception as error:
        result['error'] = repr(error)
        raise
    finally:
        marker.unlink(missing_ok=True)
        (root / 'results.json').write_text(json.dumps(result, indent=2, default=str) + '\n')
        servers.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--case', action='append', choices=['missing-undo', 'crash', 'xa', 'io'])
    parser.add_argument('--fault-library', type=Path, help='local macOS syscall fault injector for the io case')
    args = parser.parse_args()
    root = args.output.resolve()
    if not root.is_relative_to(Path('/tmp/mydb').resolve()):
        parser.error('--output must be below /tmp/mydb')
    cases = args.case or ['missing-undo', 'crash', 'xa']
    if 'io' in cases and args.fault_library is None:
        parser.error('--case io requires --fault-library')
    root.mkdir(parents=True, exist_ok=False)
    for case in cases:
        folder = root / case
        folder.mkdir()
        if case == 'missing-undo':
            missing_undo(args.binary.resolve(), folder)
        elif case == 'io':
            io_failures(args.binary.resolve(), folder, args.fault_library.resolve())
        else:
            crash_recovery(args.binary.resolve(), folder, prepared=case == 'xa')
    print(json.dumps({'passed': True, 'output': str(root)}))


if __name__ == '__main__':
    main()
