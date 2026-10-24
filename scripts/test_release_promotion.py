#!/usr/bin/env python3
"""Local release native-replica promotion after a deterministic WAL prefix.

The loopback proxy preserves actual sender WAL bytes, splitting only the
protocol envelope at a complete frame. No product hooks or synthetic WAL.
Requires the drivers used by test_release_storage.py.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import socket
import struct
import threading
import time
import zlib

import pymysql
from test_native_readiness import free_port, query, status
from test_release_storage import StorageServers, binary_sha256
from test_release_publication import Gate, heap_versions, kill, setup, sql_outcome, verify, wal_frames


def decode_wal(raw):
    offset = 0
    frames = []
    while offset < len(raw):
        assert len(raw) - offset >= 48, 'partial WAL header'
        magic, size, lsn, prev, txn, kind, reserved, timestamp, crc = struct.unpack_from('<IIQQQHHQI', raw, offset)
        end = offset + 48 + size
        assert magic == 0x4d57414c and reserved == 0 and end <= len(raw)
        payload = raw[offset + 48:end]
        assert zlib.crc32(raw[offset:offset + 44] + payload) == crc, (lsn, 'WAL CRC')
        frames.append({'lsn': lsn, 'txn': txn, 'kind': kind, 'end': end, 'payload': payload,
                       'raw': raw[offset:end]})
        offset = end
    return frames


class PrefixProxy:
    def __init__(self, upstream, root):
        self.upstream = upstream
        self.root = root
        self.listener = socket.socket()
        self.listener.bind(('127.0.0.1', 0))
        self.listener.listen()
        self.listener.settimeout(0.2)
        self.port = self.listener.getsockname()[1]
        self.stop = threading.Event()
        self.hit = threading.Event()
        self.lock = threading.Lock()
        self.sockets = []
        self.threads = []
        self.errors = []
        self.phase = None
        self.txn = None
        self.progress = 0
        self.heap = 0
        self.after = 0
        self.proof = None
        self.forwarded = []
        self.stream = (root / 'forwarded-wal.bin').open('wb')
        self.thread = threading.Thread(target=self.accept, daemon=True)
        self.thread.start()

    def arm(self, phase):
        with self.lock:
            assert self.phase is None
            self.phase = phase

    def exact(self, sock, size):
        output = bytearray()
        while len(output) < size and not self.stop.is_set():
            try:
                chunk = sock.recv(size - len(output))
            except socket.timeout:
                continue
            if not chunk:
                raise EOFError()
            output.extend(chunk)
        if len(output) != size:
            raise EOFError()
        return bytes(output)

    def accept(self):
        while not self.stop.is_set():
            try:
                client, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            worker = threading.Thread(target=self.connection, args=(client,), daemon=True)
            self.threads.append(worker)
            worker.start()

    def send(self, sock, data):
        # A busy receiver can backpressure a base-backup frame for longer
        # than the short cancellation poll interval. Resume at the exact
        # byte offset instead of injecting an unintended transport fault.
        pending = memoryview(data)
        while pending and not self.stop.is_set():
            try:
                sent = sock.send(pending)
            except socket.timeout:
                continue
            if sent == 0:
                raise EOFError()
            pending = pending[sent:]
        if pending:
            raise EOFError()

    def connection(self, client):
        upstream = None
        try:
            upstream = socket.create_connection(('127.0.0.1', self.upstream), timeout=5)
            for sock in (client, upstream):
                sock.settimeout(0.5)
                self.sockets.append(sock)
            acknowledgements = threading.Thread(target=self.copy_requests, args=(client, upstream), daemon=True)
            self.threads.append(acknowledgements)
            acknowledgements.start()
            while not self.stop.is_set():
                header = self.exact(upstream, 9)
                assert header[:4] == b'REPL', 'not native plaintext replication'
                kind, size = header[4], struct.unpack_from('<I', header, 5)[0]
                assert size <= 64 * 1024 * 1024
                payload = self.exact(upstream, size)
                trailer = self.exact(upstream, 4)
                assert zlib.crc32(header[4:] + payload) == struct.unpack('<I', trailer)[0], 'wire CRC'
                with self.lock:
                    armed = self.phase is not None
                if kind == 3 and armed:
                    wire, proof = self.filter_chunk(payload)
                    if wire:
                        self.send(client, wire)
                    if proof:
                        self.proof = proof
                        (self.root / 'cut.json').write_text(json.dumps(proof, indent=2) + '\n')
                        self.hit.set()
                        self.stop.wait()
                        return
                else:
                    self.send(client, header + payload + trailer)
        except (EOFError, ConnectionError, OSError):
            pass
        except BaseException as error:
            self.errors.append(repr(error))
            self.hit.set()
        finally:
            for sock in (client, upstream):
                if sock:
                    sock.close()

    def copy_requests(self, source, destination):
        try:
            while not self.stop.is_set():
                try:
                    chunk = source.recv(65536)
                except socket.timeout:
                    continue
                if not chunk:
                    return
                self.send(destination, chunk)
        except (EOFError, OSError):
            pass

    def filter_chunk(self, payload):
        start, end, count, length = struct.unpack_from('<QQII', payload)
        raw = payload[24:]
        assert length == len(raw)
        frames = decode_wal(raw)
        assert len(frames) == count and frames[-1]['lsn'] == end
        chosen = []
        cut = False
        for frame in frames:
            chosen.append(frame)
            if frame['kind'] == 3 and self.txn is None:
                self.txn = frame['txn']
                cut = self.phase == 'abort'
            if self.txn is not None:
                assert not (frame['kind'] == 4 and frame['txn'] == self.txn), 'passed target EndTxn without hitting cutoff'
                if frame['kind'] == 32 and frame['txn'] == self.txn:
                    assert struct.unpack_from('<Q', frame['payload'])[0] != 0
                    self.progress += 1
                    self.after = 0
                    cut = self.phase == 'progress' and self.progress == 1
                if frame['kind'] in (13,20) and frame['txn'] == 0:
                    self.heap += 1
                    if self.progress:
                        self.after += 1
                    cut = self.phase == 'inside' and self.progress == 1 and self.after == 13
            if cut:
                break
        prefix = raw[:chosen[-1]['end']]
        self.stream.write(prefix)
        self.stream.flush()
        self.forwarded.extend({key: value for key, value in frame.items() if key not in ('payload','end','raw')} for frame in chosen)
        envelope = struct.pack('<QQII', start, chosen[-1]['lsn'], len(chosen), len(prefix)) + prefix
        header = b'REPL' + struct.pack('<BI', 3, len(envelope))
        wire = header + envelope + struct.pack('<I', zlib.crc32(header[4:] + envelope))
        proof = None
        if cut:
            os.fsync(self.stream.fileno())
            proof = {'phase': self.phase, 'txn': self.txn, 'cut_lsn': chosen[-1]['lsn'],
                     'last_kind': chosen[-1]['kind'], 'progress_records': self.progress,
                     'compensation_heap_records': self.heap, 'heap_after_progress': self.after,
                     'forwarded_frames': len(self.forwarded), 'wal_and_wire_crc_valid': True,
                     'wal_sha256': binary_sha256(self.root / 'forwarded-wal.bin')}
            (self.root / 'forwarded-headers.json').write_text(json.dumps(self.forwarded, indent=2))
        return wire, proof

    def close(self):
        self.stop.set()
        self.listener.close()
        for sock in self.sockets:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()
        self.thread.join(timeout=5)
        for worker in self.threads:
            worker.join(timeout=5)
        assert not self.thread.is_alive() and not any(worker.is_alive() for worker in self.threads)
        self.stream.close()


def wait_applied(connection, target, exact=False):
    deadline = time.monotonic() + 90
    current = {}
    while time.monotonic() < deadline:
        current = status(connection, 'SHOW MYDB REPLICA STATUS')
        if int(current['Durable_Applied_LSN']) >= target:
            if exact:
                assert all(int(current[key]) == target for key in ('Received_LSN','Durable_LSN','Applied_LSN','Durable_Applied_LSN')), current
            return current
        assert not current['Resync_Required'], current
        time.sleep(0.05)
    raise TimeoutError(f'replica did not apply {target}: {current}')


def role_file(root):
    return json.loads((root / 'system/replication/role.json').read_text())


def local_wal_bytes(root, first_lsn, last_lsn=None):
    output = bytearray()
    for path in sorted((root / 'system/wal').glob('*.log')):
        raw = path.read_bytes()
        offset = 40
        while offset + 48 <= len(raw) and raw[offset:offset + 4] != bytes(4):
            length = 48 + struct.unpack_from('<I', raw, offset + 4)[0]
            lsn = struct.unpack_from('<Q', raw, offset + 8)[0]
            if lsn >= first_lsn and (last_lsn is None or lsn <= last_lsn):
                output.extend(raw[offset:offset + length])
            offset += length
    decode_wal(output)
    return output


def case(binary, root, phase, restart, buffered, prepared, result, crash=None, library=None, retain=None):
    servers = StorageServers(binary, root)
    proxy = None
    pool = ThreadPoolExecutor(max_workers=1)
    common = {'MYDB_CHECKPOINT_INTERVAL_MS':'3600000', 'MYDB_UNDO_RETENTION_MS':'3600000',
              'MYDB_UNDO_PURGE_INTERVAL_MS':'3600000', 'MYDB_WAL_TXN_BUFFER':'1' if buffered else '0',
              'MYDB_REPL_TLS_MODE':'disabled', 'MYDB_REPL_BIND_HOST':'127.0.0.1',
              'MYDB_REPL_HEARTBEAT_MS':'100', 'MYDB_REPL_CHUNK_BYTES':'65536',
              'MYDB_REPL_RECONNECT_MS':'100', 'MYDB_REPL_BASE_BACKUP_DISK_RESERVE_PCT':'1'}
    gate = Gate(root, library) if crash else None
    if gate:
        gate.extra['MYDB_PUBLICATION_ROOT'] = str((root / 'replica').resolve())
    try:
        master_port, replica_port, source_repl, target_repl = [free_port() for _ in range(4)]
        master_process, master = servers.start('master', master_port, dict(common, MYDB_ROLE='master', MYDB_REPL_PORT=str(source_repl)))
        expected = setup(master, 5000)
        result['confirmed_values_sha256'] = hashlib.sha256(repr(expected).encode()).hexdigest()
        proxy = PrefixProxy(source_repl, root)
        replica_env = dict(common, MYDB_ROLE='replica', MYDB_REPL_PORT=str(target_repl),
                           MYDB_REPL_MASTER_HOST='127.0.0.1', MYDB_REPL_MASTER_PORT=str(proxy.port))
        if gate:
            replica_env.update(gate.extra)
        replica_process, replica = servers.start('replica', replica_port, replica_env)
        target = int(status(master, 'SHOW MYDB MASTER STATUS')['Current_LSN'])
        result['baseline_status'] = wait_applied(replica, target)
        verify(replica, expected)
        query(replica, 'ALTER SYSTEM CHECKPOINT')
        initial_heap = heap_versions(root / 'replica', len(expected))
        writer = servers.connect(master_port)
        query(writer, "XA START 'promotion_gate'" if prepared else 'BEGIN')
        query(writer, 'UPDATE gate_rows SET v=v+200000,amount=amount+11')
        query(writer, "UPDATE gate_rows SET v=v+300000,payload=IF(payload IS NULL,NULL,REPEAT('p',18000))")
        if prepared:
            query(writer, "XA END 'promotion_gate'")
            query(writer, "XA PREPARE 'promotion_gate'")
        query(master, 'ALTER SYSTEM CHECKPOINT')
        target = int(status(master, 'SHOW MYDB MASTER STATUS')['Current_LSN'])
        result['forward_status'] = wait_applied(replica, target)
        verify(replica, expected)
        result['prepared_before_abort'] = len(query(replica, 'XA RECOVER'))
        if prepared:
            assert len(query(master, 'XA RECOVER')) == 1
            prepares = [frame for frame in wal_frames(root / 'replica') if frame['kind'] == 19]
            assert len(prepares) == 1, 'receiver did not retain source PREPARE WAL'
            result['prepare_wal'] = {key:value for key,value in prepares[0].items() if key != 'payload'}
        if retain:
            cut = {'txn':prepares[0]['txn'], 'cut_lsn':target}
        else:
            proxy.arm(phase)
            future = pool.submit(query, writer, "XA ROLLBACK 'promotion_gate'" if prepared else 'ROLLBACK')
            assert proxy.hit.wait(timeout=90), 'cutoff not hit'
            assert not proxy.errors, proxy.errors
            cut = proxy.proof
            assert cut is not None
            result['cut_status'] = wait_applied(replica, cut['cut_lsn'], exact=True)
            forwarded = decode_wal((root / 'forwarded-wal.bin').read_bytes())
            assert not any(frame['kind'] == 4 and frame['txn'] == cut['txn'] for frame in forwarded)
            assert cut['compensation_heap_records'] == {'abort':0,'progress':4096,'inside':4109}[phase], cut
            assert cut['progress_records'] == int(phase != 'abort')
            received = wal_frames(root / 'replica')
            local = [frame for frame in received if frame['lsn'] >= forwarded[0]['lsn']]
            assert [(frame['lsn'],frame['txn'],frame['kind'],frame['payload']) for frame in local] == [(frame['lsn'],frame['txn'],frame['kind'],frame['payload']) for frame in forwarded]
            assert local_wal_bytes(root / 'replica', forwarded[0]['lsn']) == (root / 'forwarded-wal.bin').read_bytes()
            result['local_wal_matches_transmitted_prefix'] = True
            future.result(timeout=120)
            # Source rollback completion must never leak past the tested prefix.
            source = wal_frames(root / 'master')
            assert any(frame['kind'] == 4 and frame['txn'] == cut['txn'] and frame['lsn'] > cut['cut_lsn'] for frame in source)
            assert local_wal_bytes(root / 'master', forwarded[0]['lsn'], cut['cut_lsn']) == (root / 'forwarded-wal.bin').read_bytes()
        result['cut'] = cut
        kill(master_process)
        result['old_master_sigkill'] = master_process.pid
        proxy.close()
        proxy = None
        result['old_source_fenced_before_promotion'] = True
        verify(replica, expected)
        if restart:
            query(replica, 'ALTER SYSTEM CHECKPOINT')
            kill(replica_process)
            replica_process, replica = servers.start('replica', replica_port, dict(replica_env, MYDB_ROLE='auto'))
            result['restart_status'] = status(replica, 'SHOW MYDB REPLICA STATUS')
            assert result['restart_status']['Role'] == 'replica'
            assert int(result['restart_status']['Durable_Applied_LSN']) == cut['cut_lsn']
            verify(replica, expected)
        result['pre_promotion_role'] = role_file(root / 'replica')
        if crash:
            gate.arm('promotion' if crash == 'intent' else 'rollback', 'intent-after' if crash == 'intent' else 'inside')
            promotion = pool.submit(query, replica, 'PROMOTE TO MASTER FORCE FENCED')
            result['promotion_hit'] = gate.wait(replica_process, promotion, True)
            pending = role_file(root / 'replica')
            result['interrupted_role'] = pending
            assert pending['role'] == 'replica' and pending['promotion_pending']
            assert pending['promotion_target_lsn'] == cut['cut_lsn']
            if crash == 'inside':
                local_tail = [frame for frame in wal_frames(root / 'replica') if frame['lsn'] > cut['cut_lsn']]
                progress = [frame for frame in local_tail if frame['kind'] == 32 and frame['txn'] == cut['txn']]
                assert len(progress) == 1
                after = [frame for frame in local_tail if frame['lsn'] > progress[0]['lsn'] and frame['kind'] in (13,20) and frame['txn'] == 0]
                assert len(after) == 13
                assert not any(frame['kind'] == 4 and frame['txn'] == cut['txn'] for frame in local_tail)
                result['promotion_wal_proof'] = {'progress_lsn':progress[0]['lsn'], 'heap_after_progress':len(after)}
            kill(replica_process)
            result['interrupted_promotion_sql'] = sql_outcome(promotion)
            assert not result['interrupted_promotion_sql']['success']
            replica_process, replica = servers.start('replica', replica_port, dict(replica_env, MYDB_ROLE='auto'))
            result['pending_promotion_completed_on_startup'] = True
        else:
            query(replica, 'PROMOTE TO MASTER FORCE FENCED')
        after = role_file(root / 'replica')
        result['promoted_role'] = after
        assert after['role'] == 'master' and not after['promotion_pending'], after
        assert after['timeline_uuid'] != result['pre_promotion_role']['timeline_uuid']
        assert after['fork_lsn'] == cut['cut_lsn'], after
        verify(replica, expected)
        if retain:
            result['prepared_after_promotion'] = query(replica, 'XA RECOVER')
            assert len(result['prepared_after_promotion']) == 1, 'promoted PREPARED branch is unavailable'
            blocked = servers.connect(replica_port)
            query(blocked, 'SET SESSION innodb_lock_wait_timeout=1')
            query(blocked, 'SET SESSION lock_wait_timeout=1')
            for sql in ('UPDATE gate_rows SET v=v+900000 WHERE id=7',
                        'ALTER TABLE gate_rows ADD COLUMN blocked_column INT'):
                try:
                    query(blocked, sql)
                except pymysql.MySQLError as error:
                    assert error.args[0] == 1205, (sql,error)
                else:
                    raise AssertionError(f'prepared lock was lost: {sql}')
            result['prepared_row_and_mdl_locks_preserved'] = True
            query(replica, f"XA {retain.upper()} 'promotion_gate'")
            if retain == 'commit':
                expected = tuple((i,v+500000,a+11,b'p'*18000 if p is not None else None) for i,v,a,p in expected)
            verify(replica, expected)
        assert not query(replica, 'XA RECOVER')
        query(replica, 'ALTER SYSTEM CHECKPOINT')
        settled_heap = heap_versions(root / 'replica', len(expected))
        if retain == 'commit':
            assert all(flags == 0 and txn == cut['txn'] for flags,txn in settled_heap[1].values())
        else:
            assert settled_heap == initial_heap, 'physical versions differ after promotion'
        for iteration in range(2):
            kill(replica_process)
            replica_process, replica = servers.start('replica', replica_port, dict(replica_env, MYDB_ROLE='auto'))
            verify(replica, expected)
            assert role_file(root / 'replica')['role'] == 'master'
            assert not query(replica, 'XA RECOVER')
            query(replica, 'ALTER SYSTEM CHECKPOINT')
            assert heap_versions(root / 'replica', len(expected)) == settled_heap
        query(replica, "INSERT INTO gate_rows VALUES(10000,10000,10000.1250,X'00FF')")
        try:
            query(replica, 'INSERT INTO gate_rows VALUES(10001,10000,1,NULL)')
        except pymysql.MySQLError as error:
            assert error.args[0] == 1062, error
        else:
            raise AssertionError('unique index accepted duplicate')
        query(replica, 'ALTER SYSTEM CHECKPOINT')
        kill(replica_process)
        replica_process, replica = servers.start('replica', replica_port, dict(replica_env, MYDB_ROLE='auto'))
        verify(replica, expected + ((10000,10000,Decimal('10000.1250'),b'\x00\xff'),))
        result.update(complete_values_and_indexes=True, physical_versions=True, two_promoted_restarts=True,
                      writes_and_unique_conflict=True, committed_write_survives_restart=True)
    finally:
        for process in servers.processes:
            if process.poll() is None:
                kill(process)
        if proxy:
            proxy.close()
        pool.shutdown(wait=True)
        servers.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--library', type=Path, help='macOS arm64 publication_fault library for promotion interruption cases')
    names = [f'{phase}-{restart}' for phase in ('abort','progress','inside') for restart in ('live','restart')]
    names += [f'{mode}-{restart}' for mode in ('buffered-inside','xa-inside') for restart in ('live','restart')]
    names += [f'retained-{finish}-{restart}' for finish in ('commit','rollback') for restart in ('live','restart')]
    parser.add_argument('--case', action='append', choices=names + ['promotion-intent','promotion-inside'])
    args = parser.parse_args()
    binary, root = args.binary.resolve(strict=True), args.output.resolve()
    if not root.is_relative_to(Path('/tmp/mydb').resolve()):
        parser.error('--output must be under /tmp/mydb')
    if any(name.startswith('promotion-') for name in args.case or []) and args.library is None:
        parser.error('promotion interruption cases require --library')
    root.mkdir(parents=True, exist_ok=False)
    identity = {'binary_sha256':binary_sha256(binary), 'script_sha256':binary_sha256(Path(__file__).resolve()),
                'storage_helper_sha256':binary_sha256(Path(__file__).with_name('test_release_storage.py')),
                'publication_helper_sha256':binary_sha256(Path(__file__).with_name('test_release_publication.py')),
                'readiness_helper_sha256':binary_sha256(Path(__file__).with_name('test_native_readiness.py')),
                'driver_version':importlib.metadata.version('pymysql'),
                'client_timeout_seconds':StorageServers.client_timeout}
    (root / 'harness.py').write_bytes(Path(__file__).read_bytes())
    for name in args.case or names:
        folder = root / name
        folder.mkdir()
        result = dict(identity, passed=False, case=name, rows=5000, buffered=name.startswith('buffered'), prepared=name.startswith(('xa','retained')))
        crash = name.removeprefix('promotion-') if name.startswith('promotion-') else None
        if crash:
            result['library_sha256'] = binary_sha256(args.library)
        began = time.monotonic()
        try:
            phase = 'inside' if result['buffered'] or result['prepared'] or crash else name.split('-')[0]
            case(binary, folder, phase, name.endswith('restart'), result['buffered'], result['prepared'], result, crash,
                 args.library.resolve() if args.library else None, name.split('-')[1] if name.startswith('retained') else None)
            result['passed'] = True
            print(f'PASS {name}', flush=True)
        except BaseException as error:
            result['error'] = repr(error)
            raise
        finally:
            result['seconds'] = time.monotonic() - began
            (folder / 'results.json').write_text(json.dumps(result, indent=2, default=str) + '\n')


if __name__ == '__main__':
    main()
