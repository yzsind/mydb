#!/usr/bin/env python3
"""Deterministic, externally instrumented local release publication gates.

Requires the drivers used by test_release_storage.py. On macOS arm64 compile:
clang -dynamiclib -O2 -Wall -Wextra scripts/fixtures/publication_fault.c \
    scripts/fixtures/publication_fcntl_arm64.S -o /tmp/mydb/publication_fault.dylib
No product test hooks or timing-based crash selection are used.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
import json
import hashlib
import os
from pathlib import Path
import signal
import struct
import subprocess
import time
import zlib

import pymysql
from test_release_storage import StorageServers, start, query, binary_sha256


def wal_frames(root):
    frames = []
    for path in sorted((root / 'system/wal').glob('*.log')):
        raw = path.read_bytes()
        assert raw[:8] == b'MYDBWAL1', path
        offset = 40
        while offset + 48 <= len(raw):
            if raw[offset:offset + 4] == bytes(4):
                break
            magic, size, lsn, prev, txn, kind, reserved, timestamp, crc = struct.unpack_from('<IIQQQHHQI', raw, offset)
            assert magic == 0x4d57414c and reserved == 0, (path, offset)
            payload = raw[offset + 48:offset + 48 + size]
            assert len(payload) == size and zlib.crc32(raw[offset:offset + 44] + payload) == crc, (path, offset, 'CRC')
            frames.append({'lsn': lsn, 'txn': txn, 'kind': kind, 'file': path.name, 'payload': payload})
            offset += 48 + size
    return sorted(frames, key=lambda frame: frame['lsn'])


def heap_versions(root, count):
    matches = []
    for path in root.rglob('*.heap'):
        raw = path.read_bytes()
        rows = {}
        for page_id in range(len(raw) // 16384):
            page = raw[page_id * 16384:(page_id + 1) * 16384]
            if page[:4] != struct.pack('<I', 0x4d485032):
                continue
            assert zlib.crc32(page[:16] + bytes(4) + page[20:]) == struct.unpack_from('<I', page, 16)[0], (path, page_id, 'CRC')
            for slot in range(struct.unpack_from('<H', page, 20)[0]):
                offset, size, flags = struct.unpack_from('<HHB', page, 32 + slot * 5)
                if not size or flags & 3:
                    continue
                row = page[offset:offset + size]
                assert row[0] != 0xff, (path, page_id, slot, 'forwarded row')
                if row[1] & 2:
                    continue
                txn = struct.unpack_from('<Q', row, 2)[0] if row[1] & 4 else 0
                rows[f'{page_id}:{slot}'] = (row[1] & 3, txn)
        if len(rows) == count:
            matches.append((str(path.relative_to(root)), rows))
    assert len(matches) == 1, [(name, len(rows)) for name, rows in matches]
    return matches[0]


class Gate:
    def __init__(self, root, library):
        self.root = root
        self.marker = root / 'armed'
        self.hit = root / 'hit.json'
        self.extra = {'DYLD_INSERT_LIBRARIES': str(library),
                      'MYDB_PUBLICATION_ROOT': str((root / 'server').resolve()),
                      'MYDB_PUBLICATION_MARKER': str(self.marker),
                      'MYDB_PUBLICATION_HIT': str(self.hit),
                      'MYDB_PUBLICATION_TRACE': str(root / 'trace.jsonl'),
                      'MYDB_UNDO_PURGE_INTERVAL_MS': '3600000',
                      'MYDB_WAL_SYNC_METHOD': 'fullfsync',
                      'MYDB_WAL_WRITE_BUFFER_BYTES': '65536'}

    def arm(self, target, action):
        assert not self.marker.exists() and not self.hit.exists()
        self.marker.write_text(f'{target} {action}\n')

    def wait(self, process, future, paused):
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            if self.hit.exists():
                try:
                    hit = json.loads(self.hit.read_text())
                except json.JSONDecodeError:
                    time.sleep(0.01)
                    continue
                assert hit['pid'] == process.pid
                if paused:
                    state = subprocess.check_output(['ps', '-p', str(process.pid), '-o', 'state='], text=True)
                    if 'T' not in state:
                        time.sleep(0.01)
                        continue
                    assert not future.done(), 'SQL finished before deterministic pause'
                return hit
            if future.done():
                outcome = future.exception() or future.result()
                raise AssertionError(f'fault point not hit; SQL outcome={outcome!r}')
            if process.poll() is not None:
                raise AssertionError(f'process exited before hit: {process.returncode}')
            time.sleep(0.01)
        raise TimeoutError('fault point not reached')


def kill(process):
    process.kill()
    process.wait(timeout=30)
    assert process.returncode == -signal.SIGKILL, process.returncode


def setup(connection, count):
    query(connection, 'CREATE TABLE gate_rows(id INT PRIMARY KEY,v BIGINT,amount DECIMAL(20,4),payload LONGBLOB,UNIQUE KEY uv(v),KEY ka(amount))')
    expected = tuple((i, i, Decimal(f'{i}.1250'), bytes([i % 256]) * 18000 if i % 257 == 0 else None) for i in range(count))
    with connection.cursor() as cursor:
        cursor.executemany('INSERT INTO gate_rows VALUES(%s,%s,%s,%s)', expected)
    query(connection, 'ALTER SYSTEM CHECKPOINT')
    verify(connection, expected)
    return expected


def verify(connection, expected):
    assert query(connection, 'SELECT * FROM gate_rows ORDER BY id') == expected, 'complete row values differ'
    assert query(connection, 'SELECT id,v FROM gate_rows FORCE INDEX(uv) ORDER BY v') == tuple(sorted(((row[0], row[1]) for row in expected), key=lambda row: row[1])), 'unique index differs'
    sample = expected[len(expected) // 3]
    assert query(connection, f'SELECT id FROM gate_rows FORCE INDEX(ka) WHERE amount={sample[2]}') == ((sample[0],),), 'secondary index differs'


def sql_outcome(future):
    try:
        value = future.result(timeout=30)
        return {'success': True, 'value': value}
    except pymysql.MySQLError as error:
        return {'success': False, 'error': list(error.args)}


def rollback_case(binary, library, root, mode, action, result, segment_bytes=1048576):
    servers = StorageServers(binary, root)
    gate = Gate(root, library)
    pool = ThreadPoolExecutor(max_workers=1)
    try:
        extra = dict(gate.extra, MYDB_WAL_TXN_BUFFER='0', MYDB_WAL_SEGMENT_BYTES=str(segment_bytes))
        result['wal_segment_bytes'] = segment_bytes
        process, admin, port = start(servers, extra)
        expected = setup(admin, 9000)
        result['rows'] = len(expected)
        result['confirmed_values_sha256'] = hashlib.sha256(repr(expected).encode()).hexdigest()
        initial_heap = heap_versions(root / 'server', len(expected))
        writer = servers.connect(port)
        query(writer, "XA START 'publication_gate'" if mode == 'xa' else 'BEGIN')
        if mode == 'savepoint':
            query(writer, 'UPDATE gate_rows SET v=v+100000')
            query(writer, 'SAVEPOINT kept')
        query(writer, 'UPDATE gate_rows SET v=v+200000,amount=amount+11')
        query(writer, "UPDATE gate_rows SET v=v+300000,payload=IF(payload IS NULL,NULL,REPEAT('p',18000))")
        if mode == 'xa':
            query(writer, "XA END 'publication_gate'")
            query(writer, "XA PREPARE 'publication_gate'")
        query(admin, 'ALTER SYSTEM CHECKPOINT')
        before_lsn = max(frame['lsn'] for frame in wal_frames(root / 'server'))
        gate.arm('rollback', action)
        sql = "XA ROLLBACK 'publication_gate'" if mode == 'xa' else 'ROLLBACK TO SAVEPOINT kept' if mode == 'savepoint' else 'ROLLBACK'
        future = pool.submit(query, writer, sql)
        result['hit'] = gate.wait(process, future, True)
        frames = [frame for frame in wal_frames(root / 'server') if frame['lsn'] > before_lsn]
        progress = [frame for frame in frames if frame['kind'] == 32]
        assert len(progress) == 1, len(progress)
        assert struct.unpack_from('<Q', progress[0]['payload'])[0] != 0
        heap = [frame for frame in frames if frame['kind'] in (13,20) and frame['txn'] == 0]
        after = [frame for frame in heap if frame['lsn'] > progress[0]['lsn']]
        assert len(after) == (13 if action == 'inside' else 0), len(after)
        assert len(heap) == 4096 + len(after), len(heap)
        assert progress[0]['lsn'] == result['hit']['progress_lsn']
        progress_path = root / 'server/system/wal' / progress[0]['file']
        physical = progress_path.stat()
        result['progress_file_identity'] = {'file': progress[0]['file'], 'device': physical.st_dev, 'inode': physical.st_ino}
        if result['hit']['stage'] == 'after-progress-inode-fullsync':
            assert Path(result['hit']['path']).resolve() == progress_path.resolve()
        assert heap and not any(frame['kind'] == 4 and frame['txn'] == progress[0]['txn'] for frame in frames)
        result['wal_proof'] = {'frames': len(frames), 'compensation_heap_frames': len(heap), 'after_first_progress': len(after),
                               'rollback_txn': progress[0]['txn'], 'progress_lsn': progress[0]['lsn'], 'all_frame_crc_valid': True}
        (root / 'wal-proof.json').write_text(json.dumps([{key: value for key, value in frame.items() if key != 'payload'} for frame in frames]))
        kill(process)
        result['interrupted_sql'] = sql_outcome(future)
        assert not result['interrupted_sql']['success']
        for restart in range(2):
            process, admin, port = start(servers, extra)
            verify(admin, expected)
            assert not query(admin, 'XA RECOVER'), 'aborted XA was resurrected'
            query(admin, 'ALTER SYSTEM CHECKPOINT')
            assert heap_versions(root / 'server', len(expected)) == initial_heap, 'physical row versions were not restored'
            if restart == 0:
                kill(process)
        query(admin, "INSERT INTO gate_rows VALUES(10000,10000,10000.1250,X'00FF')")
        try:
            query(admin, "INSERT INTO gate_rows VALUES(10001,10000,10001.1250,NULL)")
        except pymysql.MySQLError as error:
            assert error.args[0] == 1062, error
        else:
            raise AssertionError('unique index accepted duplicate after recovery')
        result.update(two_restarts=True, complete_values_and_indexes=True, physical_versions=True, writes_and_unique_conflict=True)
    finally:
        gate.marker.unlink(missing_ok=True)
        for process in servers.processes:
            if process.poll() is None:
                # Also releases any worker stuck in a stopped syscall.
                kill(process)
        pool.shutdown(wait=True)
        servers.close()


def metadata_case(binary, library, root, target, action, result):
    servers = StorageServers(binary, root)
    gate = Gate(root, library)
    pool = ThreadPoolExecutor(max_workers=1)
    try:
        process, admin, port = start(servers, gate.extra)
        expected = setup(admin, 80)
        writer = servers.connect(port)
        if target == 'clock':
            query(writer, 'UPDATE gate_rows SET v=v+10000')
            expected = tuple((i,v+10000,a,p) for i,v,a,p in expected)
        else:
            query(writer, 'BEGIN')
            query(writer, 'DELETE FROM gate_rows WHERE id=17')
            query(writer, 'ROLLBACK')
        path = root / 'server/system' / ('commit-clock.v1' if target == 'clock' else 'undo/undo.manifest')
        old = path.read_bytes()
        gate.arm(target, action)
        future = pool.submit(query, admin, 'ALTER SYSTEM CHECKPOINT')
        paused = action in ('rename-before', 'rename-after')
        result['hit'] = gate.wait(process, future, paused)
        current = path.read_bytes()
        json.loads(current)
        result['published_bytes_changed_at_hit'] = current != old
        if paused:
            assert (current == old) == (action == 'rename-before')
            kill(process)
            result['sql'] = sql_outcome(future)
            assert not result['sql']['success']
        else:
            result['sql'] = sql_outcome(future)
            if action in ('write-short', 'observe'):
                assert result['sql']['success'], result['sql']
            else:
                expected_code = 3675 if target == 'clock' and action == 'write-enospc' else 1030
                assert not result['sql']['success'] and result['sql']['error'][0] == expected_code, result['sql']
            if action in ('write-eio','write-enospc','barrier-eio','rename-eio'):
                assert current == old, 'failed before rename but final metadata changed'
            if action == 'dirsync-eio':
                # Byte identity after rename must not suppress a failed
                # directory barrier on the next checkpoint.
                gate.hit.rename(root / 'first-hit.json')
                gate.arm(target, action)
                again = pool.submit(query, admin, 'ALTER SYSTEM CHECKPOINT')
                result['retry_hit'] = gate.wait(process, again, False)
                result['retry_sql'] = sql_outcome(again)
                assert not result['retry_sql']['success'] and result['retry_sql']['error'][0] == 1030
            if action == 'observe':
                trace = [json.loads(line) for line in (root / 'trace.jsonl').read_text().splitlines()]
                operations = [entry['op'] for entry in trace]
                assert all(entry['result'] >= 0 for entry in trace)
                assert operations == ['write', 'barrier', 'rename', 'dir-fullsync'], operations
                result['observed_publication_order'] = operations
            query(admin, 'ALTER SYSTEM CHECKPOINT')
            result['retry_success'] = True
            verify(admin, expected)
            kill(process)
        for restart in range(2):
            process, admin, port = start(servers, gate.extra)
            verify(admin, expected)
            assert not query(admin, 'XA RECOVER')
            query(admin, 'ALTER SYSTEM CHECKPOINT')
            json.loads(path.read_bytes())
            if restart == 0:
                kill(process)
        result.update(two_restarts=True, complete_values_and_indexes=True)
    finally:
        gate.marker.unlink(missing_ok=True)
        for process in servers.processes:
            if process.poll() is None:
                kill(process)
        pool.shutdown(wait=True)
        servers.close()


def ddl_case(binary, library, root, action, result):
    servers = StorageServers(binary, root)
    gate = Gate(root, library)
    pool = ThreadPoolExecutor(max_workers=1)
    sql = 'ALTER TABLE gate_rows ADD COLUMN extra INT DEFAULT 7, ALGORITHM=COPY'
    try:
        process, admin, port = start(servers, gate.extra)
        expected = setup(admin, 80)
        gate.arm('link', action)
        future = pool.submit(query, admin, sql)
        paused = action in ('link-before','link-after')
        result['hit'] = gate.wait(process, future, paused)
        if paused:
            kill(process)
        result['sql'] = sql_outcome(future)
        assert not result['sql']['success'], result['sql']
        if not paused:
            assert result['sql']['error'][0] == 1030, result['sql']
            verify(admin, expected)
            kill(process)
        for restart in range(2):
            process, admin, port = start(servers, gate.extra)
            verify(admin, expected)
            assert 'extra' not in [row[0] for row in query(admin, 'SHOW COLUMNS FROM gate_rows')], 'uncommitted DDL became visible'
            query(admin, 'ALTER SYSTEM CHECKPOINT')
            if restart == 0:
                kill(process)
        query(admin, sql)
        assert query(admin, 'SELECT * FROM gate_rows ORDER BY id') == tuple(row + (7,) for row in expected)
        assert query(admin, 'SELECT id,v FROM gate_rows FORCE INDEX(uv) ORDER BY v') == tuple((i,i) for i in range(80))
        query(admin, 'ALTER SYSTEM CHECKPOINT')
        kill(process)
        process, admin, port = start(servers, gate.extra)
        assert query(admin, 'SELECT * FROM gate_rows ORDER BY id') == tuple(row + (7,) for row in expected)
        result.update(two_loser_restarts=True, retry_committed_and_restarted=True, complete_values_and_indexes=True)
    finally:
        gate.marker.unlink(missing_ok=True)
        for process in servers.processes:
            if process.poll() is None:
                kill(process)
        pool.shutdown(wait=True)
        servers.close()


def gap_case(binary, library, root, action, result):
    servers = StorageServers(binary, root)
    gate = Gate(root, library)
    pool = ThreadPoolExecutor(max_workers=1)
    extra = dict(gate.extra, MYDB_UNDO_SEGMENT_BYTES='32768')
    try:
        process, admin, port = start(servers, extra)
        expected = setup(admin, 400)
        earlier = servers.connect(port)
        query(earlier, 'BEGIN')
        query(earlier, 'UPDATE gate_rows SET v=v+100000 WHERE id>=100 AND id<132')
        query(earlier, 'COMMIT')
        expected = tuple((i,v+100000 if 100 <= i < 132 else v,a,p) for i,v,a,p in expected)
        query(admin, 'ALTER SYSTEM CHECKPOINT')
        initial_heap = heap_versions(root / 'server', len(expected))
        later = servers.connect(port)
        query(later, "XA START 'gap_gate'")
        query(later, 'UPDATE gate_rows SET v=v+200000 WHERE id>=132')
        query(later, 'UPDATE gate_rows SET amount=amount+11 WHERE id>=132')
        query(later, "XA END 'gap_gate'")
        query(later, "XA PREPARE 'gap_gate'")
        frames = wal_frames(root / 'server')
        undo = [frame for frame in frames if frame['kind'] == 11]
        assert undo
        # Deliberately lose the owned undo tails; all retained prepared frames
        # must come from WAL. The checkpointed predecessor's WAL was reclaimed;
        # its current rows are committed and do not need those old undo images.
        kill(process)
        truncated = []
        for path in sorted((root / 'server/system/undo').glob('*.seg')):
            with path.open('r+b') as output:
                original = output.seek(0, 2)
                output.truncate(32)
                output.flush()
                os.fsync(output.fileno())
            truncated.append({'file': path.name, 'old_size': original})
        result['truncated_owned_segments'] = truncated
        result['durable_undo_frames'] = len(undo)
        gate.arm('recovery', action)
        future = pool.submit(start, servers, extra)
        # Startup must pause before accepting connections. Servers.start has
        # already registered the owned Popen before it waits for readiness.
        deadline = time.monotonic() + 10
        while len(servers.processes) < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert len(servers.processes) == 2
        process = servers.processes[-1]
        result['hit'] = gate.wait(process, future, True)
        manifest_path = root / 'server/system/undo/undo.manifest'
        manifest = json.loads(manifest_path.read_text())
        pending = manifest['pending_recovery_segments']
        assert len(pending) > 1, pending
        finals = sum((manifest_path.parent / f'undo-{segment:08x}.seg').exists() for segment in pending)
        staged = sum((manifest_path.parent / f'undo-{segment:08x}.seg.compacting').exists() for segment in pending)
        assert finals == (1 if action == 'rename-after' else 0), (finals, staged)
        assert finals + staged == len(pending)
        assert manifest['forwardings']
        result['intent_proof'] = {'pending': len(pending), 'renamed': finals, 'staged': staged, 'forwardings': len(manifest['forwardings'])}
        (root / 'paused-manifest.json').write_text(json.dumps(manifest, indent=2))
        kill(process)
        try:
            future.result(timeout=30)
        except AssertionError:
            pass
        else:
            raise AssertionError('paused startup unexpectedly completed')
        process, admin, port = start(servers, extra)
        verify(admin, expected)
        assert len(query(admin, 'XA RECOVER')) == 1
        assert not json.loads(manifest_path.read_text())['pending_recovery_segments']
        query(admin, "XA ROLLBACK 'gap_gate'")
        for restart in range(2):
            verify(admin, expected)
            assert not query(admin, 'XA RECOVER')
            query(admin, 'ALTER SYSTEM CHECKPOINT')
            assert heap_versions(root / 'server', len(expected)) == initial_heap
            kill(process)
            process, admin, port = start(servers, extra)
        verify(admin, expected)
        query(admin, 'UPDATE gate_rows SET v=v+1000000')
        query(admin, 'ALTER SYSTEM CHECKPOINT')
        kill(process)
        process, admin, port = start(servers, extra)
        verify(admin, tuple((i,v+1000000,a,p) for i,v,a,p in expected))
        result.update(pending_publication_finished=True, prepared_rollback=True, two_restarts=True,
                      physical_versions=True, complete_values_and_indexes=True, later_allocations_and_restart=True)
    finally:
        gate.marker.unlink(missing_ok=True)
        for process in servers.processes:
            if process.poll() is None:
                kill(process)
        pool.shutdown(wait=True)
        servers.close()


def device_case(binary, library, root, action, result):
    """Exercise actual release DDL across two local mounted filesystems."""
    servers = StorageServers(binary, root)
    gate = Gate(root, library)
    image = root / 'index-volume.sparseimage'
    mount = root / 'server/objects/indexes'
    mount.mkdir(parents=True)
    mounted = False
    try:
        with (root / 'hdiutil.log').open('wb') as log:
            subprocess.run(['hdiutil','create','-size','96m','-type','SPARSE','-fs','APFS',
                            '-volname','MydbPublicationGate','-o',str(image)], stdout=log, stderr=subprocess.STDOUT, check=True, timeout=60)
            subprocess.run(['hdiutil','attach','-nobrowse','-mountpoint',str(mount),str(image)],
                           stdout=log, stderr=subprocess.STDOUT, check=True, timeout=60)
        mounted = True
        devices = {str(path): path.stat().st_dev for path in (root / 'server', mount)}
        assert len(set(devices.values())) == 2, devices
        result['actual_devices'] = devices
        if action != 'observe':
            location, fault = action.split('-', 1)
            assert location in ('heap','index')
            device = (root / 'server' if location == 'heap' else mount).stat().st_dev
            gate.extra['MYDB_PUBLICATION_FAULT_DEVICE'] = str(device)
        process, admin, port = start(servers, gate.extra)
        expected = setup(admin, 80)
        if action != 'observe':
            gate.arm('link', fault)
            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(query, admin, 'ALTER TABLE gate_rows ADD COLUMN extra INT DEFAULT 7, ALGORITHM=COPY')
                result['hit'] = gate.wait(process, future, False)
                result['sql'] = sql_outcome(future)
            assert not result['sql']['success'] and result['sql']['error'][0] == 1030
            fault_path = Path(result['hit']['path'])
            # Failed COPY may already have removed its private generation.
            owner = next(path for path in (fault_path, *fault_path.parents) if path.exists())
            assert owner.stat().st_dev == device
            verify(admin, expected)
            kill(process)
            process, admin, port = start(servers, gate.extra)
            verify(admin, expected)
            result['failed_publication_recovered'] = True
            gate.hit.rename(root / 'failure-hit.json')
        gate.arm('link', 'batch-observe')
        query(admin, 'ALTER TABLE gate_rows ADD COLUMN extra INT DEFAULT 7, ALGORITHM=COPY')
        gate.marker.unlink()
        trace = [json.loads(line) for line in (root / 'trace.jsonl').read_text().splitlines()]
        assert all(entry['result'] == 0 for entry in trace), trace
        first_link = next(i for i, entry in enumerate(trace) if entry['op'] == 'link')
        prefix = trace[:first_link]
        # Every device carrying staged contents must have completed its own
        # full barrier before any namespace publication, including the switch
        # away from a device and the final batch representative.
        seen = {}
        for i, entry in enumerate(prefix):
            state = seen.setdefault(entry['device'], {})
            if entry['op'] in ('fsync', 'fullsync'):
                state[entry['op']] = i
        assert set(seen) == set(devices.values()), seen
        for device, state in seen.items():
            assert state.get('fullsync', -1) >= 0 and state['fullsync'] >= state.get('fsync', -1), (device, state)
        linked_devices = {entry['device'] for entry in trace if entry['op'] == 'link'}
        assert linked_devices == set(devices.values()), linked_devices
        for device in linked_devices:
            last_link = max(i for i, entry in enumerate(trace) if entry['op'] == 'link' and entry['device'] == device)
            assert any(i > last_link and entry['op'] == 'dir-fullsync' and entry['device'] == device for i, entry in enumerate(trace)), (device, 'directory barrier missing')
            last_fsync = max(i for i, entry in enumerate(trace) if entry['op'] == 'fsync' and entry['device'] == device)
            assert any(i > last_fsync and entry['op'] == 'dir-fullsync' and entry['device'] == device for i, entry in enumerate(trace)), (device, 'final device barrier missing')
        expected = tuple(row + (7,) for row in expected)
        verify(admin, expected)
        for restart in range(2):
            query(admin, 'ALTER SYSTEM CHECKPOINT')
            kill(process)
            process, admin, port = start(servers, gate.extra)
            verify(admin, expected)
        result.update(per_device_staging_barriers=True, per_device_directory_barriers=True, two_restarts=True,
                      complete_values_and_indexes=True, traced_syscalls=len(trace))
    finally:
        gate.marker.unlink(missing_ok=True)
        for process in servers.processes:
            if process.poll() is None:
                kill(process)
        servers.close()
        if mounted:
            with (root / 'hdiutil.log').open('ab') as log:
                subprocess.run(['hdiutil','detach',str(mount)], stdout=log, stderr=subprocess.STDOUT, check=True, timeout=60)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True, type=Path)
    parser.add_argument('--library', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    metadata_actions = ('write-eio','write-enospc','write-short','barrier-eio','fallback-fullsync-eio',
                        'rename-eio','dirsync-eio','rename-before','rename-after','observe')
    rollback_cases = [f'rollback-{mode}-{action}' for mode in ('plain','savepoint','xa') for action in ('boundary','inside')]
    publication_cases = [f'{target}-{action}' for target in ('clock','manifest') for action in metadata_actions]
    publication_cases += [f'ddl-{action}' for action in ('link-eio','link-before','link-after','fsync-eio',
                                                       'fsync-enospc','fullsync-eio','publish-dirsync-eio')]
    publication_cases += ['gap-rename-before','gap-rename-after']
    device_cases = ['device-observe','device-heap-fullsync-eio','device-index-fsync-eio',
                    'device-heap-publish-dirsync-eio','device-index-publish-dirsync-eio']
    parser.add_argument('--case', action='append', choices=rollback_cases + publication_cases + device_cases)
    parser.add_argument('--suite', choices=['all','rollback','publication','device'], default='all',
                        help='all excludes optional disk-image mount cases; device requires hdiutil')
    parser.add_argument('--wal-segment-bytes', type=int, default=1048576,
                        help='rollback WAL segment size; 67108864 exercises the boundary without rotation')
    args = parser.parse_args()
    binary, library, root = args.binary.resolve(), args.library.resolve(), args.output.resolve()
    if not root.is_relative_to(Path('/tmp/mydb').resolve()):
        parser.error('--output must be below /tmp/mydb')
    if not 65536 <= args.wal_segment_bytes <= 2**32:
        parser.error('--wal-segment-bytes must be between 65536 and 4294967296')
    root.mkdir(parents=True, exist_ok=False)
    cases = args.case or {'all': rollback_cases + publication_cases, 'rollback': rollback_cases,
                          'publication': publication_cases, 'device': device_cases}[args.suite]
    identity = {'binary_sha256': binary_sha256(binary), 'library_sha256': binary_sha256(library),
                'script_sha256': binary_sha256(Path(__file__).resolve()),
                'helper_sha256': binary_sha256(Path(__file__).with_name('test_release_storage.py'))}
    for case in cases:
        folder = root / case
        folder.mkdir()
        result = {**identity, 'case': case, 'passed': False}
        began = time.monotonic()
        try:
            if case.startswith('rollback-'):
                _, mode, action = case.split('-')
                assert mode in ('plain','savepoint','xa') and action in ('boundary','inside')
                rollback_case(binary, library, folder, mode, action, result, args.wal_segment_bytes)
            elif case.startswith('ddl-'):
                ddl_case(binary, library, folder, case.removeprefix('ddl-'), result)
            elif case.startswith('gap-'):
                gap_case(binary, library, folder, case.removeprefix('gap-'), result)
            elif case.startswith('device-'):
                device_case(binary, library, folder, case.removeprefix('device-'), result)
            else:
                target, action = case.split('-', 1)
                assert target in ('clock','manifest')
                metadata_case(binary, library, folder, target, action, result)
            result['passed'] = True
            print(f'PASS {case}', flush=True)
        except BaseException as error:
            result['error'] = repr(error)
            raise
        finally:
            result['seconds'] = time.monotonic() - began
            (folder / 'results.json').write_text(json.dumps(result, indent=2, default=str) + '\n')


if __name__ == '__main__':
    main()
