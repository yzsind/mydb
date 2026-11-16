#!/usr/bin/env python3
"""Local P0 lifecycle and semi-sync gates with mandatory scenario evidence."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from run_fault_matrix import ROOT, Servers, free_port, query, status, catch_up, kill, FaultProxy
from safety_evidence import write_json, source_identity, snapshot
import pymysql

if not __debug__:
    raise RuntimeError('Safety checks require Python without -O')


HEALTHY_ACK_TIMEOUT_MS = 10_000
FAULT_ACK_TIMEOUT_MS = 200
ACK_DELAY_SECONDS = 0.25


def expect_error(conn, sql, codes):
    try:
        query(conn, sql)
    except pymysql.MySQLError as error:
        assert error.args[0] in codes, (sql, error.args)
        return error.args[0]
    raise AssertionError(('expected explicit error', sql))


def lifecycle(binary, root, buffering, resolution):
    root.mkdir()
    servers = Servers(binary, root)
    env = {'MYDB_WAL_TXN_BUFFER': str(buffering), 'MYDB_UNDO_RETENTION_MS': '0',
           'MYDB_CHECKPOINT_INTERVAL_MS': '3600000', 'MYDB_UNDO_PURGE_INTERVAL_MS': '100'}
    result = {'status': 'incomplete', 'buffering': buffering, 'xa_resolution': resolution, 'checks': []}
    try:
        port = free_port()
        process, conn = servers.start('node', port, env)
        query(conn, 'CREATE TABLE ddl_t(id INT PRIMARY KEY,v INT,body LONGBLOB,UNIQUE KEY uv(v))')
        with conn.cursor() as cursor:
            cursor.executemany('INSERT INTO ddl_t VALUES(%s,%s,%s)', [(i, i, os.urandom(20000) if i % 8 == 0 else None) for i in range(64)])
        expected = query(conn, 'SELECT * FROM ddl_t ORDER BY id')
        query(conn, 'BEGIN')
        query(conn, 'UPDATE ddl_t SET v=v+1000')
        query(conn, 'ALTER TABLE ddl_t ADD COLUMN added INT NOT NULL DEFAULT 7')
        # Product DDL commits the pending transaction. ROLLBACK cannot undo it.
        query(conn, 'ROLLBACK')
        expected = tuple((i, v + 1000, body, 7) for i, v, body in expected)
        query(conn, 'ALTER TABLE ddl_t MODIFY COLUMN v BIGINT')
        query(conn, 'CREATE INDEX by_added ON ddl_t(added,id)')
        query(conn, 'ALTER TABLE ddl_t DROP COLUMN added')
        query(conn, 'RENAME TABLE ddl_t TO ddl_after')
        expected = tuple(row[:3] for row in expected)
        assert query(conn, 'SELECT * FROM ddl_after ORDER BY id') == expected
        result['checks'].append('ddl-add-modify-index-drop-rename-implicit-commit')
        query(conn, 'CREATE TABLE xa_t(id INT PRIMARY KEY,v INT,body LONGBLOB,KEY by_v(v))')
        query(conn, "INSERT INTO xa_t VALUES(1,10,X'00FF'),(2,20,NULL)")
        query(conn, "XA START 'p0_xa'")
        query(conn, 'UPDATE xa_t SET v=v+100,body=REPEAT(CHAR(id),25000)')
        query(conn, "XA END 'p0_xa'")
        query(conn, "XA PREPARE 'p0_xa'")
        query(conn, 'ALTER SYSTEM CHECKPOINT')
        for recovery in range(2):
            kill(process)
            process, conn = servers.start('node', port, env)
            assert query(conn, 'SELECT * FROM ddl_after ORDER BY id') == expected
            assert query(conn, 'SELECT id,v FROM ddl_after FORCE INDEX(uv) ORDER BY v') == tuple((i, i+1000) for i in range(64))
            assert query(conn, 'CHECK TABLE ddl_after')[-1][-1] == 'OK'
            if recovery == 0:
                recovered = query(conn, 'XA RECOVER')
                assert len(recovered) == 1 and b'p0_xa' in [bytes(row[-1]) if not isinstance(row[-1], str) else row[-1].encode() for row in recovered], recovered
                assert query(conn, 'SELECT id,v FROM xa_t ORDER BY id') == ((1,10),(2,20))
                query(conn, f"XA {resolution.upper()} 'p0_xa'")
                expect_error(conn, f"XA {resolution.upper()} 'p0_xa'", {1397})
            assert query(conn, 'XA RECOVER') == ()
            committed = resolution == 'commit'
            assert query(conn, 'SELECT id,v,OCTET_LENGTH(body) FROM xa_t ORDER BY id') == (
                (1,110,25000),(2,120,25000)) if committed else query(conn, 'SELECT id,v,HEX(body) FROM xa_t ORDER BY id') == ((1,10,'00FF'),(2,20,None))
            query(conn, 'ALTER SYSTEM CHECKPOINT')
            result['checks'].append(f'xa-{resolution}-recovery-{recovery}')
        query(conn, 'TRUNCATE TABLE ddl_after')
        assert query(conn, 'SELECT * FROM ddl_after') == ()
        query(conn, "INSERT INTO ddl_after VALUES(100,123,X'00FF')")
        kill(process)
        process, conn = servers.start('node', port, env)
        assert query(conn, 'SELECT * FROM ddl_after') == ((100,123,b'\x00\xff'),)
        result['checks'].append('truncate-reinsert-crash')
        result['status'] = 'pass'
    except BaseException as error:
        result.update(status='fail', error=repr(error))
        raise
    finally:
        write_json(root / 'results.json', result)
        for process in servers.processes:
            if process.poll() is None:
                kill(process)
        servers.close()


def wait_for_slot_ack(master, target, timeout=10):
    deadline = time.monotonic() + timeout
    current = None
    while time.monotonic() < deadline:
        current = status(master, 'SHOW MYDB MASTER STATUS')
        if int(current['Connected_Senders']) > 0 and int(current['Min_Slot_Acked_LSN']) >= target:
            return current
        time.sleep(0.01)
    raise TimeoutError(f'master ACK target={target}, status={current}')


def semi_sync(binary, root, buffering, healthy_delay=0):
    root.mkdir()
    servers = Servers(binary, root)
    proxy = None
    result = {'status': 'incomplete', 'checks': [], 'buffering': buffering,
              'healthy_delay_ms': healthy_delay * 1000,
              'healthy_timeout_ms': HEALTHY_ACK_TIMEOUT_MS,
              'fault_timeout_ms': FAULT_ACK_TIMEOUT_MS}
    env = {'MYDB_WAL_TXN_BUFFER': str(buffering), 'MYDB_REPL_TLS_MODE': 'disabled',
           'MYDB_REPL_BIND_HOST': '127.0.0.1', 'MYDB_REPL_HEARTBEAT_MS': '100',
           'MYDB_REPL_RECONNECT_MS': '100', 'MYDB_REPL_MODE': 'semi_sync',
           'MYDB_REPL_SEMI_SYNC_WAIT_COUNT': '1',
           # Healthy-link correctness uses the product's default wait budget.
           # The short timeout is tested separately after crash recovery.
           'MYDB_REPL_SEMI_SYNC_TIMEOUT_MS': str(HEALTHY_ACK_TIMEOUT_MS),
           'MYDB_REPL_BASE_BACKUP_DISK_RESERVE_PCT': '0'}
    try:
        mp, rp, native = free_port(), free_port(), free_port()
        master_env = dict(env, MYDB_ROLE='master', MYDB_REPL_PORT=str(native))
        pm, master = servers.start('primary', mp, master_env)
        query(master, 'CREATE TABLE sync_t(id INT PRIMARY KEY,v INT,KEY by_v(v))')
        proxy = FaultProxy(native)
        replica_env = dict(env, MYDB_ROLE='replica', MYDB_REPL_PORT=str(free_port()),
                           MYDB_REPL_MASTER_HOST='127.0.0.1', MYDB_REPL_MASTER_PORT=str(proxy.port))
        pr, replica = servers.start('replica', rp, replica_env)
        catch_up(master, replica)
        wait_for_slot_ack(master, int(status(master, 'SHOW MYDB MASTER STATUS')['Durable_LSN']))
        proxy.delay = healthy_delay
        before = status(master, 'SHOW MYDB MASTER STATUS')
        started = time.monotonic()
        query(master, 'INSERT INTO sync_t VALUES(1,10),(2,20)')
        elapsed_ms = (time.monotonic() - started) * 1000
        after = status(master, 'SHOW MYDB MASTER STATUS')
        result.update(healthy_before=before, healthy_after=after, healthy_elapsed_ms=elapsed_ms)
        assert int(after['Semi_Sync_Acked_Commits']) > int(before['Semi_Sync_Acked_Commits']), (before,after)
        assert int(after['Semi_Sync_Timeout_Commits']) == int(before['Semi_Sync_Timeout_Commits']), (before,after)
        assert int(after['Semi_Sync_Degraded_Commits']) == int(before['Semi_Sync_Degraded_Commits']), (before,after)
        if healthy_delay:
            assert elapsed_ms >= healthy_delay * 1000, elapsed_ms
            result['checks'].append('delayed-healthy-commit-is-acknowledged')
        target = int(after['Durable_LSN'])
        catch_up(master, replica)
        progress = status(replica, 'SHOW MYDB REPLICA STATUS')
        assert int(progress['Durable_Applied_LSN']) >= target
        proxy.delay = 0
        # The actual durable acknowledgement is distinguished from client OK.
        kill(pm)
        kill(pr)
        pm, master = servers.start('primary', mp, dict(master_env, MYDB_ROLE='auto',
            MYDB_REPL_SEMI_SYNC_TIMEOUT_MS=str(FAULT_ACK_TIMEOUT_MS)))
        pr, replica = servers.start('replica', rp, dict(replica_env, MYDB_ROLE='auto'))
        catch_up(master, replica)
        assert query(replica, 'SELECT * FROM sync_t ORDER BY id') == ((1,10),(2,20))
        result['checks'].append('acked-commit-both-nodes-crash-recover')
        wait_for_slot_ack(master, int(status(master, 'SHOW MYDB MASTER STATUS')['Durable_LSN']))
        # Keep the socket connected while delaying the round trip beyond the
        # short budget. A late ACK must advance the slot, not relabel the
        # already timed-out commit as synchronously acknowledged.
        proxy.delay = ACK_DELAY_SECONDS
        before = status(master, 'SHOW MYDB MASTER STATUS')
        query(master, 'INSERT INTO sync_t VALUES(3,30)')
        after = status(master, 'SHOW MYDB MASTER STATUS')
        result.update(timeout_before=before, timeout_after=after)
        assert int(after['Semi_Sync_Timeout_Commits']) > int(before['Semi_Sync_Timeout_Commits']), (before,after)
        assert int(after['Semi_Sync_Acked_Commits']) == int(before['Semi_Sync_Acked_Commits']), (before,after)
        assert int(after['Semi_Sync_Degraded_Commits']) == int(before['Semi_Sync_Degraded_Commits']), (before,after)
        catch_up(master, replica)
        late_ack = wait_for_slot_ack(master, int(after['Durable_LSN']))
        result['late_ack'] = late_ack
        assert int(late_ack['Semi_Sync_Acked_Commits']) == int(after['Semi_Sync_Acked_Commits']), (after,late_ack)
        assert query(replica, 'SELECT * FROM sync_t ORDER BY id') == ((1,10),(2,20),(3,30))
        proxy.delay = 0
        result['checks'].append('connected-timeout-late-ack-and-catch-up')
        before = status(master, 'SHOW MYDB MASTER STATUS')
        proxy.partition()
        query(master, 'INSERT INTO sync_t VALUES(4,40)')
        after = status(master, 'SHOW MYDB MASTER STATUS')
        result.update(partition_before=before, partition_after=after)
        assert int(after['Semi_Sync_Timeout_Commits']) + int(after['Semi_Sync_Degraded_Commits']) > int(before['Semi_Sync_Timeout_Commits']) + int(before['Semi_Sync_Degraded_Commits'])
        assert int(after['Semi_Sync_Acked_Commits']) == int(before['Semi_Sync_Acked_Commits'])
        proxy.heal()
        catch_up(master, replica)
        assert query(replica, 'SELECT * FROM sync_t ORDER BY id') == ((1,10),(2,20),(3,30),(4,40))
        result['checks'].append('async-degradation-visible-and-caught-up')
        result.update(status='pass', before=before, after=after, durable_progress=progress)
    except BaseException as error:
        result.update(status='fail', error=repr(error))
        raise
    finally:
        write_json(root / 'results.json', result)
        if proxy:
            proxy.close()
        servers.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--case', choices=['all','lifecycle','semi-sync'], default='all')
    args = parser.parse_args()
    args.binary, args.output = args.binary.resolve(), args.output.resolve()
    if not args.output.is_relative_to(Path('/tmp/mydb').resolve()) or not args.binary.is_file():
        parser.error('requires a built binary and new output below /tmp/mydb')
    args.output.mkdir(parents=True, exist_ok=False)
    sources = [ROOT/'tests/jepsen', ROOT/'scripts/test_native_readiness.py', ROOT/'scripts/safety_evidence.py']
    write_json(args.output/'manifest.json', dict(source_identity(ROOT, sources), binary_sha256=hashlib.sha256(args.binary.read_bytes()).hexdigest()))
    snapshot(ROOT, args.output, sources)
    plan = [(f'lifecycle-{b}-{r}', lifecycle, (b,r)) for b in (0,1) for r in ('commit','rollback') if args.case in ('all','lifecycle')]
    plan += [(f'semi-sync-{b}', semi_sync, (b,)) for b in (0,1) if args.case in ('all','semi-sync')]
    plan += [(f'semi-sync-delayed-{b}', semi_sync, (b,ACK_DELAY_SECONDS)) for b in (0,1) if args.case in ('all','semi-sync')]
    results = [{'name': name, 'status': 'incomplete'} for name,_,_ in plan]
    write_json(args.output/'results.json', results)
    for index,(name, fn, parameters) in enumerate(plan):
        try:
            fn(args.binary, args.output/name, *parameters)
            results[index]['status'] = 'pass'
        except Exception as error:
            results[index].update(status='fail', error=repr(error))
        write_json(args.output/'results.json', results)
        print(name, results[index], flush=True)
    return 0 if all(r['status'] == 'pass' for r in results) else 1


if __name__ == '__main__':
    raise SystemExit(main())
