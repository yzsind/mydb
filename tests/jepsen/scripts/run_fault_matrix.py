#!/usr/bin/env python3
"""Owned local concurrent-crash and native-cluster histories, checked by Elle.

No existing database, SSH host, or external fencing service is accessed.
The original acknowledged history is retained even for permitted asynchronous
loss after a deliberately lagging, externally fenced promotion.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from collections import Counter
import datetime
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import random
import shutil
import socket
import subprocess
import sys
import threading
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'scripts'))
from test_native_readiness import Servers, free_port, query, status, catch_up
from safety_evidence import write_json, source_identity, snapshot
import pymysql

if not __debug__:
    raise RuntimeError('Safety checks require Python without -O')

KEY_COUNT = 8
PROJECTION = "id,marker,CAST(amount AS CHAR),body,HEX(payload),stamp,JSON_EXTRACT(meta,'$.n'),VECTOR_TO_STRING(v)"


class Keyword(str):
    pass


def edn(value):
    if isinstance(value, Keyword):
        return ':' + value
    if value is None:
        return 'nil'
    if isinstance(value, bool):
        return 'true' if value else 'false'
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, dict):
        return '{' + ' '.join(':' + k.replace('_', '-') + ' ' + edn(v) for k, v in value.items()) + '}'
    return '[' + ' '.join(map(edn, value)) + ']'


def decode_list(text):
    if not isinstance(text, str) or (text and not text.endswith(',')):
        raise AssertionError(f'invalid append list: {text!r}')
    values = [] if not text else [int(v) for v in text[:-1].split(',')]
    assert all(v > 0 for v in values) and len(values) == len(set(values)), 'duplicate/invalid append token'
    return values


def validate_outcomes(transactions, actual, details, allowed_loss=(), expected_keys=None):
    """Independent acknowledged-state, all-or-nothing and typed-row oracle."""
    allowed = set(allowed_loss)
    if expected_keys is not None:
        assert set(actual) == set(expected_keys), 'incomplete readback'
    assert transactions, 'empty transaction history'
    actual_pairs = {(key, token) for key, values in actual.items() for token in values}
    assert len(actual_pairs) == sum(map(len, actual.values())), 'duplicate token'
    possible = set()
    expected_details = {}
    for tx in transactions:
        writes = set(tx['writes'])
        present = writes & actual_pairs
        assert not (writes & allowed) or writes <= allowed, 'partial transaction loss exemption'
        assert not tx.get('unexpected'), tx
        if tx['outcome'] == 'ok':
            assert writes <= actual_pairs | allowed, ('lost acknowledged writes', writes - actual_pairs - allowed)
        elif tx['outcome'] == 'fail':
            assert not present, ('aborted write survived', present)
        else:
            assert tx['outcome'] == 'info', tx
        assert not present or present == writes, ('partial transaction', tx['id'], present, writes)
        if tx['outcome'] in ('ok', 'info'):
            possible.update(writes)
        if present:
            expected_details[tx['id']] = tx['detail']
    assert actual_pairs <= possible, ('unexplained writes', actual_pairs - possible)
    assert allowed <= {p for tx in transactions if tx['outcome'] == 'ok' for p in tx['writes']}, 'invalid async-loss exemption'
    assert details == expected_details, ('typed/cross-table residue or lost rows', set(details) ^ set(expected_details))


class Ledger:
    def __init__(self, output, seed):
        self.output, self.seed = output, seed
        self.lock = threading.Lock()
        self.events, self.transactions, self.checks = [], [], []
        self.next_id = 1
        self.origin = time.monotonic_ns()
        self.epoch = 0
        self.loss_evidence = None
        self.resolutions = {}
        self.phases = []

    def record(self, event):
        with self.lock:
            self.events.append(dict(event, epoch=self.epoch, index=len(self.events), time=time.monotonic_ns() - self.origin))

    def plan(self, worker):
        with self.lock:
            token, self.next_id = self.next_id, self.next_id + 1
        rng = random.Random(self.seed * 1000003 + token)
        keys = sorted(rng.sample(range(KEY_COUNT), 2))
        payload = None if token % 7 == 0 else bytes([token % 256, 0, 255, 39, 92]) * (5000 if token % 8 == 0 else 5)
        body = None if token % 11 == 0 else f"界 Ω \0 ' record {token}"
        amount = Decimal(str(rng.randrange(-10**18, 10**18))).scaleb(-8)
        stamp = datetime.datetime(2040, 2, 3, 4, 5, 6, token % 1000000)
        vector = [token % 31, token % 7, 2, 3]
        marker = 'm' * 180 + f'{token:020d}'
        detail = [token, marker, f'{amount:.8f}', body, None if payload is None else payload.hex().upper(),
                  str(stamp), str(token), vector]
        return {'id': token, 'worker': worker, 'keys': keys, 'writes': [(keys[0], token * 2), (keys[1], token * 2 + 1)],
                'detail': detail, 'payload': payload, 'amount': amount, 'stamp': stamp}

    def transaction(self, servers, port, worker, abort=False, gate=None, stage='commit'):
        tx = self.plan(worker)
        tx.update(epoch=self.epoch, intentional_abort=abort)
        mops = [[Keyword('r'), tx['keys'][0], None],
                *[[Keyword('append'), key, token] for key, token in tx['writes']]]
        op = {'process': worker, 'f': Keyword('txn'), 'value': mops}
        self.record(dict(op, type=Keyword('invoke')))
        conn, committing = None, False
        observed = []
        try:
            conn = servers.connect(port)
            query(conn, 'SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED')
            query(conn, 'BEGIN')
            observed.append([Keyword('r'), tx['keys'][0], decode_list(query(conn, f"SELECT v FROM rc_append WHERE id={tx['keys'][0]}")[0][0])])
            for i, (key, token) in enumerate(tx['writes']):
                with conn.cursor() as cursor:
                    assert cursor.execute('UPDATE rc_append SET v=CONCAT(v,%s) WHERE id=%s', (str(token) + ',', key)) == 1
                observed.append([Keyword('append'), key, token])
                if gate and stage == 'partial' and i == 0:
                    gate()
            with conn.cursor() as cursor:
                cursor.execute("INSERT INTO fault_detail VALUES(%s,%s,%s,%s,%s,%s,%s,TO_VECTOR(%s))",
                               (tx['id'], tx['detail'][1], tx['amount'], tx['detail'][3], tx['payload'],
                                tx['stamp'], json.dumps({'n': tx['id']}), json.dumps(tx['detail'][-1])))
            if gate and stage == 'commit':
                gate()
            if abort:
                query(conn, 'ROLLBACK')
                tx['outcome'] = 'fail'
            else:
                committing = True
                query(conn, 'COMMIT')
                tx['outcome'] = 'ok'
            self.record(dict(op, type=Keyword(tx['outcome']), value=observed))
        except pymysql.MySQLError as error:
            rolled_back = False
            if conn:
                try:
                    query(conn, 'ROLLBACK')
                    rolled_back = True
                except pymysql.MySQLError:
                    pass
            tx['outcome'] = 'fail' if not committing and rolled_back else 'info'
            tx['unexpected'] = error.args[0] not in (1205, 1213, 2003, 2006, 2013)
            self.record(dict(op, type=Keyword(tx['outcome']), error=list(error.args),
                             unexpected_error=tx['unexpected']))
        finally:
            if conn:
                try:
                    conn.close()
                except pymysql.MySQLError:
                    pass
            with self.lock:
                self.transactions.append(tx)
        return tx

    def load(self, servers, port, workers, operations):
        start = len(self.transactions)
        def worker(number):
            return [self.transaction(servers, port, number, abort=i % 5 == 4) for i in range(operations)]
        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(worker, range(workers)))
        if operations < 5:
            self.transaction(servers, port, 0, abort=True)
        completed = self.transactions[start:]
        counts = dict(Counter(tx['outcome'] for tx in completed))
        assert counts.get('ok', 0) > 0, 'phase produced no confirmed writes/reads'
        assert any(tx['intentional_abort'] and tx['outcome'] == 'fail' for tx in completed), 'rollback path not exercised'
        assert not any(tx.get('unexpected') for tx in completed), 'unexpected SQL failure'
        self.phases.append({'epoch': self.epoch, 'transactions': len(completed), 'outcomes': counts})

    def begin_epoch(self, conn, allowed, evidence):
        assert self.epoch == 0 and allowed and evidence['fenced_exit_code'] == -9
        rows = query(conn, 'SELECT id,v FROM rc_append ORDER BY id')
        assert [row[0] for row in rows] == list(range(KEY_COUNT))
        self.epoch = 1
        self.loss_evidence = evidence
        self.record({'process': 'controller', 'type': Keyword('info'), 'f': Keyword('epoch'),
                     'allowed_loss': allowed, 'evidence': evidence,
                     'value': [[Keyword('r'), key, decode_list(text)] for key, text in rows]})

    def verify(self, conn, label, allowed_loss=()):
        rows = query(conn, 'SELECT id,v FROM rc_append ORDER BY id')
        assert [r[0] for r in rows] == list(range(KEY_COUNT)), 'missing append key'
        actual = {key: decode_list(text) for key, text in rows}
        def normalize(rows):
            return {row[0]: [*list(row[:5]), str(row[5]), str(row[6]), json.loads(row[7])] for row in rows}
        heap = normalize(query(conn, f'SELECT {PROJECTION} FROM fault_detail IGNORE INDEX(by_marker,by_amount) ORDER BY id'))
        index = normalize(query(conn, f"SELECT {PROJECTION} FROM fault_detail FORCE INDEX(by_marker) WHERE marker>='' ORDER BY id"))
        assert index == heap, 'secondary index differs from heap'
        validate_outcomes(self.transactions, actual, heap, allowed_loss, range(KEY_COUNT))
        pairs = {(key, token) for key, tokens in actual.items() for token in tokens}
        for tx in self.transactions:
            if tx['outcome'] == 'info':
                resolution = bool(set(tx['writes']) & pairs)
                identity = (self.epoch, tx['id'])
                assert identity not in self.resolutions or self.resolutions[identity] == resolution, 'unknown outcome changed after recovery'
                self.resolutions[identity] = resolution
        checked = query(conn, 'CHECK TABLE fault_detail')
        assert checked and all(row[-1] == 'OK' for row in checked), checked
        self.record({'process': 10000, 'type': Keyword('invoke'), 'f': Keyword('txn'),
                     'value': [[Keyword('r'), key, None] for key in range(KEY_COUNT)]})
        self.record({'process': 10000, 'type': Keyword('ok'), 'f': Keyword('txn'), 'final_read?': True,
                     'value': [[Keyword('r'), key, actual[key]] for key in range(KEY_COUNT)]})
        directory = self.output / label
        directory.mkdir()
        (directory / 'history.edn').write_text(edn(self.events) + '\n')
        (directory / 'options.edn').write_text(edn({'allowed_loss': list(allowed_loss), 'expected_keys': list(range(KEY_COUNT))}) + '\n')
        self.checks.append(str(directory))
        print(f'CHECK {label}: {len(self.transactions)} transactions; {len(heap)} typed rows', flush=True)
        return actual


def setup(conn):
    query(conn, 'CREATE TABLE rc_append(id BIGINT PRIMARY KEY,v LONGTEXT NOT NULL)')
    for key in range(KEY_COUNT):
        query(conn, f"INSERT INTO rc_append VALUES({key},'')")
    query(conn, '''CREATE TABLE fault_detail(id BIGINT PRIMARY KEY,marker VARCHAR(256) NOT NULL,
          amount DECIMAL(38,8),body TEXT,payload LONGBLOB,stamp DATETIME(6),meta JSON,v VECTOR(4),
          UNIQUE KEY by_marker(marker),KEY by_amount(amount))''')


def kill(process):
    assert process.poll() is None, 'process exited before fault injection'
    process.kill()
    process.wait(timeout=30)
    assert process.returncode < 0, process.returncode


def crash_round(servers, port, process, conn, ledger, workers, stage):
    # Disjoint writer keys avoid waits before every worker reaches the gate.
    # Seed selection for crash transactions is overridden explicitly below.
    ready = threading.Barrier(workers + 1, timeout=30)
    release = threading.Event()
    def gate():
        ready.wait()
        assert release.wait(30), 'fault controller did not release writers'
    def worker(number):
        return ledger.transaction(servers, port, number, gate=gate, stage=stage)
    # A gate per writer must not wait behind another gated writer's row lock.
    original_plan = ledger.plan
    def disjoint_plan(number):
        tx = original_plan(number)
        tx['keys'] = [number * 2, number * 2 + 1]
        tx['writes'] = [(tx['keys'][0], tx['id'] * 2), (tx['keys'][1], tx['id'] * 2 + 1)]
        return tx
    ledger.plan = disjoint_plan
    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(worker, i) for i in range(workers)]
            reached = False
            try:
                ready.wait()
                reached = True
                query(conn, 'ALTER SYSTEM CHECKPOINT')
                release.set()
                kill(process)
            finally:
                release.set()
                if not reached:
                    ready.abort()
            outcomes = [future.result(timeout=45) for future in futures]
        assert len(outcomes) == workers
        return {'stage': stage, 'ready_writers': workers, 'outcomes': dict(Counter(tx['outcome'] for tx in outcomes))}
    finally:
        ledger.plan = original_plan


class FaultProxy:
    """Bidirectional TCP partition/delay with exact owned-socket evidence."""
    def __init__(self, upstream):
        self.upstream, self.blocked, self.closed, self.delay = upstream, False, False, 0
        self.lock, self.sockets, self.events = threading.Lock(), set(), []
        self.forwarded = 0
        self.delayed_payloads = self.fragments = 0
        self.listener = socket.socket()
        self.listener.bind(('127.0.0.1', 0))
        self.listener.listen()
        self.port = self.listener.getsockname()[1]
        self.acceptor = threading.Thread(target=self.accept, daemon=True)
        self.acceptor.start()

    def accept(self):
        while not self.closed:
            try:
                client, _ = self.listener.accept()
            except OSError:
                break
            with self.lock:
                blocked = self.blocked
            if blocked:
                client.close()
                continue
            try:
                server = socket.create_connection(('127.0.0.1', self.upstream), timeout=5)
                server.settimeout(None)
            except OSError:
                client.close()
                continue
            with self.lock:
                if self.blocked or self.closed:
                    client.close()
                    server.close()
                    continue
                self.sockets.update((client, server))
            for source, target in [(client, server), (server, client)]:
                threading.Thread(target=self.copy, args=(source, target), daemon=True).start()

    def copy(self, source, target):
        try:
            while not self.closed:
                payload = source.recv(65536)
                if not payload:
                    break
                fragments = 0
                if self.delay:
                    # Delay and fragment frames without changing TCP byte order.
                    time.sleep(self.delay)
                    for at in range(0, len(payload), 257):
                        target.sendall(payload[at:at + 257])
                        fragments += 1
                else:
                    target.sendall(payload)
                with self.lock:
                    self.forwarded += len(payload)
                    if fragments:
                        self.delayed_payloads += 1
                        self.fragments += fragments
        except OSError:
            pass
        finally:
            for sock in (source, target):
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                sock.close()
                with self.lock:
                    self.sockets.discard(sock)

    def partition(self):
        with self.lock:
            self.blocked = True
            sockets = list(self.sockets)
            self.events.append({'action': 'partition', 'closed_sockets': len(sockets)})
        for sock in sockets:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()
        assert sockets, 'partition did not hit a live replication connection'

    def heal(self):
        with self.lock:
            self.blocked = False
            self.events.append({'action': 'heal'})

    def close(self):
        self.closed = True
        self.listener.close()
        with self.lock:
            sockets = list(self.sockets)
        for sock in sockets:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()


def scenario(args, buffering, kind, root):
    root.mkdir()
    servers = Servers(args.binary, root)
    ledger = Ledger(root, args.seed)
    proxies, result = [], {'kind': kind, 'buffering': buffering, 'passed': False}
    common = {'MYDB_WAL_TXN_BUFFER': str(buffering), 'MYDB_REPL_TLS_MODE': 'disabled',
              'MYDB_REPL_BIND_HOST': '127.0.0.1', 'MYDB_REPL_HEARTBEAT_MS': '100',
              'MYDB_REPL_RECONNECT_MS': '100', 'MYDB_UNDO_RETENTION_MS': '0',
              'MYDB_UNDO_PURGE_INTERVAL_MS': str(args.purge_interval_ms), 'MYDB_CHECKPOINT_INTERVAL_MS': '1000',
              'MYDB_REPL_BASE_BACKUP_DISK_RESERVE_PCT': '0'}
    try:
        sql_port, native_port = free_port(), free_port()
        primary_env = dict(common, MYDB_ROLE='master', MYDB_REPL_PORT=str(native_port))
        primary_process, primary = servers.start('primary', sql_port, primary_env)
        setup(primary)
        ledger.load(servers, sql_port, args.workers, args.operations)
        ledger.verify(primary, 'baseline')
        if kind == 'crash':
            result['crash_rounds'] = []
            for round_number in range(args.rounds):
                for stage in ('partial', 'commit'):
                    ledger.verify(primary, f'before-crash-{round_number}-{stage}')
                    result['crash_rounds'].append(crash_round(servers, sql_port, primary_process, primary,
                                                             ledger, args.workers, stage))
                    primary_process, primary = servers.start('primary', sql_port, dict(primary_env, MYDB_ROLE='auto'))
                    ledger.verify(primary, f'recovery-{round_number}-{stage}')
                    ledger.load(servers, sql_port, args.workers, 2)
            ledger.verify(primary, 'post-recovery-writes')
        else:
            replicas = []
            for i in range(2):
                proxy = FaultProxy(native_port)
                proxies.append(proxy)
                port, repl_port = free_port(), free_port()
                env = dict(common, MYDB_ROLE='replica', MYDB_REPL_PORT=str(repl_port),
                           MYDB_REPL_MASTER_HOST='127.0.0.1', MYDB_REPL_MASTER_PORT=str(proxy.port))
                process, conn = servers.start(f'replica-{i}', port, env)
                catch_up(primary, conn)
                ledger.verify(conn, f'replica-{i}-baseline')
                replicas.append([process, conn, port, repl_port, env])
            for proxy in proxies:
                proxy.partition()
            ledger.load(servers, sql_port, args.workers, args.operations)
            target = int(status(primary, 'SHOW MYDB MASTER STATUS')['Current_LSN'])
            for _, conn, *_ in replicas:
                assert int(status(conn, 'SHOW MYDB REPLICA STATUS')['Durable_Applied_LSN']) < target
            for proxy in proxies:
                proxy.delay = 0.002
                proxy.heal()
            for i, (_, conn, *_) in enumerate(replicas):
                catch_up(primary, conn)
                ledger.verify(conn, f'partition-healed-{i}')
            result['network_faults'] = [proxy.events for proxy in proxies]
            assert all(proxy.forwarded > 0 and proxy.delayed_payloads > 0 and proxy.fragments > 0 for proxy in proxies), 'delay/fragment path not exercised'
            result['network_counters'] = [{'delayed_payloads': p.delayed_payloads, 'fragments': p.fragments} for p in proxies]
            for proxy in proxies:
                proxy.delay = 0
            process, conn, port, repl_port, env = replicas[0]
            kill(process)
            ledger.load(servers, sql_port, args.workers, args.operations)
            process, conn = servers.start('replica-0', port, dict(env, MYDB_ROLE='auto'))
            replicas[0][:2] = [process, conn]
            catch_up(primary, conn)
            ledger.verify(conn, 'replica-restarted')
            # Kill the primary with real in-flight cross-table transactions.
            result['primary_crash'] = crash_round(servers, sql_port, primary_process, primary,
                                                   ledger, args.workers, 'commit')
            primary_process, primary = servers.start('primary', sql_port, dict(primary_env, MYDB_ROLE='auto'))
            ledger.verify(primary, 'primary-restarted')
            for i, (_, conn, *_) in enumerate(replicas):
                catch_up(primary, conn)
                ledger.verify(conn, f'primary-restart-replica-{i}')
            # Planned handoff requires the actual source fence and durable apply.
            fence = status(primary, 'STOP MASTER WRITES WAIT 30')
            new_process, new_primary, new_port, new_native, _ = replicas[1]
            query(new_primary, f"PROMOTE TO MASTER AT LSN {int(fence['Fence_LSN'])} FENCE_TOKEN='{fence['Fence_Token']}' WAIT 30")
            try:
                query(primary, "INSERT INTO rc_append VALUES(99,'')")
            except pymysql.MySQLError:
                result['old_primary_write_rejected'] = True
            else:
                raise AssertionError('frozen source accepted writes')
            ledger.load(servers, new_port, args.workers, args.operations)
            ledger.verify(new_primary, 'planned-promotion')
            lost_proxy = FaultProxy(new_native)
            proxies.append(lost_proxy)
            query(primary, f"DEMOTE TO REPLICA WITH MASTER='127.0.0.1:{lost_proxy.port}'")
            catch_up(new_primary, primary)
            ledger.verify(primary, 'old-primary-demoted')
            # A lagging candidate may lose only the writes after its recorded
            # durable-applied boundary. External fencing here is process death.
            lost_proxy.partition()
            cut = int(status(primary, 'SHOW MYDB REPLICA STATUS')['Durable_Applied_LSN'])
            loss_start_index = len(ledger.events)
            ledger.record({'process': 'controller', 'type': Keyword('info'), 'f': Keyword('loss-window'), 'candidate_lsn': cut})
            first_lost = ledger.next_id
            ledger.load(servers, new_port, args.workers, args.operations)
            allowed = [pair for tx in ledger.transactions if tx['id'] >= first_lost and tx['outcome'] == 'ok'
                       for pair in tx['writes']]
            assert allowed, 'no acknowledged writes after the candidate boundary'
            source_lsn = int(status(new_primary, 'SHOW MYDB MASTER STATUS')['Current_LSN'])
            assert source_lsn > cut
            assert int(status(primary, 'SHOW MYDB REPLICA STATUS')['Durable_Applied_LSN']) == cut
            ledger.verify(new_primary, 'before-forced-promotion-source')
            before = status(primary, 'SHOW MYDB MASTER STATUS')
            kill(new_process)
            query(primary, 'PROMOTE TO MASTER FORCE FENCED')
            after = status(primary, 'SHOW MYDB MASTER STATUS')
            # Use the actual status schema below; both identities are retained.
            evidence = {'fenced_pid': new_process.pid, 'fenced_exit_code': new_process.returncode,
                        'candidate_lsn': cut, 'partition_lsn': cut, 'source_lsn': source_lsn,
                        'loss_start_index': loss_start_index,
                        'timeline_before': str(before.get('Timeline_UUID', before.get('Source_Timeline_UUID', ''))),
                        'timeline_after': str(after.get('Timeline_UUID', ''))}
            ledger.begin_epoch(primary, allowed, evidence)
            result['forced_promotion'] = {'fenced_pid': new_process.pid, 'candidate_durable_applied_lsn': cut,
                                           'source_lsn': source_lsn, 'allowed_loss': allowed}
            ledger.verify(primary, 'forced-promotion', allowed)
            ledger.load(servers, sql_port, args.workers, args.operations)
            ledger.verify(primary, 'forced-promotion-new-writes', allowed)
            query(primary, 'ALTER SYSTEM CHECKPOINT')
            kill(primary_process)
            primary_process, primary = servers.start('primary', sql_port, dict(primary_env, MYDB_ROLE='auto'))
            ledger.verify(primary, 'promoted-restart', allowed)
        result['phases'] = ledger.phases
        result['effective_config'] = common
        result['passed'] = True
        return ledger.checks
    finally:
        # Always retain histories, including harness failures before readback.
        (root / 'history.edn').write_text(edn(ledger.events) + '\n')
        (root / 'transactions.json').write_text(json.dumps(ledger.transactions, default=str, indent=2) + '\n')
        (root / 'results.json').write_text(json.dumps(result, default=str, indent=2) + '\n')
        for proxy in proxies:
            proxy.close()
        if not result['passed']:
            # A graceful shutdown would checkpoint away the failing WAL prefix.
            for process in servers.processes:
                if process.poll() is None:
                    kill(process)
        servers.close()


def semi_sync_cluster(args, buffering, root):
    """One primary, two semi-sync ack replicas (wait count 1).

    Acknowledged commits must stay acknowledged while one replica is
    partitioned, degrade to local durability while all ack replicas are cut,
    survive a primary crash with no async-loss exemption, and hand over
    through the same planned fenced promotion as the async cluster case.
    """
    root.mkdir()
    servers = Servers(args.binary, root)
    ledger = Ledger(root, args.seed)
    proxies, result = [], {'kind': 'semi-sync', 'buffering': buffering, 'passed': False}
    common = dict(MYDB_WAL_TXN_BUFFER=str(buffering), MYDB_REPL_TLS_MODE='disabled',
                  MYDB_REPL_BIND_HOST='127.0.0.1', MYDB_REPL_HEARTBEAT_MS='100',
                  MYDB_REPL_RECONNECT_MS='100', MYDB_UNDO_RETENTION_MS='0',
                  MYDB_UNDO_PURGE_INTERVAL_MS=str(args.purge_interval_ms), MYDB_CHECKPOINT_INTERVAL_MS='1000',
                  MYDB_REPL_BASE_BACKUP_DISK_RESERVE_PCT='0', MYDB_REPL_MODE='semi_sync',
                  MYDB_REPL_SEMI_SYNC_WAIT_COUNT='1', MYDB_REPL_SEMI_SYNC_TIMEOUT_MS='200')
    counters = ('Semi_Sync_Acked_Commits', 'Semi_Sync_Timeout_Commits', 'Semi_Sync_Degraded_Commits')

    def snapshot(master):
        rows = status(master, 'SHOW MYDB MASTER STATUS')
        return {key: int(rows[key]) for key in counters} | {'Durable_LSN': int(rows['Durable_LSN'])}

    try:
        sql_port, native_port = free_port(), free_port()
        primary_env = dict(common, MYDB_ROLE='master', MYDB_REPL_PORT=str(native_port))
        primary_process, primary = servers.start('primary', sql_port, primary_env)
        setup(primary)
        replicas = []
        for i in range(2):
            proxy = FaultProxy(native_port)
            proxies.append(proxy)
            port, repl_port = free_port(), free_port()
            env = dict(common, MYDB_ROLE='replica', MYDB_REPL_PORT=str(repl_port),
                       MYDB_REPL_MASTER_HOST='127.0.0.1', MYDB_REPL_MASTER_PORT=str(proxy.port))
            process, conn = servers.start(f'replica-{i}', port, env)
            catch_up(primary, conn)
            replicas.append([process, conn, port, repl_port, env, proxy])
        before = snapshot(primary)
        ledger.load(servers, sql_port, args.workers, args.operations)
        after = snapshot(primary)
        assert after['Semi_Sync_Acked_Commits'] > before['Semi_Sync_Acked_Commits'], (before, after)
        ledger.verify(primary, 'semi-sync-acknowledged-baseline')
        # One live ack replica still satisfies the wait count.
        before = snapshot(primary)
        replicas[0][5].partition()
        ledger.load(servers, sql_port, args.workers, args.operations)
        after = snapshot(primary)
        assert after['Semi_Sync_Acked_Commits'] > before['Semi_Sync_Acked_Commits'], (before, after)
        assert after['Semi_Sync_Timeout_Commits'] == before['Semi_Sync_Timeout_Commits'], (before, after)
        catch_up(primary, replicas[1][1])
        ledger.verify(replicas[1][1], 'semi-sync-live-replica-caught-up')
        # With every ack replica cut, commits degrade to local durability.
        before = snapshot(primary)
        replicas[1][5].partition()
        ledger.load(servers, sql_port, args.workers, args.operations)
        after = snapshot(primary)
        assert after['Semi_Sync_Timeout_Commits'] + after['Semi_Sync_Degraded_Commits'] \
            > before['Semi_Sync_Timeout_Commits'] + before['Semi_Sync_Degraded_Commits'], (before, after)
        assert after['Semi_Sync_Acked_Commits'] == before['Semi_Sync_Acked_Commits'], (before, after)
        result['degradation'] = {'before': before, 'after': after}
        for replica in replicas:
            replica[5].heal()
        for i, replica in enumerate(replicas):
            catch_up(primary, replica[1])
            ledger.verify(replica[1], f'semi-sync-healed-replica-{i}')
        # A primary crash may never lose an acknowledged commit: no async-loss
        # exemption is permitted in semi-sync mode.
        result['primary_crash'] = crash_round(servers, sql_port, primary_process, primary,
                                              ledger, args.workers, 'commit')
        primary_process, primary = servers.start('primary', sql_port, dict(primary_env, MYDB_ROLE='auto'))
        ledger.verify(primary, 'semi-sync-primary-restarted')
        for i, replica in enumerate(replicas):
            catch_up(primary, replica[1])
            ledger.verify(replica[1], f'semi-sync-primary-restart-replica-{i}')
        # Planned fenced handoff identical to the async cluster case.
        fence = status(primary, 'STOP MASTER WRITES WAIT 30')
        promoted_process, promoted, promoted_port, promoted_native, _, _ = replicas[1]
        query(promoted, f"PROMOTE TO MASTER AT LSN {int(fence['Fence_LSN'])} FENCE_TOKEN='{fence['Fence_Token']}' WAIT 30")
        try:
            query(primary, "INSERT INTO rc_append VALUES(99,'')")
        except pymysql.MySQLError:
            result['old_primary_write_rejected'] = True
        else:
            raise AssertionError('frozen source accepted writes')
        ledger.load(servers, promoted_port, args.workers, args.operations)
        ledger.verify(promoted, 'semi-sync-planned-promotion')
        demote_proxy = FaultProxy(promoted_native)
        proxies.append(demote_proxy)
        query(primary, f"DEMOTE TO REPLICA WITH MASTER='127.0.0.1:{demote_proxy.port}'")
        catch_up(promoted, primary)
        ledger.verify(primary, 'semi-sync-old-primary-demoted')
        result['phases'] = ledger.phases
        result['effective_config'] = common
        result['passed'] = True
        return ledger.checks
    finally:
        (root / 'history.edn').write_text(edn(ledger.events) + '\n')
        (root / 'transactions.json').write_text(json.dumps(ledger.transactions, default=str, indent=2) + '\n')
        (root / 'results.json').write_text(json.dumps(result, default=str, indent=2) + '\n')
        for proxy in proxies:
            proxy.close()
        if not result['passed']:
            # A graceful shutdown would checkpoint away the failing WAL prefix.
            for process in servers.processes:
                if process.poll() is None:
                    kill(process)
        servers.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--case', choices=['all', 'crash', 'cluster', 'semi-sync'], default='all')
    parser.add_argument('--buffering', type=int, choices=[0, 1], action='append')
    parser.add_argument('--workers', type=int, default=4)
    parser.add_argument('--operations', type=int, default=32)
    parser.add_argument('--rounds', type=int, default=2)
    parser.add_argument('--seed', type=int, default=20261115)
    parser.add_argument('--purge-interval-ms', type=int, default=100)
    parser.add_argument('--lein', default=os.environ.get('LEIN', 'lein'))
    args = parser.parse_args()
    args.binary, args.output = args.binary.resolve(), args.output.resolve()
    if not args.output.is_relative_to(Path('/tmp/mydb').resolve()):
        parser.error('--output must be below /tmp/mydb')
    if not 1 <= args.workers <= KEY_COUNT // 2 or min(args.operations, args.rounds) <= 0:
        parser.error('workers must be 1..4; operations and rounds must be positive')
    if not args.binary.is_file() or not shutil.which(args.lein):
        parser.error('a built mydb binary and Leiningen are required')
    args.output.mkdir(parents=True, exist_ok=False)
    sources = [ROOT / 'tests/jepsen', ROOT / 'scripts/test_native_readiness.py', ROOT / 'scripts/safety_evidence.py']
    manifest = dict(vars(args), binary_sha256=hashlib.sha256(args.binary.read_bytes()).hexdigest(),
                    **source_identity(ROOT, sources))
    snapshot(ROOT, args.output, sources)
    write_json(args.output / 'manifest.json', manifest)
    checks = []
    plan = [(b, kind) for b in dict.fromkeys(args.buffering or [0, 1])
            for kind in (['crash', 'cluster'] if args.case == 'all' else [args.case])]
    results = [{'buffering': b, 'kind': kind, 'status': 'incomplete'} for b, kind in plan]
    write_json(args.output / 'results.json', {'passed': False, 'scenarios': results})
    for index, (buffering, kind) in enumerate(plan):
        try:
            if kind == 'semi-sync':
                checks.extend(semi_sync_cluster(args, buffering, args.output / f'semi-sync-buffer-{buffering}'))
            else:
                checks.extend(scenario(args, buffering, kind, args.output / f'{kind}-buffer-{buffering}'))
            results[index]['status'] = 'awaiting-checker'
        except Exception as error:
            results[index].update(status='fail', error=repr(error))
        write_json(args.output / 'results.json', {'passed': False, 'scenarios': results, 'checks': checks})
    index = args.output / 'checks.edn'
    index.write_text(edn(checks) + '\n')
    env = dict(os.environ, TMPDIR='/tmp/mydb', JAVA_TOOL_OPTIONS='-Djava.io.tmpdir=/tmp/mydb')
    with (args.output / 'checker.log').open('w') as log:
        checker = subprocess.run([args.lein, 'run', '-m', 'mydb.fault-history', str(index)],
                                 cwd=ROOT / 'tests/jepsen', env=env, stdout=log, stderr=subprocess.STDOUT,
                                 timeout=300)
    for result in results:
        if result['status'] == 'awaiting-checker':
            result['status'] = 'pass' if checker.returncode == 0 else 'fail'
    passed = all(r['status'] == 'pass' for r in results)
    write_json(args.output / 'results.json', {'passed': passed, 'scenarios': results, 'checks': checks,
                                             'checker_exit_code': checker.returncode})
    return 0 if passed else 1


if __name__ == '__main__':
    raise SystemExit(main())
