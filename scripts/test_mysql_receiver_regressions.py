#!/usr/bin/env python3
"""Local MySQL ROW/FULL receiver regression suite.

Requires PyMySQL and an existing local MySQL source with binlog enabled.
Starts its own mydb process and fresh data directory beneath /tmp/mydb.
Only the uniquely named test database on the source is created/dropped.
"""
import argparse
import os
from pathlib import Path
import signal
import subprocess
import time
import uuid

import pymysql


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--mydb-bin", type=Path, default=Path("target/release/mydb"))
    ap.add_argument("--source-port", type=int, default=3371)
    ap.add_argument("--source-user", default="root")
    ap.add_argument("--source-password", default="")
    ap.add_argument("--replica-port", type=int, default=4932)
    ap.add_argument("--bench-only", action="store_true")
    ap.add_argument("--bench-rows", type=int, default=20000)
    ap.add_argument("--bench-rounds", type=int, default=3)
    args = ap.parse_args()
    if args.bench_rows < 1 or args.bench_rounds < 1:
        ap.error("benchmark rows and rounds must be positive")
    root = Path("/tmp/mydb") / ("receiver-regressions-" + uuid.uuid4().hex[:12])
    root.mkdir(parents=True)
    db = "receiver_" + uuid.uuid4().hex[:12]
    source = dict(host="127.0.0.1", port=args.source_port, user=args.source_user,
                  password=args.source_password, autocommit=True)
    replica = dict(host="127.0.0.1", port=args.replica_port, user="root",
                   password="root", autocommit=True)
    proc = None
    log = open(root / "replica.log", "a")

    def query(config, sql, params=None):
        with pymysql.connect(**config) as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    def m(sql, params=None):
        return query(source, sql, params)

    def r(sql, params=None):
        return query(replica, sql, params)

    def status():
        with pymysql.connect(**replica) as conn, conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute("SHOW REPLICA STATUS")
            return cur.fetchone()

    def start():
        nonlocal proc
        env = dict(os.environ, MYDB_DATA_DIR=str(root / "data"))
        proc = subprocess.Popen([str(args.mydb_bin.resolve()), "--host", "127.0.0.1", "--port", str(args.replica_port)],
                                env=env, stdout=log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                raise AssertionError(f"replica exited: see {root / 'replica.log'}")
            try:
                r("SELECT 1")
                return
            except pymysql.Error:
                time.sleep(0.05)
        raise AssertionError("replica startup timed out")

    def crash_restart():
        nonlocal proc
        proc.kill()
        proc.wait(timeout=15)
        start()

    def caught_up(timeout=30):
        want = m("SELECT @@GLOBAL.gtid_executed")[0][0]
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            st = status()
            assert st and not st["Last_SQL_Errno"] and not st["Last_IO_Errno"], st
            # The fixture starts at today's file position. Its GTID set need
            # only cover source transactions created after that position.
            file_pos = m("SHOW BINARY LOG STATUS")[0][:2]
            if st["Relay_Source_Log_File"] == file_pos[0] and st["Exec_Source_Log_Pos"] >= file_pos[1]:
                return
            time.sleep(0.02)
        raise AssertionError(f"catchup timeout ({want}): {status()}")

    def equal(table, columns="*", order="id"):
        sql = f"SELECT {columns} FROM {db}.{table} ORDER BY {order}"
        a, b = m(sql), r(sql)
        assert a == b, (table, a[:5], b[:5], status())

    def passed(name):
        print("PASS", name, flush=True)

    try:
        assert m("SELECT @@binlog_format,@@binlog_row_image")[0] == ("ROW", "FULL")
        file, pos = m("SHOW BINARY LOG STATUS")[0][:2]
        start()
        r("CHANGE REPLICATION SOURCE TO SOURCE_HOST='127.0.0.1', SOURCE_PORT=%s, SOURCE_USER=%s, SOURCE_PASSWORD=%s, SOURCE_AUTO_POSITION=0, SOURCE_LOG_FILE=%s, SOURCE_LOG_POS=%s",
          (args.source_port, args.source_user, args.source_password, file, pos))
        r("START REPLICA")
        m(f"CREATE DATABASE {db}")
        caught_up()
        if not args.bench_only:
            try:
                r("CHANGE REPLICATION SOURCE TO SOURCE_DELAY=1")
                raise AssertionError("running channel accepted CHANGE SOURCE")
            except pymysql.Error as err:
                assert err.args[0] == 3081, err
            passed("CODE-3: running channel returns 3081")

            m(f"CREATE TABLE {db}.j(id INT PRIMARY KEY,j JSON)")
            expressions = [
                "JSON_OBJECT('d',DATE'2026-01-15','t',TIME'-838:59:58.123456','ts',TIMESTAMP'2026-01-15 10:30:45.500001')",
                "JSON_OBJECT('dec',CAST('99999999999999999999999999999999999.123456789012345678901234567890' AS DECIMAL(65,30)),'f',1.25,'z',CAST('0.00' AS DECIMAL(3,2)))",
                "JSON_OBJECT('b',CAST('a' AS BINARY(1)),'long',CAST(REPEAT('x',60) AS BINARY(60)))",
                "JSON_OBJECT('arr',JSON_ARRAY(JSON_OBJECT('n',CAST('-0.000001' AS DECIMAL(20,6)))),'pad',REPEAT('x',2000))",
            ]
            for i, expr in enumerate(expressions, 1):
                m(f"INSERT INTO {db}.j VALUES({i},{expr})")
            caught_up()
            equal("j", "id,JSON_EXTRACT(j,'$')")
            for path in ["d", "t", "ts", "dec", "f", "z", "b", "long", "arr[0].n"]:
                equal("j", f"id,JSON_TYPE(JSON_EXTRACT(j,'$.{path}'))")
            equal("j", "id,JSON_UNQUOTE(JSON_EXTRACT(j,'$.b')),JSON_VALUE(j,'$.b')")
            equal("j", "id,JSON_UNQUOTE(JSON_EXTRACT(j,'$.long')),JSON_VALUE(j,'$.long')")
            with pymysql.connect(**source) as conn, conn.cursor() as cur:
                cur.execute("SET SESSION binlog_row_value_options='PARTIAL_JSON'")
                cur.execute(f"UPDATE {db}.j SET j=JSON_SET(j,'$.arr[0].n',CAST('12345678901234567890.123456' AS DECIMAL(30,6)),'$.date',DATE'2025-02-28') WHERE id=4")
            caught_up()
            equal("j", "id,JSON_EXTRACT(j,'$')")
            equal("j", "id,JSON_TYPE(JSON_EXTRACT(j,'$.arr[0].n')),JSON_TYPE(JSON_EXTRACT(j,'$.date'))")
            passed("BUG-1: typed JSON values, precision, binary bytes and PARTIAL_JSON")

            for table, key in [("keyed", "PRIMARY KEY"), ("keyless", "")]:
                m(f"CREATE TABLE {db}.{table}(id INT {key}, gone INT, v VARCHAR(20) CHARACTER SET latin1, j JSON, g INT AS(id+1) VIRTUAL, s INT AS(id*2) STORED)")
                m(f"INSERT INTO {db}.{table}(id,gone,v,j) VALUES(1,10,'abc',JSON_OBJECT('n',1.25)),(2,20,'def',JSON_OBJECT('n',2.5))")
                caught_up()
                m(f"ALTER TABLE {db}.{table} DROP COLUMN gone")
                m(f"UPDATE {db}.{table} SET id=3,v='é',j=JSON_SET(j,'$.n',3.75) WHERE id=1")
                m(f"DELETE FROM {db}.{table} WHERE id=2")
                m(f"ALTER TABLE {db}.{table} ADD n DECIMAL(20,3) DEFAULT 7.5 FIRST")
                m(f"ALTER TABLE {db}.{table} MODIFY v VARCHAR(20) CHARACTER SET latin1 AFTER n")
                m(f"INSERT INTO {db}.{table}(id,v,j) VALUES(4,'ok',JSON_OBJECT('n',4.125))")
                m(f"UPDATE {db}.{table} SET n=8.125,v='updated' WHERE id=3")
                m(f"DELETE FROM {db}.{table} WHERE id=4")
                m(f"INSERT INTO {db}.{table}(id,v,j) VALUES(5,'tail',JSON_OBJECT('n',5.25))")
                caught_up()
                equal(table)
            passed("BUG-2: drop/reorder/add columns, charset, generated columns, keyed/keyless DML")

            r("STOP REPLICA")
            r("CHANGE REPLICATION SOURCE TO SOURCE_DELAY=3")
            r("START REPLICA")
            begin = time.time()
            m(f"CREATE TABLE {db}.delay_t(id INT PRIMARY KEY)")
            m(f"INSERT INTO {db}.delay_t VALUES(1)")
            for _ in range(100):
                st = status()
                if st["SQL_Remaining_Delay"] is not None:
                    break
                time.sleep(0.02)
            assert st["SQL_Remaining_Delay"] is not None, st
            assert st["SQL_Delay"] == 3 and st["Seconds_Behind_Source"] is not None, st
            r("STOP REPLICA SQL_THREAD")
            try:
                assert not r(f"SELECT * FROM {db}.delay_t")
                raise AssertionError("DDL committed during delay")
            except pymysql.Error as err:
                assert err.args[0] == 1146, err
            r("START REPLICA SQL_THREAD")
            caught_up()
            elapsed = time.time() - begin
            assert elapsed >= 1.9, elapsed
            equal("delay_t")
            r("STOP REPLICA")
            r("CHANGE REPLICATION SOURCE TO SOURCE_DELAY=0")
            r("START REPLICA")
            caught_up()
            passed(f"BUG-3: DDL/DML delay with heartbeat and STOP/START ({elapsed:.2f}s)")

            m(f"CREATE TABLE {db}.xa_t(id INT PRIMARY KEY,v INT)")
            caught_up()
            def prepare(xid, row, compressed=False):
                with pymysql.connect(**source) as conn, conn.cursor() as cur:
                    if compressed:
                        cur.execute("SET SESSION binlog_transaction_compression=ON")
                    cur.execute(f"XA START {xid}")
                    if row is not None:
                        cur.execute(f"INSERT INTO {db}.xa_t VALUES(%s,%s)", row)
                    cur.execute(f"XA END {xid}")
                    cur.execute(f"XA PREPARE {xid}")
            prepare("'restart'", (1,10))
            caught_up()
            assert r(f"SELECT * FROM {db}.xa_t") == ()
            crash_restart()
            caught_up()
            assert r(f"SELECT * FROM {db}.xa_t") == ()
            m(f"INSERT INTO {db}.xa_t VALUES(2,20)")
            caught_up()
            equal("xa_t")
            m("XA COMMIT 'restart'")
            caught_up()
            equal("xa_t")
            prepare("'a','branch',1234", (3,30), True)
            prepare("X'00ff',X'80',4294967295", (4,40))
            caught_up()
            crash_restart()
            caught_up()
            m("XA ROLLBACK X'00ff',X'80',4294967295")
            m("XA COMMIT 'a','branch',1234")
            caught_up()
            equal("xa_t")
            with pymysql.connect(**source) as conn, conn.cursor() as cur:
                cur.execute("XA START 'one_phase'")
                cur.execute(f"INSERT INTO {db}.xa_t VALUES(5,50)")
                cur.execute("XA END 'one_phase'")
                cur.execute("XA COMMIT 'one_phase' ONE PHASE")
            prepare("'','',2147483648", None)
            caught_up()
            m("XA COMMIT '','',2147483648")
            prepare("'restart'", (6,60))
            caught_up()
            m("XA COMMIT 'restart'")
            m(f"UPDATE {db}.xa_t SET v=v+1")
            caught_up()
            equal("xa_t")
            passed("BUG-4: XA prepare/commit/rollback/one-phase, compression, binary XID, reuse and kill-9 recovery")

        m(f"CREATE TABLE {db}.perf(id INT PRIMARY KEY,v VARCHAR(200),b BLOB)")
        caught_up()
        timings = []
        for round_no in range(args.bench_rounds):
            with pymysql.connect(**source) as conn, conn.cursor() as cur:
                cur.execute("BEGIN")
                cur.executemany(f"INSERT INTO {db}.perf VALUES(%s,%s,%s)",
                                [(round_no*args.bench_rows+i, "v"*200, b"x"*100) for i in range(args.bench_rows)])
                cur.execute("COMMIT")
            begin = time.monotonic()
            caught_up(90)
            timings.append(time.monotonic()-begin)
        equal("perf")
        passed(f"{args.bench_rows}-row transaction apply seconds: " + ", ".join(f"{v:.3f}" for v in timings))

        if not args.bench_only:
            m(f"CREATE TABLE {db}.drift(id INT PRIMARY KEY,v INT)")
            m(f"INSERT INTO {db}.drift VALUES(1,10)")
            caught_up()
            # Deliberately create drift in this disposable replica while its
            # SQL thread is already tailing live input (not recovery replay).
            r("SET GLOBAL mydb_replica_read_only=0")
            r(f"DELETE FROM {db}.drift")
            r(f"INSERT INTO {db}.drift VALUES(2,20)")
            m(f"UPDATE {db}.drift SET id=2,v=20 WHERE id=1")
            for _ in range(150):
                st = status()
                if st["Last_SQL_Errno"]:
                    break
                time.sleep(0.02)
            assert st["Last_SQL_Errno"] == 1032, st
            passed("CODE-2: STRICT exposes missing before-image even when after-image exists")
        print("ALL PASS; artifacts:", root, flush=True)
    finally:
        if proc and proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        log.close()
        # Complete any source test branches if an assertion interrupted XA.
        for xid in ["'restart'", "'a','branch',1234", "X'00ff',X'80',4294967295", "'one_phase'", "'','',2147483648"]:
            try:
                m(f"XA ROLLBACK {xid}")
            except pymysql.Error:
                pass
        m(f"DROP DATABASE IF EXISTS {db}")


if __name__ == "__main__":
    main()
