#!/usr/bin/env python3
"""Local release gates for native replica recovery and index aggregate protocol.

Requires pymysql, cryptography and mysql-connector-python. Uses only fresh
databases, dynamically allocated loopback ports and owned server processes.
All logs and complete row comparisons are retained below /tmp/mydb.
"""
import argparse
from decimal import Decimal
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time

import mysql.connector
import pymysql


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def query(connection, sql):
    with connection.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchall() if cursor.description else cursor.rowcount


def status(connection, sql):
    with connection.cursor() as cursor:
        cursor.execute(sql)
        return dict(zip((col[0] for col in cursor.description), cursor.fetchone()))


class Servers:
    def __init__(self, binary, root):
        self.binary = str(binary)
        self.root = root
        self.processes = []
        self.connections = []

    def connect(self, port):
        connection = pymysql.connect(host="127.0.0.1", port=port, user="root", password="root",
                                     database="test", autocommit=True, read_timeout=30, write_timeout=30)
        self.connections.append(connection)
        return connection

    def start(self, name, port, extra=None):
        env = {key: value for key, value in os.environ.items() if not key.startswith("MYDB_")}
        env.update(MYDB_DATA_DIR=str(self.root / name), MYDB_DURABILITY="strict",
                   MYDB_MEMORY_TARGET="512M", MYDB_WAL_SEGMENT_BYTES="1048576",
                   MYDB_CHECKPOINT_INTERVAL_MS="2000", MYDB_SHUTDOWN_TIMEOUT_MS="1000",
                   MYDB_ROOT_PASSWORD="root", TMPDIR="/tmp/mydb")
        env.update(extra or {})
        log = self.root / (name + ".log")
        with log.open("ab") as output:
            process = subprocess.Popen([self.binary, "--host", "127.0.0.1", "--port", str(port)],
                                       env=env, stdout=output, stderr=subprocess.STDOUT)
        self.processes.append(process)
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise AssertionError(f"server exited {process.returncode}: {log}")
            try:
                return process, self.connect(port)
            except pymysql.Error as error:
                if error.args[0] in (1044, 1045, 3159):
                    raise
                time.sleep(0.1)
        raise TimeoutError(f"server startup: {log}")

    def close(self):
        for connection in self.connections:
            try:
                connection.close()
            except pymysql.Error:
                pass
        for process in self.processes:
            if process.poll() is None:
                process.terminate()
        for process in self.processes:
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def catch_up(master, replica):
    target = int(status(master, "SHOW MYDB MASTER STATUS")["Current_LSN"])
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        current = status(replica, "SHOW MYDB REPLICA STATUS")
        if int(current["Durable_Applied_LSN"]) >= target:
            return current
        time.sleep(0.05)
    raise TimeoutError(f"replica catch-up target={target}, status={current}")


def replica_case(binary, root, buffered, commit):
    servers = Servers(binary, root)
    ports = [free_port() for _ in range(4)]
    result = {"buffered": buffered, "commit": commit}
    try:
        wal = {} if buffered else {"MYDB_WAL_TXN_BUFFER": "0"}
        _, master = servers.start("master", ports[0], dict(wal, MYDB_ROLE="master", MYDB_REPL_PORT=str(ports[2])))
        query(master, "CREATE TABLE t(id INT PRIMARY KEY,v INT,d DECIMAL(20,4),b VARBINARY(16),dt DATETIME(6),KEY kv(v),KEY kd(d))")
        query(master, "INSERT INTO t VALUES(1,10,7.2500,X'FF00','2026-10-13 12:34:56.123456'),(2,20,9.5000,X'8001',NULL),(3,30,NULL,NULL,NULL),(4,40,NULL,NULL,NULL)")
        query(master, "CREATE TABLE filler(id INT PRIMARY KEY,pad VARCHAR(900))")
        replica_env = dict(wal, MYDB_ROLE="replica", MYDB_REPL_PORT=str(ports[3]),
                           MYDB_REPL_MASTER_HOST="127.0.0.1", MYDB_REPL_MASTER_PORT=str(ports[2]))
        process, replica = servers.start("replica", ports[1], replica_env)
        writer = servers.connect(ports[0])
        before = query(master, "SELECT * FROM t ORDER BY id")
        query(writer, "BEGIN")
        query(writer, "UPDATE t SET v=111,d=55.1250,b=X'0011FF' WHERE id=1")
        query(writer, "SAVEPOINT stable")
        query(writer, "UPDATE t SET v=222,d=99.0000 WHERE id=1")
        query(writer, "ROLLBACK TO SAVEPOINT stable")
        query(writer, "DELETE FROM t WHERE id=2")
        second_writer = servers.connect(ports[0])
        query(second_writer, "BEGIN")
        query(second_writer, "UPDATE t SET v=333 WHERE id=3")
        # The committed full-row write can publish a page image containing
        # another transaction's still-deferred delta and its undo pointer.
        query(master, "UPDATE t SET v=44,d=12.2500,b=X'EE112233' WHERE id=4")
        before = query(master, "SELECT * FROM t ORDER BY id")
        for identifier in range(100, 700 if buffered else 103):
            query(writer, f"INSERT INTO t VALUES({identifier},{identifier},{identifier}.1250,X'AAFF',NULL)")
        for first in range(0, 1000, 100):
            query(master, "INSERT INTO filler VALUES" + ",".join(f"({i},'{i:0900d}')" for i in range(first, first + 100)))
        result["before_status"] = catch_up(master, replica)
        for restart in range(2):
            rows = query(replica, "SELECT * FROM t ORDER BY id")
            result[f"before_restart_{restart}"] = rows
            assert rows == before, ("uncommitted source rows", rows, before)
            # Secondary/primary index reads must see the same old versions.
            assert query(replica, "SELECT id FROM t FORCE INDEX(kv) WHERE v=10") == ((1,),)
            assert query(replica, "SELECT id FROM t FORCE INDEX(PRIMARY) WHERE id=2") == ((2,),)
            query(replica, "ALTER SYSTEM CHECKPOINT")
            replica.close()
            process.kill()
            process.wait()
            process, replica = servers.start("replica", ports[1], dict(replica_env, MYDB_ROLE="auto"))
            result[f"restart_status_{restart}"] = status(replica, "SHOW MYDB REPLICA STATUS")
            rows = query(replica, "SELECT * FROM t ORDER BY id")
            result[f"after_restart_{restart}"] = rows
            assert rows == before, ("restart changed source transaction", rows, before)
        query(writer, "COMMIT" if commit else "ROLLBACK")
        query(second_writer, "ROLLBACK")
        result["after_status"] = catch_up(master, replica)
        expected = query(master, "SELECT * FROM t ORDER BY id")
        actual = query(replica, "SELECT * FROM t ORDER BY id")
        result.update(master_rows=expected, replica_rows=actual)
        assert actual == expected, ("committed source/replica mismatch", expected[:5], actual[:5], len(expected), len(actual))
        for predicate in ("v=10", "v=111", "v=20", "d=55.1250"):
            assert query(replica, f"SELECT id FROM t WHERE {predicate} ORDER BY id") == query(master, f"SELECT id FROM t WHERE {predicate} ORDER BY id"), predicate
        result["check_table"] = query(replica, "CHECK TABLE t")
        assert all(row[-1] == "OK" for row in result["check_table"]), result["check_table"]
        result["passed"] = True
    finally:
        (root / "results.json").write_text(json.dumps(result, indent=2, default=str))
        servers.close()


def aggregate_case(binary, root):
    servers = Servers(binary, root)
    prepared_connection = None
    result = []
    try:
        number = free_port()
        _, connection = servers.start("server", number)
        query(connection, "CREATE TABLE t(id INT PRIMARY KEY,u BIGINT UNSIGNED,d DECIMAL(20,4),f FLOAT,x DOUBLE,dt DATETIME(6),s VARCHAR(16) COLLATE utf8mb4_bin,b VARBINARY(16), y YEAR, da DATE, tm TIME(6), e ENUM('b','a'), bt BIT(64), w DECIMAL(65,30),KEY ku(u),KEY kd(d),KEY kf(f),KEY kx(x),KEY kt(dt),KEY ks(s),KEY kb(b),KEY ky(y),KEY ka(da),KEY km(tm),KEY ke(e),KEY kbt(bt),KEY kw(w))")
        query(connection, "INSERT INTO t VALUES(1,18446744073709551615,7.2500,1.25,1.25,'2026-10-13 12:34:56.123456','a',X'FF00',2026,'2026-10-13','12:34:56.123456','b',18446744073709551615,12345678901234567890123456789012345.125000000000000000000000000000),(2,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)")
        prepared_connection = mysql.connector.connect(host="127.0.0.1", port=number, user="root", password="root",
                                                      database="test", autocommit=True, use_pure=True, ssl_disabled=False)
        for state in range(3):
            if state == 1:
                query(connection, "DELETE FROM t WHERE id=1")
            if state == 2:
                query(connection, "DELETE FROM t")
            for column, index in (("u", "ku"), ("d", "kd"), ("f", "kf"), ("x", "kx"), ("dt", "kt"), ("s", "ks"), ("b", "kb"), ("id", "PRIMARY"), ("y", "ky"), ("da", "ka"), ("tm", "km"), ("e", "ke"), ("bt", "kbt"), ("w", "kw")):
                for aggregate in ("MIN", "MAX"):
                    fast = f"SELECT {aggregate}({column}) AS edge FROM t"
                    scan = f"SELECT {aggregate}({column}) AS edge FROM t IGNORE INDEX({index})"
                    with connection.cursor() as cursor:
                        cursor.execute(scan)
                        expected, metadata = cursor.fetchall(), cursor.description
                        cursor.execute(fast)
                        assert cursor.description == metadata, (state, fast, metadata, cursor.description)
                        assert cursor.fetchall() == expected, (state, fast)
                    with prepared_connection.cursor(prepared=True) as cursor:
                        cursor.execute(scan)
                        binary_expected = cursor.fetchall()
                        binary_metadata = cursor.description
                        cursor.execute(fast)
                        actual = cursor.fetchall()
                        # The driver may decode binary-collation text as bytes;
                        # compare identical protocol paths without coercing values.
                        assert cursor.description == binary_metadata, (state, fast, binary_metadata, cursor.description)
                        assert actual == binary_expected, (state, fast, binary_expected, actual)
                    result.append({"state": state, "sql": fast, "value": expected})
        assert any(row["value"] == ((Decimal("7.2500"),),) for row in result)
    finally:
        (root / "results.json").write_text(json.dumps(result, indent=2, default=str))
        if prepared_connection:
            prepared_connection.close()
        servers.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("target/release/mydb"))
    parser.add_argument("--case", choices=("all", "replica", "aggregate"), default="all")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    Path("/tmp/mydb").mkdir(exist_ok=True)
    root = args.output or Path(tempfile.mkdtemp(prefix="native-readiness-", dir="/tmp/mydb"))
    root.mkdir(parents=True, exist_ok=True)
    print(f"Artifacts: {root}", flush=True)
    if args.case in ("all", "aggregate"):
        path = root / "aggregate"
        path.mkdir()
        aggregate_case(binary, path)
        print("PASS aggregate text/binary metadata and values", flush=True)
    if args.case in ("all", "replica"):
        for buffered in (True, False):
            for commit in (True, False):
                name = f"replica-{'buffered' if buffered else 'eager'}-{'commit' if commit else 'rollback'}"
                path = root / name
                path.mkdir()
                replica_case(binary, path, buffered, commit)
                print(f"PASS {name}", flush=True)


if __name__ == "__main__":
    main()
