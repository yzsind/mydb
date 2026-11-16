#!/usr/bin/env python3
"""Local strict-durability acceptance with an independent acknowledged-state oracle.

Requires PyMySQL. Builds are explicit; pass a frozen server binary with --binary.
All processes and data directories are owned by this invocation under /tmp/mydb.
This tests process crashes, not power loss or arbitrary filesystem durability.
"""

import argparse
import datetime
import decimal
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import random
import signal
import socket
import ssl
import subprocess
import threading
import time
import traceback

import pymysql


CASES = ("commit", "loser", "savepoint", "ddl", "checkpoint",
         "truncate_inflight", "copy_inflight", "index_inflight")
PROJECTION = "id,k,amount,body,HEX(payload),VECTOR_TO_STRING(v),stamp,JSON_EXTRACT(meta,'$.n')"


def sql(connection, statement, args=None):
    with connection.cursor() as cursor:
        cursor.execute(statement, args)
        return cursor.fetchall() if cursor.description else ()


def canonical(value):
    if isinstance(value, (decimal.Decimal, datetime.datetime)):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    raise TypeError(type(value).__name__)


def digest(rows):
    return hashlib.sha256(json.dumps(rows, default=canonical, ensure_ascii=False).encode()).hexdigest()


class Server:
    def __init__(self, binary, root, buffering):
        self.binary, self.root, self.buffering = binary, root, buffering
        self.process = None
        self.starts = 0
        self.tls = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        self.tls.check_hostname = False
        self.tls.verify_mode = ssl.CERT_NONE

    def connect(self):
        return pymysql.connect(host="127.0.0.1", port=self.port, user="root", password="root",
                               database="test", autocommit=True, charset="utf8mb4", ssl=self.tls,
                               connect_timeout=2, read_timeout=60, write_timeout=60)

    def start(self):
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            self.port = probe.getsockname()[1]
        self.starts += 1
        self.log = self.root / f"server-{self.starts}.log"
        env = {key: value for key, value in os.environ.items() if not key.startswith("MYDB_")}
        env.update(TMPDIR="/tmp/mydb", MYDB_DATA_DIR=str(self.root / "data"),
                   MYDB_ROOT_PASSWORD="root", MYDB_DURABILITY="strict", MYDB_MEMORY_TARGET="512M",
                   MYDB_TEMP_DISK_RESERVE_PCT="0", MYDB_CHECKPOINT_INTERVAL_MS="0",
                   MYDB_CHECKPOINT_WAL_BYTES="0", MYDB_WAL_TXN_BUFFER=str(self.buffering))
        with self.log.open("wb") as output:
            self.process = subprocess.Popen([str(self.binary), "--host", "127.0.0.1", "--port", str(self.port)],
                                            stdout=output, stderr=output, env=env)
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f"server startup failed: {self.log}\n{self.log.read_text()[-4000:]}")
            try:
                connection = self.connect()
                sql(connection, "SET time_zone='+00:00'")
                return connection
            except pymysql.Error:
                time.sleep(0.025)
        raise TimeoutError(f"startup timed out: {self.log}")

    def kill(self):
        if self.process is None or self.process.poll() is not None:
            raise RuntimeError("server exited before the requested crash")
        self.process.kill()
        self.process.wait(timeout=15)
        if self.process.returncode != -signal.SIGKILL:
            raise RuntimeError(f"unexpected crash status {self.process.returncode}")
        self.process = None

    def close(self, abort=False):
        if self.process is not None and self.process.poll() is None:
            if abort:
                self.process.kill()
            else:
                self.process.terminate()
            try:
                self.process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=15)
        self.process = None


def seed(connection, count):
    sql(connection, """CREATE TABLE t(
        id BIGINT PRIMARY KEY,k BIGINT NOT NULL,amount DECIMAL(20,4),body TEXT,
        payload LONGBLOB,v VECTOR(4) NOT NULL,stamp DATETIME(6),meta JSON,
        KEY by_k(k),FULLTEXT KEY words(body))""")
    expected = {}
    sql(connection, "BEGIN")
    for first in range(1, count + 1, 200):
        values = []
        for i in range(first, min(first + 200, count + 1)):
            body = None if i % 17 == 0 else f"durablemark group{i % 7} 中文记录 {i}"
            blob = None if i % 19 == 0 else (bytes([i % 256, 0, 255, 39, 92]) * (4000 if i % 101 == 0 else 3))
            amount = None if i % 13 == 0 else decimal.Decimal(i).scaleb(-4)
            stamp = datetime.datetime(2040, 2, 3, 4, 5, 6, i)
            vector = [i % 31 + 1, i % 11, 2, 3]
            row = [i, i * 3, amount, body, None if blob is None else blob.hex().upper(),
                   json.dumps(vector, separators=(",", ":")), stamp, str(i)]
            expected[i] = row
            values.extend((i, i * 3, amount, body, blob, json.dumps(vector), stamp, json.dumps({"n": i})))
        sql(connection, "INSERT INTO t VALUES " + ",".join(["(%s,%s,%s,%s,%s,TO_VECTOR(%s),%s,%s)"] * (len(values) // 8)), values)
    sql(connection, "COMMIT")
    sql(connection, "CREATE INDEX nearest ON t(v) USING HNSW WITH(metric=l2)")
    return expected


def normalized(rows):
    result = []
    for row in rows:
        row = list(row)
        row[5] = json.loads(row[5])
        row[7] = None if row[7] is None else int(row[7])
        result.append(row)
    return result


def verify(connection, expected):
    oracle = normalized([expected[key] for key in sorted(expected)])
    for hint in ("IGNORE INDEX(by_k,words,nearest)", "FORCE INDEX(by_k)"):
        actual = normalized(sql(connection, f"SELECT {PROJECTION} FROM t {hint} WHERE k>=-9223372036854775808 ORDER BY id"))
        if actual != oracle:
            a = {row[0]: row for row in actual}
            e = {row[0]: row for row in oracle}
            different = [key for key in sorted(a.keys() | e.keys()) if a.get(key) != e.get(key)]
            raise AssertionError(f"{hint}: expected={len(oracle)} actual={len(actual)} differing_ids={different[:20]} "
                                 f"expected_digest={digest(oracle)} actual_digest={digest(actual)}")
    expected_words = tuple((row[0],) for row in oracle if row[3] is not None and "durablemark" in row[3])
    actual_words = sql(connection, "SELECT id FROM t WHERE MATCH(body) AGAINST('durablemark') ORDER BY id")
    assert actual_words == expected_words, ("FULLTEXT membership", len(expected_words), len(actual_words))
    exact = sql(connection, "SELECT /*+ NO_VECTOR_INDEX */ id,L2_DISTANCE(v,TO_VECTOR('[1,0,2,3]')) AS d FROM t ORDER BY d,id LIMIT 10")
    approximate = sql(connection, "SELECT id,L2_DISTANCE(v,TO_VECTOR('[1,0,2,3]')) AS d FROM t ORDER BY d,id LIMIT 10")
    assert len(exact) == len(approximate) == min(10, len(oracle))
    assert all(row[0] in expected and row[1] >= 0 for row in approximate)
    return digest(oracle)


def exercise(case, server, connection, expected, rng):
    count = len(expected)
    background = None
    background_errors = []
    if case == "commit":
        boundary = max(1, count // 3)
        sql(connection, "DELETE FROM t WHERE id>%s", (boundary,))
        expected = {key: row for key, row in expected.items() if key <= boundary}
    elif case == "loser":
        sql(connection, "BEGIN")
        sql(connection, "UPDATE t SET k=-k,body='uncommittedmark',payload=REPEAT('x',24000),v=TO_VECTOR('[9,9,9,9]') WHERE id<=20")
        sql(connection, "DELETE FROM t WHERE id>20 AND id<=40")
        sql(connection, "INSERT INTO t VALUES(%s,-1,1,'uncommittedmark',X'00',TO_VECTOR('[9,9,9,9]'),NULL,NULL)", (count + 1,))
        with server.connect() as other:
            sql(other, "ALTER SYSTEM CHECKPOINT")
    elif case == "savepoint":
        sql(connection, "BEGIN")
        sql(connection, "UPDATE t SET k=k+100000 WHERE id<=20")
        for key in range(1, min(20, count) + 1):
            expected[key][1] += 100000
        sql(connection, "SAVEPOINT stable")
        sql(connection, "UPDATE t SET body='rolledbackmark',payload=REPEAT('y',24000),v=TO_VECTOR('[8,8,8,8]') WHERE id<=20")
        sql(connection, "DELETE FROM t WHERE id>20 AND id<=40")
        sql(connection, "ROLLBACK TO SAVEPOINT stable")
        sql(connection, "COMMIT")
    elif case == "ddl":
        sql(connection, "TRUNCATE TABLE t")
        expected = {}
        sql(connection, "ALTER TABLE t ADD COLUMN added INT NOT NULL DEFAULT 7")
        sql(connection, "INSERT INTO t(id,k,amount,body,payload,v,stamp,meta) VALUES(1,3,1.2345,'durablemark newgeneration',X'00FF',TO_VECTOR('[1,0,2,3]'),'2040-02-03 04:05:06.000001',JSON_OBJECT('n',1))")
        expected[1] = [1, 3, decimal.Decimal("1.2345"), "durablemark newgeneration", "00FF", "[1,0,2,3]",
                       datetime.datetime(2040, 2, 3, 4, 5, 6, 1), "1"]
    elif case == "checkpoint":
        entered = threading.Event()

        def checkpoint():
            try:
                with server.connect() as other:
                    entered.set()
                    for _ in range(20):
                        sql(other, "ALTER SYSTEM CHECKPOINT")
            except pymysql.Error as error:
                background_errors.append(list(error.args))

        background = threading.Thread(target=checkpoint)
        background.start()
        assert entered.wait(10), "checkpoint worker did not connect"
        for _ in range(10):
            key = rng.randint(1, count)
            sql(connection, "UPDATE t SET k=k+1,amount=COALESCE(amount,0)+0.0001 WHERE id=%s", (key,))
            expected[key][1] += 1
            expected[key][2] = (expected[key][2] or decimal.Decimal(0)) + decimal.Decimal("0.0001")
    else:
        raise AssertionError(case)
    return expected, background, background_errors


def interrupt_ddl(case, server, rng):
    statements = {
        "truncate_inflight": "TRUNCATE TABLE t",
        "copy_inflight": "ALTER TABLE t MODIFY COLUMN body VARCHAR(1024) NULL, ALGORITHM=COPY",
        "index_inflight": "CREATE INDEX by_amount ON t(amount)",
    }
    entered = threading.Event()
    outcome = {"sql": statements[case], "acknowledged": False}

    def run():
        try:
            with server.connect() as worker:
                entered.set()
                sql(worker, statements[case])
                outcome["acknowledged"] = True
        except pymysql.Error as error:
            outcome["error"] = list(error.args)

    thread = threading.Thread(target=run)
    thread.start()
    assert entered.wait(10), "DDL worker did not connect"
    outcome["kill_delay_s"] = rng.choice((0, 0.0001, 0.001, 0.003, 0.01, 0.03, 0.1, 0.3, 0.7))
    time.sleep(outcome["kill_delay_s"])
    server.kill()
    thread.join(timeout=65)
    assert not thread.is_alive(), "DDL worker did not stop"
    if "error" in outcome:
        assert outcome["error"][0] in (2006, 2013), outcome
    return outcome


def verify_ddl(connection, case, outcome):
    if case == "copy_inflight":
        kind = sql(connection, "SELECT DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='test' AND TABLE_NAME='t' AND COLUMN_NAME='body'")[0][0].lower()
        assert kind in ("text", "varchar"), ("partial column publication", kind)
        if outcome["acknowledged"]:
            assert kind == "varchar", "acknowledged COPY schema was lost"
    elif case == "index_inflight":
        entries = sql(connection, "SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA='test' AND TABLE_NAME='t' AND INDEX_NAME='by_amount'")[0][0]
        assert entries in (0, 1), ("partial index publication", entries)
        if outcome["acknowledged"]:
            assert entries == 1, "acknowledged index was lost"


def write_after_recovery(connection, expected):
    key = max(expected, default=0) + 1
    sql(connection, "INSERT INTO t(id,k,amount,body,payload,v,stamp,meta) VALUES(%s,%s,1.2345,'durablemark recoveredwrite',X'00FF',TO_VECTOR('[1,0,2,3]'),'2040-02-03 04:05:06.000001',JSON_OBJECT('n',%s))", (key, key * 3, key))
    expected[key] = [key, key * 3, decimal.Decimal("1.2345"), "durablemark recoveredwrite", "00FF",
                     "[1,0,2,3]", datetime.datetime(2040, 2, 3, 4, 5, 6, 1), str(key)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=20)
    parser.add_argument("--rows", type=int, default=1500)
    parser.add_argument("--seed", type=int, default=12001)
    parser.add_argument("--cases", nargs="+", choices=CASES, default=list(CASES))
    parser.add_argument("--buffering", type=int, choices=(0, 1), default=1)
    args = parser.parse_args()
    binary = args.binary.resolve()
    root = args.output.resolve()
    if not binary.is_file() or args.rounds < 1 or not 40 <= args.rows <= 100000:
        parser.error("binary must exist, rounds must be positive, rows must be in 40..100000")
    if not root.is_relative_to(Path("/tmp/mydb").resolve()):
        parser.error("output must be below /tmp/mydb")
    root.mkdir(parents=True, exist_ok=False)
    report = {"binary": str(binary), "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
              "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              "platform": platform.platform(), "driver": importlib.metadata.version("PyMySQL"),
              "seed": args.seed, "rows": args.rows, "buffering": args.buffering, "results": []}
    for case in args.cases:
        for iteration in range(args.rounds):
            seed_value = args.seed + iteration
            artifact = root / f"{case}-{seed_value}"
            artifact.mkdir()
            server = Server(binary, artifact, args.buffering)
            connection, background = None, None
            result = {"case": case, "seed": seed_value, "status": "failed"}
            began = time.monotonic()
            try:
                connection = server.start()
                expected = seed(connection, args.rows)
                result["seed_digest"] = verify(connection, expected)
                if case.endswith("_inflight"):
                    result["ddl"] = interrupt_ddl(case, server, random.Random(seed_value))
                    errors = []
                else:
                    expected, background, errors = exercise(case, server, connection, expected, random.Random(seed_value))
                # The parent survives the owned server crash and retains the oracle.
                with (artifact / "expected.json").open("w") as receipt:
                    json.dump(expected, receipt, default=canonical, ensure_ascii=False)
                    receipt.flush()
                    os.fsync(receipt.fileno())
                if not case.endswith("_inflight"):
                    server.kill()
                connection.close()
                connection = None
                if background:
                    background.join(timeout=65)
                    assert not background.is_alive(), "checkpoint worker did not stop"
                    assert all(error[0] in (2006, 2013) for error in errors), errors
                connection = server.start()
                if case == "truncate_inflight":
                    count = sql(connection, "SELECT COUNT(*) FROM t")[0][0]
                    if result["ddl"]["acknowledged"]:
                        assert count == 0, "acknowledged TRUNCATE was lost"
                    if count == 0:
                        expected = {}
                if case.endswith("_inflight"):
                    verify_ddl(connection, case, result["ddl"])
                result["recovered_digest"] = verify(connection, expected)
                if case == "ddl":
                    assert sql(connection, "SELECT added FROM t") == ((7,),)
                write_after_recovery(connection, expected)
                (artifact / "expected-after-recovery-write.json").write_text(json.dumps(expected, default=canonical, ensure_ascii=False))
                # A second crash verifies recovery and a fresh acknowledged write.
                server.kill()
                connection.close()
                connection = server.start()
                result["second_recovery_digest"] = verify(connection, expected)
                if case.endswith("_inflight"):
                    verify_ddl(connection, case, result["ddl"])
                result.update(status="passed", crashes=2, background_errors=errors)
            except Exception:
                result["error"] = traceback.format_exc()
            finally:
                server.close(abort=result["status"] != "passed")
                if connection:
                    connection.close()
                if background and background.is_alive():
                    background.join(timeout=65)
                result["elapsed_s"] = round(time.monotonic() - began, 3)
                report["results"].append(result)
                (root / "results.json").write_text(json.dumps(report, indent=2))
            print(json.dumps(result), flush=True)
            if result["status"] != "passed":
                return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
