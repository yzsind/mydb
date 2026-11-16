#!/usr/bin/env python3
"""Rehearse selected complete public examples on fresh local MyDB instances."""

import argparse
import json
import os
import re
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def blocks(path):
    return re.findall(
        r"^```sql\n(.*?)^```", (ROOT / path).read_text(), re.MULTILINE | re.DOTALL
    )


def port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--binary", type=Path, default=ROOT / "target/debug/mydb")
    p.add_argument("--output", type=Path)
    args = p.parse_args()
    import pymysql
    from jsonschema import Draft202012Validator
    from jsonschema.exceptions import ValidationError

    Path("/tmp/mydb").mkdir(exist_ok=True)
    out = args.output or Path(
        tempfile.mkdtemp(prefix="docs-examples-", dir="/tmp/mydb")
    )
    out.mkdir(parents=True, exist_ok=True)
    processes = []
    logs = []
    conns = []
    checks = []
    secret = "docs-rehearsal-only-password"
    sql_port, replica_port, rest_port, repl_a, repl_b = [port() for _ in range(5)]

    def start(name, sqlport, **extra):
        env = os.environ.copy()
        for key in list(env):
            if key.startswith("MYDB_"):
                del env[key]
        env.update(
            MYDB_MEMORY_TARGET="512M",
            MYDB_ROOT_PASSWORD=secret,
            MYDB_REPL_AUTH_TOKEN="docs-local-replication-token",
            **extra,
        )
        log = (out / (name + ".log")).open("w")
        logs.append(log)
        proc = subprocess.Popen(
            [
                str(args.binary.resolve()),
                "--data-dir",
                str(out / name),
                "--port",
                str(sqlport),
            ],
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        processes.append(proc)
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                raise RuntimeError(f"{name} exited: {out / (name + '.log')}")
            try:
                c = pymysql.connect(
                    host="127.0.0.1",
                    port=sqlport,
                    user="root",
                    password=secret,
                    ssl={"check_hostname": False},
                    autocommit=True,
                )
                conns.append(c)
                return c
            except (pymysql.Error, OSError):
                time.sleep(0.2)
        raise TimeoutError(f"{name} startup")

    def query(c, sql):
        with c.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()

    def script(c, sql):
        # Selected scripts have no delimiter directives or semicolons in literals.
        sql = re.sub(r"--[^\n]*", "", sql)
        for statement in sql.split(";"):
            statement = re.sub(
                r"^\s*--[^\n]*", "", statement, flags=re.MULTILINE
            ).strip()
            if statement:
                query(c, statement)

    def wait(fn):
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            if fn():
                return
            time.sleep(0.1)
        raise TimeoutError("Progress predicate was not reached")

    try:
        c = start(
            "a",
            sql_port,
            MYDB_ROLE="master",
            MYDB_REPL_PORT=str(repl_a),
            MYDB_REST_ENABLED="ON",
            MYDB_REST_TLS_MODE="disabled",
            MYDB_REST_PORT=str(rest_port),
        )
        assert "default 0.0.0.0" in subprocess.check_output(
            [str(args.binary.resolve()), "--help"], text=True
        )
        checks.append("startup-default-host")
        for _ in range(2):
            script(c, blocks("docs/statement/examples.md")[0])
            script(c, blocks("docs/statement/ddl/alter-table.md")[0])
        checks.append("shared-setup-alter-reset-twice")
        for sql in blocks("docs/json-table.md")[1:]:
            if sql.lstrip().startswith("JSON_TABLE"):
                continue
            script(c, sql)
        assert query(
            c,
            "SELECT v FROM JSON_TABLE('[1,2]', '$[*]' COLUMNS(v INT PATH '$')) AS j ORDER BY v",
        ) == ((1,), (2,))
        assert query(
            c,
            "SELECT COUNT(*) FROM JSON_TABLE('[]', '$[*]' COLUMNS(v INT PATH '$')) AS j",
        ) == ((0,),)
        assert query(
            c, "SELECT v FROM JSON_TABLE('[null]', '$[*]' COLUMNS(v INT PATH '$')) AS j"
        ) == ((None,),)
        checks.append("json-table-examples")
        for sql in blocks("docs/operations-troubleshooting.md"):
            script(c, sql)
        checks.append("diagnostic-columns")
        query(
            c,
            "CREATE TABLE test.doc_vectors(id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)",
        )
        query(
            c,
            "INSERT INTO test.doc_vectors VALUES(1,STRING_TO_VECTOR('[1,0,0]')),(2,STRING_TO_VECTOR('[0,1,0]'))",
        )
        query(
            c,
            "CREATE INDEX doc_hnsw ON test.doc_vectors(emb) USING HNSW WITH(metric=cosine)",
        )
        inputs = out / "queries.jsonl"
        inputs.write_text(
            json.dumps({"id": 0, "vector": [1, 0, 0], "neighbors": [1]}) + "\n"
        )
        subprocess.run(
            [
                sys.executable,
                str(ROOT / "scripts/vector_query_bench.py"),
                "--engine",
                "mydb",
                "--port",
                str(sql_port),
                "--user",
                "root",
                "--database",
                "test",
                "--table",
                "doc_vectors",
                "--queries",
                str(inputs),
                "--output",
                str(out / "vector-results.json"),
                "--k",
                "1",
                "--clients",
                "2",
                "--duration",
                "1",
                "--rounds",
                "1",
            ],
            env=dict(os.environ, VECTOR_BENCH_PASSWORD=secret),
            check=True,
        )
        assert json.loads((out / "vector-results.json").read_text())["recall_at_k"] == 1
        checks.append("public-vector-query-replay")
        script(c, blocks("docs/security-deployment.md")[0])
        with pymysql.connect(
            host="127.0.0.1",
            port=sql_port,
            user="app_reader",
            password="replace-with-a-unique-password",
            ssl={"check_hostname": False},
            autocommit=True,
        ) as app:
            query(c, "CREATE TABLE test.priv_check(id INT)")
            query(app, "SELECT * FROM test.priv_check")
            try:
                query(app, "INSERT INTO test.priv_check VALUES(1)")
            except pymysql.Error:
                pass
            else:
                raise AssertionError("reader wrote data")
        checks.append("least-privilege")
        query(c, "CREATE TABLE test.retained(id INT PRIMARY KEY)")
        query(c, "INSERT INTO test.retained VALUES(7)")
        query(c, "DROP TABLE test.retained")
        name = query(
            c,
            "SELECT RECYCLE_NAME FROM information_schema.MYDB_RECYCLEBIN WHERE ORIGIN_SCHEMA='test' AND ORIGIN_TABLE='retained'",
        )[0][0]
        query(c, f"RESTORE TABLE `{name}` TO test.restored")
        assert query(c, "SELECT * FROM test.restored") == ((7,),)
        query(c, "DROP TABLE test.restored")
        name = query(
            c,
            "SELECT RECYCLE_NAME FROM information_schema.MYDB_RECYCLEBIN WHERE ORIGIN_TABLE='restored'",
        )[0][0]
        query(c, f"PURGE RECYCLEBIN `{name}`")
        checks.append("recycle-show-restore-purge")
        keyrow = query(c, "CREATE API KEY 'docs-smoke'")
        key = next(
            v
            for row in keyrow
            for v in row
            if isinstance(v, str) and v.startswith("mydb_ak_")
        )
        spec = json.loads((ROOT / "docs/rest-openapi.json").read_text())

        def validate(name, value):
            Draft202012Validator(
                {
                    "$ref": "#/components/schemas/" + name,
                    "components": spec["components"],
                }
            ).validate(value)

        def http(path, body=None, accept="application/json", method=None):
            r = urllib.request.Request(
                f"http://127.0.0.1:{rest_port}" + path,
                data=None if body is None else json.dumps(body).encode(),
                headers={
                    "Authorization": "Bearer " + key,
                    "Content-Type": "application/json",
                    "Accept": accept,
                },
                method=method,
            )
            try:
                with urllib.request.urlopen(r, timeout=30) as res:
                    return res.status, res.read()
            except urllib.error.HTTPError as e:
                return e.code, e.read()

        for request in [
            {"sql": "SELECT 1 AS n, CAST(1.25 AS DECIMAL(4,2)) AS d"},
            {
                "sql": None,
                "statements": [
                    {
                        "sql": "SELECT ?",
                        "params": [{"type": "uint64", "value": "18446744073709551615"}],
                    }
                ],
                "params": [],
            },
        ]:
            validate("SqlRequest", request)
            status, data = http("/api/v1/sql", request)
            assert status == 200, (status, data)
            validate("SqlResponse", json.loads(data))
        for request in [
            {"statements": [{"sql": "SELECT 1"}], "params": [2]},
            {"sql": "SELECT ?", "params": [9007199254740992]},
        ]:
            try:
                validate("SqlRequest", request)
            except ValidationError as error:
                assert error.message
            else:
                raise AssertionError("schema accepted invalid request")
            status, data = http("/api/v1/sql", request)
            assert status >= 400, (status, data)
        for request in [
            {"sql": "SELECT 1"},
            {"sql": "CREATE TABLE test.rest_ok(id INT)"},
        ]:
            status, data = http("/api/v1/sql", request, "application/x-ndjson")
            events = [json.loads(l) for l in data.splitlines()]
            assert status == 200
            for event in events:
                validate("StreamEvent", event)
            assert events[-1]["type"] == "complete"
        status, data = http("/api/v1/sessions", {})
        assert status == 201
        session = json.loads(data)
        validate("Session", session)
        for path in ["/api/v1/sessions/" + session["id"], "/api/v1/status"]:
            status, data = http(path)
            assert status == 200
            validate("Session" if "/sessions/" in path else "Status", json.loads(data))
        http("/api/v1/sessions/" + session["id"], method="DELETE")
        checks.append("openapi-live-json-ndjson-requests-session-status")
        d = start("persist", port(), MYDB_MAX_CONNECTIONS="321")
        assert int(query(d, "SELECT @@GLOBAL.max_connections")[0][0]) == 321
        query(d, "SET PERSIST max_connections=123")
        d.close()
        processes[-1].terminate()
        processes[-1].wait(timeout=20)
        d = start("persist", port(), MYDB_MAX_CONNECTIONS="321")
        assert int(query(d, "SELECT @@GLOBAL.max_connections")[0][0]) == 123
        query(d, "RESET PERSIST max_connections")
        d.close()
        processes[-1].terminate()
        processes[-1].wait(timeout=20)
        d = start("persist", port(), MYDB_MAX_CONNECTIONS="321")
        assert int(query(d, "SELECT @@GLOBAL.max_connections")[0][0]) == 321
        checks.append("persisted-global-overrides-environment-until-reset")
        query(c, "CREATE TABLE test.repl_check(id INT PRIMARY KEY,value INT)")
        query(c, "INSERT INTO test.repl_check VALUES(1,10)")
        b = start(
            "b",
            replica_port,
            MYDB_ROLE="replica",
            MYDB_REPL_PORT=str(repl_b),
            MYDB_REPL_MASTER_HOST="127.0.0.1",
            MYDB_REPL_MASTER_PORT=str(repl_a),
        )
        query(c, "INSERT INTO test.repl_check VALUES(2,20)")

        def status(conn, sql):
            with conn.cursor(pymysql.cursors.DictCursor) as cur:
                cur.execute(sql)
                return cur.fetchone()

        target = int(status(c, "SHOW MYDB MASTER STATUS")["Durable_LSN"])
        wait(
            lambda: (
                int(status(b, "SHOW MYDB REPLICA STATUS")["Durable_Applied_LSN"])
                >= target
            )
        )
        assert query(b, "SELECT * FROM test.repl_check ORDER BY id") == (
            (1, 10),
            (2, 20),
        )
        fence = status(c, "STOP MASTER WRITES WAIT 30")
        query(
            b,
            f"PROMOTE TO MASTER AT LSN {fence['Fence_LSN']} FENCE_TOKEN='{fence['Fence_Token']}' WAIT 30",
        )
        query(c, f"DEMOTE TO REPLICA WITH MASTER='127.0.0.1:{repl_b}'")
        query(b, "INSERT INTO test.repl_check VALUES(3,30)")
        target = int(status(b, "SHOW MYDB MASTER STATUS")["Durable_LSN"])
        wait(
            lambda: (
                int(status(c, "SHOW MYDB REPLICA STATUS")["Durable_Applied_LSN"])
                >= target
            )
        )
        assert query(c, "SELECT COUNT(*) FROM test.repl_check") == ((3,),)
        checks.append("native-seed-watermark-promotion-old-master-rejoin")
        print(json.dumps({"checks": checks, "output": str(out)}, indent=2))
    finally:
        (out / "checks.json").write_text(json.dumps(checks, indent=2) + "\n")
        for c in conns:
            if c.open:
                c.close()
        for proc in reversed(processes):
            if proc.poll() is None:
                proc.send_signal(signal.SIGTERM)
                try:
                    proc.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
        for log in logs:
            log.close()


if __name__ == "__main__":
    main()
