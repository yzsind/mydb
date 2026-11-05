#!/usr/bin/env python3
"""Owned-loopback REST SQL release checks. Evidence stays under /tmp/mydb.

Uses the MySQL CLI for TLS account administration and Python's HTTP client.
Never prints API key credentials. Pass an explicit candidate executable.
"""
from __future__ import annotations
import argparse
import concurrent.futures
import http.client
import json
from pathlib import Path
import socket
import ssl
import subprocess
import time


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--output", type=Path, default=Path("/tmp/mydb/rest/protocol"))
    parser.add_argument("--tls", action="store_true", help="Test native HTTPS and HTTP/2")
    args = parser.parse_args()
    root = args.output.resolve()
    if not root.is_relative_to(Path("/tmp/mydb").resolve()):
        parser.error("--output must be under /tmp/mydb")
    root.mkdir(parents=True, exist_ok=True)
    sql_port, rest_port = free_port(), free_port()
    data = root / f"data-{time.time_ns()}"
    log = open(root / "server.log", "ab")
    proc = None
    evidence = []

    def connection(timeout=30):
        if args.tls:
            return http.client.HTTPSConnection("127.0.0.1", rest_port, timeout=timeout,
                                               context=ssl._create_unverified_context())
        return http.client.HTTPConnection("127.0.0.1", rest_port, timeout=timeout)

    def start():
        nonlocal proc
        proc = subprocess.Popen([
            str(args.binary.resolve()), "--data-dir", str(data), "--host", "127.0.0.1",
            "--port", str(sql_port), "--rest-enabled", "ON", "--rest-port", str(rest_port),
            "--rest-tls-mode", "required" if args.tls else "disabled",
        ], stdout=log, stderr=log)
        for _ in range(600):
            if proc.poll() is not None:
                raise RuntimeError("server exited; inspect server.log")
            try:
                conn = connection(timeout=1)
                conn.request("GET", "/health/ready")
                response = conn.getresponse()
                response.read()
                conn.close()
                if response.status == 200:
                    return
            except OSError:
                pass
            time.sleep(.05)
        raise TimeoutError("REST readiness deadline")

    def sql(text, user="root", password="root"):
        result = subprocess.run([
            "mysql", "--host=127.0.0.1", f"--port={sql_port}", "--user=" + user, "--password=" + password,
            "--ssl-mode=REQUIRED", "--batch", "--raw", "--skip-column-names", "-e", text,
        ], capture_output=True, text=True, timeout=30)
        if result.returncode:
            raise RuntimeError("MySQL administration failed: " + result.stderr)
        return result.stdout.strip().splitlines()

    def call(path, payload=None, method="POST", token=None, headers=None, raw=None):
        conn = connection()
        request_headers = {"Content-Type": "application/json"}
        if token is not None:
            request_headers["Authorization"] = "Bearer " + token
        request_headers.update(headers or {})
        conn.request(method, path, body=raw if raw is not None else json.dumps(payload) if payload is not None else None,
                     headers=request_headers)
        response = conn.getresponse()
        body = response.read()
        status = response.status
        conn.close()
        if request_headers.get("Accept") == "application/x-ndjson":
            value = [json.loads(line) for line in body.splitlines()]
        else:
            value = json.loads(body) if body else None
        return status, value

    def check(name, condition):
        assert condition, name
        evidence.append(name)
        print("PASS", name, flush=True)

    def stop():
        nonlocal proc
        if proc is not None and proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=40)
        proc = None

    try:
        start()
        sql("DROP API KEY 'release'") if data.exists() and sql("SHOW API KEYS") else None
        created = sql("CREATE API KEY 'release'")[0].split("\t")
        token = created[6]
        check("default does not expire", created[4] == "NULL")
        check("complete AK reveal", sql("SHOW API KEY 'release'")[0].split("\t")[6] == token)
        check("authentication required", call("/api/v1/sql", {"sql": "SELECT 1"})[0] == 401)
        status, result = call("/api/v1/sql", {"sql": "SELECT ? AS u, ? AS d, ? AS j", "params": [
            {"type": "uint64", "value": "18446744073709551615"},
            {"type": "decimal", "value": "12345678901234567890.123456789"},
            {"type": "json", "value": "null"},
        ]}, token=token)
        check("lossless parameter execution", status == 200 and result["results"][0]["rows"] == [[
            "18446744073709551615", "12345678901234567890.123456789", "null"]])
        metadata = {"uint64": result["results"][0]["columns"][0]}
        (root / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
        check("unsigned result metadata", metadata["uint64"]["unsigned"])
        check("duplicate JSON rejected", call("/api/v1/sql", token=token,
              raw='{"sql":"SELECT 1","sql":"SELECT 2"}')[0] == 400)
        check("multiple SQL rejected", call("/api/v1/sql", {"sql": "SELECT 1; SELECT 2"}, token=token)[0] == 422)
        check("REST cannot reveal AK", call("/api/v1/sql", {"sql": "SHOW API KEY 'release'"}, token=token)[0] in (401, 403))
        status, result = call("/api/v1/sessions", {"database": "test"}, token=token)
        check("session created", status == 201)
        session = result["id"]
        endpoint = f"/api/v1/sessions/{session}/sql"
        for command in ["CREATE TABLE rest_release (id BIGINT PRIMARY KEY)", "BEGIN",
                        "INSERT INTO rest_release VALUES (1)", "ROLLBACK"]:
            check(command.split()[0] + " SQL", call(endpoint, {"sql": command}, token=token)[0] == 200)
        status, result = call(endpoint, {"atomic": True, "statements": [
            {"sql": "INSERT INTO rest_release VALUES (2)"},
            {"sql": "INSERT INTO rest_release VALUES (2)"},
        ]}, token=token)
        check("atomic rollback confirmed", status == 409 and result["error"]["rollback_confirmed"])
        status, events = call(endpoint, {"sql": "SELECT COUNT(*) FROM rest_release"}, token=token,
                              headers={"Accept": "application/x-ndjson"})
        check("NDJSON complete", status == 200 and events[-1]["type"] == "complete")
        check("transaction changes rolled back", next(e for e in events if e["type"] == "rows")["rows"] == [["0"]])
        request_id = "release_cancel_request_0001"
        with concurrent.futures.ThreadPoolExecutor() as pool:
            pending = pool.submit(call, endpoint, {"sql": "SELECT SLEEP(10)"}, token=token,
                                  headers={"X-Request-ID": request_id})
            for _ in range(100):
                if call(f"/api/v1/requests/{request_id}", method="GET", token=token)[0] == 200:
                    break
                time.sleep(.01)
            check("concurrent session rejected", call(endpoint, {"sql": "SELECT 1"}, token=token)[0] == 409)
            check("cancel accepted", call(f"/api/v1/requests/{request_id}/cancel", token=token)[0] == 202)
            check("cancel result returned", pending.result(timeout=5)[1]["error"] is not None)
        check("session close", call(f"/api/v1/sessions/{session}", method="DELETE", token=token)[0] == 204)
        status, result = call("/api/v1/sql", {"sql": "SELECT SLEEP(2)", "timeout_ms": 50}, token=token)
        check("deadline returns structured error", status == 504 and result["error"] is not None)
        status, result = call("/api/v1/sql", {"sql": "SELECT ? AS b, ? AS dt, ? AS vec", "params": [
            {"type": "binary", "value": "AP8="},
            {"type": "datetime", "value": "2026-11-04 01:02:03.123456"},
            {"type": "vector", "value": "[1,2,3]", "datatype": "FLOAT32", "dimension": 3},
        ]}, token=token)
        check("binary temporal and vector parameters", status == 200 and
              result["results"][0]["rows"][0][0] == "AP8=" and
              result["results"][0]["rows"][0][1] == "2026-11-04 01:02:03.123456" and
              json.loads(result["results"][0]["rows"][0][2]) == [1, 2, 3])
        metadata["vector"] = result["results"][0]["columns"][2]
        (root / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
        check("native vector result metadata", metadata["vector"]["vector_type"] == {"datatype":"FLOAT32", "dimension":3})
        sql("CREATE USER 'rest_app'@'127.0.0.1' IDENTIFIED BY 'pw'")
        sql("GRANT SELECT ON test.* TO 'rest_app'@'127.0.0.1'")
        app_key = sql("CREATE API KEY 'own'", user="rest_app", password="pw")[0].split("\t")[6]
        check("ordinary account manages own AK", sql("SHOW API KEY 'own'", user="rest_app", password="pw")[0].split("\t")[6] == app_key)
        check("account privileges enforced", call("/api/v1/sql", {"sql": "DELETE FROM test.rest_release"}, token=app_key)[0] == 403)
        sql("CREATE PROCEDURE test.rest_definer() SQL SECURITY DEFINER SELECT COUNT(*) FROM test.rest_release")
        sql("GRANT EXECUTE ON PROCEDURE test.rest_definer TO 'rest_app'@'127.0.0.1'")
        check("DEFINER retains credential provenance", call("/api/v1/sql", {"database": "test", "sql": "CALL rest_definer()"}, token=app_key)[0] == 200)
        sql("REVOKE SELECT ON test.* FROM 'rest_app'@'127.0.0.1'")
        check("privilege revocation is live", call("/api/v1/sql", {"sql": "SELECT * FROM test.rest_release"}, token=app_key)[0] == 403)
        sql("DROP USER 'rest_app'@'127.0.0.1'")
        check("owner deletion invalidates AK", call("/api/v1/sql", {"sql": "SELECT 1"}, token=app_key)[0] == 401)
        if args.tls:
            config = f'url = "https://127.0.0.1:{rest_port}/api/v1/sql"\nheader = "Authorization: Bearer {token}"\n'
            r = subprocess.run(["curl", "--http2", "--insecure", "--silent", "--show-error", "--config", "-",
                                "--header", "Content-Type: application/json", "--data-binary", '{"sql":"SELECT 1"}',
                                "--write-out", "\n%{http_version}"], input=config, capture_output=True, text=True, check=True)
            payload, version = r.stdout.rsplit("\n", 1)
            check("native HTTPS HTTP/2", version.startswith("2") and json.loads(payload)["error"] is None)
        stop()
        start()
        check("AK persists across restart", sql("SHOW API KEY 'release'")[0].split("\t")[6] == token)
        check("REST works after recovery", call("/api/v1/sql", {"sql": "SELECT 1"}, token=token)[0] == 200)
        sql("DROP API KEY 'release'")
        check("revocation", call("/api/v1/sql", {"sql": "SELECT 1"}, token=token)[0] == 401)
        (root / "results.json").write_text(json.dumps({"passed": evidence}, indent=2) + "\n")
    finally:
        stop()
        log.close()


if __name__ == "__main__":
    main()
