#!/usr/bin/env python3
"""Local HTTP SQL release gate. Uses owned loopback services and fresh databases.

Requires pymysql, mysql-connector-python and cryptography. All evidence stays
under --output (default /tmp/mydb/http-functions/protocol). No public endpoint
is contacted. The server binary is explicit so baseline and candidate cannot
be confused.
"""
from __future__ import annotations
import argparse
import concurrent.futures
import datetime
from decimal import Decimal
import gzip
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import socket
import socketserver
import ssl
import threading
import time
import zlib

import mysql.connector
import pymysql
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from test_native_readiness import Servers, free_port, query, catch_up


class Endpoint:
    def __init__(self, address="127.0.0.1", context=None):
        self.lock = threading.Lock()
        self.requests = []
        self.release = threading.Event()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def handle(self):
                try:
                    super().handle()
                except (BrokenPipeError, ConnectionResetError):
                    pass

            def handle_call(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                with owner.lock:
                    owner.requests.append({"method": self.command, "path": self.path,
                                           "body": body, "headers": list(self.headers.items())})
                if self.path.startswith("/hold"):
                    owner.release.wait(10)
                if self.path == "/drop":
                    self.close_connection = True
                    return
                payload = json.dumps({"method": self.command, "body": body.decode("utf-8", "replace")}).encode()
                status = 200
                headers = {}
                if self.path == "/gzip-bomb":
                    payload = gzip.compress(b"x" * 100000)
                    headers["Content-Encoding"] = "gzip"
                if self.path == "/deflate":
                    payload = zlib.compress(b"deflated")
                    headers["Content-Encoding"] = "deflate"
                if self.path == "/raw-encoding":
                    payload = b"raw"
                    headers["Content-Encoding"] = "custom"
                if self.path == "/oversized-header":
                    headers["X-Large"] = "x" * 70000
                if self.path == "/invalid-utf8":
                    payload = b"\xff\x00"
                if self.path == "/bad-gzip":
                    payload = b"not gzip"
                    headers["Content-Encoding"] = "gzip"
                self.send_response(status)
                for key, value in headers.items():
                    self.send_header(key, value)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                if self.command != "HEAD":
                    try:
                        self.wfile.write(payload)
                    except (BrokenPipeError, ConnectionResetError):
                        pass

            do_GET = do_POST = do_PUT = do_PATCH = do_DELETE = do_OPTIONS = do_HEAD = handle_call

        if ":" in address:
            class Server(ThreadingHTTPServer):
                address_family = socket.AF_INET6
        else:
            Server = ThreadingHTTPServer
        self.server = Server((address, 0), Handler)
        self.server.daemon_threads = True
        if context:
            self.server.socket = context.wrap_socket(self.server.socket, server_side=True)
        self.port = self.server.server_address[1]
        self.url = f"{'https' if context else 'http'}://{'[' + address + ']' if ':' in address else address}:{self.port}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def count(self):
        with self.lock:
            return len(self.requests)

    def close(self):
        self.release.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)


class H2Endpoint:
    """Minimal fixed-response fixture for RFC 9113 frames and ALPN negotiation.

    Response HPACK is the static-table :status=200 entry (RFC 7541 index 8).
    Request HPACK is deliberately opaque: this fixture tests negotiation and
    bounded response consumption, not a general-purpose HTTP/2 server.
    """
    def __init__(self, root):
        context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(root/"leaf-False.pem",root/"key-False.pem")
        context.set_alpn_protocols(["h2"])
        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                def exact(stream,n):
                    data=b""
                    while len(data)<n:
                        part=stream.recv(n-len(data))
                        if not part: raise EOFError()
                        data+=part
                    return data
                def frame(kind,flags,stream,payload=b""):
                    return len(payload).to_bytes(3,"big")+bytes([kind,flags])+stream.to_bytes(4,"big")+payload
                try:
                    with context.wrap_socket(self.request,server_side=True) as stream:
                        stream.settimeout(3)
                        assert exact(stream,24)==b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
                        stream.sendall(frame(4,0,0))
                        while True:
                            head=exact(stream,9);length=int.from_bytes(head[:3],"big")
                            if length>65536: return
                            kind,flags=head[3:5];identifier=int.from_bytes(head[5:],"big")&0x7fffffff
                            payload=exact(stream,length)
                            if kind==4 and not flags&1:
                                stream.sendall(frame(4,1,0))
                            elif kind==6 and not flags&1:
                                stream.sendall(frame(6,1,0,payload))
                            elif kind in (1,9) and flags&4:
                                stream.sendall(frame(1,4,identifier,b"\x88")+frame(0,1,identifier,b'{"h2":true}'))
                except (EOFError,OSError):
                    pass
        class Server(socketserver.ThreadingTCPServer):
            daemon_threads=True
            allow_reuse_address=True
        self.server=Server(("127.0.0.1",0),Handler)
        self.url=f"https://localhost:{self.server.server_address[1]}"
        self.release=threading.Event()
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True)
        self.thread.start()
    def close(self):
        self.server.shutdown();self.server.server_close();self.thread.join(2)


def q(connection, sql, params=None):
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        return cursor.fetchall() if cursor.description else cursor.rowcount


def response(connection, url, options=None):
    return json.loads(q(connection, "SELECT HTTP_GET(%s,%s)", (url, json.dumps(options or {})))[0][0])


def denied(fn, codes):
    try:
        fn()
    except pymysql.MySQLError as error:
        assert (error.args[0] & 0xffff) in codes, (error.args, codes)
        return error.args[0] & 0xffff
    raise AssertionError("expected SQL failure")


def wait_for(check, timeout=5):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if check():
            return
        time.sleep(0.01)
    raise AssertionError("condition did not become true")


def metrics(connection):
    return {name: int(value) for name, value in q(connection, "SHOW HTTP STATUS")}


def certificates(root):
    now = datetime.datetime.now(datetime.timezone.utc)
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "HTTP SQL test CA")])
    ca = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
          .serial_number(x509.random_serial_number()).not_valid_before(now-datetime.timedelta(days=1))
          .not_valid_after(now+datetime.timedelta(days=1)).add_extension(x509.BasicConstraints(ca=True, path_length=None), True)
          .sign(key, hashes.SHA256()))
    ca_file = root / "ca.pem"
    ca_file.write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    contexts = []
    for expired in (False, True):
        leaf_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        leaf_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
        leaf = (x509.CertificateBuilder().subject_name(leaf_name).issuer_name(name).public_key(leaf_key.public_key())
                .serial_number(x509.random_serial_number()).not_valid_before(now-datetime.timedelta(days=2))
                .not_valid_after(now+datetime.timedelta(days=-1 if expired else 1))
                .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost")]), False)
                .sign(key, hashes.SHA256()))
        cert_file, key_file = root / f"leaf-{expired}.pem", root / f"key-{expired}.pem"
        cert_file.write_bytes(leaf.public_bytes(serialization.Encoding.PEM))
        key_file.write_bytes(leaf_key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(cert_file, key_file)
        contexts.append(context)
    return ca_file, contexts


def run(binary, root, results):
    root.mkdir(parents=True, exist_ok=True)
    results["binary_sha256"]=hashlib.sha256(binary.read_bytes()).hexdigest()
    servers = Servers(binary, root)
    endpoints = []
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=16)
    try:
        endpoint = Endpoint(); endpoints.append(endpoint)
        ipv6 = Endpoint("::1"); endpoints.append(ipv6)
        ca, contexts = certificates(root)
        tls = Endpoint(context=contexts[0]); expired = Endpoint(context=contexts[1])
        endpoints.extend([tls, expired])
        h2=H2Endpoint(root);endpoints.append(h2)
        tls_url = f"https://localhost:{tls.port}"
        expired_url = f"https://localhost:{expired.port}"
        policy_path = root / "policy.json"
        policy = {"version": 1, "ca_bindings": {tls_url: str(ca), tls.url: str(ca), expired_url: str(ca),h2.url:str(ca)}}
        policy_path.write_text(json.dumps(policy))
        port = free_port()
        process, admin = servers.start("primary", port, {"MYDB_HTTP_LOCAL_CONFIG": str(policy_path)})
        print("HTTP protocol gate: TLS and payload boundaries", flush=True)
        results["ipv6"] = response(admin, ipv6.url + "/echo")["status"] == 200
        results["tls_private_ca"] = response(admin, tls_url + "/echo")["status"] == 200
        h2_response=response(admin,h2.url+"/echo")
        results["http2_alpn"] = h2_response["http_version"]=="HTTP/2.0" and json.loads(h2_response["body"])["h2"]
        results["tls_hostname_rejection"] = denied(lambda: response(admin, tls.url + "/echo"), {50108})
        results["tls_expiry_rejection"] = denied(lambda: response(admin, expired_url + "/echo"), {50108})
        results["deflate"] = response(admin, endpoint.url + "/deflate")["body"] == "deflated"
        raw = response(admin, endpoint.url + "/raw-encoding")
        results["unknown_content_encoding"] = raw["body"] == "raw" and not raw["content_decoded"]
        results["compressed_size_limit"] = denied(lambda: response(admin, endpoint.url+"/gzip-bomb", {"max_response_bytes": 1000, "on_error": "return"}), {50104})
        results["header_limit"] = denied(lambda: response(admin, endpoint.url+"/oversized-header"), {50104, 50110, 50111})
        results["strict_utf8"] = response(admin, endpoint.url+"/invalid-utf8", {"response_encoding":"utf8", "on_error":"return"})["error"]["code"] == "ER_HTTP_INVALID_RESPONSE"
        results["bad_compression"] = response(admin, endpoint.url+"/bad-gzip", {"on_error":"return"})["error"]["code"] == "ER_HTTP_INVALID_RESPONSE"
        before = endpoint.count()
        denied(lambda: q(admin, "SELECT HTTP_POST(%s,'once')", (endpoint.url+"/drop",)), {50111})
        results["no_retry_after_disconnect"] = endpoint.count() == before + 1

        number="123456789012345678901234567890.12345678901234567890"
        for sql,params in [
            ("SELECT HTTP_POST(%s,JSON_OBJECT('n',CAST(%s AS DECIMAL(50,20))))",(endpoint.url+"/echo",number)),
            ("SELECT HTTP_REQUEST('POST',%s,JSON_OBJECT('json',JSON_OBJECT('n',CAST(%s AS DECIMAL(50,20)))))",(endpoint.url+"/echo",number)),
            ("SELECT HTTP_REQUEST('POST',%s,%s)",(endpoint.url+"/echo",'{"json":{"n":'+number+'}}')),
        ]:
            outer=json.loads(q(admin,sql,params)[0][0]);sent=json.loads(outer["body"])["body"]
            assert json.loads(sent,parse_float=Decimal)["n"]==Decimal(number),sent
        results["exact_json_decimal_numbers"] = True

        print("HTTP protocol gate: binary prepared statements and security context", flush=True)
        prepared = mysql.connector.connect(host="127.0.0.1", port=port, user="root", password="root", database="test", autocommit=True, use_pure=True)
        try:
            cursor = prepared.cursor(prepared=True)
            before = endpoint.count()
            for value in ["first", "second", "中文 😀"]:
                cursor.execute("SELECT HTTP_POST(%s,%s)", (endpoint.url+"/echo", value))
                result = json.loads(cursor.fetchone()[0])
                assert json.loads(result["body"])["body"] == value
            results["binary_prepared_each_execution"] = endpoint.count() == before + 3
            cursor.close()
        finally:
            prepared.close()

        q(admin, "CREATE USER http_invoker IDENTIFIED BY 'x'")
        q(admin, "CREATE FUNCTION test.http_definer(p TEXT) RETURNS JSON SQL SECURITY DEFINER RETURN HTTP_GET(p)")
        q(admin, "CREATE FUNCTION test.http_invoker(p TEXT) RETURNS JSON SQL SECURITY INVOKER RETURN HTTP_GET(p)")
        q(admin, "GRANT EXECUTE ON FUNCTION test.http_definer TO http_invoker")
        q(admin, "GRANT EXECUTE ON FUNCTION test.http_invoker TO http_invoker")
        reader = pymysql.connect(host="127.0.0.1", port=port, user="http_invoker", password="x", autocommit=True)
        try:
            assert json.loads(q(reader, "SELECT test.http_definer(%s)", (endpoint.url+"/echo",))[0][0])["status"] == 200
            denied(lambda: q(reader, "SELECT test.http_invoker(%s)", (endpoint.url+"/echo",)), {50101})
            q(admin, "GRANT HTTP_USE ON *.* TO http_invoker")
            assert json.loads(q(reader, "SELECT test.http_invoker(%s)", (endpoint.url+"/echo",))[0][0])["status"] == 200
            q(admin, "REVOKE HTTP_USE ON *.* FROM http_invoker")
            denied(lambda: q(reader, "SELECT test.http_invoker(%s)", (endpoint.url+"/echo",)), {50101})
            results["definer_invoker_revocation"] = True
        finally:
            reader.close()

        print("HTTP protocol gate: relational SQL and diagnostics", flush=True)
        q(admin,"CREATE TABLE runtime_inputs(id INT PRIMARY KEY,url TEXT)")
        for identifier in (1,2,3):
            q(admin,"INSERT INTO runtime_inputs VALUES(%s,%s)",(identifier,endpoint.url+"/echo"))
        matrix = [
            ("SELECT a.id FROM runtime_inputs a JOIN runtime_inputs b ON a.id=b.id AND CAST(JSON_EXTRACT(HTTP_GET(a.url),'$.status') AS SIGNED)=200 ORDER BY a.id",3),
            ("SELECT COUNT(HTTP_GET(url)) FROM runtime_inputs",1),
            ("SELECT JSON_EXTRACT(HTTP_GET(url),'$.status') AS s,COUNT(*) FROM runtime_inputs GROUP BY s",1),
            ("SELECT id,SUM(CAST(JSON_EXTRACT(HTTP_GET(url),'$.status') AS SIGNED)) OVER() FROM runtime_inputs ORDER BY id",3),
            ("SELECT id,(SELECT JSON_EXTRACT(HTTP_GET(runtime_inputs.url),'$.status')) FROM runtime_inputs ORDER BY id",3),
            ("WITH c AS (SELECT id,HTTP_GET(url) AS r FROM runtime_inputs) SELECT id FROM c WHERE JSON_EXTRACT(r,'$.status')=200",3),
            ("SELECT HTTP_GET(url) FROM runtime_inputs WHERE id=1 UNION ALL SELECT HTTP_GET(url) FROM runtime_inputs WHERE id=2",2),
            ("SELECT test.http_definer(url) FROM runtime_inputs WHERE id=1 UNION ALL SELECT test.http_definer(url) FROM runtime_inputs WHERE id=2",2),
            ("SELECT id FROM runtime_inputs ORDER BY JSON_EXTRACT(HTTP_GET(url),'$.elapsed_ms'),id LIMIT 2",2),
        ]
        for index,(sql,count) in enumerate(matrix):
            print(f"  relational case {index+1}: {sql}",flush=True)
            before=endpoint.count()
            actual=q(admin,sql)
            assert len(actual)==count,(sql,actual)
            assert endpoint.count()>before,sql
        q(admin,"CREATE TABLE runtime_outputs(id INT PRIMARY KEY,r JSON)")
        q(admin,"INSERT INTO runtime_outputs SELECT id,HTTP_GET(url) FROM runtime_inputs")
        q(admin,"UPDATE runtime_outputs SET r=HTTP_GET(%s) WHERE id=1",(endpoint.url+"/echo",))
        q(admin,"DELETE FROM runtime_outputs WHERE id=2 AND JSON_EXTRACT(HTTP_GET(%s),'$.status')=200",(endpoint.url+"/echo",))
        assert q(admin,"SELECT COUNT(*) FROM runtime_outputs")[0][0]==2
        before=endpoint.count()
        q(admin,"BEGIN")
        q(admin,"INSERT INTO runtime_outputs VALUES(4,HTTP_POST(%s,'committed'))",(endpoint.url+"/echo",))
        q(admin,"SAVEPOINT http_savepoint")
        q(admin,"INSERT INTO runtime_outputs VALUES(5,HTTP_POST(%s,'rolled back'))",(endpoint.url+"/echo",))
        q(admin,"ROLLBACK TO SAVEPOINT http_savepoint")
        q(admin,"COMMIT")
        assert q(admin,"SELECT id FROM runtime_outputs WHERE id>=4")==((4,),)
        assert endpoint.count()==before+2
        q(admin,"CREATE PROCEDURE http_procedure(p TEXT) BEGIN SET @http_result=HTTP_GET(p); END")
        q(admin,"CALL http_procedure(%s)",(endpoint.url+"/echo",))
        assert json.loads(q(admin,"SELECT @http_result")[0][0])["status"]==200
        q(admin,"SET GLOBAL event_scheduler=ON")
        before=endpoint.count()
        q(admin,"CREATE EVENT http_event ON SCHEDULE AT CURRENT_TIMESTAMP + INTERVAL 1 SECOND DO INSERT INTO runtime_outputs VALUES(6,HTTP_GET(%s))",(endpoint.url+"/echo",))
        wait_for(lambda:q(admin,"SELECT COUNT(*) FROM runtime_outputs WHERE id=6")[0][0]==1,10)
        assert endpoint.count()==before+1
        q(admin,"SET GLOBAL event_scheduler=OFF")
        before=endpoint.count()
        q(admin,"EXPLAIN SELECT HTTP_GET(url) FROM runtime_inputs")
        assert endpoint.count()==before
        q(admin,"EXPLAIN ANALYZE SELECT HTTP_GET(url) FROM runtime_inputs")
        assert endpoint.count()==before+3
        for sql in [
            "SELECT COUNT(*) FROM (SELECT test.http_definer(url) AS r FROM runtime_inputs) d",
            "WITH c AS (SELECT test.http_definer(url) AS r FROM runtime_inputs) SELECT COUNT(*) FROM c a JOIN c b ON 1=1",
        ]:
            before=endpoint.count();q(admin,sql)
            assert endpoint.count()==before+3,sql
        results["wrapped_effects_and_cte_materialization"] = True
        results["relational_dml_savepoint_procedure_event"] = True

        marker="HTTP_SQL_PRIVATE_PARAMETER_927461"
        general=root/"http-general.log"
        q(admin,"SET GLOBAL general_log_file=%s",(str(general),))
        q(admin,"SET GLOBAL log_output='FILE'")
        q(admin,"SET GLOBAL general_log=ON")
        q(admin,"SET optimizer_trace='enabled=on'")
        q(admin,"SELECT id FROM runtime_inputs ORDER BY JSON_EXTRACT(HTTP_GET(%s, %s),'$.elapsed_ms'),id",(endpoint.url+"/echo?private="+marker,json.dumps({"auth":{"type":"bearer","token":marker}})))
        traces=q(admin,"SELECT QUERY,TRACE FROM information_schema.optimizer_trace")
        assert traces and marker not in str(traces),traces
        q(admin,"SET optimizer_trace='enabled=off'")
        q(admin,"SELECT test.http_definer(%s)",(endpoint.url+"/echo?private="+marker,))
        viewer=servers.connect(port);viewer_id=q(viewer,"SELECT CONNECTION_ID()")[0][0]
        before=endpoint.count()
        viewing=pool.submit(lambda:response(viewer,endpoint.url+"/hold?private="+marker))
        wait_for(lambda:endpoint.count()==before+1)
        processlist=q(admin,"SHOW FULL PROCESSLIST")
        assert marker not in str(processlist),processlist
        q(admin,f"KILL QUERY {viewer_id}")
        try:
            viewing.result(2)
            raise AssertionError("KILL did not stop redaction fixture")
        except pymysql.MySQLError as error:
            assert error.args[0]==1317,error.args
        q(admin,"SET GLOBAL general_log=OFF")
        wait_for(lambda:general.exists() and "[redacted HTTP statement]" in general.read_text())
        assert marker not in general.read_text()
        results["direct_indirect_log_trace_processlist_redaction"] = True

        print("HTTP protocol gate: cancellation and policy reload", flush=True)
        # Keep HTTP occupied while a separate SQL connection remains usable.
        victim = servers.connect(port)
        victim_id = q(victim, "SELECT CONNECTION_ID()")[0][0]
        before = endpoint.count()
        future = pool.submit(lambda: response(victim, endpoint.url+"/hold", {"on_error":"return"}))
        wait_for(lambda: endpoint.count() == before+1)
        started = time.monotonic()
        assert q(admin, "SELECT 42")[0][0] == 42
        results["ordinary_sql_during_http_ms"] = (time.monotonic()-started)*1000
        assert results["ordinary_sql_during_http_ms"] < 1000
        started = time.monotonic(); q(admin, f"KILL QUERY {victim_id}")
        try:
            future.result(2)
            raise AssertionError("KILL was swallowed by on_error")
        except pymysql.MySQLError as error:
            assert error.args[0] == 1317, error.args
        wait_for(lambda: metrics(admin)["inflight"] == 0, 2)
        results["kill_release_ms"] = (time.monotonic()-started)*1000
        assert q(victim, "SELECT 1")[0][0] == 1

        # Disable must cancel a network wait even if no response bytes arrive.
        before = endpoint.count()
        future = pool.submit(lambda: response(victim, endpoint.url+"/hold"))
        wait_for(lambda: endpoint.count() == before+1)
        started = time.monotonic();q(admin, "SET GLOBAL mydb_http_enabled=OFF")
        try:
            future.result(2)
            raise AssertionError("disable did not cancel")
        except pymysql.MySQLError as error:
            assert (error.args[0] & 0xffff) == 50102, error.args
        wait_for(lambda: metrics(admin)["inflight"] == 0, 2)
        results["disable_release_ms"] = (time.monotonic()-started)*1000
        q(admin, "SET GLOBAL mydb_http_enabled=ON")

        # Failed reload must preserve the current configuration.
        policy_path.write_text("invalid")
        denied(lambda: q(admin, "ALTER SYSTEM RELOAD HTTP CONFIG"), {50100})
        assert response(admin, endpoint.url+"/echo")["status"] == 200
        policy_path.write_text(json.dumps({"version":1,"egress":{"allowed_origins":[]}}))
        q(admin, "ALTER SYSTEM RELOAD HTTP CONFIG")
        before = endpoint.count()
        denied(lambda: response(admin, endpoint.url+"/echo", {"on_error":"return"}), {50103})
        assert endpoint.count() == before
        policy_path.write_text(json.dumps(policy));q(admin, "ALTER SYSTEM RELOAD HTTP CONFIG")
        results["atomic_policy_reload"] = True

        # Disconnect is observed while the SQL worker is waiting for HTTP.
        disconnected=servers.connect(port)
        before=endpoint.count()
        future=pool.submit(lambda: response(disconnected, endpoint.url+"/hold"))
        wait_for(lambda: endpoint.count()==before+1)
        started=time.monotonic()
        disconnected._sock.shutdown(socket.SHUT_RDWR)
        disconnected.close()
        try:
            future.result(2)
        except (pymysql.MySQLError, AttributeError, OSError):
            pass
        wait_for(lambda: metrics(admin)["inflight"]==0,2)
        results["disconnect_release_ms"]=(time.monotonic()-started)*1000

        # Four calls consume the default per-account capacity. More calls
        # wait without consuming SQL workers; an independent account can run.
        q(admin, "CREATE USER http_other IDENTIFIED BY 'x'")
        q(admin, "GRANT HTTP_USE ON *.* TO http_other")
        q(admin,"CREATE TABLE http_read_isolation(id INT PRIMARY KEY,r JSON)")
        q(admin,"CREATE TRIGGER http_read_isolation_trigger BEFORE INSERT ON http_read_isolation FOR EACH ROW SET NEW.r=HTTP_GET(%s)",(endpoint.url+"/echo",))
        q(admin,"CREATE VIEW http_read_isolation_view AS SELECT * FROM http_read_isolation")
        held=[]
        prepared_connections=[]
        for _ in range(6):
            connection=servers.connect(port)
            identifier=q(connection,"SELECT CONNECTION_ID()")[0][0]
            q(connection,"PREPARE held_http FROM 'SELECT HTTP_GET(?)'")
            q(connection,"SET @held_url=%s",(endpoint.url+"/hold",))
            prepared_connections.append((connection,identifier))
        before=endpoint.count()
        for connection,identifier in prepared_connections:
            job=pool.submit(lambda c=connection:q(c,"EXECUTE held_http USING @held_url"))
            held.append((identifier,job))
        wait_for(lambda: endpoint.count()>=before+4)
        time.sleep(0.1)
        assert endpoint.count()==before+4, "account concurrency limit exceeded"
        started=time.monotonic()
        assert q(admin,"SELECT COUNT(*) FROM http_read_isolation")[0][0]==0
        assert q(admin,"SELECT COUNT(*) FROM http_read_isolation_view")[0][0]==0
        results["trigger_table_read_isolation_ms"]=(time.monotonic()-started)*1000
        assert results["trigger_table_read_isolation_ms"]<1000
        results["sql_prepare_admission"] = True
        other=pymysql.connect(host="127.0.0.1",port=port,user="http_other",password="x",autocommit=True)
        try:
            started=time.monotonic()
            assert response(other,endpoint.url+"/echo")["status"]==200
            results["independent_account_latency_ms"]=(time.monotonic()-started)*1000
            assert results["independent_account_latency_ms"]<1000
        finally:
            other.close()
        for identifier,job in held:
            q(admin,f"KILL QUERY {identifier}")
        for _,job in held:
            try:
                job.result(2)
                raise AssertionError("queued KILL did not interrupt")
            except pymysql.MySQLError as error:
                assert error.args[0]==1317,error.args
        wait_for(lambda: metrics(admin)["inflight"]==0,2)
        results["account_admission_and_queued_kill"] = True

        print("HTTP protocol gate: locks, restart, and replication", flush=True)
        q(admin, "CREATE TABLE locks(id INT PRIMARY KEY,v INT)")
        q(admin, "INSERT INTO locks VALUES(1,0)")
        locker = servers.connect(port); worker = servers.connect(port)
        q(locker, "BEGIN");q(locker, "UPDATE locks SET v=1 WHERE id=1")
        before = endpoint.count()
        future = pool.submit(lambda: q(worker, "UPDATE locks SET v=CAST(JSON_EXTRACT(HTTP_POST(%s,'effect'),'$.status') AS SIGNED) WHERE id=1", (endpoint.url+"/echo",)))
        time.sleep(0.15);q(locker,"ROLLBACK")
        try:
            future.result(5)
        except pymysql.MySQLError as error:
            assert (error.args[0] & 0xffff) in {50113,1205}, error.args
        results["lock_wait_no_replay"] = endpoint.count() == before+1
        assert results["lock_wait_no_replay"]

        # Object definitions and CTAS recovery do not repeat requests.
        before = endpoint.count()
        q(admin, "CREATE TABLE copied AS SELECT HTTP_GET(%s) AS response", (endpoint.url+"/echo",))
        assert endpoint.count() == before+1
        saved = q(admin, "SELECT response FROM copied")
        q(admin, "ALTER SYSTEM CHECKPOINT")
        process.terminate();process.wait(20)
        process, admin = servers.start("primary", port, {"MYDB_HTTP_LOCAL_CONFIG":str(policy_path)})
        assert q(admin, "SELECT response FROM copied") == saved
        assert endpoint.count() == before+1
        results["ctas_restart_without_http"] = True

        # A separate native primary/replica pair verifies row replay isolation.
        source_port, replica_port, repl_port, repl_target = [free_port() for _ in range(4)]
        _, source = servers.start("source", source_port, {"MYDB_ROLE":"master","MYDB_REPL_PORT":str(repl_port)})
        q(source, "CREATE TABLE replicated(id INT PRIMARY KEY,r JSON)")
        q(source, "CREATE TRIGGER replicated_http BEFORE INSERT ON replicated FOR EACH ROW SET NEW.r=HTTP_GET(%s)", (endpoint.url+"/echo",))
        _, replica = servers.start("replica", replica_port, {"MYDB_ROLE":"replica","MYDB_REPL_PORT":str(repl_target),"MYDB_REPL_MASTER_HOST":"127.0.0.1","MYDB_REPL_MASTER_PORT":str(repl_port)})
        before = endpoint.count();q(source, "INSERT INTO replicated(id) VALUES(1)")
        catch_up(source, replica)
        assert q(source,"SELECT * FROM replicated") == q(replica,"SELECT * FROM replicated")
        assert endpoint.count() == before+1
        results["native_replication_does_not_call_trigger_http"] = True

        # CTAS binlog must contain concrete DDL and rows, never the HTTP query.
        binlog_port = free_port()
        _, logging = servers.start("binlog", binlog_port, {"MYDB_LOG_BIN":"1"})
        before=endpoint.count();q(logging,"CREATE TABLE binlogged AS SELECT HTTP_GET(%s) AS r",(endpoint.url+"/echo",))
        events=q(logging,"SHOW BINLOG EVENTS")
        assert not any("HTTP_GET" in str(row) for row in events), events
        assert endpoint.count()==before+1
        results["ctas_row_binlog"] = True

        for name,value in results.items():
            if isinstance(value,bool):
                assert value, name
        results["status"] = "passed"
        return results
    finally:
        for endpoint in endpoints:
            endpoint.release.set()
        pool.shutdown(wait=True, cancel_futures=True)
        servers.close()
        for endpoint in endpoints:
            endpoint.close()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary",required=True,type=Path)
    parser.add_argument("--output",type=Path,default=Path("/tmp/mydb/http-functions/protocol"))
    args=parser.parse_args()
    result={"status":"failed"}
    try:
        result=run(args.binary.resolve(),args.output.resolve(),result)
        print(json.dumps(result,indent=2))
    finally:
        args.output.mkdir(parents=True,exist_ok=True)
        (args.output/"results.json").write_text(json.dumps(result,indent=2)+"\n")


if __name__ == "__main__":
    main()
