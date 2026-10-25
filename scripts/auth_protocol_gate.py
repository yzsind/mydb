#!/usr/bin/env python3
"""Local protocol gate, including rejection of plaintext full-auth without TLS.
Run against a disposable mydb server. Requires mysql CLI and openssl; stdlib only.
MYDB_GATE_ROOT_PASSWORD supplies the admin password (default: root).
"""
import argparse
import json
import os
from pathlib import Path
import socket
import ssl
import struct
import subprocess
import tempfile


def packet_read(sock):
    def exact(n):
        data = b""
        while len(data) < n:
            part = sock.recv(n - len(data))
            if not part:
                raise RuntimeError("unexpected EOF")
            data += part
        return data
    header = exact(4)
    return header[3], exact(int.from_bytes(header[:3], "little"))


def packet_write(sock, seq, payload):
    sock.sendall(len(payload).to_bytes(3, "little") + bytes([seq & 255]) + payload)


def finish_authentication(sock, user, password, plugin, mode, seed, initial,
                          expected_error=1045, reject_before_full=False):
    cleartext = password.encode() + b"\0"
    for _ in range(8):
        seq, payload = packet_read(sock)
        if payload[0] == 0:
            return True
        if payload[0] == 255:
            code = int.from_bytes(payload[1:3], "little")
            assert code == expected_error, (mode, plugin, code, payload)
            return False
        assert not reject_before_full, "policy rejection incorrectly requested additional credentials"
        if payload[0] == 254:
            switched, switched_seed = payload[1:].split(b"\0", 1)
            assert switched.decode() == plugin
            seed = switched_seed[:20]
            packet_write(sock, seq + 1, initial)
        elif payload == b"\1\4":
            packet_write(sock, seq + 1, b"\2" if mode.startswith("rsa") else cleartext)
        elif payload.startswith(b"\1-----BEGIN PUBLIC KEY"):
            with tempfile.TemporaryDirectory(dir="/tmp/mydb") as directory:
                key = Path(directory) / "key.pem"
                plain = Path(directory) / "masked.bin"
                key.write_bytes(payload[1:].rstrip(b"\0"))
                plain.write_bytes(bytes(v ^ seed[i % len(seed)] for i, v in enumerate(cleartext)))
                encrypted = subprocess.check_output(["openssl", "pkeyutl", "-encrypt", "-pubin", "-inkey", str(key), "-in", str(plain), "-pkeyopt", "rsa_padding_mode:" + ("pkcs1" if mode == "rsa-pkcs1" else "oaep")])
            if mode == "rsa-invalid-short":
                encrypted = b"\0" * (len(encrypted) - 1)
            elif mode == "rsa-invalid-long":
                encrypted = b"\0" * (len(encrypted) + 1)
            elif mode == "rsa-invalid-padding":
                encrypted = b"\0" * len(encrypted)
            packet_write(sock, seq + 1, encrypted)
        elif payload != b"\1\3":
            raise AssertionError((mode, plugin, payload))
    raise AssertionError("authentication exchange exceeded packet bound")


def authenticate(port, user, password, plugin, mode, expected_error=1045, reject_before_full=False):
    sock = socket.create_connection(("127.0.0.1", port), 10)
    sock.settimeout(10)
    try:
        _, hello = packet_read(sock)
        offset = hello.index(0, 1) + 1 + 4
        seed = hello[offset:offset + 8]
        offset += 8 + 1 + 2 + 1 + 2 + 2
        seed_length = hello[offset]
        offset += 1 + 10
        seed += hello[offset:offset + max(13, seed_length - 8)][:12]
        seed = seed[:20]
        flags = 0x200 | 0x8000 | 0x80000 | 0x200000  # protocol41, secure connection, plugin auth, length-encoded auth
        seq = 1
        if mode == "tls":
            flags |= 0x800
        header = struct.pack("<IIB23x", flags, 16 * 1024 * 1024, 45)
        if mode == "tls":
            packet_write(sock, seq, header)
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE  # disposable self-signed test server
            sock = context.wrap_socket(sock, server_hostname="localhost")
            seq += 1
        cleartext = password.encode() + b"\0"
        initial = (b"\0" * 32 if password else b"") if plugin == "caching_sha2_password" else (b"" if plugin == "mysql_native_password" and not password else (b"\1" if mode.startswith("rsa") else cleartext))
        packet_write(sock, seq, header + user.encode() + b"\0" + (bytes([len(initial)]) if len(initial) < 251 else b"\xfc" + struct.pack("<H", len(initial))) + initial + plugin.encode() + b"\0")
        return finish_authentication(sock, user, password, plugin, mode, seed, initial,
                                     expected_error, reject_before_full)
    finally:
        sock.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=13319)
    args = parser.parse_args()
    Path("/tmp/mydb").mkdir(exist_ok=True)
    environment = dict(os.environ, MYSQL_PWD=os.environ.get("MYDB_GATE_ROOT_PASSWORD", "root"))
    cli = ["mysql", "-h127.0.0.1", f"-P{args.port}", "--ssl-mode=REQUIRED", "--batch", "--skip-column-names", "--unbuffered"]
    def sql(statement, user="root", database=None, expected_error=None):
        env = environment if user == "root" else dict(os.environ, MYSQL_PWD="gate-correct")
        result = subprocess.run(cli + ["-u" + user] + (["-D" + database] if database else []), input=statement, text=True, env=env, capture_output=True)
        if expected_error is not None:
            assert result.returncode != 0 and str(expected_error) in result.stderr, result.stderr
        else:
            assert result.returncode == 0, result.stderr
        return result.stdout.strip()
    results = []
    for plugin in ["caching_sha2_password", "sha256_password"]:
        user = "auth_gate_" + plugin.split("_")[0]
        sql(f"CREATE USER '{user}'@'%' IDENTIFIED WITH {plugin} BY 'gate-correct' FAILED_LOGIN_ATTEMPTS 1 PASSWORD_LOCK_TIME 1;")
        try:
            for mode in ["tls", "rsa-oaep", "rsa-pkcs1"]:
                for valid in [True, False]:
                    sql(f"ALTER USER '{user}'@'%' ACCOUNT UNLOCK; FLUSH PRIVILEGES;")
                    accepted = authenticate(args.port, user, "gate-correct" if valid else "gate-wrong", plugin, mode, expected_error=3955)
                    assert accepted == valid, (plugin, mode, valid, accepted)
                    results.append(dict(plugin=plugin, mode=mode, correct_password=valid, passed=True))
                    print(json.dumps(results[-1]), flush=True)
                    if not valid:
                        assert not authenticate(args.port, user, "gate-correct", plugin, mode, expected_error=3955)
                        results.append(dict(plugin=plugin, mode=mode, check="one_failed_password_locks_account", passed=True))
                        print(json.dumps(results[-1]), flush=True)
            # Keep transport denial independent from temporary-lock policy:
            # the TLS/RSA matrix above already requires one-failure lockout.
            sql(f"ALTER USER '{user}'@'%' FAILED_LOGIN_ATTEMPTS 0 PASSWORD_LOCK_TIME 0 ACCOUNT UNLOCK; FLUSH PRIVILEGES;")
            assert authenticate(args.port, user, "gate-correct", plugin, "tls")
            for password in ["gate-correct", "gate-wrong"]:
                sql(f"ALTER USER '{user}'@'%' ACCOUNT UNLOCK; FLUSH PRIVILEGES;")
                assert not authenticate(args.port, user, password, plugin, "plaintext-compat")
                results.append(dict(plugin=plugin, check="plaintext_full_auth_rejected_without_tls", passed=True))
                print(json.dumps(results[-1]), flush=True)
        finally:
            sql(f"DROP USER '{user}'@'%';")
    sql("CREATE USER auth_gate_ssl IDENTIFIED WITH caching_sha2_password BY 'gate-correct' REQUIRE SSL")
    try:
        assert not authenticate(args.port, "auth_gate_ssl", "gate-correct", "caching_sha2_password", "plaintext-compat", expected_error=3159, reject_before_full=True)
        results.append(dict(check="require_ssl_rejects_before_full_auth", passed=True))
        assert authenticate(args.port, "auth_gate_ssl", "gate-correct", "caching_sha2_password", "tls")
        results.append(dict(check="require_ssl_accepts_tls", passed=True))
        for result in results[-2:]:
            print(json.dumps(result), flush=True)
    finally:
        sql("DROP USER auth_gate_ssl")

    # Oversized SHA-2 passwords are rejected before expensive hashing.
    sql("CREATE USER auth_gate_bound IDENTIFIED WITH caching_sha2_password BY '" + "x" * 256 + "'")
    try:
        assert authenticate(args.port, "auth_gate_bound", "x" * 256, "caching_sha2_password", "tls")
        results.append(dict(check="sha2_256_byte_password", passed=True))
        sql("ALTER USER auth_gate_bound IDENTIFIED BY '" + "x" * 257 + "'", expected_error=1396)
        assert not authenticate(args.port, "auth_gate_bound", "x" * 4096, "caching_sha2_password", "tls")
        results.append(dict(check="sha2_oversized_password_rejected", passed=True))
        for result in results[-2:]:
            print(json.dumps(result), flush=True)
    finally:
        sql("DROP USER auth_gate_bound")

    for plugin in ["mysql_native_password", "caching_sha2_password", "sha256_password"]:
        sql(f"CREATE USER auth_gate_empty IDENTIFIED WITH {plugin} AS ''")
        try:
            assert authenticate(args.port, "auth_gate_empty", "", plugin, "tls")
            results.append(dict(plugin=plugin, check="empty_encoded_credential", passed=True))
            print(json.dumps(results[-1]), flush=True)
        finally:
            sql("DROP USER auth_gate_empty")

    # Check both projected and optimized metadata paths through real sessions.
    sql("CREATE USER auth_gate_Case IDENTIFIED BY 'gate-correct', auth_gate_case IDENTIFIED BY 'gate-correct'; GRANT CREATE USER ON *.* TO auth_gate_case;")
    upper = None
    try:
        for view in ["TABLES", "COLUMNS", "STATISTICS"]:
            assert sql(f"SELECT COUNT(*) FROM information_schema.{view} WHERE TABLE_SCHEMA='mysql' AND TABLE_NAME='user'", "auth_gate_case") == "0", view
            results.append(dict(check="private_" + view.lower(), passed=True))
        assert sql("SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA='performance_schema'", "auth_gate_case") == "8"
        results.append(dict(check="public_performance_tables", passed=True))
        sql("SELECT 1", "auth_gate_case", database="mysql", expected_error=1044)
        results.append(dict(check="handshake_database_access", passed=True))
        upper = subprocess.Popen(cli + ["-uauth_gate_Case", "-e", "SELECT CONNECTION_ID(); SELECT SLEEP(2)"], env=dict(os.environ, MYSQL_PWD="gate-correct"), stdout=subprocess.PIPE, text=True)
        connection_id = int(upper.stdout.readline().strip())
        assert sql(f"SELECT COUNT(*) FROM performance_schema.session_account_connect_attrs WHERE PROCESSLIST_ID={connection_id}", "auth_gate_case") == "0"
        upper.wait(timeout=10)
        results.append(dict(check="case_distinct_account_attributes", passed=True))
        for result in results[-6:]:
            print(json.dumps(result), flush=True)
    finally:
        if upper is not None and upper.poll() is None:
            upper.terminate()
            upper.wait(timeout=10)
        sql("DROP USER auth_gate_Case, auth_gate_case")
    print(json.dumps(dict(cases=len(results), errors=0)))


if __name__ == "__main__":
    main()
