#!/usr/bin/env python3
"""Exercise release security boundaries on owned loopback server processes."""
import argparse
import importlib.metadata
import json
import os
from pathlib import Path
import subprocess
import sys
import socket
import ssl
import struct

import pymysql

from test_native_readiness import Servers, free_port, query
from auth_protocol_gate import authenticate, finish_authentication, packet_read, packet_write


def denied(connection, statement, codes):
    try:
        query(connection, statement)
    except pymysql.MySQLError as error:
        assert error.args[0] in codes, (statement, error)
        return error.args[0]
    raise AssertionError("unauthorized statement succeeded: " + statement)


def tls_identity(port, context, hostname, should_verify):
    with socket.create_connection(("127.0.0.1", port), 10) as stream:
        stream.settimeout(10)
        packet_read(stream)
        # SSLRequest precedes TLS on the MySQL classic protocol.
        flags = 0x200 | 0x800 | 0x8000 | 0x80000
        packet_write(stream, 1, struct.pack("<IIB23x", flags, 16 * 1024 * 1024, 45))
        try:
            with context.wrap_socket(stream, server_hostname=hostname):
                assert should_verify, "untrusted TLS peer was accepted"
        except ssl.SSLCertVerificationError:
            assert not should_verify, "trusted TLS identity was rejected"


def change_user(port, user, password, plugin, mode, switch=False):
    connection = pymysql.connect(host="127.0.0.1", port=port, user="root", password="root",
                                 database="test", autocommit=True, read_timeout=10,
                                 ssl={"check_hostname": False} if mode == "tls" else None,
                                 ssl_disabled=mode != "tls")
    try:
        assert isinstance(connection._sock, ssl.SSLSocket) == (mode == "tls")
        token = (b"\0" * 32 if password else b"") if plugin == "caching_sha2_password" else (
            b"\1" if mode.startswith("rsa") else password.encode() + b"\0")
        length_encoded = bool(connection.client_flag & (1 << 21))
        offered = "mysql_native_password" if switch or (len(token) > 255 and not length_encoded) else plugin
        initial = b"" if offered != plugin else token
        size = (bytes([len(initial)]) if len(initial) < 251 else b"\xfc" + struct.pack("<H", len(initial))) if length_encoded else bytes([len(initial)])
        payload = user.encode() + b"\0" + size + initial + b"\0"
        payload += struct.pack("<H", 45) + offered.encode() + b"\0"
        if connection.client_flag & (1 << 20):
            payload += b"\0"
        connection._execute_command(17, payload)
        accepted = finish_authentication(connection._sock, user, password, plugin, mode,
                                         connection.salt[:20], token)
        if accepted:
            assert query(connection, "SELECT CURRENT_USER()") == ((user + "@%",),)
        return accepted
    finally:
        connection.close()


def transport_matrix(admin, port, results):
    cases = results.setdefault("transport_matrix", [])
    for plugin in ["caching_sha2_password", "sha256_password"]:
        user = "transport_" + plugin.split("_")[0]
        query(admin, f"CREATE USER {user} IDENTIFIED WITH {plugin} BY 'old-secret'")
        query(admin, f"ALTER USER {user} IDENTIFIED BY 'new-secret' RETAIN CURRENT PASSWORD")
        try:
            for credential, password in [("primary", "new-secret"), ("retained", "old-secret")]:
                for mode in ["tls", "rsa-oaep", "rsa-pkcs1", "plaintext-compat"]:
                    expected = mode != "plaintext-compat"
                    assert authenticate(port, user, password, plugin, mode) == expected
                    for switch in [False, True]:
                        actual = change_user(port, user, password, plugin, mode, switch)
                        assert actual == expected, (plugin, credential, mode, switch, actual, expected)
                    cases.append({"plugin": plugin, "credential": credential, "mode": mode,
                                  "initial_and_change_user": True, "accepted": expected})
            for mode in ["rsa-invalid-short", "rsa-invalid-long", "rsa-invalid-padding"]:
                assert not authenticate(port, user, "new-secret", plugin, mode)
                assert not change_user(port, user, "new-secret", plugin, mode)
                cases.append({"plugin": plugin, "mode": mode, "rejected": True})
            for tls in [False, True]:
                query(admin, "FLUSH PRIVILEGES")
                for attempt in range(2):
                    connection = pymysql.connect(host="127.0.0.1", port=port, user=user,
                                                 password="new-secret", autocommit=True,
                                                 ssl={"check_hostname": False} if tls else None,
                                                 ssl_disabled=not tls)
                    try:
                        assert isinstance(connection._sock, ssl.SSLSocket) == tls
                        assert query(connection, "SELECT 1") == ((1,),)
                    finally:
                        connection.close()
                cases.append({"plugin": plugin, "tls": tls, "cold_warm_driver_auth": True})
            query(admin, f"ALTER USER {user} IDENTIFIED BY ''")
            for mode in ["tls", "plaintext-compat"]:
                assert authenticate(port, user, "", plugin, mode)
                assert change_user(port, user, "", plugin, mode)
            query(admin, f"ALTER USER {user} IDENTIFIED BY '" + "x" * 256 + "'")
            assert authenticate(port, user, "x" * 256, plugin, "tls")
            assert not authenticate(port, user, "x" * 256, plugin, "plaintext-compat")
            for switch in [False, True]:
                assert change_user(port, user, "x" * 256, plugin, "tls", switch=switch)
                assert not change_user(port, user, "x" * 256, plugin, "plaintext-compat", switch=switch)
            cases.append({"plugin": plugin, "empty_and_maximum_password": True})
        finally:
            query(admin, f"DROP USER {user}")


def key_configuration_matrix(servers, root, results):
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ec, rsa
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    keys = root / "keys"
    keys.mkdir()
    def write(name, material, form=serialization.PrivateFormat.PKCS8, encryption=None):
        path = keys / name
        path.write_bytes(material.private_bytes(serialization.Encoding.PEM, form,
                         encryption or serialization.NoEncryption()))
        path.chmod(0o600)
        return path
    pkcs8 = write("rsa-pkcs8.pem", key)
    pkcs1 = write("rsa-pkcs1.pem", key, serialization.PrivateFormat.TraditionalOpenSSL)
    wide = write("rsa-4096.pem", rsa.generate_private_key(public_exponent=65537, key_size=4096))
    cases = results.setdefault("rsa_key_configuration", [])
    configurations = [("pkcs8", {"MYDB_AUTH_RSA_PRIVATE_KEY": str(pkcs8)}),
                      ("pkcs1", {"MYDB_AUTH_RSA_PRIVATE_KEY": str(pkcs1)}),
                      ("inline", {"MYDB_AUTH_RSA_PRIVATE_KEY_INLINE": pkcs1.read_text()}),
                      ("rsa4096", {"MYDB_AUTH_RSA_PRIVATE_KEY": str(wide)})]
    for name, extra in configurations:
        port = free_port()
        process, admin = servers.start("key-" + name, port, extra)
        try:
            for plugin in ["caching_sha2_password", "sha256_password"]:
                query(admin, f"CREATE USER key_login IDENTIFIED WITH {plugin} BY 'key-password'")
                for mode in ["rsa-oaep", "rsa-pkcs1"]:
                    assert authenticate(port, "key_login", "key-password", plugin, mode)
                    assert not authenticate(port, "key_login", "wrong-password", plugin, mode)
                assert not authenticate(port, "key_login", "key-password", plugin, "rsa-invalid-padding")
                query(admin, "DROP USER key_login")
            cases.append({"configuration": name, "passed": True})
        finally:
            admin.close()
            process.terminate()
            process.wait(timeout=15)
    invalid = keys / "malformed.pem"
    invalid.write_text("not a private key\n")
    bad_keys = [("malformed", invalid),
                ("non-rsa", write("ec.pem", ec.generate_private_key(ec.SECP256R1()))),
                ("weak-rsa", write("weak.pem", rsa.generate_private_key(public_exponent=65537, key_size=1024))),
                ("encrypted", write("encrypted.pem", key, encryption=serialization.BestAvailableEncryption(b"fixture-secret")))]
    for name, path in bad_keys:
        env = {k: v for k, v in os.environ.items() if not k.startswith("MYDB_")}
        env.update(MYDB_DATA_DIR=str(root / ("invalid-" + name)),
                   MYDB_AUTH_RSA_PRIVATE_KEY=str(path), TMPDIR="/tmp/mydb")
        log = root / ("invalid-" + name + ".log")
        with log.open("wb") as output:
            completed = subprocess.run([servers.binary, "--host", "127.0.0.1", "--port", str(free_port())],
                                       env=env, input=b"", stdout=output,
                                       stderr=subprocess.STDOUT, timeout=15)
        text = log.read_text()
        assert completed.returncode != 0 and "authentication RSA" in text, (name, text)
        assert "panicked at" not in text, (name, text)
        cases.append({"configuration": name, "startup_rejected": True})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    root = args.output.resolve()
    if not root.is_relative_to(Path("/tmp/mydb").resolve()):
        parser.error("--output must be below /tmp/mydb")
    root.mkdir(parents=True, exist_ok=False)
    binary = args.binary.resolve()
    servers = Servers(binary, root)
    results = {"driver_version": importlib.metadata.version("pymysql")}
    try:
        port = free_port()
        _, admin = servers.start("server", port)
        scripts = Path(__file__).resolve().parent
        env = dict(os.environ, TMPDIR="/tmp/mydb")
        with (root / "authentication.log").open("wb") as log:
            subprocess.run([sys.executable, scripts / "auth_protocol_gate.py", "--port", str(port)],
                           env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
        results["authentication"] = "passed"
        transport_matrix(admin, port, results)

        query(admin, "CREATE DATABASE private_db")
        query(admin, "CREATE TABLE private_db.secret(id INT PRIMARY KEY)")
        query(admin, "INSERT INTO private_db.secret VALUES(42)")
        query(admin, "CREATE USER release_reader IDENTIFIED BY 'reader-password'")
        reader = pymysql.connect(host="127.0.0.1", port=port, user="release_reader",
                                 password="reader-password", autocommit=True)
        servers.connections.append(reader)
        assert query(reader, "SELECT 1") == ((1,),)
        statements = [
            "SELECT * FROM private_db.secret",
            "CREATE USER forbidden_user IDENTIFIED BY 'forbidden'",
            "SET GLOBAL secure_file_priv=''",
            "SHUTDOWN",
            "STOP MYDB REPLICATION",
            "PROMOTE TO MASTER",
        ]
        results["privilege_denials"] = {
            statement: denied(reader, statement, {1044, 1142, 1227})
            for statement in statements
        }
        assert query(admin, "SELECT * FROM private_db.secret") == ((42,),)

        allowed = root / "allowed"
        allowed.mkdir()
        (allowed / "rows.txt").write_text("1\n")
        (root / "outside.txt").write_text("2\n")
        (allowed / "escape.txt").symlink_to(root / "outside.txt")
        query(admin, "CREATE TABLE test.file_rows(id INT)")
        load = f"LOAD DATA INFILE '{allowed / 'rows.txt'}' INTO TABLE test.file_rows"
        denied(admin, load, {1290})
        assert query(admin, f"SELECT LOAD_FILE('{allowed / 'rows.txt'}')") == ((None,),)
        query(admin, f"SET GLOBAL secure_file_priv='{allowed}'")
        query(admin, load)
        assert query(admin, "SELECT id FROM test.file_rows") == ((1,),)
        assert query(admin, f"SELECT LOAD_FILE('{allowed / 'rows.txt'}')") == ((b"1\n",),)
        for path in [root / "outside.txt", allowed / ".." / "outside.txt", allowed / "escape.txt"]:
            assert query(admin, f"SELECT LOAD_FILE('{path}')") == ((None,),), path
        query(admin, "GRANT SELECT, INSERT ON test.file_rows TO release_reader")
        denied(reader, load, {1142})
        assert query(reader, f"SELECT LOAD_FILE('{allowed / 'rows.txt'}')") == ((None,),)
        results["file_access"] = "default denial, explicit directory, traversal/symlink denial, FILE privilege passed"

        query(admin, "SET GLOBAL require_secure_transport=ON")
        try:
            unexpected = pymysql.connect(host="127.0.0.1", port=port, user="root", password="root", ssl_disabled=True)
        except pymysql.MySQLError as error:
            assert error.args[0] == 3159, error
        else:
            unexpected.close()
            raise AssertionError("plaintext handshake accepted with require_secure_transport=ON")
        secure = pymysql.connect(host="127.0.0.1", port=port, user="root", password="root",
                                 ssl={"check_hostname": False}, autocommit=True)
        servers.connections.append(secure)
        assert secure._sock.cipher(), "TLS was not negotiated"
        assert query(secure, "SELECT 1") == ((1,),)
        results["secure_transport"] = "plaintext rejected and TLS accepted"
        certificate = root / "server-certificate.pem"
        certificate.write_text(ssl.DER_cert_to_PEM_cert(secure._sock.getpeercert(binary_form=True)))
        trusted = ssl.create_default_context(cafile=str(certificate))
        tls_identity(port, trusted, "localhost", True)
        tls_identity(port, trusted, "wrong.invalid", False)
        tls_identity(port, ssl.create_default_context(), "127.0.0.1", False)
        results["tls_identity"] = "trusted certificate accepted; wrong hostname and untrusted certificate rejected"
        key_configuration_matrix(servers, root, results)
        results["passed"] = True
    finally:
        (root / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        servers.close()
    print(json.dumps({"passed": True, "output": str(root)}))


if __name__ == "__main__":
    main()
