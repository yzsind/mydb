#!/usr/bin/env python3
"""Exercise the real CLI against a local MyDB server, including Unix PTYs."""
import argparse
import json
import os
from pathlib import Path
import select
import shutil
import signal
import socket
import subprocess
import tempfile
import threading
import struct
import time
import unittest

ROOT = Path(__file__).resolve().parents[3]
OPTIONS = None
ENV = None
WORK = None


def invoke(sql=None, flags=(), input=None, env=None, ok=True):
    cmd = [OPTIONS.cli, '--no-config', '-h127.0.0.1', f'-P{OPTIONS.port}', '-uroot', '-proot', 'test', *flags]
    if sql is not None:
        cmd.extend(['-e', sql])
    result = subprocess.run(cmd, input=input, capture_output=True, env=env or ENV, timeout=30)
    if ok and result.returncode:
        raise AssertionError(f'{cmd}: {result.returncode}\n{result.stdout!r}\n{result.stderr!r}')
    return result


def active(marker):
    for _ in range(100):
        result = invoke(f"SELECT COUNT(*) FROM information_schema.processlist WHERE INFO LIKE 'SELECT SLEEP(30) AS {marker}%'", ['-N'])
        if result.stdout.strip() != b'0':
            return
        time.sleep(.02)
    raise AssertionError('query did not become active')


class Terminal:
    def __init__(self, flags=(), password=None, stdin_sql=None):
        import pty
        self.batch = stdin_sql is not None
        read_fd = None
        if stdin_sql is not None:
            read_fd, write_fd = os.pipe()
            os.write(write_fd, stdin_sql); os.close(write_fd)
        self.pid, self.fd = pty.fork()
        if not self.pid:
            if read_fd is not None: os.dup2(read_fd, 0); os.close(read_fd)
            os.execve(OPTIONS.cli, [OPTIONS.cli, '--no-config', f'-P{OPTIONS.port}', '-uroot', '-proot', '--no-history', 'test', *flags], ENV)
        if read_fd is not None: os.close(read_fd)
        self.buffer = b''
        self.transcript = b''
        if password is not None:
            self.until(b'Enter password: ')
            self.send(password + b'\r')
        if not self.batch: self.until(b'mydb [test]> ')

    def send(self, value):
        os.write(self.fd, value)

    def until(self, value, timeout=10):
        deadline = time.monotonic() + timeout
        while value not in self.buffer:
            if time.monotonic() > deadline:
                raise AssertionError(f'terminal timeout for {value!r}: {self.buffer[-2000:]!r}')
            if select.select([self.fd], [], [], .1)[0]:
                try:
                    data = os.read(self.fd, 65536)
                except OSError:
                    data = b''
                if not data:
                    raise AssertionError(f'terminal closed for {value!r}: {self.buffer[-2000:]!r}')
                self.buffer += data
                self.transcript += data
                if b'\x1b[6n' in data:
                    self.send(b'\x1b[1;1R')
        end = self.buffer.index(value) + len(value)
        result, self.buffer = self.buffer[:end], self.buffer[end:]
        return result

    def close(self):
        try:
            if not self.batch:
                try: self.send(b'quit\r')
                except OSError: pass
            for _ in range(100):
                pid, status = os.waitpid(self.pid, os.WNOHANG)
                if pid:
                    self.status = os.waitstatus_to_exitcode(status)
                    return
                if select.select([self.fd], [], [], .02)[0]:
                    try:
                        self.transcript += os.read(self.fd, 65536)
                    except OSError:
                        pass
            os.kill(self.pid, signal.SIGKILL)
            os.waitpid(self.pid, 0)
        finally:
            os.close(self.fd)
            (WORK / f'pty-{self.pid}.txt').write_bytes(self.transcript)


class Acceptance(unittest.TestCase):
    def test_batch_types_and_unicode(self):
        sql = "SELECT NULL AS n, '' AS empty, 18446744073709551615 AS u, CAST('12345678901234567890.1234567890' AS DECIMAL(30,10)) AS d, '中文😀' AS s, CAST('2026-10-27 12:34:56.123456' AS DATETIME(6)) AS ts"
        out = invoke(sql, ['-N']).stdout
        self.assertEqual(out, 'NULL\t\t18446744073709551615\t12345678901234567890.1234567890\t中文😀\t2026-10-27 12:34:56.123456\n'.encode())

    def test_escaping_raw_and_binary(self):
        self.assertEqual(invoke("SELECT CONCAT('a',CHAR(9),CHAR(10),CHAR(0),CHAR(92))", ['-N']).stdout, b'a\\t\\n\\0\\\\\n')
        self.assertEqual(invoke("SELECT UNHEX('FF000A')", ['-N', '--raw']).stdout, b'\xff\0\n\n')
        self.assertEqual(invoke("SELECT UNHEX('FF000A')", ['-N', '--binary-as-hex']).stdout, b'0xFF000A\n')
        out = invoke("SELECT '中文' AS name", ['--table']).stdout
        self.assertIn('中文'.encode(), out)
        self.assertIn(b'+', out)
        self.assertIn(b'1. row', invoke('SELECT 42 AS answer\\G').stdout)

    def test_multiline_human_results_and_show_create(self):
        invoke('DROP TABLE IF EXISTS cli_multiline; CREATE TABLE cli_multiline(id INT,c1 VECTOR(3))')
        try:
            query = 'SHOW CREATE TABLE cli_multiline'
            ddl = invoke(query, ['-BNr']).stdout.split(b'\t',1)[1].rstrip(b'\n')
            for flags in [['--table'], ['--table','--quick'], ['--vertical']]:
                output = invoke(query, flags).stdout
                self.assertIn(ddl, output)
                self.assertIn(b'(\n  `id`', output)
                self.assertNotIn(b'(\\n', output)
            mysql = shutil.which('mysql')
            if mysql:
                for flag in ['--table', '--vertical']:
                    reference = subprocess.run([mysql, '--no-defaults', '-h127.0.0.1', f'-P{OPTIONS.port}', '-uroot', '-proot', 'test', '--ssl-mode=REQUIRED', flag, '-e', query], capture_output=True, check=True, env=ENV, timeout=15)
                    self.assertEqual(invoke(query,[flag]).stdout, reference.stdout)
            if os.name == 'posix':
                terminal = Terminal()
                try:
                    terminal.send(b'SHOW CREATE TABLE cli_multiline;\r')
                    displayed = terminal.until(b'row(s) in set').replace(b'\r\n', b'\n')
                    self.assertIn(ddl, displayed)
                    terminal.until(b'mydb [test]> ')
                finally:
                    terminal.close()
                self.assertEqual(terminal.status, 0)
            query = "SELECT CONCAT('first',CHAR(10),'second',CHAR(10)) AS text, CONCAT('literal',CHAR(92),'n') AS literal, CONCAT('中',CHAR(10),CHAR(10),'文') AS unicode_text"
            for flags in [['--table'], ['--table','--quick'], ['--vertical']]:
                output = invoke(query, flags).stdout
                self.assertIn(b'first\nsecond\n', output)
                self.assertIn(b'literal\\n', output)
                self.assertNotIn(b'literal\\\\n', output)
                self.assertIn('中\n\n文'.encode(), output)
            tsv = invoke(query, ['-N']).stdout
            self.assertIn(b'first\\nsecond\\n', tsv)
            self.assertIn(b'literal\\\\n', tsv)
        finally:
            invoke('DROP TABLE cli_multiline')

    def test_error_policy_and_location(self):
        sql = b'SELECT 11;\nSELECT * FROM cli_missing_table;\nSELECT 22;\n'
        result = invoke(input=sql, ok=False)
        self.assertEqual(result.returncode, 1)
        self.assertNotIn(b'22', result.stdout)
        self.assertIn(b'<stdin>:2:', result.stderr)
        self.assertIn(b'42S02', result.stderr)
        result = invoke(input=sql, flags=['--force'], ok=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn(b'22', result.stdout)

    def test_execute_locations_and_parser_locations(self):
        result = invoke(flags=['-e', 'SELECT 1;\nSELECT 2;', '-e', 'SELECT * FROM cli_missing_table'], ok=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn(b'<execute:2>:1:', result.stderr)
        result = invoke(input=b"SELECT\n'unterminated\n", ok=False)
        self.assertIn(b'<stdin>:1:', result.stderr)
        self.assertNotIn(b'line 1:', result.stderr)

    def test_transaction_same_session(self):
        sql = "DROP TABLE IF EXISTS cli_tx; CREATE TABLE cli_tx(id INT PRIMARY KEY); BEGIN; INSERT INTO cli_tx VALUES(1); ROLLBACK; SELECT COUNT(*) FROM cli_tx; BEGIN; INSERT INTO cli_tx VALUES(2); COMMIT; SELECT id FROM cli_tx; DROP TABLE cli_tx;"
        self.assertEqual(invoke(sql, ['-N']).stdout, b'0\n2\n')

    def test_delimiter_source_and_multiple_results(self):
        child = WORK / 'child script.sql'
        child.write_text('delimiter $$\nDROP PROCEDURE IF EXISTS cli_p$$\nCREATE PROCEDURE cli_p() BEGIN SELECT 11 AS a; SELECT 22 AS b; END$$\ndelimiter ;\nCALL cli_p();\nDROP PROCEDURE cli_p;\n')
        parent = WORK / 'parent.sql'
        parent.write_text("source 'child script.sql';\nSELECT 33;\n")
        self.assertEqual(invoke(flags=['-N'], input=f'source {parent}\n'.encode()).stdout, b'11\n22\n33\n')

    def test_commands_inside_multiline_sql_are_data(self):
        self.assertEqual(invoke(input=b"SELECT 'a\nclear\nb' AS s;\n", flags=['-N']).stdout, b'a\\nclear\\nb\n')
        invoke('DROP TABLE IF EXISTS cli_command_names; CREATE TABLE cli_command_names(clear INT, print INT); INSERT INTO cli_command_names VALUES(11,22)')
        try:
            self.assertEqual(invoke(input=b'SELECT\nclear\nFROM cli_command_names;\nSELECT\nprint\nFROM cli_command_names;\n', flags=['-N']).stdout, b'11\n22\n')
            self.assertEqual(invoke(input=b'SELECT 999\n\\c\nSELECT 42;\n', flags=['-N']).stdout, b'42\n')
        finally:
            invoke('DROP TABLE cli_command_names')

    def test_source_cycle_and_unterminated_input(self):
        path = WORK / 'cycle.sql'; path.write_text('source cycle.sql\n')
        result = invoke(flags=['--force'], input=f'source {path}\n'.encode(), ok=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn(b'source cycle', result.stderr)
        for text in [b"SELECT 'unclosed", b'SELECT 1 /* unclosed']:
            self.assertIn(b'unterminated', invoke(input=text, ok=False).stderr)

    def test_sql_mode_updates_before_next_statement(self):
        self.assertEqual(invoke("SET sql_mode='NO_BACKSLASH_ESCAPES'; SELECT 'a\\'; SELECT 22;", ['-N']).stdout, b'a\\\\\n22\n')
        self.assertEqual(invoke('SET sql_mode=\'ANSI_QUOTES\'; SELECT 1 AS "a\\"; SELECT 22;', ['-N']).stdout, b'1\n22\n')

    def test_config_env_and_cli_precedence(self):
        cfg = WORK / 'client.ini'; cfg.write_text('[mydb-cli]\nport=1\nuser=root\npassword=root\ndatabase=test\n')
        env = dict(ENV, MYDB_CLI_PORT=str(OPTIONS.port))
        result = subprocess.run([OPTIONS.cli, '--config', str(cfg), '-N', '-e', 'SELECT 42'], capture_output=True, env=env, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, b'42\n')
        self.assertEqual(subprocess.run([OPTIONS.cli, '--config', str(cfg), '-P1', '-e', 'SELECT 1'], capture_output=True, env=env, timeout=10).returncode, 1)
        cfg.write_text('[mydb-cli]\nunknown=1\n')
        self.assertEqual(subprocess.run([OPTIONS.cli, '--config', str(cfg)], capture_output=True, env=env, timeout=10).returncode, 2)

    def test_upload_is_explicit_and_real(self):
        path = WORK / 'upload.tsv'; path.write_text('1\t中文\n2\txyz\n')
        setup = 'DROP TABLE IF EXISTS cli_load; CREATE TABLE cli_load(id INT, s TEXT);'
        invoke(setup)
        sql = f"LOAD DATA LOCAL INFILE '{path}' INTO TABLE cli_load"
        result = invoke(sql, ok=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn(b'LOCAL INFILE is disabled', result.stderr)
        invoke(sql, ['--local-infile=1'])
        self.assertEqual(invoke('SELECT * FROM cli_load ORDER BY id', ['-N']).stdout, '1\t中文\n2\txyz\n'.encode())
        self.assertEqual(invoke("LOAD DATA LOCAL INFILE '/tmp/mydb/nonexistent-cli-file' INTO TABLE cli_load", ['--local-infile=1'], ok=False).returncode, 1)
        invoke('DROP TABLE cli_load')

    def test_native_types_and_empty_result(self):
        sql = "SELECT JSON_OBJECT('a',1), TO_VECTOR('[1,2,3]'), ST_AsText(ST_GeomFromText('POINT(1 2)')), CAST('00:00:00' AS TIME), 1.25e2"
        out = invoke(sql, ['-N']).stdout
        self.assertIn(b'[1,2,3]', out)
        self.assertIn(b'POINT(1 2)', out)
        self.assertIn(b'00:00:00', out)
        for kind in ['FLOAT32', 'FLOAT16', 'BFLOAT16', 'INT8']:
            self.assertEqual(invoke(f"SELECT TO_VECTOR('[1,-2,3]','{kind}')", ['-N']).stdout, b'[1,-2,3]\n')
        self.assertEqual(invoke("SELECT TO_VECTOR('[1,0,1]','BINARY')", ['-N']).stdout, b'[1,0,1]\n')
        self.assertEqual(invoke('SELECT 1 AS x WHERE FALSE').stdout, b'x\n')
        self.assertEqual(invoke('SELECT 1 AS x WHERE FALSE', ['-N']).stdout, b'')

    def test_output_failure_stops_without_draining(self):
        cmd = [OPTIONS.cli, '--no-config', f'-P{OPTIONS.port}', '-uroot', '-proot', '-e', 'SELECT * FROM generate_series(1,1000000); SELECT SLEEP(30);']
        child = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=ENV)
        child.stdout.close()
        start = time.monotonic()
        child.wait(timeout=5)
        self.assertNotEqual(child.returncode, 0)
        self.assertLess(time.monotonic() - start, 3)
        child.stderr.close()

    def test_table_spill_is_complete_and_temporary(self):
        env = dict(ENV, TMPDIR=str(WORK))
        before = set(WORK.iterdir())
        result = invoke("SELECT n, REPEAT('中',100) AS text FROM generate_series(1,40000) s(n)", ['--table', '-N'], env=env)
        self.assertEqual(result.stdout.count('中'.encode()), 4000000)
        self.assertEqual(len(result.stdout.splitlines()), 40002)
        self.assertEqual(set(WORK.iterdir()), before)

    def test_spool_failure_and_invalid_input_stop(self):
        not_dir = WORK / 'not-a-directory'; not_dir.write_bytes(b'x')
        env = dict(ENV, TMPDIR=str(not_dir))
        result = invoke("SELECT REPEAT('a',1000) FROM generate_series(1,10000); SELECT 987654321;", ['--table'], env=env, ok=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn(b'result spool', result.stderr)
        self.assertNotIn(b'987654321', result.stdout)
        self.assertEqual(invoke(input=b"SELECT '\xff';", ok=False).returncode, 1)

    @unittest.skipUnless(os.name == 'posix', 'requires select on process pipes')
    def test_streaming_output_precedes_server_terminator(self):
        for flags in [['-N'], ['-E'], ['-tq']]:
            listener = socket.socket(); listener.bind(('127.0.0.1',0)); listener.listen(1)
            port = listener.getsockname()[1]
            release = threading.Event(); errors = []
            def fixture():
                try:
                    with listener.accept()[0] as sock:
                        sock.settimeout(5)
                        def receive():
                            def exact(n):
                                value=b''
                                while len(value)<n:
                                    part=sock.recv(n-len(value))
                                    if not part: raise EOFError()
                                    value+=part
                                return value
                            h=exact(4);return exact(int.from_bytes(h[:3],'little'))
                        def packet(seq,payload): sock.sendall(len(payload).to_bytes(3,'little')+bytes([seq])+payload)
                        caps=0x01ABA20D
                        greeting=b'\x0a8.0.46-fixture\0'+struct.pack('<I',123)+b'12345678\0'+struct.pack('<H',caps&65535)+b'\x2d\x02\0'+struct.pack('<H',caps>>16)+b'\x15'+bytes(10)+b'abcdefghijkl\0mysql_native_password\0'
                        packet(0,greeting);receive();packet(2,b'\0\0\0\x02\0\0\0\0')
                        def rows(names,values,finish=True):
                            seq=1;packet(seq,bytes([len(names)]));seq+=1
                            for name in names:
                                fields=[b'def',b'',b'',b'',name,b'']
                                col=b''.join(bytes([len(v)])+v for v in fields)+b'\x0c\x2d\0'+struct.pack('<I',100)+b'\xfd\0\0\0\0\0'
                                packet(seq,col);seq+=1
                            packet(seq,b''.join(bytes([len(v)])+v for v in values));seq+=1
                            if finish: packet(seq,b'\xfe\0\0\x02\0\0\0\0')
                            return seq
                        receive();rows([b'max_allowed_packet'],[b'67108864'])
                        for _ in range(2): receive();packet(1,b'\0\0\0\x02\0\0\0\0')
                        receive();rows([b'DATABASE()',b'sql_mode',b'VERSION()'],[b'test',b'',b'8.0.46-fixture'])
                        receive();seq=rows([b'answer'],[b'42'],False)
                        if not release.wait(5): raise TimeoutError('client waited for the full result')
                        packet(seq,b'\xfe\0\0\x02\0\0\0\0');receive()
                except Exception as error: errors.append(error)
                finally: listener.close()
            worker=threading.Thread(target=fixture);worker.start()
            cmd=[OPTIONS.cli,'--no-config',f'-P{port}','--ssl-mode=DISABLED','-uroot','-proot','test',*flags,'-e','SELECT 42']
            child=subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=ENV)
            try:
                self.assertTrue(select.select([child.stdout],[],[],3)[0], 'no output before terminator')
                self.assertTrue(child.stdout.readline())
                self.assertIsNone(child.poll())
                release.set();out,err=child.communicate(timeout=5)
                self.assertEqual(child.returncode,0,err)
            finally:
                release.set()
                if child.poll() is None: child.kill();child.wait()
                worker.join(timeout=6)
            self.assertEqual(errors,[])

    def test_certificate_chain_identity_and_client_identity(self):
        server_binary = OPTIONS.server_binary or str(ROOT / 'target/release/mydb')
        if not Path(server_binary).is_file() or not shutil.which('openssl'):
            self.skipTest('requires a local server binary and openssl')
        certs = WORK / 'tls'; certs.mkdir(exist_ok=True)
        def openssl(*args):
            subprocess.run(['openssl', *args], cwd=certs, check=True, capture_output=True, timeout=30)
        openssl('req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1', '-subj', '/CN=CLI Test CA', '-keyout', 'ca.key', '-out', 'ca.pem')
        for name, cn, usage in [('server', 'localhost', 'serverAuth'), ('client', 'cli-test', 'clientAuth')]:
            openssl('req', '-newkey', 'rsa:2048', '-nodes', '-subj', '/CN=' + cn, '-keyout', name + '.key', '-out', name + '.csr')
            ext = 'basicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=' + usage + '\n'
            if name == 'server': ext += 'subjectAltName=DNS:localhost\n'
            (certs / (name + '.ext')).write_text(ext)
            openssl('x509', '-req', '-in', name + '.csr', '-CA', 'ca.pem', '-CAkey', 'ca.key', '-CAcreateserial', '-days', '1', '-extfile', name + '.ext', '-out', name + '.pem')
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0)); port = sock.getsockname()[1]
        with (certs / 'server.log').open('wb') as log:
            server = subprocess.Popen([server_binary, '--host', '127.0.0.1', '--port', str(port), '--data-dir', str(certs / 'data'), '--ssl-cert', str(certs / 'server.pem'), '--ssl-key', str(certs / 'server.key')], stdout=log, stderr=subprocess.STDOUT, env=dict(ENV, MYDB_SSL_CA=str(certs / 'ca.pem')))
            try:
                for _ in range(100):
                    if invoke('SELECT 1', [f'-P{port}'], ok=False).returncode == 0: break
                    if server.poll() is not None: self.fail('TLS fixture exited: ' + (certs / 'server.log').read_text())
                    time.sleep(.05)
                else: self.fail('TLS fixture failed to start')
                verify = [f'-P{port}', f'--ssl-ca={certs / "ca.pem"}']
                self.assertEqual(invoke('SELECT 42', [*verify, '--ssl-mode=VERIFY_CA', '-N']).stdout, b'42\n')
                self.assertEqual(invoke('SELECT 42', [*verify, '--ssl-mode=VERIFY_IDENTITY'], ok=False).returncode, 1)
                self.assertEqual(invoke('SELECT 42', [*verify, '--ssl-mode=VERIFY_IDENTITY', '-hlocalhost', '-N']).stdout, b'42\n')
                invoke("CREATE USER 'cli_tls'@'localhost' IDENTIFIED BY 'client-password' REQUIRE X509; GRANT SELECT ON test.* TO 'cli_tls'@'localhost';", [f'-P{port}'])
                user = [*verify, '--ssl-mode=VERIFY_CA', '--user=cli_tls', '--password=client-password']
                self.assertEqual(invoke('SELECT 42', user, ok=False).returncode, 1)
                self.assertEqual(invoke('SELECT 42', [*user, f'--ssl-cert={certs / "client.pem"}', f'--ssl-key={certs / "client.key"}', '-N']).stdout, b'42\n')
                self.assertEqual(invoke('SELECT 42', [*user, f'--ssl-cert={certs / "client.pem"}', f'--ssl-key={certs / "server.key"}'], ok=False).returncode, 1)
            finally:
                server.send_signal(signal.SIGINT)
                try: server.wait(timeout=30)
                except subprocess.TimeoutExpired: server.kill(); server.wait()

    def test_warning_details(self):
        result = invoke("SELECT CAST('oops' AS UNSIGNED)", ['--show-warnings'])
        self.assertIn(b'1292', result.stdout)
        self.assertIn(b'Warning', result.stdout)

    def test_tls_required_disabled_and_invalid_ca(self):
        self.assertEqual(invoke('status').stdout.count(b'TLS: enabled'), 1)
        self.assertIn(b'TLS: disabled', invoke('status', ['--ssl-mode=DISABLED']).stdout)
        self.assertEqual(invoke('SELECT 1', ['--ssl-mode=VERIFY_CA'], ok=False).returncode, 2)
        self.assertEqual(invoke('SELECT 1', ['--ssl-cert=/tmp/mydb/absent'], ok=False).returncode, 2)
        bad_ca = WORK / 'bad-ca.pem'; bad_ca.write_text('not a certificate')
        self.assertEqual(invoke('SELECT 1', ['--ssl-mode=VERIFY_CA', f'--ssl-ca={bad_ca}'], ok=False).returncode, 1)

    def test_unknown_options_and_authentication(self):
        self.assertEqual(invoke('SELECT 1', ['--socket=/tmp/mydb/a'], ok=False).returncode, 2)
        result = invoke('SELECT 1', ['--password=wrong-password'], ok=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn(b'1045', result.stderr)
        self.assertNotIn(b'wrong-password', result.stderr)

    @unittest.skipUnless(os.name == 'posix', 'requires a Unix PTY')
    def test_banner_and_status_show_full_server_version(self):
        version = invoke('SELECT VERSION()', ['-N']).stdout.rstrip(b'\n')
        expected = b'Server version: ' + version + b'\r\n'
        terminal = Terminal()
        try:
            self.assertIn(expected, terminal.transcript)
            terminal.send(b'status\r')
            displayed = terminal.until(b'Client charset: utf8mb4')
            self.assertIn(expected, displayed)
            terminal.until(b'mydb [test]> ')
        finally:
            terminal.close()
        self.assertEqual(terminal.status, 0)
        self.assertEqual(invoke('SELECT 42', ['-N']).stdout, b'42\n')

    @unittest.skipUnless(os.name == 'posix', 'requires a Unix PTY')
    def test_password_tty_preserves_sql_pipe(self):
        terminal = Terminal(['--password'], password=b'root', stdin_sql=b'SELECT 42 AS answer;\n')
        try:
            self.assertIn(b'42', terminal.until(b'42\r\n'))
            self.assertNotIn(b'root', terminal.transcript)
        finally: terminal.close()
        self.assertEqual(terminal.status, 0)

    @unittest.skipUnless(os.name == 'posix', 'requires a Unix PTY')
    def test_eof_discards_buffer_and_restores_terminal(self):
        import termios
        terminal = Terminal()
        try:
            terminal.send(b'SELECT\r')
            terminal.until(b'    -> ')
            terminal.send(b'\x04')
            terminal.until(b'discarded unfinished SQL buffer')
            terminal.until(b'Bye')
            flags=termios.tcgetattr(terminal.fd)[3]
            self.assertTrue(flags & termios.ICANON)
            self.assertTrue(flags & termios.ECHO)
        finally: terminal.close()
        self.assertEqual(terminal.status, 0)

    @unittest.skipUnless(os.name == 'posix', 'requires Unix signals')
    def test_batch_cancel_stops_remaining_input(self):
        cmd = [OPTIONS.cli, '--no-config', f'-P{OPTIONS.port}', '-uroot', '-proot', '-N', '-e', 'SELECT SLEEP(30) AS cli_cancel_batch; SELECT 987654321;']
        child = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=ENV)
        try:
            active('cli_cancel_batch')
            start = time.monotonic(); child.send_signal(signal.SIGINT)
            out, err = child.communicate(timeout=5)
            self.assertEqual(child.returncode, 130, err)
            self.assertNotIn(b'987654321', out)
            self.assertLess(time.monotonic() - start, 3)
        finally:
            if child.poll() is None:
                child.kill(); child.wait()

    @unittest.skipUnless(os.name == 'posix', 'requires a Unix PTY')
    def test_interactive_cancel_reuse_completion_and_history(self):
        history = WORK / 'history'
        terminal = Terminal(['--no-history=false', f'--history-file={history}'])
        try:
            terminal.send(b'SELE\t 42 AS cli_completion;\r')
            data = terminal.until(b'row(s) in set')
            self.assertIn(b'42', data)
            terminal.until(b'mydb [test]> ')
            for i in range(4):
                marker = f'cli_cancel_terminal_{i}'
                terminal.send(f'SELECT SLEEP(30) AS {marker};\r'.encode())
                terminal.until(b'\r\n')
                active(marker)
                terminal.send(b'\x03')
                terminal.until(b'Query interrupted')
                terminal.until(b'mydb [test]> ')
                terminal.send(b'SELECT 12345;\r')
                self.assertIn(b'12345', terminal.until(b'row(s) in set'))
                terminal.until(b'mydb [test]> ')
            terminal.send(b"SET @password =\r")
            terminal.until(b'    -> ')
            terminal.send(b"'cli-hidden-secret';\r")
            terminal.until(b'Query OK')
            terminal.until(b'mydb [test]> ')
            terminal.send(b'SELECT 777')
            terminal.send(b'\x03')
            terminal.until(b'mydb [test]> ')
        finally:
            terminal.close()
        self.assertEqual(terminal.status, 0)
        data = history.read_bytes()
        self.assertIn(b'12345', data)
        self.assertNotIn(b'cli-hidden-secret', data)
        self.assertEqual(history.stat().st_mode & 0o777, 0o600)


def main():
    global OPTIONS, ENV, WORK
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cli', type=lambda p: str(Path(p).resolve()), required=True)
    parser.add_argument('--server-binary', type=lambda p: str(Path(p).resolve()))
    parser.add_argument('--port', type=int, default=14837)
    parser.add_argument('--output', type=Path, required=True)
    OPTIONS = parser.parse_args()
    WORK = OPTIONS.output.resolve()
    if not WORK.is_relative_to(Path('/tmp/mydb').resolve()):
        parser.error('--output must be below /tmp/mydb')
    WORK.mkdir(parents=True, exist_ok=True)
    ENV = {k: v for k,v in os.environ.items() if not k.startswith('MYDB_CLI_')}
    ENV.update(TMPDIR=str(WORK), TERM='xterm-256color', LC_ALL='en_US.UTF-8')
    server = None
    if OPTIONS.server_binary:
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0)); OPTIONS.port = sock.getsockname()[1]
        server_log = (WORK / 'server.log').open('wb')
        server = subprocess.Popen([OPTIONS.server_binary, '--host', '127.0.0.1', '--port', str(OPTIONS.port), '--data-dir', str(WORK / 'data')], stdout=server_log, stderr=subprocess.STDOUT, env=ENV)
        for _ in range(200):
            try:
                if invoke('SELECT 1', ok=False).returncode == 0:
                    break
            except subprocess.TimeoutExpired:
                pass
            if server.poll() is not None:
                raise RuntimeError('server exited: ' + (WORK / 'server.log').read_text())
            time.sleep(.05)
        else:
            raise RuntimeError('server startup timed out')
    try:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(Acceptance)
        result = unittest.TextTestRunner(verbosity=2).run(suite)
        (WORK / 'result.json').write_text(json.dumps({'tests': result.testsRun, 'failures': len(result.failures), 'errors': len(result.errors), 'skipped': len(result.skipped), 'cli': OPTIONS.cli, 'port': OPTIONS.port}, indent=2) + '\n')
    finally:
        if server:
            server.send_signal(signal.SIGINT)
            try: server.wait(timeout=30)
            except subprocess.TimeoutExpired: server.kill(); server.wait()
            server_log.close()
    raise SystemExit(0 if result.wasSuccessful() else 1)


if __name__ == '__main__':
    main()
