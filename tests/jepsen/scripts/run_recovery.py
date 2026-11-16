#!/usr/bin/env python3
"""Local Jepsen/Elle crash and COMMIT-disconnect scenarios (strict durability).

The packet proxy is deliberately limited to the harness's unencrypted,
uncompressed MySQL classic protocol. It never connects to an existing server.
"""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import selectors
import shutil
import socket
import subprocess
import threading
import time
import uuid

from run_local import DEFAULT_OUTPUT_ROOT, PROJECT, ROOT, stop


def receive(sock, size):
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise EOFError('peer closed')
        data.extend(chunk)
    return bytes(data)


def packet(sock):
    header = receive(sock, 4)
    return header + receive(sock, int.from_bytes(header[:3], 'little'))


def is_commit(data):
    if data[4:5] != b'\x03':
        return False
    sql = data[5:]
    # Connector/J negotiates CLIENT_QUERY_ATTRIBUTES with mydb. This harness
    # binds no attributes: parameter_count=0, parameter_set_count=1.
    if sql.startswith(b'\x00\x01'):
        sql = sql[2:]
    return sql.strip().rstrip(b';').strip().upper() == b'COMMIT'


def close_socket(sock):
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    sock.close()


class Proxy:
    def __init__(self, upstream_port, crash):
        self.upstream_port = upstream_port
        self.crash = crash
        self.lock = threading.Lock()
        self.mode = None
        self.events = []
        self.errors = []
        self.sockets = set()
        self.closed = False
        self.listener = socket.socket()
        self.listener.bind(('127.0.0.1', 0))
        self.listener.listen()
        self.port = self.listener.getsockname()[1]
        threading.Thread(target=self.accept, daemon=True).start()

    def arm(self, mode):
        if mode not in ('before', 'after-ok', 'inflight'):
            raise ValueError(mode)
        with self.lock:
            if self.mode is not None:
                raise RuntimeError('previous fault was not consumed')
            self.mode = mode

    def accept(self):
        while not self.closed:
            try:
                client, _ = self.listener.accept()
            except OSError:
                return
            try:
                server = socket.create_connection(('127.0.0.1', self.upstream_port), timeout=5)
                server.settimeout(None)
            except OSError:
                close_socket(client)
                continue
            with self.lock:
                self.sockets.update((client, server))
            threading.Thread(target=self.relay, args=(client, server), daemon=True).start()

    def relay(self, client, server):
        state = {'fault': None}

        def responses():
            try:
                while True:
                    data = packet(server)
                    if state['fault'] == 'after-ok':
                        if data[4:5] != b'\x00':
                            raise RuntimeError('expected server COMMIT OK before dropping response')
                        with self.lock:
                            self.events.append({'mode': 'after-ok', 'evidence': 'server-ok-discarded'})
                        return
                    if state['fault'] == 'inflight':
                        # Never deliver the response, even if COMMIT wins the race.
                        continue
                    client.sendall(data)
            except (OSError, EOFError):
                pass
            except Exception as exc:
                with self.lock:
                    self.errors.append(repr(exc))
            finally:
                close_socket(client)
                close_socket(server)

        response_thread = threading.Thread(target=responses, daemon=True)
        response_thread.start()
        try:
            while True:
                data = packet(client)
                mode = None
                if is_commit(data):
                    with self.lock:
                        mode, self.mode = self.mode, None
                    state['fault'] = mode
                if mode == 'before':
                    with self.lock:
                        self.events.append({'mode': mode, 'evidence': 'commit-request-not-forwarded'})
                    return
                server.sendall(data)
                if mode == 'inflight':
                    self.crash()
                    with self.lock:
                        self.events.append({'mode': mode, 'evidence': 'request-forwarded-server-sigkill'})
                    return
        except (OSError, EOFError):
            pass
        except Exception as exc:
            with self.lock:
                self.errors.append(repr(exc))
        finally:
            close_socket(client)
            close_socket(server)
            response_thread.join(timeout=5)
            with self.lock:
                self.sockets.discard(client)
                self.sockets.discard(server)

    def close(self):
        self.closed = True
        close_socket(self.listener)
        with self.lock:
            sockets = list(self.sockets)
        for sock in sockets:
            close_socket(sock)


class Server:
    def __init__(self, binary, output, env):
        self.binary, self.output, self.env = binary, output, env
        self.process = None
        self.generation = 0
        self.crashes = 0
        self.lock = threading.Lock()
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            self.port = sock.getsockname()[1]

    def start(self):
        self.generation += 1
        path = self.output / f'mydb-{self.generation:02d}.log'
        with path.open('w') as log:
            self.process = subprocess.Popen(
                [str(self.binary), '--host', '127.0.0.1', '--port', str(self.port)],
                env=self.env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f'mydb exited; see {path}')
            if f'mydb listening on 127.0.0.1:{self.port}' in path.read_text(errors='replace'):
                return
            time.sleep(.05)
        raise TimeoutError(f'recovery startup timeout; see {path}')

    def crash(self):
        with self.lock:
            if self.process is not None and self.process.poll() is None:
                self.process.kill()  # Only the process created by this runner.
                self.process.wait(timeout=10)
                self.crashes += 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, default=ROOT / 'target/release/mydb', help='mydb binary (default: target/release/mydb)')
    parser.add_argument('--lein', default=os.environ.get('LEIN', 'lein'))
    parser.add_argument('--output', type=Path, help='artifact directory (default: /tmp/mydb/jepsen/runs/<UTC timestamp>-recovery-<id>)')
    args = parser.parse_args()
    binary = args.binary.resolve()
    if not binary.is_file():
        parser.error('build mydb first: cargo build --release --bin mydb')
    java, lein = shutil.which('java'), shutil.which(args.lein)
    if not java or not lein:
        parser.error('Java and Leiningen are required')
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    output = (args.output or DEFAULT_OUTPUT_ROOT / f'{stamp}-recovery-{uuid.uuid4().hex[:8]}').resolve()
    output.mkdir(parents=True, exist_ok=False)
    print(f'Artifacts: {output}', flush=True)
    (output / 'results.json').write_text(json.dumps({'valid': False, 'status': 'incomplete', 'error': 'setup-or-run-not-completed'}) + '\n')
    classpath = subprocess.check_output([lein, 'classpath'], cwd=PROJECT, text=True, timeout=300).strip()
    classpath = os.pathsep.join(str((PROJECT / p).resolve()) if not Path(p).is_absolute() else p
                               for p in classpath.split(os.pathsep))
    env = {k: v for k, v in os.environ.items() if not k.startswith('MYDB_')}
    env.update(MYDB_DATA_DIR=str(output / 'data'), MYDB_ROOT_PASSWORD='root',
               MYDB_DURABILITY='strict', MYDB_MEMORY_TARGET='1000',
               JEPSEN_MYDB_PASSWORD='root', RUST_BACKTRACE='1')
    with binary.open('rb') as file:
        binary_hash = hashlib.file_digest(file, 'sha256').hexdigest()
    manifest = {'binary': str(binary), 'binary_sha256': binary_hash, 'durability': 'strict',
                'commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
                'working_tree': subprocess.check_output(['git', 'status', '--short'], cwd=ROOT, text=True),
                'faults': ['crash', 'before', 'after-ok', 'inflight'], 'checkpoint_base': [False, True],
                'java': subprocess.check_output([java, '-version'], stderr=subprocess.STDOUT, text=True)}
    (output / 'manifest.json').write_text(json.dumps(manifest, indent=2))
    (output / 'working-tree.patch').write_bytes(subprocess.check_output(['git', 'diff', '--binary'], cwd=ROOT))
    for source in [PROJECT / 'project.clj', *sorted((PROJECT / 'src').rglob('*.clj')),
                   *sorted((PROJECT / 'test').rglob('*.clj')), *sorted((PROJECT / 'scripts').glob('*.py'))]:
        destination = output / 'harness' / source.relative_to(PROJECT)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
    server = Server(binary, output, env)
    proxy = worker = None
    result = {'valid': False}
    try:
        server.start()
        proxy = Proxy(server.port, server.crash)
        with (output / 'console.log').open('w') as log:
            worker = subprocess.Popen(
                [java, '-Xmx2g', '-Djava.awt.headless=true', '-cp', classpath,
                 'clojure.main', '-m', 'mydb.recovery', str(proxy.port), str(output)],
                cwd=output, env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=log, start_new_session=True)
            # Raw pipe reads avoid TextIO buffering hiding already-read control lines.
            with selectors.DefaultSelector() as selector:
                selector.register(worker.stdout, selectors.EVENT_READ)
                pending = b''
                deadline = time.monotonic() + 600
                while True:
                    if time.monotonic() > deadline:
                        raise TimeoutError('recovery suite exceeded 600 seconds')
                    if not selector.select(timeout=1):
                        continue
                    chunk = os.read(worker.stdout.fileno(), 65536)
                    if not chunk:
                        break
                    pending += chunk
                    while b'\n' in pending:
                        raw, pending = pending.split(b'\n', 1)
                        line = raw.decode(errors='replace')
                        log.write(line + '\n')
                        log.flush()
                        if line.startswith('CONTROL '):
                            action = line.removeprefix('CONTROL ')
                            if action.startswith('arm-'):
                                proxy.arm(action.removeprefix('arm-'))
                            elif action == 'crash-restart':
                                server.crash()
                                server.start()
                            else:
                                raise ValueError(f'unknown controller action: {action}')
                            worker.stdin.write(b'OK\n')
                            worker.stdin.flush()
                        elif line.startswith('SCENARIO '):
                            print(line, flush=True)
            code = worker.wait(timeout=15)
        counts = {mode: sum(e['mode'] == mode for e in proxy.events)
                  for mode in ('before', 'after-ok', 'inflight')}
        result.update(exit_code=code, fault_events=proxy.events, proxy_errors=proxy.errors,
                      crashes=server.crashes, starts=server.generation)
        result['valid'] = (code == 0 and not proxy.errors and proxy.mode is None
                           and all(n == 2 for n in counts.values())
                           and server.crashes == 24 and server.generation == 25)
        return 0 if result['valid'] else 1
    except Exception as exc:
        result['error'] = repr(exc)
        raise
    finally:
        stop(worker)
        if proxy:
            proxy.close()
            result.update(fault_events=proxy.events, proxy_errors=proxy.errors)
        stop(server.process)
        result.update(crashes=server.crashes, starts=server.generation)
        (output / 'results.json').write_text(json.dumps(result, indent=2))
        print(f'Passed: {result["valid"]}; {output / "results.json"}', flush=True)


if __name__ == '__main__':
    raise SystemExit(main())
