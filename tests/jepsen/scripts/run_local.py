#!/usr/bin/env python3
"""Run RC Jepsen workloads against an isolated, owned local mydb process."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import time
import uuid

PROJECT = Path(__file__).resolve().parents[1]
ROOT = PROJECT.parents[1]
DEFAULT_OUTPUT_ROOT = Path('/tmp/mydb/jepsen/runs')


def stop(process):
    if process is not None and process.poll() is None:
        # Every process here is created in a new session; never use pkill.
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            process.wait(timeout=10)
            return
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=10)


def positive(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, default=ROOT / 'target/release/mydb', help='mydb binary (default: target/release/mydb)')
    parser.add_argument('--lein', default=os.environ.get('LEIN', 'lein'))
    parser.add_argument('--workload', choices=['all', 'rc-probe', 'append', 'rc-interleave', 'rc-branches'], default='all')
    parser.add_argument('--branch-protocol', choices=['both', 'text', 'prepared'], default='both')
    parser.add_argument('--branch-case', default='all')
    parser.add_argument('--scan-rows', type=positive, default=512)
    parser.add_argument('--interleave-case', default='all')
    parser.add_argument('--time-limit', type=positive, default=30)
    parser.add_argument('--concurrency', type=positive, default=8)
    parser.add_argument('--rate', type=positive, default=100)
    parser.add_argument('--key-count', type=positive, default=16)
    parser.add_argument('--seed', type=int, default=1)
    parser.add_argument('--output', type=Path, help='artifact directory (default: /tmp/mydb/jepsen/runs/<UTC timestamp>-<id>)')
    args = parser.parse_args()
    if not 128 <= args.scan_rows <= 8192:
        parser.error('--scan-rows must be 128..8192')
    if (args.workload in ('all', 'rc-branches') and args.branch_protocol == 'text'
            and args.branch_case in ('prepared-rebind', 'prepared-long-data', 'prepared-cursor')):
        parser.error('selected branch requires --branch-protocol prepared or both')
    binary = args.binary.resolve()
    if not binary.is_file():
        parser.error(f'build mydb first: cargo build --release --bin mydb (missing {binary})')
    lein = shutil.which(args.lein)
    if not lein:
        parser.error('Leiningen is required; install it or set LEIN=/path/to/lein')
    java = shutil.which('java')
    if not java:
        parser.error('Java 21+ is required')
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    output = (args.output or DEFAULT_OUTPUT_ROOT / f'{stamp}-{uuid.uuid4().hex[:8]}').resolve()
    output.mkdir(parents=True, exist_ok=False)
    print(f'Artifacts: {output}', flush=True)
    workloads = ['rc-probe', 'append', 'rc-interleave', 'rc-branches'] if args.workload == 'all' else [args.workload]
    results = [{'workload': w, 'status': 'incomplete', 'exit_code': None} for w in workloads]
    (output / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
    classpath = subprocess.check_output([lein, 'classpath'], cwd=PROJECT, text=True,
                                        timeout=300).strip()
    # Resolve classpath before switching to each workload's output directory.
    classpath = os.pathsep.join(str((PROJECT / p).resolve()) if not Path(p).is_absolute()
                               else p for p in classpath.split(os.pathsep))
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 0))
        port = probe.getsockname()[1]
    env = {k: v for k, v in os.environ.items() if not k.startswith('MYDB_')}
    env.update(MYDB_DATA_DIR=str(output / 'data'), MYDB_ROOT_PASSWORD='root',
               MYDB_DURABILITY='strict', MYDB_MEMORY_TARGET='1000',
               JEPSEN_MYDB_PASSWORD='root', RUST_BACKTRACE='1')
    git = lambda *a: subprocess.check_output(['git', *a], cwd=ROOT, text=True).strip()
    with binary.open('rb') as binary_file:
        binary_hash = hashlib.file_digest(binary_file, 'sha256').hexdigest()
    manifest = {
        'commit': git('rev-parse', 'HEAD'), 'working_tree': git('status', '--short'),
        'binary': str(binary), 'binary_sha256': binary_hash,
        'durability': 'strict', 'memory_target': '1000', 'port': port,
        'seed': args.seed, 'concurrency': args.concurrency, 'rate': args.rate,
        'time_limit': args.time_limit, 'key_count': args.key_count,
        'workload': args.workload, 'scan_rows': args.scan_rows, 'interleave_case': args.interleave_case,
        'branch_protocol': args.branch_protocol, 'branch_case': args.branch_case,
        'java': subprocess.check_output([java, '-version'], stderr=subprocess.STDOUT, text=True).strip(),
    }
    sources = [PROJECT / 'project.clj', Path(__file__).resolve(),
               *sorted((PROJECT / 'src').rglob('*.clj')),
               *sorted((PROJECT / 'test').rglob('*.clj'))]
    for source in sources:
        destination = output / 'harness' / source.relative_to(PROJECT)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
    (output / 'working-tree.patch').write_text(git('diff', '--binary') + '\n')
    (output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    server = worker = None
    try:
        with (output / 'mydb.log').open('w') as log:
            server = subprocess.Popen([str(binary), '--host', '127.0.0.1', '--port', str(port)],
                                      env=env, stdout=log, stderr=subprocess.STDOUT,
                                      start_new_session=True)
        deadline = time.monotonic() + 30
        while True:
            if server.poll() is not None:
                raise RuntimeError('mydb exited during startup; see mydb.log')
            if f'mydb listening on 127.0.0.1:{port}' in (output / 'mydb.log').read_text(errors='replace'):
                break
            if time.monotonic() > deadline:
                raise TimeoutError('mydb startup timeout; see mydb.log')
            time.sleep(0.05)
        workloads = ['rc-probe', 'append', 'rc-interleave', 'rc-branches'] if args.workload == 'all' else [args.workload]
        for index, workload in enumerate(workloads):
            directory = output / workload
            directory.mkdir()
            command = [java, '-Xmx2g', '-Djava.awt.headless=true',
                       f'-Dmydb.jepsen.store={directory / "store"}', '-cp', classpath,
                       'clojure.main', '-m', 'mydb.core', 'test', '--workload', workload,
                       '--port', str(port), '--concurrency', str(args.concurrency),
                       '--time-limit', str(args.time_limit), '--rate', str(args.rate),
                       '--key-count', str(args.key_count), '--seed', str(args.seed),
                       '--scan-rows', str(args.scan_rows), '--interleave-case', args.interleave_case,
                       '--branch-protocol', args.branch_protocol, '--branch-case', args.branch_case]
            print(f'Running {workload}: {args.concurrency} workers, {args.time_limit}s', flush=True)
            start = time.monotonic()
            with (directory / 'console.log').open('w') as log:
                worker = subprocess.Popen(command, cwd=directory, env=env, stdout=log,
                                          stderr=subprocess.STDOUT, start_new_session=True)
                try:
                    code = worker.wait(timeout=args.time_limit + 180)
                except subprocess.TimeoutExpired:
                    stop(worker)
                    code = 124
            results[index] = {'workload': workload, 'status': 'pass' if code == 0 else ('incomplete' if code == 124 else 'fail'), 'exit_code': code,
                              'elapsed_seconds': round(time.monotonic() - start, 3)}
            (output / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
            print(f'{workload}: exit={code}; {directory / "store/latest/results.edn"}', flush=True)
        return 0 if all(r['status'] == 'pass' for r in results) else 1
    finally:
        stop(worker)
        stop(server)


if __name__ == '__main__':
    raise SystemExit(main())
