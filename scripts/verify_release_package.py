#!/usr/bin/env python3
"""Verify packaged files, versions, TLS SQL, rollback and crash recovery natively."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import tarfile
import time
import tomllib
import zipfile


def sha256(path):
    with path.open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--package', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.package = args.package.resolve()
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    extracted = root / 'extracted'
    extracted.mkdir()
    if args.package.suffix == '.zip':
        with zipfile.ZipFile(args.package) as archive:
            archive.extractall(extracted)
    else:
        with tarfile.open(args.package) as archive:
            archive.extractall(extracted, filter='data')
    manifests = list(extracted.rglob('build-manifest.json'))
    assert len(manifests) == 1, manifests
    payload = manifests[0].parent
    manifest = json.loads(manifests[0].read_text())
    files = manifest.get('files', manifest.get('artifacts'))
    assert files
    for name, expected in files.items():
        # The Linux build also records its internal seed tool, which is not shipped.
        if name == 'mydb-test-seed' and 'artifacts' in manifest:
            continue
        assert sha256(payload / name) == expected, name
    version = tomllib.loads((Path(__file__).resolve().parents[1] / 'Cargo.toml').read_text())['package']['version']
    assert f'mydb-{version}-' in args.package.name
    for name, package in [('Cargo.lock', 'mydb'), ('mydb-cli.Cargo.lock', 'mydb-cli')]:
        lock = tomllib.loads((payload / name).read_text())
        assert next(p['version'] for p in lock['package'] if p['name'] == package) == version
    suffix = '.exe' if os.name == 'nt' else ''
    binaries = {name: str(payload / (name + suffix)) for name in ['mydb', 'mydb-cli', 'mydb-backup']}
    env = {k: v for k, v in os.environ.items() if not k.startswith('MYDB_')}
    env.update(MYDB_DATA_DIR=str(root / 'data'), MYDB_MEMORY_TARGET='512M',
               MYDB_ROOT_PASSWORD='root', MYDB_DURABILITY='strict',
               MYDB_SHUTDOWN_TIMEOUT_MS='1000')

    def run(command):
        return subprocess.check_output(command, env=env, cwd=root, text=True,
                                       encoding='utf-8', stderr=subprocess.STDOUT, timeout=90).strip()

    versions = {name: run([binaries[name], '--version']) for name in ['mydb', 'mydb-cli']}
    assert f'mydb-{version}-' in versions['mydb'], versions
    assert versions['mydb-cli'] == f'mydb-cli {version}', versions
    for binary in binaries.values():
        run([binary, '--help'])
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        port = listener.getsockname()[1]

    def sql(statement):
        return run([binaries['mydb-cli'], '--no-config', '-h127.0.0.1', f'-P{port}',
                    '-uroot', '-proot', '--ssl-mode', 'REQUIRED', '-B', '-N', 'test', '-e', statement])

    process = None

    def start(index):
        nonlocal process
        with (root / f'server-{index}.log').open('wb') as log:
            process = subprocess.Popen([binaries['mydb'], '--host', '127.0.0.1', '--port', str(port)],
                                       cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            assert process.poll() is None, f'server exited; inspect server-{index}.log'
            try:
                with socket.create_connection(('127.0.0.1', port), timeout=1):
                    return
            except OSError:
                time.sleep(.1)
        raise TimeoutError('server startup')

    try:
        start(1)
        server_version = sql('SELECT VERSION()')
        assert f'mydb-{version}-' in server_version, server_version
        sql('CREATE TABLE release_smoke(id INT PRIMARY KEY, amount DECIMAL(20,4), label VARCHAR(64), doc JSON, KEY amount_idx(amount));'
            'INSERT INTO release_smoke VALUES(1,12.3456,\'release\',\'{"ok":true}\');'
            'BEGIN; UPDATE release_smoke SET amount=99 WHERE id=1; ROLLBACK;')
        expected = '1\t12.3456\trelease\ttrue'
        query = "SELECT id,amount,label,JSON_EXTRACT(doc,'$.ok') FROM release_smoke ORDER BY id"
        assert sql(query) == expected
        process.kill()
        process.wait(timeout=30)
        start(2)
        assert sql(query) == expected, 'committed data or rollback changed after crash recovery'
        assert sql('SELECT id FROM release_smoke FORCE INDEX(amount_idx) WHERE amount=12.3456') == '1'
        result = {'version': version, 'package': args.package.name, 'sha256': sha256(args.package),
                  'source_sha256': manifest['source_sha256'], 'head': manifest.get('head'),
                  'versions': versions, 'server_version': server_version,
                  'checks': ['manifest-sha256', 'lockfile-versions', 'binary-versions', 'binary-help',
                             'tls-required', 'sql-types', 'transaction-rollback', 'crash-recovery', 'secondary-index'],
                  'status': 'passed'}
        (root / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
        print(json.dumps(result))
    finally:
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=15)


if __name__ == '__main__':
    main()
