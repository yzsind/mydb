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
from release_archive import package_prefix, runtime_files, validate_archive_paths


def sha256(path):
    with path.open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--package', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--manifest', type=Path, help='external platform build manifest')
    parser.add_argument('--files-only', action='store_true', help='verify files without executing platform binaries')
    args = parser.parse_args()
    args.package = args.package.resolve()
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    extracted = root / 'extracted'
    extracted.mkdir()
    prefix = args.package.name.removesuffix('.tar.gz').removesuffix('.zip')
    if args.package.suffix == '.zip':
        with zipfile.ZipFile(args.package) as archive:
            validate_archive_paths(archive.namelist(), prefix)
            archive.extractall(extracted)
    else:
        with tarfile.open(args.package) as archive:
            validate_archive_paths(archive.getnames(), prefix)
            archive.extractall(extracted, filter='data')
    payload = extracted / prefix
    manifest_path = args.manifest or args.package.with_name(prefix + '.build-manifest.json')
    manifest = json.loads(manifest_path.read_text())
    require(manifest['schema_version'] == 2, 'unsupported build manifest schema')
    version = manifest['version']
    require(package_prefix(version, manifest['host']) == prefix, 'archive name does not match manifest version and target')
    windows = 'windows' in manifest['host']
    files = manifest['files']
    require(files, 'manifest has no runtime files')
    actual = {p.relative_to(payload).as_posix() for p in payload.rglob('*') if p.is_file()}
    require(actual == set(files) == set(runtime_files(payload, windows=windows)), 'unexpected or missing runtime files')
    for name, expected in files.items():
        require(sha256(payload / name) == expected, f'runtime SHA-256 mismatch: {name}')
    for name, package, hash_key in [(f'mydb-{version}.Cargo.lock', 'mydb', 'cargo_lock_sha256'),
                                    (f'mydb-{version}-cli.Cargo.lock', 'mydb-cli', 'client_cargo_lock_sha256')]:
        path = args.package.parent / name
        require(sha256(path) == manifest[hash_key], f'lockfile SHA-256 mismatch: {name}')
        lock = tomllib.loads(path.read_text())
        require(next(p['version'] for p in lock['package'] if p['name'] == package) == version, f'lockfile version mismatch: {name}')
    result = {'version': version, 'package': args.package.name, 'sha256': sha256(args.package),
              'source_sha256': manifest['source_sha256'], 'head': manifest.get('head'),
              'manifest': manifest_path.name, 'manifest_sha256': sha256(manifest_path),
              'checks': ['top-level-directory', 'slim-layout', 'manifest-sha256', 'lockfile-versions'],
              'status': 'passed'}
    if args.files_only:
        result['execution'] = 'not-run-files-only'
        (root / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
        print(json.dumps(result))
        return
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
    require(f'mydb-{version}-' in versions['mydb'], versions)
    require(versions['mydb-cli'] == f'mydb-cli {version}', versions)
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
            require(process.poll() is None, f'server exited; inspect server-{index}.log')
            try:
                with socket.create_connection(('127.0.0.1', port), timeout=1):
                    return
            except OSError:
                time.sleep(.1)
        raise TimeoutError('server startup')

    try:
        start(1)
        server_version = sql('SELECT VERSION()')
        require(f'mydb-{version}-' in server_version, server_version)
        sql('CREATE TABLE release_smoke(id INT PRIMARY KEY, amount DECIMAL(20,4), label VARCHAR(64), doc JSON, KEY amount_idx(amount));'
            'INSERT INTO release_smoke VALUES(1,12.3456,\'release\',\'{"ok":true}\');'
            'BEGIN; UPDATE release_smoke SET amount=99 WHERE id=1; ROLLBACK;')
        expected = '1\t12.3456\trelease\ttrue'
        query = "SELECT id,amount,label,JSON_EXTRACT(doc,'$.ok') FROM release_smoke ORDER BY id"
        require(sql(query) == expected, 'typed SQL or transaction rollback result mismatch')
        process.kill()
        process.wait(timeout=30)
        start(2)
        require(sql(query) == expected, 'committed data or rollback changed after crash recovery')
        require(sql('SELECT id FROM release_smoke FORCE INDEX(amount_idx) WHERE amount=12.3456') == '1',
                'secondary index lookup failed after recovery')
        result.update(versions=versions, server_version=server_version, execution='native')
        result['checks'] += ['binary-versions', 'binary-help', 'tls-required', 'sql-types',
                             'transaction-rollback', 'crash-recovery', 'secondary-index']
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
