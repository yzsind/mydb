#!/usr/bin/env python3
"""Build reproducible x86-64 GNU/Linux artifacts without changing Cargo profiles.

Requires Python 3.11+ and Docker. A matching Linux Rust sysroot may be supplied to avoid toolchain
downloads. Otherwise rustup installs the pinned toolchain in the output cache.
"""
import argparse
import hashlib
import gzip
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import tarfile
import tomllib
import urllib.request

IMAGE = 'quay.io/pypa/manylinux2014_x86_64@sha256:f6a6f153c9cf45470507ebbc27013e5db14ce21331b62fd2c540561f25e5f0a5'
RUST = '1.95.0'
NEXT = '0.9.143'
NEXT_SHA256 = '6d891c18105ec2d33f6e441a4f92b7ccab47ba263e22055c8bb66884243f3389'
ROOT = Path(__file__).resolve().parent.parent


def call(args, **kwargs):
    print('+ ' + ' '.join(map(str, args)), flush=True)
    subprocess.run(list(map(str, args)), check=True, **kwargs)


def file_sha256(path):
    with path.open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def source_hash(root):
    digest = hashlib.sha256()
    files = [root / name for name in ('Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml', 'build.rs')]
    for directory in ('src', 'assets/mysql80', 'tests', '.config', 'vendor'):
        for parent, directories, names in os.walk(root / directory):
            relative = Path(parent).relative_to(root)
            directories[:] = [name for name in directories if name not in ('target', '__pycache__')
                              and not (relative == Path('tests/fuzz') and name in ('corpus', 'artifacts', 'coverage'))
                              and not (relative == Path('tests/mydb-test') and name == 'triage')]
            files.extend(Path(parent) / name for name in names if name != '.DS_Store')
    for path in sorted(files):
        if path.name.startswith('._'):
            raise SystemExit('source contains filesystem metadata sidecars: ' + str(path))
        digest.update(str(path.relative_to(root)).encode() + b'\0')
        digest.update(path.read_bytes())
    return digest.hexdigest()


def download_nextest(output):
    tools = output / 'tools'
    tools.mkdir(exist_ok=True)
    archive = tools / 'nextest.tar.gz'
    if not archive.exists() or hashlib.sha256(archive.read_bytes()).hexdigest() != NEXT_SHA256:
        url = f'https://github.com/nextest-rs/nextest/releases/download/cargo-nextest-{NEXT}/cargo-nextest-{NEXT}-x86_64-unknown-linux-musl.tar.gz'
        with urllib.request.urlopen(url, timeout=300) as response, archive.open('wb') as destination:
            shutil.copyfileobj(response, destination)
    if hashlib.sha256(archive.read_bytes()).hexdigest() != NEXT_SHA256:
        raise SystemExit('cargo-nextest archive checksum mismatch')
    binary = tools / 'cargo-nextest'
    with tarfile.open(archive) as bundle:
        members = [member for member in bundle if member.isfile() and Path(member.name).name == 'cargo-nextest']
        if len(members) != 1:
            raise SystemExit('cargo-nextest archive contains no unique executable')
        with bundle.extractfile(members[0]) as source, binary.open('wb') as destination:
            shutil.copyfileobj(source, destination)
    binary.chmod(0o755)
    return binary


def runtime_nextest_config(config, seed_binary):
    """Adapt only the shared seed generator, preserving runtime test policy."""
    error = 'nextest setup configuration changed; update the runtime adapter'
    try:
        expected = tomllib.loads(config)
        setup = expected['scripts']['setup']['mydb-test-seed']
        command = shlex.split(setup['command'])
    except (tomllib.TOMLDecodeError, KeyError, TypeError, ValueError) as exc:
        raise SystemExit(error) from exc
    if command != ['cargo', 'test', '--quiet', '--lib', '--', '--ignored',
                   '--exact', 'test_seed_setup::prepare_nextest_seed']:
        raise SystemExit(error)
    if not seed_binary.is_absolute():
        raise SystemExit('runtime seed executable must have an absolute path')
    section = re.search(r'(?ms)^\[scripts\.setup\.mydb-test-seed\][ \t]*(?:#[^\n]*)?\n(.*?)(?=^\[|\Z)', config)
    if section is None:
        raise SystemExit(error)
    runtime_command = shlex.quote(str(seed_binary))
    body, count = re.subn(r'(?m)^[ \t]*command[ \t]*=.*$',
                         lambda _: 'command = ' + json.dumps(runtime_command), section[1])
    if count != 1:
        raise SystemExit(error)
    rewritten = config[:section.start(1)] + body + config[section.end(1):]
    setup['command'] = runtime_command
    try:
        if tomllib.loads(rewritten) != expected:
            raise SystemExit(error)
    except tomllib.TOMLDecodeError as exc:
        raise SystemExit(error) from exc
    return rewritten


def verify_test_artifacts(build, manifest, nextest):
    """Reject mixed/stale test inputs before starting any runtime containers."""
    recorded = manifest.get('test_artifacts', {})
    for name in ('tests.tar.zst', 'nextest-runtime.toml'):
        path = build / name
        if not path.is_file() or file_sha256(path) != recorded.get(name):
            raise SystemExit('test artifact differs from the successful build manifest: ' + name)
    if not nextest.is_file() or file_sha256(nextest) != recorded.get('nextest_sha256'):
        raise SystemExit('nextest runner differs from the successful build manifest')


def ensure_openssl_build_tools():
    """The minimal manylinux Perl omits modules required by vendored OpenSSL."""
    probe = ['perl', '-MIPC::Cmd', '-MTime::Piece', '-MDigest::SHA', '-e',
             'print "$^V IPC::Cmd=$IPC::Cmd::VERSION Time::Piece=$Time::Piece::VERSION\\n"']
    result = subprocess.run(probe, capture_output=True, text=True)
    if result.returncode:
        if not shutil.which('yum'):
            raise SystemExit('vendored OpenSSL requires Perl IPC::Cmd in the pinned builder')
        call(['yum', '--setopt=timeout=20', '--setopt=retries=1', 'install', '-y', 'perl-core'])
        result = subprocess.run(probe, capture_output=True, text=True, check=True)
    print('OpenSSL Perl prerequisite: ' + result.stdout.strip(), flush=True)
    return result.stdout.strip()


def build_release_notices(source, artifacts):
    from release_licenses import collect_third_party
    metadata = json.loads(subprocess.check_output([
        'cargo', 'metadata', '--locked', '--features', 'tools',
        '--filter-platform', 'x86_64-unknown-linux-gnu', '--format-version', '1',
    ], cwd=source, text=True))
    collect_third_party(source, artifacts, metadata)


def inside(args):
    (args.output / 'build-manifest.json').unlink(missing_ok=True)
    if subprocess.check_output(['getconf', 'GNU_LIBC_VERSION'], text=True).strip() != 'glibc 2.17':
        raise SystemExit('builder must provide glibc 2.17')
    polluted = [name for name in ('RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS', 'CFLAGS', 'CXXFLAGS',
                                 'CPATH', 'LIBRARY_PATH') if os.environ.get(name)]
    if polluted:
        raise SystemExit('unexpected compiler overrides: ' + ', '.join(polluted))
    perl = ensure_openssl_build_tools()
    if not shutil.which('cargo'):
        # rustup verifies official toolchain downloads. Keep the installer and
        # all caches under the caller's output directory.
        installer = args.output / 'rustup-init.sh'
        call(['curl', '--fail', '--location', '--retry', '3', 'https://sh.rustup.rs', '-o', installer])
        call(['sh', installer, '-y', '--no-modify-path', '--profile', 'minimal', '--default-toolchain', RUST])
        os.environ['PATH'] = str(Path(os.environ['CARGO_HOME']) / 'bin') + ':' + os.environ['PATH']
    rust = subprocess.check_output(['rustc', '-vV'], text=True)
    if f'release: {RUST}\n' not in rust or 'host: x86_64-unknown-linux-gnu\n' not in rust:
        raise SystemExit('expected official Rust ' + RUST + ' for x86_64-unknown-linux-gnu')
    # Configuration supplied for registry mirrors must not override the ABI.
    for config in (args.source / '.cargo/config', args.source / '.cargo/config.toml',
                   Path(os.environ['CARGO_HOME']) / 'config', Path(os.environ['CARGO_HOME']) / 'config.toml'):
        if config.exists() and re.search(r'\b(rustflags|linker|target-cpu)\b', config.read_text()):
            raise SystemExit('compiler overrides in Cargo configuration: ' + str(config))
    fingerprint = source_hash(args.source)
    common = ['--locked', '--features', 'tools'] + (['--offline'] if args.offline else [])
    binaries = ['mydb', 'mydb-backup', 'mydb-test-seed']
    command = ['cargo', 'build', '--release', *common]
    for binary in binaries:
        command.extend(['--bin', binary])
    call(command, cwd=args.source)
    artifacts = args.output / 'bin'
    artifacts.mkdir(exist_ok=True)
    for binary in binaries:
        shutil.copy2(args.output / 'target/release' / binary, artifacts / binary)
    build_release_notices(args.source, artifacts)
    call(['python3', args.source / 'scripts/check_elf_abi.py',
          *[artifacts / binary for binary in binaries], '--output', args.output / 'abi.json'])
    test_artifacts = {}
    test_build = {}
    if args.test_archive:
        archive_env = dict(os.environ)
        if getattr(args, 'test_no_incremental', False):
            archive_env['CARGO_INCREMENTAL'] = '0'
        test_build = {'profile': 'test', 'incremental_override': archive_env.get('CARGO_INCREMENTAL')}
        call(['cargo-nextest', 'nextest', 'archive', *common,
              '--archive-file', args.output / 'tests.tar.zst'], cwd=args.source, env=archive_env)
        # Replace only the setup executable; preserve the project's scheduling
        # and persistence limits when running archived tests in runtime images.
        config = runtime_nextest_config((args.source / '.config/nextest.toml').read_text(),
                                        artifacts / 'mydb-test-seed')
        (args.output / 'nextest-runtime.toml').write_text(config)
        runner = Path(shutil.which('cargo-nextest'))
        test_artifacts = {name: file_sha256(args.output / name)
                          for name in ('tests.tar.zst', 'nextest-runtime.toml')}
        test_artifacts['nextest_sha256'] = file_sha256(runner)
    if fingerprint != source_hash(args.source):
        raise SystemExit('source changed during the build; discard this artifact')
    manifest = {'schema_version': 1, 'image': args.image, 'rust': rust, 'perl': perl, 'gcc': subprocess.check_output(['gcc', '--version'], text=True),
                'source_sha256': fingerprint, 'cargo_lock_sha256': hashlib.sha256((args.source / 'Cargo.lock').read_bytes()).hexdigest(),
                'features': ['default', 'tools'], 'profile': 'release', 'build_timezone': 'UTC',
                'release_binaries': ['mydb', 'mydb-backup'],
                'test_artifacts': test_artifacts,
                'test_build': test_build,
                'builder_sha256': file_sha256(Path(__file__)),
                'license_collector_sha256': file_sha256(Path(__file__).with_name('release_licenses.py')),
                'artifacts': {str(path.relative_to(artifacts)): file_sha256(path)
                              for path in sorted(artifacts.rglob('*')) if path.is_file()}}
    (args.output / 'build-manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    version = re.search(r'^version\s*=\s*"([^"]+)"', (args.source / 'Cargo.toml').read_text(), re.M)[1]
    package = args.output / f'mydb-{version}-x86-linux.tar.gz'
    contents = {binary: artifacts / binary for binary in ('mydb', 'mydb-backup')}
    contents.update({'LICENSE': args.source / 'LICENSE', 'build-manifest.json': args.output / 'build-manifest.json',
                     'abi.json': args.output / 'abi.json'})
    contents.update({name: args.source / name for name in ('README.md', 'SECURITY.md', 'Cargo.lock')
                     if (args.source / name).is_file()})
    contents['assets/mydb-architecture.svg'] = args.source / 'assets/mydb-architecture.svg'
    contents.update({'docs/' + str(path.relative_to(args.source / 'docs')): path
                     for path in (args.source / 'docs').rglob('*') if path.is_file()})
    contents.update({str(path.relative_to(artifacts)): path
                     for path in (artifacts / 'third-party').rglob('*') if path.is_file()})
    # Normalize archive metadata; build.rs still records the UTC build date.
    with package.open('wb') as raw, gzip.GzipFile(filename='', mode='wb', fileobj=raw, mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode='w') as archive:
            for name, path in sorted(contents.items()):
                info = archive.gettarinfo(str(path), arcname=name)
                info.uid = info.gid = info.mtime = 0
                info.uname = info.gname = ''
                with path.open('rb') as content:
                    archive.addfile(info, content)
    (args.output / 'SHA256SUMS').write_text(hashlib.sha256(package.read_bytes()).hexdigest() + '  ' + package.name + '\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=Path('/tmp/mydb/linux-glibc217'))
    parser.add_argument('--source', type=Path, default=ROOT)
    parser.add_argument('--image', default=IMAGE)
    parser.add_argument('--toolchain', type=Path, help='existing Linux Rust 1.95.0 sysroot')
    parser.add_argument('--nextest', type=Path, help='static Linux cargo-nextest for --test-archive')
    parser.add_argument('--cargo-home', type=Path, help='isolated dependency cache, optionally with registry mirror configuration')
    parser.add_argument('--jobs', type=int, default=8)
    parser.add_argument('--offline', action='store_true')
    parser.add_argument('--test-archive', action='store_true')
    parser.add_argument('--test-no-incremental', action='store_true',
                        help='disable incremental caching only for test-archive compilation; release flags are unchanged')
    parser.add_argument('--inside', action='store_true', help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.test_no_incremental and not args.test_archive:
        parser.error('--test-no-incremental requires --test-archive')
    args.output = args.output.resolve()
    args.source = args.source.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    if args.inside:
        inside(args)
        return
    if not re.fullmatch(r'.+@sha256:[0-9a-f]{64}', args.image):
        raise SystemExit('--image must be pinned by digest')
    if args.jobs < 1:
        raise SystemExit('--jobs must be positive')
    if args.output == args.source or args.source in args.output.parents:
        raise SystemExit('output must be outside the source checkout')
    cargo_home = (args.cargo_home or args.output / 'cargo-home').resolve()
    cargo_home.mkdir(parents=True, exist_ok=True)
    command = ['docker', 'run', '--rm', '--platform', 'linux/amd64',
               '-v', f'{args.source}:{args.source}:ro', '-v', f'{args.output}:{args.output}',
               '-v', f'{cargo_home}:{cargo_home}', '-w', str(args.source),
               '-e', f'CARGO_HOME={cargo_home}', '-e', f'RUSTUP_HOME={args.output}/rustup',
               '-e', f'CARGO_TARGET_DIR={args.output}/target', '-e', f'CARGO_BUILD_JOBS={args.jobs}',
               '-e', f'TMPDIR={args.output}', '-e', 'TZ=UTC']
    path = '/opt/rh/devtoolset-10/root/usr/bin:/opt/python/cp312-cp312/bin:/usr/local/bin:/usr/bin:/bin'
    if args.toolchain:
        command.extend(['-v', f'{args.toolchain.resolve()}:/opt/rust:ro'])
        path = '/opt/rust/bin:' + path
    if args.test_archive:
        if not args.nextest:
            args.nextest = download_nextest(args.output)
        command.extend(['-v', f'{args.nextest.resolve()}:/opt/bin/cargo-nextest:ro'])
        path = '/opt/bin:' + path
    command.extend(['-e', f'PATH={path}', args.image, 'python3',
                    args.source / 'scripts/build_linux_glibc217.py', '--inside',
                    '--source', args.source, '--output', args.output, '--image', args.image])
    if args.offline:
        command.append('--offline')
    if args.test_archive:
        command.append('--test-archive')
    if args.test_no_incremental:
        command.append('--test-no-incremental')
    call(command)


if __name__ == '__main__':
    main()
