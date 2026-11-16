"""Shared public package names and single-directory archive layout."""
import gzip
from pathlib import PurePosixPath
import shutil
import tarfile
import zipfile


def write_runtime_readme(payload, version, *, windows=False):
    command = '.\\' if windows else './'
    suffix = '.exe' if windows else ''
    server = f'{command}mydb{suffix}'
    client = f'{command}mydb-cli{suffix}'
    backup = f'{command}mydb-backup{suffix}'
    shell = 'powershell' if windows else 'sh'
    (payload / 'README.md').write_text(f'''# MyDB {version}

A MySQL-protocol-compatible database. This preview is intended for evaluation;
use a fresh data directory. On-disk formats may change between preview releases.

## Quick start

Extract this package and open a terminal in the extracted directory.
Start the server:

```{shell}
{server} --data-dir ./data --host 127.0.0.1 --port 8738
```

Connect from another terminal in the same directory:

```{shell}
{client} --no-config -h127.0.0.1 -P8738 -uroot -p test
```

A fresh data directory creates the loopback-only `root@localhost` account with
password `root`. Set the `MYDB_ROOT_PASSWORD` environment variable before the
first startup to choose a different password. TLS is advertised by default.

## Included tools

- `mydb{suffix}`: database server; run `{server} --help` for options.
- `mydb-cli{suffix}`: SQL client; run `{client} --help` for options.
- `mydb-backup{suffix}`: backup and restore tool; run `{backup} --help` for options.
- `LICENSE` and `third-party/`: project and third-party license notices and sources.

## Documentation and verification

- [Configuration](https://github.com/yzsind/mydb/blob/v{version}/docs/configuration.md)
- [Documentation](https://github.com/yzsind/mydb/blob/v{version}/docs/README.md)
- [Downloads, checksums, and build evidence](https://github.com/yzsind/mydb/releases/tag/v{version})

`SHA256SUMS`, platform build manifests, Linux ABI evidence, dependency lockfiles,
and the validation record are separate release attachments.
''', encoding='utf-8')


def runtime_files(payload, *, windows=False):
    suffix = '.exe' if windows else ''
    names = [name + suffix for name in ('mydb', 'mydb-backup', 'mydb-cli')]
    names += ['README.md', 'LICENSE']
    contents = {name: payload / name for name in names}
    for name, path in contents.items():
        if not path.is_file():
            raise ValueError(f'missing runtime package file: {name}')
    notices = payload / 'third-party'
    if not (notices / 'packages.json').is_file():
        raise ValueError('missing third-party package notices')
    contents.update({p.relative_to(payload).as_posix(): p
                     for p in notices.rglob('*') if p.is_file()})
    return contents


def package_prefix(version, target):
    platforms = {
        'aarch64-apple-darwin': 'macos-arm64',
        'x86_64-apple-darwin': 'macos-x86_64',
        'x86_64-unknown-linux-gnu': 'linux-x86_64',
        'aarch64-unknown-linux-gnu': 'linux-arm64',
        'x86_64-pc-windows-msvc': 'windows-x86_64',
        'aarch64-pc-windows-msvc': 'windows-arm64',
    }
    if target not in platforms:
        raise ValueError(f'unsupported release target: {target}')
    return f'mydb-{version}-{platforms[target]}'


def validate_archive_paths(names, prefix):
    if not names:
        raise ValueError('release archive is empty')
    for name in names:
        path = PurePosixPath(name)
        if (path.is_absolute() or '\\' in name or '..' in path.parts
                or not path.parts or path.parts[0] != prefix
                or (len(path.parts) == 1 and not name.endswith('/'))):
            raise ValueError(f'release entry is outside the top-level directory: {name}')


def write_archive(archive, prefix, contents):
    names = {name: f'{prefix}/{name}' for name in contents}
    validate_archive_paths(list(names.values()), prefix)
    if archive.suffix == '.zip':
        with zipfile.ZipFile(archive, 'w', compression=zipfile.ZIP_DEFLATED) as bundle:
            for name, path in sorted(contents.items()):
                entry = zipfile.ZipInfo(names[name])
                entry.compress_type = zipfile.ZIP_DEFLATED
                with path.open('rb') as source, bundle.open(entry, 'w') as output:
                    shutil.copyfileobj(source, output)
    else:
        with archive.open('wb') as raw, gzip.GzipFile(filename='', mode='wb', fileobj=raw, mtime=0) as zipped:
            with tarfile.open(fileobj=zipped, mode='w') as bundle:
                for name, path in sorted(contents.items()):
                    entry = bundle.gettarinfo(str(path), arcname=names[name])
                    entry.uid = entry.gid = entry.mtime = 0
                    entry.uname = entry.gname = ''
                    with path.open('rb') as source:
                        bundle.addfile(entry, source)
