#!/usr/bin/env python3
"""Run an archived test partition using its own library test binary for setup."""
import argparse
import json
from pathlib import Path
import shutil
import subprocess

from build_linux_glibc217 import ROOT, runtime_nextest_config


def archived_seed_binary(metadata, extraction):
    candidates = [binary for binary in metadata['rust-binaries'].values()
                  if binary['binary-name'] == 'mydb' and binary['kind'] == 'lib']
    if len(candidates) != 1:
        raise ValueError('archive must contain exactly one mydb library test binary')
    build = metadata['rust-build-meta']
    original = Path(build.get('build-directory', build['target-directory']))
    relative = Path(candidates[0]['binary-path']).relative_to(original)
    binary = (extraction / 'target' / relative).resolve()
    if not binary.is_relative_to((extraction / 'target').resolve()) or not binary.is_file():
        raise ValueError('library test binary is missing from the extracted archive')
    return binary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('nextest_args', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    runner = shutil.which('cargo-nextest')
    if runner is None:
        parser.error('cargo-nextest is not installed')
    output = args.output.resolve()
    extraction = output / 'extracted'
    extraction.mkdir(parents=True, exist_ok=True)
    reuse = ['--archive-file', str(args.archive.resolve()), '--extract-to', str(extraction),
             '--extract-overwrite', '--workspace-remap', str(ROOT)]
    # Use the archive's metadata, never a glob over potentially stale binaries
    # from another feature set. No Cargo invocation is needed on test workers.
    metadata = json.loads(subprocess.check_output(
        [runner, 'nextest', 'list', *reuse, '--list-type', 'binaries-only',
         '--message-format', 'json'], cwd=ROOT, text=True))
    seed = archived_seed_binary(metadata, extraction)
    config = output / 'nextest-runtime.toml'
    config.write_text(runtime_nextest_config(
        (ROOT / '.config/nextest.toml').read_text(), seed, test_harness=True))
    extra = args.nextest_args
    if extra[:1] == ['--']:
        extra = extra[1:]
    return subprocess.run(
        [runner, 'nextest', 'run', *reuse, '--config-file', str(config), *extra],
        cwd=ROOT).returncode


if __name__ == '__main__':
    raise SystemExit(main())
