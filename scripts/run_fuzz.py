#!/usr/bin/env python3
"""Build and run every declared fuzz target with retained seeds and artifacts."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import re
import time
from concurrent.futures import ThreadPoolExecutor
from safety_evidence import write_json, source_identity
import tomllib

ROOT = Path(__file__).resolve().parents[1]
# Stateful SQL inputs include setup, repeated execution oracles, DML and DDL.
# storage_model executes a full storage scenario per input, which the ASan
# build under concurrent campaigns can easily stretch past 30 seconds.
INPUT_TIMEOUTS = {'sql_execution_differential': 180, 'storage_model': 60}


def seeds(output, targets):
    subprocess.run([sys.executable, str(ROOT / 'tests/fuzz/make_parser_corpus.py'), '--output', str(output)], check=True)
    for target in targets:
        corpus = output / 'corpus' / target
        corpus.mkdir(parents=True, exist_ok=True)
        examples = [b'', b'\0', bytes(range(64)), b'\xff' * 64]
        if target == 'sql_execution_differential':
            examples = [bytes([family, stress, 7, 13, 0, 255, 128]) for family in range(16) for stress in (0, 1)]
        elif target == 'mysql_binlog':
            examples += [bytes([tag, 0]) + bytes(16) for tag in range(10)]
        elif target == 'schema_row_codec':
            examples += [bytes([0, tag]) + number.to_bytes(8, 'little') for tag in range(21)
                         for number in (0, 1, 2**63, 2**64 - 1)]
        if target in ('storage_formats', 'wal_frame', 'row_codec'):
            for path in (ROOT / 'tests/fixtures/storage-format-v1').glob('*.bin'):
                if target == 'storage_formats' or path.name == {'wal_frame': 'wal-frame.bin', 'row_codec': 'row-values.bin'}.get(target):
                    examples.append(path.read_bytes())
        for payload in examples:
            (corpus / hashlib.sha256(payload).hexdigest()).write_bytes(payload)


def run_logged(command, log_path, env, timeout):
    started = time.monotonic()
    result = {'status': 'incomplete', 'exit_code': None, 'command': command}
    try:
        with log_path.open('w') as log:
            run = subprocess.run(command, cwd=ROOT, env=env, stdout=log,
                                 stderr=subprocess.STDOUT, timeout=timeout)
        result.update(status='pass' if run.returncode == 0 else 'fail', exit_code=run.returncode)
    except subprocess.TimeoutExpired:
        result.update(error='wall-timeout', timeout_seconds=timeout)
    except OSError as error:
        result.update(error=repr(error))
    result['elapsed_seconds'] = round(time.monotonic() - started, 3)
    return result


def fuzz_stats(log):
    runs = re.findall(r'stat::number_of_executed_units:\s*(\d+)', log)
    cov = re.findall(r'cov:\s*(\d+)', log)
    return {'executions': int(runs[-1]) if runs else None,
            'edges': int(cov[-1]) if cov else None}


def triage_crashes(command, options, env, target, artifacts, output, input_timeout, skip_slow=False):
    """Replay and minimize every artifact a failing target left behind.

    A crash that reproduces under `-runs=1` is fed to libFuzzer's
    `-minimize_crash` with `-exact_artifact_path`, so the minimized input
    lands next to the original artifact for conversion into a deterministic
    regression. Triage failures never mask the failing fuzz run itself.
    With ``skip_slow`` the expensive replay of ``slow-unit-`` artifacts is
    skipped: slow units flag latency, not memory or logic defects, and each
    replay may burn several multiples of the input timeout.
    """
    crashes = []
    for crash in sorted(path for path in artifacts.iterdir() if path.is_file()):
        if skip_slow and crash.name.startswith('slow-unit-'):
            crashes.append({'file': crash.name, 'bytes': crash.stat().st_size, 'triage': 'skipped-slow'})
            continue
        entry = {'file': crash.name, 'bytes': crash.stat().st_size}
        replay = run_logged(command + ['run', *options, target, str(crash), '--', '-runs=1', f'-timeout={input_timeout}'],
                            output / f'{target}-replay-{crash.name}.log', env, 2 * input_timeout + 120)
        if replay['status'] == 'incomplete':
            entry['replay'] = 'replay-' + replay['status']
        elif replay['exit_code'] == 0:
            entry['replay'] = 'irreproducible'
        else:
            entry['replay'] = 'reproduced'
            minimized = artifacts / ('minimized-' + crash.name)
            run_logged(command + ['run', *options, target, str(crash), '--',
                                  '-minimize_crash=1', '-runs=1000000', f'-timeout={input_timeout}',
                                  f'-exact_artifact_path={minimized}'],
                       output / f'{target}-minimize-{crash.name}.log', env, max(300, 4 * input_timeout + 120))
            if minimized.is_file():
                entry['minimize'] = 'minimized'
                entry['minimized_file'] = minimized.name
                entry['minimized_bytes'] = minimized.stat().st_size
            else:
                entry['minimize'] = 'not-reduced'
        crashes.append(entry)
    return crashes


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--toolchain', default='nightly')
    parser.add_argument('--seconds', type=int, default=10)
    parser.add_argument('--runs', type=int, default=0)
    parser.add_argument('--input-timeout', type=int, help='per-input libFuzzer limit; defaults to 180 seconds for SQL execution, 30 otherwise')
    parser.add_argument('--wall-timeout', type=int, help='per-target wall limit, including cargo startup')
    parser.add_argument('--target', action='append')
    parser.add_argument('--skip-target', action='append',
                        help='declared target to leave out of this campaign (repeatable)')
    parser.add_argument('--skip-build', action='store_true')
    parser.add_argument('--optimized', action='store_true')
    parser.add_argument('--jobs', type=int, default=1,
                        help='number of targets fuzzed concurrently; 1 keeps campaigns strictly sequential')
    parser.add_argument('--skip-slow-triage', action='store_true',
                        help='record slow-unit artifacts without replaying them')
    args = parser.parse_args(argv)
    output = args.output.resolve()
    if not output.is_relative_to(Path('/tmp/mydb').resolve()):
        parser.error('--output must be below /tmp/mydb')
    if (args.seconds <= 0 or args.runs < 0 or args.jobs < 1
            or any(value is not None and value <= 0 for value in (args.input_timeout, args.wall_timeout))):
        parser.error('seconds and timeouts must be positive; runs must be nonnegative')
    output.mkdir(parents=True, exist_ok=True)
    declared = [entry['name'] for entry in tomllib.loads((ROOT / 'tests/fuzz/Cargo.toml').read_text())['bin']]
    if args.skip_target and not set(args.skip_target) <= set(declared):
        parser.error('unknown or empty fuzz target selection')
    targets = list(dict.fromkeys(args.target or declared))
    if args.skip_target:
        targets = [target for target in targets if target not in set(args.skip_target)]
    if not targets or not set(targets) <= set(declared):
        parser.error('unknown or empty fuzz target selection')
    # Write the entire plan before any operation that can fail or be interrupted.
    results = [{'target': target, 'status': 'incomplete', 'exit_code': None, 'error': 'not-started'} for target in targets]
    write_json(output / 'results.json', results)
    manifest = dict(vars(args), sanitizer='address', profile='release' if args.optimized else 'dev')
    try:
        manifest.update(source_identity(ROOT, [ROOT / 'src', ROOT / 'tests/fuzz', ROOT / 'Cargo.toml',
                                               ROOT / 'Cargo.lock', Path(__file__), ROOT / 'scripts/safety_evidence.py']))
        for label, cmd in [('rustc', ['rustc', '+' + args.toolchain, '-vV']),
                           ('cargo_fuzz', ['cargo', 'fuzz', '--version'])]:
            manifest[label] = subprocess.check_output(cmd, text=True, stderr=subprocess.STDOUT, timeout=30).strip()
        host = re.search(r'^host: (\S+)$', manifest['rustc'], re.MULTILINE)
        if host is None:
            raise ValueError('selected rustc toolchain did not report a host triple')
        manifest['build_target'] = host.group(1)
        write_json(output / 'manifest.json', manifest)
        seeds(output, targets)
        env = dict(os.environ, TMPDIR='/tmp/mydb')
        command = ['cargo', '+' + args.toolchain, 'fuzz']
        # Prebuilt cargo-fuzz may target musl even when the selected rustc uses glibc.
        options = ([] if args.optimized else ['--dev']) + [
            '--sanitizer', 'address', '--target', manifest['build_target'],
            '--fuzz-dir', str(ROOT / 'tests/fuzz'), '--target-dir', str(output / 'build')]
        if not args.skip_build:
            build = run_logged(command + ['build', *options], output / 'build.log', env, 7200)
            write_json(output / 'build.json', build)
            if build['status'] != 'pass':
                for result in results:
                    result['error'] = 'build-' + build['status']
                return 1
        def campaign(index, target):
            input_timeout = args.input_timeout or INPUT_TIMEOUTS.get(target, 30)
            corpus = output / 'corpus' / target
            # libFuzzer reads the initial corpus before checking max_total_time.
            # Leak detection may execute each seed again; include the empty input.
            corpus_size = sum(path.is_file() for path in corpus.rglob('*'))
            wall_timeout = args.wall_timeout or max(
                300, args.seconds + 2 * input_timeout + 120, 2 * (corpus_size + 1) * input_timeout + 120)
            artifacts = output / 'artifacts' / target
            artifacts.mkdir(parents=True, exist_ok=True)
            flags = [f'-runs={args.runs}' if args.runs else f'-max_total_time={args.seconds}',
                     f'-timeout={input_timeout}', '-rss_limit_mb=4096', '-print_final_stats=1', f'-artifact_prefix={artifacts}/']
            if target.startswith('parser_') or target in ('sql_parser', 'sql_lex'):
                flags.append('-dict=' + str(ROOT / 'tests/fuzz/parser.dict'))
            flags.append('-max_len=65538' if target == 'storage_formats' else '-max_len=4096')
            log_path = output / f'{target}.log'
            print(f'{target}: corpus={corpus_size}; input_timeout={input_timeout}s; wall_timeout={wall_timeout}s', flush=True)
            run = run_logged(command + ['run', *options, target, str(corpus), '--', *flags],
                             log_path, env, wall_timeout)
            run.update(input_timeout_seconds=input_timeout, wall_timeout_seconds=wall_timeout)
            stats = fuzz_stats(log_path.read_text(errors='replace') if log_path.exists() else '')
            # An exit code alone cannot establish that libFuzzer executed inputs.
            if run['status'] == 'pass' and not (stats['executions'] and stats['edges']):
                run.update(status='incomplete', error='missing-execution-or-coverage-evidence')
            crashes = triage_crashes(command, options, env, target, artifacts, output, input_timeout,
                                     skip_slow=args.skip_slow_triage)
            print(f"{target}: {run['status']}; exit={run['exit_code']}", flush=True)
            return dict(run, target=target, crashes=crashes, **stats)

        def record(index, result):
            results[index] = result
            write_json(output / 'results.json', results)

        if args.jobs == 1:
            for index, target in enumerate(targets):
                record(index, campaign(index, target))
        else:
            # Concurrent campaigns share only the artifact and corpus trees, which
            # libFuzzer keeps per target; results.json stays single-writer here.
            with ThreadPoolExecutor(max_workers=args.jobs) as pool:
                for index, result in enumerate(pool.map(campaign, range(len(targets)), targets)):
                    record(index, result)
        return 0 if all(result['status'] == 'pass' for result in results) else 1
    except (Exception, KeyboardInterrupt) as error:
        manifest['error'] = repr(error)
        write_json(output / 'manifest.json', manifest)
        return 1
    finally:
        write_json(output / 'results.json', results)


if __name__ == '__main__':
    raise SystemExit(main())
