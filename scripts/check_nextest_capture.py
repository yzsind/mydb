#!/usr/bin/env python3
"""Check macOS nextest capture-pipe isolation in a private, dependency-free crate."""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys


ROOT = Path(__file__).resolve().parent.parent
SOURCE = r'''#[cfg(test)]
mod tests {
    use std::{fs, os::{fd::AsFd, unix::fs::{FileTypeExt, MetadataExt}}, time::{Duration, SystemTime, UNIX_EPOCH}};

    fn identity(handle: impl AsFd) -> fs::Metadata {
        let file = fs::File::from(handle.as_fd().try_clone_to_owned().unwrap());
        file.metadata().unwrap()
    }

    fn probe() {
        let entries: Vec<_> = fs::read_dir("/dev/fd").unwrap().collect();
        let mut extra = Vec::new();
        for entry in entries {
            let entry = entry.unwrap();
            let Ok(fd) = entry.file_name().to_string_lossy().parse::<u32>() else { continue };
            if fd <= 2 { continue; }
            if let Ok(meta) = fs::metadata(entry.path()) {
                if meta.file_type().is_fifo() {
                    // macOS fdescfs path metadata has synthetic identities.
                    // Opening /dev/fd duplicates the descriptor, allowing fstat
                    // to identify the actual pipe shared with a sibling.
                    let file = fs::File::open(entry.path()).or_else(|_| {
                        fs::OpenOptions::new().write(true).open(entry.path())
                    }).unwrap();
                    let meta = file.metadata().unwrap();
                    extra.push((fd, meta.dev(), meta.ino()));
                }
            }
        }
        let stdout = identity(std::io::stdout());
        let stderr = identity(std::io::stderr());
        let at = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        eprintln!("CAPTURE_PROBE pid={} at_ns={at} stdout={}:{} stderr={}:{} extra={extra:?}",
            std::process::id(), stdout.dev(), stdout.ino(), stderr.dev(), stderr.ino());
        if !extra.is_empty() {
            // Keep an already-inherited descriptor alive long enough to expose
            // its sibling's LEAK as well as this process's descriptor failure.
            std::thread::sleep(Duration::from_millis(350));
            panic!("sibling capture pipes inherited: {extra:?}");
        }
    }

    #[test]
    #[ignore = "intentional leaked subprocess for the capture detector's negative control"]
    fn intentional_subprocess_leak() {
        std::process::Command::new("/bin/sleep").arg("1").spawn().unwrap();
    }
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--nextest', type=Path,
                        default=shutil.which('cargo-nextest'))
    parser.add_argument('--output', type=Path,
                        default=Path('/tmp/mydb/nextest-capture-check'))
    parser.add_argument('--iterations', type=int, default=20)
    parser.add_argument('--test-threads', type=int, default=64)
    args = parser.parse_args()
    if sys.platform != 'darwin' or not Path('/dev/fd').is_dir():
        parser.error('this diagnostic requires macOS /dev/fd')
    if args.nextest is None or not args.nextest.is_file():
        parser.error('--nextest must name an installed cargo-nextest executable')
    if args.iterations < 1 or args.test_threads < 1:
        parser.error('iterations and test-threads must be positive')

    output = args.output.resolve()
    crate = output / 'probe'
    (crate / 'src').mkdir(parents=True, exist_ok=True)
    (crate / '.config').mkdir(exist_ok=True)
    (crate / 'Cargo.toml').write_text(
        '[package]\nname = "nextest-capture-probe"\nversion = "0.0.0"\n'
        'edition = "2021"\n[workspace]\n')
    source = SOURCE + ''.join(
        f'    #[test] fn capture_{i:03}() {{ probe(); }}\n' for i in range(128)) + '}\n'
    (crate / 'src/lib.rs').write_text(source)
    (crate / '.config/nextest.toml').write_text(
        '[profile.default]\nleak-timeout = { period = "200ms", result = "fail" }\n')
    env = os.environ | {'TMPDIR': str(output)}
    executable = str(args.nextest.resolve())
    version = subprocess.check_output([executable, 'nextest', '--version'], text=True)
    command = [executable, 'nextest', 'run', '--manifest-path', str(crate / 'Cargo.toml'),
               '--no-fail-fast', '--success-output', 'immediate',
               '--test-threads', str(args.test_threads), '--stress-count', str(args.iterations)]
    with (output / 'stress.log').open('w') as log:
        stress = subprocess.run(command, cwd=ROOT, env=env, stdout=log,
                                stderr=subprocess.STDOUT)
    text = (output / 'stress.log').read_text()
    records = re.findall(
        r'CAPTURE_PROBE pid=(\d+) at_ns=(\d+) stdout=(\d+):(\d+) stderr=(\d+):(\d+) extra=(\[[^\n]*\])',
        text)
    records = list(dict.fromkeys(records))
    owners = {}
    for pid, at, out_dev, out_ino, err_dev, err_ino, _ in records:
        for identity in [(out_dev, out_ino), (err_dev, err_ino)]:
            owners.setdefault(identity, set()).add((pid, int(at)))
    inherited = []
    for pid, at, _, _, _, _, extra in records:
        for fd, device, inode in re.findall(r'\((\d+), (\d+), (\d+)\)', extra):
            siblings = sorted(other for other, when in owners.get((device, inode), set())
                              if other != pid and abs(when - int(at)) < 200_000_000)
            inherited.append({'pid': int(pid), 'fd': int(fd), 'device': int(device),
                              'inode': int(inode), 'sibling_stdio_pids': siblings})

    control_command = command[:command.index('--test-threads')] + [
        '--run-ignored', 'only', '-E', 'test(intentional_subprocess_leak)']
    with (output / 'control.log').open('w') as log:
        control = subprocess.run(control_command, cwd=ROOT, env=env, stdout=log,
                                 stderr=subprocess.STDOUT)
    control_detected = control.returncode != 0 and 'LEAK-FAIL' in (
        output / 'control.log').read_text()
    result = {'version': version.strip(), 'stress_exit_code': stress.returncode,
              'iterations': args.iterations, 'test_threads': args.test_threads,
              'observations': len(records), 'inherited_capture_pipes': inherited,
              'sibling_stdio_matches': sum(bool(item['sibling_stdio_pids']) for item in inherited),
              'control_exit_code': control.returncode, 'control_detected': control_detected}
    (output / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({key: value for key, value in result.items()
                      if key != 'inherited_capture_pipes'}))
    return 0 if (stress.returncode == 0 and not inherited and control_detected
                 and len(records) == 128 * args.iterations) else 1


if __name__ == '__main__':
    raise SystemExit(main())
