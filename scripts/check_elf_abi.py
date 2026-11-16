#!/usr/bin/env python3
"""Fail closed when a Linux x86-64 release exceeds its runtime ABI contract."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess


ALLOWED_LIBRARIES = {'libc.so.6', 'libm.so.6', 'libpthread.so.0', 'libdl.so.2',
                     'librt.so.1', 'libutil.so.1', 'libgcc_s.so.1', 'ld-linux-x86-64.so.2'}


def version_tuple(text):
    return tuple(int(part) for part in text.split('.'))


def inspect_text(header, versions, dynamic, program, maximum='2.17'):
    errors = []
    if not re.search(r'Machine:\s+Advanced Micro Devices X86-64', header):
        errors.append('expected an x86-64 ELF')
    if not re.search(r'Class:\s+ELF64', header):
        errors.append('expected ELF64')
    if not re.search(r'Type:\s+(EXEC|DYN)\b', header):
        errors.append('expected an executable or shared object')
    # Inspect version requirements, including weak imports. Symbol binding alone
    # does not determine whether the loader treats a version as optional.
    requirements = sorted(set(re.findall(r'Name:\s+((?:GLIBC|GLIBCXX|CXXABI|GCC)_[\w.]+)', versions)))
    for requirement in requirements:
        if requirement.startswith('GLIBC_'):
            suffix = requirement[len('GLIBC_'):]
            if not re.fullmatch(r'\d+(?:\.\d+)+', suffix) or version_tuple(suffix) > version_tuple(maximum):
                errors.append('unsupported version requirement: ' + requirement)
        elif requirement.startswith('GCC_'):
            suffix = requirement[len('GCC_'):]
            if not re.fullmatch(r'\d+(?:\.\d+)+', suffix) or version_tuple(suffix) > (4, 8, 0):
                errors.append('unsupported libgcc requirement: ' + requirement)
        else:
            errors.append('unexpected C++ runtime requirement: ' + requirement)
    needed = sorted(set(re.findall(r'\(NEEDED\).*\[([^]]+)\]', dynamic)))
    for library in needed:
        if library not in ALLOWED_LIBRARIES:
            errors.append('unexpected runtime library: ' + library)
    if re.search(r'\((?:RPATH|RUNPATH)\)', dynamic):
        errors.append('release must not depend on an embedded library search path')
    interpreter = re.search(r'Requesting program interpreter:\s*([^]\n]+)', program)
    if interpreter and interpreter[1] != '/lib64/ld-linux-x86-64.so.2':
        errors.append('unexpected ELF interpreter: ' + interpreter[1])
    if not requirements or not needed:
        errors.append('expected a dynamically linked GNU/Linux artifact')
    return {'requirements': requirements, 'needed': needed, 'errors': errors}


def inspect(path, maximum='2.17'):
    def read(option):
        return subprocess.check_output(['readelf', '--wide', option, str(path)], text=True)
    result = inspect_text(read('--file-header'), read('--version-info'),
                          read('--dynamic'), read('--program-headers'), maximum)
    result.update(path=str(path.resolve()), sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('artifacts', nargs='+', type=Path)
    parser.add_argument('--max-glibc', default='2.17')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    results = [inspect(path, args.max_glibc) for path in args.artifacts]
    report = {'max_glibc': args.max_glibc, 'artifacts': results,
              'passed': all(not result['errors'] for result in results)}
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + '\n')
    for result in results:
        print(('FAIL' if result['errors'] else 'PASS') + ': ' + result['path'])
        for error in result['errors']:
            print('  ' + error)
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
