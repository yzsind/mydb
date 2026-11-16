#!/usr/bin/env python3
"""Check Windows features and link executables from a macOS/Linux host.

Uses Rust's bundled LLD in an isolated PATH to avoid mismatched system LLVM
libraries. Does not install toolchains or change Cargo profiles/configuration.
All default logs, linker shims and build outputs live under /tmp/mydb.
"""

import argparse
import os
from pathlib import Path
import subprocess
import sys


REPO = Path(__file__).resolve().parents[1]
TARGET = "x86_64-pc-windows-msvc"
FEATURES = {
    "default": [],
    "no-default": ["--no-default-features"],
    "tls": ["--no-default-features", "--features", "tls"],
    "tools": ["--features", "tools"],
    "all-features": ["--all-features"],
}


def compiler_output(toolchain, *args):
    command = ["rustc"] + ([f"+{toolchain}"] if toolchain else []) + list(args)
    return subprocess.check_output(command, cwd=REPO, text=True).strip()


def run(command, label, work, env):
    log = work / f"{label}.log"
    print(f"{label}: {log}", flush=True)
    with log.open("w", encoding="utf-8") as output:
        result = subprocess.run(command, cwd=REPO, env=env,
                                stdout=output, stderr=subprocess.STDOUT)
    if result.returncode:
        print(log.read_text(errors="replace")[-6000:], file=sys.stderr)
        raise SystemExit(result.returncode)
    print(f"{label}: passed", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--toolchain", help="Installed Rust toolchain, e.g. 1.95.0")
    parser.add_argument("--work-dir", type=Path, default=Path("/tmp/mydb/windows-check"))
    parser.add_argument("--target-dir", type=Path, default=Path("/tmp/mydb/windows-target"))
    parser.add_argument("--case", action="append", choices=list(FEATURES),
                        help="Feature configuration; repeat to select several (default: all)")
    parser.add_argument("--check-only", action="store_true")
    args = parser.parse_args()
    if os.name == "nt":
        parser.error("Use native cargo check/build on Windows; this script cross-compiles.")
    work = args.work_dir.resolve()
    target_dir = args.target_dir.resolve()
    work.mkdir(parents=True, exist_ok=True)
    info = compiler_output(args.toolchain, "-vV")
    host = next(line.removeprefix("host: ") for line in info.splitlines()
                if line.startswith("host: "))
    sysroot = Path(compiler_output(args.toolchain, "--print", "sysroot"))
    linker = sysroot / "lib" / "rustlib" / host / "bin" / "rust-lld"
    if not linker.is_file():
        parser.error(f"Rust's bundled linker is missing: {linker}")
    shim_dir = work / "bin"
    shim_dir.mkdir(exist_ok=True)
    shim = shim_dir / "lld-link"
    if shim.is_symlink():
        if shim.resolve() != linker.resolve():
            shim.unlink()
            shim.symlink_to(linker)
    elif shim.exists():
        parser.error(f"Refusing to replace a non-symlink linker: {shim}")
    else:
        shim.symlink_to(linker)
    env = dict(os.environ)
    env["PATH"] = str(shim_dir) + os.pathsep + env.get("PATH", "")
    cargo = ["cargo"] + ([f"+{args.toolchain}"] if args.toolchain else []) + ["xwin"]
    common = ["--locked", "--target", TARGET, "--target-dir", str(target_dir)]
    print(info, flush=True)
    for name in args.case or FEATURES:
        run(cargo + ["check", "--all-targets"] + common + FEATURES[name],
            f"check-{name}", work, env)
    if not args.check_only:
        run(cargo + ["build", "--features", "tools", "--bin", "mydb", "--bin", "mydb-backup"]
            + common, "build-tools", work, env)
        for name in ["mydb.exe", "mydb-backup.exe"]:
            executable = target_dir / TARGET / "debug" / name
            with executable.open("rb") as file:
                header = file.read(64)
                if len(header) != 64 or header[:2] != b"MZ":
                    raise SystemExit(f"Invalid Windows executable: {executable}")
                file.seek(int.from_bytes(header[60:64], "little"))
                signature = file.read(6)
                if signature != b"PE\x00\x00\x64\x86":
                    raise SystemExit(f"Expected an x64 PE executable: {executable}")
            print(f"Windows executable: {executable}", flush=True)


if __name__ == "__main__":
    main()
