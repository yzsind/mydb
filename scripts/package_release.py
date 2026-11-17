#!/usr/bin/env python3
"""Build and package a host-native candidate with source and license evidence."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import tomllib

from build_linux_glibc217 import source_hash
from release_licenses import collect_third_party
from release_archive import package_prefix, runtime_files, write_archive, write_runtime_readme


ROOT = Path(__file__).resolve().parents[1]


def sha256(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def command(*args, env=None):
    return subprocess.check_output(args, cwd=ROOT, text=True, env=env).strip()


def build_provenance():
    return command("git", "rev-parse", "HEAD"), command("git", "status", "--short")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    destination = args.output.resolve()
    if destination.exists():
        parser.error("--output must not already exist")
    destination.parent.mkdir(parents=True, exist_ok=True)
    before = source_hash(ROOT)
    head, status = build_provenance()
    # A source-addressed build directory prevents unrelated workspace builds
    # from replacing release executables between cargo and artifact capture.
    build_dir = Path("/tmp/mydb/release-build") / before
    env = dict(os.environ, TMPDIR="/tmp/mydb", CARGO_TARGET_DIR=str(build_dir))
    client_manifest = ROOT / "clients/mydb-cli/Cargo.toml"
    rust = command("rustc", "-vV")
    host = next(line.removeprefix("host: ") for line in rust.splitlines() if line.startswith("host: "))
    metadata = json.loads(command("cargo", "metadata", "--locked", "--features", "tools",
                                  "--filter-platform", host, "--format-version", "1", env=env))
    client_metadata = json.loads(command("cargo", "metadata", "--manifest-path", str(client_manifest),
                                         "--locked", "--filter-platform", host, "--format-version", "1", env=env))
    version = tomllib.loads((ROOT / "Cargo.toml").read_text())["package"]["version"]
    prefix = package_prefix(version, host)
    with tempfile.TemporaryDirectory(prefix="candidate-", dir=destination.parent) as temporary:
        stage = Path(temporary)
        payload = stage / prefix
        payload.mkdir()
        collect_third_party(ROOT, payload, metadata, extra_metadata=[client_metadata],
                            extra_lockfiles=[ROOT / "clients/mydb-cli/Cargo.lock"])
        subprocess.run(["cargo", "build", "--locked", "--release", "--features", "tools",
                        "--bin", "mydb", "--bin", "mydb-backup"], cwd=ROOT, env=env, check=True)
        subprocess.run(["cargo", "build", "--manifest-path", str(client_manifest), "--locked", "--release"],
                       cwd=ROOT, env=env, check=True)
        target = Path(metadata["target_directory"]) / "release"
        for name in ["mydb", "mydb-backup", "mydb-cli"]:
            executable = name + (".exe" if os.name == "nt" else "")
            shutil.copy2(target / executable, payload / executable)
        shutil.copy2(ROOT / "LICENSE", payload / "LICENSE")
        write_runtime_readme(payload, version, windows=os.name == "nt")
        contents = runtime_files(payload, windows=os.name == "nt")
        if source_hash(ROOT) != before:
            raise SystemExit("source changed during build; candidate was not published")
        manifest = {"schema_version": 2, "version": version, "host": host,
                    "source_sha256": before, "head": head,
                    "worktree_status": status,
                    "rustc": rust, "profile": "release", "features": ["default", "tools"],
                    "cargo_lock_sha256": sha256(ROOT / "Cargo.lock"),
                    "client_cargo_lock_sha256": sha256(ROOT / "clients/mydb-cli/Cargo.lock"),
                    "release_binaries": ["mydb", "mydb-backup", "mydb-cli"],
                    "packager_sha256": sha256(Path(__file__).resolve()),
                    "archive_writer_sha256": sha256(Path(__file__).with_name("release_archive.py")),
                    "license_collector_sha256": sha256(Path(__file__).with_name("release_licenses.py")),
                    "files": {name: sha256(path) for name, path in sorted(contents.items())}}
        (stage / f"{prefix}.build-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        shutil.copy2(ROOT / "Cargo.lock", stage / f"mydb-{version}.Cargo.lock")
        shutil.copy2(client_manifest.with_name("Cargo.lock"), stage / f"mydb-{version}-cli.Cargo.lock")
        archive = stage / (prefix + (".zip" if os.name == "nt" else ".tar.gz"))
        write_archive(archive, prefix, contents)
        (stage / "SHA256SUMS").write_text(''.join(f"{sha256(p)}  {p.name}\n"
                                                for p in sorted(stage.iterdir()) if p.is_file()))
        destination.mkdir()
        for path in stage.iterdir():
            shutil.move(str(path), destination / path.name)
    print(json.dumps({"candidate": str(destination), "source_sha256": before, "host": host}))


if __name__ == "__main__":
    main()
