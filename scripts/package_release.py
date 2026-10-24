#!/usr/bin/env python3
"""Build and package a host-native candidate with source and license evidence."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import tomllib

from build_linux_glibc217 import source_hash
from release_licenses import collect_third_party


ROOT = Path(__file__).resolve().parents[1]


def sha256(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def command(*args, env=None):
    return subprocess.check_output(args, cwd=ROOT, text=True, env=env).strip()


def build_provenance(source):
    snapshot = ROOT / ".release-source.json"
    if snapshot.is_file():
        record = json.loads(snapshot.read_text())
        if record["source_sha256"] != source:
            raise SystemExit("snapshot provenance does not match candidate source")
        return record["head"], record["worktree_status"]
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
    head, status = build_provenance(before)
    # A source-addressed build directory prevents unrelated workspace builds
    # from replacing release executables between cargo and artifact capture.
    build_dir = Path("/tmp/mydb/release-build") / before
    env = dict(os.environ, TMPDIR="/tmp/mydb", CARGO_TARGET_DIR=str(build_dir))
    subprocess.run(["cargo", "build", "--locked", "--release", "--features", "tools",
                    "--bin", "mydb", "--bin", "mydb-backup"], cwd=ROOT, env=env, check=True)
    rust = command("rustc", "-vV")
    host = next(line.removeprefix("host: ") for line in rust.splitlines() if line.startswith("host: "))
    metadata = json.loads(command("cargo", "metadata", "--locked", "--features", "tools",
                                  "--filter-platform", host, "--format-version", "1", env=env))
    version = tomllib.loads((ROOT / "Cargo.toml").read_text())["package"]["version"]
    prefix = f"mydb-{version}-{host}"
    with tempfile.TemporaryDirectory(prefix="candidate-", dir=destination.parent) as temporary:
        stage = Path(temporary)
        payload = stage / prefix
        payload.mkdir()
        target = Path(metadata["target_directory"]) / "release"
        for name in ["mydb", "mydb-backup"]:
            executable = name + (".exe" if os.name == "nt" else "")
            shutil.copy2(target / executable, payload / executable)
        for name in ["LICENSE", "README.md", "Cargo.lock",
                     "assets/mydb-architecture.svg"]:
            output = payload / name
            output.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / name, output)
        shutil.copytree(ROOT / "docs", payload / "docs")
        collect_third_party(ROOT, payload, metadata)
        if source_hash(ROOT) != before:
            raise SystemExit("source changed during build; candidate was not published")
        manifest = {"schema_version": 1, "version": version, "host": host,
                    "source_sha256": before, "head": head,
                    "worktree_status": status,
                    "rustc": rust, "profile": "release", "features": ["default", "tools"],
                    "cargo_lock_sha256": sha256(ROOT / "Cargo.lock"),
                    "packager_sha256": sha256(Path(__file__).resolve()),
                    "license_collector_sha256": sha256(Path(__file__).with_name("release_licenses.py")),
                    "files": {str(p.relative_to(payload)): sha256(p) for p in sorted(payload.rglob("*")) if p.is_file()}}
        (payload / "build-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        archive = stage / (prefix + ".tar.gz")
        with archive.open("wb") as raw, gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as zipped:
            with tarfile.open(fileobj=zipped, mode="w") as bundle:
                for path in sorted(payload.rglob("*")):
                    if not path.is_file():
                        continue
                    entry = bundle.gettarinfo(str(path), arcname=str(path.relative_to(stage)))
                    entry.uid = entry.gid = entry.mtime = 0
                    entry.uname = entry.gname = ""
                    with path.open("rb") as file:
                        bundle.addfile(entry, file)
        (stage / "SHA256SUMS").write_text(f"{sha256(archive)}  {archive.name}\n")
        destination.mkdir()
        for path in stage.iterdir():
            shutil.move(str(path), destination / path.name)
    print(json.dumps({"candidate": str(destination), "source_sha256": before, "host": host}))


if __name__ == "__main__":
    main()
