#!/usr/bin/env python3
"""Collect verified third-party license texts and corresponding MPL sources."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import tomllib


def sha256(path):
    with path.open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def collect_third_party(root, payload, metadata):
    root = root.resolve()
    resolved = {node['id'] for node in metadata['resolve']['nodes']}
    lock = tomllib.loads((root / 'Cargo.lock').read_text())
    checksums = {(p['name'], p['version']): p.get('checksum') for p in lock['package']}
    notice_root = root / 'assets/third-party-licenses'
    notices = {(p['name'], p['version']): p for p in json.loads((notice_root / 'manifest.json').read_text())}
    packages = []
    for package in metadata["packages"]:
        if package["id"] not in resolved:
            continue
        source = Path(package["manifest_path"]).parent
        if source == root:
            continue
        origin = None
        if package["source"] is None:
            if not source.is_relative_to(root / "vendor"):
                raise SystemExit(f"untracked local dependency: {source}")
            origin = json.loads((source / "UPSTREAM.json").read_text())
        name = f"{package['name']}-{package['version']}"
        licenses = []
        explicit = package.get("license_file")
        for file in sorted(source.rglob("*")):
            if not file.is_file():
                continue
            if not file.name.lower().startswith(("license", "licence", "copying", "notice", "copyright")) and str(file) != explicit:
                continue
            relative = file.relative_to(source)
            output = payload / "third-party" / "licenses" / name / relative
            output.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(file, output)
            licenses.append(str(relative))
        provenance = None
        if not licenses:
            provenance = notices.get((package["name"], package["version"]))
            if provenance is None:
                raise SystemExit(f"missing verified upstream license for {name}")
            for notice in provenance["files"]:
                file = notice_root / notice["path"]
                if sha256(file) != notice["sha256"]:
                    raise SystemExit(f"upstream license checksum mismatch: {file}")
                output = payload / "third-party" / "licenses" / name / notice["upstream_path"]
                output.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(file, output)
                licenses.append(notice["upstream_path"])
        # Supply corresponding unmodified source for MPL dependencies.
        source_archive = None
        if "MPL-2.0" in (package.get("license") or ""):
            cached = source.parent.parent.parent / "cache" / source.parent.name / (name + ".crate")
            if not cached.is_file() or sha256(cached) != checksums[(package["name"], package["version"])]:
                raise SystemExit(f"missing or invalid source archive for {name}")
            output = payload / "third-party" / "sources" / cached.name
            output.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(cached, output)
            source_archive = str(output.relative_to(payload))
        packages.append({"name": package["name"], "version": package["version"],
                         "license": package["license"], "repository": package["repository"],
                         "authors": package["authors"],
                         "source": package["source"] or f"vendored:{source.relative_to(root)}",
                         "checksum": checksums[(package["name"], package["version"])],
                         "upstream": origin,
                         "license_files": licenses, "upstream_license_provenance": provenance,
                         "bundled_source": source_archive})
    (payload / "third-party").mkdir(parents=True, exist_ok=True)
    (payload / "third-party" / "packages.json").write_text(json.dumps(packages, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--metadata', required=True, type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    collect_third_party(args.source, args.output, json.loads(args.metadata.read_text()))


if __name__ == '__main__':
    main()
