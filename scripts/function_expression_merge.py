#!/usr/bin/env python3
"""Combine separately identified timeout cohorts without changing their samples."""

import argparse
import json
import os
from pathlib import Path
import shutil

from function_expression_bench import digest_file, dump


def merge(sources, destination):
    contexts = [json.loads((source / "context.json").read_text()) for source in sources]
    reference = contexts[0]
    identity = (
        "manifest_sha256",
        "rows",
        "worker_sha256",
        "mydb_binary_sha256",
        "modes",
        "prepared",
        "warmup",
        "repeats",
    )
    for context in contexts[1:]:
        for key in identity:
            if reference[key] != context[key]:
                raise ValueError("cohort identity differs: " + key)
    destination.mkdir(parents=True, exist_ok=True)
    seen = set()
    cohorts = []
    for source, context in zip(sources, contexts):
        files = sorted((source / "cases").glob("*/result.json"))
        cohorts.append(
            dict(
                source=str(source),
                timeout_s=context["timeout_s"],
                skip_invalid=context.get("skip_invalid", False),
                plan_once=context.get("plan_once", False),
                tool_sha256=context.get("tool_sha256"),
                completed=len(files),
            )
        )
        for path in files:
            identifier = path.parent.name
            if identifier in seen:
                raise ValueError(
                    "duplicate measured case across cohorts: " + identifier
                )
            seen.add(identifier)
            result = json.loads(path.read_text())
            result.update(
                timeout_s=context["timeout_s"],
                source_run=str(source),
                origin_result_sha256=digest_file(path),
            )
            target = destination / "cases" / identifier / "result.json"
            dump(target, result)
            os.utime(target, ns=(path.stat().st_atime_ns, path.stat().st_mtime_ns))
            post = path.with_name("validation.json")
            if post.exists():
                shutil.copy2(post, target.with_name("validation.json"))
    context = dict(reference, report_only=True, timeout_cohorts=cohorts)
    dump(destination / "context.json", context)
    for name in (
        "manifest.json",
        "provenance.json",
        "mysql-capabilities.json",
        "prepared-mysql.json",
        "prepared-serial.json",
        "prepared-parallel2.json",
        "data-generation.sql",
        "host-samples.jsonl",
        "restart-data-verification.json",
        "initial-data-validation.json",
    ):
        path = sources[0] / name
        if path.exists():
            shutil.copy2(path, destination / name)
    for path in sources[0].glob("manifest-*.json"):
        shutil.copy2(path, destination / path.name)
    for path in sources[0].glob("context-*.json"):
        shutil.copy2(path, destination / path.name)
    dump(destination / "cohorts.json", cohorts)
    return len(seen)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sources", type=Path, nargs="+")
    parser.add_argument("--destination", type=Path, required=True)
    args = parser.parse_args()
    if not args.destination.resolve().is_relative_to(Path("/tmp/mydb").resolve()):
        parser.error("generated merge directory must be under /tmp/mydb")
    print(
        "combined measured cases:",
        merge([p.resolve() for p in args.sources], args.destination.resolve()),
    )


if __name__ == "__main__":
    main()
