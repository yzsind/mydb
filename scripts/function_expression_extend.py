#!/usr/bin/env python3
"""Append workloads after verifying every previously measured case is unchanged.

Never use this to change an existing workload, data identity or timing setting.
It takes the same port locks as the runner, so an active run cannot be extended.
"""

import argparse
import fcntl
import json
from pathlib import Path
import shutil
import time

from function_expression_bench import MODES, digest_file, dump, make_inventory
from function_expression_cases import build_cases


def unchanged_cases(previous, current):
    by_id = {case["id"]: case for case in current}
    changed = [case["id"] for case in previous if case != by_id.get(case["id"])]
    if changed:
        raise ValueError("existing workloads changed: " + ", ".join(changed))
    return sorted(set(by_id) - {case["id"] for case in previous})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument(
        "--replace",
        nargs="*",
        default=[],
        help="Explicitly invalidate and archive changed benchmark definitions",
    )
    parser.add_argument("--reason", default="Append-only coverage extension")
    args = parser.parse_args()
    directory = args.directory.resolve()
    if not directory.is_relative_to(Path("/tmp/mydb").resolve()):
        parser.error("directory must be under /tmp/mydb")
    locks = []
    for port, _ in MODES.values():
        lock = Path(f"/tmp/mydb/function-expression-port-{port}.lock").open("a+")
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        locks.append(lock)
    previous = json.loads((directory / "manifest.json").read_text())
    current_cases = build_cases()
    by_id = {case["id"]: case for case in current_cases}
    changed = {
        case["id"] for case in previous["cases"] if case != by_id.get(case["id"])
    }
    if changed != set(args.replace):
        raise ValueError(
            f"changed cases must match --replace exactly: {sorted(changed)}"
        )
    added = unchanged_cases(
        [case for case in previous["cases"] if case["id"] not in changed], current_cases
    )
    added = sorted(set(added) - changed)
    old_hash = digest_file(directory / "manifest.json")
    context = json.loads((directory / "context.json").read_text())
    if context["manifest_sha256"] != old_hash:
        raise ValueError("existing context does not match the old manifest")
    dump(directory / f"manifest-{old_hash}.json", previous)
    dump(directory / f"context-{old_hash}.json", context)
    if changed:
        archive = directory / "invalidated" / old_hash
        archive.mkdir(parents=True, exist_ok=True)
        for identifier in sorted(changed):
            source = directory / "cases" / identifier
            if source.exists():
                shutil.move(str(source), str(archive / identifier))
    current = make_inventory(directory)
    context["manifest_sha256"] = digest_file(directory / "manifest.json")
    context.setdefault("extensions", []).append(
        dict(
            previous_sha256=old_hash,
            current_sha256=context["manifest_sha256"],
            unchanged_cases=len(previous["cases"]) - len(changed),
            added=added,
            replaced=sorted(changed),
            reason=args.reason,
            timestamp=time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        )
    )
    dump(directory / "context.json", context)
    print(
        f'preserved {len(previous["cases"])-len(changed)} identical workloads; appended {added}; invalidated {sorted(changed)}; total {len(current["cases"])}'
    )


if __name__ == "__main__":
    main()
