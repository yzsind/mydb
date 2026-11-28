"""Replay captured mydb changes and compare every validation operation."""

import argparse
from collections import Counter
from contextlib import closing
from functools import lru_cache
from itertools import islice
import json
from pathlib import Path
import re
import resource
import sys
import time

from graph_io import load, read_commits
from model import InteractiveModel
from prepare import kind

SHORT = {
    "messageIdContent": 4,
    "messageIdCreator": 5,
    "messageForumId": 6,
    "messageRepliesId": 7,
}
FIELDS = {
    key: tuple(schema)
    for key, schema in json.loads(
        Path(__file__).with_name("result-fields.json").read_text()
    ).items()
}


def operation(params):
    key = next(iter(params))
    if key in SHORT:
        return "IS", SHORT[key]
    n = int(re.search(r"Q(\d+)", key)[1])
    return ("IS" if "SQ" in key else "IC"), n


def normalize(label, rows):
    if label == "IC14":
        if any(a[1] < b[1] for a, b in zip(rows, rows[1:])):
            raise ValueError("Shortest path weights are not in descending order")
        return sorted(rows, key=lambda r: (-r[1], r[0]))
    if label == "IC1":
        return [
            (
                *row[:8],
                sorted(row[8]),
                sorted(row[9]),
                row[10],
                *(
                    sorted(items, key=lambda item: json.dumps(item, sort_keys=True))
                    for items in row[11:]
                ),
            )
            for row in rows
        ]
    if label == "IC12":
        return [(*row[:3], sorted(row[3]), row[4]) for row in rows]
    return rows


def expected_rows(label, oracle):
    rows = json.loads(oracle)
    if isinstance(rows, dict):
        rows = [rows]
    return [tuple(row[key] for key in FIELDS[label]) for row in rows]


@lru_cache(maxsize=4096)
def canonical_fixture(label, oracle):
    return normalize(label, expected_rows(label, oracle))


def run(path, plan_path, validation, limit, no_reuse, log, receipts, follow_capture):
    start = time.monotonic()
    model = InteractiveModel()
    catalog = load(path, model)
    loaded = time.monotonic()
    print(
        f"Loaded actual graph into verification indexes in {loaded - start:.3f}s",
        file=sys.stderr,
        flush=True,
    )
    counts = Counter()
    canonical_fixture.cache_clear()
    faults = Counter()
    first = []
    writes = 0
    check_seconds = 0.0
    with (
        closing(read_commits(path, follow=follow_capture)) as commits,
        plan_path.open() as plans,
        validation.open(encoding="utf-8") as data,
    ):
        for ordinal, line in enumerate(islice(data, limit), 1):
            raw, oracle = line.rstrip("\n").split("|", 1)
            params = json.loads(raw)
            op = kind(params)
            if op is not None:
                instruction = json.loads(next(plans))
                event = next(commits)
                if ordinal != instruction["ordinal"] or op != instruction["op"]:
                    raise ValueError(
                        f"Graph mutation ledger differs from input at {ordinal}"
                    )
                count = catalog.apply(instruction, event, model)
                writes += count
                counts[f"U{op}"] += 1
                receipts.write(f"{ordinal}\t{catalog.scn}\tU{op}\tPASS\t{count}\n")
                continue
            family, n = operation(params)
            label = f"{family}{n}"
            counts[label] += 1

            def compute():
                return normalize(
                    label,
                    model.short(n, params)
                    if family == "IS"
                    else model.complex(n, params),
                )

            if label in ("IC5", "IC9") or no_reuse:
                actual = compute()
            else:
                actual = model.cache.evaluate(label, raw, compute)
            before = time.perf_counter()
            if family == "IC" and len(oracle) <= 8192:
                expected = canonical_fixture(label, oracle)
            else:
                expected = normalize(label, expected_rows(label, oracle))
            status = "PASS" if actual == expected else "FAIL"
            receipts.write(
                f"{ordinal}\t{catalog.scn}\t{label}\t{status}\t{len(actual)}\n"
            )
            if status == "FAIL":
                faults[label] += 1
                if faults[label] <= 3:
                    idx = next(
                        (i for i, (a, b) in enumerate(zip(actual, expected)) if a != b),
                        min(len(actual), len(expected)),
                    )
                    detail = dict(
                        ordinal=ordinal,
                        scn=catalog.scn,
                        op=label,
                        params=params,
                        index=idx,
                        rows=[len(actual), len(expected)],
                        actual=actual[idx : idx + 2],
                        expected=expected[idx : idx + 2],
                    )
                    first.append(detail)
                    print(json.dumps(detail, ensure_ascii=False), file=log, flush=True)
            check_seconds += time.perf_counter() - before
            if ordinal % 5000 == 0:
                print(
                    f"Checked {ordinal} at SCN {catalog.scn}; {sum(faults.values())} mismatches; {time.monotonic() - start:.1f}s",
                    file=sys.stderr,
                    flush=True,
                )
        if limit is None and (
            next(commits, None) is not None or next(plans, None) is not None
        ):
            raise ValueError("Unexpected trailing graph changes")
    finish = time.monotonic()
    result = dict(
        counts=counts,
        mismatches=faults,
        first_mismatches=first,
        logical_rows_verified=writes,
        total_operations=sum(counts.values()),
        scn=catalog.scn,
        peak_rss=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        snapshot_load_seconds=loaded - start,
        replay_seconds=finish - loaded,
        total_seconds=finish - start,
        comparison_seconds=check_seconds,
        oracle_cache=canonical_fixture.cache_info()._asdict(),
        cache=model.cache.stats,
        cache_evaluation_seconds=model.cache.seconds,
        ic5_forums=model.forums.stats,
        ic9_ranking=model.ranking.stats,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if faults:
        return 1
    if limit is None and (sum(counts.values()) != 138747 or len(counts) != 29):
        return 2
    return 0


if __name__ == "__main__":
    cli = argparse.ArgumentParser()
    cli.add_argument("capture", type=Path)
    cli.add_argument("plan", type=Path)
    cli.add_argument("--validation", type=Path, required=True)
    cli.add_argument("--max-lines", type=int)
    cli.add_argument("--no-reuse", action="store_true")
    cli.add_argument("--mismatches", type=Path, required=True)
    cli.add_argument("--receipts", type=Path, required=True)
    cli.add_argument("--follow-capture", action="store_true", help=argparse.SUPPRESS)
    cli.add_argument("--workers", type=int, default=1)
    cli.add_argument("--native-library", type=Path)
    cli.add_argument("--native-data", type=Path)
    a = cli.parse_args()
    if a.workers < 1:
        cli.error("--workers must be positive")
    if bool(a.native_data) != bool(a.native_library):
        cli.error(
            "Native snapshot loading requires both a library and a data directory"
        )
    if a.workers > 1 or a.native_library:
        if a.max_lines is not None or a.no_reuse:
            cli.error("Parallel verification requires a complete, incremental run")
        from prepared import verify_complete
        from functools import partial

        loader = None
        if a.native_library:
            from native import load

            loader = partial(load, a.native_library, a.native_data, a.capture, a.plan)

        print(
            json.dumps(
                verify_complete(
                    a.capture,
                    a.validation,
                    a.workers,
                    a.receipts,
                    a.mismatches,
                    a.follow_capture,
                    loader,
                ),
                indent=2,
            )
        )
        sys.exit(0)
    with (
        a.mismatches.open("w", encoding="utf-8") as log,
        a.receipts.open("w", encoding="utf-8") as receipts,
    ):
        receipts.write("ordinal\tscn\toperation\tstatus\trows\n")
        sys.exit(
            run(
                a.capture,
                a.plan,
                a.validation,
                a.max_lines,
                a.no_reuse,
                log,
                receipts,
                a.follow_capture,
            )
        )
