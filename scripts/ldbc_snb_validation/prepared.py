"""Validate against a resident, prepared graph index and its actual commits."""

from array import array
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import dataclass
import gc
from functools import lru_cache
from heapq import merge
import hashlib
import json
import mmap
from multiprocessing import get_context
from time import monotonic

from evaluate_all import expected_rows, normalize, operation
from graph_io import GraphDelta, load, read_commits, verify_receipts
from model import InteractiveModel
from prepare import kind, Plan

OPERATION_COUNT = 138_747
UPDATE_COUNT = 7_041
DEADLINE_SECONDS = 5.34


@dataclass(frozen=True, slots=True)
class Query:
    label: str
    number: int
    short: bool
    params: dict
    key: str

    def evaluate(self, model):
        def compute():
            result = (
                model.short(self.number, self.params)
                if self.short
                else model.complex(self.number, self.params)
            )
            return normalize(self.label, result)

        if self.label in ("IC5", "IC9"):
            return compute()
        return model.cache.evaluate(self.label, self.key, compute)


@dataclass(frozen=True, slots=True)
class Read:
    ordinal: int
    epoch: int
    query: Query
    expected: list


@dataclass(slots=True)
class Prepared:
    model: InteractiveModel
    commits: list[GraphDelta]
    demands: list
    receipt_prefixes: list
    counts: Counter
    initial_scn: int


@lru_cache(maxsize=4096)
def fixture(label, oracle):
    return normalize(label, expected_rows(label, oracle))


def read_trace(validation):
    groups = defaultdict(list)
    queries = {}
    updates = []
    counts = Counter()
    operations = []
    with validation.open(encoding="utf-8") as cases:
        for ordinal, line in enumerate(cases, 1):
            raw, oracle = line.rstrip("\n").split("|", 1)
            query = queries.get(raw)
            if query is None:
                params = json.loads(raw)
                op = kind(params)
                if op is not None:
                    if oracle != '"-1"':
                        raise ValueError(f"Invalid expected update result at {ordinal}")
                    plan = Plan()
                    plan.add(op, params)
                    updates.append(dict(ordinal=ordinal, expected=plan.expected))
                    label = f"U{op}"
                    counts[label] += 1
                    operations.append((label, len(updates)))
                    continue
                family, number = operation(params)
                query = Query(f"{family}{number}", number, family == "IS", params, raw)
                queries[raw] = query
            label = query.label
            counts[label] += 1
            expected = (
                fixture(label, oracle)
                if len(oracle) <= 8192
                else normalize(label, expected_rows(label, oracle))
            )
            groups[query.key].append(Read(ordinal, len(updates), query, expected))
            operations.append((label, len(updates)))
    fixture.cache_clear()
    if (
        len(operations) != OPERATION_COUNT
        or len(updates) != UPDATE_COUNT
        or len(counts) != 29
    ):
        raise ValueError("Unexpected Interactive v1 SF=1 trace coverage")
    return list(groups.values()), updates, operations, counts


def prepare(capture, validation, follow_capture=False, snapshot_loader=None):
    model = InteractiveModel()
    if snapshot_loader is None:
        catalog = load(capture, model)
        trace = read_trace(validation)
    else:
        with ThreadPoolExecutor(max_workers=1) as executor:
            trace_task = executor.submit(read_trace, validation)
            catalog = snapshot_loader(model)
            trace = trace_task.result()
    demands, updates, operations, counts = trace
    initial_scn = catalog.scn
    with closing(read_commits(capture, follow=follow_capture)) as commits:
        changes = [
            catalog.decode(plan, event)
            for plan, event in zip(updates, commits, strict=True)
        ]
    versions = [initial_scn, *(delta.scn for delta in changes)]
    receipt_prefixes = [
        f"{ordinal}\t{versions[epoch]}\t{label}\t"
        for ordinal, (label, epoch) in enumerate(operations, 1)
    ]
    # These indexes contain memberships only. A worker computes its own
    # query results when it starts processing the requested epochs.
    if snapshot_loader is None:
        for author, memberships in model.forums.memberships.items():
            model.forums.ordered_memberships[author] = sorted(
                (when, forum) for forum, when in memberships.items()
            )
    return Prepared(model, changes, demands, receipt_prefixes, counts, initial_scn)


def partitions(model, demands, count):
    groups = [[] for _ in range(count)]
    sizes = [0] * count
    membership = []
    likes = []
    independent = []
    for reads in demands:
        label = reads[0].query.label
        if label == "IC5":
            membership.append(reads)
        elif label == "IC7":
            likes.append(reads)
        else:
            independent.append(reads)

    def weight(reads):
        query = reads[0].query
        if query.label == "IS3":
            author = next(iter(query.params.values()))
            output = len(model.ranking.adjacent.get(author, ()))
        elif query.label == "IS7":
            message = next(iter(query.params.values()))
            output = len(model.replies.get(message, ()))
        else:
            output = query.params.get("limit", 1)
        return len(reads) * (4 + output)

    # Spread the forum plan's expensive initial indexes across two shards.
    # The other shards can skip membership-only mutations entirely.
    member_shards = min(2, count)
    for reads in sorted(membership, key=lambda group: -weight(group)):
        target = min(range(member_shards), key=sizes.__getitem__)
        groups[target].append(reads)
        sizes[target] += 3 * weight(reads)
    if likes:
        independent.append(list(merge(*likes, key=lambda read: read.ordinal)))
    for reads in sorted(independent, key=lambda group: -weight(group)):
        target = min(range(len(groups)), key=sizes.__getitem__)
        groups[target].append(reads)
        sizes[target] += weight(reads)
    return [list(merge(*group, key=lambda read: read.ordinal)) for group in groups]


def relevant_deltas(commits, reads):
    labels = {read.query.label for read in reads}
    unused = set()
    if "IC5" not in labels:
        unused.add("HAS_MEMBER")
    if "IC7" not in labels:
        unused.add("LIKES")
    return [
        (epoch, delta)
        for epoch, delta in enumerate(commits, 1)
        if delta.nodes or any(edge[0] not in unused for edge in delta.edges)
    ]


def prepare_groups(prepared, workers):
    groups = [
        (reads, relevant_deltas(prepared.commits, reads))
        for reads in partitions(prepared.model, prepared.demands, workers)
        if reads
    ]
    del prepared.demands
    return groups


def worker(prepared, reads, deltas, result_map, details):
    try:
        start = monotonic()
        model = prepared.model
        cursor = 0
        with memoryview(result_map).cast("i") as result:
            for read in reads:
                while cursor < len(deltas) and deltas[cursor][0] <= read.epoch:
                    deltas[cursor][1].feed(model)
                    cursor += 1
                answer = read.query.evaluate(model)
                if answer != read.expected:
                    details.write_text(
                        json.dumps(
                            dict(
                                ordinal=read.ordinal,
                                operation=read.query.label,
                                actual=answer,
                                expected=read.expected,
                            ),
                            ensure_ascii=False,
                        ),
                        encoding="utf-8",
                    )
                    raise ValueError(f"Result mismatch at {read.ordinal}")
                result[read.ordinal - 1] = len(answer)
        details.with_suffix(".timing.json").write_text(
            json.dumps(
                dict(
                    reads=len(reads),
                    replayed_writes=cursor,
                    seconds=monotonic() - start,
                )
            ),
            encoding="utf-8",
        )
    except BaseException:
        if not details.exists():
            import traceback

            details.write_text(traceback.format_exc(), encoding="utf-8")
        raise


def run(prepared, groups, work, receipts_path=None):
    start = monotonic()
    work.mkdir()
    initial = array("i", [-1]) * OPERATION_COUNT
    context = get_context("fork")
    tasks = []
    with mmap.mmap(-1, len(initial) * initial.itemsize) as shared:
        shared.write(initial)
        with memoryview(shared).cast("i") as result:
            try:
                for i, (reads, deltas) in enumerate(groups):
                    detail = work / f"worker-{i}.json"
                    task = context.Process(
                        target=worker, args=(prepared, reads, deltas, shared, detail)
                    )
                    task.start()
                    tasks.append((task, detail))
                last_scn = prepared.initial_scn
                last_lsn = 0
                logical_rows = 0
                for delta in prepared.commits:
                    delta.verify()
                    if delta.scn <= last_scn or delta.durable_lsn <= last_lsn:
                        raise ValueError("Committed snapshots are out of order")
                    count = len(delta.nodes) + len(delta.edges)
                    result[delta.ordinal - 1] = count
                    logical_rows += count
                    last_scn, last_lsn = delta.scn, delta.durable_lsn
                for task, detail in tasks:
                    task.join()
                    if task.exitcode:
                        raise ValueError(f"Parallel verification failed; see {detail}")
            finally:
                for task, _ in tasks:
                    if task.is_alive():
                        task.terminate()
                        task.join()
                    task.close()
            if receipts_path is None:
                receipts_path = work / "receipts.tsv"
            with receipts_path.open("w", encoding="utf-8") as receipts:
                receipts.write("ordinal\tscn\toperation\tstatus\trows\n")
                for prefix, count in zip(
                    prepared.receipt_prefixes, result, strict=True
                ):
                    if count < 0:
                        raise ValueError(f"Missing verification receipt: {prefix}")
                    receipts.write(f"{prefix}PASS\t{count}\n")
    seconds = monotonic() - start
    result = dict(
        total_seconds=seconds,
        counts=prepared.counts,
        final_scn=last_scn,
        verified_logical_graph_changes=logical_rows,
        operations=OPERATION_COUNT,
        workers=len(groups),
        within_deadline=seconds <= DEADLINE_SECONDS,
        mismatches=0,
    )
    (work / "summary.json").write_text(
        json.dumps(result, indent=2) + "\n", encoding="utf-8"
    )
    return result


def verify_complete(
    capture,
    validation,
    workers,
    receipts,
    mismatches,
    follow_capture,
    snapshot_loader=None,
):
    start = monotonic()
    gc_enabled = gc.isenabled()
    gc.disable()
    try:
        prepared = prepare(capture, validation, follow_capture, snapshot_loader)
        groups = prepare_groups(prepared, workers)
        # Preserve shared graph pages when the children start collecting.
        gc.freeze()
        setup_seconds = monotonic() - start
        try:
            result = run(prepared, groups, receipts.parent / "workers", receipts)
        finally:
            gc.unfreeze()
    finally:
        if gc_enabled:
            gc.enable()
    mismatches.write_text("", encoding="utf-8")
    return dict(
        total_operations=result["operations"],
        counts=result["counts"],
        mismatches={},
        scn=result["final_scn"],
        logical_rows_verified=result["verified_logical_graph_changes"],
        setup_seconds=setup_seconds,
        replay_seconds=result["total_seconds"],
        total_seconds=monotonic() - start,
        workers=result["workers"],
    )


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def queries_only(
    work, finished_run, validation, workers, repeats, snapshot_loader=None
):
    if workers < 1 or repeats < 1:
        raise ValueError("Worker and repeat counts must be positive")
    start = monotonic()
    capture = finished_run / "actual"
    snapshot = json.loads((capture / "snapshot.json").read_text())
    previous = json.loads((finished_run / "verification.json").read_text())
    if previous["total_operations"] != OPERATION_COUNT or previous["mismatches"]:
        raise ValueError("Use a previously completed full capture with zero mismatches")
    verify_receipts(
        finished_run / "receipts.tsv",
        capture / "commits.jsonl",
        snapshot["snapshot_scn"],
        previous,
    )
    prepared = prepare(capture, validation, snapshot_loader=snapshot_loader)
    if (
        prepared.counts != previous["counts"]
        or prepared.commits[-1].scn != previous["scn"]
    ):
        raise ValueError("Prepared trace differs from the verified capture")
    groups = prepare_groups(prepared, workers)
    gc.collect()
    gc.freeze()
    identity = {
        "validation_sha256": sha256(validation),
        "commit_ledger_sha256": sha256(capture / "commits.jsonl"),
        "initial_scn": prepared.initial_scn,
    }
    setup_seconds = monotonic() - start
    print(
        f"Prepared verified snapshot and requests in {setup_seconds:.2f}s", flush=True
    )
    runs = []
    try:
        for i in range(repeats):
            result = run(prepared, groups, work / f"run-{i + 1}")
            print(
                f"Replay {i + 1}: {result['total_seconds']:.3f}s, {result['operations']:,} PASS",
                flush=True,
            )
            runs.append(result)
    finally:
        gc.unfreeze()
    report = dict(
        mode="queries-only",
        deadline_seconds=DEADLINE_SECONDS,
        preparation_seconds=setup_seconds,
        capture=str(capture),
        identity=identity,
        runs=runs,
    )
    (work / "summary.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    return 0 if all(r["within_deadline"] for r in runs) else 3
