#!/usr/bin/env python3
"""Summarize issue #7 reproduction artifacts.

Reads the workdir produced by issue7_bulk_load_churn.py:
  summary.json, batches.jsonl, samples.jsonl, stalls.jsonl, server-*.log

Prints batch latency outliers, WAL/checkpoint/undo metric extremes, and a
correlated timeline so a 75-90 s stall can be attributed to a specific
maintenance phase.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import statistics

STRACE_DURATION = re.compile(r"<([0-9]+\.[0-9]+)>$")

INTERESTING = (
    "checkpoint_duration_max_us",
    "checkpoint_duration_total_us",
    "last_checkpoint_duration_us",
    "last_checkpoint_snapshot_us",
    "last_checkpoint_flush_us",
    "last_checkpoint_sync_us",
    "last_checkpoint_compact_us",
    "last_checkpoint_dirty_pages",
    "last_checkpoint_trigger",
    "last_checkpoint_kind",
    "checkpoints_total",
    "wal_rotation_count",
    "wal_rotation_critical_max_us",
    "wal_rotation_critical_total_us",
    "wal_rotation_fallback_count",
    "wal_sync_max_us",
    "wal_sync_avg_us",
    "wal_bytes",
    "wal_pending_sealed_sync",
    "buffer_pool_checkpoint_preflush_pages",
    "buffer_pool_checkpoint_preflush_us",
    "buffer_pool_checkpoint_flush_pages",
    "buffer_pool_checkpoint_flush_us",
    "buffer_pool_dirty_pages",
    "buffer_pool_evictions",
    "buffer_pool_waits",
    "buffer_pool_wait_ms",
    "buffer_pool_file_syncs",
    "buffer_pool_file_sync_skipped",
    "undo_bytes",
    "undo_records",
    "undo_purged_records",
    "undo_purge_runs",
    "undo_purge_last_unix_ms",
    "undo_io_flush_ns_p99",
    "undo_io_write_frame_ns_p99",
    "memory_reclaim_attempts",
    "memory_reclaim_cache_bytes",
    "memory_reclaim_buffer_bytes",
    "memory_oom_after_reclaim",
    "zone_map_rebuilds",
    "zone_map_maintenance_lag_ms",
    "zone_map_rebuild_shrink_zones",
    "zone_map_rebuild_union_zones",
    "zone_map_candidate_generations",
    "auto_analyze_total_runs",
    "auto_analyze_total_tables_analyzed",
    "auto_analyze_last_run_unix_ms",
)


def as_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def load_json(path: Path):
    if not path.exists():
        return None
    return json.loads(path.read_text())


def load_jsonl(path: Path):
    if not path.exists():
        return []
    rows = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if line:
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return rows


def percentile(values, pct):
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round((pct / 100) * (len(ordered) - 1))))
    return ordered[index]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--workdir", default="/tmp/mydb-issue7-run1")
    parser.add_argument("--slow-batch", type=float, default=1.0)
    parser.add_argument("--window", type=float, default=8.0)
    args = parser.parse_args()
    workdir = Path(args.workdir)

    summary = load_json(workdir / "summary.json")
    batches = load_jsonl(workdir / "batches.jsonl")
    samples = load_jsonl(workdir / "samples.jsonl")
    stalls = load_jsonl(workdir / "stalls.jsonl")

    print(f"== {workdir} ==")
    if summary:
        print("wall_s", summary.get("wall_s"), "samples", summary.get("samples"),
              "slow_probes", summary.get("slow_probes"))
        for phase in ("baseline", "sustained"):
            if phase in summary:
                entry = summary[phase]
                print(f"{phase}: rows={entry.get('rows')} batches={entry.get('batches')} "
                      f"max={entry.get('max_batch_s')}s")
        if "error" in summary:
            print("ERROR:", summary["error"][:2000])

    elapsed = [float(batch["elapsed_s"]) for batch in batches]
    print(f"\nbatches: {len(batches)} rows: {sum(int(b['rows']) for b in batches)}")
    if elapsed:
        print(f"latency s: p50={statistics.median(elapsed):.3f} "
              f"p95={percentile(elapsed, 95):.3f} p99={percentile(elapsed, 99):.3f} "
              f"max={max(elapsed):.3f}")
        slow = sorted((b for b in batches if float(b["elapsed_s"]) >= args.slow_batch),
                      key=lambda b: float(b["elapsed_s"]), reverse=True)
        print(f"slow batches >= {args.slow_batch}s: {len(slow)}")
        for batch in slow[:20]:
            print(f"  {batch['ts_ms']} id={batch['id_start']}..{batch['id_end']} "
                  f"{batch['rows']} rows in {batch['elapsed_s']}s")

    # Metric extremes and deltas.
    series: dict[str, list[tuple[int, int]]] = {}
    for sample in samples:
        ts = sample.get("ts_ms")
        for name, value in (sample.get("metrics") or {}).items():
            parsed = as_int(value)
            if parsed is not None and ts is not None:
                series.setdefault(name, []).append((int(ts), parsed))
    print("\nmetric extremes:")
    for name in INTERESTING:
        points = series.get(name)
        if not points:
            continue
        first, last = points[0][1], points[-1][1]
        peak_ts, peak = max(points, key=lambda point: point[1])
        if first == 0 and peak == 0:
            continue
        print(f"  {name}: first={first} last={last} delta={last - first} peak={peak} "
              f"at {peak_ts}")

    # Checkpoint phase attribution: total minus the instrumented phases.
    total = series.get("last_checkpoint_duration_us") or []
    phases = [
        series.get(name) or []
        for name in ("last_checkpoint_snapshot_us", "last_checkpoint_flush_us",
                     "last_checkpoint_sync_us", "last_checkpoint_compact_us")
    ]
    if total and all(phases):
        by_ts = [{ts: value for ts, value in points} for points in phases]
        worst = None
        for ts, value in total:
            if not all(ts in mapping for mapping in by_ts):
                continue
            attributed = sum(mapping[ts] for mapping in by_ts)
            remainder = value - attributed
            if worst is None or remainder > worst[1]:
                worst = (ts, remainder, value, attributed)
        if worst:
            print(f"\ncheckpoint attribution worst: ts={worst[0]} "
                  f"total={worst[2] / 1e6:.3f}s phases={worst[3] / 1e6:.3f}s "
                  f"unattributed={worst[1] / 1e6:.3f}s")

    # Samples where a new checkpoint / WAL rotation completed.
    events = []
    for name in ("checkpoints_total", "wal_rotation_count", "zone_map_rebuilds"):
        points = series.get(name)
        if not points:
            continue
        previous = points[0][1]
        for ts, value in points[1:]:
            if value > previous:
                events.append((ts, f"{name} {previous}->{value}"))
            previous = value
    long_ops = []
    last_seen: dict[str, int] = {}
    for name in ("last_checkpoint_duration_us", "last_checkpoint_sync_us",
                 "last_checkpoint_compact_us", "buffer_pool_checkpoint_preflush_us"):
        points = series.get(name)
        if not points:
            continue
        for ts, value in points:
            if value and value >= 1_000_000 and last_seen.get(name) != value:
                long_ops.append((ts, f"{name}={value / 1e6:.3f}s"))
                last_seen[name] = value
    for name in ("wal_rotation_critical_max_us", "wal_sync_max_us"):
        points = series.get(name)
        if not points:
            continue
        for ts, value in points:
            if value and value >= 1_000_000 and last_seen.get(name) != value:
                long_ops.append((ts, f"{name}={value / 1e6:.3f}s"))
                last_seen[name] = value
    events.extend(long_ops)
    if events:
        print("\nmaintenance timeline (>=1s operations and completions):")
        for ts, text in sorted(events):
            print(f"  {ts} {text}")

    # Probe / connection responsiveness.
    probes = [s for s in samples if "probe_ms" in s]
    if probes:
        failed = [s for s in probes if not s.get("probe_ok", True)]
        print(f"\nprobes: {len(probes)} failed={len(failed)} "
              f"max={max(float(s['probe_ms']) for s in probes):.1f}ms "
              f"connect_max={max(float(s.get('connect_ms', 0)) for s in probes):.1f}ms")
        for sample in failed[:10]:
            print(f"  failed at {sample['ts_ms']}: {sample.get('probe_error')}")
        for sample in probes:
            if float(sample.get("probe_ms", 0)) >= 1000:
                print(f"  slow probe at {sample['ts_ms']}: {sample['probe_ms']}ms")

    slow_logs = []
    for path in sorted(workdir.glob("server-*.log")):
        for line in path.read_text(errors="replace").splitlines():
            if any(marker in line for marker in
                   ("slow checkpoint", "slow WAL rotation", "slow WAL segment",
                    "slow undo purge")):
                slow_logs.append(line.strip())
    if slow_logs:
        print(f"\nslow-maintenance log lines: {len(slow_logs)}")
        for line in slow_logs[:80]:
            print("  " + line[:300])

    strace_files = sorted(workdir.glob("strace-*.log"))
    if strace_files:
        durations: dict[str, list[float]] = {}
        slow_calls = []
        for path in strace_files:
            for line in path.read_text(errors="replace").splitlines():
                match = STRACE_DURATION.search(line)
                if not match or "(" not in line:
                    continue
                duration = float(match.group(1))
                name = line.split("(", 1)[0].split()[-1]
                durations.setdefault(name, []).append(duration)
                if duration >= 1.0:
                    slow_calls.append((duration, line))
        print(f"\nstrace syscalls: {sum(len(v) for v in durations.values())} "
              f"files={len(strace_files)}")
        for name, values in sorted(durations.items(), key=lambda item: max(item[1]), reverse=True):
            print(f"  {name}: count={len(values)} max={max(values):.3f}s "
                  f"p99={percentile(values, 99):.3f}s")
        if slow_calls:
            print(f"syscalls >= 1s: {len(slow_calls)}")
            for duration, line in sorted(slow_calls, reverse=True)[:20]:
                print(f"  {duration:.3f}s {line[:240]}")

    if stalls:
        print(f"\nstall captures: {len(stalls)}")
        for record in stalls[:10]:
            print(f"  ts={record.get('ts_ms')} last_id={record.get('last_id')} "
                  f"error={record.get('error')}")
            processlist = record.get("processlist") or []
            for row in processlist:
                if str(row.get("COMMAND", "")).upper() != "SLEEP":
                    print(f"    {row.get('ID')} {row.get('COMMAND')} {row.get('TIME')}s "
                          f"{row.get('STATE')} {str(row.get('INFO'))[:120]}")
            status = str(record.get("engine_status", ""))
            for line in status.splitlines():
                if any(word in line.lower() for word in ("checkpoint", "wal", "rotation", "undo", "purge")):
                    print("    " + line.strip()[:200])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
