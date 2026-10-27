#!/usr/bin/env python3
"""Same-directory server A/B with a cold first wave and steady warm load.

Cold means a fresh database process and no preceding target-query execution.
Optional Darwin os-files mode also verifies nonresident test-file pages after
uncached same-path copies. Reference SQL/oracle checks run separately.
Requires PyMySQL; configuration and all diagnostic outputs are JSON.
"""

import argparse
import contextlib
import csv
import ctypes
import hashlib
import json
import mmap
import os
import platform
from pathlib import Path
import re
import socket
import statistics
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace

import rrf_bench as rrf


def resident_pages(path):
    size = path.stat().st_size
    if not size:
        return 0
    pages = (size + mmap.PAGESIZE - 1) // mmap.PAGESIZE
    library = ctypes.CDLL(None, use_errno=True)
    library.mincore.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p]
    with path.open("rb") as stream:
        with mmap.mmap(stream.fileno(), size, flags=mmap.MAP_PRIVATE,
                       prot=mmap.PROT_READ | mmap.PROT_WRITE) as mapping:
            address = ctypes.addressof(ctypes.c_char.from_buffer(mapping))
            state = (ctypes.c_ubyte * pages)()
            if library.mincore(address, size, state):
                raise OSError(ctypes.get_errno(), "mincore failed")
            return sum(bool(value & 1) for value in state)


def cold_data_files(data):
    """Darwin-only, file-scoped cold copies of stopped benchmark payloads.

    F_NOCACHE writes a new inode without warming its file pages. Rename keeps
    the configured path (and buffer-pool shard hash) identical. Locks, WAL and
    manifests are not replaced. Never apply this to a running database.
    """
    data = Path(data).resolve()
    if sys.platform != "darwin":
        raise RuntimeError("os-files cold mode requires Darwin F_NOCACHE/mincore")
    import fcntl
    if not data.is_relative_to(Path("/tmp/mydb").resolve()):
        raise ValueError("cold file rewriting is restricted to /tmp/mydb benchmark copies")
    targets = [path for path in data.rglob("*") if not path.is_symlink() and path.is_file()
               and path.suffix in {".heap", ".idx", ".hvec", ".lob", ".zm"}]
    if not targets:
        raise ValueError("no benchmark data files to cold-copy")
    result = dict(method="Darwin F_NOCACHE atomic file copy + mincore", files=0,
                  bytes=0, resident_pages_before=0, resident_pages_after=0)
    for path in targets:
        stat = path.stat()
        result["resident_pages_before"] += resident_pages(path)
        descriptor, temporary = tempfile.mkstemp(prefix=".cache-cold-", dir=path.parent)
        try:
            # sys/fcntl.h: F_NOCACHE = 48, applies only to this descriptor.
            fcntl.fcntl(descriptor, 48, 1)
            os.fchmod(descriptor, stat.st_mode & 0o777)
            copied = 0
            with path.open("rb") as source:
                for block in iter(lambda: source.read(1024 * 1024), b""):
                    remaining = memoryview(block)
                    while remaining:
                        written = os.write(descriptor, remaining)
                        if not written:
                            raise OSError("short write while cold-copying data")
                        copied += written
                        remaining = remaining[written:]
            if copied != stat.st_size:
                raise RuntimeError("database file changed during cold copy")
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = None
            os.utime(temporary, ns=(stat.st_atime_ns, stat.st_mtime_ns))
            os.replace(temporary, path)
            after = resident_pages(path)
            if after:
                raise RuntimeError(f"OS cache eviction unverified: {path}, {after} resident pages")
            result["resident_pages_after"] += after
            result["bytes"] += copied
            result["files"] += 1
        finally:
            if descriptor is not None:
                os.close(descriptor)
            Path(temporary).unlink(missing_ok=True)
    return result


def file_hash(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def groups(config):
    result = []
    identities = set()
    for workload in config["workloads"]:
        manifest = json.loads(Path(workload["manifest"]).read_text())
        selected = [case for case in manifest["cases"]
                    if re.search(workload["only"], case["name"])]
        if not selected:
            raise ValueError("workload pattern selects no cases: " + workload["only"])
        for case in selected:
            for concurrency in workload["concurrency"]:
                if not isinstance(concurrency, int) or concurrency < 1:
                    raise ValueError("concurrency must be a positive integer")
                item = dict(case=case, database=manifest["database"], concurrency=concurrency,
                            manifest=workload["manifest"], manifest_sha256=file_hash(workload["manifest"]),
                            environment=workload.get("environment", "default"),
                            server_env=workload.get("server_env", {}),
                            prepared=workload.get("sql_prepare", False))
                identity = (item["environment"], item["database"], case["name"], concurrency)
                if identity in identities:
                    raise ValueError("duplicate measurement group: " + repr(identity))
                identities.add(identity)
                result.append(item)
    if not result:
        raise ValueError("no measurement groups")
    return result


def reference_key(group):
    payload = [group["database"], group["case"], group["manifest_sha256"]]
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


@contextlib.contextmanager
def server(binary, data, port, log_path, environment):
    command = [str(binary), "--host", "127.0.0.1", "--port", str(port),
               "--data-dir", str(data), "--ssl-mode", "disabled"]
    with Path(log_path).open("w") as log:
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                                   env={**os.environ, **environment})
        try:
            deadline = time.monotonic() + 60
            while True:
                if process.poll() is not None:
                    raise RuntimeError("server exited during startup: " + str(log_path))
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=1):
                        break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("server startup timeout")
                    time.sleep(.1)
            yield process
        finally:
            process.terminate()
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
                raise RuntimeError("server required forced termination")


def buffer_status(settings):
    connection = rrf.connect(settings)
    try:
        return {key: value for key, value in rrf.execute(connection, "SHOW GLOBAL STATUS")
                if key.startswith(("Mydb_buffer_pool", "Innodb_buffer_pool", "Innodb_data_"))}
    finally:
        connection.close()


def measure_pair(settings, group, pid, reference, duration, warmup):
    # Never call verify/EXPLAIN here: either would warm the measured workload.
    case = {**group["case"], "expected_digest": reference["result_sha256"]}
    before = buffer_status(settings)
    cold_args = SimpleNamespace(duration=0, requests=1, warmup=0,
                                sql_prepare=group["prepared"], server_pid=pid)
    cold = rrf.benchmark(settings, group["database"], case, group["concurrency"], cold_args)
    after_cold = buffer_status(settings)
    warm_args = SimpleNamespace(duration=duration, requests=0, warmup=warmup,
                                sql_prepare=group["prepared"], server_pid=pid)
    warm = rrf.benchmark(settings, group["database"], case, group["concurrency"], warm_args)
    return dict(cold_first_wave=cold, warm=warm,
                buffer_before=before, buffer_after_cold=after_cold,
                buffer_after_warm=buffer_status(settings))


def make_references(config, selected, settings, output, port):
    references = {}
    with server(config["versions"]["before"], config["data_dirs"][0], port,
                output / "reference-server.log", config.get("server_env", {})):
        for group in selected:
            key = reference_key(group)
            if key in references:
                continue
            connection = rrf.connect(settings, group["database"])
            try:
                references[key] = rrf.verify(connection, group["case"])
            finally:
                connection.close()
    rrf.save(output / "references.json", references)
    return references


def summarize(output):
    metadata = json.loads((output / "measurement.json").read_text())
    expected = len(groups(metadata["config"])) * len(metadata["config"]["data_dirs"]) * metadata["repeats"] * 2
    records = [json.loads(path.read_text()) for path in sorted(output.glob("run-*.json"))]
    if len(records) != expected or any(record["status"] != "passed" for record in records):
        raise ValueError("incomplete/failed measurements")
    buckets = {}
    for record in records:
        key = (record["group"], record["data_dir"])
        buckets.setdefault(key, {}).setdefault(record["version"], []).append(record)
    rows = []
    for (group_id, data), versions in buckets.items():
        if set(versions) != {"before", "after"}:
            raise ValueError("unpaired versions")
        group = versions["before"][0]["measurement_group"]
        row = dict(group=group_id, data_dir=data, name=group["case"]["name"],
                   database=group["database"], manifest=group["manifest"],
                   concurrency=group["concurrency"], environment=group["environment"])
        counts = set()
        for version, samples in versions.items():
            counts.add(len(samples))
            row[version] = dict(
                repeats=len(samples),
                warm_qps=statistics.median(s["warm"]["qps"] for s in samples),
                warm_p50_ms=statistics.median(s["warm"]["p50_ms"] for s in samples),
                warm_p99_ms=statistics.median(s["warm"]["p99_ms"] for s in samples),
                cold_p50_ms=statistics.median(s["cold_first_wave"]["p50_ms"] for s in samples),
                cold_max_ms=statistics.median(s["cold_first_wave"]["max_ms"] for s in samples),
                cpu_ms_per_operation=statistics.median(
                    s["warm"]["server"]["cpu_seconds"] / s["warm"]["operations"] * 1000
                    for s in samples))
        if len(counts) != 1:
            raise ValueError("unpaired repetitions")
        row["warm_qps_change_percent"] = (row["after"]["warm_qps"] / row["before"]["warm_qps"] - 1) * 100
        row["cold_p50_change_percent"] = (row["after"]["cold_p50_ms"] / row["before"]["cold_p50_ms"] - 1) * 100
        rows.append(row)
    rrf.save(output / "summary.json", dict(results=rows))
    flat = [{**{k: v for k, v in row.items() if k not in ("before", "after")},
             **{f"{side}_{k}": v for side in ("before", "after") for k, v in row[side].items()}}
            for row in rows]
    with (output / "summary.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(flat[0]))
        writer.writeheader()
        writer.writerows(flat)
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--phase", choices=("reference", "run", "summarize"), default="run")
    parser.add_argument("--port", type=int, default=14881)
    parser.add_argument("--duration", type=float, default=3)
    parser.add_argument("--warmup", type=int, default=10)
    parser.add_argument("--repeats", type=int, default=2)
    parser.add_argument("--cold-mode", choices=("process", "os-files"), default="process")
    args = parser.parse_args()
    if args.duration <= 0 or args.warmup < 0 or args.repeats < 1:
        parser.error("duration/repeats must be positive; warmup must be nonnegative")
    args.output.mkdir(parents=True, exist_ok=True)
    if args.phase == "summarize":
        summarize(args.output)
        return
    config = json.loads(args.config.read_text())
    selected = groups(config)
    settings = dict(host="127.0.0.1", port=args.port, user="root",
                    password=os.getenv("MYDB_PASSWORD", "root"))
    if args.phase == "reference":
        make_references(config, selected, settings, args.output, args.port)
        return
    references = json.loads((args.output / "references.json").read_text())
    missing = [group["case"]["name"] for group in selected if reference_key(group) not in references]
    if missing:
        raise ValueError("missing reference queries; rerun --phase reference: " + ", ".join(missing))
    metadata = dict(config=config, config_sha256=file_hash(args.config), duration=args.duration,
                    warmup=args.warmup, repeats=args.repeats,
                    port=args.port, runner_sha256=file_hash(__file__),
                    rrf_runner_sha256=file_hash(rrf.__file__),
                    host_environment=dict(platform=platform.platform(), python=platform.python_version(),
                                          machine=platform.machine(), client_cpus=os.cpu_count()),
                    performance_environment={key: os.environ[key] for key in (
                        "MYDB_MEMORY_TARGET", "MYDB_EXECUTOR_MEMORY_TARGET", "MYDB_BUFFER_POOL_PAGES",
                        "MYDB_BUFFER_POOL_PREFETCH_PAGES", "MYDB_BUFFER_POOL_IO_THREADS",
                        "MYDB_HNSW_MAPPED_NAVIGATION", "MYDB_VECTOR_BRUTE_SME") if key in os.environ},
                    binaries={side: file_hash(path) for side, path in config["versions"].items()},
                    cold_mode=args.cold_mode,
                    cold_definition="fresh process; first target-query wave; " +
                    ("data-file OS cache verified nonresident" if args.cold_mode == "os-files" else "OS cache not purged"))
    metadata_path = args.output / "measurement.json"
    if metadata_path.exists() and json.loads(metadata_path.read_text()) != metadata:
        raise ValueError("resume configuration or binary hashes differ")
    rrf.save(metadata_path, metadata)
    for data_index, data in enumerate(config["data_dirs"]):
        for group_id, group in enumerate(selected):
            reference = references[reference_key(group)]
            for repeat in range(args.repeats):
                for side in (("before", "after") if repeat % 2 == 0 else ("after", "before")):
                    label = f"run-d{data_index}-g{group_id:03d}-r{repeat}-{side}"
                    path = args.output / (label + ".json")
                    if path.exists() and json.loads(path.read_text()).get("status") == "passed":
                        print("REUSE", label, flush=True)
                        continue
                    if "rustc" in subprocess.check_output(["ps", "-Ao", "comm="], text=True):
                        raise RuntimeError("compiler active during measurement")
                    print("START", label, group["case"]["name"], group["concurrency"], flush=True)
                    env = {**config.get("server_env", {}), **group["server_env"]}
                    cold_files = cold_data_files(data) if args.cold_mode == "os-files" else None
                    with server(config["versions"][side], data, args.port,
                                args.output / (label + ".log"), env) as process:
                        measured = measure_pair(settings, group, process.pid, reference, args.duration, args.warmup)
                    rrf.save(path, dict(status="passed", group=group_id, data_dir=data,
                                       version=side, repeat=repeat, measurement_group=group,
                                       cold_files=cold_files, **measured))
                    print("PASS", label, round(measured["warm"]["qps"], 1), flush=True)
    summarize(args.output)


if __name__ == "__main__":
    main()
