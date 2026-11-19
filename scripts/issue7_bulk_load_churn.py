#!/usr/bin/env python3
"""Issue #7 reproduction: bulk-insert stalls after heavy churn.

The workload mirrors the public report:
  * 59-column staging table with ~7.7 KiB of VARCHAR payload per row,
  * autocommit batches of 2,000 rows,
  * a fresh connection every 25,000 rows,
  * repeated loads interleaved with client SIGKILL, server SIGKILL/crash
    recovery, stale-session KILL, and (optionally) a full backup,
  * a final sustained load while a sampler records engine/WAL/checkpoint/undo
    metrics every second.

The script is self-contained: it starts its own server on a free loopback
port under a data directory it owns.  Pass --cgroup to place the server in a
15 GiB / 4 CPU cgroup (requires passwordless sudo).

Examples:
  python3 scripts/issue7_bulk_load_churn.py --binary target/release/mydb \
      --workdir /tmp/mydb-issue7 --cgroup

  # Faster smoke test (no churn):
  python3 scripts/issue7_bulk_load_churn.py --binary target/release/mydb \
      --baseline-rows 30000 --cycles 0 --sustained-rows 30000
"""

from __future__ import annotations

import argparse
import datetime
import decimal
import json
import os
from pathlib import Path
import random
import signal
import socket
import subprocess
import sys
import threading
import time
import traceback

import pymysql

ROOT = Path(__file__).resolve().parents[1]
STRING_COUNT = 20
STRING_LEN = 384
DECIMAL_COUNT = 20
DATETIME_COUNT = 18
COLUMN_COUNT = 1 + STRING_COUNT + DECIMAL_COUNT + DATETIME_COUNT
BATCH_ROWS = 2000
CHUNK_ROWS = 25_000

METRIC_PREFIXES = (
    "checkpoint_", "last_checkpoint_", "wal_", "buffer_pool_", "undo_",
    "zone_map_", "memory_", "disk_", "restartpoint", "reduced_redo_",
    "current_lsn", "durable_lsn", "dirty_heap_pages", "dirty_index_pages",
    "index_build_", "bulk_index_", "online_constraint_", "auto_analyze_",
)


def now_unix_ms() -> int:
    return int(time.time() * 1000)


def log(message: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)


def sql(connection, statement, args=None):
    with connection.cursor() as cursor:
        cursor.execute(statement, args)
        return cursor.fetchall() if cursor.description else ()


def connect(port: int, *, read_timeout: int = 180, connect_timeout: int = 5):
    return pymysql.connect(
        host="127.0.0.1",
        port=port,
        user="root",
        password="root",
        database="test",
        autocommit=True,
        charset="utf8mb4",
        connect_timeout=connect_timeout,
        read_timeout=read_timeout,
        write_timeout=read_timeout,
    )


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def make_string_pool() -> list[str]:
    rng = random.Random(7)
    alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    return ["".join(rng.choices(alphabet, k=STRING_LEN)) for _ in range(1024)]


STRING_POOL = make_string_pool()
DECIMAL_POOL = [decimal.Decimal(f"{i * 37 % 100000}.{i % 10000:04d}") for i in range(1024)]
DATETIME_POOL = [
    datetime.datetime(2025, 1, 1) + datetime.timedelta(seconds=i * 17) for i in range(1024)
]


def row_values(row_id: int, unique_s0: bool = True) -> list[object]:
    slot = row_id & 1023
    values: list[object] = [row_id]
    # A unique indexed s0 mimics a real staging feed; a small reused pool
    # keeps the secondary index tiny and is useful as a control.
    if unique_s0:
        values.append(f"{row_id:012d}" + STRING_POOL[slot][12:])
    else:
        values.append(STRING_POOL[slot])
    values.extend(STRING_POOL[(slot + i * 31) & 1023] for i in range(1, STRING_COUNT))
    values.extend(DECIMAL_POOL[(slot + i * 17) & 1023] for i in range(DECIMAL_COUNT))
    values.extend(DATETIME_POOL[(slot + i * 11) & 1023] for i in range(DATETIME_COUNT))
    assert len(values) == COLUMN_COUNT, (len(values), COLUMN_COUNT)
    return values


def ddl() -> str:
    columns = ["id BIGINT PRIMARY KEY"]
    columns.extend(f"s{i} VARCHAR({STRING_LEN})" for i in range(STRING_COUNT))
    columns.extend(f"n{i} DECIMAL(18,4)" for i in range(DECIMAL_COUNT))
    columns.extend(f"d{i} DATETIME" for i in range(DATETIME_COUNT))
    return (
        "CREATE TABLE staging (\n  "
        + ",\n  ".join(columns)
        + ",\n  KEY s0_idx(s0)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    )


def ensure_table(port: int) -> None:
    connection = connect(port)
    try:
        sql(connection, ddl())
    except pymysql.err.MySQLError as exc:
        if exc.args[0] != 1050:  # table already exists
            raise
    finally:
        connection.close()


def max_id(port: int, attempts: int = 30) -> int:
    last: Exception | None = None
    for _ in range(attempts):
        connection = None
        try:
            connection = connect(port, read_timeout=5)
            rows = sql(connection, "SELECT COALESCE(MAX(id), 0) FROM staging")
            return int(rows[0][0])
        except Exception as exc:  # server may be recovering
            last = exc
            time.sleep(0.5)
        finally:
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass
    raise RuntimeError(f"cannot read MAX(id): {last}")


def wait_for_quiet(port: int, timeout: float = 5.0) -> None:
    """Give a disconnected session's in-flight statement time to finish.

    A SIGKILLed client can leave the server finishing the current INSERT.
    Reading MAX(id) too early makes the next worker start inside that
    batch; INSERT IGNORE absorbs the duplicates, but waiting keeps the
    row ranges clean and also exercises the peer-disconnect cleanup path.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        connection = None
        try:
            connection = connect(port, read_timeout=2)
            trx = int(sql(connection, "SELECT COUNT(*) FROM information_schema.innodb_trx")[0][0])
            active = int(sql(
                connection,
                "SELECT COUNT(*) FROM information_schema.PROCESSLIST "
                "WHERE COMMAND <> 'Sleep' AND ID <> CONNECTION_ID()",
            )[0][0])
            if trx == 0 and active == 0:
                return
        except Exception:
            return
        finally:
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass
        time.sleep(0.2)


class Server:
    def __init__(self, binary: Path, workdir: Path, args):
        self.binary = binary
        self.workdir = workdir
        self.args = args
        self.port = free_port()
        self.data = workdir / "data"
        self.data.mkdir(parents=True, exist_ok=True)
        self.process: subprocess.Popen[bytes] | None = None
        self.starts = 0
        self.cgroup = f"/sys/fs/cgroup/mydb-issue7-{os.getpid()}"
        self.co_tenants: list[subprocess.Popen[bytes]] = []

    def env(self) -> dict[str, str]:
        env = {k: v for k, v in os.environ.items() if not k.startswith("MYDB_")}
        env.update(
            MYDB_DATA_DIR=str(self.data),
            MYDB_ROOT_PASSWORD="root",
            MYDB_DURABILITY=self.args.durability,
            MYDB_MEMORY_TARGET=self.args.memory_target,
            MYDB_TEMP_DISK_RESERVE_PCT="0",
            MYDB_WAL_SEGMENT_BYTES=str(self.args.wal_segment_bytes),
            MYDB_CHECKPOINT_INTERVAL_MS=str(self.args.checkpoint_interval_ms),
            MYDB_CHECKPOINT_WAL_BYTES=str(self.args.checkpoint_wal_bytes),
        )
        for item in self.args.server_env:
            key, _, value = item.partition("=")
            if not key:
                raise SystemExit(f"invalid --server-env {item!r}")
            env[key] = value
        return env

    def setup_cgroup(self) -> None:
        if not self.args.cgroup:
            return
        subprocess.run(["sudo", "-n", "mkdir", "-p", self.cgroup], check=True)
        for name, value in (
            ("memory.max", str(self.args.cgroup_memory_bytes)),
            ("cpu.max", f"{self.args.cgroup_cpu_quota} 100000"),
            ("memory.swap.max", "0"),
        ):
            subprocess.run(
                ["sudo", "-n", "tee", f"{self.cgroup}/{name}"],
                input=value.encode(),
                stdout=subprocess.DEVNULL,
                check=True,
            )
        if self.args.cgroup_io_max:
            subprocess.run(
                ["sudo", "-n", "tee", f"{self.cgroup}/io.max"],
                input=self.args.cgroup_io_max.encode(),
                stdout=subprocess.DEVNULL,
                check=True,
            )

    def start_co_tenants(self) -> None:
        """Mimic the reporter's VM sharing CPU/memory with other services."""
        if self.co_tenants or not self.args.cgroup:
            return
        commands = []
        for _ in range(self.args.co_tenant_cpu):
            commands.append([sys.executable, "-c", "while True: pass"])
        if self.args.co_tenant_mem_mb > 0:
            commands.append([
                sys.executable, "-c",
                "import time\n"
                f"hold = bytearray({self.args.co_tenant_mem_mb} * 1024 * 1024)\n"
                "for i in range(0, len(hold), 4096): hold[i] = 1\n"
                "while True: time.sleep(1)\n",
            ])
        for command in commands:
            process = subprocess.Popen(command, start_new_session=True,
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            subprocess.run(
                ["sudo", "-n", "tee", f"{self.cgroup}/cgroup.procs"],
                input=str(process.pid).encode(),
                stdout=subprocess.DEVNULL,
                check=True,
            )
            self.co_tenants.append(process)
        if self.co_tenants:
            log(f"started {len(self.co_tenants)} co-tenant processes in {self.cgroup}")

    def stop_co_tenants(self) -> None:
        for process in self.co_tenants:
            try:
                os.killpg(os.getpgid(process.pid), signal.SIGKILL)
            except OSError:
                pass
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
        self.co_tenants.clear()

    def join_cgroup(self) -> None:
        if not self.args.cgroup or self.process is None:
            return
        subprocess.run(
            ["sudo", "-n", "tee", f"{self.cgroup}/cgroup.procs"],
            input=str(self.process.pid).encode(),
            stdout=subprocess.DEVNULL,
            check=True,
        )
        membership = Path(f"/proc/{self.process.pid}/cgroup").read_text().strip()
        log(f"server pid {self.process.pid} cgroup: {membership}")

    def start(self) -> None:
        self.setup_cgroup()
        self.starts += 1
        self.log_path = self.workdir / f"server-{self.starts:02d}.log"
        command = [
            str(self.binary),
            "--host", "127.0.0.1",
            "--port", str(self.port),
            "--data-dir", str(self.data),
            "--ssl-mode", "disabled",
        ]
        if self.args.strace:
            command = [
                "strace", "-f", "-tt", "-T",
                "-e", "trace=fsync,fdatasync,fallocate,ftruncate,rename,renameat,renameat2,unlink,unlinkat,sync_file_range",
                "-o", str(self.workdir / f"strace-{self.starts:02d}.log"),
                "--",
            ] + command
        with self.log_path.open("wb") as output:
            self.process = subprocess.Popen(
                command,
                stdout=output,
                stderr=output,
                env=self.env(),
                cwd=self.workdir,
                start_new_session=True,
            )
        self.join_cgroup()
        self.start_co_tenants()
        started = time.monotonic()
        while time.monotonic() - started < 180:
            if self.process.poll() is not None:
                tail = self.log_path.read_text(errors="replace")[-4000:]
                raise RuntimeError(f"server exited during startup:\n{tail}")
            try:
                connection = connect(self.port, read_timeout=3)
                connection.close()
                log(f"server start #{self.starts} ready in {time.monotonic() - started:.2f}s")
                return
            except pymysql.Error:
                time.sleep(0.05)
        raise TimeoutError(f"server startup timed out: {self.log_path}")

    def server_pid(self) -> int | None:
        if self.process is None:
            return None
        if not self.args.strace:
            return self.process.pid
        try:
            children = Path(
                f"/proc/{self.process.pid}/task/{self.process.pid}/children"
            ).read_text().split()
            return int(children[0]) if children else self.process.pid
        except (OSError, ValueError):
            return self.process.pid

    def kill9(self) -> None:
        if self.process is None or self.process.poll() is not None:
            raise RuntimeError("server already exited before SIGKILL")
        pgid = os.getpgid(self.process.pid)
        log(f"server pid {self.server_pid()} SIGKILLed")
        os.killpg(pgid, signal.SIGKILL)
        self.process.wait(timeout=30)

    def stop(self) -> None:
        if self.process is not None and self.process.poll() is None:
            pgid = os.getpgid(self.process.pid)
            os.killpg(pgid, signal.SIGTERM)
            try:
                self.process.wait(timeout=60)
            except subprocess.TimeoutExpired:
                os.killpg(pgid, signal.SIGKILL)
                self.process.wait(timeout=30)
        self.process = None

    def cleanup(self) -> None:
        self.stop()
        self.stop_co_tenants()
        if self.args.cgroup:
            subprocess.run(
                ["sudo", "-n", "rmdir", self.cgroup],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )


class Sampler(threading.Thread):
    def __init__(self, server: Server, output: Path):
        super().__init__(daemon=True)
        self.server = server
        self.output = output
        self.stop_event = threading.Event()
        self.connection = None
        self.samples = 0
        self.slow_probes = 0

    def sample(self) -> dict[str, object]:
        sample: dict[str, object] = {"ts_ms": now_unix_ms()}
        connection = None
        started = time.monotonic()
        try:
            connection = connect(self.server.port, read_timeout=2, connect_timeout=2)
            sample["connect_ms"] = round((time.monotonic() - started) * 1000, 2)
            rows = sql(connection, "SELECT METRIC_NAME, METRIC_VALUE FROM information_schema.MYDB_ENGINE_STATUS")
            metrics = {
                str(name): value
                for name, value in rows
                if any(str(name).startswith(prefix) for prefix in METRIC_PREFIXES)
            }
            sample["metrics"] = metrics
            sample["trx"] = int(sql(connection, "SELECT COUNT(*) FROM information_schema.innodb_trx")[0][0])
            sample["processes"] = int(sql(connection, "SELECT COUNT(*) FROM information_schema.processlist")[0][0])
            pid = self.server.server_pid()
            if pid:
                io = {}
                for line in Path(f"/proc/{pid}/io").read_text().splitlines():
                    key, value = line.split(":", 1)
                    io[key] = int(value)
                sample["server_io"] = {k: io.get(k) for k in ("read_bytes", "write_bytes", "cancelled_write_bytes")}
                status = Path(f"/proc/{pid}/status").read_text()
                for line in status.splitlines():
                    if line.startswith(("VmRSS:", "VmHWM:")):
                        key, value = line.split(":", 1)
                        sample[key] = value.strip()
            sample["max_id"] = int(sql(connection, "SELECT COALESCE(MAX(id),0) FROM staging")[0][0])
        except Exception as exc:
            sample["error"] = f"{type(exc).__name__}: {exc}"
        finally:
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass
            # A dedicated probe: can a brand-new connection run SELECT 1?
            probe_started = time.monotonic()
            try:
                probe = connect(self.server.port, read_timeout=2, connect_timeout=2)
                sql(probe, "SELECT 1")
                probe.close()
                sample["probe_ok"] = True
            except Exception as exc:
                sample["probe_ok"] = False
                sample["probe_error"] = f"{type(exc).__name__}: {exc}"
            sample["probe_ms"] = round((time.monotonic() - probe_started) * 1000, 2)
            if sample.get("probe_ms", 0) > 1000:
                self.slow_probes += 1
        return sample

    def run(self) -> None:
        with self.output.open("w", encoding="utf-8") as handle:
            while not self.stop_event.is_set():
                sample = self.sample()
                handle.write(json.dumps(sample, separators=(",", ":")) + "\n")
                handle.flush()
                self.samples += 1
                self.stop_event.wait(1.0)

    def stop(self) -> None:
        self.stop_event.set()
        self.join(timeout=10)


def capture_stall(server: "Server", path: Path, last_id: int) -> None:
    record: dict[str, object] = {"ts_ms": now_unix_ms(), "last_id": last_id}
    connection = None
    try:
        connection = connect(server.port, read_timeout=3, connect_timeout=2)
        rows = sql(connection, "SHOW ENGINE MYDB STATUS")
        if rows:
            record["engine_status"] = str(rows[0][-1])
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(
                "SELECT ID, USER, HOST, DB, COMMAND, TIME, STATE, INFO "
                "FROM information_schema.PROCESSLIST"
            )
            record["processlist"] = cursor.fetchall()
        rows = sql(connection, "SELECT METRIC_NAME, METRIC_VALUE FROM information_schema.MYDB_ENGINE_STATUS")
        record["metrics"] = {str(name): value for name, value in rows}
    except Exception as exc:
        record["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, default=str) + "\n")


class Worker:
    """Spawns a loader child so the parent can SIGKILL it mid-stream."""

    def __init__(self, script: Path, server: "Server", start_id: int, end_id: int, args):
        self.script = script
        self.server = server
        self.port = server.port
        self.start_id = start_id
        self.end_id = end_id
        self.args = args
        self.process: subprocess.Popen[str] | None = None
        self.last_id = start_id - 1
        self.last_progress = time.monotonic()
        self.batches: list[dict[str, object]] = []
        self.reader: threading.Thread | None = None
        self.lock = threading.Lock()
        self.stderr_path = args.workdir / f"worker-{start_id}-{end_id}.err"

    def start(self) -> None:
        command = [
            sys.executable, str(self.script), "--worker",
            "--port", str(self.port),
            "--start-id", str(self.start_id),
            "--end-id", str(self.end_id),
            "--max-stmt-length", str(self.args.max_stmt_length),
            "--insert-ignore" if self.args.insert_ignore else "--no-insert-ignore",
            "--unique-s0" if self.args.unique_s0 else "--no-unique-s0",
        ]
        stderr = self.stderr_path.open("w", encoding="utf-8")
        self.process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=stderr,
            text=True,
            bufsize=1,
        )
        self.reader = threading.Thread(target=self._read, args=(stderr,), daemon=True)
        self.reader.start()

    def _read(self, stderr) -> None:
        assert self.process is not None
        try:
            for line in self.process.stdout:
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if event.get("event") == "batch":
                    with self.lock:
                        self.last_id = int(event["id_end"])
                        self.last_progress = time.monotonic()
                        self.batches.append(event)
                    with (self.args.workdir / "batches.jsonl").open("a", encoding="utf-8") as handle:
                        handle.write(json.dumps(event, separators=(",", ":")) + "\n")
        finally:
            try:
                self.process.wait(timeout=5)
            except Exception:
                pass
            stderr.close()

    def wait(self, stall_threshold_s: float, log_stalls: bool = True) -> dict[str, object]:
        assert self.process is not None
        stall_started: float | None = None
        while self.process.poll() is None:
            time.sleep(0.2)
            with self.lock:
                silent = time.monotonic() - self.last_progress
                last_id = self.last_id
            if log_stalls and silent >= stall_threshold_s and stall_started is None:
                stall_started = time.monotonic()
                log(
                    f"STALL: no batch progress for {silent:.1f}s "
                    f"(last id {last_id}, worker pid {self.process.pid})"
                )
                threading.Thread(
                    target=capture_stall,
                    args=(self.server, self.args.workdir / "stalls.jsonl", last_id),
                    daemon=True,
                ).start()
            elif log_stalls and stall_started is not None and silent < 1.0:
                log(f"STALL ended after {time.monotonic() - stall_started:.1f}s at id {last_id}")
                stall_started = None
        return {"returncode": self.process.returncode}

    def current_id(self) -> int:
        with self.lock:
            return self.last_id

    def kill9(self) -> None:
        if self.process is not None and self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=10)
            log(f"client worker {self.process.pid} SIGKILLed at id {self.current_id()}")

    def wait_for_rows(self, rows: int, timeout: float = 600) -> None:
        target = self.start_id + rows
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline and self.current_id() < target:
            if self.process is not None and self.process.poll() is not None:
                return
            time.sleep(0.1)


def worker_main(args) -> int:
    connection = connect(args.port)
    placeholders = ",".join(["%s"] * COLUMN_COUNT)
    statement = (
        "INSERT IGNORE INTO staging VALUES" if args.insert_ignore
        else "INSERT INTO staging VALUES"
    )
    statement = f"{statement} ({placeholders})"
    current = args.start_id
    chunk_start = current
    while current <= args.end_id:
        # The public report opens a fresh connection every 25,000 rows.
        if current - chunk_start >= CHUNK_ROWS:
            connection.close()
            connection = connect(args.port)
            chunk_start = current
        end = min(current + BATCH_ROWS - 1, args.end_id)
        values: list[object] = []
        for row_id in range(current, end + 1):
            values.extend(row_values(row_id, args.unique_s0))
        started = time.monotonic()
        try:
            with connection.cursor() as cursor:
                cursor.max_stmt_length = args.max_stmt_length
                cursor.executemany(statement, [values[i:i + COLUMN_COUNT] for i in range(0, len(values), COLUMN_COUNT)])
        except pymysql.err.IntegrityError as exc:
            if exc.args and exc.args[0] == 1062:
                # A disconnected client can leave the server finishing the
                # previous batch. Resume after whatever actually committed.
                rows = sql(connection, "SELECT COALESCE(MAX(id),0) FROM staging")
                resume = int(rows[0][0]) + 1
                current = max(resume, current + 1)
                chunk_start = min(chunk_start, current)
                print(json.dumps({"event": "skip_duplicate", "id": current}), flush=True)
                continue
            raise
        except pymysql.err.MySQLError as exc:
            if exc.args and exc.args[0] in (2006, 2013):  # server went away (expected on crash)
                print(json.dumps({"event": "error", "id": current, "error": str(exc)}), flush=True)
                return 3
            raise
        elapsed = time.monotonic() - started
        print(
            json.dumps(
                {
                    "event": "batch",
                    "id_start": current,
                    "id_end": end,
                    "rows": end - current + 1,
                    "elapsed_s": round(elapsed, 4),
                    "ts_ms": now_unix_ms(),
                }
            ),
            flush=True,
        )
        current = end + 1
    connection.close()
    return 0


def run_worker(script: Path, server: Server, start_id: int, new_rows: int, args) -> Worker:
    worker = Worker(script, server, start_id, start_id + new_rows - 1, args)
    worker.start()
    return worker


def load_segment(script: Path, server: Server, new_rows: int, args) -> dict[str, object]:
    start_id = max_id(server.port) + 1
    log(f"loading {new_rows} rows from id {start_id}")
    worker = run_worker(script, server, start_id, new_rows, args)
    result = worker.wait(args.stall_threshold)
    return {**result, "batches": worker.batches, "start_id": start_id}


def churn_cycle(script: Path, server: Server, cycle: int, args) -> dict[str, object]:
    events: list[dict[str, object]] = []
    target = max_id(server.port) + 1 + args.churn_rows - 1
    current = max_id(server.port) + 1
    log(f"churn cycle {cycle}: ids {current}..{target}")
    # Phase 1: load until the client is killed mid-stream.
    worker = run_worker(script, server, current, args.churn_rows, args)
    crash_rows = max(1, args.churn_rows * 40 // 100)
    worker.wait_for_rows(crash_rows)
    killed_at = worker.current_id()
    worker.kill9()
    events.append({"event": "client_crash", "id": killed_at})
    wait_for_quiet(server.port)
    current = max_id(server.port) + 1
    log(f"resuming after client crash at id {killed_at}; next {current}")

    # Phase 2: load, then kill -9 the server mid-stream.
    if target >= current:
        worker = run_worker(script, server, current, target - current + 1, args)
        server_crash_rows = max(1, (target - current + 1) * 70 // 100)
        worker.wait_for_rows(server_crash_rows)
        killed_at = worker.current_id()
        server.kill9()
        worker.kill9()
        events.append({"event": "server_crash", "id": killed_at})
        server.start()
        wait_for_quiet(server.port)
        current = max_id(server.port) + 1
        log(f"recovered after server crash; next id {current}")

    # Phase 3: finish the cycle.
    if target >= current:
        worker = run_worker(script, server, current, target - current + 1, args)
        result = worker.wait(args.stall_threshold)
        events.append({"event": "finish", "start_id": current, **result, "batches": worker.batches})

    # Phase 4: mimic an operator killing stale sessions.
    connection = connect(server.port)
    try:
        stale = sql(
            connection,
            "SELECT ID FROM information_schema.PROCESSLIST "
            "WHERE COMMAND = 'Sleep' AND TIME > 10 AND ID <> CONNECTION_ID()",
        )
        for (session_id,) in stale:
            try:
                sql(connection, f"KILL {int(session_id)}")
                events.append({"event": "kill_session", "id": int(session_id)})
            except pymysql.MySQLError:
                pass
    finally:
        connection.close()
    return {"cycle": cycle, "events": events, "max_id": max_id(server.port)}


def run_backup(server: Server, workdir: Path) -> dict[str, object]:
    repository = workdir / "backup-repository"
    repository.mkdir(parents=True, exist_ok=True)
    connection = connect(server.port, read_timeout=600)
    started = time.monotonic()
    job_id = None
    try:
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(f"BACKUP DATABASE TO 'file://{repository}'")
            for row in cursor.fetchall():
                for key, value in row.items():
                    if "job" in str(key).lower() and value is not None:
                        job_id = value
        if job_id is None:
            raise RuntimeError("BACKUP DATABASE returned no Job_ID")
        log(f"backup job {job_id} started")
        while True:
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute("SHOW MYDB BACKUP JOBS")
                jobs = cursor.fetchall()
            state = ""
            for job in jobs:
                ids = [str(value) for key, value in job.items() if "job" in str(key).lower()]
                if str(job_id) in ids or not ids:
                    state = str(job.get("State") or job.get("STATE") or "").upper()
                    break
            if state == "COMPLETED":
                break
            if state in ("FAILED", "CANCELLED"):
                raise RuntimeError(f"backup job {job_id} ended {state}: {jobs}")
            if time.monotonic() - started > 900:
                raise TimeoutError("backup did not finish within 900s")
            time.sleep(1)
        return {"job_id": job_id, "elapsed_s": round(time.monotonic() - started, 2)}
    finally:
        connection.close()


def summarize_batches(batches: list[dict[str, object]]) -> dict[str, object]:
    elapsed = [float(batch["elapsed_s"]) for batch in batches]
    slow = sorted((batch for batch in batches if float(batch["elapsed_s"]) >= 1.0),
                  key=lambda batch: float(batch["elapsed_s"]), reverse=True)[:20]
    return {
        "batches": len(batches),
        "rows": sum(int(batch["rows"]) for batch in batches),
        "max_batch_s": max(elapsed) if elapsed else 0,
        "slow_batches": slow,
    }


def orchestrator(args) -> int:
    workdir = Path(args.workdir)
    if args.reset and workdir.exists():
        import shutil
        shutil.rmtree(workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    args.workdir = workdir
    script = Path(__file__).resolve()
    server = Server(Path(args.binary).resolve(), workdir, args)
    summary: dict[str, object] = {"args": vars(args) | {"workdir": str(workdir)}}
    try:
        server.start()
        ensure_table(server.port)
        sampler = Sampler(server, workdir / "samples.jsonl")
        sampler.start()
        started = time.monotonic()

        baseline = load_segment(script, server, args.baseline_rows, args)
        summary["baseline"] = {"result": baseline["returncode"], "max_id": max_id(server.port),
                               **summarize_batches(baseline["batches"])}
        log(f"baseline done: {summary['baseline']}")

        churn = []
        for cycle in range(1, args.cycles + 1):
            churn.append(churn_cycle(script, server, cycle, args))
        summary["churn"] = churn

        if args.backup_at_end:
            summary["backup"] = run_backup(server, workdir)
            log(f"backup: {summary['backup']}")

        sustained = load_segment(script, server, args.sustained_rows, args)
        summary["sustained"] = {"result": sustained["returncode"], "max_id": max_id(server.port),
                                **summarize_batches(sustained["batches"])}
        log(f"sustained done: {summary['sustained']}")

        summary["wall_s"] = round(time.monotonic() - started, 1)
        summary["samples"] = sampler.samples
        summary["slow_probes"] = sampler.slow_probes
        sampler.stop()
        server.stop()
    except Exception:
        summary["error"] = traceback.format_exc()
        raise
    finally:
        try:
            server.cleanup()
        finally:
            (workdir / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
            log(f"summary written to {workdir / 'summary.json'}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--worker", action="store_true", help="internal: run a loader child")
    parser.add_argument("--port", type=int)
    parser.add_argument("--start-id", type=int)
    parser.add_argument("--end-id", type=int)
    parser.add_argument("--binary", default=str(ROOT / "target/release/mydb"))
    parser.add_argument("--workdir", default="/tmp/mydb-issue7")
    parser.add_argument("--reset", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--baseline-rows", type=int, default=300_000)
    parser.add_argument("--churn-rows", type=int, default=150_000)
    parser.add_argument("--cycles", type=int, default=4)
    parser.add_argument("--sustained-rows", type=int, default=900_000)
    parser.add_argument("--stall-threshold", type=float, default=5.0)
    parser.add_argument("--max-stmt-length", type=int, default=1_024_000)
    parser.add_argument("--durability", default="strict")
    parser.add_argument("--memory-target", default="7G")
    parser.add_argument("--wal-segment-bytes", type=int, default=256 * 1024 * 1024)
    parser.add_argument("--checkpoint-interval-ms", type=int, default=60_000)
    parser.add_argument("--checkpoint-wal-bytes", type=int, default=4 * 1024 * 1024 * 1024)
    parser.add_argument("--backup-at-end", action="store_true")
    parser.add_argument("--insert-ignore", action=argparse.BooleanOptionalAction, default=False,
                        help="use INSERT IGNORE (slower; for duplicate-tolerant experiments)")
    parser.add_argument("--unique-s0", action=argparse.BooleanOptionalAction, default=True,
                        help="make the indexed s0 column unique per row (default) or reuse 1024 values")
    parser.add_argument("--server-env", action="append", default=[], metavar="KEY=VALUE",
                        help="extra MYDB_* environment for the server (repeatable)")
    parser.add_argument("--cgroup", action="store_true", help="run the server in a 15GiB/4CPU cgroup")
    parser.add_argument("--strace", action="store_true",
                        help="trace slow I/O syscalls (fsync/fallocate/rename) around the server")
    parser.add_argument("--cgroup-memory-bytes", type=int, default=15 * 1024**3)
    parser.add_argument("--cgroup-cpu-quota", type=int, default=400_000)
    parser.add_argument("--cgroup-io-max", default="",
                        help="cgroup v2 io.max value, e.g. '253:0 wbps=200000000 rbps=500000000'")
    parser.add_argument("--co-tenant-cpu", type=int, default=0,
                        help="spin this many co-tenant processes inside the server cgroup")
    parser.add_argument("--co-tenant-mem-mb", type=int, default=0,
                        help="hold this many MiB inside the server cgroup")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.worker:
        if args.port is None or args.start_id is None or args.end_id is None:
            raise SystemExit("--worker requires --port --start-id --end-id")
        return worker_main(args)
    if not Path(args.binary).exists():
        raise SystemExit(f"server binary not found: {args.binary}")
    return orchestrator(args)


if __name__ == "__main__":
    raise SystemExit(main())
