"""Run an incremental validation from a fresh local mydb data clone."""

import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

from graph_io import verify_receipts
from prepare import prepare

TOOLS = Path(__file__).resolve().parent
REPO = TOOLS.parents[1]


def build_capture(output, native=False):
    command = [
        "cargo",
        "build",
        "--locked",
        "--release",
        "--lib",
        "--message-format=json",
    ]
    events = subprocess.run(
        command, cwd=REPO, check=True, stdout=subprocess.PIPE, text=True
    ).stdout
    libraries = {}
    for line in events.splitlines():
        event = json.loads(line)
        if event.get("reason") != "compiler-artifact":
            continue
        target = event["target"]["name"]
        if target in ("mydb", "serde_json"):
            paths = [Path(p) for p in event["filenames"] if p.endswith(".rlib")]
            if len(paths) == 1:
                libraries[target] = paths[0]
    if set(libraries) != {"mydb", "serde_json"}:
        raise RuntimeError(
            "Could not resolve mydb and serde_json release library paths"
        )
    options = []
    if native:
        options = ["--crate-type", "cdylib", "--cfg", "native_snapshot"]
        if sys.platform == "darwin":
            options += ["-C", "link-arg=-Wl,-undefined,dynamic_lookup"]
    subprocess.run(
        [
            "rustc",
            "--edition=2021",
            "-C",
            "opt-level=3",
            *options,
            "-L",
            f"dependency={libraries['serde_json'].parent}",
            *[
                arg
                for crate, path in libraries.items()
                for arg in ("--extern", f"{crate}={path}")
            ],
            str(TOOLS / "capture.rs"),
            "-o",
            str(output),
        ],
        cwd=REPO,
        check=True,
    )


def clone(source, dest):
    if sys.platform == "darwin":
        subprocess.run(["/bin/cp", "-cR", str(source), str(dest)], check=True)
    else:
        shutil.copytree(source, dest)


@contextmanager
def launch(command, name, work, env=None):
    started = time.monotonic()
    with (
        (work / f"{name}.json").open("w") as out,
        (work / f"{name}.log").open("w") as log,
    ):
        process = subprocess.Popen(command, stdout=out, stderr=log, env=env)
        try:
            yield process, started
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


def check_exit(code, name, work):
    if code != 0:
        raise SystemExit(
            f"{name} exited with {code}; see {work / f'{name}.log'} and {work / f'{name}.json'}"
        )


def reuse_loader(finished, work):
    snapshot = json.loads((finished / "actual" / "snapshot.json").read_text())
    if snapshot.get("format") != "native-v1":
        return None
    from native import rebuild

    previous = json.loads((finished / "summary.json").read_text())
    library = Path(previous["native_library"]).resolve(strict=True)
    source = Path(previous["snapshot_source"]).resolve(strict=True)
    data = work / "graph-data"

    def load(model):
        clone(source, data)
        return rebuild(library, data, snapshot, model)

    return load


def capture_and_verify(capture_command, verify_command, work, snapshot_manifest, env):
    with launch(capture_command, "capture", work, env) as (capture, capture_start):
        while not snapshot_manifest.exists():
            code = capture.poll()
            if code is not None:
                check_exit(code, "capture", work)
                if not snapshot_manifest.exists():
                    raise ValueError("Capture exited without publishing a snapshot")
            time.sleep(0.02)
        with launch(verify_command, "verification", work) as (verify, verify_start):
            finished = {}
            processes = (
                ("capture", capture, capture_start),
                ("verification", verify, verify_start),
            )
            while len(finished) < len(processes):
                for name, process, started in processes:
                    if name in finished:
                        continue
                    code = process.poll()
                    if code is not None:
                        check_exit(code, name, work)
                        ended = time.monotonic()
                        finished[name] = ended
                        print(
                            f"{name} finished in {ended - started:.2f}s with exit {code}",
                            flush=True,
                        )
                if len(finished) < len(processes):
                    time.sleep(0.02)
    return (
        finished["capture"] - capture_start,
        finished["verification"] - verify_start,
        max(0, min(finished.values()) - verify_start),
    )


def run(args):
    work = args.work_dir.resolve()
    scratch = Path("/tmp/mydb").resolve()
    if work.parent != scratch and scratch not in work.parents:
        raise ValueError("Use a fresh work directory under /tmp/mydb")
    work.mkdir(parents=True)
    if args.queries_only:
        from prepared import queries_only

        finished = args.queries_only.resolve(strict=True)
        return queries_only(
            work,
            finished,
            args.validation.resolve(strict=True),
            args.workers,
            args.repeat,
            reuse_loader(finished, work),
        )
    native = args.mode == "all" and not args.capture_binary
    if args.native_library:
        binary = args.native_library.resolve(strict=True)
    elif args.capture_binary:
        binary = args.capture_binary.resolve(strict=True)
    else:
        suffix = ".dylib" if sys.platform == "darwin" else ".so"
        binary = work / (f"native-snapshot{suffix}" if native else "capture")
        build_capture(binary, native=native)
    source = args.snapshot.resolve(strict=True)
    validation = args.validation.resolve(strict=True)
    start = time.monotonic()
    graph = work / "graph-data"
    clone(source, graph)
    copied = time.monotonic()
    plan = work / "writes.jsonl"
    with plan.open("w", encoding="utf-8") as output:
        if prepare(validation, output) != 7041:
            raise ValueError("Expected 7,041 Interactive v1 SF=1 updates")
    actual = work / "actual"
    env = dict(os.environ, MYDB_DURABILITY="strict", MYDB_REDO_ENABLED="ON")
    command = [
        sys.executable,
        str(TOOLS / ("evaluate_all.py" if args.mode == "all" else "evaluate.py")),
        str(actual),
        str(plan),
        "--validation",
        str(validation),
        "--follow-capture",
    ]
    if args.mode == "all":
        command += [
            "--mismatches",
            str(work / "mismatches.jsonl"),
            "--receipts",
            str(work / "receipts.tsv"),
            "--workers",
            str(args.workers),
        ]
    if native:
        command += ["--native-library", str(binary), "--native-data", str(graph)]
        with launch(command, "verification", work, env) as (process, started):
            check_exit(process.wait(), "verification", work)
            evaluation_seconds = time.monotonic() - started
    else:
        capture_seconds, evaluation_seconds, overlap_seconds = capture_and_verify(
            [str(binary), str(graph), str(actual), str(plan)],
            command,
            work,
            actual / "snapshot.json",
            env,
        )
    verification = json.loads((work / "verification.json").read_text())
    snapshot = json.loads((actual / "snapshot.json").read_text())
    captured = json.loads((actual / "capture.json").read_text())
    if native:
        capture_seconds = captured["total_seconds"]
        overlap_seconds = min(
            captured["write_capture_seconds"],
            snapshot["assemble_seconds"] + snapshot["materialize_seconds"],
        )
    if args.mode == "all":
        verify_receipts(
            work / "receipts.tsv",
            actual / "commits.jsonl",
            snapshot["snapshot_scn"],
            verification,
        )
    elapsed = time.monotonic() - start
    summary = dict(
        mode=args.mode,
        snapshot_backend="native" if native else "export",
        clone_seconds=copied - start,
        capture_wall_seconds=capture_seconds,
        evaluation_wall_seconds=evaluation_seconds,
        pipeline_overlap_seconds=overlap_seconds,
        total_seconds=elapsed,
        deadline_seconds=900,
        within_deadline=elapsed <= 900,
        comparison_target_seconds=5.34,
        within_comparison_target=elapsed <= 5.34,
        operations=verification["counts"],
        verified_logical_graph_changes=verification["logical_rows_verified"],
        snapshot=snapshot,
        capture=captured,
        final_scn=verification["scn"],
    )
    if native:
        summary["native_library"] = str(binary)
        summary["snapshot_source"] = str(source)
    (work / "summary.json").write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(summary, indent=2))
    return 0 if elapsed <= 900 else 3


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    source = cli.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--snapshot", type=Path, help="Stopped mydb data directory containing test.snb"
    )
    source.add_argument(
        "--queries-only",
        type=Path,
        help="Completed full-validation work directory to prepare and replay",
    )
    cli.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Workers for full verification or prepared replay",
    )
    cli.add_argument(
        "--repeat",
        type=int,
        default=3,
        help="Replays sharing the same prepared indexes",
    )
    cli.add_argument(
        "--validation", type=Path, required=True, help="Official SF=1 validation file"
    )
    cli.add_argument("--work-dir", type=Path, required=True)
    backend = cli.add_mutually_exclusive_group()
    backend.add_argument(
        "--capture-binary", type=Path, help="Use the file-export capture tool"
    )
    backend.add_argument(
        "--native-library",
        type=Path,
        help="Use a previously built Rust snapshot library",
    )
    cli.add_argument("--mode", choices=("ic9", "all"), default="all")
    args = cli.parse_args()
    if args.workers < 1:
        cli.error("--workers must be positive")
    if args.native_library and (args.mode != "all" or args.queries_only):
        cli.error("--native-library requires a full run from a stopped snapshot")
    if args.queries_only and (args.mode != "all" or args.capture_binary):
        cli.error(
            "--queries-only requires a completed full capture and does not start a capture binary"
        )
    sys.exit(run(args))


if __name__ == "__main__":
    main()
