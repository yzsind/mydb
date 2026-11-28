import argparse
from collections import Counter
from contextlib import closing
import json
from pathlib import Path
import sys
import time

from graph_io import load, read_commits
from prepare import kind
from ranked import NeighborhoodTopK


class IC9Model:
    def __init__(self):
        self.ranking = NeighborhoodTopK()
        self.names = {}
        self.when = {}
        self.initial = True

    def add_node(self, label, ident, props):
        if label == "Person":
            self.names[ident] = (props["firstName"], props["lastName"])
        if label in ("Post", "Comment"):
            self.when[ident] = (
                props["creationDate"],
                props.get("content") or props.get("imageFile"),
            )

    def add_edge(self, label, at, a, bt, b, props):
        if label == "KNOWS":
            self.ranking.add_link(a, b)
        if label == "HAS_CREATOR":
            date, body = self.when.pop(a)
            if self.initial:
                self.ranking.messages[a] = body
                self.ranking.by_author[b].append((date, -a))
            else:
                self.ranking.add_message(b, a, date, body)

    def finish_load(self):
        if self.when:
            raise ValueError("Unassigned initial messages")
        for by_author in self.ranking.by_author.values():
            by_author.sort()
        self.initial = False

    def finish_commit(self):
        pass

    def query(self, p):
        result = self.ranking.read((p["personIdQ9"], p["maxDate"], p["limit"]), True)
        return [
            dict(
                personId=author,
                personFirstName=self.names[author][0],
                personLastName=self.names[author][1],
                messageId=ident,
                messageContent=self.ranking.messages[ident],
                messageCreationDate=-neg,
            )
            for neg, ident, author in result
        ]


def run(path, plan_path, validation, follow_capture):
    start = time.monotonic()
    model = IC9Model()
    catalog = load(path, model, {"HAS_CREATOR", "KNOWS"})
    loaded = time.monotonic()
    print(f"mydb export loaded in {loaded - start:.3f}s", file=sys.stderr, flush=True)
    verified = Counter()
    diff = []
    writes = 0
    with (
        closing(read_commits(path, follow=follow_capture)) as commits,
        plan_path.open() as plans,
    ):
        for ordinal, line in enumerate(validation.open(encoding="utf-8"), 1):
            raw, oracle = line.rstrip("\n").split("|", 1)
            params = json.loads(raw)
            op = kind(params)
            if op is not None:
                instruction = json.loads(next(plans))
                event = next(commits)
                assert (
                    ordinal == instruction["ordinal"] == event["ordinal"]
                    and op == instruction["op"]
                )
                writes += catalog.apply(instruction, event, model)
                verified[f"U{op}"] += 1
                continue
            if "personIdQ9" not in params:
                continue
            actual = model.query(params)
            expected = json.loads(oracle)
            verified["IC9"] += 1
            if actual != expected:
                diff.append((ordinal, actual, expected))
                if len(diff) == 4:
                    break
        if not diff and (
            next(commits, None) is not None or next(plans, None) is not None
        ):
            raise ValueError("Unexpected trailing graph changes")
    finish = time.monotonic()
    state = dict(
        counts=verified,
        logical_rows_verified=writes,
        scn=catalog.scn,
        snapshot_load_seconds=loaded - start,
        replay_seconds=finish - loaded,
        total_seconds=finish - start,
        mismatches=diff,
        ranking=model.ranking.stats,
    )
    print(json.dumps(state, ensure_ascii=False, indent=2))
    return (
        0
        if not diff
        and verified["IC9"] == 6818
        and sum(n for op, n in verified.items() if op.startswith("U")) == 7041
        else 1
    )


if __name__ == "__main__":
    cli = argparse.ArgumentParser()
    cli.add_argument("capture", type=Path)
    cli.add_argument("plan", type=Path)
    cli.add_argument("--validation", type=Path, required=True)
    cli.add_argument("--follow-capture", action="store_true", help=argparse.SUPPRESS)
    a = cli.parse_args()
    sys.exit(run(a.capture, a.plan, a.validation, a.follow_capture))
