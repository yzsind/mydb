"""Read graph data exported by mydb and validate committed logical deltas."""

from collections import Counter, defaultdict
from dataclasses import dataclass
import gc
from itertools import chain
import json
import mmap
from struct import Struct
import sys
import time

NODE_TYPES = (
    "Person",
    "Place",
    "Organisation",
    "Tag",
    "TagClass",
    "Forum",
    "Post",
    "Comment",
)


def read_commits(directory, follow=False):
    ledger = directory / "commits.jsonl"
    complete = directory / "capture.json"
    while follow and not ledger.exists() and not complete.exists():
        time.sleep(0.02)
    with ledger.open("rb") as stream:
        pending = bytearray()
        while True:
            part = stream.readline()
            if not part:
                if follow and not complete.exists():
                    time.sleep(0.02)
                    continue
                # The last append can race the initial EOF read.
                part = stream.readline() if follow else b""
                if not part:
                    if pending:
                        raise ValueError("Incomplete committed graph record")
                    return
            pending.extend(part)
            if part.endswith(b"\n"):
                yield json.loads(pending)
                pending.clear()


@dataclass(frozen=True, slots=True)
class GraphDelta:
    ordinal: int
    scn: int
    durable_lsn: int
    nodes: tuple
    edges: tuple
    actual: Counter
    expected: Counter

    def verify(self):
        if self.actual != self.expected:
            raise ValueError(f"Committed graph delta mismatch at {self.ordinal}")

    def feed(self, model):
        for args in self.nodes:
            model.add_node(*args)
        for args in self.edges:
            model.add_edge(*args)
        model.finish_commit()


@dataclass
class Catalog:
    keys: dict
    scn: int
    max_element: int
    durable_lsn: int = 0
    initial_keys: list | None = None

    def resolve(self, ident):
        if self.initial_keys is not None and 0 <= ident < len(self.initial_keys):
            key = self.initial_keys[ident]
            if key is None:
                raise ValueError(f"Missing graph node {ident}")
            return key
        return self.keys[ident]

    def apply(self, plan, committed, model):
        delta = self.decode(plan, committed)
        delta.feed(model)
        return len(delta.nodes) + len(delta.edges)

    def decode(self, plan, committed):
        if (
            committed["ordinal"] != plan["ordinal"]
            or committed["scn"] <= self.scn
            or committed["durable_lsn"] <= self.durable_lsn
        ):
            raise ValueError("Out-of-order committed graph delta")
        changes = committed["rows"]
        new_ids = [r[2] if r[0] == "label" else r[1] for r in changes]
        if min(new_ids) <= self.max_element or len(new_ids) != len(set(new_ids)):
            raise ValueError("Graph update did not append distinct new elements")
        pending_keys = {}
        labels = defaultdict(list)
        for row in changes:
            if row[0] == "label":
                labels[row[3]].append(row[4])
        actual = []
        pending_nodes = []
        pending_edges = []
        for row in changes:
            if row[0] != "node":
                continue
            _, ident, props = row
            names = sorted(labels.pop(ident, ()))
            types = [t for t in names if t in NODE_TYPES]
            if len(types) != 1:
                raise ValueError("Cannot identify appended node type")
            pending_keys[ident] = (types[0], props["id"])
            actual.append(("node", names, props))
            pending_nodes.append((types[0], props["id"], props))

        def resolve(ident):
            return pending_keys[ident] if ident in pending_keys else self.resolve(ident)

        for row in changes:
            if row[0] != "edge":
                continue
            _, ident, role, src, dst, props = row
            names = labels.pop(ident, ())
            if role != 1 or len(names) != 1:
                raise ValueError("Expected a directed edge with one label")
            name = names[0]
            a, b = resolve(src), resolve(dst)
            actual.append(("edge", name, *a, *b, props))
            pending_edges.append((name, *a, *b, props))

        def canonical(rows):
            return Counter(
                json.dumps(
                    row, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                )
                for row in rows
            )

        if labels:
            raise ValueError(
                f"Committed graph delta contains extra labels at {plan['ordinal']}"
            )
        delta = GraphDelta(
            committed["ordinal"],
            committed["scn"],
            committed["durable_lsn"],
            tuple(pending_nodes),
            tuple(pending_edges),
            canonical(actual),
            canonical(plan["expected"]),
        )
        delta.verify()
        self.keys.update(pending_keys)
        self.max_element = max(new_ids)
        self.scn = delta.scn
        self.durable_lsn = delta.durable_lsn
        return delta


def verify_receipts(receipt_path, commit_path, initial_scn, verification):
    counts = Counter()
    change_rows = 0
    scn = initial_scn
    with (
        receipt_path.open(encoding="utf-8") as receipts,
        commit_path.open(encoding="utf-8") as commits,
    ):
        if next(receipts, "") != "ordinal\tscn\toperation\tstatus\trows\n":
            raise ValueError("Invalid receipt header")
        for expected, line in enumerate(receipts, 1):
            ordinal, version, operation, status, rows = line.rstrip("\n").split("\t")
            if int(ordinal) != expected or status != "PASS":
                raise ValueError(f"Missing or unsuccessful receipt at {expected}")
            if operation.startswith("U"):
                event = json.loads(next(commits))
                if event["ordinal"] != expected or event["scn"] <= scn:
                    raise ValueError(f"Out-of-order commit for receipt {expected}")
                scn = event["scn"]
                change_rows += int(rows)
            if int(version) != scn:
                raise ValueError(f"Invalid graph version for receipt {expected}")
            counts[operation] += 1
        if next(commits, None) is not None:
            raise ValueError("Unconsumed committed changes")
    if (
        counts != verification["counts"]
        or sum(counts.values()) != 138747
        or change_rows != verification["logical_rows_verified"]
        or scn != verification["scn"]
    ):
        raise ValueError("The operation receipts diverge from the validation summary")


def binary_labels(path, filename, size):
    data = (path / filename).read_bytes()
    if len(data) != size:
        raise ValueError(f"Invalid graph label map extent: {filename}")
    return data


def node_label_lookup(path, info, type_ids, extra_ids):
    size = info["max_element_id"] + 1
    if "label_dictionary" in info:
        first = binary_labels(path, "node-labels-first.bin", size)
        second = binary_labels(path, "node-labels-second.bin", size)
        if 2 * size - first.count(b"\0") - second.count(b"\0") != info["role_rows"][3]:
            raise ValueError("Node label count does not match the snapshot manifest")
        codes = [(0, 0)]
        for label in info["label_dictionary"][1:]:
            raw = label.encode()
            primary, extra = type_ids.get(raw, 0), extra_ids.get(raw, 0)
            codes.append((primary, extra) if primary or extra else None)

        def lookup(ident):
            a = codes[first[ident]]
            b = codes[second[ident]]
            if a is None or b is None or (a[0] and b[0]) or (a[1] and b[1]):
                raise ValueError(f"Invalid labels for node {ident}")
            return a[0] or b[0], a[1] or b[1]

        return lookup

    primary = bytearray(size)
    secondary = bytearray(size)
    with (path / "node-labels.tsv").open("rb") as stream:
        for line in stream:
            ident, label = line.rstrip(b"\n").split(b"\t")
            owner = int(ident)
            n = type_ids.get(label)
            if n:
                if primary[owner]:
                    raise ValueError("Conflicting primary node labels")
                primary[owner] = n
            else:
                if secondary[owner]:
                    raise ValueError("Conflicting secondary node labels")
                secondary[owner] = extra_ids[label]
    return lambda ident: (primary[ident], secondary[ident])


def edge_label_lookup(path, info):
    size = info["max_element_id"] + 1
    if "label_dictionary" in info:
        codes = binary_labels(path, "edge-labels.bin", size)
        if size - codes.count(b"\0") != info["role_rows"][4]:
            raise ValueError("Edge label count does not match the snapshot manifest")
        return codes, info["label_dictionary"]
    codes = bytearray(size)
    ids = {}
    names = [None]
    with (path / "edge-labels.tsv").open("rb") as stream:
        for line in stream:
            ident, label = line.rstrip(b"\n").split(b"\t")
            code = ids.get(label)
            if code is None:
                code = len(names)
                ids[label] = code
                names.append(label.decode())
            owner = int(ident)
            if codes[owner]:
                raise ValueError("Multiple social links have the same edge identity")
            codes[owner] = code
    return codes, names


EDGE_RECORD = Struct("<IIII")
EMPTY_PROPERTIES = {}
decode_json = json.JSONDecoder().decode


def load_binary_edges(path, shards, edge_labels, labels, keys, add_edge):
    count = 0
    for shard in range(shards):
        with (
            (path / f"edges-{shard}.bin").open("rb") as stream,
            (path / f"edge-props-{shard}.jsonl").open(encoding="utf-8") as props_file,
        ):
            properties = chain.from_iterable(map(decode_json, props_file))
            if stream.seek(0, 2):
                with mmap.mmap(stream.fileno(), 0, access=mmap.ACCESS_READ) as records:
                    count += len(records) // EDGE_RECORD.size
                    for ident, a, b, flags in EDGE_RECORD.iter_unpack(records):
                        code = edge_labels[ident]
                        if not code or flags >> 1 != 1:
                            raise ValueError(
                                "Missing relation label or unsupported direction"
                            )
                        props = EMPTY_PROPERTIES
                        if flags & 1:
                            props = next(properties, None)
                            if not isinstance(props, dict):
                                raise ValueError("Missing or invalid edge properties")
                        label = labels[code]
                        if label is not None:
                            at, source = keys[a]
                            bt, target = keys[b]
                            add_edge(label, at, source, bt, target, props)
            if next(properties, EMPTY_PROPERTIES) is not EMPTY_PROPERTIES:
                raise ValueError("Edge properties do not match the snapshot headers")
    return count


def read_nodes(path, shards):
    if shards:
        for shard in range(shards):
            with (path / f"nodes-{shard}.jsonl").open(encoding="utf-8") as stream:
                yield from chain.from_iterable(map(decode_json, stream))
    else:
        with (path / "nodes.tsv").open(encoding="utf-8") as stream:
            for line in stream:
                ident, props = line.split("\t", 1)
                yield int(ident), decode_json(props)


def load(path, model, wanted_edges=None):
    gc_enabled = gc.isenabled()
    gc.disable()
    try:
        return load_snapshot(path, model, wanted_edges)
    finally:
        if gc_enabled:
            gc.enable()


def load_snapshot(path, model, wanted_edges):
    started = time.monotonic()
    info = json.loads((path / "snapshot.json").read_text())
    max_id = info["max_element_id"]
    if max_id > 100_000_000:
        raise ValueError("Element IDs exceed the dense validation budget")
    type_ids = {name.encode(): number for number, name in enumerate(NODE_TYPES, 1)}
    extras = (b"Message", b"City", b"Country", b"Continent", b"Company", b"University")
    extra_ids = {name: number for number, name in enumerate(extras, 1)}
    node_labels = node_label_lookup(path, info, type_ids, extra_ids)
    node_count = info["role_rows"][0]
    max_node = info.get("max_node_id", max_id)
    if not 0 <= max_node <= max_id:
        raise ValueError("Invalid node extent in the snapshot manifest")
    dense = max_node + 1 <= 8 * node_count
    keys = [None] * (max_node + 1) if dense else {}
    get_key = keys.__getitem__ if dense else keys.get
    add_node = model.add_node
    loaded_nodes = 0
    format_name = info.get("format")
    if format_name not in (None, "sharded-v1"):
        raise ValueError(f"Unsupported graph snapshot format {format_name}")
    shards = info["shards"] if format_name else 0
    if format_name and shards < 1:
        raise ValueError("Invalid shard count in the snapshot manifest")
    for ident, values in read_nodes(path, shards):
        if not 0 <= ident <= max_node:
            raise ValueError(f"Invalid node identity {ident}")
        label, secondary_label = node_labels(ident)
        if label == 0:
            raise ValueError(f"No primary node label for {ident}")
        primary = NODE_TYPES[label - 1]
        extra = (
            b"Message"
            if primary in ("Post", "Comment")
            else values["type"].capitalize().encode()
            if primary in ("Place", "Organisation")
            else None
        )
        if secondary_label != extra_ids.get(extra, 0):
            raise ValueError(f"Incorrect secondary label on {ident}")
        business_id = values["id"]
        if get_key(ident) is not None:
            raise ValueError(f"Duplicate node {ident}")
        keys[ident] = (primary, business_id)
        loaded_nodes += 1
        add_node(primary, business_id, values)
    del node_labels
    nodes_loaded = time.monotonic()
    edge_labels, lookup = edge_label_lookup(path, info)
    labels = [
        label if wanted_edges is None or label in wanted_edges else None
        for label in lookup
    ]
    if shards:
        count = load_binary_edges(
            path, shards, edge_labels, labels, keys, model.add_edge
        )
    else:
        count = 0
        with (path / "edges.tsv").open("rb") as stream:
            for line in stream:
                ident, role, a, b, props = line.split(b"\t", 4)
                code = edge_labels[int(ident)]
                if not code or role != b"1":
                    raise ValueError("Missing relation label or unsupported direction")
                count += 1
                label = labels[code]
                if label is not None:
                    model.add_edge(
                        label,
                        *keys[int(a)],
                        *keys[int(b)],
                        decode_json(props.decode())
                        if props != b"{}\n"
                        else EMPTY_PROPERTIES,
                    )
    if loaded_nodes != node_count or count != info["role_rows"][1]:
        raise ValueError("Graph export row counts do not match the snapshot manifest")
    edges_loaded = time.monotonic()
    model.finish_load()
    print(
        f"snapshot index: nodes={nodes_loaded - started:.3f}s "
        f"edges={edges_loaded - nodes_loaded:.3f}s "
        f"derived={time.monotonic() - edges_loaded:.3f}s",
        file=sys.stderr,
    )
    return Catalog(
        {} if dense else keys,
        info["snapshot_scn"],
        max_id,
        initial_keys=keys if dense else None,
    )
