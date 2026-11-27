"""Differential tests for the exact incremental validation operators."""

from collections import Counter, defaultdict, deque
from pathlib import Path
import json
import random
from tempfile import TemporaryDirectory
from threading import Thread
import time
from types import SimpleNamespace
import unittest

from forum import ForumActivity
from graph_io import (
    EDGE_RECORD,
    edge_label_lookup,
    load,
    node_label_lookup,
    read_commits,
)
from paths import shortest
from prepared import Query, Read, partitions, relevant_deltas


def reference_paths(adjacency, source, target):
    distance = {source: 0}
    paths = defaultdict(list, {source: [(source,)]})
    queue = deque([source])
    while queue:
        vertex = queue.popleft()
        for neighbor in adjacency[vertex]:
            next_depth = distance[vertex] + 1
            if neighbor not in distance:
                distance[neighbor] = next_depth
                queue.append(neighbor)
            if distance[neighbor] == next_depth:
                paths[neighbor].extend(path + (neighbor,) for path in paths[vertex])
    return distance.get(target, -1), sorted(paths[target])


class IncrementalTests(unittest.TestCase):
    def test_prepared_shards_preserve_queries_without_consulting_oracles(self):
        class UnreadableOracle:
            def __len__(self):
                raise AssertionError("Scheduling consulted the expected result")

        demands = []
        for i, label in enumerate(("IC5", "IC9", "IC5", "IC7", "IS3", "IC3")):
            number = int(label[2:])
            query = Query(label, number, label.startswith("IS"), {"id": i}, str(i))
            demands.append(
                [
                    Read(ordinal, 0, query, UnreadableOracle())
                    for ordinal in (i + 1, i + 7)
                ]
            )
        model = SimpleNamespace(ranking=SimpleNamespace(adjacent={4: {3, 5}}))
        groups = partitions(model, demands, 4)
        by_key = defaultdict(set)
        for shard, group in enumerate(groups):
            self.assertEqual(
                [r.ordinal for r in group], sorted(r.ordinal for r in group)
            )
            for read in group:
                by_key[read.query.key].add(shard)
        self.assertTrue(all(len(shards) == 1 for shards in by_key.values()))
        self.assertEqual(
            sorted(r.ordinal for g in groups for r in g), list(range(1, 13))
        )

    def test_delta_projection_keeps_every_mixed_commit(self):
        reads = [SimpleNamespace(query=SimpleNamespace(label="IS3"))]
        changes = [
            SimpleNamespace(nodes=(), edges=(("LIKES",),)),
            SimpleNamespace(nodes=(), edges=(("HAS_MEMBER",),)),
            SimpleNamespace(nodes=(), edges=(("LIKES",), ("KNOWS",))),
            SimpleNamespace(nodes=(("Person",),), edges=(("HAS_MEMBER",),)),
        ]
        self.assertEqual(
            relevant_deltas(changes, reads), [(3, changes[2]), (4, changes[3])]
        )

    def test_compact_and_text_label_maps_agree(self):
        with TemporaryDirectory() as temp:
            root = Path(temp)
            manifest = {"max_element_id": 3, "role_rows": [2, 1, 0, 3, 1]}
            (root / "node-labels.tsv").write_bytes(b"1\tPerson\n2\tMessage\n2\tPost\n")
            (root / "edge-labels.tsv").write_bytes(b"3\tHAS_CREATOR\n")
            (root / "node-labels-first.bin").write_bytes(bytes([0, 1, 2, 0]))
            (root / "node-labels-second.bin").write_bytes(bytes([0, 0, 3, 0]))
            (root / "edge-labels.bin").write_bytes(bytes([0, 0, 0, 4]))
            compact = dict(
                manifest,
                label_dictionary=["", "Person", "Message", "Post", "HAS_CREATOR"],
            )
            types = {b"Person": 1, b"Post": 2}
            extra = {b"Message": 1}
            text_nodes = node_label_lookup(root, manifest, types, extra)
            compact_nodes = node_label_lookup(root, compact, types, extra)
            for ident in (1, 2):
                self.assertEqual(compact_nodes(ident), text_nodes(ident))
            text_codes, text_names = edge_label_lookup(root, manifest)
            binary_codes, binary_names = edge_label_lookup(root, compact)
            self.assertEqual(binary_names[binary_codes[3]], text_names[text_codes[3]])

    def test_sharded_snapshot_preserves_edge_properties_and_order(self):
        with TemporaryDirectory() as temp:
            path = Path(temp)
            info = {
                "format": "sharded-v1",
                "shards": 2,
                "snapshot_scn": 7,
                "max_node_id": 2,
                "max_element_id": 4,
                "role_rows": [2, 2, 0, 2, 2],
                "label_dictionary": ["", "Person", "KNOWS"],
            }
            (path / "snapshot.json").write_text(json.dumps(info), encoding="utf-8")
            (path / "node-labels-first.bin").write_bytes(bytes([0, 1, 1, 0, 0]))
            (path / "node-labels-second.bin").write_bytes(bytes(5))
            (path / "edge-labels.bin").write_bytes(bytes([0, 0, 0, 2, 2]))
            (path / "nodes-0.jsonl").write_bytes(b'[[1,{"id":100}]]\n')
            (path / "nodes-1.jsonl").write_bytes(b'[[2,{"id":200}]]\n')
            (path / "edges-0.bin").write_bytes(b"")
            (path / "edge-props-0.jsonl").write_bytes(b"")
            (path / "edges-1.bin").write_bytes(
                EDGE_RECORD.pack(3, 2, 1, 3) + EDGE_RECORD.pack(4, 1, 2, 2)
            )
            props = path / "edge-props-1.jsonl"
            props.write_bytes(b'[{"creationDate":9}]\n')
            calls = []
            model = SimpleNamespace(
                add_node=lambda *args: calls.append(args),
                add_edge=lambda *args: calls.append(args),
                finish_load=lambda: calls.append("done"),
            )
            catalog = load(path, model)
            self.assertEqual(catalog.scn, 7)
            self.assertEqual(catalog.resolve(1), ("Person", 100))
            self.assertEqual(
                calls,
                [
                    ("Person", 100, {"id": 100}),
                    ("Person", 200, {"id": 200}),
                    ("KNOWS", "Person", 200, "Person", 100, {"creationDate": 9}),
                    ("KNOWS", "Person", 100, "Person", 200, {}),
                    "done",
                ],
            )
            props.write_bytes(b'[{"creationDate":9},{}]\n')
            with self.assertRaisesRegex(ValueError, "properties do not match"):
                load(path, model)
            props.write_bytes(b"[]\n")
            with self.assertRaisesRegex(
                ValueError, "Missing or invalid edge properties"
            ):
                load(path, model)

    def test_bidirectional_paths_equal_single_source_paths(self):
        for seed in range(32):
            rng = random.Random(seed)
            adjacency = [set() for _ in range(12)]
            for a in range(len(adjacency)):
                for b in range(a):
                    if rng.random() < 0.25:
                        adjacency[a].add(b)
                        adjacency[b].add(a)
            for a in range(len(adjacency)):
                for b in range(len(adjacency)):
                    distance, paths = reference_paths(adjacency, a, b)
                    exact_distance, exact_paths = shortest(
                        a, b, adjacency.__getitem__, True
                    )
                    self.assertEqual(
                        (exact_distance, sorted(exact_paths)), (distance, paths)
                    )
                    self.assertEqual(
                        shortest(a, b, adjacency.__getitem__, False)[0], distance
                    )

    def test_forum_updates_equal_recomputation(self):
        for seed in range(12):
            rng = random.Random(seed)
            adjacency = defaultdict(set)
            actual = ForumActivity(adjacency)
            memberships = []
            posts = Counter()
            requests = [
                (rng.randrange(12), rng.randrange(12), rng.randrange(5))
                for _ in range(20)
            ]
            for _ in range(200):
                author = rng.randrange(12)
                forum = rng.randrange(6)
                when = rng.randrange(12)
                act = rng.randrange(3)
                if act == 0:
                    memberships.append((author, forum, when))
                    actual.add_member(author, forum, when)
                elif act == 1:
                    posts[author, forum] += 1
                    actual.add_post(author, forum)
                else:
                    friend = rng.randrange(12)
                    if friend not in adjacency[author]:
                        adjacency[author].add(friend)
                        adjacency[friend].add(author)
                        actual.link_added(author, friend)
                for person, minimum, limit in rng.sample(requests, 5):
                    friends = set(adjacency[person])
                    friends.update(
                        neighbor
                        for direct in adjacency[person]
                        for neighbor in adjacency[direct]
                    )
                    friends.discard(person)
                    eligible = {
                        (a, forum)
                        for a, forum, joined in memberships
                        if a in friends and joined > minimum
                    }
                    counts = Counter()
                    for a, f in eligible:
                        counts[f] += posts[a, f]
                    expected = sorted((-score, f) for f, score in counts.items())[
                        :limit
                    ]
                    self.assertEqual(actual.read(person, minimum, limit), expected)

    def test_follow_commits_accepts_split_utf8_record(self):
        with TemporaryDirectory() as temp:
            root = Path(temp)
            record = '{"message":"你好"}\n'.encode()

            def publish():
                with (root / "commits.jsonl").open("wb") as stream:
                    split = record.index("你".encode()) + 1
                    stream.write(record[:split])
                    stream.flush()
                    time.sleep(0.04)
                    stream.write(record[split:])
                (root / "capture.json").write_text("{}")

            thread = Thread(target=publish)
            thread.start()
            self.assertEqual(
                list(read_commits(root, follow=True)), [{"message": "你好"}]
            )
            thread.join()


if __name__ == "__main__":
    unittest.main()
