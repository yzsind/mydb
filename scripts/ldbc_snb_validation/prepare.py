"""Map Interactive v1 update parameters to graph edits and GQL statements."""

import argparse
import json
from collections import defaultdict
from pathlib import Path


class Plan:
    def __init__(self):
        self.expected = []

    def node(self, types, props):
        self.expected.append(("node", sorted(types.split(" & ")), props))

    def edge(self, label, src_type, src, dst_type, dst, **props):
        self.expected.append(("edge", label, src_type, src, dst_type, dst, props))

    def add(self, n, p, verify=False):
        node, edge = self.node, self.edge
        if n == 1:
            person = p["personId"]
            node(
                "Person",
                dict(
                    id=person,
                    firstName=p["personFirstName"],
                    lastName=p["personLastName"],
                    gender=p["gender"],
                    birthday=p["birthday"],
                    creationDate=p["creationDate"],
                    locationIP=p["locationIp"],
                    browserUsed=p["browserUsed"],
                    language=";".join(p["languages"]),
                    email=";".join(p["emails"]),
                ),
            )
            edge("IS_LOCATED_IN", "Person", person, "Place", p["cityId"])
            for tag in p["tagIds"]:
                edge("HAS_INTEREST", "Person", person, "Tag", tag)
            for entry in p["studyAt"]:
                edge(
                    "STUDY_AT",
                    "Person",
                    person,
                    "Organisation",
                    entry["organizationId"],
                    classYear=entry["year"],
                )
            for entry in p["workAt"]:
                edge(
                    "WORK_AT",
                    "Person",
                    person,
                    "Organisation",
                    entry["organizationId"],
                    workFrom=entry["year"],
                )
        elif n == 2 or n == 3:
            label, key = ("Post", "postId") if n == 2 else ("Comment", "commentId")
            edge(
                "LIKES",
                "Person",
                p["personId"],
                label,
                p[key],
                creationDate=p["creationDate"],
            )
        elif n == 4:
            ident = p["forumId"]
            node(
                "Forum",
                dict(id=ident, title=p["forumTitle"], creationDate=p["creationDate"]),
            )
            edge("HAS_MODERATOR", "Forum", ident, "Person", p["moderatorPersonId"])
            for tag in p["tagIds"]:
                edge("HAS_TAG", "Forum", ident, "Tag", tag)
        elif n == 5:
            edge(
                "HAS_MEMBER",
                "Forum",
                p["forumId"],
                "Person",
                p["personId"],
                joinDate=p["joinDate"],
            )
        elif n in (6, 7):
            label, key = ("Post", "postId") if n == 6 else ("Comment", "commentId")
            ident = p[key]
            props = dict(
                id=ident,
                creationDate=p["creationDate"],
                locationIP=p["locationIp"],
                browserUsed=p["browserUsed"],
                length=p["length"],
                content=p["content"] or None,
            )
            if n == 6:
                props.update(language=p["language"], imageFile=p["imageFile"] or None)
            node(label + " & Message", props)
            edge("HAS_CREATOR", label, ident, "Person", p["authorPersonId"])
            edge("IS_LOCATED_IN", label, ident, "Place", p["countryId"])
            for tag in p["tagIds"]:
                edge("HAS_TAG", label, ident, "Tag", tag)
            if n == 6:
                edge("CONTAINER_OF", "Forum", p["forumId"], "Post", ident)
            else:
                post = p["replyToPostId"]
                comment = p["replyToCommentId"]
                if (post == -1) == (comment == -1):
                    raise ValueError("Expected exactly one reply target")
                edge(
                    "REPLY_OF",
                    "Comment",
                    ident,
                    "Post" if post != -1 else "Comment",
                    post if post != -1 else comment,
                )
        elif n == 8:
            edge(
                "KNOWS",
                "Person",
                p["person1Id"],
                "Person",
                p["person2Id"],
                creationDate=p["creationDate"],
            )
        else:
            raise ValueError(n)

    @property
    def statements(self):
        result = []
        groups = defaultdict(list)
        for edit in self.expected:
            if edit[0] == "node":
                _, labels, props = edit
                result.append((f"INSERT (:{' & '.join(labels)} {record(props)})", 1))
            else:
                _, label, src, a, dst, b, fields = edit
                groups[label, src, dst, tuple(fields)].append(
                    dict(src=a, dst=b, **fields)
                )
        for (label, src, dst, fields), pairs in groups.items():
            edge_props = (
                " {" + ",".join(f"{k}:r.{k}" for k in fields) + "}" if fields else ""
            )
            values = ",".join(map(record, pairs))
            text = (
                f"FOR r IN [{values}] MATCH (a:{src} {{id:r.src}}), (b:{dst} {{id:r.dst}}) "
                f"INSERT (a)-[:{label}{edge_props}]->(b)"
            )
            result.append((text, len(pairs)))
        return result


def literal(value):
    if value is None:
        return "NULL"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if type(value) is int:
        return str(value)
    raise ValueError(f"Unsupported GQL atom type {type(value).__name__}")


def record(values):
    return (
        "{" + ",".join(f"{key}:{literal(value)}" for key, value in values.items()) + "}"
    )


def kind(params):
    first = next(iter(params))
    if first == "personId":
        return 1 if "personFirstName" in params else 2 if "postId" in params else 3
    if first == "forumId":
        return 4 if "forumTitle" in params else 5
    return {"postId": 6, "commentId": 7, "person1Id": 8}.get(first)


def prepare(path, output):
    count = 0
    for ordinal, line in enumerate(path.open(encoding="utf-8"), 1):
        raw, expected = line.rstrip("\n").split("|", 1)
        params = json.loads(raw)
        op = kind(params)
        if op is None:
            continue
        if expected != '"-1"':
            raise ValueError(f"update result marker differs at {ordinal}")
        plan = Plan()
        plan.add(op, params)
        print(
            json.dumps(
                dict(
                    ordinal=ordinal,
                    op=op,
                    statements=plan.statements,
                    expected=plan.expected,
                ),
                ensure_ascii=False,
            ),
            file=output,
        )
        count += 1
    return count


if __name__ == "__main__":
    import sys

    cli = argparse.ArgumentParser()
    cli.add_argument("validation", type=Path)
    args = cli.parse_args()
    assert prepare(args.validation, sys.stdout) == 7041
