#!/usr/bin/env python3
"""Capture typed ClickBench results outside timed runs (requires PyMySQL).

Unlike --raw TSV, JSON tags preserve NULL, decimals, bytes, and embedded tabs
or newlines. Comparison first requires exact metadata/results; GROUP BY LIMIT
ties are validated against a running baseline by restricting complete groups.
"""

import argparse
import base64
import collections
import datetime
import decimal
import json
import os
from pathlib import Path


def encode(value):
    if isinstance(value, decimal.Decimal):
        return {"decimal": str(value)}
    if isinstance(value, bytes):
        return {"bytes": base64.b64encode(value).decode()}
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return {type(value).__name__: value.isoformat()}
    if isinstance(value, datetime.timedelta):
        return {"timedelta": value.total_seconds()}
    return value


def decode(value):
    if not isinstance(value, dict):
        return value
    key, data = next(iter(value.items()))
    if key == "decimal":
        return decimal.Decimal(data)
    if key == "bytes":
        return base64.b64decode(data)
    if key == "timedelta":
        return datetime.timedelta(seconds=data)
    return getattr(datetime, key).fromisoformat(data)


def connection(port):
    import pymysql
    db = pymysql.connect(host="127.0.0.1", port=port, user="root",
                         password=os.environ.get("MYSQL_PWD", "root"), database="clickbench",
                         charset="utf8mb4", autocommit=True, read_timeout=1810)
    with db.cursor() as cursor:
        cursor.execute("SET SESSION max_execution_time = 1800000")
    return db


def capture(args):
    args.output.mkdir(parents=True, exist_ok=False)
    with connection(args.port) as db, db.cursor() as cursor:
        for query in range(1, 44):
            sql = (args.run / f"query-{query:02}.sql").read_text()
            cursor.execute(sql)
            result = dict(sql=sql, columns=[d[0] for d in cursor.description],
                          types=[d[1] for d in cursor.description],
                          rows=[[encode(v) for v in row] for row in cursor.fetchall()])
            (args.output / f"query-{query:02}.json").write_text(json.dumps(result, ensure_ascii=False))
            print(f"Q{query:02} captured", flush=True)


def positions(sql, token):
    """Find workload clauses outside parentheses and quoted literals."""
    depth, quote, index = 0, None, 0
    while index < len(sql):
        char = sql[index]
        if quote:
            if char == "\\":
                index += 2
                continue
            if char == quote:
                if index + 1 < len(sql) and sql[index + 1] == quote:
                    index += 2
                    continue
                quote = None
        elif char in "'\"`":
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif depth == 0 and sql[index:index + len(token)].upper() == token.upper():
            yield index
        index += 1


def split_top(sql):
    cuts = [-1, *positions(sql, ","), len(sql)]
    return [sql[a + 1:b].strip() for a, b in zip(cuts, cuts[1:])]


def normalized(expr):
    return "".join(expr.split()).lower().strip("`")


def parse_workload(sql):
    sql = sql.strip().rstrip(";")
    clauses = {name: next(positions(sql, f" {name} "), len(sql))
               for name in ["FROM", "WHERE", "GROUP BY", "HAVING", "ORDER BY", "LIMIT"]}
    selected, aliases = [], {}
    for item in split_top(sql[7:clauses["FROM"]]):
        alias_at = list(positions(item, " AS "))
        expression = item[:alias_at[-1]] if alias_at else item
        index = len(selected)
        selected.append(expression.strip())
        aliases[normalized(expression)] = index
        if alias_at:
            aliases[normalized(item[alias_at[-1] + 4:])] = index

    def part(name):
        start = clauses[name]
        end = min((p for p in clauses.values() if p > start), default=len(sql))
        return sql[start + len(name) + 2:end].strip() if start < len(sql) else ""

    def resolve(expression):
        if expression.isdigit():
            return int(expression) - 1
        return aliases[normalized(expression)]

    groups = [resolve(expr) for expr in split_top(part("GROUP BY"))] if part("GROUP BY") else []
    ordering = []
    for expr in split_top(part("ORDER BY")) if part("ORDER BY") else []:
        if expr.upper().endswith(" DESC"):
            expr = expr[:-5]
        elif expr.upper().endswith(" ASC"):
            expr = expr[:-4]
        ordering.append(resolve(expr.strip()))
    return sql, selected, groups, ordering, clauses, part


def compare(args):
    report, failures = [], []
    with connection(args.port) as db, db.cursor() as cursor:
        def weights(values):
            if not values:
                return ()
            expressions = ["WEIGHT_STRING(%s COLLATE utf8mb4_0900_ai_ci)" if isinstance(v, str) else "%s" for v in values]
            cursor.execute("SELECT " + ",".join(expressions), tuple(values))
            return cursor.fetchone()

        for query in range(1, 44):
            old = json.loads((args.baseline / f"query-{query:02}.json").read_text())
            new = json.loads((args.candidate / f"query-{query:02}.json").read_text())
            before = [tuple(map(decode, row)) for row in old["rows"]]
            after = [tuple(map(decode, row)) for row in new["rows"]]
            try:
                assert old["sql"] == new["sql"], "workload changed"
                assert old["columns"] == new["columns"] and old["types"] == new["types"], "metadata changed"
                if before == after:
                    report.append(dict(query=query, status="exact"))
                    continue
                sql, selected, groups, ordering, clauses, part = parse_workload(old["sql"])
                assert len(before) == len(after), "result cardinality changed"
                assert groups, "non-grouped difference requires explicit row/tie verification"
                if ordering:
                    assert [weights(tuple(row[i] for i in ordering)) for row in before] == [weights(tuple(row[i] for i in ordering)) for row in after], "ORDER BY ranks changed"
                if collections.Counter(before) == collections.Counter(after):
                    report.append(dict(query=query, status="tie_permutation"))
                    continue
                key_set = set()
                base = sql[:clauses["WHERE"]] if part("WHERE") else sql[:clauses["GROUP BY"]]
                group_end = min(clauses["ORDER BY"], clauses["LIMIT"])
                suffix = sql[clauses["GROUP BY"]:group_end]
                predicates = ["(" + selected[index].replace("%", "%%") + ") <=> %s" for index in groups]
                if part("WHERE"):
                    predicates.insert(0, "(" + part("WHERE").replace("%", "%%") + ")")
                oracle_sql = base.replace("%", "%%") + " WHERE " + " AND ".join(predicates) + " " + suffix.replace("%", "%%")
                for row in after:
                    key = weights(tuple(row[index] for index in groups))
                    assert key not in key_set, "duplicate group"
                    key_set.add(key)
                    cursor.execute(oracle_sql, tuple(row[index] for index in groups))
                    actual = cursor.fetchall()
                    assert len(actual) == 1, "candidate group missing from baseline"
                    assert all(actual[0][i] == row[i] for i in range(len(row)) if i not in groups), "aggregate value changed"
                report.append(dict(query=query, status="groups_verified_on_baseline"))
            except (AssertionError, KeyError, ValueError) as error:
                failures.append(query)
                report.append(dict(query=query, status="unverified", reason=str(error)))
            print(f"Q{query:02}: {report[-1]['status']}", flush=True)
    args.output.write_text(json.dumps(dict(all_verified=not failures, failures=failures, queries=report), indent=2))
    return bool(failures)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)
    collect = sub.add_parser("capture")
    collect.add_argument("run", type=Path)
    collect.add_argument("output", type=Path)
    check = sub.add_parser("compare")
    check.add_argument("baseline", type=Path)
    check.add_argument("candidate", type=Path)
    check.add_argument("output", type=Path)
    for child in [collect, check]:
        child.add_argument("--port", type=int, default=3307)
    args = parser.parse_args()
    return capture(args) if args.mode == "capture" else compare(args)


if __name__ == "__main__":
    raise SystemExit(main())
