#!/usr/bin/env python3
"""Reproducible MySQL/mydb function and expression comparison.

All generated data belongs under /tmp/mydb. The Rust tools-feature worker uses
the same protocol and result-draining code for both servers. See --help.
"""

from __future__ import annotations

import argparse
import collections
import csv
from decimal import Decimal, InvalidOperation
import hashlib
import fcntl
from functools import lru_cache
from html.parser import HTMLParser
import itertools
import json
import math
import os
from pathlib import Path
import platform
import random
import re
import select
import signal
import statistics
import subprocess
import sys
import time
import uuid

from function_expression_cases import (
    ROOT,
    MYSQL_SOURCE,
    SCHEMA_VERSION,
    build_cases,
    registered_names,
)

DEFAULT_DIR = Path("/tmp/mydb/function-expression")
SESSION = [
    "SET SESSION sql_mode='NO_ENGINE_SUBSTITUTION'",
    "SET SESSION time_zone='+00:00'",
    "SET SESSION lc_time_names='en_US'",
    "SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci",
    "SET SESSION transaction_isolation='READ-COMMITTED'",
    "SET timestamp=1788105600",
    "SET SESSION group_concat_max_len=1048576",
    "SET SESSION max_execution_time=10000",
]
MODES = {"mysql": (33916, ""), "serial": (34916, "root"), "parallel2": (34917, "root")}


def dump(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")
    temporary.replace(path)


def digest_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


class InventoryParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.row = False
        self.code = False
        self.text = ""
        self.href = ""
        self.items = []
        self.codes = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "th" and attrs.get("scope") == "row":
            self.row, self.text, self.href, self.codes = True, "", "", []
        if self.row and tag == "a":
            self.href = attrs.get("href", "")
        if self.row and tag == "code":
            self.code = True
            self.text = ""

    def handle_data(self, data):
        if self.row and self.code:
            self.text += data

    def handle_endtag(self, tag):
        if tag == "code":
            if self.row and self.code:
                self.codes.append(self.text)
            self.code = False
        if tag == "th" and self.row:
            for text in dict.fromkeys(self.codes):
                if text:
                    self.items.append(
                        {
                            "name": text.upper().removesuffix("()"),
                            "reference": self.href,
                        }
                    )
            self.row = False


def make_inventory(directory):
    source = directory / "mysql-functions.html"
    if not source.exists():
        import urllib.request

        source.write_bytes(urllib.request.urlopen(MYSQL_SOURCE, timeout=30).read())
    parser = InventoryParser()
    parser.feed(source.read_text())
    if len(parser.items) < 350:
        raise RuntimeError("official function inventory unexpectedly small")
    cases = build_cases()
    by_name = collections.defaultdict(list)
    for case in cases:
        by_name[case["name"]].append(case["id"])
    registered = registered_names()
    aliases = {
        "CONVERT": "CONVERT_USING",
        "DEFAULT": "EXPR_DEFAULT_COLUMN",
        "BINARY": "EXPR_BINARY_CAST",
        "MATCH": "MATCH_AGAINST",
        "MOD": "MOD",
        "->": "EXPR_JSON_ARROW",
        "->>": "EXPR_JSON_LONG_ARROW",
        "+": "EXPR_ADD",
        "-": "EXPR_SUBTRACT",
        "*": "EXPR_MULTIPLY",
        "/": "EXPR_DIVIDE",
        "%": "EXPR_MODULO",
        "DIV": "EXPR_INT_DIV",
        "&": "EXPR_BIT_AND",
        "|": "EXPR_BIT_OR",
        "^": "EXPR_BIT_XOR",
        "~": "EXPR_BIT_NOT",
        "<<": "EXPR_SHIFT_LEFT",
        ">>": "EXPR_SHIFT_RIGHT",
        "=": "EXPR_EQUAL",
        "<=>": "EXPR_NULL_SAFE_EQUAL",
        "!=": "EXPR_BANG_EQUAL",
        "<>": "EXPR_NOT_EQUAL",
        "<": "EXPR_LESS",
        "<=": "EXPR_LESS_EQUAL",
        ">": "EXPR_GREATER",
        ">=": "EXPR_GREATER_EQUAL",
        "!": "EXPR_BANG",
        "AND, &&": "EXPR_AND",
        "OR, ||": "EXPR_OR",
        "XOR": "EXPR_XOR",
        "NOT, !": "EXPR_NOT",
        "BETWEEN ... AND ...": "EXPR_BETWEEN",
        "NOT BETWEEN ... AND ...": "EXPR_NOT_BETWEEN",
        "IN": "EXPR_IN_LIST",
        "NOT IN": "EXPR_NOT_IN_LIST",
        "IS": "EXPR_IS_TRUE",
        "IS NOT": "EXPR_IS_NOT_TRUE",
        "IS NULL": "EXPR_IS_NULL",
        "IS NOT NULL": "EXPR_IS_NOT_NULL",
        "LIKE": "EXPR_LIKE",
        "NOT LIKE": "EXPR_NOT_LIKE",
        "REGEXP, RLIKE": "EXPR_REGEXP",
        "NOT REGEXP": "EXPR_NOT_REGEXP",
        "CASE": "EXPR_CASE_SEARCHED",
        ":=": "EXPR_VARIABLE_ASSIGNMENT",
    }
    aliases.update(
        {
            "AND": "EXPR_AND",
            "&&": "EXPR_AND_SYMBOL",
            "OR": "EXPR_OR",
            "||": "EXPR_OR_SYMBOL",
            "NOT": "EXPR_NOT",
            "EXISTS": "EXPR_EXISTS",
            "NOT EXISTS": "EXPR_NOT_EXISTS",
            "REGEXP": "EXPR_REGEXP",
            "RLIKE": "EXPR_RLIKE",
            "MEMBER OF": "EXPR_MEMBER_OF",
            "SOUNDS LIKE": "EXPR_SOUNDS_LIKE",
        }
    )
    items = []
    for item in parser.items:
        target = aliases.get(item["name"], item["name"])
        ids = by_name.get(target, [])
        internal = item["reference"].startswith("internal-functions.html")
        unavailable = (
            "Internal metadata helper; outside the user-visible SQL expression interface."
            if internal
            else "No comparable mydb callable in the frozen registry; unsupported/absent functionality is not assigned a speedup."
        )
        items.append(
            dict(
                item,
                source="mysql80",
                mydb_registered=target in registered,
                cases=ids,
                disposition="workload" if ids else "missing_mydb_or_special_syntax",
                reason="" if ids else unavailable,
            )
        )
    for name in sorted(registered):
        ids = by_name.get(name, [])
        external = name.startswith(("HTTP_", "AI_"))
        if not ids and not external:
            raise RuntimeError(f"unaccounted mydb function: {name}")
        items.append(
            dict(
                name=name,
                source="mydb_registry",
                mydb_registered=True,
                cases=ids,
                disposition="external_dependency" if external else "workload",
                reason=(
                    "Requires configured external service; excluded by the agreed scope."
                    if external
                    else ""
                ),
            )
        )
    result = dict(
        schema_version=SCHEMA_VERSION,
        mysql_version="8.0.46",
        source=MYSQL_SOURCE,
        source_sha256=digest_file(source),
        registered_count=len(registered),
        official_count=len(parser.items),
        items=items,
        cases=cases,
    )
    dump(directory / "manifest.json", result)
    return result


class Worker:
    def __init__(self, binary, port, password, directory, database=None):
        env = dict(os.environ, FEB_PASSWORD=password)
        self.log = (directory / f"worker-{port}.log").open("ab")
        self.process = subprocess.Popen(
            [str(binary), str(port)] + ([database] if database else []),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self.log,
            env=env,
        )
        self.buffer = b""
        hello = self.line(time.monotonic() + 10)
        self.connection_id = hello["connection_id"]

    def line(self, deadline):
        while b"\n" not in self.buffer:
            remaining = deadline - time.monotonic()
            if (
                remaining <= 0
                or not select.select([self.process.stdout], [], [], max(0, remaining))[
                    0
                ]
            ):
                raise TimeoutError("worker response deadline")
            block = os.read(self.process.stdout.fileno(), 65536)
            if not block:
                raise RuntimeError(f"worker exited ({self.process.poll()})")
            self.buffer += block
        line, self.buffer = self.buffer.split(b"\n", 1)
        return json.loads(line)

    def query(self, sql, timeout=10, control=None, **options):
        self.process.stdin.write((json.dumps(dict(sql=sql, **options)) + "\n").encode())
        self.process.stdin.flush()
        start = self.line(time.monotonic() + 5)
        if start.get("event") != "started":
            raise RuntimeError(f"invalid worker response {start}")
        started = time.monotonic()
        try:
            result = self.line(time.monotonic() + timeout)
        except TimeoutError:
            if control is None:
                self.close()
                raise
            cancellation = control.query(f"KILL QUERY {self.connection_id}", timeout=3)
            try:
                result = self.line(time.monotonic() + 2)
            except TimeoutError:
                cancellation = control.query(
                    f"KILL CONNECTION {self.connection_id}", timeout=3
                )
                self.close()
                return dict(
                    status="timeout",
                    deadline_ms=timeout * 1000,
                    elapsed_ms=(time.monotonic() - started) * 1000,
                    connection_id=self.connection_id,
                    requires_reconnect=True,
                    cancellation="connection_killed",
                    cancellation_acknowledged=False,
                    error="Query did not acknowledge KILL QUERY within the cleanup deadline.",
                )
            result.update(
                status="timeout",
                deadline_ms=timeout * 1000,
                cancellation=cancellation["status"],
            )
        if result.get("identity", [None]) and result.get("identity", [None])[0] in (
            3024,
            1317,
        ):
            result["status"] = "timeout"
        if result.get("elapsed_ms", 0) > timeout * 1000:
            result["status"] = "timeout"
        result["connection_id"] = self.connection_id
        return result

    def close(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        self.log.close()


class Endpoint:
    def __init__(self, name, args, database=True):
        self.name = name
        self.args = args
        self.port, self.password = MODES[name]
        self.control = Worker(args.worker, self.port, self.password, args.directory)
        self.worker = Worker(
            args.worker,
            self.port,
            self.password,
            args.directory,
            args.database if database else None,
        )
        self.setup()

    def setup(self):
        for sql in SESSION:
            result = self.worker.query(sql, control=self.control)
            if result["status"] != "ok":
                raise RuntimeError(f"{self.name} session setup {sql}: {result}")
        if self.name == "mysql":
            self.worker.query(
                "SET SESSION innodb_parallel_read_threads=1", control=self.control
            )
        if getattr(self.args, "timeout", 10) != 10:
            self.worker.query(
                f"SET SESSION max_execution_time={int(self.args.timeout*1000)}",
                control=self.control,
            )

    def execute(self, sql, **kwargs):
        result = self.worker.query(sql, control=self.control, **kwargs)
        if result.get("requires_reconnect"):
            old_id = self.worker.connection_id
            cleanup_started = time.monotonic()
            deadline = cleanup_started + 60
            while True:
                active = self.control.query(
                    f"SELECT ID FROM information_schema.PROCESSLIST WHERE ID={old_id}",
                    timeout=3,
                )
                if active["status"] == "ok" and active["rows"] == 0:
                    break
                if time.monotonic() >= deadline:
                    raise RuntimeError(
                        f"cancelled connection {old_id} remains active; stop before starting another benchmark query"
                    )
                time.sleep(0.1)
            self.worker = Worker(
                self.args.worker,
                self.port,
                self.password,
                self.args.directory,
                self.args.database,
            )
            self.setup()
            result["replacement_connection_id"] = self.worker.connection_id
            result["quiescence_wait_ms"] = (time.monotonic() - cleanup_started) * 1000
        return result

    def close(self):
        try:
            if self.control.process.poll() is None:
                self.control.query(
                    f"KILL CONNECTION {self.worker.connection_id}", timeout=3
                )
        finally:
            self.worker.close()
            self.control.close()


DDL = """CREATE TABLE bench (
id BIGINT PRIMARY KEY, n BIGINT DEFAULT 7, u BIGINT UNSIGNED, d DECIMAL(20,6), f DOUBLE,
s VARCHAR(1024), b VARBINARY(128), dt DATETIME(6), sdate VARCHAR(32), j JSON,
gp GEOMETRY, gq GEOMETRY, gl GEOMETRY, gl2 GEOMETRY, poly GEOMETRY, gc GEOMETRY
)"""


def prepare(args):
    args.directory.mkdir(parents=True, exist_ok=True)
    payload = args.directory / "payload.txt"
    payload.write_text("function-expression-benchmark\n")
    seed = args.directory / "seed.tsv"
    with seed.open("w") as output:
        for number in range(1, args.rows + 1):
            output.write(f"{number}\n")
    for name in args.modes:
        endpoint = Endpoint(name, args, database=False)
        started = time.monotonic()
        try:
            require_ok(endpoint.execute("SET SESSION max_execution_time=0"))
            if name == "mysql":
                require_ok(endpoint.execute("SET GLOBAL local_infile=ON"))
            statements = [
                f"CREATE DATABASE IF NOT EXISTS `{args.database}`",
                f"USE `{args.database}`",
                "DROP TABLE IF EXISTS bench",
                "DROP TABLE IF EXISTS seed",
                "CREATE TABLE seed(id BIGINT PRIMARY KEY)",
            ]
            for sql in statements:
                require_ok(endpoint.execute(sql, timeout=600))
            # Client upload uses the official CLI for both endpoints. Its time is
            # preparation time and never enters expression latency samples.
            env = dict(os.environ, MYSQL_PWD=endpoint.password)
            subprocess.run(
                [
                    args.mysql_cli,
                    "--local-infile=1",
                    "--ssl-mode=DISABLED",
                    "-h127.0.0.1",
                    f"-P{endpoint.port}",
                    "-uroot",
                    args.database,
                    "-e",
                    f"LOAD DATA LOCAL INFILE '{seed}' INTO TABLE seed",
                ],
                env=env,
                check=True,
                stdout=subprocess.DEVNULL,
                timeout=600,
            )
            require_ok(endpoint.execute(DDL, timeout=600))
            date = "DATE_ADD(CAST('2020-01-01 00:00:00.123456' AS DATETIME(6)),INTERVAL id SECOND)"
            text = "IF(id%10=0,NULL,CONCAT('  Mydb',id%1000,',数据库,',REPEAT('x',id%3*32),'  '))"
            sql = f"""INSERT INTO bench SELECT id,IF(id%10=0,NULL,id%2001-1000),id%1000000,
            CAST((id%2001-1000)*0.125 AS DECIMAL(20,6)),(id%2001-1000)*0.125e0,
            {text},CAST(CONCAT('binary-',id%1000) AS BINARY),{date},CAST({date} AS CHAR),
            JSON_OBJECT('a',id%10,'b',JSON_ARRAY(1,2,3),'s',CONCAT('db',id%100)),
            POINT(id%10+1,2),POINT(id%10+2,3),
            ST_GEOMFROMTEXT(IF(id%2=0,'LINESTRING(0 0,2 2)','LINESTRING(1 0,3 2)')),
            ST_GEOMFROMTEXT(IF(id%2=0,'LINESTRING(0 1,2 3)','LINESTRING(1 1,3 3)')),
            ST_GEOMFROMTEXT(IF(id%2=0,'POLYGON((0 0,0 10,10 10,10 0,0 0),(2 2,3 2,3 3,2 3,2 2))','POLYGON((0 0,0 8,8 8,8 0,0 0),(2 2,3 2,3 3,2 3,2 2))')),
            GEOMETRYCOLLECTION(POINT(id%10+1,2),POINT(id%10+2,3)) FROM seed"""
            insert_sql = sql
            require_ok(endpoint.execute(sql, timeout=600))
            for sql in [
                "DROP TABLE IF EXISTS lookup",
                "CREATE TABLE lookup(v INT PRIMARY KEY)",
                "INSERT INTO lookup VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)",
                "DROP TABLE IF EXISTS text_bench",
                "CREATE TABLE text_bench(id BIGINT PRIMARY KEY,s TEXT,FULLTEXT(s))",
                "INSERT INTO text_bench SELECT id,CONCAT('database engine token',id%1000) FROM seed",
                "ANALYZE TABLE bench",
                "ANALYZE TABLE text_bench",
            ]:
                require_ok(endpoint.execute(sql, timeout=600))
            count = endpoint.execute(
                "SELECT COUNT(*),SUM(id),SUM(u),SUM(n),SUM(d),SUM(f),SUM(LENGTH(s)),SUM(UNIX_TIMESTAMP(dt)) FROM bench",
                timeout=600,
            )
            require_ok(count)
            if int(count["sample"][0][0]) != args.rows:
                raise RuntimeError(f"row count differs: {count}")
            dump(
                args.directory / f"prepared-{name}.json",
                dict(
                    rows=args.rows,
                    elapsed_s=time.monotonic() - started,
                    fingerprint=count,
                    schema=DDL,
                    load_sql=insert_sql,
                    seed_sha256=digest_file(seed),
                ),
            )
            print(
                f"prepared {name}: {args.rows} rows in {time.monotonic()-started:.1f}s",
                flush=True,
            )
        finally:
            endpoint.close()


def require_ok(result):
    if result["status"] != "ok":
        raise RuntimeError(json.dumps(result, ensure_ascii=False))
    return result


def sql_for(case, name, token):
    sql = case.get("mysql_sql", case["sql"]) if name == "mysql" else case["sql"]
    hint = (
        "NO_PARALLEL"
        if name == "serial"
        else "PARALLEL(2)" if name == "parallel2" else ""
    )
    return re.sub(
        r"^(SELECT|INSERT)\s+",
        lambda match: match.group(1)
        + " "
        + (f"/*+ {hint} */ " if hint else "")
        + f"/*feb:{token}*/ ",
        sql,
        count=1,
        flags=re.I,
    )


def numeric_equal_uncached(left, right):
    try:
        a, b = Decimal(left), Decimal(right)
        if a == b:
            return True
        if not a.is_finite() or not b.is_finite():
            return False
        return abs(a - b) <= max(
            Decimal("1e-12"), Decimal("1e-9") * max(abs(a), abs(b))
        )
    except (InvalidOperation, TypeError, ValueError):
        return False


@lru_cache(maxsize=65536)
def numeric_strings_equal(left, right):
    return numeric_equal_uncached(left, right)


def numeric_equal(left, right):
    # Bounded memoization only affects post-query validation. Repeated values
    # do not need repeated decimal parsing; every result row is still consumed.
    if isinstance(left, str) and isinstance(right, str):
        return numeric_strings_equal(left, right)
    return numeric_equal_uncached(left, right)


def json_identity(value):
    if value is None:
        return ("null",)
    if isinstance(value, bool):
        return ("boolean", value)
    if isinstance(value, (int, float, Decimal)):
        return ("number", Decimal(str(value)))
    if isinstance(value, str):
        return ("string", value)
    if isinstance(value, list):
        return ("array", tuple(json_identity(item) for item in value))
    if isinstance(value, dict):
        return (
            "object",
            tuple(sorted((key, json_identity(item)) for key, item in value.items())),
        )
    raise ValueError("unsupported JSON value")


def equivalent(left, right, comparison):
    if left == right:
        return True
    if left is None or right is None:
        return False
    if comparison in {"json", "json_object", "json_multiset"}:
        try:
            a = json.loads(left, parse_int=Decimal, parse_float=Decimal)
            b = json.loads(right, parse_int=Decimal, parse_float=Decimal)
            if (
                comparison == "json_multiset"
                and isinstance(a, list)
                and isinstance(b, list)
            ):
                return collections.Counter(
                    json_identity(x) for x in a
                ) == collections.Counter(json_identity(x) for x in b)
            return json_identity(a) == json_identity(b)
        except (ValueError, TypeError):
            return False
    if comparison == "geometry_multiset":

        def points(text):
            text = str(text).upper()
            if text.startswith("MULTIPOINT"):
                coordinates = (
                    text[len("MULTIPOINT") :]
                    .replace("(", "")
                    .replace(")", "")
                    .split(",")
                )
            elif text.startswith("GEOMETRYCOLLECTION"):
                coordinates = re.findall(r"POINT\s*\(([^)]+)\)", text)
            else:
                return collections.Counter()
            return collections.Counter(
                tuple(Decimal(x) for x in coordinate.split())
                for coordinate in coordinates
            )

        return points(left) == points(right) and bool(points(left))
    return comparison == "numeric" and numeric_equal(left, right)


def sort_capture(path):
    target = path.with_suffix(".sorted")
    if target.exists() and target.stat().st_mtime_ns >= path.stat().st_mtime_ns:
        return target
    env = dict(os.environ, LC_ALL="C", TMPDIR=str(path.parent))
    with target.open("wb") as output:
        subprocess.run(
            ["sort", "-t", "\t", "-k1,1n", str(path)],
            stdout=output,
            env=env,
            check=True,
        )
    return target


def compare_captures(left, right, comparison, numeric_columns=False):
    if (
        subprocess.run(["cmp", "-s", str(left), str(right)], check=False).returncode
        == 0
    ):
        with left.open() as source:
            count = sum(1 for _ in source)
        return dict(status="match", compared_rows=count, differences=[])
    paths = [sort_capture(left), sort_capture(right)]
    differences = []
    compared = 0
    try:
        with paths[0].open() as a, paths[1].open() as b:
            for la, lb in itertools.zip_longest(a, b):
                compared += 1
                if la == lb:
                    continue
                if la is None or lb is None:
                    differences.append(
                        dict(row=compared, left=la, right=lb, reason="row count")
                    )
                else:
                    va, vb = json.loads(la.split("\t", 1)[1]), json.loads(
                        lb.split("\t", 1)[1]
                    )
                    same = len(va) == len(vb) and va[0] == vb[0]
                    for x, y in zip(va[1:], vb[1:]):
                        same &= equivalent(
                            x, y, "numeric" if numeric_columns else comparison
                        )
                    if not same:
                        differences.append(dict(row=compared, left=va, right=vb))
                if len(differences) >= 20:
                    break
        return dict(
            status="match" if not differences else "mismatch",
            compared_rows=compared,
            differences=differences,
        )
    finally:
        # Each capture is used in two comparisons. Reuse its sorted version
        # within this case; the caller removes the files after all comparisons.
        pass


def check_contract(path, name, expected=None):
    invalid = []
    count = 0
    distinct = (
        set()
        if name in {"UUID", "UUID_SHORT", "RANDOM_BYTES", "RAND", "NEXTVAL"}
        else None
    )
    with path.open() as source:
        for line in source:
            count += 1
            values = json.loads(line.split("\t", 1)[1])
            value = values[1]
            if name == "UUID":
                valid = isinstance(value, str) and re.fullmatch(
                    r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", value
                )
            elif name == "RAND":
                valid = value is not None and 0 <= float(value) < 1
            elif name == "RANDOM_BYTES":
                try:
                    payload = (
                        bytes.fromhex(value["binary"])
                        if isinstance(value, dict)
                        else value.encode()
                    )
                    valid = len(payload) == 16
                except (TypeError, KeyError, AttributeError, ValueError):
                    valid = False
            elif name in {
                "UUID_SHORT",
                "CONNECTION_ID",
                "PS_CURRENT_THREAD_ID",
                "PS_THREAD_ID",
                "NEXTVAL",
            }:
                valid = value is not None and int(value) > 0
            elif name == "IS_USED_LOCK":
                valid = (int(values[0]) % 8 != 0 and value is None) or (
                    int(values[0]) % 8 == 0 and value is not None and int(value) > 0
                )
            else:
                valid = isinstance(value, str) and bool(value)
            if expected is not None:
                valid = valid and value == str(expected)
            if not valid:
                invalid.append(dict(row=count, value=value))
                if len(invalid) == 20:
                    break
            if valid and distinct is not None:
                distinct.add(payload if name == "RANDOM_BYTES" else value)
    if not count:
        invalid.append(dict(reason="empty contract result"))
    if not invalid and distinct is not None:
        unique = len(distinct)
        if name in {"UUID", "UUID_SHORT", "NEXTVAL"} and unique != count:
            invalid.append(
                dict(
                    reason="duplicate generated identifiers",
                    rows=count,
                    distinct=unique,
                )
            )
        if name in {"RAND", "RANDOM_BYTES"} and count >= 1000 and unique < 1000:
            invalid.append(
                dict(
                    reason="unexpectedly low variation for this workload",
                    rows=count,
                    distinct=unique,
                )
            )
        if name == "NEXTVAL" and (
            min(map(int, distinct)) != 1 or max(map(int, distinct)) != count
        ):
            invalid.append(dict(reason="sequence range differs from START WITH 1"))
    return dict(
        status="contract_valid" if not invalid else "contract_invalid",
        checked_rows=count,
        distinct_values=len(distinct) if distinct is not None else None,
        differences=invalid,
    )


def statement_metrics(endpoint, token):
    thread = endpoint.control.query(
        f"SELECT THREAD_ID FROM performance_schema.threads WHERE PROCESSLIST_ID={endpoint.worker.connection_id}",
        timeout=5,
    )
    if thread["status"] != "ok" or not thread.get("sample"):
        return dict(unavailable=thread)
    thread_id = int(thread["sample"][0][0])
    sql = f"""SELECT EVENT_ID,TIMER_WAIT,ROWS_SENT,ROWS_EXAMINED FROM performance_schema.events_statements_history
    WHERE THREAD_ID={thread_id}
    AND SQL_TEXT LIKE '%feb:{token}%' ORDER BY EVENT_ID DESC LIMIT 1"""
    result = endpoint.control.query(sql, timeout=5)
    if result["status"] == "ok" and result.get("sample"):
        row = result["sample"][0]
        return dict(
            event_id=row[0],
            statement_ms=float(row[1]) / 1e9 if row[1] is not None else None,
            rows_sent=row[2],
            rows_examined=row[3],
        )
    return dict(unavailable=result)


def apply_case_setup(endpoint, case):
    endpoint.setup()
    if case["category"] == "locking":
        require_ok(endpoint.execute("SELECT RELEASE_ALL_LOCKS()"))
        if case["name"] in {"IS_USED_LOCK", "RELEASE_LOCK"}:
            require_ok(endpoint.execute("SELECT GET_LOCK('feb-0',0)"))
    for sql in case.get("setup", []):
        result = endpoint.execute(sql)
        if result["status"] != "ok":
            return result
    return None


def effective_comparison(comparison, columns):
    if (
        comparison == "exact"
        and len(columns) > 1
        and columns[1].get("type") == "MYSQL_TYPE_JSON"
    ):
        return "json"
    return comparison


def parallel_evidence(plan, name):
    text = "\n".join(str(cell) for row in (plan or []) for cell in row)
    lines = text.splitlines()
    return dict(
        requested=2 if name == "parallel2" else 1,
        observed_workers=sorted(
            set(int(x) for x in re.findall(r"workers[=: ]+(\d+)", text))
        ),
        exchange="Exchange" in text,
        observed_execution_modes=sorted(
            set(re.findall(r"execution_mode=([a-z_]+)", text))
        ),
        observed_expression_modes=sorted(
            set(re.findall(r"expression_mode=([a-z_]+)", text))
        ),
        parallel_expression_operator_observed=any(
            re.search(r"workers[=: ]+2\b", line) and "expression_mode=" in line
            for line in lines
        ),
    )


def run(args, validate_only=False):
    manifest = json.loads((args.directory / "manifest.json").read_text())
    manifest_hash = digest_file(args.directory / "manifest.json")
    cases = [
        c
        for c in manifest["cases"]
        if (not args.filter or re.search(args.filter, c["id"]))
    ]
    if args.limit:
        cases = cases[: args.limit]
    context = dict(
        schema_version=SCHEMA_VERSION,
        manifest_sha256=manifest_hash,
        rows=args.rows,
        timeout_s=args.timeout,
        skip_invalid=args.skip_invalid,
        repeats=args.repeats,
        warmup=args.warmup,
        worker_sha256=digest_file(args.worker),
        modes=args.modes,
        machine=platform.platform(),
        timestamp=time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    )
    # Freeze actual executable and data identities, not just the Git commit.
    binary = args.worker.parent / "mydb"
    context["mydb_binary_sha256"] = digest_file(binary) if binary.exists() else None
    context["prepared"] = {
        name: json.loads((args.directory / f"prepared-{name}.json").read_text())
        for name in args.modes
        if (args.directory / f"prepared-{name}.json").exists()
    }
    context_path = args.directory / "context.json"
    if context_path.exists():
        previous = json.loads(context_path.read_text())
        if previous.get("report_only"):
            raise RuntimeError(
                "merged cohorts are report-only; resume an original run directory"
            )
        for key in (
            "manifest_sha256",
            "rows",
            "timeout_s",
            "repeats",
            "warmup",
            "worker_sha256",
            "mydb_binary_sha256",
            "modes",
            "prepared",
            "skip_invalid",
        ):
            if previous.get(key, False) != context.get(key, False):
                raise RuntimeError(f"resume context differs: {key}")
    else:
        dump(context_path, context)
    endpoints = {name: Endpoint(name, args) for name in args.modes}
    try:
        for ep in endpoints.values():
            require_ok(
                ep.control.query(
                    "UPDATE performance_schema.setup_consumers SET ENABLED='YES' WHERE NAME IN ('events_statements_current','events_statements_history')"
                )
            )
        for index, case in enumerate(cases):
            case_dir = args.directory / "cases" / case["id"]
            case_dir.mkdir(parents=True, exist_ok=True)
            completed = case_dir / (
                "validation.json" if validate_only else "result.json"
            )
            if completed.exists() and not args.rerun:
                continue
            result = dict(
                case_id=case["id"],
                timeout_s=args.timeout,
                name=case["name"],
                category=case["category"],
                comparison=case["comparison"],
                engines={},
            )
            captures = {}
            order = list(endpoints)
            order = order[index % len(order) :] + order[: index % len(order)]
            for name in order:
                ep = endpoints[name]
                if name == "mysql" and case.get("mysql_unavailable"):
                    result["engines"][name] = dict(
                        status="unavailable", reason=case["mysql_unavailable"]
                    )
                    continue
                setup_error = apply_case_setup(ep, case)
                if setup_error:
                    result["engines"][name] = dict(
                        status="setup_error", detail=setup_error
                    )
                    continue
                token = uuid.uuid4().hex[:12]
                sql = sql_for(case, name, token)
                query_options = {
                    "prepared": case.get("prepared", False),
                    "params": case.get("params", []),
                }
                engine = dict(
                    sql=sql,
                    status="ok",
                    metrics={},
                    plan=None,
                    connection_id=ep.worker.connection_id,
                )
                if case["name"] in {"PS_CURRENT_THREAD_ID", "PS_THREAD_ID"}:
                    thread = ep.control.query(
                        f"SELECT THREAD_ID FROM performance_schema.threads WHERE PROCESSLIST_ID={ep.worker.connection_id}"
                    )
                    if thread["status"] == "ok" and thread.get("sample"):
                        engine["contract_expected"] = thread["sample"][0][0]
                elif case["name"] == "CONNECTION_ID":
                    engine["contract_expected"] = str(ep.worker.connection_id)
                # Correctness is separate from timed result draining. Capture
                # can time out independently and never creates a false match.
                capture = case_dir / f"{name}.capture"
                if case.get("kind") == "dml":
                    mutation = ep.execute(sql, timeout=args.timeout, **query_options)
                    validation = (
                        ep.execute(
                            case["validation_sql"],
                            timeout=args.timeout,
                            capture=str(capture),
                        )
                        if mutation["status"] == "ok"
                        else dict(mutation)
                    )
                    validation["mutation"] = mutation
                else:
                    validation = ep.execute(
                        sql, timeout=args.timeout, capture=str(capture), **query_options
                    )
                engine["validation"] = validation
                if validation["status"] == "ok":
                    captures[name] = capture
                else:
                    engine["status"] = validation["status"]
                if not validate_only:
                    for metric in ("end_to_end", "analyze"):
                        if validation.get("requires_reconnect") or (
                            args.skip_invalid and validation["status"] != "ok"
                        ):
                            engine["metrics"][metric] = dict(
                                status="not_run",
                                reason="Validation did not complete successfully; timing repetitions are skipped.",
                            )
                            continue
                        if metric == "analyze" and (
                            query_options["prepared"] or case.get("kind") == "dml"
                        ):
                            engine["metrics"][metric] = dict(
                                status="unavailable",
                                reason="prepared/DML context measured only by full statement latency",
                            )
                            continue
                        measured_sql = (
                            "EXPLAIN ANALYZE FORMAT=TREE " + sql
                            if metric == "analyze"
                            else sql
                        )
                        records = []
                        for iteration in range(args.warmup + args.repeats):
                            setup_error = apply_case_setup(ep, case)
                            if setup_error:
                                records.append(setup_error)
                                break
                            response = ep.execute(
                                measured_sql,
                                timeout=args.timeout,
                                sample_limit=512 if metric == "analyze" else 4,
                                **query_options,
                            )
                            if metric == "analyze" and response["status"] == "ok":
                                engine["plan"] = response.get("sample")
                                response["statement"] = statement_metrics(ep, token)
                            response["warmup"] = iteration < args.warmup
                            records.append(response)
                            # Unsupported syntax is not a performance sample.
                            if response["status"] == "error" or response.get(
                                "requires_reconnect"
                            ):
                                break
                        engine["metrics"][metric] = dict(samples=records)
                    if engine["plan"]:
                        engine["parallel_evidence"] = parallel_evidence(
                            engine["plan"], name
                        )
                result["engines"][name] = engine
                for sql_cleanup in case.get("cleanup", []):
                    ep.execute(sql_cleanup)
            result["correctness"] = {}
            reference = captures.get("mysql")
            if case.get("expected_sql"):
                expected = case_dir / "expected.capture"
                expected_endpoint = endpoints.get("mysql") or next(
                    iter(endpoints.values())
                )
                expected_result = expected_endpoint.execute(
                    case["expected_sql"], timeout=args.timeout, capture=str(expected)
                )
                result["expected"] = expected_result
                if expected_result["status"] == "ok":
                    reference = expected
                    if "mysql" in captures:
                        result["correctness"]["mysql"] = compare_captures(
                            reference, captures["mysql"], "exact"
                        )
                else:
                    reference = None
            for name in ("serial", "parallel2"):
                if name not in captures:
                    result["correctness"][name] = dict(
                        status="unverified", reason="capture not completed"
                    )
                elif case["comparison"] == "contract":
                    result["correctness"][name] = check_contract(
                        captures[name],
                        case["name"],
                        result["engines"][name].get("contract_expected"),
                    )
                elif case["comparison"] == "environment":
                    result["correctness"][name] = dict(
                        status="environment_dependent",
                        reason="not eligible for equal-result speedup ranking",
                    )
                elif reference is not None:
                    columns = result["engines"][name]["validation"].get("columns", [])
                    float_result = len(columns) > 1 and columns[1]["type"] in {
                        "MYSQL_TYPE_DOUBLE",
                        "MYSQL_TYPE_FLOAT",
                    }
                    comparison = (
                        "exact"
                        if case["comparison"] == "numeric" and not float_result
                        else case["comparison"]
                    )
                    comparison = effective_comparison(comparison, columns)
                    result["correctness"][name] = compare_captures(
                        reference, captures[name], comparison, float_result
                    )
                else:
                    result["correctness"][name] = dict(status="no_mysql_reference")
            if (
                "serial" in captures
                and "parallel2" in captures
                and case["comparison"] not in {"contract", "environment"}
            ):
                columns = result["engines"]["serial"]["validation"].get("columns", [])
                float_result = len(columns) > 1 and columns[1]["type"] in {
                    "MYSQL_TYPE_DOUBLE",
                    "MYSQL_TYPE_FLOAT",
                }
                comparison = (
                    "exact"
                    if case["comparison"] == "numeric" and not float_result
                    else case["comparison"]
                )
                comparison = effective_comparison(comparison, columns)
                result["correctness"]["serial_vs_parallel2"] = compare_captures(
                    captures["serial"], captures["parallel2"], comparison, float_result
                )
            dump(completed, result)
            for capture in case_dir.glob("*.capture"):
                capture.unlink()
            for ordered in case_dir.glob("*.sorted"):
                ordered.unlink()
            print(
                f"[{index+1}/{len(cases)}] {case['id']}: "
                + ", ".join(f"{k}={v['status']}" for k, v in result["engines"].items())
                + " correctness="
                + str({k: v["status"] for k, v in result["correctness"].items()}),
                flush=True,
            )
    finally:
        for ep in endpoints.values():
            ep.close()


def sample_summary(metric, server=False, expected=3):
    records = metric.get("samples", [])
    samples = [x for x in records if not x.get("warmup")]
    values = [
        (x.get("statement", {}).get("statement_ms") if server else x.get("elapsed_ms"))
        for x in samples
        if x["status"] == "ok"
    ]
    values = [v for v in values if v is not None]
    status = (
        "ok"
        if len(values) == expected and len(samples) == expected
        else (
            "partial"
            if values
            else (
                "unavailable"
                if server and samples and all(x["status"] == "ok" for x in samples)
                else (
                    samples[-1]["status"]
                    if samples
                    else (
                        records[-1]["status"]
                        if records
                        else metric.get("status", "missing")
                    )
                )
            )
        )
    )
    return dict(
        status=status,
        samples=len(values),
        median_ms=statistics.median(values) if values else None,
        min_ms=min(values) if values else None,
        max_ms=max(values) if values else None,
        statuses=[x["status"] for x in samples],
    )


def report_result(path):
    result = json.loads(path.read_text())
    result["source_result_sha256"] = digest_file(path)
    post_path = path.with_name("validation.json")
    if post_path.exists() and post_path.stat().st_mtime_ns > path.stat().st_mtime_ns:
        post = json.loads(post_path.read_text())
        if (
            post["case_id"] != result["case_id"]
            or post["comparison"] != result["comparison"]
        ):
            raise ValueError("post-validation workload identity differs")
        for name, engine in result["engines"].items():
            other = post["engines"].get(name, {})
            scrub = lambda sql: re.sub(r"/\*feb:[a-z0-9]+\*/", "", sql)
            if scrub(engine.get("sql", "")) != scrub(other.get("sql", "")):
                raise ValueError("post-validation SQL differs for " + result["case_id"])
        result["initial_correctness"] = result["correctness"]
        result["correctness"] = post["correctness"]
        result["post_validation"] = post
        result["post_validation_sha256"] = digest_file(post_path)
    return result


def report(args):
    manifest = json.loads((args.directory / "manifest.json").read_text())
    context = json.loads((args.directory / "context.json").read_text())
    results = [
        report_result(p)
        for p in sorted((args.directory / "cases").glob("*/result.json"))
    ]
    rows = []
    for result in results:
        for metric in ("end_to_end", "analyze", "statement"):
            summaries = {
                name: sample_summary(
                    engine.get("metrics", {}).get(
                        "analyze" if metric == "statement" else metric, {}
                    ),
                    metric == "statement",
                    context["repeats"],
                )
                for name, engine in result["engines"].items()
            }
            for name, summary in summaries.items():
                correct = result["correctness"].get(name, {}).get("status", "reference")
                reference = summaries.get("mysql", {})
                ratio = None
                if (
                    name != "mysql"
                    and correct == "match"
                    and summary["status"] == "ok"
                    and reference.get("status") == "ok"
                    and summary["median_ms"] is not None
                    and reference.get("median_ms") is not None
                ):
                    ratio = reference["median_ms"] / summary["median_ms"]
                case = next(
                    case
                    for case in manifest["cases"]
                    if case["id"] == result["case_id"]
                )
                rows.append(
                    dict(
                        case_id=result["case_id"],
                        timeout_s=result.get("timeout_s", context["timeout_s"]),
                        category=result["category"],
                        oracle_class=case.get("oracle_class", "syntax_or_type_variant"),
                        engine=name,
                        metric=metric,
                        correctness=correct,
                        **summary,
                        mysql_over_mydb=ratio,
                        parallel_evidence=(
                            parallel_evidence(result["engines"][name].get("plan"), name)
                            if result["engines"][name].get("plan")
                            else None
                        ),
                    )
                )
    dump(
        args.directory / "summary.json",
        dict(
            complete=len(results) == len(manifest["cases"])
            and set(context["modes"]) == set(MODES),
            expected=len(manifest["cases"]),
            completed=len(results),
            rows=rows,
        ),
    )
    with (args.directory / "summary.csv").open("w") as output:
        writer = csv.DictWriter(
            output, fieldnames=list(rows[0]) if rows else ["case_id"]
        )
        writer.writeheader()
        writer.writerows(rows)
    statuses = collections.Counter(
        v["status"] for r in results for v in r["correctness"].values()
    )
    budget_label = (
        "分阶段执行："
        + ", ".join(
            f"{cohort['timeout_s']} 秒/{cohort['completed']} 项"
            for cohort in context["timeout_cohorts"]
        )
        if context.get("timeout_cohorts")
        else f"{context['timeout_s']} 秒"
    )
    text = [
        f"# 函数与表达式 {context['rows']:,} 行性能对照（{context['timestamp'][:10]}）",
        "",
        f"完成 {len(results)}/{len(manifest['cases'])} 个用例；清单包含 {manifest['registered_count']} 个 mydb 注册/文档函数及 {manifest['official_count']} 个 MySQL 官方函数/运算符条目。",
        "",
        f"主输入表 {context['rows']:,} 行（lookup 辅助表 10 行）；单客户端；查询期限{budget_label}；预热 {context['warmup']} 次，测量 {context['repeats']} 次。MySQL 8.0.46 串行与 mydb 串行、请求并行度 2 比较。",
        "",
        "完整 SQL 时间包含协议和结果解码。ANALYZE 是带观测开销、抑制原结果传输的独立执行；statement 指该执行的 Performance Schema 整句时间，不能视为纯函数时间。",
        "",
        f"正确性状态（包含两端及 mydb 内部对照）：`{dict(statuses)}`。",
        "",
        "## 分类别结果",
        "",
        f"仅结果匹配且 {context['repeats']} 次测量均成功的项目计算倍数。倍数为 MySQL/mydb，中位数大于 1 表示 mydb 更快。sys 映射函数单独分组。",
        "",
        "| 类别 | mydb 模式 | 口径 | 有效项 | mydb 更快 | mydb 更慢 | 倍数中位数 |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: |",
    ]

    def category_key(row):
        category = row["category"] + (
            "/sys_mapped" if row["oracle_class"] == "mysql80_mapped" else ""
        )
        category += f" / {row['timeout_s']}s"
        return category, row["engine"], row["metric"]

    for (category, engine, metric), group in itertools.groupby(
        sorted([r for r in rows if r["mysql_over_mydb"] is not None], key=category_key),
        key=category_key,
    ):
        ratios = [r["mysql_over_mydb"] for r in group]
        text.append(
            f"| {category} | {engine} | {metric} | {len(ratios)} | {sum(x>1 for x in ratios)} | {sum(x<1 for x in ratios)} | {statistics.median(ratios):.3f} |"
        )
    text.extend(
        [
            "",
            "## 不匹配及未完成校验",
            "",
            "| 用例 | 模式 | 状态 |",
            "| --- | --- | --- |",
        ]
    )
    for r in results:
        for name, check in r["correctness"].items():
            if check["status"] not in {
                "match",
                "contract_only",
                "contract_valid",
                "environment_dependent",
                "no_mysql_reference",
            }:
                text.append(f"| {r['case_id']} | {name} | {check['status']} |")
    text.extend(
        [
            "",
            "## 复现与原始证据",
            "",
            f"运行目录：`{args.directory}`。覆盖与 SQL 见 manifest.json；逐项执行计划、错误和校验差异见 cases/<id>/result.json；全部计时汇总见 summary.csv/summary.json。",
            "",
            "不支持、语义不同、外部依赖、超时和未验证项均保留，排除出有效倍数。未作数据库内核修复或基于用例的实现优化。",
            "",
        ]
    )
    (args.directory / "report.md").write_text("\n".join(text))
    print(
        f"report: {args.directory / 'report.md'} ({len(results)}/{len(manifest['cases'])})"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=["inventory", "prepare", "validate", "run", "report"]
    )
    parser.add_argument("--directory", type=Path, default=DEFAULT_DIR)
    parser.add_argument(
        "--worker",
        type=Path,
        default=ROOT / "target/release/function_expression_worker",
    )
    parser.add_argument("--mysql-cli", default="/opt/homebrew/opt/mysql@8.0/bin/mysql")
    parser.add_argument("--database", default="feb")
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--timeout", type=float, default=1)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--modes", nargs="+", choices=list(MODES), default=list(MODES))
    parser.add_argument("--filter")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--rerun", action="store_true")
    parser.add_argument(
        "--skip-invalid",
        action="store_true",
        help="Skip timing repetitions after failed or timed-out validation",
    )
    args = parser.parse_args()
    if args.rows <= 0 or args.timeout <= 0 or args.repeats <= 0 or args.warmup < 0:
        parser.error("invalid row count/timing parameters")
    if not re.fullmatch(r"[a-zA-Z][a-zA-Z0-9_]*", args.database):
        parser.error("database must be a simple SQL identifier")
    args.directory = args.directory.resolve()
    if not args.directory.is_relative_to(Path("/tmp/mydb").resolve()):
        parser.error("generated artifacts must be inside /tmp/mydb")
    args.directory.mkdir(parents=True, exist_ok=True)
    locks = []
    if args.command in {"prepare", "validate", "run"}:
        # Different output folders must never accidentally benchmark the same
        # server concurrently. Keep descriptors alive until process exit.
        for port in sorted(MODES[name][0] for name in args.modes):
            lock = Path(f"/tmp/mydb/function-expression-port-{port}.lock").open("a+")
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                parser.error(f"port {port} already belongs to another benchmark run")
            locks.append(lock)
            lock.seek(0)
            lock.truncate()
            lock.write(str(os.getpid()))
            lock.flush()

    def interrupted(signum, frame):
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, interrupted)
    if args.command == "inventory":
        manifest = make_inventory(args.directory)
        print(
            f"{len(manifest['cases'])} workloads; {manifest['registered_count']} mydb functions; {manifest['official_count']} MySQL reference entries"
        )
    elif args.command == "prepare":
        prepare(args)
    elif args.command == "validate":
        run(args, True)
    elif args.command == "run":
        run(args)
    else:
        report(args)


if __name__ == "__main__":
    main()
