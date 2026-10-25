#!/usr/bin/env python3
"""Full-text performance suite: scale, ngram, ranking, concurrent reads/writes, DML.

Dependency: pip install 'PyMySQL[rsa]==1.1.1'
Start the server separately. Example of a bounded local verification:
  python3 scripts/ft_bench_suite.py --port 4837 --iters 3 --duration 3 \
    --scales 10000,100000,1000000 --output /tmp/mydb/ft-suite.json

All selected scenarios must pass their correctness checks. SQL errors fail the
run. Each run owns a unique database; --keep-data retains it for investigation.
See docs-internal/fulltext-performance-plan.md for workloads and measurement limits.
"""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import math
from pathlib import Path
import re
import statistics
import sys
import threading
import time

import ft_bench as base


SCENARIOS = ("baseline", "scale", "ngram", "ranking", "concurrency", "dml")
ZH_DOCS = (
    "数据库全文检索系统 支持中文查询",
    "中文全文索引优化 引擎并发性能",
    "分布式数据库事务存储 引擎恢复",
    "向量搜索与相似度计算",
    "全文检索全文检索 支持短语",
    "中文检索 引擎测试",
    "数据库，全文：索引 包含标点",
    "database engine 中文数据库",
    "全隔文 测试边界",
    "全文检索 数据库优化",
)
RANK_DOCS = (
    "database database database engine storage",
    "database engine storage storage storage",
    "database storage recovery checkpoint planner engine execution transaction buffer",
    "engine storage",
    "storage recovery checkpoint",
    "database database engine",
    "database engine",
    "database design",
    "design storage",
    "storage systems query",
)


def log(message):
    print(f"[ft_suite] {message}", flush=True)


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def metrics(samples_ns, operations=None, elapsed=None):
    require(bool(samples_ns), "no completed samples")
    ordered = sorted(value / 1000 for value in samples_ns)
    result = {
        "operations": len(ordered) if operations is None else operations,
        "samples": len(ordered),
        "median_us": statistics.median(ordered),
        "p95_us": ordered[math.ceil(len(ordered) * 0.95) - 1],
        "p99_us": ordered[math.ceil(len(ordered) * 0.99) - 1],
        "max_us": ordered[-1],
    }
    if elapsed is not None:
        result.update(elapsed_seconds=elapsed, operations_per_second=result["operations"] / elapsed)
    return result


def ids(cursor, sql):
    return tuple(row[0] for row in base.execute(cursor, sql))


def match_sql(table, search, boolean=False, projection="id", order="id", limit=None):
    modifier = " IN BOOLEAN MODE" if boolean else ""
    expression = f"MATCH(body) AGAINST('{search}'{modifier})"
    sql = f"SELECT {projection} FROM {table} WHERE {expression} ORDER BY {order}"
    return sql + (f" LIMIT {limit}" if limit is not None else ""), expression


def check_ranked(rows, expected_count):
    require(len(rows) == expected_count, f"expected {expected_count} ranked rows, got {len(rows)}")
    require(len({row[0] for row in rows}) == len(rows), "duplicate ranked IDs")
    keys = []
    for row in rows:
        score = float(row[1])
        require(math.isfinite(score) and score > 0, f"invalid relevance score: {row}")
        keys.append((-score, row[0]))
    require(keys == sorted(keys), "relevance/tie-break ordering is incorrect")


def measure(cursor, label, sql, iters, check=None):
    warm = base.execute(cursor, sql)
    if check:
        check(warm)
    samples = []
    for _ in range(iters):
        elapsed, rows = base.timed_execute(cursor, sql)
        samples.append(elapsed)
        if check:
            check(rows)
    result = {"label": label, "sql": sql, **metrics(samples), "samples_us": [s / 1000 for s in samples]}
    log(f"{label}: median={result['median_us']:.1f} us p95={result['p95_us']:.1f} us")
    return result


def prepare(cursor, table, rows, args, document, ngram=False):
    log(f"prepare {table}: {rows} rows, parser={'ngram' if ngram else 'default'}")
    elapsed, _ = base.timed_execute(cursor, f"CREATE TABLE {table}(id BIGINT PRIMARY KEY,body TEXT)")
    result = {"table": table, "rows": rows, "create_table_ms": elapsed / 1e6}
    result["insert"] = base.load_rows(cursor, table, rows, args.batch_size, document)
    require(base.execute(cursor, f"SELECT COUNT(*) FROM {table}")[0][0] == rows, f"{table}: load count")
    sql = f"CREATE FULLTEXT INDEX ft ON {table}(body)" + (" WITH PARSER ngram" if ngram else "")
    log(f"build {table}: FULLTEXT on {rows} existing rows")
    elapsed, _ = base.timed_execute(cursor, sql)
    result["build_index"] = {"sql": sql, "elapsed_ms": elapsed / 1e6, "rows_per_second": rows * 1e9 / elapsed}
    ddl = base.execute(cursor, f"SHOW CREATE TABLE {table}")[0][1]
    require("FULLTEXT" in ddl.upper() and (not ngram or "ngram" in ddl.lower()), f"{table}: index DDL missing")
    result["ddl"] = ddl
    elapsed, _ = base.timed_execute(cursor, f"ANALYZE TABLE {table}")
    result["analyze_ms"] = elapsed / 1e6
    return result


def explain(cursor, sql):
    return [[str(value) for value in row] for row in base.execute(cursor, "EXPLAIN " + sql)]


def scale_doc(index):
    markers = " commonterm"
    if index % 10 == 0:
        markers += " mediumterm"
    if index % 1000 == 0:
        markers += " rareterm"
    return base.make_doc(index) + markers


def scale_stage(cursor, args, stage):
    stage["scales"] = []
    for count in args.scales:
        table = f"scale_{count}"
        result = prepare(cursor, table, count, args, scale_doc)
        stage["scales"].append(result)
        result["queries"] = []
        for term, step in (("rareterm", 1000), ("mediumterm", 10), ("commonterm", 1), ("missingterm", None)):
            expected = tuple(range(0, count, step)[:20]) if step else ()
            sql, expression = match_sql(table, term, limit=20)
            actual_count = base.execute(cursor, f"SELECT COUNT(*) FROM {table} WHERE {expression}")[0][0]
            expected_count = len(range(0, count, step)) if step else 0
            require(actual_count == expected_count, f"{table}/{term}: wrong cardinality {actual_count}")
            def check(rows, expected=expected):
                require(tuple(row[0] for row in rows) == expected, f"{table}: ordered membership mismatch")
            query = measure(cursor, f"{table}/{term}", sql, args.iters, check)
            query.update(matching_rows=actual_count, explain=explain(cursor, sql))
            result["queries"].append(query)
        for topk in args.topk:
            expression = "MATCH(body) AGAINST('rareterm mediumterm')"
            sql = (f"SELECT id,{expression} AS score FROM {table} WHERE {expression} "
                   f"ORDER BY score DESC,id LIMIT {topk}")
            matches = len(range(0, count, 10))
            result["queries"].append(measure(
                cursor, f"{table}/rank_top{topk}", sql, args.iters,
                lambda rows, n=min(topk, matches): check_ranked(rows, n),
            ))
        result["validated"] = True
        log(f"scale {count}: passed, index={result['build_index']['elapsed_ms']:.1f} ms")
        if not args.keep_data:
            base.execute(cursor, f"DROP TABLE {table}")


def ngrams(text):
    # The corpus uses no stopwords. Runs and Unicode character windows are
    # computed independently of the server's posting/index implementation.
    return [run[i:i + 2] for run in re.findall(r"[^\W_]+", text.lower()) for i in range(len(run) - 1)]


def contains_sequence(tokens, wanted):
    return any(tokens[i:i + len(wanted)] == wanted for i in range(len(tokens) - len(wanted) + 1))


def ngram_stage(cursor, args, stage):
    doc = lambda index: ZH_DOCS[index % len(ZH_DOCS)]
    stage.update(prepare(cursor, "chinese_docs", args.rows, args, doc, ngram=True))
    token_sets = [ngrams(body) for body in ZH_DOCS]
    predicates = (
        ("ngram natural", "全文", False, lambda ts: "全文" in ts),
        ("ngram natural union", "全文检索", False, lambda ts: bool(set(ts) & set(ngrams("全文检索")))),
        ("ngram required/excluded", "+全文 -索引", True, lambda ts: "全文" in ts and "索引" not in ts),
        ("ngram multi-token operand", "+数据库", True, lambda ts: contains_sequence(ts, ngrams("数据库"))),
        ("ngram phrase", '"全文检索"', True, lambda ts: contains_sequence(ts, ngrams("全文检索"))),
        ("ngram prefix", "全*", True, lambda ts: any(token.startswith("全") for token in ts)),
    )
    stage["queries"] = []
    for label, search, boolean, predicate in predicates:
        expected = tuple(i for i in range(args.rows) if predicate(token_sets[i % len(ZH_DOCS)]))
        full, _ = match_sql("chinese_docs", search, boolean)
        require(ids(cursor, full) == expected, f"{label}: ngram membership differs from character oracle")
        sql, _ = match_sql("chinese_docs", search, boolean, limit=20)
        def check(rows, expected=expected[:20]):
            require(tuple(row[0] for row in rows) == expected, f"{label}: LIMIT membership mismatch")
        result = measure(cursor, label, sql, args.iters, check)
        result.update(matching_rows=len(expected), explain=explain(cursor, sql),
                      matching_ids_sha256=hashlib.sha256(json.dumps(expected).encode("ascii")).hexdigest())
        stage["queries"].append(result)


def ranking_stage(cursor, args, stage):
    stage.update(prepare(cursor, "rank_docs", args.rows, args, lambda i: RANK_DOCS[i % len(RANK_DOCS)]))
    stage["queries"] = []
    for search, boolean in (("database", False), ("database engine", False),
                            ("database design", False), ("+database -design", True)):
        _, expression = match_sql("rank_docs", search, boolean)
        full = base.execute(cursor, f"SELECT id,{expression} AS score FROM rank_docs WHERE {expression}")
        oracle = sorted(full, key=lambda row: (-float(row[1]), row[0]))
        check_ranked(oracle, len(oracle))
        for topk in args.topk:
            expected = oracle[:topk]
            sql = (f"SELECT id,{expression} AS score FROM rank_docs WHERE {expression} "
                   f"ORDER BY score DESC,id LIMIT {topk}")
            def check(rows, expected=expected):
                check_ranked(rows, len(expected))
                require(tuple(row[0] for row in rows) == tuple(row[0] for row in expected),
                        f"{search}: top-K differs from complete-score oracle")
                require(all(math.isclose(float(a[1]), float(b[1]), rel_tol=1e-6, abs_tol=1e-9)
                            for a, b in zip(rows, expected)), f"{search}: projection scores differ")
            result = measure(cursor, f"rank/{search}/top{topk}", sql, args.iters, check)
            raw_order = sql.replace("ORDER BY score DESC,id", f"ORDER BY {expression} DESC,id")
            check(base.execute(cursor, raw_order))
            result.update(matching_rows=len(oracle), explain=explain(cursor, sql), oracle="all scores sorted in Python",
                          matching_ids_sha256=hashlib.sha256(json.dumps(sorted(row[0] for row in oracle)).encode("ascii")).hexdigest(),
                          topk_ids=[row[0] for row in expected])
            stage["queries"].append(result)
    base.execute(cursor, "CREATE TABLE rank_fixture(id INT PRIMARY KEY,body TEXT,FULLTEXT KEY ft(body))")
    fixture = (
        "database database database storage storage storage",
        "database storage storage storage storage storage",
        "database storage storage storage storage storage storage storage storage storage storage storage",
        "database design storage storage storage storage",
        "storage recovery checkpoint planner execution transaction",
    )
    for i, body in enumerate(fixture):
        cursor.execute("INSERT INTO rank_fixture VALUES(%s,%s)", (i, body))
    base.execute(cursor, "ANALYZE TABLE rank_fixture")
    scores = dict(base.execute(cursor, "SELECT id,MATCH(body) AGAINST('database') FROM rank_fixture"))
    database_scores = {str(key): float(value) for key, value in scores.items()}
    frequency = float(scores[0]) > float(scores[1])
    length_normalization = float(scores[1]) > float(scores[2])
    length_invariant = math.isclose(float(scores[1]), float(scores[2]), rel_tol=1e-6, abs_tol=1e-9)
    require(frequency, "term-frequency fixture failed")
    if args.engine == "mydb":
        require(length_normalization, "BM25 length-normalization fixture failed")
    else:
        require(length_invariant, "MySQL InnoDB TF-IDF length-invariance fixture failed")
    scores = dict(base.execute(cursor, "SELECT id,MATCH(body) AGAINST('database design') FROM rank_fixture"))
    require(float(scores[3]) > float(scores[1]), "BM25 rare-term fixture failed")
    stage["semantic_fixture"] = {
        "term_frequency": frequency, "length_normalization": length_normalization,
        "length_invariant": length_invariant, "rare_term": True,
        "model": "BM25" if args.engine == "mydb" else "InnoDB TF-IDF",
        "database_scores_by_id": database_scores,
        "database_design_scores_by_id": {str(key): float(value) for key, value in scores.items()},
    }


def dml_markers(chinese):
    return ("旧词", "新词", "插入", "回滚") if chinese else ("oldmarker", "newmarker", "insertmarker", "rollbackmarker")


def dml_stage(cursor, connection, args, stage):
    stage["languages"] = []
    changed = min(args.mutations, args.rows // 3)
    for chinese in (False, True):
        table = "dml_zh" if chinese else "dml_en"
        old, new, inserted, rollback = dml_markers(chinese)
        def body(marker):
            if marker == new:
                return f"{marker} 存储引擎并发优化 存储引擎" if chinese else f"{marker} storage storage recovery checkpoint"
            return f"{marker} 数据库全文检索" if chinese else f"{marker} database engine"
        result = prepare(cursor, table, args.rows, args, lambda _: body(old), ngram=chinese)
        stage["languages"].append(result)
        result["operations"] = []
        expected = {old: tuple(range(args.rows)), new: (), inserted: (), rollback: ()}

        def validate():
            require(base.execute(cursor, f"SELECT COUNT(*) FROM {table}")[0][0] == sum(map(len, expected.values())),
                    f"{table}: DML row count mismatch")
            for marker, wanted in expected.items():
                for boolean in (False, True):
                    sql, _ = match_sql(table, marker, boolean)
                    require(ids(cursor, sql) == wanted, f"{table}: stale/missing marker {marker}, boolean={boolean}")
            phrase = '"全文检索"' if chinese else '"database engine"'
            sql, _ = match_sql(table, phrase, True)
            require(ids(cursor, sql) == tuple(sorted(expected[old] + expected[inserted])),
                    f"{table}: stale/missing phrase positions after DML")
            if not chinese:
                sql, _ = match_sql(table, "newmar*", True)
                require(ids(cursor, sql) == expected[new], f"{table}: stale/missing prefix after DML")

        validate()
        for operation, begin, end in (("update", 0, changed), ("delete", changed, 2 * changed)):
            samples = []
            affected = 0
            for start in range(begin, end, args.batch_size):
                stop = min(start + args.batch_size, end)
                if operation == "update":
                    sql = cursor.mogrify(f"UPDATE {table} SET body=%s WHERE id>=%s AND id<%s", (body(new), start, stop))
                else:
                    sql = f"DELETE FROM {table} WHERE id>={start} AND id<{stop}"
                elapsed, _ = base.timed_execute(cursor, sql)
                require(cursor.rowcount == stop - start, f"{table}: {operation} affected-row mismatch")
                affected += cursor.rowcount
                samples.append(elapsed)
            result["operations"].append({"label": operation, "affected_rows": affected,
                                         **metrics(samples, affected, sum(samples) / 1e9)})
            expected[old] = tuple(range(changed if operation == "update" else 2 * changed, args.rows))
            expected[new] = tuple(range(changed))
            validate()
        values = [(args.rows + i, body(inserted)) for i in range(changed)]
        samples = []
        for start in range(0, changed, args.batch_size):
            batch = values[start:start + args.batch_size]
            sql = f"INSERT INTO {table} VALUES " + ",".join(cursor.mogrify("(%s,%s)", row) for row in batch)
            elapsed, _ = base.timed_execute(cursor, sql)
            require(cursor.rowcount == len(batch), f"{table}: reinsert affected-row mismatch")
            samples.append(elapsed)
        result["operations"].append({"label": "reinsert", "affected_rows": changed,
                                     **metrics(samples, changed, sum(samples) / 1e9)})
        expected[inserted] = tuple(range(args.rows, args.rows + changed))
        validate()
        connection.begin()
        try:
            cursor.execute(f"UPDATE {table} SET body=%s WHERE id=0", (body(rollback),))
            cursor.execute(f"DELETE FROM {table} WHERE id=%s", (args.rows - 1,))
            cursor.execute(f"INSERT INTO {table} VALUES(%s,%s)", (args.rows + changed, body(rollback)))
        finally:
            connection.rollback()
        validate()
        result["rollback_validated"] = True
        result["queries_after_dml"] = []
        for marker, wanted in expected.items():
            sql, _ = match_sql(table, marker, limit=20)
            def check(rows, wanted=wanted[:20]):
                require(tuple(row[0] for row in rows) == wanted, f"{table}: post-DML LIMIT mismatch")
            result["queries_after_dml"].append(measure(cursor, f"{table}/{marker}", sql, args.iters, check))
        result["validated"] = True
        log(f"{table}: {changed} updates + deletes + reinserts, rollback passed")


def connect_worker(args, database):
    import pymysql

    return pymysql.connect(host=args.host, port=args.port, user=args.user, password=args.password,
                           database=database, charset="utf8mb4", autocommit=True, connect_timeout=10,
                           read_timeout=args.timeout, write_timeout=args.timeout)


def concurrent_window(args, database, table, readers, writers, chinese):
    total = readers + writers
    barrier = threading.Barrier(total + 1, timeout=args.timeout)
    gate = threading.Event()
    stop = threading.Event()
    window = {}
    steady = "稳定" if chinese else "steadyterm"
    phases = ("甲态", "乙态") if chinese else ("phasealpha", "phasebeta")
    fixed = tuple(range(min(20, args.rows)))

    def worker(index):
        writer = index >= readers
        record = {"worker": index, "role": "writer" if writer else "reader", "samples_ns": [], "errors": []}
        connection = None
        try:
            connection = connect_worker(args, database)
            with connection.cursor() as cursor:
                queries = []
                for marker in ((steady, *phases) if writers else (steady,)):
                    sql, _ = match_sql(table, marker, projection="id,body", limit=20)
                    queries.append((marker, sql))
                    base.execute(cursor, sql)
                barrier.wait()
                require(gate.wait(args.timeout), "concurrent start gate timeout")
                record["first_started"] = time.perf_counter()
                iteration = 0
                while not stop.is_set() and time.perf_counter() < window["deadline"]:
                    if writer:
                        row_id = args.rows + index - readers
                        body = f"{phases[iteration % 2]} 数据库全文检索" if chinese else f"{phases[iteration % 2]} database engine"
                        delete_sql = f"DELETE FROM {table} WHERE id={row_id}"
                        insert_sql = cursor.mogrify(f"INSERT INTO {table} VALUES(%s,%s)", (row_id, body))
                        started = time.perf_counter_ns()
                        connection.begin()
                        try:
                            base.execute(cursor, delete_sql)
                            require(cursor.rowcount == 1, "writer lost its owned row")
                            base.execute(cursor, insert_sql)
                            connection.commit()
                        except BaseException:
                            connection.rollback()
                            raise
                        elapsed = time.perf_counter_ns() - started
                    else:
                        marker, sql = queries[iteration % len(queries)]
                        elapsed, rows = base.timed_execute(cursor, sql)
                        if marker == steady:
                            require(tuple(row[0] for row in rows) == fixed, "reader lost stable documents")
                        else:
                            require(all(args.rows <= row[0] < args.rows + writers and marker in row[1] for row in rows),
                                    "reader observed a stale posting or invalid writer row")
                            require(len({row[0] for row in rows}) == len(rows), "reader observed duplicate writer IDs")
                    record["samples_ns"].append(elapsed)
                    iteration += 1
                record["last_finished"] = time.perf_counter()
        except BaseException as error:
            record["errors"].append(f"{type(error).__name__}: {error}")
            stop.set()
            barrier.abort()
            gate.set()
        finally:
            if connection is not None:
                connection.close()
        return record

    with ThreadPoolExecutor(max_workers=total) as pool:
        futures = [pool.submit(worker, i) for i in range(total)]
        try:
            barrier.wait()
            window["started"] = time.perf_counter()
            window["deadline"] = window["started"] + args.duration
        except threading.BrokenBarrierError:
            stop.set()
        finally:
            gate.set()
        records = [future.result() for future in futures]
    errors = [error for record in records for error in record["errors"]]
    finished = max((record.get("last_finished", 0) for record in records), default=0)
    elapsed = max(finished - window.get("started", finished), 1e-9)
    result = {"readers": readers, "writers": writers, "configured_seconds": args.duration,
              "elapsed_seconds": elapsed, "errors": errors, "workers": []}
    for role in ("reader", "writer"):
        selected = [record for record in records if record["role"] == role]
        if not selected:
            continue
        samples = [sample for record in selected for sample in record["samples_ns"]]
        if samples:
            result[role] = metrics(samples, elapsed=elapsed)
        for record in selected:
            result["workers"].append({key: value for key, value in record.items() if key != "samples_ns"} | {
                "completed": len(record["samples_ns"]),
                "samples_us": [sample / 1000 for sample in record["samples_ns"]],
                "latency": metrics(record["samples_ns"]) if record["samples_ns"] else None,
            })
    if not errors:
        require(all(record["samples_ns"] for record in records), "a concurrent worker completed no operations")
        overlap = min(record["last_finished"] for record in records) - max(record["first_started"] for record in records)
        require(overlap > 0, "workers did not overlap")
        result["worker_overlap_seconds"] = overlap
    return result


def concurrency_stage(cursor, args, report, stage):
    stage["languages"] = []
    for chinese in (False, True):
        table = "concurrent_zh" if chinese else "concurrent_en"
        steady = "稳定词 数据库全文检索" if chinese else "steadyterm database engine"
        result = prepare(cursor, table, args.rows, args, lambda _: steady, ngram=chinese)
        stage["languages"].append(result)
        result["windows"] = []
        for readers, writers in [*((n, 0) for n in args.concurrency), (max(args.concurrency), args.writers)]:
            for index in range(writers):
                body = "甲态 数据库全文检索" if chinese else "phasealpha database engine"
                cursor.execute(f"INSERT INTO {table} VALUES(%s,%s)", (args.rows + index, body))
            log(f"{table}: {readers} readers + {writers} writers, {args.duration}s")
            window = concurrent_window(args, report["database"], table, readers, writers, chinese)
            result["windows"].append(window)
            require(not window["errors"], f"{table}: concurrent errors: {window['errors']}")
            require(base.execute(cursor, f"SELECT COUNT(*) FROM {table}")[0][0] == args.rows + writers,
                    f"{table}: final concurrent row count")
            stable_sql, _ = match_sql(table, "稳定" if chinese else "steadyterm")
            require(ids(cursor, stable_sql) == tuple(range(args.rows)), f"{table}: final stable membership")
            heap_rows = base.execute(cursor, f"SELECT id,body FROM {table} WHERE id>={args.rows} ORDER BY id")
            for marker in (("甲态", "乙态") if chinese else ("phasealpha", "phasebeta")):
                sql, _ = match_sql(table, marker, projection="id,body")
                actual = base.execute(cursor, sql)
                require(actual == tuple(row for row in heap_rows if marker in row[1]),
                        f"{table}: final posting/heap membership mismatch")
            if writers:
                base.execute(cursor, f"DELETE FROM {table} WHERE id>={args.rows}")
            log(f"{table}: QPS={window['reader']['operations_per_second']:.1f}, "
                f"writer TPS={window.get('writer', {}).get('operations_per_second', 0):.1f}")
        result["validated"] = True


def positive_list(text):
    try:
        values = sorted(set(base.positive_int(value) for value in text.split(",")))
    except (ValueError, argparse.ArgumentTypeError) as error:
        raise argparse.ArgumentTypeError("expected comma-separated positive integers") from error
    return values


def scenario_list(text):
    values = text.split(",")
    if any(value not in SCENARIOS for value in values) or len(set(values)) != len(values):
        raise argparse.ArgumentTypeError("choose distinct scenarios from " + ",".join(SCENARIOS))
    return values


def positive_seconds(text):
    value = float(text)
    if not math.isfinite(value) or value <= 0:
        raise argparse.ArgumentTypeError("duration must be finite and positive")
    return value


def configure(parser):
    parser.add_argument("--engine", choices=("mydb", "mysql80"), default="mydb",
                        help="select scoring-contract checks; query workload is unchanged (default: mydb)")
    parser.add_argument("--scenarios", type=scenario_list, default=",".join(SCENARIOS))
    parser.add_argument("--scales", type=positive_list, default="10000,100000,1000000")
    parser.add_argument("--topk", type=positive_list, default="1,20,100")
    parser.add_argument("--concurrency", type=positive_list, default="1,4,8")
    parser.add_argument("--writers", type=base.positive_int, default=2)
    parser.add_argument("--duration", type=positive_seconds, default=10,
                        help="seconds per concurrent window, connections/warmup excluded (default: 10)")
    parser.add_argument("--mutations", type=base.positive_int, default=1000,
                        help="rows per update/delete/reinsert phase, capped at rows/3 (default: 1000)")


def benchmark_suite(connection, args, report):
    report["suite"] = {
        "version": 2, "engine": args.engine, "selected": args.scenarios, "scales": args.scales, "topk": args.topk,
        "concurrency": args.concurrency, "writers": args.writers, "duration": args.duration,
        "mutation_rows": min(args.mutations, args.rows // 3),
        "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "baseline_script_sha256": hashlib.sha256(Path(base.__file__).read_bytes()).hexdigest(),
        "client_cpu_note": "Python threads share the GIL; throughput can be client-limited",
        "stages": [],
    }
    with connection.cursor() as cursor:
        report["server_version"] = base.execute(cursor, "SELECT VERSION()")[0][0]
        version = report["server_version"]
        require((args.engine == "mysql80" and version.startswith("8.0.") and "mydb" not in version.lower())
                or (args.engine == "mydb" and "mydb" in version.lower()),
                f"--engine {args.engine} does not match server version {version}")
        report["server_settings"] = {
            "transaction_isolation": base.execute(cursor, "SELECT @@transaction_isolation")[0][0],
            "optimizer_switch": base.execute(cursor, "SELECT @@optimizer_switch")[0][0],
        }
        for scenario in args.scenarios:
            stage = {"name": scenario, "passed": False}
            report["suite"]["stages"].append(stage)
            log(f"START {scenario}")
            started = time.perf_counter()
            if scenario == "baseline":
                base.benchmark(connection, args, report)
            elif scenario == "scale":
                scale_stage(cursor, args, stage)
            elif scenario == "ngram":
                ngram_stage(cursor, args, stage)
            elif scenario == "ranking":
                ranking_stage(cursor, args, stage)
            elif scenario == "concurrency":
                concurrency_stage(cursor, args, report, stage)
            elif scenario == "dml":
                dml_stage(cursor, connection, args, stage)
            stage.update(passed=True, wall_seconds=time.perf_counter() - started)
            log(f"PASS {scenario}: {stage['wall_seconds']:.2f}s")


def main():
    args = base.parse_args(configure, __doc__)
    if args.rows < 4:
        raise SystemExit("--rows must be at least 4 for the full-text suite")
    return base.run(args, benchmark_suite)


if __name__ == "__main__":
    sys.exit(main())
