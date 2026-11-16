#!/usr/bin/env python3
"""Cache complete DuckDB ClickBench results and check escaped MySQL batch output."""

import argparse
import collections
import csv
import decimal
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import uuid

from clickbench_results import parse_workload, positions, split_top

FORMAT_VERSION = 2
FLOAT_TYPES = {"FLOAT", "DOUBLE", "REAL"}
INTEGER_TYPES = {"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UTINYINT",
                 "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT"}
csv.field_size_limit(sys.maxsize)


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                     separators=(",", ":")).encode()).hexdigest()


def read_queries(path):
    queries = [line.strip() for line in path.read_text().splitlines()
               if line.strip() and not line.lstrip().startswith("--")]
    if len(queries) != 43:
        raise ValueError(f"expected 43 workload queries, got {len(queries)}")
    return queries


def duckdb_sql(sql):
    # The pinned MySQL workload uses byte LENGTH and hour DATE_FORMAT. The
    # upstream DuckDB Q43 uses minutes instead, so it is not a valid oracle.
    sql = re.sub(r"\blength\((URL|Referer)\)", r"strlen(\1)", sql, flags=re.I)
    sql = sql.replace("DATE_FORMAT(EventTime, '%Y-%m-%d %H:00:00')",
                      "strftime(EventTime, '%Y-%m-%d %H:00:00')")
    # MySQL's default SQL mode consumes backslashes in SQL literals; DuckDB
    # does not. In the pinned workload '\1' is the literal '1', NOT a capture
    # reference. Adapt the actual SQL, rather than silently changing Q29.
    escapes = {'0': '\0', 'b': '\b', 'n': '\n', 'r': '\r', 't': '\t', 'Z': '\x1a'}
    def literal(match):
        value = match[0][1:-1].replace("''", "'")
        value = re.sub(r"\\(.)", lambda m: '\\' + m[1] if m[1] in '%_' else escapes.get(m[1], m[1]), value)
        return "'" + value.replace("'", "''") + "'"
    sql = re.sub(r"'(?:[^'\\]|\\.|'')*'", literal, sql)
    # MySQL replaces all occurrences by default; DuckDB only the first.
    sql = re.sub(r"(REGEXP_REPLACE\(Referer, '(?:[^']|'')*', '(?:[^']|'')*')\)",
                 r"\1, 'g')", sql, flags=re.I)
    return sql


def context(args, queries):
    return dict(format_version=FORMAT_VERSION, workload_sha256=digest(queries),
                schema_sha256=hashlib.sha256(args.schema_file.read_bytes()).hexdigest(),
                dataset_id=args.dataset_id, row_count=args.row_count,
                semantics='utf8mb4_0900_bin/raw-tsv/UTC/mysql-default-escapes-v2')


def read_expected(path, expected_context, sql):
    entry = json.loads(path.read_text())
    checksum = entry.pop("sha256")
    if digest(entry) != checksum:
        raise ValueError("expected cache checksum mismatch")
    if entry["context"] != expected_context or entry["sql"] != sql:
        raise ValueError("stale expected cache: workload/schema/dataset/row count changed")
    if entry["duckdb_sql"] != duckdb_sql(sql):
        raise ValueError("stale DuckDB SQL adapter")
    types = entry["types"]
    if not types or not all(isinstance(t, str) for t in types):
        raise ValueError("invalid expected column types")
    if not all(isinstance(row, list) and len(row) == len(types)
               and all(v is None or isinstance(v, str) for v in row) for row in entry["rows"]):
        raise ValueError("invalid expected rows")
    return entry


class DuckDB:
    def __init__(self, args):
        self.args = args
        if not args.duckdb_database.is_file():
            raise ValueError(f"DuckDB database not found: {args.duckdb_database}")
        self.version = self.execute("SELECT version()", "-csv").strip()
        rows = json.loads(self.execute("SELECT count(*) AS n FROM hits", "-json"))[0]["n"]
        if rows != args.row_count:
            raise ValueError(f"DuckDB hits has {rows} rows; mydb/expected dataset has {args.row_count}")

    def execute(self, sql, mode):
        settings = ""
        if getattr(self.args, "memory_limit", None):
            limit = self.args.memory_limit.replace("'", "''")
            settings += f"SET memory_limit='{limit}'; "
        if getattr(self.args, "threads", None):
            settings += f"SET threads={int(self.args.threads)}; "
        result = subprocess.run([self.args.duckdb_bin, "-readonly", "-batch", "-bail",
                                 mode, "-noheader", str(self.args.duckdb_database), "-c", settings + sql],
                                capture_output=True, timeout=self.args.timeout)
        if result.returncode:
            raise ValueError(f"DuckDB failed: {result.stderr.decode(errors='replace').strip()}")
        # Universal newline conversion would corrupt embedded carriage returns.
        return result.stdout.decode("utf-8")

    def capture(self, path, ctx, sql):
        translated = duckdb_sql(sql).strip().rstrip(";")
        columns = json.loads(self.execute("DESCRIBE " + translated, "-json"))
        marker = "__clickbench_null_" + uuid.uuid4().hex
        output = self.execute(f"COPY ({translated}) TO STDOUT "
                              f"(FORMAT CSV, HEADER false, NULL '{marker}')", "-csv")
        rows = [[None if v == marker else v for v in row]
                for row in csv.reader(io.StringIO(output, newline=""))]
        entry = dict(context=ctx, sql=sql, duckdb_sql=duckdb_sql(sql),
                     duckdb_version=self.version, types=[c["column_type"] for c in columns], rows=rows)
        entry["sha256"] = digest(entry)
        path.parent.mkdir(parents=True, exist_ok=True)
        # Only complete successful captures become visible, including on reruns.
        with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as out:
            temporary = Path(out.name)
            try:
                json.dump(entry, out, ensure_ascii=False)
                out.close()
                os.replace(temporary, path)
            finally:
                temporary.unlink(missing_ok=True)
        return read_expected(path, ctx, sql)

    def verify_limit(self, entry, actual):
        """Verify candidate values AND their admissible ranks on the full oracle.

        The filter stays outside the windows: filtering candidates before RANK
        would make a fabricated low-ranked row look like a valid top result.
        Duplicate rows consume distinct witnesses. OFFSET cuts through ties too.
        DuckDB's configured memory limit/spill applies to the full relation.
        """
        sql, types = duckdb_sql(entry['sql']).strip().rstrip(';'), entry['types']
        limit_at = next(positions(sql, ' LIMIT '), None)
        if limit_at is None or not actual or len(actual) != len(entry['rows']):
            return False, 'result differs (not a verifiable LIMIT alternative)'
        limit = re.fullmatch(r'(\d+)(?:\s+OFFSET\s+(\d+))?', sql[limit_at + 7:], re.I)
        if not limit or any(len(row) != len(types) for row in actual):
            return False, 'unsupported LIMIT shape or column count'
        offset = int(limit[2] or 0)
        order_at = next(positions(sql, ' ORDER BY '), limit_at)
        base = sql[:order_at]
        order = sql[order_at + 10:limit_at] if order_at < limit_at else ''
        aliases = [f'__c{i}' for i in range(len(types))]
        hidden, ordering = [], []
        for i, item in enumerate(split_top(order) if order else []):
            match = re.fullmatch(r'(.*?)(?:\s+(ASC|DESC))?', item, re.I)
            expr, direction = match[1], match[2] or 'ASC'
            if expr.isdigit():
                ordering.append(f'__c{int(expr)-1} {direction}')
            else:
                hidden.append(f'{expr} AS __o{i}')
                aliases.append(f'__o{i}')
                ordering.append(f'__o{i} {direction}')
        if hidden:
            from_at = next(positions(base, ' FROM '))
            base = base[:from_at] + ', ' + ', '.join(hidden) + base[from_at:]
        rank = 'RANK() OVER (ORDER BY ' + ', '.join(ordering) + ')' if ordering else '1'
        peers = ('PARTITION BY ' + ', '.join(o.rsplit(' ', 1)[0] for o in ordering)) if ordering else ''
        predicates = []
        for row in actual:
            cells = []
            for i, (value, type_name) in enumerate(zip(row, types)):
                # Hex UTF-8 also handles NUL without passing a NUL in argv.
                literal = 'NULL' if value is None else f"CAST(decode(from_hex('{value.encode().hex()}')) AS {type_name})"
                if type_name in FLOAT_TYPES and value is not None:
                    cells.append(f'abs(__c{i} - {literal}) <= greatest(0.00005, abs(__c{i}) * 1e-12)')
                else:
                    cells.append(f'__c{i} IS NOT DISTINCT FROM {literal}')
            predicates.append('(' + ' AND '.join(cells) + ')')
        projected = ', '.join(f'__c{i}' for i in range(len(types)))
        query = (f'WITH src({", ".join(aliases)}) AS ({base}), ranked AS '
                 f'(SELECT *, {rank} AS __rank, COUNT(*) OVER ({peers}) AS __peers FROM src) '
                 f'SELECT {projected}, __rank, __peers, COUNT(*) AS __copies FROM ranked '
                 f'WHERE {" OR ".join(predicates)} GROUP BY ALL')
        marker = '__null_' + uuid.uuid4().hex
        output = self.execute(f"COPY ({query}) TO STDOUT (FORMAT CSV, HEADER false, NULL '{marker}')", '-csv')
        witnesses = []
        for row in csv.reader(io.StringIO(output, newline='')):
            values = [None if v == marker else v for v in row[:-3]]
            first, peers_count, copies = map(int, row[-3:])
            witnesses.extend([(values, first, first + peers_count - 1)] * min(copies, len(actual)))
        matches = {}
        def assign(i, seen):
            for j, (values, first, last) in enumerate(witnesses):
                if j in seen or (ordering and not first <= offset + i + 1 <= last):
                    continue
                if all(cell_equal(a, b, t) for a, b, t in zip(values, actual[i], types)):
                    seen.add(j)
                    if j not in matches or assign(matches[j], seen):
                        matches[j] = i
                        return True
            return False
        ok = all(assign(i, set()) for i in range(len(actual)))
        return ok, ('LIMIT alternative verified against full DuckDB values, ranks and multiplicities'
                    if ok else 'full DuckDB oracle rejected values, ranks or multiplicities')


def mysql_rows(path):
    text = path.read_bytes().decode("utf-8")
    if not text:
        return []
    if not text.endswith("\n"):
        raise ValueError("incomplete MySQL batch output (missing final newline)")
    escapes = {"0": "\0", "n": "\n", "t": "\t", "r": "\r", "\\": "\\"}

    def cell(value):
        if value == "NULL":
            return None
        def unescape(match):
            if match[1] not in escapes:
                raise ValueError(f"invalid MySQL batch escape: {match[0]!r}")
            return escapes[match[1]]
        return re.sub(r"\\(.)", unescape, value)

    return [[cell(v) for v in line.split("\t")] for line in text[:-1].split("\n")]


def cell_equal(before, after, type_name):
    if before is None or after is None:
        return before is after
    if type_name in INTEGER_TYPES or type_name in FLOAT_TYPES or type_name.startswith("DECIMAL("):
        try:
            left, right = decimal.Decimal(before), decimal.Decimal(after)
        except decimal.InvalidOperation:
            return False
        if not left.is_finite() or not right.is_finite():
            return left == right
        if type_name in FLOAT_TYPES:
            # MySQL AVG(integer) is commonly rendered to four decimal places.
            tolerance = max(decimal.Decimal("0.00005"), abs(left) * decimal.Decimal("1e-12"))
            return abs(left - right) <= tolerance
        return left == right
    return before == after


def compare_rows(entry, actual):
    expected, types, sql = entry["rows"], entry["types"], entry["sql"]
    if len(expected) != len(actual):
        return False, f"row count differs: expected {len(expected)}, actual {len(actual)}"
    if any(len(row) != len(types) for row in actual):
        return False, "column count differs (use escaped MySQL batch output, without --raw)"
    if any(v == "NULL" for row in expected for v in row):
        return False, "literal text NULL is ambiguous in MySQL batch output; typed capture required"

    def equal(left, right):
        return all(cell_equal(a, b, t) for a, b, t in zip(left, right, types))

    has_order = bool(list(positions(sql, " ORDER BY ")))
    ordering = None
    if has_order:
        try:
            ordering = parse_workload(sql)[3]
        except (KeyError, ValueError, IndexError):
            pass  # E.g. SELECT * / ORDER BY a column not in the projection.
    if has_order and ordering is None:
        ok = all(equal(a, b) for a, b in zip(expected, actual))
        return ok, "ordered rows match" if ok else "ordered rows differ; LIMIT ties may require a live oracle"
    if ordering and any(not all(cell_equal(a[i], b[i], types[i]) for i in ordering)
                        for a, b in zip(expected, actual)):
        return False, "ORDER BY ranks differ"

    # Match within each equal-rank block, or as a multiset without ORDER BY.
    # Bipartite matching preserves duplicate multiplicities with numeric tolerance.
    def match_block(before, after):
        if collections.Counter(map(tuple, before)) == collections.Counter(map(tuple, after)):
            return True
        matches = {}
        def assign(i, seen):
            for j, row in enumerate(after):
                if j not in seen and equal(before[i], row):
                    seen.add(j)
                    if j not in matches or assign(matches[j], seen):
                        matches[j] = i
                        return True
            return False
        return all(assign(i, set()) for i in range(len(before)))

    start = 0
    for end in range(1, len(expected) + 1):
        boundary = end == len(expected) or (ordering and any(
            not cell_equal(expected[start][i], expected[end][i], types[i]) for i in ordering))
        if boundary:
            if not match_block(expected[start:end], actual[start:end]):
                return False, "result values differ; LIMIT ties/unordered LIMIT may require a live oracle"
            start = end
    return True, "rows match (numeric formatting and equal-rank permutations normalized)"


def selected_queries(value):
    numbers = [int(q) for q in value.split(",")]
    if not numbers or len(set(numbers)) != len(numbers) or any(q < 1 or q > 43 for q in numbers):
        raise ValueError("queries must be unique comma-separated numbers in 1..43")
    return numbers


def run(args):
    queries = read_queries(args.queries_file)
    selected = selected_queries(args.queries)
    ctx = context(args, queries)
    oracle = None
    expected = {}
    issues = {}
    timings = {}
    if args.mode == "check":
        with (args.actual_dir / "query_times.csv").open(newline="") as src:
            for row in csv.DictReader(src):
                key = (int(row["query"]), int(row["run"]))
                if key in timings:
                    raise ValueError(f"duplicate query timing entry: {key}")
                timings[key] = row["status"]
    for q in selected:
        if args.mode == "check" and not any(timings.get((q, n)) == "ok" for n in range(1, args.runs + 1)):
            continue
        path = args.expected_dir / f"q{q:02}.json"
        try:
            try:
                expected[q] = read_expected(path, ctx, queries[q - 1])
                print(f"  Q{q:02}: reusing DuckDB cache", flush=True)
            except (OSError, ValueError, KeyError, TypeError):
                if args.duckdb_database is None:
                    raise
                if oracle is None:
                    oracle = DuckDB(args)
                print(f"  Q{q:02}: generating DuckDB cache", flush=True)
                expected[q] = oracle.capture(path, ctx, queries[q - 1])
        except FileNotFoundError as error:
            # A missing executable/database is an oracle error, never a cache skip.
            status = "missing_expected" if args.duckdb_database is None else "oracle_error"
            issues[q] = (status, str(error))
        except (OSError, ValueError, KeyError, TypeError, subprocess.TimeoutExpired) as error:
            issues[q] = ("invalid_expected", str(error))
    if args.mode == "generate":
        for q, issue in issues.items():
            print(f"  Q{q:02}: {issue[0]}: {issue[1]}", file=sys.stderr)
        return int(bool(issues))

    reports = []
    for q in selected:
        for run_no in range(1, args.runs + 1):
            row = dict(query_num=q, run=run_no, status="ok", expected_rows="", actual_rows="", message="")
            try:
                if timings.get((q, run_no)) != "ok":
                    row.update(status="query_failed", message=timings.get((q, run_no), "not executed"))
                else:
                    sql = (args.actual_dir / f"query-{q:02}.sql").read_text().strip()
                    if sql != queries[q - 1]:
                        raise ValueError("actual query SQL differs from expected workload")
                    actual = mysql_rows(args.actual_dir / f"query-{q:02}-run-{run_no}.tsv")
                    row["actual_rows"] = len(actual)
                    if q in issues:
                        status, message = issues[q]
                        if status == "missing_expected" and args.allow_missing_expected:
                            status = "skipped_missing_expected"
                        row.update(status=status, message=message)
                    else:
                        ok, message = compare_rows(expected[q], actual)
                        if (not ok and args.duckdb_database is not None
                                and list(positions(sql, ' LIMIT '))):
                            if oracle is None:
                                oracle = DuckDB(args)
                            ok, message = oracle.verify_limit(expected[q], actual)
                        row.update(status="ok" if ok else "mismatch", message=message,
                                   expected_rows=len(expected[q]["rows"]))
            except (OSError, ValueError, subprocess.TimeoutExpired) as error:
                row.update(status="invalid_actual", message=str(error))
            reports.append(row)
            print(f"  Q{q:02} run {run_no}: {row['status']} — {row['message']}", flush=True)
    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    with args.output_csv.open("w", newline="") as out:
        writer = csv.DictWriter(out, fieldnames=list(reports[0]))
        writer.writeheader()
        writer.writerows(reports)
    failures = sum(r["status"] not in {"ok", "skipped_missing_expected"} for r in reports)
    passed = sum(r["status"] == "ok" for r in reports)
    print(f"Correctness: {passed} passed, {failures} failed, {len(reports) - failures - passed} skipped")
    return int(bool(failures))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["generate", "check"])
    parser.add_argument("--expected-dir", type=Path, required=True)
    parser.add_argument("--queries-file", type=Path, required=True)
    parser.add_argument("--schema-file", type=Path, required=True)
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--row-count", required=True, type=int)
    parser.add_argument("--queries", default=",".join(map(str, range(1, 44))))
    parser.add_argument("--duckdb-database", type=Path)
    parser.add_argument("--duckdb-bin", default=os.environ.get("DUCKDB_BIN", "duckdb"))
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument("--memory-limit", default=os.environ.get("CLICKBENCH_DUCKDB_MEMORY_LIMIT", "8GB"))
    parser.add_argument("--threads", type=int, default=int(os.environ.get("CLICKBENCH_DUCKDB_THREADS", "4")))
    parser.add_argument("--actual-dir", type=Path)
    parser.add_argument("--output-csv", type=Path)
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--allow-missing-expected", action="store_true")
    args = parser.parse_args()
    if min(args.row_count, args.runs, args.timeout, args.threads) < 1:
        parser.error("row count, runs, timeout, and threads must be positive")
    if args.mode == "generate" and args.duckdb_database is None:
        parser.error("generate requires --duckdb-database")
    if args.mode == "check" and (args.actual_dir is None or args.output_csv is None):
        parser.error("check requires --actual-dir and --output-csv")
    try:
        return run(args)
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
