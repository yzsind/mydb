"""Versioned, column-dependent SQL workload definitions (no engine special cases)."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = 1
MYSQL_SOURCE = "https://dev.mysql.com/doc/refman/8.0/en/built-in-function-reference.html"

# Workloads are deliberately explicit where an argument has a restricted domain.
# Aliases inherit the same argument workload, but remain separate coverage items.
EXPRESSIONS = {
    "ABS": "ABS(n)", "ACOS": "ACOS(f/1000)", "ASIN": "ASIN(f/1000)",
    "ATAN": "ATAN(f)", "ATAN2": "ATAN2(f,2)", "BIN": "BIN(u)",
    "BIT_COUNT": "BIT_COUNT(u)", "CEIL": "CEIL(d)", "CEILING": "CEILING(d)",
    "CONV": "CONV(CAST(u AS CHAR),10,16)", "COS": "COS(f)", "COT": "COT(f+0.1)",
    "CRC32": "CRC32(s)", "DEGREES": "DEGREES(f)", "DIV": "DIV(n,7)",
    "EXP": "EXP(f/100)", "FLOOR": "FLOOR(d)", "LN": "LN(u+1)",
    "LOG": "LOG(u+1)", "LOG10": "LOG10(u+1)", "LOG2": "LOG2(u+1)",
    "MOD": "MOD(n,7)", "OCT": "OCT(u)", "POW": "POW(f/100,2)",
    "POWER": "POWER(f/100,2)", "RADIANS": "RADIANS(f)", "RAND": "RAND(id)",
    "ROUND": "ROUND(d,2)", "SIGN": "SIGN(n)", "SIN": "SIN(f)",
    "SQRT": "SQRT(u)", "TAN": "TAN(f)", "TRUNCATE": "TRUNCATE(d,2)",
    "ASCII": "ASCII(s)", "BIT_LENGTH": "BIT_LENGTH(b)", "CHAR": "CHAR(65+id%26,66)",
    "CHARACTER_LENGTH": "CHARACTER_LENGTH(s)", "CHAR_LENGTH": "CHAR_LENGTH(s)",
    "CONCAT": "CONCAT(s,'/tail')", "CONCAT_WS": "CONCAT_WS('/',s,CAST(u AS CHAR))",
    "EXPORT_SET": "EXPORT_SET(u,'Y','N',',',16)", "FIND_IN_SET": "FIND_IN_SET('db',s)",
    "FORMAT": "FORMAT(d,2,'en_US')", "HEX": "HEX(b)",
    "INSERT": "INSERT(s,2,3,'替换')", "INSTR": "INSTR(s,'db')",
    "LCASE": "LCASE(s)", "LEFT": "LEFT(s,8)", "LENGTH": "LENGTH(s)",
    "LOCATE": "LOCATE('db',s)", "LOWER": "LOWER(s)", "LPAD": "LPAD(s,128,'xy')",
    "LTRIM": "LTRIM(s)", "MAKE_SET": "MAKE_SET(u,'a','b','c','d')",
    "MID": "MID(s,2,8)", "OCTET_LENGTH": "OCTET_LENGTH(s)", "ORD": "ORD(s)",
    "POSITION": "POSITION('db' IN s)", "QUOTE": "QUOTE(s)",
    "REGEXP_INSTR": "REGEXP_INSTR(s,'[0-9]+')", "REGEXP_LIKE": "REGEXP_LIKE(s,'db[0-9]+')",
    "REGEXP_REPLACE": "REGEXP_REPLACE(s,'[0-9]+','N')",
    "REGEXP_SUBSTR": "REGEXP_SUBSTR(s,'[0-9]+')", "REPEAT": "REPEAT(s,2)",
    "REPLACE": "REPLACE(s,'db','DB')", "REVERSE": "REVERSE(s)", "RIGHT": "RIGHT(s,8)",
    "RPAD": "RPAD(s,128,'xy')", "RTRIM": "RTRIM(s)", "SOUNDEX": "SOUNDEX(s)",
    "SPACE": "SPACE(id%64)", "STRCMP": "STRCMP(s,'MyDB')", "SUBSTR": "SUBSTR(s,2,8)",
    "SUBSTRING": "SUBSTRING(s,2,8)", "SUBSTRING_INDEX": "SUBSTRING_INDEX(s,',',2)",
    "TRIM": "TRIM(BOTH ' ' FROM s)", "UCASE": "UCASE(s)",
    "UNHEX": "UNHEX(HEX(b))", "UPPER": "UPPER(s)",
    "CAST": "CAST(d AS CHAR)", "CHARSET": "CHARSET(s)",
    "COERCIBILITY": "COERCIBILITY(s)", "COLLATE": "s COLLATE utf8mb4_bin = 'a'",
    "COLLATION": "COLLATION(s)", "CONVERT_USING": "CONVERT(s USING utf8mb4)",
    "WEIGHT_STRING": "HEX(WEIGHT_STRING(s))", "COALESCE": "COALESCE(n,7)",
    "ELT": "ELT(id%3+1,'a','b','c')", "FIELD": "FIELD(n,1,2,3)",
    "GREATEST": "GREATEST(n,7)", "IF": "IF(id%2=0,s,'other')",
    "IFNULL": "IFNULL(n,7)", "INTERVAL": "INTERVAL(n,-500,0,500)",
    "ISNULL": "ISNULL(n)", "LEAST": "LEAST(n,7)",
    "NAME_CONST": "NAME_CONST('answer',42)", "NULLIF": "NULLIF(n,0)",
    "AES_ENCRYPT": "HEX(AES_ENCRYPT(s,'benchmark-key'))",
    "AES_DECRYPT": "AES_DECRYPT(AES_ENCRYPT(s,'benchmark-key'),'benchmark-key')",
    "COMPRESS": "COMPRESS(s)", "UNCOMPRESS": "UNCOMPRESS(COMPRESS(s))",
    "UNCOMPRESSED_LENGTH": "UNCOMPRESSED_LENGTH(COMPRESS(s))",
    "FROM_BASE64": "FROM_BASE64(TO_BASE64(b))", "TO_BASE64": "TO_BASE64(b)",
    "MD5": "MD5(s)", "SHA": "SHA(s)", "SHA1": "SHA1(s)", "SHA2": "SHA2(s,256)",
    "RANDOM_BYTES": "RANDOM_BYTES(16)", "BENCHMARK": "BENCHMARK(4,ABS(n))",
    "SLEEP": "SLEEP(id%2 * 0.000001)", "LOAD_FILE": "LOAD_FILE('/tmp/mydb/function-expression/payload.txt')",
    "STATEMENT_DIGEST": "STATEMENT_DIGEST(CONCAT('SELECT ',CAST(u AS CHAR)))",
    "STATEMENT_DIGEST_TEXT": "STATEMENT_DIGEST_TEXT(CONCAT('SELECT ',CAST(u AS CHAR)))",
    "GTID_SUBSET": "GTID_SUBSET(CONCAT('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-',id%100+1),'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-100')",
    "GTID_SUBTRACT": "GTID_SUBTRACT('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-100',CONCAT('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-',id%100+1))",
    "INET_ATON": "INET_ATON(CONCAT('192.0.2.',id%255))", "INET_NTOA": "INET_NTOA(3221225984+id%255)",
    "INET6_ATON": "HEX(INET6_ATON(CONCAT('2001:db8::',id%100)))",
    "INET6_NTOA": "INET6_NTOA(INET6_ATON(CONCAT('2001:db8::',id%100)))",
    "IS_IPV4": "IS_IPV4(CONCAT('192.0.2.',id%255))", "IS_IPV6": "IS_IPV6(CONCAT('2001:db8::',id%100))",
    "IS_IPV4_COMPAT": "IS_IPV4_COMPAT(INET6_ATON(CONCAT('::192.0.2.',id%255)))",
    "IS_IPV4_MAPPED": "IS_IPV4_MAPPED(INET6_ATON(CONCAT('::ffff:192.0.2.',id%255)))",
    "IS_UUID": "IS_UUID(CONCAT('6ccd780c-baba-1026-9564-',LPAD(CAST(id AS CHAR),12,'0')))",
    "UUID_TO_BIN": "HEX(UUID_TO_BIN(CONCAT('6ccd780c-baba-1026-9564-',LPAD(CAST(id AS CHAR),12,'0'))))",
    "BIN_TO_UUID": "BIN_TO_UUID(UUID_TO_BIN(CONCAT('6ccd780c-baba-1026-9564-',LPAD(CAST(id AS CHAR),12,'0'))))",
    "GET_LOCK": "GET_LOCK(CONCAT('feb-',id%8),0)",
    "IS_FREE_LOCK": "IS_FREE_LOCK(CONCAT('feb-',id%8))",
    "IS_USED_LOCK": "IS_USED_LOCK(CONCAT('feb-',id%8))",
    "RELEASE_LOCK": "RELEASE_LOCK(CONCAT('feb-',id%8))",
}

for name in "DATE DAY DAYNAME DAYOFMONTH DAYOFWEEK DAYOFYEAR HOUR LAST_DAY MICROSECOND MINUTE MONTH MONTHNAME QUARTER SECOND TIME TIMESTAMP TIME_TO_SEC TO_DAYS TO_SECONDS UNIX_TIMESTAMP WEEK WEEKDAY WEEKOFYEAR YEAR YEARWEEK".split():
    EXPRESSIONS[name] = f"{name}(dt)"
EXPRESSIONS.update({
    "ADDDATE": "ADDDATE(dt,INTERVAL 1 DAY)", "ADDTIME": "ADDTIME(dt,'01:30:00')",
    "CONVERT_TZ": "CONVERT_TZ(dt,'+00:00','+08:00')", "DATEDIFF": "DATEDIFF(dt,'2020-01-01')",
    "DATE_ADD": "DATE_ADD(dt,INTERVAL 1 DAY)", "DATE_SUB": "DATE_SUB(dt,INTERVAL 1 DAY)",
    "DATE_FORMAT": "DATE_FORMAT(dt,'%Y/%m/%d %H:%i:%s.%f')", "EXTRACT": "EXTRACT(YEAR FROM dt)",
    "FROM_DAYS": "FROM_DAYS(730000+id%20000)", "FROM_UNIXTIME": "FROM_UNIXTIME(1577836800+id)",
    "MAKEDATE": "MAKEDATE(2020+id%6,id%365+1)", "MAKETIME": "MAKETIME(id%24,id%60,id%60)",
    "PERIOD_ADD": "PERIOD_ADD(202001+id%12,1)", "PERIOD_DIFF": "PERIOD_DIFF(202001+id%12,201901)",
    "SEC_TO_TIME": "SEC_TO_TIME(id%86400)", "STR_TO_DATE": "STR_TO_DATE(sdate,'%Y-%m-%d %H:%i:%s.%f')",
    "SUBDATE": "SUBDATE(dt,INTERVAL 1 DAY)", "SUBTIME": "SUBTIME(dt,'01:30:00')",
    "TIMEDIFF": "TIMEDIFF(dt,DATE(dt))", "TIMESTAMPADD": "TIMESTAMPADD(DAY,id%10,dt)",
    "TIMESTAMPDIFF": "TIMESTAMPDIFF(DAY,dt,'2030-01-01')", "TIME_FORMAT": "TIME_FORMAT(dt,'%H:%i:%s')",
})
EXPRESSIONS.update({
    "JSON_ARRAY": "JSON_ARRAY(n,s)", "JSON_ARRAY_APPEND": "JSON_ARRAY_APPEND(j,'$.b',id%10)",
    "JSON_ARRAY_INSERT": "JSON_ARRAY_INSERT(j,'$.b[1]',id%10)",
    "JSON_CONTAINS": "JSON_CONTAINS(j,'2','$.b')", "JSON_CONTAINS_PATH": "JSON_CONTAINS_PATH(j,'one','$.a')",
    "JSON_DEPTH": "JSON_DEPTH(j)", "JSON_EXTRACT": "JSON_EXTRACT(j,'$.a')",
    "JSON_INSERT": "JSON_INSERT(j,'$.c',id%10)", "JSON_KEYS": "JSON_KEYS(j)", "JSON_LENGTH": "JSON_LENGTH(j)",
    "JSON_MERGE": "JSON_MERGE(j,'{\"a\":2}')", "JSON_MERGE_PATCH": "JSON_MERGE_PATCH(j,'{\"a\":2}')",
    "JSON_MERGE_PRESERVE": "JSON_MERGE_PRESERVE(j,'{\"a\":2}')", "JSON_OBJECT": "JSON_OBJECT('a',n,'s',s)",
    "JSON_OVERLAPS": "JSON_OVERLAPS(j,'{\"a\":2}')", "JSON_PRETTY": "JSON_PRETTY(j)",
    "JSON_QUOTE": "JSON_QUOTE(s)", "JSON_REMOVE": "JSON_REMOVE(j,'$.b')",
    "JSON_REPLACE": "JSON_REPLACE(j,'$.a',id%10)",
    "JSON_SCHEMA_VALID": "JSON_SCHEMA_VALID('{\"type\":\"object\",\"required\":[\"a\"]}',j)",
    "JSON_SCHEMA_VALIDATION_REPORT": "JSON_SCHEMA_VALIDATION_REPORT('{\"type\":\"object\",\"required\":[\"a\"]}',j)",
    "JSON_SEARCH": "JSON_SEARCH(j,'one','db%')", "JSON_SET": "JSON_SET(j,'$.a',id%10)",
    "JSON_STORAGE_FREE": "JSON_STORAGE_FREE(j)", "JSON_STORAGE_SIZE": "JSON_STORAGE_SIZE(j)",
    "JSON_TYPE": "JSON_TYPE(j)", "JSON_UNQUOTE": "JSON_UNQUOTE(JSON_EXTRACT(j,'$.s'))",
    "JSON_VALID": "JSON_VALID(j)", "JSON_VALUE": "JSON_VALUE(j,'$.a' RETURNING UNSIGNED)",
})

TIME_NAMES = set("CURDATE CURRENT_DATE CURRENT_TIME CURRENT_TIMESTAMP CURTIME LOCALTIME LOCALTIMESTAMP NOW UTC_DATE UTC_TIME UTC_TIMESTAMP".split())
CONTRACT_NAMES = set("UUID UUID_SHORT RANDOM_BYTES RAND CONNECTION_ID CURRENT_USER SESSION_USER SYSTEM_USER USER VERSION CURRENT_ROLE ICU_VERSION PS_CURRENT_THREAD_ID VERSION_MAJOR VERSION_MINOR VERSION_PATCH IS_USED_LOCK".split())
ENVIRONMENT_NAMES = set("FOUND_ROWS ROW_COUNT LAST_INSERT_ID FORMAT_PATH LOAD_FILE PS_IS_ACCOUNT_ENABLED PS_IS_CONSUMER_ENABLED PS_IS_INSTRUMENT_DEFAULT_ENABLED PS_IS_INSTRUMENT_DEFAULT_TIMED PS_IS_THREAD_INSTRUMENTED PS_THREAD_ACCOUNT PS_THREAD_ID PS_THREAD_STACK PS_THREAD_TRX_INFO".split())

GEO_REPLACEMENTS = {
    "ST_GEOMFROMTEXT('POINT(1 2)')": "gp", "ST_GEOMFROMTEXT('POINT(2 3)')": "gq",
    "ST_GEOMFROMTEXT('LINESTRING(0 0, 2 2)')": "gl", "ST_GEOMFROMTEXT('LINESTRING(0 1, 2 3)')": "gl2",
    "ST_GEOMFROMTEXT('POLYGON((0 0, 0 2, 2 2, 0 0))')": "poly",
    "ST_GEOMFROMTEXT('GEOMETRYCOLLECTION(POINT(1 2),POINT(2 3))')": "gc",
}


def fixture_functions():
    functions = {}
    for path in sorted((ROOT / "src/engine/executor/tests/function_compat/fixtures").glob("*.json")):
        for function in json.loads(path.read_text())["functions"]:
            functions[function["name"]] = dict(function, category=path.stem)
    return functions


def registered_names():
    path = ROOT / "scripts/check_function_test_coverage.py"
    spec = importlib.util.spec_from_file_location("function_coverage", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    names = module.registered_names()
    for path in (ROOT / "docs/function").glob("*/*.md"):
        if path.name == "README.md":
            continue
        text = path.read_text()
        match = re.match(r"#\s+([A-Z][A-Z0-9_]*)", text)
        if match:
            names.add(match.group(1))
    return names


def make_case(name, category, expression, **extra):
    return dict(id=name.lower(), name=name, category=category,
                sql=f"SELECT id,{expression} AS result FROM bench", comparison="exact", **extra)


def lift_nominal(function):
    nominal = next(c for c in function["cases"] if "nominal" in c["tags"])
    expression = nominal["sql"].strip().removeprefix("SELECT ")
    name = function["name"]
    unquoted = re.sub(r"'(?:''|[^'])*'", "''", expression)
    if " FROM " in unquoted.upper() and name not in {"EXTRACT", "TRIM"}:
        raise ValueError(f"explicit workload required for {name}")
    expression = EXPRESSIONS.get(name, expression)
    if function["category"] == "spatial":
        for source, target in GEO_REPLACEMENTS.items():
            expression = expression.replace(source, target)
        # Text and WKB constructors must also depend on table columns.
        if not re.search(r"\b(gp|gq|gl|gl2|poly|gc)\b", expression):
            match = re.search(r"'([^']*)'", expression)
            if match:
                value = match.group(0)
                expression = expression[:match.start()] + f"IF(id%2=0,{value},NULL)" + expression[match.end():]
            else:
                expression = re.sub(r"\b116\.4\b", "(116.4+id%10/100)", expression)
                expression = re.sub(r"\bPOINT\(1, 2\)", "POINT(id%10+1,2)", expression)
    if name not in EXPRESSIONS and function["category"] not in {"spatial", "vector"}:
        # Restricted/no-argument system calls are intentionally left as such.
        # Their per-statement/constant nature is recorded in the manifest.
        pass
    return expression, nominal


def build_cases():
    cases = []
    fixtures = fixture_functions()
    for name, function in fixtures.items():
        category = function["category"]
        if function["profile"] in {"aggregate", "window"} or category == "aggregate-window":
            continue
        expression, nominal = lift_nominal(function)
        extra = {"oracle_class": function["oracle_class"], "setup": nominal.get("setup", []),
                 "cleanup": nominal.get("cleanup", []), "profile": function["profile"]}
        if function["oracle_class"] == "mysql80_mapped":
            mysql = nominal.get("mysql_sql", "").strip().removeprefix("SELECT ")
            extra["mysql_sql"] = f"SELECT id,{mysql} AS result FROM bench"
            extra["workload_note"] = "sys schema mapped call; constant arguments unless explicitly shown"
        if function["oracle_class"] in {"mysql80_enterprise_contract", "mydb_extension"}:
            extra["mysql_unavailable"] = function["oracle_class"]
        case = make_case(name, category, expression, **extra)
        if name in CONTRACT_NAMES:
            case["comparison"] = "contract"
        elif name in ENVIRONMENT_NAMES or name == "SYSDATE":
            case["comparison"] = "environment"
        elif name.startswith("JSON_") and name not in {"JSON_QUOTE", "JSON_UNQUOTE", "JSON_PRETTY", "JSON_SEARCH"}:
            case["comparison"] = "json"
        if name in TIME_NAMES:
            case["workload_note"] = "fixed SET timestamp; evaluated with the engine's statement-time semantics"
        if not re.search(r"\b(id|n|u|d|f|s|b|dt|sdate|j|gp|gq|gl|gl2|poly|gc)\b", expression):
            case["workload_note"] = "constant/no-argument expression; input has 1M rows, call count is optimizer-dependent"
        cases.append(case)

    aggregate = "ANY_VALUE AVG BIT_AND BIT_OR BIT_XOR COUNT MAX MIN STD STDDEV STDDEV_POP STDDEV_SAMP SUM VAR_POP VAR_SAMP VARIANCE".split()
    for name in aggregate:
        expr = f"{name}(n)" if name != "COUNT" else "COUNT(n)"
        if name == "ANY_VALUE":
            expr = "ANY_VALUE(id%1024)"
        cases.append(dict(id=name.lower(), name=name, category="aggregate-window", sql=f"SELECT id%1024 AS k,{expr} AS result FROM bench GROUP BY id%1024", comparison="numeric"))
    extra_aggregate = {
        "GROUP_CONCAT": "SELECT id%1024 AS k,GROUP_CONCAT(s ORDER BY id SEPARATOR ':') AS result FROM bench GROUP BY id%1024",
        "GROUPING": "SELECT id%1024 AS k,GROUPING(id%1024) AS result FROM bench GROUP BY id%1024 WITH ROLLUP",
        "JSON_ARRAYAGG": "SELECT id%1024 AS k,JSON_ARRAYAGG(n) AS result FROM bench GROUP BY id%1024",
        "JSON_OBJECTAGG": "SELECT id%1024 AS k,JSON_OBJECTAGG(CAST(id AS CHAR),n) AS result FROM bench GROUP BY id%1024",
        "ST_COLLECT": "SELECT id%1024 AS k,ST_ASTEXT(ST_COLLECT(gp)) AS result FROM bench GROUP BY id%1024",
    }
    for name, sql in extra_aggregate.items():
        comparison = "json_object" if name == "JSON_OBJECTAGG" else "json_multiset" if name == "JSON_ARRAYAGG" else "geometry_multiset" if name == "ST_COLLECT" else "exact"
        cases.append(dict(id=name.lower(), name=name, category="aggregate-window", sql=sql, comparison=comparison))
    for name in "ROW_NUMBER RANK DENSE_RANK NTILE PERCENT_RANK CUME_DIST LAG LEAD FIRST_VALUE LAST_VALUE NTH_VALUE".split():
        args = "4" if name == "NTILE" else "n,2" if name == "NTH_VALUE" else "n" if name in {"LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE"} else ""
        cases.append(make_case(name, "aggregate-window", f"{name}({args}) OVER (PARTITION BY id%1024 ORDER BY id)", profile="window"))

    vector_missing = {
        "HAMMING_DISTANCE": "HAMMING_DISTANCE(STRING_TO_VECTOR('[1,2]'),STRING_TO_VECTOR('[1,3]'))",
        "JACCARD_DISTANCE": "JACCARD_DISTANCE(STRING_TO_VECTOR('[1,2]'),STRING_TO_VECTOR('[1,3]'))",
        "VECTOR_DATATYPE": "VECTOR_DATATYPE(STRING_TO_VECTOR('[1,2]'))",
        "VECTOR_FROM_BINARY": "VECTOR_FROM_BINARY(VECTOR_TO_BINARY(STRING_TO_VECTOR('[1,2]')))",
        "VECTOR_TO_BINARY": "VECTOR_TO_BINARY(STRING_TO_VECTOR('[1,2]'))",
    }
    for name, expr in vector_missing.items():
        cases.append(make_case(name, "vector", expr, mysql_unavailable="mydb_extension", workload_note="constant vector workload; no MySQL 8.0 equivalent"))

    operators = {
        "unary_plus": "+n", "unary_minus": "-n", "bit_not": "~u", "not": "NOT n", "bang": "!n",
        "add": "n+7", "subtract": "n-7", "multiply": "n*7", "divide": "n/7", "int_div": "n DIV 7", "modulo": "n%7", "mod_keyword": "n MOD 7",
        "bit_and": "u&7", "bit_or": "u|7", "bit_xor": "u^7", "shift_left": "u<<2", "shift_right": "u>>2",
        "equal": "n=7", "null_safe_equal": "n<=>7", "not_equal": "n<>7", "bang_equal": "n!=7",
        "less": "n<7", "less_equal": "n<=7", "greater": "n>7", "greater_equal": "n>=7",
        "and": "n>0 AND u<500", "or": "n>0 OR u<500", "xor": "n>0 XOR u<500", "and_symbol": "n>0 && u<500", "or_symbol": "n>0 || u<500",
        "is_null": "n IS NULL", "is_not_null": "n IS NOT NULL", "is_true": "n IS TRUE", "is_false": "n IS FALSE", "is_unknown": "n IS UNKNOWN",
        "is_not_true": "n IS NOT TRUE", "is_not_false": "n IS NOT FALSE", "is_not_unknown": "n IS NOT UNKNOWN",
        "between": "n BETWEEN 0 AND 500", "not_between": "n NOT BETWEEN 0 AND 500", "in_list": "n IN (1,2,3,7)", "not_in_list": "n NOT IN (1,2,3,7,NULL)",
        "like": "s LIKE '%db%'", "not_like": "s NOT LIKE '%db%'", "like_escape": "s LIKE '%!_%' ESCAPE '!'", "regexp": "s REGEXP 'db[0-9]+'", "not_regexp": "s NOT REGEXP 'db[0-9]+'", "rlike": "s RLIKE 'db[0-9]+'",
        "case_searched": "CASE WHEN n<0 THEN 'negative' WHEN n=0 THEN 'zero' ELSE s END", "case_simple": "CASE id%3 WHEN 0 THEN n WHEN 1 THEN 7 ELSE NULL END",
        "tuple_equal": "(n,u)=(7,7)", "row_constructor": "ROW(n,u)=ROW(7,7)", "tuple_in": "(n,u) IN ((1,1),(7,7))",
        "json_arrow": "j->'$.a'", "json_long_arrow": "j->>'$.s'", "binary_cast": "BINARY s", "collate": "s COLLATE utf8mb4_bin='a'",
        "interval_add": "dt+INTERVAL 1 DAY", "interval_subtract": "dt-INTERVAL 1 DAY", "interval_left": "INTERVAL 1 DAY+dt",
        "scalar_subquery": "(SELECT MIN(v) FROM lookup WHERE v=bench.id%10)", "exists": "EXISTS(SELECT 1 FROM lookup WHERE v=bench.id%10)",
        "not_exists": "NOT EXISTS(SELECT 1 FROM lookup WHERE v=bench.id%10)", "in_subquery": "u IN (SELECT v FROM lookup)", "not_in_subquery": "u NOT IN (SELECT v FROM lookup)",
        "any_subquery": "u=ANY(SELECT v FROM lookup)", "all_subquery": "u>=ALL(SELECT v FROM lookup)", "some_subquery": "u=SOME(SELECT v FROM lookup)",
        "user_variable": "@feb_value+n", "variable_assignment": "@feb_value:=n", "system_variable": "@@session.autocommit+n", "default_column": "DEFAULT(n)+id%2",
        "qualified_column": "bench.n+1", "literal": "42", "hex_literal": "0xCAFE+u", "bit_literal": "b'1010'|u", "typed_literal": "DATEDIFF(dt,DATE '2020-01-01')",
        "precedence": "n+7*3 DIV 2^1", "parentheses": "(n+7)*(3 DIV 2)^1",
    }
    for name, expr in operators.items():
        cases.append(make_case("EXPR_"+name.upper(), "expressions", expr, setup=["SET @feb_value=7"]))
    cases.append(make_case("EXPR_PIPES_CONCAT", "expressions", "s||'tail'", setup=["SET sql_mode='PIPES_AS_CONCAT'"]))
    cases.append(make_case("EXPR_PREPARED_PARAMETER", "expressions", "n+?", prepared=True, params=[7]))
    cases.extend([
        dict(id="json_table", name="JSON_TABLE", category="expressions", sql="SELECT bench.id,t.v AS result FROM bench JOIN JSON_TABLE(j,'$.b[*]' COLUMNS(v INT PATH '$')) AS t ON TRUE", comparison="exact"),
        dict(id="match_against", name="MATCH_AGAINST", category="expressions", sql="SELECT id,MATCH(s) AGAINST('database' IN BOOLEAN MODE) AS result FROM text_bench", comparison="environment", workload_note="full-text score semantics differ; no speedup ranking"),
        dict(id="generate_series", name="GENERATE_SERIES", category="expressions", sql="SELECT value AS id,value AS result FROM generate_series(1,1000000)", comparison="numeric", mysql_unavailable="mydb_extension"),
    ])
    # Type coverage supplements the primary workload rather than claiming the
    # infinite Cartesian product of types, values, syntax and SQL contexts.
    for typ, column in {"signed":"n", "unsigned":"u", "decimal":"d", "double":"f"}.items():
        for op, expression in {"add":f"{column}+7", "compare":f"{column}>7", "cast":f"CAST({column} AS CHAR)", "abs":f"ABS({column})"}.items():
            cases.append(make_case(f"TYPE_{typ.upper()}_{op.upper()}", "type-variants", expression))
    cases.extend([make_case("BASELINE_COLUMN", "baseline", "n"), make_case("BASELINE_CONSTANT", "baseline", "1"), make_case("BASELINE_TEXT", "baseline", "s"), make_case("BASELINE_JSON", "baseline", "j")])
    seen = set()
    for case in cases:
        if case["id"] in seen:
            raise ValueError(f"duplicate case {case['id']}")
        seen.add(case["id"])
    return cases
