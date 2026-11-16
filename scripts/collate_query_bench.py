#!/usr/bin/env python3
"""Compare explicit COLLATE expressions and LIKE controls on isolated local servers.

Uses like_query_bench.py's dataset, configuration, fingerprint checks and CLI.
Requires PyMySQL and cryptography. All generated files stay under /tmp/mydb.
"""

import like_query_bench as benchmark


def cases():
    result = benchmark.cases()
    for name, predicate in [
        ("collate.binary", "unicode_ci COLLATE utf8mb4_bin LIKE '%数据%'"),
        ("collate.same_ci", "unicode_ci COLLATE utf8mb4_0900_ai_ci LIKE '%数据%'"),
        ("collate.general", "unicode_ci COLLATE utf8mb4_general_ci LIKE '%数据%'"),
        ("collate.unicode", "unicode_ci COLLATE utf8mb4_unicode_ci LIKE '%数据%'"),
        ("collate.as_ci", "unicode_ci COLLATE utf8mb4_0900_as_ci LIKE '%数据%'"),
        ("collate.not_like", "unicode_ci COLLATE utf8mb4_bin NOT LIKE '%数据%'"),
        ("collate.pattern", "unicode_ci LIKE ('%数据%' COLLATE utf8mb4_bin)"),
        ("collate.nested", "(unicode_ci COLLATE utf8mb4_bin) COLLATE utf8mb4_0900_ai_ci LIKE '%数据%'"),
        ("computed.lower", "LOWER(unicode_ci) COLLATE utf8mb4_bin LIKE '%café%'"),
        ("computed.concat", "CONCAT(unicode_ci,'🙂') COLLATE utf8mb4_bin LIKE '%数据%🙂'"),
        ("computed.boolean", "unicode_ci COLLATE utf8mb4_bin LIKE '%数据%' OR unicode_ci COLLATE utf8mb4_bin LIKE '%café%'"),
        ("collate.eq", "unicode_ci COLLATE utf8mb4_bin='CAFÉ'"),
        ("collate.range", "unicode_ci COLLATE utf8mb4_bin>='数据'"),
        ("collate.between", "unicode_ci COLLATE utf8mb4_bin BETWEEN '北京' AND '测试'"),
        ("collate.in", "unicode_ci COLLATE utf8mb4_bin IN ('北京','测试','数据库','Café',NULL)"),
        ("collate.nullsafe", "unicode_ci COLLATE utf8mb4_bin<=>NULL"),
        ("ascii.explicit_binary", "ascii_ci COLLATE utf8mb4_bin LIKE '%abc%'"),
        ("ascii.explicit_ci", "ascii_bin COLLATE utf8mb4_0900_ai_ci LIKE '%ABC%'"),
    ]:
        result.append(dict(name=name, predicate=predicate, access="auto"))
    for name, sql in [
        ("projection.length", "SELECT SUM(OCTET_LENGTH(unicode_ci COLLATE utf8mb4_bin)) FROM texts"),
        ("projection.computed", "SELECT SUM(CHAR_LENGTH(LOWER(unicode_ci) COLLATE utf8mb4_bin)) FROM texts"),
        ("projection.case", "SELECT SUM(CASE WHEN unicode_ci COLLATE utf8mb4_bin LIKE '%数据%' THEN 1 ELSE 0 END) FROM texts"),
    ]:
        result.append(dict(name=name, aggregate_sql=sql, access="auto"))
    return result


if __name__ == "__main__":
    benchmark.main(cases)
