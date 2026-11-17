#!/usr/bin/env python3
"""Public JSON value and wire-metadata release regressions on disposable data."""

from _checks import require
import argparse
import os
import json
import struct
from pathlib import Path
import tempfile
import uuid

import pymysql


def verify(conn, directory, database):
    cur = conn.cursor()
    count = 0

    def query(sql):
        cur.execute(sql)
        return cur.fetchall()

    def eq(sql, expected):
        nonlocal count
        got = query(sql)
        require(got == expected, (sql, got, expected))
        count += 1

    def error(sql, code):
        nonlocal count
        try:
            query(sql)
        except pymysql.MySQLError as exc:
            require(exc.args[0] == code, (sql, exc, code))
            count += 1
            return
        raise AssertionError((sql, 'unexpected success', code))

    query(f'USE `{database}`')
    query('CREATE TABLE t(id INT PRIMARY KEY,j JSON)')
    query("INSERT INTO t VALUES (1,'5')")
    for value in ["'{bad'", "''", '5', '1.5', 'TRUE', "CAST('2020-01-01' AS DATE)"]:
        error(f'INSERT INTO t VALUES (2,{value})', 3140)
        error(f'UPDATE t SET j={value} WHERE id=1', 3140)
    eq('SELECT j FROM t', (('5',),))
    query("INSERT INTO t VALUES(2,CAST(5 AS JSON)),(3,NULL),(4,JSON_ARRAY(TRUE))")
    path = directory / 'verify.tsv'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('5\t{bad\n')
    error(f"LOAD DATA LOCAL INFILE '{path}' INTO TABLE t", 3140)
    eq('SELECT COUNT(*) FROM t', ((4,),))
    path.write_text('5\t[1,2]\n6\tnull\n')
    query(f"LOAD DATA LOCAL INFILE '{path}' INTO TABLE t")
    eq('SELECT j FROM t WHERE id>=5 ORDER BY id', (('[1, 2]',),('null',)))
    for doc in ['5','{"a":1}','"str"','null']:
        eq(f"SELECT JSON_EXTRACT('{doc}', '$[*]')", ((None,),))
        eq(f"SELECT JSON_CONTAINS_PATH('{doc}', 'one', '$[*]')", ((0,),))
    eq("SELECT JSON_EXTRACT('{\"a\":{\"b\":2}}', '$**[*]')", ((None,),))
    eq("SELECT JSON_EXTRACT('{\"a\":[1,2]}', '$**[*]')", (('[1, 2]',),))
    eq("SELECT JSON_SEARCH('{\"a\":\"v\"}', 'one','v',NULL,'$[*]')", ((None,),))
    for expr in ['TRUE','FALSE',"CAST('2020-01-01' AS DATE)","CAST('12:30:00' AS TIME)","CAST(1.25 AS DECIMAL(5,2))"]:
        eq(f'SELECT ({expr}) MEMBER OF (JSON_ARRAY({expr}))', ((1,),))
        require(cur.description[0][1] in (3,8), cur.description)
    for depth in [100,101,127,128,150,200,1000]:
        doc = '[' * depth + ']' * depth
        for sql in [f"SELECT JSON_VALID('{doc}')",f"SELECT JSON_VALUE('{doc}', '$')",f"SELECT JSON_SCHEMA_VALID('{{}}','{doc}')",f"INSERT INTO t VALUES(100,'{doc}')"]:
            if depth <= 100: query(sql)
            else: error(sql,3157)
    for sql in ["SELECT JSON_VALUE(CAST('5' AS BINARY), '$')", "SELECT JSON_TYPE(CAST('5' AS BINARY))", "SELECT JSON_UNQUOTE(CAST('\"a\"' AS BINARY))", "SELECT JSON_EXTRACT(_binary X'FF', '$')", "SELECT JSON_SCHEMA_VALID('{}',CAST('{}' AS BINARY))"]:
        error(sql,3144)
    for ddl in ['CREATE TABLE f(j JSON, INDEX idx ((CAST(j AS JSON))))', 'CREATE TABLE f(j JSON, UNIQUE KEY idx ((CAST(j AS JSON))))','CREATE INDEX idx ON t ((CAST(j AS JSON)))','ALTER TABLE t ADD INDEX idx ((CAST(j AS JSON)))']:
        error(ddl,3753)
    for typ in ['TINYINT','BLOB','BOOLEAN','VARCHAR(2)']:
        error(f"SELECT JSON_VALUE('{{\"a\":1}}', '$.a' RETURNING {typ})",1064)
    eq("SELECT JSON_VALUE('{\"a\":\"xx\"}', '$.a' RETURNING CHAR(2))", (('xx',),))
    require(cur.description[0][3] == 8, cur.description)
    query("SELECT JSON_VALUE(j, '$' RETURNING CHAR(2)) FROM t WHERE id<0")
    require(cur.description[0][3] == 8, cur.description)
    error("SELECT JSON_VALUE('{\"a\":\"xx\"}', '$.a' RETURNING CHAR(1) DEFAULT 'yy' ON ERROR)",1690)
    error("SELECT JSON_VALUE('{}', '$.a' RETURNING CHAR(1) DEFAULT 'yy' ON EMPTY)",1690)
    eq("SELECT JSON_VALID(CAST('5' AS BINARY))", ((0,),))
    for doc in ["NULL", "'{\"a\":\"x\"}'"]:
        error(f"SELECT JSON_VALUE({doc}, '$.a' RETURNING CHAR(1) DEFAULT 'yy' ON ERROR)",1690)
    query("SET @j=CAST('5' AS JSON)")
    eq("SELECT @j='5', (@j) MEMBER OF(CAST('[5]' AS JSON))", ((0,1),))
    # COM_STMT_PREPARE describes results before execution, independent of rows.
    from pymysql.constants import COMMAND
    from pymysql.protocol import FieldDescriptorPacket
    for sql, wire_type, length in [
        ("SELECT JSON_VALUE('{\"a\":\"xx\"}', '$.a' RETURNING CHAR(2))", 253, 8),
        ("SELECT (1) MEMBER OF (CAST('[1]' AS JSON))", 8, None),
    ]:
        conn._execute_command(COMMAND.COM_STMT_PREPARE, sql)
        packet = conn._read_packet()
        statement_id, columns, parameters = struct.unpack_from('<IHH', packet.get_all_data(), 1)
        require(parameters == 0 and columns == 1, 'acceptance check failed: parameters == 0 and columns == 1')
        field = conn._read_packet(FieldDescriptorPacket)
        require(field.type_code == wire_type, (sql, field.type_code))
        if length is not None:
            require(field.length == length, (sql, field.length))
        require(conn._read_packet().is_eof_packet(), 'acceptance check failed: conn._read_packet().is_eof_packet()')
        conn._execute_command(COMMAND.COM_STMT_CLOSE, struct.pack('<I', statement_id))
        count += 1
    print(json.dumps({'assertions_passed': count, 'metadata': 'integer MEMBER OF and CHAR(2)=8 bytes passed', 'preserved_difference': 'BUG-13'}, ensure_ascii=False))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, default=3328)
    args = parser.parse_args()
    Path('/tmp/mydb').mkdir(parents=True, exist_ok=True)
    database = 'json_protocol_' + uuid.uuid4().hex[:12]
    with tempfile.TemporaryDirectory(prefix='json-protocol-', dir='/tmp/mydb') as temporary:
        with pymysql.connect(host='127.0.0.1', port=args.port,
                user=os.environ.get('MYDB_TEST_USER', 'root'),
                password=os.environ.get('MYDB_TEST_PASSWORD', 'root'),
                charset='utf8mb4', autocommit=True, local_infile=True,
                connect_timeout=10, read_timeout=30, write_timeout=30) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f'CREATE DATABASE `{database}`')
                try:
                    verify(connection, Path(temporary), database)
                finally:
                    cursor.execute(f'DROP DATABASE `{database}`')


if __name__ == '__main__':
    main()
