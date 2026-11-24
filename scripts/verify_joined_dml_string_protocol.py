#!/usr/bin/env python3
"""Verify joined DML and string contracts through text and binary protocols.

Requires PyMySQL and mysql-connector-python. Uses a disposable database, local
session settings, and MYDB_TEST_USER / MYDB_TEST_PASSWORD credentials.
"""
import argparse
import json
import os
from pathlib import Path
import uuid

import mysql.connector
import pymysql
from _checks import require


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, default=14839)
    parser.add_argument('--output', default='/tmp/mydb/issues-101-105-fix/protocol.json')
    args = parser.parse_args()
    options = dict(host='127.0.0.1', port=args.port,
                   user=os.environ.get('MYDB_TEST_USER', 'root'),
                   password=os.environ.get('MYDB_TEST_PASSWORD', 'root'), autocommit=True)
    database = 'joined_string_' + uuid.uuid4().hex[:12]
    records = []
    with pymysql.connect(**options, charset='utf8mb4') as connection:
        cur = connection.cursor()
        cur.execute(f'CREATE DATABASE `{database}`')
        cur.execute(f'USE `{database}`')
        try:
            cur.execute("SET sql_mode=''")
            cur.execute('SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci')

            def check(issue, sql, expected, charset=None):
                cur.execute(sql)
                actual = cur.fetchall()
                fields = [dict(type=f.type_code, charset=f.charsetnr) for f in cur._result.fields]
                require(actual == expected, (issue, sql, actual, expected))
                if charset is not None:
                    require([f['charset'] for f in fields] == charset, (sql, fields, charset))
                records.append(dict(issue=issue, protocol='text', sql=sql, fields=fields, rows=actual))

            for sql in ['CREATE TABLE a(id INT PRIMARY KEY,v INT)',
                        'CREATE TABLE b(id INT PRIMARY KEY,v INT)',
                        'CREATE TABLE c(id INT PRIMARY KEY,v INT)',
                        'INSERT INTO a VALUES(1,10),(2,20)',
                        'INSERT INTO b VALUES(2,200),(3,300)',
                        'INSERT INTO c VALUES(2,2000),(3,3000),(4,4000)']:
                cur.execute(sql)
            cur.execute('START TRANSACTION')
            cur.execute('UPDATE a RIGHT JOIN b ON a.id=b.id RIGHT JOIN c ON b.id=c.id SET a.v=a.v+1,b.v=b.v+1,c.v=c.v+1')
            check(101, 'SELECT * FROM c ORDER BY id', ((2, 2001), (3, 3001), (4, 4001)))
            cur.execute('ROLLBACK')
            cur.execute('START TRANSACTION')
            cur.execute('DELETE a,b,c FROM a LEFT JOIN b ON a.id=b.id RIGHT JOIN c ON b.id=c.id')
            check(101, 'SELECT * FROM a', ((1, 10),))
            check(101, 'SELECT * FROM b', ((3, 300),))
            check(101, 'SELECT * FROM c', ())
            cur.execute('ROLLBACK')
            cur.execute('START TRANSACTION')
            cur.execute('UPDATE a RIGHT JOIN b USING(id) RIGHT JOIN c USING(id) SET c.v=id WHERE id=4')
            check(101, 'SELECT v FROM c WHERE id=4', ((4,),))
            cur.execute('DELETE c FROM a RIGHT JOIN b USING(id) RIGHT JOIN c USING(id) WHERE id=3')
            check(101, 'SELECT id FROM c ORDER BY id', ((2,), (4,)))
            cur.execute('ROLLBACK')
            cur.execute('START TRANSACTION')
            cur.execute('UPDATE a JOIN (SELECT id AS sid,MAX(v) AS val FROM b GROUP BY id) d ON a.id=d.sid SET a.v=d.val')
            check(102, 'SELECT * FROM a ORDER BY id', ((1, 10), (2, 200)))
            cur.execute('ROLLBACK')
            cur.execute('START TRANSACTION')
            cur.execute('DELETE a FROM a JOIN (SELECT id AS sid FROM b) d ON a.id=d.sid')
            check(102, 'SELECT * FROM a', ((1, 10),))
            cur.execute('ROLLBACK')

            for names, expected_charset in [('utf8mb4 COLLATE utf8mb4_0900_ai_ci', 255),
                                            ('latin1 COLLATE latin1_swedish_ci', 8)]:
                cur.execute(f'SET NAMES {names}')
                for operand, value in [("_binary'ABC'", 'QUJD'),
                                       ('CONVERT(0x00FF USING binary)', 'AP8='),
                                       ("UNHEX('00FF')", 'AP8='), ('NULL', None)]:
                    check(103, f'SELECT TO_BASE64({operand})', ((value,),), [expected_charset])
            cur.execute('SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci')
            cur.execute("CREATE TABLE encoded AS SELECT TO_BASE64(_binary'ABC') AS v")
            check(103, f"SELECT DATA_TYPE,CHARACTER_SET_NAME FROM information_schema.columns WHERE TABLE_SCHEMA='{database}' AND TABLE_NAME='encoded'", (('varchar', 'utf8mb4'),))
            for operand, value in [('CONVERT(0x00FF USING binary)', b''),
                                   ('CONVERT(0xFF0041 USING binary)', b'A000'),
                                   ("_binary'ABC'", b'A120')]:
                check(104, f'SELECT SOUNDEX({operand})', ((value,),), [63])
            check(104, "SELECT SOUNDEX('é')", (('é000',),), [255])

            for func in ['REGEXP_LIKE', 'REGEXP_INSTR', 'REGEXP_SUBSTR', 'REGEXP_REPLACE']:
                suffix = ",'#'" if func == 'REGEXP_REPLACE' else ''
                sql = f"SELECT {func}(NULL,'['{suffix})"
                try:
                    cur.execute(sql)
                except pymysql.MySQLError as error:
                    require(error.args[0] == 3696, (sql, error))
                    records.append(dict(issue=105, protocol='text', sql=sql, errno=3696))
                else:
                    raise AssertionError(('unexpected success', sql))
                check(105, f"SELECT {func}(NULL,'a'{suffix})", ((None,),))
            check(105, "SELECT CASE WHEN 0 THEN REGEXP_LIKE(NULL,'[') ELSE 1 END,REGEXP_LIKE(NULL,'[',NULL)", ((1, None),))

            binary = mysql.connector.connect(**options, database=database, use_pure=True, ssl_disabled=True)
            try:
                setup = binary.cursor()
                setup.execute("SET sql_mode=''")
                setup.execute('SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci')
                setup.close()
                prepared = binary.cursor(prepared=True)
                for values, expected in [((b'ABC',), 'QUJD'), ((b'\x00\xff',), 'AP8='), ((None,), None)]:
                    prepared.execute('SELECT TO_BASE64(?)', values)
                    actual = prepared.fetchall()
                    require(actual == [(expected,)], ('binary base64', actual, expected))
                    require(prepared.description[0][1] == 253, prepared.description)
                    require(prepared._prepared['columns'][0][1] == 253, prepared._prepared)
                    records.append(dict(issue=103, protocol='binary', params=values, rows=actual,
                                        prepare=prepared._prepared['columns']))
                for value, expected in [(b'\x00\xff', b''), (b'\xff\x00A', b'A000'), (None, None)]:
                    prepared.execute('SELECT SOUNDEX(CONVERT(? USING binary))', (value,))
                    actual = prepared.fetchall()
                    require(actual == [(expected,)], ('binary soundex', actual, expected))
                    require(prepared._prepared['columns'][0][8] == 63, prepared._prepared)
                    records.append(dict(issue=104, protocol='binary', params=[value], rows=actual,
                                        prepare=prepared._prepared['columns']))
                for func in ['REGEXP_LIKE', 'REGEXP_INSTR', 'REGEXP_SUBSTR', 'REGEXP_REPLACE']:
                    suffix = ",'#'" if func == 'REGEXP_REPLACE' else ''
                    sql = f'SELECT {func}(?,?{suffix})'
                    try:
                        prepared.execute(sql, (None, '['))
                        prepared.fetchall()
                    except mysql.connector.Error as error:
                        require(error.errno == 3696, (sql, error))
                        records.append(dict(issue=105, protocol='binary', sql=sql, errno=3696))
                    else:
                        raise AssertionError(('unexpected prepared success', sql))
                    prepared.execute(sql, (None, 'a'))
                    require(prepared.fetchall() == [(None,)], sql)
                for issue, sql in [(101, 'UPDATE a RIGHT JOIN b ON a.id=b.id RIGHT JOIN c ON a.id=c.id SET a.v=? WHERE a.id=?'),
                                   (102, 'UPDATE a JOIN (SELECT id AS sid,v AS sv FROM b) d ON a.id=d.sid SET a.v=? WHERE a.id=?')]:
                    binary.start_transaction()
                    prepared.execute(sql, (77, 2))
                    require(prepared.rowcount == 1, (issue, prepared.rowcount))
                    prepared.execute('SELECT v FROM a WHERE id=?', (2,))
                    require(prepared.fetchall() == [(77,)], issue)
                    binary.rollback()
                    check(issue, 'SELECT v FROM a WHERE id=2', ((20,),))
                    records.append(dict(issue=issue, protocol='binary', sql=sql, rollback='restored'))
                prepared.close()
            finally:
                binary.close()
            for suffix in ['WITH QUERY EXPANSION', 'IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION']:
                try:
                    cur.execute(f"SELECT MATCH(txt) AGAINST('alpha' {suffix}) FROM missing")
                except pymysql.MySQLError as error:
                    require(error.args[0] == 1064 and 'QUERY EXPANSION' in error.args[1], error)
                    records.append(dict(issue=106, protocol='text', unsupported=suffix, errno=1064))
                else:
                    raise AssertionError(('query expansion unexpectedly accepted', suffix))
        finally:
            connection.rollback()
            cur.execute(f'DROP DATABASE `{database}`')
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(records, default=str, indent=2))
    print(json.dumps(dict(checks=len(records), status='passed', output=str(output))))


if __name__ == '__main__':
    main()
