#!/usr/bin/env python3
"""Verify issues 87–99 through text and real binary prepared protocols.

Requires PyMySQL and mysql-connector-python. Uses a disposable database and
session settings only. Credentials use MYDB_TEST_USER / MYDB_TEST_PASSWORD.
"""
import argparse
import datetime as dt
import json
import os
from pathlib import Path
import uuid

import mysql.connector
import pymysql
from _checks import require


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, default=14838)
    parser.add_argument('--output', default='/tmp/mydb/issues-87-99-fix/protocol.json')
    args = parser.parse_args()
    options = dict(host='127.0.0.1', port=args.port,
                   user=os.environ.get('MYDB_TEST_USER', 'root'),
                   password=os.environ.get('MYDB_TEST_PASSWORD', 'root'), autocommit=True)
    database = 'temporal_json_' + uuid.uuid4().hex[:12]
    records = []
    with pymysql.connect(**options, charset='utf8mb4') as connection:
        cur = connection.cursor()
        cur.execute(f'CREATE DATABASE `{database}`')
        cur.execute(f'USE `{database}`')
        try:
            for sql in ["SET sql_mode=''", "SET time_zone='+00:00'",
                        "SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci"]:
                cur.execute(sql)

            def check(issue, sql, rows, types=None):
                cur.execute(sql)
                actual = cur.fetchall()
                metadata = [field.type_code for field in cur._result.fields]
                require(actual == rows, (issue, sql, actual, rows))
                if types is not None:
                    require(metadata == types, (issue, sql, metadata, types))
                records.append(dict(issue=issue, protocol='text', sql=sql, rows=actual, types=metadata))

            check(89, "SELECT JSON_QUOTE('abc'),JSON_QUOTE(NULL)", (('"abc"', None),), [253, 253])
            cur.execute("CREATE TABLE quoted AS SELECT JSON_QUOTE('abc') AS q")
            cur.execute('SHOW COLUMNS FROM quoted')
            require(cur.fetchall()[0][1] == 'varchar(20)', 'JSON_QUOTE CTAS character width')
            cur.execute("INSERT INTO quoted VALUES('xyz')")
            check(89, 'SELECT COUNT(*) FROM quoted', ((2,),))
            check(90, "SELECT HOUR('839:00:00'),MINUTE('839:00:00'),SECOND('839:00:00'),MICROSECOND('838:59:59.999999')", ((838, 59, 59, 0),))
            check(91, "SELECT MONTHNAME('2001-11-00'),QUARTER('2005-00-00'),STR_TO_DATE('0000-00-00','%Y-%m-%d')", (('November', 0, '0000-00-00'),))
            check(92, 'SELECT FROM_DAYS(1),FROM_DAYS(365),FROM_DAYS(366)', (('0000-00-00', '0000-00-00', dt.date(1, 1, 1)),))
            check(94, "SELECT EXTRACT(DAY_HOUR FROM '-12:34:56'),EXTRACT(DAY_SECOND FROM '12:34:56')", ((-12, 123456),))
            check(95, "SELECT CAST('bad' AS YEAR),CAST('0' AS YEAR),CAST(0 AS YEAR)", ((None, 2000, 0),))
            for sql, code in [("SELECT JSON_SEARCH('{}','bad',NULL)", 3154),
                              ("SELECT JSON_SEARCH('{}','one',NULL,NULL,'bad')", 3143)]:
                try:
                    cur.execute(sql)
                except pymysql.MySQLError as error:
                    require(error.args[0] == code, (sql, error))
                    records.append(dict(issue=96, protocol='text', sql=sql, errno=code))
                else:
                    raise AssertionError(('unexpected success', sql))
            check(97, "SELECT JSON_VALUE('{\"v\":\"123\"}','$.v' RETURNING TIME),JSON_VALUE('{\"v\":\"123\"}','$.v' RETURNING TIME DEFAULT '01:02:03' ON ERROR)", ((None, dt.timedelta(hours=1, minutes=2, seconds=3)),), [11, 11])
            check(98, "SELECT JSON_VALUE('{\"a\":1}','$.a' RETURNING SIGNED)", ((1,),), [8])
            cur.execute('CREATE TABLE jsons(id INT PRIMARY KEY,j JSON)')
            cur.execute("INSERT INTO jsons VALUES(1,'{\"a\":1}'),(2,'null')")
            for sql, types in [("SELECT JSON_VALUE('{}','$.a' RETURNING JSON)", [245]),
                               ('SELECT JSON_ARRAYAGG(j),JSON_OBJECTAGG(id,j) FROM jsons WHERE id<0', [245, 245]),
                               ('SELECT JSON_ARRAYAGG(j) OVER(ORDER BY id),JSON_OBJECTAGG(id,j) OVER(ORDER BY id) FROM jsons', [245, 245])]:
                cur.execute(sql)
                records.append(dict(issue=98, protocol='text', sql=sql, rows=cur.fetchall()))
                require([f.type_code for f in cur._result.fields] == types, (sql, cur.description))
            check(99, "SELECT DATE_ADD('2024-02-29 23:59:59.999999',INTERVAL 1 MICROSECOND),ADDTIME('23:59:59.999999','00:00:00.000001')", (('2024-03-01 00:00:00', '24:00:00'),))
            check(99, "SELECT ADDTIME('12:00:00.500000','00:00:00')", (('12:00:00.500000',),))
            cur.execute("SET timestamp=1700000000")
            cur.execute('CREATE TABLE created(id INT)')
            check(87, f"SELECT CREATE_TIME FROM information_schema.tables WHERE TABLE_SCHEMA='{database}' AND TABLE_NAME='created'", ((dt.datetime(2023, 11, 14, 22, 13, 20),),))
            cur.execute('SET timestamp=DEFAULT')

            binary = mysql.connector.connect(**options, database=database, use_pure=True, ssl_disabled=True)
            try:
                setup = binary.cursor()
                setup.execute("SET sql_mode='',time_zone='+00:00'")
                setup.close()
                prepared = binary.cursor(prepared=True)
                cases = [
                    (99, "SELECT ADDTIME(?, '00:00:00.000001')", [('23:59:59.999999',), ('12:00:00.123456',), (None,)],
                     [[dt.timedelta(days=1)], [dt.timedelta(hours=12, microseconds=123457)], [None]], [11]),
                    (89, 'SELECT JSON_QUOTE(?)', [('abc',), (None,)], [['"abc"'], [None]], [253]),
                    (98, "SELECT JSON_VALUE(?,'$.a' RETURNING JSON)", [('{"a":[1,2]}',), ('{}',), (None,)],
                     [['[1, 2]'], [None], [None]], [245]),
                ]
                for issue, sql, parameters, expected, types in cases:
                    for values, expected_row in zip(parameters, expected):
                        prepared.execute(sql, values)
                        rows = prepared.fetchall()
                        normalized = [v.decode() if isinstance(v, (bytes, bytearray)) else v for v in rows[0]]
                        require(normalized == expected_row, (issue, sql, values, rows, expected_row))
                        require([d[1] for d in prepared.description] == types, (sql, prepared.description))
                        require([d[1] for d in prepared._prepared['columns']] == types,
                                ('prepare metadata', sql, prepared._prepared['columns']))
                        records.append(dict(issue=issue, protocol='binary', sql=sql, params=values, rows=normalized, types=types))
                for sql in ['SELECT JSON_ARRAYAGG(j),JSON_OBJECTAGG(id,j) FROM jsons',
                            'SELECT JSON_ARRAYAGG(j),JSON_OBJECTAGG(id,j) FROM jsons WHERE id<0']:
                    prepared.execute(sql)
                    prepared.fetchall()
                    require([d[1] for d in prepared.description] == [245, 245], (sql, prepared.description))
                    records.append(dict(issue=98, protocol='binary', sql=sql, types=[245, 245]))
                prepared.close()
            finally:
                binary.close()
        finally:
            cur.execute('SET timestamp=DEFAULT')
            cur.execute(f'DROP DATABASE `{database}`')
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(records, default=str, indent=2))
    print(json.dumps(dict(checks=len(records), status='passed', output=str(output))))


if __name__ == '__main__':
    main()
