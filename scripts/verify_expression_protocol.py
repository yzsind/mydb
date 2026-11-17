#!/usr/bin/env python3
"""Public expression and binary-protocol release checks (requires PyMySQL).
Creates and drops its own uniquely named database; does not change global settings.
"""

from _checks import require
import argparse
import datetime
from decimal import Decimal
import json
import os
from pathlib import Path
import struct
import sys
import uuid

import pymysql
from pymysql.constants import COMMAND, FIELD_TYPE
from pymysql.protocol import FieldDescriptorPacket

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from protocol_helpers import prepared


def prepared_decimal_parameter(connection, text):
    connection._execute_command(COMMAND.COM_STMT_PREPARE, "SELECT ?+0")
    packet = connection._read_packet().get_all_data()
    statement, columns, parameters = struct.unpack_from("<IHH", packet, 1)
    require(parameters == 1 and columns == 1, 'acceptance check failed: parameters == 1 and columns == 1')
    connection._read_packet()
    require(connection._read_packet().is_eof_packet(), 'acceptance check failed: connection._read_packet().is_eof_packet()')
    connection._read_packet()
    require(connection._read_packet().is_eof_packet(), 'acceptance check failed: connection._read_packet().is_eof_packet()')
    try:
        for value in (text, "9.000000000000000000000000000001"):
            encoded = value.encode("ascii")
            payload = (struct.pack("<IBI", statement, 0, 1) + b"\0\1"
                       + bytes([FIELD_TYPE.VAR_STRING, 0, len(encoded)]) + encoded)
            connection._execute_command(COMMAND.COM_STMT_EXECUTE, payload)
            require(connection._read_packet().read_length_encoded_integer() == 1, 'acceptance check failed: connection._read_packet().read_length_encoded_integer() == 1')
            field = FieldDescriptorPacket(connection._read_packet().get_all_data(), "utf8")
            require(connection._read_packet().is_eof_packet(), 'acceptance check failed: connection._read_packet().is_eof_packet()')
            row = connection._read_packet()
            require(row.read(2) == b"\0\0", 'acceptance check failed: row.read(2) == b"\\0\\0"')
            require(field.type_code == FIELD_TYPE.NEWDECIMAL, field.type_code)
            require(row.read_length_coded_string().decode("ascii") == value, 'acceptance check failed: row.read_length_coded_string().decode("ascii") == value')
            require(row._position == len(row.get_all_data()), 'acceptance check failed: row._position == len(row.get_all_data())')
            require(connection._read_packet().is_eof_packet(), 'acceptance check failed: connection._read_packet().is_eof_packet()')
    finally:
        connection._execute_command(COMMAND.COM_STMT_CLOSE, struct.pack("<I", statement))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=3337)
    parser.add_argument("--output", default="/tmp/mydb/expression-protocol/acceptance.json")
    args = parser.parse_args()
    database = "expr_protocol_" + uuid.uuid4().hex[:12]
    records = []
    with pymysql.connect(host="127.0.0.1", port=args.port, user=os.environ.get("MYDB_TEST_USER", "root"),
                         password=os.environ.get("MYDB_TEST_PASSWORD", "root"),
                         charset="utf8mb4", autocommit=True, read_timeout=30) as connection:
        cur = connection.cursor()
        cur.execute(f"CREATE DATABASE `{database}`")
        try:
            cur.execute(f"USE `{database}`")
            cur.execute("SET sql_mode='STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION', time_zone='+00:00'")
            cur.execute("CREATE TABLE t(id INT PRIMARY KEY,d DATE,dt DATETIME(6),tm TIME(6),b BLOB,e ENUM('small','medium','large'),g BIGINT AS(d+1) STORED)")
            cur.execute("INSERT INTO t(id,d,dt,tm,b,e) VALUES(1,'2020-01-01','2020-01-01 10:11:12.123456','10:11:12.500000',x'E9FF','small')")

            def check(sql, expected, types=None, charsets=None, scales=None, warnings=None):
                cur.execute(sql)
                actual = cur.fetchall()
                fields = cur._result.fields
                require(actual == (tuple(expected),), (sql, actual, expected))
                metadata = [(f.type_code, f.charsetnr, f.scale) for f in fields]
                if types is not None:
                    require([f.type_code for f in fields] == types, (sql, metadata))
                if charsets is not None:
                    require([f.charsetnr for f in fields] == charsets, (sql, metadata))
                if scales is not None:
                    require([f.scale for f in fields] == scales, (sql, metadata))
                cur.execute("SHOW WARNINGS")
                diagnostic = cur.fetchall()
                if warnings is not None:
                    require([row[1] for row in diagnostic] == warnings, (sql, diagnostic))
                records.append(dict(sql=sql, rows=repr(actual), metadata=metadata, warnings=diagnostic))

            check("SELECT dt+0,d+0,g FROM t", [Decimal('20200101101112.123456'),20200101,20200102], [246,8,8], scales=[6,0,0])
            check("SELECT tm+INTERVAL 1 HOUR,TIMEDIFF(tm,'08:00:00') FROM t",[datetime.timedelta(seconds=40272,microseconds=500000),datetime.timedelta(seconds=7872,microseconds=500000)],[11,11],scales=[6,6])
            check("SELECT -(x'414243'),-(b'101'),ABS(x'41'),b'101' AND 1,NOT b'101',UNHEX('E9')=0xE9",[-4276803,-5,65,1,0,1])
            check("SELECT SPACE(b'1'),b'101' AND b'1' AND 1",[' ',1])
            check("SELECT '123abc' DIV 2,'3.9abc' DIV 2,'9007199254740993x' DIV 1",[61,1,9007199254740993],[8]*3,warnings=[1292]*3)
            check("SELECT CAST(1e20 AS SIGNED),CAST('99999999999999999999' AS SIGNED),CAST(9223372036854775808 AS SIGNED)",[9223372036854775807,-1,-9223372036854775808])
            check("SELECT e<'medium',e>'medium',e<2 FROM t",[0,1,1],[8]*3)
            check("SELECT LPAD('中文',4,''),RPAD('abc',2,'')",['','ab'])
            check("SELECT WEEK('10:11:12'),YEAR('10:11:12'),DATE_FORMAT('10:11:12','%Y-%m-%d'),LAST_DAY('2020-02-00')",[45,2010,'2010-11-12',datetime.date(2020,2,29)])
            # mydb preserves year-zero day-number round trips (the existing
            # expression_functions_1028::e13_from_days and verified_100 contract).
            check("SELECT FROM_DAYS(0),FROM_DAYS(1),FROM_DAYS(365),FROM_DAYS(366),YEAR(0),MONTH(0),DAY(0)",
                  ['0000-00-00','0000-01-01','0000-12-31',datetime.date(1,1,1),0,0,0])
            check("SELECT CAST(1900 AS YEAR),CAST(2156 AS YEAR)",[None,None],warnings=[1292,1292])
            check("SELECT CHAR(233),CONCAT(b,'!'),SUBSTRING(b,2) FROM t",[b'\xe9',b'\xe9\xff!',b'\xff'],charsets=[63]*3)
            check("SELECT BIN_TO_UUID(UUID_TO_BIN('6ccd780c-baba-1026-9564-5b8c656024db')),INET6_NTOA(INET6_ATON('::1'))",['6ccd780c-baba-1026-9564-5b8c656024db','::1'],charsets=[255,255])
            check("SELECT MOD(10,3),GREATEST(1,2,3),INTERVAL(5,1,2,3),ANY_VALUE(1)",[1,3,3,1],[8]*4)
            check("SELECT CAST('10:11:12.999999' AS TIME(3)),CAST('-00:00:00.999999' AS TIME(3))",[datetime.timedelta(seconds=36673),datetime.timedelta(seconds=-1)],scales=[3,3])
            check("SELECT ADDTIME(59,2.5),ADDTIME(CAST('838:59:59' AS TIME),'1:00:00')",['00:01:01.500000',None])
            check("SELECT CAST('00:00:00.000001' AS TIME(6))+0,CAST('-00:00:00.000001' AS TIME(6))+0",[Decimal('0.000001'),Decimal('-0.000001')],[246,246],scales=[6,6],warnings=[])
            check("SELECT MICROSECOND('2020-01-01')",[0],warnings=[1292])
            check("SELECT CAST('{}' AS JSON)+0",[0.0],warnings=[3156])
            check("SELECT FROM_DAYS(3652425)",[None],warnings=[1441])
            check("SELECT BINARY(1)",[b'1'],charsets=[63])
            check("SELECT CONVERT('a', BINARY(4)),CONVERT(BINARY(1),BINARY(2))",[b'a\0\0\0',b'1\0'],charsets=[63,63])
            check("SELECT TIME_FORMAT('abc','%H'),TO_SECONDS('FF')",[None,None],warnings=[1292,1292])
            for sql in ["SELECT DAYNAME('0000-00-00')", "SELECT CAST('' AS SIGNED)", "SELECT CAST('' AS DECIMAL)", "SELECT CAST('abc' AS CHAR(2))", "SELECT NOT('abc')", "SELECT CAST('2020-01-01' AS YEAR)"]:
                cur.execute(sql);cur.fetchall();cur.execute("SHOW WARNINGS")
                diagnostic=cur.fetchall()
                require(any(row[1]==1292 for row in diagnostic), (sql,diagnostic))
                records.append(dict(sql=sql,warnings=diagnostic))
            cur.execute("SET @s='0.123456789012345678901234567890'")
            cur.execute("PREPARE p FROM 'SELECT ?+0'")
            check("EXECUTE p USING @s",[Decimal('0.123456789012345678901234567890')],[246],scales=[30])
            cur.execute("DEALLOCATE PREPARE p")
            result=prepared(connection,"SELECT dt+0,g FROM t")
            require(result['values']==['20200101101112.123456',20200102], result)
            records.append(dict(binary_temporal=result))
            prepared_decimal_parameter(connection,'0.123456789012345678901234567890')
            records.append(dict(binary_parameter_reexecution='passed'))
        finally:
            cur.execute(f"DROP DATABASE `{database}`")
    output=Path(args.output)
    output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(json.dumps(records,ensure_ascii=False,indent=2), encoding="utf-8")
    print(f"PASS: {len(records)} expression/protocol acceptance groups")


if __name__ == '__main__':
    main()
