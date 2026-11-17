#!/usr/bin/env python3
"""Capture one query as typed JSON, preserving SQL NULL and binary/text values."""
import argparse
import hashlib
import json
import os
from pathlib import Path

from clickbench_results import encode


def capture(connection, sql_path, output):
    import pymysql
    sql = sql_path.read_text(encoding='utf-8')
    output.parent.mkdir(parents=True, exist_ok=True)
    # Exclusive creation prevents a failed rerun from leaving an old success.
    destination = output.open('x', encoding='utf-8')
    try:
        with destination, connection.cursor(pymysql.cursors.SSCursor) as cursor:
            cursor.execute(sql)
            if not cursor.description:
                raise ValueError('capture requires a query with a result set')
            metadata = dict(columns=[column[0] for column in cursor.description],
                            types=[column[1] for column in cursor.description],
                            sql_sha256=hashlib.sha256(sql.encode('utf-8')).hexdigest())
            destination.write(json.dumps(metadata, ensure_ascii=False)[:-1] + ',"rows":[')
            separator = ''
            while rows := cursor.fetchmany(256):
                for row in rows:
                    destination.write(separator + json.dumps([encode(value) for value in row],
                                      ensure_ascii=False, allow_nan=False))
                    separator = ','
            destination.write(']}\n')
    except BaseException:
        output.unlink(missing_ok=True)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--user', default='root')
    parser.add_argument('--password-env', default='MYSQL_PWD')
    parser.add_argument('--database', required=True)
    parser.add_argument('--sql', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    import pymysql
    with pymysql.connect(host=args.host, port=args.port, user=args.user,
            password=os.environ.get(args.password_env, ''), database=args.database,
            charset='utf8mb4', autocommit=True, connect_timeout=10, read_timeout=1800) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET SESSION time_zone='+00:00'")
        capture(connection, args.sql, args.output)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
