#!/usr/bin/env python3
"""Wait for applied replication progress and compare complete typed table rows.

The source must be quiescent after the workload. Temporary exact row multisets
spill to SQLite under /tmp/mydb, so verification memory is bounded by a batch.
"""
import argparse
import contextlib
import decimal
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import tempfile
import time

from clickbench_results import encode


def query(connection, sql, parameters=None):
    import pymysql
    with connection.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute(sql, parameters)
        return cursor.fetchall()


def source_position(connection, physical=False):
    if physical:
        rows = query(connection, 'SHOW MYDB MASTER STATUS')
        if len(rows) != 1:
            raise ValueError('source must report exactly one physical replication status')
        return int(rows[0]['Current_LSN'])
    rows = query(connection, 'SHOW MASTER STATUS')
    if len(rows) != 1:
        raise ValueError('source must report exactly one binlog status')
    return str(rows[0]['File']), int(rows[0]['Position'])


def position_reached(file, position, expected):
    expected_file, expected_position = expected
    if file == expected_file:
        return int(position) >= expected_position
    left = re.fullmatch(r'(.*\.)([0-9]+)', str(file))
    right = re.fullmatch(r'(.*\.)([0-9]+)', expected_file)
    return bool(left and right and left[1] == right[1] and int(left[2]) > int(right[2]))


def applied(status, expected, physical=False):
    if physical:
        if (str(status.get('Last_Status', '')).lower().startswith(('error', 'stopped:'))
                or str(status.get('Resync_Required', 'no')).lower() in ('yes', '1', 'true')):
            raise RuntimeError(f'physical replication failed: {status}')
        return (str(status['Receiver_Running']).lower() == 'yes'
                and int(status['Durable_Applied_LSN']) >= expected)
    for kind in ('IO', 'SQL'):
        if int(status.get(f'Last_{kind}_Errno', 0) or 0) or status.get(f'Last_{kind}_Error'):
            raise RuntimeError(f'replica {kind} error: {status.get(f"Last_{kind}_Error")}')
    return (status['Replica_IO_Running'] == 'Yes' and status['Replica_SQL_Running'] == 'Yes'
            and position_reached(status['Relay_Source_Log_File'], status['Exec_Source_Log_Pos'], expected))


def wait_applied(connection, expected, timeout, physical=False):
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError('catchup timeout must be positive and finite')
    deadline = time.monotonic() + timeout
    command = 'SHOW MYDB REPLICA STATUS' if physical else 'SHOW REPLICA STATUS'
    last = None
    while True:
        rows = query(connection, command)
        if len(rows) != 1:
            raise ValueError('replica must report exactly one replication channel')
        last = rows[0]
        if applied(last, expected, physical):
            return last
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f'replica did not apply source position {expected}; last status: {last}')
        time.sleep(min(.2, remaining))


def quote_identifier(value):
    return '`' + value.replace('`', '``') + '`'


def row_token(row):
    def cell(value):
        if isinstance(value, decimal.Decimal):
            from _tabular_results import normalize_cell
            return {'decimal': normalize_cell(str(value), 'decimal')}
        return encode(value)
    return json.dumps([cell(value) for value in row], ensure_ascii=False,
                      allow_nan=False, separators=(',', ':')).encode('utf-8')


def table_names(connection, database):
    rows = query(connection, 'SELECT TABLE_NAME FROM information_schema.TABLES '
                 'WHERE TABLE_SCHEMA=%s AND TABLE_TYPE=\'BASE TABLE\' ORDER BY TABLE_NAME', (database,))
    return [row['TABLE_NAME'] for row in rows]


def compare_tables(source, replica, database, tables=None):
    source_tables = table_names(source, database)
    replica_tables = table_names(replica, database)
    if not source_tables or source_tables != replica_tables:
        raise ValueError(f'missing or mismatched table sets: {source_tables} != {replica_tables}')
    selected = source_tables if tables is None else tables
    if not selected or len(set(selected)) != len(selected) or not set(selected) <= set(source_tables):
        raise ValueError('table selection is empty, duplicated or missing from the database')
    Path('/tmp/mydb').mkdir(parents=True, exist_ok=True)
    counts = {}
    with tempfile.TemporaryDirectory(dir='/tmp/mydb', prefix='replication-compare-') as temporary:
        with contextlib.closing(sqlite3.connect(str(Path(temporary) / 'rows.sqlite'))) as spool:
            spool.execute('PRAGMA cache_size=-8192')
            spool.execute('CREATE TABLE rows(value BLOB PRIMARY KEY, balance INTEGER NOT NULL) WITHOUT ROWID')
            for table in selected:
                columns = None
                count = [0, 0]
                spool.execute('DELETE FROM rows')
                for side, connection in enumerate((source, replica)):
                    import pymysql
                    with connection.cursor(pymysql.cursors.SSCursor) as cursor:
                        cursor.execute(f'SELECT * FROM {quote_identifier(database)}.{quote_identifier(table)}')
                        names = [column[0] for column in cursor.description]
                        if columns is not None and names != columns:
                            raise ValueError(f'{table}: result columns differ')
                        columns = names
                        while batch := cursor.fetchmany(512):
                            count[side] += len(batch)
                            spool.executemany('INSERT INTO rows VALUES(?, ?) ON CONFLICT(value) '
                                              'DO UPDATE SET balance=balance+excluded.balance',
                                              ((row_token(row), 1 if side == 0 else -1) for row in batch))
                if spool.execute('SELECT 1 FROM rows WHERE balance != 0 LIMIT 1').fetchone():
                    raise ValueError(f'{table}: complete typed rows differ (counts={count})')
                counts[table] = count[0]
                spool.commit()
    return counts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for endpoint in ('source', 'replica'):
        parser.add_argument(f'--{endpoint}-host', default='127.0.0.1')
        parser.add_argument(f'--{endpoint}-port', type=int, required=True)
        parser.add_argument(f'--{endpoint}-user', default='root')
        parser.add_argument(f'--{endpoint}-password-env', default='MYSQL_PWD')
    parser.add_argument('--database', required=True)
    parser.add_argument('--timeout', type=float, default=300)
    parser.add_argument('--physical', action='store_true')
    args = parser.parse_args()
    import pymysql
    with contextlib.ExitStack() as stack:
        connections = []
        for endpoint in ('source', 'replica'):
            connection = pymysql.connect(host=getattr(args, endpoint + '_host'),
                port=getattr(args, endpoint + '_port'), user=getattr(args, endpoint + '_user'),
                password=os.environ.get(getattr(args, endpoint + '_password_env'), ''),
                autocommit=True, charset='utf8mb4', connect_timeout=10, read_timeout=300, write_timeout=30)
            stack.callback(connection.close)
            query(connection, "SET SESSION time_zone='+00:00'")
            connections.append(connection)
        source, replica = connections
        expected = source_position(source, args.physical)
        wait_applied(replica, expected, args.timeout, args.physical)
        if not args.physical:
            query(replica, 'STOP REPLICA')
        counts = compare_tables(source, replica, args.database)
        if source_position(source, args.physical) != expected:
            raise RuntimeError('source changed during verification; stop all writers and retry')
        print(json.dumps(dict(status='passed', position=expected, tables=counts)))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
