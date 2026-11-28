"""Local SQL differential and MyDB -> MySQL ROW/FULL replication verification.

All processes and generated artifacts belong to a fresh directory in /tmp/mydb.
Usage: python3 verify.py --binary /absolute/path/to/mydb [--baseline] [--cdc]
Requires PyMySQL and local MySQL 8.0 mysqld/mysqlbinlog executables on PATH.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import struct
import tempfile
import threading
import time
import zlib

import pymysql


def main():
    os.environ['TMPDIR'] = '/tmp/mydb'
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True)
    parser.add_argument('--baseline', action='store_true')
    parser.add_argument('--cdc', action='store_true')
    parser.add_argument('--raw-only', action='store_true')
    parser.add_argument('--source-port', type=int, default=15837)
    parser.add_argument('--mysql-port', type=int, default=15838)
    args = parser.parse_args()
    Path('/tmp/mydb').mkdir(exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix='issues-108-123-', dir='/tmp/mydb'))
    output = (root / 'results.jsonl').open('w')
    processes = []
    failures = []

    def emit(label, **fields):
        line = json.dumps(dict(label=label, **fields), ensure_ascii=False, default=str)
        print(line, flush=True)
        output.write(line + '\n')
        output.flush()

    def connect(port):
        return pymysql.connect(host='127.0.0.1', port=port, user='root', password='',
                               charset='utf8mb4', autocommit=True, read_timeout=60)

    def query(port, sql, params=None):
        with connect(port) as connection:
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute(sql, params)
                return cursor.fetchall()

    def start(command, log, env=None):
        process = subprocess.Popen(command, stdout=log.open('ab'), stderr=subprocess.STDOUT, env=env)
        processes.append(process)
        return process

    def ready(port, process):
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError(f'process exited; inspect {root}')
            try:
                query(port, 'SELECT 1')
                return
            except pymysql.MySQLError:
                time.sleep(.1)
        raise TimeoutError(port)

    def probe(label, sql, ports=None):
        results = []
        for port in ports or [args.source_port, args.mysql_port]:
            try:
                results.append(dict(port=port, ok=True, rows=query(port, sql)))
            except pymysql.MySQLError as error:
                results.append(dict(port=port, ok=False, code=error.args[0], message=error.args[1]))
        emit(label, sql=sql, results=results)
        return results

    def check(label, function):
        if args.raw_only and label != '123-raw-heartbeat-cursor-under-write-rotate':
            return
        try:
            function()
            emit(label, status='PASS')
        except Exception as error:
            failures.append(label)
            emit(label, status='FAIL', error=repr(error))

    source, mysql = args.source_port, args.mysql_port
    try:
        binary = Path(args.binary).resolve()
        emit('start', root=root, binary=binary, sha256=hashlib.sha256(binary.read_bytes()).hexdigest())
        env = dict(os.environ, MYDB_ROOT_PASSWORD='', MYDB_MEMORY_TARGET='512M', MYDB_ROLE='standalone')
        source_process = start([str(binary), '--host', '127.0.0.1', '--port', str(source),
                                '--data-dir', str(root / 'mydb'), '--log-bin', '--server-id', '177'],
                               root / 'mydb.log', env)
        ready(source, source_process)
        mysqld = Path(shutil.which('mysqld')).resolve()
        mysql_args = [str(mysqld), '--no-defaults', '--basedir=' + str(mysqld.parent.parent),
                      '--datadir=' + str(root / 'mysql'), '--innodb-buffer-pool-size=64M',
                      '--innodb-redo-log-capacity=64M']
        with (root / 'mysql-init.log').open('wb') as log:
            subprocess.run(mysql_args + ['--initialize-insecure'], stdout=log, stderr=subprocess.STDOUT, check=True)
        mysql_process = start(mysql_args + ['--port=' + str(mysql), '--socket=' + str(root / 'mysql.sock'),
                              '--bind-address=127.0.0.1', '--mysqlx=OFF', '--server-id=178',
                              '--gtid-mode=ON', '--enforce-gtid-consistency=ON',
                              '--log-bin=' + str(root / 'mysql-bin')], root / 'mysql.log')
        ready(mysql, mysql_process)
        for port in [source, mysql]:
            query(port, 'CREATE DATABASE audit')

        def defaults():
            for i, (kind, literal) in enumerate([
                ('VARCHAR(32)', "_utf8mb4'默认值'"), ('VARCHAR(32)', "N'中文'"),
                ('VARCHAR(8) CHARACTER SET latin1', "_latin1 X'E9'"),
                ('VARBINARY(8)', "_binary X'00FF'"),
            ]):
                for result in probe('108-create', f'CREATE TABLE audit.d{i}(id INT PRIMARY KEY,c {kind} DEFAULT {literal})'):
                    assert result['ok'], result
                for port in [source, mysql]:
                    query(port, f'INSERT INTO audit.d{i}(id) VALUES(1)')
                values = probe('108-values', f'SELECT id,HEX(c) AS c FROM audit.d{i}')
                assert values[0]['rows'] == values[1]['rows'], values
        check('108-introduced-defaults', defaults)

        def numeric():
            for i, alias in enumerate(['DECIMAL', 'NUMERIC', 'DEC']):
                for j, suffix in enumerate(['', ' UNSIGNED', ' ZEROFILL', ' UNSIGNED ZEROFILL', ' SIGNED', ' ZEROFILL UNSIGNED']):
                    for result in probe('109-create', f'CREATE TABLE audit.n{i}_{j}(c {alias}(5,2){suffix})'):
                        assert result['ok'], result
                    values = probe('109-metadata', f"SELECT COLUMN_TYPE,NUMERIC_PRECISION,NUMERIC_SCALE FROM information_schema.columns WHERE TABLE_SCHEMA='audit' AND TABLE_NAME='n{i}_{j}'")
                    assert values[0]['rows'] == values[1]['rows'], values
        check('109-decimal-aliases', numeric)

        def case_names():
            assert query(source, 'SELECT @@lower_case_table_names AS n')[0]['n'] == 1
            query(source, 'CREATE TABLE audit.T_UPPER(id INT)')
            query(source, 'INSERT INTO audit.t_upper VALUES(1)')
            assert query(source, 'SELECT * FROM AUDIT.T_UPPER') == [{'id': 1}]
            assert not probe('110-readonly', 'SET GLOBAL lower_case_table_names=0', [source])[0]['ok']
            emit('110-metadata', rows=query(source, "SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA='audit' AND TABLE_NAME='T_UPPER'"))
        check('110-case-insensitive-contract', case_names)

        def checks():
            sql = "CREATE TABLE audit.bad_check(id INT NOT NULL,gender CHAR(1),CONSTRAINT gender_check CHECK ((gender = ANY (ARRAY`'M', 'F'`))))"
            assert all(not r['ok'] for r in probe('118-invalid-upstream-sql', sql))
            for port in [source, mysql]:
                query(port, "CREATE TABLE audit.good_check(id INT,gender CHAR(1),CHECK(gender IN ('M','F')))")
                query(port, "INSERT INTO audit.good_check VALUES(1,'M'),(2,'F'),(3,NULL)")
            assert all(r.get('code') == 3819 for r in probe('118-check-enforced', "INSERT INTO audit.good_check VALUES(4,'X')"))
            for sql in ["CREATE TABLE audit.pg_check(c INT,CHECK(c::integer > 0))",
                        "CREATE TABLE audit.pg_regex(c VARCHAR(8),CHECK(c ~ '^[A-Z]+$'))"]:
                assert all(not r['ok'] for r in probe('119-foreign-dialect-examples-not-original-sql', sql))
        check('118-119-check-dialect-and-enforcement', checks)

        def identifiers():
            for length in [64, 65]:
                name = 'c' * length
                results = probe('120-boundary', f'CREATE TABLE audit.ident{length}(id INT,CONSTRAINT `{name}` CHECK(id>0))')
                assert all(r['ok'] == (length == 64) for r in results), results
                if length == 65:
                    assert all(r.get('code') == 1059 for r in results), results
        check('120-identifier-limit', identifiers)

        def partitions():
            for port in [source, mysql]:
                query(port, 'CREATE TABLE audit.partition_parent(id INT PRIMARY KEY) PARTITION BY HASH(id) PARTITIONS 4')
                query(port, 'INSERT INTO audit.partition_parent VALUES(1),(2),(3)')
            data = probe('121-rows', 'SELECT * FROM audit.partition_parent ORDER BY id')
            assert data[0]['rows'] == data[1]['rows'], data
            metadata = probe('121-partitions', "SELECT PARTITION_NAME FROM information_schema.partitions WHERE TABLE_SCHEMA='audit' AND TABLE_NAME='partition_parent' ORDER BY PARTITION_NAME")
            assert metadata[0]['rows'] == metadata[1]['rows'], metadata
            assert all(r.get('code') == 1146 for r in probe('121-partitions-are-not-tables', 'SELECT * FROM audit.partition_parent_p0'))
        check('121-mysql-partition-object-model', partitions)

        def vectors():
            query(source, 'CREATE TABLE audit.vec(id INT PRIMARY KEY,v VECTOR(3))')
            ddl = query(source, 'SHOW CREATE TABLE audit.vec')[0]['Create Table']
            emit('122-metadata', ddl=ddl, columns=query(source, "SELECT COLUMN_TYPE FROM information_schema.columns WHERE TABLE_SCHEMA='audit' AND TABLE_NAME='vec' AND COLUMN_NAME='v'"))
            assert 'VECTOR(3' in ddl.upper(), ddl
            query(source, ddl.replace('`vec`', '`audit`.`vec_copy`', 1))
            for table in ['vec', 'vec_copy']:
                query(source, f"INSERT INTO audit.{table} VALUES(1,STRING_TO_VECTOR('[1,2,3]'))")
                assert query(source, f'SELECT VECTOR_DIM(v) AS n,VECTOR_TO_STRING(v) AS v FROM audit.{table}') == [{'n': 3, 'v': '[1,2,3]'}]
                assert not probe('122-reject-dimension-mismatch', f"INSERT INTO audit.{table} VALUES(2,STRING_TO_VECTOR('[1,2]'))", [source])[0]['ok']
            query(source, 'CREATE TABLE audit.vec_any(id INT PRIMARY KEY,v VECTOR)')
            query(source, "INSERT INTO audit.vec_any VALUES(1,STRING_TO_VECTOR('[1,2]')),(2,STRING_TO_VECTOR('[1,2,3]'))")
            assert query(source, 'SELECT VECTOR_DIM(v) AS n FROM audit.vec_any ORDER BY id') == [{'n': 2}, {'n': 3}]
            emit('122-bare-vector-is-variable-dimension', rows=query(source, 'SELECT id,VECTOR_DIM(v) AS n FROM audit.vec_any ORDER BY id'))
        check('122-local-vector-dimension-roundtrip', vectors)

        if args.cdc or args.raw_only:
            def raw_heartbeat_race():
                query(source, 'CREATE DATABASE heartbeat_race')
                query(source, 'CREATE TABLE heartbeat_race.t(id INT PRIMARY KEY,v VARCHAR(128))')
                coord = query(source, 'SHOW MASTER STATUS')[0]
                consumed = (coord['File'], int(coord['Position']))
                stop = threading.Event()
                done = threading.Event()
                writer_errors = []

                def write():
                    try:
                        with connect(source) as connection, connection.cursor() as cursor:
                            for i in range(1000):
                                if stop.is_set():
                                    break
                                cursor.execute('INSERT INTO heartbeat_race.t VALUES(%s,REPEAT(\'x\',128))', (i,))
                                if i % 5 == 0:
                                    cursor.execute('FLUSH BINARY LOGS')
                    except Exception as error:
                        writer_errors.append(repr(error))
                    finally:
                        done.set()

                with connect(source) as dump:
                    with dump.cursor() as cursor:
                        cursor.execute('SET @master_heartbeat_period=1000000')
                        cursor.execute("SET @master_binlog_checksum='CRC32'")
                    worker = threading.Thread(target=write)
                    payload = struct.pack('<IHI', consumed[1], 0, 179) + consumed[0].encode()
                    dump._execute_command(18, payload)
                    worker.start()
                    events = heartbeats = idle = 0
                    try:
                        deadline = time.monotonic() + 60
                        while time.monotonic() < deadline:
                            packet = dump._read_packet().get_all_data()
                            assert packet[0] == 0, packet.hex()
                            event = packet[1:]
                            timestamp, kind, server, size, position, flags = struct.unpack_from('<IBIIIH', event)
                            assert len(event) == size, (len(event), size)
                            assert struct.unpack_from('<I', event, size - 4)[0] == zlib.crc32(event[:-4])
                            events += 1
                            if kind == 27:
                                heartbeat = (event[19:-4].decode(), position)
                                assert heartbeat == consumed, dict(consumed=consumed, heartbeat=heartbeat, events=events)
                                heartbeats += 1
                                if done.is_set():
                                    idle += 1
                                    if idle == 3:
                                        break
                            elif kind == 4:
                                consumed = (event[27:-4].decode(), struct.unpack_from('<Q', event, 19)[0])
                            elif not flags & 0x20:
                                consumed = (consumed[0], position)
                        else:
                            raise TimeoutError('raw dump race')
                        assert not writer_errors, writer_errors
                        emit('123-raw-dump-race', events=events, heartbeats=heartbeats, cursor=consumed, inserts=1000, rotations=200)
                    finally:
                        stop.set()
                        worker.join(timeout=30)
                        assert not worker.is_alive(), 'writer did not finish'
            check('123-raw-heartbeat-cursor-under-write-rotate', raw_heartbeat_race)

            def status():
                return query(mysql, 'SHOW REPLICA STATUS')[0]

            def caught():
                target = query(source, 'SHOW MASTER STATUS')[0]['Executed_Gtid_Set']
                started = time.monotonic()
                assert query(mysql, 'SELECT WAIT_FOR_EXECUTED_GTID_SET(%s,30) AS waited', (target,)) == [{'waited': 0}], status()
                state = status()
                assert state['Replica_IO_Running'] == 'Yes' and state['Replica_SQL_Running'] == 'Yes', state
                emit('123-caught-up', seconds=round(time.monotonic() - started, 3), **{k: state[k] for k in ['Seconds_Behind_Source', 'Last_IO_Errno', 'Last_SQL_Errno', 'Source_Log_File', 'Read_Source_Log_Pos', 'Relay_Source_Log_File', 'Exec_Source_Log_Pos']})

            def cdc():
                query(source, "CREATE USER 'cdc'@'127.0.0.1' IDENTIFIED WITH mysql_native_password BY 'local-cdc'")
                query(source, "GRANT REPLICATION SLAVE,REPLICATION CLIENT ON *.* TO 'cdc'@'127.0.0.1'")
                # Start at a fresh file so the SQL probes are not replayed.
                query(source, 'FLUSH BINARY LOGS')
                coord = query(source, 'SHOW MASTER STATUS')[0]
                # The earlier SQL probes are outside this replication snapshot.
                query(mysql, 'SET GLOBAL gtid_purged=%s', ('+' + coord['Executed_Gtid_Set'],))
                query(mysql, f"CHANGE REPLICATION SOURCE TO SOURCE_HOST='127.0.0.1',SOURCE_PORT={source},SOURCE_USER='cdc',SOURCE_PASSWORD='local-cdc',SOURCE_LOG_FILE='{coord['File']}',SOURCE_LOG_POS={coord['Position']},SOURCE_HEARTBEAT_PERIOD=0.2,SOURCE_CONNECT_RETRY=1")
                query(mysql, 'START REPLICA')
                query(source, 'CREATE DATABASE cdc')
                for table in range(28):
                    if table % 3 == 0:
                        ddl = '(id INT PRIMARY KEY,n BIGINT,u BIGINT UNSIGNED,d DECIMAL(30,10),f DOUBLE,s VARCHAR(64),b VARBINARY(64),j JSON,dt DATETIME(6),ts TIMESTAMP(6),ti TIME(6),bt BIT(9))'
                    elif table % 3 == 1:
                        ddl = '(id INT PRIMARY KEY,s LONGTEXT,b LONGBLOB)'
                    else:
                        ddl = '(id INT PRIMARY KEY,v INT) PARTITION BY HASH(id) PARTITIONS 4'
                    query(source, f'CREATE TABLE cdc.t{table}' + ddl)
                caught()
                for phase in range(3):
                    for table in range(28):
                        for i in range(20):
                            key = phase * 20 + i
                            if table % 3 == 0:
                                values = f"({key},-{key},18446744073709551615,123456789.1234567890,1.25,'中文-{key}',X'00FF0102','{{\"x\":{key}}}','2026-11-27 12:34:56.123456','2026-11-27 12:34:56.123456','-12:34:56.123456',b'101010101')"
                            elif table % 3 == 1:
                                values = f"({key},REPEAT('text',2048),REPEAT(X'00FF',4096))"
                            else:
                                values = f'({key},{key * 3})'
                            query(source, f'INSERT INTO cdc.t{table} VALUES' + values)
                        assignment = ["n=n-1,j=JSON_OBJECT('x',id+1)", "s=CONCAT(s,'tail')", 'v=v+1'][table % 3]
                        query(source, f'UPDATE cdc.t{table} SET {assignment} WHERE id%5=0')
                        query(source, f'DELETE FROM cdc.t{table} WHERE id%17=0')
                    query(source, 'FLUSH BINARY LOGS')
                    caught()
                    for table in range(28):
                        fields = ["id,n,u,d,ROUND(f,3),s,HEX(b),JSON_EXTRACT(j,'$.x'),dt,ts,ti,HEX(bt)", 'id,LENGTH(s),SHA2(s,256),LENGTH(b),SHA2(b,256)', '*'][table % 3]
                        sql = f'SELECT {fields} FROM cdc.t{table} ORDER BY id'
                        left, right = query(source, sql), query(mysql, sql)
                        assert left == right, (phase, table, left[:2], right[:2])
                    emit('123-data-match', phase=phase, tables=28)
                    query(mysql, 'STOP REPLICA IO_THREAD')
                    query(mysql, 'START REPLICA IO_THREAD')
                    caught()
                heartbeats = 'SELECT COUNT_RECEIVED_HEARTBEATS AS n FROM performance_schema.replication_connection_status'
                before = int(query(mysql, heartbeats)[0]['n'])
                started = time.monotonic()
                while time.monotonic() - started < 35:
                    time.sleep(1)
                    state = status()
                    assert state['Replica_IO_Running'] == 'Yes' and state['Replica_SQL_Running'] == 'Yes', state
                    assert state['Seconds_Behind_Source'] == 0, state
                after = int(query(mysql, heartbeats)[0]['n'])
                assert after - before >= 100, (before, after)
                emit('123-idle-heartbeats', seconds=35, received=after - before)
                query(source, 'INSERT INTO cdc.t2 VALUES(1000,42)')
                caught()
                assert query(mysql, 'SELECT v FROM cdc.t2 WHERE id=1000') == [{'v': 42}]
                with (root / 'mysqlbinlog.txt').open('wb') as log:
                    subprocess.run([shutil.which('mysqlbinlog'), '--read-from-remote-server', '--to-last-log', '--host=127.0.0.1', f'--port={source}', '--user=cdc', '--password=local-cdc', '--base64-output=DECODE-ROWS', '-vv', coord['File']], stdout=log, stderr=subprocess.STDOUT, check=True)
            check('123-row-full-mysql-replica-convergence', cdc)
        emit('finished', failures=failures, baseline=args.baseline)
        if failures and not args.baseline:
            raise SystemExit(1)
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                process.terminate()
        for process in reversed(processes):
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        output.close()


if __name__ == '__main__':
    main()
