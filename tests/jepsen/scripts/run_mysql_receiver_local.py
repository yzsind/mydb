#!/usr/bin/env python3
"""Own a fresh local MySQL source; never attach to an existing instance."""
import argparse
import hashlib
from pathlib import Path
import shutil
import subprocess
import sys
import time

from run_fault_matrix import ROOT, free_port, query
from safety_evidence import write_json, source_identity, snapshot
import pymysql


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--mysqld', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--binlog-checksum', choices=['CRC32', 'NONE'], default='CRC32')
    args = parser.parse_args()
    args.binary, args.mysqld, args.output = args.binary.resolve(), args.mysqld.resolve(), args.output.resolve()
    if not args.output.is_relative_to(Path('/tmp/mydb').resolve()) or not all(p.is_file() for p in (args.binary,args.mysqld)):
        parser.error('requires built binaries and a new output below /tmp/mydb')
    args.output.mkdir(parents=True, exist_ok=False)
    result = {'status': 'incomplete'}
    write_json(args.output/'results.json', result)
    source = None
    try:
        paths = [ROOT/'scripts/test_mysql_receiver_regressions.py', Path(__file__)]
        write_json(args.output/'manifest.json', dict(source_identity(ROOT, paths),
                    binary_sha256=hashlib.sha256(args.binary.read_bytes()).hexdigest(),
                    source_version=subprocess.check_output([str(args.mysqld),'--version'],text=True),
                    source_binlog_checksum=args.binlog_checksum))
        snapshot(ROOT, args.output, paths)
        data = args.output/'source'
        command = [str(args.mysqld), '--no-defaults', '--datadir='+str(data)]
        with (args.output/'initialize.log').open('w') as log:
            subprocess.run(command+['--initialize-insecure'],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=120)
        port, replica = free_port(), free_port()
        with (args.output/'source.log').open('w') as log:
            source = subprocess.Popen(command+['--bind-address=127.0.0.1',f'--port={port}',
                       '--socket='+str(args.output/'source.sock'), '--mysqlx=OFF', '--skip-name-resolve',
                       '--server-id=1001', '--log-bin=binlog', '--gtid-mode=ON', '--enforce-gtid-consistency=ON',
                       '--binlog-format=ROW', '--binlog-row-image=FULL', '--binlog-row-metadata=FULL',
                       '--binlog-checksum='+args.binlog_checksum],
                       stdout=log,stderr=subprocess.STDOUT)
        deadline=time.monotonic()+90
        while True:
            assert source.poll() is None, 'owned MySQL source exited; see source.log'
            try:
                with pymysql.connect(unix_socket=str(args.output/'source.sock'),user='root',autocommit=True,connect_timeout=3,read_timeout=5) as conn:
                    query(conn, "CREATE USER IF NOT EXISTS 'root'@'127.0.0.1' IDENTIFIED BY ''")
                    query(conn, "GRANT ALL PRIVILEGES ON *.* TO 'root'@'127.0.0.1' WITH GRANT OPTION")
                    assert query(conn,'SELECT @@binlog_format,@@binlog_row_image')[0] == ('ROW','FULL')
                    assert query(conn,'SELECT @@global.binlog_checksum')[0][0] == args.binlog_checksum
                break
            except pymysql.MySQLError:
                if time.monotonic()>deadline: raise TimeoutError('local MySQL source startup')
                time.sleep(.1)
        with (args.output/'receiver.log').open('w') as log:
            subprocess.run([sys.executable,str(ROOT/'scripts/test_mysql_receiver_regressions.py'),
                            '--mydb-bin',str(args.binary),'--source-port',str(port),
                            '--replica-port',str(replica),'--bench-rows','64','--bench-rounds','1',
                            '--output',str(args.output/'replica-evidence')],
                           cwd=ROOT,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=900)
        result['status']='pass'
    except BaseException as error:
        result.update(status='fail',error=repr(error))
        raise
    finally:
        if source and source.poll() is None:
            source.terminate()
            try: source.wait(timeout=30)
            except subprocess.TimeoutExpired:
                source.kill();source.wait(timeout=10)
        write_json(args.output/'results.json',result)
    return 0


if __name__=='__main__':
    raise SystemExit(main())
