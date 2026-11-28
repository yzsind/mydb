"""Interleave local optimized binaries and compare a common SQL workload."""
import argparse
import json
import os
from pathlib import Path
import statistics
import subprocess
import tempfile
import time

import pymysql


def measure(binary):
    root = Path(tempfile.mkdtemp(prefix='issues-sql-perf-', dir='/tmp/mydb'))
    log = (root / 'server.log').open('wb')
    env = dict(os.environ, TMPDIR='/tmp/mydb', MYDB_ROOT_PASSWORD='', MYDB_ROLE='standalone', MYDB_MEMORY_TARGET='512M')
    process = subprocess.Popen([binary, '--host', '127.0.0.1', '--port', '16837', '--data-dir', str(root / 'data')], env=env, stdout=log, stderr=log)
    try:
        for _ in range(600):
            if process.poll() is not None:
                raise RuntimeError(root)
            try:
                connection = pymysql.connect(host='127.0.0.1', port=16837, user='root', autocommit=True)
                break
            except pymysql.MySQLError:
                time.sleep(.1)
        else:
            raise TimeoutError(root)
        with connection, connection.cursor() as cursor:
            cursor.execute('CREATE DATABASE perf')
            cursor.execute('USE perf')
            cursor.execute('CREATE TABLE t(id INT PRIMARY KEY,n INT,d DECIMAL(20,4),s VARCHAR(64),KEY(n))')
            for batch in range(20):
                values = ','.join(f"({batch * 500 + i},{i % 100},12.3456,'row-{i}')" for i in range(500))
                cursor.execute('INSERT INTO t VALUES' + values)
            for i in range(100):
                cursor.execute('SELECT * FROM t WHERE id=%s', (i * 97,))
                cursor.fetchall()
            results = {}
            started = time.perf_counter()
            for i in range(1000):
                cursor.execute('SELECT s,d FROM t WHERE id=%s', ((i * 97) % 10000,))
                assert len(cursor.fetchall()) == 1
            results['point_select_ms'] = (time.perf_counter() - started) * 1000
            started = time.perf_counter()
            for i in range(100):
                cursor.execute('SELECT n,COUNT(*),SUM(d) FROM t WHERE id BETWEEN %s AND %s GROUP BY n', (i * 50, i * 50 + 500))
                assert len(cursor.fetchall()) == 100
            results['range_aggregate_ms'] = (time.perf_counter() - started) * 1000
            started = time.perf_counter()
            for i in range(200):
                cursor.execute('UPDATE t SET d=d+0.01,s=CONCAT(\'v\',n) WHERE id=%s', (i * 37,))
            results['update_ms'] = (time.perf_counter() - started) * 1000
            started = time.perf_counter()
            for i in range(50):
                cursor.execute(f'CREATE TABLE d{i}(id INT PRIMARY KEY,c DECIMAL(5,2) UNSIGNED ZEROFILL DEFAULT 1.25,s VARCHAR(32) DEFAULT \'value\')')
            results['create_table_ms'] = (time.perf_counter() - started) * 1000
            return results
    finally:
        process.terminate()
        try:
            process.wait(20)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        log.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('before')
    parser.add_argument('after')
    args = parser.parse_args()
    samples = {'before': [], 'after': []}
    for order in [('before', 'after'), ('after', 'before'), ('before', 'after'), ('after', 'before'), ('before', 'after')]:
        for label in order:
            sample = measure(str(Path(getattr(args, label)).resolve()))
            samples[label].append(sample)
            print(json.dumps(dict(label=label, sample=sample)), flush=True)
    medians = {label: {key: statistics.median(sample[key] for sample in group) for key in group[0]} for label, group in samples.items()}
    ratios = {key: medians['after'][key] / medians['before'][key] for key in medians['before']}
    print(json.dumps(dict(medians=medians, after_over_before=ratios)), flush=True)


if __name__ == '__main__':
    main()
