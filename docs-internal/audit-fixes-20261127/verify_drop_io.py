"""Inject manifest write failure on a dedicated binlog-enabled test instance."""
import argparse
import json
from pathlib import Path
import pymysql

p = argparse.ArgumentParser()
p.add_argument('--port', type=int, required=True)
p.add_argument('--data-dir', type=Path, required=True)
a = p.parse_args()
c = pymysql.connect(host='127.0.0.1', port=a.port, user='root', password='root', autocommit=True)
with c.cursor() as s:
    s.execute('CREATE DATABASE IF NOT EXISTS dropio1127')
    s.execute('USE dropio1127')
    s.execute('CREATE TABLE `a``b`(id INT)')
    s.execute('CREATE TEMPORARY TABLE tmp(id INT)')
    s.execute('DROP TABLE `a``b`,tmp')
    s.execute('SHOW BINLOG EVENTS')
    assert any('DROP TABLE `dropio1127`.`a``b`' in str(row[-1]) for row in s.fetchall())
    s.execute('CREATE TABLE persistent_drop(id INT)')
    s.execute('CREATE TEMPORARY TABLE tmp(id INT)')
    manifest = a.data_dir / 'binlog' / 'manifest.json'
    saved = manifest.with_name('manifest.saved')
    manifest.rename(saved)
    manifest.mkdir()
    try:
        try:
            s.execute('DROP TABLE persistent_drop,tmp')
        except pymysql.MySQLError as err:
            result = {'error': err.args, 'escaped_name_logged': True}
        else:
            s.execute('SHOW WARNINGS')
            result = {'returned_success': True, 'warnings': s.fetchall()}
    finally:
        manifest.rmdir()
        saved.rename(manifest)
    print(json.dumps(result))
    assert result.get('error', [None])[0] == 1026, result
c.close()
