#!/usr/bin/env python3
"""Exercise atomic column/comment ALTERs against an isolated Docker image.

Run with MYDB_TEST_IMAGE=mydb:latest python3 scripts/test_alter_column_comment.py.
The temporary container and its anonymous data volume are always removed.
"""
import os
import secrets
import subprocess
import time
import unittest


class AlterColumnCommentTests(unittest.TestCase):
    @classmethod
    def docker(cls, *args, check=True):
        return subprocess.run(
            ['docker', *args], text=True, capture_output=True, check=check,
            timeout=120,
        )

    @classmethod
    def sql(cls, statement, check=True):
        result = cls.docker(
            'exec', cls.container, 'sh', '-c',
            'exec mydb-cli --no-config -h 127.0.0.1 -P 8738 -u root '
            '--password="$MYDB_ROOT_PASSWORD" -D test -B -N -e "$1"',
            'sh', statement, check=False,
        )
        if check and result.returncode:
            raise AssertionError(result.stderr)
        return result

    @classmethod
    def ready(cls):
        for _ in range(45):
            result = cls.sql('SELECT 1;', check=False)
            if result.returncode == 0:
                return
            time.sleep(2)
        raise RuntimeError(cls.docker('logs', '--tail', '60', cls.container).stdout)

    @classmethod
    def setUpClass(cls):
        cls.container = 'mydb-alter-comment-' + secrets.token_hex(6)
        cls.docker(
            'run', '-d', '--name', cls.container,
            '-e', 'MYDB_ROOT_PASSWORD=' + secrets.token_urlsafe(24),
            '-e', 'MYDB_MEMORY_TARGET=1G',
            os.environ.get('MYDB_TEST_IMAGE', 'mydb:latest'),
        )
        cls.addClassCleanup(cls.docker, 'rm', '-f', '-v', cls.container)
        cls.ready()

    def test_modify_comment_copy_lock_modes_and_restart(self):
        for mode in ('DEFAULT', 'EXCLUSIVE', 'SHARED', 'NONE'):
            with self.subTest(lock=mode):
                table = 'modify_' + mode.lower()
                lock = '' if mode == 'DEFAULT' else f', LOCK={mode}'
                self.sql(f"CREATE TABLE {table}(id INT PRIMARY KEY, `event` VARCHAR(128) NOT NULL COMMENT 'old', KEY event_idx(`event`)); INSERT INTO {table} VALUES(1, 'login');")
                # Same operation as the failing metadata import, with a narrowing
                # VARCHAR change that requires validation or physical copying.
                self.sql(f"ALTER TABLE {table} MODIFY COLUMN `event` VARCHAR(32) NOT NULL COMMENT '事件'{lock};")
                self.assertEqual(self.sql(f"SELECT id, `event` FROM {table} WHERE `event`='login';").stdout.strip(), '1\tlogin')
                self.assertIn("COMMENT '事件'", self.sql(f'SHOW CREATE TABLE {table};').stdout)
        self.docker('restart', '--time', '30', self.container)
        self.ready()
        for mode in ('default', 'exclusive', 'shared', 'none'):
            definition = self.sql(f'SHOW CREATE TABLE modify_{mode};').stdout
            self.assertIn('varchar(32)', definition.lower())
            self.assertIn("COMMENT '事件'", definition)
            self.assertEqual(self.sql(f'SELECT `event` FROM modify_{mode} WHERE id=1;').stdout.strip(), 'login')

    def test_change_name_type_comment_and_position(self):
        self.sql("CREATE TABLE renamed(id INT PRIMARY KEY, value VARCHAR(16) COMMENT 'old'); INSERT INTO renamed VALUES(1, '7');")
        self.sql("ALTER TABLE renamed CHANGE COLUMN value amount BIGINT NOT NULL COMMENT 'converted' FIRST, LOCK=NONE;")
        self.assertEqual(self.sql('SELECT amount, id FROM renamed;').stdout.strip(), '7\t1')
        columns = self.sql('SHOW FULL COLUMNS FROM renamed;').stdout.splitlines()
        self.assertTrue(columns[0].startswith('amount\t'), columns)
        self.assertIn('converted', columns[0])

    def test_add_comment_to_new_copy_candidate_column(self):
        self.sql('CREATE TABLE added(id INT PRIMARY KEY); INSERT INTO added VALUES(1);')
        self.sql("ALTER TABLE added ADD COLUMN value VARCHAR(16) DEFAULT '7' COMMENT 'added', MODIFY COLUMN value BIGINT COMMENT 'final', ADD INDEX value_idx(value), LOCK=NONE;")
        self.assertEqual(self.sql('SELECT id, value FROM added WHERE value=7;').stdout.strip(), '1\t7')
        self.assertIn("COMMENT 'final'", self.sql('SHOW CREATE TABLE added;').stdout)

    def test_invalid_conversion_preserves_schema_comment_and_rows(self):
        for mode in ('EXCLUSIVE', 'NONE'):
            table = 'failed_' + mode.lower()
            self.sql(f"CREATE TABLE {table}(id INT PRIMARY KEY, value VARCHAR(16) COMMENT 'original'); INSERT INTO {table} VALUES(1, 'not-a-number');")
            before = self.sql(f'SHOW CREATE TABLE {table};').stdout
            result = self.sql(f"ALTER TABLE {table} MODIFY value BIGINT COMMENT 'must-not-publish', LOCK={mode};", check=False)
            self.assertNotEqual(result.returncode, 0)
            self.assertNotIn('no atomic execution plan', result.stderr)
            self.assertEqual(self.sql(f'SHOW CREATE TABLE {table};').stdout, before)
            self.assertEqual(self.sql(f'SELECT id, value FROM {table};').stdout.strip(), '1\tnot-a-number')

    def test_metadata_comment_and_unsupported_algorithm(self):
        self.sql("CREATE TABLE metadata_comment(value VARCHAR(128) NOT NULL COMMENT 'original'); INSERT INTO metadata_comment VALUES('login');")
        self.sql("ALTER TABLE metadata_comment MODIFY value VARCHAR(128) NOT NULL COMMENT 'metadata', ALGORITHM=INSTANT;")
        before = self.sql('SHOW CREATE TABLE metadata_comment;').stdout
        self.assertIn("COMMENT 'metadata'", before)
        result = self.sql("ALTER TABLE metadata_comment MODIFY value VARCHAR(32) NOT NULL COMMENT 'rejected', ALGORITHM=INSTANT;", check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('ERROR 1846', result.stderr)
        self.assertEqual(self.sql('SHOW CREATE TABLE metadata_comment;').stdout, before)
        self.assertEqual(self.sql('SELECT value FROM metadata_comment;').stdout.strip(), 'login')

    def test_comment_length_validation_and_empty_comment(self):
        self.sql("CREATE TABLE comments(id INT PRIMARY KEY, value VARCHAR(16) COMMENT 'original'); INSERT INTO comments VALUES(1, '7');")
        before = self.sql('SHOW CREATE TABLE comments;').stdout
        too_long = 'x' * 1025
        result = self.sql(f"ALTER TABLE comments MODIFY value BIGINT COMMENT '{too_long}', ALGORITHM=COPY;", check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('ERROR 1629', result.stderr)
        self.assertEqual(self.sql('SHOW CREATE TABLE comments;').stdout, before)
        self.assertEqual(self.sql('SELECT value FROM comments;').stdout.strip(), '7')
        self.sql("ALTER TABLE comments MODIFY value BIGINT COMMENT '', ALGORITHM=COPY;")
        self.assertNotIn("COMMENT 'original'", self.sql('SHOW CREATE TABLE comments;').stdout)
        self.assertEqual(self.sql('SELECT value FROM comments;').stdout.strip(), '7')


if __name__ == '__main__':
    unittest.main(verbosity=2)
