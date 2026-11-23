#!/usr/bin/env python3
"""Wire regressions for the VIEW / IS / PS / SHOW release audits.

Run against an isolated local server with pymysql and mysql-connector-python:
  python3 scripts/test_release_audit_1123.py --port 14839
An optional --oracle-port checks portable DDL on a local MySQL 8.0 oracle.
"""
import argparse
import os
import unittest

import pymysql
import mysql.connector

ARGS = None


class ReleaseAudit(unittest.TestCase):
    def setUp(self):
        self.database = f"audit_1123_{os.getpid()}"
        self.connection = pymysql.connect(host="127.0.0.1", port=ARGS.port,
                                          user="root", password=ARGS.password, autocommit=True)
        self.q = self.connection.cursor()
        self.q.execute(f"DROP DATABASE IF EXISTS {self.database}")
        self.q.execute(f"CREATE DATABASE {self.database}")
        self.q.execute(f"USE {self.database}")
        self.q.execute("CREATE TABLE t(id INT PRIMARY KEY,a INT,b VARCHAR(20),d DATE)")
        self.q.execute("INSERT INTO t VALUES(1,1,'alpha','2026-01-01'),(2,2,'beta','2026-01-02'),(3,3,'alpha','2026-01-03')")

    def tearDown(self):
        self.q.execute(f"DROP DATABASE IF EXISTS {self.database}")
        self.connection.close()

    def rows(self, sql):
        self.q.execute(sql)
        return self.q.fetchall()

    def test_aggregate_materialization_and_writes(self):
        for expr in ["GROUP_CONCAT(b ORDER BY id)", "GROUP_CONCAT(DISTINCT a,':',b ORDER BY a)", "MAX(a)"]:
            self.q.execute(f"CREATE OR REPLACE VIEW v AS SELECT b,{expr} gc FROM t GROUP BY b")
            expected = self.rows("SELECT * FROM v ORDER BY b")
            for query in ["SELECT * FROM (SELECT * FROM v) d WHERE b IS NOT NULL ORDER BY b",
                          "WITH c AS (SELECT * FROM v) SELECT * FROM c WHERE b IS NOT NULL ORDER BY b",
                          "SELECT * FROM (SELECT * FROM v) d GROUP BY b,gc HAVING b IS NOT NULL ORDER BY b"]:
                self.assertEqual(self.rows(query), expected, query)
                self.assertEqual(len(self.q.description), 2)
            self.q.execute("CREATE TABLE dst AS SELECT * FROM (SELECT * FROM v) d WHERE b IS NOT NULL")
            self.assertEqual(self.rows("SELECT * FROM dst ORDER BY b"), expected)
            self.q.execute("DROP TABLE dst")

    def test_repeated_query_across_connections(self):
        self.q.execute("CREATE VIEW v AS SELECT id,a FROM t ORDER BY id LIMIT 5 OFFSET 1")
        sql = "SELECT id FROM (SELECT * FROM v) d ORDER BY id,a LIMIT 4"
        for _ in range(4):
            with pymysql.connect(host="127.0.0.1", port=ARGS.port, user="root", password=ARGS.password,
                                 database=self.database, autocommit=True) as c:
                with c.cursor() as q:
                    q.execute(sql)
                    self.assertEqual(q.fetchall(), ((2,), (3,)))

    def test_prepared_show_columns(self):
        self.q.execute("CREATE EVENT ev ON SCHEDULE AT '2030-01-01' DO SELECT 1")
        self.q.execute("CREATE SEQUENCE seq START WITH 7")
        with mysql.connector.connect(host="127.0.0.1", port=ARGS.port, user="root", password=ARGS.password,
                                     database=self.database, use_pure=True) as c:
            for sql in ["SHOW GRANTS", "SHOW GRANTS FOR CURRENT_USER", "SHOW CREATE EVENT ev",
                        "SHOW CREATE USER CURRENT_USER", "SHOW CREATE SEQUENCE seq", "SHOW SEQUENCES",
                        "SHOW TABLES LIKE 't%'", "SHOW DATABASES LIKE 'audit%'"]:
                self.rows(sql)
                text_columns = [x[0] for x in self.q.description]
                with c.cursor(prepared=True) as q:
                    q.execute(sql)
                    q.fetchall()
                    self.assertEqual([x[0] for x in q.description], text_columns, sql)
                    # EXECUTE refreshes description; also check the original
                    # COM_STMT_PREPARE columns retained by the connector.
                    self.assertEqual([x[0] for x in q._prepared["columns"]], text_columns, sql)
                if "CREATE EVENT" in sql:
                    self.assertIn("time_zone", text_columns)
                if " LIKE " in sql:
                    self.assertIn(" (", text_columns[0])

    def test_event_temporal_protocol(self):
        self.assertEqual(self.rows("SHOW EVENTS FROM nonexistent_release_audit"), ())
        self.q.execute("CREATE EVENT recurring ON SCHEDULE EVERY 1 DAY STARTS '2099-01-01 12:34:56' ENDS '2099-02-01 12:34:56' DISABLE DO SELECT 1")
        rows = self.rows("SHOW EVENTS")
        for index in [5, 8, 9]:
            self.assertEqual(self.q.description[index][1], 12)
        self.assertEqual(str(rows[0][8]), "2099-01-01 12:34:56")
        with mysql.connector.connect(host="127.0.0.1", port=ARGS.port, user="root", password=ARGS.password,
                                     database=self.database, use_pure=True) as c:
            with c.cursor(prepared=True) as q:
                q.execute("SHOW EVENTS")
                rows = q.fetchall()
                for index in [5, 8, 9]:
                    self.assertEqual(q._prepared["columns"][index][1], 12)
                self.assertEqual(str(rows[0][8]), "2099-01-01 12:34:56")

    def test_hash_visibility(self):
        username = f"audit_reader_{os.getpid()}"
        self.q.execute(f"CREATE USER '{username}'@'%' IDENTIFIED BY 'audit-secret'")
        try:
            with pymysql.connect(host="127.0.0.1", port=ARGS.port, user=username,
                                 password="audit-secret", autocommit=True) as c:
                with c.cursor() as q:
                    q.execute("SHOW CREATE USER CURRENT_USER")
                    ddl = q.fetchone()[0]
                    self.assertIn("AS <secret>", ddl)
                    self.assertNotIn("$A$", ddl)
                    with self.assertRaises(pymysql.MySQLError) as error:
                        q.execute(f"SHOW TRIGGERS FROM {self.database}")
                    self.assertEqual(error.exception.args[0], 1044)
                    q.execute("SELECT COUNT(*) FROM information_schema.user_attributes")
                    self.assertEqual(q.fetchone()[0], 1)
                    self.q.execute(f"GRANT SELECT ON mysql.* TO '{username}'@'%'")
                    q.execute("SELECT COUNT(*) FROM information_schema.user_attributes")
                    self.assertGreater(q.fetchone()[0], 1)
                    q.execute("SHOW CREATE USER CURRENT_USER")
                    self.assertNotIn("<secret>", q.fetchone()[0])
        finally:
            self.q.execute(f"DROP USER '{username}'@'%'")

    def test_monitoring_host_keys(self):
        extra = pymysql.connect(host="127.0.0.1", port=ARGS.port, user="root", password=ARGS.password)
        try:
            hosts = self.rows("SELECT HOST FROM performance_schema.hosts WHERE CURRENT_CONNECTIONS>0")
            self.assertTrue(hosts)
            for (host,) in hosts:
                self.assertNotIn(":", host)
            accounts = self.rows("SELECT HOST,CURRENT_CONNECTIONS FROM performance_schema.accounts WHERE USER='root' AND CURRENT_CONNECTIONS>0")
            self.assertTrue(accounts)
            self.assertTrue(all(":" not in row[0] for row in accounts))
            self.assertGreaterEqual(sum(row[1] for row in accounts), 2)
        finally:
            extra.close()

    def test_interval_ddl_mysql_replay(self):
        self.q.execute("CREATE TABLE iv(id INT,d DATE,nxt DATE AS(DATE_ADD(d,INTERVAL 1 DAY)),KEY k((DATE_ADD(d,INTERVAL 2 DAY))),CHECK(nxt<DATE_ADD(d,INTERVAL 3 DAY)))")
        ddl = self.rows("SHOW CREATE TABLE iv")[0][1]
        self.assertNotIn("__INTERVAL", ddl)
        self.q.execute(ddl.replace("`iv`", "`iv_copy`", 1).replace("`iv_chk_1`", "`iv_copy_chk_1`"))
        for row in self.rows("SELECT GENERATION_EXPRESSION FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='iv'"):
            self.assertNotIn("__INTERVAL", row[0])
        if ARGS.oracle_port:
            with pymysql.connect(host="127.0.0.1", port=ARGS.oracle_port, user="root", password=ARGS.oracle_password,
                                 autocommit=True) as c:
                with c.cursor() as q:
                    q.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
                    q.execute(f"USE {self.database}")
                    try:
                        q.execute(ddl)
                        q.execute("INSERT INTO iv(id,d) VALUES(1,'2026-01-01')")
                        q.execute("SELECT CAST(nxt AS CHAR) FROM iv")
                        self.assertEqual(q.fetchone()[0], "2026-01-02")
                    finally:
                        q.execute(f"DROP DATABASE {self.database}")

    def test_disabled_binlog(self):
        if self.rows("SELECT @@log_bin")[0][0]:
            self.skipTest("requires a binlog-disabled isolated server")
        for sql in ["SHOW MASTER STATUS", "SHOW BINLOG EVENTS", "SHOW BINLOG EVENTS IN 'absent'", "SHOW REPLICAS"]:
            self.assertEqual(self.rows(sql), (), sql)
        with self.assertRaises(pymysql.MySQLError) as error:
            self.rows("SHOW BINARY LOGS")
        self.assertEqual(error.exception.args[0], 1381)

    def test_design_differences_remain_usable(self):
        self.q.execute("CREATE VIEW v AS SELECT id,a+1 expr FROM t")
        self.q.execute("INSERT INTO v(id) VALUES(9)")
        self.assertEqual(self.rows("SELECT id FROM t WHERE id=9"), ((9,),))
        with pymysql.connect(host="127.0.0.1", port=ARGS.port, user="root", password=ARGS.password,
                             database=self.database, autocommit=True) as c:
            self.q.execute("BEGIN")
            self.rows("SELECT * FROM v")
            with c.cursor() as q:
                q.execute("ALTER VIEW v AS SELECT id,a+100 expr FROM t")
            self.assertEqual(self.rows("SELECT expr FROM v WHERE id=1"), ((101,),))
            self.q.execute("ROLLBACK")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--password", default="root")
    parser.add_argument("--oracle-port", type=int)
    parser.add_argument("--oracle-password", default="")
    ARGS, remaining = parser.parse_known_args()
    unittest.main(argv=[__file__] + remaining)
