#!/usr/bin/env python3
"""Exercise row security on an owned loopback server; retain evidence in /tmp/mydb."""
import argparse
import json
from pathlib import Path
import tempfile
import threading
import time

import pymysql
import mysql.connector
from test_native_readiness import Servers, free_port, query


def connect(port, user):
    return pymysql.connect(host="127.0.0.1", port=port, user=user, password="secret",
                           database="test", autocommit=True, read_timeout=30, write_timeout=30)


def denied(connection, sql):
    try:
        query(connection, sql)
    except pymysql.MySQLError as error:
        # PyMySQL decodes custom error numbers as a signed 16-bit integer.
        assert error.args[0] & 0xffff in (50130, 50131), (sql, error)
        return
    raise AssertionError("Expected RLS rejection: " + sql)


def run(binary, root, binlog=False):
    servers = Servers(binary, root)
    port = free_port()
    clients = []
    passed = []
    try:
        server_options = {"MYDB_LOG_BIN": "1"} if binlog else None
        process, admin = servers.start("database", port, server_options)
        for sql in [
            "CREATE TABLE secured(id INT PRIMARY KEY,tenant_id INT,payload VARCHAR(40),KEY tenant_key(tenant_id,id))",
            "INSERT INTO secured VALUES(1,1,'one'),(2,2,'two'),(3,NULL,'unknown')",
            "CREATE USER alice IDENTIFIED BY 'secret'",
            "GRANT SELECT,INSERT,UPDATE,DELETE ON test.secured TO alice",
            "ALTER TABLE secured ENABLE ROW LEVEL SECURITY",
        ]:
            query(admin, sql)
        alice = connect(port, "alice")
        clients.append(alice)
        assert query(alice, "SELECT * FROM secured") == ()
        assert query(admin, "SELECT COUNT(*) FROM secured") == ((3,),)
        passed.append("default_deny_super_bypass")

        query(admin, "CREATE POLICY tenant ON secured USING(tenant_id=1)")
        if binlog:
            events = query(admin, "SHOW BINLOG EVENTS")
            ddl = [str(event[-1]).lower() for event in events if event[2] == "Query"]
            assert any("create policy tenant" in sql for sql in ddl), ddl
            assert any("enable row level security" in sql for sql in ddl), ddl
            passed.append("binlog_emits_full_policy_ddl")
        assert query(alice, "SELECT id FROM secured ORDER BY id") == ((1,),)
        assert query(alice, "SELECT COUNT(*),SUM(id),MIN(id),MAX(id) FROM secured") == ((1, 1, 1, 1),)
        assert query(alice, "SELECT a.id,b.id FROM secured a LEFT JOIN secured b ON a.id=b.id ORDER BY a.id") == ((1, 1),)
        assert query(alice, "SELECT id FROM secured UNION ALL SELECT id FROM secured ORDER BY id") == ((1,), (1,))
        passed.append("scans_aggregates_joins_setops")

        query(alice, "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        query(alice, "BEGIN")
        assert query(alice, "SELECT id FROM secured") == ((1,),)
        query(admin, "ALTER POLICY tenant ON secured USING(tenant_id=2)")
        assert query(alice, "SELECT id FROM secured") == ((2,),)
        query(alice, "COMMIT")
        query(alice, "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        query(admin, "ALTER POLICY tenant ON secured USING(tenant_id=1)")
        passed.append("new_policy_applies_to_next_statement_inside_rr")

        denied(alice, "INSERT INTO secured VALUES(10,1,'allowed'),(11,2,'denied')")
        denied(alice, "INSERT IGNORE INTO secured VALUES(12,2,'denied')")
        assert query(admin, "SELECT COUNT(*) FROM secured") == ((3,),)
        assert query(alice, "UPDATE secured SET payload='hidden' WHERE id=2") == 0
        assert query(alice, "DELETE FROM secured WHERE id=2") == 0
        denied(alice, "UPDATE secured SET tenant_id=2 WHERE id=1")
        assert query(admin, "SELECT tenant_id,payload FROM secured WHERE id=1") == ((1, "one"),)
        passed.append("write_checks_and_statement_atomicity")

        query(admin, "CREATE TRIGGER capture BEFORE DELETE ON secured FOR EACH ROW SET @leak=OLD.payload")
        denied(alice, "REPLACE INTO secured VALUES(2,1,'proposed')")
        assert query(alice, "SELECT @leak") == ((None,),)
        query(alice, "REPLACE INTO secured VALUES(1,1,'one')")
        assert query(alice, "SELECT @leak") == (("one",),)
        query(admin, "DROP TRIGGER capture")
        passed.append("replace_hides_old_rows_from_before_delete_trigger")

        query(alice, "PREPARE point FROM 'SELECT id FROM secured WHERE id=?'")
        query(alice, "SET @id=2")
        assert query(alice, "EXECUTE point USING @id") == ()
        query(admin, "GRANT SUPER ON *.* TO alice")
        assert query(alice, "EXECUTE point USING @id") == ((2,),)
        query(admin, "REVOKE SELECT ON test.secured FROM alice")
        try:
            query(alice, "SELECT id FROM secured")
        except pymysql.MySQLError as error:
            assert error.args[0] & 0xffff in (1044, 1142, 1143), error
        else:
            raise AssertionError("SUPER must preserve object privilege checks")
        query(admin, "GRANT SELECT ON test.secured TO alice")
        denied(alice, "ALTER TABLE secured DISABLE ROW LEVEL SECURITY")
        denied(alice, "SET RLS CONTEXT 'tenant_id'='2'")
        query(alice, "SET row_security=OFF")
        assert query(alice, "SELECT COUNT(*) FROM secured") == ((3,),)
        query(admin, "REVOKE SUPER ON *.* FROM alice")
        denied(alice, "SELECT id FROM secured")
        query(alice, "SET row_security=ON")
        assert query(alice, "EXECUTE point USING @id") == ()
        query(alice, "DEALLOCATE PREPARE point")
        passed.append("super_revocation_prepared_cache_and_admin_separation")

        query(admin, "CREATE TABLE snapshot_target(id INT PRIMARY KEY,tenant_id INT,payload VARCHAR(40))")
        query(admin, "INSERT INTO snapshot_target VALUES(2,2,'hidden-secret')")
        query(admin, "ALTER TABLE snapshot_target ENABLE ROW LEVEL SECURITY")
        query(admin, "CREATE POLICY tenant ON snapshot_target USING(tenant_id=1)")
        query(admin, "GRANT SELECT ON test.snapshot_target TO alice")
        query(admin, "CREATE FUNCTION snapshot_read(lock_result INT) RETURNS VARCHAR(40) "
                     "READS SQL DATA SQL SECURITY INVOKER RETURN "
                     "(SELECT payload FROM snapshot_target WHERE id=2)")
        query(admin, "GRANT EXECUTE ON FUNCTION test.snapshot_read TO alice")
        for grant_super, expected in [(True, None), (False, "hidden-secret")]:
            gate = f"rls_snapshot_gate_{int(grant_super)}"
            acquisition = query(admin, f"SELECT GET_LOCK('{gate}',0)")
            assert int(acquisition[0][0]) == 1, (grant_super, acquisition)
            outcome = []
            def read_during_change():
                try:
                    outcome.append(query(alice,
                        f"SELECT snapshot_read(GET_LOCK('{gate}',20)) FROM secured WHERE id=1"))
                except Exception as error:
                    outcome.append(error)
            worker = threading.Thread(target=read_during_change)
            worker.start()
            try:
                deadline = time.monotonic() + 15
                while not any("lock" in str(row).lower() and "snapshot_read(GET_LOCK" in str(row)
                              for row in query(admin, "SHOW PROCESSLIST")):
                    assert worker.is_alive(), outcome
                    assert time.monotonic() < deadline, "Snapshot synchronization timed out"
                    time.sleep(.01)
                query(admin, "GRANT SUPER ON *.* TO alice" if grant_super else
                             "REVOKE SUPER ON *.* FROM alice")
            finally:
                query(admin, f"SELECT RELEASE_LOCK('{gate}')")
                worker.join(25)
            assert outcome == [((expected,),)], outcome
            assert query(alice, "SELECT snapshot_read(1) FROM secured WHERE id=1") == (
                (("hidden-secret",),) if grant_super else ((None,),))
            query(alice, "SELECT RELEASE_ALL_LOCKS()")
        passed.append("inflight_nested_function_retains_authorization_snapshot")

        def synchronized_read(function, change, expected, following):
            gate = "rls_policy_identity_gate"
            assert int(query(admin, f"SELECT GET_LOCK('{gate}',0)")[0][0]) == 1
            outcome = []
            def read():
                try:
                    outcome.append(query(alice,
                        f"SELECT {function}(GET_LOCK('{gate}',20)) FROM secured WHERE id=1"))
                except Exception as error:
                    outcome.append(error)
            worker = threading.Thread(target=read)
            worker.start()
            try:
                deadline = time.monotonic() + 15
                while not any("lock" in str(row).lower() and f"{function}(GET_LOCK" in str(row)
                              for row in query(admin, "SHOW PROCESSLIST")):
                    assert worker.is_alive(), outcome
                    assert time.monotonic() < deadline, "Policy synchronization timed out"
                    time.sleep(.01)
                query(admin, change)
            finally:
                query(admin, f"SELECT RELEASE_LOCK('{gate}')")
                worker.join(25)
            assert not worker.is_alive(), "Synchronized query did not finish"
            assert outcome == [((expected,),)], outcome
            assert query(alice, f"SELECT {function}(1) FROM secured WHERE id=1") == ((following,),)
            query(alice, "SELECT RELEASE_ALL_LOCKS()")

        synchronized_read("snapshot_read", "ALTER POLICY tenant ON snapshot_target USING(tenant_id=2)",
                          None, "hidden-secret")
        synchronized_read("snapshot_read", "ALTER POLICY tenant ON snapshot_target USING(tenant_id=1)",
                          "hidden-secret", None)
        synchronized_read("snapshot_read", "ALTER TABLE snapshot_target DISABLE ROW LEVEL SECURITY",
                          None, "hidden-secret")
        synchronized_read("snapshot_read", "ALTER TABLE snapshot_target ENABLE ROW LEVEL SECURITY",
                          "hidden-secret", None)
        synchronized_read("snapshot_read", "DROP POLICY tenant ON snapshot_target", None, None)
        query(admin, "CREATE POLICY tenant ON snapshot_target USING(tenant_id=1)")
        passed.append("inflight_late_binding_retains_policy_versions_across_alter_disable_drop")

        for sql in [
            "CREATE USER snapshot_definer",
            "CREATE ROLE snapshot_super",
            "GRANT SUPER ON *.* TO snapshot_super",
            "GRANT snapshot_super TO snapshot_definer",
            "SET DEFAULT ROLE NONE TO snapshot_definer",
            "GRANT SELECT ON test.snapshot_target TO snapshot_definer",
            "CREATE DEFINER='snapshot_definer'@'%' FUNCTION snapshot_definer_read(x INT) "
            "RETURNS VARCHAR(40) READS SQL DATA SQL SECURITY DEFINER RETURN "
            "(SELECT payload FROM snapshot_target WHERE id=2)",
            "GRANT EXECUTE ON FUNCTION test.snapshot_definer_read TO alice,snapshot_definer",
        ]:
            query(admin, sql)
        synchronized_read("snapshot_definer_read", "SET DEFAULT ROLE snapshot_super TO snapshot_definer",
                          None, "hidden-secret")
        synchronized_read("snapshot_definer_read", "SET DEFAULT ROLE NONE TO snapshot_definer",
                          "hidden-secret", None)
        passed.append("inflight_definer_default_role_activation_is_frozen")

        query(admin, "CREATE TABLE snapshot_writes(id INT PRIMARY KEY,tenant_id INT,v INT)")
        query(admin, "INSERT INTO snapshot_writes VALUES(1,1,0),(2,2,0)")
        query(admin, "ALTER TABLE snapshot_writes ENABLE ROW LEVEL SECURITY")
        query(admin, "CREATE POLICY reads ON snapshot_writes FOR SELECT USING(tenant_id=1)")
        query(admin, "CREATE POLICY writes ON snapshot_writes FOR UPDATE USING(TRUE) WITH CHECK(TRUE)")
        query(admin, "GRANT SELECT,UPDATE ON test.snapshot_writes TO alice")
        locker = pymysql.connect(host="127.0.0.1", port=port, user="root", password="root",
                                 database="test", autocommit=True)
        clients.append(locker)
        for autocommit in [True, False]:
            for grant_super in [False, True]:
                query(admin, "UPDATE snapshot_writes SET v=0")
                if not grant_super:
                    query(admin, "GRANT SUPER ON *.* TO alice")
                packet = pymysql.connect(host="127.0.0.1", port=port, user="alice", password="secret",
                    database="test", autocommit=autocommit,
                    client_flag=pymysql.constants.CLIENT.MULTI_STATEMENTS, read_timeout=30)
                clients.append(packet)
                query(locker, "BEGIN")
                query(locker, "UPDATE snapshot_writes SET v=1 WHERE id=1")
                outcome = []
                def write_packet():
                    try:
                        with packet.cursor() as cursor:
                            cursor.execute("UPDATE snapshot_writes SET v=7 WHERE id=1; "
                                           "UPDATE snapshot_writes SET v=7 WHERE id=2")
                            affected = [cursor.rowcount]
                            while cursor.nextset():
                                affected.append(cursor.rowcount)
                        outcome.append(affected)
                    except Exception as error:
                        outcome.append(error)
                worker = threading.Thread(target=write_packet)
                worker.start()
                try:
                    deadline = time.monotonic() + 15
                    while not query(admin, "SELECT * FROM performance_schema.data_lock_waits"):
                        assert worker.is_alive(), outcome
                        assert time.monotonic() < deadline, "DML lock synchronization timed out"
                        time.sleep(.01)
                    query(admin, "GRANT SUPER ON *.* TO alice" if grant_super else
                                 "REVOKE SUPER ON *.* FROM alice")
                finally:
                    query(locker, "COMMIT")
                    worker.join(25)
                assert not worker.is_alive(), "DML packet did not finish"
                assert outcome == [[1, 1 if grant_super else 0]], (autocommit, grant_super, outcome)
                if not autocommit:
                    packet.commit()
                assert query(admin, "SELECT id,v FROM snapshot_writes ORDER BY id") == (
                    (1, 7), (2, 7 if grant_super else 0))
                packet.close()
                clients.remove(packet)
                if grant_super:
                    query(admin, "REVOKE SUPER ON *.* FROM alice")
        passed.append("cached_dml_packets_rebind_each_child_after_super_changes_during_lock_waits")

        query(admin, "GRANT ALL PRIVILEGES ON test.* TO alice")
        for number, (protect_left, protect_right) in enumerate([(True, False), (False, True), (True, True)]):
            part, stage = f"exchange_part{number}", f"exchange_stage{number}"
            for sql in [
                f"CREATE TABLE {part}(id INT PRIMARY KEY,tenant_id INT) PARTITION BY RANGE(id)"
                    "(PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)",
                f"CREATE TABLE {stage}(id INT PRIMARY KEY,tenant_id INT)",
                f"INSERT INTO {part} VALUES(3,1)",
                f"INSERT INTO {stage} VALUES(1,1),(2,2)",
            ]:
                query(admin, sql)
            for name, enabled in [(part, protect_left), (stage, protect_right)]:
                if enabled:
                    query(admin, f"ALTER TABLE {name} ENABLE ROW LEVEL SECURITY")
                    query(admin, f"CREATE POLICY tenant ON {name} USING(tenant_id=1)")
            exchange = f"ALTER TABLE {part} EXCHANGE PARTITION p0 WITH TABLE {stage}"
            denied(alice, exchange)
            assert query(admin, f"SELECT id FROM {part}") == ((3,),)
            assert query(admin, f"SELECT id FROM {stage} ORDER BY id") == ((1,), (2,))
            query(admin, "GRANT BYPASS_RLS ON *.* TO alice")
            query(alice, exchange)
            assert query(alice, f"SELECT id FROM {part} ORDER BY id") == ((1,), (2,))
            assert query(alice, f"SELECT id FROM {stage}") == ((3,),)
            query(admin, "REVOKE BYPASS_RLS ON *.* FROM alice")
            denied(alice, exchange)
            query(admin, "GRANT SUPER ON *.* TO alice")
            query(alice, exchange)
            assert query(alice, f"SELECT id FROM {part}") == ((3,),)
            assert query(alice, f"SELECT id FROM {stage} ORDER BY id") == ((1,), (2,))
            query(admin, "REVOKE SUPER ON *.* FROM alice")
            assert query(alice, f"SELECT id FROM {stage} ORDER BY id") == (
                ((1,),) if protect_right else ((1,), (2,)))
        passed.append("exchange_checks_both_participants_bypass_super_and_revocation")

        query(admin, "ALTER POLICY tenant ON secured USING(tenant_id=CAST(RLS_CONTEXT('tenant_id') AS UNSIGNED))")
        query(admin, "GRANT RLS_CONTEXT_ADMIN ON *.* TO alice")
        assert query(alice, "SELECT id FROM secured") == ()
        query(alice, "SET RLS CONTEXT 'tenant_id'='1'")
        assert query(alice, "SELECT id FROM secured") == ((1,),)
        query(alice, "BEGIN")
        query(alice, "SAVEPOINT context_start")
        query(alice, "SET LOCAL RLS CONTEXT 'tenant_id'='2'")
        assert query(alice, "SELECT id FROM secured") == ((2,),)
        query(alice, "ROLLBACK TO SAVEPOINT context_start")
        assert query(alice, "SELECT id FROM secured") == ((1,),)
        query(alice, "SET LOCAL RLS CONTEXT 'tenant_id'='2'")
        query(alice, "COMMIT")
        assert query(alice, "SELECT id FROM secured") == ((1,),)
        query(alice, "RESET RLS CONTEXT")
        assert query(alice, "SELECT id FROM secured") == ()
        passed.append("context_privilege_local_savepoint_and_reset")

        # A multi-statement packet still creates a fresh security snapshot at
        # every SQL statement boundary, including context and role changes.
        query(admin, "CREATE ROLE pool_role")
        query(admin, "GRANT SUPER ON *.* TO pool_role")
        query(admin, "GRANT pool_role TO alice")
        multi = pymysql.connect(host="127.0.0.1", port=port, user="alice", password="secret",
            database="test", autocommit=True, client_flag=pymysql.constants.CLIENT.MULTI_STATEMENTS)
        clients.append(multi)
        with multi.cursor() as cursor:
            cursor.execute("SET RLS CONTEXT 'tenant_id'='1'; SELECT id FROM secured; "
                           "SET RLS CONTEXT 'tenant_id'='2'; SELECT id FROM secured; "
                           "SET ROLE pool_role; SELECT COUNT(*) FROM secured; "
                           "SET ROLE NONE; SELECT COUNT(*) FROM secured")
            results = []
            while True:
                if cursor.description:
                    results.append(cursor.fetchall())
                if not cursor.nextset():
                    break
        assert results == [((1,),), ((2,),), ((3,),), ((1,),)], results
        passed.append("multi_statement_context_and_role_snapshot_boundaries")

        binary_client = mysql.connector.connect(host="127.0.0.1", port=port,
            user="alice", password="secret", database="test", autocommit=True,
            ssl_disabled=True, use_pure=True)
        clients.append(binary_client)
        prepared = binary_client.cursor(prepared=True)
        prepared.execute("SELECT id FROM secured WHERE id=%s", (2,))
        assert prepared.fetchall() == []
        query(admin, "GRANT SUPER ON *.* TO alice")
        prepared.execute("SELECT id FROM secured WHERE id=%s", (2,))
        assert prepared.fetchall() == [(2,)]
        query(admin, "REVOKE SUPER ON *.* FROM alice")
        prepared.execute("SELECT id FROM secured WHERE id=%s", (2,))
        assert prepared.fetchall() == []
        with binary_client.cursor() as setup:
            setup.execute("SET RLS CONTEXT 'tenant_id'='2'")
        prepared.execute("SELECT id FROM secured WHERE id=%s", (2,))
        assert prepared.fetchall() == [(2,)]
        prepared.close()
        binary_client.reset_session()
        with binary_client.cursor() as check:
            check.execute("SELECT id FROM secured")
            assert check.fetchall() == []
        passed.append("binary_prepared_super_revocation_context_and_connection_reset")

        query(admin, "ALTER POLICY tenant ON secured USING(tenant_id=1)")
        for connection in clients:
            connection.close()
        clients.clear()
        process.terminate()
        process.wait(timeout=30)
        _, admin = servers.start("database", port, server_options)
        alice = connect(port, "alice")
        clients.append(alice)
        assert query(alice, "SELECT id FROM secured") == ((1,),)
        assert query(alice, "SELECT id FROM exchange_part2") == ((3,),)
        assert query(alice, "SELECT id FROM exchange_stage2 ORDER BY id") == ((1,),)
        assert query(admin, "SELECT id FROM exchange_stage2 ORDER BY id") == ((1,), (2,))
        denied(alice, "INSERT INTO secured VALUES(20,2,'denied')")
        passed.append("restart_preserves_policy_enforcement")
        return passed
    finally:
        for connection in clients:
            connection.close()
        servers.close()
        (root / "result.json").write_text(json.dumps({"passed": passed}, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--binlog", action="store_true")
    args = parser.parse_args()
    base = Path("/tmp/mydb/rls/protocol")
    base.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="run-", dir=base))
    passed = run(args.binary.resolve(), root, args.binlog)
    print(json.dumps({"passed": passed, "artifacts": str(root)}, indent=2))


if __name__ == "__main__":
    main()
