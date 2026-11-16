#!/usr/bin/env python3
"""Exercise specialized RLS access paths and metadata semantics on local servers."""
import argparse
import json
from pathlib import Path
import tempfile

import pymysql
from test_native_readiness import Servers, free_port, query
from test_row_security import connect, denied


def run(binary, root):
    servers = Servers(binary, root)
    evidence = []
    app = None
    try:
        port = free_port()
        _, admin = servers.start("database", port)
        query(admin, "CREATE USER alice IDENTIFIED BY 'secret'")
        query(admin, "GRANT SELECT,INSERT,UPDATE,DELETE,CREATE ON test.* TO alice")
        app = pymysql.connect(host="127.0.0.1", port=port, user="alice", password="secret",
            database="test", autocommit=True, local_infile=True, read_timeout=60)

        def check(name, work):
            try:
                work()
                evidence.append({"case": name, "passed": True})
            except Exception as error:
                evidence.append({"case": name, "passed": False, "error": str(error)})
            (root / "result.json").write_text(json.dumps(evidence, indent=2, default=str) + "\n")

        def protect(table):
            query(admin, f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
            query(admin, f"CREATE POLICY tenant ON {table} USING(tenant=1)")

        def vector(algorithm):
            table = "vectors_" + algorithm.lower()
            query(admin, f"CREATE TABLE {table}(id INT PRIMARY KEY,tenant INT,v VECTOR(3) NOT NULL)")
            with admin.cursor() as cursor:
                cursor.executemany(f"INSERT INTO {table} VALUES(%s,%s,%s)",
                    [(i, 2 if i < 101 else 1, f"[{i},0,0]") for i in range(1, 201)])
            options = "WITH(metric=l2,nlist=4,nprobe=4)" if algorithm == "IVF" else "WITH(metric=l2)"
            query(admin, f"CREATE INDEX vx ON {table}(v) USING {algorithm} {options}")
            protect(table)
            expected = tuple((i,) for i in range(101, 106))
            for hint in ["", "/*+ NO_VECTOR_INDEX */", "/*+ PARALLEL(4) */"]:
                sql = f"SELECT {hint} id FROM {table} ORDER BY L2_DISTANCE(v,'[0,0,0]'),id LIMIT 5"
                assert query(app, sql) == expected, sql
            query(admin, "GRANT SUPER ON *.* TO alice")
            assert query(app, f"SELECT id FROM {table} ORDER BY L2_DISTANCE(v,'[0,0,0]'),id LIMIT 5") == tuple((i,) for i in range(1, 6))
            query(admin, "REVOKE SUPER ON *.* FROM alice")
            assert query(app, f"SELECT id FROM {table} ORDER BY L2_DISTANCE(v,'[0,0,0]'),id LIMIT 5") == expected
        check("hnsw_filter_before_topk_and_super_revoke", lambda: vector("HNSW"))
        check("ivf_filter_before_topk_and_super_revoke", lambda: vector("IVF"))

        def fulltext():
            query(admin, "CREATE TABLE documents(id INT PRIMARY KEY,tenant INT,body TEXT,FULLTEXT KEY ft(body))")
            query(admin, "INSERT INTO documents VALUES(1,2,'database database database'),(2,1,'database search'),(3,1,'database engine')")
            protect("documents")
            sql = "SELECT id FROM documents WHERE MATCH(body) AGAINST('database') ORDER BY MATCH(body) AGAINST('database') DESC,id LIMIT 2"
            expected = query(admin, sql.replace("WHERE MATCH", "WHERE tenant=1 AND MATCH"))
            assert len(expected) == 2
            assert query(app, sql) == expected
        check("fulltext_filter_before_ranking_limit", fulltext)

        def spatial():
            query(admin, "CREATE TABLE places(id INT PRIMARY KEY,tenant INT,g POINT NOT NULL SRID 0,SPATIAL KEY sp(g))")
            query(admin, "INSERT INTO places VALUES(1,2,ST_GeomFromText('POINT(1 1)')),(2,1,ST_GeomFromText('POINT(2 2)'))")
            protect("places")
            sql = "SELECT id FROM places FORCE INDEX(sp) WHERE MBRContains(ST_GeomFromText('POLYGON((0 0,3 0,3 3,0 3,0 0))'),g) ORDER BY id LIMIT 1"
            assert query(app, sql) == ((2,),)
        check("spatial_index_filter_before_limit", spatial)

        query(admin, "CREATE TABLE source_rows(id INT PRIMARY KEY,tenant INT,v INT)")
        query(admin, "INSERT INTO source_rows VALUES(1,1,10),(2,2,20)")
        protect("source_rows")

        def ctas():
            query(app, "CREATE TABLE ctas_rows AS SELECT * FROM source_rows")
            assert query(admin, "SELECT id FROM ctas_rows") == ((1,),)
        check("ctas_source_is_filtered", ctas)

        def like():
            query(admin, "CREATE TABLE like_rows LIKE source_rows")
            query(admin, "INSERT INTO like_rows SELECT * FROM source_rows")
            assert query(app, "SELECT id FROM like_rows") == ((1,),)
        check("like_copies_and_rebinds_security", like)

        def rename():
            query(admin, "CREATE TABLE rename_rows(id INT PRIMARY KEY,tenant INT)")
            query(admin, "INSERT INTO rename_rows VALUES(1,1),(2,2)")
            protect("rename_rows")
            query(admin, "ALTER TABLE rename_rows CHANGE tenant renamed_tenant INT")
            assert query(app, "SELECT id FROM rename_rows") == ((1,),)
        check("column_rename_preserves_stable_policy_binding", rename)

        def drop_column():
            query(admin, "CREATE TABLE drop_rows(id INT PRIMARY KEY,tenant INT)")
            protect("drop_rows")
            try:
                query(admin, "ALTER TABLE drop_rows DROP COLUMN tenant")
            except pymysql.MySQLError:
                return
            raise AssertionError("Referenced policy column was dropped")
        check("referenced_column_drop_is_restricted", drop_column)

        def creation_settings():
            query(admin, "CREATE TABLE mode_rows(id INT PRIMARY KEY,tenant INT)")
            query(admin, "INSERT INTO mode_rows VALUES(1,1),(2,2)")
            query(admin, "ALTER TABLE mode_rows ENABLE ROW LEVEL SECURITY")
            query(admin, "SET sql_mode='NO_UNSIGNED_SUBTRACTION'")
            query(admin, "CREATE POLICY mode_policy ON mode_rows USING(tenant=1 AND (CAST(0 AS UNSIGNED)-1)<0)")
            query(admin, "SET sql_mode=''")
            query(app, "SET sql_mode=''")
            assert query(app, "SELECT id FROM mode_rows") == ((1,),)
        check("policy_uses_creation_sql_mode", creation_settings)

        def windows():
            query(admin, "CREATE TABLE window_rows(id INT PRIMARY KEY)")
            try:
                query(admin, "CREATE POLICY p ON window_rows USING(ROW_NUMBER() OVER()=1)")
            except pymysql.MySQLError as error:
                assert error.args[0] & 0xffff == 50131, error
                return
            raise AssertionError("Top-level policy window function was accepted")
        check("top_level_policy_window_is_rejected", windows)

        def local_import():
            query(admin, "CREATE TABLE imported(id INT PRIMARY KEY,tenant INT,v INT)")
            protect("imported")
            data = root / "import.tsv"
            data.write_text("1\t1\t10\n2\t2\t20\n")
            denied(app, f"LOAD DATA LOCAL INFILE '{data}' INTO TABLE imported")
            assert query(admin, "SELECT COUNT(*) FROM imported") == ((0,),)
        check("local_import_rejection_is_statement_atomic", local_import)

        def view_update():
            query(admin, "CREATE SQL SECURITY INVOKER VIEW invoke_rows AS SELECT * FROM source_rows")
            query(admin, "GRANT SELECT,UPDATE ON test.invoke_rows TO alice")
            query(app, "UPDATE invoke_rows SET v=v+1")
            assert query(admin, "SELECT id,v FROM source_rows ORDER BY id") == ((1, 11), (2, 20))
            denied(app, "UPDATE invoke_rows SET tenant=2 WHERE id=1")
        check("invoker_view_update_and_final_check", view_update)

        def multi_source_update():
            query(admin, "CREATE TABLE update_sink(id INT PRIMARY KEY,v INT)")
            query(admin, "INSERT INTO update_sink VALUES(2,0)")
            changed = query(app, "UPDATE update_sink d JOIN source_rows s ON d.id=s.id SET d.v=s.v")
            assert changed == 0, changed
            assert query(app, "SELECT v FROM update_sink") == ((0,),)
        check("multi_update_cannot_copy_hidden_source_values", multi_source_update)

        def multi_source_delete():
            query(admin, "CREATE TABLE delete_sink(id INT PRIMARY KEY)")
            query(admin, "INSERT INTO delete_sink VALUES(2)")
            assert query(app, "DELETE d FROM delete_sink d JOIN source_rows s ON d.id=s.id") == 0
            assert query(app, "SELECT id FROM delete_sink") == ((2,),)
        check("multi_delete_filters_read_only_sources", multi_source_delete)

        def multi_target_commands():
            query(admin, "CREATE TABLE command_target(id INT PRIMARY KEY,tenant INT,v INT)")
            query(admin, "CREATE TABLE command_helper(id INT PRIMARY KEY)")
            query(admin, "INSERT INTO command_target VALUES(1,1,10),(2,2,20)")
            query(admin, "INSERT INTO command_helper VALUES(2)")
            query(admin, "ALTER TABLE command_target ENABLE ROW LEVEL SECURITY")
            query(admin, "CREATE POLICY reads ON command_target FOR SELECT USING(TRUE)")
            query(admin, "CREATE POLICY writes ON command_target FOR UPDATE USING(tenant=1) WITH CHECK(tenant=1)")
            query(app, "SET @write_leak=NULL")
            assert query(app, "UPDATE command_target t JOIN command_helper h ON t.id=h.id SET t.v=(@write_leak:=t.v)") == 0
            assert query(app, "SELECT @write_leak") == ((None,),)
        check("multi_update_filters_target_before_user_side_effects", multi_target_commands)

        def locking_read_boundary():
            query(app, "SET @lock_leak=NULL")
            assert query(app, "SELECT id FROM source_rows WHERE id=2 AND (@lock_leak:=v)>0 FOR UPDATE") == ()
            assert query(app, "SELECT @lock_leak") == ((None,),)
        check("locking_read_filters_before_user_predicates", locking_read_boundary)

        def side_effect_function_boundary():
            query(admin, "CREATE FUNCTION observe_row(value_seen INT) RETURNS INT NO SQL SQL SECURITY INVOKER "
                         "BEGIN IF value_seen=20 THEN SET @hidden_observed=value_seen; "
                         "ELSE SET @visible_observed=value_seen; END IF; RETURN 1; END")
            query(admin, "CREATE FUNCTION reject_hidden(value_seen INT) RETURNS INT NO SQL SQL SECURITY INVOKER "
                         "BEGIN IF value_seen=20 THEN SIGNAL SQLSTATE '45000' "
                         "SET MESSAGE_TEXT='Hidden value reached user function'; END IF; RETURN 1; END")
            query(admin, "GRANT EXECUTE ON FUNCTION test.observe_row TO alice")
            query(admin, "GRANT EXECUTE ON FUNCTION test.reject_hidden TO alice")
            query(admin, "CREATE TABLE function_sink(id INT PRIMARY KEY,v INT)")
            query(admin, "INSERT INTO function_sink VALUES(1,0),(2,0)")
            statements = [
                f"SELECT {{probe}}(v) FROM source_rows WHERE id=2 {lock}"
                for lock in ["FOR UPDATE", "FOR SHARE"]
            ]
            for lock in ["FOR UPDATE", "FOR SHARE", "FOR UPDATE OF s", "FOR SHARE OF s",
                         "FOR UPDATE OF d", "FOR SHARE OF d"]:
                statements.extend([
                    f"SELECT {{probe}}(s.v) FROM source_rows s JOIN function_sink d ON s.id=d.id WHERE s.id=2 {lock}",
                    f"SELECT s.id FROM source_rows s JOIN function_sink d ON {{probe}}(s.v)=1 AND s.id=d.id WHERE s.id=2 {lock}",
                    f"SELECT s.id FROM source_rows s JOIN function_sink d ON s.id=d.id WHERE {{probe}}(s.v)=1 AND s.id=2 {lock}",
                ])
            statements.extend([
                "UPDATE source_rows SET v={probe}(v) WHERE id=2",
                "UPDATE source_rows SET v=v WHERE {probe}(v)=1 AND id=2",
                "DELETE FROM source_rows WHERE {probe}(v)=1 AND id=2",
                "UPDATE function_sink d JOIN source_rows s ON d.id=s.id SET d.v={probe}(s.v) WHERE s.id=2",
                "UPDATE function_sink d JOIN source_rows s ON {probe}(s.v)=1 AND d.id=s.id SET d.v=s.v WHERE s.id=2",
                "UPDATE function_sink d JOIN source_rows s ON d.id=s.id SET d.v=s.v WHERE {probe}(s.v)=1 AND s.id=2",
                "DELETE d FROM function_sink d JOIN source_rows s ON {probe}(s.v)=1 AND d.id=s.id WHERE s.id=2",
                "DELETE d FROM function_sink d JOIN source_rows s ON d.id=s.id WHERE {probe}(s.v)=1 AND s.id=2",
            ])
            for probe in ["observe_row", "reject_hidden"]:
                for statement in statements:
                    sql = statement.format(probe=probe)
                    query(app, "SET @hidden_observed=NULL,@visible_observed=NULL")
                    result = query(app, sql)
                    assert result in ((), 0), (sql, result)
                    observed = query(app, "SELECT @hidden_observed")
                    assert observed == ((None,),), (sql, result, observed)
            # Authorized rows may enter user expressions before another user
            # predicate rejects them. The hidden-only probe must still fire
            # when called directly, so a disabled function cannot pass.
            assert query(app, "SELECT observe_row(v) FROM source_rows WHERE id=1 FOR UPDATE") == ((1,),)
            assert query(app, "SELECT @visible_observed") == ((11,),)
            query(app, "SELECT observe_row(20)")
            assert query(app, "SELECT @hidden_observed") == ((20,),)
            assert query(app, "SELECT reject_hidden(11)") == ((1,),)
            try:
                query(app, "SELECT reject_hidden(20)")
            except pymysql.MySQLError as error:
                assert error.args[0] == 1644, error
            else:
                raise AssertionError("Hidden-value error probe did not fire")
            assert query(admin, "SELECT id,v FROM function_sink ORDER BY id") == ((1, 0), (2, 0))
        check("locking_and_multi_dml_never_pass_hidden_values_to_stored_functions", side_effect_function_boundary)

        def explain_visibility():
            query(admin, "CREATE TABLE plan_rows(id INT PRIMARY KEY,tenant INT)")
            query(admin, "ALTER TABLE plan_rows ENABLE ROW LEVEL SECURITY")
            query(admin, "CREATE POLICY private_rule ON plan_rows USING(tenant=7319927)")
            query(admin, "CREATE SQL SECURITY INVOKER VIEW plan_view AS SELECT * FROM plan_rows")
            query(admin, "GRANT SELECT,SHOW VIEW ON test.plan_view TO alice")
            for table in ["plan_rows", "plan_view"]:
                for format in ["TRADITIONAL", "TREE", "JSON"]:
                    result = query(app, f"EXPLAIN FORMAT={format} SELECT id FROM {table}")
                    assert "7319927" not in str(result), result
                    assert "security" in str(result).lower(), result
            query(admin, "GRANT ROW_SECURITY_ADMIN ON *.* TO alice")
            assert "7319927" in str(query(app, "EXPLAIN FORMAT=TREE SELECT id FROM plan_rows"))
            query(admin, "REVOKE ROW_SECURITY_ADMIN ON *.* FROM alice")
            assert "7319927" not in str(query(app, "EXPLAIN FORMAT=TREE SELECT id FROM plan_rows"))
            assert "ROW_SECURITY_ADMIN" not in str(query(app, "EXPLAIN SELECT id FROM ctas_rows WHERE id=1"))
        check("explain_details_require_separate_admin_privilege", explain_visibility)
    finally:
        if app:
            app.close()
        servers.close()
        (root / "result.json").write_text(json.dumps(evidence, indent=2, default=str) + "\n")
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    args = parser.parse_args()
    base = Path("/tmp/mydb/rls/access-paths")
    base.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="run-", dir=base))
    evidence = run(args.binary.resolve(), root)
    print(json.dumps({"cases": evidence, "artifacts": str(root)}, indent=2))
    if not all(case["passed"] for case in evidence):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
