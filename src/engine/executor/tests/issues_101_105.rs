//! General regressions for joined DML and string result contracts.
use super::*;

fn rows(e: &StorageQueryEngine, s: &mut SessionContext, sql: &str) -> Vec<Vec<String>> {
    let response = e
        .execute_with_session(s, sql)
        .unwrap_or_else(|err| panic!("{sql}: {err:?}"))
        .materialize()
        .unwrap();
    let QueryResponse::ResultSet(rs) = response else {
        panic!("expected rows: {sql}");
    };
    rs.rows
        .into_iter()
        .map(|r| r.into_iter().map(|v| v.to_display_string()).collect())
        .collect()
}
fn modes(check: impl Fn(&StorageQueryEngine, &mut SessionContext)) {
    for mode in [
        vec::config::VecMode::Off,
        vec::config::VecMode::On,
        vec::config::VecMode::Auto,
    ] {
        let _mode = vec::config::enter_mode(mode);
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let e = open_test_engine(dir.path());
        let mut s = SessionContext::default();
        for sql in [
            "SET sql_mode=''",
            "SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci",
        ] {
            e.execute_with_session(&mut s, sql).unwrap();
        }
        check(&e, &mut s);
    }
}
fn exec(e: &StorageQueryEngine, s: &mut SessionContext, sql: &str) {
    e.execute_with_session(s, sql)
        .unwrap_or_else(|err| panic!("{sql}: {err:?}"));
}

#[test]
fn issue101_right_chains_keep_outer_rows_targets_and_rollback() {
    modes(|e, s| {
        for sql in [
            "CREATE TABLE a(id INT PRIMARY KEY,v INT)",
            "CREATE TABLE b(id INT PRIMARY KEY,v INT)",
            "CREATE TABLE c(id INT PRIMARY KEY,v INT)",
            "INSERT INTO a VALUES(1,10),(2,20)",
            "INSERT INTO b VALUES(2,200),(3,300)",
            "INSERT INTO c VALUES(2,2000),(3,3000),(4,4000)",
        ] {
            exec(e, s, sql);
        }
        exec(e, s, "START TRANSACTION");
        exec(e,s,"UPDATE a RIGHT JOIN b ON a.id=b.id RIGHT JOIN c ON b.id=c.id SET a.v=a.v+1,b.v=b.v+1,c.v=c.v+1");
        assert_eq!(
            rows(e, s, "SELECT * FROM a ORDER BY id"),
            vec![vec!["1", "10"], vec!["2", "21"]]
        );
        assert_eq!(
            rows(e, s, "SELECT * FROM b ORDER BY id"),
            vec![vec!["2", "201"], vec!["3", "301"]]
        );
        assert_eq!(
            rows(e, s, "SELECT * FROM c ORDER BY id"),
            vec![vec!["2", "2001"], vec!["3", "3001"], vec!["4", "4001"]]
        );
        exec(e, s, "ROLLBACK");
        assert_eq!(rows(e, s, "SELECT v FROM a WHERE id=2"), vec![vec!["20"]]);
        exec(e, s, "START TRANSACTION");
        exec(
            e,
            s,
            "DELETE a,b,c FROM a LEFT JOIN b ON a.id=b.id RIGHT JOIN c ON b.id=c.id",
        );
        assert_eq!(rows(e, s, "SELECT id FROM a ORDER BY id"), vec![vec!["1"]]);
        assert_eq!(rows(e, s, "SELECT id FROM b ORDER BY id"), vec![vec!["3"]]);
        assert!(rows(e, s, "SELECT id FROM c").is_empty());
        exec(e, s, "ROLLBACK");
        exec(e, s, "START TRANSACTION");
        exec(
            e,
            s,
            "UPDATE a RIGHT JOIN b USING(id) RIGHT JOIN c USING(id) SET c.v=id WHERE id=4",
        );
        assert_eq!(rows(e, s, "SELECT v FROM c WHERE id=4"), vec![vec!["4"]]);
        exec(
            e,
            s,
            "DELETE c FROM a RIGHT JOIN b USING(id) RIGHT JOIN c USING(id) WHERE id=3",
        );
        assert_eq!(
            rows(e, s, "SELECT id FROM c ORDER BY id"),
            vec![vec!["2"], vec!["4"]]
        );
        exec(e, s, "ROLLBACK");
        // References to future participants remain invalid at the original ON boundary.
        let err = e
            .execute_with_session(
                s,
                "UPDATE a RIGHT JOIN b ON c.id=b.id RIGHT JOIN c ON a.id=c.id SET a.v=0",
            )
            .unwrap_err();
        assert_eq!(err.code(), 1054);
        exec(e, s, "DELETE FROM a");
        exec(e, s, "DELETE FROM b");
        exec(
            e,
            s,
            "UPDATE a RIGHT JOIN b ON a.id=b.id RIGHT JOIN c ON b.id=c.id SET c.v=c.v+1",
        );
        assert_eq!(
            rows(e, s, "SELECT v FROM c ORDER BY id"),
            vec![vec!["2001"], vec!["3001"], vec!["4001"]]
        );
    });
}

#[test]
fn issue102_derived_dml_sources_aliases_aggregation_and_readonly_targets() {
    modes(|e, s| {
        for sql in [
            "CREATE TABLE dst(id INT PRIMARY KEY,v INT)",
            "CREATE TABLE src(id INT,v INT)",
            "INSERT INTO dst VALUES(1,10),(2,20)",
            "INSERT INTO src VALUES(1,3),(1,7),(2,9)",
        ] {
            exec(e, s, sql);
        }
        exec(e, s, "START TRANSACTION");
        exec(e,s,"UPDATE dst a JOIN (SELECT id AS sid,MAX(v) AS sv FROM src GROUP BY id) d ON a.id=d.sid SET a.v=d.sv");
        assert_eq!(
            rows(e, s, "SELECT * FROM dst ORDER BY id"),
            vec![vec!["1", "7"], vec!["2", "9"]]
        );
        exec(e, s, "ROLLBACK");
        exec(e,s,"WITH c(x,y) AS (SELECT id,v FROM src) UPDATE dst a JOIN (SELECT x AS sid,MAX(y) AS sv FROM c GROUP BY x) d ON a.id=d.sid SET a.v=d.sv");
        assert_eq!(
            rows(e, s, "SELECT * FROM dst ORDER BY id"),
            vec![vec!["1", "7"], vec!["2", "9"]]
        );
        exec(
            e,
            s,
            "UPDATE dst a JOIN (SELECT id,v FROM src WHERE id=999) d ON a.id=d.id SET a.v=0",
        );
        exec(
            e,
            s,
            "DELETE a FROM dst a JOIN (SELECT id AS sid FROM src WHERE id=1) d ON a.id=d.sid",
        );
        assert_eq!(rows(e, s, "SELECT * FROM dst"), vec![vec!["2", "9"]]);
        let err = e
            .execute_with_session(
                s,
                "UPDATE dst a JOIN (SELECT id,v FROM src) d ON a.id=d.id SET d.v=0",
            )
            .unwrap_err();
        assert_eq!(err.code(), 1288);
        let err = e
            .execute_with_session(
                s,
                "DELETE d FROM dst a JOIN (SELECT id,v FROM src) d ON a.id=d.id",
            )
            .unwrap_err();
        assert_eq!(err.code(), 1288);
    });
}

#[test]
fn issue103_base64_is_connection_text_for_binary_inputs_and_ctas() {
    modes(|e, s| {
        for (names, charset, collation) in [
            (
                "utf8mb4 COLLATE utf8mb4_0900_ai_ci",
                "utf8mb4",
                "utf8mb4_0900_ai_ci",
            ),
            (
                "latin1 COLLATE latin1_swedish_ci",
                "latin1",
                "latin1_swedish_ci",
            ),
        ] {
            exec(e, s, &format!("SET NAMES {names}"));
            for input in [
                "_binary'ABC'",
                "CONVERT(0x00FF USING binary)",
                "UNHEX('00FF')",
                "CAST('ABC' AS BINARY)",
                "NULL",
                "_binary''",
            ] {
                assert_eq!(
                    rows(
                        e,
                        s,
                        &format!(
                            "SELECT CHARSET(TO_BASE64({input})),COLLATION(TO_BASE64({input}))"
                        )
                    ),
                    vec![vec![charset, collation]]
                );
            }
        }
        exec(e, s, "SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci");
        exec(
            e,
            s,
            "CREATE TABLE encoded AS SELECT TO_BASE64(_binary'ABC') AS v",
        );
        assert_eq!(rows(e,s,"SELECT DATA_TYPE,CHARACTER_SET_NAME FROM information_schema.columns WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='encoded' AND COLUMN_NAME='v'"),vec![vec!["varchar","utf8mb4"]]);
        exec(e, s, "INSERT INTO encoded VALUES('text')");
        assert_eq!(
            rows(e, s, "SELECT v FROM encoded ORDER BY v"),
            vec![vec!["QUJD"], vec!["text"]]
        );
    });
}

#[test]
fn issue104_soundex_binary_bytes_do_not_become_unicode_replacements() {
    modes(|e, s| {
        for (bytes, expected) in [
            ("00FF", ""),
            ("FF0041", "41303030"),
            ("C3A9", ""),
            ("F09F9880", ""),
            ("414243", "41313230"),
        ] {
            let input = format!("CONVERT(0x{bytes} USING binary)");
            assert_eq!(
                rows(
                    e,
                    s,
                    &format!("SELECT HEX(SOUNDEX({input})),CHARSET(SOUNDEX({input}))")
                ),
                vec![vec![expected, "binary"]]
            );
        }
        assert_eq!(
            rows(e, s, "SELECT SOUNDEX('é'),SOUNDEX('ABC')"),
            vec![vec!["é000", "A120"]]
        );
        exec(e, s, "CREATE TABLE sounds(id INT,b VARBINARY(20))");
        exec(
            e,
            s,
            "INSERT INTO sounds VALUES(1,0x00FF),(2,0x414243),(3,NULL)",
        );
        assert_eq!(
            rows(e, s, "SELECT HEX(SOUNDEX(b)) FROM sounds ORDER BY id"),
            vec![vec![""], vec!["41313230"], vec!["NULL"]]
        );
    });
}

#[test]
fn issue105_regexp_validates_patterns_before_null_subjects() {
    modes(|e, s| {
        exec(e, s, "CREATE TABLE subjects(id INT,t VARCHAR(20))");
        exec(e, s, "INSERT INTO subjects VALUES(1,NULL),(2,'abc')");
        for f in [
            "REGEXP_LIKE",
            "REGEXP_INSTR",
            "REGEXP_SUBSTR",
            "REGEXP_REPLACE",
        ] {
            let suffix = if f == "REGEXP_REPLACE" { ",'#'" } else { "" };
            for subject in ["NULL", "t"] {
                let sql = format!("SELECT {f}({subject},'['{suffix}) FROM subjects WHERE id=1");
                let err = e.execute_with_session(s, &sql).unwrap_err();
                assert_eq!(err.code(), 3696, "{sql}");
            }
            assert_eq!(
                rows(e, s, &format!("SELECT {f}(NULL,'a'{suffix})")),
                vec![vec!["NULL"]]
            );
            assert!(rows(
                e,
                s,
                &format!("SELECT {f}(t,'['{suffix}) FROM subjects WHERE id=999")
            )
            .is_empty());
            let text = format!("SELECT {f}(?,'['{suffix})").replace('\'', "''");
            exec(e, s, &format!("PREPARE q FROM '{text}'"));
            exec(e, s, "SET @p=NULL");
            assert_eq!(
                e.execute_with_session(s, "EXECUTE q USING @p")
                    .unwrap_err()
                    .code(),
                3696
            );
            exec(e, s, "DEALLOCATE PREPARE q");
        }
        assert_eq!(rows(e,s,"SELECT CASE WHEN 0 THEN REGEXP_LIKE(NULL,'[') ELSE 1 END,REGEXP_LIKE(NULL,'[',NULL)"),vec![vec!["1","NULL"]]);
    });
}

#[test]
fn issue106_query_expansion_remains_an_explicit_unsupported_feature() {
    modes(|e, s| {
        for suffix in [
            "WITH QUERY EXPANSION",
            "IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION",
        ] {
            let err = e
                .execute_with_session(
                    s,
                    &format!("SELECT MATCH(txt) AGAINST('alpha' {suffix}) FROM missing"),
                )
                .unwrap_err();
            assert_eq!(err.code(), 1064);
            assert!(err.message().contains("QUERY EXPANSION"));
        }
    });
}

#[test]
fn issue102_derived_sources_enforce_rls_privileges_spill_and_snapshot() {
    // nextest isolates the environment; restore it for an ordinary test run.
    struct Restore(Option<std::ffi::OsString>);
    impl Drop for Restore {
        fn drop(&mut self) {
            match &self.0 {
                Some(v) => std::env::set_var("MYDB_CTE_SPILL_THRESHOLD_MB", v),
                None => std::env::remove_var("MYDB_CTE_SPILL_THRESHOLD_MB"),
            }
        }
    }
    let _restore = Restore(std::env::var_os("MYDB_CTE_SPILL_THRESHOLD_MB"));
    std::env::set_var("MYDB_CTE_SPILL_THRESHOLD_MB", "0");
    modes(|e, root| {
        for sql in [
            "CREATE TABLE dst(id INT PRIMARY KEY,v INT)",
            "CREATE TABLE src(id INT PRIMARY KEY,tenant INT,v INT)",
            "INSERT INTO dst VALUES(1,10),(2,20)",
            "INSERT INTO src VALUES(1,1,100),(2,2,200)",
            "CREATE USER derived_reader",
            "GRANT SELECT,UPDATE ON test.dst TO derived_reader",
            "GRANT SELECT ON test.src TO derived_reader",
            "ALTER TABLE src ENABLE ROW LEVEL SECURITY",
            "CREATE POLICY tenant_scope ON src USING(tenant=1)",
        ] {
            exec(e, root, sql);
        }
        let mut reader = SessionContext {
            user: "derived_reader".into(),
            login_user: "derived_reader".into(),
            authenticated_as: crate::engine::auth::AccountKey::new("derived_reader", "%"),
            connection_id: 8102,
            ..SessionContext::default()
        };
        let spills = super::super::spill_quota::cte_spill_count();
        exec(
            e,
            &mut reader,
            "UPDATE dst a JOIN (SELECT id,v FROM src) d ON a.id=d.id SET a.v=d.v",
        );
        assert_eq!(
            rows(e, root, "SELECT * FROM dst ORDER BY id"),
            vec![vec!["1", "100"], vec!["2", "20"]]
        );
        assert!(super::super::spill_quota::cte_spill_count() > spills);
        exec(e, root, "REVOKE SELECT ON test.src FROM derived_reader");
        assert_eq!(
            e.execute_with_session(
                &mut reader,
                "UPDATE dst a JOIN (SELECT id,v FROM src) d ON a.id=d.id SET a.v=d.v"
            )
            .unwrap_err()
            .code(),
            1142
        );
        exec(
            e,
            root,
            "UPDATE dst a JOIN (SELECT id,v+1 AS nv FROM dst) d ON a.id=d.id SET a.v=d.nv",
        );
        assert_eq!(
            rows(e, root, "SELECT * FROM dst ORDER BY id"),
            vec![vec!["1", "101"], vec!["2", "21"]]
        );
    });
}

#[test]
fn issue105_binary_null_validation_retains_pattern_resource_limit() {
    let values = [Value::Null, Value::Bytes(vec![b'a'; 8193])];
    let err = crate::engine::expr::eval_regexp_like_values(&values).unwrap_err();
    assert!(err.is(crate::error::catalog::ER_REGEXP_INTERNAL_ERROR));
}
