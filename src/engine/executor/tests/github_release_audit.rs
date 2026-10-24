//! Regressions for the October 12 release audit, including rejected rewrites.
use super::*;

fn setup() -> (tempfile::TempDir, StorageQueryEngine, SessionContext) {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    (dir, engine, SessionContext::default())
}

fn rows(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> Vec<Vec<String>> {
    let response = engine
        .execute_with_session(session, sql)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    let response = response.materialize().unwrap();
    let QueryResponse::ResultSet(result) = response else {
        panic!("expected result set: {sql}")
    };
    result
        .rows
        .into_iter()
        .map(|row| row.into_iter().map(|v| v.to_display_string()).collect())
        .collect()
}

#[test]
fn release_audit_not_in_qualifies_both_scopes() {
    let (_dir, engine, mut session) = setup();
    for sql in [
        "CREATE TABLE a(id INT PRIMARY KEY,k INT NOT NULL)",
        "CREATE TABLE b(id INT PRIMARY KEY,k INT NOT NULL)",
        "INSERT INTO a VALUES(1,1),(2,2),(6,6),(7,7)",
        "INSERT INTO b VALUES(1,1),(2,2)",
    ] {
        engine.execute(sql).unwrap();
    }
    for sql in [
        "SELECT id FROM a WHERE k NOT IN(SELECT k FROM b) ORDER BY id",
        "SELECT id FROM a o WHERE k NOT IN(SELECT k FROM b i) ORDER BY id",
        "SELECT id FROM a WHERE k NOT IN(SELECT k FROM b WHERE k<3) ORDER BY id",
    ] {
        assert_eq!(
            rows(&engine, &mut session, sql),
            vec![vec!["6"], vec!["7"]],
            "{sql}"
        );
        engine.execute(&format!("EXPLAIN {sql}")).unwrap();
    }
    engine.execute("CREATE TABLE nullable(k INT)").unwrap();
    engine
        .execute("INSERT INTO nullable VALUES(1),(NULL)")
        .unwrap();
    assert!(rows(
        &engine,
        &mut session,
        "SELECT id FROM a WHERE k NOT IN(SELECT k FROM nullable)"
    )
    .is_empty());
}

#[test]
fn release_audit_grouped_correlated_scalar_keeps_functional_dependency() {
    let (_dir, engine, mut session) = setup();
    for sql in [
        "CREATE TABLE a(k INT,j INT)",
        "CREATE TABLE b(k INT,j INT,v INT)",
        "INSERT INTO a VALUES(1,8),(1,9),(2,9),(3,10)",
        "INSERT INTO b VALUES(1,8,10),(1,9,20),(2,9,30)",
    ] {
        engine.execute(sql).unwrap();
    }
    assert_eq!(rows(&engine, &mut session,
        "SELECT a.k,(SELECT MAX(v) FROM b WHERE b.k=a.k),COUNT(*) FROM a GROUP BY a.k ORDER BY a.k"),
        vec![vec!["1","20","2"],vec!["2","30","1"],vec!["3","NULL","1"]]);
    assert_eq!(rows(&engine, &mut session,
        "SELECT a.k,COUNT(*) FROM a GROUP BY a.k HAVING (SELECT MAX(v) FROM b WHERE b.k=a.k)>20"),
        vec![vec!["2","1"]]);
    let error = engine.execute_with_session(&mut session,
        "SELECT a.k,(SELECT MAX(v) FROM b WHERE b.k=a.k AND b.j=a.j),COUNT(*) FROM a GROUP BY a.k").unwrap_err();
    assert_eq!(error.code(), 1055);
}

#[test]
fn release_audit_current_diagnostics_survives_consecutive_gets() {
    let (_dir, engine, mut session) = setup();
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1)").unwrap();
    engine.execute("CREATE PROCEDURE diag() BEGIN DECLARE n INT; DECLARE e INT; DECLARE msg TEXT; DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN GET DIAGNOSTICS n=NUMBER; GET DIAGNOSTICS CONDITION 1 e=MYSQL_ERRNO,msg=MESSAGE_TEXT; SET @n=n,@e=e,@msg=msg; END; INSERT INTO t VALUES(1); END").unwrap();
    engine
        .execute_with_session(&mut session, "CALL diag()")
        .unwrap();
    assert_eq!(session.user_variable("n"), Some(&Value::Int(1)));
    assert_eq!(session.user_variable("e"), Some(&Value::Int(1062)));
    assert!(session
        .user_variable("msg")
        .unwrap()
        .to_display_string()
        .contains("Duplicate"));
}

#[test]
fn release_audit_connection_octets_and_binary_storage() {
    let (_dir, engine, mut session) = setup();
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,b VARBINARY(20))")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET NAMES latin1")
        .unwrap();
    assert_eq!(
        rows(
            &engine,
            &mut session,
            "SELECT HEX('ä'),OCTET_LENGTH('ä'),HEX(CONCAT('ä','ö'))"
        ),
        vec![vec!["E4", "1", "E4F6"]]
    );
    for sql in [
        "INSERT INTO t VALUES(1,'ä')",
        "INSERT INTO t VALUES(2,_utf8mb4 X'C3A4')",
        "PREPARE ins FROM 'INSERT INTO t VALUES(?,?)'",
        "SET @id=3,@text='ö'",
        "EXECUTE ins USING @id,@text",
    ] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
    assert_eq!(
        rows(&engine, &mut session, "SELECT id,HEX(b) FROM t ORDER BY id"),
        vec![vec!["1", "E4"], vec!["2", "C3A4"], vec!["3", "F6"]]
    );
    engine
        .execute_with_session(&mut session, "UPDATE t SET b=_utf8mb4 X'C3B6' WHERE id=3")
        .unwrap();
    assert_eq!(
        rows(&engine, &mut session, "SELECT HEX(b) FROM t WHERE id=3"),
        vec![vec!["C3B6"]]
    );
    engine
        .execute("CREATE TABLE source_text(s VARCHAR(10) CHARACTER SET latin1)")
        .unwrap();
    engine
        .execute("INSERT INTO source_text VALUES('ä')")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET NAMES utf8mb4")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t SELECT 4,s FROM source_text")
        .unwrap();
    assert_eq!(
        rows(&engine, &mut session, "SELECT HEX(b) FROM t WHERE id=4"),
        vec![vec!["E4"]]
    );
}

#[test]
fn release_audit_general_ci_keys_match_weight_string() {
    let (_dir, engine, mut session) = setup();
    engine
        .execute("CREATE TABLE t(s VARCHAR(20) COLLATE utf8mb4_general_ci UNIQUE)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES('ß')").unwrap();
    assert_eq!(
        rows(&engine, &mut session, "SELECT COUNT(*) FROM t WHERE s='s'"),
        vec![vec!["1"]]
    );
    assert_eq!(
        engine
            .execute("INSERT INTO t VALUES('s')")
            .unwrap_err()
            .code(),
        1062
    );
    assert_eq!(
        rows(
            &engine,
            &mut session,
            "SELECT _utf8mb4'ß' COLLATE utf8mb4_general_ci = _utf8mb4's'"
        ),
        vec![vec!["1"]]
    );
}

#[test]
fn release_audit_signed_cast_matches_mysql_unsigned_clipping() {
    let (_dir, engine, mut session) = setup();
    assert_eq!(rows(&engine,&mut session,
        "SELECT CAST('18446744073709551615' AS SIGNED),CAST('18446744073709551616' AS SIGNED),CAST('-18446744073709551616' AS SIGNED)"),
        vec![vec!["-1","-1","-9223372036854775808"]]);
}

#[test]
fn release_audit_spatial_components_srids_and_empty_relations() {
    let (_dir, engine, mut session) = setup();
    let polygon = "ST_GeomFromText('POLYGON((4 4,7 4,7 7,4 7,4 4))')";
    for geometry in [
        "MULTIPOINT((0 0),(5 5))",
        "MULTILINESTRING((0 0,1 1),(5 5,6 6))",
        "GEOMETRYCOLLECTION(POINT(0 0),GEOMETRYCOLLECTION(LINESTRING(5 5,6 6)))",
    ] {
        for sql in [
            format!("SELECT ST_Distance(ST_GeomFromText('{geometry}'),{polygon})"),
            format!("SELECT ST_Distance({polygon},ST_GeomFromText('{geometry}'))"),
        ] {
            assert_eq!(rows(&engine, &mut session, &sql), vec![vec!["0"]]);
        }
    }
    for name in [
        "MBRContains",
        "MBRWithin",
        "MBRIntersects",
        "MBRDisjoint",
        "MBREquals",
        "MBRTouches",
        "MBROverlaps",
        "MBRCovers",
        "MBRCoveredBy",
    ] {
        let sql = format!("SELECT {name}(POINT(0,0),ST_SRID(POINT(1,1),4326))");
        assert_eq!(engine.execute(&sql).unwrap_err().code(), 3033, "{sql}");
    }
    for name in [
        "ST_Contains",
        "ST_Within",
        "ST_Intersects",
        "ST_Disjoint",
        "ST_Touches",
        "ST_Crosses",
        "ST_Overlaps",
    ] {
        assert_eq!(
            rows(
                &engine,
                &mut session,
                &format!("SELECT {name}(ST_GeomFromText('GEOMETRYCOLLECTION EMPTY'),POINT(0,0))")
            ),
            vec![vec!["NULL"]]
        );
    }
    assert_eq!(rows(&engine,&mut session,"SELECT ST_Equals(ST_GeomFromText('GEOMETRYCOLLECTION EMPTY'),ST_GeomFromText('GEOMETRYCOLLECTION EMPTY'))"),vec![vec!["1"]]);
}

#[test]
fn release_audit_global_transaction_requires_admin() {
    let (_dir, engine, mut root) = setup();
    engine
        .execute("CREATE USER audit_usage IDENTIFIED BY 'p'")
        .unwrap();
    let key = crate::engine::auth::AccountKey::new("audit_usage", "%");
    let mut user = SessionContext {
        user: "audit_usage".into(),
        login_user: "audit_usage".into(),
        authenticated_as: key,
        auth_epoch: engine.db.auth_catalog().epoch,
        ..SessionContext::default()
    };
    for sql in [
        "SET GLOBAL TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        "SET GLOBAL TRANSACTION READ ONLY",
    ] {
        assert_eq!(
            engine
                .execute_with_session(&mut user, sql)
                .unwrap_err()
                .code(),
            1227
        );
    }
    engine
        .execute_with_session(&mut user, "SET SESSION TRANSACTION READ ONLY")
        .unwrap();
    engine
        .execute_with_session(
            &mut root,
            "GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO audit_usage",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut user,
            "SET GLOBAL TRANSACTION ISOLATION LEVEL READ COMMITTED",
        )
        .unwrap();
}

#[test]
fn release_audit_numeric_metadata_and_correct_mysql_overflow_rules() {
    let (_dir, engine, mut session) = setup();
    engine
        .execute("CREATE TABLE ordinals(e ENUM('a','b'),s SET('a','b'))")
        .unwrap();
    engine
        .execute("INSERT INTO ordinals VALUES('b','a,b')")
        .unwrap();
    let response = engine
        .execute("SELECT e+0,s+0 FROM ordinals")
        .unwrap()
        .materialize()
        .unwrap();
    let QueryResponse::ResultSet(result) = response else {
        panic!("result set")
    };
    for column in result.columns {
        assert_eq!(column.column_type, ColumnType::LongLong);
    }
    assert_eq!(
        rows(&engine, &mut session, "SELECT e+0,s+0 FROM ordinals"),
        vec![vec!["2", "3"]]
    );
    assert_eq!(
        engine
            .execute("SELECT 9223372036854775807+1")
            .unwrap_err()
            .code(),
        1690
    );
    engine
        .execute("CREATE TABLE decimals(d DECIMAL(38,0))")
        .unwrap();
    engine.execute("INSERT INTO decimals VALUES(99999999999999999999999999999999999999),(99999999999999999999999999999999999999)").unwrap();
    assert_eq!(
        rows(&engine, &mut session, "SELECT SUM(d) FROM decimals"),
        vec![vec!["199999999999999999999999999999999999998"]]
    );
}

#[test]
fn release_audit_online_copy_multiple_tail_passes_preserve_exact_rows() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = Arc::new(open_relaxed_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT,KEY kv(v))")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,10),(2,20)").unwrap();
    let before = engine.database().ddl_copy_perf_snapshot();
    let hook = engine.database().install_online_copy_publish_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext {
            connection_id: 9971,
            ..SessionContext::default()
        };
        worker.execute_with_session(&mut session, "ALTER TABLE t MODIFY v BIGINT, LOCK=NONE")
    });
    let reached = hook.wait_until_reached(Duration::from_secs(10));
    let mut writer = SessionContext {
        connection_id: 9972,
        ..SessionContext::default()
    };
    let sql = format!(
        "INSERT INTO t VALUES{}",
        (1000..3048)
            .map(|id| format!("({id},{})", id * 10))
            .collect::<Vec<_>>()
            .join(",")
    );
    let writes = (|| {
        engine.execute_with_session(&mut writer, &sql)?;
        engine.execute_with_session(&mut writer, "UPDATE t SET v=v+100000 WHERE MOD(id,2)=0")?;
        engine.execute_with_session(&mut writer, "DELETE FROM t WHERE MOD(id,3)=0")?;
        engine.execute_with_session(
            &mut writer,
            "INSERT INTO t VALUES(1001,7) ON DUPLICATE KEY UPDATE v=VALUES(v)",
        )?;
        Ok::<_, QueryError>(())
    })();
    hook.release();
    let result = ddl.join().unwrap();
    assert!(reached, "{result:?}");
    writes.unwrap();
    result.unwrap();
    let expected = [1, 2]
        .into_iter()
        .chain(1000..3048)
        .filter(|id| id % 3 != 0)
        .map(|id| {
            let value = if id == 1001 {
                7
            } else {
                id * 10 + if id % 2 == 0 { 100000 } else { 0 }
            };
            vec![id.to_string(), value.to_string()]
        })
        .collect::<Vec<_>>();
    assert_eq!(
        rows(&engine, &mut writer, "SELECT id,v FROM t ORDER BY id"),
        expected
    );
    assert_eq!(
        rows(
            &engine,
            &mut writer,
            "SELECT id,v FROM t FORCE INDEX(kv) ORDER BY id"
        ),
        expected
    );
    assert_eq!(engine.database().ddl_copy_perf_snapshot()[9] - before[9], 1);
}

#[test]
fn release_audit_index_count_merges_active_and_prepared_old_keys() {
    let (_dir, engine, mut reader) = setup();
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT,KEY kv(v))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,10),(2,20),(3,30)")
        .unwrap();
    let mut writer = SessionContext {
        connection_id: 9973,
        ..SessionContext::default()
    };
    for sql in [
        "XA START 'counter'",
        "DELETE FROM t WHERE id=2",
        "UPDATE t SET v=100 WHERE id=1",
        "INSERT INTO t VALUES(4,40)",
    ] {
        engine.execute_with_session(&mut writer, sql).unwrap();
    }
    for phase in ["active", "prepared"] {
        if phase == "prepared" {
            engine
                .execute_with_session(&mut writer, "XA END 'counter'")
                .unwrap();
            engine
                .execute_with_session(&mut writer, "XA PREPARE 'counter'")
                .unwrap();
        }
        for sql in [
            "SELECT COUNT(*) FROM t FORCE INDEX(kv)",
            "SELECT COUNT(v) FROM t FORCE INDEX(kv)",
            "SELECT COUNT(*) FROM t WHERE v>0",
        ] {
            assert_eq!(
                rows(&engine, &mut reader, sql),
                vec![vec!["3"]],
                "{phase}: {sql}"
            );
        }
    }
    engine
        .execute_with_session(&mut reader, "XA ROLLBACK 'counter'")
        .unwrap();
}
