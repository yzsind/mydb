//! Regression cases from the 2026-10-01 SQL processing audit
//! (report archived in git history).
use super::*;
use crate::engine::executor::vec::config::{with_vec_mode, VecMode};

fn fixture(sql: &[&str]) -> (tempfile::TempDir, StorageQueryEngine) {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::Builder::new()
        .prefix("sqlproc-")
        .tempdir_in("/tmp/mydb")
        .unwrap();
    let engine = open_test_engine(dir.path());
    for statement in sql {
        engine.execute(statement).unwrap();
    }
    (dir, engine)
}

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<String>> {
    let response = engine
        .execute(sql)
        .unwrap_or_else(|err| panic!("{sql}: {err}"));
    display_rows(response)
}

fn session_rows(
    engine: &StorageQueryEngine,
    session: &mut SessionContext,
    sql: &str,
) -> Vec<Vec<String>> {
    display_rows(
        engine
            .execute_with_session(session, sql)
            .unwrap_or_else(|err| panic!("{sql}: {err}")),
    )
}

fn display_rows(response: QueryResponse) -> Vec<Vec<String>> {
    let values = match response {
        QueryResponse::ResultSet(rs) => rs.rows,
        QueryResponse::StreamingResultSet(mut rs) => {
            let mut rows = Vec::new();
            while let Some(row) = rs.cursor.next_row().unwrap() {
                rows.push(row);
            }
            rows
        }
        other => panic!("expected result set: {other:?}"),
    };
    values
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|value| value.to_display_string())
                .collect()
        })
        .collect()
}

#[test]
fn sqlproc_or_keeps_table_identity_and_index_residuals_in_both_modes() {
    let (_dir, e) = fixture(&[
        "CREATE TABLE l(a INT,b INT,KEY(a))",
        "CREATE TABLE r(a INT,c INT,KEY(a))",
        "INSERT INTO l VALUES(3,1),(2,2)",
        "INSERT INTO r VALUES(2,1)",
    ]);
    for mode in [VecMode::Off, VecMode::On] {
        with_vec_mode(mode, || {
            assert_eq!(
                rows(
                    &e,
                    "SELECT l.a,r.a FROM l JOIN r ON l.b=r.c WHERE l.a=1 OR r.a=2"
                ),
                vec![vec!["3", "2"]]
            );
            assert_eq!(
                rows(&e, "SELECT a FROM l WHERE a IN(2,3) AND RAND()>2"),
                Vec::<Vec<String>>::new()
            );
            assert_eq!(
                rows(&e, "SELECT a FROM l WHERE (a=2 OR a=3) AND RAND()>2"),
                Vec::<Vec<String>>::new()
            );
            assert_eq!(
                rows(&e, "SELECT a FROM l WHERE a=2 OR l.a=3 ORDER BY a"),
                vec![vec!["2"], vec!["3"]]
            );
        });
    }
}

#[test]
fn sqlproc_partition_odku_targets_the_conflicting_heap() {
    let (_dir, e) = fixture(&[
        "CREATE TABLE t(id BIGINT AUTO_INCREMENT, email VARCHAR(20),p INT,v INT,UNIQUE(id,p),UNIQUE(email,p)) PARTITION BY HASH(p) PARTITIONS 4",
        "INSERT INTO t(email,p,v) VALUES('a',0,0),('b',1,1),('c',2,2),('d',3,3),('e',0,4),('f',1,5)",
    ]);
    assert_eq!(rows(&e, "INSERT INTO t(email,p,v) VALUES('f',1,50) ON DUPLICATE KEY UPDATE v=VALUES(v) RETURNING email,p,v"), vec![vec!["f","1","50"]]);
    e.execute("INSERT INTO t(email,p,v) VALUES('c',2,20) ON DUPLICATE KEY UPDATE v=VALUES(v)")
        .unwrap();
    assert_eq!(
        rows(&e, "SELECT email,v FROM t ORDER BY email"),
        vec![
            vec!["a", "0"],
            vec!["b", "1"],
            vec!["c", "20"],
            vec!["d", "3"],
            vec!["e", "4"],
            vec!["f", "50"]
        ]
    );
}

#[test]
fn sqlproc_rollup_ownership_across_ctes_subqueries_and_derived_tables() {
    let (_dir, e) = fixture(&[
        "CREATE TABLE t(a INT,b INT)",
        "INSERT INTO t VALUES(1,1),(1,2),(2,3)",
    ]);
    for mode in [VecMode::Off, VecMode::On] {
        with_vec_mode(mode, || {
            assert_eq!(rows(&e,"WITH c AS(SELECT a,SUM(b) s FROM t GROUP BY a WITH ROLLUP) SELECT s,COUNT(*) FROM c GROUP BY s ORDER BY s"),vec![vec!["3","2"],vec!["6","1"]]);
            assert_eq!(
                rows(
                    &e,
                    "SELECT 1 WHERE EXISTS(SELECT a FROM t GROUP BY a WITH ROLLUP)"
                ),
                vec![vec!["1"]]
            );
            assert_eq!(
                rows(
                    &e,
                    "SELECT(SELECT COUNT(*) FROM(SELECT a FROM t GROUP BY a WITH ROLLUP) d)"
                ),
                vec![vec!["3"]]
            );
            assert_eq!(rows(&e,"SELECT a,COUNT(*) FROM(SELECT a,SUM(b) s FROM t GROUP BY a) d GROUP BY a WITH ROLLUP ORDER BY a"),vec![vec!["NULL","2"],vec!["1","1"],vec!["2","1"]]);
        });
    }
}

#[test]
fn sqlproc_rewrites_preserve_literals_and_nullary_odku_functions() {
    let (_dir, e) = fixture(&[
        "CREATE TABLE t(a INT PRIMARY KEY,s VARCHAR(100),d DATETIME)",
        "INSERT INTO t VALUES(1,'old',NULL)",
    ]);
    for text in ["returning", "x returning y", "returning''x"] {
        let sql = format!("INSERT INTO t AS x VALUES(2,'{text}',NULL)");
        assert_eq!(e.execute(&sql).unwrap_err().code(), 1064);
    }
    assert_eq!(
        rows(
            &e,
            "INSERT INTO t AS x VALUES(2,'returning',NULL) RETURNING x.s"
        ),
        vec![vec!["returning"]]
    );
    e.execute("INSERT INTO t(a,s) VALUES(1,'new') ON DUPLICATE KEY UPDATE s=VALUES(s),d=NOW()")
        .unwrap();
    assert_eq!(
        rows(&e, "SELECT s,d IS NOT NULL FROM t WHERE a=1"),
        vec![vec!["new", "1"]]
    );
}

#[test]
fn sqlproc_date_values_and_unsigned_materialization_match_metadata() {
    let (_dir, e) = fixture(&[]);
    for mode in [VecMode::Off, VecMode::On] {
        with_vec_mode(mode, || {
            assert_eq!(rows(&e,"SELECT CURDATE()+0=CAST(DATE_FORMAT(CURDATE(),'%Y%m%d') AS UNSIGNED),UTC_DATE()+0=CAST(DATE_FORMAT(UTC_DATE(),'%Y%m%d') AS UNSIGNED),LAST_DAY('2024-02-01')+0"),vec![vec!["1","1","20240229"]])
        });
    }
    e.execute("CREATE TABLE dates AS SELECT CURDATE() a,UTC_DATE() b,LAST_DAY('2024-02-01') c")
        .unwrap();
    assert_eq!(rows(&e, "SELECT c+0 FROM dates"), vec![vec!["20240229"]]);
    e.execute(
        "CREATE TABLE nums AS SELECT 18446744073709551615 DIV 1 a,-5 MOD CAST(3 AS UNSIGNED) b",
    )
    .unwrap();
    assert_eq!(
        rows(&e, "SELECT a,b FROM nums"),
        vec![vec!["18446744073709551615", "-2"]]
    );
}

#[test]
fn sqlproc_field_uses_string_integer_and_mixed_numeric_domains() {
    let (_dir, e) = fixture(&[]);
    assert_eq!(rows(&e,"SELECT FIELD(1,'1.0'),FIELD('1.0',1),FIELD('a','A'),FIELD(NULL,NULL),FIELD(9007199254740993,9007199254740992),FIELD(1.0,1),FIELD(-1,CAST(18446744073709551615 AS UNSIGNED))"),vec![vec!["1","1","1","0","0","1","0"]]);
}

#[test]
fn sqlproc_field_uses_declared_types_for_nulls_decimals_and_collations() {
    let (_dir, e) = fixture(&[
        "CREATE TABLE t(s VARCHAR(10),n BIGINT)",
        "INSERT INTO t VALUES(NULL,NULL)",
    ]);
    for mode in [VecMode::Off, VecMode::On] {
        with_vec_mode(mode, || {
            assert_eq!(rows(&e,"SELECT FIELD(1,X'31'),FIELD(49,X'31'),FIELD(X'31','1'),FIELD(_binary'a',_utf8mb4'A' COLLATE utf8mb4_0900_ai_ci)"),vec![vec!["0","1","1","0"]]);
            assert_eq!(rows(&e,"SELECT FIELD('a','b',NULL),FIELD('01','1',s),FIELD('01','1',n),FIELD(9007199254740993,9007199254740992,n),FIELD(9007199254740993,9007199254740992,NULL) FROM t"),vec![vec!["0","0","1","0","1"]]);
            assert_eq!(rows(&e,"SELECT FIELD(CAST(9007199254740993 AS DECIMAL(20,0)),CAST(9007199254740992 AS DECIMAL(20,0))),FIELD('01','1',CAST(NULL AS UNSIGNED)),FIELD(UNHEX('C3'),UNHEX('FF')),FIELD('A' COLLATE utf8mb4_bin,'a'),FIELD(CURDATE(),DATE_FORMAT(CURDATE(),'%Y%m%d'))"),vec![vec!["0","1","0","0","0"]]);
        });
    }
}

#[test]
fn sqlproc_window_exclude_does_not_move_between_frames() {
    let (_dir, e) = fixture(&[
        "CREATE TABLE t(id INT,x INT)",
        "INSERT INTO t VALUES(1,1),(2,2),(3,3)",
    ]);
    assert_eq!(rows(&e,"SELECT SUM(x) OVER(ORDER BY id ROWS UNBOUNDED PRECEDING),SUM(x) OVER(ORDER BY id ROWS UNBOUNDED PRECEDING EXCLUDE CURRENT ROW),SUM(x) OVER(ORDER BY id ROWS UNBOUNDED PRECEDING EXCLUDE NO OTHERS) FROM t ORDER BY id"),vec![vec!["1","NULL","1"],vec!["3","1","3"],vec!["6","3","6"]]);
}

#[test]
fn sqlproc_quoted_identifiers_and_variables_remain_distinct() {
    let (_dir, e) = fixture(&[
        "CREATE TABLE t(`@a` INT,`@@session` INT)",
        "INSERT INTO t VALUES(7,9)",
    ]);
    let mut session = SessionContext::default();
    e.execute_with_session(&mut session, "SET @'a'=5,@'a b'=8")
        .unwrap();
    assert_eq!(
        session_rows(
            &e,
            &mut session,
            "SELECT `@a`,`@@session`,@'a',@`a b` FROM t HAVING @'a'=5"
        ),
        vec![vec!["7", "9", "5", "8"]]
    );
    e.execute_with_session(&mut session, "SET @'x'=(@'a'+1),@'y'=@'a b'")
        .unwrap();
    assert_eq!(
        session_rows(&e, &mut session, "SELECT @'x',@'y'"),
        vec![vec!["6", "8"]]
    );
}

#[test]
fn sqlproc_update_and_prepared_odku_validate_target_qualifiers() {
    let (_dir, e) = fixture(&[
        "CREATE TABLE t(id INT PRIMARY KEY,v INT)",
        "INSERT INTO t VALUES(1,0)",
    ]);
    for sql in [
        "UPDATE t SET bogus.v=5 WHERE id=1",
        "INSERT INTO t VALUES(1,5) ON DUPLICATE KEY UPDATE bogus.v=5",
    ] {
        assert_eq!(e.execute(sql).unwrap_err().code(), 1054);
    }
    assert_eq!(rows(&e, "SELECT v FROM t"), vec![vec!["0"]]);
    e.execute("UPDATE t AS x SET x.v=2 WHERE id=1").unwrap();
    let mut s = SessionContext::default();
    for sql in [
        "UPDATE t SET bogus.v=? WHERE id=?",
        "INSERT INTO t VALUES(?,?) ON DUPLICATE KEY UPDATE bogus.v=5",
    ] {
        let prepare = format!("PREPARE p FROM '{sql}'");
        match e.execute_with_session(&mut s, &prepare) {
            Err(err) => assert_eq!(err.code(), 1054),
            Ok(_) => {
                e.execute_with_session(&mut s, "SET @p1=1,@p2=1").unwrap();
                assert_eq!(
                    e.execute_with_session(&mut s, "EXECUTE p USING @p1,@p2")
                        .unwrap_err()
                        .code(),
                    1054
                );
                e.execute_with_session(&mut s, "DEALLOCATE PREPARE p")
                    .unwrap();
            }
        }
    }
    assert_eq!(rows(&e, "SELECT v FROM t"), vec![vec!["2"]]);
}

#[test]
fn sqlproc_unsupported_clauses_error_without_side_effects() {
    let (_dir, e) = fixture(&["CREATE TABLE t(a INT,b INT)", "INSERT INTO t VALUES(1,2)"]);
    for sql in [
        "SELECT SUM(a) WITHIN GROUP(ORDER BY b) FROM t",
        "SELECT FIRST_VALUE(a) IGNORE NULLS OVER(ORDER BY b) FROM t",
    ] {
        assert_eq!(e.execute(sql).unwrap_err().code(), 1235);
    }
    for sql in [
        "DO SLEEP(0) ORDER BY 1",
        "DO SLEEP(0) LIMIT 1",
        "ALTER TABLE t DROP COLUMN a CASCADE",
        "ALTER TABLE t DROP COLUMN IF EXISTS a",
    ] {
        assert_eq!(e.execute(sql).unwrap_err().code(), 1064);
    }
    assert_eq!(rows(&e, "SELECT * FROM t"), vec![vec!["1", "2"]]);
}

#[test]
fn sqlproc_grouped_derived_predicate_stays_above_expression_group() {
    let (_dir, e) = fixture(&[
        "CREATE TABLE t(a INT,b INT)",
        "INSERT INTO t VALUES(1,10),(1,30)",
    ]);
    let mut session = SessionContext::default();
    e.execute_with_session(&mut session, "SET sql_mode=''")
        .unwrap();
    for mode in [VecMode::Off, VecMode::On] {
        with_vec_mode(mode, || {
            assert_eq!(
                session_rows(
                    &e,
                    &mut session,
                    "SELECT s FROM(SELECT a+1 k,b,SUM(b) s FROM t GROUP BY a+1) d WHERE d.b>20"
                ),
                Vec::<Vec<String>>::new()
            )
        });
    }
}

#[test]
fn sqlproc_wildcard_ordinals_do_not_reinterpret_constant_projections() {
    let (_dir, e) = fixture(&["CREATE TABLE t(a INT)", "INSERT INTO t VALUES(1),(1),(2)"]);
    assert_eq!(
        rows(&e, "SELECT 10,t.* FROM t GROUP BY 1,2 ORDER BY 2"),
        vec![vec!["10", "1"], vec!["10", "2"]]
    );
    assert_eq!(
        rows(&e, "SELECT t.*,COUNT(*) FROM t GROUP BY 1 ORDER BY 1"),
        vec![vec!["1", "2"], vec!["2", "1"]]
    );
}

#[test]
fn sqlproc_union_cte_normalizes_common_domains_before_memory_or_spill() {
    let (_dir, e) = fixture(&[
        "CREATE TABLE s(v VARCHAR(10))",
        "CREATE TABLE n(v INT)",
        "INSERT INTO s SELECT '1' FROM generate_series(1,5000)",
        "INSERT INTO n VALUES(1)",
        "CREATE TABLE d(v DATE)",
        "CREATE TABLE dt(v DATETIME)",
        "INSERT INTO d VALUES('2026-01-01')",
        "INSERT INTO dt VALUES('2026-01-01 00:00:00')",
        "CREATE TABLE decs(v DECIMAL(30,12))",
        "INSERT INTO decs VALUES(1)",
    ]);
    for parallel in ["off", "on"] {
        for threshold in ["0", "8"] {
            with_test_env(
                &[
                    ("MYDB_CTE_UNION_BRANCH_PARALLEL", Some(parallel)),
                    ("MYDB_CTE_SPILL_THRESHOLD_MB", Some(threshold)),
                ],
                || {
                    assert_eq!(rows(&e,"WITH c AS(SELECT v FROM s UNION ALL SELECT v FROM n) SELECT COUNT(DISTINCT v) FROM c"),vec![vec!["1"]]);
                    assert_eq!(rows(&e,"WITH c AS(SELECT v FROM d UNION ALL SELECT v FROM dt) SELECT COUNT(DISTINCT v),MIN(v) FROM c"),vec![vec!["1","2026-01-01 00:00:00"]]);
                    assert_eq!(rows(&e,"WITH c AS(SELECT v FROM decs UNION ALL SELECT v FROM n) SELECT MIN(v),MAX(v) FROM c"),vec![vec!["1.000000000000","1.000000000000"]]);
                },
            );
        }
    }
}
