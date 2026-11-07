use super::support::execute_materialized as exec;
use super::*;

fn rows(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> Vec<Vec<String>> {
    let QueryResponse::ResultSet(result) = exec(engine, session, sql).expect(sql) else {
        panic!("{sql}");
    };
    result
        .rows
        .iter()
        .map(|row| row.iter().map(Value::to_display_string).collect())
        .collect()
}

#[test]
fn report1107_correlation_comparison_domains_and_empty_aggregates() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for sql in [
        "CREATE TABLE ko(id INT PRIMARY KEY,k INT)",
        "CREATE TABLE ks(id INT PRIMARY KEY,k VARCHAR(10),v INT)",
        "INSERT INTO ko VALUES(1,1),(2,2),(3,0),(4,NULL)",
        "INSERT INTO ks VALUES(1,'1',10),(2,'01',20),(3,'1.0',30),(4,'2',40),(5,'abc',50)",
        "CREATE TABLE ids(id INT)",
        "INSERT INTO ids VALUES(1),(2),(3),(5)",
        "CREATE TABLE ci(id INT,k VARCHAR(10) COLLATE utf8mb4_0900_ai_ci,v INT)",
        "CREATE TABLE bin(id INT,k VARCHAR(10) COLLATE utf8mb4_bin)",
        "INSERT INTO ci VALUES(1,'a',1),(2,'A',10),(3,'b',100)",
        "INSERT INTO bin VALUES(1,'a'),(2,'A'),(3,'B')",
    ] {
        exec(&engine, &mut s, sql).unwrap();
    }
    assert_eq!(rows(&engine,&mut s,"SELECT ko.id,(SELECT SUM(v) FROM ks WHERE ks.k=ko.k),(SELECT COUNT(*) FROM ks WHERE ks.k=ko.k) FROM ko ORDER BY ko.id"),[vec!["1","60","3"],vec!["2","40","1"],vec!["3","50","1"],vec!["4","NULL","0"]]);
    assert_eq!(rows(&engine,&mut s,"SELECT ko.id FROM ko WHERE EXISTS(SELECT 1 FROM ks,ids WHERE ks.k=ko.k AND ids.id=ks.id) ORDER BY ko.id"),[vec!["1"],vec!["3"]]);
    assert_eq!(rows(&engine,&mut s,"SELECT ko.id FROM ko WHERE EXISTS(SELECT 1 FROM ks WHERE ks.k=ko.k AND v>15) OR EXISTS(SELECT 1 FROM ks WHERE ks.k=ko.k AND v<15) ORDER BY ko.id"),[vec!["1"],vec!["2"],vec!["3"]]);
    assert_eq!(rows(&engine,&mut s,"SELECT id,(SELECT SUM(v) FROM ci WHERE ci.k=bin.k),(SELECT COUNT(*) FROM ci WHERE ci.k=bin.k) FROM bin ORDER BY id"),[vec!["1","1","1"],vec!["2","10","1"],vec!["3","NULL","0"]]);
    assert_eq!(rows(&engine,&mut s,"SELECT id FROM bin WHERE EXISTS(SELECT 1 FROM ci,ids WHERE ci.k=bin.k AND ids.id=ci.id) ORDER BY id"),[vec!["1"],vec!["2"]]);
    assert_eq!(rows(&engine,&mut s,"SELECT id,(SELECT BIT_OR(v) FROM ci WHERE ci.id=ko.id),(SELECT BIT_AND(v) FROM ci WHERE ci.id=ko.id),(SELECT BIT_XOR(v)+1 FROM ci WHERE ci.id=ko.id) FROM ko WHERE id=4"),[vec!["4","0","18446744073709551615","1"]]);
}

#[test]
fn report1107_outer_join_predicates_stay_above_null_extension() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for sql in [
        "CREATE TABLE t0(id INT PRIMARY KEY,a INT,b INT)",
        "CREATE TABLE t1(id INT PRIMARY KEY,a INT,b INT)",
        "CREATE TABLE t2(id INT PRIMARY KEY,a INT,b INT)",
        "INSERT INTO t0 VALUES(1,1,1),(2,2,2),(3,3,NULL),(4,NULL,4)",
        "INSERT INTO t1 VALUES(1,1,1),(2,2,20),(3,30,3),(5,5,5)",
        "INSERT INTO t2 VALUES(1,1,1),(2,2,2),(6,6,6),(7,NULL,NULL)",
    ] {
        exec(&engine, &mut s, sql).unwrap();
    }
    for hint in ["", "/*+ JOIN_FIXED_ORDER */"] {
        let sql=format!("SELECT {hint} t0.id,t1.id,t2.id FROM t0 JOIN t1 ON t0.a=t1.a RIGHT JOIN t2 ON t1.id=t2.id WHERE t0.b <=> t1.b ORDER BY t2.id");
        assert_eq!(
            rows(&engine, &mut s, &sql),
            [
                vec!["1", "1", "1"],
                vec!["NULL", "NULL", "6"],
                vec!["NULL", "NULL", "7"]
            ]
        );
    }
    for (predicate, expected) in [
        ("d.a>1", vec![vec!["2", "2"], vec!["3", "3"]]),
        ("d.a IS NULL", vec![vec!["NULL", "5"]]),
        ("d.a <=> 1", vec![vec!["1", "1"]]),
    ] {
        assert_eq!(rows(&engine,&mut s,&format!("SELECT d.id,t1.id FROM(SELECT id,a FROM t0 WHERE id>0)d RIGHT JOIN t1 ON d.id=t1.id WHERE {predicate} ORDER BY t1.id")),expected);
    }
    let sql="SELECT t2.id FROM t0 JOIN t1 ON t0.a=t1.a AND t1.a=1 RIGHT JOIN t2 ON t1.id=t2.id ORDER BY t2.id";
    assert_eq!(
        rows(&engine, &mut s, sql),
        [vec!["1"], vec!["2"], vec!["6"], vec!["7"]]
    );
}

#[test]
fn report1107_temporal_bounds_and_boolean_value_contexts() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for sql in ["CREATE TABLE t(id INT PRIMARY KEY,dt DATETIME,KEY i(dt))","INSERT INTO t VALUES(1,'2000-01-01 00:00:00'),(2,'2000-01-01 06:00:00'),(3,'2000-01-01 12:00:00'),(4,'2000-01-02 00:00:00')","CREATE TABLE nb(id INT,a INT)","INSERT INTO nb VALUES(1,5),(2,0),(3,1),(4,NULL)"]{exec(&engine,&mut s,sql).unwrap();}
    for (op, expected) in [
        ("=", vec![vec!["1"]]),
        (">=", vec![vec!["1"], vec!["2"], vec!["3"], vec!["4"]]),
        (">", vec![vec!["2"], vec!["3"], vec!["4"]]),
    ] {
        let sql = format!(
            "SELECT id FROM t WHERE dt {op} CAST('2000-01-01 12:00:00' AS DATE) ORDER BY id"
        );
        assert_eq!(rows(&engine, &mut s, &sql), expected);
    }
    for unit in ["DAY", "WEEK", "MONTH", "YEAR"] {
        for n in [
            "200000000000",
            "9223372036854775807",
            "18446744073709551615",
        ] {
            let sql =
                format!("SELECT id FROM t WHERE dt=DATE_ADD('2000-01-01',INTERVAL {n} {unit})");
            assert!(rows(&engine, &mut s, &sql).is_empty());
        }
    }
    for expr in [
        "(NOT NOT a)=1",
        "(a AND TRUE)=1",
        "(a OR FALSE)=1",
        "(TRUE AND a)+0=1",
        "IF((a AND TRUE)=1,1,0)",
        "CASE WHEN 1 THEN a OR FALSE ELSE 0 END=1",
    ] {
        assert_eq!(
            rows(
                &engine,
                &mut s,
                &format!("SELECT id FROM nb WHERE {expr} ORDER BY id")
            ),
            [vec!["1"], vec!["3"]],
            "{expr}"
        );
    }
}

#[test]
fn report1107_session_limits_and_volatile_grouped_derived() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for sql in ["CREATE TABLE t(id INT,k INT)","INSERT INTO t WITH RECURSIVE x(n) AS(SELECT 1 UNION ALL SELECT n+1 FROM x WHERE n<500) SELECT n,MOD(n,5) FROM x","SET sql_select_limit=2"]{exec(&engine,&mut s,sql).unwrap();}
    assert_eq!(
        rows(
            &engine,
            &mut s,
            "WITH u AS(WITH x AS(SELECT id FROM t) SELECT id FROM x) SELECT COUNT(*) FROM u"
        ),
        [vec!["500"]]
    );
    assert_eq!(
        rows(
            &engine,
            &mut s,
            "SELECT SQL_CALC_FOUND_ROWS id FROM t ORDER BY id"
        ),
        [vec!["1"], vec!["2"]]
    );
    assert_eq!(rows(&engine, &mut s, "SELECT FOUND_ROWS()"), [vec!["500"]]);
    assert_eq!(
        rows(&engine, &mut s, "SELECT id FROM t ORDER BY id LIMIT 3"),
        [vec!["1"], vec!["2"], vec!["3"]]
    );
    exec(&engine, &mut s, "SET sql_select_limit=DEFAULT").unwrap();
    for _ in 0..8 {
        for row in rows(
            &engine,
            &mut s,
            "SELECT k,c FROM(SELECT k,COUNT(*) c FROM t GROUP BY k)d WHERE k>=0 AND RAND()<0.5",
        ) {
            assert_eq!(row[1], "100");
        }
    }
}

#[test]
fn report1107_functional_indexes_reject_volatile_and_stored_functions() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(
        &engine,
        &mut s,
        "CREATE FUNCTION f(x INT) RETURNS INT DETERMINISTIC RETURN x+1",
    )
    .unwrap();
    for expr in [
        "UTC_TIMESTAMP()",
        "UTC_DATE()",
        "UTC_TIME()",
        "LENGTH(RANDOM_BYTES(4))",
        "FOUND_ROWS()",
        "ROW_COUNT()",
        "GET_LOCK('x',0)",
        "f(x)",
        "test.f(x)",
    ] {
        assert!(
            exec(
                &engine,
                &mut s,
                &format!("CREATE TABLE t(x INT,KEY i((x+{expr})))")
            )
            .is_err(),
            "{expr}"
        );
    }
    exec(&engine, &mut s, "CREATE TABLE t(x INT,KEY i((ABS(x)+1)))").unwrap();
    exec(&engine, &mut s, "INSERT INTO t VALUES(-2),(3)").unwrap();
    assert_eq!(
        rows(&engine, &mut s, "SELECT COUNT(*) FROM t WHERE ABS(x)+1=3"),
        [vec!["1"]]
    );
}

#[test]
fn report1107_enum_binary_preserves_charset() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for (charset, collation) in [("utf8mb4", "utf8mb4_bin"), ("latin1", "latin1_bin")] {
        exec(&engine,&mut s,&format!("CREATE TABLE t(e ENUM('a','A') BINARY,s SET('a','A') BINARY) CHARACTER SET {charset}")).unwrap();
        let result = rows(&engine, &mut s, "SHOW FULL COLUMNS FROM t");
        assert_eq!(result[0][2], collation);
        assert_eq!(result[1][2], collation);
        exec(&engine, &mut s, "INSERT INTO t VALUES('a','a'),('A','A')").unwrap();
        assert_eq!(
            rows(&engine, &mut s, "SELECT COUNT(*) FROM t WHERE e='a'"),
            [vec!["1"]]
        );
        exec(&engine, &mut s, "DROP TABLE t").unwrap();
    }
}

#[test]
fn report1107_analyzed_ddl_survives_reopen() {
    for alter in [
        "DROP COLUMN b",
        "DROP INDEX i,ADD INDEX i(b)",
        "DROP INDEX i,ADD INDEX i(b,a)",
        "DROP PRIMARY KEY,ADD PRIMARY KEY(id)",
    ] {
        for partition in ["", " PARTITION BY HASH(id) PARTITIONS 2"] {
            let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
            {
                let engine = open_relaxed_test_engine(dir.path());
                engine.execute(&format!("CREATE TABLE t(id INT,a INT,b INT,PRIMARY KEY(id,a),KEY i(a,b),KEY ib(b)){partition}")).unwrap();
                for sql in ["INSERT INTO t VALUES(1,10,20),(2,11,21)", "ANALYZE TABLE t"] {
                    engine.execute(sql).unwrap();
                }
                engine.execute(&format!("ALTER TABLE t {alter}")).unwrap();
            }
            let engine = open_relaxed_test_engine(dir.path());
            let mut s = SessionContext::default();
            assert_eq!(
                rows(&engine, &mut s, "SELECT COUNT(*) FROM t"),
                [vec!["2"]],
                "{alter}"
            );
        }
    }
}

#[test]
fn report1107_analyze_zero_datetime_and_ddl_lock_release() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for sql in ["SET sql_mode=''","CREATE TABLE t(id INT PRIMARY KEY,d DATETIME)","INSERT INTO t WITH RECURSIVE x(n) AS(SELECT 0 UNION ALL SELECT n+1 FROM x WHERE n<49) SELECT n,IF(n<6,'0000-00-00 00:00:00',DATE_ADD('2020-01-01',INTERVAL n DAY)) FROM x","ANALYZE TABLE t","ALTER TABLE t ADD COLUMN a INT"]{exec(&engine,&mut s,sql).unwrap();}
    assert_eq!(
        rows(&engine, &mut s, "SELECT COUNT(*) FROM t"),
        [vec!["50"]]
    );
}

#[test]
fn report1107_fulltext_uses_column_collation_in_every_mode() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    {
        let engine = open_relaxed_test_engine(dir.path());
        let mut s = SessionContext::default();
        for (name, collation) in [
            ("ci", "utf8mb4_0900_ai_ci"),
            ("bin", "utf8mb4_bin"),
            ("cs", "utf8mb4_0900_as_cs"),
        ] {
            exec(&engine,&mut s,&format!("CREATE TABLE {name}(id INT PRIMARY KEY,s TEXT COLLATE {collation},FULLTEXT(s))")).unwrap();
            exec(
                &engine,
                &mut s,
                &format!("INSERT INTO {name} VALUES(1,'ΟΔΟΣ café Alpha'),(2,'οδος cafe alpha')"),
            )
            .unwrap();
        }
        for mode in ["", "IN BOOLEAN MODE"] {
            for term in ["ΟΔΟΣ", "οδος", "cafe", "café", "Alpha", "alpha"] {
                assert_eq!(
                    rows(
                        &engine,
                        &mut s,
                        &format!(
                            "SELECT id FROM ci WHERE MATCH(s) AGAINST('{term}' {mode}) ORDER BY id"
                        )
                    ),
                    [vec!["1"], vec!["2"]],
                    "{term} {mode}"
                );
            }
        }
        assert_eq!(
            rows(
                &engine,
                &mut s,
                "SELECT id FROM bin WHERE MATCH(s) AGAINST('Alpha') ORDER BY id"
            ),
            [vec!["1"]]
        );
        assert_eq!(
            rows(
                &engine,
                &mut s,
                "SELECT id FROM bin WHERE MATCH(s) AGAINST('cafe') ORDER BY id"
            ),
            [vec!["2"]]
        );
        assert_eq!(
            rows(
                &engine,
                &mut s,
                "SELECT id FROM cs WHERE MATCH(s) AGAINST('cafe') ORDER BY id"
            ),
            [vec!["2"]]
        );
        assert_eq!(
            rows(
                &engine,
                &mut s,
                "SELECT id FROM ci WHERE MATCH(s) AGAINST('+caf*' IN BOOLEAN MODE) ORDER BY id"
            ),
            [vec!["1"], vec!["2"]]
        );
        assert_eq!(rows(&engine,&mut s,"SELECT id FROM ci WHERE MATCH(s) AGAINST('\"οδος cafe\"' IN BOOLEAN MODE) ORDER BY id"),[vec!["1"],vec!["2"]]);
        exec(&engine, &mut s, "UPDATE ci SET s='fresh café' WHERE id=1").unwrap();
        assert_eq!(
            rows(
                &engine,
                &mut s,
                "SELECT id FROM ci WHERE MATCH(s) AGAINST('οδος') ORDER BY id"
            ),
            [vec!["2"]]
        );
    }
    let engine = open_relaxed_test_engine(dir.path());
    let mut s = SessionContext::default();
    assert_eq!(
        rows(
            &engine,
            &mut s,
            "SELECT id FROM ci WHERE MATCH(s) AGAINST('cafe') ORDER BY id"
        ),
        [vec!["1"], vec!["2"]]
    );
    engine.execute("DELETE FROM ci WHERE id=1").unwrap();
    assert_eq!(
        rows(
            &engine,
            &mut s,
            "SELECT id FROM ci WHERE MATCH(s) AGAINST('cafe') ORDER BY id"
        ),
        [vec!["2"]]
    );
    exec(
        &engine,
        &mut s,
        "ALTER TABLE ci MODIFY s TEXT COLLATE utf8mb4_bin",
    )
    .unwrap();
    assert!(rows(
        &engine,
        &mut s,
        "SELECT id FROM ci WHERE MATCH(s) AGAINST('ΟΔΟΣ')"
    )
    .is_empty());
    assert_eq!(
        rows(
            &engine,
            &mut s,
            "SELECT id FROM ci WHERE MATCH(s) AGAINST('οδος')"
        ),
        [vec!["2"]]
    );
    for (name, collation, expected) in [
        ("ngci", "utf8mb4_0900_ai_ci", vec![vec!["1"], vec!["2"]]),
        ("ngbin", "utf8mb4_bin", vec![]),
    ] {
        exec(&engine,&mut s,&format!("CREATE TABLE {name}(id INT,s TEXT COLLATE {collation},FULLTEXT(s) WITH PARSER ngram)")).unwrap();
        exec(
            &engine,
            &mut s,
            &format!("INSERT INTO {name} VALUES(1,'café'),(2,'CAFE')"),
        )
        .unwrap();
        assert_eq!(rows(&engine,&mut s,&format!("SELECT id FROM {name} WHERE MATCH(s) AGAINST('\"cafe\"' IN BOOLEAN MODE) ORDER BY id")),expected);
    }
}

#[test]
fn report1107_ambiguous_identifiers_cannot_access_another_object() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for sql in [
        "CREATE TABLE t(id INT)",
        "INSERT INTO t VALUES(42)",
        "CREATE DATABASE d",
    ] {
        exec(&engine, &mut s, sql).unwrap();
    }
    for sql in [
        "CREATE TABLE d.`e.f`(id INT)",
        "SELECT * FROM d.`e.f`",
        "INSERT INTO d.`e.f` VALUES(1)",
        "DROP TABLE d.`e.f`",
        "ANALYZE TABLE d.`e.f`",
        "CREATE TRIGGER tr BEFORE INSERT ON d.`e.f` FOR EACH ROW SET NEW.x=1",
        "LOAD DATA INFILE '/tmp/mydb/nonexistent.csv' INTO TABLE d.`e.f`",
    ] {
        let error = exec(&engine, &mut s, sql).unwrap_err();
        assert!(matches!(error.code(), 1064 | 1235), "{sql}: {error}");
    }
    assert_eq!(rows(&engine, &mut s, "SELECT * FROM t"), [vec!["42"]]);
    for sql in [
        "CREATE TABLE `tick``name`(id INT)",
        "INSERT INTO `tick``name` VALUES(12)",
    ] {
        exec(&engine, &mut s, sql).unwrap();
    }
    assert_eq!(
        rows(&engine, &mut s, "SELECT id FROM `tick``name`"),
        [vec!["12"]]
    );
    assert_eq!(
        rows(&engine, &mut s, "SHOW TABLES LIKE 'tick%'"),
        [vec!["tick`name"]]
    );
}

#[test]
fn report1107_nullable_index_expressions_and_trigger_columns() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for expression in ["SQRT(a)", "LOG(a)", "ASIN(a)", "MAKEDATE(a,0)", "UNHEX(a)"] {
        exec(
            &engine,
            &mut s,
            &format!("CREATE TABLE t(a INT NOT NULL,KEY i(({expression})))"),
        )
        .unwrap();
        let index = rows(&engine, &mut s, "SHOW INDEX FROM t");
        assert_eq!(index[0][9], "YES", "{expression}");
        exec(&engine, &mut s, "DROP TABLE t").unwrap();
    }
    exec(&engine, &mut s, "CREATE TABLE t(id INT)").unwrap();
    for body in [
        "SET NEW.missing=1",
        "SET @x=NEW.missing",
        "BEGIN IF 0 THEN SET NEW.missing=1; END IF; END",
    ] {
        let sql = format!("CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW {body}");
        assert_eq!(exec(&engine, &mut s, &sql).unwrap_err().code(), 1054);
    }
    exec(
        &engine,
        &mut s,
        "CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW SET NEW.id=7",
    )
    .unwrap();
    exec(&engine, &mut s, "INSERT INTO t VALUES(1)").unwrap();
    assert_eq!(rows(&engine, &mut s, "SELECT * FROM t"), [vec!["7"]]);
}

#[test]
fn report1107_like_null_escape_and_literal_metadata() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    assert_eq!(rows(&engine,&mut s,"SELECT 'a' LIKE 'a' ESCAPE NULL,'a' NOT LIKE 'a' ESCAPE NULL,NULL LIKE 'a' ESCAPE NULL"),[vec!["1","0","NULL"]]);
    let QueryResponse::ResultSet(result) =
        exec(&engine, &mut s, "SELECT X'4D',b'0101',0x4D,0b0101").unwrap()
    else {
        panic!()
    };
    let names: Vec<_> = result
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    assert_eq!(names, ["X'4D'", "b'0101'", "0x4D", "0b0101"]);
}

#[test]
fn report1107_sideways_filters_do_not_duplicate_volatile_evaluation() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    for sql in ["CREATE TABLE sd(id INT PRIMARY KEY)","CREATE TABLE sm(id INT PRIMARY KEY,did INT)","CREATE TABLE sf(id INT PRIMARY KEY,mid INT)","INSERT INTO sd VALUES(1),(2)","INSERT INTO sm VALUES(1,1),(2,1),(3,2),(4,2),(5,2)","INSERT INTO sf WITH RECURSIVE s(n) AS(SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<100) SELECT n,MOD(n,5)+1 FROM s","ANALYZE TABLE sd,sm,sf"] {engine.execute(sql).unwrap();}
    for (predicate, propagates) in [("sd.id>RAND()*2", false), ("sd.id=1", true)] {
        let Statement::Select(mut select)=crate::parser::parse_statement(&format!("SELECT sf.id FROM sf JOIN sm ON sf.mid=sm.id JOIN sd ON sm.did=sd.id WHERE {predicate}")).unwrap() else {panic!()};
        crate::planner::propagate_sideways_dimension_filters(engine.db_for_tests(), &mut select);
        assert_eq!(
            crate::parser::visit::any_expr(select.selection.as_ref().unwrap(), |expr| matches!(
                expr,
                Expr::InSubquery { .. }
            )),
            propagates,
            "{predicate}"
        );
    }
}

#[test]
fn report1107_structural_ddl_preserves_unaffected_statistics() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE TABLE t(id INT PRIMARY KEY,a INT,b INT,KEY ia(a),KEY ib(b))",
        "INSERT INTO t VALUES(1,1,10),(2,2,20)",
        "ANALYZE TABLE t",
        "ALTER TABLE t ADD COLUMN c INT",
        "ALTER TABLE t DROP INDEX ia,ADD INDEX ia(b)",
    ] {
        engine.execute(sql).unwrap();
    }
    let table = engine.db_for_tests().table("t").unwrap();
    assert!(table.stats.columns.contains_key("a"));
    assert!(table.stats.indexes.contains_key("ib"));
    assert!(!table.stats.indexes.contains_key("ia"));
}
