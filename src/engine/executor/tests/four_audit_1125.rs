use super::support::execute_materialized as exec;
use super::*;

fn fixture() -> (tempfile::TempDir, StorageQueryEngine, SessionContext) {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    (dir, engine, SessionContext::default())
}

fn rows(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> Vec<Vec<Value>> {
    match exec(engine, session, sql).unwrap() {
        QueryResponse::ResultSet(rs) => rs.rows,
        other => panic!("expected rows for {sql}: {other:?}"),
    }
}

#[test]
fn audit1125_regexp_search_region_all_operands() {
    let (_dir, e, mut s) = fixture();
    let r = rows(&e, &mut s, "SELECT REGEXP_SUBSTR('aaaa','aaa',2), REGEXP_SUBSTR('123abc456','[0-9]+',2), HEX(REGEXP_SUBSTR('ééé','é.',2)), HEX(REGEXP_SUBSTR(_binary'aaaa',_binary'aaa',2)), REGEXP_SUBSTR('aaaa','^a+',2), REGEXP_SUBSTR('aaaa','a',2,3)");
    assert_eq!(
        r[0],
        vec![
            Value::String("aaa".into()),
            Value::String("23".into()),
            Value::String("C3A9C3A9".into()),
            Value::String("616161".into()),
            Value::String("aaa".into()),
            Value::String("a".into())
        ]
    );
    exec(&e, &mut s, "CREATE TABLE reg(s VARCHAR(30))").unwrap();
    exec(&e, &mut s, "INSERT INTO reg VALUES('aaaa'),('baaaa')").unwrap();
    assert_eq!(
        rows(&e, &mut s, "SELECT REGEXP_SUBSTR(s,'aaa',2) FROM reg")
            .iter()
            .map(|r| r[0].to_display_string())
            .collect::<Vec<_>>(),
        vec!["aaa", "aaa"]
    );
    assert_eq!(
        exec(&e, &mut s, "SELECT REGEXP_LIKE('<a','[[:<:]]a')")
            .unwrap_err()
            .code(),
        3685
    );
}

#[test]
fn audit1125_temporal_components_and_precision() {
    let (_dir, e, mut s) = fixture();
    let r=rows(&e,&mut s,"SELECT CAST('00010101000000' AS DATETIME), CAST('1-01-01' AS DATE), CAST('-001-01-01' AS DATE), CAST('1 10' AS TIME), DATE_ADD('2020-01-01',INTERVAL '1.9' MICROSECOND)");
    let text: Vec<_> = r[0].iter().map(Value::to_display_string).collect();
    assert_eq!(
        text,
        vec![
            "0001-01-01 00:00:00",
            "0001-01-01",
            "NULL",
            "34:00:00",
            "2020-01-01 00:00:00.000001"
        ]
    );
    assert_eq!(
        exec(&e, &mut s, "SELECT CAST('11:12:13' AS TIME(7))")
            .unwrap_err()
            .code(),
        1426
    );
}

#[test]
fn audit1125_zero_date_query_and_strict_write() {
    let (_dir, e, mut s) = fixture();
    assert_eq!(
        rows(
            &e,
            &mut s,
            "SELECT CAST('0000-00-00' AS DATE),CAST('0000-00-00' AS DATETIME)"
        )[0],
        vec![Value::Null, Value::Null]
    );
    exec(&e, &mut s, "CREATE TABLE zd(d DATE)").unwrap();
    assert!(exec(
        &e,
        &mut s,
        "INSERT INTO zd VALUES(CAST('0000-00-00' AS DATE))"
    )
    .is_err());
}

#[test]
fn audit1125_nullif_boolean_bit_and_arity() {
    let (_dir, e, mut s) = fixture();
    assert_eq!(
        rows(&e, &mut s, "SELECT NULLIF(FALSE,b'101'),NULLIF(TRUE,b'1')")[0],
        vec![Value::Int(0), Value::Null]
    );
    assert_eq!(
        exec(&e, &mut s, "SELECT IF(0,ROUND(1.5,0,0),9)")
            .unwrap_err()
            .code(),
        1582
    );
}

#[test]
fn audit1125_order_alias_and_nested_correlation() {
    let (_dir, e, mut s) = fixture();
    for sql in [
        "CREATE TABLE q(id INT PRIMARY KEY,v INT)",
        "INSERT INTO q VALUES(1,10),(2,10),(3,20)",
        "CREATE TABLE m(k INT PRIMARY KEY,w INT)",
        "INSERT INTO m VALUES(1,100),(2,200)",
    ] {
        exec(&e, &mut s, sql).unwrap();
    }
    let r = rows(
        &e,
        &mut s,
        "SELECT v,COUNT(*) AS v FROM q GROUP BY v ORDER BY v",
    );
    assert_eq!(r[0][0], Value::Int(20));
    assert!(rows(&e,&mut s,"SELECT id FROM q WHERE EXISTS(SELECT 1 FROM m WHERE m.k=q.id AND EXISTS(SELECT 1 FROM q o2 WHERE o2.id=q.id+10))").is_empty());
    assert!(exec(&e, &mut s, "SELECT id AS x,v AS x FROM q ORDER BY x").is_err());
}

#[test]
fn audit1125_window_inheritance_and_empty_on_binding() {
    let (_dir, e, mut s) = fixture();
    exec(&e, &mut s, "CREATE TABLE w(id INT,v INT)").unwrap();
    assert_eq!(exec(&e,&mut s,"SELECT SUM(v) OVER (win ROWS 1 PRECEDING) FROM w WINDOW win AS(ORDER BY id ROWS 2 PRECEDING)").unwrap_err().code(),3582);
    assert_eq!(
        exec(
            &e,
            &mut s,
            "SELECT a.id FROM w a LEFT JOIN w b ON a.id=b.id AND c.id IS NOT NULL JOIN w c ON 1=1"
        )
        .unwrap_err()
        .code(),
        1054
    );
}

#[test]
fn audit1125_update_ignore_view_bulk() {
    let (_dir, e, mut s) = fixture();
    for sql in [
        "CREATE TABLE b(id INT PRIMARY KEY,v INT)",
        "INSERT INTO b VALUES(1,7),(2,8)",
        "CREATE VIEW chk AS SELECT * FROM b WHERE v>0 WITH CHECK OPTION",
        "UPDATE IGNORE chk SET v=-2",
    ] {
        exec(&e, &mut s, sql).unwrap();
    }
    assert_eq!(
        rows(&e, &mut s, "SELECT v FROM b ORDER BY id"),
        vec![vec![Value::Int(7)], vec![Value::Int(8)]]
    );
}

#[test]
fn audit1125_routine_repeat_signal_diagnostics() {
    let (_dir, e, mut s) = fixture();
    exec(&e,&mut s,"CREATE PROCEDURE rr() BEGIN SET @a=0; REPEAT REPEAT SET @a=@a+1; UNTIL @a>3 END REPEAT; UNTIL @a>3 END REPEAT; SIGNAL SQLSTATE '01000'; SET @done=1; END").unwrap();
    exec(&e, &mut s, "CALL rr()").unwrap();
    assert_eq!(s.user_variable("a"), Some(&Value::Int(4)));
    assert_eq!(s.user_variable("done"), Some(&Value::Int(1)));
    assert_eq!(
        exec(
            &e,
            &mut s,
            "CREATE PROCEDURE badinto() BEGIN DECLARE x INT; SELECT 1 INTO x UNION SELECT 2; END"
        )
        .unwrap_err()
        .code(),
        3954
    );
    exec(
        &e,
        &mut s,
        "CREATE PROCEDURE rc() BEGIN SELECT 1; GET DIAGNOSTICS @rc=ROW_COUNT; END",
    )
    .unwrap();
    exec(&e, &mut s, "CALL rc()").unwrap();
    assert_eq!(s.user_variable("rc"), Some(&Value::Int(-1)));
}

#[test]
fn audit1125_ignore_warning_order_and_source_rows() {
    let (_dir, e, mut s) = fixture();
    for sql in [
        "CREATE TABLE iw(id INT PRIMARY KEY,v INT NOT NULL)",
        "INSERT INTO iw VALUES(1,1)",
        "INSERT IGNORE INTO iw VALUES(1,2),(3,'zz'),(4,NULL)",
    ] {
        exec(&e, &mut s, sql).unwrap();
    }
    let warnings = rows(&e, &mut s, "SHOW WARNINGS");
    assert_eq!(
        warnings
            .iter()
            .map(|row| row[1].to_display_string())
            .collect::<Vec<_>>(),
        vec!["1062", "1366", "1048"]
    );
    assert!(warnings[1][2].to_display_string().ends_with("at row 2"));
    assert_eq!(rows(&e, &mut s, "SELECT * FROM iw ORDER BY id").len(), 3);
}

#[test]
fn audit1125_load_conversion_diagnostic_source_rows() {
    let (dir, e, mut s) = fixture();
    let file = dir.path().join("rows.csv");
    std::fs::write(&file, "zz\n99zz\n\n").unwrap();
    exec(&e, &mut s, "CREATE TABLE ld(v INT)").unwrap();
    exec(
        &e,
        &mut s,
        &format!("LOAD DATA INFILE '{}' IGNORE INTO TABLE ld", file.display()),
    )
    .unwrap();
    let warnings = rows(&e, &mut s, "SHOW WARNINGS");
    assert_eq!(warnings.len(), 3);
    let mut ordinals = warnings
        .iter()
        .map(|row| {
            row[2]
                .to_display_string()
                .rsplit_once(" at row ")
                .unwrap()
                .1
                .to_string()
        })
        .collect::<Vec<_>>();
    ordinals.sort();
    assert_eq!(ordinals, vec!["1", "2", "3"]);
}

#[test]
fn audit1125_temporal_modes_extractors_and_invalid_date_arithmetic() {
    let (_dir, e, mut s) = fixture();
    assert_eq!(
        rows(&e, &mut s, "SELECT MONTH(0.1),YEAR(TRUE),YEAR(FALSE)")[0],
        vec![Value::Int(0), Value::Null, Value::Int(0)]
    );
    exec(&e, &mut s, "SET sql_mode='ALLOW_INVALID_DATES'").unwrap();
    assert_eq!(
        rows(&e, &mut s, "SELECT DATE_ADD('2020-04-31',INTERVAL 1 DAY)")[0][0],
        Value::Null
    );
    exec(&e, &mut s, "SET sql_mode=''").unwrap();
    assert!(matches!(
        rows(&e, &mut s, "SELECT CAST('0000-00-00' AS DATE)")[0][0],
        Value::Date(crate::types::ZERO_DATE)
    ));
}

#[test]
fn audit1125_cursor_and_local_set_preserve_incoming_diagnostics() {
    let (_dir, e, mut s) = fixture();
    exec(&e,&mut s,"CREATE PROCEDURE frc() BEGIN DECLARE x INT; DECLARE rc INT; DECLARE c CURSOR FOR SELECT 1; SET x=2; OPEN c; FETCH c INTO x; GET DIAGNOSTICS rc=ROW_COUNT; SET @g=rc,@f=ROW_COUNT(); CLOSE c; END").unwrap();
    for (pre, expected) in [("SELECT 1", -1), ("SET @x=1", 0)] {
        exec(&e, &mut s, pre).unwrap();
        exec(&e, &mut s, "CALL frc()").unwrap();
        assert_eq!(s.user_variable("g"), Some(&Value::Int(expected)));
        assert_eq!(s.user_variable("f"), Some(&Value::Int(expected)));
    }
}

#[test]
fn audit1125_direct_named_window_retains_frame() {
    let (_dir, e, mut s) = fixture();
    exec(&e, &mut s, "CREATE TABLE nw(id INT,v INT)").unwrap();
    exec(&e, &mut s, "INSERT INTO nw VALUES(1,10),(2,20),(3,30)").unwrap();
    let result = rows(
        &e,
        &mut s,
        "SELECT SUM(v) OVER win FROM nw WINDOW win AS(ORDER BY id ROWS 1 PRECEDING)",
    );
    assert_eq!(
        result
            .iter()
            .map(|row| row[0].to_display_string())
            .collect::<Vec<_>>(),
        vec!["10", "30", "50"]
    );
    assert_eq!(
        exec(
            &e,
            &mut s,
            "SELECT SUM(v) OVER (win) FROM nw WINDOW win AS(ORDER BY id ROWS 1 PRECEDING)"
        )
        .unwrap_err()
        .code(),
        3582
    );
}

#[test]
fn audit1125_regexp_binary_offsets_and_escaped_legacy_literal() {
    let (_dir, e, mut s) = fixture();
    let result=rows(&e,&mut s,"SELECT HEX(REGEXP_SUBSTR(x'80808080',x'808080',2)),REGEXP_SUBSTR('abc','$',4),REGEXP_SUBSTR('aaaa','a',2,4),REGEXP_LIKE(_binary'[[:<:]]',CHAR(92,91,92,91,58,60,58,92,93,92,93))");
    assert_eq!(
        result[0],
        vec![
            Value::String("808080".into()),
            Value::String("".into()),
            Value::Null,
            Value::Int(1)
        ]
    );
    assert_eq!(
        exec(&e, &mut s, "SELECT REGEXP_LIKE(_binary'a',CHAR(92))")
            .unwrap_err()
            .code(),
        3689
    );
    assert_eq!(
        exec(&e, &mut s, "SELECT REGEXP_LIKE('a','(?<x>a)|(?<x>b)')")
            .unwrap_err()
            .code(),
        3887
    );
}

#[test]
fn audit1125_interval_numeric_rounds_text_truncates() {
    let (_dir, e, mut s) = fixture();
    let result=rows(&e,&mut s,"SELECT DATE_ADD('2020-01-01',INTERVAL 1.9 MICROSECOND),DATE_ADD('2020-01-01',INTERVAL '1.9' MICROSECOND),DATE_ADD('2020-01-01',INTERVAL '1.9' DAY)");
    assert_eq!(
        result[0]
            .iter()
            .map(Value::to_display_string)
            .collect::<Vec<_>>(),
        vec![
            "2020-01-01 00:00:00.000002",
            "2020-01-01 00:00:00.000001",
            "2020-01-02"
        ]
    );
}

#[test]
fn audit1125_nested_signal_warning_continues_in_its_own_scope() {
    let (_dir, e, mut s) = fixture();
    exec(&e,&mut s,"CREATE PROCEDURE nested_warn() BEGIN BEGIN SIGNAL SQLSTATE '01000'; SET @inner_reached=1; END; SET @outer_reached=1; END").unwrap();
    exec(&e, &mut s, "CALL nested_warn()").unwrap();
    assert_eq!(s.user_variable("inner_reached"), Some(&Value::Int(1)));
    assert_eq!(s.user_variable("outer_reached"), Some(&Value::Int(1)));
    exec(
        &e,
        &mut s,
        "CREATE PROCEDURE explicit_nf() SIGNAL SQLSTATE '02000'",
    )
    .unwrap();
    assert_eq!(
        exec(&e, &mut s, "CALL explicit_nf()").unwrap_err().code(),
        1643
    );
}

#[test]
fn audit1125_dead_branches_validate_native_function_families() {
    let (_dir, e, mut s) = fixture();
    for call in [
        "ROUND(1,2,3)",
        "SUBSTRING('x')",
        "YEAR(1,2)",
        "LENGTH('a','b')",
        "JSON_OBJECT('key')",
        "JSON_SET('{}','$.a')",
        "INET_ATON('a','b')",
        "COMPRESS('a','b')",
    ] {
        let sql = format!("SELECT IF(0,{call},9)");
        assert_eq!(exec(&e, &mut s, &sql).unwrap_err().code(), 1582, "{sql}");
    }
}

#[test]
fn audit1125_zero_between_bound_and_equality_have_distinct_rules() {
    let (_dir, e, mut s) = fixture();
    exec(&e, &mut s, "SET sql_mode=''").unwrap();
    exec(&e, &mut s, "CREATE TABLE zcmp(d DATE,dt DATETIME)").unwrap();
    exec(
        &e,
        &mut s,
        "INSERT INTO zcmp VALUES('0000-00-00','0000-00-00')",
    )
    .unwrap();
    exec(
        &e,
        &mut s,
        "SET sql_mode='STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE'",
    )
    .unwrap();
    assert_eq!(rows(&e,&mut s,"SELECT d BETWEEN '0000-00-00' AND '0001-01-01',dt BETWEEN '0000-00-00' AND '0001-01-01' FROM zcmp")[0],vec![Value::Int(1),Value::Int(1)]);
    assert_eq!(
        rows(
            &e,
            &mut s,
            "SELECT CAST(d AS DATETIME),CAST(dt AS DATE) FROM zcmp"
        )[0]
        .iter()
        .map(Value::to_display_string)
        .collect::<Vec<_>>(),
        vec!["0000-00-00 00:00:00", "0000-00-00"]
    );
    assert_eq!(
        exec(&e, &mut s, "SELECT * FROM zcmp WHERE d='0000-00-00'")
            .unwrap_err()
            .code(),
        1525
    );
}

#[test]
fn audit1125_vertical_regex_escape_and_verbose_comments() {
    let (_dir, e, mut s) = fixture();
    for cp in [10, 11, 12, 13, 133, 8232, 8233] {
        let sql=format!("SELECT REGEXP_LIKE(CONVERT(CHAR({}) USING utf8mb4),CONVERT(CHAR(92,118) USING utf8mb4))",if cp==133 {"194,133".to_string()} else if cp==8232 {"226,128,168".to_string()} else if cp==8233 {"226,128,169".to_string()} else {cp.to_string()});
        assert_eq!(rows(&e, &mut s, &sql)[0][0], Value::Int(1), "{cp}");
    }
    assert_eq!(
        rows(
            &e,
            &mut s,
            "SELECT REGEXP_LIKE('a','(?x)# [[:<:]] ignored\na')"
        )[0][0],
        Value::Int(1)
    );
}

#[test]
fn audit1125_typed_zero_date_literals_keep_binding_errors() {
    let (_dir, e, mut s) = fixture();
    for sql in [
        "SELECT DATE '0000-00-00'",
        "SELECT TIMESTAMP '0000-00-00 00:00:00'",
    ] {
        assert_eq!(exec(&e, &mut s, sql).unwrap_err().code(), 1525);
    }
    assert_eq!(
        rows(&e, &mut s, "SELECT CAST('0000-00-00' AS DATE)")[0][0],
        Value::Null
    );
    assert_eq!(
        exec(
            &e,
            &mut s,
            "SELECT REGEXP_LIKE(_binary'0',CHAR(91,92,118,45,97,93))"
        )
        .unwrap_err()
        .code(),
        3688
    );
}

#[test]
fn audit1125_user_signal_text_is_not_rewritten_as_input_row_metadata() {
    let (_dir, e, mut s) = fixture();
    exec(&e, &mut s, "CREATE TABLE warning_rows(id INT,v INT)").unwrap();
    exec(&e,&mut s,"CREATE FUNCTION warning_row_fn() RETURNS INT DETERMINISTIC BEGIN SIGNAL SQLSTATE '01000' SET MESSAGE_TEXT='user warning at row 1'; RETURN 1; END").unwrap();
    exec(
        &e,
        &mut s,
        "INSERT INTO warning_rows VALUES(1,warning_row_fn()),(2,warning_row_fn())",
    )
    .unwrap();
    let warnings = rows(&e, &mut s, "SHOW WARNINGS");
    assert_eq!(
        warnings
            .iter()
            .map(|row| row[2].to_display_string())
            .collect::<Vec<_>>(),
        vec!["user warning at row 1", "user warning at row 1"]
    );
    assert_eq!(
        rows(&e, &mut s, "SELECT COUNT(*) FROM warning_rows")[0][0],
        Value::Int(2)
    );
}

#[test]
fn audit1125_failed_select_binding_never_adds_an_empty_result_set() {
    let (_dir, e, mut s) = fixture();
    for (index, select) in [
        "SELECT missing_column",
        "SELECT missing_function()",
        "SELECT test.missing_function()",
    ]
    .iter()
    .enumerate()
    {
        let name = format!("failed_bind_{index}");
        exec(&e,&mut s,&format!("CREATE PROCEDURE {name}() BEGIN DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SELECT 'h'; {select}; SELECT 'after'; END")).unwrap();
        let QueryResponse::MultiResult(results) =
            exec(&e, &mut s, &format!("CALL {name}()")).unwrap()
        else {
            panic!("CALL response");
        };
        let sets = results
            .into_iter()
            .filter_map(|response| match response {
                QueryResponse::ResultSet(rs) => Some(rs.rows),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            sets,
            vec![
                vec![vec![Value::String("h".into())]],
                vec![vec![Value::String("after".into())]]
            ],
            "{select}"
        );
    }
}
