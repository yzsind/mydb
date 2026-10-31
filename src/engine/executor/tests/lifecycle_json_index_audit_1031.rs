use super::*;

fn strings(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<String>> {
    let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap() else {
        panic!("{sql}")
    };
    rs.rows
        .iter()
        .map(|r| r.iter().map(Value::to_display_string).collect())
        .collect()
}

#[test]
fn audit1031_json_ranges() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_test_engine(dir.path());
    for path in [
        "$[0 to last-3]",
        "$[last-5 to last-3]",
        "$[last-99 to last-99]",
    ] {
        assert_eq!(
            strings(&e, &format!("SELECT JSON_EXTRACT('[1,2,3]', '{path}')")),
            vec![vec!["NULL"]]
        );
    }
}

#[test]
fn audit1031_json_decimal_and_overflow() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_test_engine(dir.path());
    assert_eq!(
        strings(&e, "SELECT CAST(CAST('1e40' AS JSON) AS DECIMAL(65,0))"),
        vec![vec![format!("1{}", "0".repeat(40))]]
    );
    assert_eq!(
        strings(
            &e,
            "SELECT JSON_VALUE(CAST('[1e40]' AS JSON), '$[0]' RETURNING DECIMAL(65,0))"
        ),
        vec![vec![format!("1{}", "0".repeat(40))]]
    );
    for (number, target) in [
        ("18446744073709551615", "SIGNED"),
        ("18446744073709551616", "UNSIGNED"),
        ("-9223372036854775809", "SIGNED"),
        ("1e40", "SIGNED"),
    ] {
        assert_eq!(
            strings(
                &e,
                &format!("SELECT JSON_VALUE(CAST('{number}' AS JSON), '$' RETURNING {target})")
            ),
            vec![vec!["NULL"]]
        );
    }
}

#[test]
fn audit1031_json_equivalence() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE ja (id INT PRIMARY KEY, j JSON)")
        .unwrap();
    e.execute("CREATE TABLE jb (id INT PRIMARY KEY, j JSON)")
        .unwrap();
    e.execute("INSERT INTO ja VALUES (1,'1'),(2,'1.0'),(3,'[1,2]')")
        .unwrap();
    e.execute("INSERT INTO jb VALUES (10,'1.00')").unwrap();
    assert_eq!(
        strings(
            &e,
            "SELECT a.id,b.id FROM ja a JOIN jb b ON a.j=b.j ORDER BY a.id"
        ),
        vec![vec!["1", "10"], vec!["2", "10"]]
    );
    assert_eq!(
        strings(&e, "SELECT COUNT(DISTINCT j) FROM ja"),
        vec![vec!["2"]]
    );
    assert_eq!(
        strings(
            &e,
            "SELECT CAST('1' AS JSON) UNION SELECT CAST('1.0' AS JSON)"
        )
        .len(),
        1
    );
}

#[test]
fn audit1031_json_binary_and_boundaries() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_test_engine(dir.path());
    assert_eq!(
        strings(
            &e,
            "SELECT JSON_TYPE(JSON_EXTRACT(JSON_ARRAY(_binary 'abc'),'$[0]'))"
        ),
        vec![vec!["BLOB"]]
    );
    assert_eq!(
        strings(&e, "SELECT JSON_TYPE('-0'), JSON_EXTRACT('-0','$')"),
        vec![vec!["INTEGER", "0"]]
    );
    let doc = format!("{}1{}", "[".repeat(100), "]".repeat(100));
    assert_eq!(
        strings(&e, &format!("SELECT JSON_DEPTH('{doc}')")),
        vec![vec!["101"]]
    );
}

#[test]
fn audit1031_functional_collation() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE fc (id INT PRIMARY KEY, s VARCHAR(100), INDEX k ((CONCAT(s,''))))")
        .unwrap();
    e.execute("INSERT INTO fc VALUES (1,'Alice')").unwrap();
    for predicate in [
        "CONCAT(s,'')='ALICE'",
        "CONCAT(s,'') IN ('ALICE')",
        "CONCAT(s,'') LIKE 'ali%'",
    ] {
        assert_eq!(
            strings(
                &e,
                &format!("SELECT id FROM fc FORCE INDEX(k) WHERE {predicate}")
            ),
            vec![vec!["1"]],
            "{predicate}"
        );
    }
    e.execute(
        "CREATE TABLE fu (id INT PRIMARY KEY, s VARCHAR(100), UNIQUE KEY k ((CONCAT(s,''))))",
    )
    .unwrap();
    e.execute("INSERT INTO fu VALUES (1,'ABC')").unwrap();
    assert!(e.execute("INSERT INTO fu VALUES (2,'abc')").is_err());
}

#[test]
fn audit1031_functional_numeric() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE fn (id INT PRIMARY KEY,a DECIMAL(10,1),b DECIMAL(10,1), UNIQUE KEY k ((a+b)))").unwrap();
    e.execute("INSERT INTO fn VALUES (1,0.5,0.0),(2,0.9,0.0),(3,2.4,0.0)")
        .unwrap();
    assert_eq!(
        strings(&e, "SELECT id FROM fn FORCE INDEX(k) WHERE a+b>2.0"),
        vec![vec!["3"]]
    );
}

#[test]
fn audit1031_functional_strict() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_test_engine(dir.path());
    e.execute("SET sql_mode = DEFAULT").unwrap();
    e.execute("CREATE TABLE fs (a VARCHAR(20),INDEX k ((CAST(a AS SIGNED))))")
        .unwrap();
    assert!(e.execute("INSERT INTO fs VALUES ('abc')").is_err());
}

#[test]
fn audit1031_mvi_decimal_rounding() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_test_engine(dir.path());
    e.execute(
        "CREATE TABLE md (id INT PRIMARY KEY,j JSON,KEY k ((CAST(j AS DECIMAL(65,0) ARRAY))))",
    )
    .unwrap();
    e.execute("INSERT INTO md VALUES (1,'[1e40]')").unwrap();
    e.execute("CREATE TABLE mi (id INT PRIMARY KEY,j JSON,KEY k ((CAST(j AS UNSIGNED ARRAY))))")
        .unwrap();
    e.execute("INSERT INTO mi VALUES (1,'[1.5]')").unwrap();
}

#[test]
fn audit1031_functional_qualifier_and_echo() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE fq (id INT PRIMARY KEY,s VARCHAR(100),INDEX k ((LOWER(s))))")
        .unwrap();
    e.execute("INSERT INTO fq VALUES (1,'Alice')").unwrap();
    let plan = explain_analyze_tree(
        &e,
        "SELECT id FROM fq f FORCE INDEX(k) WHERE LOWER(f.s)='alice'",
    );
    assert!(plan.to_lowercase().contains("lookup"), "{plan}");
    e.execute("CREATE TABLE fe (s VARCHAR(100),INDEX k ((CONCAT(_utf8mb4'p-',s))))")
        .unwrap();
    let ddl = strings(&e, "SHOW CREATE TABLE fe");
    assert!(!ddl[0][1].contains("CONVERT_USING"), "{ddl:?}");
}

#[test]
fn audit1031_json_nested_hash_keys_and_grouping() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE jh (id INT PRIMARY KEY,j JSON)")
        .unwrap();
    e.execute(
        r#"INSERT INTO jh VALUES (1,'[1,{"x":0}]'),(2,'[1.0,{"x":-0.0}]'),(3,'["1",{"x":0}]')"#,
    )
    .unwrap();
    assert_eq!(
        strings(&e, "SELECT COUNT(DISTINCT j) FROM jh"),
        vec![vec!["2"]]
    );
    assert_eq!(
        strings(&e, "SELECT COUNT(*) FROM jh a JOIN jh b ON a.j=b.j"),
        vec![vec!["5"]]
    );
    assert_eq!(
        strings(&e, "SELECT COUNT(*) FROM (SELECT j FROM jh GROUP BY j) s"),
        vec![vec!["2"]]
    );
    let a = Value::Json(crate::engine::json::parse_text("[1,0]", 1, "test").unwrap());
    let b = Value::Json(crate::engine::json::parse_text("[1.0,-0.0]", 1, "test").unwrap());
    assert_eq!(
        crate::engine::expr::ValueKey::from(&a),
        crate::engine::expr::ValueKey::from(&b)
    );
    let mut ka = crate::engine::expr::InlineKeyBuf::new();
    let mut kb = ka.clone();
    crate::engine::expr::write_value_inline_key(&a, &mut ka);
    crate::engine::expr::write_value_inline_key(&b, &mut kb);
    assert_eq!(ka, kb);
}

#[test]
fn audit1031_decimal_boundaries_and_error_policies() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_test_engine(dir.path());
    for (number, expected) in [
        ("-1e40", format!("-1{}", "0".repeat(40))),
        ("1e70", "9".repeat(65)),
    ] {
        assert_eq!(
            strings(
                &e,
                &format!("SELECT CAST(CAST('{number}' AS JSON) AS DECIMAL(65,0))")
            ),
            vec![vec![expected]]
        );
    }
    assert_eq!(
        strings(
            &e,
            "SELECT JSON_VALUE('18446744073709551615','$' RETURNING SIGNED DEFAULT '7' ON ERROR)"
        ),
        vec![vec!["7"]]
    );
    assert!(e
        .execute("SELECT JSON_VALUE('18446744073709551615','$' RETURNING SIGNED ERROR ON ERROR)")
        .is_err());
    assert_eq!(
        strings(
            &e,
            "SELECT JSON_VALUE('18446744073709551615','$' RETURNING UNSIGNED)"
        ),
        vec![vec!["18446744073709551615"]]
    );
    for text in ["-0.0", "0e0"] {
        assert_eq!(
            strings(&e, &format!("SELECT JSON_TYPE('{text}')")),
            vec![vec!["DOUBLE"]]
        );
    }
    for doc in [
        format!("{}1{}", "[".repeat(101), "]".repeat(101)),
        format!("{}{}", "[".repeat(101), "]".repeat(101)),
    ] {
        assert!(e.execute(&format!("SELECT JSON_DEPTH('{doc}')")).is_err());
    }
}

#[test]
fn audit1031_functional_string_numeric_and_binary_collation() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_test_engine(dir.path());
    e.execute(
        "CREATE TABLE sn (id INT PRIMARY KEY,a VARCHAR(30),b VARCHAR(30), UNIQUE KEY k ((a+b)))",
    )
    .unwrap();
    e.execute("INSERT INTO sn VALUES (1,'0.5','0'),(2,'0.9','0'),(3,'2.4','0')")
        .unwrap();
    assert_eq!(
        strings(&e, "SELECT id FROM sn FORCE INDEX(k) WHERE a+b>2.0"),
        vec![vec!["3"]]
    );
    e.execute("CREATE TABLE cb (id INT PRIMARY KEY,s VARCHAR(30) COLLATE utf8mb4_bin,UNIQUE KEY k ((CONCAT(s,''))))").unwrap();
    e.execute("INSERT INTO cb VALUES (1,'ABC'),(2,'abc')")
        .unwrap();
    assert_eq!(
        strings(
            &e,
            "SELECT id FROM cb FORCE INDEX(k) WHERE CONCAT(s,'')='abc'"
        ),
        vec![vec!["2"]]
    );
}

#[test]
fn audit1031_functional_restart_and_mutation() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_relaxed_test_engine(dir.path());
    e.execute("CREATE TABLE fr (id INT PRIMARY KEY,s VARCHAR(30),a DECIMAL(12,2),UNIQUE KEY k ((CONCAT(s,''))),KEY n ((a+0.01)))").unwrap();
    e.execute("INSERT INTO fr VALUES (1,'Alice',2.40)").unwrap();
    e.shutdown().unwrap();
    drop(e);
    let e = open_relaxed_test_engine(dir.path());
    assert_eq!(
        strings(
            &e,
            "SELECT id FROM fr FORCE INDEX(k) WHERE CONCAT(s,'')='ALICE'"
        ),
        vec![vec!["1"]]
    );
    assert_eq!(
        strings(&e, "SELECT id FROM fr FORCE INDEX(n) WHERE a+0.01>2.0"),
        vec![vec!["1"]]
    );
    assert!(e.execute("INSERT INTO fr VALUES (2,'alice',0.00)").is_err());
    e.execute("UPDATE fr SET s='Bob',a=3.50 WHERE id=1")
        .unwrap();
    assert_eq!(
        strings(
            &e,
            "SELECT id FROM fr FORCE INDEX(k) WHERE CONCAT(s,'')='BOB'"
        ),
        vec![vec!["1"]]
    );
    e.execute("DELETE FROM fr WHERE id=1").unwrap();
    assert!(strings(&e, "SELECT id FROM fr FORCE INDEX(n) WHERE a+0.01>2.0").is_empty());
}

#[test]
fn audit1031_close_session_releases_quota_once() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_test_engine(dir.path());
    let account = crate::engine::auth::AccountKey::new("audit_quota_once", "localhost");
    crate::engine::auth::charge_new_connection(&account, 0, 2).unwrap();
    crate::engine::auth::charge_new_connection(&account, 0, 2).unwrap();
    let mut s = SessionContext::default();
    s.authenticated_as = account.clone();
    e.close_session(&mut s).unwrap();
    e.close_session(&mut s).unwrap();
    crate::engine::auth::charge_new_connection(&account, 0, 2).unwrap();
    assert!(crate::engine::auth::charge_new_connection(&account, 0, 2).is_err());
    crate::engine::auth::release_connection(&account);
    crate::engine::auth::release_connection(&account);
}

#[test]
fn audit1031_wide_json_integer_decimal_round_trip() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_test_engine(dir.path());
    let number = "12345678901234567890123456789012345678901234567890123456789012345";
    assert_eq!(
        strings(
            &e,
            &format!("SELECT CAST(CAST('{number}' AS JSON) AS DECIMAL(65,0))")
        ),
        vec![vec![number]]
    );
    let next = "12345678901234567890123456789012345678901234567890123456789012346";
    for function in ["JSON_CONTAINS", "JSON_OVERLAPS"] {
        assert_eq!(
            strings(&e, &format!("SELECT {function}('[{number}]','{next}')")),
            vec![vec!["0"]]
        );
        assert_eq!(
            strings(&e, &format!("SELECT {function}('[{number}]','{number}')")),
            vec![vec!["1"]]
        );
    }
    e.execute(
        "CREATE TABLE mw (id INT PRIMARY KEY,j JSON, KEY k ((CAST(j AS DECIMAL(65,0) ARRAY))))",
    )
    .unwrap();
    e.execute(&format!(
        "INSERT INTO mw VALUES (1,'[{number}]'),(2,'[{}]')",
        "9".repeat(65)
    ))
    .unwrap();
    let sql = format!(
        "SELECT id FROM mw FORCE INDEX(k) WHERE CAST('{number}' AS DECIMAL(65,0)) MEMBER OF(j)"
    );
    assert_eq!(strings(&e, &sql), vec![vec!["1"]]);
}
