use super::*;

#[test]
fn xml_functions_mysql80_examples() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let cases = [
        (
            "SELECT EXTRACTVALUE('<a><b>X</b><b>Y</b></a>','//b')",
            "X Y",
        ),
        ("SELECT EXTRACTVALUE('<a>x<b>y</b>z</a>','/a')", "x z"),
        ("SELECT EXTRACTVALUE('<a><b/></a>','count(/a/b)')", "1"),
        (
            "SELECT UPDATEXML('<a><b>x</b></a>','//b','<c/>')",
            "<a><c/></a>",
        ),
        (
            "SELECT UPDATEXML('<a><b/><b/></a>','//b','bad')",
            "<a><b/><b/></a>",
        ),
    ];
    for (sql, expected) in cases {
        assert_eq!(
            super::support::query_one_row(&engine, sql),
            vec![Value::String(expected.into())],
            "{sql}"
        );
    }
}

#[test]
fn xml_functions_mysql80_captured_oracle() {
    use serde_json::{json, Value as Json};
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let fixture: Json = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/xml-functions-mysql80.json"
    ))
    .unwrap();
    for sql in fixture["session"].as_array().unwrap() {
        engine
            .execute_with_session(&mut session, sql.as_str().unwrap())
            .unwrap();
    }
    let mut differences = Vec::new();
    for case in fixture["cases"].as_array().unwrap() {
        for sql in case["setup"].as_array().into_iter().flatten() {
            engine
                .execute_with_session(&mut session, sql.as_str().unwrap())
                .unwrap();
        }
        let sql = case["sql"].as_str().unwrap();
        let outcome = engine
            .execute_with_session(&mut session, sql)
            .and_then(QueryResponse::materialize);
        let expected = &case["expected"];
        let actual = match outcome {
            Ok(QueryResponse::ResultSet(result)) => {
                let rows: Vec<Vec<Json>> = result.rows.iter().map(|row| row.iter().map(|v| {
                    if matches!(v, Value::Null) { return Json::Null; }
                    match v {Value::Double(n)=>return Json::String(format!("{n:?}")),Value::Float(n)=>return Json::String(format!("{n:?}")),_=>{}}

                    let bytes = match v { Value::Bytes(b) => b.clone(), _ => v.to_display_string().into_bytes() };
                    json!({"hex": bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()})
                }).collect()).collect();
                json!({"status":"ok", "rows":rows})
            }
            Ok(_) => json!({"status":"other"}),
            Err(error) => {
                json!({"status":"error", "errno":error.code(), "sqlstate":error.sql_state().as_str(), "message":error.message()})
            }
        };
        let QueryResponse::ResultSet(warnings) = engine
            .execute_with_session(&mut session, "SHOW WARNINGS")
            .unwrap()
        else {
            panic!("warnings result");
        };
        let warnings: Vec<_> = warnings
            .rows
            .iter()
            .map(|row| {
                json!([
                    row[0].to_display_string(),
                    row[1].to_display_string().parse::<u16>().unwrap(),
                    row[2].to_display_string()
                ])
            })
            .collect();
        let expected_outcome = if expected["status"] == "ok" {
            json!({"status":"ok","rows":expected["rows"]})
        } else {
            json!({"status":"error","errno":expected["errno"],"sqlstate":expected["sqlstate"],"message":expected["message"]})
        };
        if actual != expected_outcome || json!(warnings) != expected["warnings"] {
            differences.push(json!({"id":case["id"],"sql":sql,"expected":expected_outcome,"actual":actual,"expected_warnings":expected["warnings"],"actual_warnings":warnings}));
        }
    }
    let path = "/tmp/mydb/xml-engine-differences.json";
    std::fs::write(path, serde_json::to_string_pretty(&differences).unwrap()).unwrap();
    assert!(
        differences.is_empty(),
        "{} XML oracle differences; see {path}. First: {}",
        differences.len(),
        differences.first().unwrap_or(&Json::Null)
    );
}

#[test]
fn xml_functions_columns_selection_and_dml() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE xd(id INT PRIMARY KEY,doc TEXT)")
        .unwrap();
    engine.execute("INSERT INTO xd VALUES(1,'<a><b>X</b></a>'),(2,'<broken'),(3,'<a><b>Y</b><b>Z</b></a>'),(4,NULL)").unwrap();
    let expected = vec![
        vec![
            Value::Int(1),
            Value::String("X".into()),
            Value::String("<a><r/></a>".into()),
        ],
        vec![
            Value::Int(3),
            Value::String("Y Z".into()),
            Value::String("<a><b>Y</b><b>Z</b></a>".into()),
        ],
        vec![Value::Int(4), Value::Null, Value::Null],
    ];
    for mode in [
        crate::engine::executor::vec::config::VecMode::Off,
        crate::engine::executor::vec::config::VecMode::On,
    ] {
        crate::engine::executor::vec::config::with_vec_mode(mode, || {
            assert_eq!(super::support::query_rows(&engine,"SELECT id,EXTRACTVALUE(doc,'//b'),UPDATEXML(doc,'//b','<r/>') FROM xd WHERE id<>2 ORDER BY id"),expected);
            assert!(super::support::query_rows(&engine, "SHOW WARNINGS").is_empty());
            assert_eq!(
                super::support::query_rows(
                    &engine,
                    "SELECT IF(id=1,EXTRACTVALUE(doc,'/a/b'),NULL) FROM xd ORDER BY id"
                ),
                vec![
                    vec![Value::String("X".into())],
                    vec![Value::Null],
                    vec![Value::Null],
                    vec![Value::Null]
                ]
            );
            assert!(super::support::query_rows(&engine, "SHOW WARNINGS").is_empty());
        });
    }
    engine
        .execute("CREATE TABLE xo(id INT PRIMARY KEY,v TEXT)")
        .unwrap();
    engine
        .execute("INSERT INTO xo SELECT id,EXTRACTVALUE(doc,'//b') FROM xd WHERE id<>2")
        .unwrap();
    assert_eq!(
        super::support::query_one_row(&engine, "SELECT v FROM xo WHERE id=3"),
        vec![Value::String("Y Z".into())]
    );
    engine
        .execute("UPDATE xd SET doc=UPDATEXML(doc,'/a/b','<c>changed</c>') WHERE id=1")
        .unwrap();
    assert_eq!(
        super::support::query_one_row(&engine, "SELECT EXTRACTVALUE(doc,'//c') FROM xd WHERE id=1"),
        vec![Value::String("changed".into())]
    );
    engine
        .execute("CREATE VIEW xv AS SELECT id,EXTRACTVALUE(doc,'//b') AS v FROM xd WHERE id<>2")
        .unwrap();
    assert_eq!(
        super::support::query_one_row(&engine, "SELECT v FROM xv WHERE id=3"),
        vec![Value::String("Y Z".into())]
    );
}

#[test]
fn xml_functions_bind_constant_paths_and_rebind_prepared_parameters() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE xe(doc TEXT,path TEXT)")
        .unwrap();
    for expression in ["EXTRACTVALUE(doc,path)", "UPDATEXML(doc,path,'new')"] {
        for prefix in ["SELECT", "EXPLAIN SELECT"] {
            let error = engine
                .execute_with_session(&mut session, &format!("{prefix} {expression} FROM xe"))
                .unwrap_err();
            assert_eq!(
                (error.code(), error.message()),
                (1105, "Only constant XPATH queries are supported")
            );
        }
    }
    engine
        .execute_with_session(&mut session, "PREPARE xp FROM 'SELECT EXTRACTVALUE(?,?)'")
        .unwrap();
    for (xml, path, value) in [
        ("<a><b>X</b></a>", "//b", "X"),
        ("<a/>", "count(//b)", "0"),
        ("<a>next</a>", "/a", "next"),
    ] {
        engine
            .execute_with_session(&mut session, &format!("SET @xml='{xml}',@path='{path}'"))
            .unwrap();
        let result = engine
            .execute_with_session(&mut session, "EXECUTE xp USING @xml,@path")
            .and_then(QueryResponse::materialize)
            .unwrap();
        assert_eq!(
            super::support::result_rows(result),
            vec![vec![Value::String(value.into())]]
        );
    }
    for (bind, expected_length) in [
        ("SET @xml='<a>x</a>',@path='/a'", 67_108_864),
        ("SET @xml=NULL,@path=NULL", 67_108_864),
        ("SET @xml=1,@path=1", 16_777_216),
        ("SET @xml=NULL,@path=NULL", 16_777_216),
        ("SET @xml='<a>x</a>',@path='/a'", 16_777_216),
    ] {
        engine.execute_with_session(&mut session, bind).unwrap();
        let QueryResponse::ResultSet(result) = engine
            .execute_with_session(&mut session, "EXECUTE xp USING @xml,@path")
            .and_then(QueryResponse::materialize)
            .unwrap()
        else {
            panic!("prepared result")
        };
        assert_eq!(
            result.columns[0].source.length,
            Some(expected_length),
            "{bind}"
        );
        assert_ne!(result.columns[0].source.collation_id, Some(63), "{bind}");
    }
    engine
        .execute_with_session(&mut session, "SET @xml='<a>next</a>'")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET @path='!'")
        .unwrap();
    assert_eq!(
        engine
            .execute_with_session(&mut session, "EXECUTE xp USING @xml,@path")
            .unwrap_err()
            .code(),
        1105
    );
    engine
        .execute_with_session(&mut session, "SET @path='/a'")
        .unwrap();
    assert!(engine
        .execute_with_session(&mut session, "EXECUTE xp USING @xml,@path")
        .is_ok());
}

#[test]
fn xml_functions_routine_scope_and_user_variable_cache() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine.execute_with_session(&mut session,"CREATE FUNCTION xml_pick(i INT) RETURNS TEXT DETERMINISTIC RETURN EXTRACTVALUE('<a><b>X</b><b>Y</b></a>','//b[$i]')").unwrap();
    engine.execute_with_session(&mut session,"CREATE FUNCTION xml_shadow(i INT) RETURNS TEXT DETERMINISTIC BEGIN DECLARE v TEXT; BEGIN DECLARE i INT DEFAULT 1; SET v=EXTRACTVALUE('<a><b>X</b><b>Y</b></a>','//b[$i]'); END; RETURN CONCAT(v,xml_pick(i)); END").unwrap();
    for i in [1, 2, 1, 2] {
        let result = engine
            .execute_with_session(
                &mut session,
                &format!("SELECT xml_pick({i}),xml_shadow({i})"),
            )
            .and_then(QueryResponse::materialize)
            .unwrap();
        let expected = if i == 1 { "X" } else { "Y" };
        assert_eq!(
            super::support::result_rows(result),
            vec![vec![
                Value::String(expected.into()),
                Value::String(format!("X{expected}"))
            ]]
        );
    }
    let error = engine
        .execute_with_session(&mut session, "SELECT EXTRACTVALUE('<a/>','//b[$i]')")
        .unwrap_err();
    assert_eq!(error.code(), 1105);
    assert!(error.message().starts_with("Unknown XPATH variable"));
    for (value, expected) in [("1", "X"), ("2", "Y"), ("'string'", ""), ("NULL", "")] {
        engine
            .execute_with_session(&mut session, &format!("SET @i={value}"))
            .unwrap();
        let result = engine
            .execute_with_session(
                &mut session,
                "SELECT EXTRACTVALUE('<a><b>X</b><b>Y</b></a>','//b[$@i]')",
            )
            .and_then(QueryResponse::materialize)
            .unwrap();
        assert_eq!(
            super::support::result_rows(result),
            vec![vec![Value::String(expected.into())]]
        );
        assert!(super::support::result_rows(
            engine
                .execute_with_session(&mut session, "SHOW WARNINGS")
                .unwrap()
        )
        .is_empty());
    }
}

#[test]
fn xml_functions_cancel_and_recover() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET max_execution_time=1")
        .unwrap();
    let error = engine
        .execute_with_session(
            &mut session,
            "SELECT EXTRACTVALUE(CONCAT('<a>',REPEAT('<b/>',100000),'</a>'),'count(//b[true()])')",
        )
        .and_then(QueryResponse::materialize)
        .unwrap_err();
    assert_eq!(error.code(), 3024);
    engine
        .execute_with_session(&mut session, "SET max_execution_time=0")
        .unwrap();
    assert_eq!(
        super::support::result_rows(
            engine
                .execute_with_session(&mut session, "SELECT EXTRACTVALUE('<a>ok</a>','/a')")
                .unwrap()
        ),
        vec![vec![Value::String("ok".into())]]
    );
}
