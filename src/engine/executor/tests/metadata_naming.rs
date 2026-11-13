use super::*;

fn tempdir() -> tempfile::TempDir {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    tempfile::tempdir_in("/tmp/mydb").unwrap()
}

fn query(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> ResultSet {
    let QueryResponse::ResultSet(result) =
        support::execute_materialized(engine, session, sql).unwrap()
    else {
        panic!("expected result set: {sql}");
    };
    result
}

fn names(columns: &[Column]) -> Vec<&str> {
    columns.iter().map(|column| column.name.as_str()).collect()
}

fn assert_projection_names(
    engine: &StorageQueryEngine,
    session: &mut SessionContext,
    sql: &str,
    expected: &[&str],
) {
    let result = query(engine, session, sql);
    assert_eq!(names(&result.columns), expected, "{sql}");
    let described = engine
        .describe_prepared_statement(session, sql)
        .unwrap_or_else(|error| panic!("PREPARE {sql}: {error:?}"));
    assert_eq!(names(&described), expected, "PREPARE {sql}");
    let statement = crate::parser::parse_statement(sql).unwrap();
    let response = QueryEngine::execute_prepared_statement(engine, session, sql, statement)
        .unwrap()
        .unwrap()
        .materialize()
        .unwrap();
    let QueryResponse::ResultSet(result) = response else {
        panic!("{sql}")
    };
    assert_eq!(names(&result.columns), expected, "EXECUTE {sql}");
}

#[test]
fn metadata_naming_computed_columns_retain_metadata_provenance() {
    let dir = tempdir();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute("CREATE TABLE review_user (UserValue INT)")
        .unwrap();
    engine
        .execute("INSERT INTO review_user VALUES (1)")
        .unwrap();
    engine.execute("CREATE VIEW review_meta AS SELECT UPPER(TABLE_NAME) AS MixedAlias FROM information_schema.TABLES").unwrap();
    for limit in [0, 1] {
        for (body, expected) in [
            ("SELECT x.mixedalias FROM (SELECT UPPER(TABLE_NAME) AS MixedAlias FROM information_schema.TABLES) x", vec!["MixedAlias"]),
            ("WITH x AS (SELECT UPPER(TABLE_NAME) AS MixedAlias FROM information_schema.TABLES) SELECT mixedalias FROM x", vec!["MixedAlias"]),
            ("WITH x(DeclaredAlias) AS (SELECT COALESCE(TABLE_NAME,'') FROM information_schema.TABLES) SELECT declaredalias FROM x", vec!["DeclaredAlias"]),
            ("SELECT x.rowcount FROM (SELECT COUNT(*) AS RowCount FROM information_schema.TABLES) x", vec!["RowCount"]),
            ("SELECT x.rownum FROM (SELECT ROW_NUMBER() OVER (ORDER BY TABLE_NAME) AS RowNum FROM information_schema.TABLES) x", vec!["RowNum"]),
            ("SELECT mixedalias FROM review_meta", vec!["MixedAlias"]),
            ("SELECT x.uservalue, x.mixedalias FROM (SELECT u.uservalue, UPPER(t.TABLE_NAME) AS MixedAlias FROM review_user u JOIN information_schema.TABLES t ON 1=1) x", vec!["uservalue", "MixedAlias"]),
            ("WITH x AS (SELECT u.uservalue, UPPER(t.TABLE_NAME) AS MixedAlias FROM review_user u JOIN information_schema.TABLES t ON 1=1) SELECT USERVALUE,mixedalias FROM x", vec!["USERVALUE", "MixedAlias"]),
            ("WITH x(MixedAlias) AS (SELECT 1) SELECT mixedalias FROM x", vec!["mixedalias"]),
        ] {
            assert_projection_names(&engine, &mut session, &format!("{body} LIMIT {limit}"), &expected);
        }
    }
}

#[test]
fn metadata_naming_user_tables_in_mysql_keep_query_spelling() {
    let dir = tempdir();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute("CREATE TABLE mysql.review_user (MixedColumn INT)")
        .unwrap();
    engine
        .execute("INSERT INTO mysql.review_user VALUES (1)")
        .unwrap();
    for name in ["mixedcolumn", "MIXEDCOLUMN", "MixedColumn"] {
        for limit in [0, 1] {
            let sql = format!("SELECT {name} FROM mysql.review_user LIMIT {limit}");
            assert_projection_names(&engine, &mut session, &sql, &[name]);
        }
    }
    assert_projection_names(&engine, &mut session, "SELECT u.mixedcolumn,t.table_name FROM mysql.review_user u JOIN information_schema.TABLES t ON 1=1 LIMIT 0", &["mixedcolumn", "TABLE_NAME"]);
    assert!(engine
        .describe_prepared_statement(&mut session, "SELECT * FROM mysql.table_dynamic_metadata")
        .is_err());
}

#[test]
fn metadata_naming_projection_preserves_bound_names_and_aliases() {
    let dir = tempdir();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for (sql, expected) in [
        ("SELECT table_name FROM information_schema.tables LIMIT 1", vec!["TABLE_NAME"]),
        ("SELECT t.table_name, t.TABLE_NAME AS UserAlias FROM information_schema.tables t LIMIT 0", vec!["TABLE_NAME", "UserAlias"]),
        ("SELECT HOST, user FROM mysql.user LIMIT 0", vec!["Host", "User"]),
        ("SELECT TRX_ID FROM information_schema.innodb_trx LIMIT 0", vec!["trx_id"]),
        ("SELECT variable_NAME, TYPE FROM sys.metrics LIMIT 0", vec!["Variable_name", "Type"]),
        ("SELECT processlist_user FROM performance_schema.threads LIMIT 0", vec!["PROCESSLIST_USER"]),
        ("SELECT table_name, ROW_NUMBER() OVER (ORDER BY TABLE_NAME) AS RowNum FROM information_schema.tables LIMIT 0", vec!["TABLE_NAME", "RowNum"]),
        ("SELECT d.table_name FROM (SELECT table_name FROM information_schema.tables) d LIMIT 1", vec!["TABLE_NAME"]),
        ("SELECT d.inneralias FROM (SELECT table_name AS InnerAlias FROM information_schema.tables) d LIMIT 0", vec!["InnerAlias"]),
        ("WITH d AS (SELECT table_name FROM information_schema.tables) SELECT table_name FROM d LIMIT 1", vec!["TABLE_NAME"]),
        ("WITH d(DeclaredName) AS (SELECT table_name FROM information_schema.tables) SELECT declaredname FROM d LIMIT 0", vec!["DeclaredName"]),
        ("SELECT table_name FROM information_schema.tables WHERE 0 UNION ALL SELECT table_name FROM information_schema.tables LIMIT 0", vec!["TABLE_NAME"]),
        ("SELECT t.table_name, c.column_name FROM information_schema.tables t JOIN information_schema.columns c ON t.table_schema=c.table_schema AND t.table_name=c.table_name LIMIT 0", vec!["TABLE_NAME", "COLUMN_NAME"]),
        ("SELECT table_name FROM information_schema.tables t JOIN information_schema.columns c USING (table_name) LIMIT 0", vec!["TABLE_NAME"]),
    ] {
        let result = query(&engine, &mut session, sql);
        assert_eq!(names(&result.columns), expected, "{sql}");
        let described = engine.describe_prepared_statement(&mut session, sql).unwrap();
        assert_eq!(names(&described), expected, "PREPARE {sql}");
        let statement = crate::parser::parse_statement(sql).unwrap();
        let prepared = QueryEngine::execute_prepared_statement(&engine, &mut session, sql, statement)
            .unwrap().unwrap().materialize().unwrap();
        let QueryResponse::ResultSet(prepared) = prepared else { panic!("{sql}") };
        assert_eq!(names(&prepared.columns), expected, "EXECUTE {sql}");
    }
}

#[test]
fn metadata_naming_all_registered_fields_match_query_show_and_prepare() {
    let dir = tempdir();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for def in
        crate::engine::information_schema::system_table_definitions().filter(|def| !def.hidden)
    {
        let table = format!(
            "{}.{}",
            def.schema.to_ascii_uppercase(),
            def.name.to_ascii_lowercase()
        );
        let expected = def
            .columns
            .iter()
            .map(|column| column.name)
            .collect::<Vec<_>>();
        let sql = format!("SELECT * FROM {table} LIMIT 0");
        let actual = query(&engine, &mut session, &sql);
        assert_eq!(names(&actual.columns), expected, "{table}");
        assert_eq!(
            names(
                &engine
                    .describe_prepared_statement(&mut session, &sql)
                    .unwrap()
            ),
            expected,
            "PREPARE {table}"
        );
        let projection = def
            .columns
            .iter()
            .map(|column| format!("m.`{}`", column.name.to_ascii_lowercase()))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!("SELECT {projection} FROM {table} m LIMIT 0");
        let actual = query(&engine, &mut session, &sql);
        assert_eq!(names(&actual.columns), expected, "explicit {table}");
        let described = engine
            .describe_prepared_statement(&mut session, &sql)
            .unwrap();
        assert_eq!(names(&described), expected, "PREPARE explicit {table}");
        for (actual, column) in actual.columns.iter().zip(def.columns) {
            assert_eq!(actual.source.org_name, column.name, "origin {table}");
            assert_eq!(actual.source.table, "m", "table alias {table}");
        }
        assert!(
            described.iter().all(|column| column.source.table == "m"),
            "prepared table alias {table}"
        );
        let show = query(
            &engine,
            &mut session,
            &format!("SHOW FULL COLUMNS FROM {table}"),
        );
        assert_eq!(
            show.rows
                .iter()
                .map(|row| row[0].to_display_string())
                .collect::<Vec<_>>(),
            expected
        );
        let packets = engine.field_list_columns(&session, &table, "").unwrap();
        let packet_names = packets
            .iter()
            .map(|packet| {
                let mut offset = 0;
                let mut strings = Vec::new();
                for _ in 0..6 {
                    let length = packet[offset] as usize;
                    assert!(length < 251);
                    offset += 1;
                    strings.push(std::str::from_utf8(&packet[offset..offset + length]).unwrap());
                    offset += length;
                }
                assert_eq!(strings[4], strings[5], "COM_FIELD_LIST origin {table}");
                strings[4]
            })
            .collect::<Vec<_>>();
        assert_eq!(packet_names, expected, "COM_FIELD_LIST {table}");
    }
}

#[test]
fn metadata_naming_new_names_are_exclusive_and_have_comments() {
    let dir = tempdir();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for old in [
        "AI_MODELS",
        "AI_CONNECTIONS",
        "AI_SECRETS",
        "AI_RUNTIME_STATUS",
        "MYDB_PLAN_CACHE",
        "MYDB_XA_RECOVER",
    ] {
        let sql = format!("SELECT * FROM information_schema.{old}");
        assert_eq!(
            support::execute_materialized(&engine, &mut session, &sql)
                .unwrap_err()
                .code(),
            1146,
            "{sql}"
        );
    }
    let columns = query(&engine, &mut session, "SELECT TABLE_NAME,COLUMN_NAME,COLUMN_COMMENT FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='information_schema' AND TABLE_NAME LIKE 'MYDB\\_%'");
    assert!(!columns.rows.is_empty());
    assert!(columns
        .rows
        .iter()
        .all(|row| !row[2].to_display_string().is_empty()));
    let exact = query(&engine, &mut session, "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA='information_schema' AND BINARY TABLE_NAME='MYDB_CACHES'");
    assert_eq!(exact.rows, vec![vec![Value::String("MYDB_CACHES".into())]]);
    assert!(query(&engine, &mut session, "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA='information_schema' AND BINARY TABLE_NAME='mydb_caches'").rows.is_empty());
}

#[test]
fn metadata_naming_internal_dictionary_names_and_identities() {
    let mut tables = std::collections::BTreeSet::new();
    for table in crate::storage::data_dictionary::dd_tables() {
        assert!(tables.insert(table.name.to_ascii_lowercase()));
        assert!(table
            .name
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_'));
        let mut names = std::collections::BTreeSet::new();
        for column in &table.columns {
            assert!(names.insert(column.name.to_ascii_lowercase()));
            assert!(column
                .name
                .bytes()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_'));
        }
        for key in &table.primary_key {
            assert!(table.column_index(key).is_ok(), "{}.{}", table.name, key);
        }
    }
    use crate::storage::data_dictionary::*;
    assert_eq!(DD_GRANTS, DD_TABLE_ID_BASE + 20);
    assert_eq!(
        dd_table_schema(DD_TABLE_DYNAMIC_METADATA).unwrap().name,
        "table_dynamic_metadata"
    );
    assert_eq!(
        dd_table_schema(DD_DDL_RETIREMENTS).unwrap().name,
        "ddl_retirements"
    );
    assert_eq!(
        dd_table_schema(DD_FOREIGN_KEY_COLUMN_USAGE)
            .unwrap()
            .primary_key,
        vec!["table_id", "foreign_key_name", "ordinal_position"]
    );
}
