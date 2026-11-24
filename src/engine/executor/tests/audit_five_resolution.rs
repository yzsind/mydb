use super::*;

#[test]
fn audit_five_malformed_case_is_linear_and_valid_case_survives() {
    let started = std::time::Instant::now();
    for context in ["SELECT ", "SELECT 1 WHERE ", "DO "] {
        for depth in [18, 24, 30, 40] {
            let malformed = format!("{context}{}1 END", "CASE WHEN 1 THEN ".repeat(depth));
            assert!(crate::parser::parse_statement(&malformed).is_err());
        }
    }
    assert!(started.elapsed() < Duration::from_secs(2));
    let valid = format!(
        "SELECT {}1{}",
        "CASE WHEN 1 THEN ".repeat(24),
        " END".repeat(24)
    );
    crate::parser::parse_statement(&valid).unwrap();
    crate::parser::parse_statement(
        "SELECT CASE WHEN TRUE THEN 'a' ELSE 'b' END COLLATE utf8mb4_bin",
    )
    .unwrap();
    crate::parser::parse_statement(
        "SELECT `case`, 'CASE WHEN END', CASE 1 WHEN 1 THEN 2 ELSE 3 END FROM t",
    )
    .unwrap();
}

#[test]
fn audit_five_exact_literals_keep_scratch_precision() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    let integer = format!("1{}", "0".repeat(65));
    assert_eq!(
        rows(
            &engine,
            &format!("SELECT CAST('{}' AS DECIMAL(65,0))+1", "9".repeat(65))
        ),
        vec![vec![integer.clone()]]
    );
    assert_eq!(
        rows(
            &engine,
            &format!("SELECT {integer},{integer}+0,{integer}*1")
        ),
        vec![vec![integer.clone(), integer.clone(), integer.clone()]]
    );
    assert_eq!(
        rows(&engine, &format!("SELECT {integer}/2")),
        vec![vec![format!("5{}.0000", "0".repeat(64))]]
    );
    let fraction = format!("1.{}1", "0".repeat(59));
    assert_eq!(
        rows(&engine, &format!("SELECT {fraction},{fraction}+0")),
        vec![vec![fraction.clone(), fraction]]
    );
    let fraction = format!("0.{}", "1234567890".repeat(8));
    assert_eq!(
        rows(&engine, &format!("SELECT {fraction}")),
        vec![vec![fraction[..74].to_string()]]
    );
    engine
        .execute("CREATE TABLE exact_storage(v DECIMAL(65,30))")
        .unwrap();
    assert!(engine
        .execute(&format!("INSERT INTO exact_storage VALUES({integer})"))
        .is_err());
}

#[test]
fn audit_five_projection_names_keep_original_tokens() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT 1e0,0.1e0,1.5E2,'a' RLIKE '^a'")
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(
        result
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        vec!["1e0", "0.1e0", "1.5E2", "'a' RLIKE '^a'"]
    );
}

#[test]
fn audit_five_temporal_precision_and_year_zero_difference() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE typed_intervals(d DATE,n DECIMAL(4,1))")
        .unwrap();
    engine
        .execute("INSERT INTO typed_intervals VALUES(DATE '2024-01-01',1.5)")
        .unwrap();
    let QueryResponse::ResultSet(typed) = engine.execute("SELECT DATE_ADD(d,INTERVAL n SECOND),DATE_ADD(DATE '2024-01-01',INTERVAL 1.5 SECOND) FROM typed_intervals").unwrap().materialize().unwrap() else { panic!() };
    assert_eq!(
        typed
            .columns
            .iter()
            .map(|column| column.source.decimals)
            .collect::<Vec<_>>(),
        vec![Some(1), Some(1)]
    );
    assert!(typed.rows[0]
        .iter()
        .all(|value| value.to_display_string() == "2024-01-01 00:00:01.5"));
    assert_eq!(rows(&engine,"SELECT DATE_ADD('2024-01-01',INTERVAL 1.5 SECOND),DATE_ADD('2024-01-01',INTERVAL 1.7 MINUTE),DATE_ADD('2024-01-01 00:00:00.1',INTERVAL 1 DAY),DATEDIFF('0000-01-01','0000-01-02')"),vec![vec!["2024-01-01 00:00:01.500000","2024-01-01 00:02:00","2024-01-02 00:00:00.100000","-1"]]);
    let value = crate::engine::expr::eval_negate(Value::String("abc".into())).unwrap();
    assert!(matches!(value,Value::Double(n) if n==0.0 && n.is_sign_negative()));
}

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<String>> {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap().materialize().unwrap()
    else {
        panic!("expected rows: {sql}");
    };
    result
        .rows
        .iter()
        .map(|row| row.iter().map(Value::to_display_string).collect())
        .collect()
}

#[test]
fn audit_five_dirty_reads_ignore_pending_zone_bounds() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE dirty_zone(id INT PRIMARY KEY,v INT) ZONE_SIZE=65536")
        .unwrap();
    engine
        .execute("INSERT INTO dirty_zone VALUES(1,1),(2,2)")
        .unwrap();
    engine.execute("ANALYZE TABLE dirty_zone").unwrap();
    let mut writer = SessionContext::default();
    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut writer, "UPDATE dirty_zone SET v=1 WHERE id=1")
        .unwrap();
    let db = engine.db_for_tests();
    let table = db.table("test.dirty_zone").unwrap();
    let txn_id = writer.current_txn.unwrap();
    let mut reader = SessionContext::default();
    engine
        .execute_with_session(
            &mut reader,
            "SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
        )
        .unwrap();
    db.with_deferred_zone_map_updates(txn_id, &table, &[1], || {
        engine.execute_with_session(&mut writer, "UPDATE dirty_zone SET v=999999 WHERE id=1")?;
        let predicate = crate::parser::parse_expr_fragment("v=999999").unwrap();
        let ru = crate::storage::TxnView::read_only(
            db.visible_scn(),
            crate::storage::IsolationLevel::ReadUncommitted,
        );
        let rc = crate::storage::TxnView::read_only(
            db.visible_scn(),
            crate::storage::IsolationLevel::ReadCommitted,
        );
        assert!(
            db.zone_map_candidate_ranges_for_plan(rc, &table, &predicate)
                .is_some_and(|ranges| ranges.is_empty()),
            "fixture must hold stale, trusted bounds"
        );
        assert!(db
            .zone_map_candidate_ranges_for_plan(ru, &table, &predicate)
            .is_none());
        assert!(db
            .zone_map_candidate_ranges_for_storage_for_plan(ru, &table, table.id, &predicate)
            .is_none());
        assert!(db
            .zone_map_candidate_ranges_for_read(ru, &table, &predicate)
            .is_none());
        for sql in [
            "SELECT /*+ ZONE_MAP(dirty_zone) */ id FROM dirty_zone WHERE v=999999",
            "SELECT /*+ ZONE_MAP(dirty_zone) NO_PARALLEL */ id FROM dirty_zone WHERE v=999999",
            "SELECT /*+ ZONE_MAP(dirty_zone) PARALLEL(4) */ id FROM dirty_zone WHERE v=999999",
            "SELECT id FROM dirty_zone WHERE v*1=999999",
        ] {
            let response = engine
                .execute_with_session(&mut reader, sql)?
                .materialize()?;
            let QueryResponse::ResultSet(result) = response else {
                panic!()
            };
            assert_eq!(result.rows, vec![vec![Value::Int(1)]]);
        }
        Ok(())
    })
    .unwrap();
    engine
        .execute_with_session(&mut writer, "ROLLBACK")
        .unwrap();
}

#[test]
fn audit_five_short_circuit_folded_subqueries() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE guard_rows(id INT,w INT)")
        .unwrap();
    engine
        .execute("INSERT INTO guard_rows VALUES(1,10),(2,20)")
        .unwrap();
    for guard in [
        "0=1 AND (SELECT w FROM guard_rows)=5",
        "0 AND (SELECT w FROM guard_rows)",
        "(1=0 AND (SELECT w FROM guard_rows)=5) OR 0",
    ] {
        assert!(rows(&engine, &format!("SELECT id FROM guard_rows WHERE {guard}")).is_empty());
    }
    assert_eq!(
        rows(
            &engine,
            "SELECT id FROM guard_rows WHERE 1=1 OR (SELECT w FROM guard_rows)=5 ORDER BY id"
        ),
        vec![vec!["1"], vec!["2"]]
    );
    assert!(engine
        .execute("SELECT id FROM guard_rows WHERE 1=0 OR (SELECT w FROM guard_rows)=5")
        .and_then(QueryResponse::materialize)
        .is_err());
    assert!(rows(&engine, "SELECT id FROM guard_rows WHERE NULL AND 0=1").is_empty());
}

#[test]
fn audit_five_wide_row_encoding_boundaries() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    for count in [4, 8, 16] {
        let columns = (0..count)
            .map(|i| format!("c{i} VARCHAR(6000)"))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!(
                "CREATE TABLE wide_{count}(id INT PRIMARY KEY,{columns})"
            ))
            .unwrap();
        for delta in 0..12 {
            let len = (16_320 / count) - delta;
            let values = std::iter::repeat_n(format!("REPEAT('x',{len})"), count)
                .collect::<Vec<_>>()
                .join(",");
            engine
                .execute(&format!(
                    "INSERT INTO wide_{count} VALUES({delta},{values})"
                ))
                .unwrap();
            engine
                .execute(&format!(
                    "UPDATE wide_{count} SET c0=REPEAT('y',{}) WHERE id={delta}",
                    len + 8
                ))
                .unwrap();
            assert_eq!(
                rows(
                    &engine,
                    &format!(
                        "SELECT LENGTH(c0),LENGTH(c{}) FROM wide_{count} WHERE id={delta}",
                        count - 1
                    )
                ),
                vec![vec![(len + 8).to_string(), len.to_string()]]
            );
        }
    }
}

#[test]
fn audit_five_format_exact_and_large_double() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(rows(&engine,"SELECT FORMAT(12345678901234567890123456789012345678,2),FORMAT(123456789012345678901234567890123456789,2),FORMAT(1e30,2),FORMAT(-1e30,2)"),vec![vec!["12,345,678,901,234,567,890,123,456,789,012,345,678.00","123,456,789,012,345,678,901,234,567,890,123,456,789.00","1,000,000,000,000,000,000,000,000,000,000.00","-1,000,000,000,000,000,000,000,000,000,000.00"]]);
    assert_eq!(
        rows(
            &engine,
            "SELECT REPLACE(FORMAT(1e308,2),',',''),LEFT(REPLACE(FORMAT('1e309',2),',',''),17)"
        ),
        vec![vec![
            format!("1{}.00", "0".repeat(308)),
            "17976931348623157".into()
        ]]
    );
}

#[test]
fn audit_five_ctas_mixed_setop_domains() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE input_rows(id INT)").unwrap();
    engine.execute("INSERT INTO input_rows VALUES(1)").unwrap();
    engine.execute("CREATE TABLE mixed_dates AS SELECT 1 v FROM input_rows UNION ALL SELECT DATE '2024-01-01' FROM input_rows").unwrap();
    assert_eq!(
        rows(&engine, "SELECT v FROM mixed_dates ORDER BY v"),
        vec![vec!["1"], vec!["2024-01-01"]]
    );
    assert!(matches!(
        engine
            .db_for_tests()
            .table("test.mixed_dates")
            .unwrap()
            .columns[0]
            .data_type,
        DataTypeKind::VarChar { .. }
    ));
    for (i, value, expected) in [
        (0, "123", "123"),
        (1, "1.5", "1.5"),
        (2, "-12", "-12"),
        (3, "DATE '2024-01-01'", "2024-01-01"),
        (4, "1.5e0", "1.5"),
    ] {
        engine.execute(&format!("CREATE TABLE mixed_{i} AS SELECT {value} AS v FROM input_rows UNION ALL SELECT 'xy' FROM input_rows")).unwrap();
        assert_eq!(
            rows(&engine, &format!("SELECT v FROM mixed_{i} ORDER BY v")),
            vec![vec![expected], vec!["xy"]]
        );
        let table = engine
            .db_for_tests()
            .table(&format!("test.mixed_{i}"))
            .unwrap();
        assert!(matches!(
            table.columns[0].data_type,
            DataTypeKind::VarChar { .. }
        ));
    }
}

#[test]
fn audit_five_rrf_aliases_and_explain() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE ranked(id INT PRIMARY KEY,body TEXT,FULLTEXT f(body))")
        .unwrap();
    engine
        .execute("INSERT INTO ranked VALUES(1,'database'),(2,'engine')")
        .unwrap();
    let source="((SELECT id FROM ranked ORDER BY id LIMIT 2) lexical RRF (SELECT id FROM ranked ORDER BY id DESC LIMIT 2) dense) h";
    assert_eq!(
        rows(&engine, &format!("SELECT id FROM {source} ORDER BY id")),
        vec![vec!["1"], vec!["2"]]
    );
    let json = rows(
        &engine,
        &format!("EXPLAIN FORMAT=JSON SELECT id FROM {source}"),
    )[0][0]
        .clone();
    assert!(
        !json.contains("Some(Rows") && !json.contains("ExprType"),
        "{json}"
    );
    let json=rows(&engine,"EXPLAIN FORMAT=JSON SELECT id FROM ((SELECT id FROM ranked ORDER BY MATCH(body) AGAINST('database') DESC LIMIT 2) RRF (SELECT id FROM ranked ORDER BY id LIMIT 2)) h")[0][0].clone();
    assert!(
        json.contains("MATCH(") && !json.contains("__match_against"),
        "{json}"
    );
}
