use super::support::result_rows;
use super::*;

#[test]
fn tagged_calendar_values_use_scalar_order_and_do_not_loop_rebuilds() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    for stage in 0..2 {
        let engine = open_strict_test_engine(dir.path());
        let session = std::cell::RefCell::new(SessionContext::default());
        let run = |sql: &str| {
            engine
                .execute_with_session(&mut session.borrow_mut(), sql)
                .unwrap()
        };
        run("SET sql_mode=''");
        if stage == 0 {
            run("CREATE TABLE tagged_order(id INT PRIMARY KEY,d DATE,dt DATETIME(6),d2 DATE,dt2 DATETIME(6)) ZONE_MAP=ON ZONE_SIZE=64K");
            run("INSERT INTO tagged_order VALUES
                (1,'2020-00-15','2020-00-15 12:00:00','2019-12-31','2019-12-31 00:00:00'),
                (2,'2019-01-01','2019-01-01 00:00:00','2019-12-31','2019-12-31 00:00:00')");
            run("ANALYZE TABLE tagged_order");
        }
        for predicate in [
            "d>'2019-12-31'",
            "dt>'2019-12-31 23:59:59'",
            "d BETWEEN '2019-12-31' AND '2021-01-01'",
            "dt BETWEEN CAST('2019-12-31' AS DATETIME(6)) AND CAST('2021-01-01' AS DATETIME(6))",
            "d>d2",
            "dt>dt2",
        ] {
            for hint in ["ZONE_MAP", "NO_ZONE_MAP"] {
                let sql=format!("SELECT /*+ NO_PARALLEL {hint}(tagged_order) */ id FROM tagged_order WHERE {predicate}");
                assert_eq!(
                    result_rows(run(&sql)),
                    vec![vec![Value::Int(1)]],
                    "stage={stage} {sql}"
                );
            }
        }
        let quiet = engine.database().zone_map_maintenance_quiet_ms();
        engine
            .database()
            .set_zone_map_maintenance_quiet_ms_for_tests(0);
        let repaired = engine.database().maintain_zone_maps();
        engine
            .database()
            .set_zone_map_maintenance_quiet_ms_for_tests(quiet);
        assert_eq!(
            repaired.unwrap(),
            0,
            "intentional unsupported-order columns must not be repair debt"
        );
        if stage == 1 {
            run("UPDATE tagged_order SET d='2020-01-01',dt='2020-01-01 12:00:00' WHERE id=1");
            run("ANALYZE TABLE tagged_order");
            let table = engine.database().table("test.tagged_order").unwrap();
            let predicate = crate::parser::parse_expr_fragment("d>'2021-01-01'").unwrap();
            assert_eq!(
                engine
                    .database()
                    .zone_map_cost_details_forced(&table, &predicate)
                    .unwrap()
                    .surviving_pages,
                0
            );
        }
    }
}

#[test]
fn compiled_update_preserves_current_row_assignments_and_savepoints() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE update_bound (id BIGINT UNSIGNED PRIMARY KEY,x BIGINT DEFAULT 5,y DECIMAL(20,4),s VARCHAR(20),b VARBINARY(64),e ENUM('a','b'),bt BIT(8)) ZONE_MAP=OFF").unwrap();
    engine.execute("INSERT INTO update_bound VALUES (1,10,0,'A',X'61','a',0),(2,20,0,'B',X'62','b',0),(18446744073709551615,30,0,'C',X'63','a',0)").unwrap();
    let session = std::cell::RefCell::new(SessionContext::default());
    let run = |sql: &str| {
        engine
            .execute_with_session(&mut session.borrow_mut(), sql)
            .unwrap()
    };
    let snapshot = || {
        result_rows(run(
            "SELECT id,x,y,s,HEX(b),bt FROM update_bound ORDER BY id",
        ))
    };
    let before = snapshot();
    let update = "UPDATE update_bound SET x=x+1,y=x+0.125,s=CONCAT(s,'!'),b=CONCAT(b,X'00ff'),bt=e WHERE id IN (1,1,NULL,'2',18446744073709551615)";
    run("BEGIN");
    run("SAVEPOINT s");
    run(update);
    let changed = snapshot();
    for (index, row) in changed.iter().enumerate() {
        assert_eq!(row[1], Value::Int(11 + 10 * index as i64));
        assert_eq!(
            row[2].to_display_string(),
            format!("{}.1250", 11 + 10 * index as i64)
        );
        assert_eq!(
            row[3].to_display_string(),
            format!("{}!", ['A', 'B', 'C'][index])
        );
        assert_eq!(row[4].to_display_string(), format!("6{}00FF", index + 1));
        assert_eq!(
            row[5].to_display_string(),
            if index == 1 { "2" } else { "1" }
        );
    }
    run("ROLLBACK TO SAVEPOINT s");
    assert_eq!(snapshot(), before);
    run(update);
    run("COMMIT");
    assert_eq!(snapshot(), changed);
    run("UPDATE update_bound SET x=DEFAULT(x),y=x WHERE id IN (1,2)");
    let rows = snapshot();
    for row in &rows[..2] {
        assert_eq!(row[1], Value::Int(5));
        assert_eq!(row[2].to_display_string(), "5.0000");
    }
}
#[test]
fn timestamp_payload_predicate_output_reuse_is_initialized() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    session.sysvars.time_zone = "+00:00".into();
    engine.execute("CREATE TABLE timestamp_payload (c0 BIGINT PRIMARY KEY,c1 BIGINT,c2 DECIMAL(16,4),c3 DOUBLE,c4 DATETIME(6),c5 VARCHAR(64),c6 VARBINARY(64),c7 INT NULL,ts TIMESTAMP(6)) ZONE_MAP=ON").unwrap();
    let base = 1_577_836_800_000_000i64;
    insert_fixture_rows(
        &engine,
        "test.timestamp_payload",
        (0..10000).map(|i| {
            vec![
                Value::Int(i),
                Value::Int(i),
                Value::DecimalCompact(i, 4),
                Value::Double(i as f64),
                Value::DateTime(base + i * 1_000_000),
                Value::String(format!("s{i:020}")),
                Value::Bytes(format!("b{i:020}").into_bytes()),
                Value::Null,
                Value::Timestamp(base + i * 1_000_000),
            ]
        }),
    );
    engine.execute("ANALYZE TABLE timestamp_payload").unwrap();
    let hits = &crate::engine::executor::select::vec_bridge::NATIVE_TEMPORAL_RANGE_DECODE_HITS;
    let before = hits.load(std::sync::atomic::Ordering::Relaxed);
    for column in ["ts", "c4"] {
        for projection in [
            "COUNT(*),SUM(c1)".to_string(),
            format!("{column},c1"),
            column.to_string(),
        ] {
            let mut query = |hint: &str, cast: bool| {
                let bound = |text| {
                    if cast {
                        format!("CAST('{text}' AS DATETIME(6))")
                    } else {
                        format!("'{text}'")
                    }
                };
                let sql = format!("SELECT /*+ NO_PARALLEL {hint}(timestamp_payload) */ {projection} FROM timestamp_payload WHERE {column} BETWEEN {} AND {}",
                    bound("2020-01-01 00:16:40.000000"), bound("2020-01-01 00:16:49.000000"));
                result_rows(engine.execute_with_session(&mut session, &sql).unwrap())
            };
            let expected = query("NO_ZONE_MAP", true);
            assert!(!expected.is_empty());
            assert_eq!(
                query("ZONE_MAP", true),
                expected,
                "{column} {projection} CAST"
            );
            assert_eq!(query("ZONE_MAP", false), expected, "{column} {projection}");
            assert_eq!(
                query("NO_ZONE_MAP", false),
                expected,
                "{column} {projection}"
            );
        }
    }
    assert!(
        hits.load(std::sync::atomic::Ordering::Relaxed) > before,
        "native ranges must run without disabling output reuse"
    );
}

#[test]
fn temporal_typed_ranges_match_scalar_oracle_at_microsecond_boundaries() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let session = std::cell::RefCell::new(SessionContext::default());
    let run = |sql: &str| {
        engine
            .execute_with_session(&mut session.borrow_mut(), sql)
            .unwrap()
    };
    run("SET sql_mode=''");
    engine.execute("CREATE TABLE temporal_boundary (id INT PRIMARY KEY,dt DATETIME(6),tm TIME(6)) ZONE_MAP=ON").unwrap();
    run("INSERT INTO temporal_boundary VALUES
        (1,NULL,NULL),(2,'9999-12-31 23:59:59.999998','-838:59:58.999999'),
        (3,'9999-12-31 23:59:59.999999','-00:00:00.000001'),
        (4,'0000-00-00 00:00:00','00:00:00.000000'),
        (5,'2020-00-15 12:00:00','00:00:00.000001'),
        (6,'2020-01-01 00:00:00','838:59:58.999999')");
    engine.execute("ANALYZE TABLE temporal_boundary").unwrap();
    let table = engine.database().table("test.temporal_boundary").unwrap();
    let rows = engine
        .database()
        .scan_rows(
            &table.qualified_name(),
            crate::storage::TxnView::read_only(
                u64::MAX,
                crate::storage::IsolationLevel::ReadCommitted,
            ),
        )
        .unwrap();
    for predicate in [
        "dt BETWEEN '9999-12-31 23:59:59.999998' AND '9999-12-31 23:59:59.999998'",
        "dt >= '9999-12-31 23:59:59.999999'",
        "'9999-12-31 23:59:59.999999' = dt",
        "dt BETWEEN '0000-00-00 00:00:00' AND '2020-01-01 00:00:00'",
        "dt NOT BETWEEN NULL AND '2020-01-01 00:00:00'",
        "tm BETWEEN '-00:00:00.000001' AND '00:00:00.000001'",
        "tm < '00:00:00.000001' OR dt IS NULL",
    ] {
        let expression = crate::parser::parse_expr_fragment(predicate).unwrap();
        let mut expected = rows
            .iter()
            .filter(|row| crate::engine::expr::eval_bool(&expression, &table, &row.values).unwrap())
            .map(|row| vec![row.values[0].clone()])
            .collect::<Vec<_>>();
        expected.sort_by_key(|row| row[0].to_display_string());
        for hint in ["ZONE_MAP", "NO_ZONE_MAP"] {
            let sql = format!("SELECT /*+ NO_PARALLEL {hint}(temporal_boundary) */ id FROM temporal_boundary WHERE {predicate} ORDER BY id");
            assert_eq!(result_rows(run(&sql)), expected, "{sql}");
        }
    }
}

#[test]
fn binary_membership_uses_suffix_and_preserves_dml_restart_visibility() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        let mut session = SessionContext::default();
        engine.execute("CREATE TABLE binary_suffix (id INT PRIMARY KEY,b VARBINARY(64),pad VARCHAR(800)) ZONE_SIZE=64K").unwrap();
        insert_fixture_rows(
            &engine,
            "test.binary_suffix",
            (1..=2000).map(|i| {
                vec![
                    Value::Int(i),
                    Value::Bytes(format!("b{i:020}").into_bytes()),
                    Value::String("p".repeat(700)),
                ]
            }),
        );
        engine.execute("ANALYZE TABLE binary_suffix").unwrap();
        let table = engine.database().table("test.binary_suffix").unwrap();
        let expr = crate::parser::parse_expr_fragment("b='b00000000000000001000'").unwrap();
        let estimate = engine
            .database()
            .zone_map_cost_details_forced(&table, &expr)
            .unwrap();
        assert!(
            estimate.surviving_pages < estimate.total_pages,
            "{estimate:?}"
        );
        for constant in [
            "'b00000000000000001000'",
            "X'623030303030303030303030303030303031303030'",
        ] {
            let rows = result_rows(engine.execute(&format!("SELECT /*+ ZONE_MAP(binary_suffix) */ id FROM binary_suffix WHERE b={constant}")).unwrap().materialize().unwrap());
            assert_eq!(rows, vec![vec![Value::Int(1000)]]);
        }
        engine.execute_with_session(&mut session, "BEGIN").unwrap();
        engine
            .execute_with_session(
                &mut session,
                "UPDATE binary_suffix SET b='b00000000000000009999' WHERE id=1000",
            )
            .unwrap();
        assert_eq!(result_rows(engine.execute_with_session(&mut session,"SELECT /*+ ZONE_MAP(binary_suffix) */ id FROM binary_suffix WHERE b='b00000000000000009999'").unwrap()),vec![vec![Value::Int(1000)]]);
        assert!(result_rows(engine.execute("SELECT /*+ ZONE_MAP(binary_suffix) */ id FROM binary_suffix WHERE b='b00000000000000009999'").unwrap()).is_empty());
        engine
            .execute_with_session(&mut session, "ROLLBACK")
            .unwrap();
        assert_eq!(result_rows(engine.execute("SELECT /*+ ZONE_MAP(binary_suffix) */ id FROM binary_suffix WHERE b='b00000000000000001000'").unwrap()),vec![vec![Value::Int(1000)]]);
        engine
            .execute("UPDATE binary_suffix SET b='b00000000000000009999' WHERE id=1000")
            .unwrap();
    }
    let engine = open_strict_test_engine(dir.path());
    for hint in ["ZONE_MAP", "NO_ZONE_MAP"] {
        assert_eq!(result_rows(engine.execute(&format!("SELECT /*+ {hint}(binary_suffix) */ id FROM binary_suffix WHERE b='b00000000000000009999'")).unwrap().materialize().unwrap()),vec![vec![Value::Int(1000)]]);
    }
}

#[test]
fn session_parallel_budget_bounds_hints_and_cached_plans() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE budget_scan (id INT PRIMARY KEY,v INT) ZONE_MAP=OFF")
        .unwrap();
    insert_fixture_rows(
        &engine,
        "test.budget_scan",
        (0..2000).map(|i| vec![Value::Int(i), Value::Int(i)]),
    );
    engine.execute("ANALYZE TABLE budget_scan").unwrap();
    let mut session = SessionContext::default();
    let sql = "SELECT /*+ PARALLEL(8) */ SUM(v) FROM budget_scan";
    for budget in [1, 2, 1, 4, 1] {
        engine
            .execute_with_session(
                &mut session,
                &format!("SET SESSION mydb_parallel_degree={budget}"),
            )
            .unwrap();
        let plan = engine
            .execute_with_session(&mut session, &format!("EXPLAIN ANALYZE {sql}"))
            .unwrap()
            .materialize()
            .unwrap();
        let text = format!("{plan:?}");
        if budget == 1 {
            assert!(!text.contains("Parallel table scan"), "{text}");
            assert!(!text.contains("workers="), "{text}");
        } else {
            assert!(text.contains(&format!("workers={budget}")), "{text}");
            assert!(!text.contains("workers=8"), "{text}");
        }
        let rows = result_rows(
            engine
                .execute_with_session(&mut session, sql)
                .unwrap()
                .materialize()
                .unwrap(),
        );
        assert_eq!(rows.len(), 1);
    }
}
