use super::*;

// Select a genuinely dense data zone. Page zero is allocation metadata and
// therefore the first physical zone can contain fewer than 64 row mutations.
fn dense_rebuild_zone_ids(engine: &StorageQueryEngine, name: &str) -> Vec<i64> {
    let table = engine.database().table(name).unwrap();
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
    let mut zones = std::collections::BTreeMap::<u64, Vec<i64>>::new();
    for row in rows {
        let Value::Int(id) = row.values[0] else {
            panic!("expected integer fixture key")
        };
        zones
            .entry(u64::from(row.row_id.page_id) / u64::from(table.zone_pages()))
            .or_default()
            .push(id);
    }
    zones
        .into_values()
        .find(|ids| ids.len() >= 64)
        .expect("fixture needs a dense rebuild zone")
}

#[test]
fn zone_map_audit_null_and_prefix_differential() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE zm_audit (s VARBINARY(64)) ZONE_SIZE=64K")
        .unwrap();
    engine
        .execute("INSERT INTO zm_audit VALUES (X'6162636465666768696a6b6c6d6e6f70')")
        .unwrap();
    let check = |table: &str, predicate: &str| {
        let query = |hint: &str| {
            engine
                .execute(&format!(
                    "SELECT /*+ {hint}({table}) */ COUNT(*) FROM {table} WHERE {predicate}"
                ))
                .unwrap()
        };
        assert_eq!(
            format!("{:?}", query("ZONE_MAP")),
            format!("{:?}", query("NO_ZONE_MAP")),
            "{predicate}"
        );
    };
    check("zm_audit", "s < X'6162636465666768696a6b6c6d6e6f7071'");
    engine.execute("TRUNCATE TABLE zm_audit").unwrap();
    engine.execute("INSERT INTO zm_audit VALUES (X'6162636465666768696a6b6c6d6e6f70'),(X'6162636465666768696a6b6c6d6e6f7071')").unwrap();
    check("zm_audit", "s <> X'6162636465666768696a6b6c6d6e6f70'");
    check("zm_audit", "s > X'6162636465666768696a6b6c6d6e6f70'");
    engine
        .execute("CREATE TABLE zm_null (s VARCHAR(64)) ZONE_SIZE=64K")
        .unwrap();
    engine
        .execute(&format!(
            "INSERT INTO zm_null VALUES {}",
            vec!["(NULL)"; 128].join(",")
        ))
        .unwrap();
    check("zm_null", "s <=> NULL");
}

#[test]
fn zone_map_audit_targeted_rebuild_preserves_uncommitted_writer() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE zm_pending (id INT PRIMARY KEY, v INT) ZONE_SIZE=64K")
        .unwrap();
    engine
        .execute(&format!(
            "INSERT INTO zm_pending VALUES {}",
            (0..100)
                .map(|i| format!("({i},{i})"))
                .collect::<Vec<_>>()
                .join(",")
        ))
        .unwrap();
    engine.execute("ANALYZE TABLE zm_pending").unwrap();
    engine.execute("UPDATE zm_pending SET v=v+100").unwrap();
    let mut writer = SessionContext::default();
    writer.connection_id = 2;
    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut writer, "UPDATE zm_pending SET v=999999 WHERE id=1")
        .unwrap();
    engine
        .db_for_tests()
        .set_zone_map_maintenance_quiet_ms_for_tests(0);
    let result = engine.db_for_tests().maintain_zone_maps();
    engine
        .db_for_tests()
        .set_zone_map_maintenance_quiet_ms_for_tests(30_000);
    result.unwrap();
    engine.execute_with_session(&mut writer, "COMMIT").unwrap();
    let query = |hint: &str| {
        engine
            .execute(&format!(
                "SELECT /*+ {hint}(zm_pending) */ id FROM zm_pending WHERE v=999999"
            ))
            .unwrap()
    };
    assert_eq!(
        format!("{:?}", query("ZONE_MAP")),
        format!("{:?}", query("NO_ZONE_MAP"))
    );
}

#[test]
fn full_scan_cost_is_independent_of_zone_map_candidates() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE zm_full_cost (id INT, v INT, pad VARCHAR(700)) ZONE_MAP=ON ZONE_SIZE=64K",
        )
        .unwrap();
    engine.execute("INSERT INTO zm_full_cost SELECT n, 0, REPEAT('x', 600) FROM generate_series(1, 2000) AS gs(n)").unwrap();
    engine.execute("ANALYZE TABLE zm_full_cost").unwrap();

    fn scan(plan: &PhysicalPlan) -> (crate::engine::cbo::Cost, f64, f64, bool) {
        match plan {
            PhysicalPlan::SeqScan {
                cost,
                access_rows,
                estimated_rows,
                ..
            }
            | PhysicalPlan::ParallelSeqScan {
                cost,
                access_rows,
                estimated_rows,
                ..
            } => (*cost, *access_rows, *estimated_rows, false),
            PhysicalPlan::ZoneMapScan {
                cost,
                access_rows,
                estimated_rows,
                ..
            }
            | PhysicalPlan::ParallelZoneMapScan {
                cost,
                access_rows,
                estimated_rows,
                ..
            } => (*cost, *access_rows, *estimated_rows, true),
            _ => scan(
                crate::planner::plan_children(plan)
                    .into_iter()
                    .next()
                    .expect("scan descendant"),
            ),
        }
    }
    for parallel in ["NO_PARALLEL", "PARALLEL(4)"] {
        for body in [
            "COUNT(*) FROM zm_full_cost WHERE v <> 0",
            "id FROM zm_full_cost WHERE v <> 0 LIMIT 5",
            "COUNT(*) FROM zm_full_cost WHERE id > 1500",
            "COUNT(*) FROM zm_full_cost WHERE id > 0",
        ] {
            let plan = |hint: &str| {
                let Statement::Select(mut select) =
                    parse_statement(&format!("SELECT /*+ {parallel} */ {body}")).unwrap()
                else {
                    panic!("select")
                };
                // Construct combined hints at the planner boundary: the SQL
                // parser currently rejects some multi-hint orderings.
                for token in hint.split_whitespace() {
                    let Statement::Select(hinted) =
                        parse_statement(&format!("SELECT /*+ {token} */ {body}")).unwrap()
                    else {
                        panic!("select")
                    };
                    select.query_hints.extend(hinted.query_hints);
                }
                crate::planner::plan_select(engine.db_for_tests(), &select)
                    .unwrap()
                    .physical
            };
            let oracle = plan("FULL_SCAN(zm_full_cost) NO_ZONE_MAP(zm_full_cost)");
            let expected = scan(&oracle);
            for hint in [
                "FULL_SCAN(zm_full_cost)",
                "FULL_SCAN(zm_full_cost) ZONE_MAP(zm_full_cost)",
                "ZONE_MAP(zm_full_cost) FULL_SCAN(zm_full_cost)",
            ] {
                crate::storage::reset_zone_map_cost_estimate_calls();
                let actual = plan(hint);
                assert_eq!(scan(&actual), expected, "{parallel} {hint} {body}");
                assert_eq!(
                    crate::planner::plan_cost(&actual),
                    crate::planner::plan_cost(&oracle),
                    "parent cost: {parallel} {hint} {body}"
                );
                // An explicit ZONE_MAP hint is independently validated;
                // plain FULL_SCAN must not enumerate zone candidates.
                if hint == "FULL_SCAN(zm_full_cost)" {
                    assert_eq!(crate::storage::zone_map_cost_estimate_calls(), 0);
                }
            }
            if body.contains("v <> 0") {
                let zone = scan(&plan("ZONE_MAP(zm_full_cost)"));
                assert!(zone.3 && zone.0.total < expected.0.total);
                assert!(zone.1 < expected.1);
                assert_eq!(zone.2, expected.2);
            }
        }
    }
    for parallel in ["NO_PARALLEL", "PARALLEL(4)"] {
        for mode in [
            crate::engine::executor::vec::config::VecMode::Off,
            crate::engine::executor::vec::config::VecMode::On,
        ] {
            for predicate in ["v <> 0", "id > 1500", "id > 0"] {
                let rows = |hint: &str| {
                    crate::engine::executor::vec::config::with_vec_mode(mode, || {
                        let QueryResponse::ResultSet(result) = engine.execute(&format!("SELECT /*+ {hint} {parallel} */ COUNT(*) FROM zm_full_cost WHERE {predicate}")).unwrap() else { panic!("result set") };
                        result.rows
                    })
                };
                assert_eq!(
                    rows("FULL_SCAN(zm_full_cost)"),
                    rows("ZONE_MAP(zm_full_cost)"),
                    "{parallel} {mode:?} {predicate}"
                );
            }
        }
    }
}

#[test]
fn zone_map_not_equal_matches_row_oracle_after_mutations() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE zm_ne (id INT PRIMARY KEY, v BIGINT, s VARCHAR(100)) ZONE_MAP=ON ZONE_SIZE=64K").unwrap();
    insert_fixture_rows(
        &engine,
        "test.zm_ne",
        (1..=3000).map(|n| {
            vec![
                Value::Int(n),
                Value::Int(7),
                Value::String("abcdefghijklmnop".into()),
            ]
        }),
    );
    let compare = || {
        for predicate in [
            "v <> 7",
            "7 <> v",
            "v <> 9",
            "v <> NULL",
            "v <> 7 OR v IS NULL",
            "v <> 7 AND id > 0",
            "s <> 'abcdefghijklmnop'",
            "s <> 'abcdefghijklmnop-long'",
            "v <> 7.1",
        ] {
            let sql = format!("SELECT {{}} COUNT(*) FROM zm_ne WHERE {predicate}");
            let expected = with_vec_mode(VecMode::Off, || {
                engine
                    .execute(&sql.replace("{}", "/*+ NO_ZONE_MAP(zm_ne) */"))
                    .unwrap()
            });
            for mode in [VecMode::Off, VecMode::On] {
                let actual = with_vec_mode(mode, || {
                    engine
                        .execute(&sql.replace("{}", "/*+ ZONE_MAP(zm_ne) */"))
                        .unwrap()
                });
                let (QueryResponse::ResultSet(actual), QueryResponse::ResultSet(expected)) =
                    (actual, &expected)
                else {
                    panic!("expected result sets")
                };
                assert_eq!(actual.rows, expected.rows, "{predicate} {mode:?}");
            }
        }
    };
    compare();
    let mut reader = SessionContext::default();
    engine
        .execute_with_session(
            &mut reader,
            "SET SESSION transaction_isolation='REPEATABLE-READ'",
        )
        .unwrap();
    engine.execute_with_session(&mut reader, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut reader, "SELECT COUNT(*) FROM zm_ne")
        .unwrap();
    // The initial singleton zones can be excluded, but later writes must
    // widen their summaries before the new values become visible.
    engine
        .execute("UPDATE zm_ne SET v=9, s='abcdefghijklmnop-long' WHERE id=1")
        .unwrap();
    engine
        .execute("UPDATE zm_ne SET v=NULL WHERE id=2")
        .unwrap();
    compare();
    engine.execute("DELETE FROM zm_ne WHERE id=1").unwrap();
    compare();
    let QueryResponse::ResultSet(snapshot) = engine
        .execute_with_session(
            &mut reader,
            "SELECT /*+ ZONE_MAP(zm_ne) */ COUNT(*) FROM zm_ne WHERE v <> 9",
        )
        .unwrap()
    else {
        panic!("expected snapshot result")
    };
    assert_eq!(snapshot.rows, vec![vec![Value::Int(3000)]]);
    engine
        .execute_with_session(&mut reader, "ROLLBACK")
        .unwrap();
    let QueryResponse::ResultSet(plan) = engine
        .execute("EXPLAIN SELECT /*+ ZONE_MAP(zm_ne) */ COUNT(*) FROM zm_ne WHERE v <> 7")
        .unwrap()
    else {
        panic!("expected explain")
    };
    assert!(
        plan.rows.iter().flatten().any(|value| value
            .to_display_string()
            .to_lowercase()
            .contains("zone map")),
        "{:?}",
        plan.rows
    );
}

fn insert_repeated_integer_series(
    engine: &StorageQueryEngine,
    table: &str,
    last: i64,
    column_count: usize,
) {
    let projection = std::iter::repeat_n("n", column_count)
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!(
            "INSERT INTO {table} SELECT {projection} \
             FROM generate_series(0, {last}) AS gs(n)"
        ))
        .unwrap();
}

#[test]
fn zone_map_sql_differential_and_diagnostics() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let padding_columns = (0..32)
        .map(|index| format!("p{index} BIGINT"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!(
            "CREATE TABLE zm_t (id INT,{padding_columns}) ZONE_MAP=ON ZONE_SIZE=64K"
        ))
        .unwrap();

    insert_repeated_integer_series(&engine, "zm_t", 1599, 33);

    let on = engine
        .execute(
            "SELECT /*+ ZONE_MAP(zm_t) */ id FROM zm_t \
             WHERE id BETWEEN 1550 AND 1560",
        )
        .unwrap();
    let skipped = engine
        .db_for_tests()
        .zone_map_status_rows()
        .into_iter()
        .find(|(name, _)| *name == "zone_map_zones_skipped")
        .and_then(|(_, value)| value.parse::<u64>().ok())
        .unwrap_or(0);
    assert!(
        skipped > 0,
        "selected ZoneMapScan must skip heap zones at runtime"
    );
    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN FORMAT=JSON SELECT id FROM zm_t WHERE id BETWEEN 1550 AND 1560")
        .unwrap()
    else {
        panic!("expected explain")
    };
    assert!(
        explain.rows.iter().flatten().any(|value| value
            .to_display_string()
            .contains("\"access_type\": \"zone\"")),
        "expected CBO to choose zone access: {:?}; forced: {:?}",
        explain.rows,
        engine.execute("EXPLAIN FORMAT=JSON SELECT /*+ ZONE_MAP(zm_t) */ id FROM zm_t WHERE id BETWEEN 1550 AND 1560")
    );
    engine
        .execute("SET optimizer_switch='zone_map=off'")
        .unwrap();
    let off = engine
        .execute("SELECT id FROM zm_t WHERE id BETWEEN 1550 AND 1560")
        .unwrap();
    assert_eq!(format!("{on:?}"), format!("{off:?}"));
    engine
        .execute("SET optimizer_switch='zone_map=on'")
        .unwrap();
    let QueryResponse::ResultSet(no_hint) = engine
        .execute(
            "EXPLAIN FORMAT=JSON SELECT /*+ NO_ZONE_MAP(zm_t) */ id \
             FROM zm_t WHERE id BETWEEN 1550 AND 1560",
        )
        .unwrap()
    else {
        panic!("expected explain")
    };
    assert!(
        no_hint.rows.iter().flatten().all(|value| !value
            .to_display_string()
            .contains("\"access_type\": \"zone\"")),
        "NO_ZONE_MAP must disable zone access: {:?}",
        no_hint.rows
    );
    let QueryResponse::ResultSet(forced) = engine
        .execute(
            "EXPLAIN FORMAT=JSON SELECT /*+ ZONE_MAP(zm_t) */ id \
             FROM zm_t WHERE id BETWEEN 1550 AND 1560",
        )
        .unwrap()
    else {
        panic!("expected explain")
    };
    assert!(
        forced.rows.iter().flatten().any(|value| value
            .to_display_string()
            .contains("\"access_type\": \"zone\"")),
        "ZONE_MAP must force zone access: {:?}",
        forced.rows
    );

    let QueryResponse::ResultSet(rows) = engine
        .execute(
            "SELECT COUNT(*) FROM information_schema.mydb_zone_maps \
             WHERE table_schema='test' AND table_name='zm_t' AND column_name='id'",
        )
        .unwrap()
    else {
        panic!("expected zone map diagnostics");
    };
    assert_ne!(rows.rows[0][0], Value::Int(0));

    engine.db_for_tests().checkpoint().unwrap();
    let QueryResponse::ResultSet(storage_rows) = engine
        .execute(
            "SELECT format_version,file_bytes,live_bytes,state \
             FROM information_schema.mydb_zone_map_storage \
             WHERE table_schema='test' AND table_name='zm_t'",
        )
        .unwrap()
    else {
        panic!("expected zone-map storage diagnostics");
    };
    assert_eq!(storage_rows.rows.len(), 1);
    assert_eq!(
        storage_rows.rows[0][0],
        Value::UInt(u64::from(crate::storage::format_versions::ZONE_MAP_VERSION))
    );
    assert_eq!(storage_rows.rows[0][3], Value::String("READY".into()));
}

#[test]
fn zone_map_membership_prunes_randomly_scattered_column_with_real_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE zm_scatter (id INT PRIMARY KEY, scatter BIGINT, pad VARCHAR(800)) \
             ZONE_MAP=ON ZONE_SIZE=64K",
        )
        .unwrap();
    let padding = "x".repeat(700);
    // 2k wide rows still span well over ten 64 KiB zones, which is enough
    // to prove membership rejection and physical page skipping.  The old
    // 20k-row fixture spent most of the whole test run formatting and
    // inserting 14 MiB of duplicate padding without exercising another
    // branch.
    let row_count = 2_000u64;
    let insert_on_started = std::time::Instant::now();
    for base in (0..row_count).step_by(200) {
        let rows = (base..(base + 200).min(row_count))
            .map(|id| {
                let scatter = id.wrapping_mul(2_654_435_761) % 10_000_019;
                format!("({id},{scatter},'{padding}')")
            })
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO zm_scatter VALUES {rows}"))
            .unwrap();
    }
    let insert_on_us = insert_on_started.elapsed().as_micros();
    let analyze_started = std::time::Instant::now();
    engine.execute("ANALYZE TABLE zm_scatter").unwrap();
    let analyze_us = analyze_started.elapsed().as_micros();
    let membership_rows = engine
        .execute(
            "SELECT membership_kind,membership_bytes FROM information_schema.mydb_zone_maps \
             WHERE table_schema='test' AND table_name='zm_scatter' AND column_name='scatter'",
        )
        .unwrap();
    let analyzed_schema = engine
        .db_for_tests()
        .snapshot_table_schema("test.zm_scatter")
        .unwrap();
    let scatter_ndv = analyzed_schema
        .stats
        .columns
        .get("scatter")
        .map(|stats| stats.distinct_count)
        .unwrap_or(0);

    let target_id = 432u64;
    let target = target_id.wrapping_mul(2_654_435_761) % 10_000_019;
    let status = |name: &str| {
        engine
            .db_for_tests()
            .zone_map_status_rows()
            .into_iter()
            .find(|(candidate, _)| *candidate == name)
            .and_then(|(_, value)| value.parse::<u64>().ok())
            .unwrap_or(0)
    };
    let checks_before = status("zone_map_membership_checks");
    let rejects_before = status("zone_map_membership_rejections");
    let skipped_before = status("zone_map_zones_skipped");
    let zone_started = std::time::Instant::now();
    let zone = engine
        .execute(&format!(
            "SELECT /*+ ZONE_MAP(zm_scatter) NO_PARALLEL */ id FROM zm_scatter \
             WHERE scatter={target}"
        ))
        .unwrap();
    let zone_query_us = zone_started.elapsed().as_micros();
    let seq_started = std::time::Instant::now();
    let seq = engine
        .execute(&format!(
            "SELECT /*+ NO_ZONE_MAP(zm_scatter) NO_PARALLEL */ id FROM zm_scatter \
             WHERE scatter={target}"
        ))
        .unwrap();
    let seq_query_us = seq_started.elapsed().as_micros();
    assert_eq!(format!("{zone:?}"), format!("{seq:?}"));
    let checks = status("zone_map_membership_checks").saturating_sub(checks_before);
    let rejects = status("zone_map_membership_rejections").saturating_sub(rejects_before);
    let skipped = status("zone_map_zones_skipped").saturating_sub(skipped_before);
    assert!(
        checks > 1,
        "expected multiple scattered zones, got {checks}; membership={membership_rows:?}; memory_fallbacks={}; rows={} pages={} zone_pages={} ndv={scatter_ndv}",
        status("zone_map_memory_fallbacks")
        , analyzed_schema.stats.row_count, analyzed_schema.stats.heap_page_count, analyzed_schema.zone_pages()
    );
    assert!(rejects > 0, "membership must reject at least one zone");
    assert!(
        skipped > 0,
        "membership must translate rejection into page skipping"
    );
    let metadata_bytes = status("zone_map_resident_pages") * crate::storage::PAGE_SIZE as u64;
    let payload_bytes = row_count * padding.len() as u64;
    assert!(
        metadata_bytes < payload_bytes / 10,
        "zone metadata should stay small: metadata={metadata_bytes}, payload={payload_bytes}"
    );
    eprintln!(
        "zone-membership real-data: rows={row_count}, checks={checks}, rejects={rejects}, \
         skipped_zones={skipped}, metadata_bytes={metadata_bytes}, payload_bytes={payload_bytes}, \
         zone_query_us={zone_query_us}, seq_query_us={seq_query_us}, \
         insert_on_us={insert_on_us}, analyze_us={analyze_us}"
    );
}

#[test]
fn zone_map_decimal_column_accepts_integer_literal_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE zm_decimal (id INT, amount DECIMAL(15,2), pad VARCHAR(900)) \
             ZONE_MAP=ON ZONE_SIZE=64K",
        )
        .unwrap();
    let padding = "x".repeat(800);
    for base in (0..400).step_by(40) {
        let values = (base..base + 40)
            .map(|id| format!("({id},{id}.00,'{padding}')"))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO zm_decimal VALUES {values}"))
            .unwrap();
    }
    // Rebuild the exact/Bloom summaries from stored DECIMAL(15,2) values.
    // The persisted representation is `350.00` while the predicate below is
    // the integer literal `350`; their SQL equality must also imply identical
    // membership fingerprints.
    engine.execute("ANALYZE TABLE zm_decimal").unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute(
            "EXPLAIN FORMAT=TREE SELECT /*+ ZONE_MAP(zm_decimal) NO_PARALLEL */ id \
             FROM zm_decimal WHERE amount = 350",
        )
        .unwrap()
    else {
        panic!("expected explain")
    };
    let dump = format!("{:?}", explain.rows);
    assert!(
        dump.contains("Zone map scan"),
        "integer literal must compile as an exact DECIMAL zone bound: {dump}"
    );
    assert!(
        !dump.contains("cannot be safely pruned")
            && !dump.contains("cannot safely produce zone ranges"),
        "supported DECIMAL/integer comparison must not emit a hint warning: {dump}"
    );
    assert!(
        !dump.contains("pages=0/"),
        "a present DECIMAL value must never be excluded by membership: {dump}"
    );

    let zone = engine
        .execute(
            "SELECT /*+ ZONE_MAP(zm_decimal) NO_PARALLEL */ id \
             FROM zm_decimal WHERE amount = 350",
        )
        .unwrap();
    let seq = engine
        .execute(
            "SELECT /*+ NO_ZONE_MAP(zm_decimal) NO_PARALLEL */ id \
             FROM zm_decimal WHERE amount = 350",
        )
        .unwrap();
    assert_eq!(format!("{zone:?}"), format!("{seq:?}"));
}

#[test]
fn zone_map_no_pad_collation_preserves_trailing_spaces() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE zm_no_pad (id INT PRIMARY KEY, \
             v VARCHAR(32) COLLATE utf8mb4_0900_ai_ci, pad VARCHAR(900)) \
             ZONE_MAP=ON ZONE_SIZE=64K",
        )
        .unwrap();
    let padding = "x".repeat(800);
    engine
        .execute(&format!(
            "INSERT INTO zm_no_pad VALUES (1,'a ','{padding}')"
        ))
        .unwrap();
    engine.execute("ANALYZE TABLE zm_no_pad").unwrap();
    let zone = engine
        .execute(
            "SELECT /*+ ZONE_MAP(zm_no_pad) NO_PARALLEL */ id \
             FROM zm_no_pad WHERE v > 'a'",
        )
        .unwrap();
    let seq = engine
        .execute(
            "SELECT /*+ NO_ZONE_MAP(zm_no_pad) NO_PARALLEL */ id \
             FROM zm_no_pad WHERE v > 'a'",
        )
        .unwrap();
    assert_eq!(format!("{zone:?}"), format!("{seq:?}"));
    let QueryResponse::ResultSet(rows) = zone else {
        panic!("expected result set")
    };
    assert_eq!(rows.rows.len(), 1, "NO PAD must keep the trailing space");
}

#[test]
fn zone_map_native_vector_leaf_consumes_candidate_tasks() {
    use crate::engine::executor::select::parallel::ZONE_MAP_RUNTIME_FALLBACKS;
    use crate::engine::executor::select::vec_bridge::{
        try_native_vec_input_stream, ZONE_MAP_BATCH_SCAN_HITS,
    };
    use std::sync::atomic::Ordering;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let padding_columns = (0..24)
        .map(|index| format!("p{index} BIGINT"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!(
            "CREATE TABLE zm_vec (id INT,{padding_columns}) ZONE_MAP=ON ZONE_SIZE=64K"
        ))
        .unwrap();
    for base in (0..400).step_by(100) {
        let values = (base..base + 100)
            .map(|id| {
                let padding = std::iter::repeat_n(id.to_string(), 24)
                    .collect::<Vec<_>>()
                    .join(",");
                format!("({id},{padding})")
            })
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO zm_vec VALUES {values}"))
            .unwrap();
    }

    let Statement::Select(select) = parse_statement(
        "SELECT /*+ ZONE_MAP(zm_vec) NO_PARALLEL */ id FROM zm_vec \
         WHERE id BETWEEN 350 AND 360",
    )
    .unwrap() else {
        unreachable!()
    };
    let planned = crate::planner::plan_select(engine.db_for_tests(), &select).unwrap();
    let before = ZONE_MAP_BATCH_SCAN_HITS.load(Ordering::Relaxed);
    let candidate_generations_before = engine
        .db_for_tests()
        .zone_map_status_rows()
        .into_iter()
        .find(|(name, _)| *name == "zone_map_candidate_generations")
        .and_then(|(_, value)| value.parse::<u64>().ok())
        .unwrap_or(0);
    let fallbacks_before = ZONE_MAP_RUNTIME_FALLBACKS.load(Ordering::Relaxed);
    let txn = engine
        .db_for_tests()
        .read_only_statement_view(crate::storage::IsolationLevel::ReadCommitted, None);
    let mut input = try_native_vec_input_stream(engine.db_for_tests(), txn, &planned.physical)
        .unwrap()
        .expect("ZoneMapScan must expose a native storage-to-Batch leaf");
    let mut rows = 0usize;
    while let Some(batch) = input.stream.next_batch().unwrap() {
        rows += batch.visible_rows();
    }
    assert_eq!(rows, 11);
    assert!(ZONE_MAP_BATCH_SCAN_HITS.load(Ordering::Relaxed) > before);
    let candidate_generations_after = engine
        .db_for_tests()
        .zone_map_status_rows()
        .into_iter()
        .find(|(name, _)| *name == "zone_map_candidate_generations")
        .and_then(|(_, value)| value.parse::<u64>().ok())
        .unwrap_or(0);
    assert_eq!(
        candidate_generations_after.saturating_sub(candidate_generations_before),
        1,
        "one physical vector leaf must build one Zone Map candidate artifact"
    );
    assert_eq!(
        ZONE_MAP_RUNTIME_FALLBACKS.load(Ordering::Relaxed),
        fallbacks_before,
        "a supported native ZoneMapScan must not fall back to a full scan"
    );
}

#[test]
fn zone_map_prepared_parameters_rebuild_runtime_candidates() {
    use crate::engine::api::QueryEngine;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE zm_param (id INT, pad VARCHAR(512)) ZONE_MAP=ON ZONE_SIZE=64K")
        .unwrap();
    for base in (0..1800).step_by(100) {
        let values = (base..base + 100)
            .map(|id| format!("({id},'x')"))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO zm_param VALUES {values}"))
            .unwrap();
    }

    let sql = "SELECT /*+ ZONE_MAP(zm_param) NO_PARALLEL */ id FROM zm_param WHERE id = ?";
    let oracle_sql =
        "SELECT /*+ NO_ZONE_MAP(zm_param) NO_PARALLEL */ id FROM zm_param WHERE id = ?";
    let mut session = SessionContext::default();
    let planned = engine.prepare_cached(&mut session, sql, 1).unwrap();
    let oracle = engine.prepare_cached(&mut session, oracle_sql, 1).unwrap();
    for value in [1700i64, 5, -1, 900, 1799] {
        let params = vec![Value::Int(value)];
        let zone = crate::engine::params::with_params(params.clone(), || {
            engine
                .execute_prepared_statement(&mut session, sql, (*planned).clone())
                .expect("prepared Zone Map statement")
        })
        .unwrap()
        .materialize()
        .unwrap();
        let seq = crate::engine::params::with_params(params, || {
            engine
                .execute_prepared_statement(&mut session, oracle_sql, (*oracle).clone())
                .expect("prepared SeqScan oracle")
        })
        .unwrap()
        .materialize()
        .unwrap();
        assert_eq!(format!("{zone:?}"), format!("{seq:?}"), "value={value}");
    }
}

#[test]
fn zone_map_like_prefix_prunes_and_unanchored_hint_does_not_fake_access() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE zm_like (id INT, s VARCHAR(64) COLLATE utf8mb4_0900_ai_ci, \
             pad VARCHAR(900)) ZONE_MAP=ON ZONE_SIZE=64K",
        )
        .unwrap();
    let padding = "x".repeat(800);
    for base in (0..480).step_by(40) {
        let values = (base..base + 40)
            .map(|id| {
                let text = if id < 240 {
                    format!("apple{id:04}")
                } else {
                    format!("zebra{id:04}")
                };
                format!("({id},'{text}','{padding}')")
            })
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO zm_like VALUES {values}"))
            .unwrap();
    }

    let before = engine
        .db_for_tests()
        .zone_map_status_rows()
        .into_iter()
        .find(|(name, _)| *name == "zone_map_zones_skipped")
        .and_then(|(_, value)| value.parse::<u64>().ok())
        .unwrap_or(0);
    let zone = engine
        .execute(
            "SELECT /*+ ZONE_MAP(zm_like) NO_PARALLEL */ id FROM zm_like \
             WHERE s LIKE 'apple%' ORDER BY id",
        )
        .unwrap();
    let seq = engine
        .execute(
            "SELECT /*+ NO_ZONE_MAP(zm_like) NO_PARALLEL */ id FROM zm_like \
             WHERE s LIKE 'apple%' ORDER BY id",
        )
        .unwrap();
    assert_eq!(format!("{zone:?}"), format!("{seq:?}"));
    let after = engine
        .db_for_tests()
        .zone_map_status_rows()
        .into_iter()
        .find(|(name, _)| *name == "zone_map_zones_skipped")
        .and_then(|(_, value)| value.parse::<u64>().ok())
        .unwrap_or(0);
    assert!(after > before, "LIKE prefix must skip at least one zone");

    let sql =
        "SELECT /*+ ZONE_MAP(zm_like) NO_PARALLEL */ id FROM zm_like WHERE s LIKE ? ORDER BY id";
    let oracle_sql =
        "SELECT /*+ NO_ZONE_MAP(zm_like) NO_PARALLEL */ id FROM zm_like WHERE s LIKE ? ORDER BY id";
    let mut session = SessionContext::default();
    let planned = engine.prepare_cached(&mut session, sql, 1).unwrap();
    let oracle = engine.prepare_cached(&mut session, oracle_sql, 1).unwrap();
    for pattern in ["apple%", "zebra%", "%apple%", ""] {
        let params = vec![Value::String(pattern.to_string())];
        let zone = crate::engine::params::with_params(params.clone(), || {
            engine
                .execute_prepared_statement(&mut session, sql, (*planned).clone())
                .expect("prepared Zone Map LIKE")
        })
        .unwrap()
        .materialize()
        .unwrap();
        let seq = crate::engine::params::with_params(params, || {
            engine
                .execute_prepared_statement(&mut session, oracle_sql, (*oracle).clone())
                .expect("prepared SeqScan LIKE oracle")
        })
        .unwrap()
        .materialize()
        .unwrap();
        assert_eq!(format!("{zone:?}"), format!("{seq:?}"), "pattern={pattern}");
    }

    let QueryResponse::ResultSet(explain) = engine
        .execute(
            "EXPLAIN FORMAT=JSON SELECT /*+ ZONE_MAP(zm_like) */ id \
             FROM zm_like WHERE s LIKE '%apple%'",
        )
        .unwrap()
    else {
        panic!("expected explain")
    };
    let dump = format!("{:?}", explain.rows);
    assert!(
        !dump.contains("\"access_type\": \"zone\""),
        "unanchored LIKE must not produce a fake ZoneMapScan: {dump}"
    );
    assert!(
        dump.contains("cannot be safely pruned"),
        "forced unsupported Zone Map LIKE needs an EXPLAIN warning: {dump}"
    );
}

#[test]
fn zone_map_cbo_uses_consistent_page_scale_and_charges_metadata_work() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // The declared VARCHAR width is intentionally much larger than the
    // stored values.  The generic heap-page estimate therefore differs
    // substantially from the physical page count used by Zone Maps.
    engine
        .execute(
            "CREATE TABLE zm_cost (id INT, pad VARCHAR(700)) \
             ZONE_MAP=ON ZONE_SIZE=64K",
        )
        .unwrap();
    for base in (0..1000).step_by(100) {
        let values = (base..base + 100)
            .map(|id| format!("({id},'x')"))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO zm_cost VALUES {values}"))
            .unwrap();
    }

    fn planned_scan_cost(engine: &StorageQueryEngine, sql: &str) -> crate::engine::cbo::Cost {
        let Statement::Select(select) = parse_statement(sql).unwrap() else {
            panic!("expected SELECT");
        };
        let planned = crate::planner::plan_select(engine.db_for_tests(), &select).unwrap();
        fn find(plan: &PhysicalPlan) -> Option<crate::engine::cbo::Cost> {
            match plan {
                PhysicalPlan::SeqScan { cost, .. } | PhysicalPlan::ZoneMapScan { cost, .. } => {
                    Some(*cost)
                }
                PhysicalPlan::Projection { input, .. }
                | PhysicalPlan::Limit { input, .. }
                | PhysicalPlan::Sort { input, .. }
                | PhysicalPlan::Exchange { input, .. } => find(input),
                _ => None,
            }
        }
        find(&planned.physical)
            .unwrap_or_else(|| panic!("expected heap scan, got: {:?}", planned.physical))
    }

    let serial = planned_scan_cost(
        &engine,
        "SELECT /*+ NO_ZONE_MAP(zm_cost) NO_PARALLEL */ id \
         FROM zm_cost WHERE id BETWEEN 0 AND 999",
    );
    let full_zone = planned_scan_cost(
        &engine,
        "SELECT /*+ ZONE_MAP(zm_cost) NO_PARALLEL */ id \
         FROM zm_cost WHERE id BETWEEN 0 AND 999",
    );
    assert!(
        full_zone.total >= serial.total,
        "a Zone Map that keeps every physical page must not receive a page-scale discount: \
         zone={full_zone:?}, serial={serial:?}"
    );

    let empty_zone = planned_scan_cost(
        &engine,
        "SELECT /*+ ZONE_MAP(zm_cost) NO_PARALLEL */ id \
         FROM zm_cost WHERE id = -1",
    );
    assert!(
        empty_zone.startup > 0.0 && empty_zone.total >= empty_zone.startup,
        "even a fully pruned Zone Map scan must charge metadata discovery: {empty_zone:?}"
    );
}

#[test]
fn limit_keeps_zone_map_in_cost_competition_with_range_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE zm_limit_cost (\
               id INT, target INT, pad VARCHAR(800), INDEX idx_id(id)\
             ) ZONE_MAP=ON ZONE_SIZE=64K",
        )
        .unwrap();
    let pad = "x".repeat(700);
    for base in (0..2400).step_by(100) {
        let rows = (base..base + 100)
            .map(|id| format!("({id},{id},'{pad}')"))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO zm_limit_cost VALUES {rows}"))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE zm_limit_cost").unwrap();

    let explain_tree = |sql: &str| {
        let QueryResponse::ResultSet(explain) = engine.execute(sql).unwrap() else {
            panic!("expected EXPLAIN result")
        };
        explain.rows[0][0].to_display_string()
    };
    let no_zone = explain_tree(
        "EXPLAIN FORMAT=TREE \
         SELECT /*+ NO_ZONE_MAP(zm_limit_cost) USE_INDEX(zm_limit_cost idx_id) */ pad \
         FROM zm_limit_cost \
         WHERE id <= 2399 AND target = 2399 LIMIT 1",
    );
    assert!(
        no_zone.contains("Index range scan on zm_limit_cost using idx_id"),
        "test setup must retain a competing non-covering range index: {no_zone}"
    );

    let automatic = explain_tree(
        "EXPLAIN FORMAT=TREE SELECT pad FROM zm_limit_cost \
         WHERE id <= 2399 AND target = 2399 LIMIT 1",
    );
    assert!(
        automatic.contains("Zone map scan on zm_limit_cost"),
        "an unordered LIMIT must not bypass the cheaper Zone Map candidate: {automatic}"
    );

    let ordered = explain_tree(
        "EXPLAIN FORMAT=TREE SELECT pad FROM zm_limit_cost \
         WHERE id <= 2399 AND target = 0 ORDER BY id DESC LIMIT 1",
    );
    assert!(
        ordered.contains("Zone map scan on zm_limit_cost") && ordered.contains("TopK"),
        "an order-providing range index must compete with Zone Map + TopK: {ordered}"
    );
}

#[test]
fn maintenance_rebuilds_only_stale_zones_and_reports_progress() {
    struct QuietReset<'a>(&'a crate::storage::Database);
    impl Drop for QuietReset<'_> {
        fn drop(&mut self) {
            self.0.set_zone_map_maintenance_quiet_ms_for_tests(30_000);
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE zm_targeted (id INT PRIMARY KEY, v INT, pad VARCHAR(800)) \
             ZONE_MAP=ON ZONE_SIZE=64K",
        )
        .unwrap();
    for base in (0..600).step_by(50) {
        let rows = (base..base + 50)
            .map(|id| format!("({id},{id},'{}')", "x".repeat(700)))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO zm_targeted VALUES {rows}"))
            .unwrap();
    }
    let table = engine.db_for_tests().table("zm_targeted").unwrap();
    engine
        .db_for_tests()
        .rebuild_zone_map_for_table(&table)
        .unwrap();
    let ids = dense_rebuild_zone_ids(&engine, "zm_targeted");
    let selected = ids
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!(
            "UPDATE zm_targeted SET v = v + 10000 WHERE id IN ({selected})"
        ))
        .unwrap();

    let QueryResponse::ResultSet(before) = engine
        .execute(
            "SELECT COUNT(DISTINCT zone_id) FROM information_schema.mydb_zone_maps \
             WHERE table_name='zm_targeted'",
        )
        .unwrap()
    else {
        panic!("expected diagnostics")
    };
    let total_zones = match before.rows[0][0] {
        Value::Int(value) => value as u64,
        Value::UInt(value) => value,
        ref other => panic!("unexpected zone count {other:?}"),
    };
    assert!(total_zones > 1);

    engine
        .db_for_tests()
        .set_zone_map_maintenance_quiet_ms_for_tests(0);
    let _reset = QuietReset(engine.db_for_tests());
    assert_eq!(engine.db_for_tests().maintain_zone_maps().unwrap(), 1);
    let QueryResponse::ResultSet(progress) = engine
        .execute(
            "SELECT rebuild_state,rebuild_zones_total,rebuild_zones_done \
             FROM information_schema.mydb_zone_map_storage \
             WHERE table_name='zm_targeted'",
        )
        .unwrap()
    else {
        panic!("expected storage progress")
    };
    assert_eq!(progress.rows[0][0], Value::String("COMPLETE".into()));
    assert_eq!(progress.rows[0][1], progress.rows[0][2]);
    let rebuilt_zones = match progress.rows[0][1] {
        Value::UInt(value) => value,
        Value::Int(value) => value as u64,
        ref other => panic!("unexpected rebuilt zone count {other:?}"),
    };
    assert!(
        rebuilt_zones > 0 && rebuilt_zones < total_zones,
        "maintenance should rebuild a strict subset: rebuilt={rebuilt_zones} total={total_zones}"
    );

    let zone = engine
        .execute(
            "SELECT /*+ ZONE_MAP(zm_targeted) */ COUNT(*) FROM zm_targeted \
             WHERE v BETWEEN 10000 AND 10063",
        )
        .unwrap();
    let oracle = engine
        .execute(
            "SELECT /*+ NO_ZONE_MAP(zm_targeted) */ COUNT(*) FROM zm_targeted \
             WHERE v BETWEEN 10000 AND 10063",
        )
        .unwrap();
    assert_eq!(format!("{zone:?}"), format!("{oracle:?}"));
}

#[test]
fn maintenance_cancellation_keeps_conservative_unknown_zones_and_resumes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE zm_cancel (id INT PRIMARY KEY, v INT, pad VARCHAR(800)) \
             ZONE_MAP=ON ZONE_SIZE=64K",
        )
        .unwrap();
    for base in (0..600).step_by(50) {
        let rows = (base..base + 50)
            .map(|id| format!("({id},{id},'{}')", "x".repeat(700)))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO zm_cancel VALUES {rows}"))
            .unwrap();
    }
    let table = engine.db_for_tests().table("zm_cancel").unwrap();
    // Simulate the production restart state that triggered the bug: the heap
    // is populated but no trusted sidecar generation was loaded.
    engine.db_for_tests().forget_zone_map(table.id).unwrap();

    // Cancel after the Unknown base is initialized but before the first
    // chunk completes. The resulting base must remain safe and retryable.
    let probes = std::sync::atomic::AtomicUsize::new(0);
    let rebuilt = engine
        .db_for_tests()
        .maintain_zone_maps_until(|| probes.fetch_add(1, std::sync::atomic::Ordering::SeqCst) >= 4)
        .unwrap();
    assert_eq!(rebuilt, 0);
    let observed_probes = probes.load(std::sync::atomic::Ordering::SeqCst);
    assert!(
        observed_probes > 4,
        "expected cancellation during maintenance, probes={observed_probes}"
    );

    let QueryResponse::ResultSet(cancelled) = engine
        .execute(
            "SELECT state,rebuild_state,rebuild_zones_total,rebuild_zones_done \
             FROM information_schema.mydb_zone_map_storage \
             WHERE table_name='zm_cancel'",
        )
        .unwrap()
    else {
        panic!("expected cancelled rebuild diagnostics")
    };
    // The base is trusted, but unbuilt zones remain explicitly Unknown.
    // No partial summary can exclude rows, and the next pass can resume.
    assert_eq!(cancelled.rows[0][0], Value::String("PENDING".into()));
    let QueryResponse::ResultSet(remaining) = engine
        .execute(
            "SELECT /*+ ZONE_MAP(zm_cancel) */ COUNT(*) FROM zm_cancel WHERE v BETWEEN 0 AND 599",
        )
        .unwrap()
    else {
        panic!("expected count")
    };
    assert!(matches!(
        remaining.rows[0][0],
        Value::Int(600) | Value::UInt(600)
    ));
    assert_eq!(cancelled.rows[0][1], Value::String("IDLE".into()));
    assert_ne!(cancelled.rows[0][2], cancelled.rows[0][3]);

    // Cancellation is retryable: an ordinary maintenance pass must rebuild
    // and publish the complete trusted generation.
    assert_eq!(engine.db_for_tests().maintain_zone_maps().unwrap(), 1);
    let QueryResponse::ResultSet(completed) = engine
        .execute(
            "SELECT state,rebuild_state,rebuild_zones_total,rebuild_zones_done \
             FROM information_schema.mydb_zone_map_storage \
             WHERE table_name='zm_cancel'",
        )
        .unwrap()
    else {
        panic!("expected completed rebuild diagnostics")
    };
    assert_eq!(completed.rows[0][0], Value::String("READY".into()));
    assert_eq!(completed.rows[0][1], Value::String("COMPLETE".into()));
    assert_eq!(completed.rows[0][2], completed.rows[0][3]);
}

#[test]
fn optimize_table_synchronously_rebuilds_zone_map_and_reports_completion() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE zm_optimize(id INT PRIMARY KEY, v INT) \
             ZONE_MAP=ON ZONE_SIZE=64K",
        )
        .unwrap();
    engine
        .execute("INSERT INTO zm_optimize VALUES (1,10),(2,20),(3,30)")
        .unwrap();
    engine
        .execute("UPDATE zm_optimize SET v=999 WHERE id=1")
        .unwrap();
    engine.execute("OPTIMIZE TABLE zm_optimize").unwrap();
    let QueryResponse::ResultSet(progress) = engine
        .execute(
            "SELECT rebuild_state,rebuild_zones_total,rebuild_zones_done \
             FROM information_schema.mydb_zone_map_storage \
             WHERE table_name='zm_optimize'",
        )
        .unwrap()
    else {
        panic!("expected progress row")
    };
    assert_eq!(progress.rows[0][0], Value::String("COMPLETE".into()));
    assert_eq!(progress.rows[0][1], progress.rows[0][2]);
}

#[test]
fn targeted_rebuild_merges_racing_dml_instead_of_publishing_a_false_negative() {
    struct Reset(Arc<StorageQueryEngine>);
    impl Drop for Reset {
        fn drop(&mut self) {
            self.0
                .db_for_tests()
                .set_zone_map_rebuild_publish_barrier_for_tests(None);
            self.0
                .db_for_tests()
                .set_zone_map_maintenance_quiet_ms_for_tests(30_000);
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(open_test_engine(dir.path()));
    engine
        .execute(
            "CREATE TABLE zm_race (id INT PRIMARY KEY, v INT, pad VARCHAR(800)) \
             ZONE_MAP=ON ZONE_SIZE=64K",
        )
        .unwrap();
    let rows = (0..300)
        .map(|id| format!("({id},{id},'{}')", "r".repeat(700)))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO zm_race VALUES {rows}"))
        .unwrap();
    let table = engine.db_for_tests().table("zm_race").unwrap();
    engine
        .db_for_tests()
        .rebuild_zone_map_for_table(&table)
        .unwrap();
    let ids = dense_rebuild_zone_ids(&engine, "zm_race");
    let racing_id = ids[ids.len() / 2];
    let selected = ids
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!(
            "UPDATE zm_race SET v=v+1000 WHERE id IN ({selected})"
        ))
        .unwrap();
    engine
        .db_for_tests()
        .set_zone_map_maintenance_quiet_ms_for_tests(0);
    let barrier = Arc::new(std::sync::Barrier::new(2));
    engine
        .db_for_tests()
        .set_zone_map_rebuild_publish_barrier_for_tests(Some(Arc::clone(&barrier)));
    let _reset = Reset(Arc::clone(&engine));
    let worker = Arc::clone(&engine);
    let join = std::thread::spawn(move || worker.db_for_tests().maintain_zone_maps());

    barrier.wait();
    engine
        .execute(&format!("UPDATE zm_race SET v=999999 WHERE id={racing_id}"))
        .unwrap();
    barrier.wait();
    join.join().unwrap().unwrap();

    let zone = engine
        .execute("SELECT /*+ ZONE_MAP(zm_race) */ id FROM zm_race WHERE v=999999")
        .unwrap();
    let oracle = engine
        .execute("SELECT /*+ NO_ZONE_MAP(zm_race) */ id FROM zm_race WHERE v=999999")
        .unwrap();
    assert_eq!(format!("{zone:?}"), format!("{oracle:?}"));
}

#[test]
fn explain_analyze_reports_zone_map_runtime_breakdown() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE zm_runtime (id INT, pad VARCHAR(700)) \
             ZONE_MAP=ON ZONE_SIZE=64K",
        )
        .unwrap();
    let rows = (0..300)
        .map(|id| format!("({id},'{}')", "x".repeat(600)))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO zm_runtime VALUES {rows}"))
        .unwrap();

    let QueryResponse::ResultSet(tree) = engine
        .execute(
            "EXPLAIN ANALYZE FORMAT=TREE \
             SELECT /*+ ZONE_MAP(zm_runtime) NO_PARALLEL */ id \
             FROM zm_runtime WHERE id BETWEEN 250 AND 255",
        )
        .unwrap()
    else {
        panic!("expected TREE explain")
    };
    let tree_text = tree.rows[0][0].to_display_string();
    assert!(
        tree_text.contains("zone_candidate_ms=")
            && tree_text.contains("zone_heap_ms=")
            && tree_text.contains("zone_tasks="),
        "missing Zone Map runtime detail: {tree_text}"
    );

    let QueryResponse::ResultSet(json) = engine
        .execute(
            "EXPLAIN ANALYZE FORMAT=JSON \
             SELECT /*+ ZONE_MAP(zm_runtime) */ id \
             FROM zm_runtime WHERE id BETWEEN 250 AND 255",
        )
        .unwrap()
    else {
        panic!("expected JSON explain")
    };
    let json_text = json.rows[0][0].to_display_string();
    assert!(
        json_text.contains("\"zone_candidate_ms\"")
            && json_text.contains("\"zone_pages\"")
            && json_text.contains("\"zone_fallbacks\""),
        "missing Zone Map JSON runtime detail: {json_text}"
    );
}

#[test]
fn zone_map_cost_and_runtime_ignore_apply_only_conjuncts() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE zm_apply_split (id INT, pad VARCHAR(700)) \
             ZONE_MAP=ON ZONE_SIZE=64K",
        )
        .unwrap();
    let rows = (0..300)
        .map(|id| format!("({id},'{}')", "x".repeat(600)))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO zm_apply_split VALUES {rows}"))
        .unwrap();

    let QueryResponse::ResultSet(tree) = engine
        .execute(
            "EXPLAIN ANALYZE FORMAT=TREE \
             SELECT /*+ ZONE_MAP(zm_apply_split) NO_PARALLEL */ id \
             FROM zm_apply_split \
             WHERE id BETWEEN 250 AND 299 \
               AND id >= (SELECT ROUND(RAND(0) * (\
                   SELECT MAX(id) FROM zm_apply_split\
               )))",
        )
        .unwrap()
    else {
        panic!("expected TREE explain")
    };
    let tree = tree.rows[0][0].to_display_string();
    let scan = tree
        .lines()
        .find(|line| line.contains("Zone map scan on zm_apply_split"))
        .expect("zone-map scan line");
    assert!(
        scan.contains("filter:") && !scan.contains("zone_fallbacks="),
        "the scan-safe range must remain executable by Zone Map: {tree}"
    );
    let apply = tree
        .lines()
        .find(|line| line.contains("Apply"))
        .expect("Apply line");
    assert!(
        apply.contains("condition: id>=<expr>")
            && !apply.contains("AND")
            && !tree.contains("zone_fallbacks="),
        "the runtime must not fall back after costing a scan-safe zone predicate: {tree}"
    );
}

#[test]
fn zone_maps_diagnostics_pushes_filters_into_lazy_column_loading() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        let columns = (0..64)
            .map(|index| format!("c{index} INT"))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!(
                "CREATE TABLE zm_diag (id INT,{columns}) ZONE_MAP=ON ZONE_SIZE=64K"
            ))
            .unwrap();
        // A bulk insert deliberately leaves optional bounds Unknown. Build
        // real summaries spanning many leaves; omitted NONE records must not
        // turn this into a two-page root/metadata-only fixture.
        insert_repeated_integer_series(&engine, "zm_diag", 12000, 65);
        engine.execute("ANALYZE TABLE zm_diag").unwrap();
        engine.db_for_tests().checkpoint().unwrap();
    }

    let engine = open_strict_test_engine(dir.path());
    let resident_pages = || {
        engine
            .db_for_tests()
            .zone_map_status_rows()
            .into_iter()
            .find(|(name, _)| *name == "zone_map_resident_pages")
            .expect("shared zone-map residency metric")
            .1
            .parse::<u64>()
            .unwrap()
    };
    let frames_before = resident_pages();
    let QueryResponse::ResultSet(filtered) = engine
        .execute(
            "SELECT column_name FROM information_schema.mydb_zone_maps \
             WHERE table_schema='test' AND table_name='zm_diag' AND column_name='id'",
        )
        .unwrap()
    else {
        panic!("expected filtered diagnostics")
    };
    assert!(
        !filtered.rows.is_empty()
            && filtered
                .rows
                .iter()
                .all(|row| row[0] == Value::String("id".into()))
    );
    assert!(filtered.rows.len() >= 16, "fixture must span many zones");
    let frames_one_column = resident_pages();

    let QueryResponse::ResultSet(all_columns) = engine
        .execute(
            "SELECT column_name FROM information_schema.mydb_zone_maps \
             WHERE table_schema='test' AND table_name='zm_diag'",
        )
        .unwrap()
    else {
        panic!("expected all-column diagnostics")
    };
    assert_eq!(all_columns.rows.len(), filtered.rows.len() * 65);
    let frames_all_columns = resident_pages();
    let plan = match engine
        .execute(
            "EXPLAIN FORMAT=TREE SELECT column_name FROM information_schema.mydb_zone_maps \
             WHERE table_schema='test' AND table_name='zm_diag' AND column_name='id'",
        )
        .unwrap()
    {
        QueryResponse::ResultSet(result) => format!("{:?}", result.rows),
        other => format!("{other:?}"),
    };
    assert!(
        frames_one_column > frames_before && frames_all_columns > frames_one_column,
        "filtered diagnostics should touch fewer pages than all columns: before={frames_before} one={frames_one_column} all={frames_all_columns}; plan={plan}"
    );
}

#[test]
#[ignore = "effect benchmark; run explicitly on an otherwise idle host"]
fn zone_map_dml_and_load_data_effect_benchmark() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    for (name, enabled) in [("bench_on", "ON"), ("bench_off", "OFF")] {
        engine
            .execute(&format!(
                "CREATE TABLE {name}(id INT, v BIGINT, pad VARCHAR(128)) \
                 ZONE_MAP={enabled} ZONE_SIZE=64K"
            ))
            .unwrap();
    }
    let mut on_ns = 0u128;
    let mut off_ns = 0u128;
    for batch in 0..20 {
        for (name, elapsed) in if batch % 2 == 0 {
            [("bench_on", &mut on_ns), ("bench_off", &mut off_ns)]
        } else {
            [("bench_off", &mut off_ns), ("bench_on", &mut on_ns)]
        } {
            let base = batch * 1_000;
            let values = (base..base + 1_000)
                .map(|id| format!("({id},{},'{}')", id * 10, "x".repeat(96)))
                .collect::<Vec<_>>()
                .join(",");
            let started = std::time::Instant::now();
            engine
                .execute(&format!("INSERT INTO {name} VALUES {values}"))
                .unwrap();
            *elapsed += started.elapsed().as_nanos();
        }
    }

    let load_path = dir.path().join("zone-map-load.csv");
    let csv = (0..10_000)
        .map(|id| format!("{id},{},{}\n", id * 10, "y".repeat(96)))
        .collect::<String>();
    std::fs::write(&load_path, csv).unwrap();
    for (name, enabled) in [("load_on", "ON"), ("load_off", "OFF")] {
        engine
            .execute(&format!(
                "CREATE TABLE {name}(id INT, v BIGINT, pad VARCHAR(128)) \
                 ZONE_MAP={enabled} ZONE_SIZE=64K"
            ))
            .unwrap();
    }
    let started = std::time::Instant::now();
    engine
        .execute(&format!(
            "LOAD DATA INFILE '{}' INTO TABLE load_off \
             FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'",
            crate::util::file::sql_path(&load_path)
        ))
        .unwrap();
    let load_off_ns = started.elapsed().as_nanos();
    let started = std::time::Instant::now();
    engine
        .execute(&format!(
            "LOAD DATA INFILE '{}' INTO TABLE load_on \
             FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'",
            crate::util::file::sql_path(&load_path)
        ))
        .unwrap();
    let load_on_ns = started.elapsed().as_nanos();

    let dml_regression = on_ns as f64 / off_ns.max(1) as f64 - 1.0;
    let load_regression = load_on_ns as f64 / load_off_ns.max(1) as f64 - 1.0;
    eprintln!(
        "zone-map-effect dml_on_ms={:.3} dml_off_ms={:.3} dml_regression_pct={:.2} load_on_ms={:.3} load_off_ms={:.3} load_regression_pct={:.2}",
        on_ns as f64 / 1_000_000.0,
        off_ns as f64 / 1_000_000.0,
        dml_regression * 100.0,
        load_on_ns as f64 / 1_000_000.0,
        load_off_ns as f64 / 1_000_000.0,
        load_regression * 100.0,
    );
    let count = |table: &str| {
        let QueryResponse::ResultSet(rows) = engine
            .execute(&format!("SELECT COUNT(*) FROM {table}"))
            .unwrap()
        else {
            panic!("expected count result")
        };
        rows.rows[0][0].clone()
    };
    assert_eq!(count("bench_on"), count("bench_off"));
    assert_eq!(count("load_on"), count("load_off"));
}

#[test]
fn parallel_zone_map_scan_schedules_surviving_pages_and_matches_serial() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let padding_columns = (0..32)
        .map(|index| format!("p{index} BIGINT"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!(
            "CREATE TABLE zm_parallel (id INT,{padding_columns}) \
             ZONE_MAP=ON ZONE_SIZE=64K"
        ))
        .unwrap();
    insert_repeated_integer_series(&engine, "zm_parallel", 399, 33);

    let QueryResponse::ResultSet(explain) = engine
        .execute(
            "EXPLAIN FORMAT=TREE SELECT /*+ ZONE_MAP(zm_parallel) PARALLEL(4) */ id \
             FROM zm_parallel WHERE id BETWEEN 300 AND 399",
        )
        .unwrap()
    else {
        panic!("expected explain")
    };
    let explain_text = explain
        .rows
        .iter()
        .flatten()
        .map(Value::to_display_string)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        explain_text.contains("Parallel zone map scan"),
        "expected explicit parallel Zone Map operator, got: {explain_text}"
    );
    let QueryResponse::ResultSet(explain_json) = engine
        .execute(
            "EXPLAIN FORMAT=JSON SELECT /*+ ZONE_MAP(zm_parallel) PARALLEL(4) */ id \
             FROM zm_parallel WHERE id BETWEEN 300 AND 399",
        )
        .unwrap()
    else {
        panic!("expected JSON explain")
    };
    let explain_json = explain_json
        .rows
        .iter()
        .flatten()
        .map(Value::to_display_string)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        explain_json.contains("\"parallel_zone_map\"")
            && explain_json.contains("\"access_type\": \"zone\""),
        "expected parallel zone access in JSON explain, got: {explain_json}"
    );

    let skipped_before = engine
        .db_for_tests()
        .zone_map_status_rows()
        .into_iter()
        .find(|(name, _)| *name == "zone_map_zones_skipped")
        .and_then(|(_, value)| value.parse::<u64>().ok())
        .unwrap_or(0);

    let QueryResponse::ResultSet(parallel) = engine
        .execute(
            "SELECT /*+ ZONE_MAP(zm_parallel) PARALLEL(4) */ id \
             FROM zm_parallel WHERE id BETWEEN 300 AND 399",
        )
        .unwrap()
    else {
        panic!("expected parallel result")
    };
    let QueryResponse::ResultSet(serial) = engine
        .execute(
            "SELECT /*+ NO_ZONE_MAP(zm_parallel) NO_PARALLEL */ id \
             FROM zm_parallel WHERE id BETWEEN 300 AND 399",
        )
        .unwrap()
    else {
        panic!("expected serial result")
    };
    let mut parallel_rows = parallel.rows;
    let mut serial_rows = serial.rows;
    parallel_rows.sort_by_key(|row| row[0].to_display_string());
    serial_rows.sort_by_key(|row| row[0].to_display_string());
    assert_eq!(parallel_rows, serial_rows);
    assert_eq!(parallel_rows.len(), 100);

    // Aggregate execution consumes the same dynamic Zone Map morsels through
    // the fused row/vector aggregate path rather than materialising the scan.
    let QueryResponse::ResultSet(parallel_count) = engine
        .execute(
            "SELECT /*+ ZONE_MAP(zm_parallel) PARALLEL(4) */ COUNT(*) \
             FROM zm_parallel WHERE id BETWEEN 300 AND 399",
        )
        .unwrap()
    else {
        panic!("expected parallel aggregate result")
    };
    assert_eq!(parallel_count.rows[0][0], Value::Int(100));

    let analyze = explain_analyze_tree(
        &engine,
        "SELECT /*+ ZONE_MAP(zm_parallel) PARALLEL(4) */ id \
         FROM zm_parallel WHERE id BETWEEN 300 AND 399",
    );
    assert!(
        analyze.contains("execution_mode=vector")
            && analyze.contains("workers=4")
            && analyze.contains("zone_pages="),
        "parallel Zone Map runtime must be visible on the generic scan node: {analyze}"
    );
    let skipped_after = engine
        .db_for_tests()
        .zone_map_status_rows()
        .into_iter()
        .find(|(name, _)| *name == "zone_map_zones_skipped")
        .and_then(|(_, value)| value.parse::<u64>().ok())
        .unwrap_or(0);
    assert!(
        skipped_after > skipped_before,
        "parallel Zone Map scan must skip disproven zones"
    );
}

#[test]
fn partitioned_zone_map_prunes_each_storage_for_serial_projected_and_parallel_scans() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let padding_columns = (0..24)
        .map(|index| format!("p{index} BIGINT"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!(
            "CREATE TABLE zm_part (id INT,{padding_columns}) \
             ZONE_MAP=ON ZONE_SIZE=64K \
             PARTITION BY RANGE (id) (\
               PARTITION p0 VALUES LESS THAN (1000),\
               PARTITION p1 VALUES LESS THAN MAXVALUE)"
        ))
        .unwrap();

    // INSERT..SELECT keeps the fixture compact and avoids parsing hundreds of
    // repeated 25-column VALUES tuples while still creating multiple zones
    // in each physical partition.
    let projection = std::iter::repeat_n("n", 25).collect::<Vec<_>>().join(",");
    for start in [0, 1000] {
        engine
            .execute(&format!(
                "INSERT INTO zm_part SELECT {projection} \
                 FROM generate_series({start}, {}) AS gs(n)",
                start + 399
            ))
            .unwrap();
    }
    let table = engine.db_for_tests().table("zm_part").unwrap();
    engine
        .db_for_tests()
        .rebuild_zone_map_for_table(&table)
        .unwrap();

    let skipped_before = engine
        .db_for_tests()
        .zone_map_status_rows()
        .into_iter()
        .find(|(name, _)| *name == "zone_map_zones_skipped")
        .and_then(|(_, value)| value.parse::<u64>().ok())
        .unwrap_or(0);
    let on = engine
        .execute(
            "SELECT /*+ ZONE_MAP(zm_part) */ id FROM zm_part \
             WHERE id BETWEEN 1350 AND 1355 ORDER BY id",
        )
        .unwrap();
    let skipped_after = engine
        .db_for_tests()
        .zone_map_status_rows()
        .into_iter()
        .find(|(name, _)| *name == "zone_map_zones_skipped")
        .and_then(|(_, value)| value.parse::<u64>().ok())
        .unwrap_or(0);
    assert!(
        skipped_after > skipped_before,
        "partition ZoneMapScan must skip zones"
    );

    engine
        .execute("SET optimizer_switch='zone_map=off'")
        .unwrap();
    let off = engine
        .execute("SELECT id FROM zm_part WHERE id BETWEEN 1350 AND 1355 ORDER BY id")
        .unwrap();
    assert_eq!(format!("{on:?}"), format!("{off:?}"));

    engine
        .execute("SET optimizer_switch='zone_map=on'")
        .unwrap();
    let parallel = engine
        .execute(
            "SELECT /*+ ZONE_MAP(zm_part) PARALLEL(2) */ id FROM zm_part \
             WHERE id BETWEEN 1350 AND 1355 ORDER BY id",
        )
        .unwrap();
    assert_eq!(format!("{on:?}"), format!("{parallel:?}"));

    let QueryResponse::ResultSet(explain) = engine
        .execute(
            "EXPLAIN FORMAT=JSON SELECT /*+ ZONE_MAP(zm_part) */ id \
             FROM zm_part WHERE id BETWEEN 1350 AND 1355",
        )
        .unwrap()
    else {
        panic!("expected explain")
    };
    assert!(explain.rows.iter().flatten().any(|value| value
        .to_display_string()
        .contains("\"access_type\": \"zone\"")));

    let QueryResponse::ResultSet(diagnostics) = engine
        .execute(
            "SELECT storage_id FROM information_schema.mydb_zone_maps \
             WHERE table_schema='test' AND table_name='zm_part' AND column_name='id'",
        )
        .unwrap()
    else {
        panic!("expected diagnostics")
    };
    let storage_ids = diagnostics
        .rows
        .into_iter()
        .filter_map(|row| row.into_iter().next())
        .map(|value| value.to_display_string())
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(
        storage_ids.len(),
        2,
        "one map is required per partition heap"
    );
}

#[test]
fn exchange_partition_moves_zone_map_sidecars_and_restart_stays_conservative() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute(
                "CREATE TABLE zm_exchange (id INT, pad VARCHAR(800)) \
                 ZONE_MAP=ON ZONE_SIZE=64K PARTITION BY RANGE(id) (\
                   PARTITION p0 VALUES LESS THAN (1000),\
                   PARTITION p1 VALUES LESS THAN MAXVALUE)",
            )
            .unwrap();
        engine
            .execute(
                "CREATE TABLE zm_stage (id INT, pad VARCHAR(800)) \
                 ZONE_MAP=ON ZONE_SIZE=64K",
            )
            .unwrap();
        for base in (0..400).step_by(50) {
            let rows = (base..base + 50)
                .map(|id| format!("({id},'{}')", "p".repeat(700)))
                .collect::<Vec<_>>()
                .join(",");
            engine
                .execute(&format!("INSERT INTO zm_exchange VALUES {rows}"))
                .unwrap();
        }
        let stage_rows = (800..900)
            .map(|id| format!("({id},'{}')", "s".repeat(700)))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO zm_stage VALUES {stage_rows}"))
            .unwrap();
        engine.db_for_tests().checkpoint().unwrap();
        engine
            .execute("ALTER TABLE zm_exchange EXCHANGE PARTITION p0 WITH TABLE zm_stage")
            .unwrap();

        let zone = engine
            .execute(
                "SELECT /*+ ZONE_MAP(zm_exchange) */ id FROM zm_exchange \
                 WHERE id BETWEEN 820 AND 825 ORDER BY id",
            )
            .unwrap();
        let oracle = engine
            .execute(
                "SELECT /*+ NO_ZONE_MAP(zm_exchange) */ id FROM zm_exchange \
                 WHERE id BETWEEN 820 AND 825 ORDER BY id",
            )
            .unwrap();
        assert_eq!(format!("{zone:?}"), format!("{oracle:?}"));
    }

    let engine = open_strict_test_engine(dir.path());
    let zone = engine
        .execute(
            "SELECT /*+ ZONE_MAP(zm_exchange) */ id FROM zm_exchange \
             WHERE id BETWEEN 820 AND 825 ORDER BY id",
        )
        .unwrap();
    let oracle = engine
        .execute(
            "SELECT /*+ NO_ZONE_MAP(zm_exchange) */ id FROM zm_exchange \
             WHERE id BETWEEN 820 AND 825 ORDER BY id",
        )
        .unwrap();
    assert_eq!(format!("{zone:?}"), format!("{oracle:?}"));
}

#[test]
fn reorganize_partition_removes_obsolete_zone_map_sidecars() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE zm_reorg (id INT, pad VARCHAR(700)) \
             ZONE_MAP=ON ZONE_SIZE=64K PARTITION BY RANGE(id) (\
               PARTITION p0 VALUES LESS THAN (100),\
               PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    let rows = (0..100)
        .map(|id| format!("({id},'{}')", "x".repeat(600)))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO zm_reorg VALUES {rows}"))
        .unwrap();
    let table = engine.db_for_tests().table("zm_reorg").unwrap();
    engine
        .db_for_tests()
        .rebuild_zone_map_for_table(&table)
        .unwrap();
    engine.db_for_tests().checkpoint().unwrap();
    let old_sid = table.partitioning.as_ref().unwrap().partitions[0].storage_id;
    let old_sidecar = engine.db_for_tests().zone_map_file_path(old_sid);
    assert!(old_sidecar.exists());

    engine
        .execute(
            "ALTER TABLE zm_reorg REORGANIZE PARTITION p0 INTO (\
               PARTITION p00 VALUES LESS THAN (50),\
               PARTITION p01 VALUES LESS THAN (100))",
        )
        .unwrap();
    assert!(
        !old_sidecar.exists(),
        "obsolete partition Zone Map must be removed with the heap"
    );
    let zone = engine
        .execute(
            "SELECT /*+ ZONE_MAP(zm_reorg) */ COUNT(*) FROM zm_reorg WHERE id BETWEEN 40 AND 60",
        )
        .unwrap();
    let oracle = engine
        .execute(
            "SELECT /*+ NO_ZONE_MAP(zm_reorg) */ COUNT(*) FROM zm_reorg WHERE id BETWEEN 40 AND 60",
        )
        .unwrap();
    assert_eq!(format!("{zone:?}"), format!("{oracle:?}"));
}

#[test]
fn zone_map_partition_kind_differential_matrix() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let definitions = [
        (
            "zm_list",
            "PARTITION BY LIST(bucket) (PARTITION p0 VALUES IN (0), PARTITION p1 VALUES IN (1))",
        ),
        ("zm_hash", "PARTITION BY HASH(bucket) PARTITIONS 2"),
        ("zm_key", "PARTITION BY KEY(bucket) PARTITIONS 2"),
        (
            "zm_sub",
            "PARTITION BY RANGE(id) SUBPARTITION BY HASH(bucket) SUBPARTITIONS 2 (\
             PARTITION p0 VALUES LESS THAN (400), PARTITION p1 VALUES LESS THAN MAXVALUE)",
        ),
    ];

    for (table, partitioning) in definitions {
        engine
            .execute(&format!(
                "CREATE TABLE {table} (id INT, bucket INT, pad VARCHAR(512)) \
                 ZONE_MAP=ON ZONE_SIZE=64K {partitioning}"
            ))
            .unwrap();
        for base in (0..800).step_by(100) {
            let values = (base..base + 100)
                .map(|id| format!("({id},{},'x')", id % 2))
                .collect::<Vec<_>>()
                .join(",");
            engine
                .execute(&format!("INSERT INTO {table} VALUES {values}"))
                .unwrap();
        }
        let zone = engine
            .execute(&format!(
                "SELECT /*+ ZONE_MAP({table}) PARALLEL(2) */ id FROM {table} \
                 WHERE id BETWEEN 790 AND 795 ORDER BY id"
            ))
            .unwrap();
        let seq = engine
            .execute(&format!(
                "SELECT /*+ NO_ZONE_MAP({table}) NO_PARALLEL */ id FROM {table} \
                 WHERE id BETWEEN 790 AND 795 ORDER BY id"
            ))
            .unwrap();
        assert_eq!(format!("{zone:?}"), format!("{seq:?}"), "table={table}");
    }
}

#[test]
fn corrupt_sidecar_falls_back_to_heap_after_restart() {
    fn find_zm(dir: &std::path::Path) -> Option<std::path::PathBuf> {
        for entry in std::fs::read_dir(dir).ok()? {
            let path = entry.ok()?.path();
            if path.is_dir() {
                if let Some(found) = find_zm(&path) {
                    return Some(found);
                }
            } else if path.extension().is_some_and(|ext| ext == "zm") {
                return Some(path);
            }
        }
        None
    }

    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE zmr (id INT) ZONE_MAP=ON ZONE_SIZE=64K")
            .unwrap();
        engine.execute("INSERT INTO zmr VALUES (1),(100)").unwrap();
        engine.db_for_tests().checkpoint().unwrap();
    }
    let sidecar = find_zm(dir.path()).expect("zone sidecar");
    let mut bytes = std::fs::read(&sidecar).unwrap();
    *bytes.last_mut().unwrap() ^= 0x7f;
    std::fs::write(sidecar, bytes).unwrap();

    let engine = open_strict_test_engine(dir.path());
    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT id FROM zmr WHERE id=100").unwrap()
    else {
        panic!("expected result")
    };
    assert_eq!(result.rows, vec![vec![Value::Int(100)]]);
}

#[test]
fn zone_map_mvcc_rollback_savepoint_and_long_snapshot_are_conservative() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE zmm (id INT PRIMARY KEY, v INT) ZONE_MAP=ON ZONE_SIZE=64K")
        .unwrap();
    engine
        .execute("INSERT INTO zmm VALUES (1,10),(2,20)")
        .unwrap();

    let mut reader = SessionContext::default();
    let mut writer = SessionContext::default();
    // These are distinct connections; sharing an MDL owner lets the writer's
    // COMMIT release the reader's transaction protection.
    writer.connection_id = 2;
    engine
        .execute_with_session(
            &mut reader,
            "SET SESSION transaction_isolation = 'REPEATABLE-READ'",
        )
        .unwrap();
    engine.execute_with_session(&mut reader, "BEGIN").unwrap();
    // Establish the RR snapshot before the concurrent writer changes values.
    engine
        .execute_with_session(
            &mut reader,
            "SELECT /*+ ZONE_MAP(zmm) */ v FROM zmm WHERE id=1",
        )
        .unwrap();

    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut writer, "UPDATE zmm SET v=1000 WHERE id=1")
        .unwrap();
    engine
        .execute_with_session(&mut writer, "SAVEPOINT sp")
        .unwrap();
    engine
        .execute_with_session(&mut writer, "UPDATE zmm SET v=-1000 WHERE id=2")
        .unwrap();
    engine
        .execute_with_session(&mut writer, "ROLLBACK TO SAVEPOINT sp")
        .unwrap();
    engine.execute_with_session(&mut writer, "COMMIT").unwrap();

    // The old value must remain reachable through undo for the long snapshot;
    // rolled-back extrema may remain in the summary but cannot affect rows.
    let QueryResponse::ResultSet(old) = engine
        .execute_with_session(
            &mut reader,
            "SELECT /*+ ZONE_MAP(zmm) */ v FROM zmm WHERE v=10",
        )
        .unwrap()
    else {
        panic!("expected result")
    };
    assert_eq!(old.rows, vec![vec![Value::Int(10)]]);
    engine
        .execute_with_session(&mut reader, "ROLLBACK")
        .unwrap();

    let mut aborted = SessionContext::default();
    engine.execute_with_session(&mut aborted, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut aborted, "INSERT INTO zmm VALUES (3,999999)")
        .unwrap();
    engine
        .execute_with_session(&mut aborted, "ROLLBACK")
        .unwrap();
    let QueryResponse::ResultSet(absent) = engine
        .execute("SELECT /*+ ZONE_MAP(zmm) */ id FROM zmm WHERE v=999999")
        .unwrap()
    else {
        panic!("expected result")
    };
    assert!(absent.rows.is_empty());
}

#[test]
fn zone_map_audit_literal_domains() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE zm_domains (id INT, f FLOAT, d DOUBLE, y YEAR, dt DATETIME, e ENUM('a','c','b'), s SET('a','c','b'), pad VARCHAR(700)) ZONE_SIZE=64K").unwrap();
    engine.execute("INSERT INTO zm_domains SELECT n, 5.25, 5.25, 2024, '2024-01-01', 'c', 'c', REPEAT('x',600) FROM generate_series(1,500) AS gs(n)").unwrap();
    engine.execute("ANALYZE TABLE zm_domains").unwrap();
    for predicate in [
        "d=5.25",
        "d>6",
        "f<5.2500001",
        "f>5.2499999",
        "y=2024",
        "y>2024",
        "dt=20240101",
        "e<'b'",
        "s<'b'",
        "id=9999 OR d=6.25",
        "id=NULL",
        "id IN (NULL)",
    ] {
        let query = |hint: &str| {
            engine.execute(&format!("SELECT /*+ {hint}(zm_domains) */ id FROM zm_domains WHERE {predicate} ORDER BY id")).unwrap()
        };
        assert_eq!(
            format!("{:?}", query("ZONE_MAP")),
            format!("{:?}", query("NO_ZONE_MAP")),
            "{predicate}"
        );
        let plan = engine.execute(&format!("EXPLAIN FORMAT=TREE SELECT /*+ ZONE_MAP(zm_domains) */ id FROM zm_domains WHERE {predicate}")).unwrap();
        let dump = format!("{plan:?}");
        assert!(
            !dump.contains("cannot safely produce zone ranges"),
            "{predicate}: {dump}"
        );
        if matches!(
            predicate,
            "d>6" | "y>2024" | "id=9999 OR d=6.25" | "id=NULL" | "id IN (NULL)"
        ) {
            assert!(
                dump.contains("pages=0/"),
                "must actually prune all pages: {predicate}: {dump}"
            );
        }
    }
    for predicate in [
        "f<>5.2500001",
        "f IN (5.25,5.2500001,NULL)",
        "f BETWEEN 5.2499999 AND 5.2500001",
        "y=24",
        "y>24",
        "dt > (20240100+1)",
        "dt > CAST(20240101 AS UNSIGNED)",
        "id=9999 OR LENGTH(pad)=600",
        "e BETWEEN 'a' AND 'b'",
        "s BETWEEN 'a' AND 'b'",
    ] {
        let query = |hint: &str| {
            engine.execute(&format!("SELECT /*+ {hint}(zm_domains) */ id FROM zm_domains WHERE {predicate} ORDER BY id")).unwrap()
        };
        assert_eq!(
            format!("{:?}", query("ZONE_MAP")),
            format!("{:?}", query("NO_ZONE_MAP")),
            "{predicate}"
        );
    }
}

#[test]
fn zone_map_audit_diagnostics_and_settings() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE zm_observe (id INT, d DATE, dt DATETIME, t TIME, y YEAR) ZONE_SIZE=64K",
        )
        .unwrap();
    engine
        .execute(&format!(
            "INSERT INTO zm_observe VALUES {}",
            (1..=400)
                .map(|n| format!("({n},'2024-01-01','2024-01-01 12:00:00','12:34:56',2024)"))
                .collect::<Vec<_>>()
                .join(",")
        ))
        .unwrap();
    let dump = |sql: &str| format!("{:?}", engine.execute(sql).unwrap());
    let storage = dump(
        "SELECT state FROM information_schema.mydb_zone_map_storage WHERE table_name='zm_observe'",
    );
    assert!(storage.contains("PENDING"), "{storage}");
    assert!(dump("SELECT state FROM information_schema.mydb_zone_maps WHERE table_name='zm_observe' AND column_name='dt'").contains("Unknown"));
    engine.execute("ANALYZE TABLE zm_observe").unwrap();
    for (column, expected) in [
        ("d", "2024-01-01"),
        ("dt", "2024-01-01 12:00:00"),
        ("t", "12:34:56"),
        ("y", "2024"),
    ] {
        assert!(dump(&format!("SELECT min_value FROM information_schema.mydb_zone_maps WHERE table_name='zm_observe' AND column_name='{column}'")).contains(expected));
    }
    for name in ["quiet", "retry"] {
        let key = format!("mydb_zone_map_maintenance_{name}_ms");
        engine.execute(&format!("SET GLOBAL {key}=123")).unwrap();
        assert!(dump(&format!("SELECT @@GLOBAL.{key}")).contains("123"));
        assert!(engine
            .execute(&format!("SET GLOBAL {key}=86400001"))
            .is_err());
        engine.execute(&format!("SET GLOBAL {key}=30000")).unwrap();
    }
}

fn zone_switch_test_dir() -> tempfile::TempDir {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    tempfile::Builder::new()
        .prefix("zone-switch-")
        .tempdir_in("/tmp/mydb")
        .unwrap()
}

#[test]
fn zone_map_global_switch_sql_plans_and_maintenance() {
    let dir = zone_switch_test_dir();
    let engine = open_test_engine(dir.path());
    let db = engine.db_for_tests();
    assert!(db.zone_map_enabled());
    let padding = (0..32)
        .map(|i| format!("p{i} BIGINT"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!(
            "CREATE TABLE zm_switch (id INT,{padding}) ZONE_MAP=ON ZONE_SIZE=64K"
        ))
        .unwrap();
    insert_repeated_integer_series(&engine, "zm_switch", 399, 33);
    engine.execute("ANALYZE TABLE zm_switch").unwrap();
    let sql = "SELECT /*+ ZONE_MAP(zm_switch) */ id FROM zm_switch WHERE id BETWEEN 350 AND 360 ORDER BY id";
    let expected = engine.execute(sql).unwrap();
    let plan = engine
        .execute(&format!("EXPLAIN FORMAT=JSON {sql}"))
        .unwrap();
    assert!(format!("{plan:?}").contains("zone"));
    engine
        .execute("SET GLOBAL mydb_zone_map_enabled=OFF")
        .unwrap();
    assert!(!db.zone_map_enabled());
    for hint in [
        "ZONE_MAP(zm_switch)",
        "ZONE_MAP(zm_switch) PARALLEL(4)",
        "NO_PARALLEL",
    ] {
        let sql = format!(
            "SELECT /*+ {hint} */ id FROM zm_switch WHERE id BETWEEN 350 AND 360 ORDER BY id"
        );
        assert_eq!(
            format!("{:?}", engine.execute(&sql).unwrap()),
            format!("{expected:?}")
        );
        let plan = engine
            .execute(&format!("EXPLAIN FORMAT=JSON {sql}"))
            .unwrap();
        let QueryResponse::ResultSet(result) = plan else {
            panic!("expected plan")
        };
        assert!(!result
            .rows
            .iter()
            .flatten()
            .any(|v| v.to_display_string().contains("\"access_type\": \"zone\"")));
    }
    engine
        .execute("CREATE TABLE zm_new (v INT) ZONE_MAP=ON")
        .unwrap();
    engine
        .execute("INSERT INTO zm_new VALUES (1),(2),(NULL)")
        .unwrap();
    engine.execute("UPDATE zm_new SET v=100 WHERE v=1").unwrap();
    engine.execute("DELETE FROM zm_new WHERE v=2").unwrap();
    engine.execute("ANALYZE TABLE zm_switch,zm_new").unwrap();
    assert_eq!(db.maintain_zone_maps().unwrap(), 0);
    assert!(db.zone_map_diagnostic_rows().is_empty());
    assert!(db.zone_map_storage_rows().is_empty());
    assert!(engine
        .execute("SET SESSION mydb_zone_map_enabled=ON")
        .is_err());
    assert!(engine
        .execute("SET GLOBAL mydb_zone_map_enabled='invalid'")
        .is_err());
    engine
        .execute("SET GLOBAL mydb_zone_map_enabled=DEFAULT")
        .unwrap();
    assert!(db.zone_map_enabled());
    engine.execute("ANALYZE TABLE zm_switch,zm_new").unwrap();
    assert!(!db.zone_map_diagnostic_rows().is_empty());
    assert_eq!(
        format!("{:?}", engine.execute(sql).unwrap()),
        format!("{expected:?}")
    );
}

#[test]
fn zone_map_global_switch_reopen_never_resurrects_old_bounds() {
    let dir = zone_switch_test_dir();
    let open = || {
        let mut config = memory_test_storage_config();
        config.durability = crate::storage::DurabilityMode::Strict;
        StorageQueryEngine::open_with_storage_config_without_background_workers(dir.path(), config)
            .unwrap()
    };
    {
        let engine = open();
        engine
            .execute("CREATE TABLE zm_restart (id INT PRIMARY KEY, v INT) ZONE_SIZE=64K")
            .unwrap();
        engine
            .execute("INSERT INTO zm_restart VALUES (1,1),(2,2)")
            .unwrap();
        engine.execute("ANALYZE TABLE zm_restart").unwrap();
        engine.db_for_tests().checkpoint().unwrap();
        engine
            .execute("SET GLOBAL mydb_zone_map_enabled=OFF")
            .unwrap();
        engine
            .execute("UPDATE zm_restart SET v=999999 WHERE id=1")
            .unwrap();
        engine
            .execute("INSERT INTO zm_restart VALUES (3,-999999)")
            .unwrap();
        // Plain drop simulates a restart without a shutdown checkpoint.
    }
    let engine = open();
    assert!(engine.db_for_tests().zone_map_enabled());
    for predicate in ["v=999999", "v=-999999", "v IS NULL", "v<>2"] {
        let query = |hint| {
            engine.execute(&format!("SELECT /*+ {hint}(zm_restart) */ id FROM zm_restart WHERE {predicate} ORDER BY id")).unwrap()
        };
        assert_eq!(
            format!("{:?}", query("ZONE_MAP")),
            format!("{:?}", query("NO_ZONE_MAP"))
        );
    }
    engine.execute("ANALYZE TABLE zm_restart").unwrap();
    let QueryResponse::ResultSet(result) = engine.execute("SELECT /*+ ZONE_MAP(zm_restart) */ COUNT(*) FROM zm_restart WHERE v IN (999999,-999999)").unwrap() else { panic!("result") };
    assert_eq!(result.rows[0][0].to_display_string(), "2");
}

#[test]
fn zone_map_global_switch_long_transaction_and_persist() {
    let dir = zone_switch_test_dir();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE zm_txn (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO zm_txn VALUES (1,1)").unwrap();
    engine.execute("ANALYZE TABLE zm_txn").unwrap();
    engine
        .execute("SET GLOBAL mydb_zone_map_enabled=OFF")
        .unwrap();
    let mut writer = SessionContext::default();
    writer.connection_id = 452;
    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut writer, "UPDATE zm_txn SET v=888 WHERE id=1")
        .unwrap();
    engine
        .execute("SET GLOBAL mydb_zone_map_enabled=ON")
        .unwrap();
    engine.execute("ANALYZE TABLE zm_txn").unwrap();
    let sql = "SELECT /*+ ZONE_MAP(zm_txn) */ COUNT(*) FROM zm_txn WHERE v=888";
    let QueryResponse::ResultSet(result) = engine.execute_with_session(&mut writer, sql).unwrap()
    else {
        panic!("result")
    };
    assert_eq!(result.rows[0][0].to_display_string(), "1");
    engine.execute_with_session(&mut writer, "COMMIT").unwrap();
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("result")
    };
    assert_eq!(result.rows[0][0].to_display_string(), "1");
    engine
        .execute("SET PERSIST_ONLY mydb_zone_map_enabled=OFF")
        .unwrap();
    assert!(engine.db_for_tests().zone_map_enabled());
    // Config startup is also checked without reopening memory-mode heap data.
    let persisted = engine
        .db_for_tests()
        .persisted_variables()
        .get("mydb_zone_map_enabled")
        .unwrap();
    assert_eq!(persisted.value, "OFF");
    engine
        .execute("SET PERSIST mydb_zone_map_enabled=OFF")
        .unwrap();
    assert!(!engine.db_for_tests().zone_map_enabled());
    engine
        .execute("RESET PERSIST mydb_zone_map_enabled")
        .unwrap();
    assert!(engine
        .db_for_tests()
        .persisted_variables()
        .get("mydb_zone_map_enabled")
        .is_none());
}

#[test]
fn zone_map_global_switch_prepared_partition_and_load() {
    let dir = zone_switch_test_dir();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE zm_part (id INT, v INT) PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN MAXVALUE)").unwrap();
    engine
        .execute("INSERT INTO zm_part VALUES (1,1),(11,11)")
        .unwrap();
    engine.execute("ANALYZE TABLE zm_part").unwrap();
    let sql = "SELECT /*+ ZONE_MAP(zm_part) PARALLEL(4) */ v FROM zm_part WHERE v=?";
    let mut session = SessionContext::default();
    let planned = engine.prepare_cached(&mut session, sql, 1).unwrap();
    let run = |session: &mut SessionContext, value| {
        crate::engine::params::with_params(vec![Value::Int(value)], || {
            engine
                .execute_prepared_statement(session, sql, (*planned).clone())
                .unwrap()
        })
        .unwrap()
        .materialize()
        .unwrap()
    };
    let expected = run(&mut session, 11);
    engine
        .execute("SET GLOBAL mydb_zone_map_enabled=OFF")
        .unwrap();
    assert_eq!(
        format!("{:?}", run(&mut session, 11)),
        format!("{expected:?}")
    );
    engine
        .execute("UPDATE zm_part SET v=999 WHERE id=11")
        .unwrap();
    let result = run(&mut session, 999);
    assert!(format!("{result:?}").contains("999"));
    let file = dir.path().join("input.tsv");
    std::fs::write(&file, "2\t200\n12\t1200\n").unwrap();
    engine
        .execute(&format!(
            "LOAD DATA INFILE '{}' INTO TABLE zm_part",
            crate::util::file::sql_path(&file)
        ))
        .unwrap();
    engine.execute("ANALYZE TABLE zm_part").unwrap();
    assert!(engine.db_for_tests().zone_map_storage_rows().is_empty());
    for value in [200, 1200] {
        assert!(format!("{:?}", run(&mut session, value)).contains(&value.to_string()));
    }
    engine
        .execute("SET GLOBAL mydb_zone_map_enabled=ON")
        .unwrap();
    engine.execute("ANALYZE TABLE zm_part").unwrap();
    for value in [999, 200, 1200] {
        assert!(format!("{:?}", run(&mut session, value)).contains(&value.to_string()));
    }
}

#[test]
fn zone_map_global_switch_persisted_off_is_applied_before_loading() {
    let dir = zone_switch_test_dir();
    let open = || {
        let mut config = memory_test_storage_config();
        config.durability = crate::storage::DurabilityMode::Strict;
        StorageQueryEngine::open_with_storage_config_without_background_workers(dir.path(), config)
            .unwrap()
    };
    {
        let engine = open();
        engine.execute("CREATE TABLE zm_persist (v INT)").unwrap();
        engine.execute("INSERT INTO zm_persist VALUES (1)").unwrap();
        engine.execute("ANALYZE TABLE zm_persist").unwrap();
        engine.db_for_tests().checkpoint().unwrap();
        engine
            .execute("SET PERSIST_ONLY mydb_zone_map_enabled=OFF")
            .unwrap();
        assert!(engine.db_for_tests().zone_map_enabled());
    }
    {
        let engine = open();
        assert!(!engine.db_for_tests().zone_map_enabled());
        assert!(engine.db_for_tests().zone_map_storage_rows().is_empty());
        engine
            .execute("INSERT INTO zm_persist VALUES (9000)")
            .unwrap();
        engine
            .execute("SET PERSIST mydb_zone_map_enabled=ON")
            .unwrap();
        engine.db_for_tests().checkpoint().unwrap();
    }
    let engine = open();
    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT /*+ ZONE_MAP(zm_persist) */ COUNT(*) FROM zm_persist WHERE v=9000")
        .unwrap()
    else {
        panic!("result")
    };
    assert_eq!(rows.rows[0][0].to_display_string(), "1");
}

#[test]
fn zone_map_paged_rebuild_preserves_old_snapshot_and_rollback_extrema() {
    let dir = zone_switch_test_dir();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE zm_rebuild_snapshot (id INT PRIMARY KEY, v INT) ZONE_MAP=ON ZONE_SIZE=64K").unwrap();
    engine
        .execute("INSERT INTO zm_rebuild_snapshot VALUES (1,10),(2,20)")
        .unwrap();
    engine.execute("ANALYZE TABLE zm_rebuild_snapshot").unwrap();
    let mut reader = SessionContext {
        connection_id: 9101,
        ..SessionContext::default()
    };
    let mut writer = SessionContext {
        connection_id: 9102,
        ..SessionContext::default()
    };
    engine
        .execute_with_session(
            &mut reader,
            "SET SESSION transaction_isolation='REPEATABLE-READ'",
        )
        .unwrap();
    engine.execute_with_session(&mut reader, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut reader, "SELECT * FROM zm_rebuild_snapshot")
        .unwrap();
    engine
        .execute_with_session(
            &mut writer,
            "UPDATE zm_rebuild_snapshot SET v=100000 WHERE id=1",
        )
        .unwrap();
    // Rebuilding the current heap must not remove the old snapshot's value.
    // Call the storage entry directly: SQL ANALYZE can wait on the reader's MDL.
    let table = engine
        .db_for_tests()
        .table("test.zm_rebuild_snapshot")
        .unwrap();
    engine
        .db_for_tests()
        .rebuild_zone_map_for_table(&table)
        .unwrap();
    let QueryResponse::ResultSet(result) = engine.execute_with_session(&mut reader,
        "SELECT /*+ ZONE_MAP(zm_rebuild_snapshot) */ id,v FROM zm_rebuild_snapshot WHERE v=10 ORDER BY id").unwrap()
    else { panic!("expected result set") };
    assert_eq!(result.rows, vec![vec![Value::Int(1), Value::Int(10)]]);
    engine
        .execute_with_session(&mut reader, "ROLLBACK")
        .unwrap();
    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
    engine
        .execute_with_session(
            &mut writer,
            "UPDATE zm_rebuild_snapshot SET v=-100000 WHERE id=2",
        )
        .unwrap();
    engine
        .db_for_tests()
        .rebuild_zone_map_for_table(&table)
        .unwrap();
    engine
        .execute_with_session(&mut writer, "ROLLBACK")
        .unwrap();
    for predicate in ["v=20", "v=100000", "v=-100000", "v BETWEEN 10 AND 100000"] {
        let query = |hint| {
            engine.execute(&format!("SELECT /*+ {hint}(zm_rebuild_snapshot) */ id,v FROM zm_rebuild_snapshot WHERE {predicate} ORDER BY id")).unwrap()
        };
        let QueryResponse::ResultSet(actual) = query("ZONE_MAP") else {
            panic!("result set")
        };
        let QueryResponse::ResultSet(expected) = query("NO_ZONE_MAP") else {
            panic!("result set")
        };
        assert_eq!(actual.rows, expected.rows, "{predicate}");
    }
}

#[test]
fn zone_map_paged_xa_checkpoint_reopen_commit_and_rollback() {
    let dir = zone_switch_test_dir();
    let open = || {
        let mut config = memory_test_storage_config();
        config.durability = crate::storage::DurabilityMode::Strict;
        StorageQueryEngine::open_with_storage_config_without_background_workers(dir.path(), config)
            .unwrap()
    };
    {
        let engine = open();
        engine.execute("CREATE TABLE zm_xa_checkpoint (id INT PRIMARY KEY, v INT) ZONE_MAP=ON ZONE_SIZE=64K").unwrap();
        engine
            .execute("INSERT INTO zm_xa_checkpoint VALUES (1,10),(2,20)")
            .unwrap();
        engine.execute("ANALYZE TABLE zm_xa_checkpoint").unwrap();
        for (connection_id, xid, id, value) in [
            (9111, "zm-commit", 1, 100000),
            (9112, "zm-rollback", 2, -100000),
        ] {
            let mut session = SessionContext {
                connection_id,
                ..SessionContext::default()
            };
            for sql in [
                format!("XA START '{xid}'"),
                format!("UPDATE zm_xa_checkpoint SET v={value} WHERE id={id}"),
                format!("XA END '{xid}'"),
                format!("XA PREPARE '{xid}'"),
            ] {
                engine.execute_with_session(&mut session, &sql).unwrap();
            }
        }
        engine.db_for_tests().checkpoint().unwrap();
    }
    {
        let engine = open();
        engine.execute("XA COMMIT 'zm-commit'").unwrap();
        engine.execute("XA ROLLBACK 'zm-rollback'").unwrap();
        engine.db_for_tests().checkpoint().unwrap();
    }
    let engine = open();
    for (predicate, rows) in [
        ("v=100000", vec![vec![Value::Int(1), Value::Int(100000)]]),
        ("v=20", vec![vec![Value::Int(2), Value::Int(20)]]),
        ("v=-100000", vec![]),
    ] {
        for hint in ["ZONE_MAP", "NO_ZONE_MAP"] {
            let QueryResponse::ResultSet(actual) = engine.execute(&format!("SELECT /*+ {hint}(zm_xa_checkpoint) */ id,v FROM zm_xa_checkpoint WHERE {predicate} ORDER BY id")).unwrap() else { panic!("result set") };
            assert_eq!(actual.rows, rows, "{hint}: {predicate}");
        }
    }
}
