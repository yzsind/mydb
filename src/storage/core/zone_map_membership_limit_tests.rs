use super::*;

fn fixture(columns: &str) -> (tempfile::TempDir, crate::engine::StorageQueryEngine) {
    fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let mut config = StorageConfig::from_env();
    config.buffer_pool_pages = 256;
    config.buffer_pool_prefetch_pages = 0;
    let engine =
        crate::engine::StorageQueryEngine::open_with_storage_config_without_background_workers(
            dir.path(),
            config,
        )
        .unwrap();
    engine
        .execute(&format!(
            "CREATE TABLE t ({columns}) ZONE_MAP=ON ZONE_SIZE=64K"
        ))
        .unwrap();
    (dir, engine)
}

fn assert_scan_matches(engine: &crate::engine::StorageQueryEngine, predicate: &str) {
    let query = |hint: &str| {
        engine
            .execute(&format!(
                "SELECT /*+ {hint}(t) NO_PARALLEL */ COUNT(*) FROM t WHERE {predicate}"
            ))
            .unwrap()
            .materialize()
            .unwrap()
    };
    assert_eq!(
        format!("{:?}", query("ZONE_MAP")),
        format!("{:?}", query("NO_ZONE_MAP")),
        "{predicate}"
    );
}

#[test]
fn membership_limit_32_survives_analyze_rebuild_and_reload() {
    let columns = (0..40)
        .map(|i| format!("c{i} INT"))
        .collect::<Vec<_>>()
        .join(",");
    let (_dir, engine) = fixture(&format!("id INT PRIMARY KEY,{columns}"));
    let values = (0..512)
        .map(|id| {
            let cells = std::iter::once(id.to_string())
                .chain((0..40).map(|_| ((id % 128) * 2).to_string()))
                .collect::<Vec<_>>();
            format!("({})", cells.join(","))
        })
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO t VALUES {values}"))
        .unwrap();
    engine.execute("ANALYZE TABLE t").unwrap();
    let db = engine.db_for_tests();
    let table = db.snapshot_table_schema("test.t").unwrap();
    let selected = zone_map_membership_columns(&table);
    let expected = table.columns[1..=32]
        .iter()
        .map(|column| column.column_id)
        .collect::<BTreeSet<_>>();
    assert_eq!(selected.keys().copied().collect::<BTreeSet<_>>(), expected);

    for stage in 0..3 {
        if stage == 1 {
            db.rebuild_zone_map_for_table(&table).unwrap();
        } else if stage == 2 {
            db.checkpoint().unwrap();
            db.forget_zone_map(table.id).unwrap();
        }
        let map = db.zone_map_for(table.id, &table).unwrap();
        let zones = map.snapshot_for_tests();
        assert!(!zones.is_empty());
        let mut populated = 0;
        for zone in zones.values() {
            if !zone.columns.values().any(|summary| summary.has_non_null) {
                continue;
            }
            populated += 1;
            for column in &table.columns {
                let summary = &zone.columns[&column.column_id];
                assert_eq!(
                    summary.membership.is_some(),
                    expected.contains(&column.column_id),
                    "stage={stage}, column={}",
                    column.name
                );
                if expected.contains(&column.column_id) {
                    assert!(!equality_may_match(summary, &ZoneScalar::Signed(127)));
                }
            }
        }
        assert!(populated > 0);
        for predicate in [
            "c8=126",
            "c31=126",
            "c31=127",
            "c39=127",
            "c31 IN (126,127)",
        ] {
            assert_scan_matches(&engine, predicate);
        }
    }
    engine.execute("UPDATE t SET c31=127 WHERE id=0").unwrap();
    assert_scan_matches(&engine, "c31=127");
    engine.execute("UPDATE t SET c31=NULL WHERE id=1").unwrap();
    assert_scan_matches(&engine, "c31 IS NULL");
    engine.execute("DELETE FROM t WHERE id=0").unwrap();
    assert_scan_matches(&engine, "c31=127");

    // Equal-width candidates follow marginal benefit, not physical order.
    // The per-zone byte budget may admit fewer than the 32-column maximum.
    let mut ranked = (*table).clone();
    for i in 0..40 {
        ranked
            .stats
            .columns
            .get_mut(&format!("c{i}"))
            .unwrap()
            .distinct_count = 1_000 + i;
    }
    let ranked_ids = zone_map_membership_columns(&ranked)
        .into_keys()
        .collect::<BTreeSet<_>>();
    assert_eq!(
        ranked_ids,
        ranked.columns[1..]
            .iter()
            .rev()
            .take(ranked_ids.len())
            .map(|column| column.column_id)
            .collect()
    );
}

#[test]
fn membership_limit_small_tables_keep_all_eligible_columns() {
    for width in 1..=8 {
        let columns = (0..width)
            .map(|i| format!("c{i} BIGINT"))
            .collect::<Vec<_>>()
            .join(",");
        let (_dir, engine) = fixture(&columns);
        let db = engine.db_for_tests();
        let fresh = db.snapshot_table_schema("test.t").unwrap();
        assert_eq!(zone_map_provisional_membership_columns(&fresh).len(), width);
        let rows = (0..128)
            .map(|n| format!("({})", vec![(n * 2).to_string(); width].join(",")))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO t VALUES {rows}"))
            .unwrap();
        engine.execute("ANALYZE TABLE t").unwrap();
        let table = db.snapshot_table_schema("test.t").unwrap();
        let layouts = zone_map_membership_columns(&table);
        assert_eq!(layouts.len(), width);
        let worker = ZoneMapWorkerBatch::for_table(&table);
        assert_eq!(worker.entries.len(), width);
        assert_eq!(
            worker
                .entries
                .iter()
                .filter(|c| c.membership.is_some())
                .count(),
            width
        );
        for zone in db
            .zone_map_for(table.id, &table)
            .unwrap()
            .snapshot_for_tests()
            .values()
        {
            for summary in zone.columns.values().filter(|s| s.has_non_null) {
                assert!(summary.membership.is_some());
                assert!(!equality_may_match(summary, &ZoneScalar::Signed(127)));
            }
        }
        assert_scan_matches(&engine, &format!("c{}=126", width - 1));
        assert_scan_matches(&engine, &format!("c{}=127", width - 1));
    }
}

#[test]
fn membership_limit_rebuild_reserves_only_actual_candidates() {
    let (_dir, engine) = fixture(
        "id INT PRIMARY KEY, v INT, missing INT, z INT, all_null INT, txt TEXT, j JSON, b BLOB",
    );
    let db = engine.db_for_tests();
    let original = db.snapshot_table_schema("test.t").unwrap();
    let map = db.zone_map_for(original.id, &original).unwrap();
    let relevant = zone_map_relevant_columns(&original);
    let projected = relevant.iter().map(|(index, _)| *index).collect::<Vec<_>>();
    let base = relevant.len()
        * (std::mem::size_of::<WorkerColumnState>()
            + std::mem::size_of::<u16>()
            + std::mem::size_of::<&crate::charset::CollationInfo>()
            + 4 * std::mem::size_of::<Value>())
        + original.columns.len() * 128;
    let column_stats = crate::storage::ColumnStats {
        null_count: 0,
        distinct_count: 10,
        min: None,
        max: None,
        top_values: Vec::new(),
        frequency: Default::default(),
        average_width_bytes: None,
        histogram: Vec::new(),
    };
    for analyzed in [false, true] {
        let mut table = (*original).clone();
        table.stats.analyzed_at_unix_ms = analyzed.then_some(1);
        table.stats.row_count = 100;
        table.stats.heap_page_count = 1;
        for name in ["id", "v", "z", "all_null", "txt", "j", "b"] {
            table
                .stats
                .columns
                .insert(name.into(), column_stats.clone());
        }
        table.stats.columns.get_mut("z").unwrap().distinct_count = 0;
        table
            .stats
            .columns
            .get_mut("all_null")
            .unwrap()
            .distinct_count = 0;
        let candidates = usize::from(analyzed);
        assert_eq!(zone_map_membership_columns(&table).len(), candidates);
        let limit_bytes = base + zone_tree_bytes::<u32, ZoneMembershipLayout>(candidates);
        struct Limit(u64);
        impl Drop for Limit {
            fn drop(&mut self) {
                ZONE_MAP_TEST_SCRATCH_LIMIT.store(self.0, Ordering::SeqCst);
            }
        }
        let _limit = Limit(ZONE_MAP_TEST_SCRATCH_LIMIT.swap(limit_bytes as u64, Ordering::SeqCst));
        // An empty scan isolates configuration admission. Charging 32 slots,
        // all eight schema columns, or all five relevant columns must fail.
        let txn = TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted);
        assert!(db
            .rebuild_zone_map_scan_range(
                &table,
                table.id,
                &map,
                txn,
                txn.snapshot_scn,
                &relevant,
                &projected,
                0,
                0,
                &|| false,
            )
            .unwrap()
            .is_some());
    }
}
