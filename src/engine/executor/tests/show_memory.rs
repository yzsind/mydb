use super::*;

fn rows(engine: &StorageQueryEngine, sql: &str) -> crate::engine::api::ResultSet {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    result
}

fn u64_value(value: &Value) -> u64 {
    match value {
        Value::UInt(value) => *value,
        Value::Int(value) if *value >= 0 => *value as u64,
        other => panic!("expected unsigned integer, got {other:?}"),
    }
}

#[test]
fn show_mydb_memory_has_stable_shape_and_non_overlapping_breakdown() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE mem_t (id INT PRIMARY KEY, v VARCHAR(200), KEY idx_v(v))")
        .unwrap();
    engine
        .execute("INSERT INTO mem_t VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")
        .unwrap();
    for _ in 0..2 {
        let _ = engine
            .execute("SELECT a.id FROM mem_t a JOIN mem_t b ON a.id=b.id WHERE a.v='alpha'")
            .unwrap();
    }

    let result = rows(&engine, "SHOW MYDB MEMORY");
    assert_eq!(
        result
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "CATEGORY",
            "COMPONENT",
            "CURRENT_BYTES",
            "PEAK_BYTES",
            "LIMIT_BYTES",
            "USED_PCT",
            "HITS",
            "MISSES",
            "HIT_PCT",
            "ENTRIES",
            "EVICTIONS",
            "ACCOUNTING",
            "IN_TRACKED_TOTAL",
            "SCOPE",
            "COUNTER_SCOPE",
            "PARENT_COMPONENT",
            "REJECTIONS",
        ]
    );
    let find = |name: &str| {
        let name = match name {
            "process_rss" if cfg!(windows) => "process_working_set",
            "process_private_footprint" if cfg!(windows) => "process_private_bytes",
            "rss_minus_tracked" if cfg!(windows) => "working_set_minus_tracked",
            other => other,
        };
        result
            .rows
            .iter()
            .find(|row| row[1] == Value::String(name.to_string()))
            .unwrap_or_else(|| panic!("missing component {name}"))
    };
    for component in [
        "host_memory",
        "process_rss",
        "raw_total",
        "raw_active",
        "raw_recycled",
        "data_raw",
        "index_raw",
        "lob_raw",
        "vector_raw",
        "zone_map_raw",
        "scratch",
        "undo_resident",
        "query_execution",
        "statement_ast",
        "physical_plan",
        "tracked_engine",
        "rss_minus_tracked",
    ] {
        let _ = find(component);
    }
    assert!(
        result.rows.iter().all(|row| {
            !matches!(&row[1], Value::String(component) if component.contains("decoded"))
        }),
        "removed heap-decoder cache rows must not reappear; scan morsels are separately governed"
    );

    let raw_total = u64_value(&find("raw_total")[2]);
    let raw_active = u64_value(&find("raw_active")[2]);
    let raw_recycled = u64_value(&find("raw_recycled")[2]);
    let children = [
        "data_raw",
        "index_raw",
        "lob_raw",
        "vector_raw",
        "zone_map_raw",
    ]
    .into_iter()
    .map(|name| u64_value(&find(name)[2]))
    .sum::<u64>();
    assert_eq!(raw_active, children);
    assert_eq!(raw_total, raw_active + raw_recycled);
    assert_eq!(find("raw_total")[12], Value::String("YES".into()));
    for component in [
        "data_raw",
        "index_raw",
        "lob_raw",
        "vector_raw",
        "zone_map_raw",
    ] {
        assert_eq!(find(component)[11], Value::String("SUBSET".into()));
        assert_eq!(find(component)[12], Value::String("NO".into()));
    }

    let plan = find("physical_plan");
    assert!(u64_value(&plan[9]) > 0);
    assert_ne!(
        plan[8],
        Value::Null,
        "warm plan cache needs a hit percentage"
    );
}

#[test]
fn show_mydb_memory_sessions_is_available_without_process_filtering() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let result = rows(&engine, "SHOW MYDB MEMORY SESSIONS");
    assert_eq!(result.columns.len(), 14);
    assert_eq!(result.columns[0].name, "CONNECTION_ID");
    assert_eq!(result.columns[13].name, "STATE");
}

#[test]
fn show_memory_includes_all_governor_categories_once() {
    use crate::engine::executor::mem_budget::{
        ControlledMemoryCategory as C, ControlledMemoryCharge,
    };
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let categories = [
        ("system_metadata", C::SystemMetadata),
        ("temporal_history", C::TemporalHistory),
        ("protocol_transient", C::ProtocolTransient),
        ("binlog_transient", C::BinlogTransient),
        ("parser_transient", C::ParserTransient),
        (
            "native_replication_transient",
            C::NativeReplicationTransient,
        ),
        ("performance_schema", C::PerformanceSchema),
        ("ddl_online_log", C::DdlOnlineLog),
    ];
    let _charges: Vec<_> = categories
        .iter()
        .map(|(_, category)| ControlledMemoryCharge::try_new(*category, 1234).unwrap())
        .collect();
    let result = rows(&engine, "SHOW MYDB MEMORY");
    for (name, _) in categories {
        let matches: Vec<_> = result
            .rows
            .iter()
            .filter(|r| r[1] == Value::String(name.into()))
            .collect();
        assert_eq!(matches.len(), 1, "{name}");
        assert!(u64_value(&matches[0][2]) >= 1234, "{name}");
        assert_eq!(matches[0][11], Value::String("CHARGED".into()));
        assert_eq!(matches[0][12], Value::String("YES".into()));
    }
    let total: u64 = result
        .rows
        .iter()
        .filter(|r| r[12] == Value::String("YES".into()))
        .map(|r| u64_value(&r[2]))
        .sum();
    let tracked = result
        .rows
        .iter()
        .find(|r| r[1] == Value::String("tracked_engine".into()))
        .unwrap();
    assert_eq!(total, u64_value(&tracked[2]));
}

#[test]
fn show_memory_does_not_double_count_governed_caches_or_undo() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE memory_accounting (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    engine
        .execute("INSERT INTO memory_accounting VALUES (1, 'before')")
        .unwrap();
    engine.execute("BEGIN").unwrap();
    engine
        .execute("UPDATE memory_accounting SET v = 'after' WHERE id = 1")
        .unwrap();
    // Populate a real governed cache on the observing thread.
    let _regex = crate::engine::func_cache::cached_regex("memory[0-9]+", "").unwrap();
    let result = rows(&engine, "SHOW MYDB MEMORY");
    for name in ["regex_compile", "undo_resident"] {
        let row = result
            .rows
            .iter()
            .find(|row| row[1] == Value::String(name.into()))
            .unwrap();
        assert!(u64_value(&row[2]) > 0, "{name} must be populated");
        assert_eq!(
            row[12],
            Value::String("NO".into()),
            "{name} is already governed"
        );
    }
    engine.execute("ROLLBACK").unwrap();
}

#[test]
fn show_memory_exposes_scope_peaks_and_rejections() {
    use crate::engine::executor::mem_budget::{
        ControlledMemoryCategory as C, ControlledMemoryCharge,
    };
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let charge = ControlledMemoryCharge::try_new(C::ParserTransient, 65536).unwrap();
    drop(charge);
    assert!(ControlledMemoryCharge::try_new(C::ParserTransient, usize::MAX).is_err());
    let result = rows(&engine, "SHOW MYDB MEMORY");
    let find = |name: &str| {
        let name = match name {
            "process_rss" if cfg!(windows) => "process_working_set",
            "process_private_footprint" if cfg!(windows) => "process_private_bytes",
            "rss_minus_tracked" if cfg!(windows) => "working_set_minus_tracked",
            other => other,
        };
        result
            .rows
            .iter()
            .find(|r| r[1] == Value::String(name.into()))
            .unwrap()
    };
    let parser = find("parser_transient");
    assert!(u64_value(&parser[3]) >= 65536);
    assert!(u64_value(&parser[16]) > 0);
    assert_eq!(parser[0], Value::String("parser".into()));
    for name in [
        "expr_program_l1",
        "regex_compile",
        "json_path_compile",
        "json_schema_compile",
        "like_plan",
        "tz_spec_resolve",
    ] {
        let cache = find(name);
        assert_eq!(cache[13], Value::String("thread".into()), "{name}");
        assert_eq!(cache[14], Value::String("process".into()), "{name}");
        assert_eq!(cache[12], Value::String("NO".into()), "{name}");
        assert_eq!(cache[10], Value::Null, "unknown evictions for {name}");
    }
    assert_eq!(find("col_meta_intern")[10], Value::Null);
    assert_eq!(
        find("undo_resident")[15],
        Value::String("system_metadata".into())
    );
    assert_eq!(
        find("work_area_reserved")[11],
        Value::String("RESERVATION".into())
    );
    assert_eq!(find("tracked_engine")[4], Value::Null);
    assert!(!result
        .rows
        .iter()
        .any(|r| r[1] == Value::String("allocator_reclaimer".into())));
    let rss = u64_value(&find("process_rss")[2]) as i128;
    let tracked = u64_value(&find("tracked_engine")[2]) as i128;
    let delta = match &find("rss_minus_tracked")[2] {
        Value::Int(v) => *v as i128,
        Value::UInt(v) => *v as i128,
        other => panic!("unexpected difference {other:?}"),
    };
    assert_eq!(delta, rss - tracked);
}
