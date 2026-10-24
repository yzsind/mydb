//! Smoke tests for the sysvar registry. Tests verifying SQL-level
//! behaviour live alongside the executor (`engine::executor::tests`).

use super::*;
use crate::engine::api::SessionContext;

#[test]
fn pfs_variable_snapshot_dirty_tracking_is_mutation_driven() {
    let mut session = SessionContext::default();
    assert!(session.pfs_variables_need_publish());

    session.mark_pfs_variables_published();
    assert!(!session.pfs_variables_need_publish());

    registry()
        .write(
            "sql_select_limit",
            SetScope::Session,
            &mut session,
            None,
            SysVarValue::UInt(1),
        )
        .unwrap();
    assert_eq!(session.pfs_incremental_variable(), Some("sql_select_limit"));
    // Rewriting the same variable remains a one-entry publication.
    session.mark_pfs_variable_dirty("sql_select_limit");
    assert_eq!(session.pfs_incremental_variable(), Some("sql_select_limit"));
    // Two distinct mutations deliberately fall back to a complete snapshot.
    session.mark_pfs_variable_dirty("unique_checks");
    assert!(session.pfs_incremental_variable().is_none());
    assert!(session.pfs_variables_dirty);
    session.mark_pfs_variables_published();

    session.set_user_variable("answer", crate::types::Value::Int(42));
    assert!(session.pfs_variables_need_publish());
    session.mark_pfs_variables_published();

    // A GLOBAL variable change invalidates every session lazily without
    // touching each registry entry or rebuilding snapshots on ordinary DML.
    bump_pfs_global_variable_epoch();
    assert!(session.pfs_variables_need_publish());
}

#[test]
fn registry_initialises_with_known_variables() {
    let r = registry();
    assert!(r.len() > 100, "registry should hold many variables");
    for name in &[
        "autocommit",
        "sql_mode",
        "character_set_client",
        "character_set_connection",
        "character_set_results",
        "character_set_database",
        "character_set_system",
        "character_set_filesystem",
        "collation_connection",
        "collation_database",
        "collation_server",
        "time_zone",
        "system_time_zone",
        "transaction_isolation",
        "tx_isolation",
        "transaction_read_only",
        "tx_read_only",
        "foreign_key_checks",
        "unique_checks",
        "sql_safe_updates",
        "sql_select_limit",
        "rand_seed1",
        "rand_seed2",
        "max_execution_time",
        "optimizer_switch",
        "explain_format",
        "event_scheduler",
        "version",
        "version_comment",
        "version_compile_os",
        "version_compile_machine",
        "protocol_version",
        "hostname",
        "port",
        "datadir",
        "performance_schema",
        "lower_case_table_names",
        "read_only",
        "super_read_only",
        "offline_mode",
        "wait_timeout",
        "interactive_timeout",
        "max_allowed_packet",
        "max_connections",
        "innodb_buffer_pool_size",
        "innodb_version",
        "default_storage_engine",
        "default_table_encryption",
        "mydb_buffer_pool_pages",
        "mydb_buffer_pool_quota_heap_pct",
        "mydb_buffer_pool_quota_index_pct",
        "mydb_buffer_pool_quota_lob_pct",
        "mydb_buffer_pool_prefetch_pages",
        "log_bin",
        "binlog_format",
        "server_id",
        "general_log",
        "slow_query_log",
    ] {
        assert!(r.contains(name), "missing variable: {name}");
    }
}

#[test]
fn aliases_resolve_to_same_entry() {
    let r = registry();
    assert!(r.contains("tx_isolation"));
    assert!(r.contains("transaction_isolation"));
    let a = r.get("tx_isolation").expect("aliased lookup");
    let b = r.get("transaction_isolation").expect("canonical lookup");
    assert_eq!(a.def.name, b.def.name);
}

#[test]
fn lookups_are_case_insensitive() {
    let r = registry();
    assert!(r.get("Autocommit").is_some());
    assert!(r.get("AUTOCOMMIT").is_some());
    assert!(r.get("autocommit").is_some());
}

#[test]
fn unknown_variable_returns_1193() {
    let mut s = SessionContext::default();
    let err = registry()
        .write(
            "this_variable_does_not_exist",
            SetScope::Default,
            &mut s,
            None,
            SysVarValue::Int(1),
        )
        .unwrap_err();
    assert_eq!(err.code(), 1193);
}

#[test]
fn read_only_variable_rejects_set() {
    let mut s = SessionContext::default();
    let err = registry()
        .write(
            "version",
            SetScope::Default,
            &mut s,
            None,
            SysVarValue::String("9.0".to_string()),
        )
        .unwrap_err();
    assert_eq!(err.code(), 1238);
}

#[test]
fn global_only_variable_rejects_bare_set() {
    let mut s = SessionContext::default();
    let err = registry()
        .write(
            "read_only",
            SetScope::Default,
            &mut s,
            None,
            SysVarValue::Bool(true),
        )
        .unwrap_err();
    assert_eq!(err.code(), 1229);
}

#[test]
fn session_only_variable_rejects_set_global() {
    let mut s = SessionContext::default();
    let err = registry()
        .write(
            "sql_log_off",
            SetScope::Global,
            &mut s,
            None,
            SysVarValue::Bool(true),
        )
        .unwrap_err();
    assert_eq!(err.code(), 1228);
}

#[test]
fn autocommit_round_trips() {
    let mut s = SessionContext::default();
    assert_eq!(
        registry()
            .read("autocommit", Scope::Session, Some(&s), None)
            .unwrap(),
        SysVarValue::Bool(true)
    );
    registry()
        .write(
            "autocommit",
            SetScope::Session,
            &mut s,
            None,
            SysVarValue::Bool(false),
        )
        .unwrap();
    assert_eq!(
        registry()
            .read("autocommit", Scope::Session, Some(&s), None)
            .unwrap(),
        SysVarValue::Bool(false)
    );
}

#[test]
fn sql_select_limit_global_and_session_storage_are_isolated() {
    let mut issuer = SessionContext::system_default();
    let original = registry()
        .read("sql_select_limit", Scope::Global, Some(&issuer), None)
        .unwrap();

    registry()
        .write(
            "sql_select_limit",
            SetScope::Global,
            &mut issuer,
            None,
            SysVarValue::UInt(41),
        )
        .unwrap();
    let mut first = SessionContext::default();
    let second = SessionContext::default();
    assert_eq!(
        registry()
            .read("sql_select_limit", Scope::Session, Some(&first), None)
            .unwrap(),
        SysVarValue::UInt(41)
    );
    assert_eq!(
        registry()
            .read("sql_select_limit", Scope::Session, Some(&second), None)
            .unwrap(),
        SysVarValue::UInt(41)
    );

    registry()
        .write(
            "sql_select_limit",
            SetScope::Session,
            &mut first,
            None,
            SysVarValue::UInt(7),
        )
        .unwrap();
    assert_eq!(
        registry()
            .read("sql_select_limit", Scope::Session, Some(&first), None)
            .unwrap(),
        SysVarValue::UInt(7)
    );
    assert_eq!(
        registry()
            .read("sql_select_limit", Scope::Global, Some(&first), None)
            .unwrap(),
        SysVarValue::UInt(41)
    );
    assert_eq!(
        registry()
            .read("sql_select_limit", Scope::Session, Some(&second), None)
            .unwrap(),
        SysVarValue::UInt(41)
    );

    registry()
        .write(
            "sql_select_limit",
            SetScope::Global,
            &mut issuer,
            None,
            original,
        )
        .unwrap();
}

#[test]
fn scope_specific_access_policy_is_enforced() {
    let def = SysVarDef::new("scoped", Scope::Both, DataType::Bool)
        .with_scope_access(Access::ReadOnly, Access::ReadWrite);
    let err = def.check_writable(Scope::Session).unwrap_err();
    assert_eq!(err.code(), 1238);
    def.check_writable(Scope::Global).unwrap();
}

#[test]
fn mydb_wal_sync_method_enum_round_trips_and_rejects_invalid() {
    let mut s = SessionContext::default();
    registry()
        .write(
            "mydb_wal_sync_method",
            SetScope::Global,
            &mut s,
            None,
            SysVarValue::String("fullfsync".to_string()),
        )
        .unwrap();
    assert_eq!(
        registry()
            .read("mydb_wal_sync_method", Scope::Global, Some(&s), None)
            .unwrap(),
        SysVarValue::String("fullfsync".to_string())
    );
    let err = registry()
        .write(
            "mydb_wal_sync_method",
            SetScope::Global,
            &mut s,
            None,
            SysVarValue::String("bogus".to_string()),
        )
        .unwrap_err();
    assert_eq!(err.code(), 1231);
    // Restore a benign cheap method so this process-global change does
    // not slow unrelated tests sharing the process (cargo test).
    registry()
        .write(
            "mydb_wal_sync_method",
            SetScope::Global,
            &mut s,
            None,
            SysVarValue::String("fdatasync".to_string()),
        )
        .unwrap();
}

#[test]
fn enum_coercion_accepts_case_insensitive() {
    let mut s = SessionContext::default();
    registry()
        .write(
            "explain_format",
            SetScope::Session,
            &mut s,
            None,
            SysVarValue::String("tree".to_string()),
        )
        .unwrap();
    assert_eq!(
        registry()
            .read("explain_format", Scope::Session, Some(&s), None)
            .unwrap(),
        SysVarValue::String("TREE".to_string())
    );
}

#[test]
fn snapshot_returns_a_value_for_every_variable() {
    let s = SessionContext::default();
    let rows = registry().snapshot_for_show(Scope::Session, Some(&s), None);
    assert_eq!(rows.len(), registry().len());
    for row in &rows {
        // VALUEs may legitimately be empty (`init_connect`), but must be
        // string-rendered without panic.
        let _ = row.value.as_str();
    }
}

#[test]
fn snapshot_is_sorted_by_name() {
    let s = SessionContext::default();
    let rows = registry().snapshot_for_show(Scope::Session, Some(&s), None);
    let mut sorted = rows.clone();
    sorted.sort_by(|a, b| a.name.cmp(b.name));
    assert_eq!(
        rows.iter().map(|r| r.name).collect::<Vec<_>>(),
        sorted.iter().map(|r| r.name).collect::<Vec<_>>()
    );
}

#[test]
fn unique_checks_is_session_writable() {
    let mut s = SessionContext::default();
    registry()
        .write(
            "unique_checks",
            SetScope::Session,
            &mut s,
            None,
            SysVarValue::Bool(false),
        )
        .unwrap();
    assert!(!s.sysvars.unique_checks);
}

#[test]
fn sql_safe_updates_persists_to_session() {
    let mut s = SessionContext::default();
    registry()
        .write(
            "sql_safe_updates",
            SetScope::Session,
            &mut s,
            None,
            SysVarValue::Bool(true),
        )
        .unwrap();
    assert!(s.sysvars.sql_safe_updates);
}

#[test]
fn optimizer_switch_merges_delta() {
    let mut s = SessionContext::default();
    let before = s.sysvars.optimizer_switch.clone();
    assert!(before.contains("hash_join=on"));
    registry()
        .write(
            "optimizer_switch",
            SetScope::Session,
            &mut s,
            None,
            SysVarValue::String("hash_join=off".to_string()),
        )
        .unwrap();
    assert!(s.sysvars.optimizer_switch.contains("hash_join=off"));
    assert!(s.sysvars.optimizer_switch.contains("semijoin=on"));
}

#[test]
fn time_zone_validates_and_canonicalises() {
    let mut s = SessionContext::default();
    registry()
        .write(
            "time_zone",
            SetScope::Session,
            &mut s,
            None,
            SysVarValue::String("+8:00".to_string()),
        )
        .unwrap();
    assert_eq!(s.sysvars.time_zone, "+08:00");
    let bad = registry().write(
        "time_zone",
        SetScope::Session,
        &mut s,
        None,
        SysVarValue::String("!!".to_string()),
    );
    assert!(bad.is_err());
}

#[test]
fn isolation_alias_writes_propagate() {
    let mut s = SessionContext::default();
    registry()
        .write(
            "tx_isolation",
            SetScope::Session,
            &mut s,
            None,
            SysVarValue::String("READ-COMMITTED".to_string()),
        )
        .unwrap();
    assert_eq!(s.isolation, crate::storage::IsolationLevel::ReadCommitted);
}

#[test]
fn character_set_results_accepts_null() {
    let mut s = SessionContext::default();
    registry()
        .write(
            "character_set_results",
            SetScope::Session,
            &mut s,
            None,
            SysVarValue::Null,
        )
        .unwrap();
    assert!(s.character_set_results.is_none());
}

#[test]
fn audit_scope_defaults_and_validation() {
    let r = registry();
    let mut old = SessionContext::system_default();
    let mut peer = SessionContext::default();
    for (name, changed) in [
        ("foreign_key_checks", SysVarValue::Bool(false)),
        ("autocommit", SysVarValue::Bool(false)),
        ("transaction_read_only", SysVarValue::Bool(true)),
        ("unique_checks", SysVarValue::Bool(false)),
        ("max_execution_time", SysVarValue::UInt(4321)),
        ("group_concat_max_len", SysVarValue::UInt(4321)),
        ("default_week_format", SysVarValue::UInt(5)),
        ("div_precision_increment", SysVarValue::UInt(7)),
        ("cte_max_recursion_depth", SysVarValue::UInt(12)),
        ("max_sp_recursion_depth", SysVarValue::UInt(3)),
        ("sql_safe_updates", SysVarValue::Bool(true)),
        ("sql_quote_show_create", SysVarValue::Bool(false)),
        ("sql_notes", SysVarValue::Bool(false)),
        ("tmp_table_size", SysVarValue::UInt(4321)),
        ("sort_buffer_size", SysVarValue::UInt(4321)),
        ("join_buffer_size", SysVarValue::UInt(4321)),
        ("mydb_hnsw_ef_search", SysVarValue::UInt(81)),
        ("mydb_ivf_nprobe", SysVarValue::UInt(11)),
        ("character_set_server", SysVarValue::String("latin1".into())),
        (
            "collation_server",
            SysVarValue::String("latin1_swedish_ci".into()),
        ),
        ("completion_type", SysVarValue::String("CHAIN".into())),
    ] {
        let before = r.read(name, Scope::Session, Some(&old), None).unwrap();
        let peer_before = r.read(name, Scope::Session, Some(&peer), None).unwrap();
        r.write(name, SetScope::Global, &mut old, None, changed.clone())
            .unwrap();
        assert_eq!(
            r.read(name, Scope::Global, Some(&old), None).unwrap(),
            changed,
            "{name}: global"
        );
        assert_eq!(
            r.read(name, Scope::Session, Some(&old), None).unwrap(),
            before,
            "{name}: caller"
        );
        assert_eq!(
            r.read(name, Scope::Session, Some(&peer), None).unwrap(),
            peer_before,
            "{name}: old peer"
        );
        let fresh = SessionContext::default();
        assert_eq!(
            r.read(name, Scope::Session, Some(&fresh), None).unwrap(),
            changed,
            "{name}: inherited"
        );
        r.write(name, SetScope::Session, &mut peer, None, changed.clone())
            .unwrap();
        assert_eq!(
            r.snapshot_for_show(Scope::Global, Some(&peer), None)
                .into_iter()
                .find(|v| v.name == name)
                .unwrap()
                .value,
            changed.to_display_string()
        );
        r.write(
            name,
            SetScope::Global,
            &mut old,
            None,
            r.compiled_default(name).unwrap(),
        )
        .unwrap();
    }
    for name in [
        "max_allowed_packet",
        "max_user_connections",
        "net_buffer_length",
    ] {
        let before = r.read(name, Scope::Global, Some(&old), None).unwrap();
        assert_eq!(
            r.write(
                name,
                SetScope::Session,
                &mut old,
                None,
                SysVarValue::UInt(1024)
            )
            .unwrap_err()
            .code(),
            1238
        );
        assert_eq!(
            r.read(name, Scope::Global, Some(&old), None).unwrap(),
            before
        );
    }
    for value in [
        SysVarValue::Null,
        SysVarValue::String("FOOBAR".into()),
        SysVarValue::String("STRICT_TRANS_TABLE".into()),
    ] {
        assert_eq!(
            r.validate("sql_mode", SetScope::Session, &old, None, value)
                .unwrap_err()
                .code(),
            1231
        );
    }
    for (name, value) in [("wait_timeout", 100), ("sql_select_limit", 123)] {
        let default = r.compiled_default(name).unwrap();
        r.write(
            name,
            SetScope::Global,
            &mut old,
            None,
            SysVarValue::UInt(value),
        )
        .unwrap();
        assert_eq!(r.compiled_default(name).unwrap(), default);
        r.write(name, SetScope::Global, &mut old, None, default)
            .unwrap();
    }
    r.write(
        "net_write_timeout",
        SetScope::Global,
        &mut old,
        None,
        SysVarValue::UInt(0),
    )
    .unwrap();
    assert_eq!(crate::mysql::packet::net_write_timeout(), None);
    r.write(
        "net_write_timeout",
        SetScope::Global,
        &mut old,
        None,
        SysVarValue::UInt(60),
    )
    .unwrap();
}

#[test]
fn zone_map_audit_set_requires_database() {
    let mut session = SessionContext::default();
    for (name, value) in [
        ("mydb_zone_map_enabled", SysVarValue::Bool(false)),
        ("mydb_zone_map_maintenance", SysVarValue::Bool(false)),
        ("mydb_zone_map_maintenance_quiet_ms", SysVarValue::UInt(1)),
        ("mydb_zone_map_maintenance_retry_ms", SysVarValue::UInt(1)),
        ("mydb_zone_map_rebuild_ratio", SysVarValue::Double(0.3)),
    ] {
        let err = registry()
            .write(name, SetScope::Global, &mut session, None, value)
            .expect_err(name);
        assert!(err.message().contains("database"), "{name}: {err:?}");
    }
}

#[test]
fn zone_map_retired_private_cache_variables_are_unknown() {
    let mut session = SessionContext::default();
    for name in [
        "mydb_zone_map_cache_mb",
        "mydb_zone_map_compact_ratio",
        "mydb_zone_map_max_frame_versions",
    ] {
        assert!(!registry().contains(name), "{name}");
        for scope in [SetScope::Global, SetScope::Session] {
            let err = registry()
                .write(name, scope, &mut session, None, SysVarValue::UInt(1))
                .expect_err(name);
            assert_eq!(err.code(), 1193, "{name}: {err:?}");
        }
    }
}

#[test]
fn zone_map_shared_pool_quota_compiled_default_is_zero() {
    assert_eq!(
        registry().compiled_default("mydb_buffer_pool_quota_zone_map_pct"),
        Some(SysVarValue::UInt(0))
    );
}
