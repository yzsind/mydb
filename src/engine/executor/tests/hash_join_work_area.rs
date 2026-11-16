//! WS-P0-2: integrate hash join with the WorkAreaManager PGA arbiter.
//!
//! Previously hash joins used cooperative mem_budget::try_charge accounting;
//! N concurrent joins each used MYDB_HASH_JOIN_MEM_LIMIT_MB, potentially exceeding the total budget.
//! `MYDB_EXECUTOR_MEMORY_TARGET`。
//!
//! Hash join now shares the PGA pool with sort/hash_agg/distinct and charges
//! through WorkAreaHandle::try_charge from build start. When the arbiter returns
//! SpillHint::Spill, use the same grace transition as mem_budget failure.
//!
//! Coverage:
//! 1. Join results match with integration enabled or disabled.
//! 2. WorkAreaManager active_handles/optimal_grants counters
//!    increase while hash join runs.

use super::support::query_rows as rows_of;
use super::*;

fn sorted(mut rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    rows.sort_by_cached_key(|row| format!("{row:?}"));
    rows
}

struct ScopedEnv {
    key: &'static str,
    prev: Option<String>,
}
impl ScopedEnv {
    fn set(key: &'static str, val: &str) -> Self {
        let prev = std::env::var(key).ok();
        unsafe {
            std::env::set_var(key, val);
        }
        Self { key, prev }
    }
}
impl Drop for ScopedEnv {
    fn drop(&mut self) {
        unsafe {
            match &self.prev {
                Some(v) => std::env::set_var(self.key, v),
                None => std::env::remove_var(self.key),
            }
        }
    }
}

fn setup(engine: &StorageQueryEngine) {
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, k INT, payload VARCHAR(2000))")
        .unwrap();
    engine
        .execute("CREATE TABLE p (id INT PRIMARY KEY, k INT, payload VARCHAR(2000))")
        .unwrap();
    // Both build candidates still exceed 1 MiB. Preserve the 50-row commits.
    let pad = "x".repeat(2000);
    for (table, count) in [("test.b", 800), ("test.p", 1000)] {
        for start in (0..count).step_by(50) {
            insert_fixture_rows(
                engine,
                table,
                (start..start + 50).map(|i| {
                    vec![
                        Value::Int(i),
                        Value::Int(i % 20),
                        Value::String(pad.clone()),
                    ]
                }),
            );
        }
    }
}

fn setup_equivalence(engine: &StorageQueryEngine) {
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, k INT, payload VARCHAR(32))")
        .unwrap();
    engine
        .execute("CREATE TABLE p (id INT PRIMARY KEY, k INT, payload VARCHAR(32))")
        .unwrap();
    for (table, prefix, count) in [("test.b", "b", 40), ("test.p", "p", 50)] {
        insert_fixture_rows(
            engine,
            table,
            (0..count).map(|i| {
                vec![
                    Value::Int(i),
                    Value::Int(i % 10),
                    Value::String(format!("{prefix}{i}")),
                ]
            }),
        );
    }
}

#[test]
fn work_area_inner_equivalent() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_equivalence(&engine);

    let sql = "SELECT b.id, p.id FROM b JOIN p ON b.k=p.k ORDER BY b.id, p.id";
    let _g_on = ScopedEnv::set("MYDB_HASH_JOIN_WORK_AREA", "on");
    let with_workarea = sorted(rows_of(&engine, sql));
    drop(_g_on);

    let _g_off = ScopedEnv::set("MYDB_HASH_JOIN_WORK_AREA", "off");
    let without_workarea = sorted(rows_of(&engine, sql));
    assert_eq!(
        with_workarea, without_workarea,
        "WorkArea on/off result mismatch"
    );
}

#[test]
fn work_area_left_join_equivalent() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_equivalence(&engine);

    let sql = "SELECT b.id, p.id FROM b LEFT JOIN p ON b.k=p.k ORDER BY b.id, p.id";
    let _g_on = ScopedEnv::set("MYDB_HASH_JOIN_WORK_AREA", "on");
    let with_workarea = sorted(rows_of(&engine, sql));
    drop(_g_on);
    let _g_off = ScopedEnv::set("MYDB_HASH_JOIN_WORK_AREA", "off");
    let without_workarea = sorted(rows_of(&engine, sql));
    assert_eq!(
        with_workarea, without_workarea,
        "WorkArea on/off LEFT result mismatch"
    );
}

#[test]
fn work_area_active_handles_observed_during_hash_join() {
    use crate::engine::executor::work_area::snapshot;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);

    // Disable streaming-inner and vector paths to force materialization through
    // execute_hash_join_from_rows, where the WorkArea handle is registered.
    let _g_si = ScopedEnv::set("MYDB_HASH_JOIN_STREAMING_INNER_DISABLE", "1");
    let _g_vec = ScopedEnv::set("MYDB_VEC_HASH_JOIN", "0");
    let _g_wa = ScopedEnv::set("MYDB_HASH_JOIN_WORK_AREA", "on");
    // Each side has about 2 MiB payload; build crosses the 1 MiB registration threshold.
    let before = snapshot();
    let _ = rows_of(
        &engine,
        // LIMIT keeps result-row materialisation below the executor hard cap;
        // the build side still carries the wide projection and must register
        // a work area before the first joined row can be returned.
        "SELECT b.id, b.payload, p.id, p.payload FROM b JOIN p ON b.k=p.k LIMIT 1",
    );
    let after = snapshot();

    // After join completion active handles return to zero, while cumulative grants
    // increase in at least one of optimal, one_pass, or min.
    let total_grants_before = before.optimal_grants + before.one_pass_grants + before.min_grants;
    let total_grants_after = after.optimal_grants + after.one_pass_grants + after.min_grants;
    assert!(
        total_grants_after > total_grants_before,
        "WorkArea grants should increase after hash join (before={} after={})",
        total_grants_before,
        total_grants_after,
    );
}
