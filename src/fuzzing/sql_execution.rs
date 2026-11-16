//! Bounded SQL generation with kernel/configuration and TLP oracles.
use super::FuzzBytes;
use crate::engine::executor::vec::config::{enter_mode, VecMode};
use crate::engine::{QueryResponse, SessionContext, StorageQueryEngine};
use crate::storage::{DurabilityMode, StartupValidationMode, StorageConfig};
use crate::types::{Column, Value};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

pub(super) struct Fixture {
    pub(super) engine: Option<StorageQueryEngine>,
    pub(super) path: PathBuf,
    config: StorageConfig,
}
impl Fixture {
    fn new() -> Self {
        Self::with_durability(DurabilityMode::Memory)
    }
    pub(super) fn with_durability(durability: DurabilityMode) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let path = PathBuf::from(format!(
            "/tmp/mydb/sql-fuzz-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        // create_dir refuses to reuse another invocation's data.
        std::fs::create_dir(&path).unwrap();
        let config = StorageConfig {
            durability,
            checkpoint_interval: Duration::from_secs(3600),
            checkpoint_wal_bytes: 64 << 20,
            lock_wait: Duration::from_secs(1),
            group_commit_window: Duration::ZERO,
            wal_segment_bytes: 64 << 20,
            buffer_pool_pages: 512,
            buffer_pool_io_threads: 1,
            buffer_pool_prefetch_pages: 0,
            undo_retention: Duration::ZERO,
            heap_update_reserve_bytes: 2048,
            startup_validation: StartupValidationMode::Off,
            startup_validate_indexes: false,
        };
        let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
            &path,
            config.clone(),
        )
        .unwrap();
        for table in ["fuzz_a", "fuzz_b"] {
            engine
                .execute(&format!(
                "CREATE TABLE {table}(id INT PRIMARY KEY,k INT,d DECIMAL(38,8),s VARCHAR(1024),\
                 t TINYINT,u BIGINT UNSIGNED,f DOUBLE,c CHAR(16),b VARBINARY(64),\
                 dt DATE,ts DATETIME(6),tm TIME,KEY by_k(k))"))
                .unwrap();
        }
        Self {
            engine: Some(engine),
            path,
            config,
        }
    }
    pub(super) fn reopen(&mut self) {
        drop(self.engine.take());
        self.engine = Some(
            StorageQueryEngine::open_with_storage_config_without_background_workers(
                &self.path,
                self.config.clone(),
            )
            .unwrap(),
        );
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        drop(self.engine.take());
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn materialize(response: QueryResponse) -> (Vec<Column>, Vec<Vec<Value>>) {
    match response {
        QueryResponse::ResultSet(r) => (r.columns, r.rows),
        QueryResponse::SharedResultSet(r) => (r.columns.to_vec(), r.rows),
        QueryResponse::StreamingResultSet(mut r) => {
            let mut rows = Vec::new();
            while let Some(row) = r.cursor.next_row().unwrap() {
                rows.push(row);
            }
            (r.columns, rows)
        }
        other => panic!("query did not return rows: {other:?}"),
    }
}

pub(super) fn query(
    engine: &StorageQueryEngine,
    session: &mut SessionContext,
    sql: &str,
) -> (Vec<Column>, Vec<Vec<Value>>) {
    let (columns, mut rows) = materialize(
        engine
            .execute_with_session(session, sql)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}")),
    );
    for row in &mut rows {
        for value in row {
            if matches!(
                value,
                Value::DecimalCompact(..) | Value::Decimal128(..) | Value::DecimalWide(_)
            ) {
                *value = Value::Decimal(value.to_display_string());
            }
        }
    }
    (columns, rows)
}

fn sorted(mut rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    rows.sort_by_cached_key(|row| row.iter().map(Value::to_display_string).collect::<Vec<_>>());
    rows
}

/// Typed column values spanning integer, unsigned, floating-point (including
/// subnormals and the largest finite doubles), multibyte CHAR, binary,
/// temporal and NULL boundaries, all chosen deterministically from bytes.
fn typed_values(k: u8, raw: u64) -> String {
    let t = match k % 9 {
        0 => "NULL".into(),
        1 => "-128".into(),
        2 => "0".into(),
        3 => "127".into(),
        _ => (k as i8).to_string(),
    };
    let u = if k % 11 == 0 {
        "NULL".into()
    } else {
        (raw % 1_000_000_000_000).to_string()
    };
    let f = match k % 13 {
        0 => "NULL".into(),
        1 => "1.7976931348623157e308".into(),
        2 => "-1.7976931348623157e308".into(),
        3 => "5e-324".into(),
        _ => ((k % 200) as i32 - 100).to_string(),
    };
    let c = ["'a'", "'é'", "'𝒜'", "'0123456789abcdef'"][k as usize % 4];
    let b = ["X'00FF7F80'", "X''", "X'DEADBEEF'"][k as usize % 3];
    let dt = if k % 17 == 0 {
        "NULL"
    } else {
        ["'1000-01-01'", "'9999-12-31'", "'2024-02-29'"][k as usize % 3]
    };
    let ts = [
        "'1000-01-01 00:00:00.000000'",
        "'9999-12-31 23:59:59.999999'",
    ][k as usize % 2];
    let tm = ["'-838:59:59'", "'838:59:59'", "'00:00:00'"][k as usize % 3];
    format!("{t},{u},{f},{c},{b},{dt},{ts},{tm}")
}

pub fn sql_execution_differential(bytes: &[u8]) {
    // Fresh catalogs bound WAL/undo/cache growth across arbitrarily long runs.
    // One input is deliberately more expensive than decoder fuzz: keep it bounded.
    let _env = crate::util::env::scoped_vars(&[
        ("MYDB_PLAN_CACHE", Some("off")),
        ("MYDB_SORT_RUN_BYTES", Some("1048576")),
    ]);
    // Spilling is governed by the per-query memory budget, not by the legacy
    // sort_buffer_size sysvar; pin it so the stress union provably spills
    // (about 3 MiB of sorted rows against a 2 MiB budget) while single-table
    // scans stay comfortably inside it.
    crate::engine::executor::mem_budget::set_query_mem_limit_mb(2).unwrap();
    let fixture = Fixture::new();
    let engine = fixture.engine.as_ref().unwrap();
    let mut input = FuzzBytes::new(bytes);
    let family = input.next() % 16;
    let stress = input.next() & 1 != 0;
    let n = if stress {
        1024
    } else {
        8 + input.next() as usize % 24
    };
    let mut session = SessionContext::default();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    for table in ["fuzz_a", "fuzz_b"] {
        let values = (0..n)
            .map(|id| {
                let k = input.next();
                let key = if k % 5 == 0 {
                    "NULL".into()
                } else {
                    ((k as i8 % 9) as i32).to_string()
                };
                let d = input.u64() as i64;
                let typed = typed_values(k, d as u64 ^ (id as u64) << 8);
                format!(
                    "({id},{key},{d}.12345678,REPEAT('{}',{}),{typed})",
                    if k & 1 == 0 { "界a" } else { "Ab " },
                    if stress { 260 } else { k as usize % 8 }
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute_with_session(
                &mut session,
                &format!("INSERT INTO {table} VALUES {values}"),
            )
            .unwrap();
    }
    let threshold = input.next() as i8 % 9;
    let predicate = format!("k>{threshold} AND d<>0");
    let sql = match family {
        0 => format!("SELECT id,k,d,s FROM fuzz_a WHERE {predicate} ORDER BY k,d,id"),
        1 => "SELECT a.id,b.id,a.d+b.d AS total FROM fuzz_a a LEFT JOIN fuzz_b b ON a.k=b.k WHERE a.id<8 ORDER BY a.id,b.id".into(),
        2 => "SELECT k,COUNT(*),SUM(d),MIN(s),MAX(d) FROM fuzz_a GROUP BY k ORDER BY k".into(),
        3 => "SELECT id,k,ROW_NUMBER() OVER(PARTITION BY k ORDER BY d,id) AS rn FROM fuzz_a ORDER BY id".into(),
        4 => "SELECT a.id FROM fuzz_a a WHERE EXISTS(SELECT 1 FROM fuzz_b b WHERE b.k=a.k AND b.d>a.d) ORDER BY a.id".into(),
        5 => "WITH q AS(SELECT k,COUNT(*) AS n FROM fuzz_b GROUP BY k) SELECT a.id,q.n FROM fuzz_a a LEFT JOIN q ON a.k=q.k ORDER BY a.id".into(),
        6 => "SELECT id,s,d FROM fuzz_a UNION ALL SELECT id,s,d FROM fuzz_b ORDER BY s,d,id".into(),
        7 => "SELECT id,COALESCE(k,0),CASE WHEN k IS NULL THEN d ELSE -d END AS x,1/(id-id) AS z FROM fuzz_a ORDER BY id".into(),
        8 => "SELECT id,t,u FROM fuzz_a WHERE t<0 OR t IS NULL ORDER BY t,u,id".into(),
        9 => "SELECT a.id,a.f,b.f AS bf FROM fuzz_a a JOIN fuzz_b b ON a.id=b.id ORDER BY a.id".into(),
        10 => "SELECT dt,COUNT(*),MIN(ts),MAX(tm) FROM fuzz_a GROUP BY dt ORDER BY dt".into(),
        11 => "SELECT id,ROW_NUMBER() OVER(PARTITION BY t ORDER BY u DESC,id) AS rn FROM fuzz_a ORDER BY id".into(),
        12 => "SELECT id,LENGTH(b),HEX(c) FROM fuzz_a WHERE b IS NOT NULL ORDER BY LENGTH(b),id".into(),
        13 => "SELECT id,f FROM fuzz_a WHERE ABS(f)>1 OR f IS NULL ORDER BY f,id".into(),
        14 => "SELECT COUNT(*),SUM(u),SUM(u%1000000007) FROM fuzz_a".into(),
        _ => "SELECT a.id FROM fuzz_a a WHERE EXISTS(SELECT 1 FROM fuzz_b b WHERE b.id=a.id \
               AND (b.t=a.t OR (b.t IS NULL AND a.t IS NULL))) ORDER BY a.id".into(),
    };
    // Every family must return the same typed rows, metadata and warnings
    // across row/vector engines, serial/parallel degrees and small/large sort
    // budgets (the small budget forces spill on the stress workload).
    let differential = |session: &mut SessionContext, sql: &str| {
        let mut baseline = None;
        for (mode, degree, budget) in [
            (VecMode::Off, 1, 8 << 20),
            (VecMode::On, 1, 8 << 20),
            (VecMode::On, 4, 32768),
            (VecMode::On, 4, 8 << 20),
        ] {
            let _mode = enter_mode(mode);
            session.sysvars.mydb_parallel_degree = degree;
            session.sysvars.sort_buffer_size = budget;
            let actual = query(engine, session, sql);
            let warnings = query(engine, session, "SHOW WARNINGS");
            if let Some(expected) = &baseline {
                assert_eq!(
                    &(actual, warnings),
                    expected,
                    "execution modes diverged for {sql}"
                );
            } else {
                baseline = Some((actual, warnings));
            }
        }
    };
    let spills_before = crate::engine::status_counters::sort_merge_passes();
    differential(&mut session, &sql);
    if stress && family == 6 {
        assert!(
            crate::engine::status_counters::sort_merge_passes() > spills_before,
            "stress UNION sort never exercised a spill run"
        );
    }
    // DML changes the physical state before the independent predicate/index
    // oracles run; savepoint rollback must restore the same typed result.
    engine
        .execute_with_session(
            &mut session,
            "UPDATE fuzz_a SET k=NULL,s=CONCAT(s,'尾') WHERE id%7=0",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "DELETE FROM fuzz_a WHERE id%11=1")
        .unwrap();
    let before = query(
        engine,
        &mut session,
        "SELECT id,k,d,s FROM fuzz_a ORDER BY id",
    );
    engine
        .execute_with_session(&mut session, "SAVEPOINT f3")
        .unwrap();
    engine
        .execute_with_session(&mut session, "UPDATE fuzz_a SET d=-d,k=3")
        .unwrap();
    engine
        .execute_with_session(&mut session, "ROLLBACK TO SAVEPOINT f3")
        .unwrap();
    assert_eq!(
        before,
        query(
            engine,
            &mut session,
            "SELECT id,k,d,s FROM fuzz_a ORDER BY id"
        )
    );
    // The same family must still agree across engines after physical churn,
    // and disabling zone maps must never change the observable answer.
    differential(&mut session, &sql);
    let expected = query(engine, &mut session, &sql);
    engine
        .execute_with_session(&mut session, "SET GLOBAL mydb_zone_map_enabled=OFF")
        .unwrap();
    let zonal = query(engine, &mut session, &sql);
    engine
        .execute_with_session(&mut session, "SET GLOBAL mydb_zone_map_enabled=ON")
        .unwrap();
    assert_eq!(zonal, expected, "zone-map toggle changed results for {sql}");
    // Three-valued predicate partitioning detects dropped/duplicated rows even
    // when all execution configurations share the same optimizer bug.
    let original = sorted(query(engine, &mut session, "SELECT id,k,d,s FROM fuzz_a").1);
    let mut partitioned = Vec::new();
    for condition in [
        format!("({predicate})"),
        format!("NOT ({predicate})"),
        format!("({predicate}) IS NULL"),
    ] {
        partitioned.extend(
            query(
                engine,
                &mut session,
                &format!("SELECT id,k,d,s FROM fuzz_a WHERE {condition}"),
            )
            .1,
        );
    }
    assert_eq!(
        original,
        sorted(partitioned),
        "predicate partition mismatch: {predicate}"
    );
    // The physical access-path oracle keeps NULLs and duplicates in the multiset.
    let heap = query(
        engine,
        &mut session,
        "SELECT id,k,d,s FROM fuzz_a IGNORE INDEX(by_k) WHERE k IS NULL OR k>=-128 ORDER BY id",
    );
    let index = query(
        engine,
        &mut session,
        "SELECT id,k,d,s FROM fuzz_a FORCE INDEX(by_k) WHERE k IS NULL OR k>=-128 ORDER BY id",
    );
    assert_eq!(heap, index);
    // Adding a defaulted column must not disturb existing projections.
    let pre_ddl = query(
        engine,
        &mut session,
        "SELECT id,k,d,s FROM fuzz_a ORDER BY id",
    );
    engine
        .execute_with_session(
            &mut session,
            "ALTER TABLE fuzz_a ADD COLUMN extra INT DEFAULT 7",
        )
        .unwrap();
    assert_eq!(
        pre_ddl,
        query(
            engine,
            &mut session,
            "SELECT id,k,d,s FROM fuzz_a ORDER BY id"
        )
    );
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
}

#[cfg(test)]
mod tests {
    #[test]
    fn sql_execution_all_generated_families() {
        for family in 0..16 {
            super::sql_execution_differential(&[family, 0, 7, 0, 255, 128, 17, 43, 99]);
        }
        super::sql_execution_differential(&[6, 1, 7, 13, 0, 255, 128]);
    }
}
