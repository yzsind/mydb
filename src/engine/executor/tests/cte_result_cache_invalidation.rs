//! Historical CTE-cache settings must not enable cross-statement result reuse.

use super::*;

fn hits() -> u64 {
    crate::engine::executor::cte::CTE_RESULT_CACHE_HITS.with(|c| c.get())
}

fn scalar(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> i64 {
    let QueryResponse::ResultSet(rs) = engine.execute_with_session(session, sql).unwrap() else {
        panic!("expected result set: {sql}");
    };
    match &rs.rows[0][0] {
        Value::Int(n) => *n,
        other => panic!("expected Int scalar, got {other:?}"),
    }
}

#[test]
fn cte_cache_setting_does_not_reuse_across_statements() {
    with_test_env(&[("MYDB_CTE_RESULT_CACHE", Some("on"))], || {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_test_engine(dir.path());
        let mut s = SessionContext::default();
        for sql in [
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL PRIMARY KEY, v INT NOT NULL)",
            "INSERT INTO t VALUES (1,10),(2,20)",
            "INSERT INTO u VALUES (1,100)",
        ] {
            engine.execute_with_session(&mut s, sql).unwrap();
        }
        // Self-join the CTE so it is materialised (not inlined), exercising the
        // session result cache the way q39's `inv inv1, inv inv2` does.
        let q = "WITH c AS (SELECT v, COUNT(*) AS cnt FROM t GROUP BY v) \
                 SELECT COUNT(*) AS n FROM c a JOIN c b ON a.v = b.v";

        // Every statement materialises its own CTE under its own snapshot.
        assert_eq!(scalar(&engine, &mut s, q), 2);
        let h0 = hits();

        // Identical re-run: same answer, no cross-statement hit.
        assert_eq!(scalar(&engine, &mut s, q), 2);
        assert_eq!(hits(), h0);

        // DML on an UNRELATED table `u`: the entry for `c` (which depends only
        // on `t`) must stay valid — the whole point of per-table invalidation
        // vs the old global-SCN scheme that evicted on any commit.
        engine
            .execute_with_session(&mut s, "INSERT INTO u VALUES (2,200)")
            .unwrap();
        assert_eq!(scalar(&engine, &mut s, q), 2);
        assert_eq!(hits(), h0);

        // DML on the DEPENDENCY table `t`: must invalidate → recompute (no
        // hit) and reflect the new row rather than serving the stale count.
        let h = hits();
        engine
            .execute_with_session(&mut s, "INSERT INTO t VALUES (3,30)")
            .unwrap();
        assert_eq!(
            scalar(&engine, &mut s, q),
            3,
            "dependency DML must be reflected"
        );
        assert_eq!(hits(), h, "dependency DML must force a recompute (no hit)");
    });
}

#[test]
fn cte_cache_invalidates_on_truncate_and_alter() {
    with_test_env(&[("MYDB_CTE_RESULT_CACHE", Some("on"))], || {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_test_engine(dir.path());
        let mut s = SessionContext::default();
        for sql in [
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT NOT NULL)",
            "INSERT INTO t VALUES (1,10),(2,20)",
        ] {
            engine.execute_with_session(&mut s, sql).unwrap();
        }
        let q = "WITH c AS (SELECT v, COUNT(*) AS cnt FROM t GROUP BY v) \
                 SELECT COUNT(*) AS n FROM c a JOIN c b ON a.v = b.v";
        assert_eq!(scalar(&engine, &mut s, q), 2);
        // Warm the cache so a *stale* hit would be observable.
        assert_eq!(scalar(&engine, &mut s, q), 2);

        // TRUNCATE bumps the write epoch (via `reset_table_stats` →
        // `drop_table_modification_delta`) → invalidate.
        engine
            .execute_with_session(&mut s, "TRUNCATE TABLE t")
            .unwrap();
        assert_eq!(
            scalar(&engine, &mut s, q),
            0,
            "TRUNCATE must invalidate the cached CTE"
        );

        // ALTER bumps `schema_version` → invalidate.
        engine
            .execute_with_session(&mut s, "INSERT INTO t VALUES (1,10)")
            .unwrap();
        assert_eq!(scalar(&engine, &mut s, q), 1);
        assert_eq!(scalar(&engine, &mut s, q), 1); // warm
        engine
            .execute_with_session(&mut s, "ALTER TABLE t ADD COLUMN w INT NULL")
            .unwrap();
        engine
            .execute_with_session(&mut s, "INSERT INTO t (id, v, w) VALUES (2, 20, 5)")
            .unwrap();
        assert_eq!(
            scalar(&engine, &mut s, q),
            2,
            "post-ALTER insert must be reflected (schema_version invalidation)"
        );
    });
}
