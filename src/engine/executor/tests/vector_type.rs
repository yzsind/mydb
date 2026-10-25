//! End-to-end tests for the MySQL-9.0-compatible `VECTOR(n)` data type, the
//! vector function library (`DISTANCE` / `STRING_TO_VECTOR` / …), and the HNSW
//! approximate-nearest-neighbour index. Exercises the whole pillar through the
//! SQL engine: type parsing, internal little-endian `f32` storage round-trip,
//! the distance functions evaluated over stored columns, KNN planning
//! (`ORDER BY DISTANCE(...) LIMIT k` → `VectorIndexScan`, EXPLAIN), MVCC
//! maintenance (insert/delete/update visibility), and rebuild-on-restart.

use super::support::query_rows as rows;
use super::*;
use crate::types::ColumnType;

fn cell(engine: &StorageQueryEngine, sql: &str) -> String {
    let r = rows(engine, sql);
    assert_eq!(r.len(), 1, "expected exactly one row for `{sql}`");
    assert_eq!(r[0].len(), 1, "expected exactly one column for `{sql}`");
    r[0][0].to_display_string()
}

fn column_types(engine: &StorageQueryEngine, sql: &str) -> Vec<ColumnType> {
    match engine.execute(sql).unwrap() {
        QueryResponse::ResultSet(rs) => rs.columns.iter().map(|c| c.column_type).collect(),
        other => panic!("expected a result set for `{sql}`, got {other:?}"),
    }
}

/// First-column integer ids of a result set, in returned order.
fn id_list(engine: &StorageQueryEngine, sql: &str) -> Vec<i64> {
    rows(engine, sql)
        .into_iter()
        .map(|r| r[0].to_display_string().parse().unwrap())
        .collect()
}

fn id_list_session(
    engine: &StorageQueryEngine,
    session: &mut SessionContext,
    sql: &str,
) -> Vec<i64> {
    match engine.execute_with_session(session, sql).unwrap() {
        QueryResponse::ResultSet(rs) => rs
            .rows
            .into_iter()
            .map(|r| r[0].to_display_string().parse().unwrap())
            .collect(),
        other => panic!("expected a result set for `{sql}`, got {other:?}"),
    }
}

/// `[a,b,c]` literal text for a vector.
fn vec_text(v: &[f32]) -> String {
    let parts: Vec<String> = v.iter().map(|x| x.to_string()).collect();
    format!("[{}]", parts.join(","))
}

/// Deterministic pseudo-random vectors (no `Math.random` in this env).
fn gen_vectors(n: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut out = Vec::with_capacity(n);
    let mut state = seed;
    for _ in 0..n {
        let mut v = Vec::with_capacity(dim);
        for _ in 0..dim {
            state = state
                .wrapping_mul(0x5851_F42D_4C95_7F2D)
                .wrapping_add(0x1405_7B7E_F767_814F);
            let u = (state >> 11) as f64 / (1u64 << 53) as f64;
            v.push((u as f32) * 2.0 - 1.0);
        }
        out.push(v);
    }
    out
}

/// Brute-force top-k row ids (1-based, matching the test's insert order) by
/// squared-L2 distance to `query`, ascending.
fn brute_force_l2(vectors: &[Vec<f32>], query: &[f32], k: usize) -> Vec<i64> {
    let mut scored: Vec<(f32, i64)> = vectors
        .iter()
        .enumerate()
        .map(|(i, v)| {
            let d: f32 = v.iter().zip(query).map(|(a, b)| (a - b) * (a - b)).sum();
            (d, (i + 1) as i64)
        })
        .collect();
    scored.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    scored.into_iter().take(k).map(|(_, i)| i).collect()
}

fn explain_mentions_vector_index(engine: &StorageQueryEngine, query: &str) -> bool {
    rows(engine, &format!("EXPLAIN {query}"))
        .iter()
        .flatten()
        .any(|c| {
            let s = c.to_display_string().to_ascii_lowercase();
            s.contains("vector index") || s.contains("hnsw")
        })
}

/// Open a fresh test engine with the M5.3 brute fallback already
/// disabled and *keep* it disabled until the returned guard drops.
/// Required because `StorageQueryEngine::execute` instantiates a fresh
/// `SessionContext::default()` per call, so a one-shot
/// `SET SESSION mydb_hnsw_brute_threshold = 0` would not survive across
/// statements. The env-var override is honoured by
/// `SessionSysVarState::default`.
fn open_hnsw_test_engine(dir: &std::path::Path) -> HnswTestEngine {
    let env = disable_brute_fallback_env();
    let engine = open_test_engine(dir);
    HnswTestEngine { engine, _env: env }
}

fn open_strict_hnsw_test_engine(dir: &std::path::Path) -> HnswTestEngine {
    let env = disable_brute_fallback_env();
    let engine = open_strict_test_engine(dir);
    HnswTestEngine { engine, _env: env }
}

fn disable_brute_fallback_env() -> HnswEnv {
    HnswEnv::set(&[
        ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
        ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
    ])
}

struct HnswTestEngine {
    engine: StorageQueryEngine,
    _env: HnswEnv,
}

impl std::ops::Deref for HnswTestEngine {
    type Target = StorageQueryEngine;
    fn deref(&self) -> &Self::Target {
        &self.engine
    }
}

#[test]
fn vector_column_describe_and_wire_type() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL, raw VECTOR)")
        .unwrap();

    // DESCRIBE echoes the declared dimension (and the unconstrained form).
    let described: Vec<(String, String)> = rows(&engine, "DESCRIBE items")
        .into_iter()
        .map(|r| (r[0].to_display_string(), r[1].to_display_string()))
        .collect();
    let type_of = |name: &str| {
        described
            .iter()
            .find(|(c, _)| c == name)
            .map(|(_, t)| t.clone())
            .unwrap_or_default()
    };
    assert_eq!(type_of("emb"), "vector(3)");
    assert_eq!(type_of("raw"), "vector");

    // The VECTOR wire type is advertised for vector columns / expressions.
    assert_eq!(
        column_types(&engine, "SELECT emb, STRING_TO_VECTOR('[1,2]') FROM items"),
        vec![ColumnType::Vector, ColumnType::Vector]
    );
}

#[test]
fn vector_text_round_trip_and_dim() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(4) NOT NULL)")
        .unwrap();
    // A `[..]` string literal coerces into the VECTOR column on insert.
    engine
        .execute("INSERT INTO items VALUES (1, '[1, 2.5, -3, 0]')")
        .unwrap();
    engine
        .execute("INSERT INTO items VALUES (2, STRING_TO_VECTOR('[4,5,6,7]'))")
        .unwrap();

    // The stored vector renders back to canonical `[a,b,c]` text.
    assert_eq!(
        cell(&engine, "SELECT emb FROM items WHERE id = 1"),
        "[1,2.5,-3,0]"
    );
    assert_eq!(
        cell(
            &engine,
            "SELECT VECTOR_TO_STRING(emb) FROM items WHERE id = 2"
        ),
        "[4,5,6,7]"
    );
    // VECTOR_DIM reports the element count as an integer.
    assert_eq!(
        cell(&engine, "SELECT VECTOR_DIM(emb) FROM items WHERE id = 1"),
        "4"
    );
}

#[test]
fn vector_distance_functions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    // L2 distance between (0,0) and (3,4) is 5.
    assert_eq!(cell(&engine, "SELECT DISTANCE('[0,0]', '[3,4]')"), "5");
    assert_eq!(cell(&engine, "SELECT L2_DISTANCE('[0,0]', '[3,4]')"), "5");
    assert_eq!(
        cell(&engine, "SELECT DISTANCE('[0,0]', '[3,4]', 'L2')"),
        "5"
    );
    // Inner product of (1,2,3)·(4,5,6) = 32.
    assert_eq!(
        cell(&engine, "SELECT DOT_PRODUCT('[1,2,3]', '[4,5,6]')"),
        "32"
    );
    assert_eq!(
        cell(&engine, "SELECT INNER_PRODUCT('[1,2,3]', '[4,5,6]')"),
        "32"
    );
    // Cosine distance between identical-direction vectors is 0.
    assert_eq!(
        cell(&engine, "SELECT COSINE_DISTANCE('[1,0]', '[2,0]')"),
        "0"
    );
    // Orthogonal vectors → cosine distance 1.
    assert_eq!(
        cell(&engine, "SELECT COSINE_DISTANCE('[1,0]', '[0,1]')"),
        "1"
    );
    // CAST(... AS VECTOR) parses text; VECTOR_DIM confirms.
    assert_eq!(
        cell(&engine, "SELECT VECTOR_DIM(CAST('[7,8,9]' AS VECTOR))"),
        "3"
    );
    // NULL propagation (MySQL semantics).
    assert_eq!(cell(&engine, "SELECT DISTANCE(NULL, '[1,2]')"), "NULL");
    // Dimension mismatch is a clear error, never a silent answer.
    let err = engine
        .execute("SELECT DISTANCE('[1,2]', '[1,2,3]')")
        .unwrap_err();
    assert!(
        err.message().to_lowercase().contains("dimension"),
        "expected a dimension-mismatch error, got: {}",
        err.message()
    );
}

#[test]
fn vector_dimension_constraint() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
        .unwrap();
    engine
        .execute("INSERT INTO items VALUES (1, '[1,2,3]')")
        .unwrap();
    // Wrong dimension is rejected at insert time.
    let err = engine
        .execute("INSERT INTO items VALUES (2, '[1,2]')")
        .unwrap_err();
    assert!(
        err.message().to_lowercase().contains("dimension"),
        "expected dimension error, got: {}",
        err.message()
    );
    // The good row is intact and the bad one never landed.
    assert_eq!(cell(&engine, "SELECT COUNT(*) FROM items"), "1");
}

#[test]
fn hnsw_knn_uses_index_and_matches_bruteforce() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(6) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();

    let n = 40;
    let dim = 6;
    let vectors = gen_vectors(n, dim, 0x1234_5678_9ABC_DEF0);
    bulk_insert_vectors(&engine, &vectors);

    let query = gen_vectors(1, dim, 0xDEAD_BEEF_0000_0001)[0].clone();
    let qtext = vec_text(&query);
    let k = 5;
    let knn = format!("SELECT id FROM items ORDER BY DISTANCE(emb, '{qtext}') LIMIT {k}");

    // The KNN shape is driven by the HNSW index (no full Sort).
    assert!(
        explain_mentions_vector_index(&engine, &knn),
        "EXPLAIN should show the vector index for `{knn}`"
    );

    // The candidate pool (ef_search=64) exceeds n, so the result is exact:
    // it must equal the brute-force top-k, in ascending-distance order.
    let got = id_list(&engine, &knn);
    let expected = brute_force_l2(&vectors, &query, k);
    assert_eq!(got, expected, "KNN result must match brute-force top-{k}");

    // The projected DISTANCE column is ascending (nearest first).
    let dists: Vec<f64> = rows(
        &engine,
        &format!(
            "SELECT DISTANCE(emb, '{qtext}') AS d FROM items \
             ORDER BY DISTANCE(emb, '{qtext}') LIMIT {k}"
        ),
    )
    .into_iter()
    .map(|r| r[0].to_display_string().parse().unwrap())
    .collect();
    for w in dists.windows(2) {
        assert!(
            w[0] <= w[1] + 1e-6,
            "distances must be ascending: {dists:?}"
        );
    }
}

#[test]
fn hnsw_prepared_binary_query_vector_uses_index_and_rebinds() {
    use crate::engine::api::QueryEngine as _;
    use crate::engine::params::with_params;
    use crate::parser::{parse_statement, Statement};
    use crate::planner::{plan_select, PhysicalPlan};

    fn has_vector_index(plan: &PhysicalPlan) -> bool {
        match plan {
            PhysicalPlan::VectorIndexScan { .. } => true,
            PhysicalPlan::Projection { input, .. }
            | PhysicalPlan::Limit { input, .. }
            | PhysicalPlan::Sort { input, .. }
            | PhysicalPlan::TopK { input, .. } => has_vector_index(input),
            _ => false,
        }
    }

    fn prepared_ids(
        engine: &StorageQueryEngine,
        session: &mut SessionContext,
        sql: &str,
        stmt: &Statement,
        query: &[f32],
    ) -> Vec<i64> {
        let param = Value::Vector(crate::engine::vector::codec::from_f32(query));
        let response = with_params(vec![param], || {
            engine
                .execute_prepared_statement(session, sql, stmt.clone())
                .expect("prepared SELECT should be handled by storage engine")
                .unwrap()
        });
        match response.materialize().unwrap() {
            QueryResponse::ResultSet(rs) => rs
                .rows
                .into_iter()
                .map(|r| r[0].to_display_string().parse().unwrap())
                .collect(),
            other => panic!("expected result set, got {other:?}"),
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(6) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();

    let n = 40;
    let dim = 6;
    let vectors = gen_vectors(n, dim, 0xCAFE_BABE_0000_0001);
    bulk_insert_vectors(&engine, &vectors);

    let sql = "SELECT /*+ HNSW_EF(64) */ id FROM items ORDER BY DISTANCE(emb, ?) LIMIT 5";
    let mut parsed = parse_statement(sql).unwrap();
    let mut next = 0;
    crate::parser::number_statement_placeholders(&mut parsed, &mut next);
    let Statement::Select(select) = parsed.clone() else {
        panic!("expected SELECT");
    };
    let planned = plan_select(engine.database(), &select).unwrap();
    assert!(
        has_vector_index(&planned.physical),
        "parameterized KNN must plan as VectorIndexScan, got {:?}",
        planned.physical
    );

    let mut session = SessionContext::default();
    let prepared = engine.prepare_cached(&mut session, sql, 1).unwrap();
    let q1 = gen_vectors(1, dim, 0x1111_2222_3333_4444)[0].clone();
    let q2 = gen_vectors(1, dim, 0x9999_AAAA_BBBB_CCCC)[0].clone();

    assert_eq!(
        prepared_ids(&engine, &mut session, sql, &prepared, &q1),
        brute_force_l2(&vectors, &q1, 5)
    );
    assert_eq!(
        prepared_ids(&engine, &mut session, sql, &prepared, &q2),
        brute_force_l2(&vectors, &q2, 5)
    );
}

#[test]
fn parameterized_brute_knn_rejects_wrong_dimension() {
    use crate::engine::api::QueryEngine as _;
    use crate::engine::params::with_params;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
        .unwrap();
    engine
        .execute("INSERT INTO items VALUES (1, '[1,2,3]'), (2, '[4,5,6]')")
        .unwrap();
    let sql = "SELECT id FROM items ORDER BY L2_DISTANCE(emb, ?) LIMIT 1";
    let mut session = SessionContext::default();
    let prepared = engine.prepare_cached(&mut session, sql, 1).unwrap();
    let err = with_params(
        vec![Value::Vector(crate::engine::vector::codec::from_f32(&[
            1.0, 2.0,
        ]))],
        || {
            engine
                .execute_prepared_statement(&mut session, sql, (*prepared).clone())
                .expect("prepared SELECT should be handled")
                .unwrap_err()
        },
    );
    assert_eq!(err.code(), 50002);
    assert!(
        err.message().contains("VECTOR(3)"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn dot_index_supports_pgvector_and_max_inner_product_ordering() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(2) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_dot ON items(emb) USING HNSW WITH (metric=dot)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO items VALUES (1, '[1,0]'), (2, '[3,0]'), \
             (3, '[-2,0]'), (4, '[2,1]')",
        )
        .unwrap();
    for sql in [
        "SELECT id FROM items ORDER BY emb <#> '[1,0]' LIMIT 3",
        "SELECT id FROM items ORDER BY MAX_INNER_PRODUCT_DISTANCE(emb, '[1,0]') LIMIT 3",
        "SELECT id FROM items ORDER BY DOT_PRODUCT(emb, '[1,0]') DESC LIMIT 3",
        "SELECT id FROM items ORDER BY INNER_PRODUCT(emb, '[1,0]') DESC LIMIT 3",
    ] {
        assert!(explain_mentions_vector_index(&engine, sql), "{sql}");
        assert_eq!(id_list(&engine, sql), vec![2, 4, 1]);
    }

    // Descending *distance* is farthest-first, which an ANN nearest-neighbour
    // graph cannot provide. It must retain exact full-sort semantics and warn.
    let mut session = SessionContext::default();
    let response = engine
        .execute_with_session(
            &mut session,
            "SELECT id FROM items ORDER BY L2_DISTANCE(emb, '[1,0]') DESC LIMIT 3",
        )
        .unwrap();
    assert!(matches!(response, QueryResponse::ResultSet(_)));
    assert!(session.warnings.iter().any(|w| w.code() == 1287));
}

#[test]
fn auto_parameterized_knn_limit_keeps_constant_offset_in_runtime_k() {
    let _auto = HnswEnv::set(&[("MYDB_AUTO_PARAMETERIZE", "select")]);
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(2) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    for i in 1..=40 {
        engine
            .execute(&format!("INSERT INTO items VALUES ({i}, '[{i},0]')"))
            .unwrap();
    }
    // LIMIT 10 and 12 share the same auto-param bucket. OFFSET remains a
    // constant in the cached plan, so the candidate request must be 13/15.
    for (limit, expected_len) in [(10usize, 10usize), (12, 12), (10, 10)] {
        let sql = format!(
            "SELECT id FROM items ORDER BY L2_DISTANCE(emb, '[0,0]') LIMIT {limit} OFFSET 3"
        );
        let got = id_list(&engine, &sql);
        assert_eq!(got.len(), expected_len, "{sql}");
        assert_eq!(got[0], 4, "OFFSET must start at the fourth neighbour");
    }
}

#[test]
fn hnsw_knn_insert_visible_in_warm_graph() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    for i in 1..=10 {
        let v = [i as f32, 0.0, 0.0];
        engine
            .execute(&format!(
                "INSERT INTO items VALUES ({i}, '{}')",
                vec_text(&v)
            ))
            .unwrap();
    }

    // Warm the graph with a first query near id=5.
    let q = "SELECT id FROM items ORDER BY DISTANCE(emb, '[5,0,0]') LIMIT 1";
    assert_eq!(id_list(&engine, q), vec![5]);

    // Insert a new row that is the nearest to a fresh query — the warm graph
    // must include it (note_vector_inserts hook), not just rows seen at build.
    engine
        .execute("INSERT INTO items VALUES (99, '[100,0,0]')")
        .unwrap();
    assert_eq!(
        id_list(
            &engine,
            "SELECT id FROM items ORDER BY DISTANCE(emb, '[100,0,0]') LIMIT 1"
        ),
        vec![99]
    );
}

#[test]
fn hnsw_knn_delete_filtered_by_mvcc() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    for i in 1..=10 {
        let v = [i as f32, 0.0, 0.0];
        engine
            .execute(&format!(
                "INSERT INTO items VALUES ({i}, '{}')",
                vec_text(&v)
            ))
            .unwrap();
    }
    let q = "SELECT id FROM items ORDER BY DISTANCE(emb, '[5,0,0]') LIMIT 3";
    assert_eq!(id_list(&engine, q), vec![5, 4, 6]);

    // Delete the nearest row; the stale graph node must be filtered by the
    // executor's MVCC visibility check (it is no longer returned).
    engine.execute("DELETE FROM items WHERE id = 5").unwrap();
    let after = id_list(&engine, q);
    assert!(
        !after.contains(&5),
        "deleted row must not appear: {after:?}"
    );
    assert_eq!(after, vec![4, 6, 3]);
}

#[test]
fn hnsw_knn_update_reranks_by_live_vector() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    for i in 1..=10 {
        let v = [i as f32, 0.0, 0.0];
        engine
            .execute(&format!(
                "INSERT INTO items VALUES ({i}, '{}')",
                vec_text(&v)
            ))
            .unwrap();
    }
    let q = "SELECT id FROM items ORDER BY DISTANCE(emb, '[5,0,0]') LIMIT 1";
    assert_eq!(id_list(&engine, q), vec![5]);

    // Move id=5 far away: it must drop out (executor re-ranks by the *live*
    // heap vector, not the stale graph copy).
    engine
        .execute("UPDATE items SET emb = '[999,0,0]' WHERE id = 5")
        .unwrap();
    let nearest = id_list(&engine, q);
    assert_eq!(
        nearest,
        vec![4],
        "stale near vector must not win: {nearest:?}"
    );

    // Move id=1 (was far) right onto the query: the update hook refreshes the
    // graph so it is now the nearest.
    engine
        .execute("UPDATE items SET emb = '[5,0,0]' WHERE id = 1")
        .unwrap();
    assert_eq!(id_list(&engine, q), vec![1]);
}

#[test]
fn hnsw_knn_with_limit_offset() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    for i in 1..=10 {
        let v = [i as f32, 0.0, 0.0];
        engine
            .execute(&format!(
                "INSERT INTO items VALUES ({i}, '{}')",
                vec_text(&v)
            ))
            .unwrap();
    }
    // Distances to [3.4,0,0]: id3=0.4, id4=0.6, id2=1.4, id5=1.6 — strictly
    // ordered (no ties), so 3,4,2,5,1,… The scan fetches k=offset+limit
    // candidates; LIMIT/OFFSET then windows them. OFFSET 1 LIMIT 2 → 2nd,3rd.
    let q = "SELECT id FROM items ORDER BY DISTANCE(emb,'[3.4,0,0]') LIMIT 2 OFFSET 1";
    assert!(explain_mentions_vector_index(&engine, q));
    assert_eq!(id_list(&engine, q), vec![4, 2]);
}

#[test]
fn hnsw_knn_with_where_filter() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE items (id INT PRIMARY KEY, grp INT NOT NULL, emb VECTOR(3) NOT NULL, \
             INDEX idx_emb (emb) USING HNSW)",
        )
        .unwrap();
    for i in 1..=20 {
        let v = [i as f32, 0.0, 0.0];
        let grp = i % 2; // even ids → grp 0, odd ids → grp 1
        engine
            .execute(&format!(
                "INSERT INTO items VALUES ({i}, {grp}, '{}')",
                vec_text(&v)
            ))
            .unwrap();
    }
    // Nearest to id=10 among grp=1 (odd ids): 9, 11, 7, ... (even excluded).
    let q = "SELECT id FROM items WHERE grp = 1 \
             ORDER BY DISTANCE(emb, '[10,0,0]') LIMIT 3";
    assert!(explain_mentions_vector_index(&engine, q));
    assert_eq!(id_list(&engine, q), vec![9, 11, 7]);
}

#[test]
fn hnsw_inline_index_with_cosine_metric() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(2) NOT NULL, \
             INDEX idx_emb (emb) USING HNSW WITH (metric=cosine))",
        )
        .unwrap();
    // Vectors at distinct angles; cosine ranks by direction, not magnitude.
    let pts = [
        (1, [10.0f32, 0.0]), // 0°
        (2, [1.0, 1.0]),     // 45°
        (3, [0.0, 5.0]),     // 90°
        (4, [-1.0, 0.0]),    // 180°
    ];
    for (id, v) in pts {
        engine
            .execute(&format!(
                "INSERT INTO items VALUES ({id}, '{}')",
                vec_text(&v)
            ))
            .unwrap();
    }
    // Query direction ~5° off the x-axis: nearest by cosine is id=1 (0°),
    // then id=2 (45°). A COSINE query matches the COSINE index.
    let q = "SELECT id FROM items ORDER BY DISTANCE(emb, '[10,1]', 'COSINE') LIMIT 2";
    assert!(
        explain_mentions_vector_index(&engine, q),
        "COSINE query should hit the COSINE-built index"
    );
    assert_eq!(id_list(&engine, q), vec![1, 2]);

    // A metric mismatch (L2 query against a COSINE index) must NOT use the
    // HNSW graph — it falls back to a correct full scan + sort.
    let l2q = "SELECT id FROM items ORDER BY DISTANCE(emb, '[10,1]', 'L2') LIMIT 1";
    assert!(
        !explain_mentions_vector_index(&engine, l2q),
        "L2 query must not use the COSINE index"
    );
    // L2-nearest to (10,1) is id=1 (10,0).
    assert_eq!(id_list(&engine, l2q), vec![1]);
}

#[test]
fn hnsw_knn_survives_restart() {
    let _env = disable_brute_fallback_env();
    let dir = tempfile::tempdir().unwrap();
    let dim = 5;
    let n = 30;
    let vectors = gen_vectors(n, dim, 0xCAFE_F00D_1234_5678);
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(5) NOT NULL)")
            .unwrap();
        engine
            .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
            .unwrap();
        bulk_insert_vectors(&engine, &vectors);
        // Warm + verify before the restart.
        let query = vec_text(&vectors[0]);
        assert_eq!(
            id_list(
                &engine,
                &format!("SELECT id FROM items ORDER BY DISTANCE(emb, '{query}') LIMIT 1")
            ),
            vec![1]
        );
    }

    // Reopen: the in-memory graph is gone (derived data), so the first KNN
    // rebuilds it from the persisted heap and still answers correctly.
    let reopened = open_strict_test_engine(dir.path());
    let query = gen_vectors(1, dim, 0x0BAD_C0DE_0000_0009)[0].clone();
    let qtext = vec_text(&query);
    let knn = format!("SELECT id FROM items ORDER BY DISTANCE(emb, '{qtext}') LIMIT 5");
    assert!(explain_mentions_vector_index(&reopened, &knn));
    assert_eq!(
        id_list(&reopened, &knn),
        brute_force_l2(&vectors, &query, 5),
        "post-restart KNN must rebuild the graph and match brute-force"
    );
}

#[test]
fn vector_table_recovers_after_delete_and_update() {
    // Regression: DELETE/UPDATE undo records carry the row's before-image,
    // which for a vector table includes a `Value::Vector`. WAL replay must be
    // able to *skip* past it (`encoded_row_skip_len`) or recovery fails. INSERT
    // undo has no before-image, so an insert-only restart can't catch this.
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
            .unwrap();
        engine
            .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
            .unwrap();
        for i in 1..=5 {
            let v = [i as f32, 0.0, 0.0];
            engine
                .execute(&format!(
                    "INSERT INTO items VALUES ({i}, '{}')",
                    vec_text(&v)
                ))
                .unwrap();
        }
        engine.execute("DELETE FROM items WHERE id = 2").unwrap();
        engine
            .execute("UPDATE items SET emb = '[9,0,0]' WHERE id = 4")
            .unwrap();
    }

    // Reopen: recovery replays the DELETE/UPDATE undo records (containing
    // vectors) and the data must be exactly as left.
    let reopened = open_strict_test_engine(dir.path());
    let surviving: Vec<(i64, String)> = rows(
        &reopened,
        "SELECT id, VECTOR_TO_STRING(emb) FROM items ORDER BY id",
    )
    .into_iter()
    .map(|r| {
        (
            r[0].to_display_string().parse().unwrap(),
            r[1].to_display_string(),
        )
    })
    .collect();
    assert_eq!(
        surviving,
        vec![
            (1, "[1,0,0]".to_string()),
            (3, "[3,0,0]".to_string()),
            (4, "[9,0,0]".to_string()),
            (5, "[5,0,0]".to_string()),
        ]
    );
    // The rebuilt graph still answers KNN correctly post-recovery.
    assert_eq!(
        id_list(
            &reopened,
            "SELECT id FROM items ORDER BY DISTANCE(emb, '[3,0,0]') LIMIT 2"
        ),
        vec![3, 1]
    );
}

#[test]
fn vector_index_validation_errors() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL, name VARCHAR(20))",
        )
        .unwrap();
    // HNSW over a non-VECTOR column is rejected.
    let err = engine
        .execute("CREATE INDEX bad ON items(name) USING HNSW")
        .unwrap_err();
    assert!(
        err.message().to_uppercase().contains("VECTOR"),
        "non-vector HNSW index should be rejected: {}",
        err.message()
    );
    // A UNIQUE vector index is rejected.
    let err = engine
        .execute("CREATE UNIQUE INDEX bad ON items(emb) USING HNSW")
        .unwrap_err();
    assert!(
        err.message().to_lowercase().contains("non-unique")
            || err.message().to_lowercase().contains("unique"),
        "unique HNSW index should be rejected: {}",
        err.message()
    );
}

#[test]
fn vector_index_dropped_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    for i in 1..=5 {
        let v = [i as f32, 0.0, 0.0];
        engine
            .execute(&format!(
                "INSERT INTO items VALUES ({i}, '{}')",
                vec_text(&v)
            ))
            .unwrap();
    }
    let knn = "SELECT id FROM items ORDER BY DISTANCE(emb, '[3,0,0]') LIMIT 1";
    assert!(explain_mentions_vector_index(&engine, knn));
    assert_eq!(id_list(&engine, knn), vec![3]);

    // After DROP INDEX the KNN must no longer use the HNSW path, yet still
    // answer correctly via a full scan + sort.
    engine.execute("DROP INDEX idx_emb ON items").unwrap();
    assert!(!explain_mentions_vector_index(&engine, knn));
    assert_eq!(id_list(&engine, knn), vec![3]);
}

// -------- M1: `.hnsw` snapshot persistence + startup load + catch-up --------

/// RAII for env-var modification inside a test — restore the prior value
/// (or unset) on drop. `cargo nextest` already isolates per-test
/// processes; this guard makes the tests safe under plain `cargo test`
/// (shared-process parallel) too.
pub(super) struct HnswEnv {
    keys: Vec<(&'static str, Option<String>)>,
}

impl HnswEnv {
    pub(super) fn set(pairs: &[(&'static str, &str)]) -> Self {
        let keys = pairs
            .iter()
            .map(|(k, v)| {
                let prior = std::env::var(k).ok();
                std::env::set_var(k, v);
                (*k, prior)
            })
            .collect();
        HnswEnv { keys }
    }
}

impl Drop for HnswEnv {
    fn drop(&mut self) {
        for (k, prior) in &self.keys {
            match prior {
                Some(v) => std::env::set_var(k, v),
                None => std::env::remove_var(k),
            }
        }
    }
}

fn find_object_files_with_extension(
    data_dir: &std::path::Path,
    extension: &str,
) -> Vec<std::path::PathBuf> {
    let mut pending = vec![crate::storage::naming::objects_root(data_dir)];
    let mut files = Vec::new();
    while let Some(path) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(path) else {
            continue;
        };
        for entry in entries.filter_map(Result::ok) {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|ext| ext == extension) {
                files.push(path);
            }
        }
    }
    files
}

/// Locate every `.hnsw` memory-backend snapshot in the ID-sharded object
/// store.
fn find_hnsw_files(data_dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    find_object_files_with_extension(data_dir, "hnsw")
}

#[test]
fn hnsw_persisted_after_checkpoint_loads_on_restart() {
    // `.hnsw` checkpoint-snapshot persistence is memory-backend only; pin it.
    let _mem = HnswEnv::set(&[("MYDB_VECTOR_STORAGE", "memory")]);
    // Force opportunistic persist on every checkpoint regardless of how few
    // rows we inserted (default dirty-ratio gate is 10%, which a 20-row
    // table with 20 inserts trivially clears, but spelling it out keeps the
    // test robust to future default tweaks). Also disable the M5.3 brute
    // fallback so the post-restart KNN keeps asserting on the HNSW path.
    let _env = HnswEnv::set(&[
        ("MYDB_HNSW_PERSIST", "on"),
        ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
        ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
        ("MYDB_HNSW_PERSIST_DIRTY_RATIO", "0.0"),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let dim = 4;
    let n = 20;
    let vectors = gen_vectors(n, dim, 0x1111_2222_3333_4444);
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(4) NOT NULL)")
            .unwrap();
        engine
            .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
            .unwrap();
        bulk_insert_vectors(&engine, &vectors);
        // Warm the in-memory HNSW graph so the checkpoint's persist pass
        // has something to write. We can't rely on a SQL-level KNN query
        // to do this: the current SELECT fast-path implements ORDER BY
        // DISTANCE LIMIT k via a brute-force scan + sort that never
        // visits `vector_graph_get_or_build`, so the graph stays cold.
        engine.database().vector_graph_warm_all_for_test();
        engine.checkpoint().unwrap();
    }
    // `.hnsw` snapshot exists after the checkpoint.
    let snapshots = find_hnsw_files(dir.path());
    assert_eq!(
        snapshots.len(),
        1,
        "expected exactly one .hnsw file, got: {snapshots:?}"
    );
    let snapshot_bytes = std::fs::read(&snapshots[0]).unwrap();
    // Header sanity: magic "HNW1" + version 2 in little-endian.
    assert_eq!(&snapshot_bytes[0..4], b"1WNH"); // "HNW1" little-endian: 0x484E5731 → 31 57 4E 48 → reversed string
    assert_eq!(
        u16::from_le_bytes(snapshot_bytes[4..6].try_into().unwrap()),
        2,
        "snapshot must be v2"
    );

    // Reopen: the graph loads from disk + catches up zero new rows.
    let reopened = open_strict_test_engine(dir.path());
    let query = gen_vectors(1, dim, 0x9999_8888_7777_6666)[0].clone();
    let qtext = vec_text(&query);
    let knn_sql = format!("SELECT id FROM items ORDER BY DISTANCE(emb, '{qtext}') LIMIT 5");
    assert!(explain_mentions_vector_index(&reopened, &knn_sql));
    assert_eq!(
        id_list(&reopened, &knn_sql),
        brute_force_l2(&vectors, &query, 5),
        "KNN after restart must match brute-force"
    );
}

#[test]
fn switching_vector_backend_requires_drop_and_create() {
    let dir = tempfile::tempdir().unwrap();
    {
        let _memory = HnswEnv::set(&[
            ("MYDB_VECTOR_STORAGE", "memory"),
            ("MYDB_HNSW_PERSIST", "on"),
            ("MYDB_HNSW_PERSIST_DIRTY_RATIO", "0"),
            ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
            ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
        ]);
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(2) NOT NULL)")
            .unwrap();
        engine
            .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
            .unwrap();
        engine
            .execute("INSERT INTO items VALUES (1, '[1,0]'), (2, '[2,0]')")
            .unwrap();
        engine.database().vector_graph_warm_all_for_test();
        engine.checkpoint().unwrap();
        assert_eq!(find_hnsw_files(dir.path()).len(), 1);
    }
    {
        let _paged = HnswEnv::set(&[
            ("MYDB_VECTOR_STORAGE", "paged"),
            ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
            ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
        ]);
        let engine = open_strict_test_engine(dir.path());
        let err = engine
            .execute(
                "SELECT /*+ HNSW_EF(16) */ id FROM items \
                 ORDER BY DISTANCE(emb, '[1,0]') LIMIT 1",
            )
            .unwrap_err();
        assert_eq!(err.code(), 1235, "{err:?}");
        assert!(err.message().contains("DROP") && err.message().contains("CREATE"));
    }
}

#[test]
fn hnsw_catch_up_picks_up_rows_inserted_after_persist() {
    // Crash window: snapshot captures rows 1..=10; rows 11..=18 land after
    // the checkpoint and live only in the WAL → heap. On reopen the
    // catch-up walk must add them to the loaded graph so KNN still sees
    // every row.
    let _env = HnswEnv::set(&[
        ("MYDB_VECTOR_STORAGE", "memory"),
        ("MYDB_HNSW_PERSIST", "on"),
        ("MYDB_HNSW_PERSIST_DIRTY_RATIO", "0.0"),
        // M5.3 — keep the post-restart KNN on the HNSW path.
        ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
        ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let dim = 4;
    let n = 18;
    let vectors = gen_vectors(n, dim, 0xAABB_CCDD_EEFF_0011);
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(4) NOT NULL)")
            .unwrap();
        engine
            .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
            .unwrap();
        bulk_insert_vectors_at(&engine, &vectors[..10], 1);
        // Warm + checkpoint: this snapshot reflects only rows 1..=10.
        engine.database().vector_graph_warm_all_for_test();
        engine.checkpoint().unwrap();
        // Now insert rows 11..=18 *after* the checkpoint — they live only
        // in the WAL + heap, NOT in the on-disk graph.
        bulk_insert_vectors_at(&engine, &vectors[10..], 11);
    }

    // Reopen: load snapshot (10 nodes) → heap scan adds the missing 8 →
    // KNN matches brute-force over all 18 rows.
    let reopened = open_strict_test_engine(dir.path());
    let query = gen_vectors(1, dim, 0xDEAD_BEEF_C0DE_F00D)[0].clone();
    let qtext = vec_text(&query);
    let knn_sql = format!("SELECT id FROM items ORDER BY DISTANCE(emb, '{qtext}') LIMIT 5");
    assert!(explain_mentions_vector_index(&reopened, &knn_sql));
    assert_eq!(
        id_list(&reopened, &knn_sql),
        brute_force_l2(&vectors, &query, 5),
        "post-restart KNN must include rows inserted after the checkpoint"
    );
}

#[test]
fn hnsw_corrupt_snapshot_falls_back_to_rebuild() {
    // `.hnsw` snapshot semantics are memory-backend only; pin the backend so
    // this test is independent of the default (paged has no snapshot file).
    let _mem = HnswEnv::set(&[("MYDB_VECTOR_STORAGE", "memory")]);
    let _env = HnswEnv::set(&[
        ("MYDB_HNSW_PERSIST", "on"),
        ("MYDB_HNSW_PERSIST_DIRTY_RATIO", "0.0"),
        // M5.3 — keep the post-restart KNN on the HNSW path.
        ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
        ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let dim = 4;
    let n = 12;
    let vectors = gen_vectors(n, dim, 0x6789_ABCD_EF01_2345);
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(4) NOT NULL)")
            .unwrap();
        engine
            .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
            .unwrap();
        bulk_insert_vectors(&engine, &vectors);
        engine.database().vector_graph_warm_all_for_test();
        engine.checkpoint().unwrap();
    }
    // Corrupt the snapshot's CRC trailer.
    let snapshots = find_hnsw_files(dir.path());
    assert_eq!(snapshots.len(), 1);
    let mut bytes = std::fs::read(&snapshots[0]).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF;
    std::fs::write(&snapshots[0], &bytes).unwrap();

    // Reopen: load fails (bad CRC) → falls back to full heap rebuild → KNN
    // still answers correctly.
    let reopened = open_strict_test_engine(dir.path());
    let query = gen_vectors(1, dim, 0xC0FF_EE00_BAD0_FACE)[0].clone();
    let qtext = vec_text(&query);
    let knn_sql = format!("SELECT id FROM items ORDER BY DISTANCE(emb, '{qtext}') LIMIT 5");
    assert!(explain_mentions_vector_index(&reopened, &knn_sql));
    assert_eq!(
        id_list(&reopened, &knn_sql),
        brute_force_l2(&vectors, &query, 5),
        "post-corrupt-file KNN must still match brute-force via rebuild"
    );
}

#[test]
fn hnsw_persist_disabled_skips_disk() {
    // `.hnsw` snapshot toggling is memory-backend only; pin the backend so the
    // assertion tests what it claims (paged is WAL-durable regardless).
    let _mem = HnswEnv::set(&[("MYDB_VECTOR_STORAGE", "memory")]);
    // With MYDB_HNSW_PERSIST=off the checkpoint must NOT write any
    // `.hnsw` file, and KNN after restart still works via a full rebuild.
    let _env = HnswEnv::set(&[("MYDB_HNSW_PERSIST", "off")]);
    let dir = tempfile::tempdir().unwrap();
    let dim = 4;
    let n = 10;
    let vectors = gen_vectors(n, dim, 0x0101_0202_0303_0404);
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(4) NOT NULL)")
            .unwrap();
        engine
            .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
            .unwrap();
        bulk_insert_vectors(&engine, &vectors);
        engine.database().vector_graph_warm_all_for_test();
        engine.checkpoint().unwrap();
    }
    assert!(
        find_hnsw_files(dir.path()).is_empty(),
        "MYDB_HNSW_PERSIST=off must not produce any .hnsw file"
    );

    // Reopen still works via the rebuild fallback.
    let reopened = open_strict_test_engine(dir.path());
    let query = gen_vectors(1, dim, 0xFADE_F00D_DEAD_BABE)[0].clone();
    let qtext = vec_text(&query);
    let knn_sql = format!("SELECT id FROM items ORDER BY DISTANCE(emb, '{qtext}') LIMIT 3");
    assert_eq!(
        id_list(&reopened, &knn_sql),
        brute_force_l2(&vectors, &query, 3),
    );
}

#[test]
fn hnsw_drop_index_removes_snapshot_file() {
    // `.hnsw` snapshot is memory-backend only; pin the backend.
    let _mem = HnswEnv::set(&[("MYDB_VECTOR_STORAGE", "memory")]);
    let _env = HnswEnv::set(&[
        ("MYDB_HNSW_PERSIST", "on"),
        ("MYDB_HNSW_PERSIST_DIRTY_RATIO", "0.0"),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let dim = 3;
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    for i in 1..=6 {
        let v = vec![i as f32, 0.0, 0.0];
        engine
            .execute(&format!(
                "INSERT INTO items VALUES ({i}, '{}')",
                vec_text(&v)
            ))
            .unwrap();
    }
    // Warm + checkpoint produces the snapshot.
    engine.database().vector_graph_warm_all_for_test();
    engine.checkpoint().unwrap();
    assert_eq!(find_hnsw_files(dir.path()).len(), 1);
    // DROP INDEX must remove both the in-memory graph and the on-disk file.
    engine.execute("DROP INDEX idx_emb ON items").unwrap();
    assert!(
        find_hnsw_files(dir.path()).is_empty(),
        "DROP INDEX must remove the .hnsw snapshot"
    );
    let _ = dim;
}

#[test]
fn hnsw_drop_table_removes_snapshot_file() {
    // `.hnsw` snapshot is memory-backend only; pin the backend.
    let _mem = HnswEnv::set(&[("MYDB_VECTOR_STORAGE", "memory")]);
    let _env = HnswEnv::set(&[
        ("MYDB_HNSW_PERSIST", "on"),
        ("MYDB_HNSW_PERSIST_DIRTY_RATIO", "0.0"),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    for i in 1..=5 {
        let v = vec![i as f32, 0.0, 0.0];
        engine
            .execute(&format!(
                "INSERT INTO items VALUES ({i}, '{}')",
                vec_text(&v)
            ))
            .unwrap();
    }
    engine.database().vector_graph_warm_all_for_test();
    engine.checkpoint().unwrap();
    assert_eq!(find_hnsw_files(dir.path()).len(), 1);
    engine.execute("DROP TABLE items").unwrap();
    assert!(
        find_hnsw_files(dir.path()).is_empty(),
        "DROP TABLE must remove every .hnsw snapshot for the table"
    );
}

#[test]
fn hnsw_eager_preload_warms_graph_at_startup() {
    // M5.3 — keep KNN on the HNSW path even at this fixture's row count.
    let _env_outer = HnswEnv::set(&[
        ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
        ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let dim = 4;
    let n = 12;
    let vectors = gen_vectors(n, dim, 0x5555_4444_3333_2222);
    {
        let _env = HnswEnv::set(&[
            ("MYDB_HNSW_PERSIST", "on"),
            ("MYDB_HNSW_PERSIST_DIRTY_RATIO", "0.0"),
        ]);
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(4) NOT NULL)")
            .unwrap();
        engine
            .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
            .unwrap();
        bulk_insert_vectors(&engine, &vectors);
        engine.database().vector_graph_warm_all_for_test();
        engine.checkpoint().unwrap();
    }
    // Check the graph before any SQL accessor can load the lazy descriptor.
    // Then verify KNN results against brute force. Corrupt/absent snapshot
    // fallback is covered by the separate persistence tests above.
    let _env = HnswEnv::set(&[("MYDB_HNSW_PERSIST", "on"), ("MYDB_HNSW_PRELOAD", "eager")]);
    let reopened = open_strict_test_engine(dir.path());
    assert_eq!(
        reopened
            .database()
            .hnsw_stats_for_test("items", "idx_emb")
            .map(|(_, live, _)| live),
        Some(n),
        "eager preload must resolve lazy catalog descriptors before the first query"
    );
    let query = gen_vectors(1, dim, 0xABCD_EF01_2345_6789)[0].clone();
    let qtext = vec_text(&query);
    let knn_sql = format!("SELECT id FROM items ORDER BY DISTANCE(emb, '{qtext}') LIMIT 3");
    assert!(explain_mentions_vector_index(&reopened, &knn_sql));
    assert_eq!(
        id_list(&reopened, &knn_sql),
        brute_force_l2(&vectors, &query, 3),
    );
}

// -------- M2: tombstone + checkpoint compaction --------

/// `(node_count, live_count, stale_count)` for the `items.idx_emb` HNSW
/// graph, or `None` if the graph isn't warm yet.
fn hnsw_stats(engine: &StorageQueryEngine) -> Option<(usize, usize, u32)> {
    engine.database().hnsw_stats_for_test("items", "idx_emb")
}

#[test]
fn hnsw_create_index_on_populated_table_builds_graph_eagerly() {
    // "CREATE INDEX takes effect immediately": building the index on a table
    // that already holds rows must warm the HNSW graph at DDL time, so the
    // first KNN query doesn't pay the cold-build cost. We assert the graph is
    // warm *without* running any query first (default MYDB_HNSW_BUILD_ON_DDL).
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
        .unwrap();
    // Populate BEFORE creating the index.
    for i in 1..=12 {
        engine
            .execute(&format!(
                "INSERT INTO items VALUES ({i}, '{}')",
                vec_text(&[i as f32, 0.0, 0.0])
            ))
            .unwrap();
    }
    // No vector index yet → no graph.
    assert!(hnsw_stats(&engine).is_none());

    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();

    // The graph is warm immediately after DDL — no query triggered the build.
    let (nodes, live, stale) = hnsw_stats(&engine).expect("graph warm right after CREATE INDEX");
    assert_eq!(nodes, 12, "every existing row must be in the graph");
    assert_eq!(live, 12);
    assert_eq!(stale, 0);

    // And it answers KNN correctly.
    assert_eq!(
        id_list(
            &engine,
            "SELECT id FROM items ORDER BY DISTANCE(emb, '[5,0,0]') LIMIT 3"
        ),
        vec![5, 4, 6]
    );
}

#[test]
fn hnsw_create_index_on_empty_table_is_finalized_and_survives_restart() {
    // A finalized empty graph must be warm immediately so later INSERTs are
    // maintained incrementally. Reopening must trust that graph rather than
    // repeatedly rebuilding it from the heap.
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
            .unwrap();
        engine
            .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
            .unwrap();
        assert_eq!(hnsw_stats(&engine), Some((0, 0, 0)));
        engine
            .execute("INSERT INTO items VALUES (1, '[1,0,0]'), (2, '[2,0,0]')")
            .unwrap();
        assert_eq!(hnsw_stats(&engine), Some((2, 2, 0)));
    }

    let reopened = open_strict_test_engine(dir.path());
    assert_eq!(
        id_list(
            &reopened,
            "SELECT id FROM items ORDER BY DISTANCE(emb, '[1,0,0]') LIMIT 2"
        ),
        vec![1, 2]
    );
}

#[test]
fn hnsw_build_on_ddl_disabled_defers_to_first_query() {
    // The GUC gate: with MYDB_HNSW_BUILD_ON_DDL=off the graph stays cold even
    // on a populated table (pure lazy build on first query).
    let _env = HnswEnv::set(&[("MYDB_HNSW_BUILD_ON_DDL", "off")]);
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
        .unwrap();
    for i in 1..=6 {
        engine
            .execute(&format!(
                "INSERT INTO items VALUES ({i}, '{}')",
                vec_text(&[i as f32, 0.0, 0.0])
            ))
            .unwrap();
    }
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    assert!(
        hnsw_stats(&engine).is_none(),
        "build-on-ddl disabled: graph must stay cold until first query"
    );
}

#[test]
fn hnsw_delete_tombstones_warm_graph() {
    // DELETE must mark every node keyed by the dropped row_id as a
    // tombstone: `stale_count` rises, `live` falls, `node_count` stays
    // the same (graph rebuild is deferred to the next checkpoint).
    // MYDB_HNSW_COMPACT_DISABLE pins the tombstones in place so the
    // assertions are stable regardless of which checkpoints fire.
    let _env = HnswEnv::set(&[("MYDB_HNSW_COMPACT_DISABLE", "1")]);
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    for i in 1..=8 {
        let v = vec![i as f32, 0.0, 0.0];
        engine
            .execute(&format!(
                "INSERT INTO items VALUES ({i}, '{}')",
                vec_text(&v)
            ))
            .unwrap();
    }
    engine.database().vector_graph_warm_all_for_test();
    let (nodes, live, stale) = hnsw_stats(&engine).expect("warm graph");
    assert_eq!(nodes, 8);
    assert_eq!(live, 8);
    assert_eq!(stale, 0);

    engine
        .execute("DELETE FROM items WHERE id IN (2, 5)")
        .unwrap();
    let (nodes, live, stale) = hnsw_stats(&engine).expect("warm graph");
    assert_eq!(nodes, 8, "node count is unchanged until compaction");
    assert_eq!(live, 6);
    assert_eq!(stale, 2);

    // KNN must skip tombstoned rows (the executor's MVCC check also
    // filters them, but defence in depth — the in-graph tombstone is
    // the only barrier when the row is invisible *but* still keyed in
    // the heap).
    let knn = "SELECT id FROM items ORDER BY DISTANCE(emb, '[1,0,0]') LIMIT 8";
    let ids = id_list(&engine, knn);
    assert!(!ids.contains(&2), "id=2 was deleted");
    assert!(!ids.contains(&5), "id=5 was deleted");
}

#[test]
fn vector_delete_maintenance_obeys_rollback_savepoint_xa_and_autocommit() {
    let _env = HnswEnv::set(&[("MYDB_HNSW_COMPACT_DISABLE", "1")]);
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    engine
        .execute(
            "INSERT INTO items VALUES (1,'[1,0,0]'),(2,'[2,0,0]'),\
             (3,'[3,0,0]'),(4,'[4,0,0]')",
        )
        .unwrap();
    engine.database().vector_graph_warm_all_for_test();

    let mut tx = SessionContext::default();
    engine.execute_with_session(&mut tx, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut tx, "DELETE FROM items WHERE id=1")
        .unwrap();
    engine.execute_with_session(&mut tx, "ROLLBACK").unwrap();

    engine.execute_with_session(&mut tx, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut tx, "SAVEPOINT before_delete")
        .unwrap();
    engine
        .execute_with_session(&mut tx, "DELETE FROM items WHERE id=2")
        .unwrap();
    engine
        .execute_with_session(&mut tx, "ROLLBACK TO SAVEPOINT before_delete")
        .unwrap();
    engine.execute_with_session(&mut tx, "COMMIT").unwrap();

    let mut xa = SessionContext::default();
    engine
        .execute_with_session(&mut xa, "XA START 'vector-rb'")
        .unwrap();
    engine
        .execute_with_session(&mut xa, "DELETE FROM items WHERE id=4")
        .unwrap();
    engine
        .execute_with_session(&mut xa, "XA END 'vector-rb'")
        .unwrap();
    engine
        .execute_with_session(&mut xa, "XA ROLLBACK 'vector-rb'")
        .unwrap();

    assert_eq!(hnsw_stats(&engine), Some((4, 4, 0)));
    assert_eq!(
        id_list(
            &engine,
            "SELECT id FROM items ORDER BY DISTANCE(emb,'[1,0,0]') LIMIT 4"
        ),
        vec![1, 2, 3, 4]
    );

    // Autocommit DELETE is applied at its commit hook.
    engine.execute("DELETE FROM items WHERE id=3").unwrap();
    assert_eq!(hnsw_stats(&engine), Some((4, 3, 1)));
    let status = rows(&engine, "SHOW ENGINE MYDB STATUS");
    assert!(status.iter().any(|r| {
        r[1].to_display_string() == "hnsw_tombstones_rolled_back"
            && r[2].to_display_string().parse::<u64>().unwrap_or(0) >= 3
    }));
}

#[test]
fn hnsw_update_marks_old_node_and_adds_new() {
    let _env = HnswEnv::set(&[("MYDB_HNSW_COMPACT_DISABLE", "1")]);
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    for i in 1..=4 {
        engine
            .execute(&format!(
                "INSERT INTO items VALUES ({i}, '{}')",
                vec_text(&vec![i as f32, 0.0, 0.0])
            ))
            .unwrap();
    }
    engine.database().vector_graph_warm_all_for_test();
    let (nodes_before, live_before, stale_before) = hnsw_stats(&engine).unwrap();
    assert_eq!(nodes_before, 4);
    assert_eq!(live_before, 4);
    assert_eq!(stale_before, 0);

    // Changing the vector column tombstones the old node and adds a new
    // one for the same row_id.
    engine
        .execute("UPDATE items SET emb = '[42,0,0]' WHERE id = 1")
        .unwrap();
    let (nodes, live, stale) = hnsw_stats(&engine).unwrap();
    assert_eq!(nodes, 5, "old node remains, new node appended");
    assert_eq!(live, 4, "tombstone offsets the new insert");
    assert_eq!(stale, 1);

    // KNN to the new vector finds id=1 even though its old node is
    // still in the graph (tombstoned).
    let ids = id_list(
        &engine,
        "SELECT id FROM items ORDER BY DISTANCE(emb, '[42,0,0]') LIMIT 1",
    );
    assert_eq!(ids, vec![1]);
}

#[test]
fn hnsw_update_unchanged_vector_does_not_tombstone() {
    let _env = HnswEnv::set(&[("MYDB_HNSW_COMPACT_DISABLE", "1")]);
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL, label VARCHAR(8))",
        )
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    for i in 1..=4 {
        engine
            .execute(&format!(
                "INSERT INTO items VALUES ({i}, '{}', 'a')",
                vec_text(&vec![i as f32, 0.0, 0.0])
            ))
            .unwrap();
    }
    engine.database().vector_graph_warm_all_for_test();
    engine
        .execute("UPDATE items SET label = 'b' WHERE id = 1")
        .unwrap();
    let (nodes, live, stale) = hnsw_stats(&engine).unwrap();
    assert_eq!(nodes, 4);
    assert_eq!(live, 4);
    assert_eq!(
        stale, 0,
        "non-vector column update must not bloat the graph"
    );
}

#[test]
fn hnsw_compaction_triggers_at_ratio_during_checkpoint() {
    // Compaction (tombstone-stat gating) is a memory-backend behaviour; the
    // paged backend leaves tombstones in place. Pin the backend.
    let _mem = HnswEnv::set(&[("MYDB_VECTOR_STORAGE", "memory")]);
    // Aggressive ratio gate (1%): any tombstone triggers compaction.
    let _env = HnswEnv::set(&[
        ("MYDB_HNSW_PERSIST", "off"), // isolate compaction from persist
        ("MYDB_HNSW_COMPACT_RATIO", "0.01"),
        ("MYDB_HNSW_COMPACT_DISABLE", "0"),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    for i in 1..=20 {
        engine
            .execute(&format!(
                "INSERT INTO items VALUES ({i}, '{}')",
                vec_text(&vec![i as f32, 0.0, 0.0])
            ))
            .unwrap();
    }
    engine.database().vector_graph_warm_all_for_test();
    engine
        .execute("DELETE FROM items WHERE id IN (3, 7, 11, 15)")
        .unwrap();
    let (_, live_before, stale_before) = hnsw_stats(&engine).unwrap();
    assert_eq!(stale_before, 4);
    assert_eq!(live_before, 16);

    engine.checkpoint().unwrap();
    engine.database().run_vector_maintenance_for_test();
    let (nodes_after, live_after, stale_after) = hnsw_stats(&engine).unwrap();
    assert_eq!(stale_after, 0, "compaction clears every tombstone");
    assert_eq!(live_after, 16, "live count is preserved");
    assert_eq!(
        nodes_after, 16,
        "compacted graph contains only the surviving 16 nodes"
    );

    // KNN must still return correct neighbours after the rebuild.
    let ids = id_list(
        &engine,
        "SELECT id FROM items ORDER BY DISTANCE(emb, '[1,0,0]') LIMIT 3",
    );
    assert!(!ids.contains(&3));
    assert!(!ids.contains(&7));
    assert_eq!(ids.len(), 3);
}

#[test]
fn hnsw_compaction_disabled_keeps_tombstones() {
    // Even with a 0% ratio gate, compaction must skip when explicitly
    // disabled via `MYDB_HNSW_COMPACT_DISABLE=1`.
    let _env = HnswEnv::set(&[
        ("MYDB_HNSW_PERSIST", "off"),
        ("MYDB_HNSW_COMPACT_RATIO", "0.0"),
        ("MYDB_HNSW_COMPACT_DISABLE", "1"),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    for i in 1..=10 {
        engine
            .execute(&format!(
                "INSERT INTO items VALUES ({i}, '{}')",
                vec_text(&vec![i as f32, 0.0, 0.0])
            ))
            .unwrap();
    }
    engine.database().vector_graph_warm_all_for_test();
    engine
        .execute("DELETE FROM items WHERE id IN (1, 2, 3)")
        .unwrap();
    engine.checkpoint().unwrap();
    let (nodes, live, stale) = hnsw_stats(&engine).unwrap();
    assert_eq!(stale, 3, "MYDB_HNSW_COMPACT_DISABLE pins tombstones");
    assert_eq!(live, 7);
    assert_eq!(nodes, 10);
}

#[test]
fn hnsw_compaction_absolute_gate_independently_of_ratio() {
    // Compaction gating is memory-backend behaviour; pin the backend.
    let _mem = HnswEnv::set(&[("MYDB_VECTOR_STORAGE", "memory")]);
    // Ratio gate set unreachably high so only the absolute gate
    // (`MYDB_HNSW_COMPACT_ABSOLUTE=1`) can fire compaction.
    let _env = HnswEnv::set(&[
        ("MYDB_HNSW_PERSIST", "off"),
        ("MYDB_HNSW_COMPACT_RATIO", "2.0"),
        ("MYDB_HNSW_COMPACT_ABSOLUTE", "1"),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    for i in 1..=12 {
        engine
            .execute(&format!(
                "INSERT INTO items VALUES ({i}, '{}')",
                vec_text(&vec![i as f32, 0.0, 0.0])
            ))
            .unwrap();
    }
    engine.database().vector_graph_warm_all_for_test();
    engine.execute("DELETE FROM items WHERE id = 1").unwrap();
    engine.checkpoint().unwrap();
    engine.database().run_vector_maintenance_for_test();
    let (nodes, live, stale) = hnsw_stats(&engine).unwrap();
    assert_eq!(stale, 0, "absolute gate fires at the first tombstone");
    assert_eq!(live, 11);
    assert_eq!(nodes, 11);
}

// -------- M4: parallel cold-build over the heap scan --------

/// Insert `vectors` into `items.emb` keyed by 1-based id, using
/// multi-row VALUES batches so the test is fast even at a few hundred
/// rows. The vector text helper escapes `[a,b,c]`.
fn bulk_insert_vectors(engine: &StorageQueryEngine, vectors: &[Vec<f32>]) {
    bulk_insert_vectors_at(engine, vectors, 1);
}

fn bulk_insert_vectors_at(engine: &StorageQueryEngine, vectors: &[Vec<f32>], first_id: usize) {
    const BATCH: usize = 256;
    for (chunk_idx, chunk) in vectors.chunks(BATCH).enumerate() {
        let mut sql = String::from("INSERT INTO items VALUES ");
        for (i, v) in chunk.iter().enumerate() {
            if i > 0 {
                sql.push(',');
            }
            let id = first_id + chunk_idx * BATCH + i;
            sql.push_str(&format!("({}, '{}')", id, vec_text(v)));
        }
        engine.execute(&sql).unwrap();
    }
}

/// Recall of a SQL KNN result vs the brute-force top-k over `vectors`.
/// Returns the fraction of brute-force ids the SQL result recovered.
fn knn_recall_vs_bruteforce(
    engine: &StorageQueryEngine,
    vectors: &[Vec<f32>],
    queries: &[Vec<f32>],
    k: usize,
) -> f64 {
    let mut hits = 0usize;
    let mut total = 0usize;
    for q in queries {
        let qtext = vec_text(q);
        let knn = format!("SELECT id FROM items ORDER BY DISTANCE(emb, '{qtext}') LIMIT {k}");
        let truth: std::collections::HashSet<i64> =
            brute_force_l2(vectors, q, k).into_iter().collect();
        for id in id_list(engine, &knn) {
            if truth.contains(&id) {
                hits += 1;
            }
        }
        total += k;
    }
    hits as f64 / total as f64
}

#[test]
fn hnsw_parallel_build_matches_brute_force_recall() {
    // M4: with MYDB_HNSW_BUILD_THREADS>1 the cold-build path splits the
    // heap into disjoint page ranges and concurrent workers each call
    // graph.insert(&self) under the M3 per-node latches. The resulting
    // graph topology depends on slab append order — which is now
    // non-deterministic — so we don't compare row-for-row against the
    // serial build; instead we hold the same recall floor as the serial
    // recall test (`recall_matches_brute_force_l2`) which is the actual
    // user-visible guarantee.
    let _env = HnswEnv::set(&[
        ("MYDB_HNSW_PERSIST", "off"),
        ("MYDB_HNSW_BUILD_ON_DDL", "off"),
        ("MYDB_HNSW_BUILD_THREADS", "4"),
        // Force the parallel branch even on a small test heap so we
        // genuinely exercise std::thread::scope + per-node latches.
        ("MYDB_HNSW_BUILD_MIN_PAGES_PER_WORKER", "1"),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(8) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();

    let dim = 8;
    let n = 160;
    let vectors = gen_vectors(n, dim, 0x4444_3333_2222_1111);
    bulk_insert_vectors(&engine, &vectors);

    // The graph must be cold until we explicitly warm it: the SQL KNN
    // fast path goes brute-force on cold graphs, and `note_vector_inserts`
    // skips cold graphs.
    assert!(
        engine
            .database()
            .hnsw_stats_for_test("items", "idx_emb")
            .is_none(),
        "graph must stay cold until the first warm"
    );
    // Warm — triggers `vector_graph_load_or_build` → parallel page-range
    // workers (persist=off, so no snapshot load happens).
    engine.database().vector_graph_warm_all_for_test();

    let (nodes, live, stale) = engine
        .database()
        .hnsw_stats_for_test("items", "idx_emb")
        .expect("graph warm after parallel cold-build");
    assert_eq!(nodes, n, "every heap row must land in the graph");
    assert_eq!(live, n);
    assert_eq!(stale, 0);

    // Recall @10 against brute-force, averaged over four query vectors.
    // Same 0.85 floor as the M3 concurrent-insert recall test (see
    // engine::vector::hnsw::tests::concurrent_inserts_match_serial_recall) —
    // non-deterministic append order can shave a few percent vs the
    // single-thread baseline but must not tank recall.
    let queries = gen_vectors(4, dim, 0x9999_8888_7777_6666);
    let recall = knn_recall_vs_bruteforce(&engine, &vectors, &queries, 10);
    assert!(
        recall >= 0.85,
        "parallel-build recall@10 = {recall:.3} too low"
    );
}

#[test]
fn hnsw_build_threads_one_uses_serial_path_and_still_correct() {
    // Belt-and-braces: with threads=1 we must use the original serial
    // scan (no std::thread::scope spawn), and the build still works.
    let _env = HnswEnv::set(&[
        ("MYDB_HNSW_PERSIST", "off"),
        ("MYDB_HNSW_BUILD_THREADS", "1"),
        ("MYDB_HNSW_BUILD_MIN_PAGES_PER_WORKER", "1"),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(6) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();

    let dim = 6;
    let n = 120;
    let vectors = gen_vectors(n, dim, 0xCAFE_F00D_BEEF_5050);
    bulk_insert_vectors(&engine, &vectors);

    engine.database().vector_graph_warm_all_for_test();
    let (nodes, live, _) = engine
        .database()
        .hnsw_stats_for_test("items", "idx_emb")
        .expect("graph warm");
    assert_eq!(nodes, n);
    assert_eq!(live, n);

    // Serial recall must also stay above the 0.85 floor — same input,
    // same insertion order as the pre-M4 path.
    let queries = gen_vectors(8, dim, 0x0123_4567_89AB_CDEF);
    let recall = knn_recall_vs_bruteforce(&engine, &vectors, &queries, 10);
    assert!(recall >= 0.85, "serial recall@10 = {recall:.3} too low");
}

#[test]
fn hnsw_build_threads_zero_falls_back_to_serial() {
    // Empty / unparseable / zero MYDB_HNSW_BUILD_THREADS must take the
    // serial path. Zero is reserved as "off"; an unparseable value
    // falls back to the auto default (>=1).
    let _env = HnswEnv::set(&[
        ("MYDB_HNSW_PERSIST", "off"),
        ("MYDB_HNSW_BUILD_THREADS", "0"),
        ("MYDB_HNSW_BUILD_MIN_PAGES_PER_WORKER", "1"),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(4) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    let dim = 4;
    let n = 80;
    let vectors = gen_vectors(n, dim, 0xABBA_CDDC_EFFE_0110);
    bulk_insert_vectors(&engine, &vectors);

    engine.database().vector_graph_warm_all_for_test();
    let (nodes, _, _) = engine
        .database()
        .hnsw_stats_for_test("items", "idx_emb")
        .expect("graph warm");
    assert_eq!(nodes, n);
}

#[test]
fn hnsw_parallel_build_picks_up_persisted_snapshot_delta() {
    // `.hnsw` snapshot + catch-up is memory-backend only; pin the backend.
    let _mem = HnswEnv::set(&[("MYDB_VECTOR_STORAGE", "memory")]);
    // Composes M1 + M4: a persisted .hnsw snapshot pins half the rows,
    // and the catch-up walk for the other half runs through the
    // parallel page-range workers. The graph after warm must contain
    // every row exactly once — no duplicates from the snapshot rows
    // being re-inserted by the parallel pass (the `known` dedup set
    // is shared across workers as a &HashSet<RowId>).
    let _env = HnswEnv::set(&[
        ("MYDB_HNSW_PERSIST", "on"),
        ("MYDB_HNSW_PERSIST_DIRTY_RATIO", "0.0"),
        ("MYDB_HNSW_BUILD_THREADS", "4"),
        ("MYDB_HNSW_BUILD_MIN_PAGES_PER_WORKER", "1"),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let dim = 6;
    let n_snap = 60;
    let n_extra = 40;
    let mut vectors = gen_vectors(n_snap + n_extra, dim, 0x1357_2468_ACEB_FDB0);
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(6) NOT NULL)")
            .unwrap();
        engine
            .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
            .unwrap();
        // First batch: lands in the snapshot.
        bulk_insert_vectors(&engine, &vectors[..n_snap]);
        engine.database().vector_graph_warm_all_for_test();
        engine.checkpoint().unwrap();
        // Second batch: lives in the heap only — the next reopen's
        // catch-up walk has to pick them up via the parallel workers.
        for (idx, v) in vectors[n_snap..].iter().enumerate() {
            engine
                .execute(&format!(
                    "INSERT INTO items VALUES ({}, '{}')",
                    n_snap + idx + 1,
                    vec_text(v)
                ))
                .unwrap();
        }
    }

    let reopened = open_strict_test_engine(dir.path());
    reopened.database().vector_graph_warm_all_for_test();
    let (nodes, live, _) = reopened
        .database()
        .hnsw_stats_for_test("items", "idx_emb")
        .expect("graph warm after reopen");
    assert_eq!(
        nodes,
        n_snap + n_extra,
        "snapshot rows must not be re-added by the catch-up walk"
    );
    assert_eq!(live, n_snap + n_extra);

    let queries = gen_vectors(12, dim, 0xBEAD_BABA_CAFE_F00D);
    let recall = knn_recall_vs_bruteforce(&reopened, &vectors, &queries, 10);
    assert!(
        recall >= 0.85,
        "snapshot+delta parallel recall@10 = {recall:.3} too low"
    );
    // Touch `vectors` after the asserts so the closure-capture lifetime
    // is unambiguous — defensive against future clippy passes.
    vectors.shrink_to_fit();
}

// =============================================================================
// M5 — query path completions (docs/hnsw-production-design.md §M5).
// =============================================================================

/// M5.1 — `mydb_hnsw_ef_search` session variable advertises through the
/// sysvar registry with the documented default, and round-trips through
/// `SET SESSION` for runtime tuning.
#[test]
fn m5_ef_search_sysvar_registered_with_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Default values from `SessionSysVarState::default()`.
    assert_eq!(cell(&engine, "SELECT @@SESSION.mydb_hnsw_ef_search"), "64");
    assert_eq!(
        cell(&engine, "SELECT @@SESSION.mydb_hnsw_ef_dynamic_factor"),
        "4"
    );
    assert_eq!(
        cell(&engine, "SELECT @@SESSION.mydb_hnsw_iterative_scan"),
        "strict_order"
    );
    assert_eq!(
        cell(&engine, "SELECT @@SESSION.mydb_hnsw_max_scan_tuples"),
        "20000"
    );
    assert_eq!(
        cell(&engine, "SELECT @@SESSION.mydb_hnsw_brute_threshold"),
        "10000"
    );
    assert_eq!(
        cell(&engine, "SELECT @@SESSION.mydb_hnsw_brute_filter_rows"),
        "1000"
    );
    // SET SESSION must update the visible value and reject obviously bad
    // values for the typed knobs.
    engine
        .execute("SET SESSION mydb_hnsw_iterative_scan = 'relaxed_order'")
        .unwrap();
    assert!(engine
        .execute("SET SESSION mydb_hnsw_iterative_scan = 'wrong'")
        .is_err());
}

/// M5.1 — the HINT `/*+ HNSW_EF(n) */` overrides the default at plan time
/// and shows up in EXPLAIN's Extra column. Resolution priority is
/// HINT > session > default.
#[test]
fn m5_hint_hnsw_ef_overrides_default_and_session() {
    let _env = disable_brute_fallback_env();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(4) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    for i in 1..=20i32 {
        let v = [(i as f32) * 0.1, 0.0, 0.0, 0.0];
        engine
            .execute(&format!(
                "INSERT INTO items VALUES ({i}, '{}')",
                vec_text(&v)
            ))
            .unwrap();
    }
    // Baseline — no hint, default ef_search=64.
    let baseline = rows(
        &engine,
        "EXPLAIN SELECT id FROM items ORDER BY DISTANCE(emb, '[0,0,0,0]') LIMIT 5",
    );
    assert!(baseline
        .iter()
        .flatten()
        .any(|c| c.to_display_string().contains("ef_search=64")));
    // Hint wins.
    let hinted = rows(
        &engine,
        "EXPLAIN SELECT /*+ HNSW_EF(123) */ id FROM items ORDER BY DISTANCE(emb, '[0,0,0,0]') LIMIT 5",
    );
    assert!(
        hinted
            .iter()
            .flatten()
            .any(|c| c.to_display_string().contains("ef_search=123")),
        "EXPLAIN should reflect HNSW_EF(123); got: {hinted:?}"
    );
}

/// M5.2 — the `iterative_scan = off` policy disables widening; a
/// pathological `LIMIT k` larger than the candidate pool returns whatever
/// the single pass yields (still correct since we re-sort by distance
/// when policy is `strict_order` — but `off` is the gate to verify
/// dispatch picks up the session value).
#[test]
fn m5_iterative_scan_strict_order_default_returns_full_topk() {
    let _env = disable_brute_fallback_env();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(4) NOT NULL, tag INT)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    // 40 rows alternating tag=0/1. WHERE tag=1 has ~50% selectivity; even
    // with a tight base ef the iterative loop must widen until 5 survive.
    let dim = 4;
    let n = 40;
    let vectors = gen_vectors(n, dim, 0xAAAA_BBBB_CCCC_DDDD);
    for (i, v) in vectors.iter().enumerate() {
        let tag = (i % 2) as i32;
        engine
            .execute(&format!(
                "INSERT INTO items VALUES ({}, '{}', {})",
                i + 1,
                vec_text(v),
                tag
            ))
            .unwrap();
    }
    // Tight base ef. Without iterative widening the residual filter could
    // drop below k=5. With strict_order (the default), we must still get
    // exactly 5 rows.
    engine
        .execute("SET SESSION mydb_hnsw_ef_search = 8")
        .unwrap();
    let knn = "SELECT id FROM items WHERE tag = 1 ORDER BY DISTANCE(emb, '[0,0,0,0]') LIMIT 5";
    let got = id_list(&engine, knn);
    assert_eq!(got.len(), 5, "iterative scan must surface all 5 rows");
    // And every returned row matches the filter.
    for row in rows(
        &engine,
        "SELECT tag FROM items WHERE tag = 1 ORDER BY DISTANCE(emb, '[0,0,0,0]') LIMIT 5",
    ) {
        assert_eq!(row[0].to_display_string(), "1");
    }
}

#[test]
fn hnsw_topk_candidates_widen_for_snapshot_visibility_and_keep_transaction_delta() {
    let _env = disable_brute_fallback_env();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(2) NOT NULL)")
        .unwrap();
    for id in 100..140 {
        engine
            .execute(&format!("INSERT INTO items VALUES ({id}, '[{id},1]')"))
            .unwrap();
    }
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    let query = "SELECT id FROM items ORDER BY DISTANCE(emb, '[0,1]') LIMIT 5";
    let mut snapshot = SessionContext::default();
    engine
        .execute_with_session(
            &mut snapshot,
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        )
        .unwrap();
    engine.execute_with_session(&mut snapshot, "BEGIN").unwrap();
    assert_eq!(
        id_list_session(&engine, &mut snapshot, query),
        vec![100, 101, 102, 103, 104]
    );
    // The current graph now ranks twenty rows ahead of every snapshot-visible
    // row. Returning only the first k candidates must widen, not return empty.
    for id in 1..=20 {
        engine
            .execute(&format!("INSERT INTO items VALUES ({id}, '[{id},1]')"))
            .unwrap();
    }
    for policy in ["strict_order", "off"] {
        engine
            .execute_with_session(
                &mut snapshot,
                &format!("SET SESSION mydb_hnsw_iterative_scan = '{policy}'"),
            )
            .unwrap();
        assert_eq!(
            id_list_session(&engine, &mut snapshot, query),
            vec![100, 101, 102, 103, 104]
        );
    }
    engine
        .execute_with_session(&mut snapshot, "ROLLBACK")
        .unwrap();
    assert_eq!(id_list(&engine, query), vec![1, 2, 3, 4, 5]);
    engine
        .execute_with_session(
            &mut snapshot,
            "SET SESSION mydb_hnsw_iterative_scan = 'strict_order'",
        )
        .unwrap();
    engine.execute_with_session(&mut snapshot, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut snapshot, "UPDATE items SET emb = '[0,1]' WHERE id=139")
        .unwrap();
    assert_eq!(
        id_list_session(&engine, &mut snapshot, query),
        vec![139, 1, 2, 3, 4]
    );
    engine
        .execute_with_session(&mut snapshot, "ROLLBACK")
        .unwrap();
    assert_eq!(id_list(&engine, query), vec![1, 2, 3, 4, 5]);
}

/// M5.3 — the brute-force fallback skips the HNSW plan when the table is
/// smaller than the threshold. EXPLAIN should NOT mention "vector index"
/// in that case (the planner falls back to a plain seq scan + sort).
#[test]
fn m5_brute_fallback_under_threshold_skips_hnsw() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    for i in 1..=10i32 {
        let v = [(i as f32) * 0.25, 0.0, 0.0];
        engine
            .execute(&format!(
                "INSERT INTO items VALUES ({i}, '{}')",
                vec_text(&v)
            ))
            .unwrap();
    }
    let knn = "SELECT id FROM items ORDER BY DISTANCE(emb, '[0,0,0]') LIMIT 3";
    // Default threshold is 10000 → 10-row table goes brute.
    assert!(
        !explain_mentions_vector_index(&engine, knn),
        "small table should brute-fallback by default"
    );
    // Result is still correct (smallest emb[0] wins on L2).
    assert_eq!(id_list(&engine, knn), vec![1, 2, 3]);
    // The hint forces HNSW even on a small table.
    let force_hnsw =
        "SELECT /*+ HNSW_EF(32) */ id FROM items ORDER BY DISTANCE(emb, '[0,0,0]') LIMIT 3";
    assert!(
        explain_mentions_vector_index(&engine, force_hnsw),
        "HNSW_EF hint must beat the brute-fallback gate"
    );
    // `NO_VECTOR_INDEX` forces brute even when the threshold is lowered
    // via env (which survives the per-`execute` `SessionContext::default()`
    // rebuild, unlike `SET SESSION`).
    crate::util::env::with_vars(&[("MYDB_HNSW_BRUTE_THRESHOLD", Some("0"))], || {
        // With threshold=0 the small-table gate no longer fires → HNSW is
        // back in play.
        assert!(explain_mentions_vector_index(&engine, knn));
        let force_brute = "SELECT /*+ NO_VECTOR_INDEX */ id FROM items \
            ORDER BY DISTANCE(emb, '[0,0,0]') LIMIT 3";
        assert!(
            !explain_mentions_vector_index(&engine, force_brute),
            "NO_VECTOR_INDEX hint must skip the HNSW path"
        );
        assert_eq!(id_list(&engine, force_brute), vec![1, 2, 3]);
    });
}

/// M5.4 — `<->` (L2) and `<#>` (negative inner product) pgvector
/// operators desugar to the existing distance functions before parsing.
/// The KNN plan must recognise them and the result must match the
/// function form.
#[test]
fn m5_pgvector_arrow_operators_desugar_and_use_index() {
    let _env = disable_brute_fallback_env();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    let vectors: Vec<Vec<f32>> = (1..=15)
        .map(|i| vec![(i as f32) * 0.1, ((i % 4) as f32) * 0.2, 0.0])
        .collect();
    bulk_insert_vectors(&engine, &vectors);
    // `<->` is L2 — equivalent to L2_DISTANCE.
    let arrow_sql = "SELECT id FROM items ORDER BY emb <-> '[0.0,0.0,0.0]' LIMIT 5";
    let func_sql = "SELECT id FROM items ORDER BY L2_DISTANCE(emb, '[0.0,0.0,0.0]') LIMIT 5";
    assert!(
        explain_mentions_vector_index(&engine, arrow_sql),
        "EXPLAIN of `<->` query must show vector index"
    );
    assert_eq!(
        id_list(&engine, arrow_sql),
        id_list(&engine, func_sql),
        "`<->` ORDER BY must match L2_DISTANCE ORDER BY"
    );
    // `<#>` is pgvector's negative inner product — smaller is "closer".
    // We don't have an HNSW DOT_PRODUCT index for these vectors (they were
    // built with the default L2 metric), so this exercises the desugar
    // surface: the planner falls back to a full sort, but the result must
    // match `-DOT_PRODUCT` ORDER BY.
    let neg_ip_sql = "SELECT id FROM items ORDER BY emb <#> '[1.0,0.0,0.0]' LIMIT 3";
    let neg_ip_func_sql =
        "SELECT id FROM items ORDER BY (0 - DOT_PRODUCT(emb, '[1.0,0.0,0.0]')) LIMIT 3";
    assert_eq!(
        id_list(&engine, neg_ip_sql),
        id_list(&engine, neg_ip_func_sql)
    );
    // `<=>` is still MySQL's NULL-safe equality — must NOT be rewritten.
    // A scalar NULL-safe comparison returns 0 / 1 — the row count below
    // is just a sanity check that parsing doesn't break.
    let nullsafe = rows(&engine, "SELECT 1 <=> 1, 1 <=> NULL");
    assert_eq!(nullsafe.len(), 1);
    assert_eq!(nullsafe[0][0].to_display_string(), "1");
    assert_eq!(nullsafe[0][1].to_display_string(), "0");
}

/// M5.5 — partitioned tables: per-partition HNSW graphs + scatter-gather
/// merge. Pre-M5 the planner rejected partitioned tables and the query
/// fell back to a sort over a full scan; now each partition keeps its
/// own graph and the executor merges per-partition top-k.
#[test]
fn m5_partitioned_table_knn_uses_per_partition_graphs() {
    let _env = disable_brute_fallback_env();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE items (\
                id INT NOT NULL,\
                bucket INT NOT NULL,\
                emb VECTOR(3) NOT NULL,\
                PRIMARY KEY (id, bucket)\
            ) PARTITION BY HASH(bucket) PARTITIONS 4",
        )
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    // Spread 32 rows across 4 partitions; each `bucket` value routes to a
    // different partition under HASH(bucket).
    let vectors: Vec<Vec<f32>> = (1..=32)
        .map(|i| {
            let f = (i as f32) * 0.05;
            vec![f, f * 0.5, f * -0.25]
        })
        .collect();
    for (i, v) in vectors.iter().enumerate() {
        let id = (i + 1) as i32;
        let bucket = (i % 4) as i32;
        engine
            .execute(&format!(
                "INSERT INTO items VALUES ({id}, {bucket}, '{}')",
                vec_text(v)
            ))
            .unwrap();
    }
    let query = vec![0.05f32, 0.025, -0.0125]; // closest to id=1
    let qtext = vec_text(&query);
    let knn = format!("SELECT id FROM items ORDER BY DISTANCE(emb, '{qtext}') LIMIT 5");
    let explain_dump = rows(&engine, &format!("EXPLAIN {}", knn));
    assert!(
        explain_mentions_vector_index(&engine, &knn),
        "EXPLAIN should show vector index for partitioned KNN; got: {explain_dump:?}"
    );
    let got = id_list(&engine, &knn);
    let expected = brute_force_l2(&vectors, &query, 5);
    assert_eq!(
        got, expected,
        "scatter-gather KNN must match brute-force top-5"
    );
    // Verify per-partition graphs exist (graph map should hold 4 entries
    // under the same index name, one per storage_id). For partitioned
    // tables the executor's first call to `vector_index_search_for_partition`
    // is what triggers the lazy build.
    let (nodes, live, _) = engine
        .database()
        .hnsw_stats_for_test("items", "idx_emb")
        .expect("graph warm after KNN search (no entry for items/idx_emb)");
    assert!(nodes >= 32, "got nodes={nodes}, expected ≥ 32");
    assert!(live >= 32, "got live={live}, expected ≥ 32");
}

/// M5.5 — DML against a partitioned table updates the matching partition
/// graph, not some single union graph. A row inserted into bucket=2 must
/// be visible in a KNN search even if buckets 0/1/3 have many other
/// rows competing for the candidate pool.
#[test]
fn m5_partitioned_table_dml_routes_to_matching_partition_graph() {
    let _env = disable_brute_fallback_env();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE items (\
                id INT NOT NULL,\
                bucket INT NOT NULL,\
                emb VECTOR(2) NOT NULL,\
                PRIMARY KEY (id, bucket)\
            ) PARTITION BY HASH(bucket) PARTITIONS 4",
        )
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    // Seed each bucket with one row.
    for b in 0..4i32 {
        engine
            .execute(&format!(
                "INSERT INTO items VALUES ({}, {}, '[{},0]')",
                b + 1,
                b,
                (b as f32) + 10.0
            ))
            .unwrap();
    }
    // Warm graphs.
    let _ = id_list(
        &engine,
        "SELECT id FROM items ORDER BY DISTANCE(emb, '[10,0]') LIMIT 1",
    );
    // Insert a new closer-to-origin row in bucket=2 only.
    engine
        .execute("INSERT INTO items VALUES (99, 2, '[0.01,0]')")
        .unwrap();
    let got = id_list(
        &engine,
        "SELECT id FROM items ORDER BY DISTANCE(emb, '[0,0]') LIMIT 1",
    );
    assert_eq!(got, vec![99], "warm partition graph must reflect insert");
    // DELETE that row — KNN must surface it no more.
    engine.execute("DELETE FROM items WHERE id = 99").unwrap();
    let got = id_list(
        &engine,
        "SELECT id FROM items ORDER BY DISTANCE(emb, '[0,0]') LIMIT 1",
    );
    assert_ne!(got, vec![99], "warm partition graph must reflect delete");
}

/// Run `sql` under an explicit `MYDB_VECTORIZED` mode, returning the first
/// column of every row as a string (or the error message). `with_vec_mode`
/// uses a thread-local override so it composes with the process-wide
/// `OnceLock` mode cache — no env mutation, safe for parallel tests.
fn rows_under_mode(
    engine: &StorageQueryEngine,
    mode: crate::engine::executor::vec::config::VecMode,
    sql: &str,
) -> Result<Vec<String>, String> {
    crate::engine::executor::vec::config::with_vec_mode(mode, || match engine.execute(sql) {
        Ok(QueryResponse::ResultSet(rs)) => Ok(rs
            .rows
            .into_iter()
            .map(|r| r[0].to_display_string())
            .collect()),
        Ok(other) => panic!("expected a result set for `{sql}`, got {other:?}"),
        Err(e) => Err(format!("{e:?}")),
    })
}

/// Regression: the vectorized Top-N path for `ORDER BY DISTANCE(col, const)
/// LIMIT k` over a real `VECTOR` column. The `Bytes` accumulator lane used
/// to drop `Value::Vector` payloads (its `push` only matched `Value::Bytes`),
/// emptying every stored vector and tripping VECTOR validation (errno 50001).
/// The fix routes VECTOR through the type-preserving `Generic` lane. Asserts
/// vectorized output matches the row pipeline (the oracle) exactly.
#[test]
fn vec_knn_topk_matches_row_pipeline() {
    use crate::engine::executor::vec::config::VecMode;
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE v(id INT PRIMARY KEY, emb VECTOR(8) NOT NULL)")
        .unwrap();
    // >= 50 rows so Auto/On vectorize the Top-N rather than the small-input
    // row fallback; 200 keeps a comfortable margin.
    let vs = gen_vectors(200, 8, 42);
    for (i, vv) in vs.iter().enumerate() {
        engine
            .execute(&format!(
                "INSERT INTO v VALUES ({}, '{}')",
                i + 1,
                vec_text(vv)
            ))
            .unwrap();
    }

    // KNN top-k with a STRING_TO_VECTOR constant query — the exact shape
    // from the bug report.
    let knn = "SELECT id FROM v \
               ORDER BY DISTANCE(emb, STRING_TO_VECTOR('[1,1,1,1,1,1,1,1]')) LIMIT 10";
    let off = rows_under_mode(&engine, VecMode::Off, knn).expect("row pipeline must succeed");
    let on = rows_under_mode(&engine, VecMode::On, knn);
    assert_eq!(Ok(off.clone()), on, "vec KNN top-k must match row pipeline");

    // Same shape with a string-literal query vector (different constant
    // encoding path) and an integer-only vector.
    let knn_lit = "SELECT id FROM v ORDER BY DISTANCE(emb, '[0,0,0,0,0,0,0,0]') LIMIT 5";
    let off_lit = rows_under_mode(&engine, VecMode::Off, knn_lit).unwrap();
    assert_eq!(
        Ok(off_lit),
        rows_under_mode(&engine, VecMode::On, knn_lit),
        "vec KNN top-k (literal query) must match row pipeline"
    );

    // Projecting the VECTOR column itself must also round-trip through the
    // vec pipeline — guards the `Generic` lane's value_at against the old
    // type-erasing `Bytes` lane (which rendered vectors as raw bytes).
    let proj = "SELECT VECTOR_TO_STRING(emb) FROM v ORDER BY id LIMIT 3";
    let off_proj = rows_under_mode(&engine, VecMode::Off, proj).unwrap();
    assert_eq!(
        Ok(off_proj),
        rows_under_mode(&engine, VecMode::On, proj),
        "projected VECTOR column must match row pipeline under vectorization"
    );
}

// -------- Brute-force exact KNN (no HNSW index): VectorBruteScan --------

/// True if the EXPLAIN (traditional) for `query` shows the brute-force
/// vector scan (not a vector index, not a fallback Sort).
fn explain_mentions_brute_vector(engine: &StorageQueryEngine, query: &str) -> bool {
    rows(engine, &format!("EXPLAIN {query}"))
        .iter()
        .flatten()
        .any(|c| {
            c.to_display_string()
                .to_ascii_lowercase()
                .contains("brute-force vector")
        })
}

#[test]
fn brute_knn_no_index_matches_bruteforce_and_explains() {
    let dir = tempfile::tempdir().unwrap();
    // Plain engine: no HNSW brute-fallback override, and crucially NO index
    // is created — so `ORDER BY DISTANCE(...) LIMIT k` must route to the new
    // VectorBruteScan, not a VectorIndexScan or a generic Sort.
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(6) NOT NULL)")
        .unwrap();

    let n = 50;
    let dim = 6;
    let vectors = gen_vectors(n, dim, 0x0BAD_F00D_1234_5678);
    bulk_insert_vectors(&engine, &vectors);

    let query = gen_vectors(1, dim, 0xFACE_0FF1_CE00_0001)[0].clone();
    let qtext = vec_text(&query);
    let k = 7;
    let knn = format!("SELECT id FROM items ORDER BY DISTANCE(emb, '{qtext}') LIMIT {k}");

    // Plan shape: brute-force vector scan, and definitely NOT the HNSW index.
    assert!(
        explain_mentions_brute_vector(&engine, &knn),
        "EXPLAIN should show the brute-force vector scan for `{knn}`"
    );
    assert!(
        !explain_mentions_vector_index(&engine, &knn),
        "no HNSW index exists — must not claim a vector index scan"
    );

    // Exact: brute scan must equal the independently computed top-k, in the
    // same (distance, id) order.
    let got = id_list(&engine, &knn);
    let expected = brute_force_l2(&vectors, &query, k);
    assert_eq!(got, expected, "brute KNN must match the computed top-{k}");

    // The FORMAT=TREE plan suppresses the redundant Sort (the scan already
    // emits rows ascending by distance).
    let tree: String = rows(&engine, &format!("EXPLAIN FORMAT=TREE {knn}"))
        .into_iter()
        .flatten()
        .map(|c| c.to_display_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        tree.to_ascii_lowercase().contains("brute-force scan"),
        "tree plan should name the brute-force scan: {tree}"
    );
    assert!(
        !tree.to_ascii_lowercase().contains("sort"),
        "brute scan satisfies the ORDER BY; no Sort expected: {tree}"
    );
}

#[test]
fn brute_knn_no_index_parallel_matches_bruteforce() {
    // Force the parallel page-range fan-out even for a modest table by
    // dropping the per-worker page floor to 1. Multi-page + multi-core =>
    // the BruteTopK merge path is exercised; the result must still be exact.
    let _env = HnswEnv::set(&[
        ("MYDB_VECTOR_BRUTE_MIN_PAGES_PER_WORKER", "1"),
        // On SME-capable ARM this exercises the production batch kernel; on
        // every other target the runtime capability check safely falls back.
        ("MYDB_VECTOR_BRUTE_SME", "on"),
        ("MYDB_VECTOR_BRUTE_SME_BATCH_ROWS", "64"),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(32) NOT NULL)")
        .unwrap();

    // Enough wide rows to span several heap pages so the scan splits.
    let n = 160;
    let dim = 32;
    let vectors = gen_vectors(n, dim, 0x5151_2727_3939_0001);
    bulk_insert_vectors(&engine, &vectors);

    let query = gen_vectors(1, dim, 0x1234_ABCD_5678_EF01)[0].clone();
    let qtext = vec_text(&query);
    let k = 10;
    let knn = format!("SELECT id FROM items ORDER BY DISTANCE(emb, '{qtext}') LIMIT {k}");

    let got = id_list(&engine, &knn);
    let expected = brute_force_l2(&vectors, &query, k);
    assert_eq!(
        got, expected,
        "parallel brute KNN must match the computed top-{k}"
    );
}

#[test]
fn brute_knn_no_index_excludes_deleted_and_respects_metric() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
        .unwrap();
    // Axis-aligned vectors with an obvious nearest order to [10,0,0].
    let data = [
        (1, [9.0_f32, 0.0, 0.0]),
        (2, [0.0, 9.0, 0.0]),
        (3, [8.0, 0.0, 0.0]),
        (4, [0.0, 0.0, 9.0]),
        (5, [10.0, 1.0, 0.0]),
    ];
    for (id, v) in &data {
        engine
            .execute(&format!(
                "INSERT INTO items VALUES ({id}, '{}')",
                vec_text(v)
            ))
            .unwrap();
    }
    // Delete the would-be nearest neighbour; it must drop out of the result.
    engine.execute("DELETE FROM items WHERE id = 5").unwrap();

    let knn = "SELECT id FROM items ORDER BY DISTANCE(emb, '[10,0,0]') LIMIT 3";
    assert!(explain_mentions_brute_vector(&engine, knn));
    // Nearest to [10,0,0] among survivors: id1 (9,0,0), id3 (8,0,0), then the
    // off-axis ones tie at distance and break by id (2 before 4).
    assert_eq!(id_list(&engine, knn), vec![1, 3, 2]);

    // COSINE metric ranks by direction: id1/id3/id5-deleted all point along
    // +x (cosine distance 0), so among survivors id1 and id3 come first.
    let cos = "SELECT id FROM items ORDER BY COSINE_DISTANCE(emb, '[10,0,0]') LIMIT 2";
    let got = id_list(&engine, cos);
    assert_eq!(
        got,
        vec![1, 3],
        "cosine nearest must be the +x-aligned rows"
    );
}

#[test]
fn brute_knn_no_vector_index_hint_uses_brute_not_generic() {
    // With an HNSW index present, `/*+ NO_VECTOR_INDEX */` must bypass the
    // graph — and now lands on the brute-force scan (exact, faster than the
    // old generic Sort), still returning the correct top-k.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(6) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    let n = 30;
    let dim = 6;
    let vectors = gen_vectors(n, dim, 0xC0FF_EE00_1111_2222);
    bulk_insert_vectors(&engine, &vectors);
    let query = gen_vectors(1, dim, 0x9999_8888_7777_6666)[0].clone();
    let qtext = vec_text(&query);
    let k = 5;
    let knn = format!(
        "SELECT /*+ NO_VECTOR_INDEX */ id FROM items ORDER BY DISTANCE(emb, '{qtext}') LIMIT {k}"
    );
    assert!(
        explain_mentions_brute_vector(&engine, &knn),
        "NO_VECTOR_INDEX should route to the brute-force scan"
    );
    assert!(!explain_mentions_vector_index(&engine, &knn));
    assert_eq!(id_list(&engine, &knn), brute_force_l2(&vectors, &query, k));
}

#[test]
fn brute_vector_bulk_reader_sees_dirty_updates_deletes_and_nulls() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3))")
        .unwrap();
    engine
        .execute(
            "INSERT INTO items VALUES (1, '[9,9,9]'), (2, '[0,0,0]'), (3, '[2,2,2]'), (4, NULL)",
        )
        .unwrap();
    engine
        .execute("UPDATE items SET emb = '[0.1,0.1,0.1]' WHERE id = 1")
        .unwrap();
    engine.execute("DELETE FROM items WHERE id = 2").unwrap();
    let query =
        "SELECT id FROM items WHERE emb IS NOT NULL ORDER BY DISTANCE(emb, '[0,0,0]') LIMIT 2";
    // The unfiltered form selects the specialized brute operator and must
    // preserve ASC NULLs-first ordering while reading dirty-page winners.
    let brute = "SELECT id FROM items ORDER BY DISTANCE(emb, '[0,0,0]') LIMIT 2";
    assert!(explain_mentions_brute_vector(&engine, brute));
    assert_eq!(id_list(&engine, brute), vec![4, 1]);
    assert_eq!(id_list(&engine, query), vec![1, 3]);
}

// ===================== Paged HNSW backend (MYDB_VECTOR_STORAGE=paged) ========
//
// These exercise the buffer-pool-resident `.hvec` backend through the full SQL
// engine, behind the `MYDB_VECTOR_STORAGE=paged` routing GUC. They assert the
// paged graph answers KNN identically to brute force, is maintained on
// DELETE/UPDATE, survives a restart purely from its durable on-disk graph
// (no heap rebuild needed), and is reclaimed on DROP INDEX.

/// Set the paged backend live AND disable the brute-force fallback, so KNN is
/// forced onto the `VectorIndexScan` (paged graph) path. The returned guard
/// restores prior env on drop.
fn paged_index_env() -> HnswEnv {
    HnswEnv::set(&[
        ("MYDB_VECTOR_STORAGE", "paged"),
        ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
        ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
    ])
}

/// Locate every `.hvec` paged-graph file in the ID-sharded object store.
fn find_hvec_files(data_dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    find_object_files_with_extension(data_dir, "hvec")
}

#[test]
fn paged_hnsw_knn_matches_brute_and_uses_index() {
    let _env = paged_index_env();
    let dir = tempfile::tempdir().unwrap();
    let dim = 8;
    let n = 30;
    let vectors = gen_vectors(n, dim, 0xBEEF_0001_2222_3333);
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(8) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    bulk_insert_vectors(&engine, &vectors);
    let query = gen_vectors(1, dim, 0x0123_4567_89AB_CDEF)[0].clone();
    let qtext = vec_text(&query);
    let k = 10;
    let knn = format!("SELECT id FROM items ORDER BY DISTANCE(emb, '{qtext}') LIMIT {k}");

    // The paged graph is on the index path…
    assert!(
        explain_mentions_vector_index(&engine, &knn),
        "paged backend KNN must use the VectorIndexScan path"
    );
    // …and answers identically to brute force (graph is exact-recall here).
    assert_eq!(
        id_list(&engine, &knn),
        brute_force_l2(&vectors, &query, k),
        "paged HNSW KNN must match brute-force nearest neighbours"
    );
    // The durable `.hvec` graph file was created (not a `.hnsw` snapshot).
    assert!(
        !find_hvec_files(dir.path()).is_empty(),
        "paged backend must produce a .hvec graph file"
    );
}

#[test]
fn paged_hnsw_delete_and_update_are_reflected() {
    let _env = paged_index_env();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    for i in 1..=8 {
        let v = [i as f32, 0.0, 0.0];
        engine
            .execute(&format!(
                "INSERT INTO items VALUES ({i}, '{}')",
                vec_text(&v)
            ))
            .unwrap();
    }
    let knn3 = "SELECT id FROM items ORDER BY DISTANCE(emb, '[3,0,0]') LIMIT 1";
    // Warm the graph onto the index path and confirm the exact hit.
    assert!(explain_mentions_vector_index(&engine, knn3));
    assert_eq!(id_list(&engine, knn3), vec![3]);

    // DELETE the nearest row: the next-nearest must surface, and the deleted
    // row must never come back (tombstoned in the paged graph + MVCC filtered).
    engine.execute("DELETE FROM items WHERE id = 3").unwrap();
    let near3 = id_list(
        &engine,
        "SELECT id FROM items ORDER BY DISTANCE(emb, '[3,0,0]') LIMIT 3",
    );
    assert!(
        !near3.contains(&3),
        "deleted row must not be returned: {near3:?}"
    );
    assert_eq!(near3, vec![2, 4, 1]);

    // UPDATE row 1's vector to be nearest to a far query; it should now win.
    engine
        .execute("UPDATE items SET emb = '[100,0,0]' WHERE id = 1")
        .unwrap();
    assert_eq!(
        id_list(
            &engine,
            "SELECT id FROM items ORDER BY DISTANCE(emb, '[100,0,0]') LIMIT 1"
        ),
        vec![1],
        "updated vector must be reflected in the paged graph ranking"
    );
}

#[test]
fn paged_hnsw_survives_restart_from_durable_graph() {
    let _env = paged_index_env();
    let dir = tempfile::tempdir().unwrap();
    let dim = 6;
    let n = 20;
    let vectors = gen_vectors(n, dim, 0xD00D_FEED_4444_5555);
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(6) NOT NULL)")
            .unwrap();
        engine
            .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
            .unwrap();
        bulk_insert_vectors(&engine, &vectors);
        // Build the graph (first query) before the restart.
        let q = vec_text(&vectors[0]);
        assert_eq!(
            id_list(
                &engine,
                &format!("SELECT id FROM items ORDER BY DISTANCE(emb, '{q}') LIMIT 1")
            ),
            vec![1]
        );
        assert!(!find_hvec_files(dir.path()).is_empty());
    }

    // Reopen the same data dir: the paged graph is durable on disk (buffer
    // pool + WAL), so the first post-restart KNN reads it back and answers
    // correctly — still on the index path.
    let reopened = open_strict_test_engine(dir.path());
    let query = gen_vectors(1, dim, 0x7777_6666_5555_4444)[0].clone();
    let qtext = vec_text(&query);
    let k = 5;
    let knn = format!("SELECT id FROM items ORDER BY DISTANCE(emb, '{qtext}') LIMIT {k}");
    assert!(
        explain_mentions_vector_index(&reopened, &knn),
        "post-restart paged KNN must use the index path"
    );
    assert_eq!(
        id_list(&reopened, &knn),
        brute_force_l2(&vectors, &query, k),
        "paged graph must answer correctly after restart"
    );
}

#[test]
fn paged_hnsw_missing_stats_after_restart_uses_physical_row_estimate() {
    // Keep the ordinary brute crossover enabled. Before this regression fix,
    // restart cleared the live modification sidecar, `total_rows` became zero,
    // and the first unhinted KNN chose an O(n) brute heap scan without ever
    // opening the finalized `.hvec` file.
    let _env = HnswEnv::set(&[
        ("MYDB_VECTOR_STORAGE", "paged"),
        ("MYDB_HNSW_BRUTE_THRESHOLD", "32"),
        ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
        ("MYDB_HNSW_EF_SEARCH", "1"),
    ]);
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(6) NOT NULL)")
            .unwrap();
        engine
            .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW WITH (ef_search=1)")
            .unwrap();
        // The fixture stays well beyond the configured non-zero brute
        // crossover, proving physical page estimates keep the index eligible.
        let values = (1..=128)
            .map(|id| format!("({id},'[{},0,0,0,0,0]')", id as f32))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO items VALUES {values}"))
            .unwrap();
    }

    let reopened = open_strict_test_engine(dir.path());
    let knn = "SELECT id FROM items ORDER BY DISTANCE(emb, '[1,0,0,0,0,0]') LIMIT 1";
    let storage_key = reopened
        .database()
        .storage_key_for_table(&reopened.database().table("test.items").unwrap());
    assert_eq!(
        reopened
            .database()
            .table("test.items")
            .unwrap()
            .stats
            .row_count,
        0,
        "the regression setup requires missing catalog row statistics"
    );
    let heap_pages = reopened
        .database()
        .heap_page_count_by_name(&storage_key)
        .unwrap();
    let explain_dump = rows(&reopened, &format!("EXPLAIN {knn}"));
    assert!(
        explain_dump.iter().flatten().any(|c| {
            let value = c.to_display_string().to_ascii_lowercase();
            value.contains("vector index") || value.contains("hnsw")
        }),
        "a finalized paged HNSW must remain eligible when catalog row stats are missing; heap_pages={heap_pages}, explain={explain_dump:?}"
    );
    assert_eq!(id_list(&reopened, knn), vec![1]);
}

#[test]
fn ivf_corrupt_native_file_rebuilds_from_heap() {
    // B3: a corrupt `.hvec` meta page (or a dim/metric change) used to make the
    // open return a hard 1030 that propagated to every KNN query with no
    // self-heal. The heap is the durable record, so the fix discards the
    // unusable file and rebuilds from the heap instead of failing.
    let _env = HnswEnv::set(&[
        ("MYDB_VECTOR_STORAGE", "paged"),
        ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
        ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let dim = 8;
    let n = 32;
    let vectors = gen_vectors(n, dim, 0x0C0D_E123_4567_89AB);
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(8) NOT NULL)")
            .unwrap();
        engine
            .execute("CREATE INDEX idx_emb ON items(emb) USING IVF WITH (nlist=8)")
            .unwrap();
        bulk_insert_vectors(&engine, &vectors);
        // Build + finalize the paged `.hvec` (first query).
        let q = vec_text(&vectors[0]);
        assert_eq!(
            id_list(
                &engine,
                &format!("SELECT id FROM items ORDER BY DISTANCE(emb, '{q}') LIMIT 1")
            ),
            vec![1]
        );
    }
    // Corrupt the `.hvec` meta page (clobber the magic at offset 0) on disk.
    let files = find_ivf_files(dir.path());
    assert!(!files.is_empty(), "a native .hvec file should exist");
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(&files[0])
            .unwrap();
        f.seek(SeekFrom::Start(0)).unwrap();
        f.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).unwrap();
        f.flush().unwrap();
    }
    // Reopen: the KNN must NOT error; the index self-heals by rebuilding from
    // the heap and answers correctly.
    let reopened = open_strict_test_engine(dir.path());
    let q = vec_text(&vectors[0]);
    let nearest = id_list(
        &reopened,
        &format!("SELECT id FROM items ORDER BY DISTANCE(emb, '{q}') LIMIT 1"),
    );
    assert_eq!(
        nearest,
        vec![1],
        "corrupt native .hvec must rebuild from heap and still answer (B3)"
    );
}

/// Locate every `.hvec` paged inverted-file under a data dir's indexes dir.
fn find_ivf_files(data_dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    find_object_files_with_extension(data_dir, "hvec")
}

#[test]
fn paged_ivf_rebuilds_after_restart() {
    // Native IVF rebuilds from committed heap values on cold open. Its paged
    // payloads remain governed by the buffer pool and physical WAL.
    let _env = HnswEnv::set(&[
        ("MYDB_VECTOR_STORAGE", "paged"),
        ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
        ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let dim = 16;
    let n = 48;
    let vectors = gen_vectors(n, dim, 0xEE11_2233_4455_6677);
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(16) NOT NULL)")
            .unwrap();
        engine
            .execute("CREATE INDEX idx_emb ON items(emb) USING IVF WITH (nlist=12)")
            .unwrap();
        bulk_insert_vectors(&engine, &vectors);
        // Build the paged inverted file (first query) before restart.
        let q = vec_text(&vectors[0]);
        assert_eq!(
            id_list(
                &engine,
                &format!("SELECT /*+ IVF_NPROBE(12) */ id FROM items ORDER BY DISTANCE(emb, '{q}') LIMIT 1")
            ),
            vec![1]
        );
        assert!(
            !find_ivf_files(dir.path()).is_empty(),
            "a native .hvec file should exist"
        );
    }
    // Reopen the data directory, rebuild, and stay on the index query path.
    let reopened = open_strict_test_engine(dir.path());
    let query = gen_vectors(1, dim, 0x1212_3434_5656_7878)[0].clone();
    let qtext = vec_text(&query);
    let k = 5;
    let knn = format!(
        "SELECT /*+ IVF_NPROBE(12) */ id FROM items ORDER BY DISTANCE(emb, '{qtext}') LIMIT {k}"
    );
    assert!(
        explain_mentions_vector_index(&reopened, &knn),
        "post-restart paged IVF KNN must use the index path"
    );
    // Full-probe (nprobe=nlist) scans every list, so the candidate pool must
    // contain the exact top-k that the executor's f32 re-rank then orders.
    assert_eq!(
        id_list(&reopened, &knn),
        brute_force_l2(&vectors, &query, k),
        "paged IVF must answer correctly after restart"
    );
}

#[test]
fn paged_hnsw_drop_index_removes_hvec() {
    let _env = paged_index_env();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    for i in 1..=5 {
        let v = [i as f32, 0.0, 0.0];
        engine
            .execute(&format!(
                "INSERT INTO items VALUES ({i}, '{}')",
                vec_text(&v)
            ))
            .unwrap();
    }
    let knn = "SELECT id FROM items ORDER BY DISTANCE(emb, '[3,0,0]') LIMIT 1";
    assert!(explain_mentions_vector_index(&engine, knn));
    assert_eq!(id_list(&engine, knn), vec![3]);
    assert!(!find_hvec_files(dir.path()).is_empty());

    // DROP INDEX must remove the .hvec file and route KNN back to a full scan.
    engine.execute("DROP INDEX idx_emb ON items").unwrap();
    assert!(
        find_hvec_files(dir.path()).is_empty(),
        "DROP INDEX must delete the paged .hvec file"
    );
    assert!(!explain_mentions_vector_index(&engine, knn));
    assert_eq!(id_list(&engine, knn), vec![3]);
}

// ── Stage 1 — observability + correctness ──────────────────────────────────

/// Fetch one column's display strings for `SELECT <col> FROM <view> WHERE …`.
fn col_strings(engine: &StorageQueryEngine, sql: &str) -> Vec<String> {
    rows(engine, sql)
        .into_iter()
        .map(|r| r[0].to_display_string())
        .collect()
}

#[test]
fn mydb_vector_indexes_view_reports_catalog_and_runtime() {
    let _env = HnswEnv::set(&[("MYDB_HNSW_BUILD_ON_DDL", "off")]);
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(6) NOT NULL)")
        .unwrap();
    engine
        .execute(
            "CREATE INDEX idx_emb ON items(emb) USING HNSW \
             WITH (metric=cosine, m=24, ef_construction=120, ef_search=80)",
        )
        .unwrap();

    // Cold (never queried): catalog metadata present, LOADED = 0, runtime NULL.
    let base = "FROM information_schema.mydb_vector_indexes WHERE TABLE_NAME = 'items'";
    assert_eq!(
        col_strings(&engine, &format!("SELECT METRIC {base}")),
        vec!["COSINE"]
    );
    assert_eq!(
        col_strings(&engine, &format!("SELECT M {base}")),
        vec!["24"]
    );
    assert_eq!(
        col_strings(&engine, &format!("SELECT EF_CONSTRUCTION {base}")),
        vec!["120"]
    );
    assert_eq!(
        col_strings(&engine, &format!("SELECT DEFAULT_EF_SEARCH {base}")),
        vec!["80"]
    );
    assert_eq!(
        col_strings(&engine, &format!("SELECT DECLARED_DIM {base}")),
        vec!["6"]
    );
    assert_eq!(
        col_strings(&engine, &format!("SELECT LOADED {base}")),
        vec!["0"]
    );

    // Insert rows and run a KNN so the graph warms up.
    let vectors = gen_vectors(30, 6, 0xAB);
    bulk_insert_vectors(&engine, &vectors);
    let q = vec_text(&gen_vectors(1, 6, 0xCD)[0]);
    let _ = id_list(
        &engine,
        &format!("SELECT id FROM items ORDER BY DISTANCE(emb, '{q}', 'COSINE') LIMIT 3"),
    );

    // Warm: LOADED = 1, backend reported, live nodes == row count.
    assert_eq!(
        col_strings(&engine, &format!("SELECT LOADED {base}")),
        vec!["1"]
    );
    assert_eq!(
        col_strings(&engine, &format!("SELECT LIVE_NODES {base}")),
        vec!["30"]
    );
    assert_eq!(
        col_strings(&engine, &format!("SELECT RUNTIME_DIM {base}")),
        vec!["6"]
    );
    let backend = col_strings(&engine, &format!("SELECT BACKEND {base}"));
    assert!(
        backend == vec!["paged"] || backend == vec!["paged_mapped"] || backend == vec!["memory"],
        "backend should be paged/paged_mapped/memory, got {backend:?}"
    );
    let extended = rows(
        &engine,
        &format!(
            "SELECT PAGED_FILE_SIZE_BYTES, NAV_PAGES, SQ8_EXACT_FALLBACK_NODES, \
                    TOMBSTONES_ON_DISK, SQ8_COVERED_NODES, \
                    DEFAULT_NPROBE, KMEANS_ITERS, TRAIN_SAMPLE_MULT {base}"
        ),
    );
    assert_eq!(extended.len(), 1);
    assert_eq!(extended[0].len(), 8);
}

#[test]
fn vector_with_validation_show_create_roundtrip_and_json_explain() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
        .unwrap();
    let err = engine
        .execute("CREATE INDEX bad ON items(emb) USING HNSW COMMENT 'M=abc'")
        .unwrap_err();
    assert_eq!(err.code(), 1235);

    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE INDEX idx_emb ON items(emb) USING HNSW \
             WITH (metric=cosine, m=20, ef_construction=96, ef_search=48) COMMENT 'FUTURE_OPT=1'",
        )
        .unwrap();
    assert!(session.warnings.is_empty());
    let create = rows(&engine, "SHOW CREATE TABLE items")[0][1].to_display_string();
    assert!(create.contains("USING HNSW"), "{create}");
    assert!(create.contains("metric=cosine"), "{create}");
    // Recreate from SHOW CREATE and verify parsed parameters survive.
    let copy = create.replacen("`items`", "`items_copy`", 1);
    engine.execute(&copy).unwrap();
    assert_eq!(
        col_strings(
            &engine,
            "SELECT CONCAT(METRIC, ':', M, ':', EF_CONSTRUCTION, ':', DEFAULT_EF_SEARCH) \
             FROM information_schema.mydb_vector_indexes WHERE TABLE_NAME='items_copy'",
        ),
        vec!["COSINE:20:96:48"]
    );

    engine
        .execute("INSERT INTO items VALUES (1, '[1,0,0]'), (2, '[0,1,0]')")
        .unwrap();
    let json = rows(
        &engine,
        "EXPLAIN FORMAT=JSON SELECT /*+ HNSW_EF(32) */ id FROM items \
         ORDER BY COSINE_DISTANCE(emb, '[1,0,0]') LIMIT 1",
    )[0][0]
        .to_display_string();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert!(json.contains("\"vector_scan\""), "{parsed:#}");
    assert!(json.contains("\"algo\": \"HNSW\""), "{parsed:#}");
    assert!(json.contains("\"metric\": \"COSINE\""), "{parsed:#}");
}

#[test]
fn load_data_vector_text_and_base64_and_ctas_warning() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE src (id INT PRIMARY KEY, emb VECTOR(2) NOT NULL)")
        .unwrap();
    let text_path = dir.path().join("vectors.tsv");
    std::fs::write(&text_path, "1\t[1.5,2.5]\n2\t[3,4]\n").unwrap();
    engine
        .execute(&format!(
            "LOAD DATA INFILE '{}' INTO TABLE src",
            crate::util::file::sql_path(&text_path)
        ))
        .unwrap();
    assert_eq!(
        col_strings(&engine, "SELECT VECTOR_TO_STRING(emb) FROM src ORDER BY id"),
        vec!["[1.5,2.5]", "[3,4]"]
    );
    let xml_path = dir.path().join("vectors.xml");
    std::fs::write(
        &xml_path,
        "<rows><row><id>3</id><emb>[5,6]</emb></row></rows>",
    )
    .unwrap();
    engine
        .execute(&format!(
            "LOAD XML INFILE '{}' INTO TABLE src ROWS IDENTIFIED BY '<row>'",
            crate::util::file::sql_path(&xml_path)
        ))
        .unwrap();
    assert_eq!(
        cell(&engine, "SELECT VECTOR_TO_STRING(emb) FROM src WHERE id=3"),
        "[5,6]"
    );
    engine
        .execute("CREATE INDEX src_vec ON src(emb) USING HNSW")
        .unwrap();
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE copied AS SELECT * FROM src")
        .unwrap();
    assert!(session.warnings.iter().any(|w| {
        w.code() == 1287 && w.message().to_ascii_lowercase().contains("create index")
    }));
    assert_eq!(cell(&engine, "SELECT COUNT(*) FROM copied"), "3");
    drop(engine);

    let _base64 = HnswEnv::set(&[("MYDB_LOAD_DATA_VECTOR_FORMAT", "base64")]);
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE encoded (id INT PRIMARY KEY, emb VECTOR(2) NOT NULL)")
        .unwrap();
    let encoded_path = dir.path().join("vectors-base64.tsv");
    std::fs::write(&encoded_path, "1\tAACAPwAAAEA=\n2\tAABAQAAAgEA=\n").unwrap();
    engine
        .execute(&format!(
            "LOAD DATA INFILE '{}' INTO TABLE encoded",
            crate::util::file::sql_path(&encoded_path)
        ))
        .unwrap();
    assert_eq!(
        col_strings(
            &engine,
            "SELECT VECTOR_TO_STRING(emb) FROM encoded ORDER BY id"
        ),
        vec!["[1,2]", "[3,4]"]
    );
}

#[test]
fn vector_backend_files_follow_index_and_table_rename() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_strict_hnsw_test_engine(dir.path());
        engine
            .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(2) NOT NULL)")
            .unwrap();
        engine
            .execute("CREATE INDEX idx_old ON items(emb) USING HNSW")
            .unwrap();
        engine
            .execute("INSERT INTO items VALUES (1, '[1,0]'), (2, '[2,0]')")
            .unwrap();
        let _ = id_list(
            &engine,
            "SELECT /*+ HNSW_EF(16) */ id FROM items ORDER BY DISTANCE(emb, '[1,0]') LIMIT 1",
        );
        let before_paths = find_object_files_with_extension(dir.path(), "hvec");
        assert_eq!(before_paths.len(), 1, "{before_paths:?}");
        engine
            .execute("ALTER TABLE items RENAME INDEX idx_old TO idx_new")
            .unwrap();
        engine.execute("RENAME TABLE items TO renamed").unwrap();
        let after_paths = find_object_files_with_extension(dir.path(), "hvec");
        assert_eq!(before_paths, after_paths, "vector path changed on rename");
    }
    let engine = open_strict_hnsw_test_engine(dir.path());
    assert_eq!(
        id_list(
            &engine,
            "SELECT /*+ HNSW_EF(16) */ id FROM renamed ORDER BY DISTANCE(emb, '[1,0]') LIMIT 1"
        ),
        vec![1]
    );
}

#[test]
fn join_knn_retains_final_relational_topk_and_preserves_results() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(2) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE TABLE tags (item_id INT PRIMARY KEY, label VARCHAR(10))")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    for i in 1..=20 {
        engine
            .execute(&format!("INSERT INTO items VALUES ({i}, '[{i},0]')"))
            .unwrap();
        engine
            .execute(&format!("INSERT INTO tags VALUES ({i}, 't{i}')"))
            .unwrap();
    }
    let sql = "SELECT /*+ HNSW_EF(64) */ items.id FROM items \
               JOIN tags ON tags.item_id=items.id \
               ORDER BY L2_DISTANCE(items.emb, '[0,0]') LIMIT 5";
    assert!(!explain_mentions_vector_index(&engine, sql));
    assert_eq!(id_list(&engine, sql), vec![1, 2, 3, 4, 5]);
}

#[test]
fn cosine_distance_zero_vector_returns_null() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    // Zero vector → cosine is undefined → NULL (not a fabricated distance).
    assert_eq!(
        cell(
            &engine,
            "SELECT COSINE_DISTANCE(STRING_TO_VECTOR('[0,0,0]'), STRING_TO_VECTOR('[1,2,3]'))"
        ),
        "NULL"
    );
    assert_eq!(
        cell(
            &engine,
            "SELECT DISTANCE(STRING_TO_VECTOR('[1,2,3]'), STRING_TO_VECTOR('[0,0,0]'), 'COSINE')"
        ),
        "NULL"
    );
    // A non-zero pair still yields a real distance (sanity).
    assert_ne!(
        cell(
            &engine,
            "SELECT COSINE_DISTANCE(STRING_TO_VECTOR('[1,0,0]'), STRING_TO_VECTOR('[1,0,0]'))"
        ),
        "NULL"
    );
    // L2 over a zero vector is well-defined and must NOT be nulled.
    assert_ne!(
        cell(
            &engine,
            "SELECT L2_DISTANCE(STRING_TO_VECTOR('[0,0,0]'), STRING_TO_VECTOR('[1,2,3]'))"
        ),
        "NULL"
    );
}

#[test]
fn cosine_index_rejects_zero_rows_and_zero_knn_query() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW WITH (metric=cosine)")
        .unwrap();

    let err = engine
        .execute("INSERT INTO items VALUES (1, '[0,0,0]')")
        .unwrap_err();
    assert_eq!(err.code(), 50001);
    engine
        .execute("INSERT INTO items VALUES (2, '[1,0,0]')")
        .unwrap();
    let err = engine
        .execute("SELECT id FROM items ORDER BY DISTANCE(emb, '[0,0,0]', 'COSINE') LIMIT 1")
        .unwrap_err();
    assert_eq!(err.code(), 50001);
}

#[test]
fn knn_boundaries_and_vector_errors_are_consistent() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    assert!(id_list(
        &engine,
        "SELECT id FROM items ORDER BY L2_DISTANCE(emb, '[0,0,0]') LIMIT 5"
    )
    .is_empty());
    engine
        .execute("INSERT INTO items VALUES (1, '[1,0,0]'), (2, '[2,0,0]')")
        .unwrap();
    assert_eq!(
        id_list(
            &engine,
            "SELECT id FROM items ORDER BY L2_DISTANCE(emb, '[0,0,0]') LIMIT 99"
        ),
        vec![1, 2]
    );
    assert!(id_list(
        &engine,
        "SELECT id FROM items ORDER BY L2_DISTANCE(emb, '[0,0,0]') LIMIT 0"
    )
    .is_empty());
    let err = engine
        .execute("SELECT id FROM items ORDER BY L2_DISTANCE(emb, '[1,2]') LIMIT 1")
        .unwrap_err();
    assert_eq!(err.code(), 50002, "{err:?}");
    let err = engine
        .execute("SELECT id FROM items ORDER BY L2_DISTANCE(missing, '[1,2,3]') LIMIT 1")
        .unwrap_err();
    assert_eq!(err.code(), 1054, "{err:?}");
    let err = engine
        .execute("SELECT STRING_TO_VECTOR('not-a-vector')")
        .unwrap_err();
    assert_eq!(err.code(), 50001, "{err:?}");
}

#[test]
fn hnsw_metric_mismatch_warns_1287() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(4) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW WITH (metric=cosine)")
        .unwrap();
    for i in 0..8 {
        engine
            .execute(&format!(
                "INSERT INTO items VALUES ({}, '[{},{},0,1]')",
                i + 1,
                i,
                i
            ))
            .unwrap();
    }
    // Query asks for L2 but the index is COSINE — planner falls back and warns.
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "SELECT id FROM items ORDER BY DISTANCE(emb, '[1,1,0,1]', 'L2') LIMIT 3",
        )
        .unwrap();
    let codes: Vec<u16> = session.warnings.iter().map(|w| w.code()).collect();
    assert!(
        codes.contains(&1287),
        "metric mismatch must surface warning 1287; got {codes:?}"
    );
}

#[test]
fn show_engine_mydb_status_includes_hnsw_section() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(4) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    let status = rows(&engine, "SHOW ENGINE MYDB STATUS");
    // Variable_name is the 2nd column (Type, Name, Status).
    let names: Vec<String> = status.iter().map(|r| r[1].to_display_string()).collect();
    assert!(
        names.iter().any(|n| n == "hnsw_indexes"),
        "SHOW ENGINE MYDB STATUS must include an hnsw_indexes header"
    );
    assert!(
        names.iter().any(|n| n == "hnsw.test.items.idx_emb"),
        "SHOW ENGINE MYDB STATUS must list the index; got {names:?}"
    );
}

// ── Stage 2 — SQL compatibility surface ────────────────────────────────────

/// Extract the `ef_search=N` value EXPLAIN prints in the vector-scan Extra.
fn explain_ef_search(engine: &StorageQueryEngine, query: &str) -> Option<u64> {
    rows(engine, &format!("EXPLAIN {query}"))
        .iter()
        .flatten()
        .find_map(|c| {
            c.to_display_string()
                .split_whitespace()
                .find_map(|tok| tok.strip_prefix("ef_search=").and_then(|n| n.parse().ok()))
        })
}

#[test]
fn per_index_default_ef_search_drives_plan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(4) NOT NULL)")
        .unwrap();
    // Per-index EF_SEARCH=200 should drive the plan above the 64 built-in.
    engine
        .execute(
            "CREATE INDEX idx_emb ON items(emb) USING HNSW \
             WITH (metric=l2, ef_search=200)",
        )
        .unwrap();
    for i in 0..20 {
        engine
            .execute(&format!(
                "INSERT INTO items VALUES ({}, '[{},{},0,1]')",
                i + 1,
                i,
                i % 3
            ))
            .unwrap();
    }
    // k=3 → dynamic ef (4·k=12) and k are both below the index default, so the
    // index's EF_SEARCH=200 is the effective base.
    let knn = "SELECT id FROM items ORDER BY DISTANCE(emb, '[1,1,0,1]') LIMIT 3";
    assert_eq!(
        explain_ef_search(&engine, knn),
        Some(200),
        "per-index EF_SEARCH=200 must drive the plan"
    );
    // A per-query HINT still overrides the index default.
    let hinted = "SELECT /*+ HNSW_EF(50) */ id FROM items \
                  ORDER BY DISTANCE(emb, '[1,1,0,1]') LIMIT 3";
    assert_eq!(explain_ef_search(&engine, hinted), Some(50));
}

#[test]
fn new_vector_functions_evaluate_in_sql() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    assert_eq!(
        cell(&engine, "SELECT VECTOR_NORM(STRING_TO_VECTOR('[3,4]'))"),
        "5"
    );
    assert_eq!(
        cell(
            &engine,
            "SELECT VECTOR_TO_STRING(L2_NORMALIZE(STRING_TO_VECTOR('[3,4]')))"
        ),
        "[0.6,0.8]"
    );
    assert_eq!(
        cell(
            &engine,
            "SELECT VECTOR_TO_STRING(SUBVECTOR(STRING_TO_VECTOR('[10,20,30,40]'), 2, 2))"
        ),
        "[20,30]"
    );
    assert_eq!(
        cell(
            &engine,
            "SELECT MAX_INNER_PRODUCT_DISTANCE(STRING_TO_VECTOR('[1,2]'), STRING_TO_VECTOR('[3,4]'))"
        ),
        "-11"
    );
}

#[test]
fn pgvector_dot_form_alias_sets_ef_search() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET hnsw.ef_search = 123")
        .expect("SET hnsw.ef_search should resolve via the pgvector dot-form alias");
    let resp = engine
        .execute_with_session(&mut session, "SELECT @@mydb_hnsw_ef_search")
        .unwrap();
    let got = match resp {
        QueryResponse::ResultSet(rs) => rs.rows[0][0].to_display_string(),
        other => panic!("expected a result set, got {other:?}"),
    };
    assert_eq!(got, "123", "dot-form alias must update the canonical var");
}

// ── Stage 3 — background compaction (manual OPTIMIZE trigger) ───────────────

#[test]
fn optimize_table_compacts_hnsw_tombstones() {
    // Tombstone reclamation drops dead nodes only on the memory backend (the
    // paged backend keeps skipping on-disk tombstones); pin memory.
    let _mem = HnswEnv::set(&[("MYDB_VECTOR_STORAGE", "memory")]);
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(4) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    for i in 0..20 {
        engine
            .execute(&format!(
                "INSERT INTO items VALUES ({}, '[{},{},0,1]')",
                i + 1,
                i,
                i % 4
            ))
            .unwrap();
    }
    let knn = "SELECT id FROM items ORDER BY DISTANCE(emb, '[1,1,0,1]') LIMIT 5";
    // Warm the graph, then delete half the rows — the DML hook tombstones them.
    let _ = id_list(&engine, knn);
    engine.execute("DELETE FROM items WHERE id <= 10").unwrap();

    let base = "FROM information_schema.mydb_vector_indexes WHERE TABLE_NAME = 'items'";
    assert_eq!(
        col_strings(&engine, &format!("SELECT STALE_NODES {base}")),
        vec!["10"],
        "10 deletes must leave 10 tombstones"
    );
    assert_eq!(
        col_strings(&engine, &format!("SELECT LIVE_NODES {base}")),
        vec!["10"]
    );

    // OPTIMIZE TABLE reclaims the tombstones now (bypassing the ratio gate).
    engine.execute("OPTIMIZE TABLE items").unwrap();
    assert_eq!(
        col_strings(&engine, &format!("SELECT STALE_NODES {base}")),
        vec!["0"],
        "OPTIMIZE must reclaim all tombstones"
    );
    assert_eq!(
        col_strings(&engine, &format!("SELECT TOTAL_NODES {base}")),
        vec!["10"],
        "only survivors remain after compaction"
    );

    // The graph still answers KNN correctly — only live rows, no dead nodes.
    let got = id_list(&engine, knn);
    assert!(
        got.iter().all(|id| *id > 10),
        "post-compaction KNN must return only live rows; got {got:?}"
    );
    assert_eq!(got.len(), 5);
}

#[test]
fn optimize_table_rebuilds_paged_hnsw_and_reclaims_tombstones() {
    let _paged = HnswEnv::set(&[
        ("MYDB_VECTOR_STORAGE", "paged"),
        ("MYDB_HNSW_COMPACT_DISABLE", "1"),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(8) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    let vectors = (1..=32)
        .map(|i| vec![i as f32, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
        .collect::<Vec<_>>();
    bulk_insert_vectors(&engine, &vectors);
    let _ = id_list(
        &engine,
        "SELECT /*+ HNSW_EF(64) */ id FROM items ORDER BY DISTANCE(emb, '[1,0,0,0,0,0,0,0]') LIMIT 5",
    );
    engine.execute("DELETE FROM items WHERE id <= 24").unwrap();
    // Materialize dirty node pages so the pre/post file-size comparison is
    // apples-to-apples. Automatic checkpoint compaction is disabled above;
    // explicit OPTIMIZE must be the operation that reclaims tombstones.
    engine.checkpoint().unwrap();
    let base = "FROM information_schema.mydb_vector_indexes WHERE TABLE_NAME='items'";
    assert_eq!(
        col_strings(&engine, &format!("SELECT TOMBSTONES_ON_DISK {base}")),
        vec!["24"]
    );
    let before: u64 = cell(&engine, &format!("SELECT PAGED_FILE_SIZE_BYTES {base}"))
        .parse()
        .unwrap();
    engine.execute("OPTIMIZE TABLE items").unwrap();
    assert_eq!(
        col_strings(&engine, &format!("SELECT TOMBSTONES_ON_DISK {base}")),
        vec!["0"]
    );
    assert_eq!(
        col_strings(&engine, &format!("SELECT TOTAL_NODES {base}")),
        vec!["8"]
    );
    let after: u64 = cell(&engine, &format!("SELECT PAGED_FILE_SIZE_BYTES {base}"))
        .parse()
        .unwrap();
    assert!(
        after <= before,
        "paged OPTIMIZE should not grow file: {before} -> {after}"
    );
    assert!(id_list(
        &engine,
        "SELECT /*+ HNSW_EF(64) */ id FROM items ORDER BY DISTANCE(emb, '[32,0,0,0,0,0,0,0]') LIMIT 5"
    )
    .iter()
    .all(|id| *id > 24));
}

// ── Stage 6 — partitioned-table brute KNN ──────────────────────────────────

#[test]
fn partitioned_table_brute_knn_matches_brute_force() {
    // No HNSW index on a partitioned table → the no-index brute path. It used
    // to hard-reject partitioned tables (RowIds alias across partitions); now
    // the bounded top-k carries each winner's storage_id so the executor reads
    // the right partition. brute is enabled by default in open_test_engine.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(8) NOT NULL) \
             PARTITION BY HASH(id) PARTITIONS 4",
        )
        .unwrap();
    let n = 40;
    let dim = 8;
    let vectors = gen_vectors(n, dim, 0x9E37_79B9);
    bulk_insert_vectors(&engine, &vectors);
    let query = gen_vectors(1, dim, 0x1357_9BDF)[0].clone();
    let qtext = vec_text(&query);
    let k = 7;
    let knn = format!("SELECT id FROM items ORDER BY DISTANCE(emb, '{qtext}') LIMIT {k}");

    // The brute-force vector operator handles the partitioned scan now.
    let explained = rows(&engine, &format!("EXPLAIN {knn}"))
        .iter()
        .flatten()
        .any(|c| {
            c.to_display_string()
                .to_ascii_lowercase()
                .contains("brute-force vector scan")
        });
    assert!(
        explained,
        "partitioned no-index KNN must use the brute operator"
    );

    // Results (across all partitions) match the exact brute-force top-k.
    let got = id_list(&engine, &knn);
    let expected = brute_force_l2(&vectors, &query, k);
    assert_eq!(
        got, expected,
        "partitioned brute KNN must match brute-force top-{k}"
    );
}

// ── Stage 7 — replica eager preload (warm-all primitive) ───────────────────

#[test]
fn warm_all_vector_graphs_preloads_cold_indexes() {
    // The replica-promotion path (M8) calls `warm_all_vector_graphs` so a
    // freshly-promoted master answers its first KNN without a cold-build stall.
    // Here we exercise that primitive directly: a never-queried index is cold
    // (LOADED=0); after warming it is hot with all rows present.
    let _env = HnswEnv::set(&[("MYDB_HNSW_BUILD_ON_DDL", "off")]);
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(4) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    for i in 0..16 {
        engine
            .execute(&format!(
                "INSERT INTO items VALUES ({}, '[{},{},0,1]')",
                i + 1,
                i,
                i % 3
            ))
            .unwrap();
    }

    let base = "FROM information_schema.mydb_vector_indexes WHERE TABLE_NAME = 'items'";
    // Cold: the index has never been queried, so no graph is warm.
    assert_eq!(
        col_strings(&engine, &format!("SELECT LOADED {base}")),
        vec!["0"]
    );

    // Eager-warm everything (the promotion hook's primitive).
    engine.database().warm_all_vector_graphs();

    assert_eq!(
        col_strings(&engine, &format!("SELECT LOADED {base}")),
        vec!["1"],
        "warm_all must build the cold graph"
    );
    assert_eq!(
        col_strings(&engine, &format!("SELECT LIVE_NODES {base}")),
        vec!["16"],
        "warmed graph must hold every row"
    );
}

// N1, docs/ddl-audit-2026-07-07-followup.md: DROP TABLE removes only its own HNSW graph
// files, never another table's .hvec/.hnsw through a matching __id__ substring in its name.
#[test]
fn n1_drop_table_does_not_delete_colliding_table_vector_graph() {
    let dir = tempfile::tempdir().unwrap();
    // Strict durability so the paged `.hvec` graph is actually written to
    // disk (Memory mode keeps it in the buffer pool only).
    let engine = open_strict_test_engine(dir.path());

    // Anchor table with an HNSW graph.
    engine
        .execute("CREATE TABLE anchor (id INT PRIMARY KEY, emb VECTOR(4) NOT NULL)")
        .unwrap();
    engine
        .execute("INSERT INTO anchor VALUES (1, STRING_TO_VECTOR('[1,0,0,0]')), (2, STRING_TO_VECTOR('[0,1,0,0]'))")
        .unwrap();
    engine
        .execute("CREATE INDEX anchor_idx ON anchor(emb) USING HNSW")
        .unwrap();
    // Trigger graph materialization.
    let _ = engine
        .execute("SELECT id FROM anchor ORDER BY DISTANCE(emb, STRING_TO_VECTOR('[1,0,0,0]'), 'L2') LIMIT 1")
        .unwrap();

    let anchor_id = engine
        .database()
        .catalog()
        .tables
        .get("test.anchor")
        .unwrap()
        .id;

    // The probe table name embeds anchor's ID, so its filename contains __anchor_id__,
    // which the old substring deletion incorrectly matched.
    let probe = format!("probe__{anchor_id}");
    engine
        .execute(&format!(
            "CREATE TABLE {probe} (id INT PRIMARY KEY, emb VECTOR(4) NOT NULL)"
        ))
        .unwrap();
    engine
        .execute(&format!(
            "INSERT INTO {probe} VALUES (1, STRING_TO_VECTOR('[1,0,0,0]'))"
        ))
        .unwrap();
    engine
        .execute(&format!(
            "CREATE INDEX probe_idx ON {probe}(emb) USING HNSW"
        ))
        .unwrap();
    let _ = engine
        .execute(&format!("SELECT id FROM {probe} ORDER BY DISTANCE(emb, STRING_TO_VECTOR('[1,0,0,0]'), 'L2') LIMIT 1"))
        .unwrap();

    let probe_table = engine
        .database()
        .catalog()
        .tables
        .get(&format!("test.{probe}"))
        .unwrap()
        .clone();
    let probe_index_id = probe_table
        .indexes
        .iter()
        .find(|index| index.name.eq_ignore_ascii_case("probe_idx"))
        .unwrap()
        .index_id;
    let probe_graph_exists = || {
        ["hvec", "hnsw"].iter().any(|ext| {
            crate::storage::naming::index_object_path_by_id(
                dir.path(),
                probe_table.id,
                probe_table.active_generation.max(1),
                probe_index_id,
                ext,
            )
            .exists()
        })
    };
    assert!(probe_graph_exists(), "probe graph must exist before drop");

    // Drop anchor: the former __anchor_id__ substring deletion also removed probe files;
    // the fix deletes only anchor's exact files.
    engine.execute("DROP TABLE anchor").unwrap();

    assert!(
        probe_graph_exists(),
        "dropping anchor must not delete probe's colliding vector graph file"
    );
    // Probe KNN remains usable.
    let out = rows(
        &engine,
        &format!("SELECT id FROM {probe} ORDER BY DISTANCE(emb, STRING_TO_VECTOR('[1,0,0,0]'), 'L2') LIMIT 1"),
    );
    assert_eq!(
        out.len(),
        1,
        "probe KNN must still return a result after anchor drop"
    );
}

// ---------------------------------------------------------------------------
// IVF inverted-file index (memory backend). Mirrors the HNSW coverage:
// DDL round-trip, KNN correctness vs brute force, DML visibility, EXPLAIN.
// ---------------------------------------------------------------------------

#[test]
fn ivf_knn_matches_bruteforce_full_probe() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(6) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING IVF WITH (metric=l2, nlist=5)")
        .unwrap();

    // Small n so a generous nprobe makes the candidate pool cover every row —
    // the executor's exact f32 re-rank then yields the exact top-k.
    let n = 30;
    let dim = 6;
    let vectors = gen_vectors(n, dim, 0x0BAD_F00D_1234_5678);
    bulk_insert_vectors(&engine, &vectors);

    let query = gen_vectors(1, dim, 0xFEED_FACE_0000_0001)[0].clone();
    let qtext = vec_text(&query);
    let k = 5;
    // /*+ IVF_NPROBE(64) */ probes every list; the pool then holds all rows.
    let knn = format!(
        "SELECT /*+ IVF_NPROBE(64) */ id FROM items ORDER BY DISTANCE(emb, '{qtext}') LIMIT {k}"
    );

    // Driven by the IVF index (no full Sort).
    assert!(
        explain_mentions_vector_index(&engine, &knn),
        "EXPLAIN should show the vector index for `{knn}`"
    );

    let got = id_list(&engine, &knn);
    let expected = brute_force_l2(&vectors, &query, k);
    assert_eq!(
        got, expected,
        "full-probe IVF KNN must match brute-force top-{k}"
    );
}

#[test]
fn ivf_explain_shows_algo_and_nprobe() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(4) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING IVF WITH (nlist=4, nprobe=3)")
        .unwrap();
    for i in 0..40 {
        let v = gen_vectors(1, 4, 0x100 + i as u64)[0].clone();
        engine
            .execute(&format!(
                "INSERT INTO items VALUES ({}, '{}')",
                i + 1,
                vec_text(&v)
            ))
            .unwrap();
    }
    let explain: String = rows(
        &engine,
        "EXPLAIN SELECT id FROM items ORDER BY DISTANCE(emb, '[0,0,0,0]') LIMIT 3",
    )
    .into_iter()
    .flatten()
    .map(|c| c.to_display_string())
    .collect::<Vec<_>>()
    .join("\n");
    assert!(
        explain.contains("IVF"),
        "EXPLAIN should name IVF: {explain}"
    );
    assert!(
        explain.contains("nprobe"),
        "EXPLAIN should show nprobe: {explain}"
    );
}

#[test]
fn knn_planner_enumerates_same_column_vector_indexes_and_honors_algorithm_hint() {
    let _env = HnswEnv::set(&[("MYDB_HNSW_BUILD_ON_DDL", "off")]);
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(4) NOT NULL)")
        .unwrap();
    // The first catalog index deliberately has the wrong metric; the old
    // first-index-only planner missed the later compatible IVF index.
    engine
        .execute("CREATE INDEX idx_cos ON items(emb) USING HNSW WITH (metric=cosine)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_l2 ON items(emb) USING IVF WITH (metric=l2, nlist=4)")
        .unwrap();
    let explain = rows(
        &engine,
        "EXPLAIN SELECT /*+ IVF_NPROBE(2) */ id FROM items ORDER BY DISTANCE(emb,'[0,0,0,0]') LIMIT 2",
    )
    .into_iter()
    .flatten()
    .map(|v| v.to_display_string())
    .collect::<Vec<_>>()
    .join("\n");
    assert!(explain.contains("idx_l2"), "unexpected plan:\n{explain}");
    assert!(explain.contains("IVF"), "unexpected plan:\n{explain}");
}

#[test]
fn ivf_show_create_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING IVF WITH (metric=cosine, nlist=16)")
        .unwrap();
    let show =
        |e: &StorageQueryEngine| rows(e, "SHOW CREATE TABLE items")[0][1].to_display_string();
    let create = show(&engine);
    assert!(
        create.contains("USING IVF"),
        "SHOW CREATE TABLE must echo USING IVF, got:\n{create}"
    );
    // The echoed DDL must reparse (round-trip) into an equivalent table.
    engine.execute("DROP TABLE items").unwrap();
    engine.execute(&create).unwrap();
    let create2 = show(&engine);
    assert_eq!(
        create, create2,
        "SHOW CREATE TABLE must be stable across round-trip"
    );
}

#[test]
fn ivf_insert_and_delete_visible() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(4) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING IVF WITH (nlist=4)")
        .unwrap();
    for i in 0..40 {
        let v = gen_vectors(1, 4, 0x900 + i as u64)[0].clone();
        engine
            .execute(&format!(
                "INSERT INTO items VALUES ({}, '{}')",
                i + 1,
                vec_text(&v)
            ))
            .unwrap();
    }
    // Warm the index with a query so the incremental DML hooks are exercised.
    let q = "[0.5,0.5,0.5,0.5]";
    let _ = id_list(
        &engine,
        &format!("SELECT /*+ IVF_NPROBE(4) */ id FROM items ORDER BY DISTANCE(emb, '{q}') LIMIT 3"),
    );
    // Insert a row equal to the query vector — it must become the nearest.
    engine
        .execute(&format!("INSERT INTO items VALUES (999, '{q}')"))
        .unwrap();
    let nearest = id_list(
        &engine,
        &format!("SELECT /*+ IVF_NPROBE(4) */ id FROM items ORDER BY DISTANCE(emb, '{q}') LIMIT 1"),
    );
    assert_eq!(
        nearest,
        vec![999],
        "inserted exact-match row must be nearest"
    );
    // Delete it — it must no longer surface.
    engine.execute("DELETE FROM items WHERE id = 999").unwrap();
    let after = id_list(
        &engine,
        &format!("SELECT /*+ IVF_NPROBE(4) */ id FROM items ORDER BY DISTANCE(emb, '{q}') LIMIT 5"),
    );
    assert!(
        !after.contains(&999),
        "deleted row must not resurface: {after:?}"
    );
}

#[test]
fn ivf_ddl_rejects_invalid_column() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t1 (id INT PRIMARY KEY, emb VECTOR(3))")
        .unwrap();
    // Nullable vector column is rejected.
    assert!(
        engine
            .execute("CREATE INDEX i ON t1(emb) USING IVF")
            .is_err(),
        "IVF on a nullable column must be rejected"
    );
    engine
        .execute("CREATE TABLE t2 (id INT PRIMARY KEY, name VARCHAR(20) NOT NULL)")
        .unwrap();
    // Non-vector column is rejected.
    assert!(
        engine
            .execute("CREATE INDEX i ON t2(name) USING IVF")
            .is_err(),
        "IVF on a non-vector column must be rejected"
    );
}

#[test]
fn ivf_update_moves_vector_in_knn() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(4) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING IVF WITH (nlist=4)")
        .unwrap();
    for i in 0..40 {
        let v = gen_vectors(1, 4, 0xA00 + i as u64)[0].clone();
        engine
            .execute(&format!(
                "INSERT INTO items VALUES ({}, '{}')",
                i + 1,
                vec_text(&v)
            ))
            .unwrap();
    }
    let q = "[9,9,9,9]"; // far outside the [-1,1] cluster
                         // Warm the index.
    let _ = id_list(
        &engine,
        &format!("SELECT /*+ IVF_NPROBE(4) */ id FROM items ORDER BY DISTANCE(emb, '{q}') LIMIT 1"),
    );
    // Move row 7 to exactly the query point — it must become the nearest.
    engine
        .execute(&format!("UPDATE items SET emb = '{q}' WHERE id = 7"))
        .unwrap();
    let nearest = id_list(
        &engine,
        &format!("SELECT /*+ IVF_NPROBE(4) */ id FROM items ORDER BY DISTANCE(emb, '{q}') LIMIT 1"),
    );
    assert_eq!(
        nearest,
        vec![7],
        "updated row must become nearest to its new position"
    );
}

#[test]
fn ivf_transaction_delta_is_read_your_writes_and_rollback_safe() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING IVF WITH (nlist=3, nprobe=3)")
        .unwrap();
    engine
        .execute("INSERT INTO items VALUES (1,'[1,0,0]'),(2,'[2,0,0]'),(3,'[3,0,0]')")
        .unwrap();
    // Materialize the shared paged index before applying transaction-local
    // mutations, so the test exercises delta merge rather than cold rebuild.
    assert_eq!(
        id_list(
            &engine,
            "SELECT /*+ IVF_NPROBE(3) */ id FROM items ORDER BY DISTANCE(emb,'[1,0,0]') LIMIT 1"
        ),
        vec![1]
    );

    let mut tx = SessionContext::default();
    engine.execute_with_session(&mut tx, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut tx, "UPDATE items SET emb='[9,9,9]' WHERE id=2")
        .unwrap();
    assert_eq!(
        id_list_session(
            &engine,
            &mut tx,
            "SELECT /*+ IVF_NPROBE(3) */ id FROM items ORDER BY DISTANCE(emb,'[9,9,9]') LIMIT 1"
        ),
        vec![2],
        "KNN must merge this transaction's deferred vector replacement"
    );
    engine.execute_with_session(&mut tx, "ROLLBACK").unwrap();
    assert_eq!(
        id_list(
            &engine,
            "SELECT /*+ IVF_NPROBE(3) */ id FROM items ORDER BY DISTANCE(emb,'[2,0,0]') LIMIT 1"
        ),
        vec![2],
        "rollback must leave the old shared IVF candidate intact"
    );

    engine.execute_with_session(&mut tx, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut tx, "SAVEPOINT before_vector_update")
        .unwrap();
    engine
        .execute_with_session(&mut tx, "UPDATE items SET emb='[8,8,8]' WHERE id=3")
        .unwrap();
    engine
        .execute_with_session(&mut tx, "ROLLBACK TO SAVEPOINT before_vector_update")
        .unwrap();
    engine.execute_with_session(&mut tx, "COMMIT").unwrap();
    assert_eq!(
        id_list(
            &engine,
            "SELECT /*+ IVF_NPROBE(3) */ id FROM items ORDER BY DISTANCE(emb,'[3,0,0]') LIMIT 1"
        ),
        vec![3]
    );

    assert!(
        engine
            .database()
            .vector_index_runtime_stats()
            .values()
            .all(|s| s.finalized),
        "rollback and rollback-to-savepoint must leave IVF finalized"
    );
    engine.execute_with_session(&mut tx, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut tx, "INSERT INTO items VALUES (4,'[4,0,0]')")
        .unwrap();
    engine.execute_with_session(&mut tx, "ROLLBACK").unwrap();
    assert!(engine
        .database()
        .vector_index_runtime_stats()
        .values()
        .all(|s| s.finalized));

    drop(engine);
    let reopened = open_strict_hnsw_test_engine(dir.path());
    assert_eq!(
        id_list(
            &reopened,
            "SELECT /*+ IVF_NPROBE(3) */ id FROM items ORDER BY DISTANCE(emb,'[2,0,0]') LIMIT 1"
        ),
        vec![2],
        "rollback must restore the durable finalized marker; reopen remains correct"
    );
}

#[test]
fn ivf_optimize_retrains_and_stays_correct() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(5) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING IVF WITH (nlist=6)")
        .unwrap();
    let n = 60;
    let mut vectors = gen_vectors(n, 5, 0xB0B0_1234);
    bulk_insert_vectors(&engine, &vectors);
    // Warm + delete a third of the rows (creates tombstones + drift).
    let q = vec_text(&gen_vectors(1, 5, 0xC0DE)[0]);
    let _ = id_list(
        &engine,
        &format!("SELECT /*+ IVF_NPROBE(6) */ id FROM items ORDER BY DISTANCE(emb, '{q}') LIMIT 5"),
    );
    for i in 0..20 {
        engine
            .execute(&format!("DELETE FROM items WHERE id = {}", i + 1))
            .unwrap();
    }
    // OPTIMIZE retrains the coarse quantizer over the surviving rows.
    engine.execute("OPTIMIZE TABLE items").unwrap();

    // After retrain the full-probe KNN must still equal brute force over the
    // surviving rows.
    let survivors: Vec<(i64, Vec<f32>)> = (20..n)
        .map(|i| ((i + 1) as i64, vectors[i].clone()))
        .collect();
    let query = gen_vectors(1, 5, 0xC0DE)[0].clone();
    let mut scored: Vec<(f32, i64)> = survivors
        .iter()
        .map(|(id, v)| {
            let d: f32 = v.iter().zip(&query).map(|(a, b)| (a - b) * (a - b)).sum();
            (d, *id)
        })
        .collect();
    scored.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    let expected: Vec<i64> = scored.into_iter().take(5).map(|(_, i)| i).collect();
    let got = id_list(
        &engine,
        &format!(
            "SELECT /*+ IVF_NPROBE(64) */ id FROM items ORDER BY DISTANCE(emb, '{q}') LIMIT 5"
        ),
    );
    let _ = &mut vectors;
    assert_eq!(
        got, expected,
        "post-OPTIMIZE KNN must match brute force over survivors"
    );
}

#[test]
fn ivf_information_schema_reports_algo_and_nlist() {
    let _env = HnswEnv::set(&[("MYDB_HNSW_BUILD_ON_DDL", "off")]);
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_hnsw_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(4) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING IVF WITH (nlist=5)")
        .unwrap();
    let base = "FROM information_schema.mydb_vector_indexes WHERE TABLE_NAME = 'items'";
    // ALGO is available even when cold.
    assert_eq!(
        col_strings(&engine, &format!("SELECT ALGO {base}")),
        vec!["IVF"]
    );
    assert_eq!(
        col_strings(&engine, &format!("SELECT REQUESTED_NLIST {base}")),
        vec!["5"]
    );
    // Cold: no warm graph yet → NLIST NULL, backend NULL.
    assert_eq!(
        col_strings(&engine, &format!("SELECT NLIST {base}")),
        vec!["NULL"]
    );

    for i in 0..30 {
        let v = gen_vectors(1, 4, 0xD00 + i as u64)[0].clone();
        engine
            .execute(&format!(
                "INSERT INTO items VALUES ({}, '{}')",
                i + 1,
                vec_text(&v)
            ))
            .unwrap();
    }
    // Warm the index with a KNN query.
    let _ = id_list(
        &engine,
        "SELECT /*+ IVF_NPROBE(5) */ id FROM items ORDER BY DISTANCE(emb, '[0,0,0,0]') LIMIT 3",
    );
    // Backend is the IVF memory (`ivf_native`) or paged (`ivf_paged_native`) sub-backend
    // depending on MYDB_VECTOR_STORAGE (default: paged).
    let backend = col_strings(&engine, &format!("SELECT BACKEND {base}"));
    assert!(
        backend == vec!["ivf_native"] || backend == vec!["ivf_paged_native"],
        "unexpected IVF backend: {backend:?}"
    );
    assert_eq!(
        col_strings(&engine, &format!("SELECT NLIST {base}")),
        vec!["5"]
    );
    assert_eq!(
        col_strings(&engine, &format!("SELECT LIVE_NODES {base}")),
        vec!["30"]
    );
    assert_eq!(
        col_strings(&engine, &format!("SELECT LOADED {base}")),
        vec!["1"]
    );
    assert_eq!(
        col_strings(&engine, &format!("SELECT BUILD_STATE {base}")),
        vec!["READY"]
    );
    if backend == vec!["ivf_paged_native"] {
        // Native pages may still be dirty in the buffer pool. File size is
        // a physical statistic, so flush before requiring an on-disk extent.
        engine.checkpoint().unwrap();
        assert!(
            col_strings(&engine, &format!("SELECT PAGED_FILE_SIZE_BYTES {base}"))[0]
                .parse::<u64>()
                .unwrap()
                > 0
        );
        assert!(
            col_strings(&engine, &format!("SELECT SQ8_COVERED_NODES {base}"))[0]
                .parse::<u64>()
                .unwrap()
                == 0
        );
    }
}

#[test]
fn online_type_copy_vector_generations_allow_tail_writes() {
    for statement in [
        "ALTER TABLE t MODIFY v BIGINT, LOCK=NONE",
        "OPTIMIZE TABLE t",
    ] {
        for backend in ["memory", "paged"] {
            let _env = HnswEnv::set(&[
                ("MYDB_VECTOR_STORAGE", backend),
                ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
                ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
            ]);
            for algo in ["HNSW", "IVF"] {
                let dir = tempfile::tempdir().unwrap();
                let engine = std::sync::Arc::new(open_strict_test_engine(dir.path()));
                engine
                    .execute("CREATE TABLE t(id INT PRIMARY KEY, v INT, emb VECTOR(3) NOT NULL)")
                    .unwrap();
                // Seed the same committed rows before index creation. The
                // regression exercises tail DML during publication below,
                // so 32 separate durable setup commits add no coverage.
                let values = (1..=32)
                    .map(|i| format!("({i},{i},'[{i},0,0]')"))
                    .collect::<Vec<_>>()
                    .join(",");
                engine
                    .execute(&format!("INSERT INTO t VALUES {values}"))
                    .unwrap();
                engine
                    .execute(&format!("CREATE INDEX emb_idx ON t(emb) USING {algo}"))
                    .unwrap();
                let hook = engine.database().install_online_copy_publish_test_hook();
                let worker = engine.clone();
                let ddl = std::thread::spawn(move || {
                    let mut session = SessionContext::default();
                    session.connection_id = 7891;
                    worker.execute_with_session(&mut session, statement)
                });
                let reached = hook.wait_until_reached(std::time::Duration::from_secs(10));
                let mut writer = SessionContext::default();
                writer.connection_id = 7892;
                let writes = [
                    "UPDATE t SET emb='[100,0,0]' WHERE id=1",
                    "DELETE FROM t WHERE id=2",
                    "INSERT INTO t VALUES(33,33,'[0,0,0]')",
                ]
                .map(|sql| engine.execute_with_session(&mut writer, sql));
                hook.release();
                let result = ddl.join().unwrap();
                assert!(reached, "{backend}/{algo}: {result:?}");
                for write in writes {
                    write.unwrap();
                }
                let result = result.unwrap();
                if statement.starts_with("OPTIMIZE") {
                    let QueryResponse::ResultSet(result) = result else {
                        panic!("status")
                    };
                    assert_eq!(result.rows.len(), 1, "{:?}", result.rows);
                    assert_eq!(result.rows[0][3], Value::String("OK".into()));
                }
                let query = "SELECT id FROM t ORDER BY DISTANCE(emb,'[0,0,0]') LIMIT 1";
                assert_eq!(id_list(&engine, query), vec![33], "{backend}/{algo}");
                drop(engine);
                let engine = open_strict_test_engine(dir.path());
                assert_eq!(id_list(&engine, query), vec![33], "reopen {backend}/{algo}");
            }
        }
    }
}

#[test]
fn ivf_physical_replica_reopens_invalidated_warm_graph() {
    let _env = paged_index_env();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING IVF WITH (nlist=3)")
        .unwrap();
    engine
        .execute("INSERT INTO items VALUES (1,'[1,0,0]'),(2,'[2,0,0]')")
        .unwrap();
    let sql = "SELECT /*+ IVF_NPROBE(1) */ id FROM items ORDER BY DISTANCE(emb,'[2,0,0]') LIMIT 1";
    assert_eq!(id_list(&engine, sql), vec![2]);
    engine.database().set_physical_replica_mode(true).unwrap();
    engine.database().invalidate_replicated_vector_graphs();
    assert!(engine.database().vector_index_runtime_stats().is_empty());
    assert_eq!(id_list(&engine, sql), vec![2]);
    assert!(!engine.database().vector_index_runtime_stats().is_empty());
    engine.database().set_physical_replica_mode(false).unwrap();
}

#[test]
fn ivf_replica_wal_dml_refreshes_warm_directory() {
    fn copy_tree(from: &std::path::Path, to: &std::path::Path) {
        std::fs::create_dir_all(to).unwrap();
        for entry in std::fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            let dest = to.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_tree(&entry.path(), &dest);
            } else {
                std::fs::copy(entry.path(), dest).unwrap();
            }
        }
    }
    let _env = paged_index_env();
    let source = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    {
        let engine = open_strict_test_engine(source.path());
        engine
            .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
            .unwrap();
        engine
            .execute("INSERT INTO items VALUES (1,'[10,0,0]'),(2,'[20,0,0]'),(3,'[30,0,0]')")
            .unwrap();
        engine
            .execute("CREATE INDEX idx_emb ON items(emb) USING IVF WITH (nlist=2)")
            .unwrap();
    }
    copy_tree(source.path(), target.path());
    let mut role = crate::repl::role::RoleState::new_standalone();
    role.demote_to_replica("127.0.0.1:1".into());
    crate::repl::role::save(target.path(), &role).unwrap();
    let master = open_strict_test_engine(source.path());
    let replica = open_strict_test_engine(target.path());
    replica.database().set_physical_replica_mode(true).unwrap();
    let sql = "SELECT /*+ IVF_NPROBE(2) */ id FROM items ORDER BY DISTANCE(emb,'[0,0,0]') LIMIT 1";
    assert_eq!(id_list(&replica, sql), vec![1]);
    // Mirror the applier's real WAL projection, without TCP timing in the test.
    let mut next_lsn = crate::wal::LogManager::scan(source.path())
        .unwrap()
        .next_lsn;
    for (dml, expected) in [
        ("INSERT INTO items VALUES (4,'[0,0,0]')", 4),
        ("UPDATE items SET emb='[40,0,0]' WHERE id=4", 1),
        ("DELETE FROM items WHERE id=1", 2),
    ] {
        master.execute(dml).unwrap();
        master.database().wal_log().lock().flush().unwrap();
        let scan = crate::wal::LogManager::scan(source.path()).unwrap();
        for record in scan.records.iter().filter(|r| r.lsn >= next_lsn) {
            replica.database().apply_wal_record(record).unwrap();
        }
        next_lsn = scan.next_lsn;
        let mut session = SessionContext::default();
        assert_eq!(
            id_list_session(&replica, &mut session, sql),
            vec![expected],
            "replica after {dml}"
        );
        assert!(
            session.warnings.is_empty(),
            "unexpected fallback: {:?}",
            session.warnings
        );
        let stats = replica.database().vector_index_runtime_stats();
        assert_eq!(stats.len(), 1);
        let stats = stats.values().next().unwrap();
        assert_eq!(stats.backend, "ivf_native");
        assert!(!stats.finalized);
        let brute = sql.replace("IVF_NPROBE(2)", "NO_VECTOR_INDEX()");
        assert_eq!(id_list(&replica, sql), id_list(&replica, &brute));
    }
}

// Source audit 2026-09-27: reject malformed indexed rows before heap commit.
fn source_audit_dimension_rejection(backend: &str) {
    let _env = HnswEnv::set(&[("MYDB_VECTOR_STORAGE", backend)]);
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE audit_dim (id INT PRIMARY KEY, v VECTOR NOT NULL)")
        .unwrap();
    engine
        .execute("INSERT INTO audit_dim VALUES (1, '[1,2,3]')")
        .unwrap();
    assert!(engine
        .execute("CREATE INDEX iv ON audit_dim(v) USING HNSW")
        .is_err());
    engine
        .execute("ALTER TABLE audit_dim MODIFY v VECTOR(3) NOT NULL")
        .unwrap();
    engine
        .execute("CREATE INDEX iv ON audit_dim(v) USING HNSW")
        .unwrap();
    for sql in [
        "INSERT INTO audit_dim VALUES (2, '[1,2]')",
        "INSERT INTO audit_dim VALUES (2, '[1,2,3]'), (3, '[1,2,3,4]')",
        "UPDATE audit_dim SET v='[1,2]' WHERE id=1",
    ] {
        assert!(engine.execute(sql).is_err(), "accepted invalid row: {sql}");
        assert_eq!(cell(&engine, "SELECT COUNT(*) FROM audit_dim"), "1");
        assert_eq!(cell(&engine, "SELECT VECTOR_DIM(v) FROM audit_dim"), "3");
    }
    for hint in ["", "/*+ NO_VECTOR_INDEX */"] {
        assert_eq!(
            id_list(
                &engine,
                &format!("SELECT {hint} id FROM audit_dim ORDER BY DISTANCE(v, '[1,2,3]') LIMIT 3")
            ),
            [1]
        );
    }
    engine.execute("DROP TABLE audit_dim").unwrap();
}

#[test]
fn source_audit_b1_b2_paged_dimension_rejection() {
    source_audit_dimension_rejection("paged");
}
#[test]
fn source_audit_b1_b2_memory_dimension_rejection() {
    source_audit_dimension_rejection("memory");
}

#[test]
fn source_audit_b3_alter_vector_index_requires_not_null() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE audit_null (id INT PRIMARY KEY, v VECTOR(3) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX iv ON audit_null(v) USING HNSW")
        .unwrap();
    for sql in [
        "ALTER TABLE audit_null MODIFY v VECTOR(3) NULL",
        "ALTER TABLE audit_null CHANGE v w VECTOR(3) NULL",
        "ALTER TABLE audit_null ADD COLUMN x INT, MODIFY v VECTOR(3) NULL",
    ] {
        assert!(
            engine.execute(sql).is_err(),
            "accepted invalid schema: {sql}"
        );
    }
    assert!(engine.execute("SELECT x FROM audit_null").is_err());
    assert!(engine
        .execute("INSERT INTO audit_null VALUES (1,NULL)")
        .is_err());
    engine.execute("DROP INDEX iv ON audit_null").unwrap();
    engine
        .execute("ALTER TABLE audit_null MODIFY v VECTOR(3) NULL")
        .unwrap();
    engine
        .execute("INSERT INTO audit_null VALUES (1,NULL)")
        .unwrap();
}

#[test]
fn source_audit_b5_restart_reconciles_snapshot_vectors() {
    let _env = HnswEnv::set(&[
        ("MYDB_VECTOR_STORAGE", "memory"),
        ("MYDB_HNSW_PERSIST", "on"),
        ("MYDB_HNSW_PERSIST_DIRTY_RATIO", "0.0"),
        ("MYDB_HNSW_BUILD_THREADS", "4"),
        ("MYDB_HNSW_BUILD_MIN_PAGES_PER_WORKER", "1"),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let (snapshot_path, snapshot_bytes);
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(2) NOT NULL)")
            .unwrap();
        engine
            .execute("INSERT INTO items VALUES (1,'[1,2]'), (2,'[2,3]')")
            .unwrap();
        engine
            .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
            .unwrap();
        engine.checkpoint().unwrap();
        snapshot_path = find_hnsw_files(dir.path()).pop().unwrap();
        snapshot_bytes = std::fs::read(&snapshot_path).unwrap();
        engine
            .execute("UPDATE items SET emb='[1000,2000]' WHERE id=1")
            .unwrap();
        engine
            .execute("INSERT INTO items VALUES (3,'[-1000,-2000]')")
            .unwrap();
        engine.checkpoint().unwrap();
    }
    // Keep the durable current heap and deliberately restore the older graph
    // snapshot. This models an ordinary snapshot dirty gate, independent of
    // destructor/checkpoint scheduling in the test harness.
    std::fs::write(&snapshot_path, snapshot_bytes).unwrap();
    let reopened = open_strict_test_engine(dir.path());
    let table = reopened.database().snapshot_table_schema("items").unwrap();
    let index = table
        .indexes
        .iter()
        .find(|i| i.name == "idx_emb")
        .unwrap_or_else(|| panic!("reopen lost HNSW catalog entry: {table:?}"));
    let graph = reopened
        .database()
        .vector_graph_get_or_build(&table, index)
        .unwrap();
    // Inspect graph distance directly: SQL re-ranking would hide an old vector.
    let crate::engine::vector::VectorGraph::Memory(memory) = graph.as_ref() else {
        panic!("expected memory graph")
    };
    let found = memory.search(&[1000.0, 2000.0], 1, 100);
    assert_eq!(found[0].1, 0.0, "snapshot retained the old embedding");
    assert_eq!(graph.len(), 3);
    assert_eq!(
        id_list(
            &reopened,
            "SELECT id FROM items ORDER BY DISTANCE(emb,'[1000,2000]') LIMIT 1"
        ),
        [1]
    );
}

#[test]
fn source_audit_variable_vector_requires_declared_index_dimension() {
    let _env = HnswEnv::set(&[("MYDB_VECTOR_STORAGE", "memory")]);
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR NOT NULL)")
        .unwrap();
    for algo in ["HNSW", "IVF"] {
        assert!(engine
            .execute(&format!("CREATE INDEX idx_emb ON items(emb) USING {algo}"))
            .is_err());
    }
    engine
        .execute("ALTER TABLE items MODIFY emb VECTOR(2) NOT NULL")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING IVF")
        .unwrap();
    engine
        .execute("INSERT INTO items VALUES (1,'[1,2]')")
        .unwrap();
    assert_eq!(
        id_list(
            &engine,
            "SELECT id FROM items ORDER BY DISTANCE(emb,'[1,2]') LIMIT 1"
        ),
        [1]
    );
}

#[test]
fn hnsw_mapped_navigation_is_revoked_on_physical_replica_transition() {
    let _env = paged_index_env();
    let dir = tempfile::Builder::new()
        .prefix("mapped-replica-")
        .tempdir_in("/tmp/mydb")
        .unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE mapped_role (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
        .unwrap();
    engine
        .execute("INSERT INTO mapped_role VALUES (1,'[1,0,0]'),(2,'[2,0,0]')")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON mapped_role(emb) USING HNSW")
        .unwrap();
    let sql = "SELECT id FROM mapped_role ORDER BY DISTANCE(emb,'[2,0,0]') LIMIT 1";
    assert_eq!(id_list(&engine, sql), vec![2]);
    assert!(engine
        .database()
        .vector_index_runtime_stats()
        .values()
        .any(|stats| stats.backend == "paged_mapped"));
    engine.database().set_physical_replica_mode(true).unwrap();
    assert!(engine
        .database()
        .vector_index_runtime_stats()
        .values()
        .all(|stats| stats.backend != "paged_mapped"));
    assert_eq!(id_list(&engine, sql), vec![2]);
    engine.database().set_physical_replica_mode(false).unwrap();
}

// ===================== Graph/heap consistency regressions =====================

fn insert_vector_rows(engine: &StorageQueryEngine, rows: &[(i64, Vec<f32>)]) {
    for chunk in rows.chunks(100) {
        let values: Vec<String> = chunk
            .iter()
            .map(|(id, v)| format!("({id}, '{}')", vec_text(v)))
            .collect();
        engine
            .execute(&format!("INSERT INTO items VALUES {}", values.join(",")))
            .unwrap();
    }
}

/// Tombstones clustered around the query must not consume the whole `ef`
/// candidate pool and make the scan report an exhausted graph while live
/// rows remain reachable.
fn assert_clustered_tombstones_do_not_truncate_knn(backend: &str) {
    let _env = HnswEnv::set(&[
        ("MYDB_VECTOR_STORAGE", backend),
        ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
        ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
        ("MYDB_HNSW_PERSIST", "off"),
        ("MYDB_HNSW_COMPACT_DISABLE", "1"),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    let mut rows: Vec<(i64, Vec<f32>)> = (1..=300)
        .map(|i| (i, vec![i as f32 * 0.001, (i % 7) as f32 * 0.001, 0.0]))
        .collect();
    rows.extend((1..=50).map(|i| (1000 + i, vec![10.0 + i as f32, 0.0, 0.0])));
    insert_vector_rows(&engine, &rows);
    engine.database().vector_graph_warm_all_for_test();
    engine.execute("DELETE FROM items WHERE id <= 295").unwrap();
    let (_, live, stale) = hnsw_stats(&engine).unwrap();
    assert_eq!((live, stale), (55, 295));

    let knn = "SELECT id FROM items ORDER BY DISTANCE(emb, '[0,0,0]') LIMIT 10";
    assert!(explain_mentions_vector_index(&engine, knn));
    assert_eq!(
        id_list(&engine, knn),
        vec![296, 297, 298, 299, 300, 1001, 1002, 1003, 1004, 1005],
        "{backend}: KNN must return k live rows despite clustered tombstones"
    );
}

#[test]
fn memory_hnsw_clustered_tombstones_do_not_truncate_knn() {
    assert_clustered_tombstones_do_not_truncate_knn("memory");
}

#[test]
fn paged_hnsw_clustered_tombstones_do_not_truncate_knn() {
    assert_clustered_tombstones_do_not_truncate_knn("paged");
}

/// A row committed while a lazy graph build is running (after its heap scan,
/// before publication) must still reach the published graph.
fn assert_cold_build_keeps_concurrent_commit(backend: &str) {
    let _env = HnswEnv::set(&[
        ("MYDB_VECTOR_STORAGE", backend),
        ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
        ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
        ("MYDB_HNSW_PERSIST", "off"),
        ("MYDB_HNSW_BUILD_ON_DDL", "0"),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(open_strict_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    let rows: Vec<(i64, Vec<f32>)> = (1..=40)
        .map(|i| (i, vec![10.0 + i as f32, 0.0, 0.0]))
        .collect();
    insert_vector_rows(&engine, &rows);
    assert!(hnsw_stats(&engine).is_none(), "graph must start cold");

    let writer = Arc::clone(&engine);
    *crate::storage::VECTOR_BUILD_BEFORE_PUBLISH_HOOK.lock() = Some(Box::new(move || {
        std::thread::spawn(move || {
            writer
                .execute("INSERT INTO items VALUES (999, '[0,0,0]')")
                .unwrap();
            writer.execute("DELETE FROM items WHERE id = 1").unwrap();
        })
        .join()
        .unwrap();
    }));
    let knn = "SELECT id FROM items ORDER BY DISTANCE(emb, '[0,0,0]') LIMIT 3";
    // The first query triggers the build; its snapshot may predate the insert.
    let _ = id_list(&engine, knn);
    assert!(crate::storage::VECTOR_BUILD_BEFORE_PUBLISH_HOOK
        .lock()
        .is_none());
    assert_eq!(
        id_list(&engine, knn),
        vec![999, 2, 3],
        "{backend}: rows committed during a cold build must be indexed"
    );
    let (_, live, _) = hnsw_stats(&engine).unwrap();
    assert_eq!(live, 40, "{backend}: build-window DELETE must be applied");
}

#[test]
fn memory_hnsw_cold_build_keeps_concurrent_commit() {
    assert_cold_build_keeps_concurrent_commit("memory");
}

#[test]
fn paged_hnsw_cold_build_keeps_concurrent_commit() {
    assert_cold_build_keeps_concurrent_commit("paged");
}

/// A durable paged graph that is cold after restart must not miss DML that
/// commits before the first KNN query reopens it.
#[test]
fn paged_hnsw_dml_while_cold_after_restart_is_indexed() {
    let _env = HnswEnv::set(&[
        ("MYDB_VECTOR_STORAGE", "paged"),
        ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
        ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let rows: Vec<(i64, Vec<f32>)> = (1..=40)
        .map(|i| (i, vec![10.0 + i as f32, 0.0, 0.0]))
        .collect();
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
            .unwrap();
        engine
            .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
            .unwrap();
        insert_vector_rows(&engine, &rows);
        engine.database().vector_graph_warm_all_for_test();
        engine.checkpoint().unwrap();
    }
    let engine = open_strict_test_engine(dir.path());
    assert!(
        hnsw_stats(&engine).is_none(),
        "graph must be cold after restart"
    );
    engine
        .execute("INSERT INTO items VALUES (999, '[0,0,0]')")
        .unwrap();
    engine.execute("DELETE FROM items WHERE id = 1").unwrap();
    let knn = "SELECT id FROM items ORDER BY DISTANCE(emb, '[0,0,0]') LIMIT 3";
    assert!(explain_mentions_vector_index(&engine, knn));
    assert_eq!(id_list(&engine, knn), vec![999, 2, 3]);
}

/// A selective residual filter must not make KNN return fewer than `k` rows
/// while more matches exist, and a filter matching nothing must terminate
/// promptly instead of widening `ef` without bound.
#[test]
fn hnsw_selective_filter_returns_k_rows_and_empty_filter_terminates() {
    let _env = HnswEnv::set(&[
        ("MYDB_VECTOR_STORAGE", "memory"),
        ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
        ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
        ("MYDB_HNSW_MAX_SCAN_TUPLES", "64"),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    // 1500 rows near the origin, plus 12 far away rows that alone pass the
    // filter: the ANN budget (64 tuples) is spent long before reaching them.
    let mut rows: Vec<(i64, Vec<f32>)> = (1..=1500)
        .map(|i| (i, vec![(i % 40) as f32 * 0.01, (i / 40) as f32 * 0.01, 0.0]))
        .collect();
    rows.extend((1..=12).map(|i| (5000 + i, vec![100.0 + i as f32, 0.0, 0.0])));
    insert_vector_rows(&engine, &rows);
    engine.database().vector_graph_warm_all_for_test();

    let knn = "SELECT id FROM items WHERE id > 5000 ORDER BY DISTANCE(emb, '[0,0,0]') LIMIT 10";
    assert!(explain_mentions_vector_index(&engine, knn));
    assert_eq!(id_list(&engine, knn), (5001..=5010).collect::<Vec<_>>());

    let started = std::time::Instant::now();
    let none = "SELECT id FROM items WHERE id < 0 ORDER BY DISTANCE(emb, '[0,0,0]') LIMIT 10";
    assert!(id_list(&engine, none).is_empty());
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "an empty filter must not widen ef without bound: {:?}",
        started.elapsed()
    );
}

/// A failed paged graph write must not leave a trusted index that silently
/// lacks the committed row: the graph is invalidated and rebuilt.
#[test]
fn paged_hnsw_failed_write_invalidates_and_rebuilds() {
    use crate::engine::vector::graph::FAIL_PAGED_WRITES_FOR_TEST;
    use std::sync::atomic::Ordering;
    let _env = paged_index_env();
    let dir = tempfile::tempdir().unwrap();
    let rows: Vec<(i64, Vec<f32>)> = (1..=40)
        .map(|i| (i, vec![10.0 + i as f32, 0.0, 0.0]))
        .collect();
    let knn = "SELECT id FROM items ORDER BY DISTANCE(emb, '[0,0,0]') LIMIT 3";
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
            .unwrap();
        engine
            .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
            .unwrap();
        insert_vector_rows(&engine, &rows);
        engine.database().vector_graph_warm_all_for_test();
        FAIL_PAGED_WRITES_FOR_TEST.store(true, Ordering::Release);
        let inserted = engine.execute("INSERT INTO items VALUES (999, '[0,0,0]')");
        FAIL_PAGED_WRITES_FOR_TEST.store(false, Ordering::Release);
        inserted.unwrap();
        assert!(
            hnsw_stats(&engine).is_none(),
            "failed graph must be evicted"
        );
        assert_eq!(id_list(&engine, knn), vec![999, 1, 2]);
        engine.checkpoint().unwrap();
    }
    let engine = open_strict_test_engine(dir.path());
    assert_eq!(id_list(&engine, knn), vec![999, 1, 2]);
}

/// Rows deleted after a `.hnsw` snapshot was written must not come back as
/// live graph nodes when the snapshot is loaded and reconciled on restart.
#[test]
fn hnsw_snapshot_reload_tombstones_rows_deleted_after_persist() {
    let _env = HnswEnv::set(&[
        ("MYDB_VECTOR_STORAGE", "memory"),
        ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
        ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
        ("MYDB_HNSW_PERSIST", "on"),
        ("MYDB_HNSW_PERSIST_DIRTY_RATIO", "0.0"),
        ("MYDB_HNSW_COMPACT_DISABLE", "1"),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let rows: Vec<(i64, Vec<f32>)> = (1..=60).map(|i| (i, vec![i as f32, 0.0, 0.0])).collect();
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
            .unwrap();
        engine
            .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
            .unwrap();
        insert_vector_rows(&engine, &rows);
        engine.database().vector_graph_warm_all_for_test();
        engine.checkpoint().unwrap();
        assert!(!find_hnsw_files(dir.path()).is_empty(), "snapshot written");
        engine.execute("DELETE FROM items WHERE id <= 20").unwrap();
    }
    let engine = open_strict_test_engine(dir.path());
    let knn = "SELECT id FROM items ORDER BY DISTANCE(emb, '[0,0,0]') LIMIT 3";
    assert_eq!(id_list(&engine, knn), vec![21, 22, 23]);
    let (nodes, live, stale) = hnsw_stats(&engine).unwrap();
    assert_eq!((nodes, live, stale), (60, 40, 20));
}

/// A snapshot that still sees a row deleted or moved by a later commit must
/// keep finding it through the index until that snapshot ends.
fn assert_old_snapshot_keeps_superseded_versions(backend: &str) {
    let _env = HnswEnv::set(&[
        ("MYDB_VECTOR_STORAGE", backend),
        ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
        ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
        ("MYDB_HNSW_COMPACT_DISABLE", "1"),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    let mut rows: Vec<(i64, Vec<f32>)> = (1..=40)
        .map(|i| (i, vec![10.0 + i as f32, 0.0, 0.0]))
        .collect();
    rows.push((999, vec![0.0, 0.0, 0.0]));
    insert_vector_rows(&engine, &rows);
    engine.database().vector_graph_warm_all_for_test();
    let knn = "SELECT id FROM items ORDER BY DISTANCE(emb, '[0,0,0]') LIMIT 3";

    let mut reader = SessionContext::default();
    for sql in [
        "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        "BEGIN",
    ] {
        engine.execute_with_session(&mut reader, sql).unwrap();
    }
    assert_eq!(id_list_session(&engine, &mut reader, knn), vec![999, 1, 2]);
    engine.execute("DELETE FROM items WHERE id = 999").unwrap();
    engine
        .execute("UPDATE items SET emb = '[5000,0,0]' WHERE id = 1")
        .unwrap();
    assert_eq!(id_list(&engine, knn), vec![2, 3, 4]);
    assert_eq!(
        id_list_session(&engine, &mut reader, knn),
        vec![999, 1, 2],
        "{backend}: the old snapshot must still find superseded versions"
    );
    engine.execute_with_session(&mut reader, "COMMIT").unwrap();
    // Once the reader is gone the superseded versions are retired.
    engine.database().drain_vector_purges();
    let (_, live, stale) = hnsw_stats(&engine).unwrap();
    assert_eq!((live, stale), (40, 2), "{backend}");
    assert_eq!(id_list(&engine, knn), vec![2, 3, 4]);
}

#[test]
fn memory_hnsw_old_snapshot_keeps_superseded_versions() {
    assert_old_snapshot_keeps_superseded_versions("memory");
}

#[test]
fn paged_hnsw_old_snapshot_keeps_superseded_versions() {
    assert_old_snapshot_keeps_superseded_versions("paged");
}

/// Background maintenance detaches paged tombstones from the navigable
/// graph, so searches stop traversing deleted nodes.
#[test]
fn paged_hnsw_maintenance_detaches_tombstones() {
    let _env = HnswEnv::set(&[
        ("MYDB_VECTOR_STORAGE", "paged"),
        ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
        ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
        ("MYDB_HNSW_COMPACT_RATIO", "0.1"),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
    let rows: Vec<(i64, Vec<f32>)> = (1..=400)
        .map(|i| {
            let (x, y) = ((i % 20) as f32, (i / 20) as f32);
            (i, vec![x, y, ((i * 7) % 13) as f32 * 0.1])
        })
        .collect();
    insert_vector_rows(&engine, &rows);
    engine.database().vector_graph_warm_all_for_test();
    // Remove a contiguous block of the plane, plus scattered rows.
    engine
        .execute("DELETE FROM items WHERE (id % 20 < 10 AND id / 20 < 10) OR id % 7 = 0")
        .unwrap();
    let db = engine.database();
    assert!(db.paged_edges_into_tombstones_for_test("idx_emb") > 0);
    db.run_vector_maintenance_for_test();
    assert_eq!(db.paged_edges_into_tombstones_for_test("idx_emb"), 0);

    let survivors: Vec<(i64, Vec<f32>)> = rows
        .iter()
        .filter(|(i, _)| !((i % 20 < 10 && i / 20 < 10) || i % 7 == 0))
        .cloned()
        .collect();
    for q in [[0.0f32, 0.0, 0.0], [5.0, 5.0, 0.5], [19.0, 19.0, 1.0]] {
        let knn = format!(
            "SELECT id FROM items ORDER BY DISTANCE(emb, '{}') LIMIT 5",
            vec_text(&q)
        );
        let mut expected: Vec<(f32, i64)> = survivors
            .iter()
            .map(|(id, v)| (v.iter().zip(&q).map(|(a, b)| (a - b) * (a - b)).sum(), *id))
            .collect();
        expected.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        let expected: Vec<i64> = expected.iter().take(5).map(|(_, id)| *id).collect();
        assert_eq!(id_list(&engine, &knn), expected, "query {q:?}");
    }
    // New writes keep working on the repaired graph.
    engine
        .execute("INSERT INTO items VALUES (9999, '[0,0,0]')")
        .unwrap();
    assert_eq!(
        id_list(
            &engine,
            "SELECT id FROM items ORDER BY DISTANCE(emb, '[0,0,0]') LIMIT 1"
        ),
        vec![9999]
    );
}

/// Incremental paged-graph maintenance logs page deltas after each page's
/// anchoring image; recovery must replay them onto never-flushed pages.
#[test]
fn paged_hnsw_delta_wal_recovers_after_crash() {
    let _env = paged_index_env();
    let dir = tempfile::tempdir().unwrap();
    let mut rows: Vec<(i64, Vec<f32>)> = gen_vectors(300, 16, 0x5EED_0042)
        .into_iter()
        .enumerate()
        .map(|(i, v)| (i as i64 + 1, v))
        .collect();
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(16) NOT NULL)")
            .unwrap();
        insert_vector_rows(&engine, &rows);
        engine
            .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
            .unwrap();
        engine.checkpoint().unwrap();
        let extra: Vec<(i64, Vec<f32>)> = gen_vectors(200, 16, 0x5EED_0043)
            .into_iter()
            .enumerate()
            .map(|(i, v)| (1000 + i as i64, v))
            .collect();
        let wal = engine.database().wal_bytes_for_test();
        for (id, v) in &extra {
            engine
                .execute(&format!(
                    "INSERT INTO items VALUES ({id}, '{}')",
                    vec_text(v)
                ))
                .unwrap();
        }
        let per_row = (engine.database().wal_bytes_for_test() - wal) / extra.len() as u64;
        assert!(
            per_row < 48 * 1024,
            "graph maintenance should log page deltas, not full images: {per_row} B/row"
        );
        engine
            .execute("DELETE FROM items WHERE id % 5 = 0")
            .unwrap();
        engine
            .execute("UPDATE items SET emb = emb WHERE id % 7 = 0")
            .unwrap();
        rows.extend(extra);
        rows.retain(|(id, _)| id % 5 != 0);
        // The derived completion marker follows CommitTxn. Make that marker
        // durable to exercise complete-delta redo rather than safe rebuild.
        engine.database().wal_log().lock().flush().unwrap();
        // Crash: no checkpoint, no buffer-pool writeback.
        std::mem::forget(engine);
    }
    let engine = open_strict_test_engine(dir.path());
    let vectors: Vec<Vec<f32>> = rows.iter().map(|(_, v)| v.clone()).collect();
    for q in gen_vectors(5, 16, 0x5EED_0044) {
        let knn = format!(
            "SELECT id FROM items ORDER BY DISTANCE(emb, '{}') LIMIT 5",
            vec_text(&q)
        );
        let expected: Vec<i64> = brute_force_l2(&vectors, &q, 5)
            .into_iter()
            .map(|i| rows[i as usize - 1].0)
            .collect();
        assert_eq!(id_list(&engine, &knn), expected);
    }
    // The durable graph was replayed (tombstones included), not rebuilt.
    assert_eq!(hnsw_stats(&engine).unwrap(), (500, 400, 100));
}

#[path = "hnsw_regressions.rs"]
mod hnsw_regressions;

#[test]
fn review_20261024_hnsw_replica_wal_dml_refreshes_warm_graph() {
    for backend in ["memory", "paged"] {
        for partitioned in [false, true] {
            hnsw_replica_case(backend, partitioned);
        }
    }
}

fn hnsw_replica_case(backend: &str, partitioned: bool) {
    use std::io::{Read, Seek, SeekFrom};
    fn copy_tree(from: &std::path::Path, to: &std::path::Path) {
        std::fs::create_dir_all(to).unwrap();
        for entry in std::fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            let dest = to.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_tree(&entry.path(), &dest);
            } else {
                std::fs::copy(entry.path(), dest).unwrap();
            }
        }
    }
    let _env = paged_index_env();
    let _backend = HnswEnv::set(&[("MYDB_VECTOR_STORAGE", backend)]);
    let source = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    {
        let engine = open_strict_test_engine(source.path());
        let partition = if partitioned {
            " PARTITION BY RANGE(id)(PARTITION p0 VALUES LESS THAN(2),PARTITION p1 VALUES LESS THAN MAXVALUE)"
        } else {
            ""
        };
        engine
            .execute(&format!(
                "CREATE TABLE items(id INT PRIMARY KEY,emb VECTOR(3) NOT NULL){partition}"
            ))
            .unwrap();
        engine
            .execute("INSERT INTO items VALUES (1,'[10,0,0]'),(2,'[20,0,0]'),(3,'[30,0,0]')")
            .unwrap();
        engine
            .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
            .unwrap();
    }
    copy_tree(source.path(), target.path());
    let mut role = crate::repl::role::RoleState::new_standalone();
    role.demote_to_replica("127.0.0.1:1".into());
    crate::repl::role::save(target.path(), &role).unwrap();
    let master = open_strict_test_engine(source.path());
    let replica = open_strict_test_engine(target.path());
    replica.database().set_physical_replica_mode(true).unwrap();
    let sql = "SELECT /*+ HNSW_EF(64) */ id FROM items ORDER BY DISTANCE(emb,'[0,0,0]') LIMIT 1";
    assert_eq!(id_list(&replica, sql), vec![1]);
    // Mirror the applier's real WAL projection, without TCP timing in the test.
    let mut next_lsn = replica.database().wal_log().lock().last_lsn() + 1;
    for (dml, expected) in [
        ("INSERT INTO items VALUES (4,'[0,0,0]')", 4),
        ("UPDATE items SET emb='[40,0,0]' WHERE id=4", 1),
        ("DELETE FROM items WHERE id=1", 2),
    ] {
        master.execute(dml).unwrap();
        master.database().wal_log().lock().flush().unwrap();
        let scan = crate::wal::LogManager::scan(source.path()).unwrap();
        // Persist the same physical frames as the real receiver before page
        // redo. Applying only decoded records cannot support a restart test.
        let after = next_lsn - 1;
        let prefixes = master
            .database()
            .wal_log()
            .lock()
            .pin_prefix_after(after, None)
            .unwrap();
        for prefix in prefixes {
            let mut file = prefix.file.open().unwrap().try_clone().unwrap();
            file.seek(SeekFrom::Start(prefix.start_offset)).unwrap();
            let mut bytes = Vec::new();
            file.take(prefix.len - prefix.start_offset)
                .read_to_end(&mut bytes)
                .unwrap();
            let mut offset = 0;
            while offset < bytes.len() {
                let frame = crate::wal::decode_frame_for_replication(&bytes[offset..]).unwrap();
                if frame.header.lsn > replica.database().wal_log().lock().last_lsn() {
                    replica
                        .database()
                        .wal_log()
                        .lock()
                        .append_replication_frame(&bytes[offset..offset + frame.frame_len], &frame)
                        .unwrap();
                }
                offset += frame.frame_len;
            }
        }
        replica.database().wal_log().lock().flush().unwrap();
        for record in scan.records.iter().filter(|r| r.lsn >= next_lsn) {
            replica.database().apply_wal_record(record).unwrap();
        }
        next_lsn = scan.next_lsn;
        let mut session = SessionContext::default();
        assert_eq!(
            id_list_session(&replica, &mut session, sql),
            vec![expected],
            "replica after {dml}"
        );
        assert!(
            session.warnings.is_empty(),
            "unexpected fallback: {:?}",
            session.warnings
        );
        let stats = replica.database().vector_index_runtime_stats();
        assert_eq!(stats.len(), 1);
        let stats = stats.values().next().unwrap();
        assert_eq!(stats.backend, "memory");

        let brute = sql.replace("HNSW_EF(64)", "NO_VECTOR_INDEX()");
        assert_eq!(id_list(&replica, sql), id_list(&replica, &brute));
    }
    replica.database().set_physical_replica_mode(false).unwrap();
    crate::repl::role::save(
        target.path(),
        &crate::repl::role::RoleState::new_standalone(),
    )
    .unwrap();
    assert_eq!(
        id_list(&replica, sql),
        vec![2],
        "promoted {backend}, partitioned={partitioned}"
    );
    drop(replica);
    let promoted = open_strict_test_engine(target.path());
    assert_eq!(
        id_list(&promoted, sql),
        vec![2],
        "promoted restart {backend}, partitioned={partitioned}"
    );
}

#[test]
fn vector_indexes_reject_retired_algorithm_in_all_ddl_forms() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE retired_algorithm(id INT PRIMARY KEY,v VECTOR(3) NOT NULL)")
        .unwrap();
    for sql in [
        "CREATE INDEX obsolete ON retired_algorithm(v) USING IVF_SQ8",
        "ALTER TABLE retired_algorithm ADD INDEX obsolete(v) USING IVF_SQ8",
        "CREATE TABLE retired_inline(id INT PRIMARY KEY,v VECTOR(3) NOT NULL,KEY obsolete(v) USING IVF_SQ8)",
    ] {
        let err = engine.execute(sql).expect_err("retired algorithm must not be accepted");
        assert!(err.is(crate::error::catalog::ER_NOT_SUPPORTED_YET)
                && err.to_string().contains("IVF_SQ8"), "{sql}: {err:?}");
    }
    assert_eq!(
        engine
            .database()
            .snapshot_table_schema("retired_algorithm")
            .unwrap()
            .indexes
            .iter()
            .filter(|i| i.name == "obsolete")
            .count(),
        0
    );
    engine
        .execute("CREATE INDEX native ON retired_algorithm(v) USING IVF WITH (nlist=1)")
        .unwrap();
}
