//! Candidate changes must consume each child once, including when a preferred
//! NLJ is rejected by estimates, actual cardinality, or the INLJ row limit.
use super::*;
use crate::engine::executor::select::analyze_stats;
use crate::parser::JoinType;
use crate::storage::{DurabilityMode, IsolationLevel, StorageConfig};

fn scan(table: PlanTable, estimated_rows: f64) -> PhysicalPlan {
    PhysicalPlan::SeqScan {
        table,
        filter: None,
        estimated_rows,
        access_rows: estimated_rows,
        cost: crate::engine::cbo::Cost::ZERO,
        possible_keys: Vec::new(),
        runtime_filters: Vec::new(),
        hint_note: None,
        projected_columns: None,
    }
}

fn values(rows: &[PlanRow]) -> Vec<String> {
    let mut values: Vec<_> = rows
        .iter()
        .map(|row| {
            format!(
                "{:?}",
                row.fields.iter().map(|f| &f.value).collect::<Vec<_>>()
            )
        })
        .collect();
    values.sort();
    values
}

#[test]
fn candidate_switches_reuse_inputs_and_match_nested_loop_oracle() {
    // This direct row-adapter entry needs the scalar trace envelope. Normal
    // SQL uses the outer native operator to publish its measurements.
    let _mode = crate::engine::executor::vec::config::enter_mode(
        crate::engine::executor::vec::config::VecMode::Off,
    );
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
        dir.path(),
        StorageConfig {
            durability: DurabilityMode::Memory,
            ..StorageConfig::from_env()
        },
    )
    .unwrap();
    for sql in [
        "CREATE TABLE l(id INT PRIMARY KEY,k INT)",
        "CREATE TABLE r(id INT PRIMARY KEY,k INT, KEY k(k))",
        "INSERT INTO l VALUES(1,1),(2,1),(3,2),(4,9),(5,NULL)",
        "INSERT INTO r VALUES(1,1),(2,1),(3,2),(4,8),(5,NULL)",
    ] {
        engine.execute(sql).unwrap();
    }
    let db = engine.db_for_tests();
    let txn = TxnView::read_only(db.visible_scn(), IsolationLevel::ReadCommitted);
    let table = |name: &str| {
        let schema = db.snapshot_table_schema(&format!("test.{name}")).unwrap();
        PlanTable::new(schema.id, schema, Some(name.into()))
    };
    let left = scan(table("l"), 5.0);
    let right_table = table("r");
    let index = right_table
        .schema
        .indexes
        .iter()
        .find(|i| i.name == "k")
        .unwrap()
        .clone();
    let expr = |sql| crate::parser::parse_expr_fragment(sql).unwrap();
    for null_safe in [false, true] {
        let condition = expr(if null_safe {
            "l.k <=> r.k"
        } else {
            "l.k = r.k"
        });
        for join_type in [
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::Semi,
            JoinType::Anti,
        ] {
            // Both estimates trigger a switch: 5 * 1 fits the NLJ threshold,
            // but 5 * 5 does not; the larger estimate rejects it immediately.
            for estimated_right in [1.0, 100.0] {
                let right = scan(right_table.clone(), estimated_right);
                let left_rows = execute_plan_rows(db, txn, &left).unwrap();
                let right_rows = execute_plan_rows(db, txn, &right).unwrap();
                let expected = execute_nested_loop_join_from_rows(
                    db,
                    txn,
                    &left,
                    &right,
                    join_type,
                    Some(&condition),
                    &left_rows,
                    &right_rows,
                )
                .unwrap();
                for build_side in [BuildSide::Left, BuildSide::Right] {
                    if matches!(join_type, JoinType::Semi | JoinType::Anti)
                        && build_side == BuildSide::Left
                    {
                        continue; // The planner always builds the right side for SEMI/ANTI.
                    }
                    let candidates = vec![
                        AdaptiveJoinCandidate::IndexNestedLoop {
                            right_table: right_table.clone(),
                            right_index: index.clone(),
                            condition: Some(condition.clone()),
                            eq_prefix_len: 1,
                            estimated_rows: 5.0,
                            cost: crate::engine::cbo::Cost::ZERO,
                            right_covering: false,
                            right_projected_columns: None,
                        },
                        AdaptiveJoinCandidate::Hash {
                            left_keys: vec![expr("l.k")],
                            right_keys: vec![expr("r.k")],
                            null_safe: vec![null_safe],
                            build_side,
                            residual: None,
                            estimated_rows: 5.0,
                            cost: crate::engine::cbo::Cost::ZERO,
                        },
                        AdaptiveJoinCandidate::NestedLoop {
                            condition: Some(condition.clone()),
                            estimated_rows: 5.0,
                            cost: crate::engine::cbo::Cost::ZERO,
                        },
                    ];
                    for expression_keys in [false, true] {
                        let mut candidates = candidates.clone();
                        if expression_keys {
                            if let AdaptiveJoinCandidate::Hash {
                                left_keys,
                                right_keys,
                                ..
                            } = &mut candidates[1]
                            {
                                left_keys[0] = expr("l.k + 0");
                                right_keys[0] = expr("r.k + 0");
                            }
                        }
                        for preferred in [
                            AdaptiveJoinChoice::IndexNestedLoop,
                            AdaptiveJoinChoice::NestedLoop,
                        ] {
                            for bloom in ["0", "10000"] {
                                crate::util::env::with_vars(
                                    &[
                                        ("MYDB_ADAPTIVE_JOIN_INLJ_MAX_LEFT_ROWS", Some("1")),
                                        ("MYDB_ADAPTIVE_JOIN_NLJ_MAX_COMPARISONS", Some("5")),
                                        ("MYDB_HASH_JOIN_BLOOM_MAX_BUILD_ROWS", Some(bloom)),
                                        ("MYDB_HASH_JOIN_BLOOM_MIN_BUILD_ROWS", Some("1")),
                                    ],
                                    || {
                                        let (stats, actual) = analyze_stats::with_collector(|| {
                                            execute_adaptive_join(
                                                db,
                                                txn,
                                                &left,
                                                &right,
                                                join_type,
                                                &candidates,
                                                preferred,
                                            )
                                        });
                                        let context = format!("{join_type:?} {build_side:?} {preferred:?} bloom={bloom} null_safe={null_safe} estimate={estimated_right} expressions={expression_keys}");
                                        assert_eq!(
                                            values(&actual.unwrap()),
                                            values(&expected),
                                            "{context}"
                                        );
                                        assert_eq!(
                                            analyze_stats::lookup(&stats, &left)
                                                .expect("left scan statistics")
                                                .loops,
                                            1,
                                            "left reread: {context}"
                                        );
                                    },
                                );
                            }
                        }
                    }
                }
            }
        }
    }
    // No hash candidate: the rejected index candidate must hand its left
    // rows to the ordinary NLJ fallback rather than scanning again.
    let right = scan(right_table.clone(), 5.0);
    let condition = expr("l.k=r.k");
    let candidates = [
        AdaptiveJoinCandidate::IndexNestedLoop {
            right_table,
            right_index: index,
            condition: Some(condition.clone()),
            eq_prefix_len: 1,
            estimated_rows: 5.0,
            cost: crate::engine::cbo::Cost::ZERO,
            right_covering: false,
            right_projected_columns: None,
        },
        AdaptiveJoinCandidate::NestedLoop {
            condition: Some(condition),
            estimated_rows: 5.0,
            cost: crate::engine::cbo::Cost::ZERO,
        },
    ];
    crate::util::env::with_vars(
        &[("MYDB_ADAPTIVE_JOIN_INLJ_MAX_LEFT_ROWS", Some("1"))],
        || {
            let (stats, rows) = analyze_stats::with_collector(|| {
                execute_adaptive_join(
                    db,
                    txn,
                    &left,
                    &right,
                    JoinType::Inner,
                    &candidates,
                    AdaptiveJoinChoice::IndexNestedLoop,
                )
            });
            assert_eq!(rows.unwrap().len(), 5);
            assert_eq!(
                analyze_stats::lookup(&stats, &left)
                    .expect("left scan statistics")
                    .loops,
                1
            );
            assert_eq!(analyze_stats::lookup(&stats, &right).unwrap().loops, 1);
        },
    );
}

#[test]
fn reused_vector_input_preserves_collations_unsigned_decimal_and_payloads() {
    use crate::engine::api::QueryResponse;
    type RowBytes = Vec<Option<Vec<u8>>>;
    fn normalized_rows(rows: impl Iterator<Item = Vec<Value>>) -> Vec<RowBytes> {
        // Compare exact text-protocol values, not storage carriers such as
        // String/SmallString or the different compact decimal variants.
        let mut rows: Vec<RowBytes> = rows
            .map(|row| {
                row.into_iter()
                    .map(|value| match value {
                        Value::Null => None,
                        Value::Bytes(bytes) => Some(bytes),
                        value => Some(value.to_display_string().into_bytes()),
                    })
                    .collect()
            })
            .collect();
        rows.sort();
        rows
    }
    fn query(engine: &StorageQueryEngine, sql: &str) -> Vec<RowBytes> {
        let rows = match engine.execute(sql).unwrap() {
            QueryResponse::ResultSet(result) => result.rows,
            QueryResponse::StreamingResultSet(mut result) => {
                let mut rows = Vec::new();
                while let Some(row) = result.cursor.next_row().unwrap() {
                    rows.push(row);
                }
                rows
            }
            _ => panic!("expected rows"),
        };
        normalized_rows(rows.into_iter())
    }
    crate::util::env::with_vars(
        &[
            ("MYDB_ADAPTIVE_JOIN", Some("1")),
            ("MYDB_VEC_HASH_JOIN", Some("on")),
            ("MYDB_ADAPTIVE_JOIN_INLJ_MAX_LEFT_ROWS", Some("1")),
            ("MYDB_ADAPTIVE_JOIN_NLJ_MAX_COMPARISONS", Some("1")),
            ("MYDB_HASH_JOIN_BLOOM_MAX_BUILD_ROWS", Some("10000")),
            ("MYDB_HASH_JOIN_BLOOM_MIN_BUILD_ROWS", Some("1")),
        ],
        || {
            for (data_type, keys) in [
                (
                    "VARCHAR(30) COLLATE utf8mb4_general_ci",
                    ["'Aa'", "'aA'", "'é'", "NULL", "'e'"],
                ),
                (
                    "BIGINT UNSIGNED",
                    [
                        "18446744073709551615",
                        "9223372036854775808",
                        "1",
                        "NULL",
                        "42",
                    ],
                ),
                (
                    "DECIMAL(18,3)",
                    ["1.001", "1.000", "-0.125", "NULL", "0.000"],
                ),
                ("VARBINARY(30)", ["'a'", "'A'", "X'00FF'", "NULL", "X'00'"]),
                (
                    "DATE",
                    [
                        "'2024-02-29'",
                        "'2023-02-28'",
                        "'2020-01-01'",
                        "NULL",
                        "'2030-12-31'",
                    ],
                ),
            ] {
                let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
                let engine =
                    StorageQueryEngine::open_with_storage_config_without_background_workers(
                        dir.path(),
                        StorageConfig {
                            durability: DurabilityMode::Memory,
                            ..StorageConfig::from_env()
                        },
                    )
                    .unwrap();
                engine
                    .execute(&format!(
                        "CREATE TABLE l(id INT PRIMARY KEY,k {data_type},payload VARBINARY(512))"
                    ))
                    .unwrap();
                engine.execute(&format!("CREATE TABLE r(id INT PRIMARY KEY,k {data_type},payload VARBINARY(512),KEY k(k))")).unwrap();
                for (name, count) in [("l", 5), ("r", 1000)] {
                    let rows = (0..count)
                        .map(|i| format!("({i},{},REPEAT('{name}',129))", keys[i % keys.len()]))
                        .collect::<Vec<_>>()
                        .join(",");
                    engine
                        .execute(&format!("INSERT INTO {name} VALUES {rows}"))
                        .unwrap();
                    engine.execute(&format!("ANALYZE TABLE {name}")).unwrap();
                }
                let expected = query(
                    &engine,
                    "SELECT /*+ NO_HASH_JOIN */ l.*,r.* FROM l JOIN r ON l.k=r.k",
                );
                let db = engine.db_for_tests();
                let txn = TxnView::read_only(db.visible_scn(), IsolationLevel::ReadCommitted);
                let table = |name: &str| {
                    let schema = db.snapshot_table_schema(&format!("test.{name}")).unwrap();
                    PlanTable::new(schema.id, schema, Some(name.into()))
                };
                let left = scan(table("l"), 5.0);
                let right = scan(table("r"), 1000.0);
                let left_rows = execute_plan_rows(db, txn, &left).unwrap();
                let left_keys = [crate::parser::parse_expr_fragment("l.k").unwrap()];
                let right_keys = [crate::parser::parse_expr_fragment("r.k").unwrap()];
                for build_side in [BuildSide::Left, BuildSide::Right] {
                    let before = crate::engine::executor::select::vec_bridge::VEC_HASH_JOIN_HITS
                        .load(Ordering::Relaxed);
                    let actual = execute_adaptive_hash_join(
                        db,
                        txn,
                        &left,
                        &right,
                        JoinType::Inner,
                        &left_keys,
                        &right_keys,
                        &[false],
                        build_side,
                        None,
                        None,
                        Some(left_rows.clone()),
                        AdaptiveJoinChoice::IndexNestedLoop,
                    )
                    .unwrap();
                    let actual = normalized_rows(
                        actual
                            .into_iter()
                            .map(|row| row.fields.into_iter().map(|field| field.value).collect()),
                    );
                    assert_eq!(actual.len(), expected.len(), "{data_type} {build_side:?}");
                    for (actual, expected) in actual.iter().zip(&expected) {
                        assert_eq!(actual, expected, "{data_type} {build_side:?}");
                    }
                    assert!(
                        crate::engine::executor::select::vec_bridge::VEC_HASH_JOIN_HITS
                            .load(Ordering::Relaxed)
                            > before,
                        "vector join not exercised: {data_type} {build_side:?}"
                    );
                }
            }
        },
    );
}
