use super::*;

fn open() -> (tempfile::TempDir, StorageQueryEngine) {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
        dir.path(),
        crate::engine::executor::tests::memory_test_storage_config(),
    )
    .unwrap();
    (dir, engine)
}
fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    match engine.execute(sql).unwrap().materialize().unwrap() {
        QueryResponse::ResultSet(result) => result.rows,
        _ => panic!("result set required: {sql}"),
    }
}
fn setup(engine: &StorageQueryEngine, datatype: &str, algo: &str) {
    engine.execute(&format!("CREATE TABLE items(id INT PRIMARY KEY,tenant INT,body VARCHAR(100),v VECTOR(4,{datatype}) NOT NULL,KEY it(tenant))")).unwrap();
    for start in (0..512).step_by(64) {
        let values = (start..start + 64)
            .map(|id| {
                format!(
                    "({id},{},'text-{id}','[{},{},1,2]')",
                    id % 16,
                    id % 113,
                    id / 113
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO items VALUES {values}"))
            .unwrap();
    }
    let options = if algo == "HNSW" {
        "m=8,ef_construction=64"
    } else {
        "nlist=8,nprobe=8"
    };
    engine
        .execute(&format!(
            "CREATE INDEX vx ON items(v) USING {algo} WITH(metric=l2,{options})"
        ))
        .unwrap();
}

#[test]
fn filtered_vector_exact_matches_relational_oracle_and_materializes_only_winners() {
    let (_dir, engine) = open();
    setup(&engine, "FLOAT32", "HNSW");
    for predicate in [
        "tenant=3",
        "tenant IN(1,3,5)",
        "id BETWEEN 10 AND 90",
        "NOT(tenant=2) AND body LIKE '%3%'",
        "JSON_EXTRACT(JSON_OBJECT('t',tenant),'$.t')=3",
        "tenant=0 OR tenant=15",
        "tenant IS NULL",
        "(tenant,id) IN ((3,3),(3,19),(3,35))",
        "id<0",
    ] {
        let query = format!("SELECT /*+ NO_VECTOR_INDEX */ id FROM items WHERE {predicate} ORDER BY L2_DISTANCE(v,'[3.123,1.019,1,2]') LIMIT 7 OFFSET 2");
        let oracle = query.replace(
            "L2_DISTANCE(v,'[3.123,1.019,1,2]')",
            "(L2_DISTANCE(v,'[3.123,1.019,1,2]')+0)",
        );
        assert_eq!(rows(&engine, &query), rows(&engine, &oracle), "{predicate}");
    }
    let plan = rows(&engine, "EXPLAIN ANALYZE FORMAT=JSON SELECT id FROM items WHERE tenant=3 ORDER BY L2_DISTANCE(v,'[3.123,1.019,1,2]') LIMIT 7");
    let text = plan[0][0].to_display_string();
    assert!(text.contains("filtered_vector_top_k"), "{text}");
    assert!(
        text.contains("\"materialized\":7") || text.contains("\"materialized\": 7"),
        "{text}"
    );
}

#[test]
fn filtered_vector_all_strategies_match_exhaustive_native_search() {
    for datatype in ["FLOAT32", "FLOAT16", "BFLOAT16", "INT8"] {
        for algo in ["HNSW", "IVF"] {
            let (_dir, engine) = open();
            setup(&engine, datatype, algo);
            let db = engine.db_for_tests();
            let Statement::Select(select) = crate::parser::parse_statement("SELECT id FROM items WHERE tenant IN (1,3,5) ORDER BY L2_DISTANCE(v,'[3.123,1.019,1,2]') LIMIT 7").unwrap() else { unreachable!() };
            let plan = crate::planner::plan_select(db, &select).unwrap().physical;
            let mut leaf = None;
            plan.walk(|node| {
                if matches!(node, PhysicalPlan::FilteredVectorTopK { .. }) {
                    leaf = Some(node.clone());
                }
            });
            let leaf = leaf.expect("filtered vector leaf");
            let txn = db.begin_read_only_txn().unwrap();
            let mut expected = None;
            for strategy in [
                crate::planner::FilteredVectorStrategy::Exact,
                crate::planner::FilteredVectorStrategy::PrefilterAnn,
                crate::planner::FilteredVectorStrategy::IterativeAnn,
            ] {
                let mut leaf = leaf.clone();
                if let PhysicalPlan::FilteredVectorTopK {
                    strategy: s,
                    ef_hint,
                    projected_columns,
                    ..
                } = &mut leaf
                {
                    *s = strategy;
                    *ef_hint = Some(if algo == "IVF" { 8 } else { 1024 });
                    *projected_columns = Some(Arc::new(vec![0]));
                }
                let result = execute_plan_rows(db, txn, &leaf).unwrap();
                let ids: Vec<_> = result.iter().map(|r| r.fields[0].value.clone()).collect();
                if let Some(expected) = &expected {
                    assert_eq!(&ids, expected, "{datatype} {algo} {strategy:?}");
                } else {
                    expected = Some(ids);
                }
            }
        }
    }
}

#[test]
fn filtered_vector_nested_index_merge_streams_a_predicate_checked_superset() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use crate::planner::{IndexAccessKind, IndexMergeLeaf, IndexMergeNode};
    use crate::storage::{IndexKey, KeyPart};
    let (_dir, engine) = open();
    setup(&engine, "FLOAT32", "HNSW");
    let sql = "SELECT /*+ NO_VECTOR_INDEX */ id FROM items WHERE (tenant=3 AND id>20) OR tenant=4 OR id<10 ORDER BY L2_DISTANCE(v,'[3.123,1.019,1,2]') LIMIT 7";
    let expected = rows(
        &engine,
        &sql.replace("ORDER BY L2_DISTANCE", "ORDER BY 0+L2_DISTANCE"),
    );
    let Statement::Select(select) = crate::parser::parse_statement(sql).unwrap() else {
        unreachable!()
    };
    let plan = crate::planner::plan_select(engine.db_for_tests(), &select)
        .unwrap()
        .physical;
    let mut leaf = None;
    plan.walk(|p| {
        if matches!(p, PhysicalPlan::FilteredVectorTopK { .. }) {
            leaf = Some(p.clone());
        }
    });
    let mut leaf = leaf.unwrap();
    if let PhysicalPlan::FilteredVectorTopK {
        table,
        scalar_access,
        filter,
        projected_columns,
        parallel_degree,
        ..
    } = &mut leaf
    {
        let index = |name: &str| {
            table
                .schema
                .indexes
                .iter()
                .find(|i| i.name.eq_ignore_ascii_case(name))
                .unwrap()
                .clone()
        };
        let key = |value| IndexKey::from(vec![KeyPart::Int(value)]);
        let source = |name: &str, access| {
            IndexMergeNode::Leaf(IndexMergeLeaf {
                index: index(name),
                access,
                row_id_ordered: false,
            })
        };
        let root = IndexMergeNode::Union {
            sort_union: true,
            children: vec![
                IndexMergeNode::Intersection {
                    children: vec![
                        source(
                            "it",
                            IndexAccessKind::Lookup {
                                key: key(3),
                                exact: true,
                            },
                        ),
                        source(
                            "PRIMARY",
                            IndexAccessKind::Range {
                                low: Some(IndexBound {
                                    key: key(20),
                                    inclusive: false,
                                }),
                                high: None,
                            },
                        ),
                    ],
                },
                source(
                    "it",
                    IndexAccessKind::MultiLookup {
                        keys: vec![key(4)],
                        exact: true,
                    },
                ),
                source(
                    "PRIMARY",
                    IndexAccessKind::Range {
                        low: None,
                        high: Some(IndexBound {
                            key: key(10),
                            inclusive: false,
                        }),
                    },
                ),
            ],
        };
        *scalar_access = Box::new(PhysicalPlan::IndexMerge {
            table: table.clone(),
            root,
            filter: filter.clone(),
            estimated_rows: 100.0,
            access_rows: 100.0,
            cost: Default::default(),
            possible_keys: Vec::new(),
            hint_note: None,
            projected_columns: None,
        });
        *parallel_degree = 1;
        *projected_columns = Some(Arc::new(vec![0]));
    }
    let txn = engine.db_for_tests().begin_read_only_txn().unwrap();
    for limit in [16 * 1024, 64 * 1024] {
        let budget = Arc::new(MemBudget::new(limit));
        let actual = with_query_budget(budget.clone(), || {
            filtered_vector::execute_filtered_vector_topk(engine.db_for_tests(), txn, &leaf)
        })
        .unwrap()
        .into_iter()
        .map(|r| vec![r.fields[0].value.clone()])
        .collect::<Vec<_>>();
        assert_eq!(actual, expected);
        assert_eq!(budget.used(), 0);
        assert!(budget.peak_used() <= limit);
    }
}

#[test]
fn filtered_vector_keeps_volatile_and_relational_boundaries() {
    let (_dir, engine) = open();
    setup(&engine, "FLOAT32", "HNSW");
    for sql in [
        "SELECT id FROM items WHERE RAND()>0.5 ORDER BY L2_DISTANCE(v,'[3.123,1.019,1,2]') LIMIT 7",
        "SELECT DISTINCT tenant,L2_DISTANCE(v,'[3.123,1.019,1,2]') AS d FROM items WHERE tenant=3 ORDER BY d LIMIT 7",
    ] {
        let Statement::Select(select) = crate::parser::parse_statement(sql).unwrap() else { unreachable!() };
        let plan = crate::planner::plan_select(engine.db_for_tests(), &select).unwrap().physical;
        assert!(!plan.any(|node| matches!(node, PhysicalPlan::FilteredVectorTopK { .. })), "{sql}");
    }
    engine
        .execute("CREATE TABLE labels(tenant INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO labels VALUES(3)").unwrap();
    let sql = "SELECT /*+ HNSW_EF(1024) */ i.id FROM items i JOIN labels l ON l.tenant=i.tenant WHERE i.tenant=3 ORDER BY L2_DISTANCE(i.v,'[3.123,1.019,1,2]') LIMIT 7";
    let Statement::Select(select) = crate::parser::parse_statement(sql).unwrap() else {
        unreachable!()
    };
    let plan = crate::planner::plan_select(engine.db_for_tests(), &select)
        .unwrap()
        .physical;
    assert!(!plan.any(|p| matches!(p, PhysicalPlan::FilteredVectorTopK { .. })));
    assert!(
        !plan.any(|p| matches!(p, PhysicalPlan::VectorIndexScan { .. })),
        "joins must retain their complete relational Top-K input"
    );
    assert_eq!(
        rows(&engine, sql),
        rows(
            &engine,
            &sql.replace("ORDER BY L2_DISTANCE", "ORDER BY 0+L2_DISTANCE")
        )
    );
}

#[test]
fn filtered_vector_budget_and_frontier_exhaustion_complete_reranking_exactly() {
    for algo in ["HNSW", "IVF"] {
        let (_dir, engine) = open();
        setup(&engine, "FLOAT32", algo);
        for (predicate, budget) in [("tenant>=0", 16), ("id<10", 0)] {
            let sql = format!("SELECT id FROM items WHERE {predicate} ORDER BY L2_DISTANCE(v,'[3.123,1.019,1,2]') LIMIT 7");
            let oracle = sql.replace("ORDER BY L2_DISTANCE", "ORDER BY 0+L2_DISTANCE");
            let expected = rows(&engine, &oracle);
            let Statement::Select(select) = crate::parser::parse_statement(&sql).unwrap() else {
                unreachable!()
            };
            let plan = crate::planner::plan_select(engine.db_for_tests(), &select)
                .unwrap()
                .physical;
            let mut leaf = None;
            plan.walk(|node| {
                if matches!(node, PhysicalPlan::FilteredVectorTopK { .. }) {
                    leaf = Some(node.clone());
                }
            });
            let mut leaf = leaf.unwrap();
            if let PhysicalPlan::FilteredVectorTopK {
                strategy,
                ef_hint,
                projected_columns,
                ..
            } = &mut leaf
            {
                *strategy = crate::planner::FilteredVectorStrategy::IterativeAnn;
                *ef_hint = Some(if algo == "IVF" { 8 } else { 1024 });
                *projected_columns = Some(Arc::new(vec![0]));
            }
            let mut session = crate::engine::api::SessionContext::default();
            session.sysvars.mydb_hnsw_max_scan_tuples = budget;
            session.sysvars.mydb_ivf_max_scan_tuples = budget;
            let txn = engine.db_for_tests().begin_read_only_txn().unwrap();
            let (measurements, actual) =
                crate::engine::expr::with_session_expr_info(&session, || {
                    super::super::analyze_stats::with_collector(|| {
                        filtered_vector::execute_filtered_vector_topk(
                            engine.db_for_tests(),
                            txn,
                            &leaf,
                        )
                    })
                });
            let actual: Vec<_> = actual
                .unwrap()
                .into_iter()
                .map(|row| vec![row.fields[0].value.clone()])
                .collect();
            assert_eq!(actual, expected, "{algo} {predicate}");
            assert!(
                measurements
                    .get_for_plan(&leaf)
                    .unwrap()
                    .filtered_vector
                    .fallbacks
                    > 0,
                "{algo} {predicate}: an incomplete rerank pool requires exact completion"
            );
        }
    }
}

#[test]
fn filtered_vector_incomplete_navigation_keeps_off_mode_budget_semantics() {
    let (_dir, engine) = open();
    setup(&engine, "FLOAT32", "HNSW");
    let sql =
        "SELECT id FROM items WHERE id=451 ORDER BY L2_DISTANCE(v,'[3.123,1.019,1,2]') LIMIT 7";
    let Statement::Select(select) = crate::parser::parse_statement(sql).unwrap() else {
        unreachable!()
    };
    let plan = crate::planner::plan_select(engine.db_for_tests(), &select)
        .unwrap()
        .physical;
    let mut leaf = None;
    plan.walk(|node| {
        if matches!(node, PhysicalPlan::FilteredVectorTopK { .. }) {
            leaf = Some(node.clone());
        }
    });
    let mut leaf = leaf.unwrap();
    if let PhysicalPlan::FilteredVectorTopK {
        strategy,
        ef_hint,
        projected_columns,
        ..
    } = &mut leaf
    {
        *strategy = crate::planner::FilteredVectorStrategy::PrefilterAnn;
        *ef_hint = Some(1);
        *projected_columns = Some(Arc::new(vec![0]));
    }
    let txn = engine.db_for_tests().begin_read_only_txn().unwrap();
    for mode in ["strict_order", "off"] {
        let mut session = crate::engine::api::SessionContext::default();
        session.sysvars.mydb_hnsw_iterative_scan = mode.into();
        session.sysvars.mydb_hnsw_max_scan_tuples = 7;
        let (stats, actual) = crate::engine::expr::with_session_expr_info(&session, || {
            super::super::analyze_stats::with_collector(|| {
                filtered_vector::execute_filtered_vector_topk(engine.db_for_tests(), txn, &leaf)
            })
        });
        let actual = actual.unwrap();
        assert!(actual
            .iter()
            .all(|row| row.fields[0].value == Value::Int(451)));
        let fallbacks = stats.get_for_plan(&leaf).unwrap().filtered_vector.fallbacks;
        if mode == "off" {
            assert_eq!(fallbacks, 0, "off must preserve bounded ANN completion");
        } else {
            assert_eq!(actual.len(), 1);
            assert!(
                fallbacks > 0,
                "default mode must complete an unfinished navigation radius"
            );
        }
    }
}

#[test]
#[ignore = "local filtered vector performance experiment"]
fn filtered_vector_execution_bench() {
    let (_dir, engine) = open();
    engine.execute("CREATE TABLE perf_items(id INT PRIMARY KEY,tenant INT,body VARCHAR(100),v VECTOR(128) NOT NULL,KEY it(tenant))").unwrap();
    let mut state = 17u64;
    for start in (0..20000).step_by(100) {
        let values = (start..start + 100)
            .map(|id| {
                let vector = (0..128)
                    .map(|_| {
                        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                        format!("{}", ((state >> 33) as i32 % 1000) as f64 / 100.0)
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                format!("({id},{},'text-{id}','[{vector}]')", id % 100)
            })
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO perf_items VALUES {values}"))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE perf_items").unwrap();
    let query = format!(
        "SELECT id FROM perf_items WHERE tenant=3 ORDER BY L2_DISTANCE(v,'[{}]') LIMIT 10",
        vec!["1.234"; 128].join(",")
    );
    let legacy = query.replace("ORDER BY L2_DISTANCE", "ORDER BY 0+L2_DISTANCE");
    assert_eq!(rows(&engine, &query), rows(&engine, &legacy));
    for sql in [&query, &legacy] {
        for _ in 0..5 {
            rows(&engine, sql);
        }
    }
    for round in 0..5 {
        for (label, sql) in if round % 2 == 0 {
            [("fused", &query), ("generic", &legacy)]
        } else {
            [("generic", &legacy), ("fused", &query)]
        } {
            let start = std::time::Instant::now();
            for _ in 0..100 {
                std::hint::black_box(rows(&engine, sql));
            }
            eprintln!(
                "FILTERED_BENCH {label} round={round} us={:.2}",
                start.elapsed().as_secs_f64() * 1e4
            );
        }
    }
    setup(&engine, "FLOAT32", "HNSW");
    let query = "SELECT id FROM items WHERE SHA2(REPEAT(body,1024),256) <> '' ORDER BY L2_DISTANCE(v,'[3.123,1.019,1,2]') LIMIT 10";
    let legacy = query.replace("ORDER BY L2_DISTANCE", "ORDER BY 0+L2_DISTANCE");
    assert_eq!(rows(&engine, query), rows(&engine, &legacy));
    eprintln!(
        "EXPENSIVE_PLAN {:?}",
        rows(&engine, &format!("EXPLAIN ANALYZE FORMAT=JSON {query}"))
    );
    for round in 0..5 {
        for (label, sql) in if round % 2 == 0 {
            [("iterative", query), ("generic", legacy.as_str())]
        } else {
            [("generic", legacy.as_str()), ("iterative", query)]
        } {
            let start = std::time::Instant::now();
            for _ in 0..30 {
                std::hint::black_box(rows(&engine, sql));
            }
            eprintln!(
                "EXPENSIVE_BENCH {label} round={round} us={:.2}",
                start.elapsed().as_secs_f64() * 1e6 / 30.0
            );
        }
    }
}

#[test]
fn filtered_vector_partition_pruning_parallel_merge_and_snapshots_match_oracle() {
    let (_dir, engine) = open();
    engine.execute("CREATE TABLE partitioned_items(id INT PRIMARY KEY,tenant INT,v VECTOR(4) NOT NULL) PARTITION BY RANGE(id)(PARTITION p0 VALUES LESS THAN(128),PARTITION p1 VALUES LESS THAN(256),PARTITION p2 VALUES LESS THAN MAXVALUE)").unwrap();
    for start in (0..384).step_by(64) {
        let values = (start..start + 64)
            .map(|i| format!("({i},{},'[{},{},1,2]')", i % 7, i % 113, i / 113))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO partitioned_items VALUES {values}"))
            .unwrap();
    }
    for predicate in ["tenant>=0", "tenant=3", "id>=128 AND id<256", "id<0"] {
        let sql=format!("SELECT id FROM partitioned_items WHERE {predicate} ORDER BY L2_DISTANCE(v,'[3.123,1.019,1,2]') LIMIT 11");
        let oracle = sql.replace("ORDER BY L2_DISTANCE", "ORDER BY 0+L2_DISTANCE");
        assert_eq!(rows(&engine, &sql), rows(&engine, &oracle));
        let Statement::Select(select) = crate::parser::parse_statement(&sql).unwrap() else {
            unreachable!()
        };
        let mut plan = crate::planner::plan_select(engine.db_for_tests(), &select)
            .unwrap()
            .physical;
        plan.walk_mut(|p| {
            if let PhysicalPlan::FilteredVectorTopK {
                parallel_degree, ..
            } = p
            {
                *parallel_degree = 2;
            }
        });
        let mut txn = engine.db_for_tests().begin_read_only_txn().unwrap();
        txn.historical = true;
        let output = execute_plan_rows(engine.db_for_tests(), txn, &plan).unwrap();
        let ids: Vec<_> = output
            .iter()
            .map(|r| vec![r.fields[0].value.clone()])
            .collect();
        assert_eq!(ids, rows(&engine, &oracle));
    }
}

#[test]
fn filtered_vector_quantized_metrics_and_binary_filters_match_exact_distances() {
    for algo in ["HNSW", "IVF"] {
        for metric in ["l2", "cosine", "dot"] {
            for quantization in ["none", "sq8", "sq4", "pq", "bq"] {
                let (_dir, engine) = open();
                setup(&engine, "FLOAT32", algo);
                engine.execute("DROP INDEX vx ON items").unwrap();
                let algorithm = if algo == "HNSW" {
                    "m=8,ef_construction=64"
                } else {
                    "nlist=8,nprobe=8"
                };
                let pq = if quantization == "pq" {
                    ",pq_m=2,pq_bits=4,pq_train_samples=64,pq_iterations=3"
                } else {
                    ""
                };
                engine.execute(&format!("CREATE INDEX vx ON items(v) USING {algo} WITH(metric={metric},quantization={quantization},rerank_mult=128,{algorithm}{pq})")).unwrap();
                let function = match metric {
                    "l2" => "L2_DISTANCE",
                    "cosine" => "COSINE_DISTANCE",
                    _ => "MAX_INNER_PRODUCT_DISTANCE",
                };
                let hint = if algo == "HNSW" {
                    "HNSW_EF(1024)"
                } else {
                    "IVF_NPROBE(8)"
                };
                let sql=format!("SELECT id FROM items WHERE tenant IN(1,3,5) ORDER BY {function}(v,'[3.123,1.019,1,2]') LIMIT 7");
                let sql = sql.replacen("SELECT", &format!("SELECT /*+ {hint} */"), 1);
                let oracle = sql.replace(
                    &format!("ORDER BY {function}"),
                    &format!("ORDER BY 0+{function}"),
                );
                assert_eq!(
                    rows(&engine, &sql),
                    rows(&engine, &oracle),
                    "{algo} {metric} {quantization}"
                );
            }
        }
        let (_dir, engine) = open();
        engine
            .execute("CREATE TABLE bits(id INT PRIMARY KEY,tenant INT,v VECTOR(9,BINARY) NOT NULL)")
            .unwrap();
        let values = (0..128)
            .map(|i| {
                format!(
                    "({i},{},'[{}]')",
                    i % 3,
                    (0..9)
                        .map(|b| ((i >> b) & 1).to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO bits VALUES {values}"))
            .unwrap();
        for metric in ["hamming", "jaccard"] {
            engine
                .execute(&format!(
                    "CREATE INDEX vx ON bits(v) USING {algo} WITH(metric={metric},rerank_mult=128)"
                ))
                .unwrap();
            let function = if metric == "hamming" {
                "HAMMING_DISTANCE"
            } else {
                "JACCARD_DISTANCE"
            };
            let sql=format!("SELECT {function}(v,TO_VECTOR('[1,0,1,0,1,0,1,0,1]','BINARY')) AS d FROM bits WHERE tenant=1 ORDER BY d LIMIT 7");
            let oracle = sql.replace(
                "ORDER BY d",
                &format!("ORDER BY 0+{function}(v,TO_VECTOR('[1,0,1,0,1,0,1,0,1]','BINARY'))"),
            );
            assert_eq!(rows(&engine, &sql), rows(&engine, &oracle));
            engine.execute("DROP INDEX vx ON bits").unwrap();
        }
    }
}

#[test]
#[ignore = "local filtered ANN recall calibration"]
fn filtered_vector_recall_calibration() {
    let (_dir, engine) = open();
    engine.execute("CREATE TABLE recall_items(id INT PRIMARY KEY,tenant INT,v VECTOR(16) NOT NULL,KEY it(tenant))").unwrap();
    let mut state = 83u64;
    let mut vector = || {
        (0..16)
            .map(|_| {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                format!("{}", ((state >> 33) % 10000) as f64 / 1000.0)
            })
            .collect::<Vec<_>>()
            .join(",")
    };
    for start in (0..4096).step_by(128) {
        let values = (start..start + 128)
            .map(|id| format!("({id},{},'[{}]')", id % 1000, vector()))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO recall_items VALUES {values}"))
            .unwrap();
    }
    engine.execute("CREATE INDEX vx ON recall_items(v) USING HNSW WITH(metric=l2,m=16,ef_construction=128)").unwrap();
    engine.execute("ANALYZE TABLE recall_items").unwrap();
    for threshold in [10, 100, 900, 1000] {
        let (mut hits, mut total) = (0usize, 0usize);
        for _ in 0..64 {
            let probe = vector();
            let sql=format!("SELECT /*+ HNSW_EF(256) */ id FROM recall_items WHERE tenant<{threshold} ORDER BY L2_DISTANCE(v,'[{probe}]') LIMIT 10");
            let oracle = sql.replace("ORDER BY L2_DISTANCE", "ORDER BY 0+L2_DISTANCE");
            let actual = rows(&engine, &sql);
            let expected = rows(&engine, &oracle);
            assert_eq!(actual.len(), expected.len());
            hits += actual.iter().filter(|r| expected.contains(r)).count();
            total += expected.len();
        }
        let recall = hits as f64 / total as f64;
        eprintln!(
            "FILTERED_RECALL threshold={threshold} hits={hits} total={total} recall={recall:.6}"
        );
        assert!(recall >= 0.99, "threshold={threshold} recall={recall}");
    }
}

#[test]
fn filtered_vector_historical_completion_does_not_depend_on_current_scalar_keys() {
    let (_dir, engine) = open();
    setup(&engine, "FLOAT32", "HNSW");
    let sql =
        "SELECT id FROM items WHERE tenant=3 ORDER BY L2_DISTANCE(v,'[3.123,1.019,1,2]') LIMIT 7";
    let expected = rows(&engine, sql);
    let db = engine.db_for_tests();
    let mut txn = db.begin_read_only_txn().unwrap();
    let _snapshot = db.register_statement_snapshot(txn.snapshot_scn);
    let Statement::Select(select) = crate::parser::parse_statement(sql).unwrap() else {
        unreachable!()
    };
    let mut plan = crate::planner::plan_select(db, &select).unwrap().physical;
    plan.walk_mut(|p| {
        if let PhysicalPlan::FilteredVectorTopK { strategy, .. } = p {
            *strategy = crate::planner::FilteredVectorStrategy::PrefilterAnn;
        }
    });
    engine
        .execute("UPDATE items SET tenant=99,v='[1000,1000,1,2]' WHERE tenant=3")
        .unwrap();
    assert!(rows(&engine, sql).is_empty());
    txn.historical = true;
    let result = execute_plan_rows(db, txn, &plan).unwrap();
    let actual: Vec<_> = result
        .iter()
        .map(|r| vec![r.fields[0].value.clone()])
        .collect();
    assert_eq!(actual, expected);
}

#[test]
fn filtered_vector_rebuild_after_snapshot_check_preserves_visible_ranking() {
    let (_dir, engine) = open();
    let engine = Arc::new(engine);
    setup(&engine, "FLOAT32", "HNSW");
    let sql =
        "SELECT id FROM items WHERE tenant IN(1,3,5) ORDER BY L2_DISTANCE(v,'[3.123,1.019,1,2]') LIMIT 7";
    let expected = rows(
        &engine,
        &sql.replace("ORDER BY L2_DISTANCE", "ORDER BY 0+L2_DISTANCE"),
    );
    let db = engine.db_for_tests();
    let txn = db.begin_read_only_txn().unwrap();
    let _snapshot = db.register_statement_snapshot(txn.snapshot_scn);
    assert!(!db.vector_snapshot_requires_exact(txn));
    let Statement::Select(select) = crate::parser::parse_statement(sql).unwrap() else {
        unreachable!()
    };
    let plan = crate::planner::plan_select(db, &select).unwrap().physical;
    let mut leaf = None;
    plan.walk(|p| {
        if matches!(p, PhysicalPlan::FilteredVectorTopK { .. }) {
            leaf = Some(p.clone());
        }
    });
    let mut leaf = leaf.unwrap();
    if let PhysicalPlan::FilteredVectorTopK {
        strategy,
        projected_columns,
        ef_hint,
        ..
    } = &mut leaf
    {
        *strategy = crate::planner::FilteredVectorStrategy::PrefilterAnn;
        *projected_columns = Some(Arc::new(vec![0]));
        *ef_hint = Some(1024);
    }
    let writer = engine.clone();
    filtered_vector::AFTER_SNAPSHOT_CHECK.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            writer
                .execute("UPDATE items SET tenant=99,v='[1000,1000,1,2]' WHERE tenant=3")
                .unwrap();
            let schema = writer.db_for_tests().table("test.items").unwrap();
            let index = schema.indexes.iter().find(|i| i.name == "vx").unwrap();
            writer
                .db_for_tests()
                .rebuild_vector_for_test(&schema, index, schema.id)
                .unwrap();
            assert!(writer.db_for_tests().vector_snapshot_requires_exact(txn));
        }));
    });
    let actual = filtered_vector::execute_filtered_vector_topk(db, txn, &leaf)
        .unwrap()
        .into_iter()
        .map(|row| vec![row.fields[0].value.clone()])
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

#[test]
#[ignore = "local filtered exact cold/warm/concurrent performance matrix"]
fn filtered_vector_exact_performance_matrix() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    for n in [2048, 8192] {
        for dim in [16, 128, 768] {
            let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
            let mut config = crate::engine::executor::tests::memory_test_storage_config();
            config.durability = crate::storage::DurabilityMode::Strict;
            config.buffer_pool_pages = 4096;
            let open = || {
                StorageQueryEngine::open_with_storage_config_without_background_workers(
                    dir.path(),
                    config.clone(),
                )
                .unwrap()
            };
            let engine = open();
            engine.execute(&format!("CREATE TABLE matrix_items(id INT PRIMARY KEY,tenant INT,v VECTOR({dim}) NOT NULL,KEY it(tenant))")).unwrap();
            let mut state = 19u64;
            for start in (0..n).step_by(64) {
                let values = (start..start + 64)
                    .map(|i| {
                        let vector = (0..dim)
                            .map(|_| {
                                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                                format!("{}", (state >> 33) % 10000)
                            })
                            .collect::<Vec<_>>()
                            .join(",");
                        format!("({},{},'[{vector}]')", (i * 7919) % n, (i * 13) % 1000)
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                engine
                    .execute(&format!("INSERT INTO matrix_items VALUES {values}"))
                    .unwrap();
            }
            engine.execute("ANALYZE TABLE matrix_items").unwrap();
            engine.db_for_tests().checkpoint().unwrap();
            drop(engine);
            for cut in [10, 100, 900] {
                for k in [10, 100] {
                    let vector = vec!["4321.123"; dim].join(",");
                    let fused=format!("SELECT id FROM matrix_items WHERE tenant<{cut} ORDER BY L2_DISTANCE(v,'[{vector}]') LIMIT {k}");
                    let generic = fused.replace("ORDER BY L2_DISTANCE", "ORDER BY 0+L2_DISTANCE");
                    let mut expected = None;
                    for cold_round in 0..5 {
                        let pairs = if cold_round % 2 == 0 {
                            [("fused", &fused), ("generic", &generic)]
                        } else {
                            [("generic", &generic), ("fused", &fused)]
                        };
                        for (mode, sql) in pairs {
                            let engine = open();
                            let start = std::time::Instant::now();
                            let actual = rows(&engine, sql);
                            eprintln!("FILTER_MATRIX n={n} dim={dim} cut={cut} k={k} phase=buffer_cold round={cold_round} mode={mode} us={:.3}",start.elapsed().as_secs_f64()*1e6);
                            if let Some(expected) = &expected {
                                assert_eq!(&actual, expected);
                            } else {
                                expected = Some(actual);
                            }
                        }
                    }
                    let engine = open();
                    for sql in [&fused, &generic] {
                        for _ in 0..5 {
                            rows(&engine, sql);
                        }
                    }
                    for workers in [1, 4] {
                        for round in 0..3 {
                            let pairs = if round % 2 == 0 {
                                [("fused", &fused), ("generic", &generic)]
                            } else {
                                [("generic", &generic), ("fused", &fused)]
                            };
                            for (mode, sql) in pairs {
                                let start = std::time::Instant::now();
                                if workers == 1 {
                                    for _ in 0..20 {
                                        std::hint::black_box(rows(&engine, sql));
                                    }
                                } else {
                                    std::thread::scope(|scope| {
                                        let handles: Vec<_> = (0..workers)
                                            .map(|_| {
                                                scope.spawn(|| {
                                                    for _ in 0..20 {
                                                        std::hint::black_box(rows(&engine, sql));
                                                    }
                                                })
                                            })
                                            .collect();
                                        for handle in handles {
                                            handle.join().unwrap();
                                        }
                                    });
                                }
                                eprintln!("FILTER_MATRIX n={n} dim={dim} cut={cut} k={k} phase=warm workers={workers} round={round} mode={mode} us={:.3}",start.elapsed().as_secs_f64()*1e6/(workers*20) as f64);
                            }
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn filtered_vector_optional_selection_respects_small_query_budgets() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    let (_dir, engine) = open();
    setup(&engine, "FLOAT32", "HNSW");
    let sql="SELECT /*+ NO_VECTOR_INDEX */ id FROM items WHERE tenant>=0 ORDER BY L2_DISTANCE(v,'[3.123,1.019,1,2]') LIMIT 7";
    let expected = rows(&engine, sql);
    let Statement::Select(select) = crate::parser::parse_statement(sql).unwrap() else {
        unreachable!()
    };
    let plan = crate::planner::plan_select(engine.db_for_tests(), &select)
        .unwrap()
        .physical;
    let mut leaf = None;
    plan.walk(|node| {
        if matches!(node, PhysicalPlan::FilteredVectorTopK { .. }) {
            leaf = Some(node.clone());
        }
    });
    let mut leaf = leaf.unwrap();
    if let PhysicalPlan::FilteredVectorTopK {
        parallel_degree,
        projected_columns,
        ..
    } = &mut leaf
    {
        *parallel_degree = 4;
        *projected_columns = Some(Arc::new(vec![0]));
    }
    let txn = engine.db_for_tests().begin_read_only_txn().unwrap();
    for limit in [8 * 1024, 24 * 1024, 32 * 1024, 64 * 1024] {
        let budget = Arc::new(MemBudget::new(limit));
        let (measurements, result) = with_query_budget(budget.clone(), || {
            super::super::analyze_stats::with_collector(|| {
                filtered_vector::execute_filtered_vector_topk(engine.db_for_tests(), txn, &leaf)
            })
        });
        let result = result.unwrap();
        if limit == 8 * 1024 {
            assert!(
                measurements
                    .get_for_plan(&leaf)
                    .unwrap()
                    .filtered_vector
                    .fallbacks
                    > 0,
                "optional selection must give way to bounded streaming"
            );
        }
        let actual: Vec<_> = result
            .iter()
            .map(|r| vec![r.fields[0].value.clone()])
            .collect();
        assert_eq!(actual, expected, "limit={limit}");
        assert_eq!(budget.used(), 0, "workspace reservations must be released");
        assert!(budget.peak_used() > 0);
        assert!(budget.peak_used() <= budget.limit());
    }
}

#[test]
fn filtered_vector_fused_index_pipeline_scores_safe_residuals_in_parallel() {
    let (_dir, engine) = open();
    setup(&engine, "FLOAT32", "HNSW");
    let sql = "SELECT /*+ NO_VECTOR_INDEX */ id FROM items FORCE INDEX(PRIMARY) WHERE id>=20 AND ABS(tenant-7)<3 ORDER BY L2_DISTANCE(v,'[3.123,1.019,1,2]') LIMIT 11 OFFSET 2";
    let oracle = sql.replace("ORDER BY L2_DISTANCE", "ORDER BY 0+L2_DISTANCE");
    assert_eq!(rows(&engine, sql), rows(&engine, &oracle));
    let plan = rows(&engine, &format!("EXPLAIN ANALYZE FORMAT=JSON {sql}"));
    let json: serde_json::Value = serde_json::from_str(&plan[0][0].to_display_string()).unwrap();
    fn work(node: &serde_json::Value) -> Option<&serde_json::Value> {
        if let Some(value) = node.get("filtered_vector_work") {
            return Some(value);
        }
        match node {
            serde_json::Value::Object(values) => values.values().find_map(work),
            serde_json::Value::Array(values) => values.iter().find_map(work),
            _ => None,
        }
    }
    let counters = work(&json).expect("filtered exact work");
    assert!(counters["batches"].as_u64().unwrap() > 0, "{json}");
    assert!(
        counters["predicate_evaluations"].as_u64().unwrap() >= 492,
        "{json}"
    );
    assert!(counters["heap_reranks"].as_u64().unwrap() < 492, "{json}");
    assert_eq!(counters["materialized"].as_u64().unwrap(), 13);
}

#[test]
fn filtered_vector_fused_partial_resident_retry_evaluates_each_row_once() {
    let (_dir, engine) = open();
    engine
        .execute("CREATE TABLE retry_score(id INT PRIMARY KEY,v VECTOR(3) NOT NULL)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO retry_score VALUES(0,'[1,2,3]'),(1,'[2,3,4]'),(2,'[3,4,5]'),(3,'[4,5,6]')",
        )
        .unwrap();
    engine
        .execute("ALTER TABLE retry_score ADD COLUMN extra INT DEFAULT 7")
        .unwrap();
    engine
        .execute("UPDATE retry_score SET extra=8 WHERE id<3")
        .unwrap();
    let db = engine.db_for_tests();
    let schema = db.table("retry_score").unwrap();
    let index = schema
        .indexes
        .iter()
        .find(|index| index.name.eq_ignore_ascii_case("PRIMARY"))
        .unwrap();
    let txn = db.begin_read_only_txn().unwrap();
    let mut cursor = db
        .range_index_snapshot_cursor(&schema, index, None, None, txn)
        .unwrap();
    let mut ids = Vec::new();
    loop {
        let batch = cursor.next_chunk_row_ids(64).unwrap();
        if batch.is_empty() {
            break;
        }
        ids.extend(batch);
    }
    ids.sort_unstable();
    assert_eq!(ids.len(), 4);
    let _scratch = crate::engine::vector::workspace::VectorQueryCharge::try_new(64 * 1024).unwrap();
    let mut counts = [0; 4];
    let query = crate::engine::vector::codec::from_f32(&[0.0, 1.0, 2.0]);
    crate::engine::api::clear_statement_warnings();
    let scores = db
        .score_filtered_vector_rows_many_with_handle(
            &db.cached_heap_handle(schema.id).unwrap(),
            &schema,
            &ids,
            txn,
            schema.column_index("v").unwrap(),
            &query,
            crate::engine::vector::Metric::L2,
            None,
            None,
            &[schema.column_index("id").unwrap()],
            |values| {
                let Value::Int(id) = values[0] else {
                    panic!("integer id")
                };
                counts[id as usize] += 1;
                crate::engine::api::push_statement_warning(
                    crate::error::ConditionLevel::Warning,
                    crate::error::catalog::ER_TRUNCATED_WRONG_VALUE,
                    format!("row {id}"),
                );
                Ok(id != 1)
            },
        )
        .unwrap();
    assert_eq!(counts, [1, 1, 1, 1]);
    assert_eq!(crate::engine::api::take_statement_warnings().len(), 4);
    assert_eq!(scores.len(), 3);
    // Scoring errors must be deferred until after the predicate accepts a
    // row, including when an old-layout window uses the general reader.
    let heap = db.cached_heap_handle(schema.id).unwrap();
    let column = schema.column_index("v").unwrap();
    let columns = [schema.column_index("id").unwrap()];
    let decoded = [0.0, 1.0, 2.0];
    let cosine = crate::engine::vector::distance::PreparedF32Distance::new(
        &decoded,
        crate::engine::vector::Metric::Cosine,
    );
    let mut counts = [0; 4];
    let actual = db
        .score_filtered_vector_rows_many_with_handle(
            &heap,
            &schema,
            &ids,
            txn,
            column,
            &query,
            crate::engine::vector::Metric::Cosine,
            None,
            Some(&cosine),
            &columns,
            |values| {
                let Value::Int(id) = values[0] else {
                    panic!("integer id")
                };
                counts[id as usize] += 1;
                Ok(id != 1)
            },
        )
        .unwrap();
    assert_eq!(counts, [1, 1, 1, 1]);
    assert_eq!(actual.len(), 3);
    let survivors = actual.iter().map(|(id, _)| *id).collect::<Vec<_>>();
    let expected = db
        .score_vector_rows_many_with_handle(
            &heap,
            &schema,
            &survivors,
            txn,
            column,
            &query,
            crate::engine::vector::Metric::Cosine,
            None,
        )
        .unwrap();
    for ((id, score), (expected_id, expected_score, _)) in actual.iter().zip(expected) {
        assert_eq!(*id, expected_id);
        assert_eq!(score.to_bits(), expected_score.to_bits());
    }
    assert!(db
        .score_filtered_vector_rows_many_with_handle(
            &heap,
            &schema,
            &ids,
            txn,
            column,
            &query,
            crate::engine::vector::Metric::Hamming,
            None,
            None,
            &columns,
            |_| Ok(false),
        )
        .unwrap()
        .is_empty());
    assert!(db
        .score_filtered_vector_rows_many_with_handle(
            &heap,
            &schema,
            &ids,
            txn,
            column,
            &query,
            crate::engine::vector::Metric::Hamming,
            None,
            None,
            &columns,
            |_| Ok(true),
        )
        .is_err());
}

#[test]
fn filtered_vector_exact_respects_session_workers_in_fused_and_budget_fallback() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use crate::engine::executor::tuning::{enter, ExecutorTuningSnapshot};
    let (_dir, engine) = open();
    setup(&engine, "FLOAT32", "HNSW");
    let sql = "SELECT /*+ NO_VECTOR_INDEX */ id FROM items FORCE INDEX(PRIMARY) WHERE id>=20 ORDER BY L2_DISTANCE(v,'[3.123,1.019,1,2]') LIMIT 7";
    let expected = rows(&engine, sql);
    let Statement::Select(select) = crate::parser::parse_statement(sql).unwrap() else {
        unreachable!()
    };
    let db = engine.db_for_tests();
    let plan = crate::planner::plan_select(db, &select).unwrap().physical;
    let mut leaf = None;
    plan.walk(|node| {
        if matches!(node, PhysicalPlan::FilteredVectorTopK { .. }) {
            leaf = Some(node.clone());
        }
    });
    let mut leaf = leaf.unwrap();
    if let PhysicalPlan::FilteredVectorTopK {
        parallel_degree,
        projected_columns,
        ..
    } = &mut leaf
    {
        // A cached plan can request more workers than the current session.
        *parallel_degree = 4;
        *projected_columns = Some(Arc::new(vec![0]));
    }
    let txn = db.begin_read_only_txn().unwrap();
    for limit in [8 * 1024 * 1024, 128 * 1024] {
        for cap in [4, 1, 2] {
            let budget = Arc::new(MemBudget::new(limit));
            let _tuning = enter(ExecutorTuningSnapshot::capture_for_session(cap));
            let (measurements, result) = with_query_budget(budget.clone(), || {
                super::super::analyze_stats::with_collector(|| {
                    filtered_vector::execute_filtered_vector_topk(db, txn, &leaf)
                })
            });
            let actual = result
                .unwrap()
                .iter()
                .map(|row| vec![row.fields[0].value.clone()])
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
            let workers = measurements.get_for_plan(&leaf).unwrap().workers;
            assert_eq!(workers, cap.min(2) as u32, "limit={limit} cap={cap}");
            assert_eq!(budget.used(), 0);
            assert!(budget.peak_used() <= limit);
        }
    }
}

#[test]
fn filtered_vector_exact_parallel_hints_and_disable_are_honored() {
    struct RestoreCost(f64);
    impl Drop for RestoreCost {
        fn drop(&mut self) {
            crate::engine::cbo::consts::set_variable("cbo_parallel_setup_cost", self.0).unwrap();
        }
    }
    let _restore = RestoreCost(crate::engine::cbo::cbo_consts().parallel_setup_cost());
    crate::engine::cbo::consts::set_variable("cbo_parallel_setup_cost", 0.0).unwrap();
    crate::util::env::with_vars(
        &[
            ("MYDB_PARALLEL_DEGREE", Some("4")),
            ("MYDB_PARALLEL_CPU_BOUND_WORKERS", Some("4")),
            ("MYDB_PARALLEL_DISABLE", Some("0")),
        ],
        || {
            let (_dir, engine) = open();
            setup(&engine, "FLOAT32", "HNSW");
            let degree = |extra: &str| {
                let sql = format!("SELECT /*+ NO_VECTOR_INDEX {extra} */ id FROM items FORCE INDEX(PRIMARY) WHERE id>=20 ORDER BY L2_DISTANCE(v,'[3,1,1,2]') LIMIT 7");
                let Statement::Select(select) = crate::parser::parse_statement(&sql).unwrap()
                else {
                    unreachable!()
                };
                let plan = crate::planner::plan_select(engine.db_for_tests(), &select)
                    .unwrap()
                    .physical;
                let mut degree = None;
                plan.walk(|node| {
                    if let PhysicalPlan::FilteredVectorTopK {
                        parallel_degree, ..
                    } = node
                    {
                        degree = Some(*parallel_degree);
                    }
                });
                degree.expect("filtered vector plan")
            };
            assert_eq!(degree(""), 4);
            assert_eq!(degree("NO_PARALLEL"), 1);
            assert_eq!(degree("PARALLEL(2)"), 2);
            assert_eq!(degree("PARALLEL(4) NO_PARALLEL"), 1);
            crate::util::env::with_vars(&[("MYDB_PARALLEL_DISABLE", Some("1"))], || {
                assert_eq!(degree(""), 1);
                assert_eq!(degree("PARALLEL(4)"), 1);
            });
        },
    );
}
