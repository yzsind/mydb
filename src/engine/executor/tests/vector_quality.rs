//! Native-vector SQL boundary, atomicity and prepared-execution regressions.
use super::*;

fn vector_filter_cost_fixture(
    algorithm: &str,
) -> (tempfile::TempDir, StorageQueryEngine, SessionContext) {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE vector_cost(id INT PRIMARY KEY,bucket INT NOT NULL,v VECTOR(3) NOT NULL)",
        )
        .unwrap();
    let rows = (0..4096)
        .map(|i| {
            format!(
                "({i},{},'[{}, {}, {}]')",
                (i * 7919) % 4096,
                i,
                i % 7,
                i % 11
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO vector_cost VALUES {rows}"))
        .unwrap();
    let options = if algorithm == "ivf" {
        ",nlist=64"
    } else {
        ",m=4,ef_construction=32"
    };
    engine
        .execute(&format!(
            "CREATE INDEX ann ON vector_cost(v) USING {algorithm} WITH(metric=l2{options})"
        ))
        .unwrap();
    engine.execute("ANALYZE TABLE vector_cost").unwrap();
    let mut session = SessionContext::default();
    for sql in [
        "SET SESSION mydb_hnsw_ef_search=16",
        "SET SESSION mydb_ivf_nprobe=4",
        "SET SESSION mydb_hnsw_ef_dynamic_factor=0",
        "SET SESSION mydb_hnsw_brute_threshold=1",
        "SET SESSION mydb_hnsw_brute_filter_rows=0",
    ] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
    (dir, engine, session)
}

#[test]
fn vector_filter_survival_cost_requires_a_covered_prefilter_access() {
    use crate::engine::api::QueryEngine as _;
    use crate::planner::PhysicalPlan;
    let (_dir, engine, mut session) = vector_filter_cost_fixture("hnsw");
    let sql =
        "SELECT id FROM vector_cost WHERE bucket>=? ORDER BY L2_DISTANCE(v,TO_VECTOR(?)) LIMIT 10";
    let parsed = engine.prepare_cached(&mut session, sql, 2).unwrap();
    let params = vec![Value::Int(3584), Value::String("[17.1,2.1,3.1]".into())];
    for indexed in [false, true] {
        if indexed {
            engine
                .execute("CREATE INDEX bucket_idx ON vector_cost(bucket)")
                .unwrap();
        }
        let memo = crate::engine::expr::with_session_expr_info(&session, || {
            crate::engine::executor::compiled_select::compile_prepared_select(
                &engine.db,
                &session,
                &parsed,
                &params,
                engine.plan_cache(),
            )
        })
        .unwrap();
        assert_eq!(
            memo.compiled
                .physical
                .any(|p| matches!(p, PhysicalPlan::VectorIndexScan { .. })),
            !indexed,
            "do not treat an unindexed residual as a free prefilter"
        );
    }
}

#[test]
fn vector_prepared_filter_cost_accounts_for_candidate_survival() {
    use crate::engine::api::{PreparedSelectMemoState, QueryEngine as _};
    use crate::planner::PhysicalPlan;
    for algorithm in ["hnsw", "ivf"] {
        let (_dir, engine, mut session) = vector_filter_cost_fixture(algorithm);
        let sql =
            "SELECT id FROM vector_cost WHERE id>=? ORDER BY L2_DISTANCE(v,TO_VECTOR(?)) LIMIT 10";
        let parsed = engine.prepare_cached(&mut session, sql, 2).unwrap();
        let strong = if algorithm == "ivf" { 3768 } else { 3584 };
        for (lower, ann) in [(0, true), (512, true), (strong, false)] {
            let params = vec![Value::Int(lower), Value::String("[17.1,2.1,3.1]".into())];
            let memo = crate::engine::expr::with_session_expr_info(&session, || {
                crate::engine::executor::compiled_select::compile_prepared_select(
                    &engine.db,
                    &session,
                    &parsed,
                    &params,
                    engine.plan_cache(),
                )
            })
            .unwrap();
            assert_eq!(
                memo.compiled
                    .physical
                    .any(|p| matches!(p, PhysicalPlan::VectorIndexScan { .. })),
                ann,
                "{algorithm} filter lower={lower} must cost surviving candidates"
            );
            if !ann {
                let mut state = PreparedSelectMemoState::Ready(memo.into());
                let result = engine
                    .execute_prepared_select(&mut session, &mut state, &parsed, sql, &params)
                    .unwrap()
                    .unwrap()
                    .materialize()
                    .unwrap();
                let QueryResponse::ResultSet(actual) = result else {
                    panic!("rows required")
                };
                let expected = query_rows(&engine, &format!("SELECT /*+ NO_VECTOR_INDEX */ id FROM vector_cost WHERE id>={lower} ORDER BY L2_DISTANCE(v,TO_VECTOR('[17.1,2.1,3.1]')) LIMIT 10"));
                assert_eq!(actual.rows, expected);
            }
        }
        engine
            .execute_with_session(
                &mut session,
                &format!("SET SESSION mydb_{algorithm}_iterative_scan='off'"),
            )
            .unwrap();
        let params = vec![Value::Int(strong), Value::String("[17.1,2.1,3.1]".into())];
        let memo = crate::engine::expr::with_session_expr_info(&session, || {
            crate::engine::executor::compiled_select::compile_prepared_select(
                &engine.db,
                &session,
                &parsed,
                &params,
                engine.plan_cache(),
            )
        })
        .unwrap();
        assert!(
            memo.compiled
                .physical
                .any(|p| matches!(p, PhysicalPlan::VectorIndexScan { .. })),
            "{algorithm} with widening disabled must not pay for additional candidates"
        );
    }
}

#[test]
fn vector_prepared_cost_replans_when_session_effort_changes() {
    use crate::engine::api::{PreparedSelectMemoState, QueryEngine as _};
    use crate::planner::PhysicalPlan;
    for algorithm in ["hnsw", "ivf"] {
        let (_dir, engine, mut session) = vector_filter_cost_fixture(algorithm);
        let sql = "SELECT id FROM vector_cost WHERE id>=512 ORDER BY L2_DISTANCE(v,TO_VECTOR(?)) LIMIT 10";
        let parsed = engine.prepare_cached(&mut session, sql, 1).unwrap();
        let params = vec![Value::String("[17.1,2.1,3.1]".into())];
        let mut state = PreparedSelectMemoState::default();
        let mut plans = Vec::new();
        let (variable, low, high) = if algorithm == "ivf" {
            ("mydb_ivf_nprobe", 4, 64)
        } else {
            ("mydb_hnsw_ef_search", 16, 512)
        };
        for (effort, ann) in [(low, true), (high, false), (low, true)] {
            engine
                .execute_with_session(&mut session, &format!("SET SESSION {variable}={effort}"))
                .unwrap();
            engine
                .execute_prepared_select(&mut session, &mut state, &parsed, sql, &params)
                .unwrap()
                .unwrap()
                .materialize()
                .unwrap();
            let PreparedSelectMemoState::Ready(memo) = &state else {
                panic!("memo required")
            };
            assert_eq!(
                memo.compiled
                    .physical
                    .any(|p| matches!(p, PhysicalPlan::VectorIndexScan { .. })),
                ann,
                "{algorithm} effort={effort} must re-evaluate the cost crossover"
            );
            plans.push(memo.compiled.clone());
        }
        assert!(!Arc::ptr_eq(&plans[0], &plans[1]));
        assert!(Arc::ptr_eq(&plans[0], &plans[2]));
    }
}

#[test]
fn vector_prepared_filter_selectivity_switches_between_ann_and_exact() {
    use crate::engine::api::{PreparedSelectMemoState, QueryEngine as _};
    use crate::planner::PhysicalPlan;
    let _env = HnswEnv::set(&[
        ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
        ("MYDB_HNSW_BRUTE_FILTER_ROWS", "8"),
        ("MYDB_HNSW_EF_SEARCH", "128"),
    ]);
    for algorithm in ["hnsw", "ivf"] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = open_test_engine(dir.path());
        engine
            .execute("CREATE TABLE ann_filter(id INT PRIMARY KEY,v VECTOR(3) NOT NULL)")
            .unwrap();
        let rows = (0..96)
            .map(|i| format!("({i},'[{}, {}, {}]')", i, i % 7, i % 11))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO ann_filter VALUES {rows}"))
            .unwrap();
        let options = if algorithm == "ivf" { ",nlist=8" } else { "" };
        engine
            .execute(&format!(
                "CREATE INDEX ann ON ann_filter(v) USING {algorithm} WITH(metric=l2{options})"
            ))
            .unwrap();
        engine.execute("ANALYZE TABLE ann_filter").unwrap();
        let sql =
            "SELECT id FROM ann_filter WHERE id>=? ORDER BY L2_DISTANCE(v,TO_VECTOR(?)),id LIMIT ?";
        let mut session = SessionContext::default();
        let parsed = engine.prepare_cached(&mut session, sql, 3).unwrap();
        let strong = vec![
            Value::Int(92),
            Value::String("[17.1,2.1,3.1]".into()),
            Value::Int(2),
        ];
        let memo = crate::engine::expr::with_session_expr_info(&session, || {
            crate::engine::executor::compiled_select::compile_prepared_select(
                &engine.db,
                &session,
                &parsed,
                &strong,
                engine.plan_cache(),
            )
        })
        .unwrap();
        assert!(
            !memo
                .compiled
                .physical
                .any(|p| matches!(p, PhysicalPlan::VectorIndexScan { .. })),
            "selective {algorithm} must retain the exact scan"
        );
        let mut state = PreparedSelectMemoState::default();
        let mut plans = Vec::new();
        for lower in [0, 92, 0] {
            let params = vec![
                Value::Int(lower),
                Value::String("[17.1,2.1,3.1]".into()),
                Value::Int(2),
            ];
            let response = engine
                .execute_prepared_select(&mut session, &mut state, &parsed, sql, &params)
                .unwrap()
                .unwrap()
                .materialize()
                .unwrap();
            let QueryResponse::ResultSet(actual) = response else {
                panic!("rows required")
            };
            let expected=query_rows(&engine,&format!("SELECT /*+ NO_VECTOR_INDEX */ id FROM ann_filter WHERE id>={lower} ORDER BY L2_DISTANCE(v,TO_VECTOR('[17.1,2.1,3.1]')),id LIMIT 2"));
            assert_eq!(actual.rows, expected, "{algorithm} lower={lower}");
            let PreparedSelectMemoState::Ready(memo) = &state else {
                panic!("memo required")
            };
            assert_eq!(
                memo.compiled
                    .physical
                    .any(|p| matches!(p, PhysicalPlan::VectorIndexScan { .. })),
                lower == 0,
                "{algorithm} lower={lower}"
            );
            plans.push(memo.compiled.clone());
        }
        assert!(!Arc::ptr_eq(&plans[0], &plans[1]));
        assert!(Arc::ptr_eq(&plans[0], &plans[2]));
    }
}

#[test]
fn vector_prepared_limit_plans_ann_without_execution_bindings() {
    use crate::engine::api::QueryEngine as _;
    use crate::planner::PhysicalPlan;
    for algorithm in ["hnsw", "ivf"] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = open_test_engine(dir.path());
        engine
            .execute("CREATE TABLE ann_limit(id INT PRIMARY KEY,v VECTOR(3) NOT NULL)")
            .unwrap();
        let rows = (0..96)
            .map(|i| format!("({i},'[{}, {}, {}]')", i, i % 7, i % 11))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO ann_limit VALUES {rows}"))
            .unwrap();
        let options = if algorithm == "ivf" { ",nlist=8" } else { "" };
        engine
            .execute(&format!(
                "CREATE INDEX ann ON ann_limit(v) USING {algorithm} WITH(metric=l2{options})"
            ))
            .unwrap();
        let hint = if algorithm == "ivf" {
            "IVF_NPROBE(8)"
        } else {
            "HNSW_EF(128)"
        };
        let sql = format!("SELECT /*+ {hint} */ id FROM ann_limit ORDER BY L2_DISTANCE(v,TO_VECTOR(?)) LIMIT ? OFFSET ?");
        let mut session = SessionContext::default();
        let parsed = engine.prepare_cached(&mut session, &sql, 3).unwrap();
        let params = vec![
            Value::String("[17.1,2.1,3.1]".into()),
            Value::Int(5),
            Value::Int(2),
        ];
        assert!(!crate::engine::params::has_active_params());
        let memo = crate::engine::executor::compiled_select::compile_prepared_select(
            &engine.db,
            &session,
            &parsed,
            &params,
            engine.plan_cache(),
        )
        .unwrap();
        assert!(
            memo.compiled
                .physical
                .any(|plan| matches!(plan, PhysicalPlan::VectorIndexScan { k: 7, .. })),
            "{algorithm}: {:?}",
            memo.compiled.physical
        );
        let (_, compiled) =
            crate::engine::executor::compiled_select::compile_uncached_prepared_select(
                &engine.db,
                &session,
                &parsed,
                &params,
                engine.plan_cache().current_epoch(),
            )
            .unwrap();
        assert!(
            compiled
                .physical
                .any(|plan| matches!(plan, PhysicalPlan::VectorIndexScan { k: 7, .. })),
            "uncached {algorithm}: {:?}",
            compiled.physical
        );
        let mut state = crate::engine::api::PreparedSelectMemoState::default();
        for (query, k, offset) in [
            ("[17.1,2.1,3.1]", 5, 2),
            ("[67.2,3.1,4.2]", 7, 1),
            ("[17.1,2.1,3.1]", 5, 2),
            ("[1.1,2.1,3.1]", 0, 0),
        ] {
            let params = vec![
                Value::String(query.into()),
                Value::Int(k),
                Value::Int(offset),
            ];
            let actual = engine
                .execute_prepared_select(&mut session, &mut state, &parsed, &sql, &params)
                .unwrap()
                .unwrap()
                .materialize()
                .unwrap();
            let QueryResponse::ResultSet(actual) = actual else {
                panic!("rows required")
            };
            let expected = query_rows(&engine,&format!("SELECT /*+ NO_VECTOR_INDEX */ id FROM ann_limit ORDER BY L2_DISTANCE(v,TO_VECTOR('{query}')) LIMIT {k} OFFSET {offset}"));
            assert_eq!(actual.rows, expected, "{algorithm} {query} {k} {offset}");
        }
        assert!(!crate::engine::params::has_active_params());
    }
}

#[test]
fn vector_datatypes_conditionals_preserve_native_domains_through_scalar_escape() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    for datatype in ["FLOAT32", "FLOAT16", "BFLOAT16", "INT8", "BINARY"] {
        engine
            .execute(&format!(
                "CREATE TABLE native_{datatype}(id INT PRIMARY KEY,v VECTOR(3,{datatype}))"
            ))
            .unwrap();
        engine
            .execute(&format!(
                "INSERT INTO native_{datatype} VALUES(1,'[1,0,1]'),(2,NULL)"
            ))
            .unwrap();
        let original = query_rows(
            &engine,
            &format!("SELECT v FROM native_{datatype} WHERE id=1"),
        )[0][0]
            .clone();
        let Value::Vector(expected) = &original else {
            panic!("vector required")
        };
        for source in [
            format!("native_{datatype}"),
            format!("(SELECT id,v FROM native_{datatype}) d"),
        ] {
            let sql=format!("SELECT IF(id=1,v,NULL),CASE WHEN id=1 THEN v ELSE NULL END,COALESCE(NULL,v),IFNULL(v,NULL),NULLIF(v,TO_VECTOR('[0,0,0]','{datatype}')) FROM {source} ORDER BY id");
            let QueryResponse::ResultSet(result) =
                engine.execute(&sql).unwrap().materialize().unwrap()
            else {
                panic!("rows required")
            };
            assert_eq!(
                result.rows,
                vec![vec![original.clone(); 5], vec![Value::Null; 5]],
                "{datatype}: {source}"
            );
            for column in &result.columns {
                assert_eq!(
                    column.source.vector_type,
                    Some((expected.datatype, Some(3))),
                    "{datatype}: {}",
                    column.name
                );
            }
        }
    }
}

#[test]
fn vector_datatypes_functions_read_text_and_options_from_columns() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE vector_args(id INT PRIMARY KEY,txt VARCHAR(20),dt VARCHAR(10),metric VARCHAR(10),pos VARCHAR(2),n VARCHAR(2))").unwrap();
    engine.execute("INSERT INTO vector_args VALUES(1,'[3,4]','FLOAT16','L2','2','1'),(2,'[0,1]','INT8','DOT','1','2')").unwrap();
    assert_eq!(
        query_rows(&engine, "SELECT VECTOR_DATATYPE(TO_VECTOR(txt,dt)),VECTOR_TO_STRING(SUBVECTOR(txt,pos,n)),DISTANCE(txt,'[0,0]',metric) FROM vector_args ORDER BY id"),
        vec![
            vec![Value::String("FLOAT16".into()), Value::String("[4]".into()), Value::Double(5.0)],
            vec![Value::String("INT8".into()), Value::String("[0,1]".into()), Value::Double(0.0)],
        ]
    );
}

#[test]
fn vector_datatypes_binary_subvectors_cover_unaligned_bit_ranges() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let bits = [1, 0, 1, 1, 0, 0, 1, 0, 1, 1, 1, 0, 1, 0, 0, 1, 1];
    let text = format!(
        "[{}]",
        bits.iter()
            .map(i32::to_string)
            .collect::<Vec<_>>()
            .join(",")
    );
    engine
        .execute("CREATE TABLE packed(id INT PRIMARY KEY,v VECTOR(17,BINARY))")
        .unwrap();
    engine
        .execute(&format!("INSERT INTO packed VALUES(1,'{text}')"))
        .unwrap();
    for start in 0..bits.len() {
        for count in 1..=bits.len() - start {
            let expected_text = format!(
                "[{}]",
                bits[start..start + count]
                    .iter()
                    .map(i32::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            );
            let sql = format!("SELECT SUBVECTOR(v,{},{}),HAMMING_DISTANCE(SUBVECTOR(v,{},{}),TO_VECTOR('{expected_text}','BINARY')) FROM packed", start+1,count,start+1,count);
            let row = &query_rows(&engine, &sql)[0];
            let Value::Vector(value) = &row[0] else {
                panic!("native vector required")
            };
            assert_eq!(value.datatype, crate::vector::VectorDataType::Binary);
            assert_eq!(value.dimension, count as u32);
            let expected = (0..count.div_ceil(8))
                .map(|byte| {
                    (0..8)
                        .filter(|bit| byte * 8 + bit < count)
                        .fold(0u8, |out, bit| {
                            out | ((bits[start + byte * 8 + bit] as u8) << bit)
                        })
                })
                .collect::<Vec<_>>();
            assert_eq!(value.payload, expected, "start={start}, count={count}");
            assert_eq!(row[1], Value::Double(0.0));
        }
    }
    for (start, count) in [(0, 1), (1, 0), (17, 2), (i64::MAX, 2), (1, i64::MAX)] {
        let sql = format!("SELECT SUBVECTOR(v,{start},{count}) FROM packed");
        assert!(
            engine
                .execute(&sql)
                .and_then(QueryResponse::materialize)
                .is_err(),
            "{sql}"
        );
    }
}

#[test]
fn vector_datatypes_invalid_dml_is_atomic_and_keeps_indexes_consistent() {
    let _env = HnswEnv::set(&[("MYDB_VECTOR_STORAGE", "memory")]);
    for (datatype, invalid) in [
        ("FLOAT32", "[3.5e38,1]"),
        ("FLOAT16", "[65520,1]"),
        ("BFLOAT16", "[3.4e38,1]"),
        ("INT8", "[128,1]"),
        ("BINARY", "[2,1]"),
    ] {
        for algorithm in ["HNSW", "IVF"] {
            let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
            let engine = open_test_engine(dir.path());
            engine
                .execute(&format!(
                    "CREATE TABLE atomic_v(id INT PRIMARY KEY,v VECTOR(2,{datatype}) NOT NULL)"
                ))
                .unwrap();
            engine
                .execute("INSERT INTO atomic_v VALUES(1,'[0,1]'),(2,'[1,0]')")
                .unwrap();
            let metric = if datatype == "BINARY" {
                "HAMMING"
            } else {
                "L2"
            };
            let options = if algorithm == "IVF" { ",nlist=1" } else { "" };
            engine.execute(&format!("CREATE INDEX vx ON atomic_v(v) USING {algorithm} WITH(metric={metric}{options})")).unwrap();
            let before = query_rows(
                &engine,
                "SELECT id,VECTOR_TO_BINARY(v) FROM atomic_v ORDER BY id",
            );
            for dml in [
                format!("INSERT INTO atomic_v VALUES(3,'[1,1]'),(4,'{invalid}')"),
                format!("UPDATE atomic_v SET v=CASE WHEN id=1 THEN '[1,1]' ELSE '{invalid}' END"),
                "INSERT INTO atomic_v VALUES(3,'[1]')".into(),
                "UPDATE atomic_v SET v=NULL WHERE id=1".into(),
            ] {
                let error = engine
                    .execute(&dml)
                    .expect_err("invalid vector DML must fail");
                assert!(!error.message().is_empty());
                assert_eq!(
                    query_rows(
                        &engine,
                        "SELECT id,VECTOR_TO_BINARY(v) FROM atomic_v ORDER BY id"
                    ),
                    before,
                    "{datatype}/{algorithm}: {dml}"
                );
                let sql = format!("SELECT id FROM atomic_v ORDER BY DISTANCE(v,TO_VECTOR('[0,1]','{datatype}'),'{metric}'),id LIMIT 2");
                assert_eq!(
                    query_rows(&engine, &sql),
                    vec![vec![Value::Int(1)], vec![Value::Int(2)]],
                    "{datatype}/{algorithm}: {dml}"
                );
            }
            let mut session = SessionContext::default();
            for sql in [
                "BEGIN",
                "UPDATE atomic_v SET v='[1,1]' WHERE id=1",
                "DELETE FROM atomic_v WHERE id=2",
                "ROLLBACK",
            ] {
                engine.execute_with_session(&mut session, sql).unwrap();
            }
            assert_eq!(
                query_rows(
                    &engine,
                    "SELECT id,VECTOR_TO_BINARY(v) FROM atomic_v ORDER BY id"
                ),
                before
            );
        }
    }
}

#[test]
fn vector_datatypes_prepared_binary_errors_do_not_poison_rebinding() {
    use crate::engine::api::QueryEngine as _;
    use crate::engine::params::with_params;
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let sql = "SELECT VECTOR_FROM_BINARY(?,?,?) AS v";
    let statement = engine.prepare_cached(&mut session, sql, 3).unwrap();
    for (datatype, dimension, payload, valid) in [
        ("FLOAT16", 1, vec![0x00, 0x3c], true),
        ("FLOAT16", 1, vec![0x00, 0x7c], false),
        ("BFLOAT16", 1, vec![0x80, 0x3f], true),
        ("BINARY", 7, vec![0x80], false),
        ("BINARY", 9, vec![0x05, 0x01], true),
        ("BINARY", 10, vec![0x05, 0x01], true),
        ("BINARY", 9, vec![0x05, 0x01], true),
        ("INT8", 2, vec![0x80], false),
        ("INT8", 2, vec![0x80, 0x7f], true),
        ("FLOAT32", 1, vec![0, 0, 0xc0, 0x7f], false),
        ("FLOAT32", 1, 1.0f32.to_le_bytes().to_vec(), true),
    ] {
        let params = vec![
            Value::Bytes(payload.clone()),
            Value::String(datatype.into()),
            Value::Int(dimension),
        ];
        let response = with_params(params, || {
            engine.execute_prepared_statement(&mut session, sql, (*statement).clone())
        })
        .expect("prepared SELECT")
        .and_then(QueryResponse::materialize);
        if !valid {
            assert!(response.is_err(), "{datatype}/{dimension}/{payload:?}");
            continue;
        }
        let QueryResponse::ResultSet(result) = response.unwrap() else {
            panic!("rows required")
        };
        let Value::Vector(value) = &result.rows[0][0] else {
            panic!("vector required")
        };
        assert_eq!(value.payload, payload);
        assert_eq!(value.dimension, dimension as u32);
        assert_eq!(value.datatype.name(), datatype);
        assert_eq!(
            result.columns[0].source.vector_type,
            Some((value.datatype, Some(value.dimension)))
        );
    }
}

#[test]
fn vector_datatypes_prepared_subvector_dimension_rebinds_with_same_parameter_type() {
    use crate::engine::api::QueryEngine as _;
    use crate::engine::params::with_params;
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE dimension_v(id INT PRIMARY KEY,v VECTOR(9,BINARY) NOT NULL)")
        .unwrap();
    engine
        .execute("INSERT INTO dimension_v VALUES(1,'[1,0,1,0,1,0,1,0,1]')")
        .unwrap();
    let mut session = SessionContext::default();
    for expression in [
        "SUBVECTOR(v,1,?)",
        "IF(id=1,SUBVECTOR(v,1,?),NULL)",
        "CASE WHEN id=1 THEN SUBVECTOR(v,1,?) ELSE NULL END",
    ] {
        let sql = format!("SELECT {expression} AS v FROM dimension_v");
        let sql = sql.as_str();
        let statement = engine.prepare_cached(&mut session, sql, 1).unwrap();
        let mut memo = crate::engine::api::PreparedSelectMemoState::default();
        for count in [1, 2, 7, 8, 9, 1] {
            let params = vec![Value::Int(count)];
            let response = match engine.execute_prepared_select(
                &mut session,
                &mut memo,
                &statement,
                sql,
                &params,
            ) {
                Some(response) => response.unwrap(),
                None => with_params(params, || {
                    engine.execute_prepared_statement(&mut session, sql, (*statement).clone())
                })
                .unwrap()
                .unwrap(),
            }
            .materialize()
            .unwrap();
            let QueryResponse::ResultSet(result) = response else {
                panic!("rows required")
            };
            let Value::Vector(value) = &result.rows[0][0] else {
                panic!("vector required")
            };
            assert_eq!(value.dimension, count as u32, "{sql}");
            assert_eq!(
                result.columns[0].source.vector_type,
                Some((value.datatype, Some(count as u32)))
            );
        }
    }
}

#[test]
fn vector_distance_native_columns_match_row_sort_projection_and_parallel_ranges() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    for datatype in ["FLOAT32", "FLOAT16", "BFLOAT16", "INT8", "BINARY"] {
        let table = format!("distance_{datatype}");
        engine
            .execute(&format!(
                "CREATE TABLE {table}(id INT PRIMARY KEY,v VECTOR(9,{datatype}))"
            ))
            .unwrap();
        let values = (0..256)
            .map(|id| {
                let vector = if id % 13 == 0 {
                    "NULL".into()
                } else {
                    let text = (0..9)
                        .map(|dimension| {
                            if id % 17 == 0 {
                                0
                            } else if datatype == "BINARY" {
                                (id + dimension) % 2
                            } else {
                                (id * 3 + dimension * 7) % 9 - 4
                            }
                            .to_string()
                        })
                        .collect::<Vec<_>>()
                        .join(",");
                    format!("'[{text}]'")
                };
                format!("({id},{vector})")
            })
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO {table} VALUES {values}"))
            .unwrap();
        engine.execute(&format!("ANALYZE TABLE {table}")).unwrap();
        let metrics: &[(&str, Option<&str>)] = if datatype == "BINARY" {
            &[
                ("HAMMING_DISTANCE", None),
                ("JACCARD_DISTANCE", None),
                ("DISTANCE", None),
                ("VEC_DISTANCE", Some("JACCARD")),
            ]
        } else {
            &[
                ("L2_DISTANCE", None),
                ("COSINE_DISTANCE", None),
                ("DOT_PRODUCT", None),
                ("INNER_PRODUCT", None),
                ("MAX_INNER_PRODUCT_DISTANCE", None),
                ("DISTANCE", None),
                ("VEC_DISTANCE", Some("COSINE")),
            ]
        };
        for &(name, metric) in metrics {
            let query_type = if datatype == "BINARY" {
                "BINARY"
            } else {
                "FLOAT32"
            };
            let query = format!("TO_VECTOR('[1,0,1,0,1,0,1,0,1]','{query_type}')");
            let tail = metric.map(|m| format!(",'{m}'")).unwrap_or_default();
            for expression in [
                format!("{name}(v,{query}{tail})"),
                format!("{name}({query},v{tail})"),
            ] {
                for direction in ["ASC", "DESC"] {
                    let sql=format!("SELECT /*+ NO_VECTOR_INDEX PARALLEL(4) */ id,{expression} AS d FROM {table} WHERE id>=128 ORDER BY d {direction},id LIMIT 17 OFFSET 3");
                    let expected = with_vec_mode(VecMode::Off, || query_rows(&engine, &sql));
                    for mode in [VecMode::Auto, VecMode::On] {
                        assert_eq!(
                            with_vec_mode(mode, || query_rows(&engine, &sql)),
                            expected,
                            "{mode:?}: {sql}"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn vector_distance_prepared_range_topk_rebinds_queries_limits_and_errors() {
    use crate::engine::api::QueryEngine as _;
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    use crate::engine::params::with_params;
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE prepared_distance(id INT PRIMARY KEY,v VECTOR(3))")
        .unwrap();
    let values = (0..256)
        .map(|id| {
            format!(
                "({id},'[{}, {}, {}]')",
                id % 3,
                i32::from(id % 3 == 1),
                i32::from(id % 3 == 2)
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO prepared_distance VALUES {values}"))
        .unwrap();
    let sql="SELECT /*+ NO_VECTOR_INDEX PARALLEL(4) */ id FROM prepared_distance WHERE id>=? ORDER BY COSINE_DISTANCE(v,TO_VECTOR(?)),id LIMIT ? OFFSET ?";
    let mut session = SessionContext::default();
    let statement = engine.prepare_cached(&mut session, sql, 4).unwrap();
    let mut memo = crate::engine::api::PreparedSelectMemoState::default();
    for (query, lower, k, offset, valid) in [
        ("[1,0,0]", 128, 7, 0, true),
        ("[0,1,0]", 100, 13, 3, true),
        ("[0,1]", 128, 7, 0, false),
        ("[bad]", 128, 7, 0, false),
        ("[0,0,0]", 128, 7, 0, true),
        ("[0,0,1]", 128, 0, 0, true),
        ("[1,0,0]", 128, 7, 0, true),
    ] {
        let literal=format!("SELECT /*+ NO_VECTOR_INDEX PARALLEL(4) */ id FROM prepared_distance WHERE id>={lower} ORDER BY COSINE_DISTANCE(v,TO_VECTOR('{query}')),id LIMIT {k} OFFSET {offset}");
        let expected = with_vec_mode(VecMode::Off, || {
            engine
                .execute(&literal)
                .and_then(QueryResponse::materialize)
        });
        assert_eq!(expected.is_ok(), valid, "{literal}");
        let params = vec![
            Value::Int(lower),
            Value::String(query.into()),
            Value::Int(k),
            Value::Int(offset),
        ];
        let response = with_vec_mode(VecMode::On, || {
            match engine.execute_prepared_select(&mut session, &mut memo, &statement, sql, &params)
            {
                Some(result) => result,
                None => with_params(params, || {
                    engine.execute_prepared_statement(&mut session, sql, (*statement).clone())
                })
                .unwrap(),
            }
            .and_then(QueryResponse::materialize)
        });
        match (expected, response) {
            (Ok(QueryResponse::ResultSet(expected)), Ok(QueryResponse::ResultSet(actual))) => {
                assert_eq!(actual.rows, expected.rows, "{literal}")
            }
            (Err(expected), Err(actual)) => assert_eq!(actual.code(), expected.code(), "{literal}"),
            _ => panic!("prepared result/error differed: {literal}"),
        }
    }
}

#[test]
fn vector_distance_projection_uses_native_batch_contract() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE distance_contract(id INT PRIMARY KEY,v VECTOR(3))")
        .unwrap();
    let values = (0..256)
        .map(|id| format!("({id},'[{},1,2]')", id % 7))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO distance_contract VALUES {values}"))
        .unwrap();
    let before = crate::engine::expr_program::expr_program_stats();
    let output = with_vec_mode(VecMode::On, || {
        query_rows(&engine,"SELECT id,COSINE_DISTANCE(v,TO_VECTOR('[1,2,3]')) FROM distance_contract WHERE id>=128 ORDER BY id")
    });
    let after = crate::engine::expr_program::expr_program_stats();
    assert_eq!(output.len(), 128);
    assert!(after.column_eval_rows > before.column_eval_rows);
    assert_eq!(
        after.legacy_escape_rows, before.legacy_escape_rows,
        "native vector projection must not interpret/copy every row"
    );
}
