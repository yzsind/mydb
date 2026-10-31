//! Native vector datatype SQL regressions.
use super::support::query_rows;
use super::vector_type::HnswEnv;
use super::*;

#[path = "vector_quality.rs"]
mod quality;

fn scalar(engine: &StorageQueryEngine, sql: &str) -> String {
    query_rows(engine, sql)[0][0].to_display_string()
}

#[test]
fn vector_datatypes_sql_storage_and_functions() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine.execute("CREATE TABLE typed (id INT PRIMARY KEY, a VECTOR(3,FLOAT16), b VECTOR(3,BFLOAT16), c VECTOR(3,INT8), d VECTOR(9,BINARY))").unwrap();
    engine.execute("INSERT INTO typed VALUES (1,'[1,2.5,-3]','[1,2.5,-3]','[-128,0,127]','[1,0,1,0,0,0,0,0,1]')").unwrap();
    assert_eq!(
        scalar(&engine, "SELECT VECTOR_DATATYPE(a) FROM typed"),
        "FLOAT16"
    );
    assert_eq!(
        scalar(&engine, "SELECT VECTOR_DATATYPE(b) FROM typed"),
        "BFLOAT16"
    );
    assert_eq!(
        scalar(&engine, "SELECT VECTOR_TO_STRING(c) FROM typed"),
        "[-128,0,127]"
    );
    assert_eq!(scalar(&engine, "SELECT VECTOR_DIM(d) FROM typed"), "9");
    assert_eq!(
        scalar(&engine, "SELECT HEX(VECTOR_TO_BINARY(d)) FROM typed"),
        "0501"
    );
    assert_eq!(
        scalar(
            &engine,
            "SELECT VECTOR_TO_STRING(SUBVECTOR(d,2,8)) FROM typed"
        ),
        "[0,1,0,0,0,0,0,1]"
    );
    assert_eq!(
        scalar(
            &engine,
            "SELECT VECTOR_DATATYPE(CAST('[1,2,3]' AS VECTOR(3,FLOAT16)))"
        ),
        "FLOAT16"
    );
    assert_eq!(
        scalar(
            &engine,
            "SELECT L2_DISTANCE(a,TO_VECTOR('[1,2.5,-2.5]')) FROM typed"
        ),
        "0.5"
    );
    assert_eq!(
        scalar(
            &engine,
            "SELECT VECTOR_DATATYPE(VECTOR_FROM_BINARY(X'0501','BINARY',9))"
        ),
        "BINARY"
    );
    assert_eq!(
        scalar(
            &engine,
            "SELECT DISTANCE(d,TO_VECTOR('[1,0,0,0,0,0,0,0,0]','BINARY')) FROM typed"
        ),
        "2"
    );
    assert_eq!(
        scalar(
            &engine,
            "SELECT JACCARD_DISTANCE(TO_VECTOR('[0,0]','BINARY'),TO_VECTOR('[0,0]','BINARY'))"
        ),
        "0"
    );
    for sql in [
        "INSERT INTO typed(id,c) VALUES(2,'[1.5,2,3]')",
        "INSERT INTO typed(id,c) VALUES(2,'[128,2,3]')",
        "SELECT TO_VECTOR('[65520]','FLOAT16')",
        "SELECT VECTOR_FROM_BINARY(X'FF','BINARY',7)",
        "CREATE TABLE bad(v VECTOR(3,FLOAT16,INT8))",
    ] {
        assert!(engine.execute(sql).is_err(), "must reject: {sql}");
    }
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    assert_eq!(
        scalar(&engine, "SELECT VECTOR_DATATYPE(a) FROM typed"),
        "FLOAT16"
    );
    assert_eq!(
        scalar(&engine, "SELECT HEX(VECTOR_TO_BINARY(d)) FROM typed"),
        "0501"
    );
}

#[test]
fn vector_datatypes_exact_knn_preserves_query_precision() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    for datatype in ["FLOAT16", "BFLOAT16", "INT8"] {
        engine
            .execute(&format!(
                "CREATE TABLE t_{datatype}(id INT PRIMARY KEY, v VECTOR(2,{datatype}) NOT NULL)"
            ))
            .unwrap();
        engine
            .execute(&format!(
                "INSERT INTO t_{datatype} VALUES(1,'[0,0]'),(2,'[1,1]'),(3,'[2,2]')"
            ))
            .unwrap();
        assert_eq!(scalar(&engine,&format!("SELECT id FROM t_{datatype} ORDER BY L2_DISTANCE(v,TO_VECTOR('[0.6,0.6]')) LIMIT 1")),"2");
    }
}

fn native_index_matrix(backend: &str) {
    use crate::engine::vector::{Metric, VectorSearchEffort, VectorSearchRequest};
    use crate::vector::VectorDataType;
    let _env = HnswEnv::set(&[
        ("MYDB_VECTOR_STORAGE", backend),
        ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
        ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
    ]);
    for datatype in ["FLOAT32", "FLOAT16", "BFLOAT16", "INT8", "BINARY"] {
        let binary = datatype == "BINARY";
        let metrics: &[&str] = if binary {
            &["HAMMING", "JACCARD"]
        } else {
            &["L2", "COSINE", "DOT"]
        };
        for &metric in metrics {
            for algo in ["HNSW", "IVF"] {
                let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
                let engine = open_test_engine(dir.path());
                let dim = if binary { 9 } else { 4 };
                engine.execute(&format!("CREATE TABLE items(id INT PRIMARY KEY,v VECTOR({dim},{datatype}) NOT NULL)")).unwrap();
                let mut values = Vec::new();
                for id in 1..=32 {
                    let components: Vec<String> = if binary {
                        (0..9)
                            .map(|bit| ((id >> (bit % 6)) & 1).to_string())
                            .collect()
                    } else {
                        (0..4)
                            .map(|d| (((id * (d + 3) + d * 7) % 23) + 1).to_string())
                            .collect()
                    };
                    values.push(format!("({id},'[{}]')", components.join(",")));
                }
                engine
                    .execute(&format!("INSERT INTO items VALUES {}", values.join(",")))
                    .unwrap();
                let options = if algo == "HNSW" {
                    "m=4, ef_construction=64"
                } else {
                    "nlist=4, kmeans_iters=2"
                };
                engine.execute(&format!("CREATE INDEX vx ON items(v) USING {algo} WITH (metric={metric}, {options})")).unwrap();
                let db = engine.database();
                let table = db.table("test.items").unwrap();
                let index = table.indexes.iter().find(|i| i.name == "vx").unwrap();
                let graph = db.vector_graph_get_or_build(&table, index).unwrap();
                assert_eq!(graph.len(), 32, "{backend}/{datatype}/{metric}/{algo}");
                let query_text = if binary {
                    "[1,0,1,0,1,0,1,0,1]"
                } else {
                    "[1.25,2.5,3.75,4.125]"
                };
                let query = crate::engine::vector::codec::parse_typed_text(
                    query_text,
                    if binary {
                        VectorDataType::Binary
                    } else {
                        VectorDataType::Float32
                    },
                )
                .unwrap();
                let effort = if algo == "HNSW" {
                    VectorSearchEffort::Hnsw { ef: 64 }
                } else {
                    VectorSearchEffort::Ivf { nprobe: 4 }
                };
                let result = db
                    .vector_index_search_for_partition(
                        &table,
                        index,
                        table.id,
                        &query,
                        VectorSearchRequest {
                            candidate_limit: 32,
                            rerank_limit: 32,
                            effort,
                        },
                    )
                    .unwrap();
                assert!(
                    !result.candidates.is_empty(),
                    "{backend}/{datatype}/{metric}/{algo}"
                );
                let metric_value = Metric::parse(metric).unwrap();
                let stored = query_rows(&engine, "SELECT id,v FROM items");
                let mut truth = Vec::new();
                for row in stored {
                    let id = row[0].to_display_string().parse::<i64>().unwrap();
                    let Value::Vector(v) = &row[1] else {
                        panic!("typed vector required")
                    };
                    assert_eq!(v.datatype, VectorDataType::parse(datatype).unwrap());
                    let mut dot = 0.0;
                    let mut na = 0.0;
                    let mut nb = 0.0;
                    let mut l2 = 0.0;
                    let mut different = 0;
                    let mut intersection = 0;
                    let mut union = 0;
                    for d in 0..dim {
                        let a = v.element(d);
                        let b = query.element(d);
                        dot += a * b;
                        na += a * a;
                        nb += b * b;
                        l2 += (a - b) * (a - b);
                        different += usize::from(a != b);
                        intersection += usize::from(a != 0.0 && b != 0.0);
                        union += usize::from(a != 0.0 || b != 0.0);
                    }
                    let score = match metric_value {
                        Metric::L2 => l2,
                        Metric::Dot => -dot,
                        Metric::Cosine => 1.0 - dot / (na.sqrt() * nb.sqrt()),
                        Metric::Hamming => different as f64,
                        Metric::Jaccard => {
                            if union == 0 {
                                0.0
                            } else {
                                1.0 - intersection as f64 / union as f64
                            }
                        }
                    };
                    truth.push((score, id));
                }
                truth.sort_by(|a, b| a.0.total_cmp(&b.0));
                let threshold = truth[2].0;
                let query_constructor = if binary {
                    format!("TO_VECTOR('{query_text}','BINARY')")
                } else {
                    format!("TO_VECTOR('{query_text}')")
                };
                let sql=format!("SELECT id FROM items ORDER BY DISTANCE(v,{query_constructor},'{metric}') LIMIT 3");
                let selected = query_rows(&engine, &sql);
                assert_eq!(selected.len(), 3);
                for row in selected {
                    let id = row[0].to_display_string().parse::<i64>().unwrap();
                    let score = truth.iter().find(|(_, rid)| *rid == id).unwrap().0;
                    assert!(
                        score <= threshold + threshold.abs() * 1e-6 + 1e-7,
                        "{backend}/{datatype}/{metric}/{algo}: {score} > {threshold}"
                    );
                }
                engine.execute("DELETE FROM items WHERE id=2").unwrap();
                let replacement = if binary {
                    "[1,1,1,1,1,1,1,1,1]"
                } else {
                    "[1,2,3,4]"
                };
                engine
                    .execute(&format!("UPDATE items SET v='{replacement}' WHERE id=1"))
                    .unwrap();
                assert_eq!(
                    scalar(&engine, "SELECT VECTOR_DATATYPE(v) FROM items WHERE id=1"),
                    datatype
                );
                assert!(query_rows(&engine, &sql)
                    .iter()
                    .all(|row| row[0].to_display_string() != "2"));
            }
        }
    }
}

#[test]
fn vector_datatypes_native_indexes_memory_matrix() {
    native_index_matrix("memory");
}
#[test]
fn vector_datatypes_native_indexes_paged_matrix() {
    native_index_matrix("paged");
}

#[test]
fn vector_datatypes_wide_binary_overflow_and_derived_domains() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_strict_test_engine(dir.path());
    let bits = 65_537;
    let text = format!(
        "[{}]",
        std::iter::repeat_n("0", bits).collect::<Vec<_>>().join(",")
    );
    engine
        .execute(&format!(
            "CREATE TABLE wide(id INT PRIMARY KEY,v VECTOR({bits},BINARY) NOT NULL)"
        ))
        .unwrap();
    engine
        .execute(&format!("INSERT INTO wide VALUES(1,'{text}')"))
        .unwrap();
    assert_eq!(
        scalar(&engine, "SELECT VECTOR_DIM(v) FROM wide"),
        bits.to_string()
    );
    assert_eq!(
        scalar(
            &engine,
            "SELECT id FROM wide ORDER BY DISTANCE(v,v) LIMIT 1"
        ),
        "1"
    );
    assert_eq!(
        scalar(
            &engine,
            "SELECT VECTOR_DATATYPE(v) FROM (SELECT TO_VECTOR('[1,2]','FLOAT16') AS v) d"
        ),
        "FLOAT16"
    );
    let rows = query_rows(
        &engine,
        "SELECT TO_VECTOR('[1,2]','FLOAT16') AS v UNION ALL SELECT TO_VECTOR('[3,4]','BFLOAT16')",
    );
    assert!(rows.iter().all(
        |r| matches!(&r[0],Value::Vector(v) if v.datatype==crate::vector::VectorDataType::Float32)
    ));
    assert!(engine
        .execute("SELECT TO_VECTOR('[0,1]','BINARY') UNION ALL SELECT TO_VECTOR('[1,2]','FLOAT16')")
        .is_err());
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    assert_eq!(
        scalar(&engine, "SELECT VECTOR_DIM(v) FROM wide"),
        bits.to_string()
    );
}

#[test]
fn vector_datatypes_native_paged_restart_snapshot_and_mapping() {
    use crate::engine::vector::graph::VectorGraph;
    for mapped in ["0", "1"] {
        let _env = HnswEnv::set(&[
            ("MYDB_VECTOR_STORAGE", "paged"),
            ("MYDB_HNSW_MAPPED_NAVIGATION", mapped),
            ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
            ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
        ]);
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let mut queries = Vec::new();
        {
            let engine = open_strict_test_engine(dir.path());
            for datatype in ["FLOAT16", "BFLOAT16", "INT8", "BINARY"] {
                for algo in ["HNSW", "IVF"] {
                    let name = format!("items_{datatype}_{algo}");
                    let preparation =
                        crate::platform::fs::FixtureSyncBatch::new(dir.path()).unwrap();
                    let binary = datatype == "BINARY";
                    let dim = if binary { 9 } else { 4 };
                    engine.execute(&format!("CREATE TABLE {name}(id INT PRIMARY KEY,v VECTOR({dim},{datatype}) NOT NULL)")).unwrap();
                    let vector = |id: i32| {
                        if binary {
                            format!(
                                "[{},{},{},{},1,0,0,0,1]",
                                id & 1,
                                (id >> 1) & 1,
                                (id >> 2) & 1,
                                (id >> 3) & 1
                            )
                        } else {
                            format!("[{},{},{},{}]", id, id + 1, id + 2, id + 3)
                        }
                    };
                    let values = (1..=16)
                        .map(|id| format!("({id},'{}')", vector(id)))
                        .collect::<Vec<_>>()
                        .join(",");
                    engine
                        .execute(&format!("INSERT INTO {name} VALUES {values}"))
                        .unwrap();
                    let options = if algo == "HNSW" {
                        "m=4, ef_construction=64"
                    } else {
                        "nlist=4, kmeans_iters=2"
                    };
                    engine
                        .execute(&format!(
                            "CREATE INDEX vx ON {name}(v) USING {algo} WITH ({options})"
                        ))
                        .unwrap();
                    preparation.finish().unwrap();
                    let query = if binary {
                        format!("TO_VECTOR('{}','BINARY')", vector(7))
                    } else {
                        format!("TO_VECTOR('{}')", vector(7))
                    };
                    let sql = format!("SELECT id FROM {name} ORDER BY DISTANCE(v,{query}) LIMIT 1");
                    assert_eq!(
                        scalar(&engine, &sql),
                        "7",
                        "{datatype}/{algo}/mapped={mapped}"
                    );
                    let table = engine.database().table(&format!("test.{name}")).unwrap();
                    let index = table.indexes.iter().find(|i| i.name == "vx").unwrap();
                    let graph = engine
                        .database()
                        .vector_graph_get_or_build(&table, index)
                        .unwrap();
                    let file = match graph.as_ref() {
                        VectorGraph::NativeHnsw(g) => g.file().unwrap(),
                        VectorGraph::NativeIvf(g) => g.file().unwrap(),
                        _ => panic!("native backend required"),
                    };
                    assert_eq!(
                        file.with_navigation_mapping(|_| true).is_some(),
                        mapped == "1"
                    );
                    let mut reader = SessionContext::default();
                    engine
                        .execute_with_session(
                            &mut reader,
                            "SET SESSION transaction_isolation='REPEATABLE-READ'",
                        )
                        .unwrap();
                    engine.execute_with_session(&mut reader, "BEGIN").unwrap();
                    assert_eq!(
                        support::result_rows(
                            engine.execute_with_session(&mut reader, &sql).unwrap()
                        )[0][0]
                            .to_display_string(),
                        "7"
                    );
                    engine
                        .execute(&format!("DELETE FROM {name} WHERE id=7"))
                        .unwrap();
                    assert_eq!(
                        support::result_rows(
                            engine.execute_with_session(&mut reader, &sql).unwrap()
                        )[0][0]
                            .to_display_string(),
                        "7"
                    );
                    engine
                        .execute_with_session(&mut reader, "ROLLBACK")
                        .unwrap();
                    assert_ne!(scalar(&engine, &sql), "7");
                    drop(graph);
                    queries.push(sql);
                }
            }
        }
        // Leave the committed deletes in WAL, as each original case did.
        let engine = open_strict_test_engine(dir.path());
        for sql in &queries {
            assert_ne!(scalar(&engine, sql), "7", "reopen mapped={mapped}: {sql}");
        }
        engine.database().set_physical_replica_mode(true).unwrap();
        for sql in &queries {
            assert_ne!(scalar(&engine, sql), "7", "replica mapped={mapped}: {sql}");
        }
        engine.database().set_physical_replica_mode(false).unwrap();
        for sql in &queries {
            assert_ne!(
                scalar(&engine, sql),
                "7",
                "standalone mapped={mapped}: {sql}"
            );
        }
    }
}

#[test]
fn vector_datatypes_prepared_result_domains_follow_runtime_bindings() {
    use crate::engine::api::QueryEngine as _;
    use crate::engine::params::with_params;
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let sql = "SELECT v FROM (SELECT TO_VECTOR(?,?) AS v) d";
    let statement = engine.prepare_cached(&mut session, sql, 2).unwrap();
    for binary_params in [false, true] {
        for (text, datatype, dimension) in [
            ("[1,2]", "FLOAT32", 2),
            ("[123]", "FLOAT32", 1),
            ("[1,2]", "FLOAT16", 2),
            ("[123]", "FLOAT16", 1),
            ("[0,1]", "BINARY", 2),
        ] {
            let params = if binary_params {
                vec![
                    Value::Bytes(text.as_bytes().to_vec()),
                    Value::Bytes(datatype.as_bytes().to_vec()),
                ]
            } else {
                vec![Value::String(text.into()), Value::String(datatype.into())]
            };
            let response = with_params(params, || {
                engine
                    .execute_prepared_statement(&mut session, sql, (*statement).clone())
                    .unwrap()
                    .unwrap()
            });
            let QueryResponse::ResultSet(result) = response.materialize().unwrap() else {
                panic!("result set required")
            };
            let Value::Vector(value) = &result.rows[0][0] else {
                panic!("typed vector required")
            };
            assert_eq!(
                value.datatype,
                crate::vector::VectorDataType::parse(datatype).unwrap()
            );
            assert_eq!(value.dimension, dimension);
            assert_eq!(
                result.columns[0].source.vector_type,
                Some((value.datatype, Some(dimension)))
            );
        }
    }
}

#[test]
fn vector_datatypes_cosine_index_rejects_preexisting_zero_rows() {
    let _env = HnswEnv::set(&[("MYDB_HNSW_BUILD_ON_DDL", "off")]);
    for datatype in ["FLOAT32", "FLOAT16", "BFLOAT16", "INT8"] {
        for algo in ["HNSW", "IVF"] {
            let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
            let engine = open_test_engine(dir.path());
            engine
                .execute(&format!(
                    "CREATE TABLE items(id INT PRIMARY KEY,v VECTOR(2,{datatype}) NOT NULL)"
                ))
                .unwrap();
            engine
                .execute("INSERT INTO items VALUES(1,'[0,0]')")
                .unwrap();
            assert!(
                engine
                    .execute(&format!(
                        "CREATE INDEX vx ON items(v) USING {algo} WITH (metric=cosine)"
                    ))
                    .is_err(),
                "{datatype}/{algo}"
            );
            let table = engine.database().table("test.items").unwrap();
            assert!(!table.indexes.iter().any(|i| i.name == "vx"));
        }
    }
}

#[test]
fn vector_datatypes_ivf_numeric_centers_allow_wide_native_dimensions() {
    let _env = super::vector_type::HnswEnv::set(&[("MYDB_VECTOR_STORAGE", "memory")]);
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let dimension = 16384usize;
    engine
        .execute(&format!(
            "CREATE TABLE wide_native(id INT PRIMARY KEY, v VECTOR({dimension},INT8) NOT NULL)"
        ))
        .unwrap();
    let text = |v: &str| format!("[{}]", vec![v; dimension].join(","));
    for (id, v) in [(1, "-128"), (2, "0"), (3, "127")] {
        engine
            .execute(&format!(
                "INSERT INTO wide_native VALUES({id},'{}')",
                text(v)
            ))
            .unwrap();
    }
    engine
        .execute("CREATE INDEX vx ON wide_native(v) USING IVF WITH (nlist=2, kmeans_iters=2)")
        .unwrap();
    assert_eq!(scalar(&engine, &format!("SELECT /*+ IVF_NPROBE(2) */ id FROM wide_native ORDER BY DISTANCE(v,TO_VECTOR('{}','INT8')) LIMIT 1", text("127"))), "3");
}

#[test]
fn vector_datatypes_physical_wal_replication_and_promotion() {
    physical_vector_replication_matrix(None);
}
#[test]
fn vector_quantization_sq8_physical_replication() {
    physical_vector_replication_matrix(Some("sq8"));
}
#[test]
fn vector_quantization_sq4_physical_replication() {
    physical_vector_replication_matrix(Some("sq4"));
}
#[test]
fn vector_quantization_pq_physical_replication() {
    physical_vector_replication_matrix(Some("pq"));
}
#[test]
fn vector_quantization_bq_physical_replication() {
    physical_vector_replication_matrix(Some("bq"));
}
fn physical_vector_replication_matrix(quantization: Option<&str>) {
    use std::io::{Read, Seek, SeekFrom};
    let vector = |binary: bool, value: usize| {
        if binary {
            format!(
                "[{}]",
                (0..128)
                    .map(|bit| if bit < value { "1" } else { "0" })
                    .collect::<Vec<_>>()
                    .join(",")
            )
        } else {
            format!("[{value},0,0]")
        }
    };
    for backend in ["memory", "paged"] {
        let _env = HnswEnv::set(&[
            ("MYDB_VECTOR_STORAGE", backend),
            ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
            ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
        ]);
        let source = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let target = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let mut cases = Vec::new();
        let preparation = crate::platform::fs::FixtureSyncBatch::new(source.path()).unwrap();
        // Distinct tables retain the full type/algorithm matrix while sharing
        // one physical bootstrap, replica, promotion and promoted restart.
        {
            let engine = open_strict_test_engine(source.path());
            for datatype in ["FLOAT32", "FLOAT16", "BFLOAT16", "INT8", "BINARY"] {
                if datatype == "BINARY" && quantization.is_some() {
                    continue;
                }
                for algo in ["HNSW", "IVF"] {
                    let table = format!("items_{datatype}_{algo}");
                    let binary = datatype == "BINARY";
                    let dimension = if binary { 128 } else { 3 };
                    engine.execute(&format!("CREATE TABLE {table}(id INT PRIMARY KEY,v VECTOR({dimension},{datatype}) NOT NULL)")).unwrap();
                    let rows = (1..=127)
                        .map(|id| format!("({id},'{}')", vector(binary, id)))
                        .collect::<Vec<_>>()
                        .join(",");
                    engine
                        .execute(&format!("INSERT INTO {table} VALUES {rows}"))
                        .unwrap();
                    let options = if algo == "HNSW" {
                        "m=4, ef_construction=64"
                    } else {
                        "nlist=2, kmeans_iters=2"
                    };
                    let quant_options = quantization
                        .map(|q| {
                            format!(
                                ",quantization={q},rerank_mult=32{}",
                                if q == "pq" {
                                    ",pq_bits=4,pq_train_samples=127,pq_iterations=3"
                                } else {
                                    ""
                                }
                            )
                        })
                        .unwrap_or_default();
                    engine.execute(&format!("CREATE INDEX vx ON {table}(v) USING {algo} WITH ({options}{quant_options})")).unwrap();
                    let hint = if algo == "HNSW" {
                        "HNSW_EF(64)"
                    } else {
                        "IVF_NPROBE(2)"
                    };
                    let query_type = if binary { "BINARY" } else { "FLOAT32" };
                    let sql = format!("SELECT /*+ {hint} */ id FROM {table} ORDER BY DISTANCE(v,TO_VECTOR('{}','{query_type}')) LIMIT 1", vector(binary, 0));
                    cases.push((table, datatype, binary, hint, sql));
                }
            }
        }
        preparation.finish().unwrap();
        crate::util::file::copy_fixture_tree(source.path(), target.path()).unwrap();
        let mut role = crate::repl::role::RoleState::new_standalone();
        role.demote_to_replica("127.0.0.1:1".into());
        crate::repl::role::save(target.path(), &role).unwrap();
        let master = open_strict_test_engine(source.path());
        let replica = open_strict_test_engine(target.path());
        replica.database().set_physical_replica_mode(true).unwrap();
        let mut next_lsn = replica.database().wal_log().lock().last_lsn() + 1;
        for (table, _, _, _, sql) in &cases {
            assert_eq!(scalar(&replica, sql), "1", "{backend}/{table}");
        }
        // One durable decision per DML phase covers every independent codec;
        // the replica must expose each table's result after the whole apply.
        for (phase, expected) in [(0, "128"), (1, "1"), (2, "127"), (3, "127")] {
            let mut writer = SessionContext::default();
            master.execute_with_session(&mut writer, "BEGIN").unwrap();
            for (table, _, binary, _, _) in &cases {
                let zero = vector(*binary, 0);
                let far = vector(*binary, 127);
                let dml = match phase {
                    0 => format!("INSERT INTO {table} VALUES(128,'{zero}')"),
                    1 => format!("UPDATE {table} SET v='{far}' WHERE id=128"),
                    2 => format!("UPDATE {table} SET v='{zero}' WHERE id=127"),
                    _ => format!("DELETE FROM {table} WHERE id=1"),
                };
                master
                    .execute_with_session(&mut writer, &dml)
                    .unwrap_or_else(|err| panic!("{backend}/{table}: {dml}: {err:?}"));
            }
            master.execute_with_session(&mut writer, "COMMIT").unwrap();
            master.database().wal_log().lock().flush().unwrap();
            let scan = crate::wal::LogManager::scan(source.path()).unwrap();
            let prefixes = master
                .database()
                .wal_log()
                .lock()
                .pin_prefix_after(next_lsn - 1, None)
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
                            .append_replication_frame(
                                &bytes[offset..offset + frame.frame_len],
                                &frame,
                            )
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
            for (table, _, _, hint, sql) in &cases {
                assert_eq!(
                    scalar(&replica, sql),
                    expected,
                    "{backend}/{table}: phase={phase}"
                );
                assert_eq!(
                    scalar(&replica, sql),
                    scalar(&replica, &sql.replace(hint, "NO_VECTOR_INDEX"))
                );
            }
        }
        // Match the production promotion order before opening local writes.
        replica
            .database()
            .persist_replicated_undo_for_promotion()
            .unwrap();
        replica
            .database()
            .rollback_all_active_replication_txns()
            .unwrap();
        replica
            .database()
            .restore_replicated_xa_for_promotion()
            .unwrap();
        replica.database().prepare_sequence_promotion().unwrap();
        replica.database().checkpoint().unwrap();
        replica.database().set_physical_replica_mode(false).unwrap();
        replica.database().prepare_sequence_promotion().unwrap();
        replica.database().checkpoint().unwrap();
        crate::repl::role::save(
            target.path(),
            &crate::repl::role::RoleState::new_standalone(),
        )
        .unwrap();
        for (table, _, _, _, sql) in &cases {
            assert_eq!(scalar(&replica, sql), "127", "{backend}/{table}");
        }
        let mut local = SessionContext::default();
        replica.execute_with_session(&mut local, "BEGIN").unwrap();
        for (table, _, binary, _, _) in &cases {
            replica
                .execute_with_session(
                    &mut local,
                    &format!(
                        "UPDATE {table} SET v='{}' WHERE id=127",
                        vector(*binary, 127)
                    ),
                )
                .unwrap();
            replica
                .execute_with_session(
                    &mut local,
                    &format!("INSERT INTO {table} VALUES(129,'{}')", vector(*binary, 0)),
                )
                .unwrap();
        }
        replica.execute_with_session(&mut local, "COMMIT").unwrap();
        for (table, _, _, _, sql) in &cases {
            assert_eq!(scalar(&replica, sql), "129", "{backend}/{table}");
        }
        drop(replica);
        let promoted = open_strict_test_engine(target.path());
        for (table, datatype, _, _, sql) in &cases {
            assert_eq!(
                scalar(&promoted, sql),
                "129",
                "promoted restart {backend}/{table}"
            );
            assert_eq!(
                scalar(
                    &promoted,
                    &format!("SELECT VECTOR_DATATYPE(v) FROM {table} LIMIT 1")
                ),
                *datatype
            );
        }
    }
}

#[test]
fn vector_datatypes_maximum_payload_heap_overflow_round_trip() {
    let _env = HnswEnv::set(&[("MYDB_VECTOR_STORAGE", "memory")]);
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_strict_test_engine(dir.path());
    let payload = "00".repeat(65532);
    for (datatype, dim) in [
        ("FLOAT32", 16383),
        ("FLOAT16", 32766),
        ("BFLOAT16", 32766),
        ("INT8", 65532),
        ("BINARY", 524256),
    ] {
        engine.execute(&format!("CREATE TABLE cap_{datatype}(id INT PRIMARY KEY,v VECTOR({dim},{datatype}) NOT NULL)")).unwrap();
        engine.execute(&format!("INSERT INTO cap_{datatype} VALUES(1,VECTOR_FROM_BINARY(X'{payload}','{datatype}',{dim}))")).unwrap();
        assert_eq!(
            scalar(
                &engine,
                &format!("SELECT VECTOR_DIM(v) FROM cap_{datatype}")
            ),
            dim.to_string()
        );
        assert_eq!(
            scalar(
                &engine,
                &format!("SELECT OCTET_LENGTH(VECTOR_TO_BINARY(v)) FROM cap_{datatype}")
            ),
            "65532"
        );
    }
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    for datatype in ["FLOAT32", "FLOAT16", "BFLOAT16", "INT8", "BINARY"] {
        assert_eq!(
            scalar(
                &engine,
                &format!("SELECT VECTOR_DATATYPE(v) FROM cap_{datatype}")
            ),
            datatype
        );
        assert_eq!(
            scalar(
                &engine,
                &format!("SELECT HEX(VECTOR_TO_BINARY(v)) FROM cap_{datatype}")
            ),
            payload
        );
    }
}

#[test]
fn vector_datatypes_duplicate_hnsw_postings_survive_mutations_and_restart() {
    use crate::engine::vector::{codec, VectorSearchEffort, VectorSearchRequest};
    use crate::vector::VectorDataType;
    for backend in ["memory", "paged"] {
        let _env = HnswEnv::set(&[
            ("MYDB_VECTOR_STORAGE", backend),
            ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
            ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
        ]);
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = open_strict_test_engine(dir.path());
        let preparation = crate::platform::fs::FixtureSyncBatch::new(dir.path()).unwrap();
        let mut cases = Vec::new();
        for datatype in ["FLOAT16", "BFLOAT16", "INT8", "BINARY"] {
            let metrics: &[&str] = if datatype == "BINARY" {
                &["HAMMING", "JACCARD"]
            } else {
                &["L2", "COSINE", "DOT"]
            };
            for metric in metrics {
                let name = format!("dup_{datatype}_{metric}");
                engine
                    .execute(&format!(
                        "CREATE TABLE {name}(id INT PRIMARY KEY,v VECTOR(2,{datatype}) NOT NULL)"
                    ))
                    .unwrap();
                let values = (1..=512)
                    .map(|id| format!("({id},'[0,1]')"))
                    .collect::<Vec<_>>()
                    .join(",");
                engine
                    .execute(&format!("INSERT INTO {name} VALUES {values}"))
                    .unwrap();
                engine.execute(&format!("CREATE INDEX vx ON {name}(v) USING HNSW WITH (metric={metric}, m=4, ef_construction=64)")).unwrap();
                let query =
                    codec::parse_typed_text("[0,1]", VectorDataType::parse(datatype).unwrap())
                        .unwrap();
                cases.push((name, datatype, query));
            }
        }
        preparation.finish().unwrap();
        let request = VectorSearchRequest {
            candidate_limit: 512,
            rerank_limit: 512,
            effort: VectorSearchEffort::Hnsw { ef: 512 },
        };
        let check = |engine: &StorageQueryEngine| {
            for (name, _, query) in &cases {
                let table = engine.database().table(&format!("test.{name}")).unwrap();
                let index = table.indexes.iter().find(|i| i.name == "vx").unwrap();
                let graph = engine
                    .database()
                    .vector_graph_get_or_build(&table, index)
                    .unwrap();
                let result = graph.search(query, request).unwrap();
                assert_eq!(result.candidates.len(), 512, "{backend}/{name}");
                assert_eq!(
                    result
                        .candidates
                        .iter()
                        .map(|(row, _)| *row)
                        .collect::<std::collections::HashSet<_>>()
                        .len(),
                    512,
                    "{backend}/{name}"
                );
            }
        };
        check(&engine);
        for (name, datatype, _) in &cases {
            engine
                .execute(&format!("DELETE FROM {name} WHERE id=1"))
                .unwrap();
            engine
                .execute(&format!("INSERT INTO {name} VALUES(513,'[-0,1]')"))
                .unwrap();
            let stored = query_rows(
                &engine,
                &format!("SELECT VECTOR_TO_BINARY(v) FROM {name} WHERE id=513"),
            );
            let expected =
                codec::parse_typed_text("[-0,1]", VectorDataType::parse(datatype).unwrap())
                    .unwrap();
            assert_eq!(stored[0][0], Value::Bytes(expected.payload));
        }
        check(&engine);
        drop(engine);
        let engine = open_strict_test_engine(dir.path());
        check(&engine);
    }
}

#[test]
fn every_vector_backend_loads_persistent_structure_without_heap_builds() {
    for backend in ["memory", "paged"] {
        let _env = HnswEnv::set(&[
            ("MYDB_VECTOR_STORAGE", backend),
            ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
            ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
            ("MYDB_HNSW_COMPACT_DISABLE", "1"),
        ]);
        let mut cases = Vec::new();
        for datatype in ["FLOAT32", "FLOAT16", "BFLOAT16", "INT8", "BINARY"] {
            for algorithm in ["HNSW", "IVF"] {
                cases.push((datatype, algorithm, "none"));
            }
        }
        for quantization in ["sq8", "sq4", "pq", "bq"] {
            for algorithm in ["HNSW", "IVF"] {
                cases.push(("FLOAT32", algorithm, quantization));
            }
        }
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let mut expected_cases = Vec::new();
        let scans;
        {
            // Each matrix entry has a distinct typed index; one checkpoint
            // and restart must recover all of them without heap rebuilding.
            let preparation = crate::platform::fs::FixtureSyncBatch::new(dir.path()).unwrap();
            let engine = open_strict_test_engine(dir.path());
            for (datatype, algorithm, quantization) in cases {
                let table = format!("items_{datatype}_{algorithm}_{quantization}");
                let query = if datatype == "BINARY" {
                    "[1,0,0,0]"
                } else {
                    "[1,2,3,4]"
                };
                let sql = format!("SELECT id FROM {table} ORDER BY DISTANCE(v, '{query}') LIMIT 1");
                engine
                    .execute(&format!(
                        "CREATE TABLE {table}(id INT PRIMARY KEY,v VECTOR(4,{datatype}) NOT NULL)"
                    ))
                    .unwrap();
                let mut values = Vec::new();
                for id in 1..=32 {
                    let value = if datatype == "BINARY" {
                        format!(
                            "[{},{},{},{}]",
                            id & 1,
                            (id >> 1) & 1,
                            (id >> 2) & 1,
                            (id >> 3) & 1
                        )
                    } else {
                        format!("[{id},{},{},{}]", id + 1, id + 2, id + 3)
                    };
                    values.push(format!("({id},'{value}')"));
                }
                // Recovery is exercised after the index checkpoint below.
                // Pre-index setup needs only one committed batch.
                engine
                    .execute(&format!("INSERT INTO {table} VALUES {}", values.join(",")))
                    .unwrap();
                let options = if algorithm == "HNSW" {
                    "m=4,ef_construction=32,ef_search=64"
                } else {
                    "nlist=4,nprobe=4"
                };
                let quant = if quantization == "pq" {
                    "quantization=pq,pq_m=2,pq_bits=4"
                } else {
                    ""
                };
                let quant = if quantization == "none" {
                    String::new()
                } else if quant.is_empty() {
                    format!(",quantization={quantization}")
                } else {
                    format!(",{quant}")
                };
                engine
                    .execute(&format!(
                        "CREATE INDEX vx ON {table}(v) USING {algorithm} WITH({options}{quant})"
                    ))
                    .unwrap();
                let expected = query_rows(&engine, &sql);
                assert!(!expected.is_empty());
                expected_cases.push((sql, expected));
            }
            engine.checkpoint().unwrap();
            preparation.finish().unwrap();
            scans = crate::storage::Database::vector_build_heap_scans_for_test();
        }
        let engine = open_strict_test_engine(dir.path());
        for (sql, expected) in expected_cases {
            assert_eq!(query_rows(&engine, &sql), expected, "{backend}: {sql}");
            assert_eq!(
                crate::storage::Database::vector_build_heap_scans_for_test(),
                scans,
                "normal restart rebuilt {backend}: {sql}"
            );
            assert!(query_rows(&engine, "SHOW WARNINGS").is_empty());
        }
    }
}

#[test]
fn vector_index_crash_replays_committed_deltas_without_heap_builds() {
    const NAME: &str = "engine::executor::tests::vector_datatypes::vector_index_crash_replays_committed_deltas_without_heap_builds";
    const CASES: [(&str, &str, &str); 6] = [
        ("FLOAT32", "HNSW", "none"),
        ("FLOAT16", "HNSW", "none"),
        ("BFLOAT16", "IVF", "none"),
        ("INT8", "IVF", "none"),
        ("FLOAT32", "HNSW", "sq8"),
        ("FLOAT32", "IVF", "pq"),
    ];
    let table_name = |(datatype, algorithm, quantization): &(&str, &str, &str)| {
        format!("items_{datatype}_{algorithm}_{quantization}")
    };
    if let Ok(root) = std::env::var("MYDB_VECTOR_CRASH_CHILD") {
        let backend = std::env::var("MYDB_VECTOR_CRASH_BACKEND").unwrap();
        let phase = std::env::var("MYDB_VECTOR_CRASH_PHASE")
            .unwrap()
            .parse::<u64>()
            .unwrap();
        let selection = std::env::var("MYDB_VECTOR_CRASH_CASE").unwrap();
        let selected = if selection == "all" {
            None
        } else {
            Some(selection.parse::<usize>().unwrap())
        };
        assert!(selected.is_none_or(|i| i < CASES.len()));
        let _env = HnswEnv::set(&[
            ("MYDB_VECTOR_STORAGE", &backend),
            ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
            ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
            ("MYDB_HNSW_COMPACT_DISABLE", "1"),
        ]);
        let engine = open_strict_test_engine(std::path::Path::new(&root));
        let mut writer = SessionContext::default();
        engine.execute_with_session(&mut writer, "BEGIN").unwrap();
        for (i, case) in CASES.iter().enumerate() {
            if selected.is_some_and(|selected| selected != i) {
                continue;
            }
            let name = table_name(case);
            for sql in [
                format!("UPDATE {name} SET v='[-100,-100,-100,-100]' WHERE id=1"),
                format!("DELETE FROM {name} WHERE id=2"),
                format!("INSERT INTO {name} VALUES(999,'[0,0,0,0]')"),
            ] {
                engine.execute_with_session(&mut writer, &sql).unwrap();
            }
        }
        engine.database().set_vector_crash_phase_for_test(phase);
        engine.execute_with_session(&mut writer, "COMMIT").unwrap();
        assert_eq!(phase, 3);
        std::fs::write(
            std::path::Path::new(&root).join("acknowledged"),
            b"COMMIT returned success",
        )
        .unwrap();
        std::process::exit(86);
    }
    for backend in ["memory", "paged"] {
        let _env = HnswEnv::set(&[
            ("MYDB_VECTOR_STORAGE", backend),
            ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
            ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
            ("MYDB_HNSW_COMPACT_DISABLE", "1"),
        ]);
        let base = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let preparation = crate::platform::fs::FixtureSyncBatch::new(base.path()).unwrap();
        let engine = open_strict_test_engine(base.path());
        for case @ (datatype, algorithm, quantization) in &CASES {
            let name = table_name(case);
            engine
                .execute(&format!(
                    "CREATE TABLE {name}(id INT PRIMARY KEY,v VECTOR(4,{datatype}) NOT NULL)"
                ))
                .unwrap();
            let values = (1..=32)
                .map(|id| format!("({id},'[{id},{},{},{}]')", id + 1, id + 2, id + 3))
                .collect::<Vec<_>>()
                .join(",");
            engine
                .execute(&format!("INSERT INTO {name} VALUES {values}"))
                .unwrap();
            let options = if *algorithm == "HNSW" {
                "m=4,ef_construction=32,ef_search=128"
            } else {
                "nlist=4,nprobe=4"
            };
            let quant = if *quantization == "pq" {
                ",quantization=pq,pq_m=2,pq_bits=4".to_owned()
            } else {
                format!(",quantization={quantization}")
            };
            engine
                .execute(&format!(
                    "CREATE INDEX vx ON {name}(v) USING {algorithm} WITH({options}{quant})"
                ))
                .unwrap();
        }
        engine.checkpoint().unwrap();
        drop(engine);
        preparation.finish().unwrap();
        for phase in 1..=6 {
            if backend == "memory" && (phase == 4 || phase == 5) {
                continue;
            }
            // Phases 1/2/3 follow the whole journal/decision/apply boundary,
            // so every codec participates in the same interrupted transaction.
            // Keep page-write phases 4/5 and pre-decision phase 6 per codec:
            // their hooks must independently be reachable for every backend.
            let selections: Vec<Option<usize>> = if phase <= 3 {
                vec![None]
            } else {
                (0..CASES.len()).map(Some).collect()
            };
            for selected in selections {
                let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
                crate::util::file::copy_fixture_tree(base.path(), dir.path()).unwrap();
                let status = std::process::Command::new(std::env::current_exe().unwrap())
                    .args(["--exact", NAME, "--nocapture"])
                    .env("MYDB_VECTOR_CRASH_CHILD", dir.path())
                    .env("MYDB_VECTOR_CRASH_BACKEND", backend)
                    .env(
                        "MYDB_VECTOR_CRASH_CASE",
                        selected.map_or_else(|| "all".to_owned(), |i| i.to_string()),
                    )
                    .env("MYDB_VECTOR_CRASH_PHASE", phase.to_string())
                    .output()
                    .unwrap();
                assert_eq!(
                    status.status.code(),
                    Some(86),
                    "{backend}/{selected:?}/phase={phase}: {}",
                    String::from_utf8_lossy(&status.stderr)
                );
                if phase == 3 {
                    assert!(dir.path().join("acknowledged").exists());
                }
                let scans = crate::storage::Database::vector_build_heap_scans_for_test();
                let engine = open_strict_test_engine(dir.path());
                let query = crate::engine::vector::codec::parse_text("[0,0,0,0]").unwrap();
                for (i, case @ (_, algorithm, _)) in CASES.iter().enumerate() {
                    let name = table_name(case);
                    let committed = phase != 6 && selected.is_none_or(|selected| selected == i);
                    let db = engine.database();
                    let table = db.table(&name).unwrap();
                    let index = table
                        .indexes
                        .iter()
                        .find(|index| index.is_vector())
                        .unwrap();
                    let graph = db.vector_graph_get_or_build(&table, index).unwrap();
                    assert_eq!(graph.len(), 32, "{backend}/{name}/phase={phase}");
                    let candidates = graph
                        .search(
                            &query,
                            crate::engine::vector::VectorSearchRequest {
                                candidate_limit: 64,
                                rerank_limit: 64,
                                effort: if *algorithm == "HNSW" {
                                    crate::engine::vector::VectorSearchEffort::Hnsw { ef: 4096 }
                                } else {
                                    crate::engine::vector::VectorSearchEffort::Ivf { nprobe: 4096 }
                                },
                            },
                        )
                        .unwrap();
                    let actual = candidates
                        .candidates
                        .into_iter()
                        .map(|(row, _)| row)
                        .collect::<std::collections::HashSet<_>>();
                    let expected = db
                        .scan_rows(
                            &table.qualified_name(),
                            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
                        )
                        .unwrap()
                        .into_iter()
                        .map(|row| row.row_id)
                        .collect::<std::collections::HashSet<_>>();
                    assert_eq!(actual.len(), 32);
                    assert_eq!(
                        actual, expected,
                        "raw index must cover committed heap rows: {backend}/{name}/phase={phase}"
                    );
                    assert_eq!(
                        scalar(
                            &engine,
                            &format!(
                                "SELECT id FROM {name} ORDER BY DISTANCE(v,'[0,0,0,0]') LIMIT 1"
                            )
                        ),
                        if committed { "999" } else { "1" },
                        "{backend}/{name}/phase={phase}"
                    );
                    let tail = format!(
                        "id,DISTANCE(v,'[0,0,0,0]') AS d FROM {name} ORDER BY d,id LIMIT 32"
                    );
                    assert_eq!(
                        query_rows(&engine, &format!("SELECT {tail}")),
                        query_rows(&engine, &format!("SELECT /*+ NO_VECTOR_INDEX */ {tail}")),
                        "{backend}/{name}/phase={phase}"
                    );
                    assert_eq!(
                        scalar(&engine, &format!("SELECT COUNT(*) FROM {name} WHERE id=2")),
                        if committed { "0" } else { "1" }
                    );
                    if !committed {
                        assert_eq!(
                            scalar(
                                &engine,
                                &format!("SELECT COUNT(*) FROM {name} WHERE id=999")
                            ),
                            "0"
                        );
                    }
                    assert_eq!(
                        crate::storage::Database::vector_build_heap_scans_for_test(),
                        scans
                    );
                    assert!(query_rows(&engine, "SHOW WARNINGS").is_empty());
                }
            }
        }
    }
}

#[test]
fn hnsw_retirements_survive_checkpoint_with_old_reader_and_process_exit() {
    const NAME: &str = "engine::executor::tests::vector_datatypes::hnsw_retirements_survive_checkpoint_with_old_reader_and_process_exit";
    if let Ok(root) = std::env::var("MYDB_RETIREMENT_CRASH_CHILD") {
        let backend = std::env::var("MYDB_RETIREMENT_CRASH_BACKEND").unwrap();
        let _env = HnswEnv::set(&[
            ("MYDB_VECTOR_STORAGE", &backend),
            ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
            ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
        ]);
        let engine = open_strict_test_engine(std::path::Path::new(&root));
        engine
            .execute("CREATE TABLE items(id INT PRIMARY KEY,v VECTOR(4) NOT NULL)")
            .unwrap();
        for id in 1..=40 {
            engine
                .execute(&format!("INSERT INTO items VALUES({id},'[{id},1,2,3]')"))
                .unwrap();
        }
        engine
            .execute(
                "CREATE INDEX vx ON items(v) USING HNSW WITH(m=4,ef_construction=32,ef_search=128)",
            )
            .unwrap();
        engine
            .execute("DELETE FROM items WHERE id>=10 AND id<=20")
            .unwrap();
        engine.database().maintain_vector_indexes(|| false).unwrap();
        let mut reader = SessionContext::default();
        engine
            .execute_with_session(
                &mut reader,
                "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
            )
            .unwrap();
        engine.execute_with_session(&mut reader, "BEGIN").unwrap();
        engine
            .execute_with_session(&mut reader, "SELECT id FROM items WHERE id=1")
            .unwrap();
        engine.execute("DELETE FROM items WHERE id=1").unwrap();
        engine
            .execute("UPDATE items SET v='[100,100,100,100]' WHERE id=2")
            .unwrap();
        engine
            .execute("INSERT INTO items VALUES(999,'[0,0,0,0]')")
            .unwrap();
        assert!(engine.database().pending_vector_retirements_for_test() >= 2);
        engine.checkpoint().unwrap();
        assert!(engine.database().pending_vector_retirements_for_test() >= 2);
        std::process::exit(86);
    }
    for backend in ["memory", "paged"] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", NAME, "--nocapture"])
            .env("MYDB_RETIREMENT_CRASH_CHILD", dir.path())
            .env("MYDB_RETIREMENT_CRASH_BACKEND", backend)
            .output()
            .unwrap();
        assert_eq!(
            status.status.code(),
            Some(86),
            "{backend}: {}",
            String::from_utf8_lossy(&status.stderr)
        );
        let _env = HnswEnv::set(&[
            ("MYDB_VECTOR_STORAGE", backend),
            ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
            ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
        ]);
        let scans = crate::storage::Database::vector_build_heap_scans_for_test();
        let engine = open_strict_test_engine(dir.path());
        assert_eq!(engine.database().pending_vector_retirements_for_test(), 0);
        let table = engine.database().table("test.items").unwrap();
        let index = table
            .indexes
            .iter()
            .find(|index| index.name == "vx")
            .unwrap();
        let graph = engine
            .database()
            .vector_graph_get_or_build_for_partition(&table, index, table.id)
            .unwrap();
        assert_eq!(
            graph.len(),
            29,
            "old physical node versions were not retired: {backend}"
        );
        assert_eq!(
            scalar(
                &engine,
                "SELECT id FROM items ORDER BY DISTANCE(v,'[0,0,0,0]') LIMIT 1"
            ),
            "999"
        );
        assert_eq!(
            crate::storage::Database::vector_build_heap_scans_for_test(),
            scans
        );
    }
}

#[test]
fn vector_checkpoint_publication_failures_preserve_wal_and_committed_results() {
    for backend in ["memory", "paged"] {
        for (datatype, algorithm, quantization) in [
            ("FLOAT32", "HNSW", "none"),
            ("FLOAT16", "HNSW", "none"),
            ("INT8", "IVF", "none"),
            ("FLOAT32", "HNSW", "sq8"),
            ("BFLOAT16", "IVF", "sq4"),
            ("FLOAT32", "IVF", "pq"),
        ] {
            let _env = HnswEnv::set(&[
                ("MYDB_VECTOR_STORAGE", backend),
                ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
                ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
                ("MYDB_HNSW_COMPACT_DISABLE", "1"),
            ]);
            let prepare = |path: &std::path::Path| {
                let preparation = crate::platform::fs::FixtureSyncBatch::new(path).unwrap();
                let engine = open_strict_test_engine(path);
                engine
                    .execute(&format!(
                        "CREATE TABLE items(id INT PRIMARY KEY,v VECTOR(4,{datatype}) NOT NULL)"
                    ))
                    .unwrap();
                let values = (1..=20)
                    .map(|id| format!("({id},'[{id},1,2,3]')"))
                    .collect::<Vec<_>>()
                    .join(",");
                engine
                    .execute(&format!("INSERT INTO items VALUES {values}"))
                    .unwrap();
                let params = if algorithm == "HNSW" {
                    "m=4,ef_construction=32"
                } else {
                    "nlist=2,nprobe=2"
                };
                let quant = if quantization == "pq" {
                    "quantization=pq,pq_m=2,pq_bits=4".to_owned()
                } else {
                    format!("quantization={quantization}")
                };
                engine
                    .execute(&format!(
                        "CREATE INDEX vx ON items(v) USING {algorithm} WITH({params},{quant})"
                    ))
                    .unwrap();
                engine.checkpoint().unwrap();
                preparation.finish().unwrap();
                engine
            };
            // Publication stages are independent failures over one immutable
            // checkpoint. Preserve a separate private image and recovery for
            // every stage rather than rebuilding its identical input.
            let base = (backend == "memory").then(|| {
                let base = tempfile::tempdir_in("/tmp/mydb").unwrap();
                drop(prepare(base.path()));
                base
            });
            for stage in if backend == "memory" {
                vec![1, 2, 3, 4, 5]
            } else {
                vec![6]
            } {
                let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
                let engine = if let Some(base) = &base {
                    crate::util::file::copy_fixture_tree(base.path(), dir.path()).unwrap();
                    open_strict_test_engine(dir.path())
                } else {
                    prepare(dir.path())
                };
                engine
                    .execute("UPDATE items SET v='[0,0,0,0]' WHERE id=20")
                    .unwrap();
                let ends = |root: &std::path::Path| {
                    // Recovery scans may truncate the active tail. Freeze and
                    // flush the writer, then inspect without modifying its file.
                    let log = engine.database().wal_log();
                    let mut writer = log.lock();
                    writer.flush().unwrap();
                    crate::wal::LogManager::scan_with_mode(root, crate::wal::WalReadMode::Strict)
                        .unwrap()
                        .records
                        .iter()
                        .filter(|r| {
                            matches!(r.payload, crate::wal::WalPayload::CheckpointEnd { .. })
                        })
                        .count()
                };
                let before = ends(dir.path());
                engine
                    .database()
                    .inject_vector_persist_failure_for_test(stage);
                let err = engine.checkpoint().unwrap_err();
                assert!(
                    err.message()
                        .contains("injected vector persistence failure"),
                    "{backend}/{datatype}/{algorithm}/{stage}: {err:?}"
                );
                assert_eq!(
                    ends(dir.path()),
                    before,
                    "a failed publication emitted CheckpointEnd"
                );
                drop(engine);
                let scans = crate::storage::Database::vector_build_heap_scans_for_test();
                let reopened = open_strict_test_engine(dir.path());
                let sql = "SELECT id FROM items ORDER BY DISTANCE(v,'[0,0,0,0]') LIMIT 1";
                assert_eq!(
                    scalar(&reopened, sql),
                    "20",
                    "{backend}/{datatype}/{algorithm}/{stage}"
                );
                assert_eq!(
                    crate::storage::Database::vector_build_heap_scans_for_test(),
                    scans
                );
                assert!(query_rows(&reopened, "SHOW WARNINGS").is_empty());
            }
        }
    }
}

#[test]
fn prepared_vector_decision_after_snapshot_recovers_all_persistent_backends() {
    use crate::engine::vector::{VectorSearchEffort, VectorSearchRequest};
    for durability in [
        crate::storage::DurabilityMode::Strict,
        crate::storage::DurabilityMode::Relaxed,
    ] {
        for backend in ["memory", "paged"] {
            let _env = HnswEnv::set(&[
                ("MYDB_VECTOR_STORAGE", backend),
                ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
                ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
                ("MYDB_HNSW_COMPACT_DISABLE", "1"),
            ]);
            let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
            let open = || {
                let mut config = memory_test_storage_config();
                config.durability = durability;
                config.buffer_pool_pages = 2048;
                StorageQueryEngine::open_with_storage_config_without_background_workers(
                    dir.path(),
                    config,
                )
                .unwrap()
            };
            let engine = open();
            let preparation = crate::platform::fs::FixtureSyncBatch::new(dir.path()).unwrap();
            let mut cases = Vec::new();
            for (datatype, algorithm, quantization) in [
                ("FLOAT32", "HNSW", "none"),
                ("FLOAT16", "HNSW", "none"),
                ("INT8", "IVF", "none"),
                ("FLOAT32", "HNSW", "sq4"),
                ("FLOAT32", "IVF", "sq8"),
                ("FLOAT32", "HNSW", "bq"),
                ("FLOAT32", "IVF", "pq"),
            ] {
                let name = format!("items_{datatype}_{algorithm}_{quantization}");
                engine
                    .execute(&format!(
                        "CREATE TABLE {name}(id INT PRIMARY KEY,v VECTOR(4,{datatype}) NOT NULL)"
                    ))
                    .unwrap();
                let values = (1..=20)
                    .map(|id| format!("({id},'[{id},1,2,3]')"))
                    .collect::<Vec<_>>()
                    .join(",");
                engine
                    .execute(&format!("INSERT INTO {name} VALUES {values}"))
                    .unwrap();
                let params = if algorithm == "HNSW" {
                    "m=4,ef_construction=32,ef_search=128"
                } else {
                    "nlist=2,nprobe=2"
                };
                let quant = if quantization == "pq" {
                    "quantization=pq,pq_m=2,pq_bits=4".to_owned()
                } else {
                    format!("quantization={quantization}")
                };
                engine
                    .execute(&format!(
                        "CREATE INDEX vx ON {name}(v) USING {algorithm} WITH({params},{quant})"
                    ))
                    .unwrap();
                cases.push((name, algorithm));
            }
            preparation.finish().unwrap();
            // The completion interruption occurs after commit_txn has applied
            // the entire journal. One XA covers every independent typed index
            // at exactly that same decision/checkpoint boundary.
            let mut session = SessionContext::default();
            engine
                .execute_with_session(&mut session, "XA START 'decision-after-snapshot'")
                .unwrap();
            for (name, _) in &cases {
                engine
                    .execute_with_session(
                        &mut session,
                        &format!("INSERT INTO {name} VALUES(999,'[0,0,0,0]')"),
                    )
                    .unwrap();
            }
            engine
                .execute_with_session(&mut session, "XA END 'decision-after-snapshot'")
                .unwrap();
            engine
                .execute_with_session(&mut session, "XA PREPARE 'decision-after-snapshot'")
                .unwrap();
            engine.checkpoint().unwrap();
            drop(engine);
            let recovered = open();
            let count_batches = || {
                crate::wal::LogManager::scan(dir.path())
                    .unwrap()
                    .records
                    .iter()
                    .filter(|r| {
                        matches!(r.payload, crate::wal::WalPayload::VectorIndexBatch { .. })
                    })
                    .count()
            };
            let before = count_batches();
            assert!(
                before > 0,
                "PREPARED must retain its immutable index journal"
            );
            recovered.database().test_interrupt_vector_completion();
            let err = recovered
                .execute("XA COMMIT 'decision-after-snapshot'")
                .unwrap_err();
            assert!(
                err.message()
                    .contains("injected XA vector completion interruption"),
                "{err:?}"
            );
            assert_eq!(
                count_batches(),
                before,
                "a PREPARED immutable journal must not be re-logged at COMMIT"
            );
            recovered.database().wal_log().lock().flush().unwrap();
            drop(recovered);
            let scans = crate::storage::Database::vector_build_heap_scans_for_test();
            let reopened = open();
            for (name, algorithm) in &cases {
                assert_eq!(
                    scalar(
                        &reopened,
                        &format!("SELECT id FROM {name} ORDER BY DISTANCE(v,'[0,0,0,0]') LIMIT 1")
                    ),
                    "999",
                    "{durability:?}/{backend}/{name}"
                );
                let db = reopened.database();
                let table = db.table(name).unwrap();
                let index = table.indexes.iter().find(|i| i.name == "vx").unwrap();
                let graph = db.vector_graph_get_or_build(&table, index).unwrap();
                assert_eq!(graph.len(), 21, "{durability:?}/{backend}/{name}");
                let query = crate::engine::vector::codec::parse_text("[0,0,0,0]").unwrap();
                let result = graph
                    .search(
                        &query,
                        VectorSearchRequest {
                            candidate_limit: 32,
                            rerank_limit: 32,
                            effort: if *algorithm == "HNSW" {
                                VectorSearchEffort::Hnsw { ef: 4096 }
                            } else {
                                VectorSearchEffort::Ivf { nprobe: 4096 }
                            },
                        },
                    )
                    .unwrap();
                let actual = result
                    .candidates
                    .into_iter()
                    .map(|(row, _)| row)
                    .collect::<std::collections::HashSet<_>>();
                let expected = db
                    .scan_rows(
                        &table.qualified_name(),
                        TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
                    )
                    .unwrap()
                    .into_iter()
                    .map(|row| row.row_id)
                    .collect::<std::collections::HashSet<_>>();
                assert_eq!(
                    actual, expected,
                    "raw index must cover every committed row: {backend}/{name}"
                );
                assert_eq!(
                    crate::storage::Database::vector_build_heap_scans_for_test(),
                    scans
                );
                assert!(query_rows(&reopened, "SHOW WARNINGS").is_empty());
            }
        }
    }
}

#[test]
fn vector_source_images_survive_partial_transfer_restartpoints_without_heap_builds() {
    use std::io::{Read, Seek, SeekFrom};
    fn copy_tree(source: &std::path::Path, target: &std::path::Path) {
        crate::util::file::copy_fixture_tree(source, target).unwrap();
    }
    fn project(
        master: &StorageQueryEngine,
        replica: &StorageQueryEngine,
        source: &std::path::Path,
        after: u64,
        through: u64,
    ) {
        for prefix in master
            .database()
            .wal_log()
            .lock()
            .pin_prefix_after(after, None)
            .unwrap()
        {
            let mut file = prefix.file.open().unwrap().try_clone().unwrap();
            file.seek(SeekFrom::Start(prefix.start_offset)).unwrap();
            let mut bytes = Vec::new();
            file.take(prefix.len - prefix.start_offset)
                .read_to_end(&mut bytes)
                .unwrap();
            let mut offset = 0;
            while offset < bytes.len() {
                let frame = crate::wal::decode_frame_for_replication(&bytes[offset..]).unwrap();
                if frame.header.lsn <= through
                    && frame.header.lsn > replica.database().wal_log().lock().last_lsn()
                {
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
        let scan = crate::wal::LogManager::scan(source).unwrap();
        for record in scan
            .records
            .iter()
            .filter(|record| record.lsn > after && record.lsn <= through)
        {
            replica.database().apply_wal_record(record).unwrap();
        }
    }
    for backend in ["memory", "paged"] {
        for algorithm in ["HNSW", "IVF"] {
            let _env = HnswEnv::set(&[
                ("MYDB_VECTOR_STORAGE", backend),
                ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
                ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
                ("MYDB_HNSW_COMPACT_DISABLE", "1"),
            ]);
            let source = tempfile::tempdir_in("/tmp/mydb").unwrap();
            let target = tempfile::tempdir_in("/tmp/mydb").unwrap();
            {
                let preparation =
                    crate::platform::fs::FixtureSyncBatch::new(source.path()).unwrap();
                let engine = open_strict_test_engine(source.path());
                engine
                    .execute("CREATE TABLE items(id INT PRIMARY KEY,v VECTOR(2048) NOT NULL)")
                    .unwrap();
                let values = (1..=130)
                    .map(|id| {
                        let vector = std::iter::repeat_n(id.to_string(), 2048)
                            .collect::<Vec<_>>()
                            .join(",");
                        format!("({id},'[{vector}]')")
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                engine
                    .execute(&format!("INSERT INTO items VALUES {values}"))
                    .unwrap();
                let options = if algorithm == "HNSW" {
                    "m=4,ef_construction=32,ef_search=256"
                } else {
                    "nlist=4,nprobe=4,kmeans_iters=2"
                };
                engine
                    .execute(&format!(
                        "CREATE INDEX vx ON items(v) USING {algorithm} WITH({options})"
                    ))
                    .unwrap();
                engine.checkpoint().unwrap();
                drop(engine);
                preparation.finish().unwrap();
            }
            crate::util::file::copy_fixture_tree(source.path(), target.path()).unwrap();
            let mut role = crate::repl::role::RoleState::new_standalone();
            role.demote_to_replica("127.0.0.1:1".into());
            crate::repl::role::save(target.path(), &role).unwrap();
            let master = open_strict_test_engine(source.path());
            let replica = open_strict_test_engine(target.path());
            replica.database().set_physical_replica_mode(true).unwrap();
            let table = master.database().table("items").unwrap();
            let index = table
                .indexes
                .iter()
                .find(|index| index.is_vector())
                .unwrap();
            let before = replica.database().wal_log().lock().last_lsn();
            let old_lineage = replica
                .database()
                .vector_graph_get_or_build(&table, index)
                .unwrap()
                .identity_lineage();
            let query = format!(
                "SELECT id FROM items ORDER BY DISTANCE(v,'[{}]') LIMIT 1",
                std::iter::repeat_n("0", 2048).collect::<Vec<_>>().join(",")
            );
            assert_eq!(scalar(&replica, &query), "1");
            master
                .database()
                .rebuild_vector_for_test(&table, index, table.id)
                .unwrap();
            master.database().wal_log().lock().flush().unwrap();
            let scan = crate::wal::LogManager::scan(source.path()).unwrap();
            let images = scan
                .records
                .iter()
                .filter(|record| {
                    record.lsn > before
                        && matches!(
                            record.payload,
                            crate::wal::WalPayload::VectorIndexImage { .. }
                        )
                })
                .collect::<Vec<_>>();
            assert!(
                images.len() >= 2,
                "{backend}/{algorithm}: image must span multiple WAL chunks"
            );
            let first = images[0].lsn;
            let image_path = match &images[0].payload {
                crate::wal::WalPayload::VectorIndexImage { path, .. } => path.clone(),
                _ => unreachable!(),
            };
            project(&master, &replica, source.path(), before, first);
            replica.checkpoint().unwrap();
            assert!(
                crate::wal::LogManager::scan(target.path())
                    .unwrap()
                    .records
                    .iter()
                    .any(|record| record.lsn == first),
                "restartpoint reclaimed an incomplete image prefix"
            );
            drop(replica);
            // Emulate loss of unsynchronized staging bytes. WAL must reconstruct
            // this private file from its retained first chunk after a crash.
            let incoming = target.path().join(&image_path).with_extension(format!(
                "{}.incoming",
                std::path::Path::new(&image_path)
                    .extension()
                    .unwrap()
                    .to_str()
                    .unwrap()
            ));
            assert!(incoming.exists());
            std::fs::write(&incoming, []).unwrap();
            let scans = crate::storage::Database::vector_build_heap_scans_for_test();
            let fork = tempfile::tempdir_in("/tmp/mydb").unwrap();
            copy_tree(target.path(), fork.path());
            let promoted = open_strict_test_engine(fork.path());
            promoted
                .database()
                .persist_replicated_undo_for_promotion()
                .unwrap();
            promoted
                .database()
                .rollback_all_active_replication_txns()
                .unwrap();
            promoted
                .database()
                .restore_replicated_xa_for_promotion()
                .unwrap();
            promoted.database().prepare_sequence_promotion().unwrap();
            promoted.checkpoint().unwrap();
            promoted
                .database()
                .set_physical_replica_mode(false)
                .unwrap();
            promoted.database().prepare_sequence_promotion().unwrap();
            promoted.checkpoint().unwrap();
            crate::repl::role::save(fork.path(), &crate::repl::role::RoleState::new_standalone())
                .unwrap();
            let abandoned = fork
                .path()
                .join(incoming.strip_prefix(target.path()).unwrap());
            assert!(
                !abandoned.exists(),
                "promotion must retire an unpublished partial image"
            );
            assert_eq!(scalar(&promoted, &query), "1");
            assert_eq!(
                promoted
                    .database()
                    .vector_graph_get_or_build(&table, index)
                    .unwrap()
                    .identity_lineage(),
                old_lineage
            );
            drop(promoted);
            let promoted = open_strict_test_engine(fork.path());
            assert_eq!(scalar(&promoted, &query), "1");
            drop(promoted);
            let replica = open_strict_test_engine(target.path());
            assert_eq!(scalar(&replica, &query), "1");
            assert_eq!(
                replica
                    .database()
                    .vector_graph_get_or_build(&table, index)
                    .unwrap()
                    .identity_lineage(),
                old_lineage,
                "a local checkpoint must preserve the source structure identity"
            );
            assert!(
                std::fs::metadata(&incoming).unwrap().len() > 0,
                "partial image was not reconstructed from WAL"
            );
            project(&master, &replica, source.path(), first, scan.next_lsn - 1);
            let new_path = replica
                .database()
                .active_vector_file_for_test(&table, index, table.id)
                .unwrap();
            let published = master
                .database()
                .active_vector_file_for_test(&table, index, table.id)
                .unwrap();
            assert_eq!(
                new_path,
                target
                    .path()
                    .join(published.strip_prefix(source.path()).unwrap()),
                "source publication must supersede a local checkpoint pointer"
            );
            let published_lineage = master
                .database()
                .vector_graph_get_or_build(&table, index)
                .unwrap()
                .identity_lineage();
            assert_ne!(published_lineage, old_lineage);
            assert_eq!(
                replica
                    .database()
                    .vector_graph_get_or_build(&table, index)
                    .unwrap()
                    .identity_lineage(),
                published_lineage
            );
            assert_eq!(scalar(&replica, &query), "1");
            assert_eq!(
                crate::storage::Database::vector_build_heap_scans_for_test(),
                scans
            );
            assert!(query_rows(&replica, "SHOW WARNINGS").is_empty());
        }
    }
}

#[test]
fn copy_checkpoint_between_decision_and_graph_transfer_preserves_index_journal() {
    fn copy_tree(source: &std::path::Path, target: &std::path::Path) {
        crate::util::file::copy_fixture_tree(source, target).unwrap();
    }
    let _env = HnswEnv::set(&[
        ("MYDB_VECTOR_STORAGE", "memory"),
        ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
        ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
    ]);
    for (algorithm, quantization) in [
        ("HNSW", "none"),
        ("IVF", "none"),
        ("HNSW", "sq8"),
        ("IVF", "pq"),
    ] {
        let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let source = root.path().join("source");
        let recovered = root.path().join("recovered");
        let engine = std::sync::Arc::new(open_strict_test_engine(&source));
        engine
            .execute("CREATE TABLE items(id INT PRIMARY KEY,extra INT,v VECTOR(4) NOT NULL)")
            .unwrap();
        for id in 1..=32 {
            engine
                .execute(&format!(
                    "INSERT INTO items VALUES({id},{id},'[{id},1,2,3]')"
                ))
                .unwrap();
        }
        let options = if algorithm == "HNSW" {
            "m=4,ef_construction=32,ef_search=128"
        } else {
            "nlist=2,nprobe=2"
        };
        let quant = if quantization == "pq" {
            "quantization=pq,pq_m=2,pq_bits=4".to_owned()
        } else {
            format!("quantization={quantization}")
        };
        engine
            .execute(&format!(
                "CREATE INDEX vx ON items(v) USING {algorithm} WITH({options},{quant})"
            ))
            .unwrap();
        engine.checkpoint().unwrap();
        let catchup = engine.database().install_online_copy_publish_test_hook();
        let committed = engine.database().install_vector_copy_commit_test_hook();
        let worker = engine.clone();
        let ddl = std::thread::spawn(move || {
            let mut session = SessionContext::default();
            session.connection_id = 8131;
            worker.execute_with_session(
                &mut session,
                "ALTER TABLE items MODIFY extra BIGINT, LOCK=NONE",
            )
        });
        let reached = catchup.wait_until_reached(std::time::Duration::from_secs(10));
        if !reached {
            catchup.release();
            committed.release();
            panic!("COPY did not reach catch-up: {:?}", ddl.join().unwrap());
        }
        let mut writer = SessionContext::default();
        writer.connection_id = 8132;
        for sql in [
            "UPDATE items SET v='[100,100,100,100]' WHERE id=1",
            "DELETE FROM items WHERE id=2",
            "INSERT INTO items VALUES(999,999,'[0,0,0,0]')",
        ] {
            engine.execute_with_session(&mut writer, sql).unwrap();
        }
        catchup.release();
        let reached = committed.wait_until_reached(std::time::Duration::from_secs(10));
        if !reached {
            committed.release();
            panic!(
                "COPY did not reach the committed transfer gap: {:?}",
                ddl.join().unwrap()
            );
        }
        let checkpoint = engine.checkpoint();
        if checkpoint.is_ok() {
            copy_tree(&source, &recovered);
        }
        committed.release();
        ddl.join().unwrap().unwrap();
        checkpoint.unwrap();
        let scans = crate::storage::Database::vector_build_heap_scans_for_test();
        let restarted = open_strict_test_engine(&recovered);
        let tail = "id,DISTANCE(v,'[0,0,0,0]') AS d FROM items ORDER BY d,id LIMIT 32";
        assert_eq!(
            scalar(
                &restarted,
                "SELECT id FROM items ORDER BY DISTANCE(v,'[0,0,0,0]') LIMIT 1"
            ),
            "999",
            "{algorithm}/{quantization}"
        );
        assert_eq!(
            query_rows(&restarted, &format!("SELECT {tail}")),
            query_rows(&restarted, &format!("SELECT /*+ NO_VECTOR_INDEX */ {tail}")),
            "{algorithm}/{quantization}"
        );
        assert_eq!(
            scalar(&restarted, "SELECT COUNT(*) FROM items WHERE id=2"),
            "0"
        );
        assert_eq!(
            crate::storage::Database::vector_build_heap_scans_for_test(),
            scans
        );
        assert!(query_rows(&restarted, "SHOW WARNINGS").is_empty());
    }
}

#[test]
fn failed_vector_rebuild_keeps_old_index_and_resumes_its_durable_job_after_restart() {
    let _env = HnswEnv::set(&[
        ("MYDB_VECTOR_STORAGE", "memory"),
        ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
        ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
        ("MYDB_HNSW_COMPACT_DISABLE", "1"),
    ]);
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items(id INT PRIMARY KEY,v VECTOR(4) NOT NULL)")
        .unwrap();
    for id in 1..=32 {
        engine
            .execute(&format!("INSERT INTO items VALUES({id},'[{id},1,2,3]')"))
            .unwrap();
    }
    engine.execute("CREATE INDEX vx ON items(v) USING HNSW WITH(m=4,ef_construction=32,ef_search=256,quantization=sq8)").unwrap();
    let table = engine.database().table("items").unwrap();
    let index = table
        .indexes
        .iter()
        .find(|index| index.is_vector())
        .unwrap();
    let old_uid = engine
        .database()
        .vector_graph_get_or_build(&table, index)
        .unwrap()
        .identity_lineage();
    for id in 100..164 {
        engine
            .execute(&format!(
                "INSERT INTO items VALUES({id},'[{},1,2,3]')",
                1000 + id
            ))
            .unwrap();
    }
    engine.database().inject_vector_persist_failure_for_test(1);
    let err = engine
        .database()
        .maintain_vector_indexes(|| false)
        .unwrap_err();
    assert!(
        err.message()
            .contains("injected vector persistence failure"),
        "{err:?}"
    );
    assert!(engine.database().ensure_operational().is_ok());
    assert_eq!(
        engine
            .database()
            .vector_graph_get_or_build(&table, index)
            .unwrap()
            .identity_lineage(),
        old_uid
    );
    let marker = crate::storage::naming::index_object_path_by_id(
        dir.path(),
        table.id,
        table.active_generation.max(1),
        index.index_id,
        "vmaintenance",
    );
    let first = std::fs::read(&marker).unwrap();
    assert_eq!(
        scalar(
            &engine,
            "SELECT id FROM items ORDER BY DISTANCE(v,'[1100,1,2,3]') LIMIT 1"
        ),
        "100"
    );
    engine.database().inject_vector_persist_failure_for_test(1);
    assert!(engine.database().maintain_vector_indexes(|| false).is_err());
    assert_eq!(
        std::fs::read(&marker).unwrap(),
        first,
        "a retry replaced the first durable failure"
    );
    engine.checkpoint().unwrap();
    drop(engine);
    let scans = crate::storage::Database::vector_build_heap_scans_for_test();
    let restarted = open_strict_test_engine(dir.path());
    assert_eq!(
        crate::storage::Database::vector_build_heap_scans_for_test(),
        scans,
        "startup rebuilt a failed job's valid old object"
    );
    assert_eq!(
        restarted
            .database()
            .vector_graph_get_or_build(&table, index)
            .unwrap()
            .identity_lineage(),
        old_uid
    );
    assert!(marker.exists());
    restarted
        .database()
        .maintain_vector_indexes(|| false)
        .unwrap();
    assert!(
        !marker.exists(),
        "successful explicit maintenance did not retire its durable failure"
    );
    assert_ne!(
        restarted
            .database()
            .vector_graph_get_or_build(&table, index)
            .unwrap()
            .identity_lineage(),
        old_uid
    );
    assert_eq!(
        scalar(
            &restarted,
            "SELECT id FROM items ORDER BY DISTANCE(v,'[1100,1,2,3]') LIMIT 1"
        ),
        "100"
    );
    assert!(query_rows(&restarted, "SHOW WARNINGS").is_empty());
}

#[test]
fn checkpoint_during_committed_vector_batch_never_overstates_snapshot_coverage() {
    fn copy_tree(source: &std::path::Path, target: &std::path::Path) {
        crate::util::file::copy_fixture_tree(source, target).unwrap();
    }
    let _env = HnswEnv::set(&[
        ("MYDB_VECTOR_STORAGE", "memory"),
        ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
        ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
        ("MYDB_HNSW_COMPACT_DISABLE", "1"),
    ]);
    for (datatype, algorithm, quantization) in [
        ("FLOAT32", "HNSW", "none"),
        ("FLOAT16", "HNSW", "none"),
        ("INT8", "IVF", "none"),
        ("FLOAT32", "HNSW", "sq8"),
        ("FLOAT32", "IVF", "pq"),
    ] {
        let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let source = root.path().join("source");
        let target = root.path().join("recovered");
        let open = |path: &std::path::Path| {
            let mut config = memory_test_storage_config();
            config.durability = crate::storage::DurabilityMode::Strict;
            config.undo_retention = std::time::Duration::ZERO;
            config.buffer_pool_pages = 2048;
            StorageQueryEngine::open_with_storage_config_without_background_workers(path, config)
                .unwrap()
        };
        let engine = std::sync::Arc::new(open(&source));
        engine
            .execute(&format!(
                "CREATE TABLE items(id INT PRIMARY KEY,v VECTOR(4,{datatype}) NOT NULL)"
            ))
            .unwrap();
        for id in 1..=20 {
            engine
                .execute(&format!("INSERT INTO items VALUES({id},'[{id},1,2,3]')"))
                .unwrap();
        }
        let options = if algorithm == "HNSW" {
            "m=4,ef_construction=32,ef_search=128"
        } else {
            "nlist=2,nprobe=2"
        };
        let quant = if quantization == "pq" {
            "quantization=pq,pq_m=2,pq_bits=4".to_owned()
        } else {
            format!("quantization={quantization}")
        };
        engine
            .execute(&format!(
                "CREATE INDEX vx ON items(v) USING {algorithm} WITH({options},{quant})"
            ))
            .unwrap();
        engine.checkpoint().unwrap();
        let hook = engine.database().install_vector_apply_test_hook();
        let worker = engine.clone();
        let writer = std::thread::spawn(move || {
            let mut session = SessionContext::default();
            for sql in [
                "BEGIN",
                "UPDATE items SET v='[100,100,100,100]' WHERE id=1",
                "INSERT INTO items VALUES(999,'[0,0,0,0]')",
                "DELETE FROM items WHERE id=2",
                "COMMIT",
            ] {
                worker.execute_with_session(&mut session, sql).unwrap();
            }
        });
        let reached = hook.wait_until_reached(std::time::Duration::from_secs(10));
        if !reached {
            hook.release();
            writer.join().unwrap();
            panic!("vector maintenance did not pause");
        }
        let checkpoint = engine.checkpoint();
        if checkpoint.is_ok() {
            copy_tree(&source, &target);
        }
        hook.release();
        writer.join().unwrap();
        checkpoint.unwrap();
        let scans = crate::storage::Database::vector_build_heap_scans_for_test();
        let recovered = open(&target);
        assert_eq!(
            scalar(
                &recovered,
                "SELECT id FROM items ORDER BY DISTANCE(v,'[0,0,0,0]') LIMIT 1"
            ),
            "999",
            "{datatype}/{algorithm}/{quantization}"
        );
        let tail = "id,DISTANCE(v,'[0,0,0,0]') d FROM items ORDER BY d,id LIMIT 20";
        assert_eq!(
            query_rows(&recovered, &format!("SELECT {tail}")),
            query_rows(&recovered, &format!("SELECT /*+ NO_VECTOR_INDEX */ {tail}"))
        );
        assert_eq!(scalar(&recovered,"SELECT LIVE_NODES FROM information_schema.mydb_vector_indexes WHERE INDEX_NAME='vx'"),"20","a partial retirement collector left live superseded versions");
        assert_eq!(
            crate::storage::Database::vector_build_heap_scans_for_test(),
            scans
        );
        assert!(query_rows(&recovered, "SHOW WARNINGS").is_empty());
    }
}

#[test]
fn storage_single_insert_uses_durable_deferred_vector_maintenance() {
    for backend in ["memory", "paged"] {
        for (datatype, algorithm) in [("FLOAT32", "HNSW"), ("FLOAT16", "HNSW"), ("INT8", "IVF")] {
            let _env = HnswEnv::set(&[
                ("MYDB_VECTOR_STORAGE", backend),
                ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
                ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
            ]);
            let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
            {
                let engine = open_strict_test_engine(dir.path());
                engine
                    .execute(&format!(
                        "CREATE TABLE items(id INT PRIMARY KEY,v VECTOR(4,{datatype}) NOT NULL)"
                    ))
                    .unwrap();
                for id in 1..=20 {
                    engine
                        .execute(&format!("INSERT INTO items VALUES({id},'[{id},1,2,3]')"))
                        .unwrap();
                }
                let options = if algorithm == "HNSW" {
                    "m=4,ef_construction=32,ef_search=128"
                } else {
                    "nlist=2,nprobe=2"
                };
                engine
                    .execute(&format!(
                        "CREATE INDEX vx ON items(v) USING {algorithm} WITH({options})"
                    ))
                    .unwrap();
                engine.checkpoint().unwrap();
                let db = engine.database();
                let table = db.table("items").unwrap();
                let index = table
                    .indexes
                    .iter()
                    .find(|index| index.is_vector())
                    .unwrap();
                let dtype = match datatype {
                    "FLOAT16" => crate::vector::VectorDataType::Float16,
                    "INT8" => crate::vector::VectorDataType::Int8,
                    _ => crate::vector::VectorDataType::Float32,
                };
                let value =
                    crate::engine::vector::codec::parse_typed_text("[0,0,0,0]", dtype).unwrap();
                let txn = db.begin_dml_txn().unwrap();
                db.insert_row(
                    txn,
                    "test.items",
                    vec![Value::Int(999), Value::Vector(value.clone())],
                )
                .unwrap();
                assert_eq!(
                    db.vector_graph_get_or_build(&table, index).unwrap().len(),
                    20,
                    "single-row storage insert mutated the public graph before its decision"
                );
                db.rollback_txn(txn.txn_id).unwrap();
                assert_eq!(
                    db.vector_graph_get_or_build(&table, index).unwrap().len(),
                    20,
                    "rollback left a graph ghost"
                );
                let txn = db.begin_dml_txn().unwrap();
                db.insert_row(
                    txn,
                    "test.items",
                    vec![Value::Int(1000), Value::Vector(value)],
                )
                .unwrap();
                db.commit_txn(txn.txn_id).unwrap();
            }
            let scans = crate::storage::Database::vector_build_heap_scans_for_test();
            let reopened = open_strict_test_engine(dir.path());
            assert_eq!(
                scalar(
                    &reopened,
                    "SELECT id FROM items ORDER BY DISTANCE(v,'[0,0,0,0]') LIMIT 1"
                ),
                "1000",
                "{backend}/{datatype}/{algorithm}"
            );
            assert_eq!(
                scalar(&reopened, "SELECT COUNT(*) FROM items WHERE id=999"),
                "0"
            );
            assert_eq!(
                crate::storage::Database::vector_build_heap_scans_for_test(),
                scans
            );
            assert!(query_rows(&reopened, "SHOW WARNINGS").is_empty());
        }
    }
}

#[test]
fn pinned_readers_keep_linear_vector_retirement_obligations() {
    for backend in ["memory", "paged"] {
        let _env = HnswEnv::set(&[
            ("MYDB_VECTOR_STORAGE", backend),
            ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
            ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
            ("MYDB_HNSW_UPDATE_INPLACE", "0"),
            ("MYDB_HNSW_COMPACT_DISABLE", "1"),
        ]);
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = open_strict_test_engine(dir.path());
        for sql in [
            "CREATE TABLE items(id INT PRIMARY KEY,v VECTOR(2) NOT NULL)",
            "INSERT INTO items VALUES(1,'[0,0]')",
            "CREATE INDEX vx ON items(v) USING HNSW WITH(m=4,ef_construction=32)",
        ] {
            engine.execute(sql).unwrap();
        }
        let mut reader = SessionContext::default();
        for sql in [
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
            "BEGIN",
            "SELECT COUNT(*) FROM items",
        ] {
            engine.execute_with_session(&mut reader, sql).unwrap();
        }
        for id in 1..=64 {
            engine
                .execute(&format!("UPDATE items SET v='[{id},0]' WHERE id=1"))
                .unwrap();
        }
        assert_eq!(
            engine
                .database()
                .pending_vector_retirement_versions_for_test(),
            64,
            "{backend}: superseded versions were scheduled repeatedly"
        );
        let old = super::support::result_rows(
            engine
                .execute_with_session(
                    &mut reader,
                    "SELECT VECTOR_TO_STRING(v) FROM items WHERE id=1",
                )
                .unwrap()
                .materialize()
                .unwrap(),
        );
        assert_eq!(old.len(), 1);
        assert_eq!(old[0][0].to_display_string(), "[0,0]");
        engine.execute_with_session(&mut reader, "COMMIT").unwrap();
        engine.database().maintain_vector_indexes(|| false).unwrap();
        assert_eq!(
            engine
                .database()
                .pending_vector_retirement_versions_for_test(),
            0
        );
        assert_eq!(scalar(&engine,"SELECT LIVE_NODES FROM information_schema.mydb_vector_indexes WHERE INDEX_NAME='vx'"),"1");
        assert_eq!(
            scalar(
                &engine,
                "SELECT VECTOR_TO_STRING(v) FROM items ORDER BY DISTANCE(v,'[64,0]') LIMIT 1"
            ),
            "[64,0]"
        );
    }
}

#[test]
fn resident_checkpoint_retirements_stay_bounded_without_background_workers() {
    fn snapshot_count(root: &std::path::Path) -> usize {
        std::fs::read_dir(root)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                if entry.file_type().unwrap().is_dir() {
                    snapshot_count(&entry.path())
                } else {
                    usize::from(entry.path().extension().is_some_and(|ext| ext == "hnsw"))
                }
            })
            .sum()
    }
    let _env = HnswEnv::set(&[
        ("MYDB_VECTOR_STORAGE", "memory"),
        ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
        ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
    ]);
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let preparation = crate::platform::fs::FixtureSyncBatch::new(dir.path()).unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut fixtures = Vec::new();
    for (algorithm, options) in [
        ("HNSW", "m=4,ef_construction=32"),
        ("IVF", "nlist=2,nprobe=2,quantization=pq,pq_m=2,pq_bits=4"),
    ] {
        let name = format!("items_{}", algorithm.to_ascii_lowercase());
        engine
            .execute(&format!(
                "CREATE TABLE {name}(id INT PRIMARY KEY,v VECTOR(4) NOT NULL)"
            ))
            .unwrap();
        let values = (1..=32)
            .map(|id| format!("({id},'[{id},1,2,3]')"))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO {name} VALUES {values}"))
            .unwrap();
        engine
            .execute(&format!(
                "CREATE INDEX vx ON {name}(v) USING {algorithm} WITH({options})"
            ))
            .unwrap();
        let table = engine.database().table(&name).unwrap();
        let index = table
            .indexes
            .iter()
            .find(|index| index.is_vector())
            .unwrap()
            .clone();
        let initial = engine
            .database()
            .active_vector_file_for_test(&table, &index, table.id)
            .unwrap();
        fixtures.push((name, table, index, initial));
    }
    preparation.finish().unwrap();
    for _ in 0..3 {
        engine.checkpoint().unwrap();
    }
    for (name, table, index, initial) in &fixtures {
        assert_eq!(
            engine
                .database()
                .active_vector_file_for_test(table, index, table.id)
                .unwrap(),
            *initial,
            "clean checkpoints must preserve the already durable object: {name}"
        );
    }
    let mut session = SessionContext::default();
    session.connection_id = 8135;
    // Both independent index families cross every one of the original seventy
    // durable checkpoint boundaries. Check each directory separately so one
    // family's missing snapshot cannot hide another family's leaked object.
    for iteration in 0..70 {
        engine.execute_with_session(&mut session, "BEGIN").unwrap();
        for (name, _, _, _) in &fixtures {
            engine
                .execute_with_session(
                    &mut session,
                    &format!(
                        "UPDATE {name} SET v='[1,1,2,{}]' WHERE id=1",
                        3 + iteration % 2
                    ),
                )
                .unwrap();
        }
        engine.execute_with_session(&mut session, "COMMIT").unwrap();
        engine.checkpoint().unwrap();
        for (name, _, _, initial) in &fixtures {
            assert_eq!(
                snapshot_count(initial.parent().unwrap()),
                1,
                "retired resident files must be reclaimed: {name}"
            );
        }
    }
    for (name, _, _, _) in &fixtures {
        let coverage = scalar(&engine, &format!("SELECT LAST_PERSISTED_LSN FROM information_schema.mydb_vector_indexes WHERE TABLE_NAME='{name}' AND INDEX_NAME='vx'"));
        assert!(
            coverage.parse::<u64>().unwrap() > 0,
            "resident coverage must be observable: {name}"
        );
    }
    drop(engine);
    let scans = crate::storage::Database::vector_build_heap_scans_for_test();
    let engine = open_strict_test_engine(dir.path());
    for (name, _, _, _) in &fixtures {
        assert_eq!(
            scalar(
                &engine,
                &format!("SELECT id FROM {name} ORDER BY DISTANCE(v,'[0,1,2,3]') LIMIT 1")
            ),
            "1",
            "{name}"
        );
    }
    assert_eq!(
        crate::storage::Database::vector_build_heap_scans_for_test(),
        scans
    );
    assert!(query_rows(&engine, "SHOW WARNINGS").is_empty());
    let root = dir.path().join("system/vector-purges.v1");
    let mut damaged = std::fs::read(&root).unwrap();
    // The checked vector-state envelope adds its independent u16 version.
    assert_eq!(damaged.len(), 19);
    damaged[0] ^= 1;
    std::fs::write(&root, &damaged).unwrap();
    let completed = || {
        crate::wal::LogManager::scan(dir.path())
            .unwrap()
            .records
            .iter()
            .filter(|record| matches!(record.payload, crate::wal::WalPayload::CheckpointEnd { .. }))
            .count()
    };
    let before = completed();
    let err = engine.checkpoint().unwrap_err();
    assert!(err.is(crate::error::catalog::ER_INDEX_CORRUPT), "{err:?}");
    assert_eq!(
        completed(),
        before,
        "corrupt mandatory state cannot publish CheckpointEnd"
    );
    assert!(
        engine.execute("SELECT 1").is_err(),
        "known root corruption must fence service"
    );
    assert_eq!(
        std::fs::read(&root).unwrap(),
        damaged,
        "checkpoint replaced corruption evidence"
    );
    drop(engine);
    let mut config = memory_test_storage_config();
    config.durability = crate::storage::DurabilityMode::Strict;
    assert!(
        StorageQueryEngine::open_with_storage_config_without_background_workers(dir.path(), config)
            .is_err()
    );
    assert_eq!(std::fs::read(&root).unwrap(), damaged);
}

#[test]
fn physical_replica_explain_preserves_authoritative_statistics_and_index_state() {
    for backend in ["memory", "paged"] {
        let _env = HnswEnv::set(&[
            ("MYDB_VECTOR_STORAGE", backend),
            ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
            ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
        ]);
        for (datatype, algorithm) in [("FLOAT32", "HNSW"), ("FLOAT16", "HNSW"), ("FLOAT32", "IVF")]
        {
            let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
            let engine = open_strict_test_engine(dir.path());
            engine.execute(&format!("CREATE TABLE items(id INT PRIMARY KEY,v VECTOR(4,{datatype}) NOT NULL,body TEXT)")).unwrap();
            let values = (1..=128)
                .map(|id| format!("({id},'[{id},1,2,3]','persisted database index')"))
                .collect::<Vec<_>>()
                .join(",");
            engine
                .execute(&format!("INSERT INTO items VALUES {values}"))
                .unwrap();
            let options = if algorithm == "HNSW" {
                "m=4,ef_construction=32,ef_search=256"
            } else {
                "nlist=4,nprobe=4"
            };
            engine
                .execute(&format!(
                    "CREATE INDEX vx ON items(v) USING {algorithm} WITH({options})"
                ))
                .unwrap();
            engine
                .execute("CREATE FULLTEXT INDEX ft ON items(body)")
                .unwrap();
            engine.database().set_physical_replica_mode(true).unwrap();
            let table = engine.database().table("items").unwrap();
            let before = (
                table.schema_version,
                serde_json::to_string(&table.stats).unwrap(),
                engine.database().table_modification_delta(table.id),
            );
            assert!(table.stats_needs_refresh_with_delta(before.2));
            let lsn = engine.database().wal_log().lock().last_lsn();
            let scans = crate::storage::Database::vector_build_heap_scans_for_test();
            engine
                .database()
                .refresh_stale_stats_for_tables(["items"])
                .unwrap();
            assert_eq!(engine.database().auto_analyze_stale_tables(0.1).unwrap(), 0);
            let hint = if algorithm == "HNSW" {
                "HNSW_EF(256)"
            } else {
                "IVF_NPROBE(4)"
            };
            let sql = format!(
                "SELECT /*+ {hint} */ id FROM items ORDER BY DISTANCE(v,'[0,1,2,3]') LIMIT 1"
            );
            let plan = query_rows(&engine, &format!("EXPLAIN {sql}"));
            assert!(
                plan.iter()
                    .flatten()
                    .any(|value| value.to_display_string().contains("Using vector index")),
                "{plan:?}"
            );
            assert_eq!(scalar(&engine, &sql), "1");
            assert_eq!(
                scalar(
                    &engine,
                    "SELECT COUNT(*) FROM items WHERE MATCH(body) AGAINST('persisted')"
                ),
                "128"
            );
            let after = engine.database().table("items").unwrap();
            assert_eq!(
                (
                    after.schema_version,
                    serde_json::to_string(&after.stats).unwrap(),
                    engine.database().table_modification_delta(after.id)
                ),
                before
            );
            assert_eq!(
                engine.database().wal_log().lock().last_lsn(),
                lsn,
                "a replica query appended local WAL"
            );
            assert_eq!(
                crate::storage::Database::vector_build_heap_scans_for_test(),
                scans
            );
            let err = engine.database().analyze_table("items").unwrap_err();
            assert!(
                err.is(crate::error::catalog::ER_OPTION_PREVENTS_STATEMENT),
                "{err:?}"
            );
            assert_eq!(
                scalar(&engine, "SELECT 1"),
                "1",
                "rejected maintenance must not fail-stop a healthy replica"
            );
            assert_eq!(engine.database().wal_log().lock().last_lsn(), lsn);
        }
    }
}
