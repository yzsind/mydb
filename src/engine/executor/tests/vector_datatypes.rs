//! Native vector datatype SQL regressions.
use super::support::query_rows;
use super::vector_type::HnswEnv;
use super::*;

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
            for algo in ["HNSW", "IVF", "IVF_SQ8"] {
                if binary && algo == "IVF_SQ8" {
                    continue;
                }
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
                    "M=4 EF_CONSTRUCTION=64"
                } else {
                    "NLIST=4 KMEANS_ITERS=2"
                };
                engine.execute(&format!("CREATE INDEX vx ON items(v) USING {algo} COMMENT 'METRIC={metric} {options}'")).unwrap();
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
        for datatype in ["FLOAT16", "BFLOAT16", "INT8", "BINARY"] {
            for algo in ["HNSW", "IVF"] {
                let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
                let engine = open_strict_test_engine(dir.path());
                let binary = datatype == "BINARY";
                let dim = if binary { 9 } else { 4 };
                engine.execute(&format!("CREATE TABLE items(id INT PRIMARY KEY,v VECTOR({dim},{datatype}) NOT NULL)")).unwrap();
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
                for id in 1..=16 {
                    engine
                        .execute(&format!("INSERT INTO items VALUES({id},'{}')", vector(id)))
                        .unwrap();
                }
                let options = if algo == "HNSW" {
                    "M=4 EF_CONSTRUCTION=64"
                } else {
                    "NLIST=4 KMEANS_ITERS=2"
                };
                engine
                    .execute(&format!(
                        "CREATE INDEX vx ON items(v) USING {algo} COMMENT '{options}'"
                    ))
                    .unwrap();
                let query = if binary {
                    format!("TO_VECTOR('{}','BINARY')", vector(7))
                } else {
                    format!("TO_VECTOR('{}')", vector(7))
                };
                let sql = format!("SELECT id FROM items ORDER BY DISTANCE(v,{query}) LIMIT 1");
                assert_eq!(
                    scalar(&engine, &sql),
                    "7",
                    "{datatype}/{algo}/mapped={mapped}"
                );
                let table = engine.database().table("test.items").unwrap();
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
                    support::result_rows(engine.execute_with_session(&mut reader, &sql).unwrap())
                        [0][0]
                        .to_display_string(),
                    "7"
                );
                engine.execute("DELETE FROM items WHERE id=7").unwrap();
                assert_eq!(
                    support::result_rows(engine.execute_with_session(&mut reader, &sql).unwrap())
                        [0][0]
                        .to_display_string(),
                    "7"
                );
                engine
                    .execute_with_session(&mut reader, "ROLLBACK")
                    .unwrap();
                assert_ne!(scalar(&engine, &sql), "7");
                drop(graph);
                drop(engine);
                let engine = open_strict_test_engine(dir.path());
                assert_ne!(scalar(&engine, &sql), "7");
                engine.database().set_physical_replica_mode(true).unwrap();
                assert_ne!(scalar(&engine, &sql), "7");
                engine.database().set_physical_replica_mode(false).unwrap();
                assert_ne!(scalar(&engine, &sql), "7");
            }
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
        for algo in ["HNSW", "IVF", "IVF_SQ8"] {
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
                        "CREATE INDEX vx ON items(v) USING {algo} COMMENT 'METRIC=COSINE'"
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
        .execute("CREATE INDEX vx ON wide_native(v) USING IVF COMMENT 'NLIST=2 KMEANS_ITERS=2'")
        .unwrap();
    assert_eq!(scalar(&engine, &format!("SELECT /*+ IVF_NPROBE(2) */ id FROM wide_native ORDER BY DISTANCE(v,TO_VECTOR('{}','INT8')) LIMIT 1", text("127"))), "3");
}

#[test]
fn vector_datatypes_physical_wal_replication_and_promotion() {
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
    for backend in ["memory", "paged"] {
        let _env = HnswEnv::set(&[
            ("MYDB_VECTOR_STORAGE", backend),
            ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
            ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
        ]);
        for datatype in ["FLOAT32", "FLOAT16", "BFLOAT16", "INT8", "BINARY"] {
            for algo in ["HNSW", "IVF", "IVF_SQ8"] {
                if datatype == "BINARY" && algo == "IVF_SQ8" {
                    continue;
                }
                let source = tempfile::tempdir_in("/tmp/mydb").unwrap();
                let target = tempfile::tempdir_in("/tmp/mydb").unwrap();
                let binary = datatype == "BINARY";
                let dimension = if binary { 128 } else { 3 };
                let vector = |value: usize| {
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
                let zero = vector(0);
                let far = vector(127);
                {
                    let engine = open_strict_test_engine(source.path());
                    engine.execute(&format!("CREATE TABLE items(id INT PRIMARY KEY,v VECTOR({dimension},{datatype}) NOT NULL)")).unwrap();
                    let rows = (1..=127)
                        .map(|id| format!("({id},'{}')", vector(id)))
                        .collect::<Vec<_>>()
                        .join(",");
                    engine
                        .execute(&format!("INSERT INTO items VALUES {rows}"))
                        .unwrap();
                    let options = if algo == "HNSW" {
                        "M=4 EF_CONSTRUCTION=64"
                    } else {
                        "NLIST=2 KMEANS_ITERS=2"
                    };
                    engine
                        .execute(&format!(
                            "CREATE INDEX vx ON items(v) USING {algo} COMMENT '{options}'"
                        ))
                        .unwrap();
                }
                copy_tree(source.path(), target.path());
                let mut role = crate::repl::role::RoleState::new_standalone();
                role.demote_to_replica("127.0.0.1:1".into());
                crate::repl::role::save(target.path(), &role).unwrap();
                let master = open_strict_test_engine(source.path());
                let replica = open_strict_test_engine(target.path());
                replica.database().set_physical_replica_mode(true).unwrap();
                let hint = if algo == "HNSW" {
                    "HNSW_EF(64)"
                } else {
                    "IVF_NPROBE(2)"
                };
                let query_type = if binary { "BINARY" } else { "FLOAT32" };
                let sql = format!("SELECT /*+ {hint} */ id FROM items ORDER BY DISTANCE(v,TO_VECTOR('{}','{query_type}')) LIMIT 1", zero);
                assert_eq!(scalar(&replica, &sql), "1");
                let mut next_lsn = replica.database().wal_log().lock().last_lsn() + 1;
                for (dml, expected) in [
                    (format!("INSERT INTO items VALUES(128,'{zero}')"), "128"),
                    (format!("UPDATE items SET v='{far}' WHERE id=128"), "1"),
                    (format!("UPDATE items SET v='{zero}' WHERE id=127"), "127"),
                    ("DELETE FROM items WHERE id=1".into(), "127"),
                ] {
                    master.execute(&dml).unwrap_or_else(|err| {
                        panic!("{backend}/{datatype}/{algo}: {dml}: {err:?}")
                    });
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
                            let frame =
                                crate::wal::decode_frame_for_replication(&bytes[offset..]).unwrap();
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
                    assert_eq!(
                        scalar(&replica, &sql),
                        expected,
                        "{backend}/{datatype}/{algo}: {dml}"
                    );
                    assert_eq!(
                        scalar(&replica, &sql),
                        scalar(&replica, &sql.replace(hint, "NO_VECTOR_INDEX"))
                    );
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
                assert_eq!(scalar(&replica, &sql), "127");
                replica
                    .execute(&format!("UPDATE items SET v='{far}' WHERE id=127"))
                    .unwrap();
                replica
                    .execute(&format!("INSERT INTO items VALUES(129,'{}')", zero))
                    .unwrap();
                assert_eq!(scalar(&replica, &sql), "129");
                drop(replica);
                let promoted = open_strict_test_engine(target.path());
                assert_eq!(
                    scalar(&promoted, &sql),
                    "129",
                    "promoted restart {backend}/{datatype}/{algo}"
                );
                assert_eq!(
                    scalar(&promoted, "SELECT VECTOR_DATATYPE(v) FROM items LIMIT 1"),
                    datatype
                );
            }
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
        for datatype in ["FLOAT16", "BFLOAT16", "INT8", "BINARY"] {
            let metrics: &[&str] = if datatype == "BINARY" {
                &["HAMMING", "JACCARD"]
            } else {
                &["L2", "COSINE", "DOT"]
            };
            for metric in metrics {
                let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
                let engine = open_strict_test_engine(dir.path());
                engine
                    .execute(&format!(
                        "CREATE TABLE dup(id INT PRIMARY KEY,v VECTOR(2,{datatype}) NOT NULL)"
                    ))
                    .unwrap();
                let values = (1..=512)
                    .map(|id| format!("({id},'[0,1]')"))
                    .collect::<Vec<_>>()
                    .join(",");
                engine
                    .execute(&format!("INSERT INTO dup VALUES {values}"))
                    .unwrap();
                engine
                .execute(&format!("CREATE INDEX vx ON dup(v) USING HNSW COMMENT 'METRIC={metric} M=4 EF_CONSTRUCTION=64'"))
                .unwrap();
                let query =
                    codec::parse_typed_text("[0,1]", VectorDataType::parse(datatype).unwrap())
                        .unwrap();
                let request = VectorSearchRequest {
                    candidate_limit: 512,
                    rerank_limit: 512,
                    effort: VectorSearchEffort::Hnsw { ef: 512 },
                };
                let check = |engine: &StorageQueryEngine| {
                    let table = engine.database().table("test.dup").unwrap();
                    let index = table.indexes.iter().find(|i| i.name == "vx").unwrap();
                    let graph = engine
                        .database()
                        .vector_graph_get_or_build(&table, index)
                        .unwrap();
                    let result = graph.search(&query, request).unwrap();
                    assert_eq!(result.candidates.len(), 512, "{backend}/{datatype}");
                    assert_eq!(
                        result
                            .candidates
                            .iter()
                            .map(|(row, _)| *row)
                            .collect::<std::collections::HashSet<_>>()
                            .len(),
                        512
                    );
                };
                check(&engine);
                engine.execute("DELETE FROM dup WHERE id=1").unwrap();
                engine
                    .execute("INSERT INTO dup VALUES(513,'[-0,1]')")
                    .unwrap();
                check(&engine);
                let stored =
                    query_rows(&engine, "SELECT VECTOR_TO_BINARY(v) FROM dup WHERE id=513");
                let expected =
                    codec::parse_typed_text("[-0,1]", VectorDataType::parse(datatype).unwrap())
                        .unwrap();
                assert_eq!(stored[0][0], Value::Bytes(expected.payload));
                drop(engine);
                let engine = open_strict_test_engine(dir.path());
                check(&engine);
            }
        }
    }
}
