//! Vector WITH options, compressed payloads and heap re-ranking regressions.
use super::support::query_rows;
use super::vector_type::HnswEnv;
use super::*;

fn text(engine: &StorageQueryEngine, sql: &str) -> String {
    query_rows(engine, sql)[0][0].to_display_string()
}
fn options(algo: &str, q: &str) -> String {
    let algorithm = if algo == "HNSW" {
        "m=4,ef_construction=64,ef_search=64"
    } else {
        "nlist=4,nprobe=4,kmeans_iters=2"
    };
    format!(
        "{algorithm},quantization={q},rerank_mult=32{}",
        if q == "pq" {
            ",pq_m=3,pq_bits=4,pq_train_samples=32,pq_iterations=3"
        } else {
            ""
        }
    )
}
fn populate(engine: &StorageQueryEngine) {
    let rows = (0..32)
        .map(|id| {
            format!(
                "({id},'[{}]')",
                (0..7)
                    .map(|j| ((id * (j + 3) + j * j) % 39 - 19).to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO items VALUES {rows}"))
        .unwrap();
}
fn matrix(backend: &str) {
    let _env = HnswEnv::set(&[
        ("MYDB_VECTOR_STORAGE", backend),
        ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
        ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
    ]);
    let mut count = 0;
    for datatype in ["FLOAT32", "FLOAT16", "BFLOAT16", "INT8"] {
        for metric in ["L2", "COSINE", "DOT"] {
            for algo in ["HNSW", "IVF"] {
                for quant in ["none", "sq8", "sq4", "pq", "bq"] {
                    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
                    let engine = open_strict_test_engine(dir.path());
                    engine.execute(&format!("CREATE TABLE items(id INT PRIMARY KEY,v VECTOR(7,{datatype}) NOT NULL)")).unwrap();
                    populate(&engine);
                    let ddl=format!("CREATE INDEX vx ON items(v) USING {algo} WITH (metric={metric},{}) COMMENT 'owner=search'",options(algo,quant));
                    engine
                        .execute(&ddl)
                        .unwrap_or_else(|e| panic!("{backend} {datatype} {ddl}: {e}"));
                    let tail=format!("id, DISTANCE(v,TO_VECTOR('[0.123,1.937,-2.781,3.619,-4.483,5.271,-6.157]'),'{metric}') d FROM items ORDER BY d LIMIT 32");
                    let sql = format!("SELECT /*+ HNSW_EF(64) IVF_NPROBE(64) */ {tail}");
                    let exact =
                        query_rows(&engine, &format!("SELECT /*+ NO_VECTOR_INDEX */ {tail}"));
                    let actual = query_rows(&engine, &sql);
                    assert_eq!(
                        actual, exact,
                        "{backend} {datatype} {algo} {quant} {metric}"
                    );
                    let explain = query_rows(&engine, &format!("EXPLAIN {sql}"));
                    assert!(
                        explain
                            .iter()
                            .flatten()
                            .any(|v| v.to_display_string().contains("Using vector index")),
                        "{explain:?}"
                    );
                    assert_eq!(text(&engine,"SELECT QUANTIZATION FROM information_schema.mydb_vector_indexes WHERE INDEX_NAME='vx'"),quant);
                    if quant != "none" {
                        let work=query_rows(&engine,"SELECT QUANTIZATION_STATE,QUANTIZER_BYTES,COMPRESSED_BYTES,HEAP_RERANKS,EXACT_FALLBACKS FROM information_schema.mydb_vector_indexes WHERE INDEX_NAME='vx'");
                        assert_eq!(work[0][0].to_display_string(), "READY");
                        assert!(work[0][1].to_display_string().parse::<u64>().unwrap() > 0);
                        assert!(work[0][2].to_display_string().parse::<u64>().unwrap() > 0);
                        assert!(work[0][3].to_display_string().parse::<u64>().unwrap() >= 32);
                        assert_eq!(work[0][4].to_display_string(), "0");
                    }
                    let show =
                        query_rows(&engine, "SHOW CREATE TABLE items")[0][1].to_display_string();
                    assert!(show.contains(&format!("quantization={quant}")), "{show}");
                    engine
                        .execute(&show.replacen("`items`", "`copy`", 1))
                        .unwrap();
                    assert_eq!(text(&engine,"SELECT INDEX_COMMENT FROM information_schema.statistics WHERE TABLE_NAME='copy' AND INDEX_NAME='vx'"),"owner=search");
                    count += 1;
                }
            }
        }
    }
    assert_eq!(count, 120);
}
#[test]
fn vector_quantization_matrix_memory() {
    matrix("memory");
}
#[test]
fn vector_quantization_matrix_paged() {
    matrix("paged");
}

#[test]
fn vector_quantization_rejects_invalid_options_and_binary() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine.execute("CREATE TABLE items(id INT PRIMARY KEY,v VECTOR(7) NOT NULL,b VECTOR(7,BINARY) NOT NULL)").unwrap();
    for option in [
        "typo=1",
        "m=1",
        "m=1025",
        "m=64,ef_construction=32",
        "ef_construction=65536",
        "m='16'",
        "quantization=pq,pq_m=8",
        "quantization=pq,pq_bits=5",
        "quantization=pq,pq_train_samples=255",
        "quantization=sq8,pq_m=1",
        "nlist=4",
        "metric=hamming",
    ] {
        let sql = format!("CREATE INDEX bad ON items(v) USING HNSW WITH ({option})");
        assert!(engine.execute(&sql).is_err(), "{sql}");
    }
    for q in ["sq8", "sq4", "pq", "bq"] {
        assert!(engine
            .execute(&format!(
                "CREATE INDEX bad ON items(b) USING IVF WITH (quantization={q})"
            ))
            .is_err());
    }
    for sql in [
        "CREATE INDEX bad ON items(v) USING HNSW COMMENT 'M = 16'",
        "ALTER TABLE items ADD INDEX bad(v) USING IVF COMMENT 'NLIST=4'",
        "CREATE TABLE bad(v VECTOR(7) NOT NULL,KEY k(v) USING HNSW COMMENT 'DISTANCE=L2')",
    ] {
        let error = engine.execute(sql).unwrap_err();
        assert!(error.message().contains("WITH"), "{error}");
    }
}

#[test]
fn vector_quantization_pending_pq_trains_after_growth() {
    for backend in ["memory", "paged"] {
        for algo in ["HNSW", "IVF"] {
            let _env = HnswEnv::set(&[
                ("MYDB_VECTOR_STORAGE", backend),
                ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
                ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
            ]);
            let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
            let engine = open_strict_test_engine(dir.path());
            engine
                .execute("CREATE TABLE items(id INT PRIMARY KEY,v VECTOR(7) NOT NULL)")
                .unwrap();
            engine
                .execute(&format!(
                    "CREATE INDEX vx ON items(v) USING {algo} WITH ({})",
                    options(algo, "pq")
                ))
                .unwrap();
            assert_eq!(text(&engine,"SELECT QUANTIZATION_STATE FROM information_schema.mydb_vector_indexes WHERE INDEX_NAME='vx'"),"PENDING_TRAINING");
            engine
                .execute("INSERT INTO items VALUES(90,'[1,2,3,4,5,6,7]')")
                .unwrap();
            let sql = "SELECT id FROM items ORDER BY L2_DISTANCE(v,'[1,2,3,4,5,6,7]') LIMIT 1";
            assert_eq!(text(&engine, sql), "90");
            assert_eq!(text(&engine,"SELECT QUANTIZATION_STATE FROM information_schema.mydb_vector_indexes WHERE INDEX_NAME='vx'"),"PENDING_TRAINING");
            populate(&engine);
            assert_eq!(text(&engine, sql), "90");
            assert_eq!(text(&engine,"SELECT QUANTIZATION_STATE FROM information_schema.mydb_vector_indexes WHERE INDEX_NAME='vx'"),"READY");
        }
    }
}

#[test]
fn vector_quantization_dml_mvcc_and_restart() {
    for backend in ["memory", "paged"] {
        for algo in ["HNSW", "IVF"] {
            for q in ["sq8", "sq4", "pq", "bq"] {
                let _env = HnswEnv::set(&[
                    ("MYDB_VECTOR_STORAGE", backend),
                    ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
                    ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
                ]);
                let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
                {
                    let engine = open_strict_test_engine(dir.path());
                    engine
                        .execute("CREATE TABLE items(id INT PRIMARY KEY,v VECTOR(7) NOT NULL)")
                        .unwrap();
                    populate(&engine);
                    engine
                        .execute(&format!(
                            "ALTER TABLE items ADD INDEX vx(v) USING {algo} WITH ({})",
                            options(algo, q)
                        ))
                        .unwrap();
                    let sql="SELECT id FROM items ORDER BY L2_DISTANCE(v,'[100,101,102,103,104,105,106]') LIMIT 1";
                    let before = text(&engine, sql);
                    let mut reader = SessionContext::default();
                    engine
                        .execute_with_session(
                            &mut reader,
                            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
                        )
                        .unwrap();
                    engine.execute_with_session(&mut reader, "BEGIN").unwrap();
                    assert_eq!(
                        match engine.execute_with_session(&mut reader, sql).unwrap() {
                            QueryResponse::ResultSet(r) => r.rows[0][0].to_display_string(),
                            _ => panic!(),
                        },
                        before
                    );
                    engine
                        .execute("UPDATE items SET v='[100,101,102,103,104,105,106]' WHERE id=0")
                        .unwrap();
                    assert_eq!(text(&engine, sql), "0");
                    assert_eq!(
                        match engine.execute_with_session(&mut reader, sql).unwrap() {
                            QueryResponse::ResultSet(r) => r.rows[0][0].to_display_string(),
                            _ => panic!(),
                        },
                        before
                    );
                    engine.execute_with_session(&mut reader, "COMMIT").unwrap();
                    let mut writer = SessionContext::default();
                    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
                    engine
                        .execute_with_session(&mut writer, "DELETE FROM items WHERE id=0")
                        .unwrap();
                    engine
                        .execute_with_session(&mut writer, "ROLLBACK")
                        .unwrap();
                    assert_eq!(text(&engine, sql), "0");
                    engine.execute("ALTER SYSTEM CHECKPOINT").unwrap();
                }
                let engine = open_strict_test_engine(dir.path());
                if q == "sq8" {
                    let status = query_rows(&engine, "SHOW ENGINE MYDB STATUS");
                    assert!(
                        status
                            .iter()
                            .flatten()
                            .any(|v| v.to_display_string().contains("quantization=sq8")),
                        "{status:?}"
                    );
                }
                assert_eq!(text(&engine,"SELECT QUANTIZATION FROM information_schema.mydb_vector_indexes WHERE TABLE_SCHEMA='test' AND TABLE_NAME='items' AND INDEX_NAME='vx'"),q);
                assert_eq!(text(&engine,"SELECT LOADED FROM information_schema.mydb_vector_indexes WHERE TABLE_SCHEMA='test' AND TABLE_NAME='items' AND INDEX_NAME='vx'"),"0");
                assert_eq!(text(&engine,"SELECT id FROM items ORDER BY L2_DISTANCE(v,'[100,101,102,103,104,105,106]') LIMIT 1"),"0");
                assert_eq!(text(&engine,"SELECT QUANTIZATION FROM information_schema.mydb_vector_indexes WHERE INDEX_NAME='vx'"),q);
            }
        }
    }
}

#[test]
fn vector_quantization_high_dimensional_overflow_and_model_recovery() {
    use std::io::{Seek, SeekFrom, Write};
    let _env = HnswEnv::set(&[
        ("MYDB_VECTOR_STORAGE", "paged"),
        ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
        ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
    ]);
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let dimension = 8191;
    let vector = |x: &str| format!("[{}]", vec![x; dimension].join(","));
    let path;
    {
        let engine = open_strict_test_engine(dir.path());
        engine.execute(&format!("CREATE TABLE items(id INT PRIMARY KEY,v VECTOR({dimension}) NOT NULL,KEY vx(v) USING HNSW WITH (m=4,ef_construction=32,quantization=sq4))")).unwrap();
        engine
            .execute(&format!(
                "INSERT INTO items VALUES(1,'{}'),(2,'{}')",
                vector("1"),
                vector("2")
            ))
            .unwrap();
        let sql = format!(
            "SELECT id FROM items ORDER BY L2_DISTANCE(v,'{}') LIMIT 1",
            vector("2")
        );
        assert_eq!(text(&engine, &sql), "2");
        engine
            .execute(&format!(
                "UPDATE items SET v='{}' WHERE id=2",
                vector("100")
            ))
            .unwrap();
        assert_eq!(
            text(
                &engine,
                &format!(
                    "SELECT id FROM items ORDER BY L2_DISTANCE(v,'{}') LIMIT 1",
                    vector("100")
                )
            ),
            "2"
        );
        let table = engine.database().table("test.items").unwrap();
        let index = table.indexes.iter().find(|i| i.name == "vx").unwrap();
        let graph = engine
            .database()
            .vector_graph_get_or_build_for_partition(&table, index, table.id)
            .unwrap();
        let crate::engine::vector::VectorGraph::NativeHnsw(native) = graph.as_ref() else {
            panic!("compressed native HNSW expected")
        };
        let file = native.file().unwrap();
        let meta = file.meta();
        assert!(meta.quant_model_head > 0);
        let model = file
            .read_quantization_blob(meta.quant_model_head, 4 * 1024 * 1024)
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&model).unwrap();
        assert_eq!(json[1]["dimension"], dimension as u64);
        path = {
            fn find(dir: &std::path::Path) -> Option<std::path::PathBuf> {
                for e in std::fs::read_dir(dir).unwrap() {
                    let e = e.unwrap();
                    let p = e.path();
                    if p.is_dir() {
                        if let Some(p) = find(&p) {
                            return Some(p);
                        }
                    } else if p.extension().is_some_and(|e| e == "hvec") {
                        return Some(p);
                    }
                }
                None
            }
            find(dir.path()).unwrap()
        };
        engine.database().checkpoint().unwrap();
        // A multi-page model and out-of-range payload both used overflow chains.
        assert!(file.meta().node_next_page_id > 4);
    }
    {
        let mut f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.seek(SeekFrom::Start(16384 + 48)).unwrap();
        f.write_all(&[0x55; 64]).unwrap();
        f.sync_all().unwrap();
    }
    let engine = open_strict_test_engine(dir.path());
    assert_eq!(
        text(
            &engine,
            &format!(
                "SELECT id FROM items ORDER BY L2_DISTANCE(v,'{}') LIMIT 1",
                vector("100")
            )
        ),
        "2"
    );
}

#[test]
fn vector_quantization_retraining_and_parallel_build() {
    let _env = HnswEnv::set(&[
        ("MYDB_VECTOR_STORAGE", "memory"),
        ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
        ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
        ("MYDB_HNSW_BUILD_THREADS", "4"),
        ("MYDB_HNSW_COMPACT_DISABLE", "0"),
    ]);
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items(id INT PRIMARY KEY,v VECTOR(7) NOT NULL)")
        .unwrap();
    for start in (0..2048).step_by(128) {
        let rows = (start..start + 128)
            .map(|n| format!("({n},'[{},1,2,3,4,5,6]')", n as f64 / 10.0))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO items VALUES{rows}"))
            .unwrap();
    }
    engine.execute("CREATE INDEX vx ON items(v) USING HNSW WITH(m=4,ef_construction=32,ef_search=512,quantization=sq4,rerank_mult=512)").unwrap();
    assert_eq!(
        text(
            &engine,
            "SELECT id FROM items ORDER BY L2_DISTANCE(v,'[90,1,2,3,4,5,6]') LIMIT 1"
        ),
        "900"
    );
    let before = text(
        &engine,
        "SELECT GENERATION FROM information_schema.mydb_vector_indexes WHERE INDEX_NAME='vx'",
    );
    engine
        .execute("UPDATE items SET v='[10000,1,2,3,4,5,6]' WHERE id<103")
        .unwrap();
    engine.database().maintain_vector_indexes(|| false);
    assert_ne!(
        before,
        text(
            &engine,
            "SELECT GENERATION FROM information_schema.mydb_vector_indexes WHERE INDEX_NAME='vx'"
        )
    );
    let rows = query_rows(
        &engine,
        "SELECT id FROM items ORDER BY L2_DISTANCE(v,'[10000,1,2,3,4,5,6]') LIMIT 3",
    );
    assert_eq!(rows.len(), 3);
    assert!(rows
        .iter()
        .all(|r| r[0].to_display_string().parse::<u32>().unwrap() < 103));
    let after = text(
        &engine,
        "SELECT GENERATION FROM information_schema.mydb_vector_indexes WHERE INDEX_NAME='vx'",
    );
    assert_ne!(before, after);
    engine
        .execute("ALTER TABLE items RENAME INDEX vx TO vy")
        .unwrap();
    assert_eq!(
        text(
            &engine,
            "SELECT QUANTIZATION FROM information_schema.mydb_vector_indexes WHERE INDEX_NAME='vy'"
        ),
        "sq4"
    );
    engine.execute("OPTIMIZE TABLE items").unwrap();
    assert_eq!(
        text(
            &engine,
            "SELECT QUANTIZATION FROM information_schema.mydb_vector_indexes WHERE INDEX_NAME='vy'"
        ),
        "sq4"
    );
}

#[test]
fn vector_quantization_partitioned_prepared_and_relational_boundaries() {
    for backend in ["memory", "paged"] {
        for q in ["sq8", "sq4", "pq", "bq"] {
            let _env = HnswEnv::set(&[
                ("MYDB_VECTOR_STORAGE", backend),
                ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
                ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
            ]);
            let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
            let engine = open_strict_test_engine(dir.path());
            engine.execute(&format!("CREATE TABLE items(id INT PRIMARY KEY,v VECTOR(7,FLOAT16) NOT NULL,KEY vx(v) USING HNSW WITH({})) PARTITION BY HASH(id) PARTITIONS 2",options("HNSW",q))).unwrap();
            populate(&engine);
            let first = text(&engine, "SELECT VECTOR_TO_STRING(v) FROM items WHERE id=0");
            let second = text(&engine, "SELECT VECTOR_TO_STRING(v) FROM items WHERE id=1");
            let mut session = SessionContext::default();
            engine.execute_with_session(&mut session,"PREPARE st FROM 'SELECT id FROM items ORDER BY L2_DISTANCE(v,TO_VECTOR(?)) LIMIT 1'").unwrap();
            for (query, expected) in [(&first, "0"), (&second, "1"), (&first, "0")] {
                engine
                    .execute_with_session(&mut session, &format!("SET @q='{query}'"))
                    .unwrap();
                let QueryResponse::ResultSet(result) = engine
                    .execute_with_session(&mut session, "EXECUTE st USING @q")
                    .unwrap()
                else {
                    panic!()
                };
                assert_eq!(result.rows[0][0].to_display_string(), expected);
            }
            engine
                .execute_with_session(&mut session, "SET @q='[1,2]'")
                .unwrap();
            assert!(engine
                .execute_with_session(&mut session, "EXECUTE st USING @q")
                .is_err());
            assert_eq!(text(&engine,&format!("SELECT COUNT(*) OVER() FROM items ORDER BY L2_DISTANCE(v,'{first}') LIMIT 1")),"32");
            engine
                .execute("CREATE TABLE rhs(id INT PRIMARY KEY)")
                .unwrap();
            engine.execute("INSERT INTO rhs VALUES(31)").unwrap();
            assert_eq!(text(&engine,&format!("SELECT a.id FROM items a JOIN rhs b ON a.id=b.id ORDER BY L2_DISTANCE(a.v,'{first}') LIMIT 1")),"31");
            let sql = format!(
                "SELECT id FROM items WHERE id%3=0 ORDER BY L2_DISTANCE(v,'{second}') LIMIT 3"
            );
            let exact = sql.replacen("SELECT", "SELECT /*+ NO_VECTOR_INDEX */", 1);
            assert_eq!(query_rows(&engine, &sql), query_rows(&engine, &exact));
            let distinct =
                format!("SELECT DISTINCT L2_DISTANCE(v,'{first}') d FROM items ORDER BY d LIMIT 3");
            assert_eq!(
                query_rows(&engine, &distinct),
                query_rows(
                    &engine,
                    &distinct.replacen("SELECT", "SELECT /*+ NO_VECTOR_INDEX */", 1)
                )
            );
            engine
                .execute_with_session(&mut session, "SET mydb_hnsw_rerank_mult=4")
                .unwrap();
            let QueryResponse::ResultSet(explain)=engine.execute_with_session(&mut session,&format!("EXPLAIN FORMAT=JSON SELECT /*+ HNSW_EF(1024) */ id FROM items ORDER BY L2_DISTANCE(v,'{first}') LIMIT 10")).unwrap() else{panic!()};
            let json = explain.rows[0][0].to_display_string();
            assert!(json.contains("\"rerank_pool\": 40"), "{json}");
        }
    }
}

#[test]
fn vector_quantization_copy_validates_type_and_dimension_before_publication() {
    for backend in ["memory", "paged"] {
        for algo in ["HNSW", "IVF"] {
            for q in ["sq8", "sq4", "pq", "bq"] {
                let _env = HnswEnv::set(&[
                    ("MYDB_VECTOR_STORAGE", backend),
                    ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
                    ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
                ]);
                let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
                let engine = open_strict_test_engine(dir.path());
                engine
                    .execute("CREATE TABLE items(id INT PRIMARY KEY,v VECTOR(7,FLOAT16) NOT NULL)")
                    .unwrap();
                engine
                    .execute(&format!(
                        "CREATE INDEX vx ON items(v) USING {algo} WITH({})",
                        options(algo, q)
                    ))
                    .unwrap();
                if q == "pq" {
                    let error=engine.execute("ALTER TABLE items MODIFY COLUMN v VECTOR(2,FLOAT16) NOT NULL, ALGORITHM=COPY").unwrap_err();
                    assert!(error.message().contains("pq_m"), "{error}");
                    assert_eq!(text(&engine,"SELECT DECLARED_DIM FROM information_schema.mydb_vector_indexes WHERE INDEX_NAME='vx'"),"7");
                }
                assert!(engine.execute("ALTER TABLE items MODIFY COLUMN v VECTOR(7,BINARY) NOT NULL, ALGORITHM=COPY").is_err());
                assert!(engine
                    .execute("ALTER TABLE items MODIFY COLUMN v VECTOR NOT NULL, ALGORITHM=COPY")
                    .is_err());
                populate(&engine);
                let point = text(&engine, "SELECT VECTOR_TO_STRING(v) FROM items WHERE id=0");
                engine.execute("ALTER TABLE items MODIFY COLUMN v VECTOR(7,BFLOAT16) NOT NULL, ALGORITHM=COPY").unwrap();
                assert_eq!(
                    text(
                        &engine,
                        &format!("SELECT id FROM items ORDER BY L2_DISTANCE(v,'{point}') LIMIT 1")
                    ),
                    "0"
                );
                assert_eq!(text(&engine,"SELECT QUANTIZATION FROM information_schema.mydb_vector_indexes WHERE INDEX_NAME='vx'"),q);
                let create =
                    query_rows(&engine, "SHOW CREATE TABLE items")[0][1].to_display_string();
                assert!(create.contains("vector(7,bfloat16)"), "{create}");
            }
        }
    }
}
