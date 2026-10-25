use super::*;

#[test]
fn hybrid_rrf_rejects_unbound_outer_references_and_ordinal_domains() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE ordinal_key(id INT PRIMARY KEY,e ENUM('z','a'),s SET('z','a'))")
        .unwrap();
    engine
        .execute("INSERT INTO ordinal_key VALUES(1,'z','z'),(2,'a','a')")
        .unwrap();
    for column in ["e", "s"] {
        let sql = format!("SELECT * FROM ((SELECT {column} FROM ordinal_key ORDER BY {column} LIMIT 2) RRF (SELECT {column} FROM ordinal_key ORDER BY {column} LIMIT 2)) h");
        assert!(engine
            .execute(&sql)
            .and_then(QueryResponse::materialize)
            .is_err());
    }
    let result = query(&engine, "SELECT id,match_count FROM ((SELECT CAST(e AS UNSIGNED) id FROM ordinal_key ORDER BY id LIMIT 2) RRF (SELECT CAST(e AS UNSIGNED) id FROM ordinal_key ORDER BY id LIMIT 2)) h ORDER BY id");
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0], vec![Value::UInt(1), Value::UInt(2)]);
    for sql in [
        "SELECT o.id,(SELECT COUNT(*) FROM ((SELECT o.id AS k ORDER BY k LIMIT 1) RRF (SELECT o.id AS k ORDER BY k LIMIT 1)) h) FROM ordinal_key o",
        "SELECT h.k FROM ordinal_key o JOIN LATERAL (SELECT * FROM ((SELECT o.id AS k ORDER BY k LIMIT 1) RRF (SELECT o.id AS k ORDER BY k LIMIT 1)) r) h ON TRUE",
    ] {
        assert!(engine.execute(sql).and_then(QueryResponse::materialize).is_err(), "{sql}");
    }
}

#[test]
fn hybrid_rrf_explain_static_tree_is_stable() {
    let (_dir, engine) = table_engine();
    let sql = "SELECT id FROM ((SELECT id FROM rrft ORDER BY id LIMIT 2) RRF (SELECT id FROM rrft ORDER BY id DESC LIMIT 2)) h ORDER BY rrf_score DESC,best_rank,id LIMIT 2";
    let text =
        |prefix: &str| query(&engine, &format!("{prefix} {sql}")).rows[0][0].to_display_string();
    let plain = text("EXPLAIN FORMAT=TREE");
    let analyzed = text("EXPLAIN ANALYZE FORMAT=TREE");
    let static_tree = analyzed
        .lines()
        .filter(|line| !line.starts_with("Query I/O:"))
        .map(|line| {
            let cut = line
                .find(" (actual time=")
                .or_else(|| line.find(" (never executed)"))
                .or_else(|| line.find(" (included in operator"))
                .unwrap_or(line.len());
            &line[..cut]
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(plain.trim(), static_tree.trim());
}

fn query(engine: &StorageQueryEngine, sql: &str) -> ResultSet {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap().materialize().unwrap()
    else {
        panic!("expected SELECT: {sql}");
    };
    result
}

#[test]
fn hybrid_rrf_fuses_all_lists_and_explains_contributions() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE ranking(seq INT PRIMARY KEY,id INT,a INT,b INT)")
        .unwrap();
    engine
        .execute("INSERT INTO ranking VALUES(1,10,1,3),(2,20,3,1),(3,30,2,2)")
        .unwrap();
    let sql="SELECT id,rrf_score,best_rank,match_count,rrf_details FROM ((SELECT id FROM ranking ORDER BY a LIMIT 2) AS lexical RRF (SELECT id FROM ranking ORDER BY b LIMIT 2) AS dense WITH(SCORE_DETAILS=TRUE)) h ORDER BY rrf_score DESC,best_rank,id";
    let result = query(&engine, sql);
    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0][0], Value::Int(30));
    assert_eq!(result.rows[0][1], Value::Double(2.0 / 62.0));
    assert_eq!(result.rows[0][2], Value::UInt(2));
    assert_eq!(result.rows[0][3], Value::UInt(2));
    assert!(matches!(result.rows[0][4], Value::Json(_)));
    let filtered=query(&engine,"SELECT id FROM ((SELECT id FROM ranking ORDER BY a LIMIT 2) RRF (SELECT id FROM ranking ORDER BY b LIMIT 2)) h WHERE id=10 ORDER BY rrf_score DESC LIMIT 1");
    assert_eq!(filtered.rows, vec![vec![Value::Int(10)]]);
}

#[test]
fn hybrid_rrf_preserves_duplicates_and_zero_weight_semantics() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE duplicate_ranks(seq INT PRIMARY KEY,id INT)")
        .unwrap();
    engine
        .execute("INSERT INTO duplicate_ranks VALUES(1,10),(2,10),(3,20)")
        .unwrap();
    let result=query(&engine,"SELECT id,rrf_score,best_rank,match_count FROM ((SELECT id FROM duplicate_ranks ORDER BY seq LIMIT 3) RRF (SELECT id FROM duplicate_ranks ORDER BY seq DESC LIMIT 3) WITH(RANK_CONSTANT=0,WEIGHTS=(1,0))) h ORDER BY id");
    assert_eq!(
        result.rows,
        vec![
            vec![
                Value::Int(10),
                Value::Double(1.0),
                Value::UInt(1),
                Value::UInt(1)
            ],
            vec![
                Value::Int(20),
                Value::Double(1.0 / 3.0),
                Value::UInt(3),
                Value::UInt(1)
            ]
        ]
    );
}

#[test]
fn hybrid_rrf_composite_identity_and_errors() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE identities(tenant INT,id INT,PRIMARY KEY(tenant,id))")
        .unwrap();
    engine
        .execute("INSERT INTO identities VALUES(1,1),(2,1)")
        .unwrap();
    let result=query(&engine,"SELECT tenant,id,match_count FROM ((SELECT tenant,id FROM identities ORDER BY tenant LIMIT 2) RRF (SELECT tenant,id FROM identities ORDER BY tenant DESC LIMIT 2)) h ORDER BY tenant");
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(1), Value::Int(1), Value::UInt(2)],
            vec![Value::Int(2), Value::Int(1), Value::UInt(2)]
        ]
    );
    for options in [
        "WEIGHTS=(0,0)",
        "WEIGHTS=(-1,1)",
        "RANK_CONSTANT=-1",
        "RANK_CONSTANT=0.5",
    ] {
        let sql=format!("SELECT * FROM ((SELECT id FROM identities ORDER BY id LIMIT 2) RRF (SELECT id FROM identities ORDER BY id LIMIT 2) WITH({options})) h");
        assert!(
            engine
                .execute(&sql)
                .and_then(QueryResponse::materialize)
                .is_err(),
            "{sql}"
        );
    }
}

fn table_engine() -> (tempfile::TempDir, StorageQueryEngine) {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE rrft(id INT PRIMARY KEY, grp INT, body TEXT,v VECTOR(2) NOT NULL,FULLTEXT KEY ft(body))").unwrap();
    engine.execute("INSERT INTO rrft VALUES(1,1,'database systems','[2,0]'),(2,1,'database database','[1,0]'),(3,2,'unrelated document','[0,0]')").unwrap();
    engine
        .execute("CREATE INDEX hv ON rrft(v) USING HNSW")
        .unwrap();
    (dir, engine)
}

#[test]
fn hybrid_rrf_native_and_scalar_paths_match_without_implicit_cap() {
    let (_dir, engine) = table_engine();
    let values = (1..=1100)
        .map(|i| format!("({i})"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute("CREATE TABLE all_candidates(id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute(&format!("INSERT INTO all_candidates VALUES {values}"))
        .unwrap();
    let sql="SELECT id,rrf_score FROM ((SELECT id FROM all_candidates ORDER BY id LIMIT 1100) RRF (SELECT id FROM all_candidates ORDER BY id DESC LIMIT 1100)) h ORDER BY rrf_score DESC,best_rank,id";
    let native = query(&engine, sql);
    assert_eq!(native.rows.len(), 1100);
    let _mode = crate::engine::executor::vec::config::enter_mode(
        crate::engine::executor::vec::config::VecMode::Off,
    );
    assert_eq!(query(&engine, sql).rows, native.rows);
}

#[test]
fn hybrid_rrf_local_cte_scopes_derived_inputs_and_outer_cte() {
    let (_dir, engine) = table_engine();
    let local="SELECT id,match_count FROM ((WITH c AS(SELECT id FROM rrft WHERE id=1) SELECT id FROM c ORDER BY id LIMIT 2) AS a RRF (WITH c AS(SELECT id FROM rrft WHERE id=2) SELECT id FROM c ORDER BY id LIMIT 2) AS b) h ORDER BY id";
    assert_eq!(
        query(&engine, local).rows,
        vec![
            vec![Value::Int(1), Value::UInt(1)],
            vec![Value::Int(2), Value::UInt(1)]
        ]
    );
    let derived="WITH fused AS (SELECT * FROM ((SELECT id FROM (SELECT id,grp FROM rrft) AS x ORDER BY grp,id LIMIT 3) RRF (SELECT id FROM rrft ORDER BY id DESC LIMIT 3)) f) SELECT id,match_count FROM fused WHERE id>1 ORDER BY id";
    assert_eq!(
        query(&engine, derived).rows,
        vec![
            vec![Value::Int(2), Value::UInt(2)],
            vec![Value::Int(3), Value::UInt(2)]
        ]
    );
}

#[test]
fn hybrid_rrf_outer_join_filter_and_offset_do_not_change_retrieval() {
    let (_dir, engine) = table_engine();
    let base="((SELECT id FROM rrft ORDER BY id LIMIT 3) RRF (SELECT id FROM rrft ORDER BY id DESC LIMIT 3))";
    let sql=format!("SELECT d.id FROM {base} h JOIN rrft d ON d.id=h.id WHERE d.id=2 ORDER BY h.rrf_score DESC LIMIT 1");
    assert_eq!(query(&engine, &sql).rows, vec![vec![Value::Int(2)]]);
    let full = query(
        &engine,
        &format!("SELECT id FROM {base} h ORDER BY rrf_score DESC,best_rank,id"),
    );
    let page = query(
        &engine,
        &format!("SELECT id FROM {base} h ORDER BY rrf_score DESC,best_rank,id LIMIT 1 OFFSET 1"),
    );
    assert_eq!(page.rows, full.rows[1..2]);
    assert!(query(&engine, &format!("SELECT * FROM {base} h LIMIT 0"))
        .rows
        .is_empty());
}

#[test]
fn hybrid_rrf_fulltext_and_vector_branches_choose_compatible_indexes() {
    let (_dir, engine) = table_engine();
    let sql="SELECT id,match_count,rrf_details FROM ((SELECT id FROM rrft WHERE MATCH(body) AGAINST('database') ORDER BY MATCH(body) AGAINST('database') DESC,id LIMIT 3) AS lexical RRF (SELECT /*+ HNSW_EF(64) */ id FROM rrft ORDER BY L2_DISTANCE(v,TO_VECTOR('[0,0]')),id LIMIT 3) AS semantic WITH(SCORE_DETAILS=TRUE)) h ORDER BY id";
    let result = query(&engine, sql);
    assert_eq!(
        result
            .rows
            .iter()
            .map(|row| row[1].clone())
            .collect::<Vec<_>>(),
        vec![Value::UInt(2), Value::UInt(2), Value::UInt(1)]
    );
    for format in ["TREE", "JSON"] {
        let result = query(&engine, &format!("EXPLAIN FORMAT={format} {sql}"));
        let text = result
            .rows
            .into_iter()
            .flatten()
            .map(|value| value.to_display_string())
            .collect::<Vec<_>>()
            .join(" ");
        assert!(text.to_ascii_lowercase().contains("rrf"), "{text}");
        assert!(text.to_ascii_lowercase().contains("vector"), "{text}");
        assert!(text.to_ascii_lowercase().contains("full"), "{text}");
        if format == "JSON" {
            serde_json::from_str::<serde_json::Value>(&text).unwrap();
        }
    }
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET mydb_hnsw_iterative_scan='relaxed_order'")
        .unwrap();
    let QueryResponse::ResultSet(relaxed) = engine
        .execute_with_session(&mut session, sql)
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!("rows")
    };
    assert_eq!(relaxed.rows, result.rows);
}

#[test]
fn hybrid_rrf_prepared_parameters_rebind_options_windows_and_filters() {
    use crate::engine::api::QueryEngine;
    let (_dir, engine) = table_engine();
    let sql="SELECT id,rrf_score FROM ((SELECT id FROM rrft WHERE id>=? ORDER BY id LIMIT ?) RRF (SELECT id FROM rrft WHERE id<=? ORDER BY id DESC LIMIT ?) WITH(RANK_CONSTANT=?,WEIGHTS=(?,?))) h ORDER BY rrf_score DESC,best_rank,id LIMIT ?";
    let mut session = SessionContext::default();
    let parsed = engine.prepare_cached(&mut session, sql, 8).unwrap();
    let mut memo = crate::engine::api::PreparedSelectMemoState::default();
    for (weights, expected) in [((1, 0), 1), ((0, 2), 3)] {
        let params = vec![
            Value::Int(1),
            Value::Int(3),
            Value::Int(3),
            Value::Int(3),
            Value::Int(0),
            Value::Int(weights.0),
            Value::Int(weights.1),
            Value::Int(1),
        ];
        let fast = engine.execute_prepared_select(&mut session, &mut memo, &parsed, sql, &params);
        let result = match fast {
            Some(result) => result.unwrap(),
            None => crate::engine::params::with_params(params, || {
                engine
                    .execute_prepared_statement(&mut session, sql, (*parsed).clone())
                    .unwrap()
                    .unwrap()
            }),
        }
        .materialize()
        .unwrap();
        let QueryResponse::ResultSet(result) = result else {
            panic!("rows")
        };
        assert_eq!(result.rows[0][0], Value::Int(expected));
        assert_eq!(
            result.rows[0][1],
            Value::Double(weights.0.max(weights.1) as f64)
        );
    }
    engine.execute_with_session(&mut session,"PREPARE hr FROM 'SELECT id FROM ((SELECT id FROM rrft ORDER BY id LIMIT ?) RRF (SELECT id FROM rrft ORDER BY id DESC LIMIT ?) WITH(WEIGHTS=(?,?))) h ORDER BY rrf_score DESC LIMIT 1'").unwrap();
    engine
        .execute_with_session(&mut session, "SET @l=3,@a=1,@b=0")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "EXECUTE hr USING @l,@l,@a,@b")
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!("rows")
    };
    assert_eq!(result.rows[0][0], Value::Int(1));
}

#[test]
fn hybrid_rrf_repeatable_read_and_transaction_writes_share_visibility() {
    let (_dir, engine) = table_engine();
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        )
        .unwrap();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut session, "SELECT id FROM rrft")
        .unwrap()
        .materialize()
        .unwrap();
    engine.execute("DELETE FROM rrft WHERE id=1").unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO rrft VALUES(4,3,'private memory','[4,0]')",
        )
        .unwrap();
    let sql="SELECT id FROM ((SELECT id FROM rrft ORDER BY id LIMIT 10) RRF (SELECT id FROM rrft ORDER BY id DESC LIMIT 10)) h ORDER BY id";
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, sql)
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!("rows")
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)],
            vec![Value::Int(4)]
        ]
    );
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    assert_eq!(
        query(&engine, sql).rows,
        vec![vec![Value::Int(2)], vec![Value::Int(3)]]
    );
}

#[test]
fn hybrid_rrf_collation_decimal_and_nullable_keys_are_explicit() {
    let (_dir, engine) = table_engine();
    engine.execute("CREATE TABLE typed_ranks(seq INT PRIMARY KEY,k VARCHAR(16) COLLATE utf8mb4_0900_ai_ci,n DECIMAL(12,2),b VARBINARY(16))").unwrap();
    engine.execute("INSERT INTO typed_ranks VALUES(1,'Alpha',1.00,X'0100'),(2,'alpha',2.50,X'0101'),(3,NULL,3.00,X'0102')").unwrap();
    let strings=query(&engine,"SELECT k,match_count FROM ((SELECT k FROM typed_ranks WHERE k IS NOT NULL ORDER BY seq LIMIT 3) RRF (SELECT k FROM typed_ranks WHERE k IS NOT NULL ORDER BY seq DESC LIMIT 3)) h");
    assert_eq!(strings.rows.len(), 1);
    assert_eq!(strings.rows[0][1], Value::UInt(2));
    for key in ["n", "b"] {
        let sql=format!("SELECT {key},match_count FROM ((SELECT {key} FROM typed_ranks ORDER BY seq LIMIT 3) RRF (SELECT {key} FROM typed_ranks ORDER BY seq DESC LIMIT 3)) h");
        assert_eq!(query(&engine, &sql).rows.len(), 3);
    }
    assert!(engine.execute("SELECT * FROM ((SELECT k FROM typed_ranks ORDER BY seq LIMIT 3) RRF (SELECT k FROM typed_ranks ORDER BY seq LIMIT 3)) h").and_then(QueryResponse::materialize).is_err());
    let sql="SELECT * FROM ((SELECT id FROM rrft ORDER BY id LIMIT 3) RRF (SELECT id FROM rrft ORDER BY id LIMIT 3) WITH(RANK_CONSTANT=60.0000000000000000000000001)) h";
    assert!(engine
        .execute(sql)
        .and_then(QueryResponse::materialize)
        .is_err());
}

#[test]
fn hybrid_rrf_empty_disabled_and_zero_limit_inputs_keep_schema() {
    let (_dir, engine) = table_engine();
    let result=query(&engine,"SELECT * FROM ((SELECT id FROM rrft ORDER BY id LIMIT 0) RRF (SELECT id FROM rrft ORDER BY id LIMIT 0) WITH(SCORE_DETAILS=TRUE)) h");
    assert!(result.rows.is_empty());
    assert_eq!(result.columns.len(), 5);
    let result=query(&engine,"SELECT id FROM ((SELECT id FROM rrft ORDER BY id LIMIT 0) RRF (SELECT id FROM rrft ORDER BY id DESC LIMIT 2)) h ORDER BY id");
    assert_eq!(result.rows, vec![vec![Value::Int(2)], vec![Value::Int(3)]]);
    let result=query(&engine,"SELECT id FROM ((SELECT id FROM rrft ORDER BY id LIMIT 1) RRF (SELECT id FROM (SELECT id,1/0 AS broken FROM rrft) d ORDER BY id LIMIT 3) WITH(WEIGHTS=(1,0))) h");
    assert_eq!(result.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn hybrid_rrf_grouped_branches_and_outer_window_are_relational() {
    let (_dir, engine) = table_engine();
    let sql="SELECT grp,match_count,ROW_NUMBER() OVER(ORDER BY rrf_score DESC,best_rank,grp) AS pos FROM ((SELECT grp FROM rrft GROUP BY grp ORDER BY COUNT(*) DESC,grp LIMIT 2) RRF (SELECT DISTINCT grp FROM rrft ORDER BY grp DESC LIMIT 2)) h ORDER BY pos";
    let result = query(&engine, sql);
    assert_eq!(result.rows.len(), 2);
    assert!(result.rows.iter().all(|row| row[1] == Value::UInt(2)));
}

#[test]
fn hybrid_rrf_shared_memory_failure_releases_state() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    let (_dir, engine) = table_engine();
    engine
        .execute("CREATE TABLE many_ranks(id INT PRIMARY KEY)")
        .unwrap();
    let values = (1..=600)
        .map(|i| format!("({i})"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO many_ranks VALUES {values}"))
        .unwrap();
    let budget = Arc::new(MemBudget::new(64 * 1024));
    let result = with_query_budget(budget.clone(), || {
        engine.execute("SELECT * FROM ((SELECT id FROM many_ranks ORDER BY id LIMIT 600) RRF (SELECT id FROM many_ranks ORDER BY id DESC LIMIT 600)) h").and_then(QueryResponse::materialize)
    });
    assert!(
        matches!(result,Err(ref error) if error.code()==1037),
        "{result:?}"
    );
    assert_eq!(budget.used(), 0);
    assert_eq!(query(&engine, "SELECT 1").rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn hybrid_rrf_explain_analyze_has_fusion_and_real_runtime_rows() {
    let (_dir, engine) = table_engine();
    let sql="SELECT id,rrf_score FROM ((SELECT id FROM rrft ORDER BY id LIMIT 3) RRF (SELECT id FROM rrft ORDER BY id DESC LIMIT 3)) h ORDER BY rrf_score DESC";
    for prefix in [
        "EXPLAIN ",
        "EXPLAIN ANALYZE FORMAT=TREE ",
        "EXPLAIN ANALYZE FORMAT=JSON ",
    ] {
        let result = query(&engine, &format!("{prefix}{sql}"));
        let text = result
            .rows
            .into_iter()
            .flatten()
            .map(|v| v.to_display_string())
            .collect::<Vec<_>>()
            .join(" ");
        assert!(
            text.to_ascii_lowercase().contains("rrf"),
            "{prefix}: {text}"
        );
        if prefix.contains("JSON") {
            let value = serde_json::from_str::<serde_json::Value>(&text).unwrap();
            fn fusion(value: &serde_json::Value) -> Option<&serde_json::Value> {
                if value.get("rrf").is_some() {
                    return value.get("rrf");
                }
                match value {
                    serde_json::Value::Object(map) => map.values().find_map(fusion),
                    serde_json::Value::Array(values) => values.iter().find_map(fusion),
                    _ => None,
                }
            }
            let work = fusion(&value).expect("measured RRF work");
            assert_eq!(work["input_rows"], serde_json::json!([3, 3]));
            assert_eq!(work["unique_rows"], serde_json::json!([3, 3]));
            assert_eq!(work["union_rows"], 3);
            assert_eq!(work["overlap_rows"], 3);
        }
    }
}

#[test]
fn hybrid_rrf_rejects_impure_and_nonrepeatable_details() {
    let (_dir, engine) = table_engine();
    for first in [
        "SELECT @x:=id AS id FROM rrft ORDER BY id LIMIT 3",
        "SELECT GET_LOCK('rrf',0) AS id FROM rrft ORDER BY id LIMIT 3",
        "SELECT id FROM rrft ORDER BY id LIMIT 3 FOR UPDATE",
    ] {
        let sql =
            format!("SELECT * FROM (({first}) RRF (SELECT id FROM rrft ORDER BY id LIMIT 3)) h");
        assert!(
            engine
                .execute(&sql)
                .and_then(QueryResponse::materialize)
                .is_err(),
            "{sql}"
        );
    }
    let sql="SELECT * FROM ((SELECT id FROM rrft ORDER BY RAND() LIMIT 3) RRF (SELECT id FROM rrft ORDER BY id LIMIT 3) WITH(SCORE_DETAILS=TRUE)) h";
    assert!(engine
        .execute(sql)
        .and_then(QueryResponse::materialize)
        .is_err());
    engine
        .execute("CREATE FUNCTION harmless_rank() RETURNS INT DETERMINISTIC RETURN 1")
        .unwrap();
    let sql="SELECT * FROM ((SELECT id FROM rrft WHERE harmless_rank()=1 ORDER BY id LIMIT 3) RRF (SELECT id FROM rrft ORDER BY id LIMIT 3)) h";
    assert!(
        matches!(engine.execute(sql).and_then(QueryResponse::materialize),Err(error) if error.code()==1235)
    );
}

#[test]
fn hybrid_rrf_safe_output_heap_and_filter_barriers() {
    let (_dir, engine) = table_engine();
    let source="((SELECT id FROM rrft ORDER BY id LIMIT 3) RRF (SELECT id FROM rrft ORDER BY id DESC LIMIT 3))";
    let all = query(
        &engine,
        &format!("SELECT id,rrf_score FROM {source} h ORDER BY rrf_score DESC,best_rank,id"),
    );
    for k in [1, 2, 3] {
        let top=query(&engine,&format!("SELECT id,rrf_score FROM {source} h ORDER BY rrf_score DESC,best_rank,id LIMIT {k}"));
        assert_eq!(top.rows, all.rows[..k]);
    }
    let blocked=query(&engine,&format!("SELECT id FROM (SELECT * FROM {source} x WHERE id=2) h ORDER BY rrf_score DESC LIMIT 1"));
    assert_eq!(blocked.rows, vec![vec![Value::Int(2)]]);
    let zero=query(&engine,"SELECT * FROM ((SELECT CAST(NULL AS SIGNED) AS id ORDER BY id LIMIT 1) RRF (SELECT CAST(1 AS SIGNED) AS id ORDER BY id LIMIT 1)) h LIMIT 0");
    assert!(zero.rows.is_empty());
}

#[test]
fn hybrid_rrf_rejects_transitive_forbidden_inputs_before_materialization() {
    let (_dir, engine) = table_engine();
    for source in [
        "WITH c AS(SELECT id FROM rrft UNION ALL SELECT id FROM rrft) SELECT * FROM ((SELECT id FROM c ORDER BY id LIMIT 3) RRF (SELECT id FROM rrft ORDER BY id LIMIT 3)) h",
        "WITH bad AS(SELECT id FROM rrft UNION SELECT id FROM rrft), c AS(SELECT id FROM bad) SELECT * FROM ((SELECT id FROM c ORDER BY id LIMIT 3) RRF (SELECT id FROM rrft ORDER BY id LIMIT 3)) h",
        "WITH c AS(SELECT * FROM ((SELECT id FROM rrft ORDER BY id LIMIT 3) RRF (SELECT id FROM rrft ORDER BY id LIMIT 3)) h) SELECT * FROM ((SELECT id FROM c ORDER BY id LIMIT 3) RRF (SELECT id FROM rrft ORDER BY id LIMIT 3)) h",
    ] {assert!(matches!(engine.execute(source).and_then(QueryResponse::materialize),Err(error) if error.code()==1235),"{source}");}
    let valid="WITH bad AS(SELECT id FROM rrft UNION SELECT id FROM rrft) SELECT id FROM ((WITH bad AS(SELECT id FROM rrft) SELECT id FROM bad ORDER BY id LIMIT 3) RRF (SELECT id FROM rrft ORDER BY id LIMIT 3)) h ORDER BY id";
    assert_eq!(query(&engine, valid).rows.len(), 3);
}

#[test]
fn hybrid_rrf_analyze_local_sources_and_sibling_fusions() {
    let (_dir, engine) = table_engine();
    let source="((WITH c AS(SELECT id FROM rrft) SELECT id FROM c ORDER BY id LIMIT 3) RRF (SELECT id FROM (SELECT id FROM rrft) d ORDER BY id DESC LIMIT 3))";
    let sql = format!("SELECT a.id FROM {source} a JOIN {source} b ON a.id=b.id ORDER BY a.id");
    assert_eq!(query(&engine, &sql).rows.len(), 3);
    for prefix in ["EXPLAIN FORMAT=JSON ", "EXPLAIN ANALYZE FORMAT=JSON "] {
        let result = query(&engine, &format!("{prefix}{sql}"));
        let text = result.rows[0][0].to_display_string();
        serde_json::from_str::<serde_json::Value>(&text).unwrap();
        assert!(text.matches("rrf_fusion").count() >= 2, "{text}");
    }
}

#[test]
fn hybrid_rrf_identity_domains_ignore_display_metadata_and_honor_padding() {
    let (_dir, engine) = table_engine();
    engine
        .execute("CREATE TABLE domains(id BIGINT,k VARCHAR(8) COLLATE utf8mb4_general_ci,d DATE)")
        .unwrap();
    engine
        .execute("INSERT INTO domains VALUES(1,'Alpha','2026-01-01'),(2,'alpha ','2026-01-02')")
        .unwrap();
    let integer=query(&engine,"SELECT id,match_count FROM ((SELECT id FROM domains WHERE id=1 ORDER BY id LIMIT 1) RRF (SELECT CAST(1 AS SIGNED) AS id ORDER BY id LIMIT 1)) h");
    assert_eq!(integer.rows.len(), 1);
    assert_eq!(integer.rows[0][1], Value::UInt(2));
    let dates=query(&engine,"SELECT d FROM ((SELECT d FROM domains ORDER BY d LIMIT 2) RRF (SELECT CAST('2026-01-01' AS DATE) AS d ORDER BY d LIMIT 1)) h ORDER BY d");
    assert_eq!(dates.rows.len(), 2);
    let padded=query(&engine,"SELECT k,match_count FROM ((SELECT k FROM domains ORDER BY id LIMIT 2) RRF (SELECT k FROM domains ORDER BY id DESC LIMIT 2)) h");
    assert_eq!(padded.rows.len(), 1);
    assert_eq!(padded.rows[0][1], Value::UInt(2));
}

#[test]
fn hybrid_rrf_randomized_nway_matches_independent_rank_oracle() {
    use std::collections::BTreeMap;
    let (_dir, engine) = table_engine();
    engine
        .execute("CREATE TABLE oracle_ranks(seq INT PRIMARY KEY,entity INT,a INT,b INT)")
        .unwrap();
    let rows = (0..24i64)
        .map(|i| (i + 1, i % 9 + 1, (i * 17 + 3) % 31, (i * 7 + 5) % 29))
        .collect::<Vec<_>>();
    let values = rows
        .iter()
        .map(|(seq, id, a, b)| format!("({seq},{id},{a},{b})"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO oracle_ranks VALUES {values}"))
        .unwrap();
    for trial in 0..28usize {
        let count = 2 + trial % 7;
        let constant = (trial % 5) * 15;
        let mut expected = BTreeMap::<i64, (f64, u64, u64)>::new();
        let mut branches = Vec::new();
        let mut weights = Vec::new();
        for branch in 0..count {
            let weight = if branch == 0 {
                1.0
            } else {
                ((trial + branch) % 4) as f64 / 2.0
            };
            weights.push(weight);
            let limit = 1 + (trial * 7 + branch * 3) % rows.len();
            let field = if branch % 2 == 0 { "a" } else { "b" };
            let descending = (trial + branch) % 2 == 0;
            branches.push(format!("(SELECT entity AS id FROM oracle_ranks ORDER BY {field} {},seq LIMIT {limit}) AS ranker_{branch}",if descending{"DESC"}else{"ASC"}));
            if weight == 0.0 {
                continue;
            }
            let mut ranked = rows.clone();
            ranked.sort_by_key(|row| {
                let value = if branch % 2 == 0 { row.2 } else { row.3 };
                (if descending { -value } else { value }, row.0)
            });
            let mut seen = std::collections::BTreeSet::new();
            for (position, row) in ranked.into_iter().take(limit).enumerate() {
                if seen.insert(row.1) {
                    let rank = position as u64 + 1;
                    let entry = expected.entry(row.1).or_insert((0.0, u64::MAX, 0));
                    entry.0 += weight / (constant as f64 + rank as f64);
                    entry.1 = entry.1.min(rank);
                    entry.2 += 1;
                }
            }
        }
        let mut expected = expected.into_iter().collect::<Vec<_>>();
        expected.sort_by(|a, b| {
            b.1 .0
                .total_cmp(&a.1 .0)
                .then(a.1 .1.cmp(&b.1 .1))
                .then(a.0.cmp(&b.0))
        });
        let sql=format!("SELECT id,rrf_score,best_rank,match_count FROM ({} WITH(RANK_CONSTANT={constant},WEIGHTS=({}))) h ORDER BY rrf_score DESC,best_rank,id",branches.join(" RRF "),weights.iter().map(f64::to_string).collect::<Vec<_>>().join(","));
        let actual = query(&engine, &sql);
        assert_eq!(actual.rows.len(), expected.len(), "{sql}");
        for (actual, (id, (score, best, matches))) in actual.rows.iter().zip(expected) {
            assert_eq!(actual[0], Value::Int(id), "{sql}");
            assert_eq!(actual[1], Value::Double(score), "{sql}");
            assert_eq!(actual[2], Value::UInt(best));
            assert_eq!(actual[3], Value::UInt(matches));
        }
    }
}

#[test]
fn hybrid_rrf_read_committed_pins_one_view_across_concurrent_branches() {
    let (_dir, engine) = table_engine();
    let (arrived_tx, arrived_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let receiver = std::sync::Mutex::new(resume_rx);
    let hook: crate::engine::executor::select::hybrid::BranchHook = Arc::new(move |branch| {
        if branch == 0 {
            arrived_tx.send(()).unwrap();
            receiver
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10))
                .unwrap();
        }
    });
    std::thread::scope(|scope| {
        let writer_engine = &engine;
        let writer = scope.spawn(move || {
            arrived_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            writer_engine
                .execute("DELETE FROM rrft WHERE id=1")
                .unwrap();
            writer_engine
                .execute("INSERT INTO rrft VALUES(4,4,'later commit','[4,0]')")
                .unwrap();
            resume_tx.send(()).unwrap();
        });
        let _hook = crate::engine::executor::select::hybrid::enter_branch_hook(Some(hook));
        let result=query(&engine,"SELECT id,match_count FROM ((SELECT id FROM rrft ORDER BY id LIMIT 10) RRF (SELECT id FROM rrft ORDER BY id DESC LIMIT 10)) h ORDER BY id");
        assert_eq!(
            result.rows,
            vec![
                vec![Value::Int(1), Value::UInt(2)],
                vec![Value::Int(2), Value::UInt(2)],
                vec![Value::Int(3), Value::UInt(2)]
            ]
        );
        writer.join().unwrap();
    });
}

#[test]
fn hybrid_rrf_deadline_errors_do_not_return_partial_fusion() {
    let (_dir, engine) = table_engine();
    let before = std::time::Instant::now();
    let result=engine.execute("SELECT /*+ MAX_EXECUTION_TIME(10) */ * FROM ((SELECT id FROM rrft ORDER BY id LIMIT 3) RRF (SELECT id FROM rrft ORDER BY SLEEP(0.05),id LIMIT 3)) h").and_then(QueryResponse::materialize);
    assert!(
        matches!(result,Err(ref error) if error.code()==3024 || error.code()==1317),
        "{result:?}"
    );
    assert!(before.elapsed() < Duration::from_secs(3));
    assert_eq!(query(&engine, "SELECT 1").rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn hybrid_rrf_committed_fulltext_vector_and_schema_survive_reopen() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let sql="SELECT id,match_count FROM ((SELECT id FROM persistent_ranks WHERE MATCH(body) AGAINST('database') ORDER BY MATCH(body) AGAINST('database') DESC,id LIMIT 5) RRF (SELECT /*+ HNSW_EF(64) */ id FROM persistent_ranks ORDER BY L2_DISTANCE(v,TO_VECTOR('[1,0]')),id LIMIT 5)) h ORDER BY id";
    let expected = {
        let engine = open_relaxed_test_engine(dir.path());
        engine.execute("CREATE TABLE persistent_ranks(id INT PRIMARY KEY,body TEXT,v VECTOR(2) NOT NULL,FULLTEXT KEY ft(body))").unwrap();
        engine.execute("INSERT INTO persistent_ranks VALUES(1,'database systems','[1,0]'),(2,'database transactions','[2,0]'),(3,'other topic','[3,0]')").unwrap();
        engine
            .execute("CREATE INDEX hv ON persistent_ranks(v) USING HNSW")
            .unwrap();
        query(&engine, sql).rows
    };
    let reopened = open_relaxed_test_engine(dir.path());
    assert_eq!(query(&reopened, sql).rows, expected);
}

#[test]
fn hybrid_rrf_computed_identity_uses_declared_result_domain() {
    let (_dir, engine) = table_engine();
    let sql="SELECT identity,match_count FROM ((SELECT CASE WHEN id=1 THEN 7 ELSE '7' END AS identity FROM rrft WHERE id=1 ORDER BY id LIMIT 1) RRF (SELECT CASE WHEN id=1 THEN 7 ELSE '7' END AS identity FROM rrft WHERE id=2 ORDER BY id LIMIT 1)) h";
    let result = query(&engine, sql);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0].to_display_string(), "7");
    assert_eq!(result.rows[0][1], Value::UInt(2));
}

#[test]
fn hybrid_rrf_checks_all_branch_privileges_including_disabled_inputs() {
    let (_dir, engine) = table_engine();
    engine
        .execute("CREATE USER 'rrf_reader'@'%' IDENTIFIED BY 'pw'")
        .unwrap();
    engine
        .execute("GRANT SELECT(id) ON test.rrft TO 'rrf_reader'@'%'")
        .unwrap();
    let mut reader = SessionContext::default();
    reader.user = "rrf_reader".into();
    reader.login_user = "rrf_reader".into();
    reader.authenticated_as = AccountKey::new("rrf_reader", "%");
    reader.auth_epoch = engine.db.auth_catalog().epoch;
    let allowed="SELECT id,rrf_score FROM ((SELECT id FROM rrft ORDER BY id LIMIT 3) RRF (SELECT id FROM rrft ORDER BY id DESC LIMIT 3)) h ORDER BY id";
    assert!(engine
        .execute_with_session(&mut reader, allowed)
        .unwrap()
        .materialize()
        .is_ok());
    assert!(engine
        .execute_with_session(
            &mut reader,
            "SELECT id FROM (SELECT x.* FROM (SELECT id FROM rrft) x) y"
        )
        .unwrap()
        .materialize()
        .is_ok());
    let denied="SELECT id FROM ((SELECT id FROM rrft ORDER BY id LIMIT 3) RRF (SELECT id FROM rrft ORDER BY grp LIMIT 3) WITH(WEIGHTS=(1,0))) h";
    assert!(
        matches!(engine.execute_with_session(&mut reader,denied).and_then(QueryResponse::materialize),Err(error) if error.code()==1142 || error.code()==1143)
    );
    engine
        .execute("REVOKE SELECT(id) ON test.rrft FROM 'rrf_reader'@'%'")
        .unwrap();
    reader.auth_epoch = engine.db.auth_catalog().epoch;
    assert!(engine
        .execute_with_session(&mut reader, allowed)
        .and_then(QueryResponse::materialize)
        .is_err());
}

#[test]
fn hybrid_rrf_constant_lists_preserve_zero_column_source_cardinality() {
    let (_dir, engine) = table_engine();
    let source="((SELECT CAST(1 AS SIGNED) AS id ORDER BY id LIMIT 1) RRF (SELECT CAST(1 AS SIGNED) AS id ORDER BY id LIMIT 1))";
    let result = query(&engine, &format!("SELECT id,match_count FROM {source} h"));
    assert_eq!(result.rows, vec![vec![Value::Int(1), Value::UInt(2)]]);
    let _mode = crate::engine::executor::vec::config::enter_mode(
        crate::engine::executor::vec::config::VecMode::Off,
    );
    assert_eq!(
        query(&engine, &format!("SELECT id,match_count FROM {source} h")).rows,
        result.rows
    );
}

#[test]
fn hybrid_rrf_vector_ties_sort_full_rerank_pool_before_limit() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE vector_ties(id INT PRIMARY KEY,v VECTOR(2) NOT NULL)")
        .unwrap();
    engine
        .execute("INSERT INTO vector_ties VALUES(9,'[1,0]'),(7,'[1,0]'),(3,'[1,0]'),(1,'[1,0]')")
        .unwrap();
    engine
        .execute("CREATE INDEX hv ON vector_ties(v) USING HNSW")
        .unwrap();
    let branch = "SELECT /*+ HNSW_EF(64) */ id FROM vector_ties ORDER BY L2_DISTANCE(v,TO_VECTOR('[1,0]')),id LIMIT 1";
    let fusion = format!("SELECT id FROM (({branch}) RRF (SELECT id FROM vector_ties ORDER BY id LIMIT 1) WITH(WEIGHTS=(1,0))) h");
    for mode in ["strict_order", "relaxed_order"] {
        let mut session = SessionContext::default();
        engine
            .execute_with_session(
                &mut session,
                &format!("SET mydb_hnsw_iterative_scan='{mode}'"),
            )
            .unwrap();
        for sql in [branch, fusion.as_str()] {
            let QueryResponse::ResultSet(result) = engine
                .execute_with_session(&mut session, sql)
                .unwrap()
                .materialize()
                .unwrap()
            else {
                panic!("rows")
            };
            assert_eq!(result.rows, vec![vec![Value::Int(1)]], "{mode}: {sql}");
        }
    }
}
