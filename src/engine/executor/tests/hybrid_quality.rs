//! Independent oracles and failure-recovery coverage for rank fusion.
use super::*;

#[test]
fn hybrid_rrf_details_conserve_scores_and_keep_best_duplicate_rank() {
    let (_dir, engine) = table_engine();
    engine
        .execute("CREATE TABLE detail_ranks(seq INT PRIMARY KEY,id INT,s INT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO detail_ranks VALUES(1,10,90),(2,10,80),(3,20,70),(4,NULL,100),(5,30,60)",
        )
        .unwrap();
    let sql="SELECT id,rrf_score,best_rank,match_count,rrf_details FROM ((SELECT id FROM detail_ranks WHERE seq<=3 ORDER BY seq LIMIT 3) AS first_list RRF (SELECT id FROM detail_ranks WHERE seq IN (3,5) ORDER BY s DESC LIMIT 2) AS second_list RRF (SELECT id FROM detail_ranks WHERE id IS NULL ORDER BY seq LIMIT 1) AS disabled WITH(RANK_CONSTANT=7,WEIGHTS=(1,2,0),SCORE_DETAILS=TRUE)) h ORDER BY id";
    let result = query(&engine, sql);
    assert_eq!(result.rows.len(), 3);
    for (row, (id, ranks, ordered)) in result.rows.iter().zip([
        (10, [Some(1), None, None], [Some(1), None, None]),
        (20, [Some(3), Some(1), None], [Some(3), Some(70), None]),
        (30, [None, Some(2), None], [None, Some(60), None]),
    ]) {
        assert_eq!(row[0], Value::Int(id));
        let details: serde_json::Value = serde_json::from_str(&row[4].to_display_string()).unwrap();
        let details = details.as_array().unwrap();
        assert_eq!(details.len(), 3);
        let mut score = 0.0;
        for (i, detail) in details.iter().enumerate() {
            let weight = [1.0, 2.0, 0.0][i];
            let contribution = ranks[i].map_or(0.0, |rank| weight / (7.0 + rank as f64));
            assert_eq!(detail["branch_id"], i + 1);
            assert_eq!(detail["name"], ["first_list", "second_list", "disabled"][i]);
            assert_eq!(detail["weight"], weight);
            assert_eq!(detail["disabled"], i == 2);
            assert_eq!(detail["rank"], serde_json::to_value(ranks[i]).unwrap());
            assert_eq!(detail["contribution"], contribution);
            if let Some(value) = ordered[i] {
                assert_eq!(detail["ordered_values"][0]["value"], value);
                assert_eq!(detail["ordered_values"][1]["value"], id);
            } else {
                assert!(detail["ordered_values"].is_null());
            }
            score += contribution;
        }
        assert_eq!(row[1], Value::Double(score));
        assert_eq!(
            row[2],
            Value::UInt(ranks.into_iter().flatten().min().unwrap())
        );
        assert_eq!(
            row[3],
            Value::UInt(ranks.into_iter().flatten().count() as u64)
        );
    }
}

#[test]
fn hybrid_rrf_details_recognize_distance_functions_and_metrics() {
    let (_dir, engine) = table_engine();
    for (expression, metric) in [
        ("DISTANCE(v,TO_VECTOR('[1,1]'))", "L2"),
        ("VEC_DISTANCE(v,TO_VECTOR('[1,1]'),'cosine')", "COSINE"),
        ("DISTANCE(v,TO_VECTOR('[1,1]'),'inner_product')", "DOT"),
        ("MAX_INNER_PRODUCT_DISTANCE(v,TO_VECTOR('[1,1]'))", "DOT"),
    ] {
        let sql = format!("SELECT rrf_details FROM ((SELECT id FROM rrft ORDER BY {expression},id LIMIT 2) RRF (SELECT id FROM rrft ORDER BY id LIMIT 2) WITH(SCORE_DETAILS=TRUE)) h");
        let rows = query(&engine, &sql).rows;
        for row in rows {
            let details: serde_json::Value =
                serde_json::from_str(&row[0].to_display_string()).unwrap();
            assert_eq!(
                details[0]["ranker"]["kind"], "vector_distance",
                "{expression}: {details}"
            );
            assert_eq!(details[0]["ranker"]["metric"], metric, "{expression}");
        }
    }
}

#[test]
fn hybrid_rrf_prepared_default_metric_tracks_operand_domains_in_local_ctes() {
    use crate::engine::api::QueryEngine as _;
    use crate::engine::params::with_params;
    let (_dir, engine) = table_engine();
    let mut session = SessionContext::default();
    let sql = "SELECT rrf_details FROM ((WITH q AS (SELECT TO_VECTOR(?,?) AS a,TO_VECTOR(?,?) AS b) SELECT 1 AS id FROM q ORDER BY DISTANCE(a,b) LIMIT 1) RRF (SELECT 1 AS id ORDER BY id LIMIT 1) WITH(SCORE_DETAILS=TRUE)) h";
    let statement = engine.prepare_cached(&mut session, sql, 4).unwrap();
    for (datatype, metric, distance) in [
        ("FLOAT32", "L2", 2.0f64.sqrt()),
        ("BINARY", "HAMMING", 2.0),
        ("FLOAT16", "L2", 2.0f64.sqrt()),
    ] {
        let params = vec![
            Value::String("[1,0]".into()),
            Value::String(datatype.into()),
            Value::String("[0,1]".into()),
            Value::String(datatype.into()),
        ];
        let response = with_params(params, || {
            engine.execute_prepared_statement(&mut session, sql, (*statement).clone())
        })
        .unwrap()
        .unwrap()
        .materialize()
        .unwrap();
        let QueryResponse::ResultSet(result) = response else {
            panic!("rows required")
        };
        assert_eq!(result.rows.len(), 1);
        let details: serde_json::Value =
            serde_json::from_str(&result.rows[0][0].to_display_string()).unwrap();
        assert_eq!(details[0]["ranker"]["kind"], "vector_distance");
        assert_eq!(
            details[0]["ranker"]["metric"], metric,
            "{datatype}: {details}"
        );
        assert_eq!(
            details[0]["ordered_values"][0]["value"].as_f64().unwrap(),
            distance
        );
    }
}

#[test]
fn hybrid_rrf_prepared_invalid_options_recover_on_next_execution() {
    use crate::engine::api::QueryEngine as _;
    use crate::engine::params::with_params;
    let (_dir, engine) = table_engine();
    let mut session = SessionContext::default();
    let sql="SELECT id,rrf_score FROM ((SELECT id FROM rrft ORDER BY id LIMIT ? OFFSET ?) RRF (SELECT id FROM rrft ORDER BY id DESC LIMIT ?) WITH(RANK_CONSTANT=?,WEIGHTS=(?,?))) h ORDER BY rrf_score DESC,best_rank,id";
    let statement = engine.prepare_cached(&mut session, sql, 6).unwrap();
    let valid = vec![
        Value::Int(3),
        Value::Int(0),
        Value::Int(3),
        Value::UInt(u32::MAX as u64),
        Value::Int(1),
        Value::Int(0),
    ];
    for (slot, invalid) in [
        (0, Value::Int(-1)),
        (1, Value::Int(1)),
        (3, Value::UInt(u32::MAX as u64 + 1)),
        (3, Value::Decimal("60.0000000000000000000000001".into())),
        (3, Value::Null),
        (4, Value::Double(f64::NAN)),
        (4, Value::Double(f64::INFINITY)),
        (4, Value::Int(-1)),
        (4, Value::String("1".into())),
        (4, Value::Int(0)),
    ] {
        let mut params = valid.clone();
        params[slot] = invalid;
        let response = with_params(params, || {
            engine.execute_prepared_statement(&mut session, sql, (*statement).clone())
        })
        .expect("prepared SELECT")
        .and_then(QueryResponse::materialize);
        assert!(response.is_err(), "invalid option slot {slot}");
        let response = with_params(valid.clone(), || {
            engine.execute_prepared_statement(&mut session, sql, (*statement).clone())
        })
        .unwrap()
        .unwrap()
        .materialize()
        .unwrap();
        let QueryResponse::ResultSet(result) = response else {
            panic!("rows required")
        };
        assert_eq!(result.rows.len(), 3);
        for (i, row) in result.rows.iter().enumerate() {
            assert_eq!(row[0], Value::Int(i as i64 + 1));
            assert_eq!(
                row[1],
                Value::Double(1.0 / (u32::MAX as f64 + i as f64 + 1.0))
            );
        }
    }
}

#[test]
fn hybrid_rrf_native_vector_indexes_match_independent_rank_oracle() {
    native_vector_rank_matrix("memory");
}

#[test]
fn hybrid_rrf_native_paged_vector_indexes_match_independent_rank_oracle() {
    native_vector_rank_matrix("paged");
}

fn native_vector_rank_matrix(backend: &str) {
    use crate::engine::executor::tests::vector_type::HnswEnv;
    use std::collections::{BTreeMap, BTreeSet};
    let _env = HnswEnv::set(&[
        ("MYDB_VECTOR_STORAGE", backend),
        ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
        ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
    ]);
    for datatype in ["FLOAT32", "FLOAT16", "BFLOAT16", "INT8", "BINARY"] {
        let binary = datatype == "BINARY";
        let dim = if binary { 9 } else { 3 };
        let metrics: &[&str] = if binary {
            &["HAMMING", "JACCARD"]
        } else {
            &["L2", "COSINE", "DOT"]
        };
        let vectors = (1..=40i64)
            .map(|seq| {
                (0..dim)
                    .map(|d| {
                        if binary {
                            ((seq >> (d % 6)) & 1) as f64
                        } else {
                            ((seq * (d as i64 + 3) + d as i64 * 7) % 23 + 1) as f64
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let text = |v: &[f64]| {
            format!(
                "[{}]",
                v.iter().map(f64::to_string).collect::<Vec<_>>().join(",")
            )
        };
        for &metric in metrics {
            for algorithm in ["HNSW", "IVF"] {
                let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
                let engine = open_test_engine(dir.path());
                engine.execute(&format!("CREATE TABLE vector_ranks(seq INT PRIMARY KEY,entity INT,v VECTOR({dim},{datatype}) NOT NULL)")).unwrap();
                let values = vectors
                    .iter()
                    .enumerate()
                    .map(|(i, v)| format!("({},{},'{}')", i + 1, i % 11 + 1, text(v)))
                    .collect::<Vec<_>>()
                    .join(",");
                engine
                    .execute(&format!("INSERT INTO vector_ranks VALUES {values}"))
                    .unwrap();
                let (options, hint) = if algorithm == "HNSW" {
                    ("m=4,ef_construction=64", "HNSW_EF(64)")
                } else {
                    ("nlist=4,kmeans_iters=2", "IVF_NPROBE(4)")
                };
                engine.execute(&format!("CREATE INDEX vx ON vector_ranks(v) USING {algorithm} WITH(metric={metric},{options})")).unwrap();
                let mut branches = Vec::new();
                let mut expected = BTreeMap::<i64, (f64, u64, u64)>::new();
                for (branch, weight) in [1.0, 2.0].into_iter().enumerate() {
                    let q = (0..dim)
                        .map(|d| {
                            if binary {
                                ((d + branch) % 2) as f64
                            } else {
                                (d + branch * 3 + 1) as f64 + 0.25
                            }
                        })
                        .collect::<Vec<_>>();
                    let limit = if branch == 0 { 13 } else { 17 };
                    let condition = if branch == 0 { "seq%3<>0" } else { "seq%4<>0" };
                    branches.push(format!("(SELECT /*+ {hint} */ entity AS id FROM vector_ranks WHERE {condition} ORDER BY DISTANCE(v,TO_VECTOR('{}','{}'),'{metric}'),seq LIMIT {limit})",text(&q),if binary {"BINARY"} else {"FLOAT32"}));
                    let mut ranked = vectors
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| (i + 1) % (branch + 3) != 0)
                        .map(|(i, v)| {
                            let dot = v.iter().zip(&q).map(|(a, b)| a * b).sum::<f64>();
                            let score = match metric {
                                "L2" => v.iter().zip(&q).map(|(a, b)| (a - b).powi(2)).sum::<f64>(),
                                "DOT" => -dot,
                                "COSINE" => {
                                    1.0 - dot
                                        / (v.iter().map(|a| a * a).sum::<f64>().sqrt()
                                            * q.iter().map(|b| b * b).sum::<f64>().sqrt())
                                }
                                "HAMMING" => {
                                    v.iter().zip(&q).filter(|(a, b)| a != b).count() as f64
                                }
                                "JACCARD" => {
                                    1.0 - v
                                        .iter()
                                        .zip(&q)
                                        .filter(|(a, b)| **a != 0.0 && **b != 0.0)
                                        .count() as f64
                                        / v.iter()
                                            .zip(&q)
                                            .filter(|(a, b)| **a != 0.0 || **b != 0.0)
                                            .count()
                                            as f64
                                }
                                _ => unreachable!(),
                            };
                            (score, i + 1, (i % 11 + 1) as i64)
                        })
                        .collect::<Vec<_>>();
                    ranked.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
                    let mut seen = BTreeSet::new();
                    for (i, (_, _, id)) in ranked.into_iter().take(limit).enumerate() {
                        if seen.insert(id) {
                            let rank = i as u64 + 1;
                            let entry = expected.entry(id).or_insert((0.0, u64::MAX, 0));
                            entry.0 += weight / (7.0 + rank as f64);
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
                let sql=format!("SELECT id,rrf_score,best_rank,match_count FROM ({} WITH(RANK_CONSTANT=7,WEIGHTS=(1,2))) h ORDER BY rrf_score DESC,best_rank,id",branches.join(" RRF "));
                let expected = expected
                    .into_iter()
                    .map(|(id, (score, best, matches))| {
                        vec![
                            Value::Int(id),
                            Value::Double(score),
                            Value::UInt(best),
                            Value::UInt(matches),
                        ]
                    })
                    .collect::<Vec<_>>();
                assert_eq!(
                    query(&engine, &sql).rows,
                    expected,
                    "{datatype}/{metric}/{algorithm}"
                );
                let explain = query(&engine, &format!("EXPLAIN {sql}"))
                    .rows
                    .into_iter()
                    .flatten()
                    .map(|value| value.to_display_string())
                    .collect::<Vec<_>>()
                    .join(" ");
                assert_eq!(
                    explain
                        .matches(&format!("Using vector index ({algorithm})"))
                        .count(),
                    2,
                    "{datatype}/{metric}: {explain}"
                );
                assert_eq!(
                    query(&engine, &sql.replace(hint, "NO_VECTOR_INDEX()")).rows,
                    expected,
                    "scalar {datatype}/{metric}/{algorithm}"
                );
            }
        }
    }
}
