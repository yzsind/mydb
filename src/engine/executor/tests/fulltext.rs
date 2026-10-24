use super::*;

#[test]
fn fulltext_repairs_certify_partial_updates_prefixes_and_phrases() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE ft_versions(id INT PRIMARY KEY,body TEXT,FULLTEXT KEY ft(body))")
        .unwrap();
    engine.execute("INSERT INTO ft_versions VALUES(1,'database systems archive'),(2,'database journal history')").unwrap();
    let mut reader = SessionContext::default();
    engine
        .execute_with_session(
            &mut reader,
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        )
        .unwrap();
    engine.execute_with_session(&mut reader, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut reader, "SELECT body FROM ft_versions ORDER BY id")
        .unwrap();
    // Replacing two equally long tokens preserves the database posting. Its
    // eventual retirement must not attribute its creation to this update.
    engine
        .execute("UPDATE ft_versions SET body='database pending content' WHERE id=1")
        .unwrap();
    let mut writer = SessionContext::default();
    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
    engine
        .execute_with_session(
            &mut writer,
            "UPDATE ft_versions SET body='transient pending content' WHERE id=1",
        )
        .unwrap();
    for predicate in [
        "'database'",
        "'+data*' IN BOOLEAN MODE",
        "'\"database systems\"' IN BOOLEAN MODE",
        "'+systems +archive' IN BOOLEAN MODE",
    ] {
        let sql = format!(
            "SELECT id FROM ft_versions WHERE MATCH(body) AGAINST({predicate}) ORDER BY id"
        );
        let QueryResponse::ResultSet(result) =
            engine.execute_with_session(&mut reader, &sql).unwrap()
        else {
            panic!("expected rows")
        };
        let expected = if predicate.contains("systems") {
            vec![vec![Value::Int(1)]]
        } else {
            vec![vec![Value::Int(1)], vec![Value::Int(2)]]
        };
        assert_eq!(result.rows, expected, "{sql}");
    }
    assert!(exec_set(
        &engine,
        "SELECT id FROM ft_versions WHERE MATCH(body) AGAINST('transient')"
    )
    .rows
    .is_empty());
    engine
        .execute_with_session(&mut writer, "SAVEPOINT middle")
        .unwrap();
    engine
        .execute_with_session(
            &mut writer,
            "UPDATE ft_versions SET body='second hidden words' WHERE id=1",
        )
        .unwrap();
    for action in ["ROLLBACK TO SAVEPOINT middle", "ROLLBACK"] {
        engine.execute_with_session(&mut writer, action).unwrap();
        assert!(exec_set(
            &engine,
            "SELECT id FROM ft_versions WHERE MATCH(body) AGAINST('hidden transient')"
        )
        .rows
        .is_empty());
        let QueryResponse::ResultSet(result) = engine.execute_with_session(&mut reader, "SELECT id FROM ft_versions WHERE MATCH(body) AGAINST('+systems +archive' IN BOOLEAN MODE)").unwrap() else {panic!("expected rows")};
        assert_eq!(result.rows, vec![vec![Value::Int(1)]]);
    }
    engine.execute_with_session(&mut reader, "COMMIT").unwrap();
    assert!(exec_set(
        &engine,
        "SELECT id FROM ft_versions WHERE MATCH(body) AGAINST('systems archive')"
    )
    .rows
    .is_empty());
}

#[test]
fn fulltext_comment_ddl_roundtrip_and_auto_increment_validation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let sql = "CREATE TABLE `mysql_struct_test2` (\
        `id` bigint unsigned NOT NULL AUTO_INCREMENT,\
        `char_col` char(3) CHARACTER SET utf8mb4 COLLATE utf8mb4_la_0900_as_cs DEFAULT 'ab',\
        FULLTEXT KEY `idx_fulltext` (`char_col`) COMMENT '全文索引'\
        ) ENGINE=InnoDB AUTO_INCREMENT=16 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci";
    let err = engine.execute(sql).unwrap_err();
    assert_eq!(err.code(), 1075, "{}", err.message());
    let valid = sql.replace("FULLTEXT KEY", "PRIMARY KEY (`id`), FULLTEXT KEY");
    engine.execute(&valid).unwrap();
    engine
        .execute("INSERT INTO mysql_struct_test2 () VALUES ()")
        .unwrap();
    let rows = rows_to_strings(exec_set(
        &engine,
        "SELECT id, char_col FROM mysql_struct_test2",
    ));
    assert_eq!(rows, vec![vec!["16".to_string(), "ab".to_string()]]);
    let ddl =
        rows_to_strings(exec_set(&engine, "SHOW CREATE TABLE mysql_struct_test2"))[0][1].clone();
    assert!(ddl.contains("COMMENT '全文索引'"), "{ddl}");
    assert!(ddl.contains("utf8mb4_la_0900_as_cs"), "{ddl}");
    engine
        .execute(&ddl.replace("`mysql_struct_test2`", "`fulltext_comment_copy`"))
        .unwrap();
    let indexes = exec_set(&engine, "SHOW INDEX FROM fulltext_comment_copy");
    assert!(indexes
        .rows
        .iter()
        .any(|row| row.contains(&Value::String("全文索引".into()))));
    engine.execute("ALTER TABLE fulltext_comment_copy ADD FULLTEXT KEY ft2 (char_col) COMMENT '第二个索引' WITH PARSER ngram").unwrap();
    let ddl =
        rows_to_strings(exec_set(&engine, "SHOW CREATE TABLE fulltext_comment_copy"))[0][1].clone();
    assert!(ddl.contains("COMMENT '第二个索引'"), "{ddl}");
    engine
        .execute(&ddl.replace("`fulltext_comment_copy`", "`fulltext_comment_copy2`"))
        .unwrap();
}

fn rows_to_strings(rs: ResultSet) -> Vec<Vec<String>> {
    rs.rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|v| match v {
                    Value::Null => "NULL".to_string(),
                    Value::String(s) => s,
                    other => other.to_display_string(),
                })
                .collect()
        })
        .collect()
}

fn exec_set(engine: &StorageQueryEngine, sql: &str) -> ResultSet {
    let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap().materialize().unwrap() else {
        panic!("expected result set for {sql}");
    };
    rs
}

/// Column-0 values of a result set, in row order.
fn first_col(rs: ResultSet) -> Vec<String> {
    rows_to_strings(rs)
        .into_iter()
        .map(|mut row| row.remove(0))
        .collect()
}

fn docs_engine(dir: &std::path::Path) -> StorageQueryEngine {
    let engine = open_test_engine(dir);
    engine
        .execute("CREATE TABLE docs (id INT PRIMARY KEY, body TEXT, FULLTEXT KEY ft (body))")
        .unwrap();
    engine
        .execute("INSERT INTO docs VALUES (1, 'database systems database tuning')")
        .unwrap();
    engine
        .execute("INSERT INTO docs VALUES (2, 'database design')")
        .unwrap();
    engine
        .execute("INSERT INTO docs VALUES (3, 'distributed consensus protocols')")
        .unwrap();
    engine
}

#[test]
fn natural_language_match_returns_only_matching_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = docs_engine(dir.path());
    // Rows 1 and 2 contain "database"; row 3 does not.
    let rs = exec_set(
        &engine,
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('database') ORDER BY id",
    );
    assert_eq!(first_col(rs), vec!["1".to_string(), "2".to_string()]);
}

#[test]
fn fulltext_boolean_contexts_and_multiple_match_fingerprints_are_exact() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE ft_ctx (id INT PRIMARY KEY, body TEXT, title TEXT, tag VARCHAR(16), \
             FULLTEXT KEY ft_body(body), FULLTEXT KEY ft_title(title))",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO ft_ctx VALUES \
             (1,'database internals','storage guide','no'),\
             (2,'other topic','database guide','keep'),\
             (3,'database tuning','other title','no'),\
             (4,'unrelated words','misc notes','keep')",
        )
        .unwrap();

    assert_eq!(
        first_col(exec_set(
            &engine,
            "SELECT id FROM ft_ctx WHERE MATCH(body) AGAINST('database') \
             OR tag='keep' ORDER BY id"
        )),
        vec!["1", "2", "3", "4"]
    );
    assert_eq!(
        first_col(exec_set(
            &engine,
            "SELECT id FROM ft_ctx WHERE MATCH(body) AGAINST('database') = 0 ORDER BY id"
        )),
        vec!["2", "4"]
    );
    assert_eq!(
        first_col(exec_set(
            &engine,
            "SELECT id FROM ft_ctx WHERE NOT MATCH(body) AGAINST('database') ORDER BY id"
        )),
        vec!["2", "4"]
    );
    assert_eq!(
        first_col(exec_set(
            &engine,
            "SELECT id FROM ft_ctx WHERE MATCH(body) AGAINST('database') \
             AND MATCH(title) AGAINST('guide') ORDER BY id"
        )),
        vec!["1"]
    );
    assert_eq!(
        first_col(exec_set(
            &engine,
            "SELECT id FROM ft_ctx WHERE MATCH(body) AGAINST('database') \
             OR MATCH(title) AGAINST('guide') ORDER BY id"
        )),
        vec!["1", "2", "3"]
    );

    let projected = rows_to_strings(exec_set(
        &engine,
        "SELECT id, MATCH(body) AGAINST('database') AS b, \
         MATCH(title) AGAINST('guide') AS t FROM ft_ctx ORDER BY id",
    ));
    assert_eq!(projected.len(), 4);
    assert!(projected[0][1].parse::<f64>().unwrap() > 0.0);
    assert!(projected[0][2].parse::<f64>().unwrap() > 0.0);
    assert_eq!(projected[3][1].parse::<f64>().unwrap(), 0.0);
    assert_eq!(projected[3][2].parse::<f64>().unwrap(), 0.0);

    assert_eq!(
        first_col(exec_set(
            &engine,
            "SELECT id FROM ft_ctx WHERE MATCH(body) AGAINST('database') AND id=1",
        )),
        vec!["1"]
    );
    let base_explain = rows_to_strings(exec_set(
        &engine,
        "EXPLAIN FORMAT=TREE SELECT id FROM ft_ctx \
         WHERE MATCH(body) AGAINST('database') AND id=1",
    ))
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join(" ");
    assert!(
        base_explain.contains("FULLTEXT + B-tree candidates"),
        "ordinary selective conjunct should contribute candidates: {base_explain}"
    );
}

#[test]
fn prepared_against_parameter_is_bound_per_execute() {
    use crate::engine::api::QueryEngine;

    let dir = tempfile::tempdir().unwrap();
    let engine = docs_engine(dir.path());
    let mut session = SessionContext::default();
    let sql = "SELECT id FROM docs WHERE MATCH(body) AGAINST(?) ORDER BY id";
    let planned = engine.prepare_cached(&mut session, sql, 1).unwrap();
    for (query, expected) in [
        ("database", vec!["1", "2"]),
        ("consensus", vec!["3"]),
        ("missing", vec![]),
    ] {
        let result =
            crate::engine::params::with_params(vec![Value::String(query.to_string())], || {
                engine
                    .execute_prepared_statement(&mut session, sql, (*planned).clone())
                    .expect("prepared MATCH")
            })
            .unwrap();
        let QueryResponse::ResultSet(result) = result.materialize().unwrap() else {
            panic!("expected result set")
        };
        assert_eq!(first_col(result), expected, "query={query}");
    }
}

#[test]
fn analyze_collects_fulltext_document_and_token_df_stats() {
    let dir = tempfile::tempdir().unwrap();
    let engine = docs_engine(dir.path());
    engine.execute("ANALYZE TABLE docs").unwrap();
    let table = engine.db_for_tests().table("docs").unwrap();
    let stats = table
        .stats
        .indexes
        .get("ft")
        .and_then(|stats| stats.fulltext.as_ref())
        .expect("FULLTEXT stats after ANALYZE");
    assert_eq!(stats.doc_count, 3);
    assert!(stats.avg_token_count > 0.0);
    assert_eq!(
        stats
            .token_df_top
            .iter()
            .find(|entry| entry.token == "database")
            .map(|entry| entry.document_count),
        Some(2)
    );
}

#[test]
fn natural_language_ranks_higher_term_frequency_first() {
    let dir = tempfile::tempdir().unwrap();
    let engine = docs_engine(dir.path());
    // Both rows match "database"; row 1 has it twice → higher BM25 score.
    let rs = exec_set(
        &engine,
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('database') \
         ORDER BY MATCH(body) AGAINST('database') DESC",
    );
    assert_eq!(first_col(rs), vec!["1".to_string(), "2".to_string()]);
}

#[test]
fn natural_language_rarer_term_lifts_rank() {
    let dir = tempfile::tempdir().unwrap();
    let engine = docs_engine(dir.path());
    // Row 2 matches both "database" and the rarer "design"; row 1 only
    // "database". Matching the rarer term should rank row 2 first.
    let rs = exec_set(
        &engine,
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('database design') \
         ORDER BY MATCH(body) AGAINST('database design') DESC",
    );
    assert_eq!(first_col(rs), vec!["2".to_string(), "1".to_string()]);
}

#[test]
fn order_by_relevance_alias_matches_raw_expr() {
    let dir = tempfile::tempdir().unwrap();
    let engine = docs_engine(dir.path());
    // Blueprint smoke shape: relevance aliased in SELECT, ORDER BY the alias.
    let rs = exec_set(
        &engine,
        "SELECT id, MATCH(body) AGAINST('database design') AS score FROM docs \
         WHERE MATCH(body) AGAINST('database design') ORDER BY score DESC",
    );
    assert_eq!(first_col(rs), vec!["2".to_string(), "1".to_string()]);
}

#[test]
fn select_relevance_score_is_positive_for_matches() {
    let dir = tempfile::tempdir().unwrap();
    let engine = docs_engine(dir.path());
    let rs = exec_set(
        &engine,
        "SELECT id, MATCH(body) AGAINST('database') AS score FROM docs \
         WHERE MATCH(body) AGAINST('database') ORDER BY id",
    );
    let rows = rows_to_strings(rs);
    assert_eq!(rows.len(), 2);
    for row in &rows {
        let score: f64 = row[1].parse().unwrap();
        assert!(score > 0.0, "expected positive relevance, got {}", row[1]);
    }
}

#[test]
fn match_is_case_insensitive() {
    let dir = tempfile::tempdir().unwrap();
    let engine = docs_engine(dir.path());
    // Upper-case query must still match lower-cased indexed tokens.
    let rs = exec_set(
        &engine,
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('DATABASE') ORDER BY id",
    );
    assert_eq!(first_col(rs), vec!["1".to_string(), "2".to_string()]);
}

#[test]
fn match_no_corpus_hit_returns_empty() {
    let dir = tempfile::tempdir().unwrap();
    let engine = docs_engine(dir.path());
    let rs = exec_set(
        &engine,
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('nonexistentword')",
    );
    assert!(first_col(rs).is_empty());
}

#[test]
fn match_without_fulltext_index_errors_1191() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE plain (id INT PRIMARY KEY, body TEXT)")
        .unwrap();
    engine
        .execute("INSERT INTO plain VALUES (1, 'hello world')")
        .unwrap();
    let err = engine
        .execute("SELECT id FROM plain WHERE MATCH(body) AGAINST('hello')")
        .unwrap_err();
    assert_eq!(err.code(), 1191, "expected MySQL error 1191, got {err:?}");
}

#[test]
fn select_star_excludes_relevance_pseudo_column() {
    let dir = tempfile::tempdir().unwrap();
    let engine = docs_engine(dir.path());
    let rs = exec_set(
        &engine,
        "SELECT * FROM docs WHERE MATCH(body) AGAINST('database') ORDER BY id",
    );
    // Only the two declared columns (id, body) — no hidden score column.
    assert_eq!(rs.columns.len(), 2, "columns: {:?}", rs.columns);
    for col in &rs.columns {
        assert!(
            !col.name.starts_with("__ft_score_"),
            "score pseudo-column leaked into SELECT *: {}",
            col.name
        );
    }
    let rows = rows_to_strings(rs);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].len(), 2);
}

#[test]
fn explain_shows_fulltext_scan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = docs_engine(dir.path());
    let rs = exec_set(
        &engine,
        "EXPLAIN SELECT id FROM docs WHERE MATCH(body) AGAINST('database')",
    );
    let dump = rows_to_strings(rs)
        .into_iter()
        .map(|r| r.join(" | "))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        dump.contains("fulltext"),
        "EXPLAIN should report a fulltext access type, got:\n{dump}"
    );
}

#[test]
fn ngram_chinese_natural_language_matches() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE zh (id INT PRIMARY KEY, body TEXT, \
             FULLTEXT KEY ft (body) WITH PARSER ngram)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO zh VALUES (1, '数据库全文检索')")
        .unwrap();
    engine
        .execute("INSERT INTO zh VALUES (2, '全文索引很快')")
        .unwrap();
    engine
        .execute("INSERT INTO zh VALUES (3, '分布式系统')")
        .unwrap();
    // Both rows 1 and 2 contain the full-text two-character ngram; row 3 does not.
    let rs = exec_set(
        &engine,
        "SELECT id FROM zh WHERE MATCH(body) AGAINST('全文') ORDER BY id",
    );
    assert_eq!(first_col(rs), vec!["1".to_string(), "2".to_string()]);
}

#[test]
fn ngram_fulltext_safely_prefilters_unanchored_like() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE like_ngram (id INT PRIMARY KEY, \
             body VARCHAR(128) COLLATE utf8mb4_bin, \
             FULLTEXT KEY ft_body(body) WITH PARSER ngram)",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO like_ngram VALUES \
             (1,'高性能中文数据库'),(2,'中文全文检索'),(3,'分布式系统'),\
             (4,'prefix foo suffix'),(5,'FOO only'),(6,'literal ab_cd 中文'),\
             (7,'中文 data 系统')",
        )
        .unwrap();

    let accelerated = exec_set(
        &engine,
        "SELECT id FROM like_ngram WHERE body LIKE '%中文%' ORDER BY id",
    );
    assert_eq!(first_col(accelerated), vec!["1", "2", "6", "7"]);
    assert_eq!(
        first_col(exec_set(
            &engine,
            "SELECT id FROM like_ngram WHERE body LIKE '%中文%系统%' ORDER BY id",
        )),
        vec!["7"],
        "grams from separate literal segments may be intersected but never crossed"
    );
    assert_eq!(
        first_col(exec_set(
            &engine,
            "SELECT id FROM like_ngram WHERE body LIKE '%foo%' ORDER BY id",
        )),
        vec!["4"],
        "binary collation keeps LIKE case-sensitive"
    );
    assert_eq!(
        first_col(exec_set(
            &engine,
            "SELECT id FROM like_ngram WHERE body LIKE '%ab!_cd%' ESCAPE '!' ORDER BY id",
        )),
        vec!["6"],
        "escaped wildcard is literal and short-token fallback stays exact"
    );
    let explain = rows_to_strings(exec_set(
        &engine,
        "EXPLAIN SELECT id FROM like_ngram WHERE body LIKE '%中文%'",
    ))
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join(" ");
    assert!(
        explain.contains("LIKE ngram prefilter"),
        "expected ngram LIKE access, got {explain}"
    );

    // The convenience `engine.execute()` creates a fresh session per call;
    // keep one session here so the SESSION optimizer switch is observable by
    // both execution and the following EXPLAIN.
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "SET optimizer_switch='like_ngram_prefilter=off'",
        )
        .unwrap();
    let QueryResponse::ResultSet(oracle) = engine
        .execute_with_session(
            &mut session,
            "SELECT id FROM like_ngram WHERE body LIKE '%中文%' ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(first_col(oracle), vec!["1", "2", "6", "7"]);
    let QueryResponse::ResultSet(disabled) = engine
        .execute_with_session(
            &mut session,
            "EXPLAIN SELECT id FROM like_ngram WHERE body LIKE '%中文%'",
        )
        .unwrap()
    else {
        panic!("expected EXPLAIN result set");
    };
    let disabled = rows_to_strings(disabled)
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        !disabled.contains("LIKE ngram prefilter"),
        "disabled switch must replan without ngram: {disabled}"
    );
}

#[test]
fn prepared_ngram_like_reclassifies_tokens_and_safely_falls_back() {
    use crate::engine::api::QueryEngine;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE like_ngram_ps (id INT PRIMARY KEY, \
             body VARCHAR(128) COLLATE utf8mb4_bin, \
             FULLTEXT KEY ft_body(body) WITH PARSER ngram)",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO like_ngram_ps VALUES \
             (1,'分布式中文数据库'),(2,'中文全文索引'),(3,'分布式系统'),(4,'other')",
        )
        .unwrap();
    let mut session = SessionContext::default();
    let sql = "SELECT id FROM like_ngram_ps WHERE body LIKE ? ORDER BY id";
    let planned = engine.prepare_cached(&mut session, sql, 1).unwrap();
    for (pattern, expected) in [
        ("%中文%", vec!["1", "2"]),
        ("%系统%", vec!["3"]),
        // Shorter than the configured gram: no posting may be used, so the
        // runtime template must fall back to a complete residual scan.
        ("%中%", vec!["1", "2"]),
        ("中文%", vec!["2"]),
    ] {
        let response =
            crate::engine::params::with_params(vec![Value::String(pattern.to_string())], || {
                engine
                    .execute_prepared_statement(&mut session, sql, (*planned).clone())
                    .expect("prepared ngram LIKE")
            })
            .unwrap();
        let QueryResponse::ResultSet(result) = response.materialize().unwrap() else {
            panic!("expected result set");
        };
        assert_eq!(first_col(result), expected, "pattern={pattern}");
    }
}

#[test]
fn accent_insensitive_latin_ngram_like_falls_back_without_false_negatives() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE like_ai (id INT PRIMARY KEY, \
             body VARCHAR(128) COLLATE utf8mb4_0900_ai_ci, \
             FULLTEXT KEY ft_body(body) WITH PARSER ngram)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO like_ai VALUES (1,'café foo'),(2,'CAFE FOO'),(3,'other')")
        .unwrap();
    assert_eq!(
        first_col(exec_set(
            &engine,
            "SELECT id FROM like_ai WHERE body LIKE '%cafe%' ORDER BY id"
        )),
        vec!["1", "2"]
    );
    let explain = rows_to_strings(exec_set(
        &engine,
        "EXPLAIN SELECT id FROM like_ai WHERE body LIKE '%cafe%'",
    ))
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join(" ");
    assert!(
        !explain.contains("LIKE ngram prefilter"),
        "AI Latin grams are not representation-safe: {explain}"
    );
}

#[test]
fn show_create_table_renders_fulltext_key() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT, title TEXT, \
             FULLTEXT KEY ft (body), FULLTEXT KEY ftz (title) WITH PARSER ngram)",
        )
        .unwrap();
    let rs = exec_set(&engine, "SHOW CREATE TABLE docs");
    let ddl = rows_to_strings(rs)[0][1].clone();
    assert!(
        ddl.contains("FULLTEXT KEY `ft` (`body`)"),
        "missing default FULLTEXT key, got:\n{ddl}"
    );
    assert!(
        ddl.contains("FULLTEXT KEY `ftz` (`title`) WITH PARSER `ngram`"),
        "missing ngram FULLTEXT key, got:\n{ddl}"
    );
    // The default-parser key must not gain a spurious WITH PARSER clause.
    assert!(
        !ddl.contains("(`body`) WITH PARSER"),
        "default key should not emit WITH PARSER, got:\n{ddl}"
    );
}

#[test]
fn update_maintains_inverted_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = docs_engine(dir.path());
    // Row 1 starts as 'database systems database tuning'. Replace it so the
    // old tokens disappear and a new token appears.
    engine
        .execute("UPDATE docs SET body = 'kubernetes orchestration' WHERE id = 1")
        .unwrap();
    // 'database' must no longer match row 1 (only row 2 retains it).
    let rs = exec_set(
        &engine,
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('database') ORDER BY id",
    );
    assert_eq!(first_col(rs), vec!["2".to_string()]);
    // The freshly-inserted token must now match row 1.
    let rs = exec_set(
        &engine,
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('kubernetes')",
    );
    assert_eq!(first_col(rs), vec!["1".to_string()]);
}

#[test]
fn standalone_create_fulltext_index_ngram_matches_and_persists() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE zh (id INT PRIMARY KEY, body TEXT)")
        .unwrap();
    // The standalone DDL must persist the ngram tokenizer, same as the
    // inline FULLTEXT KEY / ALTER TABLE ADD FULLTEXT paths.
    engine
        .execute("CREATE FULLTEXT INDEX ft ON zh (body) WITH PARSER ngram")
        .unwrap();
    engine
        .execute("INSERT INTO zh VALUES (1, '数据库全文检索')")
        .unwrap();
    engine
        .execute("INSERT INTO zh VALUES (2, '全文索引很快')")
        .unwrap();
    engine
        .execute("INSERT INTO zh VALUES (3, '分布式系统')")
        .unwrap();
    // The full-text two-character ngram matches rows 1 and 2, but not row 3; only the ngram
    // tokenizer splits CJK this way; the default tokenizer would not match.
    let rs = exec_set(
        &engine,
        "SELECT id FROM zh WHERE MATCH(body) AGAINST('全文') ORDER BY id",
    );
    assert_eq!(first_col(rs), vec!["1".to_string(), "2".to_string()]);
    // SHOW CREATE TABLE must render the parser so the DDL round-trips.
    let rs = exec_set(&engine, "SHOW CREATE TABLE zh");
    let ddl = rows_to_strings(rs)[0][1].clone();
    assert!(
        ddl.contains("FULLTEXT KEY `ft` (`body`) WITH PARSER `ngram`"),
        "missing ngram FULLTEXT key, got:\n{ddl}"
    );
}

#[test]
fn standalone_create_fulltext_index_default_parser_omits_with_parser() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)")
        .unwrap();
    engine
        .execute("CREATE FULLTEXT INDEX ft ON docs (body)")
        .unwrap();
    engine
        .execute("INSERT INTO docs VALUES (1, 'database systems')")
        .unwrap();
    engine
        .execute("INSERT INTO docs VALUES (2, 'distributed consensus')")
        .unwrap();
    let rs = exec_set(
        &engine,
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('database')",
    );
    assert_eq!(first_col(rs), vec!["1".to_string()]);
    // A default-parser FULLTEXT index must not gain a spurious WITH PARSER.
    let rs = exec_set(&engine, "SHOW CREATE TABLE docs");
    let ddl = rows_to_strings(rs)[0][1].clone();
    assert!(
        ddl.contains("FULLTEXT KEY `ft` (`body`)"),
        "missing FULLTEXT key, got:\n{ddl}"
    );
    assert!(
        !ddl.contains("WITH PARSER"),
        "default-parser key should not emit WITH PARSER, got:\n{ddl}"
    );
}

#[test]
fn standalone_create_fulltext_index_ngram_survives_restart() {
    // The `WITH PARSER ngram` choice picked via standalone CREATE FULLTEXT
    // INDEX must persist through the data dictionary, just like the inline
    // path (see `ngram_parser_survives_restart`).
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE zh (id INT PRIMARY KEY, body TEXT)")
            .unwrap();
        engine
            .execute("CREATE FULLTEXT INDEX ft ON zh (body) WITH PARSER ngram")
            .unwrap();
        engine
            .execute("INSERT INTO zh VALUES (1, '数据库全文检索')")
            .unwrap();
        engine
            .execute("INSERT INTO zh VALUES (2, '分布式系统')")
            .unwrap();
    }
    // Reopen: catalog is rebuilt from the dictionary heaps.
    let engine = open_strict_test_engine(dir.path());
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id FROM zh WHERE MATCH(body) AGAINST('全文')")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(first_col(rs), vec!["1".to_string()]);
}

#[test]
fn ngram_parser_survives_restart() {
    // The `WITH PARSER ngram` choice must persist through the data
    // dictionary, otherwise a CJK index silently reverts to the default
    // tokenizer on reopen and stops matching.
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute(
                "CREATE TABLE zh (id INT PRIMARY KEY, body TEXT, \
                 FULLTEXT KEY ft (body) WITH PARSER ngram)",
            )
            .unwrap();
        engine
            .execute("INSERT INTO zh VALUES (1, '数据库全文检索')")
            .unwrap();
        engine
            .execute("INSERT INTO zh VALUES (2, '分布式系统')")
            .unwrap();
    }
    // Reopen: catalog is rebuilt from the dictionary heaps.
    let engine = open_strict_test_engine(dir.path());
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id FROM zh WHERE MATCH(body) AGAINST('全文')")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(first_col(rs), vec!["1".to_string()]);
}

#[test]
fn boolean_must_and_must_not() {
    let dir = tempfile::tempdir().unwrap();
    let engine = docs_engine(dir.path());
    // Row 1 'database systems database tuning' has database, not design.
    // Row 2 'database design' has both → excluded by -design.
    let rs = exec_set(
        &engine,
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('+database -design' IN BOOLEAN MODE)",
    );
    assert_eq!(first_col(rs), vec!["1".to_string()]);
}

#[test]
fn boolean_phrase_requires_adjacency() {
    let dir = tempfile::tempdir().unwrap();
    let engine = docs_engine(dir.path());
    // Only row 2 has the tokens "database" and "design" adjacent and in
    // order; row 1 has both tokens but not adjacent.
    let rs = exec_set(
        &engine,
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('\"database design\"' IN BOOLEAN MODE)",
    );
    assert_eq!(first_col(rs), vec!["2".to_string()]);
}

#[test]
fn boolean_prefix_wildcard() {
    let dir = tempfile::tempdir().unwrap();
    let engine = docs_engine(dir.path());
    // "data*" matches the indexed token "database" (rows 1 and 2).
    let rs = exec_set(
        &engine,
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('data*' IN BOOLEAN MODE) ORDER BY id",
    );
    assert_eq!(first_col(rs), vec!["1".to_string(), "2".to_string()]);
}

#[test]
fn boolean_must_not_alone_is_empty() {
    let dir = tempfile::tempdir().unwrap();
    let engine = docs_engine(dir.path());
    // A query with only a negative term and no positives matches nothing
    // (MySQL semantics): no Must/Should set to subtract from.
    let rs = exec_set(
        &engine,
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('-database' IN BOOLEAN MODE)",
    );
    assert!(first_col(rs).is_empty());
}

#[test]
fn boolean_ngram_chinese() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE zh (id INT PRIMARY KEY, body TEXT, \
             FULLTEXT KEY ft (body) WITH PARSER ngram)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO zh VALUES (1, '数据库全文检索')")
        .unwrap();
    engine
        .execute("INSERT INTO zh VALUES (2, '全文索引很快')")
        .unwrap();
    engine
        .execute("INSERT INTO zh VALUES (3, '分布式系统')")
        .unwrap();
    // The required positive full-text ngram appears in rows 1 and 2.
    let rs = exec_set(
        &engine,
        "SELECT id FROM zh WHERE MATCH(body) AGAINST('+全文' IN BOOLEAN MODE) ORDER BY id",
    );
    assert_eq!(first_col(rs), vec!["1".to_string(), "2".to_string()]);
    // Require the full-text ngram and exclude the index ngram from row 2; only row 1 matches.
    let rs = exec_set(
        &engine,
        "SELECT id FROM zh WHERE MATCH(body) AGAINST('+全文 -索引' IN BOOLEAN MODE)",
    );
    assert_eq!(first_col(rs), vec!["1".to_string()]);
}

#[test]
fn phrase_does_not_match_across_columns() {
    // Posting positions carry an inter-column gap, so a phrase cannot match
    // tokens that straddle two indexed columns. Row 1 splits "alpha"/"beta"
    // across (title, body); row 2 has them adjacent within `title`.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, title TEXT, body TEXT, \
             FULLTEXT KEY ft (title, body))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 'alpha', 'beta gamma')")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (2, 'alpha beta', 'gamma')")
        .unwrap();
    // Phrase "alpha beta": only row 2 has them adjacent in one column.
    let rs = exec_set(
        &engine,
        "SELECT id FROM t WHERE MATCH(title, body) AGAINST('\"alpha beta\"' IN BOOLEAN MODE) \
         ORDER BY id",
    );
    assert_eq!(first_col(rs), vec!["2".to_string()]);
    // Both terms present (no adjacency) → both rows, across columns.
    let rs = exec_set(
        &engine,
        "SELECT id FROM t WHERE MATCH(title, body) AGAINST('+alpha +beta' IN BOOLEAN MODE) \
         ORDER BY id",
    );
    assert_eq!(first_col(rs), vec!["1".to_string(), "2".to_string()]);
}

#[test]
fn phrase_with_repeated_tokens_uses_positions() {
    // Repeated tokens exercise the persisted-position phrase match: "bar foo"
    // is adjacent in both rows; "baz foo" is adjacent in neither.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE p (id INT PRIMARY KEY, body TEXT, FULLTEXT KEY ft (body))")
        .unwrap();
    engine
        .execute("INSERT INTO p VALUES (1, 'foo bar foo bar baz')")
        .unwrap();
    engine
        .execute("INSERT INTO p VALUES (2, 'baz bar foo')")
        .unwrap();
    let rs = exec_set(
        &engine,
        "SELECT id FROM p WHERE MATCH(body) AGAINST('\"bar foo\"' IN BOOLEAN MODE) ORDER BY id",
    );
    assert_eq!(first_col(rs), vec!["1".to_string(), "2".to_string()]);
    let rs = exec_set(
        &engine,
        "SELECT id FROM p WHERE MATCH(body) AGAINST('\"baz foo\"' IN BOOLEAN MODE)",
    );
    assert!(first_col(rs).is_empty());
}

/// Strict-durability engine so index backfill exercises the on-disk
/// (production) build path rather than the Memory-mode rebuild.
fn open_strict_ft_engine(dir: &std::path::Path) -> StorageQueryEngine {
    StorageQueryEngine::open_with_storage_config(
        dir,
        StorageConfig {
            durability: crate::storage::DurabilityMode::Strict,
            checkpoint_interval: std::time::Duration::from_millis(60_000),
            checkpoint_wal_bytes: 64 * 1024 * 1024,
            lock_wait: std::time::Duration::from_millis(1_000),
            group_commit_window: std::time::Duration::from_micros(0),
            wal_segment_bytes: 64 * 1024 * 1024,
            buffer_pool_pages: 512,
            buffer_pool_io_threads: 1,
            buffer_pool_prefetch_pages: 0,
            undo_retention: std::time::Duration::from_millis(60_000),
            heap_update_reserve_bytes: 2048,
            startup_validation: crate::storage::StartupValidationMode::Off,
            startup_validate_indexes: false,
        },
    )
    .unwrap()
}

/// Regression for S6 (alter-table-review-2026-07): `ALTER TABLE … ADD
/// FULLTEXT` used to be rejected as Unsupported — the parser dropped
/// `FulltextOrSpatial` to the catch-all, and `apply_fulltext_parsers`' ALTER
/// branch was dead code. It must now build the inverted index from the
/// existing rows and answer MATCH queries. (Strict durability: the Memory
/// backend has a separate, pre-existing fulltext-rebuild gap.)
#[test]
fn alter_table_add_fulltext_builds_and_matches() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_ft_engine(dir.path());
    engine
        .execute("CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)")
        .unwrap();
    engine
        .execute("INSERT INTO docs VALUES (1, 'database systems database tuning')")
        .unwrap();
    engine
        .execute("INSERT INTO docs VALUES (2, 'database design')")
        .unwrap();
    engine
        .execute("INSERT INTO docs VALUES (3, 'distributed consensus protocols')")
        .unwrap();

    // The formerly-unsupported ALTER path.
    engine
        .execute("ALTER TABLE docs ADD FULLTEXT ft (body)")
        .unwrap();

    // SHOW CREATE must round-trip the FULLTEXT key.
    let create = rows_to_strings(exec_set(&engine, "SHOW CREATE TABLE docs"));
    assert!(
        create[0][1].to_uppercase().contains("FULLTEXT"),
        "SHOW CREATE must include the FULLTEXT key: {}",
        create[0][1]
    );

    // The index must have been built from the pre-existing rows.
    let rs = exec_set(
        &engine,
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('database') ORDER BY id",
    );
    assert_eq!(first_col(rs), vec!["1".to_string(), "2".to_string()]);
}

/// Regression for S6: `ALTER TABLE … ADD SPATIAL` must build the R-tree over
/// existing geometry rows (also previously rejected as Unsupported).
#[test]
fn alter_table_add_spatial_builds_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_ft_engine(dir.path());
    engine
        .execute("CREATE TABLE g (id INT PRIMARY KEY, geom GEOMETRY NOT NULL)")
        .unwrap();
    engine
        .execute("INSERT INTO g VALUES (1, ST_GeomFromText('POINT(1 1)'))")
        .unwrap();
    engine
        .execute("INSERT INTO g VALUES (2, ST_GeomFromText('POINT(9 9)'))")
        .unwrap();
    // Formerly Unsupported; must now succeed and round-trip.
    engine
        .execute("ALTER TABLE g ADD SPATIAL INDEX sp (geom)")
        .unwrap();
    let create = rows_to_strings(exec_set(&engine, "SHOW CREATE TABLE g"));
    assert!(
        create[0][1].to_uppercase().contains("SPATIAL"),
        "SHOW CREATE must include the SPATIAL key: {}",
        create[0][1]
    );
    // A window query using the spatial index returns the point inside it.
    let rs = exec_set(
        &engine,
        "SELECT id FROM g WHERE MBRContains(ST_GeomFromText('POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))'), geom)",
    );
    assert_eq!(first_col(rs), vec!["1".to_string()]);
}

// ---------------------------------------------------------------------------
// P2 regression: gaps found by the 2026-07 review.
// ---------------------------------------------------------------------------

/// End-to-end DELETE maintenance: after a row is removed, MATCH must stop
/// returning it. Storage-layer coverage exists
/// (`fulltext_insert_delete_maintains_inverted_index`) but the SQL surface
/// had no test — a regression that only broke transaction/visibility glue
/// could slip through.
#[test]
fn delete_removes_row_from_match() {
    let dir = tempfile::tempdir().unwrap();
    let engine = docs_engine(dir.path());
    // Baseline: rows 1 and 2 match "database".
    let rs = exec_set(
        &engine,
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('database') ORDER BY id",
    );
    assert_eq!(first_col(rs), vec!["1".to_string(), "2".to_string()]);

    engine.execute("DELETE FROM docs WHERE id = 1").unwrap();

    let rs = exec_set(
        &engine,
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('database') ORDER BY id",
    );
    assert_eq!(first_col(rs), vec!["2".to_string()]);

    // Deleting the last remaining match must make the query return empty.
    engine.execute("DELETE FROM docs WHERE id = 2").unwrap();
    let rs = exec_set(
        &engine,
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('database')",
    );
    assert_eq!(first_col(rs), Vec::<String>::new());
}

/// Stopwords are transparently filtered on the build side; on the query
/// side the same tokenizer must strip them too — otherwise `MATCH('the')`
/// would try to look up an absent token and return empty as if the term
/// were rare, misleading users. Verifies query-side stopword filtering
/// yields "no clauses → no rows" (MySQL semantics for stopword-only
/// queries in natural language mode).
#[test]
fn natural_language_stopwords_are_filtered_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let engine = docs_engine(dir.path());
    // "the" is in the default MySQL stopword list — a query of only
    // stopwords should return zero rows.
    let rs = exec_set(
        &engine,
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('the')",
    );
    assert_eq!(first_col(rs), Vec::<String>::new());
    // Mixed query: "the database" should behave as if "database" alone.
    let rs = exec_set(
        &engine,
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('the database') ORDER BY id",
    );
    assert_eq!(first_col(rs), vec!["1".to_string(), "2".to_string()]);
}

/// Boolean-mode unsupported operators must fail loudly (1235), not
/// silently discard. The exhaustive parser-level check lives in
/// `boolean::tests::unsupported_operators_error_1235`; this one covers
/// the end-to-end SQL path.
#[test]
fn boolean_unsupported_operators_error_1235() {
    let dir = tempfile::tempdir().unwrap();
    let engine = docs_engine(dir.path());
    let queries = [
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('(database)' IN BOOLEAN MODE)",
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('>heavy' IN BOOLEAN MODE)",
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('<light' IN BOOLEAN MODE)",
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('~excl' IN BOOLEAN MODE)",
    ];
    for q in queries {
        let err = engine
            .execute(q)
            .err()
            .unwrap_or_else(|| panic!("expected 1235 for `{q}`"));
        assert_eq!(err.code(), 1235, "query `{q}`: {}", err.message());
        assert!(
            err.message().contains("is not supported"),
            "query `{q}`: {}",
            err.message()
        );
    }
}

/// P1-B: prefix `word*` hits must participate in BM25. Previously
/// `score` was 0 for any row matched only through a prefix expansion,
/// because prefix tokens were never added to `positive_tokens`. Now
/// the concrete tokens the wildcard expands to feed BM25 like a
/// regular Term, so a matched row scores strictly greater than 0.
#[test]
fn boolean_prefix_wildcard_scores_matched_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = docs_engine(dir.path());
    let rs = exec_set(
        &engine,
        "SELECT id, MATCH(body) AGAINST('data*' IN BOOLEAN MODE) AS s \
         FROM docs \
         WHERE MATCH(body) AGAINST('data*' IN BOOLEAN MODE) \
         ORDER BY id",
    );
    let rows = rows_to_strings(rs);
    assert_eq!(rows.len(), 2, "expected 2 rows, got {rows:?}");
    for row in &rows {
        let score: f64 = row[1].parse().unwrap_or_else(|_| {
            panic!("score column not numeric in {row:?}");
        });
        assert!(
            score > 0.0,
            "prefix-only match must score > 0, row {row:?} scored {score}"
        );
    }
}

/// P0-A: the tokenizer config in effect at CREATE time must be captured
/// in the index, so that changing MYDB_FT_* between restarts can never
/// silently break searches over historical postings. We can't easily
/// change env inside a test, but we can verify queries still work after
/// a close/reopen — the load path uses the persisted meta.
#[test]
fn fulltext_meta_snapshot_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_strict_ft_engine(dir.path());
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY, body TEXT, FULLTEXT KEY ft (body))")
            .unwrap();
        engine
            .execute("INSERT INTO t VALUES (1, 'database systems tuning')")
            .unwrap();
    }
    // Reopen and query — the index meta comes back from disk; the query
    // tokenizer is built from that meta and must reproduce the same tokens.
    let engine = open_strict_ft_engine(dir.path());
    let rs = exec_set(
        &engine,
        "SELECT id FROM t WHERE MATCH(body) AGAINST('tuning')",
    );
    assert_eq!(first_col(rs), vec!["1".to_string()]);
}

/// Multi-column FULLTEXT natural-language: matched-set must be
/// independent of source column, and both columns' contributions
/// factor into scoring. Complements `phrase_does_not_match_across_columns`
/// which only exercises phrase-cross-column negatives.
#[test]
fn natural_language_multicolumn_ranks_hits_in_both() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE d (\
             id INT PRIMARY KEY, title TEXT, body TEXT, \
             FULLTEXT KEY ft (title, body))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO d VALUES (1, 'database concepts', 'introduction to storage')")
        .unwrap();
    // Row 2 has the query token in body, not title — still must match.
    engine
        .execute("INSERT INTO d VALUES (2, 'introduction', 'a database internals guide')")
        .unwrap();
    engine
        .execute("INSERT INTO d VALUES (3, 'graph theory', 'consensus protocols')")
        .unwrap();
    let rs = exec_set(
        &engine,
        "SELECT id FROM d WHERE MATCH(title, body) AGAINST('database') ORDER BY id",
    );
    assert_eq!(first_col(rs), vec!["1".to_string(), "2".to_string()]);
}

#[test]
fn fulltext_audit_ngram_boundaries_and_zero_score() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE ng(id INT PRIMARY KEY, body TEXT, FULLTEXT KEY ft(body) WITH PARSER ngram)").unwrap();
    engine.execute("INSERT INTO ng VALUES(1,'数据库'),(2,'数据 据库'),(3,'数据，据库'),(4,'无关文本'),(5,'数据库数据库'),(6,'数据 无关 据库'),(7,'据库 数据')").unwrap();
    for term in ["+数据库", "数据库", "\"数据库\""] {
        let rows = exec_set(
            &engine,
            &format!(
                "SELECT id FROM ng WHERE MATCH(body) AGAINST('{term}' IN BOOLEAN MODE) ORDER BY id"
            ),
        );
        assert_eq!(first_col(rows), vec!["1", "2", "3", "5"], "{term}");
    }
    let rows = exec_set(
        &engine,
        "SELECT id FROM ng WHERE MATCH(body) AGAINST('+数据 -数据库' IN BOOLEAN MODE) ORDER BY id",
    );
    assert_eq!(first_col(rows), vec!["6", "7"]);
    engine
        .execute("INSERT INTO ng VALUES(8,'哈哈'),(9,'哈哈哈')")
        .unwrap();
    assert_eq!(
        first_col(exec_set(
            &engine,
            "SELECT id FROM ng WHERE MATCH(body) AGAINST('+哈哈哈' IN BOOLEAN MODE)"
        )),
        vec!["9"]
    );
    let rows = exec_set(
        &engine,
        "SELECT MATCH(body) AGAINST('数据库') FROM ng WHERE id=4",
    );
    assert_eq!(rows.columns[0].column_type, ColumnType::Double);
    match rows.rows[0][0] {
        Value::Double(score) => assert_eq!(score.to_bits(), 0),
        ref other => panic!("{other:?}"),
    }
}

#[test]
fn fulltext_audit_ddl_rejects_ignored_options() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE docs(id INT PRIMARY KEY, body TEXT)")
        .unwrap();
    for option in ["(body) WITH PARSER bogus"] {
        for sql in [
            format!("CREATE TABLE bad(body TEXT,FULLTEXT KEY ft{option})"),
            format!("CREATE FULLTEXT INDEX ft ON docs{option}"),
            format!("ALTER TABLE docs ADD FULLTEXT KEY ft{option}"),
        ] {
            let err = engine.execute(&sql).unwrap_err();
            assert!(err.message().contains("FULLTEXT"), "{sql}: {err:?}");
        }
    }
    engine
        .execute("CREATE FULLTEXT INDEX ft ON docs(body) WITH PARSER NGRAM")
        .unwrap();
}
