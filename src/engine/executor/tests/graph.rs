use super::*;

fn query(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> QueryResponse {
    engine
        .execute_with_session(session, sql)
        .unwrap_or_else(|err| panic!("{sql}: {err}"))
}

fn rows(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = query(engine, session, sql) else {
        panic!("expected rows from {sql}");
    };
    result.rows
}

fn open_graph(engine: &StorageQueryEngine, session: &mut SessionContext) {
    query(engine, session, "CREATE GRAPH kg ANY PROPERTY GRAPH");
    query(engine, session, "SESSION SET GRAPH kg");
}

#[test]
fn property_graph_paths_match_small_multigraph_oracle() {
    let tmp = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_strict_test_engine(tmp.path());
    let session = &mut SessionContext::default();
    open_graph(&engine, session);
    query(&engine, session, "INSERT (a:N {k: 0}), (b:N {k: 1}), (c:N {k: 2}), (a)-[:E]->(a), (a)-[:E]->(b), (a)-[:E]->(b), (b)-[:E]->(a), (b)-[:E]->(c), (a)~[:U]~(c)");

    let edges = [(0, 0), (0, 1), (0, 1), (1, 0), (1, 2)];
    for mode in ["WALK", "TRAIL", "ACYCLIC"] {
        let mut expected = Vec::new();
        let mut frontier = vec![(0, vec![0], Vec::<usize>::new())];
        for hops in 0..=3 {
            let mut next = Vec::new();
            for (node, path, used) in frontier {
                expected.push(vec![Value::Int(node), Value::Int(hops)]);
                for (index, (from, to)) in edges.iter().copied().enumerate() {
                    if from != node
                        || mode == "TRAIL" && used.contains(&index)
                        || mode == "ACYCLIC" && path.contains(&to)
                    {
                        continue;
                    }
                    let mut used = used.clone();
                    let mut path = path.clone();
                    used.push(index);
                    path.push(to);
                    next.push((to, path, used));
                }
            }
            frontier = next;
        }
        expected.sort_by_key(|r| (r[1].to_display_string(), r[0].to_display_string()));
        let sql = format!("MATCH p = {mode} (a:N {{k: 0}})-[:E]->{{0,3}}(b) RETURN CAST(b.k AS INT64) AS k, PATH_LENGTH(p) AS h ORDER BY h, k");
        assert_eq!(rows(&engine, session, &sql), expected);
    }
    assert_eq!(
        rows(
            &engine,
            session,
            "MATCH p = ANY SHORTEST (a:N {k: 0})-[:E]->{1,3}(b:N {k: 1}) RETURN PATH_LENGTH(p)"
        ),
        vec![vec![Value::Int(1)]]
    );
    assert!(rows(&engine, session, "MATCH p = ANY SHORTEST (a:N {k: 0})-[:E]->{1,3}(b:N {k: 1}) WHERE PATH_LENGTH(p) > 1 RETURN PATH_LENGTH(p)").is_empty());
    assert_eq!(
        rows(
            &engine,
            session,
            "MATCH (:N {k: 2})~[:U]~(b) RETURN CAST(b.k AS INT64)"
        ),
        vec![vec![Value::Int(0)]]
    );
    assert_eq!(
        rows(
            &engine,
            session,
            "MATCH (:N {k: 2})<-[:E]-(b) RETURN CAST(b.k AS INT64)"
        ),
        vec![vec![Value::Int(1)]]
    );
    assert_eq!(
        rows(
            &engine,
            session,
            "MATCH (a:N {k: 2}) OPTIONAL MATCH (a)-[:E]->(b) RETURN b"
        ),
        vec![vec![Value::Null]]
    );
    assert!(engine
        .execute_with_session(session, "MATCH ()-[]->{1,17}() RETURN 1")
        .is_err());
}

#[test]
fn property_graph_indexes_follow_updates_and_statement_rollback() {
    let tmp = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_strict_test_engine(tmp.path());
    let session = &mut SessionContext::default();
    open_graph(&engine, session);
    query(
        &engine,
        session,
        "INSERT (:Entity {k: 'a'}), (:Entity {k: 'b'})",
    );
    query(
        &engine,
        session,
        "CREATE UNIQUE GRAPH INDEX keys ON kg NODE LABEL Entity (k STRING)",
    );
    assert!(rows(
        &engine,
        session,
        "MATCH (n:Entity {k: 'a'}) WHERE n.k = 'b' RETURN n"
    )
    .is_empty());
    query(&engine, session, "BEGIN");
    assert!(engine
        .execute_with_session(session, "INSERT (:Entity {k: 'c'}), (:Entity {k: 'a'})")
        .is_err());
    assert!(rows(&engine, session, "MATCH (n:Entity {k: 'c'}) RETURN n").is_empty());
    query(&engine, session, "SAVEPOINT s");
    query(
        &engine,
        session,
        "MATCH (n:Entity {k: 'a'}) SET n.k = 'z', n:Temp",
    );
    assert_eq!(
        rows(
            &engine,
            session,
            "MATCH (n:Entity & Temp {k: 'z'}) RETURN CAST(n.k AS STRING)"
        )
        .len(),
        1
    );
    query(&engine, session, "ROLLBACK TO s");
    assert_eq!(
        rows(&engine, session, "MATCH (n:Entity {k: 'a'}) RETURN n").len(),
        1
    );
    assert!(rows(&engine, session, "MATCH (n:Entity {k: 'z'}) RETURN n").is_empty());
    query(&engine, session, "COMMIT");
    query(
        &engine,
        session,
        "MATCH (n:Entity {k: 'b'}) REMOVE n:Entity, n.k",
    );
    query(&engine, session, "INSERT (:Entity {k: 'b'})");
    assert_eq!(
        rows(&engine, session, "MATCH (n:Entity {k: 'b'}) RETURN n").len(),
        1
    );
    query(&engine, session, "DROP GRAPH INDEX keys ON kg");
    query(&engine, session, "INSERT (:Entity {k: 'b'})");
    assert!(engine
        .execute_with_session(
            session,
            "CREATE UNIQUE GRAPH INDEX keys ON kg NODE LABEL Entity (k STRING)"
        )
        .is_err());
    assert_eq!(
        rows(&engine, session, "MATCH (n:Entity {k: 'b'}) RETURN n").len(),
        2
    );
    query(
        &engine,
        session,
        "CREATE GRAPH INDEX keys ON kg NODE LABEL Entity (k STRING)",
    );
}

#[test]
fn property_graph_shares_transactions_and_durable_storage() {
    let tmp = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    {
        let engine = open_strict_test_engine(tmp.path());
        let first = &mut SessionContext::default();
        let second = &mut SessionContext::default();
        second.connection_id = 2;
        open_graph(&engine, first);
        query(&engine, second, "SESSION SET GRAPH kg");
        query(
            &engine,
            first,
            "CREATE TABLE documents (id INT PRIMARY KEY)",
        );
        query(
            &engine,
            first,
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        );
        query(&engine, first, "START TRANSACTION READ ONLY");
        assert!(rows(&engine, first, "SELECT id FROM documents").is_empty());
        query(&engine, second, "BEGIN");
        query(&engine, second, "INSERT INTO documents VALUES (5)");
        query(&engine, second, "INSERT (:Doc {key: 5})");
        query(&engine, second, "COMMIT");
        assert!(rows(&engine, first, "MATCH (n:Doc) RETURN n").is_empty());
        query(&engine, first, "COMMIT");
        assert_eq!(
            rows(&engine, first, "MATCH (n:Doc) RETURN CAST(n.key AS INT64)"),
            vec![vec![Value::Int(5)]]
        );
        query(&engine, first, "BEGIN");
        query(&engine, first, "INSERT INTO documents VALUES (6)");
        query(&engine, first, "INSERT (:Doc {key: 6})");
        assert!(engine.execute_with_session(first, "DROP GRAPH kg").is_err());
        query(&engine, first, "ROLLBACK");
        for sql in [
            "SELECT * FROM __mydb_graph_kg",
            "DROP TABLE __mydb_graph_kg",
            "CREATE TABLE __mydb_graph_spoof (id INT)",
            "DROP DATABASE test",
        ] {
            assert!(engine.execute_with_session(first, sql).is_err(), "{sql}");
        }
        assert!(!rows(&engine, first, "SHOW TABLES")
            .iter()
            .any(|row| row[0].to_display_string().contains("__mydb_graph_")));
    }
    let engine = open_strict_test_engine(tmp.path());
    let first = &mut SessionContext::default();
    query(&engine, first, "SESSION SET GRAPH kg");
    assert_eq!(
        rows(&engine, first, "MATCH (n:Doc) RETURN CAST(n.key AS INT64)"),
        vec![vec![Value::Int(5)]]
    );
    assert_eq!(
        rows(&engine, first, "SELECT id FROM documents"),
        vec![vec![Value::Int(5)]]
    );
    query(&engine, first, "DROP GRAPH kg");
    query(&engine, first, "CREATE GRAPH kg ANY PROPERTY GRAPH");
    assert!(engine
        .execute_with_session(first, "MATCH (n) RETURN n")
        .is_err());
}

#[test]
fn property_graph_numbered_parameters_and_detach_delete() {
    let tmp = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_strict_test_engine(tmp.path());
    let session = &mut SessionContext::default();
    open_graph(&engine, session);
    query(
        &engine,
        session,
        "INSERT (a:N {k: 'a'})-[:E]->(b:N {k: 'b'})-[:E]->(c:N {k: 'c'})",
    );
    let sourced =
        "FOR key IN [?, ?] MATCH (n:N {k: key}) RETURN CAST(n.k AS STRING) AS key ORDER BY key";
    let response = crate::engine::params::with_params(
        vec![Value::String("c".into()), Value::String("a".into())],
        || {
            engine
                .execute_prepared_statement(session, sourced, parse_statement(sourced).unwrap())
                .unwrap()
                .unwrap()
        },
    );
    let QueryResponse::ResultSet(result) = response else {
        panic!("expected prepared rows");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::String("a".into())],
            vec![Value::String("c".into())]
        ]
    );
    assert!(engine
        .execute_with_session(session, "MATCH (b:N {k: 'b'}) DELETE b")
        .is_err());
    query(&engine, session, "MATCH (b:N {k: 'b'}) DETACH DELETE b");
    assert!(rows(&engine, session, "MATCH ()-[:E]->() RETURN 1").is_empty());
    assert_eq!(rows(&engine, session, "MATCH (n:N) RETURN n").len(), 2);
}

#[test]
fn property_graph_failed_catalog_publication_leaves_no_partial_graph() {
    let tmp = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    {
        let engine = open_strict_test_engine(tmp.path());
        engine.database().fail_next_catalog_commit_for_test();
        let result = engine.execute("CREATE GRAPH kg ANY PROPERTY GRAPH");
        assert!(result.is_err());
    }
    let engine = open_strict_test_engine(tmp.path());
    let session = &mut SessionContext::default();
    assert!(engine
        .execute_with_session(session, "SESSION SET GRAPH kg")
        .is_err());
    open_graph(&engine, session);
    assert_eq!(
        rows(&engine, session, "MATCH (n) RETURN n"),
        Vec::<Vec<Value>>::new()
    );
}

#[test]
fn property_graph_handles_numeric_equality_without_integer_rounding() {
    let tmp = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_strict_test_engine(tmp.path());
    let session = &mut SessionContext::default();
    open_graph(&engine, session);
    query(
        &engine,
        session,
        "INSERT (:N {k: 9007199254740992}), (:N {k: 9007199254740993})",
    );
    query(
        &engine,
        session,
        "CREATE UNIQUE GRAPH INDEX key_lookup ON kg NODE LABEL N (k INT64)",
    );
    assert_eq!(
        rows(
            &engine,
            session,
            "MATCH (n:N) WHERE n.k = 9007199254740992.0 RETURN CAST(n.k AS INT64)"
        ),
        vec![vec![Value::Int(9007199254740992)]],
    );
    assert_eq!(
        rows(&engine, session, "FOR x IN [1, 1.0, 2] RETURN DISTINCT x").len(),
        2
    );
    assert_eq!(
        rows(
            &engine,
            session,
            "FOR x IN [1, NULL, 2] RETURN CAST(x AS INT64) AS k ORDER BY k"
        ),
        vec![vec![Value::Int(1)], vec![Value::Int(2)], vec![Value::Null]],
    );
    assert_eq!(
        rows(
            &engine,
            session,
            "FOR x IN [1, NULL, 2] RETURN CAST(x AS INT64) AS k ORDER BY k DESC"
        ),
        vec![vec![Value::Null], vec![Value::Int(2)], vec![Value::Int(1)]],
    );
}

#[test]
fn property_graph_batched_walk_keeps_multiplicity_and_snapshot_payloads() {
    let tmp = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_strict_test_engine(tmp.path());
    let reader = &mut SessionContext::default();
    let writer = &mut SessionContext::default();
    writer.connection_id = 2;
    open_graph(&engine, reader);
    query(&engine, writer, "SESSION SET GRAPH kg");
    query(&engine, writer, "INSERT (:Seed {key: 0, name: 'start'})");
    let edges = ", (a)-[:E]->(b)".repeat(24);
    let loops = ", (b)-[:E]->(b)".repeat(8);
    for key in 1..=24 {
        query(
            &engine,
            writer,
            &format!(
                "MATCH (a:Seed) INSERT (b:Leaf {{key: {key}, name: 'node{key}'}}){edges}{loops}"
            ),
        );
    }
    let compact = "MATCH (a:Seed)-[:E]->{0,2}(b)                    RETURN CAST(b.key AS INT64) AS k, CAST(b.name AS STRING) ORDER BY k";
    let named = "MATCH p = WALK (a:Seed)-[:E]->{0,2}(b)                  RETURN CAST(b.key AS INT64) AS k, CAST(b.name AS STRING) ORDER BY k";
    query(
        &engine,
        reader,
        "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
    );
    let scalar = "MATCH (a:Seed)-[:E]->{0,2}(b) RETURN CAST(b.name AS STRING) AS n ORDER BY n";
    let old_names = rows(&engine, reader, scalar);
    assert_eq!(old_names.len(), 5185);
    assert_eq!(rows(&engine, reader, scalar), old_names);
    query(&engine, reader, "START TRANSACTION READ ONLY");
    let expected = rows(&engine, reader, named);
    assert_eq!(expected.len(), 5185);
    assert_eq!(rows(&engine, reader, compact), expected);

    query(
        &engine,
        writer,
        "MATCH (n:Leaf {key: 1}) SET n.name = 'new'",
    );
    query(&engine, writer, "MATCH (n:Leaf {key: 2}) DETACH DELETE n");
    assert_eq!(rows(&engine, reader, compact), expected);
    assert_eq!(rows(&engine, reader, scalar), old_names);
    query(&engine, reader, "COMMIT");
    let changed = rows(&engine, reader, compact);
    assert_eq!(changed.len(), 4969);
    let new_names = rows(&engine, reader, scalar);
    assert_eq!(new_names.len(), 4969);
    assert!(new_names.contains(&vec![Value::String("new".into())]));
    assert!(!new_names.contains(&vec![Value::String("node2".into())]));

    assert_eq!(changed[1], vec![Value::Int(1), Value::String("new".into())]);
    assert_eq!(changed[1], changed[2]);
    query(
        &engine,
        writer,
        "MATCH (a:Seed) INSERT (b:Leaf {key: 25, name: 'fresh'}), (a)-[:E]->(b)",
    );
    let expanded = rows(&engine, reader, compact);
    assert_eq!(expanded.len(), 4970);
    assert_eq!(
        expanded.last(),
        Some(&vec![Value::Int(25), Value::String("fresh".into())])
    );
}
