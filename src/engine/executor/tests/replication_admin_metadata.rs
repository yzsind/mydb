use super::*;

#[test]
fn prepared_replication_show_metadata_matches_execution_without_side_effects() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "SHOW REPLICA STATUS",
        "SHOW SLAVE STATUS",
        "SHOW REPLICA STATUS FOR CHANNEL 'channel'",
        "SHOW RELAYLOG EVENTS LIMIT 0",
    ] {
        session.warnings.clear();
        let described = engine
            .describe_prepared_statement(&mut session, sql)
            .unwrap();
        assert!(
            session.warnings.is_empty(),
            "PREPARE must not execute legacy warning emission"
        );
        let QueryResponse::ResultSet(result) =
            engine.execute_with_session(&mut session, sql).unwrap()
        else {
            panic!("result set");
        };
        assert_eq!(described, result.columns, "{sql}");
    }
    for sql in [
        "START REPLICA",
        "STOP REPLICA",
        "RESET REPLICA ALL",
        "CHANGE REPLICATION SOURCE TO SOURCE_HOST='localhost'",
    ] {
        assert!(
            engine
                .describe_prepared_statement(&mut session, sql)
                .unwrap()
                .is_empty(),
            "{sql}"
        );
    }
}
