use super::*;

#[test]
fn prepared_binlog_admin_metadata_matches_each_response_kind() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for (sql, names) in [
        (
            "SHOW MASTER STATUS",
            vec![
                "File",
                "Position",
                "Binlog_Do_DB",
                "Binlog_Ignore_DB",
                "Executed_Gtid_Set",
            ],
        ),
        (
            "SHOW BINARY LOGS",
            vec!["Log_name", "File_size", "Encrypted"],
        ),
        (
            "SHOW REPLICAS",
            vec!["Server_id", "Host", "Port", "Source_id", "Replica_UUID"],
        ),
        (
            "SHOW SLAVE HOSTS",
            vec!["Server_id", "Host", "Port", "Source_id", "Replica_UUID"],
        ),
        (
            "SHOW BINLOG EVENTS LIMIT 0",
            vec![
                "Log_name",
                "Pos",
                "Event_type",
                "Server_id",
                "End_log_pos",
                "Info",
            ],
        ),
        ("SHOW PROFILES", vec!["Query_ID", "Duration", "Query"]),
        ("SHOW PROFILE", vec!["Status", "Duration"]),
        ("FLUSH BINARY LOGS", vec![]),
        ("RESET MASTER", vec![]),
        ("PURGE BINARY LOGS TO 'mydb-bin.000001'", vec![]),
        ("FLUSH TABLES WITH READ LOCK", vec![]),
        ("UNLOCK TABLES", vec![]),
    ] {
        let columns = engine
            .describe_prepared_statement(&mut session, sql)
            .unwrap();
        assert_eq!(
            columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            names,
            "{sql}"
        );
    }
}
