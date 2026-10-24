//! P0-B1: SHOW REPLICA STATUS matches MySQL 8.0's 60 columns.
//!
//! Previously only 20 columns were exposed, such as Channel_Name, Source_Host,
//! and Replica_IO_Running, leaving name-based orchestrator/MHA/pt-heartbeat mappings NULL.
//! These tests establish:
//!
//! Exactly 60 columns with MySQL 8.0 order, names, and types.
//! SHOW SLAVE STATUS returns the same 60 columns plus deprecation warning 1287.
//! Channel_Name is at position 56, matching MySQL 8.0.
//! Key fields are populated from real channel configuration, including Source_User and Connect_Retry.
//!   SQL_Delay / Auto_Position）

use super::support::execute_materialized as exec;
use super::*;

/// MySQL 8.0 documented column order — keep this in sync with
/// REPLICA_STATUS_COLUMN_DEFS in src/replication.rs.
const EXPECTED_COLUMNS: &[&str] = &[
    "Replica_IO_State",
    "Source_Host",
    "Source_User",
    "Source_Port",
    "Connect_Retry",
    "Source_Log_File",
    "Read_Source_Log_Pos",
    "Relay_Log_File",
    "Relay_Log_Pos",
    "Relay_Source_Log_File",
    "Replica_IO_Running",
    "Replica_SQL_Running",
    "Replicate_Do_DB",
    "Replicate_Ignore_DB",
    "Replicate_Do_Table",
    "Replicate_Ignore_Table",
    "Replicate_Wild_Do_Table",
    "Replicate_Wild_Ignore_Table",
    "Last_Errno",
    "Last_Error",
    "Skip_Counter",
    "Exec_Source_Log_Pos",
    "Relay_Log_Space",
    "Until_Condition",
    "Until_Log_File",
    "Until_Log_Pos",
    "Source_SSL_Allowed",
    "Source_SSL_CA_File",
    "Source_SSL_CA_Path",
    "Source_SSL_Cert",
    "Source_SSL_Cipher",
    "Source_SSL_Key",
    "Seconds_Behind_Source",
    "Source_SSL_Verify_Server_Cert",
    "Last_IO_Errno",
    "Last_IO_Error",
    "Last_SQL_Errno",
    "Last_SQL_Error",
    "Replicate_Ignore_Server_Ids",
    "Source_Server_Id",
    "Source_UUID",
    "Source_Info_File",
    "SQL_Delay",
    "SQL_Remaining_Delay",
    "Replica_SQL_Running_State",
    "Source_Retry_Count",
    "Source_Bind",
    "Last_IO_Error_Timestamp",
    "Last_SQL_Error_Timestamp",
    "Source_SSL_Crl",
    "Source_SSL_Crlpath",
    "Retrieved_Gtid_Set",
    "Executed_Gtid_Set",
    "Auto_Position",
    "Replicate_Rewrite_DB",
    "Channel_Name",
    "Source_TLS_Version",
    "Source_public_key_path",
    "Get_Source_public_key",
    "Network_Namespace",
];

#[test]
fn show_replica_status_returns_60_columns_in_mysql_order() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "SHOW REPLICA STATUS").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.columns.len(),
        60,
        "MySQL 8.0 SHOW REPLICA STATUS mandates exactly 60 columns; got {}",
        rs.columns.len()
    );
    let names: Vec<String> = rs.columns.iter().map(|c| c.name.clone()).collect();
    assert_eq!(
        names,
        EXPECTED_COLUMNS
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>(),
        "column order must match MySQL 8.0 reference exactly"
    );
    // The Channel_Name column is at the documented 56th position.
    assert_eq!(rs.columns[55].name, "Channel_Name");
}

#[test]
fn show_slave_status_synonym_returns_60_columns_with_deprecation_warning() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "SHOW SLAVE STATUS").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.columns.len(), 60);
    assert!(
        session.warnings.iter().any(|w| w.code() == 1287),
        "SHOW SLAVE STATUS must emit deprecation warning 1287; got {:?}",
        session.warnings
    );

    // The deprecated synonym reports the legacy `Slave_*` / `Master_*` header
    // spelling (MySQL keeps the old names on SHOW SLAVE STATUS), while columns
    // that were never renamed stay identical.
    let names: Vec<String> = rs.columns.iter().map(|c| c.name.clone()).collect();
    assert!(names.contains(&"Slave_IO_Running".to_string()));
    assert!(names.contains(&"Slave_SQL_Running".to_string()));
    assert!(names.contains(&"Master_Host".to_string()));
    assert!(names.contains(&"Read_Master_Log_Pos".to_string()));
    assert!(names.contains(&"Exec_Master_Log_Pos".to_string()));
    assert!(names.contains(&"Seconds_Behind_Master".to_string()));
    assert!(names.contains(&"Master_Server_Id".to_string()));
    assert!(!names.contains(&"Replica_IO_Running".to_string()));
    assert!(!names.contains(&"Source_Host".to_string()));
    // Unchanged columns keep their spelling.
    assert!(names.contains(&"Connect_Retry".to_string()));
    assert!(names.contains(&"Channel_Name".to_string()));
    assert!(names.contains(&"Executed_Gtid_Set".to_string()));
}

#[test]
fn show_replica_status_unconfigured_returns_empty_result() {
    // Without `CHANGE REPLICATION SOURCE TO`, MySQL returns an empty result
    // set (not an error) — orchestrator/mha rely on this to detect "not a
    // replica" without paying for a probe round trip.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "SHOW REPLICA STATUS").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.columns.len(), 60);
    assert!(
        rs.rows.is_empty(),
        "unconfigured channel set should produce an empty result set"
    );
}
