//! Column-name aliasing shared by the (feature-gated) real replication module
//! and its stub. `SHOW REPLICA STATUS` reports the MySQL 8.0.22+ column names
//! (`Replica_*` / `Source_*`); the deprecated `SHOW SLAVE STATUS` reports the
//! historical names (`Slave_*` / `Master_*`). The row *values* are identical —
//! only the header differs. Schema construction and the compatibility adapter
//! both use [`legacy_replica_column_alias`] for the spelling rules.

use crate::types::Column;

/// Map a `SHOW REPLICA STATUS` column name to its legacy `SHOW SLAVE STATUS`
/// spelling. Returns `None` for columns whose name never changed
/// (`Connect_Retry`, `Relay_Log_File`, `Replicate_Do_DB`, `Executed_Gtid_Set`,
/// `Channel_Name`, …), so callers keep the original name in that case.
pub fn legacy_replica_column_alias(new_name: &str) -> Option<&'static str> {
    Some(match new_name {
        "Replica_IO_State" => "Slave_IO_State",
        "Source_Host" => "Master_Host",
        "Source_User" => "Master_User",
        "Source_Port" => "Master_Port",
        "Source_Log_File" => "Master_Log_File",
        "Read_Source_Log_Pos" => "Read_Master_Log_Pos",
        "Relay_Source_Log_File" => "Relay_Master_Log_File",
        "Replica_IO_Running" => "Slave_IO_Running",
        "Replica_SQL_Running" => "Slave_SQL_Running",
        "Exec_Source_Log_Pos" => "Exec_Master_Log_Pos",
        "Source_SSL_Allowed" => "Master_SSL_Allowed",
        "Source_SSL_CA_File" => "Master_SSL_CA_File",
        "Source_SSL_CA_Path" => "Master_SSL_CA_Path",
        "Source_SSL_Cert" => "Master_SSL_Cert",
        "Source_SSL_Cipher" => "Master_SSL_Cipher",
        "Source_SSL_Key" => "Master_SSL_Key",
        "Seconds_Behind_Source" => "Seconds_Behind_Master",
        "Source_SSL_Verify_Server_Cert" => "Master_SSL_Verify_Server_Cert",
        "Source_Server_Id" => "Master_Server_Id",
        "Source_UUID" => "Master_UUID",
        "Source_Info_File" => "Master_Info_File",
        "Replica_SQL_Running_State" => "Slave_SQL_Running_State",
        "Source_Retry_Count" => "Master_Retry_Count",
        "Source_Bind" => "Master_Bind",
        "Source_SSL_Crl" => "Master_SSL_Crl",
        "Source_SSL_Crlpath" => "Master_SSL_Crlpath",
        "Source_TLS_Version" => "Master_TLS_Version",
        "Source_public_key_path" => "Master_public_key_path",
        "Get_Source_public_key" => "Get_master_public_key",
        _ => return None,
    })
}

/// Rewrite a `SHOW REPLICA STATUS` column header to the legacy
/// `SHOW SLAVE STATUS` header in place. Columns whose name never changed are
/// left untouched.
pub fn to_legacy_slave_status_columns(mut columns: Vec<Column>) -> Vec<Column> {
    for column in &mut columns {
        if let Some(old) = legacy_replica_column_alias(&column.name) {
            column.name = old.to_string();
        }
    }
    columns
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unchanged_columns_return_none() {
        for name in [
            "Connect_Retry",
            "Relay_Log_File",
            "Relay_Log_Pos",
            "Replicate_Do_DB",
            "Last_Errno",
            "Executed_Gtid_Set",
            "Auto_Position",
            "Channel_Name",
            "Network_Namespace",
        ] {
            assert_eq!(legacy_replica_column_alias(name), None, "{name}");
        }
    }

    #[test]
    fn renamed_columns_map_to_legacy_spelling() {
        assert_eq!(
            legacy_replica_column_alias("Replica_IO_Running"),
            Some("Slave_IO_Running")
        );
        assert_eq!(
            legacy_replica_column_alias("Source_Host"),
            Some("Master_Host")
        );
        assert_eq!(
            legacy_replica_column_alias("Seconds_Behind_Source"),
            Some("Seconds_Behind_Master")
        );
        assert_eq!(
            legacy_replica_column_alias("Get_Source_public_key"),
            Some("Get_master_public_key")
        );
    }
}
