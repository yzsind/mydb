//! One command classification and result schema for execution and PREPARE.
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum AdminCommand {
    MasterStatus,
    BinaryLogs,
    Replicas,
    Profiles,
    Profile,
    BinlogEvents,
    FlushBinaryLogs,
    ResetMaster,
    LockTables,
    UnlockTables,
    PurgeTo(&'static str),
    PurgeBefore(&'static str),
}

impl AdminCommand {
    fn exact(sql: &str) -> Option<Self> {
        Some(match sql {
            "SHOW MASTER STATUS" | "SHOW BINARY LOG STATUS" => Self::MasterStatus,
            "SHOW BINARY LOGS" | "SHOW MASTER LOGS" => Self::BinaryLogs,
            "SHOW REPLICAS" | "SHOW SLAVE HOSTS" => Self::Replicas,
            "SHOW PROFILES" => Self::Profiles,
            "SHOW PROFILE" => Self::Profile,
            "SHOW BINLOG EVENTS" => Self::BinlogEvents,
            "FLUSH BINARY LOGS" => Self::FlushBinaryLogs,
            "RESET MASTER" | "RESET BINARY LOGS AND GTIDS" => Self::ResetMaster,
            "FLUSH TABLES WITH READ LOCK" => Self::LockTables,
            "UNLOCK TABLES" => Self::UnlockTables,
            _ => return None,
        })
    }

    fn from_normalized(sql: &str) -> Option<Self> {
        Self::exact(sql).or_else(|| {
            Some(match sql {
                _ if sql.starts_with("SHOW PROFILE") => Self::Profile,
                _ if sql.starts_with("SHOW BINLOG EVENTS") => Self::BinlogEvents,
                _ if sql.starts_with("PURGE BINARY LOGS TO ") => {
                    Self::PurgeTo("PURGE BINARY LOGS TO")
                }
                _ if sql.starts_with("PURGE MASTER LOGS TO ") => {
                    Self::PurgeTo("PURGE MASTER LOGS TO")
                }
                _ if sql.starts_with("PURGE BINARY LOGS BEFORE ") => {
                    Self::PurgeBefore("PURGE BINARY LOGS BEFORE")
                }
                _ if sql.starts_with("PURGE MASTER LOGS BEFORE ") => {
                    Self::PurgeBefore("PURGE MASTER LOGS BEFORE")
                }
                _ => return None,
            })
        })
    }

    pub(super) fn classify(sql: &str) -> Option<Self> {
        // Common canonical commands need no normalization allocation. Keep the
        // full normalizer for case/whitespace variants and argument-bearing SQL.
        Self::exact(sql.trim().trim_end_matches(';').trim())
            .or_else(|| Self::from_normalized(&normalize_admin_sql(sql)))
    }

    pub(super) fn parse(sql: &str) -> Result<Self, QueryError> {
        Self::classify(sql).ok_or_else(|| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!("unsupported binary log admin statement: {sql}"),
            )
        })
    }

    pub(super) fn columns(self) -> Vec<Column> {
        use ColumnType::{Long, LongLong, VarString};
        let fields: &[(&str, ColumnType)] = match self {
            Self::MasterStatus => &[
                ("File", VarString),
                ("Position", LongLong),
                ("Binlog_Do_DB", VarString),
                ("Binlog_Ignore_DB", VarString),
                ("Executed_Gtid_Set", VarString),
            ],
            Self::BinaryLogs => &[
                ("Log_name", VarString),
                ("File_size", LongLong),
                ("Encrypted", VarString),
            ],
            Self::Replicas => &[
                ("Server_id", Long),
                ("Host", VarString),
                ("Port", Long),
                ("Source_id", Long),
                ("Replica_UUID", VarString),
            ],
            Self::Profiles => &[
                ("Query_ID", Long),
                (
                    "Duration",
                    ColumnType::Decimal {
                        precision: 7,
                        scale: 4,
                    },
                ),
                ("Query", VarString),
            ],
            Self::Profile => &[
                ("Status", VarString),
                (
                    "Duration",
                    ColumnType::Decimal {
                        precision: 7,
                        scale: 4,
                    },
                ),
            ],
            Self::BinlogEvents => &[
                ("Log_name", VarString),
                ("Pos", LongLong),
                ("Event_type", VarString),
                ("Server_id", Long),
                ("End_log_pos", LongLong),
                ("Info", VarString),
            ],
            Self::FlushBinaryLogs
            | Self::ResetMaster
            | Self::LockTables
            | Self::UnlockTables
            | Self::PurgeTo(_)
            | Self::PurgeBefore(_) => &[],
        };
        fields
            .iter()
            .map(|&(name, column_type)| Column {
                name: name.to_owned(),
                column_type,
                source: Default::default(),
            })
            .collect()
    }
}

/// Describe the response without accessing the binlog manager or executing any
/// command. In particular, administrative OK responses have no result columns.
pub(crate) fn admin_result_columns(sql: &str) -> Result<Vec<Column>, QueryError> {
    Ok(AdminCommand::parse(sql)?.columns())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_fast_path_preserves_normalized_classification() {
        for base in [
            "SHOW MASTER STATUS",
            "SHOW BINARY LOG STATUS",
            "SHOW BINARY LOGS",
            "SHOW MASTER LOGS",
            "SHOW REPLICAS",
            "SHOW SLAVE HOSTS",
            "SHOW PROFILES",
            "SHOW PROFILE",
            "SHOW BINLOG EVENTS",
            "FLUSH BINARY LOGS",
            "RESET MASTER",
            "RESET BINARY LOGS AND GTIDS",
            "FLUSH TABLES WITH READ LOCK",
            "UNLOCK TABLES",
            "SHOW PROFILEs",
            "SHOW PROFILE FOR QUERY 1",
            "SHOW BINLOG EVENTS LIMIT 2",
            "PURGE MASTER LOGS TO 'Case  Sensitive.000001'",
            "PURGE BINARY LOGS BEFORE '2026-01-01'",
            "SHOW VARIABLES",
            "SELECT 1",
        ] {
            for sql in [
                base.to_owned(),
                base.to_ascii_lowercase(),
                format!("  {base};  "),
                base.replace(' ', " \t "),
                format!("{base} ; ;"),
            ] {
                assert_eq!(
                    AdminCommand::classify(&sql),
                    AdminCommand::from_normalized(&normalize_admin_sql(&sql)),
                    "{sql}"
                );
            }
        }
    }

    #[test]
    fn admin_aliases_and_whitespace_share_response_metadata() {
        for (left, right) in [
            ("SHOW MASTER STATUS", "  show   binary\tlog status;  "),
            ("SHOW BINARY LOGS", "show master logs"),
            ("SHOW REPLICAS", "show slave hosts"),
            (
                "SHOW BINLOG EVENTS",
                "SHOW BINLOG EVENTS IN 'BINARY LOGS' FROM 4 LIMIT 0",
            ),
            ("RESET MASTER", "RESET BINARY LOGS AND GTIDS"),
            ("PURGE BINARY LOGS TO 'x'", "PURGE MASTER LOGS TO 'x'"),
        ] {
            assert!(is_binlog_admin_sql(left));
            assert!(is_binlog_admin_sql(right));
            assert_eq!(
                admin_result_columns(left).unwrap(),
                admin_result_columns(right).unwrap()
            );
        }
        for sql in [
            "SELECT 1",
            "SHOW REPLICA STATUS",
            "SHOW VARIABLES",
            "RESET SOMETHING",
        ] {
            assert!(!is_binlog_admin_sql(sql));
            assert_eq!(admin_result_columns(sql).unwrap_err().code(), 1235);
            assert_eq!(
                handle_admin_sql(sql, &SessionContext::default())
                    .unwrap_err()
                    .code(),
                1235
            );
        }
        for sql in [
            "purge binary logs to 'MixedCase.000001'",
            "purge master logs to 'MixedCase.000001'",
        ] {
            let AdminCommand::PurgeTo(prefix) = AdminCommand::parse(sql).unwrap() else {
                panic!("purge command");
            };
            assert_eq!(
                unquote_admin_string(sql, prefix).unwrap(),
                "MixedCase.000001"
            );
        }
    }

    #[test]
    fn describe_matches_execution_without_running_admin_actions() {
        let mut session = SessionContext::default();
        session.binlog_snapshot_status = Some(Arc::new(BinlogStatusSnapshot {
            position: BinlogPosition {
                file: "mydb-bin.000001".into(),
                position: 4,
            },
            executed_gtid_set: String::new(),
        }));
        for sql in [
            "SHOW MASTER STATUS",
            "SHOW BINARY LOG STATUS",
            "SHOW PROFILES",
            "SHOW PROFILE",
            "SHOW BINLOG EVENTS LIMIT 0",
        ] {
            let QueryResponse::ResultSet(result) = handle_admin_sql(sql, &session).unwrap() else {
                panic!("result set");
            };
            assert_eq!(admin_result_columns(sql).unwrap(), result.columns, "{sql}");
        }
        let before = GLOBAL_READ_LOCK_OWNERS.lock().clone();
        for sql in [
            "FLUSH BINARY LOGS",
            "RESET MASTER",
            "RESET BINARY LOGS AND GTIDS",
            "FLUSH TABLES WITH READ LOCK",
            "UNLOCK TABLES",
            "PURGE MASTER LOGS BEFORE '2026-01-01'",
        ] {
            assert!(admin_result_columns(sql).unwrap().is_empty());
        }
        assert_eq!(*GLOBAL_READ_LOCK_OWNERS.lock(), before);
    }

    #[test]
    fn admin_result_types_preserve_protocol_column_domains() {
        use ColumnType::{Long, LongLong, VarString};
        for (command, expected) in [
            (
                AdminCommand::MasterStatus,
                vec![VarString, LongLong, VarString, VarString, VarString],
            ),
            (
                AdminCommand::BinaryLogs,
                vec![VarString, LongLong, VarString],
            ),
            (
                AdminCommand::Replicas,
                vec![Long, VarString, Long, Long, VarString],
            ),
            (
                AdminCommand::BinlogEvents,
                vec![VarString, LongLong, VarString, Long, LongLong, VarString],
            ),
            (
                AdminCommand::Profiles,
                vec![
                    Long,
                    ColumnType::Decimal {
                        precision: 7,
                        scale: 4,
                    },
                    VarString,
                ],
            ),
            (
                AdminCommand::Profile,
                vec![
                    VarString,
                    ColumnType::Decimal {
                        precision: 7,
                        scale: 4,
                    },
                ],
            ),
        ] {
            let columns = command.columns();
            assert_eq!(
                columns
                    .iter()
                    .map(|column| column.column_type)
                    .collect::<Vec<_>>(),
                expected
            );
            assert!(columns
                .iter()
                .all(|column| column.source == Default::default()));
        }
    }
}
