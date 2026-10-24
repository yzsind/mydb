use crate::replication_sql::{parse_show_relaylog_events_clause, ReplicationShow};
pub use crate::replication_types::{
    ReplThreadError, ReplicationChannelConfig, ReplicationSslMode, ReplicationStatus,
};
use std::path::PathBuf;
use std::sync::Arc;

use crate::engine::{QueryResponse, ResultSet, StorageQueryEngine};
use crate::parser::Statement;
use crate::storage::error;
use crate::types::QueryError;

include!("replication_state.rs");

pub struct ReplicationManager;

impl ReplicationManager {
    /// Stub: no channels in the non-replica build, so shutdown-time
    /// "stop all channels" is a no-op. Kept API-compatible with the
    /// replica build so `graceful_shutdown` doesn't need feature-gated
    /// callsites.
    pub fn stop_all_channels(&self) -> Result<(), QueryError> {
        Ok(())
    }
}

pub fn init_global(
    _data_dir: impl Into<PathBuf>,
    _engine: Arc<StorageQueryEngine>,
) -> Arc<ReplicationManager> {
    Arc::new(ReplicationManager)
}

pub fn global_manager() -> Option<Arc<ReplicationManager>> {
    None
}

pub fn maybe_start_from_env(_manager: &Arc<ReplicationManager>) -> Result<(), QueryError> {
    if crate::util::env::var_os("MYDB_REPL_SOURCE_HOST").is_some()
        || env_bool("MYDB_REPL_ENABLE")
        || env_bool("MYDB_REPL_START")
    {
        return Err(mysql_replica_disabled_error());
    }
    Ok(())
}

pub fn enforce_replica_read_only(statement: &Statement) -> Result<(), QueryError> {
    if !replica_read_only()
        || replication_applier_active()
        || !is_replica_write_statement(statement)
    {
        return Ok(());
    }
    Err(error(crate::error::catalog::ER_OPTION_PREVENTS_STATEMENT,
        "mydb is running with mydb_replica_read_only=ON; SET GLOBAL mydb_replica_read_only=0 before local writes",
    ))
}

pub fn handle_admin_sql(sql: &str) -> Result<QueryResponse, QueryError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if let Some(kind) = ReplicationShow::classify(trimmed) {
        kind.warn_if_legacy();
        match kind {
            ReplicationShow::Status { .. } => {
                parse_channel_only(trimmed, &["SHOW REPLICA STATUS", "SHOW SLAVE STATUS"])?;
            }
            ReplicationShow::RelaylogEvents => {
                parse_show_relaylog_events_clause(trimmed)?;
            }
        }
        return Ok(QueryResponse::ResultSet(ResultSet {
            columns: kind.columns(),
            rows: Vec::new(),
        }));
    }
    Err(mysql_replica_disabled_error())
}

impl ReplicationManager {
    pub fn snapshot_all_channels(&self) -> Vec<(ReplicationChannelConfig, ReplicationStatus)> {
        Vec::new()
    }
}

fn parse_channel_only(sql: &str, prefixes: &[&str]) -> Result<(), QueryError> {
    let upper = sql.to_ascii_uppercase();
    let Some(prefix) = prefixes.iter().find(|prefix| upper.starts_with(**prefix)) else {
        return Err(error(
            crate::error::catalog::ER_PARSE_ERROR,
            format!("bad replication command '{sql}'"),
        ));
    };
    let rest = sql[prefix.len()..].trim();
    if rest.is_empty() {
        return Ok(());
    }
    let rest_upper = rest.to_ascii_uppercase();
    if rest_upper.starts_with("FOR CHANNEL ") && !rest["FOR CHANNEL ".len()..].trim().is_empty() {
        return Ok(());
    }
    if rest_upper.starts_with("FOR CHANNEL ") {
        return Err(error(
            crate::error::catalog::ER_PARSE_ERROR,
            "FOR CHANNEL requires a channel name",
        ));
    }
    Err(error(
        crate::error::catalog::ER_NOT_SUPPORTED_YET,
        format!("replication command option '{rest}' is not supported by mydb"),
    ))
}

fn mysql_replica_disabled_error() -> QueryError {
    error(
        crate::error::catalog::ER_NOT_SUPPORTED_YET,
        "mydb was built without MySQL replica support; rebuild with --features mysql-replica",
    )
}

fn env_bool(name: &str) -> bool {
    crate::util::env::flag(name, false)
}
