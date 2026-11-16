//! Strict typed backup SQL; retired syntax is rejected before execution.
use super::coordinator::{Coordinator, Policy};
use super::*;
use sqlparser::{
    dialect::MySqlDialect,
    tokenizer::{Token, Tokenizer},
};
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BackupCommand {
    Database {
        repository: String,
    },
    Wal {
        repository: String,
    },
    Alter {
        repository: String,
        policy: Option<Policy>,
        discard_pending: bool,
        max_pending_wal_bytes: Option<u64>,
    },
    Status,
    Jobs,
    List {
        repository: String,
    },
    Cancel {
        id: String,
    },
}
impl BackupCommand {
    pub(crate) fn is_read_only(&self) -> bool {
        matches!(self, Self::Status | Self::Jobs | Self::List { .. })
    }
}
pub(crate) fn recognizes(sql: &str) -> bool {
    let sql = crate::sql_lex::normalize_admin_sql(sql);
    [
        "BACKUP DATABASE",
        "BACKUP WAL",
        "ALTER MYDB BACKUP",
        "SHOW MYDB BACKUP",
        "CANCEL MYDB BACKUP",
    ]
    .iter()
    .any(|prefix| {
        sql == *prefix
            || sql.starts_with(&format!("{prefix} "))
            || sql.starts_with(&format!("{prefix}S "))
    })
}
struct Tokens {
    values: Vec<Token>,
    index: usize,
}
impl Tokens {
    fn word(&mut self, value: &str) -> bool {
        if matches!(self.values.get(self.index),Some(Token::Word(word))if word.quote_style.is_none()&&word.value.eq_ignore_ascii_case(value))
        {
            self.index += 1;
            true
        } else {
            false
        }
    }
    fn expect(&mut self, value: &str) -> Result<(), QueryError> {
        if self.word(value) {
            Ok(())
        } else {
            Err(backup_err(format!("expected {value} in backup SQL")))
        }
    }
    fn string(&mut self) -> Result<String, QueryError> {
        match self.values.get(self.index) {
            Some(Token::SingleQuotedString(value)) => {
                self.index += 1;
                Ok(value.clone())
            }
            _ => Err(backup_err(
                "backup URI/job ID must be a single-quoted string",
            )),
        }
    }
    fn symbol(&mut self, token: Token) -> bool {
        if self.values.get(self.index) == Some(&token) {
            self.index += 1;
            true
        } else {
            false
        }
    }
    fn end(&self) -> Result<(), QueryError> {
        if self.index == self.values.len() {
            Ok(())
        } else {
            Err(backup_err("unknown, duplicate, or retired backup option"))
        }
    }
}
pub(crate) fn parse(sql: &str) -> Result<BackupCommand, QueryError> {
    let mut values = Tokenizer::new(&MySqlDialect {}, sql)
        .tokenize()
        .map_err(|e| backup_err(e.to_string()))?
        .into_iter()
        .filter(|t| !matches!(t, Token::Whitespace(_)))
        .collect::<Vec<_>>();
    if values.last() == Some(&Token::SemiColon) {
        values.pop();
    }
    let mut t = Tokens { values, index: 0 };
    let command = if t.word("BACKUP") {
        let database = t.word("DATABASE");
        if !database {
            t.expect("WAL")?;
        }
        t.expect("TO")?;
        let repository = t.string()?;
        repository_from_uri(&repository)?;
        if database {
            BackupCommand::Database { repository }
        } else {
            BackupCommand::Wal { repository }
        }
    } else if t.word("ALTER") {
        t.expect("MYDB")?;
        t.expect("BACKUP")?;
        t.expect("TO")?;
        let repository = t.string()?;
        repository_from_uri(&repository)?;
        t.expect("SET")?;
        let mut policy = None;
        let mut discard = false;
        let mut budget = None;
        let mut seen = BTreeSet::new();
        loop {
            let name = match t.values.get(t.index) {
                Some(Token::Word(word)) if word.quote_style.is_none() => {
                    word.value.to_ascii_uppercase()
                }
                _ => return Err(backup_err("expected backup policy option")),
            };
            t.index += 1;
            if !seen.insert(name.clone()) || !t.symbol(Token::Eq) {
                return Err(backup_err("duplicate option or missing = in backup policy"));
            }
            match name.as_str() {
                "WAL_ARCHIVE" => {
                    policy = Some(if t.word("DISABLED") {
                        Policy::Disabled
                    } else if t.word("MANUAL") {
                        Policy::Manual
                    } else if t.word("CONTINUOUS") {
                        Policy::Continuous
                    } else {
                        return Err(backup_err(
                            "WAL_ARCHIVE must be DISABLED, MANUAL, or CONTINUOUS",
                        ));
                    });
                }
                "DISCARD_PENDING" => {
                    discard = if t.word("TRUE") {
                        true
                    } else if t.word("FALSE") {
                        false
                    } else {
                        return Err(backup_err("DISCARD_PENDING must be TRUE/FALSE"));
                    };
                }
                "MAX_PENDING_WAL_BYTES" => {
                    let Some(Token::Number(value, false)) = t.values.get(t.index) else {
                        return Err(backup_err(
                            "MAX_PENDING_WAL_BYTES requires an unsigned integer",
                        ));
                    };
                    budget = Some(
                        value
                            .parse::<u64>()
                            .ok()
                            .filter(|n| *n > 0)
                            .ok_or_else(|| backup_err("invalid pending WAL budget"))?,
                    );
                    t.index += 1;
                }
                _ => return Err(backup_err(format!("unknown backup option {name}"))),
            }
            if !t.symbol(Token::Comma) {
                break;
            }
        }
        if discard && policy != Some(Policy::Disabled) {
            return Err(backup_err("DISCARD_PENDING requires WAL_ARCHIVE=DISABLED"));
        }
        BackupCommand::Alter {
            repository,
            policy,
            discard_pending: discard,
            max_pending_wal_bytes: budget,
        }
    } else if t.word("SHOW") {
        t.expect("MYDB")?;
        if t.word("BACKUPS") {
            t.expect("FROM")?;
            let repository = t.string()?;
            repository_from_uri(&repository)?;
            BackupCommand::List { repository }
        } else {
            t.expect("BACKUP")?;
            if t.word("STATUS") {
                BackupCommand::Status
            } else {
                t.expect("JOBS")?;
                BackupCommand::Jobs
            }
        }
    } else {
        t.expect("CANCEL")?;
        t.expect("MYDB")?;
        t.expect("BACKUP")?;
        let id = t.string()?;
        validate_id(&id)?;
        BackupCommand::Cancel { id }
    };
    t.end()?;
    Ok(command)
}
fn result(names: &[&str], rows: Vec<Vec<Value>>) -> QueryResponse {
    QueryResponse::ResultSet(ResultSet {
        columns: names
            .iter()
            .map(|name| {
                column(
                    name,
                    if [
                        "Requested_LSN",
                        "Requested_Source_Time",
                        "Durable_Through_LSN",
                        "Covered_Time",
                        "Catalog_Generation",
                        "Bytes",
                        "Source_End_LSN",
                        "Source_End_Time",
                    ]
                    .contains(name)
                    {
                        ColumnType::LongLong
                    } else {
                        ColumnType::VarString
                    },
                )
            })
            .collect(),
        rows,
    })
}
fn jobs_result(jobs: Vec<super::coordinator::Job>) -> QueryResponse {
    result(
        &[
            "Job_ID",
            "Kind",
            "State",
            "Phase",
            "Base_ID",
            "Requested_LSN",
            "Requested_Source_Time",
            "Durable_Through_LSN",
            "Covered_Time",
            "Catalog_Generation",
            "Bytes",
            "Error",
            "Created_Unix_Ms",
            "Started_Unix_Ms",
            "Completed_Unix_Ms",
            "Failure_Class",
            "Phase_Times_Us",
            "Repository_ID",
            "Files_Done",
            "Phase_Remaining_Bytes",
        ],
        jobs.into_iter()
            .map(|job| {
                vec![
                    Value::String(job.id),
                    Value::String(job.kind),
                    Value::String(job.state),
                    Value::String(job.phase),
                    job.base_id.map(Value::String).unwrap_or(Value::Null),
                    Value::UInt(job.requested_lsn),
                    Value::UInt(job.requested_source_time),
                    Value::UInt(job.durable_through_lsn),
                    Value::UInt(job.covered_time),
                    Value::UInt(job.catalog_generation),
                    Value::UInt(job.bytes),
                    Value::String(job.error),
                    Value::UInt(job.created_unix_ms),
                    Value::UInt(job.started_unix_ms),
                    Value::UInt(job.completed_unix_ms),
                    Value::String(job.failure_class),
                    Value::String(serde_json::to_string(&job.phase_times_us).unwrap_or_default()),
                    job.repository_id.map(Value::String).unwrap_or(Value::Null),
                    Value::UInt(job.files_done),
                    job.phase_remaining_bytes
                        .map(Value::UInt)
                        .unwrap_or(Value::Null),
                ]
            })
            .collect(),
    )
}
pub(crate) fn execute(
    db: &Arc<Database>,
    command: &BackupCommand,
) -> Result<QueryResponse, QueryError> {
    use BackupCommand::*;
    match command {
        List { repository } => {
            let repo =
                super::repository::Repository::open(&repository_from_uri(repository)?, None)?;
            let _lease = super::repository::Lease::acquire(&repo.root, false)?;
            let mut rows = Vec::new();
            let mut deleted = BTreeSet::new();
            repo.visit_live(&repo.position()?, |_, event| {
                match event {
                    super::repository::Publication::Prune { bases, .. } => deleted.extend(bases),
                    super::repository::Publication::Base { base }
                        if !deleted.contains(&base.id) =>
                    {
                        rows.push(vec![
                            Value::String(base.id),
                            Value::String(base.source_id),
                            base.stream_id.map(Value::String).unwrap_or(Value::Null),
                            Value::UInt(base.source_end_lsn),
                            Value::UInt(base.source_end_time),
                        ])
                    }
                    _ => {}
                }
                if rows.len() > 100_000 {
                    return Err(backup_err("listing exceeds row budget"));
                }
                Ok(())
            })?;
            Ok(result(
                &[
                    "Base_ID",
                    "Source_ID",
                    "Stream_ID",
                    "Source_End_LSN",
                    "Source_End_Time",
                ],
                rows,
            ))
        }
        Status if db.is_snapshot_read_only() => Ok(result(
            &["Name", "Value"],
            vec![vec![
                Value::String("Lifecycle".into()),
                Value::String("READ_ONLY_IMAGE".into()),
            ]],
        )),
        Jobs if db.is_snapshot_read_only() => Ok(jobs_result(vec![])),
        _ => {
            let coordinator = Coordinator::get(db)?;
            match command {
                Database { repository } => Ok(jobs_result(vec![
                    coordinator.backup(&repository_from_uri(repository)?)?
                ])),
                Wal { repository } => Ok(jobs_result(vec![
                    coordinator.backup_wal(&repository_from_uri(repository)?)?
                ])),
                Alter {
                    repository,
                    policy,
                    discard_pending,
                    max_pending_wal_bytes,
                } => {
                    let job = coordinator.configure_patch(
                        &repository_from_uri(repository)?,
                        *policy,
                        *discard_pending,
                        *max_pending_wal_bytes,
                    )?;
                    Ok(job
                        .map(|job| jobs_result(vec![job]))
                        .unwrap_or(QueryResponse::Ok))
                }
                Jobs => Ok(jobs_result(coordinator.jobs())),
                Cancel { id } => {
                    coordinator.cancel(id)?;
                    Ok(QueryResponse::Ok)
                }
                Status => {
                    let control = coordinator.control.lock().unwrap();
                    let mut rows = Vec::new();
                    let mut put = |name: &str, value: String| {
                        rows.push(vec![Value::String(name.into()), Value::String(value)])
                    };
                    put("Source_LSN", db.wal_log().lock().last_lsn().to_string());
                    put(
                        "Backup_Process_IO",
                        super::resources::io_snapshot().to_string(),
                    );
                    put(
                        "Pending_WAL_Bytes",
                        db.capture
                            .budget
                            .pending
                            .load(Ordering::Acquire)
                            .saturating_mul(u64::from(
                                db.capture.budget.enabled.load(Ordering::Acquire),
                            ))
                            .to_string(),
                    );
                    put(
                        "WAL_Pressure",
                        (db.capture.budget.check(1).is_err()).to_string(),
                    );
                    put(
                        "WAL_Floor",
                        db.capture
                            .floor()
                            .map(|n| n.to_string())
                            .unwrap_or_default(),
                    );
                    if let Some(state) = control.as_ref() {
                        put(
                            "Requested_Policy",
                            format!("{:?}", state.requested).to_uppercase(),
                        );
                        put(
                            "Effective_Policy",
                            format!("{:?}", state.effective).to_uppercase(),
                        );
                        put("Lifecycle", state.lifecycle.clone());
                        put(
                            "Health",
                            if state.lifecycle == "BROKEN" {
                                "BROKEN"
                            } else if db.capture.budget.check(1).is_err() {
                                "WAL_PRESSURE"
                            } else if state.lifecycle == "CLOSED" && state.close_publication_pending
                            {
                                "CLOSED_PENDING_REPOSITORY"
                            } else if !state.error.is_empty() {
                                if state.effective == Policy::Continuous {
                                    "RETRYING"
                                } else {
                                    "ERROR"
                                }
                            } else if state.lifecycle == "CLOSED" {
                                "CLOSED"
                            } else if state.effective == Policy::Manual
                                && state.lifecycle == "ACTIVE"
                            {
                                "WAITING_FOR_COMMAND"
                            } else {
                                "OK"
                            }
                            .into(),
                        );
                        put("Repository_ID", state.repository_id.clone());
                        put("Source_ID", state.source_id.clone());
                        put("Timeline_ID", state.timeline_id.clone());
                        put("Stream_ID", state.stream_id.clone().unwrap_or_default());
                        put("Latest_Base", state.latest_base.clone().unwrap_or_default());
                        put("Latest_Base_LSN", state.latest_base_lsn.to_string());
                        put("Latest_Base_Time", state.latest_base_time.to_string());
                        put("QUERY_READY", (state.latest_base.is_some()).to_string());
                        put(
                            "CONTINUATION_READY",
                            (state.latest_base.is_some()).to_string(),
                        );
                        put("Recoverable_From_LSN", state.latest_base_lsn.to_string());
                        put(
                            "Recoverable_Through_LSN",
                            state.cursor.max(state.latest_base_lsn).to_string(),
                        );
                        put("Next_Retry_Unix_Ms", state.next_retry_unix_ms.to_string());
                        put("Durable_Archived_LSN", state.cursor.to_string());
                        put("Covered_Time", state.covered_time.to_string());
                        put(
                            "MAX_PENDING_WAL_BYTES",
                            state.max_pending_wal_bytes.to_string(),
                        );
                        put("Last_Error", state.error.clone());
                        put(
                            "Close_Publication_Pending",
                            state.close_publication_pending.to_string(),
                        );
                        put("Discarded", state.close_discarded.to_string());
                        put("Consecutive_Errors", state.consecutive_errors.to_string());
                        put("Last_Error_Unix_Ms", state.last_error_unix_ms.to_string());
                        put(
                            "Source_Time_Lag_Us",
                            now_ms()
                                .saturating_mul(1000)
                                .saturating_sub(state.covered_time)
                                .to_string(),
                        );
                        put(
                            "Backup_Resources",
                            serde_json::to_string(super::resources::limits()?)
                                .map_err(|e| backup_err(e.to_string()))?,
                        );
                    } else {
                        put("Requested_Policy", "DISABLED".into());
                        put("Effective_Policy", "DISABLED".into());
                        put("Lifecycle", "UNCONFIGURED".into());
                    }
                    Ok(result(&["Name", "Value"], rows))
                }
                List { .. } => unreachable!(),
            }
        }
    }
}
