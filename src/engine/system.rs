use crate::parser::{ShowFilter, ShowLimit, ShowScope};
use crate::storage::{error, Database};
use crate::types::{Column, ColumnType, QueryError, Value};

use super::api::{QueryEngine, QueryResponse, ResultSet, SessionContext};
use super::show::apply_show_filter;

#[derive(Debug, Default)]
pub struct StubQueryEngine;

impl QueryEngine for StubQueryEngine {
    fn execute(
        &self,
        session: &mut SessionContext,
        sql: &str,
    ) -> Result<QueryResponse, QueryError> {
        // Every stub-handled statement begins with `SELECT`, `SHOW`,
        // `SET`, or `GET` (case-insensitive). Anything else cannot
        // match — we used to find this out *after* paying
        // `normalize_sql` (an ~SQL-length allocation + lower / collapse
        // pass) and `to_ascii_uppercase` (a second copy). The byte
        // prefix on the *raw* SQL drops out of the stub on the first
        // non-matching byte, sparing both allocations on every
        // INSERT/UPDATE/DELETE the OLTP path forwards in.
        let first = sql
            .as_bytes()
            .iter()
            .copied()
            .find(|b| !b.is_ascii_whitespace())
            .map(|b| b.to_ascii_uppercase());
        if !matches!(first, Some(b'S' | b'G' | b'/' | b'-')) {
            // None of the stub-handled families start with anything else.
            // `/` and `-` are kept so a leading `/* … */` or `-- …`
            // comment doesn't prevent the stub from matching the real
            // statement; `normalize_sql` strips them next.
            return Err(crate::error::catalog::ER_PARSE_ERROR
                .error(format!("mydb does not support this query yet: {sql}")));
        }
        let normalized = normalize_sql(sql);
        let upper = normalized.to_ascii_uppercase();

        if is_runtime_storage_set(&upper) {
            return Err(error(
                crate::error::catalog::ER_UNKNOWN_SYSTEM_VARIABLE,
                "runtime mydb variables require the storage engine",
            ));
        }

        if upper == "SET SQL_MODE=''" {
            session.sql_mode.clear();
            // This exact no-whitespace JDBC startup form is handled before
            // the storage engine's generic SET/sysvar path. Keep the same
            // distinction as `execute_set_statement`: an explicit empty
            // sql_mode requests MySQL's non-strict assignment semantics,
            // whereas the compiled empty default retains mydb's historical
            // strict posture.
            session.temporal_sql_mode_bits = crate::engine::api::TEMPORAL_MODE_EXPLICIT;
            return Ok(QueryResponse::Ok);
        }

        // Only the `SHOW VARIABLES/STATUS/COUNT/DIAGNOSTICS/COLLATION/
        // CHARSET` family is handled here, and they all start with the
        // ASCII keyword `SHOW`. For everything else (the dominant case
        // on OLTP workloads — INSERTs, UPDATEs, SELECTs that don't
        // match a literal stub above) the `parse_statement` call below
        // was pure overhead: it ran sqlparser only to throw the result
        // away because `_ => {}` matched. The byte prefix lets us skip
        // the entire branch on every non-SHOW statement (≈ 99 % of the
        // mix on TPC-C / sysbench). Diagnostics aren't a hot path so
        // the GET DIAGNOSTICS variants stay behind the parse — they
        // start with `GET` which the prefix check also covers below.
        let upper_bytes = upper.as_bytes();
        let looks_like_show = upper_bytes.starts_with(b"SHOW ")
            || upper_bytes.starts_with(b"SHOW\t")
            || upper_bytes == b"SHOW";
        let looks_like_get_diagnostics = upper_bytes.starts_with(b"GET ");
        if looks_like_show || looks_like_get_diagnostics {
            if let Ok(statement) = crate::parser::parse_statement(&normalized) {
                match statement {
                    crate::parser::Statement::ShowVariables { scope, filter } => {
                        return show_variables_result(session, None, scope, filter.as_ref());
                    }
                    crate::parser::Statement::ShowStatus { scope, filter } => {
                        return show_status_result(session, None, None, scope, filter.as_ref());
                    }
                    crate::parser::Statement::ShowCountDiagnostics { errors_only } => {
                        return Ok(show_count_result(
                            if errors_only {
                                "@@session.error_count"
                            } else {
                                "@@session.warning_count"
                            },
                            if errors_only {
                                session.error_count()
                            } else {
                                session.warning_count()
                            },
                        ));
                    }
                    crate::parser::Statement::ShowDiagnostics { errors_only, limit } => {
                        return Ok(show_diagnostics_result(session, errors_only, limit));
                    }
                    crate::parser::Statement::ShowCollation { filter } => {
                        return show_collation_result(filter.as_ref());
                    }
                    crate::parser::Statement::ShowCharset { filter } => {
                        return crate::engine::show::show_charset_result(filter.as_ref());
                    }
                    _ => {}
                }
            }
        }

        if upper == "SELECT VERSION()" {
            return Ok(QueryResponse::ResultSet(ResultSet {
                columns: vec![Column {
                    source: Default::default(),
                    name: "VERSION()".to_string(),
                    column_type: ColumnType::VarString,
                }],
                rows: vec![vec![Value::String(
                    crate::server_version::MYSQL_COMPAT_VERSION.to_string(),
                )]],
            }));
        }

        if upper == "SELECT DATABASE()" || upper == "SELECT SCHEMA()" {
            return Ok(QueryResponse::ResultSet(ResultSet {
                columns: vec![Column {
                    source: Default::default(),
                    name: "DATABASE()".to_string(),
                    column_type: ColumnType::VarString,
                }],
                rows: vec![vec![if session.current_database.is_empty() {
                    Value::Null
                } else {
                    Value::String(session.current_database.clone())
                }]],
            }));
        }

        Err(crate::error::catalog::ER_PARSE_ERROR
            .error(format!("mydb does not support this query yet: {sql}")))
    }
}

/// Normalise `sql` into a stable plan-cache key.
///
/// Quoted text and optimizer hints are preserved, ordinary comments become a
/// separator, executable comments are version-gated, and code whitespace/case
/// is canonicalized in one output allocation.
pub fn normalize_sql(sql: &str) -> String {
    normalize_sql_with_mode(
        sql,
        crate::sql_lex::SqlLexMode::from_parser_bits(crate::parser::current_parser_sql_mode_bits()),
    )
}

/// SQL-mode-aware cache-key normalization. Ordinary comments are semantic
/// whitespace, optimizer hints remain part of the key, and executable comments
/// are version-gated before normalization. In particular, a line-comment
/// newline is never collapsed before the comment has been removed.
pub(crate) fn normalize_sql_with_mode(sql: &str, mode: crate::sql_lex::SqlLexMode) -> String {
    let Ok(prepared) = crate::sql_lex::PreparedSql::new(sql, mode, crate::parser::MYSQL_VERSION_ID)
    else {
        // Invalid SQL is not cacheable. Preserve it conservatively so two
        // different malformed statements cannot accidentally share a key.
        return sql.trim().to_string();
    };
    let effective = prepared.effective();
    let mut out = Vec::with_capacity(effective.len());
    let mut pending_space = false;

    for segment in crate::sql_lex::segments(effective, mode, crate::parser::MYSQL_VERSION_ID) {
        let Ok(segment) = segment else {
            return sql.trim().to_string();
        };
        use crate::sql_lex::SqlSegmentKind;
        let bytes = &effective.as_bytes()[segment.range];
        match segment.kind {
            SqlSegmentKind::LineComment | SqlSegmentKind::BlockComment => {
                pending_space |= !out.is_empty();
            }
            SqlSegmentKind::OptimizerHint => {
                if pending_space && !out.is_empty() && out.last() != Some(&b' ') {
                    out.push(b' ');
                }
                pending_space = false;
                out.extend_from_slice(bytes);
            }
            SqlSegmentKind::Quoted { .. } => {
                if pending_space && !out.is_empty() && out.last() != Some(&b' ') {
                    out.push(b' ');
                }
                pending_space = false;
                out.extend_from_slice(bytes);
            }
            SqlSegmentKind::Code | SqlSegmentKind::ExecutableComment { .. } => {
                for byte in bytes {
                    if byte.is_ascii_whitespace() {
                        pending_space |= !out.is_empty();
                    } else {
                        if pending_space && !out.is_empty() && out.last() != Some(&b' ') {
                            out.push(b' ');
                        }
                        pending_space = false;
                        out.push(byte.to_ascii_lowercase());
                    }
                }
            }
        }
    }

    while out.last() == Some(&b' ') {
        out.pop();
    }
    if out.last() == Some(&b';') {
        out.pop();
        while out.last() == Some(&b' ') {
            out.pop();
        }
    }
    String::from_utf8(out).expect("normalization preserves UTF-8")
}

/// Byte-level helper used by [`normalize_sql`]. Skips leading whitespace
/// and `/* … */` block comments, plus `-- …\n` line comments — same set
/// the original [`strip_leading_comments`] handled, kept compatible for
/// the plan-cache key tests. Unterminated comments cause the cursor to
/// stop at the comment's start position (mirroring the previous behaviour).
fn skip_leading_ws_and_comments_bytes(sql: &str) -> usize {
    let bytes = sql.as_bytes();
    let mut pos = 0usize;
    while bytes.get(pos).is_some_and(u8::is_ascii_whitespace) {
        pos += 1;
    }
    // The overwhelming OLTP case has no leading comment. Prove that with two
    // byte checks and avoid constructing the general SQL segment iterator on
    // every statement merely to decide whether diagnostics should be kept.
    // `#` and `-- ` line comments plus `/* ... */` retain the complete lexer
    // oracle below, including executable-comment/version semantics.
    let begins_comment = bytes.get(pos) == Some(&b'#')
        || (bytes.get(pos) == Some(&b'/') && bytes.get(pos + 1) == Some(&b'*'))
        || (bytes.get(pos) == Some(&b'-')
            && bytes.get(pos + 1) == Some(&b'-')
            && bytes
                .get(pos + 2)
                .is_some_and(|byte| byte.is_ascii_whitespace()));
    if !begins_comment {
        return pos;
    }
    let mode =
        crate::sql_lex::SqlLexMode::from_parser_bits(crate::parser::current_parser_sql_mode_bits());
    crate::sql_lex::skip_trivia(sql, 0, mode, crate::parser::MYSQL_VERSION_ID, false).unwrap_or(0)
}

pub(crate) fn preserves_statement_diagnostics(sql: &str) -> bool {
    // Byte-prefix pre-filter: only `SHOW …` / `SELECT …@@…` forms can
    // match, so skip the normalize + full-body uppercase (two allocations
    // that ran once per statement — measurable on OLTP profiles, and
    // pathological on multi-kilobyte bulk-DML bodies) for everything else.
    // Leading comments are skipped with the same helper `normalize_sql`
    // uses, so `/*c*/ SHOW WARNINGS` still matches.
    let bytes = sql.as_bytes();
    let start = skip_leading_ws_and_comments_bytes(sql);
    let trimmed = &bytes[start.min(bytes.len())..];
    let plausible = (trimmed.len() > 4 && trimmed[..4].eq_ignore_ascii_case(b"SHOW"))
        || (trimmed.len() > 3 && trimmed[..3].eq_ignore_ascii_case(b"GET"))
        || (trimmed.len() > 6
            && trimmed[..6].eq_ignore_ascii_case(b"SELECT")
            && sql.contains("@@"));
    if !plausible {
        return false;
    }
    let upper = normalize_sql(sql).to_ascii_uppercase();
    upper.starts_with("SHOW WARNINGS")
        || upper.starts_with("GET DIAGNOSTICS")
        || upper.starts_with("GET CURRENT DIAGNOSTICS")
        || upper.starts_with("GET STACKED DIAGNOSTICS")
        || upper.starts_with("SHOW ERRORS")
        || is_show_count_warnings(&upper)
        || is_show_count_errors(&upper)
        || references_diagnostics_count(&upper)
}

fn is_show_count_warnings(upper: &str) -> bool {
    upper.starts_with("SHOW COUNT(*) WARNINGS")
}

fn is_show_count_errors(upper: &str) -> bool {
    upper.starts_with("SHOW COUNT(*) ERRORS")
}

pub(crate) fn references_diagnostics_count(upper: &str) -> bool {
    upper.starts_with("SELECT ")
        && [
            "@@WARNING_COUNT",
            "@@SESSION.WARNING_COUNT",
            "@@ERROR_COUNT",
            "@@SESSION.ERROR_COUNT",
        ]
        .iter()
        .any(|name| upper.contains(name))
}

pub(crate) fn show_count_result(name: &str, count: u16) -> QueryResponse {
    QueryResponse::ResultSet(ResultSet {
        columns: vec![Column {
            source: Default::default(),
            name: name.to_string(),
            column_type: ColumnType::LongLong,
        }],
        rows: vec![vec![Value::UInt(count as u64)]],
    })
}

pub(crate) fn show_diagnostics_result(
    session: &SessionContext,
    errors_only: bool,
    limit: Option<ShowLimit>,
) -> QueryResponse {
    let rows = session
        .warnings
        .iter()
        .filter(|warning| !errors_only || warning.level() == crate::error::ConditionLevel::Error)
        .map(|warning| {
            vec![
                Value::String(warning.level().as_str().to_string()),
                Value::Int(warning.code() as i64),
                Value::String(warning.message().to_string()),
            ]
        });
    let rows = if let Some(limit) = limit {
        rows.skip(limit.offset).take(limit.row_count).collect()
    } else {
        rows.collect()
    };
    QueryResponse::ResultSet(ResultSet {
        columns: vec![
            Column {
                source: Default::default(),
                name: "Level".to_string(),
                column_type: ColumnType::VarString,
            },
            Column {
                source: Default::default(),
                name: "Code".to_string(),
                column_type: ColumnType::Long,
            },
            Column {
                source: Default::default(),
                name: "Message".to_string(),
                column_type: ColumnType::VarString,
            },
        ],
        rows,
    })
}

pub(crate) fn show_variables_result(
    session: &SessionContext,
    db: Option<&Database>,
    scope: ShowScope,
    filter: Option<&ShowFilter>,
) -> Result<QueryResponse, QueryError> {
    // Route everything through the sysvar registry. The registry handles
    // session-vs-global overlay correctly and stays in sync with SET.
    let registry_scope = match scope {
        ShowScope::Global => crate::engine::sysvar::Scope::Global,
        ShowScope::Session => crate::engine::sysvar::Scope::Session,
    };
    let rows = crate::engine::sysvar::snapshot(registry_scope, Some(session), db);
    let mut result = variable_result(rows.into_iter().map(|r| (r.name, r.value)).collect());
    apply_show_filter(&mut result.rows, &result.columns, filter, "Variable_name")?;
    Ok(QueryResponse::ResultSet(result))
}

fn variable_result(rows: Vec<(&'static str, String)>) -> ResultSet {
    ResultSet {
        columns: vec![
            Column {
                source: Default::default(),
                name: "Variable_name".to_string(),
                column_type: ColumnType::VarString,
            },
            Column {
                source: crate::types::ColumnSource {
                    collation_id: Some(63),
                    ..Default::default()
                },
                name: "Value".to_string(),
                column_type: ColumnType::VarString,
            },
        ],
        rows: rows
            .into_iter()
            .map(|(name, value)| vec![Value::String(name.to_string()), Value::String(value)])
            .collect(),
    }
}

fn is_runtime_storage_set(upper: &str) -> bool {
    upper.starts_with("SET ")
        && ([
            "MYDB_BUFFER_POOL_PAGES",
            "MYDB_UNDO_RETENTION_MS",
            "MYDB_UNDO_SEGMENT_BYTES",
            "MYDB_UNDO_PURGE_INTERVAL_MS",
            "MYDB_UNDO_PURGE_BATCH_RECORDS",
            "MYDB_UNDO_TABLESPACE_MAX_BYTES",
            "MYDB_UNDO_RETENTION_GUARANTEE",
            "MYDB_CHECKPOINT_WAL_BYTES",
            "MYDB_MEMORY_TARGET",
            "MYDB_EXECUTOR_MEMORY_TARGET",
            "MYDB_QUERY_MEM_LIMIT_MB",
            "MYDB_PROCESS_MEM_LIMIT_MB",
            "MYDB_TEMP_DISK_LIMIT",
            "MYDB_TEMP_FILE_LIMIT_PER_QUERY",
            "MYDB_TEMP_DISK_RESERVE_PCT",
            "MYDB_WORK_AREA_OVERSUBSCRIBE_PCT",
            "MYDB_REPLICA_READ_ONLY",
            "MYDB_CURSOR_PER_SESSION_MAX",
            "MYDB_CURSOR_GLOBAL_MAX",
            "MYDB_CURSOR_MAX_IDLE_MS",
        ]
        .iter()
        .any(|name| upper.contains(name))
            || upper.contains("CBO_"))
}

pub(crate) fn show_status_result(
    session: &SessionContext,
    db: Option<&Database>,
    registry: Option<&crate::engine::session_registry::SessionRegistry>,
    scope: ShowScope,
    filter: Option<&ShowFilter>,
) -> Result<QueryResponse, QueryError> {
    // MySQL splits the status namespace by scope:
    //   * SESSION view — session-only counters plus all global status (the
    //     latter just mirror as in MySQL since these are process-global).
    //   * GLOBAL view — process-global counters. MySQL declares Ssl_cipher
    //     and Ssl_version SHOW_SCOPE_ALL, and their callbacks read this
    //     connection's TLS session even in GLOBAL output.
    if let Some(ShowFilter::Like(pattern)) = filter {
        if !pattern.bytes().any(|byte| matches!(byte, b'%' | b'_')) {
            let definition =
                crate::engine::status_counters::status_var_def(pattern).or_else(|| {
                    crate::engine::status_counters::status_var_defs()
                        .iter()
                        .find(|definition| definition.name.eq_ignore_ascii_case(pattern))
                });
            let Some(definition) = definition else {
                return Ok(QueryResponse::ResultSet(ResultSet {
                    columns: status_result_columns(),
                    rows: Vec::new(),
                }));
            };
            if matches!(
                definition.source,
                crate::engine::status_counters::StatusSourceGroup::Connection
                    | crate::engine::status_counters::StatusSourceGroup::Session
            ) {
                if let Some(value) = quick_status_value(session, registry, scope, definition.name) {
                    return Ok(QueryResponse::ResultSet(ResultSet {
                        columns: status_result_columns(),
                        rows: vec![vec![
                            Value::String(definition.name.to_string()),
                            Value::String(value),
                        ]],
                    }));
                }
            }
        }
    }
    let mut pairs = status_pairs(session, db, registry, scope);
    pairs.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    let mut rows = pairs
        .into_iter()
        .map(|(name, value)| vec![Value::String(name), Value::String(value)])
        .collect();
    let columns = status_result_columns();
    apply_show_filter(&mut rows, &columns, filter, "Variable_name")?;
    Ok(QueryResponse::ResultSet(ResultSet { columns, rows }))
}

fn quick_status_value(
    session: &SessionContext,
    registry: Option<&crate::engine::session_registry::SessionRegistry>,
    scope: ShowScope,
    name: &str,
) -> Option<String> {
    if name.eq_ignore_ascii_case("Ssl_cipher") {
        return Some(session.ssl_cipher.as_deref().unwrap_or("").to_string());
    }
    if name.eq_ignore_ascii_case("Ssl_version") {
        return Some(session.ssl_version.as_deref().unwrap_or("").to_string());
    }
    if matches!(scope, ShowScope::Session) {
        let snapshot = session.status.as_ref().map(|status| status.snapshot());
        if let Some(value) = snapshot
            .as_ref()
            .and_then(|status| status.session_counter_by_name(name))
            .or_else(|| {
                snapshot
                    .as_ref()
                    .and_then(|status| status.com_counter_by_name(name))
            })
        {
            return Some(value.to_string());
        }
    }

    let conn = crate::mysql::connection_manager::connection_metrics();
    let aggregated = registry.map(|registry| registry.global_session_status());
    let value = match name {
        "Threads_connected" => conn.active,
        "Connections" => conn.accepted,
        "Aborted_connects" => conn.handshake_failures,
        "Aborted_clients" => conn.aborted_clients,
        "Threads_created" => conn.threads_created,
        "Threads_running" => registry
            .map(|registry| registry.running_foreground_count())
            .unwrap_or(conn.active),
        "Threads_cached" => 0,
        "Max_used_connections" => conn.max_used_connections,
        "Connection_errors_max_connections" => conn.connection_errors_max_connections,
        "Prepared_stmt_count" => crate::engine::status_counters::prepared_stmt_count(),
        "Uptime" => crate::engine::status_counters::uptime_seconds(),
        "Uptime_since_flush_status" => {
            crate::engine::status_counters::uptime_since_flush_status_seconds()
        }
        "Questions" => aggregated
            .as_ref()
            .map(|status| status.value(crate::engine::status_counters::SessionCounter::Questions))
            .unwrap_or(0),
        "Queries" => conn.queries_executed,
        "Bytes_received" => crate::engine::status_counters::bytes_received(),
        "Bytes_sent" => crate::engine::status_counters::bytes_sent(),
        "Handler_commit" => crate::engine::status_counters::handler_commit(),
        "Handler_rollback" => crate::engine::status_counters::handler_rollback(),
        "Handler_read_rnd_next" => crate::engine::status_counters::handler_read_rnd_next(),
        "Slow_queries" => crate::engine::status_counters::slow_queries(),
        "Created_tmp_files" => crate::engine::status_counters::created_tmp_files(),
        "Created_tmp_tables" => crate::engine::status_counters::created_tmp_tables(),
        "Created_tmp_disk_tables" => crate::engine::status_counters::created_tmp_disk_tables(),
        "Sort_merge_passes" => crate::engine::status_counters::sort_merge_passes(),
        _ => {
            return aggregated
                .as_ref()
                .and_then(|status| {
                    status
                        .session_counter_by_name(name)
                        .or_else(|| status.com_counter_by_name(name))
                })
                .or_else(|| crate::engine::status_counters::com_counter_by_name(name))
                .map(|value| value.to_string());
        }
    };
    Some(value.to_string())
}

pub(crate) fn status_pairs(
    session: &SessionContext,
    db: Option<&Database>,
    registry: Option<&crate::engine::session_registry::SessionRegistry>,
    scope: ShowScope,
) -> Vec<(String, String)> {
    let mut pairs = db
        .map(|db| crate::engine::perf_schema::global_status_pairs(db, registry))
        .unwrap_or_default();
    let mut overrides = std::collections::HashMap::<String, String>::new();
    overrides.insert(
        "Ssl_cipher".to_string(),
        session.ssl_cipher.as_deref().unwrap_or("").to_string(),
    );
    overrides.insert(
        "Ssl_version".to_string(),
        session.ssl_version.as_deref().unwrap_or("").to_string(),
    );
    if !matches!(scope, ShowScope::Session) {
        pairs.extend(overrides);
        return pairs;
    }
    if let Some(status) = &session.status {
        let snapshot = status.snapshot();
        for (name, value) in snapshot.session_pfs_counters() {
            overrides.insert(name.to_string(), value.to_string());
        }
        for (name, value) in snapshot.com_counters() {
            overrides.insert(name.to_string(), value.to_string());
        }
    }
    for (name, value) in &mut pairs {
        if let Some(session_value) = overrides.remove(name) {
            *value = session_value;
        }
    }
    pairs.extend(overrides);
    pairs
}

pub(crate) fn status_result_columns() -> Vec<Column> {
    vec![
        Column {
            source: Default::default(),
            name: "Variable_name".to_string(),
            column_type: ColumnType::VarString,
        },
        Column {
            source: Default::default(),
            name: "Value".to_string(),
            column_type: ColumnType::VarString,
        },
    ]
}

pub(crate) fn show_collation_result(
    filter: Option<&ShowFilter>,
) -> Result<QueryResponse, QueryError> {
    let columns = vec![
        Column {
            source: Default::default(),
            name: "Collation".to_string(),
            column_type: ColumnType::VarString,
        },
        Column {
            source: Default::default(),
            name: "Charset".to_string(),
            column_type: ColumnType::VarString,
        },
        Column {
            source: Default::default(),
            name: "Id".to_string(),
            column_type: ColumnType::Long,
        },
        Column {
            source: Default::default(),
            name: "Default".to_string(),
            column_type: ColumnType::VarString,
        },
        Column {
            source: Default::default(),
            name: "Compiled".to_string(),
            column_type: ColumnType::VarString,
        },
        Column {
            source: Default::default(),
            name: "Sortlen".to_string(),
            column_type: ColumnType::Long,
        },
        Column {
            source: Default::default(),
            name: "Pad_attribute".to_string(),
            column_type: ColumnType::VarString,
        },
    ];
    let mut rows = crate::charset::collations_rows();
    apply_show_filter(&mut rows, &columns, filter, "Collation")?;
    Ok(QueryResponse::ResultSet(ResultSet { columns, rows }))
}

#[cfg(test)]
mod normalize_tests {
    use super::normalize_sql;

    #[test]
    fn lowercases_keywords_outside_quotes() {
        assert_eq!(
            normalize_sql("SELECT a FROM t WHERE id = 1"),
            normalize_sql("select a from t where id = 1")
        );
    }

    #[test]
    fn collapses_whitespace_and_comments() {
        assert_eq!(
            normalize_sql("/* comment */  SELECT a   FROM   t  ;"),
            normalize_sql("SELECT a FROM t")
        );
    }

    #[test]
    fn normalization_preserves_comment_semantics() {
        assert_eq!(
            normalize_sql("SELECT a/* ordinary */FROM t"),
            normalize_sql("SELECT a FROM t")
        );
        assert_ne!(
            normalize_sql("SELECT 1 -- comment\n + 2"),
            normalize_sql("SELECT 1 -- comment + 2")
        );
        assert_ne!(
            normalize_sql("SELECT /*+ NO_INDEX(t) */ * FROM t"),
            normalize_sql("SELECT * FROM t")
        );
        assert_eq!(
            normalize_sql("SELECT /*!80000 SQL_NO_CACHE */ * FROM t"),
            normalize_sql("SELECT SQL_NO_CACHE * FROM t")
        );
        assert_eq!(
            normalize_sql("SELECT /*!99999 SQL_NO_CACHE */ * FROM t"),
            normalize_sql("SELECT * FROM t")
        );
        assert!(normalize_sql("SELECT 1--2").contains("--2"));
    }

    #[test]
    fn preserves_quoted_string_literal_case() {
        assert_ne!(
            normalize_sql("SELECT 'Hello' FROM t"),
            normalize_sql("SELECT 'hello' FROM t")
        );
    }

    #[test]
    fn preserves_backtick_identifier_case() {
        // Backtick-quoted identifiers stay byte-identical so users
        // who explicitly chose case have it respected by the cache.
        assert_ne!(
            normalize_sql("SELECT `Foo` FROM t"),
            normalize_sql("SELECT `foo` FROM t")
        );
    }

    #[test]
    fn handles_escaped_quote_inside_string() {
        let a = normalize_sql("SELECT 'It''s' FROM t");
        let b = normalize_sql("select 'It''s' from t");
        assert_eq!(a, b);
        assert!(a.contains("'It''s'"));
    }

    #[test]
    fn handles_backslash_escape_inside_string() {
        // Backslash escapes the next byte verbatim, including a quote.
        // The byte after `\` must NOT be treated as terminator.
        let s = normalize_sql(r"SELECT 'a\'b' FROM t WHERE x = 1");
        assert!(s.contains(r"'a\'b'"));
    }

    #[test]
    fn handles_double_quoted_string_with_backslash() {
        let s = normalize_sql(r#"SELECT "He\\"o" FROM t"#);
        assert!(s.contains(r#""He\\"o""#) || s.contains(r#""He\"#));
    }

    #[test]
    fn handles_long_quoted_payload_unchanged() {
        // sysbench-shape: long single-quoted body with many bytes that
        // are neither quote nor backslash. memchr2 should skip these
        // bytes verbatim.
        let body = "a".repeat(2048);
        let sql = format!("INSERT INTO t VALUES('{body}')");
        let out = normalize_sql(&sql);
        assert!(out.contains(&format!("'{body}'")));
    }

    #[test]
    fn preserves_utf8_inside_quotes() {
        // Multi-byte UTF-8 bytes (>=0x80) must be copied verbatim
        // inside a quoted region; lowercase coercion only applies to
        // ASCII letters outside quotes.
        let sql = "SELECT '你好' FROM t";
        let out = normalize_sql(sql);
        assert!(out.contains("'你好'"));
    }

    #[test]
    fn preserves_utf8_outside_quotes_as_identifier() {
        // Non-ASCII bytes outside quotes should pass through unchanged
        // (`to_ascii_lowercase` is identity for bytes >= 0x80).
        let sql = "SELECT \u{4F60}\u{597D} FROM t";
        let out = normalize_sql(sql);
        assert!(out.contains("\u{4F60}\u{597D}"));
    }

    #[test]
    fn handles_unterminated_quote() {
        // Defensive: an unterminated literal should not panic or hang.
        let sql = "SELECT 'abc";
        let _ = normalize_sql(sql);
    }
}
