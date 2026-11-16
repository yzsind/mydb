//! Syntax and result metadata for MySQL replication administration.
use crate::storage::error;
use crate::types::{Column, ColumnType, QueryError};

/// SHOW response kind shared by replication execution and PREPARE.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplicationShow {
    Status { legacy: bool },
    RelaylogEvents,
}

impl ReplicationShow {
    pub(crate) fn classify(sql: &str) -> Option<Self> {
        let sql = sql.trim().trim_end_matches(';').trim();
        let starts = |prefix: &str| {
            sql.as_bytes()
                .get(..prefix.len())
                .is_some_and(|head| head.eq_ignore_ascii_case(prefix.as_bytes()))
        };
        if starts("SHOW REPLICA STATUS") {
            Some(Self::Status { legacy: false })
        } else if starts("SHOW SLAVE STATUS") {
            Some(Self::Status { legacy: true })
        } else if starts("SHOW RELAYLOG EVENTS") {
            Some(Self::RelaylogEvents)
        } else {
            None
        }
    }

    pub(crate) fn columns(self) -> Vec<Column> {
        match self {
            Self::Status { legacy } => crate::replication_types::ReplicationStatus::columns(legacy),
            Self::RelaylogEvents => show_relaylog_events_columns(),
        }
    }

    pub(crate) fn warn_if_legacy(self) {
        if self == (Self::Status { legacy: true }) {
            crate::engine::api::push_statement_warning(
                crate::error::ConditionLevel::Warning,
                crate::error::catalog::ER_WARN_DEPRECATED_SYNTAX,
                "'SHOW SLAVE STATUS' is deprecated and will be removed in a future release. Please use SHOW REPLICA STATUS instead",
            );
        }
    }
}

/// Binding only: no channel lookup, command execution or warning emission.
pub(crate) fn admin_result_columns(sql: &str) -> Vec<Column> {
    ReplicationShow::classify(sql)
        .map(ReplicationShow::columns)
        .unwrap_or_default()
}

#[derive(Debug)]
pub(crate) struct ShowRelaylogEventsRequest {
    pub(crate) log_name: Option<String>,
    pub(crate) from: u64,
    pub(crate) offset: usize,
    pub(crate) limit: Option<usize>,
    pub(crate) channel: Option<String>,
}

pub(crate) fn parse_show_relaylog_events_clause(
    sql: &str,
) -> Result<ShowRelaylogEventsRequest, QueryError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("SHOW RELAYLOG EVENTS") {
        return Err(error(
            crate::error::catalog::ER_PARSE_ERROR,
            "expected SHOW RELAYLOG EVENTS prefix",
        ));
    }
    let mut req = ShowRelaylogEventsRequest {
        log_name: None,
        from: 0,
        offset: 0,
        limit: None,
        channel: None,
    };
    let mut tail = trimmed["SHOW RELAYLOG EVENTS".len()..].trim();

    // FOR CHANNEL '<name>' may appear at the end — strip it first so the
    // optional clauses below get a clean tail.
    let upper_tail = tail.to_ascii_uppercase();
    if let Some(pos) = upper_tail.rfind("FOR CHANNEL") {
        let value_part = tail[pos + "FOR CHANNEL".len()..].trim();
        let (quoted, after) = extract_quoted_string(value_part)?;
        if !after.trim().is_empty() {
            return Err(error(
                crate::error::catalog::ER_PARSE_ERROR,
                "extra tokens after FOR CHANNEL clause",
            ));
        }
        req.channel = Some(quoted);
        tail = tail[..pos].trim();
    }

    let upper_tail = tail.to_ascii_uppercase();
    if let Some(rest) = upper_tail.strip_prefix("IN ") {
        let pos_after_in = tail.len() - rest.len();
        let value_part = tail[pos_after_in..].trim();
        let (quoted, after) = extract_quoted_string(value_part)?;
        req.log_name = Some(quoted);
        tail = after.trim();
    }
    let upper_tail = tail.to_ascii_uppercase();
    if let Some(rest) = upper_tail.strip_prefix("FROM ") {
        let pos_after_from = tail.len() - rest.len();
        let value_part = tail[pos_after_from..].trim();
        let (num, after) = take_number_u64(value_part)?;
        req.from = num;
        tail = after.trim();
    }
    let upper_tail = tail.to_ascii_uppercase();
    if let Some(rest) = upper_tail.strip_prefix("LIMIT ") {
        let pos_after_limit = tail.len() - rest.len();
        let value_part = tail[pos_after_limit..].trim();
        let (first, after) = take_number_u64(value_part)?;
        let after = after.trim();
        if let Some(after_comma) = after.strip_prefix(',') {
            let (second, after2) = take_number_u64(after_comma.trim())?;
            req.offset = first as usize;
            req.limit = Some(second as usize);
            tail = after2.trim();
        } else {
            req.limit = Some(first as usize);
            tail = after;
        }
    }
    if !tail.is_empty() {
        return Err(error(
            crate::error::catalog::ER_PARSE_ERROR,
            format!("unexpected tail in SHOW RELAYLOG EVENTS: {tail}"),
        ));
    }
    Ok(req)
}

pub(crate) fn extract_quoted_string(input: &str) -> Result<(String, &str), QueryError> {
    let bytes = input.as_bytes();
    if bytes.is_empty() {
        return Err(error(
            crate::error::catalog::ER_PARSE_ERROR,
            "expected quoted string",
        ));
    }
    let quote = bytes[0];
    if quote != b'\'' && quote != b'"' && quote != b'`' {
        return Err(error(
            crate::error::catalog::ER_PARSE_ERROR,
            format!(
                "expected quote character, got '{}'",
                input.chars().next().unwrap_or(' ')
            ),
        ));
    }
    let mut out = String::new();
    let mut chars = input[1..].char_indices().peekable();
    while let Some((i, ch)) = chars.next() {
        if ch == quote as char {
            if chars.peek().is_some_and(|(_, next)| *next == ch) {
                chars.next();
                out.push(ch);
                continue;
            }
            return Ok((out, &input[i + 2..]));
        }
        out.push(ch);
    }
    Err(error(
        crate::error::catalog::ER_PARSE_ERROR,
        "unterminated quoted string",
    ))
}

pub(crate) fn take_number_u64(input: &str) -> Result<(u64, &str), QueryError> {
    let mut end = 0;
    for (idx, ch) in input.char_indices() {
        if ch.is_ascii_digit() {
            end = idx + ch.len_utf8();
        } else {
            break;
        }
    }
    if end == 0 {
        return Err(error(
            crate::error::catalog::ER_PARSE_ERROR,
            "expected unsigned integer",
        ));
    }
    let n: u64 = input[..end].parse().map_err(|_| {
        error(
            crate::error::catalog::ER_PARSE_ERROR,
            "integer parse failed",
        )
    })?;
    Ok((n, &input[end..]))
}

pub(crate) fn show_relaylog_events_columns() -> Vec<Column> {
    [
        ("Log_name", ColumnType::VarString),
        ("Pos", ColumnType::LongLong),
        ("Event_type", ColumnType::VarString),
        ("Server_id", ColumnType::Long),
        ("End_log_pos", ColumnType::LongLong),
        ("Info", ColumnType::VarString),
    ]
    .into_iter()
    .map(|(name, column_type)| Column {
        source: Default::default(),
        name: name.to_string(),
        column_type,
    })
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn show_classification_and_columns_cover_aliases_without_binding_side_effects() {
        for (sql, expected) in [
            (
                "SHOW REPLICA STATUS",
                Some(ReplicationShow::Status { legacy: false }),
            ),
            (
                " show replica status for channel '副本'; ",
                Some(ReplicationShow::Status { legacy: false }),
            ),
            (
                "SHOW SLAVE STATUS",
                Some(ReplicationShow::Status { legacy: true }),
            ),
            (
                "show relaylog events limit 0",
                Some(ReplicationShow::RelaylogEvents),
            ),
            ("SHOW REPLICAS", None),
            ("SHOW MASTER STATUS", None),
            ("SELECT 1", None),
        ] {
            assert_eq!(ReplicationShow::classify(sql), expected, "{sql}");
        }
        let modern = crate::replication_types::ReplicationStatus::columns(false);
        let legacy = crate::replication_types::ReplicationStatus::columns(true);
        assert_eq!(
            legacy,
            crate::replication_columns::to_legacy_slave_status_columns(modern.clone())
        );
        assert_eq!(admin_result_columns("SHOW REPLICA STATUS"), modern);
        assert_eq!(admin_result_columns("SHOW SLAVE STATUS"), legacy);
        assert_eq!(
            admin_result_columns("SHOW RELAYLOG EVENTS"),
            show_relaylog_events_columns()
        );
        crate::engine::api::take_statement_warnings();
        admin_result_columns("SHOW SLAVE STATUS");
        assert!(crate::engine::api::take_statement_warnings().is_empty());
        ReplicationShow::Status { legacy: true }.warn_if_legacy();
        assert_eq!(crate::engine::api::take_statement_warnings().len(), 1);
        for sql in [
            "START REPLICA",
            "STOP SLAVE",
            "RESET REPLICA ALL",
            "CHANGE MASTER TO MASTER_HOST='host'",
        ] {
            assert!(admin_result_columns(sql).is_empty());
        }
    }

    #[test]
    fn relaylog_clauses_keep_unicode_quotes_offsets_and_errors() {
        let r = parse_show_relaylog_events_clause(
            "SHOW RELAYLOG EVENTS IN '日志''一' FROM 12 LIMIT 2, 7 FOR CHANNEL '副本';",
        )
        .unwrap();
        assert_eq!(r.log_name.as_deref(), Some("日志'一"));
        assert_eq!((r.from, r.offset, r.limit), (12, 2, Some(7)));
        assert_eq!(r.channel.as_deref(), Some("副本"));
        for sql in [
            "SHOW RELAYLOG EVENTS FROM -1",
            "SHOW RELAYLOG EVENTS LIMIT 1,",
            "SHOW RELAYLOG EVENTS FOR CHANNEL 'x' garbage",
            "SHOW RELAYLOG EVENTS IN 'unclosed",
        ] {
            assert_eq!(
                parse_show_relaylog_events_clause(sql).unwrap_err().code(),
                1064,
                "{sql}"
            );
        }
    }
}
