//! Catalog-aware conservative dependency hints for scheduling and redaction.
use crate::storage::Database;
use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, OnceLock, RwLock},
};
struct Entry {
    epoch: u64,
    names: Arc<BTreeSet<String>>,
    trigger_tables: Arc<BTreeSet<String>>,
    owner: std::sync::Weak<Database>,
}
static CACHE: OnceLock<RwLock<HashMap<usize, Entry>>> = OnceLock::new();
fn cache() -> &'static RwLock<HashMap<usize, Entry>> {
    CACHE.get_or_init(Default::default)
}
fn contains(sql: &str, names: &BTreeSet<String>) -> bool {
    if names.is_empty() {
        return false;
    }
    // False positives inside literals are deliberate: they redact more and
    // route one query away from the ordinary worker pool without executing it.
    if sql
        .split(|c: char| !(c.is_alphanumeric() || c == '_' || c == '$'))
        .any(|word| names.contains(&word.to_ascii_lowercase()))
    {
        return true;
    }
    // Quoted identifiers can contain spaces, punctuation, and escaped quotes.
    // Search their complete spellings conservatively, including SQL escapes.
    names
        .iter()
        .filter(|name| {
            name.chars()
                .any(|c| !(c.is_alphanumeric() || c == '_' || c == '$'))
        })
        .any(|name| {
            [
                name.clone(),
                name.replace('`', "``"),
                name.replace('"', "\"\""),
            ]
            .iter()
            .any(|spelling| {
                !spelling.is_empty()
                    && sql
                        .as_bytes()
                        .windows(spelling.len())
                        .any(|w| w.eq_ignore_ascii_case(spelling.as_bytes()))
            })
        })
}
fn has_keyword(sql: &str, keywords: &[&str]) -> bool {
    use crate::sql_lex::{segments, SqlLexMode, SqlSegmentKind};
    // Scheduling runs before a parser-mode guard is installed. Inspect both
    // escape interpretations conservatively, without treating quoted text or
    // ordinary comments as executable DML.
    for no_backslash_escapes in [false, true] {
        let mode = SqlLexMode {
            no_backslash_escapes,
            ..SqlLexMode::default()
        };
        for segment in segments(sql, mode, u32::MAX) {
            let segment = match segment {
                Ok(s) => s,
                Err(_) => return true,
            };
            let range = match segment.kind {
                SqlSegmentKind::Code => segment.range,
                SqlSegmentKind::ExecutableComment { body, .. } => body,
                _ => continue,
            };
            if sql[range]
                .split(|c: char| !(c.is_alphanumeric() || c == '_'))
                .any(|w| keywords.iter().any(|k| w.eq_ignore_ascii_case(k)))
            {
                return true;
            }
        }
    }
    false
}
fn may_write(sql: &str) -> bool {
    has_keyword(sql, &["INSERT", "UPDATE", "DELETE", "REPLACE", "LOAD"])
}
fn matches(sql: &str, names: &BTreeSet<String>, tables: &BTreeSet<String>) -> bool {
    contains(sql, names) || (!tables.is_empty() && contains(sql, tables) && may_write(sql))
}
pub(crate) fn sensitive(sql: &str) -> bool {
    if super::mentions(sql) {
        return true;
    }
    cache()
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .values()
        .any(|entry| matches(sql, &entry.names, &entry.trigger_tables))
}
pub(crate) fn may_call(db: &Arc<Database>, sql: &str) -> bool {
    let key = Arc::as_ptr(db) as usize;
    let epoch = db.catalog_publication_epoch();
    if let Some(entry) = cache()
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .get(&key)
        .filter(|e| e.epoch == epoch)
    {
        return super::mentions(sql) || matches(sql, &entry.names, &entry.trigger_tables);
    }
    let (epoch, sources) = db.http_dependency_sources();
    let mut names = BTreeSet::new();
    let mut trigger_tables = BTreeSet::new();
    loop {
        let before = names.len() + trigger_tables.len();
        for (name, body, is_trigger) in &sources {
            let dynamic = has_keyword(body, &["EXECUTE"]);
            if super::mentions(body) || dynamic || matches(body, &names, &trigger_tables) {
                if *is_trigger {
                    trigger_tables.insert(name.to_ascii_lowercase());
                } else {
                    names.insert(name.to_ascii_lowercase());
                }
            }
        }
        if names.len() + trigger_tables.len() == before {
            break;
        }
    }
    let result = super::mentions(sql) || matches(sql, &names, &trigger_tables);
    let mut cache = cache().write().unwrap_or_else(|e| e.into_inner());
    // A database's shutdown removes its entry. Avoid unbounded metadata from
    // embedding applications that open many short-lived engines.
    if cache.len() >= 1024 && !cache.contains_key(&key) {
        cache.retain(|_, e| e.owner.strong_count() > 0);
    }
    if cache.len() >= 1024 && !cache.contains_key(&key) {
        return true;
    }
    cache.insert(
        key,
        Entry {
            epoch,
            names: Arc::new(names),
            trigger_tables: Arc::new(trigger_tables),
            owner: Arc::downgrade(db),
        },
    );
    result
}
pub(crate) fn retire(db: &Database) {
    cache()
        .write()
        .unwrap_or_else(|e| e.into_inner())
        .remove(&(db as *const Database as usize));
}

/// SQL-level EXECUTE names a connection-owned template, unlike binary EXECUTE
/// which already supplies the original SQL to the protocol dispatcher.
pub(crate) fn bound_sql<'a>(
    session: &'a crate::engine::api::SessionContext,
    sql: &'a str,
) -> &'a str {
    use crate::sql_lex::{PreparedSql, SqlLexMode, SqlTokenCursor, SqlTokenKind};
    if session.sql_prepared_statements.is_empty() {
        return sql;
    }
    let bytes = sql.as_bytes();
    if !memchr::memchr2_iter(b'e', b'E', bytes).any(|i| {
        bytes
            .get(i..i + 7)
            .is_some_and(|s| s.eq_ignore_ascii_case(b"EXECUTE"))
    }) {
        return sql;
    }
    let mode = SqlLexMode::from_parser_bits(crate::engine::api::parse_temporal_sql_mode_bits(
        &session.sql_mode,
    ));
    let Ok(effective) = PreparedSql::new(sql, mode, crate::parser::MYSQL_VERSION_ID) else {
        return sql;
    };
    let mut tokens =
        SqlTokenCursor::new(effective.effective(), mode, crate::parser::MYSQL_VERSION_ID);
    if tokens.eat_keyword(b"EXECUTE") != Ok(true) {
        return sql;
    }
    let Ok(Some(token)) = tokens.next() else {
        return sql;
    };
    let name = match token.kind {
        SqlTokenKind::Word => std::borrow::Cow::Borrowed(tokens.token_text(&token)),
        SqlTokenKind::QuotedIdentifier => match tokens.decoded_quoted_text(&token) {
            Some(s) => std::borrow::Cow::Owned(s),
            None => return sql,
        },
        _ => return sql,
    };
    session
        .sql_prepared_statements
        .get(name.as_ref())
        .or_else(|| {
            session
                .sql_prepared_statements
                .get(&name.to_ascii_lowercase())
        })
        .map(|p| p.original_sql.as_ref())
        .unwrap_or(sql)
}
