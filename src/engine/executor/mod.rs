use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use crate::parser::{
    parse_routine_body_sql, parse_statement, parse_statements, AlterAction, AlterAlgorithm,
    BinaryOp, CreateIndex, Delete, Expr, Insert, InsertSource, JsonTableColumnDef,
    JsonTableOnResponse, JsonTableSource, LimitValue, Literal, LockTableMode, LockTableSpec,
    OrderBy, QuantifiedComparisonRight, RoutineKind, RoutineStmt, Select, SelectItem, SetOp,
    SetOperation, Statement, SubQuery, TableRef, UnaryOp, Update, WindowFrame, WindowFrameBound,
    WindowFrameExclusion, WindowFrameUnits, WithCte,
};
use crate::planner::{self, BuildSide, PhysicalPlan, PlanTable};
use crate::storage::{
    error, is_system_database, split_table_name, value_to_string, DataTypeKind, Database,
    IndexBound, IndexKey, IndexSchema, IsolationLevel, KeyPart, MdlDuration, MdlLockType,
    MdlObjectKey, MdlObjectType, MdlRequest, RowId, TableSchema, TriggerEventSchema,
    TriggerTimingSchema, TxnView, DEFAULT_DATABASE,
};
#[cfg(test)]
use crate::storage::{DurabilityMode, StorageConfig};
use crate::types::{Column, ColumnType, QueryError, Value};
use crate::util::logging::log_message;

use super::api::{
    QueryEngine, QueryResponse, ResultCursor, ResultSet, SessionContext, SessionTableLock,
    StreamingResultSet,
};
use super::auth::{
    access_denied, current_time_ms, verify_auth_response, verify_cached_caching_sha2,
    verify_plaintext_auth_string, AccountKey, AuthOutcome, AuthRequest, AuthorizationContext,
    SslRequirement,
};
use super::expr::{
    compare_values, const_expr_type, eval_arithmetic, eval_bool, eval_case, eval_expr,
    eval_function, expr_column_index, expr_column_name, expr_literal_value, expr_name,
    expr_result_type, flip_cmp, int_value, literal_value, numeric, truthy, uint_value, ValueKey,
};
#[cfg(test)]
use super::logging::truncate_sql_for_log;
use super::logging::{redact_sql_for_log, QueryLogConfig, QueryLogMetadata, SqlLogService};
use super::session_registry::SessionRegistry;
use super::show::{
    col, describe_show_columns, describe_show_create_table, describe_show_index, describe_table,
    show_charset_result, show_columns, show_create_database, show_create_event,
    show_create_routine, show_create_table, show_create_trigger, show_create_user_result,
    show_databases, show_engine_mydb_status, show_engine_mydb_status_columns, show_engine_result,
    show_engines_result, show_events, show_grants_result, show_index, show_open_tables,
    show_plugins_result, show_privileges_result, show_processlist_result, show_routine_status,
    show_table_status, show_table_status_columns, show_tables, show_tables_columns, show_triggers,
};
use super::system::{
    normalize_sql, preserves_statement_diagnostics, show_collation_result, show_count_result,
    show_diagnostics_result, show_status_result, show_variables_result, StubQueryEngine,
};

#[macro_use]
mod row_decode_macros;
mod auth_recipe;
mod compiled_select;
pub(crate) mod cte;
pub(crate) mod cte_store;
mod ddl_deprecation;
mod dispatch;
pub(crate) mod row_spool;
mod stream_mdl;
pub(crate) use dispatch::{
    SETOP_SPILL_BYTES_READ, SETOP_SPILL_BYTES_WRITTEN, SETOP_SPILL_COUNT, SETOP_SPILL_FILE_OPENS,
    SETOP_SPILL_MAX_DEPTH, SETOP_SPILL_MERGE_PASSES, SETOP_SPILL_PARTITIONS,
    SETOP_SPILL_REPARTITIONS,
};
mod dml;
pub(crate) mod events;
#[path = "aggregate/mod.rs"]
pub(crate) mod hash_aggregate;
mod indexes;
pub(crate) mod insert;
mod load_data;
mod locking_join;
mod login;
pub(crate) mod mem_budget;
pub(crate) mod memory_footprint;
pub(crate) mod memory_grant;
mod multi_table;
pub(crate) mod partition_clause;
pub(crate) mod partition_router;
mod prepared_dml;
pub(super) mod qualification;
mod returning;
pub(crate) mod routines;
pub(crate) mod select;
mod select_into_file;
pub(crate) mod sort;
pub(crate) mod spill_hint;
pub(crate) mod spill_quota;
mod spill_value;
pub(crate) mod temporal;
mod triggers;
pub(crate) mod tuning;
mod tx;
pub(crate) mod vec;
pub(crate) mod view_normalize;
pub(crate) mod work_area;

pub(crate) fn col_meta_intern_pool_counters() -> (u64, u64) {
    select::plan::col_meta_intern_pool_counters()
}

pub(crate) fn col_meta_intern_stats() -> (u64, u64) {
    select::plan::col_meta_intern_stats()
}

pub(crate) fn col_meta_intern_capacity() -> usize {
    select::plan::col_meta_intern_capacity()
}

pub(crate) fn col_meta_intern_fallbacks() -> u64 {
    select::plan::col_meta_intern_fallbacks()
}

use self::dispatch::*;
use self::dml::*;
use self::events::*;
use self::indexes::*;
use self::insert::*;
use self::load_data::*;
use self::qualification::{qualify_statement_tables, qualify_table_name};
use self::routines::*;
use self::select::*;
use self::select_into_file::*;
use self::triggers::*;
use self::tx::*;

fn begin_statement_diagnostics(session: &mut SessionContext, sql: &str) -> bool {
    let preserve_warnings = preserves_statement_diagnostics(sql);
    crate::engine::perf_events::clear_thread_wait_samples();
    crate::engine::perf_events::start_statement_rows_examined();
    crate::engine::api::clear_statement_warnings();
    // Reset the per-statement NOW() freeze (B-24) so every top-level
    // statement re-samples the wall clock exactly once, and every
    // NOW()/CURRENT_TIMESTAMP/UNIX_TIMESTAMP call within it returns the
    // same value — matching MySQL 8.0's documented behaviour.
    crate::engine::api::clear_statement_now_freeze();
    // Reset per-statement `RAND(N)` LCG state so `SELECT RAND(5)`
    // executed twice returns the same first value both times, matching
    // MySQL's documented seeded RAND repeatability.
    crate::engine::expr::clear_statement_rand_state();
    crate::engine::api::clear_statement_insert_id();
    crate::engine::api::clear_statement_matched_rows();
    let _ = crate::engine::api::take_statement_dml_candidate_rows();
    crate::engine::api::clear_statement_info();
    crate::engine::api::clear_insert_duplicates();
    crate::engine::api::clear_statement_odku_stats();
    session.statement_insert_id = 0;
    crate::engine::perf_events::begin_statement(session, sql);
    if !preserve_warnings {
        session.clear_warnings();
    }
    // Performance Schema digest text is utf8mb3 even when the client uses
    // utf8mb4. Supplementary-plane source text therefore records
    // ER_INVALID_CHARACTER_STRING while the statement itself still executes.
    // Model that common ingestion boundary before parsing so DDL, DML,
    // prepared execution and expressions share the diagnostic.
    warn_statement_digest_charset_conversion(sql);
    // SELECT reads the previous diagnostic counters, then replaces the
    // diagnostics area at completion like other non-diagnostic statements.
    preserve_warnings
        && !crate::engine::system::references_diagnostics_count(
            &normalize_sql(sql).to_ascii_uppercase(),
        )
}

fn warn_statement_digest_charset_conversion(sql: &str) {
    // Performance Schema's digest text is utf8mb3, so only supplementary
    // Unicode scalars can fail this conversion. Almost all OLTP SQL is ASCII;
    // even localized identifiers normally stay in the BMP. Avoid constructing
    // sqlparser's token vector unless the source contains a scalar that could
    // actually trigger ER_INVALID_CHARACTER_STRING. We still tokenize the
    // uncommon candidate so a supplementary character in a string literal or
    // comment is not mistaken for an identifier.
    if !digest_text_may_contain_supplementary(sql) {
        return;
    }

    use sqlparser::dialect::MySqlDialect;
    use sqlparser::tokenizer::{Token, Tokenizer};

    let has_supplementary_identifier =
        Tokenizer::new(&MySqlDialect {}, sql)
            .tokenize()
            .is_ok_and(|tokens| {
                tokens.into_iter().any(|token| {
                    matches!(
                        token,
                        Token::Word(word)
                            if word.value.chars().any(|ch| u32::from(ch) > 0xFFFF)
                    )
                })
            });
    if has_supplementary_identifier {
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Warning,
            crate::error::catalog::ER_INVALID_CHARACTER_STRING,
            "Cannot convert string from utf8mb4 to utf8mb3",
        );
    }
}

#[inline]
fn digest_text_may_contain_supplementary(sql: &str) -> bool {
    !sql.is_ascii() && sql.chars().any(|ch| u32::from(ch) > 0xFFFF)
}

/// Emit source-syntax deprecation diagnostics that cannot be reconstructed
/// from the normalized internal AST. Integer display widths, `YEAR(4)`, and
/// FLOAT precision clauses are intentionally discarded by the type converter
/// because they do not affect storage; warnings still remain part of MySQL's
/// observable statement semantics.
///
/// Run this only after successful execution. A rejected DDL statement must not
/// leave warnings behind, and expression user-variable assignments are warned
/// once per assignment token (whereas `SET @v := ...` remains supported
/// syntax and is not deprecated).
pub(crate) fn warn_successful_source_syntax(sql: &str) {
    warn_successful_source_syntax_with_mode(sql, crate::parser::current_parser_sql_mode_bits());
}

pub(crate) fn warn_successful_source_syntax_with_mode(sql: &str, parser_mode_bits: u16) {
    // The diagnostics below apply only to DDL type spellings or to a small
    // set of lexical markers.  Tokenizing every successful statement made a
    // literal-heavy bulk INSERT pay a second full sqlparser pass after it had
    // already executed (and Performance Schema may do another for DIGEST).
    // Keep the tokenizer as the correctness oracle once a marker is present;
    // the cheap prefilter is deliberately allowed to return false positives.
    if !source_syntax_warning_scan_required(sql, parser_mode_bits) {
        return;
    }

    use sqlparser::dialect::MySqlDialect;
    use sqlparser::tokenizer::{Token, Tokenizer};

    if sql.contains("/*!") {
        let mode = crate::sql_lex::SqlLexMode::from_parser_bits(parser_mode_bits);
        for segment in
            crate::sql_lex::segments(sql, mode, crate::parser::MYSQL_VERSION_ID).flatten()
        {
            if let crate::sql_lex::SqlSegmentKind::ExecutableComment {
                gate: Some(_),
                body,
                ..
            } = segment.kind
            {
                if sql
                    .as_bytes()
                    .get(body.start)
                    .is_some_and(|byte| !byte.is_ascii_whitespace())
                {
                    crate::engine::api::push_static_statement_warning(
                        crate::error::ConditionLevel::Warning,
                        crate::error::catalog::ER_WARN_NO_SPACE_VERSION_COMMENT,
                        "Immediately starting the version comment after the version number is deprecated and may change behavior in a future release. Please insert a white-space character after the version number.",
                    );
                }
            }
        }
    }
    let Ok(tokens) = Tokenizer::new(&MySqlDialect {}, sql).tokenize() else {
        return;
    };
    let tokens: Vec<Token> = tokens
        .into_iter()
        .filter(|token| !matches!(token, Token::Whitespace(_) | Token::EOF))
        .collect();
    let word = |token: &Token, expected: &str| matches!(token, Token::Word(value) if value.value.eq_ignore_ascii_case(expected));
    let first_word_is = |expected: &str| tokens.first().is_some_and(|token| word(token, expected));
    let is_convert_type_name = |index: usize| {
        if !matches!(tokens.get(index.wrapping_sub(1)), Some(Token::Comma)) {
            return false;
        }
        let mut depth = 0usize;
        for previous in (0..index.saturating_sub(1)).rev() {
            match &tokens[previous] {
                Token::RParen => depth += 1,
                Token::LParen if depth > 0 => depth -= 1,
                Token::LParen => {
                    return previous
                        .checked_sub(1)
                        .and_then(|function| tokens.get(function))
                        .is_some_and(|token| word(token, "CONVERT"));
                }
                _ => {}
            }
        }
        false
    };

    let is_stored_routine_ddl = first_word_is("CREATE")
        && tokens
            .iter()
            .take(6)
            .any(|token| word(token, "FUNCTION") || word(token, "PROCEDURE"));
    // These warnings are observable in exact source order. Token::Double-
    // ExclamationMark represents one MySQL logical prefix (not two), while a
    // three-bang run tokenizes as DoubleExclamationMark + ExclamationMark.
    for token in &tokens {
        let message = match token {
            Token::Assignment if !first_word_is("SET") && !is_stored_routine_ddl => Some(
                "Setting user variables within expressions is deprecated and will be removed in a future release. Consider alternatives: 'SET variable=expression, ...', or 'SELECT expression(s) INTO variables(s)'.",
            ),
            Token::ExclamationMark | Token::DoubleExclamationMark => Some(
                "'!' is deprecated and will be removed in a future release. Please use NOT instead",
            ),
            Token::Overlap => Some(
                "'&&' is deprecated and will be removed in a future release. Please use AND instead",
            ),
            Token::StringConcat
                if parser_mode_bits & crate::engine::api::SQL_MODE_PIPES_AS_CONCAT == 0 => Some(
                "'|| as a synonym for OR' is deprecated and will be removed in a future release. Please use OR instead",
            ),
            _ => None,
        };
        if let Some(message) = message {
            crate::engine::api::push_statement_warning(
                crate::error::ConditionLevel::Warning,
                crate::error::catalog::ER_WARN_DEPRECATED_SYNTAX,
                message,
            );
        }
    }

    for (index, token) in tokens.iter().enumerate() {
        if !word(token, "BINARY") {
            continue;
        }
        let is_character_set_name =
            index >= 2 && word(&tokens[index - 1], "SET") && word(&tokens[index - 2], "CHARACTER");
        if is_character_set_name {
            continue;
        }
        let previous_opens_operand = match index.checked_sub(1).and_then(|i| tokens.get(i)) {
            None => true,
            Some(
                Token::LParen
                | Token::Comma
                | Token::Eq
                | Token::Neq
                | Token::Lt
                | Token::LtEq
                | Token::Gt
                | Token::GtEq
                | Token::Plus
                | Token::Minus
                | Token::Mul
                | Token::Div,
            ) => true,
            Some(previous) => [
                "SELECT", "WHERE", "AND", "OR", "XOR", "NOT", "WHEN", "THEN", "ELSE", "ON",
                "HAVING", "LIKE", "REGEXP", "RLIKE", "RETURN", "DISTINCT", "BETWEEN", "IN",
            ]
            .iter()
            .any(|keyword| word(previous, keyword)),
        };
        if previous_opens_operand && !is_convert_type_name(index) {
            crate::engine::api::push_statement_warning(
                crate::error::ConditionLevel::Warning,
                crate::error::catalog::ER_WARN_DEPRECATED_SYNTAX,
                "'BINARY expr' is deprecated; use CAST(expr AS BINARY) instead",
            );
        } else if (first_word_is("CREATE") || first_word_is("ALTER"))
            // `BINARY[(n)]` is a real binary-string data type.  Only the
            // legacy attribute attached to a character/ENUM/SET type is
            // deprecated.  In a length-bearing declaration the immediately
            // preceding token is `)`, while the no-length text forms retain
            // their type keyword immediately before `BINARY`.
            && !matches!(tokens.get(index + 1), Some(Token::LParen))
            && index.checked_sub(1).and_then(|i| tokens.get(i)).is_some_and(
                |previous| {
                    matches!(previous, Token::RParen)
                        || [
                            "CHAR",
                            "VARCHAR",
                            "TINYTEXT",
                            "TEXT",
                            "MEDIUMTEXT",
                            "LONGTEXT",
                            "ENUM",
                            "SET",
                        ]
                        .iter()
                        .any(|keyword| word(previous, keyword))
                },
            )
        {
            crate::engine::api::push_statement_warning(
                crate::error::ConditionLevel::Warning,
                crate::error::catalog::ER_WARN_DEPRECATED_SYNTAX,
                "'BINARY as attribute of a type' is deprecated; use a CHARACTER SET clause with _bin collation instead",
            );
        }
    }

    if !first_word_is("CREATE") && !first_word_is("ALTER") {
        if !first_word_is("SET") {
            for token in &tokens {
                let Token::Word(value) = token else {
                    continue;
                };
                if value
                    .value
                    .trim_start_matches('_')
                    .eq_ignore_ascii_case("utf8mb3")
                {
                    crate::engine::api::push_statement_warning(
                        crate::error::ConditionLevel::Warning,
                        crate::error::catalog::ER_WARN_DEPRECATED_SYNTAX,
                        "'utf8mb3' is deprecated; use utf8mb4 instead",
                    );
                } else if value.value.eq_ignore_ascii_case("macce") {
                    crate::engine::api::push_statement_warning(
                        crate::error::ConditionLevel::Warning,
                        crate::error::catalog::ER_WARN_DEPRECATED_SYNTAX,
                        "'macce' is deprecated; use utf8mb4 instead",
                    );
                }
            }
        }
        return;
    }
    let is_type_ddl = tokens
        .iter()
        .take(4)
        .any(|token| word(token, "TABLE") || word(token, "FUNCTION") || word(token, "PROCEDURE"));
    if !is_type_ddl {
        return;
    }

    let warning_superfluous_temporal_delimiter_count = tokens
        .iter()
        .enumerate()
        .filter(|(index, token)| {
            if !word(token, "DEFAULT") {
                return false;
            }
            let Some(Token::SingleQuotedString(value) | Token::DoubleQuotedString(value)) =
                tokens.get(index + 1)
            else {
                return false;
            };
            value.starts_with(char::is_whitespace)
                && tokens[..*index]
                    .iter()
                    .rev()
                    .take_while(|token| !matches!(token, Token::Comma | Token::LParen))
                    .any(|token| {
                        ["DATE", "TIME", "DATETIME", "TIMESTAMP"]
                            .iter()
                            .any(|name| word(token, name))
                    })
        })
        .count();

    fn matching_type_clause_end(tokens: &[Token], type_index: usize) -> usize {
        if !matches!(tokens.get(type_index + 1), Some(Token::LParen)) {
            return type_index;
        }
        let mut depth = 0usize;
        for (offset, token) in tokens[type_index + 1..].iter().enumerate() {
            match token {
                Token::LParen => depth += 1,
                Token::RParen => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        return type_index + 1 + offset;
                    }
                }
                _ => {}
            }
        }
        type_index
    }

    fn floating_type_has_scale(tokens: &[Token], type_index: usize) -> bool {
        let paren_owner = if matches!(tokens.get(type_index + 1), Some(Token::LParen)) {
            type_index
        } else if matches!(
            tokens.get(type_index + 1),
            Some(Token::Word(value)) if value.value.eq_ignore_ascii_case("PRECISION")
        ) && matches!(tokens.get(type_index + 2), Some(Token::LParen))
        {
            type_index + 1
        } else {
            return false;
        };
        let clause_end = matching_type_clause_end(tokens, paren_owner);
        tokens[paren_owner + 2..clause_end]
            .iter()
            .any(|token| matches!(token, Token::Comma))
    }

    let mut warning_1681_count = 0usize;
    let mut warning_year_count = 0usize;
    let mut warning_utf8mb3_count = 0usize;
    let mut warning_utf8mb3_collation_count = 0usize;
    let mut warning_3720_count = 0usize;
    let mut warning_3719_count = 0usize;
    for (index, token) in tokens.iter().enumerate() {
        let Token::Word(type_word) = token else {
            continue;
        };
        let type_name = type_word.value.to_ascii_uppercase();
        if type_name.trim_start_matches('_') == "UTF8MB3" {
            warning_utf8mb3_count += 1;
            if type_name
                .trim_start_matches('_')
                .to_ascii_lowercase()
                .ends_with("_bin")
            {
                warning_utf8mb3_collation_count += 1;
            }
            continue;
        }
        if type_name
            .trim_start_matches('_')
            .to_ascii_lowercase()
            .starts_with("utf8mb3_")
        {
            warning_utf8mb3_collation_count += 1;
            continue;
        }
        if matches!(type_name.as_str(), "NCHAR" | "NVARCHAR" | "NATIONAL") {
            warning_3720_count += 1;
            continue;
        }
        if type_name == "UTF8" {
            warning_3719_count += 1;
            continue;
        }
        let clause_end = matching_type_clause_end(&tokens, index);
        let has_parens = clause_end != index;
        match type_name.as_str() {
            "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "INTEGER" | "BIGINT" | "INT1"
            | "INT2" | "INT3" | "INT4" | "INT8" => {
                if has_parens {
                    warning_1681_count += 1;
                }
            }
            "FLOAT" | "DOUBLE" | "REAL" => {
                // FLOAT(p) is bit precision and is not deprecated. Only the
                // legacy fixed display form FLOAT(M,D)/DOUBLE(M,D), including
                // DOUBLE PRECISION(M,D), produces warning 1681.
                if floating_type_has_scale(&tokens, index) {
                    warning_1681_count += 1;
                }
                if tokens
                    .get(clause_end + 1)
                    .is_some_and(|next| word(next, "UNSIGNED"))
                {
                    warning_1681_count += 1;
                }
            }
            "DECIMAL" | "NUMERIC" => {
                if tokens
                    .get(clause_end + 1)
                    .is_some_and(|next| word(next, "UNSIGNED"))
                {
                    warning_1681_count += 1;
                }
            }
            "YEAR" => {
                if has_parens {
                    warning_year_count += 1;
                }
                if tokens
                    .get(clause_end + 1)
                    .is_some_and(|next| word(next, "UNSIGNED"))
                {
                    warning_1681_count += 1;
                }
            }
            "ZEROFILL" => warning_1681_count += 1,
            _ => {}
        }
    }

    for _ in 0..warning_1681_count {
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Warning,
            crate::error::catalog::ER_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT,
            "Deprecated numeric type declaration syntax",
        );
    }
    for _ in 0..warning_superfluous_temporal_delimiter_count {
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Warning,
            crate::error::catalog::ER_WARN_DEPRECATED_SUPERFLUOUS_DELIMITER,
            "Leading whitespace in a temporal default is superfluous and deprecated",
        );
    }
    for _ in 0..warning_year_count {
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Warning,
            crate::error::catalog::ER_WARN_DEPRECATED_SYNTAX,
            "The YEAR(4) data type is deprecated; use YEAR instead",
        );
    }
    for _ in 0..warning_utf8mb3_count {
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Warning,
            crate::error::catalog::ER_WARN_DEPRECATED_SYNTAX,
            "'utf8mb3' is deprecated and will be removed in a future release. Please use utf8mb4 instead",
        );
    }
    for _ in 0..warning_utf8mb3_collation_count {
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Warning,
            crate::error::catalog::ER_WARN_DEPRECATED_UTF8MB3_COLLATION,
            "utf8mb3 collation is deprecated; use an appropriate utf8mb4 collation instead",
        );
    }
    for _ in 0..warning_3720_count {
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Warning,
            crate::error::catalog::ER_DEPRECATED_NATIONAL,
            "NATIONAL/NCHAR/NVARCHAR implies deprecated character set utf8mb3",
        );
    }
    for _ in 0..warning_3719_count {
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Warning,
            crate::error::catalog::ER_DEPRECATED_UTF8_ALIAS,
            "The utf8 alias currently denotes utf8mb3; use utf8mb4 explicitly",
        );
    }
}

#[inline]
pub(crate) fn source_syntax_warning_scan_required(sql: &str, parser_mode_bits: u16) -> bool {
    fn starts_with_keyword(sql: &str, keyword: &[u8]) -> bool {
        let bytes = sql.trim_start().as_bytes();
        bytes.len() >= keyword.len()
            && bytes[..keyword.len()].eq_ignore_ascii_case(keyword)
            && !bytes
                .get(keyword.len())
                .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
    }

    let bytes = sql.as_bytes();
    // Comment-leading statements are uncommon and can hide the real first
    // keyword, so conservatively retain the tokenizer for them.
    let trimmed = sql.trim_start();
    let comment_leading =
        trimmed.starts_with("/*") || trimmed.starts_with("--") || trimmed.starts_with('#');
    if comment_leading || starts_with_keyword(sql, b"CREATE") || starts_with_keyword(sql, b"ALTER")
    {
        return true;
    }
    // One quote-aware pass. Bulk BLOB/VECTOR INSERTs can contain hundreds of
    // KiB of base64 or text literal bytes per statement. Searching that payload
    // four times for warning markers both wasted bandwidth and produced false
    // positives (e.g. "BINARY" inside user data) that triggered a full
    // sqlparser tokenization after successful execution. Skip quoted regions
    // with memchr and inspect only SQL syntax bytes.
    let word_at = |index: usize, word: &[u8]| {
        bytes[index..]
            .get(..word.len())
            .is_some_and(|candidate| candidate.eq_ignore_ascii_case(word))
    };
    let mut index = 0usize;
    while index < bytes.len() {
        match bytes[index] {
            quote @ (b'\'' | b'"' | b'`') => {
                index += 1;
                let backslash_escapes = quote != b'`'
                    && !(quote == b'"'
                        && parser_mode_bits & crate::engine::api::SQL_MODE_ANSI_QUOTES != 0)
                    && parser_mode_bits & crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES == 0;
                loop {
                    let Some(relative) = memchr::memchr2(quote, b'\\', &bytes[index..]) else {
                        return false;
                    };
                    index += relative;
                    if bytes[index] == b'\\' && backslash_escapes {
                        index = (index + 2).min(bytes.len());
                    } else if bytes[index] == b'\\' {
                        index += 1;
                    } else if bytes.get(index + 1) == Some(&quote) {
                        index += 2;
                    } else {
                        index += 1;
                        break;
                    }
                }
            }
            b':' if bytes.get(index + 1) == Some(&b'=') => return true,
            b'!' => return true,
            b'&' if bytes.get(index + 1) == Some(&b'&') => return true,
            b'|' if bytes.get(index + 1) == Some(&b'|') => return true,
            b'b' | b'B' | b'u' | b'U' | b'm' | b'M'
                if word_at(index, b"BINARY")
                    || word_at(index, b"UTF8MB3")
                    || word_at(index, b"MACCE") =>
            {
                return true;
            }
            _ => index += 1,
        }
    }
    false
}

#[cfg(test)]
mod source_syntax_warning_prefilter_tests {
    use super::source_syntax_warning_scan_required;

    fn scan(sql: &str) -> bool {
        source_syntax_warning_scan_required(sql, 0)
    }

    #[test]
    fn ordinary_bulk_insert_skips_source_warning_tokenizer() {
        assert!(!scan("INSERT INTO t(a,b) VALUES (1,'abc'),(2,'def')"));
        assert!(!scan(
            "INSERT INTO t VALUES (FROM_BASE64('QklOQVJZIT09JiY='))"
        ));
        assert!(!scan("INSERT INTO t VALUES ('BINARY ! && || := UTF8MB3')"));
    }

    #[test]
    fn every_source_warning_family_reaches_tokenizer() {
        for sql in [
            "CREATE TABLE t(a INT(11))",
            "ALTER TABLE t ADD COLUMN y YEAR(4)",
            "SELECT @v := 1",
            "SELECT !TRUE",
            "SELECT 1 && 1",
            "SELECT 1 || 0",
            "SELECT BINARY 'a'",
            "SELECT _utf8mb3'a'",
            "SELECT _macce'a'",
            "/* lead */ SELECT 1",
        ] {
            assert!(scan(sql), "warning candidate unexpectedly skipped: {sql}");
        }
    }
}

fn capture_dml_statement_result(
    session: &SessionContext,
    affected_rows: u64,
) -> crate::engine::api::DmlStatementResult {
    crate::engine::api::DmlStatementResult {
        affected_rows,
        matched_rows: session.pending_statement_matched_rows,
        insert_id: session.statement_insert_id,
        warnings: session.warnings.clone(),
        info: session.pending_statement_info.clone().unwrap_or_else(|| {
            session
                .pending_statement_matched_rows
                .map(|matched| {
                    format!(
                        "Rows matched: {matched} Changed: {affected_rows} Warnings: {}",
                        session.warning_count(),
                    )
                })
                .unwrap_or_default()
        }),
        autocommit: session.autocommit,
        in_transaction: session.in_transaction(),
    }
}

fn finish_statement_diagnostics(
    session: &mut SessionContext,
    preserve_warnings: bool,
    result: &Result<QueryResponse, QueryError>,
) {
    let warnings = crate::engine::api::take_statement_warnings();
    if !preserve_warnings {
        let mut conditions = warnings;
        if let Err(err) = result {
            conditions.push(crate::engine::api::SqlWarning::from_error(
                crate::error::ConditionLevel::Error,
                err,
            ));
        }
        session.set_warnings(conditions);
    }
    let insert_id = crate::engine::api::take_statement_insert_id();
    session.statement_insert_id =
        crate::engine::api::take_statement_ok_insert_id().unwrap_or(insert_id);
    // Carry the OK-packet info trailer back to the protocol thread. The
    // executor runs on a blocking-pool thread, so the thread-local set by
    // `set_statement_info` would be invisible to the socket writer. Peek
    // (don't consume) so the same-thread test path that reads the
    // thread-local directly still sees it.
    session.pending_statement_matched_rows = crate::engine::api::statement_matched_rows();
    let pending_info = crate::engine::api::peek_statement_info();
    session.pending_statement_info = if pending_info.is_empty() {
        None
    } else {
        Some(pending_info)
    };
    if insert_id != 0 {
        session
            .last_insert_id
            .store(insert_id, std::sync::atomic::Ordering::Relaxed);
    }
    if let Ok(response) = result {
        session.last_row_count = response_row_count(response);
        session.found_rows = response.found_rows();
    } else {
        session.last_row_count = -1;
    }
    if !preserve_warnings {
        session.diagnostics_row_count = session.last_row_count;
    }
}

fn cap_direct_group_concat_limit_warnings(sql: &str, result: &Result<QueryResponse, QueryError>) {
    fn contains_ascii_case_insensitive(haystack: &str, needle: &[u8]) -> bool {
        haystack
            .as_bytes()
            .windows(needle.len())
            .any(|window| window.eq_ignore_ascii_case(needle))
    }

    let Ok(response) = result else {
        return;
    };
    // The capping operation below only handles materialized SELECT output.
    // In particular, do not scan an entire bulk INSERT's literal payload.
    if !matches!(response, QueryResponse::ResultSet(_)) {
        return;
    }
    if !contains_ascii_case_insensitive(sql, b"GROUP_CONCAT")
        || !contains_ascii_case_insensitive(sql, b"LIMIT")
    {
        return;
    }
    if let Ok(statement) = parse_statement(sql) {
        select::cap_prepared_group_concat_limit_warnings(&statement, response);
    }
}

fn response_row_count(response: &QueryResponse) -> i64 {
    match response {
        QueryResponse::Ok => 0,
        QueryResponse::OkAffected { affected_rows } => {
            i64::try_from(*affected_rows).unwrap_or(i64::MAX)
        }
        QueryResponse::ResultSet(_)
        | QueryResponse::SharedResultSet(_)
        | QueryResponse::StreamingResultSet(_) => -1,
        QueryResponse::MultiResult(results) => results.last().map(response_row_count).unwrap_or(0),
    }
}

fn maybe_log_binlog_ddl(
    db: &Database,
    session: &SessionContext,
    is_ddl: bool,
    binloggable_ddl: bool,
    sql: &str,
    result: Result<QueryResponse, QueryError>,
) -> Result<QueryResponse, QueryError> {
    if is_ddl
        && binloggable_ddl
        && result.is_ok()
        && !crate::binlog::current_ddl_transaction_staged()
    {
        let _sql_log_bin_guard = crate::binlog::SessionSqlLogBinGuard::new(session.sql_log_bin);
        let ctx = binlog_ctx_from_session(session);
        let safe_sql = credential_safe_binlog_sql(db, session, sql)?;
        crate::binlog::record_ddl_autocommit_with(&session.current_database, &safe_sql, &ctx)?;
    }
    result
}

/// Variant that normalizes `sql` lazily, only when the binlog DDL audit
/// is actually going to fire. Used from the bulk-DML hot path so the
/// 500KB `normalize_sql` clone never happens on an INSERT body that
/// will never reach `record_ddl_autocommit`.
fn maybe_log_binlog_ddl_lazy(
    db: &Database,
    session: &SessionContext,
    is_ddl: bool,
    binloggable_ddl: bool,
    sql: &str,
    result: Result<QueryResponse, QueryError>,
) -> Result<QueryResponse, QueryError> {
    if is_ddl
        && binloggable_ddl
        && result.is_ok()
        && !crate::binlog::current_ddl_transaction_staged()
    {
        let _sql_log_bin_guard = crate::binlog::SessionSqlLogBinGuard::new(session.sql_log_bin);
        let normalized = credential_safe_binlog_sql(db, session, &normalize_sql(sql))?;
        let ctx = binlog_ctx_from_session(session);
        crate::binlog::record_ddl_autocommit_with(&session.current_database, &normalized, &ctx)?;
    }
    result
}

/// Keep catalog authentication text, excluding catalog policy defaults.
fn auth_account_prefix(sql: &str) -> Result<String, QueryError> {
    use crate::sql_lex::{SqlLexMode, SqlTokenCursor, SqlTokenKind};
    let mode = SqlLexMode::from_parser_bits(crate::parser::current_parser_sql_mode_bits());
    let mut cursor = SqlTokenCursor::new(sql, mode, crate::server_version::MYSQL_COMPAT_VERSION_ID);
    let mut previous = "";
    let mut end = sql.len();
    while let Some(token) = cursor
        .next()
        .map_err(|err| error(crate::error::catalog::ER_PARSE_ERROR, err.to_string()))?
    {
        let text = cursor.token_text(&token);
        if token.kind == SqlTokenKind::Word
            && [
                "REQUIRE",
                "WITH",
                "PASSWORD",
                "ACCOUNT",
                "ATTRIBUTE",
                "COMMENT",
                "FAILED_LOGIN_ATTEMPTS",
                "PASSWORD_LOCK_TIME",
            ]
            .iter()
            .any(|word| text.eq_ignore_ascii_case(word))
            && !previous.eq_ignore_ascii_case("IDENTIFIED")
        {
            end = token.span.start;
            break;
        }
        previous = text;
    }
    Ok(sql[..end]
        .trim()
        .strip_prefix("CREATE USER ")
        .unwrap_or(sql[..end].trim())
        .to_string())
}

/// The parser has already validated the batch. Locate the shared option tail
/// after the last account's authentication production, keeping literal commas
/// and quoted keyword-looking passwords opaque.
fn alter_users_shared_options_sql(sql: &str) -> Result<&str, QueryError> {
    use crate::sql_lex::{SqlLexMode, SqlTokenCursor, SqlTokenKind};
    let mode = SqlLexMode::from_parser_bits(crate::parser::current_parser_sql_mode_bits());
    let mut cursor = SqlTokenCursor::new(sql, mode, crate::server_version::MYSQL_COMPAT_VERSION_ID);
    let mut tokens = Vec::new();
    while let Some(token) = cursor
        .next()
        .map_err(|err| error(crate::error::catalog::ER_PARSE_ERROR, err.to_string()))?
    {
        tokens.push(token);
    }
    let mut idx = tokens
        .iter()
        .rposition(|token| token.kind == SqlTokenKind::Symbol(b','))
        .map_or(0, |idx| idx + 1);
    let word = |idx: usize, expected: &str| {
        tokens.get(idx).is_some_and(|token| {
            token.kind == SqlTokenKind::Word
                && sql[token.span.clone()].eq_ignore_ascii_case(expected)
        })
    };
    let symbol = |idx: usize, expected: u8| {
        tokens
            .get(idx)
            .is_some_and(|token| token.kind == SqlTokenKind::Symbol(expected))
    };
    idx += 1; // account user
    if symbol(idx, b'@') {
        idx += 2;
    } else if symbol(idx, b'(') && symbol(idx + 1, b')') {
        idx += 2;
    }
    if word(idx, "IDENTIFIED") {
        idx += 1;
        if word(idx, "WITH") {
            idx += 2;
        }
        if word(idx, "BY") || word(idx, "AS") {
            idx += 1;
            idx += if word(idx, "RANDOM") && word(idx + 1, "PASSWORD") {
                2
            } else {
                1
            };
        }
    }
    loop {
        if word(idx, "REPLACE") {
            idx += 2;
        } else if word(idx, "RETAIN") || word(idx, "DISCARD") {
            idx += 3;
        } else {
            break;
        }
    }
    Ok(&sql[tokens.get(idx).map_or(sql.len(), |token| token.span.start)..])
}

/// Account DDL must never put cleartext credentials in QUERY_EVENT. MySQL
/// rewrites successful CREATE/ALTER USER and SET PASSWORD statements to the
/// plugin hash before binlogging; rebuild that replay SQL from the committed
/// auth catalog. Other DDL is returned unchanged.
fn credential_safe_binlog_sql(
    db: &Database,
    session: &SessionContext,
    sql: &str,
) -> Result<String, QueryError> {
    let Ok(statement) = parse_statement(sql) else {
        return Ok(sql.to_string());
    };
    let account_sql = |account: &AccountKey| -> Result<String, QueryError> {
        let QueryResponse::ResultSet(result) = show_create_user_result(db, account)? else {
            unreachable!("SHOW CREATE USER returns a result set")
        };
        result
            .rows
            .first()
            .and_then(|row| row.first())
            .map(Value::to_display_string)
            .ok_or_else(|| {
                crate::mysql::errors::error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "SHOW CREATE USER returned no replay SQL",
                )
            })
    };
    match statement {
        Statement::CreateUser(create) => {
            let mut rendered = create
                .accounts
                .iter()
                .map(|spec| account_sql(&AccountKey::new(&spec.account.user, &spec.account.host)))
                .collect::<Result<Vec<_>, _>>()?;
            if rendered.len() == 1 {
                let sql = rendered.remove(0);
                return Ok(if create.if_not_exists {
                    sql.replacen("CREATE USER", "CREATE USER IF NOT EXISTS", 1)
                } else {
                    sql
                });
            }
            let tail_start = |value: &str| {
                [
                    " REQUIRE ",
                    " WITH ",
                    " PASSWORD ",
                    " ACCOUNT ",
                    " ATTRIBUTE ",
                ]
                .into_iter()
                .filter_map(|marker| value.find(marker))
                .min()
                .unwrap_or(value.len())
            };
            let tail_pos = tail_start(rendered.last().expect("non-empty CREATE USER"));
            let tail = rendered.last().unwrap()[tail_pos..].to_string();
            let accounts = rendered
                .iter()
                .map(|value| {
                    let end = tail_start(value);
                    value[..end]
                        .strip_prefix("CREATE USER ")
                        .unwrap_or(&value[..end])
                        .to_string()
                })
                .collect::<Vec<_>>()
                .join(", ");
            let if_not_exists = if create.if_not_exists {
                " IF NOT EXISTS"
            } else {
                ""
            };
            Ok(format!("CREATE USER{if_not_exists} {accounts}{tail}"))
        }
        Statement::AlterUsers(users) => {
            let options = alter_users_shared_options_sql(sql)?;
            let if_exists = users.first().is_some_and(|user| user.if_exists);
            let mut accounts = Vec::with_capacity(users.len());
            for alter in users {
                let account = if alter.current_user {
                    session.authenticated_as.clone()
                } else {
                    AccountKey::new(&alter.account.user, &alter.account.host)
                };
                let mut rendered = if alter.password.is_some()
                    || alter.random_password
                    || alter.plugin.is_some()
                {
                    match account_sql(&account) {
                        Ok(value) => auth_account_prefix(&value)?,
                        Err(_) if alter.if_exists => account.display(),
                        Err(err) => return Err(err),
                    }
                } else {
                    account.display()
                };
                if alter.retain_current_password {
                    rendered.push_str(" RETAIN CURRENT PASSWORD");
                }
                if alter.discard_old_password {
                    rendered.push_str(" DISCARD OLD PASSWORD");
                }
                accounts.push(rendered);
            }
            Ok(format!(
                "ALTER USER{} {} {}",
                if if_exists { " IF EXISTS" } else { "" },
                accounts.join(", "),
                options
            ))
        }
        Statement::AlterUser(alter) => {
            let account = if alter.current_user {
                session.authenticated_as.clone()
            } else {
                AccountKey::new(alter.account.user, alter.account.host)
            };
            match account_sql(&account) {
                Ok(sql) => Ok(sql.replacen("CREATE USER", "ALTER USER", 1)),
                Err(_) if alter.if_exists => {
                    Ok(format!("ALTER USER IF EXISTS {}", account.display()))
                }
                Err(err) => Err(err),
            }
        }
        Statement::SetPassword { account, .. } => {
            let account = account
                .map(|account| dispatch::show_account_key(&account, session))
                .unwrap_or_else(|| session.authenticated_as.clone());
            Ok(account_sql(&account)?.replacen("CREATE USER", "ALTER USER", 1))
        }
        _ => Ok(sql.to_string()),
    }
}

/// Lifts the replication-relevant slice of session state into a
/// `BinlogQueryContext` for the next QUERY_EVENT (DDL or transaction BEGIN).
fn binlog_ctx_from_session(session: &SessionContext) -> crate::binlog::BinlogQueryContext {
    let client_id = crate::charset::collation_id(&session.character_set_client);
    let conn_id = crate::charset::collation_id(&session.collation_connection);
    let server_id = crate::charset::collation_id(&session.sysvars.collation_server);
    crate::binlog::BinlogQueryContext {
        thread_id: session.connection_id,
        sql_mode: crate::binlog::sql_mode_str_to_bits(&session.sql_mode),
        auto_increment_increment: session.sysvars.auto_increment_increment as u16,
        auto_increment_offset: session.sysvars.auto_increment_offset as u16,
        client_charset_id: client_id,
        connection_collation_id: conn_id,
        server_collation_id: server_id,
        time_zone: session.sysvars.time_zone.clone(),
        lc_time_names: 0,
        charset_database_id: conn_id,
        explicit_defaults_for_timestamp: session.sysvars.explicit_defaults_for_timestamp,
        foreign_key_checks: session.foreign_key_checks,
        unique_checks: session.sysvars.unique_checks,
    }
}

fn should_use_shared_plan_cache(
    enabled: bool,
    session: &SessionContext,
    statement: &Statement,
) -> bool {
    enabled
        && !statement_refs_temp_table(session, statement)
        && !statement_is_temp_ddl(session, statement)
}

/// Process-start execution-plan cache switch (`MYDB_PLAN_CACHE`, default on).
/// Keep this separate from the parsed-statement cache: disabling compiled
/// plans is a correctness/differential-testing escape hatch and should not
/// force repeated tokenization as an unrelated side effect.
pub(crate) fn plan_cache_enabled_from_env() -> bool {
    let resolve = || {
        crate::util::env::var("MYDB_PLAN_CACHE")
            .map(|raw| {
                !matches!(
                    raw.trim().to_ascii_lowercase().as_str(),
                    "0" | "off" | "false" | "no"
                )
            })
            .unwrap_or(true)
    };
    #[cfg(not(test))]
    {
        static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *ENABLED.get_or_init(resolve)
    }
    #[cfg(test)]
    {
        resolve()
    }
}

/// System-schema DML is implemented by literal-aware virtual-table handlers
/// (`performance_schema.setup_*`, `sys.sys_config`, etc.), not the regular
/// storage executor. Preserve its AST literals even when general text DML
/// auto-parameterization is enabled.
fn auto_param_mode_for_statement(
    db: &Database,
    session: &SessionContext,
    statement: &Statement,
    configured: crate::engine::auto_param::AutoParameterizeMode,
) -> crate::engine::auto_param::AutoParameterizeMode {
    if configured == crate::engine::auto_param::AutoParameterizeMode::Off {
        return configured;
    }
    let table = match statement {
        Statement::Insert(stmt) => Some(stmt.table.as_str()),
        Statement::Update(stmt) => Some(stmt.table.as_str()),
        Statement::Delete(stmt) => Some(stmt.table.as_str()),
        _ => None,
    };
    let Some(table) = table else {
        return configured;
    };
    let (database, _) = split_table_name(&session.current_database, table);
    if is_system_database(&database) {
        return crate::engine::auto_param::AutoParameterizeMode::Select;
    }
    if configured == crate::engine::auto_param::AutoParameterizeMode::Select
        && (safe_integer_counter_update(db, session, statement)
            || safe_numeric_counter_update(db, session, statement)
            || safe_typed_point_update(db, session, statement)
            || safe_large_literal_update(statement))
    {
        return crate::engine::auto_param::AutoParameterizeMode::All;
    }
    configured
}

/// Shared syntactic gate for default-on UPDATE parameterization. Type and
/// literal provenance checks remain owned by the individual policies below.
fn plain_parameterizable_update(statement: &Statement) -> Option<&crate::parser::Update> {
    let Statement::Update(update) = statement else {
        return None;
    };
    if update.assignments.is_empty()
        || update.selection.is_none()
        || update.view_check_predicate.is_some()
        || !update.order_by.is_empty()
        || update.limit.is_some()
        || update.ignore
        || update.low_priority
        || update.primary_alias.is_some()
        || !update.joins.is_empty()
        || !update.partitions.is_empty()
        || !update.query_hints.is_empty()
        || !update.hint_warnings.is_empty()
        || update.returning.is_some()
    {
        return None;
    }
    Some(update)
}

fn parameterized_update_column_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Identifier(name) => Some(name.as_str()),
        Expr::CompoundIdentifier(parts) => parts.last().map(String::as_str),
        _ => None,
    }
}

/// Default-on parameterisation for a plain UPDATE carrying a large direct
/// value.  Text-protocol LOB writes otherwise rebuild and qualify the same
/// statement shape for every payload, even though the existing prepared-DML
/// executor already preserves the exact conversion, trigger, FK and locking
/// semantics once literals have been turned into late-bound values.
///
/// Keep this deliberately narrower than operator-selected `All`: only a
/// single-table, unordered UPDATE with direct literal assignments is
/// promoted.  Expressions involving functions/operators retain their source
/// literal provenance and continue through the legacy path.  The compiled-DML
/// validator remains the final gate; unsupported predicates simply miss the
/// cache and fall back.
fn safe_large_literal_update(statement: &Statement) -> bool {
    const LARGE_LITERAL_BYTES: usize = 64 * 1024;

    let Some(update) = plain_parameterizable_update(statement) else {
        return false;
    };

    fn direct_value_bytes(expr: &Expr) -> Option<usize> {
        match expr {
            Expr::Literal(crate::parser::Literal::String(value)) => Some(value.len()),
            Expr::Function {
                name,
                args,
                distinct: false,
            } if name.eq_ignore_ascii_case("CONVERT_USING")
                && matches!(args.as_slice(), [
                    Expr::Literal(crate::parser::Literal::String(_)),
                    Expr::Literal(crate::parser::Literal::String(charset)),
                    Expr::Literal(crate::parser::Literal::Bool(true)),
                ] if charset.eq_ignore_ascii_case("binary")) =>
            {
                let Expr::Literal(crate::parser::Literal::String(value)) = &args[0] else {
                    unreachable!("binary introducer shape checked above")
                };
                Some(value.len())
            }
            Expr::Literal(_) => Some(0),
            _ => None,
        }
    }

    update
        .assignments
        .iter()
        .filter_map(|assignment| direct_value_bytes(&assignment.value))
        .any(|bytes| bytes >= LARGE_LITERAL_BYTES)
        && update
            .assignments
            .iter()
            .all(|assignment| direct_value_bytes(&assignment.value).is_some())
}

/// Conservative default-on DML parameterization subset.
///
/// Atomic integer counters are a general OLTP primitive (`SET n=n±const`
/// plus integer comparisons). Replacing those integer literals with late-bound
/// values activates the stable expression/index kernels without erasing string,
/// temporal, division-by-zero, hint, modifier, or strict-conversion provenance.
/// Anything outside this syntactic + catalog-typed subset keeps the legacy
/// literal AST unless the operator explicitly selects `MYDB_AUTO_PARAMETERIZE=all`.
fn safe_integer_counter_update(
    db: &Database,
    session: &SessionContext,
    statement: &Statement,
) -> bool {
    safe_counter_update::<false>(db, session, statement)
}

/// Decimal/float assignments retain the same integer-only predicate policy.
fn safe_numeric_counter_update(
    db: &Database,
    session: &SessionContext,
    statement: &Statement,
) -> bool {
    safe_counter_update::<true>(db, session, statement)
}

fn safe_counter_update<const APPROXIMATE_OR_DECIMAL: bool>(
    db: &Database,
    session: &SessionContext,
    statement: &Statement,
) -> bool {
    let Some(update) = plain_parameterizable_update(statement) else {
        return false;
    };
    let (database, table_name) = split_table_name(&session.current_database, &update.table);
    if is_system_database(&database) {
        return false;
    }
    let Ok(table) = db.table_in_database(&database, &table_name) else {
        return false;
    };
    let column = |name: &str| {
        table
            .column_index(name)
            .ok()
            .and_then(|index| table.columns.get(index))
    };
    let numeric_counter_column = |name: &str| {
        column(name).is_some_and(|column| {
            if APPROXIMATE_OR_DECIMAL {
                matches!(
                    column.data_type,
                    DataTypeKind::Float { .. }
                        | DataTypeKind::Double { .. }
                        | DataTypeKind::Decimal { .. }
                )
            } else {
                matches!(
                    column.data_type,
                    DataTypeKind::TinyInt
                        | DataTypeKind::SmallInt
                        | DataTypeKind::MediumInt
                        | DataTypeKind::Int
                        | DataTypeKind::BigInt
                )
            }
        })
    };
    let integer_column = |name: &str| {
        column(name).is_some_and(|column| {
            matches!(
                column.data_type,
                DataTypeKind::TinyInt
                    | DataTypeKind::SmallInt
                    | DataTypeKind::MediumInt
                    | DataTypeKind::Int
                    | DataTypeKind::BigInt
            )
        })
    };
    fn numeric_literal<const WIDE: bool>(expr: &Expr) -> bool {
        if !WIDE {
            return integer_literal(expr);
        }
        matches!(
            expr,
            Expr::Literal(
                Literal::Int(_) | Literal::UInt(_) | Literal::Float(_) | Literal::Decimal(_)
            )
        )
    }
    fn integer_literal(expr: &Expr) -> bool {
        matches!(expr, Expr::Literal(Literal::Int(_) | Literal::UInt(_)))
    }
    let assignments_safe = update.assignments.iter().all(|assignment| {
        if assignment.table_qualifier.is_some() || !numeric_counter_column(&assignment.column) {
            return false;
        }
        let Expr::Binary { left, op, right } = &assignment.value else {
            return false;
        };
        let same_target = |expr: &Expr| {
            parameterized_update_column_name(expr)
                .is_some_and(|name| name.eq_ignore_ascii_case(&assignment.column))
        };
        match op {
            BinaryOp::Add => {
                (same_target(left) && numeric_literal::<APPROXIMATE_OR_DECIMAL>(right))
                    || (numeric_literal::<APPROXIMATE_OR_DECIMAL>(left) && same_target(right))
            }
            BinaryOp::Sub => same_target(left) && numeric_literal::<APPROXIMATE_OR_DECIMAL>(right),
            _ => false,
        }
    });
    if !assignments_safe {
        return false;
    }
    fn integer_predicate(expr: &Expr, integer_column: &impl Fn(&str) -> bool) -> bool {
        let Expr::Binary { left, op, right } = expr else {
            return false;
        };
        if *op == BinaryOp::And {
            return integer_predicate(left, integer_column)
                && integer_predicate(right, integer_column);
        }
        if !matches!(
            op,
            BinaryOp::Eq
                | BinaryOp::NullSafeEq
                | BinaryOp::NotEq
                | BinaryOp::Lt
                | BinaryOp::LtEq
                | BinaryOp::Gt
                | BinaryOp::GtEq
        ) {
            return false;
        }
        let typed_column =
            |expr: &Expr| parameterized_update_column_name(expr).is_some_and(integer_column);
        (typed_column(left) && integer_literal(right))
            || (integer_literal(left) && typed_column(right))
    }
    integer_predicate(
        update.selection.as_ref().expect("checked above"),
        &integer_column,
    )
}

/// Catalog-typed simple point UPDATEs.
///
/// The complete parser remains the cold-shape authority. This additional gate
/// proves that every late-bound value reaches the same column coercion site as
/// its literal form: assignments are direct compatible literals or self +/-
/// numeric literals; predicates are ANDs of compatible column/literal
/// comparisons. No arbitrary expression can cross the boundary, so functions,
/// shifts, NULL tests, multiplication/division and string-to-number expression
/// errors retain the legacy evaluator and its exact diagnostic timing.
fn safe_typed_point_update(db: &Database, session: &SessionContext, statement: &Statement) -> bool {
    let Some(update) = plain_parameterizable_update(statement) else {
        return false;
    };
    let (database, table_name) = split_table_name(&session.current_database, &update.table);
    if is_system_database(&database) {
        return false;
    }
    let Ok(table) = db.table_in_database(&database, &table_name) else {
        return false;
    };
    let column = |name: &str| {
        table
            .column_index(name)
            .ok()
            .and_then(|index| table.columns.get(index))
    };
    fn numeric_literal(expr: &Expr) -> bool {
        matches!(
            expr,
            Expr::Literal(
                Literal::Int(_) | Literal::UInt(_) | Literal::Float(_) | Literal::Decimal(_)
            )
        )
    }
    fn numeric_type(kind: &DataTypeKind) -> bool {
        matches!(
            kind,
            DataTypeKind::TinyInt
                | DataTypeKind::SmallInt
                | DataTypeKind::MediumInt
                | DataTypeKind::Int
                | DataTypeKind::BigInt
                | DataTypeKind::Float { .. }
                | DataTypeKind::Double { .. }
                | DataTypeKind::Decimal { .. }
        )
    }
    fn literal_compatible(column: &crate::storage::ColumnSchema, expr: &Expr) -> bool {
        let Expr::Literal(literal) = expr else {
            return false;
        };
        if matches!(literal, Literal::Null) {
            return column.nullable;
        }
        match column.data_type {
            DataTypeKind::TinyInt
            | DataTypeKind::SmallInt
            | DataTypeKind::MediumInt
            | DataTypeKind::Int
            | DataTypeKind::BigInt
            | DataTypeKind::Bool
            | DataTypeKind::Year => matches!(literal, Literal::Int(_) | Literal::UInt(_)),
            DataTypeKind::Float { .. }
            | DataTypeKind::Double { .. }
            | DataTypeKind::Decimal { .. } => matches!(
                literal,
                Literal::Int(_) | Literal::UInt(_) | Literal::Float(_) | Literal::Decimal(_)
            ),
            DataTypeKind::Char { .. }
            | DataTypeKind::VarChar { .. }
            | DataTypeKind::Text { .. } => matches!(literal, Literal::String(_)),
            DataTypeKind::Binary { .. }
            | DataTypeKind::VarBinary { .. }
            | DataTypeKind::Blob { .. } => matches!(
                literal,
                Literal::String(_) | Literal::HexBytes(_) | Literal::BitBytes(_)
            ),
            DataTypeKind::Date
            | DataTypeKind::Time { .. }
            | DataTypeKind::DateTime { .. }
            | DataTypeKind::Timestamp { .. } => matches!(literal, Literal::String(_)),
            // Complex/domain types keep their dedicated legacy coercion and
            // validation surfaces.
            _ => false,
        }
    }

    let assignments_safe = update.assignments.iter().all(|assignment| {
        if assignment.table_qualifier.is_some() {
            return false;
        }
        let Some(target) = column(&assignment.column) else {
            return false;
        };
        if literal_compatible(target, &assignment.value) {
            return true;
        }
        if !numeric_type(&target.data_type) {
            return false;
        }
        let Expr::Binary { left, op, right } = &assignment.value else {
            return false;
        };
        let same_target = |expr: &Expr| {
            parameterized_update_column_name(expr)
                .is_some_and(|name| name.eq_ignore_ascii_case(&assignment.column))
        };
        match op {
            BinaryOp::Add => {
                (same_target(left) && numeric_literal(right))
                    || (numeric_literal(left) && same_target(right))
            }
            BinaryOp::Sub => same_target(left) && numeric_literal(right),
            _ => false,
        }
    });
    if !assignments_safe {
        return false;
    }
    fn typed_predicate(expr: &Expr, table: &crate::storage::TableSchema) -> bool {
        let Expr::Binary { left, op, right } = expr else {
            return false;
        };
        if *op == BinaryOp::And {
            return typed_predicate(left, table) && typed_predicate(right, table);
        }
        if !matches!(
            op,
            BinaryOp::Eq
                | BinaryOp::NullSafeEq
                | BinaryOp::NotEq
                | BinaryOp::Lt
                | BinaryOp::LtEq
                | BinaryOp::Gt
                | BinaryOp::GtEq
        ) {
            return false;
        }
        let pair_safe = |column_expr: &Expr, literal_expr: &Expr| {
            parameterized_update_column_name(column_expr)
                .and_then(|name| table.column_index(name).ok())
                .and_then(|index| table.columns.get(index))
                .is_some_and(|schema| literal_compatible(schema, literal_expr))
        };
        pair_safe(left, right) || pair_safe(right, left)
    }
    typed_predicate(update.selection.as_ref().expect("checked above"), &table)
}

pub(super) fn plan_semantics(session: &SessionContext) -> crate::engine::cache::PlanSemantics {
    crate::engine::cache::PlanSemantics {
        sql_mode_fingerprint: crate::binlog::sql_mode_str_to_bits(&session.sql_mode),
        optimizer_switch_fingerprint: crate::engine::cache::optimizer_switch_fingerprint(
            &session.sysvars.optimizer_switch,
        ),
        sql_select_limit: session.sysvars.sql_select_limit,
        use_invisible_indexes: session.sysvars.use_invisible_indexes,
        time_zone: Arc::from(session.sysvars.time_zone.as_str()),
        character_set_client: Arc::from(session.character_set_client.as_str()),
        character_set_connection: Arc::from(session.character_set_connection.as_str()),
        collation_connection: Arc::from(session.collation_connection.as_str()),
        div_precision_increment: session.sysvars.div_precision_increment.min(30) as u8,
        group_concat_max_len: session.group_concat_max_len,
    }
}

/// Kill switch for the compile-once prepared-DML fast path
/// (`MYDB_PREPARED_DML_CACHE`, default on). Same shape as the read flag
/// — off forces every INSERT/UPDATE/DELETE EXECUTE through the legacy
/// clone + 8-wrapper dispatch path, used for differential testing of the
/// cached vs uncached executor and as an escape hatch if the fast path
/// is suspected to misbehave.
fn prepared_dml_cache_enabled() -> bool {
    use std::sync::OnceLock;
    static FLAG: OnceLock<bool> = OnceLock::new();
    *FLAG.get_or_init(|| {
        crate::util::env::var("MYDB_PREPARED_DML_CACHE")
            .map(|v| {
                !matches!(
                    v.as_str(),
                    "0" | "off" | "OFF" | "false" | "FALSE" | "no" | "NO"
                )
            })
            .unwrap_or(true)
    })
}

/// Cheap byte-level discriminator that returns true *only* when `sql`
/// is a `SHOW VARIABLES`/`SHOW STATUS` statement whose live answer must
/// come from the storage engine (engine status / runtime variables),
/// not from the stub. The old implementation `parse_statement(sql)`
/// each call — for a TPC-C-style workload every COM_QUERY-routed SQL
/// (the 300 K-plus that aren't BEGIN/COMMIT/ROLLBACK) paid one extra
/// sqlparser run just to answer "is this a SHOW VARIABLES?". The byte
/// scan below is two orders of magnitude cheaper and the false-positive
/// rate is zero for any well-formed input: a leading `SHOW` followed by
/// whitespace and `VARIABLES`/`STATUS` is unambiguous syntax in MySQL.
fn statement_requires_storage_show_context(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    // Skip leading ASCII whitespace + `/* … */` + `-- … \n` comments
    // using the same logic as `normalize_sql`. We deliberately bound
    // the comment skipping to a small budget so a pathological input
    // doesn't slow the discriminator past the cost of `parse_statement`.
    let body = skip_leading_ws_and_comments_for_probe(bytes);
    if body.len() < 5 || !body[..4].eq_ignore_ascii_case(b"SHOW") {
        return false;
    }
    let rest = trim_leading_ws(&body[4..]);
    let after_show = match rest.first().copied() {
        Some(b) if b.is_ascii_whitespace() => return false,
        _ => rest,
    };
    // `SHOW [GLOBAL|SESSION|LOCAL] {VARIABLES|STATUS}` — the scope prefix
    // is optional and used to be misread as a non-VARIABLES/STATUS form,
    // sending the statement to the stub engine which has no `Database`
    // attached and therefore returns no real status rows.
    let after_scope = if let Some(after) = consume_keyword_followed_by_ws(after_show, b"GLOBAL")
        .or_else(|| consume_keyword_followed_by_ws(after_show, b"SESSION"))
        .or_else(|| consume_keyword_followed_by_ws(after_show, b"LOCAL"))
    {
        trim_leading_ws(after)
    } else {
        after_show
    };
    starts_with_keyword_followed_by_ws(after_scope, b"VARIABLES")
        || starts_with_keyword_followed_by_ws(after_scope, b"STATUS")
}

fn consume_keyword_followed_by_ws<'a>(s: &'a [u8], kw: &[u8]) -> Option<&'a [u8]> {
    if s.len() < kw.len() {
        return None;
    }
    if !s[..kw.len()].eq_ignore_ascii_case(kw) {
        return None;
    }
    let next = s.get(kw.len()).copied();
    match next {
        Some(b) if b.is_ascii_whitespace() => Some(&s[kw.len()..]),
        _ => None,
    }
}

fn trim_leading_ws(s: &[u8]) -> &[u8] {
    let mut i = 0;
    while i < s.len() && s[i].is_ascii_whitespace() {
        i += 1;
    }
    &s[i..]
}

/// True iff `s` begins with `kw` (ASCII case-insensitive) and the byte
/// at `kw.len()` either is whitespace, is `;`, or is past the end —
/// distinguishing `STATUS` (match) from `STATUSXYZ` (no match).
fn starts_with_keyword_followed_by_ws(s: &[u8], kw: &[u8]) -> bool {
    if s.len() < kw.len() {
        return false;
    }
    if !s[..kw.len()].eq_ignore_ascii_case(kw) {
        return false;
    }
    match s.get(kw.len()).copied() {
        None => true,
        Some(b) => b.is_ascii_whitespace() || b == b';',
    }
}

/// Small helper that mirrors `normalize_sql`'s leading-trivia stripping
/// for the SHOW probe. Identical to the equivalent code path in
/// `normalize_sql`, hoisted here so the probe doesn't have to
/// materialise a full normalised String just to look at the first
/// keyword.
fn skip_leading_ws_and_comments_for_probe(input: &[u8]) -> &[u8] {
    let Ok(sql) = std::str::from_utf8(input) else {
        return input;
    };
    let mode =
        crate::sql_lex::SqlLexMode::from_parser_bits(crate::parser::current_parser_sql_mode_bits());
    let start = crate::sql_lex::skip_trivia(sql, 0, mode, 80045, false).unwrap_or(0);
    &input[start..]
}

/// One-shot classification for the four OLTP-client transaction-control
/// keywords that don't carry any payload. Match is case-insensitive but
/// requires the trimmed input to be *exactly* the keyword (no comments,
/// no embedded whitespace, no `WORK` suffix). Anything fancier falls
/// through to the regular sqlparser dispatch path. Returning a `Copy`
/// enum value keeps the fast path branch-free at the call site.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SimpleTxnControl {
    Begin,
    Commit,
    Rollback,
}

/// True for the bare `BEGIN` / `COMMIT` / `ROLLBACK` texts that `execute`
/// dispatches without parsing.
pub(crate) fn is_simple_txn_control_sql(sql: &str) -> bool {
    simple_txn_control_kind(sql.trim_matches(|c: char| c.is_whitespace() || c == ';')).is_some()
}

fn simple_txn_control_kind(trimmed: &str) -> Option<SimpleTxnControl> {
    // Longest match is "ROLLBACK" (8 bytes). Anything shorter than
    // "BEGIN" (5 bytes) cannot match.
    let bytes = trimmed.as_bytes();
    if !(5..=8).contains(&bytes.len()) {
        return None;
    }
    // Manual case-insensitive compare avoids allocating a normalized
    // copy of the input.
    let eq_ascii_ci = |a: &[u8], b: &[u8]| -> bool {
        a.len() == b.len()
            && a.iter()
                .zip(b.iter())
                .all(|(x, y)| x.eq_ignore_ascii_case(y))
    };
    if eq_ascii_ci(bytes, b"BEGIN") {
        return Some(SimpleTxnControl::Begin);
    }
    if eq_ascii_ci(bytes, b"COMMIT") {
        return Some(SimpleTxnControl::Commit);
    }
    if eq_ascii_ci(bytes, b"ROLLBACK") {
        return Some(SimpleTxnControl::Rollback);
    }
    None
}

fn dispatch_simple_txn_control(
    db: &Arc<Database>,
    session: &mut SessionContext,
    kind: SimpleTxnControl,
) -> Result<QueryResponse, QueryError> {
    use crate::engine::executor::tx::{
        begin_explicit_txn, ensure_no_xa_branch_for_local_txn, finish_transaction_with_completion,
    };
    let label = match kind {
        SimpleTxnControl::Begin => "BEGIN",
        SimpleTxnControl::Commit => "COMMIT",
        SimpleTxnControl::Rollback => "ROLLBACK",
    };
    ensure_no_xa_branch_for_local_txn(session, label)?;
    match kind {
        SimpleTxnControl::Begin => begin_explicit_txn(db, session)?,
        // Bare COMMIT/ROLLBACK still honour `completion_type` (chain/release
        // defaults) — pass no explicit clause so the sysvar drives behaviour.
        SimpleTxnControl::Commit => {
            finish_transaction_with_completion(db, session, None, None, false)?
        }
        SimpleTxnControl::Rollback => {
            finish_transaction_with_completion(db, session, None, None, true)?
        }
    }
    Ok(QueryResponse::Ok)
}

/// Skip ASCII whitespace + `-- …\n` + `/* … */` comments at `start`.
fn skip_ws_and_comments_for_sniff(sql: &str, start: usize) -> usize {
    let bytes = sql.as_bytes();
    let mut pos = start.min(bytes.len());
    while bytes.get(pos).is_some_and(u8::is_ascii_whitespace) {
        pos += 1;
    }
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
    crate::sql_lex::skip_trivia(sql, start, mode, 80045, false).unwrap_or(start)
}

/// Only a top-level SELECT owns a SELECT-plan key. CALL/SET/DML can
/// execute nested SELECTs, but their outer SQL must never name those plans.
fn statement_skips_plan_cache_hint<'q>(statement: impl Into<crate::parser::QueryRef<'q>>) -> bool {
    let statement = statement.into();
    !matches!(statement, crate::parser::QueryRef::Select(_))
}

/// Cheap byte-prefix sniff for bulk-DML statements
/// (`INSERT` / `REPLACE` / `UPDATE` / `DELETE`). When this returns true
/// the engine entry parses the raw SQL bytes and skips the 500KB
/// `normalize_sql` clone — these statements never benefit from the
/// auto-parameterized plan cache (each call's body is one-shot) so
/// the case-canonicalisation that the cache key needs is irrelevant.
/// SELECT / SHOW / DDL / etc. keep the historical normalize path so
/// the auto-param fingerprint stays invariant under casing or
/// whitespace differences.
fn bulk_dml_keyword_prefix(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let start = skip_ws_and_comments_for_sniff(sql, 0);
    let rest = &bytes[start..];
    let matches_kw = |kw: &[u8]| -> bool {
        rest.len() > kw.len()
            && rest[..kw.len()].eq_ignore_ascii_case(kw)
            && matches!(
                rest[kw.len()],
                b' ' | b'\t' | b'\r' | b'\n' | b'(' | b'`' | b'"'
            )
    };
    matches_kw(b"INSERT")
        || matches_kw(b"REPLACE")
        || matches_kw(b"UPDATE")
        || matches_kw(b"DELETE")
}

fn select_keyword_prefix(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let start = skip_ws_and_comments_for_sniff(sql, 0);
    let rest = &bytes[start..];
    rest.len() > b"SELECT".len()
        && rest[..b"SELECT".len()].eq_ignore_ascii_case(b"SELECT")
        && matches!(
            rest[b"SELECT".len()],
            b' ' | b'\t' | b'\r' | b'\n' | b'(' | b'/'
        )
}

/// Produce a parser-cache key for a conservative literal statement skeleton.
/// The skeleton is never parsed/executed on trust: the first exact parse must
/// auto-parameterize to the identical value vector before it is inserted.
/// Later executions with the same non-literal bytes can therefore reuse that
/// immutable placeholder AST without retokenizing the full statement.
///
/// SELECT and single-statement DML share this scanner. Both consume the
/// skeleton the same way — an immutable placeholder AST plus a value vector,
/// bound late by the compiled prepared vehicle — so restricting it to reads
/// only meant literal-bearing INSERT/UPDATE/DELETE re-tokenized and re-planned
/// their whole body on every execution.
fn text_shape_key_and_params(
    sql: &str,
    parser_mode_bits: u16,
) -> Option<(crate::engine::cache::ParsedStatementKey, Vec<Value>)> {
    if !(select_keyword_prefix(sql) || bulk_dml_keyword_prefix(sql))
        || has_non_trailing_statement_separator_with_mode(
            sql,
            crate::sql_lex::SqlLexMode::from_parser_bits(parser_mode_bits),
        )
        // Keep executable comments and optimizer hints on the exact parser
        // path: both can change semantics/plan shape while an ordinary
        // comment is safely normalized to whitespace.
        || sql.contains("/*!")
        || sql.contains("/*+")
        // The lightweight scanner deliberately admits only an unambiguous
        // ASCII subset. Escaped/non-ASCII string decoding remains owned by
        // the full parser, so a later spelling cannot alias a proven shape
        // while producing a different runtime value.
        || sql.as_bytes().iter().any(|byte| *byte == b'\\' || !byte.is_ascii())
    {
        return None;
    }
    let (shape, params) = scan_literal_normalized_dml(sql, false)?;
    Some((
        crate::engine::cache::ParsedStatementKey::new(&shape, parser_mode_bits),
        params,
    ))
}

const TEXT_DML_SHAPE_MAX_BYTES: usize = 4096;

/// Compact digest-only skeleton for ordinary literal text DML. Unlike an
/// execution artifact this is never trusted for values or semantics; it only
/// supplies the input to Performance Schema's normal digest tokenizer. The
/// tokenizer therefore still owns keyword/identifier normalization, while
/// distinct literal spellings share its bounded derived-text cache.
fn text_dml_digest_shape(sql: &str) -> Option<Arc<str>> {
    if sql.len() > TEXT_DML_SHAPE_MAX_BYTES
        || !bulk_dml_keyword_prefix(sql)
        || sql.contains("/*!")
        || sql.contains("/*+")
        || sql.as_bytes().iter().any(|byte| !byte.is_ascii())
    {
        return None;
    }
    Some(Arc::from(scan_literal_normalized_shape(sql)?))
}

pub(crate) fn has_non_trailing_statement_separator(sql: &str) -> bool {
    let mode =
        crate::sql_lex::SqlLexMode::from_parser_bits(crate::parser::current_parser_sql_mode_bits());
    has_non_trailing_statement_separator_with_mode(sql, mode)
}

pub(crate) fn has_non_trailing_statement_separator_with_mode(
    sql: &str,
    mode: crate::sql_lex::SqlLexMode,
) -> bool {
    // An executable separator must also exist in the raw source.  Check this
    // before `PreparedSql` builds its segment map: bulk DML normally has no
    // semicolon, and memchr is sufficient to prove the answer in that case.
    if !sql.as_bytes().contains(&b';') {
        return false;
    }
    // Most bulk statements contain semicolons only inside quoted values.
    // Segment once without allocating PreparedSql's segment/range vectors.
    // Executable comments retain the canonical masking path: their bodies
    // can change quote/comment boundaries in the effective SQL text.
    let mut separator = false;
    for segment in crate::sql_lex::segments(sql, mode, 80045) {
        let Ok(segment) = segment else {
            return true;
        };
        match segment.kind {
            crate::sql_lex::SqlSegmentKind::Code => {
                let bytes = &sql.as_bytes()[segment.range];
                if separator {
                    if bytes.iter().any(|b| !b.is_ascii_whitespace()) {
                        return true;
                    }
                } else if let Some(pos) = memchr::memchr(b';', bytes) {
                    separator = true;
                    if bytes[pos + 1..].iter().any(|b| !b.is_ascii_whitespace()) {
                        return true;
                    }
                }
            }
            crate::sql_lex::SqlSegmentKind::Quoted { .. } if separator => return true,
            crate::sql_lex::SqlSegmentKind::ExecutableComment { .. } => {
                return has_non_trailing_separator_with_executable_comments(sql, mode);
            }
            _ => {}
        }
    }
    false
}

fn has_non_trailing_separator_with_executable_comments(
    sql: &str,
    mode: crate::sql_lex::SqlLexMode,
) -> bool {
    let Ok(prepared) = crate::sql_lex::PreparedSql::new(sql, mode, 80045) else {
        return true;
    };
    let sql = prepared.effective();
    let bytes = sql.as_bytes();
    let Ok(ranges) = crate::sql_lex::code_ranges(sql, mode, 80045) else {
        return true;
    };
    for range in ranges {
        for offset in range.clone() {
            if bytes[offset] != b';' {
                continue;
            }
            let suffix = &sql[offset + 1..];
            for segment in crate::sql_lex::segments(suffix, mode, 80045) {
                let Ok(segment) = segment else {
                    return true;
                };
                match segment.kind {
                    crate::sql_lex::SqlSegmentKind::Code
                        if suffix.as_bytes()[segment.range]
                            .iter()
                            .any(|byte| !byte.is_ascii_whitespace()) =>
                    {
                        return true;
                    }
                    crate::sql_lex::SqlSegmentKind::Quoted { .. } => return true,
                    _ => {}
                }
            }
            return false;
        }
    }
    false
}

fn scan_literal_normalized_dml(
    sql: &str,
    require_statement_separator: bool,
) -> Option<(String, Vec<Value>)> {
    // Reject oversized single statements before scanning their payload for
    // ambiguous bytes. The implementation applies the same eligibility cap;
    // multi-statement batches retain their independent admission policy.
    if !require_statement_separator && sql.len() > TEXT_DML_SHAPE_MAX_BYTES {
        return None;
    }
    // Cold-path parameter equality cannot protect later spellings of a hot
    // shape. Keep ambiguous literals/comments on the authoritative parser
    // path for both single statements and batches.
    if sql.contains("/*!")
        || sql.contains("/*+")
        || sql
            .as_bytes()
            .iter()
            .any(|byte| *byte == b'\\' || !byte.is_ascii())
    {
        return None;
    }
    scan_literal_normalized_dml_impl(sql, require_statement_separator, true)
}

fn scan_literal_normalized_shape(sql: &str) -> Option<String> {
    scan_literal_normalized_dml_impl(sql, false, false).map(|(shape, _)| shape)
}

fn scan_literal_normalized_dml_impl(
    sql: &str,
    require_statement_separator: bool,
    collect_params: bool,
) -> Option<(String, Vec<Value>)> {
    // Single counter statements are deliberately small. Avoid turning this
    // candidate scan into extra linear work/allocation for large one-shot
    // UPDATE bodies that will fall back to the general executor anyway.
    if !require_statement_separator && sql.len() > TEXT_DML_SHAPE_MAX_BYTES {
        return None;
    }
    let bytes = sql.as_bytes();
    let mut i = 0;
    let mut shape = String::with_capacity(sql.len().min(4096));
    let mut params = Vec::new();
    let mut saw_literal = false;
    let mut saw_separator = false;
    let mut last_was_space = false;
    while i < bytes.len() {
        let b = bytes[i];
        if b.is_ascii_whitespace() {
            if !last_was_space {
                shape.push(' ');
                last_was_space = true;
            }
            i += 1;
            continue;
        }
        if crate::sql_lex::is_dash_line_comment_at(sql, i) {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            if !last_was_space {
                shape.push(' ');
                last_was_space = true;
            }
            continue;
        }
        if b == b'#' {
            i += 1;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            if !last_was_space {
                shape.push(' ');
                last_was_space = true;
            }
            continue;
        }
        if b == b'/' && bytes.get(i + 1) == Some(&b'*') {
            i += 2;
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            if i + 1 >= bytes.len() {
                return None;
            }
            i += 2;
            if !last_was_space {
                shape.push(' ');
                last_was_space = true;
            }
            continue;
        }
        last_was_space = false;
        match b {
            b'\'' => {
                let next = if collect_params {
                    let (value, next) = scan_sql_string_literal(bytes, i)?;
                    params.push(Value::String(value));
                    next
                } else {
                    skip_sql_string_literal(bytes, i)?
                };
                saw_literal = true;
                shape.push('?');
                i = next;
            }
            b'`' => {
                let start = i;
                i += 1;
                let mut closed = false;
                while i < bytes.len() {
                    if bytes[i] == b'`' {
                        i += 1;
                        closed = true;
                        break;
                    }
                    i += 1;
                }
                if !closed {
                    return None;
                }
                shape.push_str(&sql[start..i]);
            }
            b'?' | b'"' => return None,
            b';' => {
                saw_separator = true;
                shape.push(';');
                i += 1;
            }
            b'0'..=b'9' => {
                let start = i;
                i += 1;
                while i < bytes.len()
                    && (bytes[i].is_ascii_digit()
                        || matches!(bytes[i], b'.' | b'e' | b'E' | b'+' | b'-'))
                {
                    if matches!(bytes[i], b'+' | b'-')
                        && !matches!(bytes.get(i.wrapping_sub(1)), Some(b'e' | b'E'))
                    {
                        break;
                    }
                    i += 1;
                }
                let token = &sql[start..i];
                if collect_params {
                    params.push(parse_scanned_numeric_literal(token)?);
                }
                saw_literal = true;
                shape.push('?');
            }
            b'a'..=b'z' | b'A'..=b'Z' | b'_' => {
                let start = i;
                i += 1;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                // Preserve identifier spelling in the cache key. Folding the
                // whole token would also fold case-sensitive table names; a
                // cache hit could then execute a plan compiled for a different
                // table. Keyword-case variants may occupy separate entries,
                // which is the conservative correctness tradeoff.
                shape.push_str(&sql[start..i]);
            }
            _ => {
                shape.push(b as char);
                i += 1;
            }
        }
    }
    if (require_statement_separator && !saw_separator) || !saw_literal {
        return None;
    }
    let mut shape = shape.trim().to_string();
    if !require_statement_separator {
        // `has_non_trailing_statement_separator` has already proved that
        // every semicolon is trailing. Keep `UPDATE ...` and `UPDATE ...;`
        // in one cache entry.
        while shape.ends_with(';') {
            shape.pop();
            shape.truncate(shape.trim_end().len());
        }
    }
    Some((shape, params))
}

fn skip_sql_string_literal(bytes: &[u8], start: usize) -> Option<usize> {
    let mut i = start + 1;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' if bytes.get(i + 1) == Some(&b'\'') => i += 2,
            b'\'' => return Some(i + 1),
            b'\\' => i += 2,
            _ => i += 1,
        }
    }
    None
}

fn scan_sql_string_literal(bytes: &[u8], start: usize) -> Option<(String, usize)> {
    let mut i = start + 1;
    let mut out = String::new();
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                if bytes.get(i + 1) == Some(&b'\'') {
                    out.push('\'');
                    i += 2;
                } else {
                    return Some((out, i + 1));
                }
            }
            b'\\' => {
                let next = *bytes.get(i + 1)?;
                out.push(next as char);
                i += 2;
            }
            b => {
                out.push(b as char);
                i += 1;
            }
        }
    }
    None
}

fn parse_scanned_numeric_literal(token: &str) -> Option<Value> {
    crate::engine::auto_param::scanned_numeric_literal_value(token)
}

#[derive(Debug, Clone, Copy, Default)]
struct LockWaitStats {
    count: u64,
    elapsed: Duration,
}

impl LockWaitStats {
    fn record(&mut self, elapsed: Duration) {
        self.count = self.count.saturating_add(1);
        self.elapsed = self.elapsed.saturating_add(elapsed);
    }
}

impl QueryLogMetadata {
    fn with_lock_wait_stats(mut self, stats: LockWaitStats) -> Self {
        self.lock_wait_count = self.lock_wait_count.saturating_add(stats.count);
        self.lock_wait_ms += stats.elapsed.as_secs_f64() * 1000.0;
        self
    }
}

pub struct StorageQueryEngine {
    db: Arc<Database>,
    log_config: QueryLogConfig,
    sql_log: Arc<SqlLogService>,
    registry: Arc<SessionRegistry>,
    statement_cache: Arc<crate::engine::cache::StatementCache>,
    plan_cache_enabled: bool,
    plan_cache: Arc<crate::engine::cache::PlanCache>,
    prepared_dml_cache: Arc<crate::engine::cache::PreparedDmlPlanCache>,
    prepared_dml_batch_cache: Arc<crate::engine::cache::PreparedDmlBatchCache>,
    #[allow(dead_code)]
    event_scheduler: Option<self::events::EventSchedulerHandle>,
    #[allow(dead_code)]
    undo_purge_worker: UndoPurgeHandle,
    #[allow(dead_code)]
    checkpointer: CheckpointerHandle,
    memory_pressure_worker: crate::engine::background::BackgroundWorker,
    #[allow(dead_code)]
    auto_analyze_worker: crate::engine::auto_analyze::AutoAnalyzeHandle,
    #[allow(dead_code)]
    zone_map_maintenance_worker: crate::engine::zone_map_maintenance::ZoneMapMaintenanceHandle,
    vector_maintenance_worker: crate::engine::vector_maintenance::VectorMaintenanceHandle,
}

struct ObservedStreamCursor {
    inner: Option<Box<dyn ResultCursor>>,
    rows_sent: u64,
    warnings: crate::engine::api::SqlWarnings,
    finish: Option<Box<dyn FnOnce(u64, Option<QueryError>) + Send>>,
}

impl std::fmt::Debug for ObservedStreamCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObservedStreamCursor")
            .field("rows_sent", &self.rows_sent)
            .finish()
    }
}

impl ObservedStreamCursor {
    fn finish(&mut self, error: Option<QueryError>) {
        if let Some(finish) = self.finish.take() {
            // The producer may still be scanning after an early consumer drop.
            // Joining it before reading the counter makes the partial result exact.
            if let Some(mut inner) = self.inner.take() {
                self.warnings.append(&mut inner.take_warnings());
                drop(inner);
            }
            finish(self.rows_sent, error);
        }
    }
}

impl ResultCursor for ObservedStreamCursor {
    fn external_io(&self) -> bool {
        self.inner.as_ref().is_some_and(|inner| inner.external_io())
    }
    fn external_batch_rows(&self) -> usize {
        self.inner
            .as_ref()
            .map_or(1, |inner| inner.external_batch_rows())
    }

    fn next_row(&mut self) -> Result<Option<Vec<Value>>, QueryError> {
        let Some(inner) = self.inner.as_mut() else {
            return Ok(None);
        };
        match inner.next_row() {
            Ok(Some(row)) => {
                self.rows_sent = self.rows_sent.saturating_add(1);
                Ok(Some(row))
            }
            Ok(None) => {
                self.finish(None);
                Ok(None)
            }
            Err(error) => {
                self.finish(Some(error.clone()));
                Err(error)
            }
        }
    }

    fn take_warnings(&mut self) -> crate::engine::api::SqlWarnings {
        let mut warnings = std::mem::take(&mut self.warnings);
        if let Some(inner) = self.inner.as_mut() {
            warnings.append(&mut inner.take_warnings());
        }
        warnings
    }
}

impl Drop for ObservedStreamCursor {
    fn drop(&mut self) {
        if self.finish.is_some() {
            self.finish(Some(
                crate::error::catalog::ER_QUERY_INTERRUPTED
                    .error("result stream closed before completion"),
            ));
        }
    }
}

struct UndoPurgeHandle {
    worker: crate::engine::background::BackgroundWorker,
    last_wake_ms: AtomicU64,
}

impl UndoPurgeHandle {
    fn disabled() -> Self {
        Self {
            worker: crate::engine::background::BackgroundWorker::disabled(),
            last_wake_ms: AtomicU64::new(0),
        }
    }

    fn spawn(db: Arc<Database>) -> Self {
        if db.is_snapshot_read_only() {
            return Self::disabled();
        }
        let worker = crate::engine::background::BackgroundWorker::spawn(
            "mydb-undo-purge",
            move |worker_shutdown| {
                // Drain-driven cadence. Each `purge_obsolete_undo()`
                // reports whether its batch filled (`true` ⇒ still behind,
                // more eligible undo pending; `false` ⇒ caught up). Per
                // wake we drain up to `DRAIN_BATCHES` full batches — bounded
                // so we periodically yield the CPU and re-read shutdown —
                // then pick the next sleep:
                //   * still behind after the drain → `MIN_INTERVAL_MS` so we
                //     come straight back and keep undo bounded under load
                //     (capacity = DRAIN_BATCHES·batch·(1000/MIN) records/s,
                //     far above any real DML rate).
                //   * caught up → back off ×1.5 toward `MAX_INTERVAL_MS` so
                //     an idle/quiet process stops paying the per-tick cost.
                // The old `undo_purge_pressure_snapshot()` signal was
                // removed: it read the undo table via `try_read()`, which a
                // write-heavy workload keeps failing, so it reported "idle"
                // exactly when purge needed to run hardest — the interval
                // ran away to 5s and undo grew without bound.
                const MIN_INTERVAL_MS: u64 = 100;
                const MAX_INTERVAL_MS: u64 = 5_000;
                const DRAIN_BATCHES: u32 = 8;
                let mut current_ms = db.undo_purge_interval_ms().max(MIN_INTERVAL_MS);
                while !worker_shutdown.is_requested() {
                    let base_ms = db.undo_purge_interval_ms().max(MIN_INTERVAL_MS);
                    // Operator dropped the interval at runtime → cap
                    // current at the new base so the loop doesn't keep
                    // backing off past the configured value.
                    if current_ms > base_ms.max(MAX_INTERVAL_MS) {
                        current_ms = base_ms;
                    }
                    if worker_shutdown.wait_timeout(Duration::from_millis(current_ms)) {
                        break;
                    }
                    db.gc_committed_txn_slots();
                    let mut behind = false;
                    for _ in 0..DRAIN_BATCHES {
                        if db.purge_obsolete_undo() {
                            behind = true;
                        } else {
                            behind = false;
                            break;
                        }
                        if worker_shutdown.is_requested() {
                            break;
                        }
                    }
                    if behind {
                        current_ms = MIN_INTERVAL_MS;
                    } else {
                        let grown = current_ms.saturating_add(current_ms / 2);
                        current_ms = grown.min(base_ms.max(MAX_INTERVAL_MS));
                    }
                }
            },
        );
        Self {
            worker,
            last_wake_ms: AtomicU64::new(0),
        }
    }

    fn shutdown(&self) {
        self.worker.shutdown();
    }

    fn wake(&self) {
        // Coalesce foreground commit notifications to at most 10/s. This
        // bounds purge/WAL contention on a c=1 autocommit stream while still
        // cutting the idle-backoff reclaim lag from 5s to roughly 100ms.
        let now = current_time_ms();
        let mut prior = self.last_wake_ms.load(AtomicOrdering::Relaxed);
        loop {
            if now.saturating_sub(prior) < 100 {
                return;
            }
            match self.last_wake_ms.compare_exchange_weak(
                prior,
                now,
                AtomicOrdering::AcqRel,
                AtomicOrdering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => prior = actual,
            }
        }
        self.worker.signal().request_wake();
    }
}

/// Background checkpointer. Statements no longer run `maybe_checkpoint`
/// inline (a triggered checkpoint used to charge its full flush + fsync
/// latency to whatever user query happened to cross the threshold, and an
/// idle process never checkpointed at all); this worker polls the
/// lock-free trigger probe once per second and runs the checkpoint off
/// the query path.
struct CheckpointerHandle {
    worker: crate::engine::background::BackgroundWorker,
}

impl CheckpointerHandle {
    fn disabled() -> Self {
        Self {
            worker: crate::engine::background::BackgroundWorker::disabled(),
        }
    }

    fn spawn(db: Arc<Database>) -> Self {
        if db.is_snapshot_read_only() {
            return Self::disabled();
        }
        let worker = crate::engine::background::BackgroundWorker::spawn(
            "mydb-checkpointer",
            move |worker_shutdown| {
                while !worker_shutdown.is_requested() {
                    if worker_shutdown.wait_timeout(Duration::from_millis(1000)) {
                        break;
                    }
                    if let Err(err) = db.maybe_checkpoint() {
                        crate::util::logging::log_message(format_args!(
                            "background checkpoint failed: {} ({})",
                            err.message(),
                            err.sql_state()
                        ));
                    }
                    // Pre-create the next WAL segment off the commit path so
                    // segment rotation is a cheap rename instead of a
                    // mutex-held file create + fsync that stalls committers.
                    if let Err(err) = db.ensure_next_wal_segment_prepared() {
                        crate::util::logging::log_message(format_args!(
                            "background WAL segment prep failed: {} ({})",
                            err.message(),
                            err.sql_state()
                        ));
                    }
                }
            },
        );
        Self { worker }
    }

    fn shutdown(&self) {
        self.worker.shutdown();
    }
}

enum ScannedTextDmlProbe {
    NotApplicable,
    /// The scanner candidate was rejected by the authoritative parser or the
    /// prepared-DML compiler. Reuse this AST in the ordinary dispatcher
    /// instead of parsing the identical COM_QUERY a second time.
    Parsed(Statement),
    Executed(Result<QueryResponse, QueryError>, QueryLogMetadata),
}

pub(crate) fn prepared_sparse_batch_hits() -> u64 {
    prepared_dml::sparse_point_batch_hits()
}

impl StorageQueryEngine {
    fn execute_with_operation(
        &self,
        session: &mut SessionContext,
        sql: &str,
        finish_diagnostics: bool,
        operation: impl FnOnce(
            &mut SessionContext,
        ) -> (Result<QueryResponse, QueryError>, QueryLogMetadata),
    ) -> Result<QueryResponse, QueryError> {
        let _reclaim_scope = mem_budget::ReclaimDatabaseScope::enter(&self.db);
        let _http_dependent = crate::engine::http::may_call(&self.db, sql);
        session.check_cancelled()?;
        self.db.ensure_operational()?;
        let _redo_scope = crate::storage::RedoScopeGuard::new(session.redo_enabled);
        let trace_enabled = session
            .sysvars
            .optimizer_trace
            .to_ascii_lowercase()
            .contains("enabled=on");
        let _trace_session =
            crate::engine::cbo::trace::activate(session.connection_id, trace_enabled);
        crate::engine::cbo::trace::begin(sql);
        let preserve_warnings = begin_statement_diagnostics(session, sql);
        let warning_scope = crate::engine::api::StatementWarningSinkScope::enter();
        let _executor_tuning = crate::engine::executor::tuning::enter(
            crate::engine::executor::tuning::ExecutorTuningSnapshot::capture_for_session(
                session.sysvars.mydb_parallel_degree,
            ),
        );
        let started = Instant::now();
        let _http_scope = crate::engine::http::enter(&self.db);
        if _http_dependent {
            crate::engine::http::activate();
        }
        let _cancel_guard = crate::engine::cancel::install_session_cancel_context(session);
        crate::engine::cancel::set_process_base_state(process_state_for_sql(sql));
        // LOAD_FILE is evaluated only on this connection thread and reads the
        // live authorization/session state (including routine definer
        // switches). The guard restores a nested statement's previous
        // context on every exit path.
        // SAFETY: db/session outlive this statement, including the constant
        // SELECT lane that bypasses dispatch. Sequence calls remain serial.
        let sequence_dependent = crate::planner::sql_mentions_sequence_fn(sql);
        let _sequence_ctx = unsafe {
            crate::engine::expr::install_sequence_eval_context(
                std::ptr::NonNull::from(self.db.as_ref()),
                std::ptr::NonNull::from(&*session),
            )
        };
        let _load_file_ctx = unsafe {
            crate::engine::expr::install_load_file_eval_context(
                std::ptr::NonNull::from(self.db.as_ref()),
                std::ptr::NonNull::from(&*session),
            )
        };
        // Install the parser sql_mode view for the whole statement lifetime
        // so nested parses (view expansion, prepared re-parse, multi-
        // statement `parse_statements`) see ANSI_QUOTES / NO_BACKSLASH_ESCAPES
        // consistently with the session state.
        let _parser_mode_guard = crate::parser::ParserSqlModeGuard::activate_effective_session(
            crate::engine::api::parse_temporal_sql_mode_bits(&session.sql_mode),
        );
        // `nextval`/`setval` mutate per-session + durable state and must be
        // evaluated on the connection thread — never a parallel worker. Force
        // the statement serial when it references a sequence function.
        let _force_serial = (sequence_dependent
            || crate::planner::sql_mentions_user_var_assign(sql)
            || crate::planner::sql_mentions_load_file_fn(sql)
            || crate::engine::http::may_call(&self.db, sql))
        .then(crate::planner::force_serial_scope);
        // Publish the session's `group_concat_max_len` to the
        // thread-local that `GroupConcatAcc::new` reads. Scoped via
        // a guard so the value never leaks across statements.
        let group_concat_max_len = session.group_concat_max_len;
        let (result, metadata) =
            crate::engine::executor::select::aggregate::with_group_concat_max_len(
                group_concat_max_len,
                || operation(session),
            );
        crate::engine::cbo::trace::complete_statement();
        // Sequence state belongs to the live connection. A deferred cursor
        // must not outlive that state or evaluate on a worker's copied session.
        let result = if sequence_dependent {
            result.and_then(QueryResponse::materialize)
        } else {
            result
        };
        let result =
            result.and_then(|response| response.localize_timestamps(&session.sysvars.time_zone));
        if metadata.wake_purge && result.is_ok() {
            self.undo_purge_worker.wake();
        }
        cap_direct_group_concat_limit_warnings(sql, &result);
        if result.is_ok() && finish_diagnostics {
            // Source-syntax diagnostics belong to parsing/preparation and
            // therefore precede DDL conversion or execution warnings in
            // MySQL's condition area. We discover them after success so a
            // rejected statement cannot leak deprecation warnings; splice
            // them in front of warnings already produced by execution.
            let execution_warnings = crate::engine::api::take_statement_warnings();
            warn_successful_source_syntax(sql);
            let syntax_warnings = crate::engine::api::take_statement_warnings();
            crate::engine::api::append_statement_warnings(syntax_warnings);
            crate::engine::api::append_statement_warnings(execution_warnings);
        }
        if !matches!(result, Ok(QueryResponse::StreamingResultSet(_))) {
            warning_scope.drain_to_local();
        }
        drop(_cancel_guard);
        if finish_diagnostics {
            finish_statement_diagnostics(session, preserve_warnings, &result);
        }
        self.finish_or_observe_query(
            session,
            sql,
            started,
            metadata,
            result,
            text_dml_digest_shape(sql).map(Arc::from),
        )
    }

    /// Test-only accessor for the underlying [`Database`]. Lets
    /// integration tests probe diagnostic state (e.g. the visibility
    /// map's all-visible page count) without going through SQL.
    #[cfg(test)]
    pub(crate) fn database(&self) -> &Arc<Database> {
        &self.db
    }

    /// Internal replication fast path accessor. The binlog applier needs
    /// storage-level DML so row events do not get re-stringified into SQL
    /// or accidentally fire SQL-layer side effects twice. Also used by the
    /// mydb-native physical-replication applier in `src/repl/replica/` to
    /// drive `Database::apply_wal_record` once a frame has been written to
    /// the local WAL.
    pub fn database_for_replication(&self) -> &Arc<Database> {
        &self.db
    }

    pub(crate) fn session_registry(&self) -> &Arc<SessionRegistry> {
        &self.registry
    }

    /// Public DB handle for connection-layer callers (COM_FIELD_LIST etc.)
    /// that need to read schemas/catalog without spinning a full SQL
    /// dispatch. Mirrors the legacy `database_for_replication` accessor.
    pub fn db_handle(&self) -> &Arc<Database> {
        &self.db
    }
}

/// COM_FIELD_LIST wildcard matcher — ASCII case-insensitive LIKE with
/// only `%` and `_` (no ESCAPE clause, matching MySQL's mysql_list_fields
/// surface). Used to filter the columns COM_FIELD_LIST returns.
fn field_list_like_match(pattern: &[u8], value: &[u8]) -> bool {
    match pattern.first() {
        None => value.is_empty(),
        Some(&b'%') => {
            let rest = &pattern[1..];
            field_list_like_match(rest, value)
                || (!value.is_empty() && field_list_like_match(pattern, &value[1..]))
        }
        Some(&b'_') => !value.is_empty() && field_list_like_match(&pattern[1..], &value[1..]),
        Some(&p) => {
            !value.is_empty()
                && p.eq_ignore_ascii_case(&value[0])
                && field_list_like_match(&pattern[1..], &value[1..])
        }
    }
}

// `field_list_columns` is now implemented as a trait method on
// `QueryEngine for StorageQueryEngine` (further down the file). Keeping
// it on the trait lets the connection layer reach it through the
// `Arc<E>` engine handle and lets test doubles fall back to the empty
// default.

fn statement_cache_capacity_from_env() -> usize {
    crate::util::env::var("MYDB_PLAN_CACHE_SIZE")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(4096)
}

fn require_secure_transport() -> bool {
    static REQUIRE: OnceLock<bool> = OnceLock::new();
    *REQUIRE.get_or_init(|| {
        crate::util::env::var("MYDB_REQUIRE_SECURE_TRANSPORT")
            .map(|raw| matches!(raw.as_str(), "1" | "true" | "TRUE" | "on" | "ON"))
            .unwrap_or(false)
    })
}

fn enforce_tls_requirement(
    requirement: &SslRequirement,
    request: &AuthRequest<'_>,
) -> Result<(), QueryError> {
    if require_secure_transport() && !request.tls_established {
        return Err(crate::mysql::errors::error(
            crate::mysql::errors::ER_SECURE_TRANSPORT_REQUIRED,
            "Connections using insecure transport are prohibited",
        ));
    }
    match requirement {
        SslRequirement::None => Ok(()),
        SslRequirement::Ssl => {
            if request.tls_established {
                Ok(())
            } else {
                Err(crate::mysql::errors::error(
                    crate::mysql::errors::ER_SECURE_TRANSPORT_REQUIRED,
                    "Connections using insecure transport are prohibited while account requires SSL",
                ))
            }
        }
        SslRequirement::X509 => {
            if request.tls_established && request.x509_subject.is_some() {
                Ok(())
            } else {
                Err(crate::mysql::errors::error(
                    crate::mysql::errors::ER_ACCESS_DENIED_ERROR,
                    format!(
                        "Access denied for user '{}': account requires a valid X509 client certificate",
                        request.username
                    ),
                ))
            }
        }
        SslRequirement::Specified {
            cipher,
            issuer,
            subject,
        } => {
            enforce_tls_requirement(&SslRequirement::X509, request)?;
            if let Some(required) = cipher {
                if !request
                    .tls_cipher
                    .is_some_and(|actual| actual.eq_ignore_ascii_case(required))
                {
                    return Err(access_denied(request.username, request.host));
                }
            }
            if let Some(required) = issuer {
                if !request.x509_issuer.is_some_and(|actual| actual == required) {
                    return Err(access_denied(request.username, request.host));
                }
            }
            if let Some(required) = subject {
                if !request
                    .x509_subject
                    .is_some_and(|actual| actual == required)
                {
                    return Err(access_denied(request.username, request.host));
                }
            }
            Ok(())
        }
    }
}

fn spawn_event_scheduler_if_enabled(
    db: &Arc<Database>,
    registry: &Arc<SessionRegistry>,
) -> Option<self::events::EventSchedulerHandle> {
    // G10: honour the start-time MYDB_EVENT_SCHEDULER setting before
    // deciding whether to run a worker (default remains ON).
    if db.is_snapshot_read_only() {
        return None;
    }
    self::events::apply_startup_state_from_env();
    if self::events::scheduler_state() == self::events::EventSchedulerState::Disabled {
        return None;
    }
    Some(self::events::EventSchedulerHandle::spawn(
        Arc::clone(db),
        Arc::clone(registry),
    ))
}

fn build_caches() -> (
    Arc<crate::engine::cache::StatementCache>,
    Arc<crate::engine::cache::PlanCache>,
    Arc<crate::engine::cache::PreparedDmlPlanCache>,
    Arc<crate::engine::cache::PreparedDmlBatchCache>,
) {
    let cap = statement_cache_capacity_from_env();
    let stmt_cache = Arc::new(crate::engine::cache::StatementCache::new(cap));
    // Parsing is independent of catalog state. Plans and compiled DML share a
    // separate epoch so DDL invalidates executable artifacts without flushing
    // exact-source ASTs and forcing every connection to tokenize again.
    let plan_cache = Arc::new(crate::engine::cache::PlanCache::new(cap));
    let epoch = plan_cache.epoch_handle();
    let prepared_dml_cache = Arc::new(crate::engine::cache::PreparedDmlPlanCache::with_epoch(
        cap,
        Arc::clone(&epoch),
    ));
    let prepared_dml_batch_cache = Arc::new(
        crate::engine::cache::PreparedDmlBatchCache::with_epoch(cap, epoch),
    );
    crate::engine::cache::register_global(&stmt_cache);
    crate::engine::cache::register_global_plan(&plan_cache);
    crate::engine::cache::register_global_prepared_dml(&prepared_dml_cache);
    crate::engine::cache::register_global_prepared_dml_batch(&prepared_dml_batch_cache);
    (
        stmt_cache,
        plan_cache,
        prepared_dml_cache,
        prepared_dml_batch_cache,
    )
}

impl StorageQueryEngine {
    #[allow(dead_code)]
    pub fn open(data_dir: impl Into<PathBuf>) -> Result<Self, QueryError> {
        Self::open_with_log_config(data_dir, QueryLogConfig::default())
    }

    pub fn open_with_log_config(
        data_dir: impl Into<PathBuf>,
        log_config: QueryLogConfig,
    ) -> Result<Self, QueryError> {
        Self::open_full(data_dir, log_config, Arc::new(SessionRegistry::new()))
    }

    pub fn open_full(
        data_dir: impl Into<PathBuf>,
        log_config: QueryLogConfig,
        registry: Arc<SessionRegistry>,
    ) -> Result<Self, QueryError> {
        Self::open_full_with_startup_options(data_dir, log_config, registry, Vec::new())
    }

    pub fn open_full_with_startup_options(
        data_dir: impl Into<PathBuf>,
        log_config: QueryLogConfig,
        registry: Arc<SessionRegistry>,
        startup_sysvars: Vec<crate::startup_config::StartupSysVarAssignment>,
    ) -> Result<Self, QueryError> {
        Self::open_full_options(data_dir, log_config, registry, startup_sysvars, None)
    }

    pub fn open_backup_image(
        data_dir: impl Into<PathBuf>,
        runtime: PathBuf,
        log_config: QueryLogConfig,
        registry: Arc<SessionRegistry>,
        startup_sysvars: Vec<crate::startup_config::StartupSysVarAssignment>,
    ) -> Result<Self, QueryError> {
        Self::open_full_options(
            data_dir,
            log_config,
            registry,
            startup_sysvars,
            Some(runtime),
        )
    }

    fn open_full_options(
        data_dir: impl Into<PathBuf>,
        log_config: QueryLogConfig,
        registry: Arc<SessionRegistry>,
        startup_sysvars: Vec<crate::startup_config::StartupSysVarAssignment>,
        image_runtime: Option<PathBuf>,
    ) -> Result<Self, QueryError> {
        let (statement_cache, plan_cache, prepared_dml_cache, prepared_dml_batch_cache) =
            build_caches();
        let plan_cache_enabled = plan_cache_enabled_from_env();
        let data_dir = data_dir.into();
        let has_backup_state = data_dir.join("system/backup-control").exists()
            || data_dir.join("system/wal-retention.json").exists()
            || data_dir.join("system/wal-retention.id").exists();
        let _backup_startup = if has_backup_state && !crate::storage::image::is_snapshot(&data_dir)?
        {
            Some(crate::backup::prepare_archive_retention(&data_dir)?)
        } else {
            None
        };
        let db = Arc::new(if let Some(runtime) = image_runtime {
            Database::open_image(data_dir.clone(), runtime)?
        } else {
            Database::open(data_dir.clone())?
        });
        let runtime_dir = crate::storage::image_io::runtime_dir(db.data_dir()).unwrap_or(data_dir);
        let audit_path = runtime_dir.clone();
        let sql_log = SqlLogService::open(runtime_dir, &log_config)?;
        // Explicit option-file settings are lower priority than SET PERSIST.
        crate::engine::sysvar::registry::apply_option_file_on_startup(
            db.as_ref(),
            &startup_sysvars,
        )?;
        // Apply `SET PERSIST` / `SET PERSIST_ONLY` entries from the
        // on-disk file as GLOBAL writes BEFORE we accept connections, so
        // every session born after this point sees the persisted globals.
        crate::engine::sysvar::apply_persisted_on_startup(db.as_ref());
        // P0-A2 audit log: enable when MYDB_AUDIT_LOG is set. The path
        // defaults to <data_dir>/logs/audit.jsonl when the env var is
        // literally "on"; otherwise the value is treated as the audit
        // log file path.
        let _ = crate::engine::audit_log::enable_audit_log(audit_path.as_path());
        let event_scheduler = spawn_event_scheduler_if_enabled(&db, &registry);
        let undo_purge_worker = UndoPurgeHandle::spawn(Arc::clone(&db));
        let checkpointer = CheckpointerHandle::spawn(Arc::clone(&db));
        let memory_pressure_worker = mem_budget::spawn_memory_pressure_worker(Arc::clone(&db));
        let auto_analyze_worker =
            crate::engine::auto_analyze::AutoAnalyzeHandle::spawn(Arc::clone(&db));
        let zone_map_maintenance_worker =
            crate::engine::zone_map_maintenance::ZoneMapMaintenanceHandle::spawn(Arc::clone(&db));
        let vector_maintenance_worker =
            crate::engine::vector_maintenance::VectorMaintenanceHandle::spawn(Arc::clone(&db));
        Ok(Self {
            db,
            log_config,
            sql_log,
            registry,
            statement_cache,
            plan_cache_enabled,
            plan_cache,
            prepared_dml_cache,
            prepared_dml_batch_cache,
            event_scheduler,
            undo_purge_worker,
            checkpointer,
            memory_pressure_worker,
            auto_analyze_worker,
            zone_map_maintenance_worker,
            vector_maintenance_worker,
        })
    }

    #[cfg(test)]
    pub(crate) fn open_with_storage_config(
        data_dir: impl Into<PathBuf>,
        storage_config: StorageConfig,
    ) -> Result<Self, QueryError> {
        Self::open_with_storage_config_and_workers(data_dir, storage_config, true)
    }

    /// Opens the regular SQL engine while omitting idle production background
    /// workers. Most executor tests drive maintenance synchronously and each
    /// nextest case runs in its own process, so spawning five sleeping threads
    /// per fixture only adds process/thread setup and teardown contention.
    #[cfg(test)]
    pub(crate) fn open_with_storage_config_without_background_workers(
        data_dir: impl Into<PathBuf>,
        storage_config: StorageConfig,
    ) -> Result<Self, QueryError> {
        Self::open_with_storage_config_and_workers(data_dir, storage_config, false)
    }

    #[cfg(test)]
    fn open_with_storage_config_and_workers(
        data_dir: impl Into<PathBuf>,
        storage_config: StorageConfig,
        background_workers: bool,
    ) -> Result<Self, QueryError> {
        let (statement_cache, plan_cache, prepared_dml_cache, prepared_dml_batch_cache) =
            build_caches();
        let plan_cache_enabled = plan_cache_enabled_from_env();
        let data_dir = data_dir.into();
        let db = Arc::new(Database::open_with_config(
            data_dir.clone(),
            storage_config,
        )?);
        let log_config = QueryLogConfig::default();
        let audit_path = data_dir.clone();
        let sql_log = SqlLogService::open(data_dir, &log_config)?;
        crate::engine::sysvar::apply_persisted_on_startup(db.as_ref());
        let _ = crate::engine::audit_log::enable_audit_log(audit_path.as_path());
        let registry = Arc::new(SessionRegistry::new());
        let event_scheduler = background_workers
            .then(|| spawn_event_scheduler_if_enabled(&db, &registry))
            .flatten();
        let memory_pressure_worker = if background_workers {
            mem_budget::spawn_memory_pressure_worker(Arc::clone(&db))
        } else {
            crate::engine::background::BackgroundWorker::disabled()
        };
        let (
            undo_purge_worker,
            checkpointer,
            auto_analyze_worker,
            zone_map_maintenance_worker,
            vector_maintenance_worker,
        ) = if background_workers {
            (
                UndoPurgeHandle::spawn(Arc::clone(&db)),
                CheckpointerHandle::spawn(Arc::clone(&db)),
                crate::engine::auto_analyze::AutoAnalyzeHandle::spawn(Arc::clone(&db)),
                crate::engine::zone_map_maintenance::ZoneMapMaintenanceHandle::spawn(Arc::clone(
                    &db,
                )),
                crate::engine::vector_maintenance::VectorMaintenanceHandle::spawn(Arc::clone(&db)),
            )
        } else {
            (
                UndoPurgeHandle::disabled(),
                CheckpointerHandle::disabled(),
                crate::engine::auto_analyze::AutoAnalyzeHandle::disabled(),
                crate::engine::zone_map_maintenance::ZoneMapMaintenanceHandle::disabled(),
                crate::engine::vector_maintenance::VectorMaintenanceHandle::disabled(),
            )
        };
        Ok(Self {
            db,
            log_config,
            sql_log,
            registry,
            statement_cache,
            plan_cache_enabled,
            plan_cache,
            prepared_dml_cache,
            prepared_dml_batch_cache,
            event_scheduler,
            undo_purge_worker,
            checkpointer,
            memory_pressure_worker,
            auto_analyze_worker,
            zone_map_maintenance_worker,
            vector_maintenance_worker,
        })
    }

    #[cfg(test)]
    pub fn statement_cache(&self) -> &Arc<crate::engine::cache::StatementCache> {
        &self.statement_cache
    }

    #[cfg(test)]
    pub fn plan_cache(&self) -> &Arc<crate::engine::cache::PlanCache> {
        &self.plan_cache
    }

    #[cfg(test)]
    pub(crate) fn prepared_dml_batch_cache_stats_for_tests(
        &self,
    ) -> crate::engine::cache::PlanCacheStats {
        self.prepared_dml_batch_cache.stats()
    }

    #[cfg(test)]
    pub(crate) fn db_for_tests(&self) -> &Arc<Database> {
        &self.db
    }

    #[cfg(test)]
    pub(crate) fn sql_log_for_tests(&self) -> &Arc<SqlLogService> {
        &self.sql_log
    }

    pub fn registry(&self) -> &Arc<SessionRegistry> {
        &self.registry
    }

    #[allow(dead_code)]
    pub fn checkpoint(&self) -> Result<(), QueryError> {
        self.db.checkpoint()
    }

    fn flush_sql_cache(&self) {
        crate::engine::cache::flush_sql_caches(
            Some(&self.statement_cache),
            Some(&self.plan_cache),
            Some(&self.prepared_dml_cache),
            Some(&self.prepared_dml_batch_cache),
        );
    }

    fn execute_alter_system_statement(
        &self,
        session: &SessionContext,
        statement: &Statement,
    ) -> Option<Result<QueryResponse, QueryError>> {
        if self.db.is_snapshot_read_only()
            && matches!(
                statement,
                Statement::AlterSystemCheckpoint
                    | Statement::AlterSystemFlushDataCache
                    | Statement::AlterSystemFlushSqlCache
            )
        {
            return Some(Err(crate::storage::image_io::read_only_error()));
        }
        let result = match statement {
            Statement::AlterSystemCheckpoint => authorize_statement(&self.db, session, statement)
                .and_then(|_| self.db.checkpoint())
                .map(|()| QueryResponse::Ok),
            Statement::AlterSystemFlushDataCache => {
                authorize_statement(&self.db, session, statement)
                    .and_then(|_| self.db.flush_data_cache())
                    .map(|()| QueryResponse::Ok)
            }
            Statement::AlterSystemFlushSqlCache => {
                authorize_statement(&self.db, session, statement).map(|()| {
                    self.flush_sql_cache();
                    QueryResponse::Ok
                })
            }
            _ => return None,
        };
        Some(result)
    }

    pub fn shutdown(&self) -> Result<(), QueryError> {
        crate::engine::http::shutdown_database(&self.db)?;
        crate::engine::ai::runtime::shutdown_database(&self.db)?;
        crate::backup::shutdown_jobs(&self.db)?;
        // No background writer may race the final checkpoint. In
        // particular, auto-analyze / zone-map maintenance / undo purge can
        // append WAL without owning a client connection, so draining the
        // MySQL connection count alone is not a quiescence barrier.
        if let Some(scheduler) = &self.event_scheduler {
            scheduler.shutdown();
        }
        self.auto_analyze_worker.shutdown();
        self.zone_map_maintenance_worker.shutdown();
        self.vector_maintenance_worker.shutdown();
        self.undo_purge_worker.shutdown();
        self.memory_pressure_worker.shutdown();
        self.checkpointer.shutdown();
        crate::backup::shutdown_archiver(&self.db)?;

        // Maintenance that can append WAL is ordered before CheckpointBegin.
        // Drain any residual deferred-pinned/re-dirtied pages with bounded
        // sharp passes so graceful shutdown leaves a self-contained recovery
        // point instead of a recovery tail too large to scan under the process
        // memory target.
        self.db.checkpoint_for_shutdown()
    }

    pub fn close_session(&self, session: &mut SessionContext) -> Result<(), QueryError> {
        let cleanup = self.reset_session(session);
        // P0-D1: release the per-account simultaneous-session counter only on
        // real disconnect/change-user. COM_RESET_CONNECTION keeps the same
        // authenticated connection and therefore must not release/re-charge.
        if !session.internal_system_session {
            crate::engine::auth::release_connection(&session.authenticated_as);
        }
        cleanup
    }

    pub fn reset_session(&self, session: &mut SessionContext) -> Result<(), QueryError> {
        *session.mdl_fast_cache.borrow_mut() = Default::default();
        session.clear_sql_prepared_statements();
        let held_global_read_lock = crate::binlog::unlock_tables_for_session_scoped(
            session,
            self.db.global_read_lock_scope(),
        );
        let simple = session.current_txn.is_none()
            && !session.explicit_txn
            && session.xa_branch.is_none()
            && session.temp_tables.is_empty()
            && session.explicit_table_locks.is_empty()
            && session.mdl_txn_grants.borrow().1.is_empty()
            && !held_global_read_lock;
        if simple {
            // Pending MDL requests live in the global queue, not mdl_txn_grants.
            self.db
                .clear_mdl_waits_for_owner(session.connection_id as u64);
            // The common connection-pool/short-connect teardown owns no
            // storage resources. Avoid taking the transaction, MDL, global
            // read-lock and temp-catalog mutexes merely to discover that
            // every map is empty; account quota release remains in
            // `close_session` below.
            crate::engine::expr::release_all_user_locks(session.connection_id);
            session.set_current_txn(None);
            session.spare_txn_runtime = None;
            session.clear_transaction_snapshot();
            session.explicit_txn = false;
            session.read_only_txn = false;
            session.clear_active_transaction_isolation();
            return Ok(());
        }
        let rollback = rollback_session_txn(&self.db, session);
        // XA END detaches current_txn but leaves an unprepared branch owned
        // by this session. Roll it back independently: a later statement may
        // also have opened a different current_txn. PREPARED branches have
        // already left xa_branch and must survive connection cleanup.
        let xa_rollback = match session.xa_branch.as_ref() {
            Some(branch) if self.db.is_txn_active(branch.txn_id) => {
                self.db.rollback_txn(branch.txn_id)
            }
            _ => Ok(()),
        };
        if xa_rollback.is_ok() {
            if let Some(branch) = &session.xa_branch {
                self.db.release_xa(&branch.xid, branch.txn_id);
            }
            session.xa_branch = None;
        }
        session.explicit_table_locks.clear();
        self.db.release_mdl_owner(session.connection_id as u64);
        crate::binlog::unlock_tables_for_session_scoped(session, self.db.global_read_lock_scope());
        crate::engine::expr::release_all_user_locks(session.connection_id);
        let temporary_tables = close_temp_tables(&self.db, session);
        session.spare_txn_runtime = None;
        rollback.and(xa_rollback).and(temporary_tables)
    }
}

impl QueryEngine for StorageQueryEngine {
    fn sql_may_call_http(&self, sql: &str) -> bool {
        crate::engine::http::may_call(&self.db, sql)
    }

    fn enter_memory_reclaim_scope(&self) -> Option<mem_budget::ReclaimDatabaseScope> {
        Some(mem_budget::ReclaimDatabaseScope::enter(&self.db))
    }

    fn reclaim_connection_admission_memory(
        &self,
        requested_bytes: usize,
    ) -> Result<(), QueryError> {
        crate::engine::executor::mem_budget::reclaim_for_connection_admission(
            &self.db,
            requested_bytes,
        )
    }

    fn row_lock_async_signal(&self) -> Option<crate::engine::api::AsyncRowLockSignal> {
        let target = crate::storage::take_nonblocking_row_wait_target()?;
        Some(self.db.subscribe_row_lock_async_signal(target))
    }

    fn execute(
        &self,
        session: &mut SessionContext,
        sql: &str,
    ) -> Result<QueryResponse, QueryError> {
        self.execute_with_operation(session, sql, true, |session| {
            self.execute_with_log_metadata(session, sql)
        })
    }

    fn execute_text_dml_batch(
        &self,
        session: &mut SessionContext,
        sql: &str,
    ) -> Option<(
        Vec<crate::engine::api::DmlStatementResult>,
        Result<QueryResponse, QueryError>,
    )> {
        if crate::engine::http::may_call(&self.db, sql) {
            return None;
        }
        // The compiled batch bypasses execute_with_log_metadata, which normally
        // installs the session timeout for the coalesced row-lock acquisition.
        let _lock_wait = crate::storage::LockWaitTimeoutGuard::new(Some(
            session
                .sysvars
                .innodb_lock_wait_timeout
                .unwrap_or_else(crate::engine::api::global_innodb_lock_wait_timeout),
        ));
        let _reclaim = mem_budget::ReclaimDatabaseScope::enter(&self.db);
        if source_syntax_warning_scan_required(
            sql,
            crate::engine::api::parse_temporal_sql_mode_bits(&session.sql_mode),
        ) {
            return None;
        }
        let _mode = crate::parser::ParserSqlModeGuard::activate_effective_session(
            crate::engine::api::parse_temporal_sql_mode_bits(&session.sql_mode),
        );
        let (shape, params) = scan_literal_normalized_dml(sql, true)?;
        let batch = self.prepare_text_dml_batch(session, sql, shape, &params)?;
        let mut statements = Vec::with_capacity(batch.plans.len());
        let result = self.execute_with_operation(session, sql, false, |session| {
            self.execute_text_prepared_dml_batch(session, sql, params, batch, Some(&mut statements))
        });
        if result.is_err() {
            finish_statement_diagnostics(session, false, &result);
        }
        Some((statements, result))
    }

    fn describe_prepared_statement(
        &self,
        session: &mut SessionContext,
        sql: &str,
    ) -> Result<Vec<Column>, QueryError> {
        StorageQueryEngine::describe_prepared_statement(self, session, sql)
    }

    fn shutdown(&self) -> Result<(), QueryError> {
        StorageQueryEngine::shutdown(self)
    }

    fn prepare_statement_template(
        &self,
        session: &mut SessionContext,
        _sql: &str,
        mut statement: Statement,
    ) -> Result<Statement, QueryError> {
        let _reclaim_scope = mem_budget::ReclaimDatabaseScope::enter(&self.db);
        if self.plan_cache_enabled {
            self.db.record_plan_cache_miss();
        }
        dispatch::ai::validate_shape(&statement)?;
        if let Statement::Ai(command) = &statement {
            if command.kind == "secret" && command.value.is_some() {
                if session.ssl_cipher.is_none() {
                    return Err(crate::engine::ai::error(
                        "ER_AI_ACCESS_DENIED",
                        "Secret VALUE requires TLS before PREPARE",
                    ));
                }
                if !matches!(command.value, Some(Expr::Placeholder(_))) {
                    return Err(crate::engine::ai::error(
                        "ER_AI_INVALID_ARGUMENT",
                        "prepared Secret DDL requires VALUE ?",
                    ));
                }
            }
        }
        // Templates bind the database, never a temporary table's physical key.
        // EXECUTE resolves the logical name against the current session registry.
        if session.temp_tables.is_empty() {
            qualify_statement_tables(&self.db, session, &mut statement)?;
        } else {
            let mut context = SessionContext::default();
            context.current_database = session.current_database.clone();
            qualify_statement_tables(&self.db, &context, &mut statement)?;
        }
        Ok(statement)
    }

    fn parse_cached_statement(
        &self,
        session: &SessionContext,
        sql: &str,
    ) -> Result<Arc<Statement>, QueryError> {
        let _reclaim_scope = mem_budget::ReclaimDatabaseScope::enter(&self.db);
        let parse_bits = crate::engine::api::parse_temporal_sql_mode_bits(&session.sql_mode);
        if crate::engine::ai::sql::sql_may_have_ai(sql) {
            let _mode = crate::parser::ParserSqlModeGuard::activate(parse_bits);
            return parse_statement(sql)
                .map(Arc::new)
                .map_err(parse_error_to_query_error);
        }
        let key = crate::engine::cache::ParsedStatementKey::new(sql, parse_bits);
        if let Some(ast) = self.statement_cache.get(&key) {
            self.db.record_select_parse_cache_hit();
            return Ok(ast);
        }
        let parsed = {
            let _mode_guard = crate::parser::ParserSqlModeGuard::activate(parse_bits);
            let source = crate::parser::strip_comments_in_explain_header(sql);
            parse_statement(source.as_ref()).map_err(parse_error_to_query_error)?
        };
        Ok(self.statement_cache.insert(key, Arc::new(parsed)))
    }

    fn record_select_parse_cache_hit(&self) {
        self.db.record_select_parse_cache_hit();
    }

    fn prepare_cached(
        &self,
        session: &mut SessionContext,
        sql: &str,
        expected_param_count: usize,
    ) -> Result<Arc<Statement>, QueryError> {
        let _reclaim_scope = mem_budget::ReclaimDatabaseScope::enter(&self.db);
        let _http_dependent = crate::engine::http::may_call(&self.db, sql);
        session.check_cancelled()?;
        // The parser cache owns an unqualified immutable AST. Qualification is
        // intentionally repeated at PREPARE time so temporary-table shadowing
        // and the current database can never leak across sessions.
        let mut parsed = (*self.parse_cached_statement(session, sql)?).clone();
        let mut next = 0;
        crate::parser::number_statement_placeholders(&mut parsed, &mut next);
        if next != expected_param_count {
            return Err(crate::mysql::errors::error(
                crate::mysql::errors::ER_WRONG_ARGUMENTS,
                format!(
                    "placeholder count mismatch: parsed {next}, expected {expected_param_count}"
                ),
            ));
        }
        let _http_scope = crate::engine::http::enter(&self.db);
        if _http_dependent {
            crate::engine::http::activate();
        }
        let _cancel_guard = crate::engine::cancel::install_session_cancel_context(session);
        crate::engine::cancel::set_process_base_state(
            crate::engine::session_registry::ProcessState::Preparing,
        );
        let qualified = self.prepare_statement_template(session, sql, parsed)?;
        drop(_cancel_guard);
        Ok(Arc::new(qualified))
    }

    fn try_prepare_cached_inline(
        &self,
        session: &mut SessionContext,
        sql: &str,
        expected_param_count: usize,
    ) -> Option<Result<(Arc<Statement>, Vec<Column>), QueryError>> {
        // Bound work on the I/O worker. A miss uses the blocking prepare job;
        // only small, already-parsed DML with no result columns is eligible.
        if sql.len() > 4096 {
            return None;
        }
        let parse_bits = crate::engine::api::parse_temporal_sql_mode_bits(&session.sql_mode);
        let key = crate::engine::cache::ParsedStatementKey::new(sql, parse_bits);
        let parsed = self.statement_cache.get(&key)?;
        let no_result = match parsed.as_ref() {
            Statement::Insert(stmt) => stmt.returning.is_none(),
            Statement::Update(stmt) => stmt.returning.is_none(),
            Statement::Delete(stmt) => stmt.returning.is_none(),
            _ => false,
        };
        if !no_result {
            return None;
        }
        Some((|| {
            session.check_cancelled()?;
            let mut parsed = (*parsed).clone();
            let mut count = 0;
            crate::parser::number_statement_placeholders(&mut parsed, &mut count);
            if count != expected_param_count {
                return Err(crate::mysql::errors::error(
                    crate::mysql::errors::ER_WRONG_ARGUMENTS,
                    format!("placeholder count mismatch: parsed {count}, expected {expected_param_count}"),
                ));
            }
            let _http_scope = crate::engine::http::enter(&self.db);
            let _cancel_guard = crate::engine::cancel::install_session_cancel_context(session);
            crate::engine::cancel::set_process_base_state(
                crate::engine::session_registry::ProcessState::Preparing,
            );
            self.db.record_select_parse_cache_hit();
            let qualified = self.prepare_statement_template(session, sql, parsed)?;
            Ok((Arc::new(qualified), Vec::new()))
        })())
    }

    fn invalidate_statement_cache(&self) {
        // Compatibility hook name retained for protocol callers. DDL affects
        // compiled artifacts, not the unqualified exact-source parse cache.
        self.plan_cache.bump_epoch();
    }

    fn notify_lock_waiters_for_kill(&self) {
        self.db.notify_lock_waiters();
    }

    fn statement_cache_stats(&self) -> Option<crate::engine::cache::CacheStats> {
        Some(self.statement_cache.stats())
    }

    fn log_general_event(&self, session: &SessionContext, command_type: &str, argument: &str) {
        self.sql_log.record_general(session, command_type, argument);
    }

    fn flush_logs(&self, kind: crate::engine::logging::FlushLogKind) -> Result<(), QueryError> {
        self.sql_log.flush(kind)
    }

    fn authenticate(
        &self,
        username: &str,
        host: &str,
        auth_response: &[u8],
        auth_plugin: Option<&str>,
    ) -> Result<AuthorizationContext, QueryError> {
        self.authenticate_request(AuthRequest::legacy(
            username,
            host,
            auth_response,
            auth_plugin,
        ))
        .map(|outcome| outcome.context)
    }

    fn auth_plugin_for_user(
        &self,
        username: &str,
        host: &str,
    ) -> Result<Option<String>, QueryError> {
        self.db.with_auth_catalog(|auth| {
            auth.match_account(username, host)
                .map(|account| account.plugin.clone())
        })
    }

    /// Plaintext-password verify path, used by `caching_sha2_password`
    /// full authentication after RSA decrypt (N1/N2/N6). We recompute the
    /// plugin-specific stored-hash form of the plaintext and constant-time
    /// compare to the catalog entry. Falls back to the secondary auth
    /// string (L3 retain). Lock / lifetime / failed-login bookkeeping
    /// mirrors the fast-path in `authenticate_request`.
    fn authenticate_with_plaintext_password(
        &self,
        username: &str,
        host: &str,
        plaintext: &str,
        request: &AuthRequest<'_>,
    ) -> Result<AuthOutcome, QueryError> {
        let login = login::LoginAccount::load(&self.db, username, host)?;
        let account = &login.account;

        enforce_tls_requirement(&account.ssl_type, request)?;

        let primary_ok = verify_plaintext_auth_string(
            &account.plugin,
            &account.authentication_string,
            plaintext,
        );
        let secondary_ok = !primary_ok
            && account
                .secondary_authentication_string
                .as_ref()
                .is_some_and(|secondary| {
                    verify_plaintext_auth_string(&account.plugin, secondary, plaintext)
                });
        if !primary_ok && !secondary_ok {
            crate::engine::auth::failed_login_record(
                &account.key,
                account.failed_login_attempts,
                account.password_lock_time_days,
                current_time_ms(),
            );
            if let Some(until) = crate::engine::auth::active_login_lock(&account, current_time_ms())
            {
                return Err(crate::engine::auth::temporary_login_lock_error(
                    &account, until,
                ));
            }
            return Err(access_denied(username, host));
        }
        let effective_expired = login.validate_verified(username, request)?;
        // Successful full auth populates the auth cache so the *next*
        // connection from this account hits the fast path.
        crate::engine::auth::auth_cache_remember_password(
            &account.key,
            plaintext,
            if primary_ok {
                &account.authentication_string
            } else {
                account
                    .secondary_authentication_string
                    .as_deref()
                    .expect("verified secondary credential")
            },
        );
        login.finish(effective_expired)
    }

    fn is_in_auth_cache(&self, username: &str, peer_host: &str) -> bool {
        self.db
            .with_auth_catalog(|c| {
                crate::engine::auth::auth_cache_contains_for(c, username, peer_host)
            })
            .unwrap_or(false)
    }

    fn authenticate_request(&self, request: AuthRequest<'_>) -> Result<AuthOutcome, QueryError> {
        let login = login::LoginAccount::load(&self.db, request.username, request.host)?;
        let account = &login.account;

        enforce_tls_requirement(&account.ssl_type, &request)?;
        let plugin = request.auth_plugin.unwrap_or(account.plugin.as_str());
        if plugin != account.plugin {
            return Err(access_denied(request.username, request.host));
        }
        // The empty-password token is either zero length or a single NUL.
        // It is a complete credential, so verify it even on a cache miss.
        let empty_caching_sha2 =
            account.plugin == "caching_sha2_password" && matches!(request.auth_response, [] | [0]);
        let sha256_negotiation = account.plugin == "sha256_password"
            && (request.auth_response == [1]
                || (!request.tls_established && !matches!(request.auth_response, [] | [0])));
        let primary_ok = if sha256_negotiation {
            false
        } else if empty_caching_sha2 {
            account.authentication_string.is_empty()
                || verify_plaintext_auth_string(&account.plugin, &account.authentication_string, "")
        } else if account.plugin == "caching_sha2_password" {
            verify_cached_caching_sha2(
                &account.key,
                &account.authentication_string,
                request.auth_response,
                request.nonce,
            )
        } else {
            verify_auth_response(
                &account.plugin,
                &account.authentication_string,
                request.auth_response,
                request.nonce,
            )
        };
        // L3: fall through to the secondary (retained) authentication string
        // if primary fails. Lets `ALTER USER ... IDENTIFIED BY 'new' RETAIN
        // CURRENT PASSWORD` keep app traffic working during rolling password
        // rotations.
        let secondary_ok = !primary_ok
            && !sha256_negotiation
            && account
                .secondary_authentication_string
                .as_ref()
                .is_some_and(|secondary| {
                    if empty_caching_sha2 {
                        secondary.is_empty()
                            || verify_plaintext_auth_string(&account.plugin, secondary, "")
                    } else if account.plugin == "caching_sha2_password" {
                        verify_cached_caching_sha2(
                            &account.key,
                            secondary,
                            request.auth_response,
                            request.nonce,
                        )
                    } else {
                        verify_auth_response(
                            &account.plugin,
                            secondary,
                            request.auth_response,
                            request.nonce,
                        )
                    }
                });
        if !primary_ok && !secondary_ok {
            // SHA2 negotiation / a caching fast miss is not a completed password attempt.
            // The protocol follows it with TLS/RSA/plaintext full auth, which
            // verifies the credential and charges a failure exactly once.
            if !matches!(
                account.plugin.as_str(),
                "caching_sha2_password" | "sha256_password"
            ) || empty_caching_sha2
            {
                crate::engine::auth::failed_login_record(
                    &account.key,
                    account.failed_login_attempts,
                    account.password_lock_time_days,
                    current_time_ms(),
                );
                if let Some(until) =
                    crate::engine::auth::active_login_lock(&account, current_time_ms())
                {
                    return Err(crate::engine::auth::temporary_login_lock_error(
                        &account, until,
                    ));
                }
            }
            return Err(access_denied(request.username, request.host));
        }
        let effective_expired = login.validate_verified(request.username, &request)?;
        login.finish(effective_expired)
    }

    fn kill_session(&self, session: &SessionContext, connection_id: u32) -> Result<(), QueryError> {
        dispatch::kill_session(&self.db, &self.registry, session, connection_id, false)
    }

    fn authorize_global_privilege(
        &self,
        session: &SessionContext,
        privilege: crate::engine::auth::Privilege,
    ) -> Result<(), QueryError> {
        let required = crate::engine::auth::RequiredPrivilege {
            privilege,
            scope: crate::engine::auth::PrivilegeScope::Global,
        };
        let allowed = self.db.with_auth_catalog(|auth| {
            auth.account_has_privilege(&session.authorization_context(), &required, false)
        })?;
        if allowed {
            Ok(())
        } else {
            Err(crate::engine::auth::privilege_denied(
                &required.privilege,
                &required.scope,
            ))
        }
    }

    fn init_session_database(
        &self,
        session: &mut SessionContext,
        database: &str,
    ) -> Result<(), QueryError> {
        crate::engine::auth::require_database_access(&self.db, session, database)?;
        let db_name = crate::storage::normalize_name(database);
        if !self.db.database_exists(&db_name) {
            return Err(error(
                crate::error::catalog::ER_BAD_DB_ERROR,
                format!("unknown database '{database}'"),
            ));
        }
        session.current_database = db_name;
        session.mark_pfs_variables_dirty();
        Ok(())
    }

    fn execute_load_data_statement(
        &self,
        session: &mut SessionContext,
        statement: Statement,
    ) -> Result<QueryResponse, QueryError> {
        let _reclaim_scope = mem_budget::ReclaimDatabaseScope::enter(&self.db);
        session.check_cancelled()?;
        let _fk_checks_guard = crate::storage::FkChecksGuard::new(session.foreign_key_checks);
        let _redo_scope = crate::storage::RedoScopeGuard::new(session.redo_enabled);
        begin_statement_diagnostics(session, "LOAD DATA");
        if let Err(err) = authorize_statement(&self.db, session, &statement) {
            let result = Err(err);
            finish_statement_diagnostics(session, false, &result);
            return result;
        }
        if let Statement::LoadData(stmt) = statement {
            let _http_scope = crate::engine::http::enter(&self.db);
            let _cancel_guard = crate::engine::cancel::install_session_cancel_context(session);
            // Install the session expr view so storage-side `coerce_row`
            // and the general LOAD path see the real sql_mode (G4). The
            // `_raw` variant lets the inner path keep its `&mut session`
            // borrow (txn machinery); the eval path only reads through the
            // pointer. Session stays pinned for the closure's duration.
            let session_ptr = std::ptr::NonNull::from(&*session);
            let result = unsafe {
                crate::engine::expr::with_session_expr_info_raw(session_ptr, || {
                    execute_load_data(&self.db, session, stmt, &self.registry)
                })
            };
            drop(_cancel_guard);
            finish_statement_diagnostics(session, false, &result);
            result
        } else {
            let err = Err(crate::storage::error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "expected LOAD DATA statement",
            ));
            finish_statement_diagnostics(session, false, &err);
            err
        }
    }

    fn execute_load_data_statement_from_reader(
        &self,
        session: &mut SessionContext,
        statement: Statement,
        reader: Box<dyn std::io::Read + Send>,
    ) -> Result<QueryResponse, QueryError> {
        let _reclaim_scope = mem_budget::ReclaimDatabaseScope::enter(&self.db);
        session.check_cancelled()?;
        let _fk_checks_guard = crate::storage::FkChecksGuard::new(session.foreign_key_checks);
        let _redo_scope = crate::storage::RedoScopeGuard::new(session.redo_enabled);
        begin_statement_diagnostics(session, "LOAD DATA LOCAL");
        if let Err(err) = authorize_statement(&self.db, session, &statement) {
            let result = Err(err);
            finish_statement_diagnostics(session, false, &result);
            return result;
        }
        // LOCAL bypasses the ordinary text dispatcher. Its metadata lease
        // must span upload, private-generation rollback and ordered replay.
        let mdl_scope = stream_mdl::StatementScope::enter(&self.db, session.connection_id as u64);
        let requests = dispatch::prepared_statement_mdl_requests(&self.db, session, &statement);
        let mdl_guard = match dispatch::acquire_prepared_statement_mdl(
            &self.db, session, &statement, &requests,
        ) {
            Ok(guard) => guard,
            Err(err) => {
                let result = Err(err);
                finish_statement_diagnostics(session, false, &result);
                return result;
            }
        };
        let sequence_dependent = crate::parser::collect_statement_function_names(&statement)
            .iter()
            .any(|name| crate::storage::sequence::is_sequence_function(name));
        let _serial = sequence_dependent.then(crate::planner::force_serial_scope);
        // SAFETY: the reader executes serial SET expressions on the owning
        // blocking thread; this guard ends before db/session can be released.
        let _sequence_context = unsafe {
            crate::engine::expr::install_sequence_eval_context(
                std::ptr::NonNull::from(self.db.as_ref()),
                std::ptr::NonNull::from(&*session),
            )
        };
        let result = if let Statement::LoadData(stmt) = statement {
            let _http_scope = crate::engine::http::enter(&self.db);
            let _cancel_guard = crate::engine::cancel::install_session_cancel_context(session);
            let session_ptr = std::ptr::NonNull::from(&*session);
            let result = unsafe {
                crate::engine::expr::with_session_expr_info_raw(session_ptr, || {
                    execute_load_data_with_reader(
                        &self.db,
                        session,
                        stmt,
                        &self.registry,
                        Some(reader),
                    )
                })
            };
            drop(_cancel_guard);
            result
        } else {
            let err = Err(crate::storage::error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "expected LOAD DATA statement",
            ));
            err
        };
        let result = stream_mdl::finish(
            &self.db,
            session.connection_id as u64,
            mdl_guard,
            result,
            mdl_scope.outermost(),
        );
        finish_statement_diagnostics(session, false, &result);
        result
    }

    fn close_session(&self, session: &mut SessionContext) -> Result<(), QueryError> {
        StorageQueryEngine::close_session(self, session)
    }

    fn session_cleanup_is_trivial(&self, session: &SessionContext) -> bool {
        session.current_txn.is_none()
            && !session.explicit_txn
            && session.xa_branch.is_none()
            && session.temp_tables.is_empty()
            && session.explicit_table_locks.is_empty()
    }

    fn reset_session(&self, session: &mut SessionContext) -> Result<(), QueryError> {
        StorageQueryEngine::reset_session(self, session)
    }

    fn protocol_temp_dir(&self) -> std::path::PathBuf {
        self.db.data_dir().to_path_buf()
    }

    fn execute_parsed(
        &self,
        session: &mut SessionContext,
        sql: &str,
        statement: Arc<Statement>,
    ) -> Result<QueryResponse, QueryError> {
        let _group_concat_scope =
            select::aggregate::enter_group_concat_max_len(session.group_concat_max_len);
        let _reclaim_scope = mem_budget::ReclaimDatabaseScope::enter(&self.db);
        let _http_dependent = crate::engine::http::may_call(&self.db, sql);
        session.check_cancelled()?;
        let preserve_warnings = begin_statement_diagnostics(session, sql);
        let started = Instant::now();
        let _http_scope = crate::engine::http::enter(&self.db);
        if _http_dependent {
            crate::engine::http::activate();
        }
        let _cancel_guard = crate::engine::cancel::install_session_cancel_context(session);
        // SAFETY: db/session outlive this statement, including the constant
        // SELECT lane that bypasses dispatch. Sequence calls remain serial.
        let sequence_dependent = crate::planner::sql_mentions_sequence_fn(sql);
        let _sequence_ctx = unsafe {
            crate::engine::expr::install_sequence_eval_context(
                std::ptr::NonNull::from(self.db.as_ref()),
                std::ptr::NonNull::from(&*session),
            )
        };
        let _load_file_ctx = unsafe {
            crate::engine::expr::install_load_file_eval_context(
                std::ptr::NonNull::from(self.db.as_ref()),
                std::ptr::NonNull::from(&*session),
            )
        };
        let _force_serial = (sequence_dependent
            || crate::planner::sql_mentions_user_var_assign(sql)
            || crate::planner::sql_mentions_load_file_fn(sql)
            || crate::engine::http::may_call(&self.db, sql))
        .then(crate::planner::force_serial_scope);
        let (result, metadata) = match statement.as_ref() {
            Statement::Select(select)
                if !crate::engine::ai::sql::sql_may_have_ai(sql)
                    && crate::engine::executor::select::const_operator_supported(select)
                    && !routines::select_references_stored_function(
                        &self.db,
                        &session.current_database,
                        select,
                    ) =>
            {
                let _deadline = crate::engine::cancel::install_query_deadline(
                    std::time::Duration::from_millis(dispatch::select_deadline_ms(select, session)),
                );
                // A source-free SELECT has an empty privilege/MDL recipe and
                // no planner dependency.  Execute its formal constant
                // operator directly from the immutable exact-source AST: this
                // keeps the normal diagnostics/cancel/logging envelope while
                // avoiding an AST clone, qualification, view expansion and
                // repeated cache-key construction on every COM_QUERY hit.
                let result = dispatch::ensure_xa_statement_allowed(session, &statement)
                    .and_then(|()| dispatch::enforce_account_resource_limits(session, &statement))
                    .and_then(|()| {
                        let (txn, _snapshot_guard) = if session.explicit_txn || !session.autocommit
                        {
                            tx::lazy_read_only_transaction_view_with_guard(&self.db, session)
                        } else {
                            let (txn, guard) =
                                tx::autocommit_read_only_view_with_guard(&self.db, session);
                            (txn, Some(guard))
                        };
                        crate::engine::expr::with_session_expr_info(session, || {
                            crate::engine::executor::select::execute_const_select(
                                &self.db, txn, select,
                            )
                        })
                    });
                (result, QueryLogMetadata::default())
            }
            _ => self.execute_with_log_metadata_parsed(session, sql, (*statement).clone()),
        };
        // Sequence state belongs to the live connection. A deferred cursor
        // must not outlive that state or evaluate on a worker's copied session.
        let result = if sequence_dependent {
            result.and_then(QueryResponse::materialize)
        } else {
            result
        };
        let result =
            result.and_then(|response| response.localize_timestamps(&session.sysvars.time_zone));
        drop(_cancel_guard);
        finish_statement_diagnostics(session, preserve_warnings, &result);
        self.finish_or_observe_query(session, sql, started, metadata, result, None)
    }

    fn field_list_columns(
        &self,
        session: &SessionContext,
        table_name: &str,
        wildcard: &str,
    ) -> Result<Vec<Vec<u8>>, QueryError> {
        authorize_statement(
            &self.db,
            session,
            &Statement::Describe {
                table: table_name.to_string(),
            },
        )?;
        let (db_name, name) =
            crate::storage::split_table_name(&session.current_database, table_name);
        let schema = self.db.table(&qualify_table_name(session, table_name))?;
        let mut out = Vec::with_capacity(schema.columns.len());
        let pattern_bytes = if wildcard.is_empty() {
            None
        } else {
            Some(wildcard.as_bytes().to_vec())
        };
        for (idx, column) in schema.columns.iter().enumerate() {
            if let Some(pattern) = pattern_bytes.as_deref() {
                if !field_list_like_match(pattern, column.name.as_bytes()) {
                    continue;
                }
            }
            if column.invisible {
                continue;
            }
            let mut source = schema.wire_column_source(idx);
            source.schema = db_name.clone();
            source.table = name.clone();
            source.org_table = name.clone();
            source.org_name = column.name.clone();
            let column_type = column.data_type.column_type();
            let mut packet = crate::mysql::protocol::column_definition_packet_full(
                &column.name,
                column_type,
                &source,
            );
            // COM_FIELD_LIST appends a lenenc default-value string after the
            // standard ColumnDefinition41. Strict clients (mysql 9.6 cli's
            // auto-rehash among them) treat a missing tail as a protocol
            // error and silently drop the connection — the next user query
            // then surfaces as ER_LOST_CONNECTION on a fresh socket. We
            // emit an empty default (lenenc 0) — matching what mysqld
            // sends for columns without an explicit DEFAULT — which is
            // enough to keep the protocol in sync. Filling in real default
            // values is a separate enhancement.
            crate::mysql::protocol::write_lenenc_int(&mut packet, 0);
            out.push(packet);
        }
        Ok(out)
    }

    fn execute_prepared_statement(
        &self,
        session: &mut SessionContext,
        sql: &str,
        statement: Statement,
    ) -> Option<Result<QueryResponse, QueryError>> {
        let _group_concat_scope =
            select::aggregate::enter_group_concat_max_len(session.group_concat_max_len);
        let _reclaim_scope = mem_budget::ReclaimDatabaseScope::enter(&self.db);
        if let Err(err) = session.check_cancelled() {
            return Some(Err(err));
        }
        // SAFETY: db/session outlive this statement, including the constant
        // SELECT lane that bypasses dispatch. Sequence calls remain serial.
        let sequence_dependent = crate::planner::sql_mentions_sequence_fn(sql);
        let _sequence_ctx = unsafe {
            crate::engine::expr::install_sequence_eval_context(
                std::ptr::NonNull::from(self.db.as_ref()),
                std::ptr::NonNull::from(&*session),
            )
        };
        let _load_file_ctx = unsafe {
            crate::engine::expr::install_load_file_eval_context(
                std::ptr::NonNull::from(self.db.as_ref()),
                std::ptr::NonNull::from(&*session),
            )
        };
        let _force_serial = (sequence_dependent
            || crate::planner::sql_mentions_load_file_fn(sql)
            || crate::engine::http::may_call(&self.db, sql))
        .then(crate::planner::force_serial_scope);
        // Prepared-statement EXECUTE bypasses `execute_statement` (which is
        // where L8 quota enforcement normally runs). Charge the per-account
        // budget here so sqlx / Connector/J prepared-binding apps don't sneak
        // past the limit.
        let full_routine_dispatch = matches!(statement, Statement::Call(_))
            || routines::statement_references_stored_function(
                &self.db,
                &session.current_database,
                &statement,
            );
        // Routine statements re-enter the full dispatcher, which charges the
        // account itself. Keep the lightweight path's accounting exactly once.
        if !session.internal_system_session && !full_routine_dispatch {
            let limits = &session.account_limits;
            if let Err(err) = crate::engine::auth::charge_question(
                &session.authenticated_as,
                limits.max_questions,
            ) {
                return Some(Err(err));
            }
            if crate::replication::is_replica_write_statement(&statement) {
                if let Err(err) = crate::engine::auth::charge_update(
                    &session.authenticated_as,
                    limits.max_updates,
                ) {
                    return Some(Err(err));
                }
            }
        }
        let preserve_warnings = begin_statement_diagnostics(session, sql);
        if let Some(simple_kind) =
            simple_txn_control_kind(sql.trim_matches(|c: char| c.is_whitespace() || c == ';'))
        {
            let result = dispatch_simple_txn_control(&self.db, session, simple_kind);
            finish_statement_diagnostics(session, preserve_warnings, &result);
            return Some(result);
        }
        if self.plan_cache_enabled {
            self.db.record_plan_cache_hit();
        }
        let use_shared_plan_cache =
            should_use_shared_plan_cache(self.plan_cache_enabled, session, &statement);
        let _http_scope = crate::engine::http::enter(&self.db);
        if crate::engine::http::may_call(&self.db, sql) {
            crate::engine::http::activate();
        }
        let _cancel_guard = crate::engine::cancel::install_session_cancel_context(session);
        let result = if use_shared_plan_cache && !statement_skips_plan_cache_hint(&statement) {
            // Prepared statements feed the plan cache through the same hint
            // mechanism as COM_QUERY. Reuse the PREPARE-time strings when
            // available; the disabled path avoids this key construction.
            let (normalized_sql, database) = match crate::engine::cache::current_prepared_hint() {
                Some(prep) => (prep.normalized_sql, prep.database),
                None => {
                    let normalized = normalize_sql(sql);
                    (
                        Arc::<str>::from(normalized.as_str()),
                        Arc::<str>::from(session.current_database.as_str()),
                    )
                }
            };
            let hint = crate::engine::cache::QueryHint {
                normalized_sql,
                database,
                semantics: plan_semantics(session),
                parameterized: true,
                plan_cache: Arc::downgrade(&self.plan_cache),
            };
            crate::engine::cache::with_query_hint(hint, || {
                self.execute_qualified_parsed_statement(session, sql, statement)
            })
        } else {
            self.execute_qualified_parsed_statement(session, sql, statement)
        };
        // Sequence state belongs to the live connection. A deferred cursor
        // must not outlive that state or evaluate on a worker's copied session.
        let result = if sequence_dependent {
            result.and_then(QueryResponse::materialize)
        } else {
            result
        };
        let result =
            result.and_then(|response| response.localize_timestamps(&session.sysvars.time_zone));
        drop(_cancel_guard);
        finish_statement_diagnostics(session, preserve_warnings, &result);
        Some(result)
    }

    fn execute_prepared_select(
        &self,
        session: &mut SessionContext,
        state: &mut crate::engine::api::PreparedSelectMemoState,
        parsed: &Arc<Statement>,
        sql: &str,
        params: &[crate::types::Value],
    ) -> Option<Result<QueryResponse, QueryError>> {
        let _group_concat_scope =
            select::aggregate::enter_group_concat_max_len(session.group_concat_max_len);
        let _reclaim_scope = mem_budget::ReclaimDatabaseScope::enter(&self.db);
        if let Err(err) = dispatch::ensure_xa_statement_allowed(session, parsed) {
            return Some(Err(err));
        }
        // Sequence evaluation mutates live session/durable state. Route it
        // through the ordinary statement lane, which drains under that scope,
        // and keep the read-only prepared fast path unchanged.
        if crate::planner::sql_mentions_sequence_fn(sql)
            || crate::engine::ai::sql::sql_may_have_ai(sql)
            || crate::engine::http::may_call(&self.db, sql)
        {
            return None;
        }
        use crate::engine::api::PreparedSelectMemoState;
        if statement_refs_temp_table(session, parsed) {
            return None;
        }
        let current_epoch = self.plan_cache.current_epoch();
        if !self.plan_cache_enabled {
            let (template, compiled) = match state {
                PreparedSelectMemoState::Ineligible => return None,
                PreparedSelectMemoState::Uncached(template)
                    if compiled_select::uncached_template_is_valid(&self.db, template, session) =>
                {
                    let template = Arc::clone(template);
                    let compiled =
                        match crate::engine::expr::with_session_expr_info(session, || {
                            compiled_select::compile_uncached_physical_select(
                                &self.db,
                                &template,
                                params,
                                current_epoch,
                            )
                        }) {
                            Ok(compiled) => compiled,
                            Err(err) => return Some(Err(err)),
                        };
                    (template, compiled)
                }
                _ => match crate::engine::expr::with_session_expr_info(session, || {
                    compiled_select::compile_uncached_prepared_select(
                        &self.db,
                        session,
                        parsed,
                        params,
                        current_epoch,
                    )
                }) {
                    Some((template, compiled)) => {
                        *state = PreparedSelectMemoState::Uncached(Arc::clone(&template));
                        (template, compiled)
                    }
                    None => {
                        *state = PreparedSelectMemoState::Ineligible;
                        return None;
                    }
                },
            };
            let select = &template.select;
            if !tx::can_use_read_only_select(session, select)
                && !tx::can_use_lazy_read_only_transaction_select(session, select)
                && !tx::can_use_current_transaction_read_select(session, select)
            {
                return None;
            }
            return Some(
                self.run_uncached_prepared_select(session, &template, &compiled, sql, params),
            );
        }
        // Resolve the memo: reuse a still-valid plan, recompile a stale one,
        // compile an unclassified one, short-circuit a known-ineligible one.
        let reusable = match &*state {
            PreparedSelectMemoState::Ready(rp) => {
                compiled_select::reusable_memo(&self.db, rp, current_epoch, session, params)
            }
            _ => None,
        };
        let rp = match state {
            PreparedSelectMemoState::Ineligible => return None,
            _ if reusable.is_some() => {
                self.db.record_prepared_plan_reuse();
                let rp = reusable.expect("checked above");
                *state = PreparedSelectMemoState::Ready(Arc::clone(&rp));
                rp
            }
            _ => {
                let previous = match &*state {
                    PreparedSelectMemoState::Ready(memo) => Some(memo.as_ref()),
                    _ => None,
                };
                match crate::engine::expr::with_session_expr_info(session, || {
                    compiled_select::compile_prepared_select_from_memo(
                        &self.db,
                        session,
                        parsed,
                        params,
                        &self.plan_cache,
                        previous,
                    )
                }) {
                    Some(plan) => {
                        if let PreparedSelectMemoState::Ready(previous) = &*state {
                            compiled_select::adopt_previous_memo(&plan, previous);
                        }
                        let arc = Arc::new(plan);
                        *state = PreparedSelectMemoState::Ready(Arc::clone(&arc));
                        arc
                    }
                    None => {
                        *state = PreparedSelectMemoState::Ineligible;
                        return None;
                    }
                }
            }
        };
        // Session-state eligibility. The compile-once fast path models
        // two read shapes (no row locks taken on either):
        //   1. Autocommit SELECT — fresh per-statement read view.
        //   2. In-transaction SELECT under RC/RR (non-Serializable) —
        //      uses the transaction's pinned snapshot under RR, or a
        //      fresh per-statement snapshot under RC, matching the
        //      `lazy_read_only_transaction_select` branch of the legacy
        //      dispatcher in `dispatch.rs:5499`. SERIALIZABLE,
        //      FOR UPDATE / FOR SHARE, and statements running inside a
        //   3. Non-locking SELECT after the transaction has already issued
        //      a write — reuses the current txn id so own uncommitted writes
        //      stay visible while still skipping the AST clone + replanning.
        let select = &rp.presentation_select;
        if !tx::can_use_read_only_select(session, select)
            && !tx::can_use_lazy_read_only_transaction_select(session, select)
            && !tx::can_use_current_transaction_read_select(session, select)
        {
            return None;
        }
        Some(self.run_prepared_select(session, &rp, sql, params))
    }

    fn execute_prepared_dml(
        &self,
        session: &mut SessionContext,
        state: &mut crate::engine::api::PreparedDmlState,
        parsed: &Arc<Statement>,
        sql: &str,
        params: &[crate::types::Value],
    ) -> Option<Result<QueryResponse, QueryError>> {
        let _reclaim_scope = mem_budget::ReclaimDatabaseScope::enter(&self.db);
        let _redo_scope = crate::storage::RedoScopeGuard::new(session.redo_enabled);
        // Trait-method shim → inherent impl (kept inherent so the
        // engine can call into the fast path without a trait-object
        // hop in tests / inner helpers).
        self.execute_prepared_dml_impl(session, state, parsed, sql, params)
    }

    fn execute_large_text_dml_inline(
        &self,
        session: &mut SessionContext,
        sql: &str,
    ) -> Option<Result<QueryResponse, QueryError>> {
        if crate::engine::http::may_call(&self.db, sql) {
            return None;
        }
        let _reclaim_scope = mem_budget::ReclaimDatabaseScope::enter(&self.db);
        let parse_bits = crate::engine::api::parse_temporal_sql_mode_bits(&session.sql_mode);
        let statement = {
            let _mode_guard = crate::parser::ParserSqlModeGuard::activate(parse_bits);
            parse_statement(sql).ok()?
        };
        let digest_sql = crate::parser::take_large_literal_digest_sql();
        if !safe_large_literal_update(&statement) || !can_use_prepared_dml(session) {
            return None;
        }
        Some(self.execute_owned_text_dml_inline(session, sql, statement, digest_sql))
    }

    fn cached_text_statement(
        &self,
        session: &SessionContext,
        sql: &str,
    ) -> Option<(Arc<Statement>, Vec<Value>, Arc<str>)> {
        let mode = crate::engine::auto_param::AutoParameterizeMode::from_env();
        if matches!(mode, crate::engine::auto_param::AutoParameterizeMode::Off) {
            return None;
        }
        // A DML shape is only ever published while DML auto-parameterization
        // is on, so under `Select` the lookup below could not hit — and it is
        // not free: `text_shape_key_and_params` scans the whole statement and
        // builds a shape string plus a value vector before the (certain)
        // cache miss. Decline before paying for it.
        if !matches!(mode, crate::engine::auto_param::AutoParameterizeMode::All)
            && !select_keyword_prefix(sql)
        {
            return None;
        }
        let parser_mode_bits = crate::engine::api::parse_temporal_sql_mode_bits(&session.sql_mode);
        let (key, params) = text_shape_key_and_params(sql, parser_mode_bits)?;
        let statement = self.statement_cache.get(&key)?;
        if !matches!(
            statement.as_ref(),
            Statement::Select(_)
                | Statement::Insert(_)
                | Statement::Update(_)
                | Statement::Delete(_)
        ) {
            return None;
        }
        self.db.record_select_parse_cache_hit();
        Some((statement, params, Arc::clone(&key.sql)))
    }

    fn execute_parsed_with_params(
        &self,
        session: &mut SessionContext,
        sql: &str,
        statement: Arc<Statement>,
        params: Vec<Value>,
        shape_sql: Arc<str>,
    ) -> Result<QueryResponse, QueryError> {
        let _group_concat_scope =
            select::aggregate::enter_group_concat_max_len(session.group_concat_max_len);
        let _reclaim_scope = mem_budget::ReclaimDatabaseScope::enter(&self.db);
        let _http_dependent = crate::engine::http::may_call(&self.db, sql);
        session.check_cancelled()?;
        self.db.ensure_operational()?;
        let preserve_warnings = begin_statement_diagnostics(session, sql);
        let started = Instant::now();
        let _http_scope = crate::engine::http::enter(&self.db);
        if _http_dependent {
            crate::engine::http::activate();
        }
        let _cancel_guard = crate::engine::cancel::install_session_cancel_context(session);
        // SAFETY: db/session outlive this statement, including the constant
        // SELECT lane that bypasses dispatch. Sequence calls remain serial.
        let sequence_dependent = crate::planner::sql_mentions_sequence_fn(sql);
        let _sequence_ctx = unsafe {
            crate::engine::expr::install_sequence_eval_context(
                std::ptr::NonNull::from(self.db.as_ref()),
                std::ptr::NonNull::from(&*session),
            )
        };
        let _load_file_ctx = unsafe {
            crate::engine::expr::install_load_file_eval_context(
                std::ptr::NonNull::from(self.db.as_ref()),
                std::ptr::NonNull::from(&*session),
            )
        };
        let _force_serial = (sequence_dependent
            || crate::planner::sql_mentions_user_var_assign(sql)
            || crate::planner::sql_mentions_load_file_fn(sql)
            || crate::engine::http::may_call(&self.db, sql))
        .then(crate::planner::force_serial_scope);
        let Statement::Select(select) = statement.as_ref() else {
            return self.execute_parsed(session, sql, statement);
        };
        let fingerprint =
            crate::engine::auto_param::typed_text_shape_fingerprint(&shape_sql, select, &params);
        let statement = (*statement).clone();
        let auto_result = crate::engine::auto_param::AutoParamResult {
            params,
            fingerprint: Some(fingerprint),
        };
        let (result, metadata) =
            self.execute_single_statement_tail(session, sql, statement, auto_result, false);
        // Sequence state belongs to the live connection. A deferred cursor
        // must not outlive that state or evaluate on a worker's copied session.
        let result = if sequence_dependent {
            result.and_then(QueryResponse::materialize)
        } else {
            result
        };
        let result =
            result.and_then(|response| response.localize_timestamps(&session.sysvars.time_zone));
        drop(_cancel_guard);
        finish_statement_diagnostics(session, preserve_warnings, &result);
        self.finish_or_observe_query(session, sql, started, metadata, result, Some(shape_sql))
    }
}

impl StorageQueryEngine {
    /// Owned-AST COM_QUERY path for the conservative large-literal UPDATE
    /// subset. The protocol already admitted only autocommit DML, and the
    /// shape gate above guarantees `execute_single_statement_tail` reaches the
    /// compiled prepared-DML vehicle. Keeping ownership here avoids cloning a
    /// multi-megabyte literal just to cross the generic `Arc<Statement>` API.
    fn execute_owned_text_dml_inline(
        &self,
        session: &mut SessionContext,
        sql: &str,
        statement: Statement,
        digest_sql: Option<String>,
    ) -> Result<QueryResponse, QueryError> {
        let _reclaim_scope = mem_budget::ReclaimDatabaseScope::enter(&self.db);
        if let Err(err) = session.check_cancelled() {
            return Err(err);
        }
        let preserve_warnings = begin_statement_diagnostics(session, sql);
        let started = Instant::now();
        let _http_scope = crate::engine::http::enter(&self.db);
        let _cancel_guard = crate::engine::cancel::install_session_cancel_context(session);
        // SAFETY: db/session outlive this statement, including the constant
        // SELECT lane that bypasses dispatch. Sequence calls remain serial.
        let sequence_dependent = crate::planner::sql_mentions_sequence_fn(sql);
        let _sequence_ctx = unsafe {
            crate::engine::expr::install_sequence_eval_context(
                std::ptr::NonNull::from(self.db.as_ref()),
                std::ptr::NonNull::from(&*session),
            )
        };
        let _load_file_ctx = unsafe {
            crate::engine::expr::install_load_file_eval_context(
                std::ptr::NonNull::from(self.db.as_ref()),
                std::ptr::NonNull::from(&*session),
            )
        };
        let (result, metadata) = self.execute_with_log_metadata_parsed(session, sql, statement);
        // Sequence state belongs to the live connection. A deferred cursor
        // must not outlive that state or evaluate on a worker's copied session.
        let result = if sequence_dependent {
            result.and_then(QueryResponse::materialize)
        } else {
            result
        };
        let result =
            result.and_then(|response| response.localize_timestamps(&session.sysvars.time_zone));
        if metadata.wake_purge && result.is_ok() {
            self.undo_purge_worker.wake();
        }
        drop(_cancel_guard);
        finish_statement_diagnostics(session, preserve_warnings, &result);
        self.finish_or_observe_query(
            session,
            sql,
            started,
            metadata,
            result,
            digest_sql.map(Arc::from),
        )
    }

    /// Execute a validated, memoized prepared read plan with the same
    /// diagnostics / cancel / logging wrapper as the legacy path, but
    /// borrowing the cached AST + plan (no clone) and the session
    /// immutably (no `session.clone()`). Parameters bind late through the
    /// thread-local context installed here.
    fn run_prepared_select(
        &self,
        session: &mut SessionContext,
        rp: &crate::engine::api::PreparedSelectMemo,
        sql: &str,
        params: &[crate::types::Value],
    ) -> Result<QueryResponse, QueryError> {
        self.with_prepared_select_execution(
            session,
            &rp.presentation_select,
            sql,
            params,
            |session, txn, connection_id| {
                compiled_select::run_compiled_select(&self.db, session, rp, txn, connection_id)
            },
        )
    }

    fn run_uncached_prepared_select(
        &self,
        session: &mut SessionContext,
        template: &crate::engine::api::PreparedSelectTemplate,
        compiled: &crate::engine::cache::CompiledSelectPlan,
        sql: &str,
        params: &[crate::types::Value],
    ) -> Result<QueryResponse, QueryError> {
        self.with_prepared_select_execution(
            session,
            &template.select,
            sql,
            params,
            |session, txn, connection_id| {
                compiled_select::run_uncached_compiled_select(
                    &self.db,
                    session,
                    template,
                    compiled,
                    txn,
                    connection_id,
                )
            },
        )
    }

    /// Cached and uncached plans share read-view lifetime, parameter scopes,
    /// diagnostics and retry handling. The closure is statically dispatched;
    /// neither path needs an AST clone or a boxed execution callback.
    fn with_prepared_select_execution(
        &self,
        session: &mut SessionContext,
        select: &Select,
        sql: &str,
        params: &[crate::types::Value],
        run: impl FnOnce(&SessionContext, TxnView, u64) -> Result<QueryResponse, QueryError>,
    ) -> Result<QueryResponse, QueryError> {
        session.check_cancelled()?;
        let started = Instant::now();
        let preserve_warnings = begin_statement_diagnostics(session, sql);
        // Pick the read view that matches the eligibility branch — the
        // autocommit path takes a fresh statement view; the lazy
        // in-transaction path threads the transaction's RR pin so
        // repeated SELECTs are stable across intervening commits. Both
        // need `&mut session` (isolation getters take a one-shot
        // `next_txn_isolation`, RR pins `txn_snapshot_scn` after the
        // first statement), so we build the view here before locking
        // the session into the shared-borrow execution closure below.
        let (txn, _snapshot_guard) = tx::prepared_select_read_view(&self.db, session, select)?;
        let connection_id = session.connection_id as u64;
        let _http_scope = crate::engine::http::enter(&self.db);
        let _cancel_guard = crate::engine::cancel::install_session_cancel_context(session);
        let result = crate::engine::params::with_params_slice(params, || {
            crate::engine::expr::with_session_expr_info(session, || {
                if crate::engine::params::auto_parameterized_active()
                    && params.iter().any(|value| {
                        matches!(
                            value,
                            Value::String(_) | Value::SmallString(_) | Value::Bytes(_)
                        )
                    })
                {
                    select::validate_cached_temporal_literals(&self.db, select)?;
                }
                // Compiled plans bypass execute_select, including its registry scope.
                // A business-table plan can still scan system tables in joins/subqueries.
                select::with_session_registry((*self.registry).clone(), || {
                    run(session, txn, connection_id)
                })
            })
        });
        let result =
            result.and_then(|response| response.localize_timestamps(&session.sysvars.time_zone));
        drop(_cancel_guard);
        if result.as_ref().is_err_and(|err| {
            is_lock_wait_pending(err) || err.is(crate::error::catalog::ER_NEED_REPREPARE)
        }) {
            // The protocol retries these pre-execution conditions. They are
            // not completed failed SQL statements and must not be logged as such.
            return result;
        }
        finish_statement_diagnostics(session, preserve_warnings, &result);
        self.finish_or_observe_query(
            session,
            sql,
            started,
            QueryLogMetadata::default(),
            result,
            None,
        )
    }
}

impl StorageQueryEngine {
    /// Compile-once / late-bind fast path for prepared INSERT / UPDATE /
    /// DELETE. Mirrors [`Self::execute_prepared_select`] in shape: classify
    /// the statement once, cache the result on the `PreparedStatement`,
    /// and on every subsequent execute either reuse the memo (skipping
    /// the legacy dispatch wrappers + `session.clone()` +
    /// `Statement::clone()`) or short-circuit `None` so the caller drops
    /// into the legacy `execute_prepared_statement` path.
    pub(crate) fn execute_prepared_dml_impl(
        &self,
        session: &mut SessionContext,
        state: &mut crate::engine::api::PreparedDmlState,
        parsed: &Arc<Statement>,
        sql: &str,
        params: &[crate::types::Value],
    ) -> Option<Result<QueryResponse, QueryError>> {
        if crate::engine::http::may_call(&self.db, sql) {
            return None;
        }
        use crate::engine::api::PreparedDmlState;
        if !self.plan_cache_enabled || !prepared_dml_cache_enabled() {
            return None;
        }
        // Session-state guards — anything the legacy dispatch chain
        // catches earlier than `execute_qualified_statement` must be
        // rejected up-front so the fast path never makes a write the
        // legacy path would have refused.
        if !can_use_prepared_dml(session) || statement_refs_temp_table(session, parsed) {
            return None;
        }
        let current_epoch = self.plan_cache.current_epoch();
        let dp = match state {
            PreparedDmlState::Ineligible => return None,
            PreparedDmlState::Ready(dp)
                if prepared_dml::memo_is_valid(&self.db, dp, session, current_epoch) =>
            {
                Arc::clone(dp)
            }
            _ => {
                match prepared_dml::compile_prepared_dml(&self.db, session, parsed, current_epoch) {
                    Some(plan) => {
                        let arc = Arc::new(plan);
                        *state = PreparedDmlState::Ready(Arc::clone(&arc));
                        arc
                    }
                    None => {
                        *state = PreparedDmlState::Ineligible;
                        return None;
                    }
                }
            }
        };
        // Replica / read-only / global-read-lock enforcement that
        // `execute_statement` does on every entry. Run on every execute
        // because these are global / session-state checks, not memo-able.
        if let Err(err) = self.enforce_prepared_dml_global_gates(&dp.statement, session) {
            return Some(Err(err));
        }
        if let Err(err) = dispatch::enforce_session_write_access(&self.db, session, &dp.statement)
            .and_then(|()| dispatch::enforce_account_resource_limits(session, &dp.statement))
        {
            return Some(Err(err));
        }
        Some(self.run_prepared_dml(session, &dp, sql, params))
    }

    fn enforce_prepared_dml_global_gates(
        &self,
        statement: &Statement,
        session: &SessionContext,
    ) -> Result<(), QueryError> {
        dispatch::ensure_xa_statement_allowed(session, statement)?;
        if self.db.is_snapshot_read_only() {
            crate::backup::native::enforce_statement(statement)?;
        }
        crate::replication::enforce_replica_read_only(statement)?;
        crate::repl::enforce_role_read_only(statement)?;
        crate::backup::wait_for_write_gate(
            &self.db,
            session
                .current_txn
                .or_else(|| session.xa_branch.as_ref().map(|branch| branch.txn_id)),
            statement,
        )?;
        crate::binlog::wait_for_global_read_lock_scoped(
            statement,
            self.db.global_read_lock_scope(),
            session,
        )?;
        Ok(())
    }

    fn execute_prepared_dml_core(
        &self,
        session: &mut SessionContext,
        dp: &crate::engine::api::PreparedDmlPlan,
        params: &[crate::types::Value],
        started: Instant,
    ) -> (Result<QueryResponse, QueryError>, LockWaitStats) {
        // Cached prepared DML bypasses execute_statement. Install the same
        // database-owned budget here so pressure can spill/evict storage pages,
        // including while transaction cleanup runs, instead of returning 1037
        // with a full reclaimable buffer pool and no database to reclaim from.
        if mem_budget::current_budget().is_none() {
            let budget = session.cancel_handle.as_ref().map_or_else(
                || mem_budget::MemBudget::for_database(&self.db),
                |handle| {
                    mem_budget::MemBudget::for_session_with_database(
                        handle.session_handle_arc(),
                        &self.db,
                    )
                },
            );
            return mem_budget::with_query_budget(budget, || {
                let (result, waits) = self.execute_prepared_dml_core(session, dp, params, started);
                (result.map_err(mem_budget::terminal_oom), waits)
            });
        }
        if let Err(err) = session.check_cancelled() {
            return (Err(err), LockWaitStats::default());
        }
        self.db.record_prepared_dml_reuse();
        let connection_id = session.connection_id as u64;
        let mut wait_stats = LockWaitStats::default();
        let mut waited_for_lock = false;
        let _sql_log_bin_guard = crate::binlog::SessionSqlLogBinGuard::new(session.sql_log_bin);
        let _lock_wait_guard = crate::storage::LockWaitTimeoutGuard::new(Some(
            session
                .sysvars
                .innodb_lock_wait_timeout
                .unwrap_or_else(crate::engine::api::global_innodb_lock_wait_timeout),
        ));
        let _seq_ctx_guard = unsafe {
            crate::engine::expr::install_sequence_eval_context(
                std::ptr::NonNull::from(&*self.db),
                std::ptr::NonNull::from(&*session),
            )
        };
        loop {
            if let Err(err) = session.check_cancelled() {
                return (Err(err), wait_stats);
            }
            let _http_scope = crate::engine::http::enter(&self.db);
            let _cancel_guard = crate::engine::cancel::install_session_cancel_context(session);
            // SAFETY: `session` lives for the entire duration of this
            // function — we hold the `&mut SessionContext` parameter on
            // the stack and never move or drop it. The raw-pointer form
            // of `with_session_expr_info` lets the inner `&mut session`
            // borrow (needed by `execute_in_transaction_with_session`)
            // coexist with the thread-local pointer the expression
            // evaluator reads through; the borrow checker can't see the
            // pointer so wouldn't allow the borrowed-reference variant
            // here.
            let session_ptr = std::ptr::NonNull::from(&*session);
            let attempt = unsafe {
                crate::engine::expr::with_session_expr_info_raw(session_ptr, || {
                    crate::engine::params::with_params_slice(params, || {
                        prepared_dml::execute_prepared_dml_plan(
                            &self.db,
                            session,
                            &self.registry,
                            dp,
                            connection_id,
                        )
                        .and_then(QueryResponse::materialize)
                    })
                })
            };
            drop(_cancel_guard);
            match attempt {
                Ok(resp) => {
                    if waited_for_lock {
                        clear_session_lock_wait(&self.db, session);
                    }
                    return (Ok(resp), wait_stats);
                }
                Err(err) if is_lock_wait_pending(&err) && crate::engine::http::sent() => {
                    clear_session_lock_wait(&self.db, session);
                    let failure = crate::engine::http::error("ER_HTTP_REPLAY_FORBIDDEN", "lock wait requires replay after an HTTP send; external effects may have occurred");
                    if let Err(cleanup) =
                        self.finalize_failed_statement_transaction(session, &failure)
                    {
                        return (Err(cleanup), wait_stats);
                    }
                    return (Err(failure), wait_stats);
                }
                Err(err) if is_lock_wait_pending(&err) => {
                    waited_for_lock = true;
                    if let Err(err) =
                        wait_for_lock_signal_or_cancel(&self.db, session, started, &mut wait_stats)
                    {
                        clear_session_lock_wait(&self.db, session);
                        if let Err(rollback_err) =
                            self.finalize_failed_statement_transaction(session, &err)
                        {
                            return (Err(rollback_err), wait_stats);
                        }
                        return (Err(err), wait_stats);
                    }
                    // Loop again; retry the cached plan against a fresh
                    // lock attempt.
                }
                Err(err) if err.is(crate::error::catalog::ER_NEED_REPREPARE) => {
                    // The under-MDL version check rejected the cached plan
                    // before starting DML or evaluating expressions. Its
                    // statement locks have been released. Rebind the AST through
                    // the ordinary dispatcher, including any new FK MDL
                    // dependencies. This also applies inside a transaction:
                    // only this not-yet-executed statement is rebound, never
                    // prior writes or expressions. Transaction-duration MDL and
                    // snapshot semantics remain enforced by the dispatcher.
                    if let Err(rollback_err) =
                        self.finalize_failed_statement_transaction(session, &err)
                    {
                        return (Err(rollback_err), wait_stats);
                    }
                    clear_session_lock_wait(&self.db, session);
                    let (result, rebound_waits) =
                        crate::engine::params::with_params_slice(params, || {
                            self.execute_statement_with_lock_wait_mode(
                                session,
                                dp.statement.as_ref().clone(),
                                true,
                                None,
                            )
                        });
                    wait_stats.count = wait_stats.count.saturating_add(rebound_waits.count);
                    wait_stats.elapsed = wait_stats.elapsed.saturating_add(rebound_waits.elapsed);
                    return (result, wait_stats);
                }
                Err(err) => {
                    if let Err(rollback_err) =
                        self.finalize_failed_statement_transaction(session, &err)
                    {
                        return (Err(rollback_err), wait_stats);
                    }
                    return (Err(err), wait_stats);
                }
            }
        }
    }

    /// Parse-free steady-state path for short, literal-bearing text DML.
    ///
    /// The byte scanner is only a candidate generator. On a cold shape the
    /// complete parser and `AutoParameterizeMode::All` must extract exactly the
    /// same values in the same order before the compiled plan is published.
    /// This is the same authority boundary used by the multi-statement DML
    /// batch cache, now applied to the far more common single-row
    /// INSERT/UPDATE/DELETE COM_QUERY traffic. Later executions bind the new
    /// literals directly to the validated plan, avoiding sqlparser, AST
    /// rewrites/clones and the generic dispatcher. Schema identity/version and
    /// every plan-relevant session semantic remain in the cache validator/key.
    fn try_execute_scanned_text_dml(
        &self,
        session: &mut SessionContext,
        sql: &str,
        shape: String,
        scanned_params: Vec<Value>,
    ) -> ScannedTextDmlProbe {
        if !self.plan_cache_enabled
            || !prepared_dml_cache_enabled()
            || !can_use_prepared_dml(session)
        {
            return ScannedTextDmlProbe::NotApplicable;
        }
        let key = crate::engine::cache::StatementKey {
            sql: Arc::<str>::from(shape),
            database: Arc::<str>::from(session.current_database.as_str()),
            semantics: plan_semantics(session),
        };
        let current_epoch = self.prepared_dml_cache.current_epoch();
        let dp = if let Some(plan) = self.prepared_dml_cache.get(&key, |plan| {
            prepared_dml::memo_is_valid(&self.db, plan, session, current_epoch)
                && should_use_shared_plan_cache(self.plan_cache_enabled, session, &plan.statement)
        }) {
            plan
        } else {
            let parsed = match parse_statement(sql) {
                Ok(statement) => statement,
                // Preserve the canonical error surface in the ordinary parse
                // path. Invalid statements are cold and do not merit carrying
                // a second error type through this optimization probe.
                Err(_) => return ScannedTextDmlProbe::NotApplicable,
            };
            let fallback_statement = parsed;
            let mut statement = fallback_statement.clone();
            // Default-on late binding is restricted to shapes whose runtime
            // coercion/error surface is already proven. Plain INSERT values
            // use the same column-coercion path with literals or placeholders;
            // atomic integer counters are checked by the established
            // provenance/type oracle. General UPDATE/DELETE expressions stay
            // on the legacy evaluator: parameterizing e.g. string-to-number
            // expressions can otherwise change strict-mode error timing.
            let proven_shape = matches!(&statement, Statement::Insert(insert) if !insert.delayed)
                || safe_integer_counter_update(&self.db, session, &statement)
                || safe_numeric_counter_update(&self.db, session, &statement)
                || safe_typed_point_update(&self.db, session, &statement);
            if !proven_shape
                || !should_use_shared_plan_cache(self.plan_cache_enabled, session, &statement)
            {
                return ScannedTextDmlProbe::Parsed(fallback_statement);
            }
            let auto_result = crate::engine::auto_param::auto_parameterize(
                crate::engine::auto_param::AutoParameterizeMode::All,
                &mut statement,
            );
            // The scanner is deliberately only a cheap candidate generator.
            // The full parser/rewriter is the oracle on the first execution;
            // require the placeholder AST to agree as well. Equal zero values
            // cannot prove whether unary negation was folded into a parameter.
            // A mismatch preserves the ordinary literal semantics.
            if auto_result.params != scanned_params
                || parse_statement(&key.sql).ok().as_ref() != Some(&statement)
            {
                return ScannedTextDmlProbe::Parsed(fallback_statement);
            }
            // Preserve the shared parsed-shape surface used by COM_QUERY and
            // QueryEngine::cached_text_statement. Publish the still-
            // unqualified placeholder AST before the compiled DML plan takes
            // its own qualified copy.
            self.cache_text_statement_shape(session, sql, &statement, &auto_result);
            if qualify_statement_tables(&self.db, session, &mut statement).is_err() {
                return ScannedTextDmlProbe::Parsed(fallback_statement);
            }
            let parsed = Arc::new(statement);
            let Some(plan) =
                prepared_dml::compile_prepared_dml(&self.db, session, &parsed, current_epoch)
            else {
                return ScannedTextDmlProbe::Parsed(fallback_statement);
            };
            self.prepared_dml_cache.insert(key, Arc::new(plan))
        };

        if let Err(err) = self.enforce_prepared_dml_global_gates(&dp.statement, session) {
            return ScannedTextDmlProbe::Executed(Err(err), QueryLogMetadata::default());
        }
        if let Err(err) = enforce_session_write_access(&self.db, session, &dp.statement) {
            return ScannedTextDmlProbe::Executed(Err(err), QueryLogMetadata::default());
        }
        if let Err(err) = enforce_account_resource_limits(session, &dp.statement) {
            return ScannedTextDmlProbe::Executed(Err(err), QueryLogMetadata::default());
        }
        let (result, wait_stats) =
            self.execute_prepared_dml_core(session, &dp, &scanned_params, Instant::now());
        ScannedTextDmlProbe::Executed(
            result,
            QueryLogMetadata {
                wake_purge: true,
                ..QueryLogMetadata::default()
            }
            .with_lock_wait_stats(wait_stats),
        )
    }

    fn try_execute_text_prepared_dml(
        &self,
        session: &mut SessionContext,
        statement: &Statement,
        fingerprint: &str,
        params: &[crate::types::Value],
    ) -> Option<(Result<QueryResponse, QueryError>, LockWaitStats)> {
        if !self.plan_cache_enabled
            || !prepared_dml_cache_enabled()
            || !matches!(
                statement,
                Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_)
            )
            || !can_use_prepared_dml(session)
            || !should_use_shared_plan_cache(self.plan_cache_enabled, session, statement)
        {
            return None;
        }

        let dp = self.get_or_compile_text_prepared_dml(session, statement, fingerprint)?;

        if let Err(err) = self.enforce_prepared_dml_global_gates(&dp.statement, session) {
            return Some((Err(err), LockWaitStats::default()));
        }
        if let Err(err) = enforce_session_write_access(&self.db, session, &dp.statement) {
            return Some((Err(err), LockWaitStats::default()));
        }
        if let Err(err) = enforce_account_resource_limits(session, &dp.statement) {
            return Some((Err(err), LockWaitStats::default()));
        }

        Some(self.execute_prepared_dml_core(session, &dp, params, Instant::now()))
    }

    fn get_or_compile_text_prepared_dml(
        &self,
        session: &SessionContext,
        statement: &Statement,
        fingerprint: &str,
    ) -> Option<Arc<crate::engine::api::PreparedDmlPlan>> {
        let key = crate::engine::cache::StatementKey {
            sql: Arc::<str>::from(fingerprint),
            database: Arc::<str>::from(session.current_database.as_str()),
            semantics: plan_semantics(session),
        };
        let current_epoch = self.prepared_dml_cache.current_epoch();
        let dp = if let Some(plan) = self.prepared_dml_cache.get(&key, |plan| {
            prepared_dml::memo_is_valid(&self.db, plan, session, current_epoch)
        }) {
            plan
        } else {
            let mut qualified = statement.clone();
            qualify_statement_tables(&self.db, session, &mut qualified).ok()?;
            let parsed = Arc::new(qualified);
            let plan =
                prepared_dml::compile_prepared_dml(&self.db, session, &parsed, current_epoch)?;
            self.prepared_dml_cache.insert(key, Arc::new(plan))
        };
        Some(dp)
    }

    fn prepare_text_dml_batch(
        &self,
        session: &SessionContext,
        sql: &str,
        shape: String,
        scanned_params: &[Value],
    ) -> Option<Arc<crate::engine::cache::PreparedDmlBatch>> {
        if !self.plan_cache_enabled
            || !prepared_dml_cache_enabled()
            || !can_use_prepared_dml(session)
            || crate::binlog::enabled()
        {
            return None;
        }
        let key = crate::engine::cache::StatementKey {
            sql: Arc::<str>::from(shape),
            database: Arc::<str>::from(session.current_database.as_str()),
            semantics: plan_semantics(session),
        };
        let current_epoch = self.prepared_dml_batch_cache.current_epoch();
        let batch = if let Some(batch) = self.prepared_dml_batch_cache.get(&key, |batch| {
            batch.plans.iter().all(|plan| {
                prepared_dml::memo_is_valid(&self.db, plan, session, current_epoch)
                    && should_use_shared_plan_cache(
                        self.plan_cache_enabled,
                        session,
                        &plan.statement,
                    )
            })
        }) {
            batch
        } else {
            let statements = parse_statements(sql).ok()?;
            let candidates = parse_statements(&key.sql).ok()?;
            if candidates.len() != statements.len() {
                return None;
            }
            if statements.len() <= 1
                || statements.iter().any(|stmt| {
                    !matches!(
                        stmt,
                        Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_)
                    )
                })
            {
                return None;
            }
            let mut flat_params = Vec::new();
            let mut param_counts = Vec::with_capacity(statements.len());
            let mut plans = Vec::with_capacity(statements.len());
            for (mut statement, mut candidate) in statements.into_iter().zip(candidates) {
                let auto_result = crate::engine::auto_param::auto_parameterize(
                    crate::engine::auto_param::AutoParameterizeMode::All,
                    &mut statement,
                );
                let fingerprint = auto_result.fingerprint.as_deref()?;
                crate::parser::visit::number_statement_placeholders(&mut candidate, &mut 0);
                if candidate != statement {
                    return None;
                }
                if !should_use_shared_plan_cache(self.plan_cache_enabled, session, &statement) {
                    return None;
                }
                let plan =
                    self.get_or_compile_text_prepared_dml(session, &statement, fingerprint)?;
                param_counts.push(auto_result.params.len());
                flat_params.extend(auto_result.params);
                plans.push(plan);
            }
            if flat_params != scanned_params {
                return None;
            }
            self.prepared_dml_batch_cache
                .insert(key, plans, param_counts)
        };
        let expected_params: usize = batch.param_counts.iter().sum();
        if expected_params != scanned_params.len() {
            return None;
        }
        Some(batch)
    }

    fn try_execute_text_prepared_dml_batch(
        &self,
        session: &mut SessionContext,
        sql: &str,
        shape: String,
        scanned_params: Vec<Value>,
    ) -> Option<(Result<QueryResponse, QueryError>, QueryLogMetadata)> {
        let batch = self.prepare_text_dml_batch(session, sql, shape, &scanned_params)?;
        Some(self.execute_text_prepared_dml_batch(session, sql, scanned_params, batch, None))
    }

    fn execute_text_prepared_dml_batch(
        &self,
        session: &mut SessionContext,
        sql: &str,
        scanned_params: Vec<Value>,
        batch: Arc<crate::engine::cache::PreparedDmlBatch>,
        mut statements: Option<&mut Vec<crate::engine::api::DmlStatementResult>>,
    ) -> (Result<QueryResponse, QueryError>, QueryLogMetadata) {
        let metadata = QueryLogMetadata {
            wake_purge: true,
            ..QueryLogMetadata::default()
        };
        let mut wait_stats = LockWaitStats::default();
        let mut response = QueryResponse::Ok;
        let mut offset = 0usize;
        let homogeneous_point_update = batch
            .plans
            .first()
            .is_some_and(|first| batch.plans.iter().all(|plan| Arc::ptr_eq(first, plan)))
            && session.account_limits.max_questions == 0
            && session.account_limits.max_updates == 0;
        let mut gates_prechecked = false;
        if homogeneous_point_update {
            // Resource limits are statement-counted even though the physical
            // update may be coalesced. Eligibility above admits only unlimited
            // accounts, so charging every child before mutation preserves the
            // counters without introducing a quota-failure boundary that
            // could otherwise need successful-prefix replay.
            for plan in batch.plans.iter() {
                if let Err(err) = self.enforce_prepared_dml_global_gates(&plan.statement, session) {
                    return (Err(err), metadata.with_lock_wait_stats(wait_stats));
                }
                if let Err(err) = enforce_session_write_access(&self.db, session, &plan.statement) {
                    return (Err(err), metadata.with_lock_wait_stats(wait_stats));
                }
                if let Err(err) = enforce_account_resource_limits(session, &plan.statement) {
                    return (Err(err), metadata.with_lock_wait_stats(wait_stats));
                }
            }
            gates_prechecked = true;
            let mut param_sets = Vec::with_capacity(batch.param_counts.len());
            let mut batch_offset = 0usize;
            for count in batch.param_counts.iter().copied() {
                let end = batch_offset + count;
                param_sets.push(&scanned_params[batch_offset..end]);
                batch_offset = end;
            }
            match prepared_dml::execute_homogeneous_point_update_batch(
                &self.db,
                session,
                batch
                    .plans
                    .first()
                    .expect("non-empty homogeneous DML batch"),
                &param_sets,
                statements.as_deref_mut(),
            ) {
                Ok(Some(batch_response)) => {
                    if statements.is_some() {
                        for plan in batch.plans.iter() {
                            if let Some(kind) =
                                crate::engine::status_counters::classify_statement_com(
                                    &plan.statement,
                                )
                            {
                                crate::engine::status_counters::record_session_com(session, kind);
                            }
                        }
                    }
                    return (
                        Ok(batch_response),
                        metadata.with_lock_wait_stats(wait_stats),
                    );
                }
                Ok(None) => {
                    // Eligibility rejection, lock conflict, or a semantic
                    // preflight error: the batch attempt was rolled back to
                    // its statement savepoint. Replay children below without
                    // charging the already-checked account/global gates twice.
                }
                Err(err) => {
                    return (Err(err), metadata.with_lock_wait_stats(wait_stats));
                }
            }
        }
        for (plan, count) in batch.plans.iter().zip(batch.param_counts.iter().copied()) {
            if statements.is_some() {
                begin_statement_diagnostics(session, sql);
                if let Some(kind) =
                    crate::engine::status_counters::classify_statement_com(&plan.statement)
                {
                    crate::engine::status_counters::record_session_com(session, kind);
                }
            }
            if !gates_prechecked {
                if let Err(err) = self.enforce_prepared_dml_global_gates(&plan.statement, session) {
                    return (Err(err), metadata.with_lock_wait_stats(wait_stats));
                }
                if let Err(err) = enforce_session_write_access(&self.db, session, &plan.statement) {
                    return (Err(err), metadata.with_lock_wait_stats(wait_stats));
                }
                if let Err(err) = enforce_account_resource_limits(session, &plan.statement) {
                    return (Err(err), metadata.with_lock_wait_stats(wait_stats));
                }
            }
            let end = offset + count;
            let (result, statement_wait_stats) = self.execute_prepared_dml_core(
                session,
                plan,
                &scanned_params[offset..end],
                Instant::now(),
            );
            wait_stats.count = wait_stats.count.saturating_add(statement_wait_stats.count);
            wait_stats.elapsed = wait_stats
                .elapsed
                .saturating_add(statement_wait_stats.elapsed);
            if let Some(out) = statements.as_deref_mut() {
                finish_statement_diagnostics(session, false, &result);
                if let Ok(QueryResponse::OkAffected { affected_rows }) = &result {
                    out.push(capture_dml_statement_result(session, *affected_rows));
                } else if matches!(result, Ok(QueryResponse::Ok)) {
                    out.push(capture_dml_statement_result(session, 0));
                }
            }
            match result {
                Ok(next_response) => response = next_response,
                Err(err) => return (Err(err), metadata.with_lock_wait_stats(wait_stats)),
            }
            offset = end;
        }
        (Ok(response), metadata.with_lock_wait_stats(wait_stats))
    }

    /// Drive the validated memo through the lock-wait retry loop. DML
    /// can park on a row lock (the legacy path uses
    /// `execute_statement_with_lock_wait_mode` for this), so the fast
    /// path needs the same retry semantics. Each retry borrows the
    /// cached `PreparedDmlPlan` and re-enters the storage layer with
    /// fresh `set_fk_checks_enabled` / `with_session_expr_info` /
    /// param-context state; no `session.clone()`, no
    /// `Statement::clone()`. Returns the final `QueryResponse` or the
    /// terminal error (after lock-wait timeout, deadlock, or
    /// rollback-after-error).
    fn run_prepared_dml(
        &self,
        session: &mut SessionContext,
        dp: &crate::engine::api::PreparedDmlPlan,
        sql: &str,
        params: &[crate::types::Value],
    ) -> Result<QueryResponse, QueryError> {
        if let Err(err) = session.check_cancelled() {
            return Err(err);
        }
        let started = Instant::now();
        let preserve_warnings = begin_statement_diagnostics(session, sql);
        let metadata = QueryLogMetadata {
            is_ddl: false,
            ..QueryLogMetadata::default()
        };
        let (result, wait_stats) = self.execute_prepared_dml_core(session, dp, params, started);
        let result =
            result.and_then(|response| response.localize_timestamps(&session.sysvars.time_zone));
        if result.is_ok() {
            self.undo_purge_worker.wake();
        }
        finish_statement_diagnostics(session, preserve_warnings, &result);
        let metadata = metadata.with_lock_wait_stats(wait_stats);
        self.finish_or_observe_query(session, sql, started, metadata, result, None)
    }
}

/// Session-state gate for the prepared-DML fast path. Conservative:
/// rejects any session whose dispatcher takes a non-default path that
/// the memo doesn't model — XA branches (their own state machine),
/// explicit/non-autocommit transactions, sessions running under a view
/// security-definer override, sessions inside a `LOCK TABLES` region.
/// Keep the first cut to single-statement autocommit DML: multi-statement
/// transaction retry semantics stay on the legacy path, whose savepoint
/// and lock-wait behavior is already covered by the executor tests.
fn can_use_prepared_dml(session: &SessionContext) -> bool {
    // Originally `autocommit && !explicit_txn && current_txn.is_none()`.
    // Relaxed to also cover explicit / non-autocommit transactions
    // (BenchmarkSQL TPC-C, anything using JDBC `con.setAutoCommit(false)`,
    // most ORMs). The fast path routes every execute through
    // `execute_in_transaction_with_session`, which already handles
    // implicit (autocommit=on) and explicit (autocommit=off /
    // `BEGIN`) transactions with the same statement-savepoint +
    // lock-wait retry semantics as the legacy dispatcher. Per-statement
    // diagnostics, FK checks, and
    // the `repl::after_local_commit_durable` semi-sync wait all run
    // identically; the fast path only skips the per-execute
    // `session.clone()` + `Statement::clone()` + the dispatcher's
    // match-arm chain. The remaining checks gate the session-state
    // gymnastics the legacy path performs that don't live inside the
    // prepared-DML plan:
    //   * `xa_branch` — XA START/END/PREPARE/COMMIT routes through
    //      its own state machine.
    //   * `read_only_txn` — error reporting for a read-only transaction
    //      attempting a write happens earlier on the legacy path.
    //   * `explicit_table_locks` — the legacy dispatcher enforces
    //      LOCK TABLES allow-list before reaching execute_qualified.
    //   * `view_security_overrides` — the legacy path swaps the
    //      current user, the fast path does not.
    session.xa_branch.is_none()
        && !session.read_only_txn
        && session.next_txn_read_only != Some(true)
        && !session.has_explicit_table_locks()
        && session.view_security_overrides.is_empty()
}

impl StorageQueryEngine {
    fn describe_prepared_statement(
        &self,
        session: &mut SessionContext,
        sql: &str,
    ) -> Result<Vec<Column>, QueryError> {
        // PREPARE binds metadata only. Never execute the stub: SET and
        // diagnostics handlers can mutate session state before EXECUTE.
        // Parse the original sql (not the lowercased `normalize_sql` output)
        // so COM_STMT_PREPARE column metadata preserves the MySQL-style
        // expression-column names (P2-C1), consistent with the execute path.
        let mut statement = (*self.parse_cached_statement(session, sql)?).clone();
        authorize_statement(&self.db, session, &statement)?;
        qualify_statement_tables(&self.db, session, &mut statement)?;
        // Bind routine return domains without executing their bodies. Keep the
        // same versioned descriptors available to PREPARE's column inference.
        // SAFETY: these pointers remain valid for this synchronous binding
        // scope; the resolver/descriptor path does not evaluate functions.
        let _routine_context = unsafe {
            crate::engine::expr::install_stored_fn_context(
                std::ptr::NonNull::from(&self.db),
                std::ptr::NonNull::from(&mut *session),
                std::ptr::NonNull::from(self.registry.as_ref()),
                false,
            )
        };
        // Bind view output metadata through the same expansion used by
        // EXECUTE; a view name is not a physical catalog table.
        routines::with_view_function_binding(|| {
            dispatch::expand_views(&self.db, session, &mut statement, 0)
        })?;
        routines::resolve_stored_functions(&self.db, session, &mut statement, &self.registry)?;
        // DESCRIBE/PREPARE is a binding operation and must observe the same
        // type-affecting session state as EXECUTE. Previously this path ran
        // outside both TLS scopes, so GROUP_CONCAT always advertised the
        // default 1024 cap and div/collation inference could use defaults.
        let group_concat_max_len = session.group_concat_max_len;
        crate::engine::executor::select::aggregate::with_group_concat_max_len(
            group_concat_max_len,
            || {
                crate::engine::expr::with_session_expr_info(session, || {
                    describe_statement_columns(&self.db, session, &statement)
                })
            },
        )
    }

    fn execute_with_log_metadata(
        &self,
        session: &mut SessionContext,
        sql: &str,
    ) -> (Result<QueryResponse, QueryError>, QueryLogMetadata) {
        // Fast-path 0: `BEGIN` / `COMMIT` / `ROLLBACK`. OLTP clients
        // (TPC-C, sysbench, plain JDBC `con.commit()`) send these on
        // every transaction, but the full dispatch path parses the SQL
        // through sqlparser, walks the auto-param pass, builds the
        // plan-cache hint, and re-enters the retry loop just to land
        // on the three-line `Statement::Begin/Commit/Rollback` arm at
        // the bottom of `dispatch::execute_statement`. A trimmed
        // ASCII match is < 50 ns and skips two `parse_statement` calls
        // per transaction (one in the StubEngine probe, one in the
        // main path) plus the qualify/auto-param/loop overhead. The
        // outer `<Self as QueryEngine>::execute` still wraps the
        // result with `begin/finish_statement_diagnostics` and
        // `log_query`, so behaviour is unchanged.
        // Install this session's row-lock wait timeout (`innodb_lock_wait_timeout`)
        // for the whole statement — the storage-layer wait loop reads it via a
        // thread-local. Held across every path below (fast paths take no row
        // locks, but keeping it here means one install point covers all).
        let _lock_wait_guard = crate::storage::LockWaitTimeoutGuard::new(Some(
            session
                .sysvars
                .innodb_lock_wait_timeout
                .unwrap_or_else(crate::engine::api::global_innodb_lock_wait_timeout),
        ));
        // A comment-only / whitespace-only query carries no statement. MySQL
        // answers such a COM_QUERY with a plain OK packet, and the `mysql` CLI
        // forwards mysqldump's bare `--` banner lines to the server verbatim —
        // so raising a 1064 parse error here aborts `mysql < dump.sql` on the
        // first `--`. Return an empty OK to match MySQL (single source of truth
        // in `crate::parser::sql_is_effectively_empty`).
        if crate::parser::sql_is_effectively_empty(sql) {
            return (Ok(QueryResponse::Ok), QueryLogMetadata::default());
        }
        let trimmed = sql.trim_matches(|c: char| c.is_whitespace() || c == ';');
        if let Some(simple_kind) = simple_txn_control_kind(trimmed) {
            let result = dispatch_simple_txn_control(&self.db, session, simple_kind);
            return (result, QueryLogMetadata::default());
        }
        // Stub-engine compatibility handlers (`SHOW VARIABLES`, …) only
        // apply to short statements that the stub itself recognises. A
        // 500KB bulk-INSERT body has no shot at matching, so we skip the
        // attempt entirely above a cheap length cap and save four full
        // `normalize_sql` / `to_ascii_uppercase` / `parse_statement`
        // passes that the old code paid unconditionally.
        const STUB_PROBE_MAX_BYTES: usize = 4096;
        if sql.len() <= STUB_PROBE_MAX_BYTES {
            if !statement_requires_storage_show_context(sql) {
                if let Ok(response) = StubQueryEngine.execute(session, sql) {
                    return (Ok(response), QueryLogMetadata::default());
                }
            }
        }

        // Parse the SQL once. For *bulk DML* (a literal-heavy
        // INSERT/REPLACE/UPDATE/DELETE body) we feed the parser the
        // raw bytes so 500KB never gets `normalize_sql`'d just to be
        // parsed — the bulk-INSERT fast path and the legacy sqlparser
        // pipeline both already handle whitespace, MySQL comments, and
        // case folding. For everything else (mostly SELECT) we keep
        // the historical contract of parsing the normalized form so
        // that the auto-parameterized plan-cache fingerprint stays
        // invariant under casing/whitespace/comment changes — sqlparser
        // preserves identifier case in `Ident.value`, so two queries
        // differing only in `a.name` vs `A.name` would otherwise
        // produce two different fingerprints.
        let auto_mode = crate::engine::auto_param::AutoParameterizeMode::from_env();
        let has_multi_separator = has_non_trailing_statement_separator(sql);
        // The multi-statement DML batch fast path is NOT gated on the
        // auto-parameterize mode: it is a self-contained prepared-DML
        // vehicle whose compile step re-derives the literal fingerprint
        // with `Mode::All` and verifies the scanned literals match the
        // parsed ones exactly (`flat_params != scanned_params → None`),
        // falling back to the legacy parse+plan path on any mismatch.
        // JDBC `rewriteBatchedStatements` sends every UPDATE batch as a
        // `;`-joined text — TPC-C profiling showed re-parsing those
        // bodies per execute at ~7.4% of total CPU. The `bulk_dml_
        // keyword_prefix` pre-check keeps the byte scan away from
        // multi-statement scripts that cannot be DML batches.
        let bulk_dml_kind = bulk_dml_keyword_prefix(sql);
        let mut scanned_statement = None;
        if has_multi_separator && bulk_dml_kind {
            if let Some((shape, params)) = scan_literal_normalized_dml(sql, true) {
                if let Some((result, metadata)) =
                    self.try_execute_text_prepared_dml_batch(session, sql, shape, params)
                {
                    return (result, metadata);
                }
            }
        } else if bulk_dml_kind {
            if let Some((shape, params)) = scan_literal_normalized_dml(sql, false) {
                match self.try_execute_scanned_text_dml(session, sql, shape, params) {
                    ScannedTextDmlProbe::NotApplicable => {}
                    ScannedTextDmlProbe::Parsed(statement) => {
                        scanned_statement = Some(statement);
                    }
                    ScannedTextDmlProbe::Executed(result, metadata) => {
                        return (result, metadata);
                    }
                }
            }
        }

        if has_multi_separator {
            return self.execute_mode_aware_batch(session, sql, auto_mode);
        }
        let mut statement = if let Some(statement) = scanned_statement {
            statement
        } else if bulk_dml_kind {
            match parse_statement(sql) {
                Ok(statement) => statement,
                Err(err) => {
                    return (
                        Err(parse_error_to_query_error(err)),
                        QueryLogMetadata::default(),
                    )
                }
            }
        } else {
            // Drop comments sitting in an `EXPLAIN`/`DESCRIBE` header *before*
            // `normalize_sql` collapses newlines: otherwise the `\n` that
            // terminates a `-- …` line comment becomes a space and the comment
            // swallows the rest of the statement (e.g. `EXPLAIN\n-- c\nSELECT 1`
            // → `explain -- c select 1` → parse error). For non-EXPLAIN SQL the
            // helper returns the input untouched.
            // Parse the original (only explain-header comments stripped) SQL,
            // NOT the lowercased `normalize_sql` output: P2-C1 names unaliased
            // expression columns by their source text, so the casing/spacing
            // must survive to the parser (`COUNT(*)` not `count(*)`). Plan-cache
            // dedup happens structurally via the auto-parameterisation
            // fingerprint below, independent of source case, so nothing depends
            // on parsing the normalised form here.
            match self.parse_cached_statement(session, sql) {
                Ok(statement) => (*statement).clone(),
                Err(err) => return (Err(err), QueryLogMetadata::default()),
            }
        };

        // Phase 2 auto-parameterization. SELECT statements get their safe
        // literals extracted into `Expr::Placeholder` + a parallel
        // `Vec<Value>`, so two queries that differ only in literal values
        // collapse to a single cached plan. The fingerprint becomes the
        // plan-cache key so different literal values still hit the same
        // entry. Behaviour is gated by `MYDB_AUTO_PARAMETERIZE`.
        let statement_auto_mode =
            auto_param_mode_for_statement(&self.db, session, &statement, auto_mode);
        let promoted_safe_counter = auto_mode
            == crate::engine::auto_param::AutoParameterizeMode::Select
            && safe_integer_counter_update(&self.db, session, &statement);
        let mut auto_result =
            crate::engine::auto_param::auto_parameterize(statement_auto_mode, &mut statement);
        if promoted_safe_counter {
            // The default-on subset borrows placeholder evaluation only. Do
            // not silently opt it into the prepared-DML plan cache: that
            // changes cache observability and bypasses legacy wrapper hooks.
            auto_result.fingerprint = None;
        }

        if let Statement::MultiStatement { .. } = statement {
            self.execute_mode_aware_batch(session, sql, auto_mode)
        } else {
            self.cache_text_statement_shape(session, sql, &statement, &auto_result);
            self.execute_single_statement_tail(session, sql, statement, auto_result, true)
        }
    }

    /// Parse only the next child using the session state left by its prefix.
    /// Keep lexical ranges while the mode is stable; rescan original source
    /// after a mode change, preserving completed prefix effects and errors.
    fn execute_mode_aware_batch(
        &self,
        session: &mut SessionContext,
        sql: &str,
        auto_mode: crate::engine::auto_param::AutoParameterizeMode,
    ) -> (Result<QueryResponse, QueryError>, QueryLogMetadata) {
        let mut mode = crate::engine::api::parse_temporal_sql_mode_bits(&session.sql_mode);
        let (mut effective, mut ranges) = crate::parser::split_query_statements(sql, mode);
        let mut base = 0;
        let mut index = 0;
        let mut metadata = QueryLogMetadata::default();
        let mut response = QueryResponse::Ok;
        while index < ranges.len() {
            let _http_statement = crate::engine::http::fresh_statement();
            let range = ranges[index].clone();
            let _mode = crate::parser::ParserSqlModeGuard::activate(mode);
            let child_sql = &effective[range.clone()];
            let mut statement = match parse_statement(child_sql) {
                Ok(statement) => statement,
                Err(error) => return (Err(parse_error_to_query_error(error)), metadata),
            };
            let statement_auto_mode =
                auto_param_mode_for_statement(&self.db, session, &statement, auto_mode);
            let promoted_safe_counter = auto_mode
                == crate::engine::auto_param::AutoParameterizeMode::Select
                && safe_integer_counter_update(&self.db, session, &statement);
            let mut auto_result =
                crate::engine::auto_param::auto_parameterize(statement_auto_mode, &mut statement);
            if promoted_safe_counter {
                auto_result.fingerprint = None;
            }
            let (result, child_metadata) = self.execute_single_statement_tail(
                session,
                child_sql,
                statement,
                auto_result,
                true,
            );
            metadata.is_ddl |= child_metadata.is_ddl;
            metadata.lock_wait_count = metadata
                .lock_wait_count
                .saturating_add(child_metadata.lock_wait_count);
            metadata.lock_wait_ms += child_metadata.lock_wait_ms;
            match result {
                Ok(next) => response = next,
                Err(error) => return (Err(error), metadata),
            }
            index += 1;
            if index < ranges.len() {
                // Finish streaming prefix expressions before executing their successor.
                if let Err(error) = response.materialize() {
                    return (Err(error), metadata);
                }
                response = QueryResponse::Ok;
            }
            let next_mode = crate::engine::api::parse_temporal_sql_mode_bits(&session.sql_mode);
            if next_mode != mode && index < ranges.len() {
                base += range.end;
                if sql.as_bytes().get(base) == Some(&b';') {
                    base += 1;
                }
                (effective, ranges) =
                    crate::parser::split_query_statements(&sql[base..], next_mode);
                index = 0;
                mode = next_mode;
            }
        }
        (Ok(response), metadata)
    }

    /// Publish the literal skeleton of a just-executed text statement so
    /// later executions with different literals can reuse this AST.
    ///
    /// Only statement kinds the compiled prepared vehicles accept are
    /// published: SELECT feeds `execute_prepared_select`, and
    /// INSERT/UPDATE/DELETE feed `execute_prepared_dml`. A missing fingerprint
    /// means auto-parameterization declined the statement (mode, or a
    /// plan-shape-sensitive literal), so no shape may be shared.
    fn cache_text_statement_shape(
        &self,
        session: &SessionContext,
        sql: &str,
        statement: &Statement,
        auto_result: &crate::engine::auto_param::AutoParamResult,
    ) {
        if auto_result.fingerprint.is_none()
            || !matches!(
                statement,
                Statement::Select(_)
                    | Statement::Insert(_)
                    | Statement::Update(_)
                    | Statement::Delete(_)
            )
        {
            return;
        }
        let parser_mode_bits = crate::engine::api::parse_temporal_sql_mode_bits(&session.sql_mode);
        let Some((key, scanned_params)) = text_shape_key_and_params(sql, parser_mode_bits) else {
            return;
        };
        // This equality is the authority boundary: a lexical candidate is
        // published only when the complete parser + auto-parameterizer
        // produced the same values in the same placeholder order.
        if scanned_params != auto_result.params
            || parse_statement(&key.sql).ok().as_ref() != Some(statement)
        {
            return;
        }
        self.statement_cache
            .insert(key, Arc::new(statement.clone()));
    }

    /// COM_QUERY inline route: the session obtains the exact cached AST and
    /// classifies it as a lock-safe read (pure SELECT / SHOW / DESCRIBE
    /// without `FOR UPDATE`/`FOR SHARE`), so we can skip the
    /// `tokio::task::spawn_blocking` hand-off and run on the current
    /// runtime worker. This is the same tail as
    /// `execute_with_log_metadata` once the AST is available: auto-param,
    /// plan-cache hint, lock-wait dispatch, log_query.
    fn execute_with_log_metadata_parsed(
        &self,
        session: &mut SessionContext,
        sql: &str,
        mut statement: Statement,
    ) -> (Result<QueryResponse, QueryError>, QueryLogMetadata) {
        let auto_mode = crate::engine::auto_param::AutoParameterizeMode::from_env();
        let statement_auto_mode =
            auto_param_mode_for_statement(&self.db, session, &statement, auto_mode);
        let promoted_safe_counter = auto_mode
            == crate::engine::auto_param::AutoParameterizeMode::Select
            && safe_integer_counter_update(&self.db, session, &statement);
        let mut auto_result =
            crate::engine::auto_param::auto_parameterize(statement_auto_mode, &mut statement);
        if promoted_safe_counter {
            auto_result.fingerprint = None;
        }

        self.cache_text_statement_shape(session, sql, &statement, &auto_result);

        self.execute_single_statement_tail(session, sql, statement, auto_result, false)
    }

    /// Shared post-parse path for normal COM_QUERY and the protocol's
    /// already-parsed inline SELECT route. Auto-parameter binding, cache-key
    /// construction, prepared-DML reuse, lock-wait dispatch and binlog audit
    /// live here exactly once.
    fn execute_single_statement_tail(
        &self,
        session: &mut SessionContext,
        sql: &str,
        statement: Statement,
        auto_result: crate::engine::auto_param::AutoParamResult,
        reparse_when_cache_hint_is_unsafe: bool,
    ) -> (Result<QueryResponse, QueryError>, QueryLogMetadata) {
        let is_ddl = is_ddl_statement(&statement);
        let wake_purge = is_ddl
            || matches!(
                &statement,
                Statement::Insert(_)
                    | Statement::Update(_)
                    | Statement::Delete(_)
                    | Statement::Commit { .. }
                    | Statement::Rollback { .. }
                    | Statement::SetAutocommit(true)
                    | Statement::XaCommit { .. }
                    | Statement::XaRollback { .. }
            );
        let metadata = QueryLogMetadata {
            is_ddl,
            wake_purge,
            ..QueryLogMetadata::default()
        };
        let use_shared_plan_cache =
            should_use_shared_plan_cache(self.plan_cache_enabled, session, &statement);
        let skip_plan_cache_hint = statement_skips_plan_cache_hint(&statement);
        let hint = if use_shared_plan_cache && !skip_plan_cache_hint {
            let cache_sql = match auto_result.fingerprint.as_deref() {
                Some(fp) => Arc::<str>::from(fp),
                None => Arc::<str>::from(normalize_sql(sql).as_str()),
            };
            Some(crate::engine::cache::QueryHint {
                normalized_sql: cache_sql,
                database: Arc::<str>::from(session.current_database.as_str()),
                semantics: plan_semantics(session),
                parameterized: auto_result.fingerprint.is_some(),
                plan_cache: Arc::downgrade(&self.plan_cache),
            })
        } else {
            None
        };
        let binloggable_ddl = is_ddl
            && !matches!(statement, Statement::OptimizeTable { .. })
            && !statement_is_temp_ddl(session, &statement);
        let ddl_binlog_context =
            (binloggable_ddl && session.sql_log_bin && crate::binlog::enabled()).then(|| {
                let ctx = binlog_ctx_from_session(session);
                crate::binlog::enter_ddl_txn_context(
                    &session.current_database,
                    &normalize_sql(sql),
                    &ctx,
                )
            });
        if let Some((result, wait_stats)) =
            auto_result.fingerprint.as_deref().and_then(|fingerprint| {
                self.try_execute_text_prepared_dml(
                    session,
                    &statement,
                    fingerprint,
                    &auto_result.params,
                )
            })
        {
            return (result, metadata.with_lock_wait_stats(wait_stats));
        }
        let execute = || {
            crate::engine::params::with_auto_params(auto_result.params, || {
                if reparse_when_cache_hint_is_unsafe && skip_plan_cache_hint {
                    self.execute_statement_with_lock_wait_reparse(session, statement, sql)
                } else {
                    self.execute_statement_with_lock_wait(session, statement)
                }
            })
        };
        let (result, wait_stats) = match hint {
            Some(hint) => crate::engine::cache::with_query_hint(hint, execute),
            None => execute(),
        };
        let result = maybe_log_binlog_ddl_lazy(
            self.db.as_ref(),
            session,
            is_ddl,
            binloggable_ddl,
            sql,
            result,
        );
        drop(ddl_binlog_context);
        (result, metadata.with_lock_wait_stats(wait_stats))
    }

    #[cfg(test)]
    fn execute_parsed_statement(
        &self,
        session: &mut SessionContext,
        sql: &str,
        statement: Statement,
    ) -> Result<QueryResponse, QueryError> {
        let started = Instant::now();
        let is_ddl = is_ddl_statement(&statement);
        let metadata = QueryLogMetadata {
            is_ddl,
            ..QueryLogMetadata::default()
        };
        let binloggable_ddl = is_ddl
            && !matches!(statement, Statement::OptimizeTable { .. })
            && !statement_is_temp_ddl(session, &statement);
        let ddl_binlog_context =
            (binloggable_ddl && session.sql_log_bin && crate::binlog::enabled()).then(|| {
                let ctx = binlog_ctx_from_session(session);
                crate::binlog::enter_ddl_txn_context(
                    &session.current_database,
                    &normalize_sql(sql),
                    &ctx,
                )
            });
        let (result, wait_stats) = self.execute_statement_with_lock_wait(session, statement);
        let result = maybe_log_binlog_ddl(
            self.db.as_ref(),
            session,
            is_ddl,
            binloggable_ddl,
            &normalize_sql(sql),
            result,
        );
        drop(ddl_binlog_context);
        let metadata = metadata.with_lock_wait_stats(wait_stats);
        self.finish_or_observe_query(session, sql, started, metadata, result, None)
    }

    fn execute_qualified_parsed_statement(
        &self,
        session: &mut SessionContext,
        sql: &str,
        statement: Statement,
    ) -> Result<QueryResponse, QueryError> {
        let started = Instant::now();
        let is_ddl = is_ddl_statement(&statement);
        let metadata = QueryLogMetadata {
            is_ddl,
            ..QueryLogMetadata::default()
        };
        let binloggable_ddl = is_ddl
            && !matches!(statement, Statement::OptimizeTable { .. })
            && !statement_is_temp_ddl(session, &statement);
        let ddl_binlog_context =
            (binloggable_ddl && session.sql_log_bin && crate::binlog::enabled()).then(|| {
                let ctx = binlog_ctx_from_session(session);
                crate::binlog::enter_ddl_txn_context(
                    &session.current_database,
                    &normalize_sql(sql),
                    &ctx,
                )
            });
        let already_qualified = !statement_refs_temp_table(session, &statement);
        let (result, wait_stats) =
            self.execute_statement_with_lock_wait_mode(session, statement, already_qualified, None);
        let result = maybe_log_binlog_ddl(
            self.db.as_ref(),
            session,
            is_ddl,
            binloggable_ddl,
            &normalize_sql(sql),
            result,
        );
        drop(ddl_binlog_context);
        let metadata = metadata.with_lock_wait_stats(wait_stats);
        self.finish_or_observe_query(session, sql, started, metadata, result, None)
    }

    fn execute_statement_with_lock_wait(
        &self,
        session: &mut SessionContext,
        statement: Statement,
    ) -> (Result<QueryResponse, QueryError>, LockWaitStats) {
        self.execute_statement_with_lock_wait_mode(session, statement, false, None)
    }

    /// One-shot text bulk DML should not deep-clone a multi-megabyte literal
    /// AST merely to retain a retry copy. Execute the owned AST directly and,
    /// only if lock waiting actually occurs, rebuild the next attempt from the
    /// original SQL. COM_QUERY carries literals (not protocol placeholders),
    /// so the reparsed statement is semantically identical even when the first
    /// attempt went through auto-parameterization.
    fn execute_statement_with_lock_wait_reparse(
        &self,
        session: &mut SessionContext,
        statement: Statement,
        retry_sql: &str,
    ) -> (Result<QueryResponse, QueryError>, LockWaitStats) {
        self.execute_statement_with_lock_wait_mode(session, statement, false, Some(retry_sql))
    }

    fn execute_statement_with_lock_wait_mode(
        &self,
        session: &mut SessionContext,
        statement: Statement,
        already_qualified: bool,
        retry_sql: Option<&str>,
    ) -> (Result<QueryResponse, QueryError>, LockWaitStats) {
        let _redo_scope = crate::storage::RedoScopeGuard::new(session.redo_enabled);
        crate::engine::cancel::set_process_base_state(process_state_for_statement(&statement));
        if let Some(result) = self.execute_alter_system_statement(session, &statement) {
            return (result, LockWaitStats::default());
        }
        // Make the session's `use_invisible_indexes` optimizer flag
        // visible to every index-candidate iteration below (planner and
        // executor index pickers alike) for the duration of this
        // statement. Plans built under the flag bypass the shared plan
        // cache (`should_use_shared_plan_cache`).
        let _invisible_scope = crate::storage::schema::InvisibleIndexScope::install(
            session.sysvars.use_invisible_indexes,
        );
        let _zone_map_scope = crate::storage::schema::ZoneMapOptimizerScope::install(
            !session.sysvars.optimizer_switch.contains("zone_map=off"),
        );
        let _like_ngram_scope = crate::storage::schema::LikeNgramPrefilterScope::install(
            !session
                .sysvars
                .optimizer_switch
                .contains("like_ngram_prefilter=off"),
        );
        let optimizer_switch_has = |name: &str, enabled: bool| {
            let expected = format!("{name}={}", if enabled { "on" } else { "off" });
            session
                .sysvars
                .optimizer_switch
                .split(',')
                .any(|part| part.trim() == expected)
        };
        let index_merge_master = !optimizer_switch_has("index_merge", false);
        let _index_merge_scope = crate::planner::IndexMergeOptimizerScope::install(
            index_merge_master && !optimizer_switch_has("index_merge_union", false),
            index_merge_master && !optimizer_switch_has("index_merge_sort_union", false),
            index_merge_master && !optimizer_switch_has("index_merge_intersection", false),
        );
        let started = Instant::now();
        let mut waited_for_lock = false;
        let mut wait_stats = LockWaitStats::default();
        let mut next_statement = Some(statement);
        loop {
            if let Err(err) = session.check_cancelled() {
                if waited_for_lock {
                    clear_session_lock_wait(&self.db, session);
                }
                return (Err(err), wait_stats);
            }
            // Per-statement `session.clone()` is a heavy `SessionContext`
            // (HashMaps + Arcs + Vecs) copy that we only used to give the
            // expression evaluator an immutable view of the session while
            // `execute_statement_with_qualification` borrowed `&mut session`
            // concurrently. The `_raw` variant installs a `NonNull` in the
            // TLS slot, so we can keep the original session pinned across
            // the closure without the alloc-heavy clone. The eval path
            // only reads through the pointer (its mutations route through
            // the `&mut session` that's also live), so the aliasing is
            // safe under the documented contract.
            let session_ptr = std::ptr::NonNull::from(&*session);
            let attempt_statement = match next_statement.take() {
                Some(statement) => statement,
                None => {
                    let Some(sql) = retry_sql else {
                        unreachable!("clone retry path always seeds the next statement")
                    };
                    match parse_statement(sql) {
                        Ok(statement) => statement,
                        Err(err) => {
                            if waited_for_lock {
                                clear_session_lock_wait(&self.db, session);
                            }
                            return (Err(parse_error_to_query_error(err)), wait_stats);
                        }
                    }
                }
            };
            let retry_clone = retry_sql.is_none().then(|| attempt_statement.clone());
            match unsafe {
                super::expr::with_session_expr_info_raw(session_ptr, || {
                    execute_statement_with_qualification(
                        &self.db,
                        session,
                        attempt_statement,
                        already_qualified,
                        &self.registry,
                    )
                })
            } {
                Ok(response) => {
                    if waited_for_lock {
                        clear_session_lock_wait(&self.db, session);
                    }
                    return (Ok(response), wait_stats);
                }
                Err(err) if is_lock_wait_pending(&err) && crate::engine::http::sent() => {
                    clear_session_lock_wait(&self.db, session);
                    let failure = crate::engine::http::error("ER_HTTP_REPLAY_FORBIDDEN", "lock wait requires replay after an HTTP send; external effects may have occurred");
                    if let Err(cleanup) =
                        self.finalize_failed_statement_transaction(session, &failure)
                    {
                        return (Err(cleanup), wait_stats);
                    }
                    return (Err(failure), wait_stats);
                }
                Err(err) if is_lock_wait_pending(&err) => {
                    waited_for_lock = true;
                    next_statement = retry_clone;
                    if let Err(err) =
                        wait_for_lock_signal_or_cancel(&self.db, session, started, &mut wait_stats)
                    {
                        clear_session_lock_wait(&self.db, session);
                        if let Err(rollback_err) =
                            self.finalize_failed_statement_transaction(session, &err)
                        {
                            return (Err(rollback_err), wait_stats);
                        }
                        return (Err(err), wait_stats);
                    }
                }
                Err(err) => {
                    if waited_for_lock {
                        clear_session_lock_wait(&self.db, session);
                    }
                    if let Err(rollback_err) =
                        self.finalize_failed_statement_transaction(session, &err)
                    {
                        return (Err(rollback_err), wait_stats);
                    }
                    return (Err(err), wait_stats);
                }
            }
        }
    }

    /// Last-resort statement-boundary cleanup. DML paths normally finalize
    /// through `finish_statement_txn`, but validation/DDL errors can occur
    /// after lazily opening an autocommit transaction and before reaching that
    /// helper. Never let such an orphan transaction retain row/table/MDL
    /// locks. Explicit transactions retain their transaction and rely on the
    /// per-DML statement savepoint, except for errors that MySQL defines as
    /// transaction-aborting (deadlock). KILL QUERY and lock timeout abort only the statement.
    fn finalize_failed_statement_transaction(
        &self,
        session: &mut SessionContext,
        err: &QueryError,
    ) -> Result<(), QueryError> {
        // `LockWouldBlock` is an internal scheduling marker, not the
        // user-visible ER_LOCK_WAIT_TIMEOUT. Neither error aborts an explicit
        // transaction.  The prepared-DML statement wrapper has already
        // restored its statement savepoint before this reaches us; retain the
        // explicit transaction so the protocol layer can replay only this
        // statement on the blocking pool.
        if crate::storage::is_lock_would_block_error(err) {
            return Ok(());
        }
        let orphaned_autocommit = session.autocommit
            && !session.explicit_txn
            && session.xa_branch.is_none()
            && session.current_txn.is_some();
        if orphaned_autocommit
            || (is_transaction_rollback_error(err)
                && (session.current_txn.is_some() || session.explicit_txn || !session.autocommit))
        {
            let keep_explicit = is_transaction_rollback_error(err)
                && session.explicit_txn
                && session.xa_branch.is_none();
            rollback_session_txn(&self.db, session)?;
            if keep_explicit {
                // MySQL keeps OPTION_BEGIN across this implicit rollback:
                // later statements run in a new transaction that the client
                // still ends with COMMIT/ROLLBACK instead of autocommitting.
                tx::begin_explicit_txn(&self.db, session)?;
            }
        }
        Ok(())
    }

    fn finish_or_observe_query(
        &self,
        session: &SessionContext,
        sql: &str,
        started: Instant,
        metadata: QueryLogMetadata,
        result: Result<QueryResponse, QueryError>,
        digest_shape: Option<Arc<str>>,
    ) -> Result<QueryResponse, QueryError> {
        match result {
            Ok(QueryResponse::StreamingResultSet(stream)) => Ok(QueryResponse::StreamingResultSet(
                self.observe_stream_query(session, sql, started, metadata, stream, digest_shape),
            )),
            result => {
                if let Some(shape) = digest_shape {
                    crate::engine::perf_events::with_statement_digest_sql_override(shape, || {
                        self.log_query(session, sql, started.elapsed(), metadata, &result)
                    });
                } else {
                    self.log_query(session, sql, started.elapsed(), metadata, &result);
                }
                result
            }
        }
    }

    fn observe_stream_query(
        &self,
        session: &SessionContext,
        sql: &str,
        started: Instant,
        metadata: QueryLogMetadata,
        mut stream: StreamingResultSet,
        digest_shape: Option<Arc<str>>,
    ) -> StreamingResultSet {
        let counter = crate::engine::perf_events::current_rows_examined_counter();
        crate::engine::perf_events::clear_statement_rows_examined();
        let metadata = Self::enrich_dml_metadata(metadata);
        let session = session.clone();
        let sql = sql.to_owned();
        let sql_log = Arc::clone(&self.sql_log);
        let log_config = self.log_config.clone();
        let inner = std::mem::replace(
            &mut stream.cursor,
            Box::new(crate::engine::api::VecResultCursor::new(Vec::new())),
        );
        stream.cursor = Box::new(ObservedStreamCursor {
            inner: Some(inner),
            rows_sent: 0,
            warnings: Default::default(),
            finish: Some(Box::new(move |rows_sent, error| {
                let mut metadata = metadata;
                let (total, sequential) = counter.as_ref().map_or((0, 0), |counter| counter.take());
                metadata.rows_examined = total;
                metadata.sequential_rows_examined = sequential;
                let result = match error {
                    Some(error) => Err(error),
                    None => Ok(QueryResponse::StreamingResultSet(StreamingResultSet {
                        columns: Vec::new(),
                        found_rows: Some(rows_sent),
                        cursor: Box::new(crate::engine::api::VecResultCursor::new(Vec::new())),
                    })),
                };
                let record = || {
                    Self::record_query_completion(
                        &sql_log,
                        &log_config,
                        &session,
                        &sql,
                        started.elapsed(),
                        metadata,
                        &result,
                    )
                };
                if let Some(shape) = digest_shape {
                    crate::engine::perf_events::with_statement_digest_sql_override(shape, record);
                } else {
                    record();
                }
            })),
        });
        stream
    }

    fn log_query(
        &self,
        session: &SessionContext,
        sql: &str,
        elapsed: Duration,
        mut metadata: QueryLogMetadata,
        result: &Result<QueryResponse, QueryError>,
    ) {
        // P1-D3: drain the per-thread rows-examined counter populated
        // by storage scan helpers. Done before record_statement so
        // performance_schema digest accounting can also use the real
        // value going forward (it currently ignores it but the field
        // is now plumbed end-to-end).
        metadata.rows_examined = crate::engine::perf_events::take_rows_examined();
        metadata.sequential_rows_examined = crate::engine::perf_events::take_scan_rows_examined();
        crate::engine::perf_events::clear_statement_rows_examined();
        metadata = Self::enrich_dml_metadata(metadata);
        Self::record_query_completion(
            &self.sql_log,
            &self.log_config,
            session,
            sql,
            elapsed,
            metadata,
            result,
        );
    }

    fn enrich_dml_metadata(mut metadata: QueryLogMetadata) -> QueryLogMetadata {
        let odku = crate::engine::api::take_statement_odku_stats();
        metadata.dml_path = match (odku.batched_rows > 0, odku.legacy_rows > 0) {
            (true, true) => "odku_mixed",
            (true, false) => "odku_batch",
            (false, true) => "odku_legacy",
            (false, false) => "",
        };
        metadata.dml_rows_input = odku.rows_input;
        metadata.dml_rows_batched = odku.batched_rows;
        metadata.dml_conflict_rows = odku.conflict_rows;
        metadata.dml_legacy_rows = odku.legacy_rows;
        metadata.dml_race_rollbacks = odku.race_rollbacks;
        metadata
    }

    fn record_query_completion(
        sql_log: &SqlLogService,
        log_config: &QueryLogConfig,
        session: &SessionContext,
        sql: &str,
        elapsed: Duration,
        metadata: QueryLogMetadata,
        result: &Result<QueryResponse, QueryError>,
    ) {
        let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
        let sql = sql.trim();
        // Fold statement totals once: InnoDB reads include index access,
        // whereas Handler_read_rnd_next covers sequential scan rows only.
        crate::engine::status_counters::add_rows_read(
            session,
            metadata.rows_examined,
            metadata.sequential_rows_examined,
        );

        if result
            .as_ref()
            .err()
            .is_some_and(crate::storage::is_lock_would_block_error)
        {
            crate::storage::record_inline_lock_would_block_log_suppressed();
            return;
        }

        crate::engine::perf_events::record_statement(session, sql, elapsed, metadata, result);

        if let Err(err) = result {
            crate::engine::perf_errors::record_raised(session, err);
        }

        sql_log.record_slow(session, sql, elapsed, metadata, result);

        // P0-A2 audit log: structured per-statement entry. Off by
        // default; activated by MYDB_AUDIT_LOG. The check is a single
        // atomic load when off, so the cost on the hot path stays
        // imperceptible.
        if crate::engine::audit_log::is_active() {
            let err_ref = result.as_ref().err();
            crate::engine::audit_log::record_statement(session, sql, elapsed_ms, err_ref);
        }

        // nextest captures stderr separately for every case. Successful DDL
        // and load-inflated "slow" lines add large formatting/capture costs
        // without diagnostic value; production builds retain both logs, and
        // failures remain visible in tests.
        let emit_routine_test_log = !cfg!(test);
        let emit_ddl_log = metadata.is_ddl && emit_routine_test_log;
        let emit_slow_log = elapsed > log_config.slow_sql_threshold && emit_routine_test_log;
        if emit_ddl_log || emit_slow_log || result.is_err() {
            let sql_for_log = redact_sql_for_log(sql);
            if emit_ddl_log {
                let status = if result.is_ok() { "ok" } else { "failed" };
                log_message(format_args!(
                    "[ddl] conn={} user={:?} host={:?} db={:?} elapsed_ms={elapsed_ms:.3} status={} sql={sql_for_log:?}",
                    session.connection_id,
                    session.user,
                    session.host,
                    session.current_database,
                    status
                ));
            }

            if emit_slow_log {
                log_message(slow_sql_log_line(
                    session,
                    elapsed_ms,
                    log_config,
                    metadata,
                    &sql_for_log,
                ));
            }

            if let Err(err) = result {
                log_message(format_args!(
                    "[failed-sql] conn={} user={:?} host={:?} db={:?} elapsed_ms={elapsed_ms:.3} errno={} sql_state={} error={:?} sql={sql_for_log:?}",
                    session.connection_id,
                    session.user,
                    session.host,
                    session.current_database,
                    err.code(),
                    err.sql_state(),
                    if crate::engine::ai::sql::sql_may_have_ai(sql) || crate::engine::http::sensitive(sql) { crate::engine::http::redaction_marker(sql, true) } else { err.message() },
                ));
            }
        }
    }

    #[cfg(test)]
    fn slow_sql_log_line_for_test(
        session: &SessionContext,
        elapsed_ms: f64,
        config: &QueryLogConfig,
        metadata: QueryLogMetadata,
        sql: &str,
    ) -> String {
        slow_sql_log_line(session, elapsed_ms, config, metadata, sql)
    }

    #[allow(dead_code)]
    pub fn execute(&self, sql: &str) -> Result<QueryResponse, QueryError> {
        let mut session = SessionContext::default();
        <Self as QueryEngine>::execute(self, &mut session, sql)?.materialize()
    }

    #[allow(dead_code)]
    pub fn execute_with_session(
        &self,
        session: &mut SessionContext,
        sql: &str,
    ) -> Result<QueryResponse, QueryError> {
        let response = <Self as QueryEngine>::execute(self, session, sql)?;
        let explicit_found_rows = match &response {
            QueryResponse::StreamingResultSet(result) => result.found_rows,
            _ => None,
        };
        let result = response.materialize();
        // Streaming workers finish after the statement dispatcher has copied
        // its synchronous condition area into `session`. Materialization
        // publishes their deferred diagnostics on this thread; merge them so
        // the test/client helper observes the same SHOW WARNINGS state as the
        // protocol streaming writer.
        let mut deferred = crate::engine::api::take_statement_warnings();
        session.warnings.append(&mut deferred);
        if let Ok(QueryResponse::ResultSet(result_set)) = &result {
            session.found_rows = explicit_found_rows.unwrap_or(result_set.rows.len() as u64);
        }
        result
    }

    #[cfg(test)]
    pub fn execute_streaming_for_test(&self, sql: &str) -> Result<QueryResponse, QueryError> {
        let mut session = SessionContext::default();
        let _streaming = select::force_streaming_response_scope();
        <Self as QueryEngine>::execute(self, &mut session, sql)
    }

    #[cfg(test)]
    pub fn execute_streaming_with_session_for_test(
        &self,
        session: &mut SessionContext,
        sql: &str,
    ) -> Result<QueryResponse, QueryError> {
        let _streaming = select::force_streaming_response_scope();
        <Self as QueryEngine>::execute(self, session, sql)
    }

    #[cfg(test)]
    fn execute_prepared_for_test(
        &self,
        session: &mut SessionContext,
        sql: &str,
        statement: Statement,
    ) -> Result<QueryResponse, QueryError> {
        self.execute_parsed_statement(session, sql, statement)?
            .materialize()
    }
}

pub(super) fn process_state_for_statement(
    statement: &Statement,
) -> crate::engine::session_registry::ProcessState {
    use crate::engine::session_registry::ProcessState;
    match statement {
        Statement::Insert(_) | Statement::LoadData(_) => ProcessState::Update,
        Statement::Update(_) | Statement::Delete(_) => ProcessState::Updating,
        Statement::CreateTable(_) | Statement::CreateTableLike { .. } => {
            ProcessState::CreatingTable
        }
        Statement::CreateIndex(_) => ProcessState::CreatingIndex,
        Statement::AlterTable(_) => ProcessState::AlteringTable,
        Statement::AnalyzeTable { .. } => ProcessState::Analyzing,
        Statement::CheckTable { .. } => ProcessState::CheckingTable,
        Statement::Rollback { .. } | Statement::RollbackToSavepoint(_) => ProcessState::RollingBack,
        _ => ProcessState::Executing,
    }
}

fn process_state_for_sql(sql: &str) -> crate::engine::session_registry::ProcessState {
    use crate::engine::session_registry::ProcessState;
    let mut words = sql.trim_start().split_ascii_whitespace();
    match words.next() {
        Some(keyword)
            if keyword.eq_ignore_ascii_case("INSERT")
                || keyword.eq_ignore_ascii_case("REPLACE")
                || keyword.eq_ignore_ascii_case("LOAD") =>
        {
            ProcessState::Update
        }
        Some(keyword)
            if keyword.eq_ignore_ascii_case("UPDATE") || keyword.eq_ignore_ascii_case("DELETE") =>
        {
            ProcessState::Updating
        }
        Some(keyword) if keyword.eq_ignore_ascii_case("ALTER") => ProcessState::AlteringTable,
        Some(keyword) if keyword.eq_ignore_ascii_case("ANALYZE") => ProcessState::Analyzing,
        Some(keyword) if keyword.eq_ignore_ascii_case("CHECK") => ProcessState::CheckingTable,
        Some(keyword) if keyword.eq_ignore_ascii_case("ROLLBACK") => ProcessState::RollingBack,
        Some(keyword) if keyword.eq_ignore_ascii_case("CREATE") => match words.next() {
            Some(object) if object.eq_ignore_ascii_case("TABLE") => ProcessState::CreatingTable,
            Some(object)
                if object.eq_ignore_ascii_case("INDEX")
                    || object.eq_ignore_ascii_case("UNIQUE") =>
            {
                ProcessState::CreatingIndex
            }
            _ => ProcessState::Preparing,
        },
        _ => ProcessState::Preparing,
    }
}

fn parse_error_to_query_error(err: crate::parser::ParseError) -> QueryError {
    if err.spec == crate::error::catalog::ER_WRONG_ARGUMENTS
        || err.spec == crate::error::catalog::ER_INVALID_GROUP_FUNC_USE
        || err.spec == crate::error::catalog::ER_PK_INDEX_CANT_BE_INVISIBLE
        || err.spec == crate::error::catalog::ER_STACK_OVERRUN_NEED_MORE
        || err.spec == crate::error::catalog::ER_TOO_HIGH_LEVEL_OF_NESTING_FOR_SELECT
    {
        // This semantic validation and these resource bounds are detected
        // while parsing, but have canonical standalone messages rather than
        // syntax location/expectation details.
        err.spec.error(err.message)
    } else {
        err.spec
            .error(format!("{}; expected {:?}", err, err.expected))
    }
}

fn is_lock_wait_pending(err: &QueryError) -> bool {
    crate::storage::is_lock_wait_pending_error(err)
}

fn is_transaction_rollback_error(err: &QueryError) -> bool {
    err.is(crate::error::catalog::ER_LOCK_DEADLOCK)
}

fn clear_session_lock_wait(db: &Arc<Database>, session: &SessionContext) {
    if let Some(txn_id) = session.current_txn {
        let _ = db.clear_txn_wait(txn_id);
    }
    db.clear_mdl_waits_for_owner(session.connection_id as u64);
}

fn slow_sql_log_line(
    session: &SessionContext,
    elapsed_ms: f64,
    config: &QueryLogConfig,
    metadata: QueryLogMetadata,
    sql_for_log: &str,
) -> String {
    let dml_detail = if metadata.dml_path.is_empty() {
        String::new()
    } else {
        format!(
            " dml_path={} rows_input={} rows_batched={} conflict_rows={} legacy_rows={} race_rollbacks={}",
            metadata.dml_path,
            metadata.dml_rows_input,
            metadata.dml_rows_batched,
            metadata.dml_conflict_rows,
            metadata.dml_legacy_rows,
            metadata.dml_race_rollbacks,
        )
    };
    format!(
        "[slow-sql] conn={} user={:?} host={:?} db={:?} elapsed_ms={elapsed_ms:.3} threshold_ms={} lock_wait_count={} lock_wait_ms={:.3}{dml_detail} sql={sql_for_log:?}",
        session.connection_id,
        session.user,
        session.host,
        session.current_database,
        config.slow_sql_threshold_ms(),
        metadata.lock_wait_count,
        metadata.lock_wait_ms,
    )
}

fn wait_for_lock_signal_or_cancel(
    db: &Arc<Database>,
    session: &SessionContext,
    started: Instant,
    stats: &mut LockWaitStats,
) -> Result<(), QueryError> {
    session.check_cancelled()?;
    let metadata_wait_state = db
        .metadata_lock_snapshot()
        .into_iter()
        .find(|row| {
            row.lock_status == "PENDING"
                && row.owner_thread_id == Some(session.connection_id as u64)
        })
        .map(|row| match row.object_type {
            "SCHEMA" => crate::engine::session_registry::ProcessState::WaitingSchemaMetadataLock,
            "EVENT" => crate::engine::session_registry::ProcessState::WaitingEventMetadataLock,
            "FUNCTION" => {
                crate::engine::session_registry::ProcessState::WaitingFunctionMetadataLock
            }
            "PROCEDURE" => {
                crate::engine::session_registry::ProcessState::WaitingProcedureMetadataLock
            }
            "TRIGGER" => crate::engine::session_registry::ProcessState::WaitingTriggerMetadataLock,
            "GLOBAL" | "COMMIT" => {
                crate::engine::session_registry::ProcessState::WaitingGlobalMetadataLock
            }
            _ => crate::engine::session_registry::ProcessState::WaitingTableMetadataLock,
        });
    // Row-lock waits deliberately keep the base DML state (`updating`), as
    // MySQL 8.0 does. Only an actual pending MDL ticket overlays it.
    let _metadata_wait = metadata_wait_state.and_then(crate::engine::cancel::enter_process_stage);
    let wait_started = Instant::now();
    if metadata_wait_state.is_some() {
        let timeout = session
            .sysvars
            .lock_wait_timeout
            .or_else(crate::engine::api::configured_global_lock_wait_timeout);
        // Override the row-lock TLS setting only around this metadata wait.
        let _mdl_timeout = crate::storage::LockWaitTimeoutGuard::new(timeout);
        db.wait_for_lock_signal(started)?;
    } else {
        db.wait_for_lock_signal_for_txn(session.current_txn, started)?;
    }
    let elapsed = wait_started.elapsed();
    stats.record(elapsed);
    crate::engine::perf_events::record_lock_wait_sample(elapsed);
    session.check_cancelled()
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod parameterized_update_gate_tests {
    use super::*;

    #[test]
    fn default_parameterization_keeps_update_modifiers_outside_the_shared_gate() {
        let plain = crate::parser::parse_statement("UPDATE t SET n=n+1 WHERE id=1").unwrap();
        assert!(plain_parameterizable_update(&plain).is_some());
        for sql in [
            "UPDATE t SET n=n+1",
            "UPDATE IGNORE t SET n=n+1 WHERE id=1",
            "UPDATE LOW_PRIORITY t SET n=n+1 WHERE id=1",
            "UPDATE t AS a SET n=n+1 WHERE id=1",
            "UPDATE t SET n=n+1 WHERE id=1 ORDER BY id",
            "UPDATE t SET n=n+1 WHERE id=1 LIMIT 1",
            "SELECT 1",
        ] {
            let statement = crate::parser::parse_statement(sql).unwrap();
            assert!(plain_parameterizable_update(&statement).is_none(), "{sql}");
        }
    }
}
