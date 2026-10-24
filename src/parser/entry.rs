//! Public parser entry points and the canonical one-statement pipeline.

use super::*;

thread_local! {
    static LARGE_LITERAL_DIGEST_SQL: std::cell::RefCell<Option<String>> = const {
        std::cell::RefCell::new(None)
    };
}

thread_local! {
    static PARSER_MEMORY_DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static PARSER_EXPANSION_CHARGE: std::cell::RefCell<Option<crate::engine::executor::mem_budget::ControlledMemoryCharge>> = const { std::cell::RefCell::new(None) };
    static PARSER_FRAGMENT_DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    #[cfg(test)]
    static TEST_PARSE_STATEMENT_CALLS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn parse_statement_calls_for_test() -> u64 {
    TEST_PARSE_STATEMENT_CALLS.with(std::cell::Cell::get)
}

pub(super) struct ParserFragmentGuard;

impl ParserFragmentGuard {
    pub(super) fn activate() -> Self {
        PARSER_FRAGMENT_DEPTH.with(|depth| depth.set(depth.get().saturating_add(1)));
        Self
    }
}

impl Drop for ParserFragmentGuard {
    fn drop(&mut self) {
        PARSER_FRAGMENT_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }
}

pub(super) fn parser_fragment_mode() -> bool {
    PARSER_FRAGMENT_DEPTH.with(|depth| depth.get() != 0)
}

/// Admission envelope for parser-owned temporary allocations. The next-power-
/// of-two basis models allocator capacity rather than logical input length;
/// three buffers cover tokenizer/rewriter/AST construction peaks measured on
/// large statements. Nested fragment parses share the outer envelope.
pub(super) struct ParserMemoryGuard {
    previous_depth: usize,
    syntax_prepaid: bool,
    _charge: Option<crate::engine::executor::mem_budget::ControlledMemoryCharge>,
}

fn parser_syntax_bytes(units: usize) -> usize {
    let per_unit = std::mem::size_of::<SqlSelect>()
        + std::mem::size_of::<SqlExpr>()
        + 2 * std::mem::size_of::<Expr>()
        + 4 * std::mem::size_of::<sqlparser::tokenizer::TokenWithSpan>();
    units.saturating_mul(per_unit)
}

/// Only ASCII identifier runs are coalesced. Numeric spellings, quoted
/// contents, punctuation and UTF-8 bytes are over-counted, giving an upper
/// bound without constructing token spans for short inputs.
fn code_syntax_units(sql: &str) -> usize {
    let bytes = sql.as_bytes();
    let mut offset = 0;
    let mut units = 0;
    while offset < bytes.len() {
        let byte = bytes[offset];
        offset += 1;
        if byte.is_ascii_whitespace() {
            continue;
        }
        units += 1;
        if byte.is_ascii_alphabetic() || matches!(byte, b'_' | b'$') {
            while offset < bytes.len()
                && (bytes[offset].is_ascii_alphanumeric() || matches!(bytes[offset], b'_' | b'$'))
            {
                offset += 1;
            }
        }
    }
    units
}

fn short_syntax_units(sql: &str) -> usize {
    if memchr::memchr3(b'\'', b'"', b'`', sql.as_bytes()).is_none() {
        return code_syntax_units(sql);
    }
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let mut units = 0;
    for segment in crate::sql_lex::segments(sql, mode, MYSQL_VERSION_ID) {
        // PreparedSql immediately reports the same lexical error, before any
        // AST is allocated; the input envelope covers that error path.
        let Ok(segment) = segment else {
            return 0;
        };
        units += match segment.kind {
            crate::sql_lex::SqlSegmentKind::Code => code_syntax_units(&sql[segment.range]),
            crate::sql_lex::SqlSegmentKind::Quoted { .. } => 1,
            crate::sql_lex::SqlSegmentKind::OptimizerHint
            | crate::sql_lex::SqlSegmentKind::ExecutableComment { active: true, .. } => {
                segment.range.len()
            }
            _ => 0,
        };
    }
    units
}

impl ParserMemoryGuard {
    /// Reserve token and AST capacity independently of literal payload bytes.
    /// Lexical units, including embedded operators in permissive number
    /// tokens, bound syntax nodes. Geometric vector growth and simultaneous
    /// upstream/lowered trees use their actual Rust layouts. Payload stays in the
    /// input-buffer envelope instead of being charged as millions of nodes.
    pub(super) fn reserve_syntax(
        &mut self,
        sql: &str,
        mode: crate::sql_lex::SqlLexMode,
    ) -> Result<(), ParseError> {
        let Some(charge) = self._charge.as_mut() else {
            return Ok(());
        };
        if self.syntax_prepaid {
            return Ok(());
        }
        let mut units = 0usize;
        let mut query_units = 1usize;
        let mut parentheses = 0usize;
        let mut next_cancel_check = 1024usize;
        let mut cursor = crate::sql_lex::SqlTokenCursor::new(sql, mode, MYSQL_VERSION_ID);
        while let Some(token) = cursor.next().map_err(|error| parse_lex_error(sql, error))? {
            units = units.saturating_add(1);
            // Full SELECT containers require query/tail keywords. A plain
            // parenthesized row/expression needs only a possible Query box;
            // its Expr/token storage is already in the scalar envelope.
            if token.kind == crate::sql_lex::SqlTokenKind::Symbol(b'(') {
                parentheses = parentheses.saturating_add(1);
            }
            if token.kind == crate::sql_lex::SqlTokenKind::Word
                && [
                    "SELECT",
                    "WITH",
                    "VALUES",
                    "TABLE",
                    "UNION",
                    "INTERSECT",
                    "EXCEPT",
                    "ORDER",
                    "LIMIT",
                    "OFFSET",
                ]
                .iter()
                .any(|word| cursor.token_text(&token).eq_ignore_ascii_case(word))
            {
                query_units = query_units.saturating_add(1);
            }
            // The structural cursor deliberately groups a permissive numeric
            // spelling. Charge embedded signs/dots as potential AST operators;
            // a long integer/hex payload itself is just one scalar node.
            if token.kind == crate::sql_lex::SqlTokenKind::Number {
                let number = cursor.token_text(&token).as_bytes();
                let operators = if number.len() <= 32 {
                    number
                        .iter()
                        .filter(|byte| matches!(byte, b'+' | b'-' | b'.'))
                        .count()
                } else {
                    memchr::memchr3_iter(b'+', b'-', b'.', number).count()
                };
                units = units.saturating_add(2 * operators);
            }
            if units > 1_048_576 {
                return Err(parse_error_at_zero(
                    "SQL syntax exceeds the parser node budget",
                ));
            }
            if units >= next_cancel_check {
                next_cancel_check = units.saturating_add(1024);
                crate::engine::cancel::check_cancelled().map_err(|error| {
                    parse_error_with_spec_at_zero(
                        error
                            .spec()
                            .unwrap_or(crate::error::catalog::ER_QUERY_INTERRUPTED),
                        error.message(),
                    )
                })?;
            }
        }
        let scalar_bytes = std::mem::size_of::<SqlExpr>()
            + 2 * std::mem::size_of::<Expr>()
            + 4 * std::mem::size_of::<sqlparser::tokenizer::TokenWithSpan>();
        let query_bytes =
            2 * (std::mem::size_of::<SqlSelect>() + std::mem::size_of::<sqlparser::ast::Query>());
        charge
            .try_grow(
                units
                    .saturating_mul(scalar_bytes)
                    .saturating_add(query_units.saturating_mul(query_bytes))
                    .saturating_add(
                        parentheses
                            .saturating_mul(2 * std::mem::size_of::<sqlparser::ast::Query>()),
                    ),
            )
            .map_err(|error| {
                parse_error_with_spec_at_zero(
                    crate::error::catalog::ER_OUTOFMEMORY,
                    error.message(),
                )
            })
    }

    fn try_literal_bulk(&mut self, sql: &str) -> Result<Option<Statement>, ParseError> {
        if self.syntax_prepaid || self._charge.is_none() {
            return Ok(None);
        }
        let Some(layout) = literal_bulk_layout(sql) else {
            return Ok(None);
        };
        crate::engine::cancel::check_cancelled().map_err(|error| {
            parse_error_with_spec_at_zero(
                error
                    .spec()
                    .unwrap_or(crate::error::catalog::ER_QUERY_INTERRUPTED),
                error.message(),
            )
        })?;
        // Literal rows never build an upstream AST/token tape. Five Expr
        // slots per value cover first-row Vec growth and boxed unary minus.
        // Row descriptors cover span capacity, worker results and output.
        // Nested function/vector decoders are excluded by the preflight.
        let row_bytes = 2 * std::mem::size_of::<(usize, usize)>()
            + 2 * std::mem::size_of::<Option<Vec<Expr>>>()
            + std::mem::size_of::<Vec<Expr>>();
        let bytes = parser_syntax_bytes(layout.header_units)
            .saturating_add(
                layout
                    .values
                    .saturating_mul(5 * std::mem::size_of::<Expr>()),
            )
            .saturating_add(layout.rows.saturating_mul(row_bytes))
            .saturating_add(sql.len());
        let charge = self._charge.as_mut().unwrap();
        charge.try_grow(bytes).map_err(|error| {
            parse_error_with_spec_at_zero(crate::error::catalog::ER_OUTOFMEMORY, error.message())
        })?;
        let parsed = try_parse_bulk_insert(&sql[layout.source_start..]);
        if parsed.is_none() {
            // Scoped workers and all partially decoded rows have been dropped
            // before generic admission replaces this literal-only envelope.
            charge.shrink(bytes);
        }
        Ok(parsed)
    }

    pub(super) fn enter(sql: &str) -> Result<Self, ParseError> {
        Self::enter_with_syntax(sql, true)
    }

    fn enter_with_syntax(sql: &str, prepay_short: bool) -> Result<Self, ParseError> {
        let previous_depth = PARSER_MEMORY_DEPTH.with(|depth| {
            let previous = depth.get();
            depth.set(previous.saturating_add(1));
            previous
        });
        if previous_depth != 0 {
            return Ok(Self {
                previous_depth,
                syntax_prepaid: false,
                _charge: None,
            });
        }
        let capacity = sql
            .len()
            .max(1)
            .checked_next_power_of_two()
            .unwrap_or(usize::MAX)
            .saturating_mul(3)
            .max(4 * 1024);
        // For short input, prepay the conservative syntax bound with the
        // input envelope: one global admission instead of two. Counting raw
        // comments/quotes can only overestimate the effective syntax.
        let syntax_prepaid = prepay_short && sql.len() <= 256;
        let capacity = if syntax_prepaid {
            let mut words = sql.split_ascii_whitespace();
            let fixed_sequence = matches!(sql.as_bytes().first(), Some(b'C' | b'c' | b'A' | b'a'))
                && words.next().is_some_and(|word| {
                    word.eq_ignore_ascii_case("CREATE") || word.eq_ignore_ascii_case("ALTER")
                })
                && words
                    .next()
                    .is_some_and(|word| word.eq_ignore_ascii_case("SEQUENCE"))
                && !sql.as_bytes().contains(&b';');
            // The short typed sequence grammar stores only fixed numeric
            // options and names; its heap fits the 4 KiB input envelope.
            // Any future expression-valued option must leave this category.
            let units = if fixed_sequence {
                0
            } else {
                short_syntax_units(sql)
            };
            capacity.saturating_add(parser_syntax_bytes(units))
        } else {
            capacity
        };
        match crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::ParserTransient,
            capacity,
        ) {
            Ok(charge) => Ok(Self {
                previous_depth,
                syntax_prepaid,
                _charge: Some(charge),
            }),
            Err(error) => {
                PARSER_MEMORY_DEPTH.with(|depth| depth.set(previous_depth));
                Err(parse_error_with_spec_at_zero(
                    crate::error::catalog::ER_OUTOFMEMORY,
                    error.message(),
                ))
            }
        }
    }
}

/// Charge output amplification that cannot be bounded by input token count
/// (large projection labels and grouping-set products). Nested parses share
/// the outer lease, released on every success/error/unwind path.
pub(super) fn reserve_parser_expansion(bytes: usize) -> Result<(), ParseError> {
    if bytes == 0 || PARSER_MEMORY_DEPTH.with(|depth| depth.get() == 0) {
        return Ok(());
    }
    PARSER_EXPANSION_CHARGE
        .with(|slot| {
            let mut slot = slot.borrow_mut();
            if let Some(charge) = slot.as_mut() {
                charge.try_grow(bytes)
            } else {
                crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                    crate::engine::executor::mem_budget::ControlledMemoryCategory::ParserTransient,
                    bytes,
                )
                .map(|charge| {
                    *slot = Some(charge);
                })
            }
        })
        .map_err(|error| {
            parse_error_with_spec_at_zero(crate::error::catalog::ER_OUTOFMEMORY, error.message())
        })
}

impl Drop for ParserMemoryGuard {
    fn drop(&mut self) {
        if self.previous_depth == 0 {
            PARSER_EXPANSION_CHARGE.with(|slot| {
                slot.borrow_mut().take();
            });
        }
        PARSER_MEMORY_DEPTH.with(|depth| depth.set(self.previous_depth));
    }
}

/// Take the compact SQL skeleton produced by the most recent successful
/// large-literal fast parse on this thread. The executor uses it for
/// performance-schema digesting, avoiding a second scan over the payload.
pub(crate) fn take_large_literal_digest_sql() -> Option<String> {
    LARGE_LITERAL_DIGEST_SQL.with(|slot| slot.borrow_mut().take())
}

/// One statement parsed from a multi-statement SQL batch.
///
/// `effective_range` indexes [`ParsedStatementBatch::effective_sql`], not the
/// original input. This matters for executable comments: their delimiters are
/// masked before statement splitting, so each child remains valid standalone
/// SQL even when a semicolon occurred inside `/*! ... */`.
pub(crate) struct ParsedStatementPart {
    pub(crate) effective_range: std::ops::Range<usize>,
    pub(crate) statement: Statement,
}

/// Routine-aware batch parse result that owns executable-comment expansion
/// once for the whole request. Ordinary SQL remains borrowed, so enabling
/// multi-statements does not duplicate the COM_QUERY body per child.
pub(crate) struct ParsedStatementBatch<'a> {
    pub(crate) effective_sql: std::borrow::Cow<'a, str>,
    pub(crate) parts: Vec<ParsedStatementPart>,
}

/// Split protocol statements without parsing later children ahead of execution.
/// A syntax error in a later statement must not discard successful prefix results.
/// Keep routine bodies intact and expand executable comments before taking spans.
pub(crate) fn split_query_statements(
    sql: &str,
    mode_bits: u16,
) -> (std::borrow::Cow<'_, str>, Vec<std::ops::Range<usize>>) {
    let _mode = ParserSqlModeGuard::activate(mode_bits);
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(mode_bits);
    let effective = crate::sql_lex::PreparedSql::new(sql, mode, MYSQL_VERSION_ID)
        .map(|sql| sql.into_effective())
        .unwrap_or_else(|err| {
            // A malformed final quote/comment belongs to that child, not the
            // already valid prefix (which may contain executable comments).
            let prefix =
                crate::sql_lex::PreparedSql::new(&sql[..err.offset], mode, MYSQL_VERSION_ID)
                    .expect("lexically valid prefix")
                    .into_effective();
            std::borrow::Cow::Owned(format!("{}{}", prefix, &sql[err.offset..]))
        });
    let base = effective.as_ptr() as usize;
    let ranges = split_routine_statements(&effective)
        .into_iter()
        .filter(|part| {
            !crate::sql_lex::effective_is_empty(part, mode, MYSQL_VERSION_ID).unwrap_or(false)
        })
        .map(|part| {
            let start = part.as_ptr() as usize - base;
            start..start + part.len()
        })
        .collect();
    (effective, ranges)
}

#[derive(Clone, Copy)]
pub(super) enum SmallControl {
    Transaction,
    Session,
    ExplainConnection,
}

/// These short typed grammars allocate no expression/upstream AST. Their
/// names and fixed option lists fit the existing 4 KiB input envelope. A
/// non-match still receives full syntax admission before generic parsing.
pub(super) fn small_control_route(sql: &str) -> Option<SmallControl> {
    if sql.len() > 256 {
        return None;
    }
    let sql = sql.trim_start_matches(|ch: char| ch.is_ascii_whitespace());
    match sql.as_bytes().first().copied()?.to_ascii_uppercase() {
        b'B' if starts_with_keyword(sql, "BEGIN") => Some(SmallControl::Transaction),
        b'C' if starts_with_keyword(sql, "COMMIT") => Some(SmallControl::Transaction),
        b'R' if starts_with_keyword(sql, "ROLLBACK") || starts_with_keyword(sql, "RELEASE") => {
            Some(SmallControl::Transaction)
        }
        b'S' if starts_with_keyword(sql, "START") || starts_with_keyword(sql, "SAVEPOINT") => {
            Some(SmallControl::Transaction)
        }
        b'E' if starts_with_keyword(sql, "EXPLAIN")
            && contains_keyword_ci(sql.as_bytes(), b"CONNECTION") =>
        {
            Some(SmallControl::ExplainConnection)
        }
        b'S' if starts_with_keyword(sql, "SET") => {
            let mut rest = sql[3..].trim_start();
            let mut global = false;
            for scope in ["SESSION", "LOCAL", "GLOBAL"] {
                if starts_with_keyword(rest, scope) {
                    global = scope == "GLOBAL";
                    rest = rest[scope.len()..].trim_start();
                    break;
                }
            }
            if ["TRANSACTION", "CHARACTER", "CHARSET"]
                .iter()
                .any(|word| starts_with_keyword(rest, word))
            {
                return Some(SmallControl::Session);
            }
            if global {
                return None;
            }
            if let Some(tail) = rest.strip_prefix("@@") {
                rest = tail.trim_start();
            }
            for scope in ["SESSION", "LOCAL"] {
                if starts_with_keyword(rest, scope) {
                    rest = rest[scope.len()..]
                        .trim_start()
                        .strip_prefix('.')?
                        .trim_start();
                    break;
                }
            }
            if !starts_with_keyword(rest, "AUTOCOMMIT") {
                return None;
            }
            let value = rest[10..].trim_start().strip_prefix('=')?.trim();
            let value = value.strip_suffix(';').unwrap_or(value).trim();
            ["0", "1", "ON", "OFF", "TRUE", "FALSE"]
                .iter()
                .any(|word| value.eq_ignore_ascii_case(word))
                .then_some(SmallControl::Session)
        }
        _ => None,
    }
}

/// Parse exactly one SQL statement.
pub fn parse_statement(sql: &str) -> Result<Statement, ParseError> {
    #[cfg(test)]
    TEST_PARSE_STATEMENT_CALLS.with(|calls| calls.set(calls.get().saturating_add(1)));
    let control = small_control_route(sql);
    let mut memory = ParserMemoryGuard::enter_with_syntax(sql, control.is_none())?;
    LARGE_LITERAL_DIGEST_SQL.with(|slot| *slot.borrow_mut() = None);
    let lex_mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let prepared = crate::sql_lex::PreparedSql::new(sql, lex_mode, MYSQL_VERSION_ID)
        .map_err(|err| parse_lex_error(sql, err))?;
    if let Some(control) = control {
        let parsed = match control {
            SmallControl::Transaction => parse_transaction_statement(prepared.effective()),
            SmallControl::Session => parse_set_session_statement(prepared.effective()),
            SmallControl::ExplainConnection => parse_explain_connection(prepared.effective()),
        }
        .map_err(|error| SourceMap::slice(sql, 0..sql.len()).error(sql, error))?;
        if let Some(statement) = parsed {
            return Ok(statement);
        }
    }
    if let Some(statement) = memory.try_literal_bulk(prepared.effective())? {
        return Ok(statement);
    }
    memory.reserve_syntax(prepared.effective(), lex_mode)?;
    parse_effective_statement(prepared.effective(), lex_mode)
        .map_err(|error| SourceMap::slice(sql, 0..sql.len()).error(sql, error))
}

/// Parse a routine-aware multi-statement batch. Every non-empty span is sent
/// through the same one-statement pipeline as [`parse_statement`].
pub fn parse_statements(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let batch = parse_statement_batch(sql)?;
    Ok(batch
        .parts
        .into_iter()
        .map(|part| {
            debug_assert!(batch.effective_sql.get(part.effective_range).is_some());
            part.statement
        })
        .collect())
}

/// Parse a routine-aware batch while retaining the effective SQL span for
/// every child. The executor uses those spans to feed each statement through
/// the normal single-statement tail (DDL audit, plan cache, lock retry) without
/// copying the SQL body once per statement.
pub(crate) fn parse_statement_batch(sql: &str) -> Result<ParsedStatementBatch<'_>, ParseError> {
    let mut memory = ParserMemoryGuard::enter(sql)?;
    let lex_mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let prepared = crate::sql_lex::PreparedSql::new(sql, lex_mode, MYSQL_VERSION_ID)
        .map_err(|err| parse_lex_error(sql, err))?;
    let effective = prepared.effective();
    memory.reserve_syntax(effective, lex_mode)?;
    let effective_start = effective.as_ptr() as usize;
    let mut parts = Vec::new();
    for part in split_routine_statements(effective) {
        let empty = crate::sql_lex::effective_is_empty(part, lex_mode, MYSQL_VERSION_ID)
            .map_err(|err| parse_lex_error(part, err))?;
        if !empty {
            let start = (part.as_ptr() as usize)
                .checked_sub(effective_start)
                .expect("statement part must borrow from effective SQL");
            let end = start + part.len();
            debug_assert!(end <= effective.len());
            parts.push(ParsedStatementPart {
                effective_range: start..end,
                statement: parse_effective_statement(part, lex_mode)
                    .map_err(|err| SourceMap::slice(sql, start..end).error(sql, err))?,
            });
        }
    }
    Ok(ParsedStatementBatch {
        effective_sql: prepared.into_effective(),
        parts,
    })
}

/// Parse one statement whose executable comments have already been expanded.
pub(super) fn parse_effective_statement(
    effective: &str,
    lex_mode: crate::sql_lex::SqlLexMode,
) -> Result<Statement, ParseError> {
    let sql = sql_after_leading_trivia(effective, lex_mode)?;
    let offset = effective.len() - sql.len();
    parse_statement_source(sql, lex_mode)
        .map_err(|err| SourceMap::slice(effective, offset..effective.len()).error(effective, err))
}

fn parse_statement_source(
    sql: &str,
    lex_mode: crate::sql_lex::SqlLexMode,
) -> Result<Statement, ParseError> {
    // PreparedSql preserves byte offsets and line layout, so downstream spans
    // continue to identify the effective source token exactly.
    let _raw_sql_guard = ParserRawSqlGuard::activate(sql);
    // Share the canonical lexical head across all grammar-island gates.
    // Each gate retains its original position relative to validation.
    let head = crate::sql_lex::first_keyword(sql, lex_mode, MYSQL_VERSION_ID)
        .map_err(|err| parse_lex_error(sql, err))?
        .map(|span| &sql.as_bytes()[span]);
    let head_is = |keyword: &[u8]| {
        head.is_some_and(|head| head.len() == keyword.len() && head.eq_ignore_ascii_case(keyword))
    };
    if head_is(b"SET") {
        let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
        let mut cursor = crate::sql_lex::SqlTokenCursor::new(sql, mode, MYSQL_VERSION_ID);
        if cursor
            .eat_keyword(b"SET")
            .map_err(|err| parse_lex_error(sql, err))?
            && cursor
                .eat_keyword(b"PASSWORD")
                .map_err(|err| parse_lex_error(sql, err))?
        {
            // A malformed auth statement must not fall through to generic SET
            // and acquire unrelated session-variable semantics.
            return parse_manual_auth_statement(sql).ok_or_else(|| {
                parse_error_at_zero("invalid SET PASSWORD statement; PASSWORD() is not supported")
            });
        }
    }
    if head_is(b"SET") {
        if let Some(statement) = parse_role_statement(sql)? {
            return Ok(statement);
        }
    }
    if head_is(b"BEGIN")
        || head_is(b"COMMIT")
        || head_is(b"ROLLBACK")
        || head_is(b"START")
        || head_is(b"SAVEPOINT")
        || head_is(b"RELEASE")
    {
        if let Some(statement) = parse_transaction_statement(sql)? {
            return Ok(statement);
        }
    }
    if head_is(b"CREATE") || head_is(b"ALTER") {
        if let Some(statement) = parse_view_statement(sql)? {
            return Ok(statement);
        }
    }
    if head_is(b"CREATE") || head_is(b"ALTER") {
        validate_enum_set_member_lengths(sql)?;
    }

    if head_is(b"SELECT") || head_is(b"WITH") || head_is(b"TABLE") {
        if let Some(statement) = parse_select_into_statement(sql)? {
            return Ok(statement);
        }
    }

    // Route grammar islands by the first token. Their internal parsers remain
    // defensive, but ordinary SELECT/DML no longer pay for unrelated probes.
    if head_is(b"INSERT") || head_is(b"REPLACE") {
        if let Some(statement) = try_parse_bulk_insert(sql) {
            return Ok(statement);
        }
    }
    // Native sequence grammar already owns quote decoding and SQL mode.
    // Avoid the sqlparser-only quoted-token normalization for this island.
    if head_is(b"CREATE") {
        if let Some(statement) = super::grammar::try_parse_create_sequence(sql)? {
            return Ok(statement);
        }
    } else if head_is(b"ALTER") {
        if let Some(statement) = super::grammar::try_parse_alter_sequence(sql)? {
            return Ok(statement);
        }
    } else if head_is(b"SHOW") {
        if let Some(statement) = super::grammar::try_parse_show_sequence(sql)? {
            return Ok(statement);
        }
    }

    // The bulk grammar already decodes strings with MySQL rules and bails
    // out on dialect-specific escapes. Keep normalization off that hot path.
    let original = sql;
    let (normalized_literals, literal_map) = normalize_mysql_quoted_tokens(sql)?;
    let result = (|| {
        let sql = normalized_literals.as_ref();
        let _normalized_source = ParserRawSqlGuard::activate(sql);
        if head_is(b"DO") {
            if let Some(statement) = try_parse_do_statement(sql)? {
                return Ok(statement);
            }
        }
        if head_is(b"HANDLER") {
            if let Some(statement) = recognise_unsupported_dml_head(sql) {
                return Ok(statement);
            }
        }
        if head_is(b"CREATE") || head_is(b"ALTER") {
            if let Some(statement) = try_parse_create_database(sql)? {
                return Ok(statement);
            }
        }

        if head_is(b"TABLE") {
            let (table_rewritten, source_map) = edit_source(
                sql,
                &[TextEdit {
                    range: 0.."TABLE".len(),
                    replacement: "SELECT * FROM",
                }],
            )?;
            return parse_statement_via_sqlparser(&table_rewritten)
                .map_err(|err| source_map.error(sql, err));
        }
        parse_statement_via_sqlparser(sql)
    })();
    result.map_err(|error| literal_map.error(original, error))
}

/// The spacing restriction applies only in expression/function-call grammar.
fn native_function_requires_adjacency(name: &str) -> bool {
    const NATIVE_NAMES: &[&[u8]] = &[
        b"ADDDATE",
        b"BIT_AND",
        b"BIT_OR",
        b"BIT_XOR",
        b"CAST",
        b"COUNT",
        b"CURDATE",
        b"CURTIME",
        b"DATE_ADD",
        b"DATE_SUB",
        b"EXTRACT",
        b"GROUP_CONCAT",
        b"MAX",
        b"MID",
        b"MIN",
        b"NOW",
        b"POSITION",
        b"SESSION_USER",
        b"STD",
        b"STDDEV",
        b"STDDEV_POP",
        b"STDDEV_SAMP",
        b"SUBDATE",
        b"SUBSTR",
        b"SUBSTRING",
        b"SUM",
        b"SYSDATE",
        b"SYSTEM_USER",
        b"TRIM",
        b"VARIANCE",
        b"VAR_POP",
        b"VAR_SAMP",
    ];

    NATIVE_NAMES
        .iter()
        .any(|native| name.as_bytes().eq_ignore_ascii_case(native))
}

/// Return the effective statement beginning at its first executable token.
fn sql_after_leading_trivia(
    sql: &str,
    mode: crate::sql_lex::SqlLexMode,
) -> Result<&str, ParseError> {
    let start = crate::sql_lex::skip_trivia(sql, 0, mode, MYSQL_VERSION_ID, false)
        .map_err(|err| parse_lex_error(sql, err))?;
    Ok(&sql[start..])
}

// Parser context, dialect adapter and canonical sqlparser pipeline.
/// mydb's SQL dialect: MySQL 8.0 with a small set of column-option
/// extensions that sqlparser 0.61's `MySqlDialect` does not accept.
///
/// The only extension today is the column-level `CHARSET <name>` synonym
/// for `CHARACTER SET <name>`. MySQL accepts both spellings at column and
/// table level; sqlparser only wires the single-keyword `CHARSET` at table
/// level, so `CREATE TABLE t (b VARCHAR(255) CHARSET latin1)` fails with a
/// 1064 while the `CHARACTER SET` form parses. We hook
/// [`Dialect::parse_column_option`] — the exact position sqlparser calls
/// when scanning the options that follow a column's type — rather than
/// rewriting SQL text, so a column literally named `charset` (a
/// non-reserved keyword, legal as an identifier) is never touched; only the
/// option slot after a type is.
///
/// The wrapper delegates every other decision to an inner [`MySqlDialect`]
/// and, crucially, reports MySQL's `TypeId` from [`Dialect::dialect`] so
/// sqlparser's internal `dialect_of!(self is MySqlDialect)` checks (which
/// gate AUTO_INCREMENT, LOCK TABLES, `!`-as-`NOT`, etc.) keep firing.
///
/// When bumping the sqlparser version, re-check `MySqlDialect`'s `impl
/// Dialect` and mirror any newly overridden method here — a missing
/// delegation silently reverts that behaviour to the trait default.
#[derive(Debug, Default, Clone)]
pub(crate) struct MydbDialect {
    inner: MySqlDialect,
    column_option_active: std::cell::Cell<bool>,
}

thread_local! {
    /// Per-thread parser view of the session's sql_mode bits. The parser
    /// runs synchronously on the thread that called `parse_statement`
    /// (either a tokio worker for the inline lock-safe fast path, or a
    /// `spawn_blocking` worker for everything else), so a thread-local is
    /// the right scope: the sqlparser `Dialect` trait only sees `&self`
    /// and cannot carry per-parse state on its own.
    ///
    /// Bits are the shared `SQL_MODE_*` word from
    /// `crate::engine::api` (u16). Only two bits are consumed here:
    /// `SQL_MODE_ANSI_QUOTES` (double-quoted identifier delimiter) and
    /// `SQL_MODE_NO_BACKSLASH_ESCAPES` (backslash is literal, not an
    /// escape prefix). Default `0` = MySQL default posture (backslash
    /// escapes enabled, `"..."` is a string literal).
    static PARSER_SQL_MODE_BITS: std::cell::Cell<u16> = const { std::cell::Cell::new(0) };
    static PARSER_SQL_MODE_OVERRIDE: std::cell::Cell<Option<u16>> = const { std::cell::Cell::new(None) };
}

/// RAII guard that installs a per-thread parser sql_mode view for the
/// lifetime of one parse. Restores the previous value on drop so nested
/// parses (view definition expansion, prepared-statement re-parse) don't
/// leak mode bits to their caller.
pub struct ParserSqlModeGuard {
    previous: u16,
}

impl ParserSqlModeGuard {
    /// Activate the guard with the given sql_mode bits. Pass the u16
    /// produced by `crate::engine::api::parse_temporal_sql_mode_bits` —
    /// callers that only have the u64 `sql_mode_str_to_bits` output can
    /// re-derive it, but the u16 form is cheaper because it already
    /// combines the parser-relevant bits with the temporal/numeric ones
    /// on the session.
    pub fn activate(bits: u16) -> Self {
        let previous = PARSER_SQL_MODE_BITS.with(|c| c.replace(bits));
        Self { previous }
    }

    /// Activate the session's mode unless a protocol prepared statement has
    /// installed its PREPARE-time parser snapshot on this worker.
    pub(crate) fn activate_effective_session(bits: u16) -> Self {
        let effective = PARSER_SQL_MODE_OVERRIDE
            .with(|slot| slot.get())
            .unwrap_or(bits);
        Self::activate(effective)
    }
}

impl Drop for ParserSqlModeGuard {
    fn drop(&mut self) {
        PARSER_SQL_MODE_BITS.with(|c| c.set(self.previous));
    }
}

pub(crate) fn current_parser_sql_mode_bits() -> u16 {
    PARSER_SQL_MODE_BITS.with(|c| c.get())
}

pub(crate) struct ParserSqlModeOverrideGuard {
    previous: Option<u16>,
}

impl ParserSqlModeOverrideGuard {
    pub(crate) fn activate(bits: u16) -> Self {
        let previous = PARSER_SQL_MODE_OVERRIDE.with(|slot| slot.replace(Some(bits)));
        Self { previous }
    }
}

impl Drop for ParserSqlModeOverrideGuard {
    fn drop(&mut self) {
        PARSER_SQL_MODE_OVERRIDE.with(|slot| slot.set(self.previous));
    }
}

#[inline]
fn parser_ansi_quotes() -> bool {
    current_parser_sql_mode_bits() & crate::engine::api::SQL_MODE_ANSI_QUOTES != 0
}

#[inline]
fn parser_no_backslash_escapes() -> bool {
    current_parser_sql_mode_bits() & crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES != 0
}

#[inline]
fn parser_pipes_as_concat() -> bool {
    current_parser_sql_mode_bits() & crate::engine::api::SQL_MODE_PIPES_AS_CONCAT != 0
}

#[inline]
fn parser_high_not_precedence() -> bool {
    current_parser_sql_mode_bits() & crate::engine::api::SQL_MODE_HIGH_NOT_PRECEDENCE != 0
}

// Binding powers follow MySQL 8.0's sql_yacc.yy declarations. Keep gaps so
// mode-dependent operators can occupy their documented position without
// perturbing neighbouring levels.
const PREC_ASSIGNMENT: u8 = 5;
const PREC_OR: u8 = 10;
const PREC_XOR: u8 = 20;
const PREC_AND: u8 = 30;
const PREC_LOW_NOT: u8 = 35;
const PREC_BETWEEN: u8 = 40;
const PREC_COMPARISON: u8 = 50;
const PREC_PREDICATE: u8 = 51;
const PREC_BIT_OR: u8 = 60;
const PREC_BIT_AND: u8 = 70;
const PREC_SHIFT: u8 = 80;
const PREC_PLUS_MINUS: u8 = 90;
const PREC_MUL_DIV_MOD: u8 = 100;
const PREC_BIT_XOR: u8 = 110;
const PREC_CONCAT: u8 = 120;
const PREC_PREFIX: u8 = 130;
const PREC_COLLATE: u8 = 140;
const PREC_PERIOD: u8 = 160;

impl Dialect for MydbDialect {
    fn dialect(&self) -> std::any::TypeId {
        // Preserve MySQL identity so `dialect_of!(self is MySqlDialect)` holds.
        self.inner.dialect()
    }

    fn parse_prefix(
        &self,
        parser: &mut SqlParser,
    ) -> Option<Result<sqlparser::ast::Expr, ParserError>> {
        // Upstream protects query recursion but not all expression-prefix
        // frames. Grow before entering another large frame, including when
        // parsing on a small worker stack or under sanitizer instrumentation.
        if stacker::remaining_stack().is_some_and(|bytes| bytes < 512 * 1024) {
            return Some(super::budget::grow_sqlparser_prefix(parser));
        }
        if current_parser_sql_mode_bits() & crate::engine::api::SQL_MODE_IGNORE_SPACE == 0 {
            let head = parser.peek_token_ref();
            if let Token::Word(word) = &head.token {
                let next = parser.peek_nth_token_ref(1);
                if word.quote_style.is_none()
                    && next.token == Token::LParen
                    && head.span.end != next.span.start
                    && native_function_requires_adjacency(&word.value)
                    && !(0..=parser.get_current_index())
                        .rev()
                        .map(|index| &parser.token_at(index).token)
                        .find(|token| !matches!(token, Token::Whitespace(_)))
                        .is_some_and(|token| *token == Token::Period)
                {
                    return Some(Err(ParserError::ParserError(format!(
                        "whitespace before '(' after native function {} requires IGNORE_SPACE at Line: {}, Column: {}",
                        word.value, head.span.start.line, head.span.start.column,
                    ))));
                }
            }
        }
        let native = super::adapter::parse_high_prefix(parser, PREC_PREFIX)
            .or_else(|| super::adapter::parse_cast_prefix(parser, PREC_COLLATE))
            .or_else(|| super::adapter::parse_json_value_prefix(parser))
            .or_else(|| super::adapter::parse_scalar_clause_prefix(parser))
            .or_else(|| super::adapter::parse_interval_prefix(parser))
            .or_else(|| super::adapter::parse_trim_prefix(parser))
            .or_else(|| self.inner.parse_prefix(parser));
        native.map(|result| {
            result.and_then(|expr| {
                // Returning from a dialect prefix bypasses sqlparser's COLLATE
                // suffix. Mirror that suffix, including its column-option scope.
                if !self.column_option_active.get() && parser.parse_keyword(Keyword::COLLATE) {
                    Ok(SqlExpr::Collate {
                        expr: Box::new(expr),
                        collation: parser.parse_object_name(false)?,
                    })
                } else {
                    Ok(expr)
                }
            })
        })
    }

    fn supports_table_versioning(&self) -> bool {
        true
    }

    fn supports_group_by_with_modifier(&self) -> bool {
        true
    }

    fn parse_column_option(
        &self,
        parser: &mut SqlParser,
    ) -> Result<Option<Result<Option<ColumnOption>, ParserError>>, ParserError> {
        if self.column_option_active.get() {
            return Ok(None);
        }
        if let Some(suffix) = super::adapter::parse_column_type_suffix(parser) {
            return Ok(Some(suffix.map(Some)));
        }
        if parser.parse_keyword(Keyword::CONSTRAINT) {
            let name = parser.parse_identifier()?;
            return match parser.parse_optional_column_option()? {
                Some(ColumnOption::Check(mut constraint)) => {
                    constraint.name = Some(name);
                    Ok(Some(Ok(Some(ColumnOption::Check(constraint)))))
                }
                _ => Err(ParserError::ParserError(
                    "CONSTRAINT requires a CHECK column option".into(),
                )),
            };
        }
        // MySQL accepts a bare `CHARSET x` in a column definition as a
        // synonym for `CHARACTER SET x`; sqlparser only wires the
        // single-keyword form at table level. Emit the same
        // `ColumnOption::CharacterSet` the `CHARACTER SET` form produces so
        // downstream schema handling is byte-for-byte identical.
        if parser.parse_keyword(Keyword::CHARSET) {
            return Ok(Some(
                parser
                    .parse_object_name(false)
                    .map(|name| Some(ColumnOption::CharacterSet(name))),
            ));
        }
        // `VARCHAR(20) BINARY` / `CHAR(20) BINARY` requests the binary
        // collation of the effective character set. Keep an internal marker
        // until table charset defaults are available in the DDL layer.
        if parser.parse_keyword(Keyword::BINARY) {
            return Ok(Some(Ok(Some(ColumnOption::Collation(
                sqlparser::ast::Ident::new(BINARY_ATTRIBUTE_COLLATION).into(),
            )))));
        }
        // Within a column definition MySQL accepts bare `KEY` as the short
        // form of `PRIMARY KEY` (`id INT KEY AUTO_INCREMENT`). Table-level
        // `KEY name (...)` is parsed through the constraint path and does not
        // reach this hook.
        if parser.parse_keyword(Keyword::KEY) {
            return Ok(Some(Ok(Some(ColumnOption::PrimaryKey(
                sqlparser::ast::PrimaryKeyConstraint {
                    name: None,
                    index_name: None,
                    index_type: None,
                    columns: Vec::new(),
                    index_options: Vec::new(),
                    characteristics: None,
                },
            )))));
        }
        if let Some(option) = self.inner.parse_column_option(parser)? {
            return Ok(Some(option));
        }
        // The recursive callback returns None above, entering the upstream
        // column-option parser exactly once. The scope remains parser-local.
        let _scope = super::adapter::ColumnOptionScope::enter(&self.column_option_active);
        Ok(Some(parser.parse_optional_column_option()))
    }

    fn is_column_alias(&self, kw: &Keyword, parser: &mut SqlParser) -> bool {
        // MySQL treats `ON` as a reserved word, so it can never be an unquoted
        // column alias. sqlparser's default disagrees: `ON` is in
        // `RESERVED_FOR_TABLE_ALIAS` but not `RESERVED_FOR_COLUMN_ALIAS`, so a
        // FROM-less SELECT projection binds it as an alias — `SELECT 5 ON
        // DUPLICATE KEY UPDATE ...` parses as `SELECT 5 AS `on`` and swallows
        // the `ON` that introduces the `INSERT ... ON DUPLICATE KEY UPDATE`
        // tail, leaving `parse_on_insert` nothing to match and failing with
        // "found: DUPLICATE". (With a FROM clause the table-alias path already
        // stops at `ON`, which is why only the FROM-less source regressed.)
        // Excluding `ON` here aligns with MySQL and fixes the ODKU parse; the
        // default `is_select_item_alias` routes through this method too.
        if matches!(kw, Keyword::ON | Keyword::FOR) {
            return false;
        }
        // EXCLUDE is non-reserved in MySQL; the window-frame parser consumes
        // it before the projection-alias hook when it introduces a clause.
        if matches!(kw, Keyword::EXCLUDE) {
            return true;
        }
        self.inner.is_column_alias(kw, parser)
    }

    // ---- Pure delegation to the inner MySqlDialect below. ----
    fn is_identifier_start(&self, ch: char) -> bool {
        self.inner.is_identifier_start(ch)
    }
    fn is_identifier_part(&self, ch: char) -> bool {
        self.inner.is_identifier_part(ch)
    }
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        // ANSI_QUOTES turns `"..."` into an identifier delimiter (as in
        // ANSI SQL), on top of MySQL's usual backtick delimiter. Without
        // this bit `"..."` remains a string literal — the default MySQL
        // behaviour that the inner dialect reports.
        if parser_ansi_quotes() && ch == '"' {
            return true;
        }
        self.inner.is_delimited_identifier_start(ch)
    }
    fn identifier_quote_style(&self, identifier: &str) -> Option<char> {
        self.inner.identifier_quote_style(identifier)
    }
    fn supports_string_literal_backslash_escape(&self) -> bool {
        // NO_BACKSLASH_ESCAPES: `\'` inside a string literal is a
        // two-character sequence (backslash + apostrophe), not an
        // escaped quote. Reporting `false` here makes sqlparser's
        // tokenizer stop consuming `\` as an escape prefix and emit the
        // raw bytes verbatim, matching MySQL 8.0.
        if parser_no_backslash_escapes() {
            return false;
        }
        self.inner.supports_string_literal_backslash_escape()
    }
    fn supports_string_literal_concatenation(&self) -> bool {
        self.inner.supports_string_literal_concatenation()
    }
    fn ignores_wildcard_escapes(&self) -> bool {
        self.inner.ignores_wildcard_escapes()
    }
    fn supports_numeric_prefix(&self) -> bool {
        self.inner.supports_numeric_prefix()
    }
    fn supports_bitwise_shift_operators(&self) -> bool {
        self.inner.supports_bitwise_shift_operators()
    }
    fn supports_multiline_comment_hints(&self) -> bool {
        self.inner.supports_multiline_comment_hints()
    }
    fn parse_infix(
        &self,
        parser: &mut SqlParser,
        expr: &SqlExpr,
        precedence: u8,
    ) -> Option<Result<SqlExpr, ParserError>> {
        if let Some(result) = super::adapter::parse_logical_chain(parser, expr, precedence) {
            return Some(result);
        }
        if let Some(result) =
            super::adapter::parse_pattern_infix(parser, expr, precedence, PREC_PREFIX)
        {
            return Some(result);
        }
        if let Some(result) = super::adapter::parse_sounds_like(parser, expr, precedence) {
            return Some(result);
        }
        // MySQL's grammar is asymmetric here:
        //   bit_expr [NOT] BETWEEN bit_expr AND predicate
        // The high bound is a recursively parsed predicate, so a second
        // BETWEEN associates to the right. sqlparser's generic helper parses
        // both bounds at the BETWEEN precedence and therefore associates a
        // chain to the left.
        let negated_between = matches!(parser.peek_token_ref().token,
            Token::Word(ref word) if word.keyword == Keyword::NOT)
            && matches!(parser.peek_nth_token_ref(1).token,
                Token::Word(ref word) if word.keyword == Keyword::BETWEEN);
        let plain_between = matches!(parser.peek_token_ref().token,
            Token::Word(ref word) if word.keyword == Keyword::BETWEEN);
        if plain_between || negated_between {
            if negated_between {
                let _ = parser.parse_keyword(Keyword::NOT);
            }
            let _ = parser.parse_keyword(Keyword::BETWEEN);
            let result = (|| {
                let low = parser.parse_subexpr(PREC_BETWEEN)?;
                parser.expect_keyword_is(Keyword::AND)?;
                // One below BETWEEN admits another predicate at the same
                // level. Comparison leakage is rotated back out by the
                // centralized conversion normalizer.
                let high = parser.parse_subexpr(PREC_BETWEEN - 1)?;
                Ok(SqlExpr::Between {
                    expr: Box::new(expr.clone()),
                    negated: negated_between,
                    low: Box::new(low),
                    high: Box::new(high),
                })
            })();
            return Some(result);
        }
        // DIV and MOD share multiplicative precedence and left associativity.
        let op = if parser.parse_keyword(Keyword::DIV) {
            Some(SqlBinaryOperator::MyIntegerDivide)
        } else if parser.parse_keyword(Keyword::MOD) {
            Some(SqlBinaryOperator::Modulo)
        } else if matches!(
            parser.peek_token_ref().token,
            Token::CustomBinaryOperator(_)
        ) {
            let Token::CustomBinaryOperator(operator) = parser.next_token().token else {
                unreachable!()
            };
            Some(SqlBinaryOperator::Custom(operator))
        } else {
            None
        };
        if let Some(op) = op {
            return Some(
                parser
                    .parse_subexpr(precedence)
                    .map(|right| SqlExpr::BinaryOp {
                        left: Box::new(expr.clone()),
                        op,
                        right: Box::new(right),
                    }),
            );
        }
        self.inner.parse_infix(parser, expr, precedence)
    }
    fn get_next_precedence(&self, parser: &SqlParser) -> Option<Result<u8, ParserError>> {
        if super::adapter::is_sounds_like(parser) {
            return Some(Ok(PREC_PREDICATE));
        }
        let token = &parser.peek_token_ref().token;
        let precedence = match token {
            Token::CustomBinaryOperator(_) => PREC_CONCAT,
            Token::Assignment => PREC_ASSIGNMENT,
            Token::Word(ref word) if word.keyword == Keyword::OR => PREC_OR,
            Token::StringConcat => {
                if parser_pipes_as_concat() {
                    PREC_CONCAT
                } else {
                    PREC_OR
                }
            }
            Token::Word(ref word) if word.keyword == Keyword::XOR => PREC_XOR,
            Token::Word(ref word) if word.keyword == Keyword::AND => PREC_AND,
            Token::Overlap => PREC_AND,
            Token::Word(ref word) if word.keyword == Keyword::NOT => {
                match parser.peek_nth_token_ref(1).token {
                    Token::Word(ref next) if next.keyword == Keyword::IN => PREC_PREDICATE,
                    Token::Word(ref next) if next.keyword == Keyword::BETWEEN => PREC_BETWEEN,
                    Token::Word(ref next)
                        if matches!(
                            next.keyword,
                            Keyword::LIKE | Keyword::RLIKE | Keyword::REGEXP
                        ) =>
                    {
                        PREC_PREDICATE
                    }
                    _ => return None,
                }
            }
            Token::Word(ref word) if word.keyword == Keyword::BETWEEN => PREC_BETWEEN,
            Token::Word(ref word) if word.keyword == Keyword::IN => PREC_PREDICATE,
            Token::Word(ref word)
                if matches!(
                    word.keyword,
                    Keyword::LIKE | Keyword::RLIKE | Keyword::REGEXP
                ) =>
            {
                PREC_PREDICATE
            }
            Token::Word(ref word) if matches!(word.keyword, Keyword::IS | Keyword::MEMBER) => {
                PREC_COMPARISON
            }
            Token::Eq
            | Token::DoubleEq
            | Token::Neq
            | Token::Lt
            | Token::LtEq
            | Token::Gt
            | Token::GtEq
            | Token::Spaceship => PREC_COMPARISON,
            Token::Pipe => PREC_BIT_OR,
            Token::Ampersand => PREC_BIT_AND,
            Token::ShiftLeft | Token::ShiftRight => PREC_SHIFT,
            Token::Plus | Token::Minus => PREC_PLUS_MINUS,
            Token::Mul | Token::Div | Token::Mod => PREC_MUL_DIV_MOD,
            Token::Word(ref word) if matches!(word.keyword, Keyword::DIV | Keyword::MOD) => {
                PREC_MUL_DIV_MOD
            }
            Token::Caret => PREC_BIT_XOR,
            Token::Period => PREC_PERIOD,
            _ => return None,
        };
        Some(Ok(precedence))
    }
    fn prec_value(&self, prec: Precedence) -> u8 {
        match prec {
            Precedence::Period => PREC_PERIOD,
            Precedence::DoubleColon => PREC_COLLATE,
            Precedence::AtTz => PREC_COLLATE,
            Precedence::MulDivModOp => PREC_MUL_DIV_MOD,
            Precedence::PlusMinus => PREC_PLUS_MINUS,
            Precedence::Xor | Precedence::Caret => PREC_BIT_XOR,
            Precedence::Ampersand => PREC_BIT_AND,
            Precedence::Pipe | Precedence::Colon => PREC_BIT_OR,
            Precedence::Between => PREC_PREDICATE,
            Precedence::Eq | Precedence::Like | Precedence::Is => PREC_COMPARISON,
            Precedence::PgOther => PREC_COLLATE,
            Precedence::UnaryNot => {
                if parser_high_not_precedence() {
                    PREC_PREFIX
                } else {
                    PREC_LOW_NOT
                }
            }
            Precedence::And => PREC_AND,
            Precedence::Or => PREC_OR,
        }
    }
    fn parse_statement(&self, parser: &mut SqlParser) -> Option<Result<SqlStatement, ParserError>> {
        self.inner.parse_statement(parser)
    }
    fn require_interval_qualifier(&self) -> bool {
        self.inner.require_interval_qualifier()
    }
    fn supports_limit_comma(&self) -> bool {
        self.inner.supports_limit_comma()
    }
    fn supports_create_table_select(&self) -> bool {
        self.inner.supports_create_table_select()
    }
    fn supports_insert_set(&self) -> bool {
        self.inner.supports_insert_set()
    }
    fn supports_user_host_grantee(&self) -> bool {
        self.inner.supports_user_host_grantee()
    }
    fn is_table_factor_alias(&self, explicit: bool, kw: &Keyword, parser: &mut SqlParser) -> bool {
        self.inner.is_table_factor_alias(explicit, kw, parser)
    }
    fn supports_table_hints(&self) -> bool {
        self.inner.supports_table_hints()
    }
    fn requires_single_line_comment_whitespace(&self) -> bool {
        self.inner.requires_single_line_comment_whitespace()
    }
    fn supports_match_against(&self) -> bool {
        self.inner.supports_match_against()
    }
    fn supports_select_modifiers(&self) -> bool {
        self.inner.supports_select_modifiers()
    }
    fn supports_set_names(&self) -> bool {
        self.inner.supports_set_names()
    }
    fn supports_comma_separated_set_assignments(&self) -> bool {
        self.inner.supports_comma_separated_set_assignments()
    }
    fn supports_data_type_signed_suffix(&self) -> bool {
        self.inner.supports_data_type_signed_suffix()
    }
    fn supports_cross_join_constraint(&self) -> bool {
        self.inner.supports_cross_join_constraint()
    }
    fn supports_double_ampersand_operator(&self) -> bool {
        self.inner.supports_double_ampersand_operator()
    }
    fn supports_bang_not_operator(&self) -> bool {
        true
    }
    fn supports_binary_kw_as_cast(&self) -> bool {
        self.inner.supports_binary_kw_as_cast()
    }
    fn supports_comment_optimizer_hint(&self) -> bool {
        self.inner.supports_comment_optimizer_hint()
    }
    fn supports_constraint_keyword_without_name(&self) -> bool {
        self.inner.supports_constraint_keyword_without_name()
    }
}

fn recognise_unsupported_dml_head(sql: &str) -> Option<Statement> {
    let trimmed = sql.trim_start();
    if starts_with_keyword(trimmed, "HANDLER") {
        let rest = trimmed["HANDLER".len()..].trim_start();
        let mut tokens = rest.split_whitespace();
        let _table = tokens.next();
        if let Some(verb) = tokens.next() {
            if verb.eq_ignore_ascii_case("OPEN")
                || verb.eq_ignore_ascii_case("READ")
                || verb.eq_ignore_ascii_case("CLOSE")
            {
                return Some(Statement::Unsupported {
                    kind: "HANDLER".to_string(),
                    sql: sql.to_string(),
                });
            }
        }
    }
    None
}

pub(super) fn parse_statement_via_sqlparser(sql: &str) -> Result<Statement, ParseError> {
    if let Some(statement) = try_parse_plain_query_or_delete(sql)? {
        return Ok(statement);
    }
    // Large text-protocol UPDATEs commonly carry the new BLOB/TEXT value as
    // a string literal in the COM_QUERY body.  The compatibility pipeline
    // below intentionally handles a very broad grammar, but historically it
    // also ran dozens of unrelated keyword probes / rewrites over that
    // literal before sqlparser saw it.  A 1 MiB `UPDATE ... SET b='...'`
    // therefore paid many full-input scans while the equivalent prepared
    // statement parsed only once at PREPARE time.
    //
    // Route the conservative, compatibility-rewrite-free UPDATE subset
    // straight to sqlparser.  Eligibility is decided by one quote/comment-
    // aware token walk and is based solely on grammar capabilities (never on
    // table/column names or benchmark shape).  Any extension token, join,
    // subquery, modifier, ORDER/LIMIT/RETURNING, or unfamiliar operator falls
    // back to the canonical pipeline, which remains the correctness oracle.
    if let Some(statement) = try_parse_plain_update_fast(sql)? {
        return Ok(statement);
    }
    if let Some(statement) = try_parse_plain_modified_dml(sql) {
        return Ok(statement);
    }
    parse_statement_via_sqlparser_legacy(sql)
}

/// Conservative grammar subset with no compatibility rewrites. The gate
/// depends only on syntax: no parenthesized constructs, legacy assignments,
/// joins, locking shorthand, numeric aliases, or DELETE modifiers/targets.
/// Both routes use the same dialect, parameter identity and AST validation.
pub(super) fn try_parse_plain_query_or_delete(sql: &str) -> Result<Option<Statement>, ParseError> {
    use crate::sql_lex::{SqlTokenCursor, SqlTokenKind};
    let (head, len) = extract_first_keyword(sql);
    let delete = &head[..len] == b"DELETE";
    if !delete && &head[..len] != b"SELECT" {
        return Ok(None);
    }
    if sql.as_bytes().contains(&b'(')
        || sql.contains(":=")
        || (delete && (sql.as_bytes().contains(&b'*') || sql.as_bytes().contains(&b',')))
    {
        return Ok(None);
    }
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let mut cursor = SqlTokenCursor::new(sql, mode, MYSQL_VERSION_ID);
    let mut previous: Option<crate::sql_lex::SqlToken> = None;
    while let Some(token) = cursor.next().map_err(|error| parse_lex_error(sql, error))? {
        if delete
            && token.kind == SqlTokenKind::Word
            && cursor.token_text(&token).eq_ignore_ascii_case("OFFSET")
        {
            return Ok(None);
        }
        if token.kind == SqlTokenKind::Word
            && [
                "JOIN",
                "STRAIGHT_JOIN",
                "LOCK",
                "SYSTEM_TIME",
                "GROUPING",
                "LOW_PRIORITY",
                "HIGH_PRIORITY",
                "QUICK",
                "IGNORE",
                "RETURNING",
            ]
            .iter()
            .any(|word| cursor.token_text(&token).eq_ignore_ascii_case(word))
        {
            return Ok(None);
        }
        if token.kind == SqlTokenKind::Number {
            if let Some(previous) = previous.as_ref() {
                let numeric_alias = match previous.kind {
                    SqlTokenKind::Number => !cursor.token_text(previous).ends_with(['+', '-']),
                    SqlTokenKind::String | SqlTokenKind::QuotedIdentifier => true,
                    SqlTokenKind::Word => ![
                        "SELECT", "DISTINCT", "ALL", "LIMIT", "OFFSET", "AND", "OR", "XOR", "NOT",
                        "WHERE", "BY", "WHEN", "THEN", "ELSE", "HAVING", "BETWEEN", "DIV", "MOD",
                    ]
                    .iter()
                    .any(|word| cursor.token_text(previous).eq_ignore_ascii_case(word)),
                    _ => false,
                };
                if numeric_alias {
                    return Ok(None);
                }
            }
        }
        previous = Some(token);
    }
    let dialect = MydbDialect::default();
    let mut statements = adapter::parse_sql(&dialect, sql)
        .map_err(|error| ParseError::from_sqlparser(sql, error))?;
    if statements.len() != 1 {
        return Ok(None);
    }
    let _source = ParserRawSqlGuard::activate(sql);
    convert_statement(sql, statements.pop().unwrap()).map(Some)
}

/// Canonical compatibility parser.  Kept as a separate function so parser
/// regression tests can compare the conservative UPDATE fast path with the
/// full rewrite pipeline.
pub(super) fn parse_statement_via_sqlparser_legacy(sql: &str) -> Result<Statement, ParseError> {
    if !first_keyword_in(sql, &[b"EXPLAIN", b"DESC", b"DESCRIBE"]) {
        return parse_statement_via_sqlparser_normalized(sql);
    }
    let (normalized, source_map) = normalize_explain(sql)?;
    parse_statement_via_sqlparser_normalized(&normalized).map_err(|err| source_map.error(sql, err))
}

fn parse_statement_via_sqlparser_normalized(sql: &str) -> Result<Statement, ParseError> {
    let mut trace = None;
    let original = sql;
    let result = (|| {
        let (sql, ddl_query_hints, ddl_hint_warnings) = strip_ddl_optimizer_hint(sql)?;
        let (head_buf, head_len) = extract_first_keyword(&sql);
        let head_kw = &head_buf[..head_len];
        let head_is = |keyword: &[u8]| head_kw == keyword;
        // The extension parses its own target recursively, so it does not need
        // the outer DML/DDL rewrite pipeline. Native TREE/JSON fall through.
        if head_kw == b"EXPLAIN" {
            if let Some(statement) = parse_explain_extension(&sql)? {
                return Ok(statement);
            }
        }
        validate_cast_type_widths(&sql)?;
        // Route each manual grammar family once. The manual parsers retain their
        // defensive checks, but unrelated statements no longer cascade through
        // every parser and repeat head/trivia scans.
        if head_is(b"SET") {
            if let Some(statement) = parse_manual_set_persist_statement(&sql)? {
                return Ok(statement);
            }
            if let Some(statement) = parse_set_session_statement(&sql)? {
                return Ok(statement);
            }
            if let Some(statement) = parse_manual_set_resource_group(&sql)? {
                return Ok(statement);
            }
        } else if head_is(b"RESET") {
            if let Some(statement) = parse_manual_reset_persist_statement(&sql)? {
                return Ok(statement);
            }
        }
        if matches!(
            head_kw,
            b"CREATE" | b"ALTER" | b"DROP" | b"CALL" | b"SHOW" | b"DELIMITER"
        ) {
            if let Some(statement) = parse_manual_routine_statement(&sql)? {
                return Ok(statement);
            }
        }
        // Typed/manual grammars and rewrite-free fast paths need no rewrite
        // context. Keep provenance local to the compatibility pipeline.
        trace = Some(RewriteTrace::enter());
        let _extensions = extensions::Guard::enter(original);
        let sql = rewrite_sql(sql, rewrite_drop_tables_keyword);
        let sql = try_rewrite_sql(sql, rewrite_returning_with_aliases)?;
        let sql = try_rewrite_sql(sql, rewrite_insert_target_alias)?;
        let canonical_sql = rewrite_sql(sql, rewrite_parenthesized_ctas);
        let sql = canonical_sql.as_ref();
        // The preceding rewrites preserve the statement head.

        if head_is(b"PREPARE") || head_is(b"EXECUTE") || head_is(b"DEALLOCATE") || head_is(b"DROP")
        {
            if let Some(statement) = parse_manual_prepared_statement(sql)? {
                return Ok(statement);
            }
        }

        if head_is(b"LOAD") {
            if let Some(statement) = parse_load_data_statement(sql)? {
                return Ok(statement);
            }
        }
        if head_is(b"LOCK") || head_is(b"UNLOCK") {
            if let Some(statement) = parse_manual_lock_tables_statement(sql) {
                return Ok(statement);
            }
        }
        if head_is(b"XA") {
            if let Some(statement) = parse_xa_statement(sql) {
                return Ok(statement);
            }
        }
        if head_is(b"GET") {
            let RoutineStmt::GetDiagnostics {
                stacked,
                condition,
                items,
            } = parse_routine_get_diagnostics(sql.trim_end().trim_end_matches(';').trim_end())?
            else {
                unreachable!()
            };
            return Ok(Statement::GetDiagnostics {
                stacked,
                condition,
                items,
            });
        }
        if head_is(b"SHOW") {
            if let Some(statement) = parse_manual_show_create_database_statement(sql)? {
                return Ok(statement);
            }
            if let Some(statement) = parse_manual_show_diagnostics_statement(sql)? {
                return Ok(statement);
            }
            if let Some(statement) = parse_manual_show_index_statement(sql)? {
                return Ok(statement);
            }
            if let Some(statement) = parse_manual_metadata_show_statement(sql)? {
                return Ok(statement);
            }
            if let Some(statement) = parse_manual_show_open_tables_statement(sql)? {
                return Ok(statement);
            }
            if let Some(statement) = parse_manual_show_local_scope_statement(sql)? {
                return Ok(statement);
            }
            reject_invalid_show_mydb_memory(sql)?;
        }
        if crate::backup::sql::recognizes(sql) {
            return crate::backup::sql::parse(sql)
                .map(|command| Statement::BackupAdmin { command })
                .map_err(|e| parse_error_full_span(sql, e.message()));
        }
        let admin_head = MYSQL_ADMIN_HEADS.contains(&head_kw);
        if admin_head {
            if let Some(statement) = parse_manual_mysql_admin_statement(sql) {
                return Ok(statement);
            }
        }
        if head_is(b"ALTER") {
            if let Some(statement) = parse_manual_alter_table_check_statement(sql)? {
                return Ok(statement);
            }
        }
        if head_is(b"UPDATE") || head_is(b"WITH") {
            if let Some(statement) = parse_manual_update_order_limit(sql)? {
                return Ok(statement);
            }
        }
        if head_is(b"DELETE") || head_is(b"WITH") {
            detect_delete_limit_offset(sql)?;
        }

        let prescan = prescan_keywords(sql, head_kw);

        let (sql, update_modifiers) = rewrite_sql_with(canonical_sql, strip_update_modifiers);
        let (sql, delete_modifiers) = rewrite_sql_with(sql, strip_delete_modifiers);
        let sql = rewrite_sql(sql, strip_delete_target_wildcards);
        let (sql, create_table_pct_override) = try_rewrite_sql_with(sql, |sql| {
            strip_create_table_pctfree_pctused(sql, prescan.has_pctfree_or_pctused)
        })?;
        let (sql, create_zone_options) =
            try_rewrite_sql_with(sql, |sql| strip_zone_map_options(sql, true))?;
        let sql = rewrite_sql(sql, |sql| {
            strip_index_key_block_size(sql, prescan.has_key_block_size)
        });
        let sql = rewrite_sql(sql, |sql| {
            strip_column_format_storage(sql, prescan.has_column_format_or_storage)
        });
        // Compute the shared inline-index gate only when either feature occurs.
        // Prefix comparisons need no full-SQL uppercase allocation.
        let inline_indexes = (prescan.has_index_visibility || prescan.has_include) && {
            if first_keyword_is(&sql, b"ALTER") {
                true
            } else {
                let rest = sql.trim_start();
                let rest =
                    rest[rest.find(char::is_whitespace).unwrap_or(rest.len())..].trim_start();
                rest.as_bytes()
                    .get(..5)
                    .is_some_and(|word| word.eq_ignore_ascii_case(b"TABLE"))
                    || rest
                        .as_bytes()
                        .get(..9)
                        .is_some_and(|word| word.eq_ignore_ascii_case(b"TEMPORARY"))
            }
        };
        let (sql, inline_index_visibility) = try_rewrite_sql_with(sql, |sql| {
            strip_inline_index_visibility(sql, inline_indexes && prescan.has_index_visibility)
        })?;
        let (sql, inline_index_includes) = try_rewrite_sql_with(sql, |sql| {
            strip_inline_index_includes(sql, inline_indexes && prescan.has_include)
        })?;
        let sql = rewrite_sql(sql, normalize_assignment_operator);
        let sql = rewrite_sql(sql, rewrite_digit_leading_alias);
        let sql = if head_is(b"CREATE") || head_is(b"ALTER") {
            let sql = rewrite_sql(sql, rewrite_enum_set_legacy_literals);
            rewrite_sql(sql, rewrite_double_quoted_comments)
        } else {
            sql
        };
        let sql = rewrite_sql(sql, rewrite_values_row_keyword);
        let sql = normalize_create_table_like_parenthesized(sql, prescan.has_like);
        let (sql, create_index_visible) = rewrite_sql_with(sql, strip_create_index_visibility);
        let sql = rewrite_sql(sql, rewrite_comma_update_targets);
        let sql = rewrite_sql(sql, rewrite_empty_insert_rows);
        let sql = try_rewrite_sql(sql, rewrite_grouping_sets_keyword)?;
        let sql = try_rewrite_sql(sql, rewrite_empty_grouping_set_tuples)?;
        let sql = try_rewrite_sql(sql, rewrite_legacy_text_lengths)?;
        let sql = rewrite_sql(sql, rewrite_insert_partition_columns);
        let (sql, ctas_conflict_policy) = rewrite_sql_with(sql, rewrite_ctas_conflict_policy);
        let sql = try_rewrite_sql(sql, rewrite_alter_drop_key)?;
        let sql = rewrite_sql(sql, rewrite_alter_parenthesized_add);
        let sql = try_rewrite_sql(sql, rewrite_legacy_lock_in_share_mode)?;
        let (sql, window_frame_exclusions) = try_rewrite_sql_with(sql, |sql| {
            strip_window_frame_exclusions(sql, prescan.has_exclude)
        })?;
        let (sql, (partition_for_create, partition_actions_for_alter)) =
            try_rewrite_sql_with(sql, |sql| {
                strip_partition_clauses(sql).map(|(sql, create, alter)| (sql, (create, alter)))
            })?;
        let (sql, fulltext_parsers) =
            try_rewrite_sql_with(sql, |sql| strip_fulltext_parsers(sql, prescan.has_fulltext))?;
        let (sql, create_index_fulltext) = rewrite_sql_with(sql, |sql| {
            strip_create_index_fulltext(sql, prescan.has_fulltext)
        });
        let post_parse = PostParseState {
            ddl_query_hints,
            ddl_hint_warnings,
            window_frame_exclusions,
            create_index_visible,
            create_index_fulltext,
            update_modifiers,
            delete_modifiers,
            partition_for_create,
            partition_actions_for_alter,
            fulltext_parsers,
            create_table_pct_override,
            create_zone_options,
            inline_index_visibility,
            inline_index_includes,
        };
        let sql = try_rewrite_sql(sql, rewrite_mysql_right_nested_join_groups)?;
        let dialect = MydbDialect::default();
        let statements = match adapter::parse_sql(&dialect, &sql) {
            Ok(statements) => statements,
            Err(err) => return Err(ParseError::from_sqlparser(&sql, err)),
        };
        if statements.len() != 1 {
            return Ok(Statement::MultiStatement {
                count: statements.len(),
                sql: sql.to_string(),
            });
        }
        let _rewritten_source = ParserRawSqlGuard::activate(&sql);
        let mut statement = convert_statement(&sql, statements.into_iter().next().unwrap())?;
        // Binding assignment is an extra full-AST traversal. Keep it completely
        // off the ordinary SQL parse path: apart from avoiding needless work,
        // this also preserves the stack headroom of expression-heavy statements.
        if sql
            .as_bytes()
            .windows(b"SYSTEM_TIME".len())
            .any(|window| window.eq_ignore_ascii_case(b"SYSTEM_TIME"))
        {
            visit::assign_system_time_bindings(&mut statement);
        }
        post_parse.apply(&mut statement, &sql)?;
        if let Statement::CreateTableAsSelect {
            conflict_policy, ..
        } = &mut statement
        {
            *conflict_policy = ctas_conflict_policy;
        }
        Ok(statement)
    })();
    result.map_err(|error| match trace.as_ref() {
        Some(trace) => trace.error(original, error),
        None => error,
    })
}
/// Parse a plain single-table UPDATE without entering the compatibility
/// rewrite cascade.  `Ok(None)` means "use the canonical parser".
pub(super) fn try_parse_plain_update_fast(sql: &str) -> Result<Option<Statement>, ParseError> {
    use crate::sql_lex::{SqlTokenCursor, SqlTokenKind};

    struct ExtractedLiteral {
        span: std::ops::Range<usize>,
        digest_span: std::ops::Range<usize>,
        expr: Expr,
    }

    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let mut cursor = SqlTokenCursor::new(sql, mode, MYSQL_VERSION_ID);
    let Some(first) = cursor.next().map_err(|err| parse_lex_error(sql, err))? else {
        return Ok(None);
    };
    if first.kind != SqlTokenKind::Word
        || !cursor
            .token_text(&first)
            .as_bytes()
            .eq_ignore_ascii_case(b"UPDATE")
    {
        return Ok(None);
    }

    let mut depth: i32 = 0;
    let mut seen_set = false;
    let mut saw_assignment = false;
    let mut previous: Option<crate::sql_lex::SqlToken> = Some(first);
    let mut extracted: Vec<ExtractedLiteral> = Vec::new();
    let mut has_placeholder = false;
    while let Some(token) = cursor.next().map_err(|err| parse_lex_error(sql, err))? {
        match token.kind {
            SqlTokenKind::String => {
                // sqlparser necessarily tokenizes and owns string literal
                // payloads.  For a large COM_QUERY value that dominates the
                // request.  Extract it once, parse a tiny `?` skeleton, then
                // late-bind the already decoded literal into the AST.  Small
                // strings stay on the ordinary direct-sqlparser path.
                const LARGE_LITERAL_MIN: usize = 64 * 1024;
                if token.span.len() >= LARGE_LITERAL_MIN {
                    let digest_span = token.span.clone();
                    let decoded = match cursor.decoded_quoted_text(&token) {
                        Some(decoded) => decoded,
                        None => return Ok(None),
                    };
                    let mut span = token.span.clone();
                    let mut expr = Expr::Literal(Literal::String(decoded));
                    if let Some(prefix) = previous.as_ref().filter(|prefix| {
                        prefix.kind == SqlTokenKind::Word
                            && prefix.span.end == token.span.start
                            && cursor.token_text(prefix).starts_with('_')
                    }) {
                        // Keep the first version deliberately exact and
                        // narrow.  `_binary` is emitted by MySQL text-protocol
                        // clients for BLOB parameters and is byte-preserving
                        // for the ASCII payload admitted here.  Other charset
                        // introducers retain the canonical conversion path.
                        if !cursor
                            .token_text(prefix)
                            .as_bytes()
                            .eq_ignore_ascii_case(b"_binary")
                        {
                            return Ok(None);
                        }
                        let Expr::Literal(Literal::String(value)) = &expr else {
                            unreachable!("large quoted literal is a string")
                        };
                        if !value.is_ascii() {
                            return Ok(None);
                        }
                        span.start = prefix.span.start;
                        expr = Expr::Function {
                            name: "CONVERT_USING".to_string(),
                            args: vec![
                                expr,
                                Expr::Literal(Literal::String("binary".to_string())),
                                Expr::Literal(Literal::Bool(true)),
                            ],
                            distinct: false,
                        };
                    }
                    extracted.push(ExtractedLiteral {
                        span,
                        digest_span,
                        expr,
                    });
                }
            }
            SqlTokenKind::QuotedIdentifier | SqlTokenKind::Number => {}
            SqlTokenKind::Placeholder => has_placeholder = true,
            SqlTokenKind::Symbol(b'(') => depth += 1,
            SqlTokenKind::Symbol(b')') => {
                depth -= 1;
                if depth < 0 {
                    return Ok(None);
                }
            }
            // `:=`, `!!`, vendor operators and multi-table target lists need
            // one of the canonical rewrites.  `!=` is deliberately sent to
            // the oracle too; false negatives only cost performance.
            SqlTokenKind::Symbol(b':')
            | SqlTokenKind::Symbol(b'!')
            | SqlTokenKind::Symbol(b'{')
            | SqlTokenKind::Symbol(b'}') => return Ok(None),
            SqlTokenKind::Symbol(b',') if depth == 0 && !seen_set => return Ok(None),
            SqlTokenKind::Symbol(b'=') if seen_set => saw_assignment = true,
            SqlTokenKind::Word => {
                let word = cursor.token_text(&token).as_bytes();
                if depth == 0 && word.eq_ignore_ascii_case(b"SET") {
                    seen_set = true;
                    continue;
                }
                // Every item here either activates a mydb compatibility
                // rewrite/manual grammar or changes the UPDATE into a shape
                // whose post-parse state is not empty.  Keep the list
                // conservative: an ordinary identifier with one of these
                // spellings merely falls back to the legacy parser.
                if [
                    b"LOW_PRIORITY".as_slice(),
                    b"IGNORE",
                    b"ORDER",
                    b"LIMIT",
                    b"RETURNING",
                    b"PARTITION",
                    b"WITH",
                    b"SELECT",
                    b"JOIN",
                    b"STRAIGHT_JOIN",
                    b"LEFT",
                    b"RIGHT",
                    b"INNER",
                    b"CROSS",
                    b"NATURAL",
                    b"USING",
                    b"INTERVAL",
                    b"ROLLUP",
                    b"CUBE",
                    b"EXCLUDE",
                    b"GROUPING",
                    b"REGEXP",
                    b"RLIKE",
                    b"SOUNDS",
                    b"MOD",
                    b"EXTRACT",
                    b"CAST",
                    b"WEIGHT_STRING",
                    b"CHARSET",
                    b"CHARACTER",
                    b"NCHAR",
                    b"NATIONAL",
                    b"ZEROFILL",
                    b"COLUMN_FORMAT",
                    b"STORAGE",
                    b"ARRAY",
                    b"FULLTEXT",
                    b"ZONE_MAP",
                    b"ZONE_SIZE",
                    b"LIKE",
                    b"ESCAPE",
                ]
                .iter()
                .any(|keyword| word.eq_ignore_ascii_case(keyword))
                {
                    return Ok(None);
                }
            }
            SqlTokenKind::Symbol(_) => {}
        }
        previous = Some(token);
    }
    if depth != 0 || !seen_set || !saw_assignment {
        return Ok(None);
    }

    // The same conservative grammar gate also admits ordinary UPDATEs.
    // Use the shared adapter/converter directly, retaining user parameter
    // identities and optimizer hints; no synthetic literal binding is needed.
    if extracted.is_empty() {
        let dialect = MydbDialect::default();
        let mut statements = match adapter::parse_sql(&dialect, sql) {
            Ok(statements) if statements.len() == 1 => statements,
            _ => return Ok(None),
        };
        return match convert_statement(sql, statements.pop().unwrap()) {
            Ok(statement @ Statement::Update(_)) => Ok(Some(statement)),
            _ => Ok(None),
        };
    }

    if has_placeholder {
        // COM_STMT_PREPARE already parses only the small SQL template and
        // gains nothing from literal extraction.  Avoid mixing its parameter
        // indexes with synthetic placeholders.
        return Ok(None);
    }
    let skeleton = {
        let mut out = String::with_capacity(
            sql.len()
                .saturating_sub(extracted.iter().map(|item| item.span.len()).sum::<usize>())
                .saturating_add(extracted.len()),
        );
        let mut copied = 0;
        for item in &extracted {
            if item.span.start < copied || item.span.end > sql.len() {
                return Ok(None);
            }
            out.push_str(&sql[copied..item.span.start]);
            out.push('?');
            copied = item.span.end;
        }
        out.push_str(&sql[copied..]);
        Some(out)
    };
    let digest_skeleton = {
        let mut out = String::with_capacity(
            sql.len()
                .saturating_sub(
                    extracted
                        .iter()
                        .map(|item| item.digest_span.len())
                        .sum::<usize>(),
                )
                .saturating_add(extracted.len()),
        );
        let mut copied = 0;
        for item in &extracted {
            if item.digest_span.start < copied || item.digest_span.end > sql.len() {
                return Ok(None);
            }
            out.push_str(&sql[copied..item.digest_span.start]);
            out.push('?');
            copied = item.digest_span.end;
        }
        out.push_str(&sql[copied..]);
        Some(out)
    };
    let parse_sql = skeleton.as_deref().unwrap_or(sql);
    let dialect = MydbDialect::default();
    let statements = match adapter::parse_sql(&dialect, parse_sql) {
        Ok(statements) if statements.len() == 1 => statements,
        // The eligibility gate is intentionally not a second grammar.  If
        // sqlparser rejects the statement, let the full compatibility path
        // diagnose or rewrite it exactly as before.
        _ => return Ok(None),
    };
    // Literal provenance spans belong to the extracted template, not the
    // large source payload. Restore the outer source before any fallback.
    let _template_source = ParserRawSqlGuard::activate(parse_sql);
    let mut statement = match convert_statement(parse_sql, statements.into_iter().next().unwrap()) {
        Ok(statement) => statement,
        Err(_) if skeleton.is_some() => return Ok(None),
        Err(err) => return Err(err),
    };
    let mut next = 0;
    visit::number_statement_placeholders(&mut statement, &mut next);
    if next != extracted.len() {
        return Ok(None);
    }
    struct LiteralBinder {
        values: Vec<Option<Expr>>,
        complete: bool,
    }
    impl visit::AstVisitorMut for LiteralBinder {
        fn enter_expr(&mut self, expr: &mut Expr) {
            let Expr::Placeholder(index) = expr else {
                return;
            };
            let Some(value) = self.values.get_mut(*index).and_then(Option::take) else {
                self.complete = false;
                return;
            };
            *expr = value;
        }
    }
    let mut binder = LiteralBinder {
        values: extracted.into_iter().map(|item| Some(item.expr)).collect(),
        complete: true,
    };
    visit::visit_statement_mut(&mut binder, &mut statement);
    if !binder.complete || binder.values.iter().any(Option::is_some) {
        return Ok(None);
    }
    if matches!(statement, Statement::Update(_)) {
        if let Some(digest_skeleton) = digest_skeleton {
            LARGE_LITERAL_DIGEST_SQL.with(|slot| *slot.borrow_mut() = Some(digest_skeleton));
        }
        Ok(Some(statement))
    } else {
        Ok(None)
    }
}

/// Parenthesise MySQL's right-nested join form before handing it to
/// sqlparser.
///
/// MySQL accepts
/// `a LEFT JOIN b RIGHT JOIN c ON inner_pred ON outer_pred` and binds it as
/// `a LEFT JOIN (b RIGHT JOIN c ON inner_pred) ON outer_pred`. sqlparser's
/// standard grammar instead builds `(a LEFT JOIN b) RIGHT JOIN c ...` and
/// rejects the second `ON`.  This lexical rewrite preserves the MySQL join
/// tree explicitly.  It is shape based (two same-level ON clauses after a
/// nested join operator), quote/comment aware, and can handle multiple
/// independent groups in one statement.
fn rewrite_mysql_right_nested_join_groups(
    sql: &str,
) -> Result<std::borrow::Cow<'_, str>, ParseError> {
    use crate::sql_lex::{SqlTokenCursor, SqlTokenKind};

    fn word_is(sql: &str, token: &crate::sql_lex::SqlToken, expected: &[u8]) -> bool {
        token.kind == SqlTokenKind::Word
            && sql.as_bytes()[token.span.clone()].eq_ignore_ascii_case(expected)
    }

    fn join_operator_end(
        sql: &str,
        tokens: &[crate::sql_lex::SqlToken],
        index: usize,
    ) -> Option<usize> {
        let token = tokens.get(index)?;
        if word_is(sql, token, b"JOIN") || word_is(sql, token, b"STRAIGHT_JOIN") {
            return Some(index);
        }
        if !(word_is(sql, token, b"LEFT")
            || word_is(sql, token, b"RIGHT")
            || word_is(sql, token, b"INNER")
            || word_is(sql, token, b"CROSS")
            || word_is(sql, token, b"FULL"))
        {
            return None;
        }
        let mut end = index + 1;
        if tokens
            .get(end)
            .is_some_and(|token| word_is(sql, token, b"OUTER"))
        {
            end += 1;
        }
        tokens
            .get(end)
            .is_some_and(|token| word_is(sql, token, b"JOIN"))
            .then_some(end)
    }

    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let mut rewritten = std::borrow::Cow::Borrowed(sql);
    let mut rewrite_bytes = 0usize;
    for _ in 0..128 {
        let current = rewritten.as_ref();
        // The initial linear scan is unavoidable and must not impose a
        // size limit on SQL that needs no join rewrite (e.g. large literals).
        if matches!(&rewritten, std::borrow::Cow::Owned(_)) {
            rewrite_bytes = rewrite_bytes.saturating_add(current.len());
        }
        if rewrite_bytes > 4 * 1024 * 1024 {
            return Err(ParseError::at_zero(
                "join rewrite exceeds the supported parser work budget",
            ));
        }
        let mut cursor = SqlTokenCursor::new(current, mode, MYSQL_VERSION_ID);
        let mut tokens = Vec::new();
        while let Some(token) = cursor
            .next()
            .map_err(|err| ParseError::at_zero(err.to_string()))?
        {
            tokens.push(token);
        }
        let mut depths = Vec::with_capacity(tokens.len());
        let mut depth = 0usize;
        for token in &tokens {
            depths.push(depth);
            match token.kind {
                SqlTokenKind::Symbol(b'(') => depth += 1,
                SqlTokenKind::Symbol(b')') => depth = depth.saturating_sub(1),
                _ => {}
            }
        }

        let mut edit = None;
        for outer_start in (0..tokens.len()).rev() {
            if !(word_is(current, &tokens[outer_start], b"LEFT")
                || word_is(current, &tokens[outer_start], b"RIGHT"))
            {
                continue;
            }
            let Some(outer_end) = join_operator_end(current, &tokens, outer_start) else {
                continue;
            };
            let Some(right_start) = tokens.get(outer_end + 1) else {
                continue;
            };
            let outer_depth = depths[outer_start];
            if depths[outer_end + 1] != outer_depth
                || right_start.kind == SqlTokenKind::Symbol(b'(')
            {
                continue;
            }

            let mut nested_end = None;
            let mut index = outer_end + 2;
            while index < tokens.len() && depths[index] == outer_depth {
                if word_is(current, &tokens[index], b"ON") {
                    break;
                }
                if let Some(end) = join_operator_end(current, &tokens, index) {
                    nested_end = Some(end);
                    break;
                }
                index += 1;
            }
            let Some(nested_end) = nested_end else {
                continue;
            };
            let Some(inner_on) = ((nested_end + 1)..tokens.len())
                .find(|&idx| depths[idx] == outer_depth && word_is(current, &tokens[idx], b"ON"))
            else {
                continue;
            };
            let Some(outer_on) = ((inner_on + 1)..tokens.len())
                .find(|&idx| depths[idx] == outer_depth && word_is(current, &tokens[idx], b"ON"))
            else {
                continue;
            };
            edit = Some((right_start.span.start, tokens[outer_on].span.start));
            break;
        }

        let Some((open, close)) = edit else {
            return Ok(rewritten);
        };
        let mut owned = current.to_string();
        owned.insert(close, ')');
        owned.insert(open, '(');
        rewritten = std::borrow::Cow::Owned(owned);
    }
    Err(ParseError::at_zero(
        "join nesting exceeds the supported parser rewrite depth",
    ))
}

/// Adapt quoted user variables and the two non-MySQL backslash escapes
/// recognized by sqlparser. Leave actual BEL/FF bytes and escaped backslashes
/// untouched. The common path performs no tokenization or allocation.
fn normalize_mysql_quoted_tokens(
    sql: &str,
) -> Result<(std::borrow::Cow<'_, str>, SourceMap), ParseError> {
    use crate::sql_lex::{SqlTokenCursor, SqlTokenKind};
    let escapes = !parser_no_backslash_escapes() && (sql.contains("\\a") || sql.contains("\\f"));
    let quoted_variable = memchr::memchr_iter(b'@', sql.as_bytes()).any(|idx| {
        sql.as_bytes()[idx + 1..]
            .iter()
            .copied()
            .find(|byte| !byte.is_ascii_whitespace())
            .is_some_and(|byte| matches!(byte, b'\'' | b'"' | b'`'))
    });
    if !escapes && !quoted_variable {
        return edit_source(sql, &[]);
    }
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let mut cursor = SqlTokenCursor::new(sql, mode, MYSQL_VERSION_ID);
    let mut edits = Vec::new();
    let mut at = None;
    let mut variable_position = true;
    let set_statement = first_keyword_is(sql, b"SET");
    let mut assignment_target = set_statement;
    let mut depth = 0usize;
    while let Some(token) = cursor.next().map_err(|err| parse_lex_error(sql, err))? {
        if matches!(
            token.kind,
            SqlTokenKind::String | SqlTokenKind::QuotedIdentifier
        ) {
            if let Some(start) = at.take() {
                let name = cursor.decoded_quoted_text(&token).unwrap();
                edits.push((
                    start..token.span.end,
                    if assignment_target {
                        format!("`@{}`", name.replace('`', "``"))
                    } else {
                        format!("@`{}`", name.replace('`', "``"))
                    },
                ));
                variable_position = false;
                continue;
            }
            if escapes && token.kind == SqlTokenKind::String {
                let raw = cursor.token_text(&token);
                let mut output = String::with_capacity(raw.len());
                let mut chars = raw.chars();
                while let Some(ch) = chars.next() {
                    if ch == '\\' {
                        if let Some(next) = chars.next() {
                            if !matches!(next, 'a' | 'f') {
                                output.push(ch);
                            }
                            output.push(next);
                        } else {
                            output.push(ch);
                        }
                    } else {
                        output.push(ch);
                    }
                }
                if output != raw {
                    edits.push((token.span.clone(), output));
                }
            }
        }
        match token.kind {
            SqlTokenKind::Symbol(b'(') => depth += 1,
            SqlTokenKind::Symbol(b')') => depth = depth.saturating_sub(1),
            SqlTokenKind::Symbol(b'=') if depth == 0 && set_statement => assignment_target = false,
            SqlTokenKind::Symbol(b',') if depth == 0 && set_statement => assignment_target = true,
            _ => {}
        }
        at = (variable_position && token.kind == SqlTokenKind::Symbol(b'@'))
            .then_some(token.span.start);
        variable_position = match token.kind {
            SqlTokenKind::Word => [
                "SELECT", "SET", "WHERE", "HAVING", "AND", "OR", "XOR", "NOT", "BY", "THEN",
                "ELSE", "WHEN", "RETURN", "DO", "INTO", "LIKE", "ESCAPE", "AS", "USING", "VALUES",
            ]
            .iter()
            .any(|word| cursor.token_text(&token).eq_ignore_ascii_case(word)),
            SqlTokenKind::Symbol(b')' | b'@')
            | SqlTokenKind::String
            | SqlTokenKind::QuotedIdentifier => false,
            _ => true,
        };
    }
    if edits.is_empty() {
        return edit_source(sql, &[]);
    }
    let edits = edits
        .iter()
        .map(|(range, replacement)| TextEdit {
            range: range.clone(),
            replacement,
        })
        .collect::<Vec<_>>();
    edit_source(sql, &edits)
}

/// sqlparser 0.61 restricts `LIKE ... ESCAPE` to a literal `Value`, while
/// MySQL accepts a general scalar expression (including a placeholder or
/// scalar subquery). Replace only those non-literal operands with private
/// one-character markers, then restore their expressions after AST
/// conversion. The rewrite is lexical and shape-based, never SQL-text
/// specific.
/// MySQL accepts the historical plural spelling `DROP TABLES ...` as an
/// alias for `DROP TABLE ...`. sqlparser deliberately implements only the
/// standard singular token, so normalize just the leading statement keyword
/// pair. Quoted identifiers, comments and string contents are left untouched.
fn rewrite_drop_tables_keyword(sql: &str) -> std::borrow::Cow<'_, str> {
    let bytes = sql.as_bytes();
    let mut pos = 0usize;
    while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
        pos += 1;
    }
    let drop_end = pos + "DROP".len();
    if drop_end > bytes.len()
        || !bytes[pos..drop_end].eq_ignore_ascii_case(b"DROP")
        || bytes
            .get(drop_end)
            .is_none_or(|byte| !byte.is_ascii_whitespace())
    {
        return std::borrow::Cow::Borrowed(sql);
    }
    let mut tables_start = drop_end;
    while tables_start < bytes.len() && bytes[tables_start].is_ascii_whitespace() {
        tables_start += 1;
    }
    let tables_end = tables_start + "TABLES".len();
    if tables_end > bytes.len()
        || !bytes[tables_start..tables_end].eq_ignore_ascii_case(b"TABLES")
        || bytes
            .get(tables_end)
            .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
    {
        return std::borrow::Cow::Borrowed(sql);
    }
    let mut rewritten = String::with_capacity(sql.len() - 1);
    rewritten.push_str(&sql[..tables_start]);
    rewritten.push_str("TABLE");
    rewritten.push_str(&sql[tables_end..]);
    std::borrow::Cow::Owned(rewritten)
}

pub(super) fn starts_with_keyword(sql: &str, keyword: &str) -> bool {
    // Byte-level compare on purpose: `sql` can begin with a multi-byte
    // UTF-8 char (e.g. a string literal tail like `'é' = …`), and a
    // `&str` slice of `keyword.len()` bytes would panic on the char
    // boundary. ASCII keywords never match inside a multi-byte
    // sequence, so byte comparison is exact.
    let bytes = sql.as_bytes();
    let keyword = keyword.as_bytes();
    if bytes.len() < keyword.len() {
        return false;
    }
    if !bytes[..keyword.len()].eq_ignore_ascii_case(keyword) {
        return false;
    }
    match bytes.get(keyword.len()) {
        None => true,
        Some(&b) => !b.is_ascii_alphanumeric() && b != b'_',
    }
}

/// Extract the first SQL keyword after shared mode-aware lexical trivia.
/// The keyword is uppercased into a 16-byte buffer; the returned `len` is the
/// keyword's byte length (truncated to 16).
///
/// This is the workhorse for routing `parse_statement` past its
/// manual pre-parse cascade. The historical cascade was the dominant
/// cost on multi-row sysbench INSERT statements (~25 % of CPU): each
/// `parse_manual_*` would `mysql_tokens(trimmed)` or
/// `split_whitespace().collect::<Vec<_>>().join(" ")` *over the whole
/// 1 MB payload* before realising it wasn't a match. Routing on the
/// head keyword exits in O(1) bytes instead.
pub(crate) fn extract_first_keyword(sql: &str) -> ([u8; 16], usize) {
    let mut buf = [0u8; 16];
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let Ok(Some(range)) = crate::sql_lex::first_keyword(sql, mode, MYSQL_VERSION_ID) else {
        return (buf, 0);
    };
    let len = range.len().min(buf.len());
    for (target, source) in buf[..len]
        .iter_mut()
        .zip(&sql.as_bytes()[range.start..range.start + len])
    {
        *target = source.to_ascii_uppercase();
    }
    (buf, len)
}

/// Returns true if `sql` begins (after whitespace / optional leading
/// block comment) with exactly `keyword` (uppercase ASCII). Use for
/// fast head-keyword gating in pre-parse routing.
pub(crate) fn first_keyword_is(sql: &str, keyword: &[u8]) -> bool {
    let (buf, len) = extract_first_keyword(sql);
    len == keyword.len() && &buf[..len] == keyword
}

/// Returns true if `sql` begins with one of the supplied keywords.
pub(crate) fn first_keyword_in(sql: &str, keywords: &[&[u8]]) -> bool {
    let (buf, len) = extract_first_keyword(sql);
    keywords.iter().any(|k| k.len() == len && &buf[..len] == *k)
}

/// Case-insensitive ASCII substring search. Uses `memchr::memchr2` to
/// find candidate starting positions matching either case of the first
/// byte, then verifies the rest byte-by-byte without allocating a
/// uppercased / lowercased copy of `haystack`.
///
/// Used in the `strip_*`/`rewrite_*` cascade to short-circuit on a 1 MB
/// payload that contains none of the target keyword bytes, avoiding
/// the previous `to_ascii_uppercase()` allocations. Note: this is a
/// substring test — it does **not** enforce identifier boundaries — so
/// it can return `true` on coincidental matches inside other words.
/// That is fine for short-circuit gating: a false positive simply
/// falls through to the existing slow path, which does its own
/// precise keyword handling.
pub(crate) fn contains_keyword_ci(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    if haystack.len() < needle.len() {
        return false;
    }
    let first_lo = needle[0].to_ascii_lowercase();
    let first_hi = needle[0].to_ascii_uppercase();
    let scan_limit = haystack.len() - needle.len() + 1;
    let mut start = 0;
    while start < scan_limit {
        let Some(rel) = memchr::memchr2(first_lo, first_hi, &haystack[start..scan_limit]) else {
            return false;
        };
        let i = start + rel;
        let mut ok = true;
        for j in 1..needle.len() {
            let h = haystack[i + j];
            let n = needle[j];
            if h.to_ascii_lowercase() != n.to_ascii_lowercase() {
                ok = false;
                break;
            }
        }
        if ok {
            return true;
        }
        start = i + 1;
    }
    false
}

/// Pre-scan results: which keywords appear anywhere in the SQL (outside
/// quoted strings is not enforced — false positives just fall through to
/// the slow path). Computed once per `parse_statement` and passed down to
/// strip/rewrite functions to replace ~5 redundant 1 MB scans on every
/// multi-row INSERT.
///
/// Each flag is meaningful only if the head keyword could plausibly
/// admit the keyword. For heads that preclude a keyword we leave the
/// flag at `false` (no scan performed), so the downstream strip function
/// short-circuits without touching the payload.
///
/// **Why this struct exists**: the previous round head-gated each strip
/// function individually, but a *non-head-gated* function like
/// group-by modifier stripping still ran a fresh `contains_keyword_ci`
/// on the full 1 MB payload for every parse, regardless of head. With
/// 4 such functions running back-to-back on the same payload the
/// `memchr` cost added up to ~8 % of total parse time on sysbench.
#[derive(Default, Clone, Copy)]
pub(crate) struct PreScan {
    pub has_fulltext: bool,
    pub has_rollup: bool,
    pub has_cube: bool,
    pub has_exclude: bool,
    pub has_pctfree_or_pctused: bool,
    pub has_like: bool,
    pub has_key_block_size: bool,
    pub has_index_visibility: bool,
    pub has_column_format_or_storage: bool,
    pub has_include: bool,
}

/// Compute pre-scan keyword presence with head-keyword gating. Each
/// keyword is only scanned for if the head keyword could plausibly
/// contain it. For `INSERT … VALUES (…)` (the sysbench bulk path) this
/// degenerates to *zero* full-payload scans.
///
/// Conservative: when in doubt, scan. The strip/rewrite layer is
/// idempotent on false positives — it just falls through.
pub(crate) fn prescan_keywords(sql: &str, head: &[u8]) -> PreScan {
    let mut ps = PreScan::default();

    // Classify head into broad categories. A few keywords like INSERT
    // *could* embed a SELECT (so admit ROLLUP/EXCLUDE) but the vastly
    // most common shape is `INSERT … VALUES (…)`. We refine that below.
    let is_query_head = matches!(
        head,
        b"SELECT"
            | b"WITH"
            | b"VALUES"
            | b"TABLE"
            | b"INSERT"
            | b"REPLACE"
            | b"UPDATE"
            | b"DELETE"
            | b"CALL"
            | b"DO"
            | b"EXPLAIN"
            | b"DESCRIBE"
    );
    let is_ddl_head = matches!(
        head,
        b"CREATE" | b"ALTER" | b"DROP" | b"RENAME" | b"TRUNCATE"
    );

    if !is_query_head && !is_ddl_head {
        return ps;
    }

    // One quote/comment-aware walk replaces the previous independent
    // substring scan per flag. Besides reducing O(k*n) work, keywords inside
    // a large literal no longer trigger unrelated compatibility rewrites.
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let mut cursor = crate::sql_lex::SqlTokenCursor::new(sql, mode, MYSQL_VERSION_ID);
    while let Ok(Some(token)) = cursor.next() {
        if token.kind != crate::sql_lex::SqlTokenKind::Word {
            continue;
        }
        let word = cursor.token_text(&token).as_bytes();
        let is = |keyword: &[u8]| word.eq_ignore_ascii_case(keyword);
        if is_query_head {
            ps.has_rollup |= is(b"ROLLUP");
            ps.has_cube |= is(b"CUBE");
            ps.has_exclude |= is(b"EXCLUDE");
        }
        if is_ddl_head {
            ps.has_fulltext |= is(b"FULLTEXT");
        }
        if matches!(head, b"CREATE") {
            ps.has_pctfree_or_pctused |= is(b"PCTFREE") || is(b"PCTUSED");
            ps.has_like |= is(b"LIKE");
        }
        if matches!(head, b"CREATE" | b"ALTER") {
            ps.has_key_block_size |= is(b"KEY_BLOCK_SIZE");
            ps.has_index_visibility |= is(b"INVISIBLE") || is(b"VISIBLE");
            ps.has_column_format_or_storage |= is(b"COLUMN_FORMAT") || is(b"STORAGE");
            ps.has_include |= is(b"INCLUDE");
        }
    }

    ps
}

fn detect_delete_limit_offset(sql: &str) -> Result<(), ParseError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if find_dml_verb_after_optional_cte(trimmed, "DELETE").is_none() {
        return Ok(());
    }
    let Some(limit_pos) = find_top_level_keyword(trimmed, "LIMIT") else {
        return Ok(());
    };
    let limit_sql = trimmed[limit_pos + "LIMIT".len()..].trim();
    if limit_sql.is_empty() {
        return Ok(());
    }
    if let Ok((_, offset)) = parse_limit_tail(limit_sql) {
        if offset.is_some() {
            return Err(parse_error_full_span(
                sql,
                "DELETE LIMIT offset is not supported",
            ));
        }
    }
    Ok(())
}

pub(super) fn parse_order_by_tail(sql: &str) -> Result<Vec<OrderBy>, ParseError> {
    let dialect = MydbDialect::default();
    let wrapped = format!("SELECT 1 FROM t ORDER BY {sql}");
    let statements = adapter::parse_sql(&dialect, &wrapped)
        .map_err(|err| ParseError::from_sqlparser(sql, err))?;
    let _source = ParserRawSqlGuard::activate(&wrapped);
    let Some(SqlStatement::Query(query)) = statements.into_iter().next() else {
        return Err(parse_error_at_zero("ORDER BY clause"));
    };
    match query.order_by.as_ref().map(|order| &order.kind) {
        Some(OrderByKind::Expressions(items)) => items
            .iter()
            .map(|item| {
                Ok(OrderBy {
                    expr: convert_expr(&item.expr)?,
                    asc: item.options.asc.unwrap_or(true),
                    nulls_first: item.options.nulls_first,
                })
            })
            .collect(),
        _ => Err(parse_error_at_zero("ORDER BY clause")),
    }
}

pub(super) fn parse_limit_tail(
    sql: &str,
) -> Result<(Option<LimitValue>, Option<LimitValue>), ParseError> {
    let dialect = MydbDialect::default();
    let wrapped = format!("SELECT 1 FROM t LIMIT {sql}");
    let statements = adapter::parse_sql(&dialect, &wrapped)
        .map_err(|err| ParseError::from_sqlparser(sql, err))?;
    let _source = ParserRawSqlGuard::activate(&wrapped);
    let Some(SqlStatement::Query(query)) = statements.into_iter().next() else {
        return Err(parse_error_at_zero("LIMIT clause"));
    };
    convert_limit_clause(query.limit_clause.as_ref()).map_err(|err| match err {
        LimitClauseError::Invalid(msg) => parse_error_at_zero(msg),
        LimitClauseError::Unsupported => parse_error_at_zero("LIMIT clause"),
    })
}

pub(super) fn parse_error_full_span(sql: &str, message: impl Into<String>) -> ParseError {
    ParseError::unsupported_at(
        sql,
        Span {
            start: 0,
            end: sql.len(),
        },
        message,
    )
}

/// P1-6 — locate the byte offset of the real UPDATE/DELETE keyword,
/// skipping a leading `WITH ... AS (...)` CTE prologue if present.  Used
/// by the three text pre-parsers ([`strip_update_modifiers`],
/// [`strip_delete_modifiers`], [`parse_manual_update_order_limit`]) so
/// that a WITH-prefixed DML statement is not silently mis-tokenised.
pub(super) fn find_dml_verb_after_optional_cte(sql: &str, verb: &str) -> Option<usize> {
    let trimmed_start = sql.trim_start();
    let leading = sql.len() - trimmed_start.len();
    if starts_with_keyword(trimmed_start, verb) {
        return Some(leading);
    }
    if !starts_with_keyword(trimmed_start, "WITH") {
        return None;
    }
    // Walk the top-level looking for the verb at depth 0 outside string /
    // comment / paren nesting.  Reuse the exact same rules as
    // `find_top_level_keyword` — this is essentially a specialised
    // wrapper that only accepts the target verb.
    find_top_level_keyword(sql, verb)
}

pub(super) fn find_top_level_keyword(sql: &str, keyword: &str) -> Option<usize> {
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    crate::sql_lex::StructuralCursor::new(sql, mode, MYSQL_VERSION_ID)
        .find_top_level_keyword(keyword.as_bytes())
        .ok()
        .flatten()
        .map(|range| range.start)
}

pub(super) fn parse_error_from_sql_span<T>(
    span: sqlparser::tokenizer::Span,
    message: impl Into<String>,
    expected: Vec<String>,
) -> Result<T, ParseError> {
    let line = span.start.line.max(1) as usize;
    let column = span.start.column.max(1) as usize;
    let byte_span = raw_source_byte_span(span);
    Err(ParseError {
        spec: crate::error::catalog::ER_PARSE_ERROR,
        message: message.into(),
        span: byte_span.unwrap_or(Span { start: 0, end: 0 }),
        line,
        column,
        expected,
    })
}

pub(super) fn parse_error_at_span(
    span: sqlparser::tokenizer::Span,
    message: impl Into<String>,
) -> ParseError {
    let line = span.start.line.max(1) as usize;
    let column = span.start.column.max(1) as usize;
    let byte_span = raw_source_byte_span(span);
    ParseError {
        spec: crate::error::catalog::ER_PARSE_ERROR,
        message: message.into(),
        span: byte_span.unwrap_or(Span { start: 0, end: 0 }),
        line,
        column,
        expected: Vec::new(),
    }
}

pub(super) fn parse_error_at_zero(message: impl Into<String>) -> ParseError {
    parse_error_with_spec_at_zero(crate::error::catalog::ER_PARSE_ERROR, message)
}

pub(super) fn parse_error_with_spec_at_zero(
    spec: crate::error::ErrorSpec,
    message: impl Into<String>,
) -> ParseError {
    ParseError {
        spec,
        message: message.into(),
        span: Span { start: 0, end: 0 },
        line: 1,
        column: 1,
        expected: Vec::new(),
    }
}

pub(super) fn parse_lex_error(input: &str, err: crate::sql_lex::SqlLexError) -> ParseError {
    ParseError::from_lex(input, err)
}

pub(super) fn line_column(input: &str, offset: usize) -> (usize, usize) {
    let mut line = 1;
    let mut col = 1;
    for (idx, ch) in input.char_indices() {
        if idx >= offset {
            break;
        }
        if ch == '\n' {
            line += 1;
            col = 1;
        } else {
            col += 1;
        }
    }
    (line, col)
}

/// Token-aware alias for MySQL's deprecated spelling; literals and comments
/// never participate in this rewrite.
fn rewrite_legacy_lock_in_share_mode(sql: &str) -> Result<std::borrow::Cow<'_, str>, ParseError> {
    use crate::sql_lex::{SqlLexMode, SqlTokenCursor, SqlTokenKind};
    if !sql
        .as_bytes()
        .windows(4)
        .any(|w| w.eq_ignore_ascii_case(b"lock"))
    {
        return Ok(std::borrow::Cow::Borrowed(sql));
    }
    let mut cursor = SqlTokenCursor::new(
        sql,
        SqlLexMode::from_parser_bits(current_parser_sql_mode_bits()),
        MYSQL_VERSION_ID,
    );
    let mut tokens = Vec::new();
    while let Some(token) = cursor
        .next()
        .map_err(|err| ParseError::at_zero(err.to_string()))?
    {
        tokens.push(token);
    }
    let mut out = String::new();
    let mut copied = 0;
    for chunk in tokens.windows(4) {
        if chunk
            .iter()
            .zip(["LOCK", "IN", "SHARE", "MODE"])
            .all(|(t, w)| {
                t.kind == SqlTokenKind::Word && sql[t.span.clone()].eq_ignore_ascii_case(w)
            })
        {
            out.push_str(&sql[copied..chunk[0].span.start]);
            out.push_str("FOR SHARE");
            copied = chunk[3].span.end;
        }
    }
    if copied == 0 {
        return Ok(std::borrow::Cow::Borrowed(sql));
    }
    out.push_str(&sql[copied..]);
    Ok(std::borrow::Cow::Owned(out))
}
