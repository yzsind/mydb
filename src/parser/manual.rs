//! Shared lexical primitives for hand-written MySQL grammar islands.

use super::*;

fn manual_auth_route(
    sql: &str,
    mode: crate::sql_lex::SqlLexMode,
) -> Result<bool, crate::sql_lex::SqlLexError> {
    let mut cursor = crate::sql_lex::SqlTokenCursor::new(sql, mode, MYSQL_VERSION_ID);
    if cursor.eat_keyword(b"CREATE")? {
        return Ok(cursor.eat_keyword(b"USER")? || cursor.eat_keyword(b"ROLE")?);
    }
    if cursor.eat_keyword(b"ALTER")? {
        return cursor.eat_keyword(b"USER");
    }
    if cursor.eat_keyword(b"DROP")? {
        return Ok(cursor.eat_keyword(b"USER")? || cursor.eat_keyword(b"ROLE")?);
    }
    if cursor.eat_keyword(b"RENAME")? {
        return cursor.eat_keyword(b"USER");
    }
    if cursor.eat_keyword(b"GRANT")? || cursor.eat_keyword(b"REVOKE")? {
        return Ok(true);
    }
    if cursor.eat_keyword(b"SET")? {
        if cursor.eat_keyword(b"ROLE")? || cursor.eat_keyword(b"PASSWORD")? {
            return Ok(true);
        }
        return Ok(cursor.eat_keyword(b"DEFAULT")? && cursor.eat_keyword(b"ROLE")?);
    }
    if cursor.eat_keyword(b"SHOW")? {
        if cursor.eat_keyword(b"GRANTS")? {
            return Ok(true);
        }
        return Ok(cursor.eat_keyword(b"CREATE")? && cursor.eat_keyword(b"USER")?);
    }
    if cursor.eat_keyword(b"INSTALL")? || cursor.eat_keyword(b"UNINSTALL")? {
        return cursor.eat_keyword(b"COMPONENT");
    }
    Ok(false)
}

/// Compatibility token vector used by LOCK TABLES and routine
/// parsers. Lexical boundaries and quote decoding come exclusively from the
/// shared mode-aware cursor.
pub(super) fn mysql_tokens(sql: &str) -> Vec<String> {
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let mut cursor = crate::sql_lex::SqlTokenCursor::new(sql, mode, MYSQL_VERSION_ID);
    let mut out = Vec::new();
    let mut current = String::new();
    let mut previous_end = 0usize;
    while let Ok(Some(token)) = cursor.next() {
        if token.span.start > previous_end && !current.is_empty() {
            out.push(std::mem::take(&mut current));
        }
        previous_end = token.span.end;
        match token.kind {
            crate::sql_lex::SqlTokenKind::QuotedIdentifier
            | crate::sql_lex::SqlTokenKind::String => {
                if !current.is_empty() {
                    out.push(std::mem::take(&mut current));
                }
                if let Some(decoded) = cursor.decoded_quoted_text(&token) {
                    out.push(decoded);
                }
            }
            crate::sql_lex::SqlTokenKind::Symbol(symbol)
                if matches!(symbol, b'@' | b',' | b'(' | b')' | b'=') =>
            {
                if !current.is_empty() {
                    out.push(std::mem::take(&mut current));
                }
                out.push((symbol as char).to_string());
            }
            _ => current.push_str(cursor.token_text(&token)),
        }
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

/// `DO expr [, expr] ...` — MySQL 13.2.3. sqlparser 0.61 does not recognise
/// the DO statement at the top level, so we rewrite the head as `SELECT`
/// and reuse sqlparser to harvest the expression list, then repackage as
/// `Statement::Do`. The dispatch layer evaluates each expression for its
/// side effect and discards the result without sending a result set.
pub(super) fn try_parse_do_statement(sql: &str) -> Result<Option<Statement>, ParseError> {
    let trimmed = sql.trim_start();
    if !starts_with_keyword(trimmed, "DO") {
        return Ok(None);
    }
    let leading_len = sql.len() - trimmed.len();
    // Skip leading whitespace before "DO" + the "DO" token + the
    // mandatory whitespace separator that `starts_with_keyword` already
    // guaranteed.
    let after_do_idx = leading_len + "DO".len();
    let after_do = &sql[after_do_idx..];
    let body = after_do.trim();
    if body.is_empty() {
        return Err(ParseError::at_zero("DO requires at least one expression"));
    }
    let body = body.trim_end_matches(';').trim_end();
    if body.is_empty() {
        return Err(ParseError::at_zero("DO requires at least one expression"));
    }
    // DO shares the native expression grammar through the dialect below.
    let rewritten = format!("SELECT {}", body);
    let dialect = MydbDialect::default();
    let mut statements = adapter::parse_sql(&dialect, &rewritten)
        .map_err(|err| ParseError::from_sqlparser(&rewritten, err))?;
    if statements.len() != 1 {
        return Err(ParseError::at_zero("DO expects a single expression list"));
    }
    let _source = ParserRawSqlGuard::activate(&rewritten);
    let stmt = statements.pop().unwrap();
    let SqlStatement::Query(query) = stmt else {
        return Err(ParseError::at_zero("DO requires expressions"));
    };
    let SetExpr::Select(select) = *query.body else {
        return Err(ParseError::at_zero("DO requires expressions"));
    };
    let group_by_present = match &select.group_by {
        sqlparser::ast::GroupByExpr::Expressions(items, _) => !items.is_empty(),
        sqlparser::ast::GroupByExpr::All(_) => true,
    };
    if query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !select.from.is_empty()
        || select.selection.is_some()
        || group_by_present
        || select.having.is_some()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
    {
        return Err(ParseError::at_zero(
            "DO does not accept FROM/WHERE/GROUP BY/HAVING/ORDER BY clauses",
        ));
    }
    let mut exprs = Vec::with_capacity(select.projection.len());
    for item in select.projection {
        let expr = match item {
            sqlparser::ast::SelectItem::UnnamedExpr(e) => e,
            sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => expr,
            sqlparser::ast::SelectItem::Wildcard(_)
            | sqlparser::ast::SelectItem::QualifiedWildcard(_, _) => {
                return Err(ParseError::at_zero(
                    "DO does not accept `*` in its expression list",
                ));
            }
        };
        exprs.push(convert_expr(&expr)?);
    }
    if exprs.is_empty() {
        return Err(ParseError::at_zero("DO requires at least one expression"));
    }
    Ok(Some(Statement::Do(exprs)))
}

/// Cursor over the raw statement bytes used by [`try_parse_bulk_insert`].
struct InsertCursor<'a> {
    source: &'a str,
    i: usize,
    backslash_escapes: bool,
}

/// Locate the closing quote for the escape-free literal subset owned by the
/// bulk INSERT parser. A backslash or doubled quote deliberately returns
/// `None`, preserving the existing fallback to the full sqlparser oracle.
/// The common long-payload case is one vectorized `memchr2` instead of one
/// branch per byte.
#[inline]
fn find_simple_single_quote(bytes: &[u8], start: usize) -> Option<usize> {
    let relative = memchr::memchr2(b'\'', b'\\', bytes.get(start..)?)?;
    let pos = start + relative;
    if bytes[pos] == b'\\' || bytes.get(pos + 1) == Some(&b'\'') {
        None
    } else {
        Some(pos)
    }
}

/// Identifier-continuation byte for the bulk-INSERT fast path. Unlike the
/// fulltext helper of the same shape, this includes `$` and any non-ASCII byte,
/// matching the set of characters MySQL allows in an unquoted identifier.
#[inline]
fn is_insert_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$' || b >= 0x80
}

impl<'a> InsertCursor<'a> {
    #[inline]
    fn peek(&self) -> Option<u8> {
        self.source.as_bytes().get(self.i).copied()
    }

    #[inline]
    fn skip_ws(&mut self) {
        while let Some(c) = self.peek() {
            if c.is_ascii_whitespace() {
                self.i += 1;
            } else {
                break;
            }
        }
    }

    /// Skip whitespace and ordinary comments between INSERT and its modifiers.
    /// Executable comments and optimizer hints retain semantics, so those bail
    /// to the canonical sqlparser path instead of being discarded here.
    fn skip_ws_and_plain_comments(&mut self) -> Option<()> {
        loop {
            self.skip_ws();
            if self.source.as_bytes().get(self.i..self.i + 2) != Some(b"/*") {
                return Some(());
            }
            if matches!(
                self.source.as_bytes().get(self.i + 2),
                Some(b'!') | Some(b'+')
            ) {
                return None;
            }
            let mut end = self.i + 2;
            while self.source.as_bytes().get(end..end + 2) != Some(b"*/") {
                if end + 2 > self.source.as_bytes().len() {
                    return None;
                }
                end += 1;
            }
            self.i = end + 2;
        }
    }

    /// Case-insensitively match `kw` (which must be lowercase ASCII) at the
    /// cursor with a trailing word boundary, advancing past it on success.
    fn eat_kw(&mut self, kw: &str) -> bool {
        let kb = kw.as_bytes();
        let end = self.i + kb.len();
        if end > self.source.as_bytes().len() {
            return false;
        }
        if !self.source.as_bytes()[self.i..end]
            .iter()
            .zip(kb)
            .all(|(c, k)| c.to_ascii_lowercase() == *k)
        {
            return false;
        }
        // Word boundary: the keyword must not be a prefix of a longer identifier.
        if self
            .source
            .as_bytes()
            .get(end)
            .copied()
            .is_some_and(is_insert_ident_byte)
        {
            return false;
        }
        self.i = end;
        true
    }

    fn peek_kw(&self, kw: &str) -> bool {
        let kb = kw.as_bytes();
        let end = self.i + kb.len();
        if end > self.source.as_bytes().len() {
            return false;
        }
        self.source.as_bytes()[self.i..end]
            .iter()
            .zip(kb)
            .all(|(c, k)| c.to_ascii_lowercase() == *k)
            && !self
                .source
                .as_bytes()
                .get(end)
                .copied()
                .is_some_and(is_insert_ident_byte)
    }

    /// Parse a single identifier (unquoted or simple backtick-quoted), matching
    /// sqlparser's `Ident.value` (backticks stripped, content verbatim).
    /// Returns `None` (bail) on anything fancy (doubled backticks, empty).
    fn parse_ident(&mut self) -> Option<String> {
        match self.peek()? {
            b'`' => {
                self.i += 1;
                let start = self.i;
                while let Some(c) = self.peek() {
                    if c == b'`' {
                        break;
                    }
                    self.i += 1;
                }
                if self.peek() != Some(b'`') {
                    return None; // unterminated
                }
                let end = self.i;
                // A doubled backtick (`` `` ``) is an escape we don't decode here.
                if self.source.as_bytes().get(self.i + 1).copied() == Some(b'`') {
                    return None;
                }
                if end == start {
                    return None; // empty `` `` ``
                }
                self.i += 1; // consume closing backtick
                             // SAFETY: slice of the original &str on identifier bytes.
                std::str::from_utf8(&self.source.as_bytes()[start..end])
                    .ok()
                    .map(|s| s.to_string())
            }
            c if c.is_ascii_alphabetic() || c == b'_' || c == b'$' || c >= 0x80 => {
                let start = self.i;
                while let Some(c) = self.peek() {
                    if is_insert_ident_byte(c) {
                        self.i += 1;
                    } else {
                        break;
                    }
                }
                std::str::from_utf8(&self.source.as_bytes()[start..self.i])
                    .ok()
                    .map(|s| s.to_string())
            }
            _ => None,
        }
    }

    /// Parse a (possibly schema-qualified) table name into `a` / `a.b`.
    fn parse_object_name(&mut self) -> Option<String> {
        let mut name = self.parse_ident()?;
        loop {
            let save = self.i;
            self.skip_ws();
            if self.peek() == Some(b'.') {
                self.i += 1;
                self.skip_ws();
                let part = self.parse_ident()?;
                name.push('.');
                name.push_str(&part);
            } else {
                self.i = save;
                break;
            }
        }
        Some(name)
    }

    /// Parse a parenthesized identifier list `(c1, c2, …)`. Caller has verified
    /// the cursor is at `(`. Returns `None` for an empty list (bail).
    fn parse_ident_list(&mut self) -> Option<Vec<String>> {
        debug_assert_eq!(self.peek(), Some(b'('));
        self.i += 1;
        let mut cols = Vec::new();
        loop {
            self.skip_ws();
            cols.push(self.parse_ident()?);
            self.skip_ws();
            match self.peek()? {
                b',' => self.i += 1,
                b')' => {
                    self.i += 1;
                    break;
                }
                _ => return None,
            }
        }
        if cols.is_empty() {
            None
        } else {
            Some(cols)
        }
    }

    /// Parse the replication-friendly ODKU subset
    /// `ON DUPLICATE KEY UPDATE dst=VALUES(src), ...`.
    ///
    /// This is deliberately narrow: arbitrary expressions, qualifiers,
    /// aliases, and DEFAULT continue through sqlparser. The emitted AST is the
    /// same shape as `convert_assignment` + `convert_expr` for this subset.
    fn parse_values_odku_assignments(&mut self) -> Option<Vec<Assignment>> {
        self.skip_ws();
        if !self.eat_kw("on") {
            return None;
        }
        self.skip_ws();
        if !self.eat_kw("duplicate") {
            return None;
        }
        self.skip_ws();
        if !self.eat_kw("key") {
            return None;
        }
        self.skip_ws();
        if !self.eat_kw("update") {
            return None;
        }

        let mut assignments = Vec::new();
        loop {
            self.skip_ws();
            let column = self.parse_ident()?;
            self.skip_ws();
            if self.peek()? != b'=' {
                return None;
            }
            self.i += 1;
            self.skip_ws();
            let function_name = self.parse_ident()?;
            if !function_name.eq_ignore_ascii_case("values") {
                return None;
            }
            self.skip_ws();
            if self.peek()? != b'(' {
                return None;
            }
            self.i += 1;
            self.skip_ws();
            let source_column = self.parse_ident()?;
            self.skip_ws();
            if self.peek()? != b')' {
                return None;
            }
            self.i += 1;
            assignments.push(Assignment {
                column,
                value: Expr::Function {
                    name: function_name,
                    args: vec![Expr::Identifier(source_column)],
                    distinct: false,
                },
                table_qualifier: None,
            });
            self.skip_ws();
            match self.peek() {
                Some(b',') => self.i += 1,
                None => break,
                Some(_) => return None,
            }
        }
        if assignments.is_empty() {
            None
        } else {
            Some(assignments)
        }
    }

    /// Parse one literal row value into an `Expr` identical to what
    /// `convert_expr` produces for the corresponding sqlparser node. Returns
    /// `None` (bail) for any non-trivial value.
    fn parse_value(&mut self) -> Option<Expr> {
        self.skip_ws();
        match self.peek()? {
            b'\'' => {
                self.i += 1;
                let start = self.i;
                self.i = bulk_string::end(self.source.as_bytes(), start, self.backslash_escapes)?;
                let value =
                    bulk_string::decode(self.source.get(start..self.i)?, self.backslash_escapes)?;
                self.i += 1;
                Some(Expr::Literal(Literal::String(value)))
            }
            b'-' | b'+' => {
                let neg = self.peek()? == b'-';
                self.i += 1;
                self.skip_ws();
                // Operand must itself be a numeric literal; `-col` / `-(…)` bail.
                let lit = self.parse_number_token()?;
                let inner = Expr::Literal(lit);
                if neg {
                    // Matches sqlparser: `-N` => UnaryOp::Minus over the literal.
                    Some(Expr::Unary {
                        op: UnaryOp::Neg,
                        expr: Box::new(inner),
                    })
                } else {
                    // Unary plus is dropped by `convert_expr`.
                    Some(inner)
                }
            }
            c if c.is_ascii_digit() => Some(Expr::Literal(self.parse_number_token()?)),
            c if c.is_ascii_alphabetic() => {
                if self.eat_kw("null") {
                    Some(Expr::Literal(Literal::Null))
                } else if self.eat_kw("true") {
                    Some(Expr::Literal(Literal::Bool(true)))
                } else if self.eat_kw("false") {
                    Some(Expr::Literal(Literal::Bool(false)))
                } else if let Some(hex) = self.parse_x_hex_literal() {
                    Some(Expr::Literal(Literal::HexBytes(hex)))
                } else if let Some(bytes) = self.parse_from_base64() {
                    Some(Expr::BoundValue(crate::types::Value::Bytes(bytes)))
                } else if let Some(folded) = self.parse_string_to_vector() {
                    // `STRING_TO_VECTOR('[..]')` / `TO_VECTOR('[..]')` over a
                    // literal — folded to a pre-parsed vector constant so a bulk
                    // vector INSERT skips the full sqlparser pipeline (≈46µs/row
                    // at dim=768) and the per-row function dispatch.
                    Some(folded)
                } else {
                    None // identifier / function / DEFAULT / expression — bail
                }
            }
            _ => None,
        }
    }

    /// Parse MySQL's binary-string hex literal `X'ABCD'` without routing a
    /// large bulk INSERT through sqlparser. The syntax is intentionally narrow:
    /// `0xABCD`, charset introducers, odd digit counts, invalid digits and any
    /// escaped quote all fall back to the canonical parser, preserving their
    /// context-sensitive MySQL semantics and diagnostics.
    fn parse_x_hex_literal(&mut self) -> Option<Vec<u8>> {
        let start = self.i;
        if !matches!(self.source.as_bytes().get(start), Some(b'x') | Some(b'X'))
            || self.source.as_bytes().get(start + 1) != Some(&b'\'')
        {
            return None;
        }
        let digits_start = start + 2;
        let quote = find_simple_single_quote(self.source.as_bytes(), digits_start)?;
        let digits = self.source.as_bytes().get(digits_start..quote)?;
        // X'..' requires pairs; unlike 0x..., MySQL rejects an odd nibble.
        if digits.len() % 2 != 0 || !digits.iter().all(|byte| byte.is_ascii_hexdigit()) {
            return None;
        }
        let text = std::str::from_utf8(digits).ok()?;
        let decoded = decode_hex_bytes(text)?;
        self.i = quote + 1;
        Some(decoded)
    }

    /// Fold the constant subset of `FROM_BASE64('<literal>')` to bytes. This is
    /// useful beyond VECTOR (bulk BLOB ingestion has the same wire-size problem)
    /// and mirrors `fold_const_string_to_vector`'s canonical-parser fold.
    fn parse_from_base64(&mut self) -> Option<Vec<u8>> {
        if !self.eat_kw("from_base64") {
            return None;
        }
        self.skip_ws();
        if self.peek()? != b'(' {
            return None;
        }
        self.i += 1;
        self.skip_ws();
        if self.peek()? != b'\'' {
            return None;
        }
        self.i += 1;
        let start = self.i;
        self.i = find_simple_single_quote(self.source.as_bytes(), start)?;
        let encoded = std::str::from_utf8(&self.source.as_bytes()[start..self.i]).ok()?;
        self.i += 1;
        self.skip_ws();
        if self.peek()? != b')' {
            return None;
        }
        self.i += 1;
        decode_base64_bytes(encoded)
    }

    /// Scan and classify an unsigned numeric literal token, mirroring
    /// `parse_number_literal` exactly. The first byte is a digit.
    fn parse_number_token(&mut self) -> Option<Literal> {
        let start = self.i;
        let mut seen_exp = false;
        while let Some(c) = self.peek() {
            if c.is_ascii_digit() || c == b'.' {
                self.i += 1;
            } else if c == b'e' || c == b'E' {
                seen_exp = true;
                self.i += 1;
                // Optional `+` / `-` immediately after the exponent
                // marker. Without this the fast path would bail on
                // `1e-5`, `2E+10` etc. — legal numeric literals that
                // sqlparser accepts.
                if matches!(self.peek(), Some(b'+') | Some(b'-')) {
                    self.i += 1;
                }
            } else {
                break;
            }
        }
        // `seen_exp` guards against a stray sign otherwise: this loop
        // only advances on `+`/`-` in the exponent slot.
        let _ = seen_exp;
        // The token must end at a value delimiter; `123abc` / `0x1f` bail.
        match self.peek() {
            None => {}
            Some(c) if c.is_ascii_whitespace() || c == b',' || c == b')' => {}
            Some(_) => return None,
        }
        let text = std::str::from_utf8(&self.source.as_bytes()[start..self.i]).ok()?;
        if text.is_empty() {
            return None;
        }
        if text.contains('e') || text.contains('E') {
            let v: f64 = text.parse().ok()?;
            return if v.is_finite() {
                Some(Literal::Float(v))
            } else {
                None
            };
        }
        if text.contains('.') {
            return if text.parse::<f64>().map(|v| v.is_finite()).unwrap_or(false) {
                Some(Literal::Decimal(text.to_string()))
            } else {
                None
            };
        }
        if let Ok(v) = text.parse::<i64>() {
            return Some(Literal::Int(v));
        }
        if let Ok(v) = text.parse::<u64>() {
            return Some(Literal::UInt(v));
        }
        if text.bytes().all(|b| b.is_ascii_digit()) {
            return Some(Literal::Decimal(text.to_string()));
        }
        None
    }

    /// Recognise a constant `STRING_TO_VECTOR('<lit>')` / `TO_VECTOR('<lit>')`
    /// argument and fold it to a pre-parsed `Expr::BoundValue(Value::Vector)`.
    /// The cursor is positioned at the function-name identifier. Returns `None`
    /// (caller bails the whole statement to sqlparser) on any deviation:
    /// a different function, an escaped/non-literal string argument, malformed
    /// vector text, or unexpected layout. The folded result is byte-identical to
    /// what the sqlparser path produces via [`fold_const_string_to_vector`], so
    /// the fast/slow AST-consistency invariant is preserved.
    fn parse_string_to_vector(&mut self) -> Option<Expr> {
        if !self.eat_kw("string_to_vector") && !self.eat_kw("to_vector") {
            return None;
        }
        self.skip_ws();
        if self.peek()? != b'(' {
            return None;
        }
        self.i += 1;
        self.skip_ws();
        // Escape-free single-quoted string only (mirrors `parse_value`'s string
        // arm); a backslash or `''` escape bails so sqlparser owns the decoding.
        if self.peek()? != b'\'' {
            return None;
        }
        self.i += 1;
        let start = self.i;
        self.i = find_simple_single_quote(self.source.as_bytes(), start)?;
        let s = self.source.get(start..self.i)?;
        self.i += 1; // consume closing quote
        self.skip_ws();
        if self.peek()? != b')' {
            return None;
        }
        self.i += 1;
        // On a malformed vector literal, bail so sqlparser reproduces the exact
        // eval-time error rather than us surfacing it here.
        let bytes = crate::engine::vector::codec::parse_text(s).ok()?;
        Some(Expr::BoundValue(crate::types::Value::Vector(bytes)))
    }

    /// Parse one `(v, v, …)` value tuple. Caller has verified `(`.
    fn parse_value_tuple(&mut self) -> Option<Vec<Expr>> {
        self.parse_value_tuple_with_capacity(0)
    }

    /// Parse one tuple while preallocating its expected arity. Bulk VALUES
    /// lists have a fixed row shape, so the first tuple supplies an exact hint
    /// for every remaining tuple and avoids 3-4 RawVec growths per row.
    fn parse_value_tuple_with_capacity(&mut self, expected_columns: usize) -> Option<Vec<Expr>> {
        debug_assert_eq!(self.peek(), Some(b'('));
        self.i += 1;
        let mut row = Vec::with_capacity(expected_columns);
        loop {
            self.skip_ws();
            if self.peek()? == b')' {
                // Empty tuple `()` — bail (let sqlparser define the semantics).
                return None;
            }
            row.push(self.parse_value()?);
            self.skip_ws();
            match self.peek()? {
                b',' => self.i += 1,
                b')' => {
                    self.i += 1;
                    break;
                }
                _ => return None,
            }
        }
        Some(row)
    }
}

/// Minimum total span byte length before the bulk-INSERT fast path fans the
/// per-tuple parse out across cores. Below this the worker-spawn overhead would
/// dominate, so the scan stays serial. A 256 KiB+ `VALUES` list is unambiguously
/// a bulk load (the float-heavy VECTOR ingest case), not an OLTP single-row
/// insert — keeping the threshold high means ordinary transactional INSERTs never
/// pay the parallelism overhead or risk oversubscribing under concurrency.
const BULK_PARSE_PARALLEL_MIN_BYTES: usize = 256 * 1024;

/// Host logical-core count, cached. Upper bound on any single bulk parse's
/// worker count and the denominator for fair sharing across concurrent parses.
pub(super) fn host_cores() -> usize {
    static CORES: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CORES.get_or_init(|| {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    })
}

/// Number of bulk-INSERT statements currently parsing in parallel. Used to share
/// cores fairly so that N concurrent bulk loads do not oversubscribe the machine
/// (N × full-degree threads thrashing the scheduler is *slower* than letting each
/// connection parse on its own core — connection-level concurrency already
/// scales aggregate ingest).
static ACTIVE_BULK_PARSERS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
pub(super) static ACTIVE_BULK_PARSE_WORKERS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// RAII counter for [`ACTIVE_BULK_PARSERS`]: increments on construction, returns
/// the post-increment active count, decrements on drop (covers early returns).
struct BulkParseAdmission;
impl BulkParseAdmission {
    fn enter() -> (Self, usize) {
        let active = ACTIVE_BULK_PARSERS.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        (BulkParseAdmission, active)
    }
}
impl Drop for BulkParseAdmission {
    fn drop(&mut self) {
        ACTIVE_BULK_PARSERS.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Strict process-wide admission for spawned parser workers. Connection-level
/// parser threads are already scheduled by tokio; this counter only covers the
/// extra scoped threads and therefore prevents N concurrent bulk statements
/// from creating N×CPU stacks.
pub(super) struct BulkParseWorkerPermits {
    pub(super) count: usize,
}

impl BulkParseWorkerPermits {
    pub(super) fn acquire(requested: usize) -> Self {
        let limit = host_cores().saturating_sub(1).max(1);
        let mut current = ACTIVE_BULK_PARSE_WORKERS.load(std::sync::atomic::Ordering::Acquire);
        loop {
            let count = requested.min(limit.saturating_sub(current));
            if count == 0 {
                return Self { count: 0 };
            }
            match ACTIVE_BULK_PARSE_WORKERS.compare_exchange_weak(
                current,
                current + count,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            ) {
                Ok(_) => return Self { count },
                Err(observed) => current = observed,
            }
        }
    }
}

impl Drop for BulkParseWorkerPermits {
    fn drop(&mut self) {
        ACTIVE_BULK_PARSE_WORKERS.fetch_sub(self.count, std::sync::atomic::Ordering::AcqRel);
    }
}

/// Worker count for one bulk-INSERT tuple parse, given how many bulk parses are
/// active right now. `MYDB_PARALLEL_DISABLE=1` forces serial. The cap is the host
/// cores (overridable via `MYDB_BULK_INSERT_PARALLELISM`); the *effective* degree
/// is that cap divided by the active parse count, so a lone bulk load uses the
/// whole machine while concurrent loads fall back toward one core each. This
/// phase is CPU-bound and embarrassingly parallel (float text → `f32`), and is
/// governed independently of the query-execution `MYDB_PARALLEL_DEGREE`.
fn bulk_insert_parse_degree(active: usize) -> usize {
    if crate::util::env::var("MYDB_PARALLEL_DISABLE")
        .ok()
        .as_deref()
        == Some("1")
    {
        return 1;
    }
    let cap = crate::util::env::var("MYDB_BULK_INSERT_PARALLELISM")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .map(|d| d.max(1))
        .unwrap_or_else(host_cores);
    let fair = host_cores() / active.max(1);
    cap.min(fair.max(1))
}

/// Structurally locate each `(...)` value tuple for the bulk-INSERT fast path,
/// starting at `i` (just before the first tuple, after `VALUES`). Returns the
/// byte range `[open_paren, past_close_paren)` of every tuple, or `None` to bail
/// the whole statement to sqlparser on any layout the fast path does not own
/// (unknown escape inside a string or an unterminated tuple/string).
/// The returned tail offset lets the caller either consume a supported suffix
/// (the narrow ODKU subset) or bail to sqlparser for every other trailing form.
///
/// The scan is quote- and paren-aware so commas and parentheses *inside* string
/// literals — e.g. the `'[1,2,3]'` text of `STRING_TO_VECTOR('[1,2,3]')` — never
/// split a tuple. It does not interpret tuple contents (that is left to
/// `parse_value_tuple`, which validates each span), and it mirrors the serial
/// loop's accept/bail decisions exactly, so the parallel and serial paths emit
/// identical statements.
fn scan_bulk_value_spans_prefix(b: &[u8], mut i: usize) -> Option<(Vec<(usize, usize)>, usize)> {
    let n = b.len();
    let backslash_escapes =
        current_parser_sql_mode_bits() & crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES == 0;
    let mut spans: Vec<(usize, usize)> = Vec::new();
    let mut first_width = None;
    loop {
        while i < n && b[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= n || b[i] != b'(' {
            return None;
        }
        let start = i;
        i += 1;
        let mut depth = 1u32;
        let mut width = 1usize;
        while i < n {
            match b[i] {
                b'\'' => {
                    // Skip the same conservative literal subset as the decoder.
                    i += 1;
                    i = bulk_string::end(b, i, backslash_escapes)?.saturating_add(1);
                }
                b',' if depth == 1 => {
                    width += 1;
                    i += 1;
                }
                b'(' => {
                    depth += 1;
                    i += 1;
                }
                b')' => {
                    depth -= 1;
                    i += 1;
                    if depth == 0 {
                        break;
                    }
                }
                _ => i += 1,
            }
        }
        if depth != 0 {
            return None; // unterminated tuple
        }
        // Do not allocate every later row with an oversized first-row hint.
        // The general parser preserves semantic error ordering for unequal
        // widths while allocating each row from its own syntax.
        if first_width.is_some_and(|first| first != width) {
            return None;
        }
        first_width = Some(width);
        spans.push((start, i));
        while i < n && b[i].is_ascii_whitespace() {
            i += 1;
        }
        match b.get(i) {
            None => break,        // clean end of statement
            Some(b',') => i += 1, // next tuple
            Some(_) => break,     // caller decides whether the tail is owned
        }
    }
    if spans.is_empty() {
        None
    } else {
        Some((spans, i))
    }
}

#[cfg(test)]
pub(super) fn scan_bulk_value_spans(b: &[u8], i: usize) -> Option<Vec<(usize, usize)>> {
    let (spans, tail) = scan_bulk_value_spans_prefix(b, i)?;
    if tail == b.len() {
        Some(spans)
    } else {
        None
    }
}

/// Parse the value tuples located by [`scan_bulk_value_spans`]. A large batch
/// (the float-heavy VECTOR bulk-load case) fans the independent per-tuple parses
/// out across cores; small batches stay serial to avoid worker-spawn overhead.
/// Returns `None` (bail the whole statement) if any tuple is not a literal tuple
/// the fast path owns. Every tuple is parsed by the same [`InsertCursor::
/// parse_value_tuple`], so the result is byte-identical regardless of degree.
fn parse_bulk_value_tuples(source: &str, spans: &[(usize, usize)]) -> Option<Vec<Vec<Expr>>> {
    let total_bytes = spans.last()?.1 - spans.first()?.0;
    // Admission: count this parse as active so the degree reflects current
    // concurrency. Held until the function returns (the worker join happens
    // before that), so the active count covers the whole parallel section.
    let (_admit, active) = BulkParseAdmission::enter();
    let requested = if total_bytes < BULK_PARSE_PARALLEL_MIN_BYTES {
        1
    } else {
        bulk_insert_parse_degree(active).min(spans.len())
    };
    let permits = BulkParseWorkerPermits::acquire(if requested > 1 { requested } else { 0 });
    let degree = if requested <= 1 || permits.count == 0 {
        1
    } else {
        permits.count
    };
    parse_bulk_value_tuples_from_source(source, spans, degree)
}

/// Parse all tuple `spans` with an explicit worker count (`degree`). `degree <= 1`
/// is the serial path. Every tuple — regardless of degree — is parsed by the same
/// [`InsertCursor::parse_value_tuple`] starting at its span, so the output is
/// byte-identical across degrees (the property the parallel/serial differential
/// test relies on). Returns `None` (bail the whole statement) if any tuple is not
/// a literal tuple the fast path owns.
#[cfg(test)]
pub(super) fn parse_bulk_value_tuples_with_degree(
    b: &[u8],
    spans: &[(usize, usize)],
    degree: usize,
) -> Option<Vec<Vec<Expr>>> {
    parse_bulk_value_tuples_from_source(std::str::from_utf8(b).ok()?, spans, degree)
}

fn parse_bulk_value_tuples_from_source(
    source: &str,
    spans: &[(usize, usize)],
    degree: usize,
) -> Option<Vec<Vec<Expr>>> {
    let backslash_escapes =
        current_parser_sql_mode_bits() & crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES == 0;
    let &(first_start, _) = spans.first()?;
    let mut first_cursor = InsertCursor {
        source,

        i: first_start,
        backslash_escapes,
    };
    let first = first_cursor.parse_value_tuple()?;
    let expected_columns = first.len();

    if degree <= 1 {
        let mut rows = Vec::with_capacity(spans.len());
        rows.push(first);
        for &(start, _) in &spans[1..] {
            let mut tc = InsertCursor {
                source,

                i: start,
                backslash_escapes,
            };
            rows.push(tc.parse_value_tuple_with_capacity(expected_columns)?);
        }
        return Some(rows);
    }
    const PARSER_WORKER_STACK_BYTES: usize = 256 * 1024;
    // Parallelism is optional. If the hard governor cannot reserve every
    // stack by capacity, use the allocation-free serial path.
    let worker_count = degree.min(spans.len());
    let Some(_stack_charge) =
        crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::ParserTransient,
            worker_count
                .saturating_sub(1)
                .saturating_mul(PARSER_WORKER_STACK_BYTES),
        )
    else {
        return parse_bulk_value_tuples_from_source(source, spans, 1);
    };
    let n = spans.len();
    let mut results: Vec<Option<Vec<Expr>>> = Vec::with_capacity(n);
    results.resize_with(n, || None);
    results[0] = Some(first);
    // Split the result buffer into contiguous per-worker chunks with
    // `split_at_mut` (no `unsafe`): each worker owns a disjoint `&mut` slice and
    // the matching span range. `thread::scope` joins all workers before the
    // function returns, so the later read of `results` sees every write.
    let chunk = n.div_ceil(degree);
    std::thread::scope(|s| {
        let mut rest: &mut [Option<Vec<Expr>>] = &mut results[1..];
        let mut base = 1usize;
        while !rest.is_empty() {
            let take = chunk.min(rest.len());
            let (head, tail) = rest.split_at_mut(take);
            let spans_chunk = &spans[base..base + take];
            let mut parse_chunk = move || {
                for (k, &(start, _)) in spans_chunk.iter().enumerate() {
                    let mut tc = InsertCursor {
                        source,

                        i: start,
                        backslash_escapes,
                    };
                    head[k] = tc.parse_value_tuple_with_capacity(expected_columns);
                }
            };
            // Include the calling thread in the degree instead of parking it
            // while creating another OS thread for the final chunk.
            if tail.is_empty() {
                parse_chunk();
                break;
            }
            let spawn = std::thread::Builder::new()
                .name("mydb-sql-parser".to_string())
                .stack_size(PARSER_WORKER_STACK_BYTES)
                .spawn_scoped(s, parse_chunk);
            if spawn.is_err() {
                return;
            }
            base += take;
            rest = tail;
        }
    });
    let mut rows = Vec::with_capacity(n);
    for r in results {
        rows.push(r?);
    }
    Some(rows)
}

/// Fast-path parser for `INSERT|REPLACE [IGNORE] [INTO] tbl [(cols)]
/// VALUES (..),(..),…` with literal-only row values.
///
/// Returns `Some(Statement::Insert)` **only** when the whole statement is
/// understood with full confidence and the produced AST is identical to what
/// [`parse_statement_via_sqlparser`] would yield; otherwise `None`, so the
/// caller falls back to sqlparser (which also owns error reporting for
/// malformed input). Conservative by design — signed numbers are supported,
/// but unknown escapes, double-quoted strings, functions, `DEFAULT`,
/// expressions, general `ON DUPLICATE KEY UPDATE` expressions, `SET`/`SELECT`
/// sources, `PARTITION`, row aliases (`AS`), priority modifiers, and unknown
/// trailing tokens all bail. The common replication subset
/// `col=VALUES(col)` is parsed directly and checked against sqlparser in the
/// differential tests.
/// Allocation-free preflight for the literal-only INSERT route. Nested calls,
/// expressions, tails and differing row widths stay on the canonical budget.
/// The existing decoder still decides eligibility; this only bounds the
/// allocations it may attempt before returning a complete AST or falling back.
pub(super) struct LiteralBulkLayout {
    pub source_start: usize,
    pub header_units: usize,
    pub rows: usize,
    pub values: usize,
}

pub(super) fn literal_bulk_layout(sql: &str) -> Option<LiteralBulkLayout> {
    use crate::sql_lex::{SqlTokenCursor, SqlTokenKind};
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let mut cursor = SqlTokenCursor::new(sql, mode, MYSQL_VERSION_ID);
    let first = cursor.next().ok()??;
    if first.kind != SqlTokenKind::Word
        || !["INSERT", "REPLACE"]
            .iter()
            .any(|word| cursor.token_text(&first).eq_ignore_ascii_case(word))
    {
        return None;
    }
    let mut layout = LiteralBulkLayout {
        source_start: first.span.start,
        header_units: 1,
        rows: 0,
        values: 0,
    };
    let mut depth = 0usize;
    let mut offset = loop {
        let token = cursor.next().ok()??;
        layout.header_units += 1;
        match token.kind {
            SqlTokenKind::Symbol(b'(') => depth += 1,
            SqlTokenKind::Symbol(b')') => depth = depth.checked_sub(1)?,
            SqlTokenKind::Word if depth == 0 => {
                let word = cursor.token_text(&token);
                if word.eq_ignore_ascii_case("VALUES") || word.eq_ignore_ascii_case("VALUE") {
                    break token.span.end;
                }
                if ["SELECT", "WITH", "SET"]
                    .iter()
                    .any(|name| word.eq_ignore_ascii_case(name))
                {
                    return None;
                }
            }
            _ => {}
        }
    };
    let bytes = sql.as_bytes();
    let mut first_width = None;
    loop {
        while bytes.get(offset).is_some_and(u8::is_ascii_whitespace) {
            offset += 1;
        }
        if bytes.get(offset) != Some(&b'(') {
            return None;
        }
        offset += 1;
        let mut width = 1usize;
        loop {
            match *bytes.get(offset)? {
                b'\'' => {
                    offset = bulk_string::end(bytes, offset + 1, !mode.no_backslash_escapes)? + 1;
                }
                b',' => {
                    width += 1;
                    offset += 1;
                }
                b')' => {
                    offset += 1;
                    break;
                }
                b'(' | b'"' | b'`' | b'/' | b'#' => return None,
                b'-' if bytes.get(offset + 1) == Some(&b'-') => return None,
                _ => offset += 1,
            }
        }
        if first_width.is_some_and(|first| first != width) {
            return None;
        }
        first_width = Some(width);
        layout.rows += 1;
        layout.values = layout.values.checked_add(width)?;
        // Every admitted scalar needs at most a sign/prefix and literal
        // token, plus delimiters. Let the exact general lexer handle the cap
        // when this conservative token bound is too large.
        if layout
            .values
            .saturating_mul(3)
            .saturating_add(layout.rows * 2)
            .saturating_add(layout.header_units)
            > 1_048_576
        {
            return None;
        }
        while bytes.get(offset).is_some_and(u8::is_ascii_whitespace) {
            offset += 1;
        }
        match bytes.get(offset) {
            None => return Some(layout),
            Some(b',') => offset += 1,
            _ => return None,
        }
    }
}

pub(super) fn try_parse_bulk_insert(sql: &str) -> Option<Statement> {
    let mut c = InsertCursor {
        source: sql,

        i: 0,
        backslash_escapes: current_parser_sql_mode_bits()
            & crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES
            == 0,
    };
    c.skip_ws();
    let replace = if c.eat_kw("insert") {
        false
    } else if c.eat_kw("replace") {
        true
    } else {
        return None;
    };
    c.skip_ws_and_plain_comments()?;
    // Optional `IGNORE` (INSERT only). Priority/DELAYED modifiers bail.
    let mut ignore = false;
    c.skip_ws();
    if c.peek_kw("low_priority") || c.peek_kw("delayed") || c.peek_kw("high_priority") {
        return None;
    }
    if !replace && c.eat_kw("ignore") {
        ignore = true;
        c.skip_ws();
    }
    // Optional `INTO`.
    let _ = c.eat_kw("into");
    c.skip_ws();
    let table = c.parse_object_name()?;
    c.skip_ws();
    let columns = if c.peek() == Some(b'(') {
        Some(c.parse_ident_list()?)
    } else {
        None
    };
    c.skip_ws();
    if !(c.eat_kw("values") || c.eat_kw("value")) {
        return None;
    }
    // Locate every `(...)` value tuple with a cheap quote/paren-aware structural
    // scan, then parse them — fanning the independent per-tuple parses out across
    // cores for a large bulk load (parse, dominated by float text → `f32`, is
    // ≈72% of VECTOR ingest at dim=768 and is embarrassingly parallel by row).
    // Any structural anomaly bails the whole statement to sqlparser, identically
    // to the serial path.
    let (spans, tail) = scan_bulk_value_spans_prefix(c.source.as_bytes(), c.i)?;
    c.i = tail;
    let on_duplicate_key_update = if c.i == c.source.as_bytes().len() {
        None
    } else if replace || ignore {
        return None;
    } else {
        Some(c.parse_values_odku_assignments()?)
    };
    let rows: Vec<Vec<Expr>> = parse_bulk_value_tuples(sql, &spans)?;
    Some(Statement::Insert(Insert {
        table,
        target_alias: None,
        columns,
        source: InsertSource::Values(rows),
        view_check_predicate: None,
        on_duplicate_key_update,
        insert_alias: None,
        ignore,
        replace,
        delayed: false,
        partitions: Vec::new(),
        query_hints: Vec::new(),
        hint_warnings: Vec::new(),
        returning: None,
    }))
}

// LOAD/auth/admin/transaction manual grammar families.
pub(super) fn parse_load_data_statement(sql: &str) -> Result<Option<Statement>, ParseError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if !first_keyword_is(trimmed, b"LOAD") {
        return Ok(None);
    }

    let mut p = LoadDataParser::new(trimmed);
    match p.parse() {
        Ok(stmt) => Ok(Some(Statement::LoadData(stmt))),
        Err(msg) if msg == "Field separator argument is not what is expected" =>
            Err(parse_error_with_spec_at_zero(crate::error::catalog::ER_WRONG_FIELD_TERMINATORS, msg)),
        Err(msg) => Err(ParseError::new(
            trimmed,
            Span { start: 0, end: trimmed.len() },
            msg,
            vec!["LOAD {DATA | XML} [LOW_PRIORITY|CONCURRENT] [LOCAL] INFILE 'path' [REPLACE|IGNORE] INTO TABLE tbl_name [PARTITION (...)] [CHARACTER SET cs] [FIELDS ...] [LINES ...] [ROWS IDENTIFIED BY '<tag>'] [IGNORE n LINES] [(col_or_@var, ...)] [SET col=expr, ...]".to_string()],
        )),
    }
}

struct LoadDataParser<'a> {
    sql: &'a str,
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> LoadDataParser<'a> {
    fn new(sql: &'a str) -> Self {
        Self {
            sql,
            bytes: sql.as_bytes(),
            pos: 0,
        }
    }

    fn parse(&mut self) -> Result<LoadData, String> {
        self.skip_ws();
        self.expect_keyword("LOAD")?;
        self.skip_ws();
        // LOAD DATA vs LOAD XML.
        let is_xml = if self.try_keyword("XML") {
            true
        } else {
            self.expect_keyword("DATA")?;
            false
        };
        self.skip_ws();

        let (query_hints, hint_warnings) = self.parse_optimizer_hint()?;
        self.skip_ws();

        // G9 — `LOW_PRIORITY | CONCURRENT` prefix (accepted, no-op).
        let priority = if self.try_keyword("LOW_PRIORITY") {
            self.skip_ws();
            LoadPriority::LowPriority
        } else if self.try_keyword("CONCURRENT") {
            self.skip_ws();
            LoadPriority::Concurrent
        } else {
            LoadPriority::None
        };

        let local = self.try_keyword("LOCAL");
        if local {
            self.skip_ws();
        }

        self.expect_keyword("INFILE")?;
        self.skip_ws();
        let file_path = self.parse_string_literal()?;
        self.skip_ws();

        let mut ignore_duplicates = false;
        let mut replace = false;
        if self.try_keyword("REPLACE") {
            replace = true;
            self.skip_ws();
        } else if self.try_keyword("IGNORE") {
            ignore_duplicates = true;
            self.skip_ws();
        }

        self.expect_keyword("INTO")?;
        self.skip_ws();
        self.expect_keyword("TABLE")?;
        self.skip_ws();
        let table = self.parse_identifier()?;
        self.skip_ws();
        let partitions = if self.try_keyword("PARTITION") {
            self.skip_ws();
            self.parse_identifier_list()?
        } else {
            Vec::new()
        };
        self.skip_ws();

        // G3 — `CHARACTER SET charset_name` (after PARTITION, before FIELDS).
        let charset = if self.try_keyword("CHARACTER") {
            self.skip_ws();
            self.expect_keyword("SET")?;
            self.skip_ws();
            let cs = self.parse_charset_name()?;
            self.skip_ws();
            Some(cs)
        } else if self.try_keyword("CHARSET") {
            self.skip_ws();
            let cs = self.parse_charset_name()?;
            self.skip_ws();
            Some(cs)
        } else {
            None
        };

        let mut field_terminated_by = "\t".to_string();
        let mut field_enclosed_by = None;
        let mut field_optionally_enclosed = false;
        // G10 — escape char is optional: `ESCAPED BY ''` disables escape
        // processing; the MySQL default is backslash.
        let mut field_escaped_by = Some('\\');
        let mut line_terminated_by = "\n".to_string();
        let mut line_starting_by = String::new();

        if self.try_keyword("FIELDS") || self.try_keyword("COLUMNS") {
            self.skip_ws();
            if self.try_keyword("TERMINATED") {
                self.skip_ws();
                self.expect_keyword("BY")?;
                self.skip_ws();
                field_terminated_by = self.parse_string_literal()?;
                self.skip_ws();
            }
            if self.try_keyword("OPTIONALLY") {
                self.skip_ws();
                self.expect_keyword("ENCLOSED")?;
                self.skip_ws();
                self.expect_keyword("BY")?;
                self.skip_ws();
                let s = self.parse_string_literal()?;
                // G10 — `ENCLOSED BY ''` means no enclosure char.
                field_enclosed_by = load_single_character(&s)?;
                field_optionally_enclosed = true;
                self.skip_ws();
            } else if self.try_keyword("ENCLOSED") {
                self.skip_ws();
                self.expect_keyword("BY")?;
                self.skip_ws();
                let s = self.parse_string_literal()?;
                field_enclosed_by = load_single_character(&s)?;
                self.skip_ws();
            }
            if self.try_keyword("ESCAPED") {
                self.skip_ws();
                self.expect_keyword("BY")?;
                self.skip_ws();
                let s = self.parse_string_literal()?;
                // Empty string → escape processing disabled (None).
                field_escaped_by = load_single_character(&s)?;
                self.skip_ws();
            }
        }

        if self.try_keyword("LINES") {
            self.skip_ws();
            if self.try_keyword("STARTING") {
                self.skip_ws();
                self.expect_keyword("BY")?;
                self.skip_ws();
                line_starting_by = self.parse_string_literal()?;
                self.skip_ws();
            }
            if self.try_keyword("TERMINATED") {
                self.skip_ws();
                self.expect_keyword("BY")?;
                self.skip_ws();
                line_terminated_by = self.parse_string_literal()?;
                self.skip_ws();
            }
        }

        // LOAD XML — `ROWS IDENTIFIED BY '<tag>'`.
        let xml_rows_identified_by = if is_xml && self.try_keyword("ROWS") {
            self.skip_ws();
            self.expect_keyword("IDENTIFIED")?;
            self.skip_ws();
            self.expect_keyword("BY")?;
            self.skip_ws();
            let tag = self.parse_string_literal()?;
            self.skip_ws();
            Some(tag)
        } else {
            None
        };

        let mut ignore_rows: usize = 0;
        if self.try_keyword("IGNORE") {
            self.skip_ws();
            ignore_rows = self.parse_usize()?;
            self.skip_ws();
            if !self.try_keyword("LINES") {
                self.try_keyword("ROWS");
            }
            self.skip_ws();
        }

        // Target list: `(col_or_@var [, ...])`. G2 — `@user_var` targets.
        let targets = if self.peek_byte() == Some(b'(') {
            Some(self.parse_target_list()?)
        } else {
            None
        };
        self.skip_ws();

        // G1 — `SET col = {expr | DEFAULT} [, ...]`.
        let mut set_clauses = Vec::new();
        if self.try_keyword("SET") {
            self.skip_ws();
            loop {
                let col = self.parse_identifier()?;
                self.skip_ws();
                if self.peek_byte() != Some(b'=') {
                    return Err(format!(
                        "expected '=' in SET clause at position {}",
                        self.pos
                    ));
                }
                self.pos += 1;
                self.skip_ws();
                let expr = self.parse_set_expr()?;
                set_clauses.push((col, expr));
                self.skip_ws();
                if self.peek_byte() == Some(b',') {
                    self.pos += 1;
                    self.skip_ws();
                } else {
                    break;
                }
            }
        }

        // Tail-exhaustion check: consume trailing whitespace / ';' and fail
        // on any residue. This kills the historic G1 silent-drop where a
        // trailing `SET ...` (or any unrecognised tail) was swallowed.
        self.skip_ws();
        while self.peek_byte() == Some(b';') {
            self.pos += 1;
            self.skip_ws();
        }
        if self.pos < self.bytes.len() {
            return Err(format!(
                "unexpected trailing input at position {}: '{}'",
                self.pos,
                &self.sql[self.pos..].trim_end()
            ));
        }

        Ok(LoadData {
            local,
            priority,
            is_xml,
            file_path,
            table,
            partitions,
            charset,
            targets,
            set_clauses,
            query_hints,
            hint_warnings,
            ignore_rows,
            field_terminated_by,
            field_enclosed_by,
            field_optionally_enclosed,
            field_escaped_by,
            line_terminated_by,
            line_starting_by,
            xml_rows_identified_by,
            ignore_duplicates,
            replace,
            data: None,
        })
    }

    fn skip_ws(&mut self) {
        let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
        if let Ok(pos) =
            crate::sql_lex::skip_trivia(self.sql, self.pos, mode, MYSQL_VERSION_ID, true)
        {
            self.pos = pos;
        }
    }

    fn peek_byte(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn parse_optimizer_hint(&mut self) -> Result<(Vec<QueryHint>, Vec<String>), String> {
        if !self.bytes[self.pos..].starts_with(b"/*+") {
            return Ok((Vec::new(), Vec::new()));
        }
        let hint_start = self.pos + 3;
        let mut pos = hint_start;
        while pos + 1 < self.bytes.len() {
            if self.bytes[pos] == b'*' && self.bytes[pos + 1] == b'/' {
                let text = &self.sql[hint_start..pos];
                self.pos = pos + 2;
                return Ok(parse_query_hints(text));
            }
            pos += 1;
        }
        Err("unterminated optimizer hint".to_string())
    }

    fn try_keyword(&mut self, keyword: &str) -> bool {
        let end = self.pos + keyword.len();
        if end > self.bytes.len() {
            return false;
        }
        if !self.sql[self.pos..end].eq_ignore_ascii_case(keyword) {
            return false;
        }
        if end < self.bytes.len()
            && (self.bytes[end].is_ascii_alphanumeric() || self.bytes[end] == b'_')
        {
            return false;
        }
        self.pos = end;
        true
    }

    fn expect_keyword(&mut self, keyword: &str) -> Result<(), String> {
        if !self.try_keyword(keyword) {
            return Err(format!("expected '{}' at position {}", keyword, self.pos));
        }
        Ok(())
    }

    fn parse_string_literal(&mut self) -> Result<String, String> {
        let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
        let mut cursor =
            crate::sql_lex::SqlTokenCursor::new(&self.sql[self.pos..], mode, MYSQL_VERSION_ID);
        let token = cursor
            .next()
            .map_err(|err| err.to_string())?
            .ok_or_else(|| format!("expected string literal at position {}", self.pos))?;
        if token.span.start != 0 || token.kind != crate::sql_lex::SqlTokenKind::String {
            return Err(format!("expected string literal at position {}", self.pos));
        }
        let decoded = cursor
            .decoded_quoted_text(&token)
            .ok_or_else(|| format!("invalid string literal at position {}", self.pos))?;
        self.pos += token.span.end;
        Ok(decoded)
    }

    fn parse_identifier(&mut self) -> Result<String, String> {
        if self.peek_byte() == Some(b'`') {
            self.pos += 1;
            let start = self.pos;
            while self.pos < self.bytes.len() && self.bytes[self.pos] != b'`' {
                self.pos += 1;
            }
            if self.pos >= self.bytes.len() {
                return Err("unterminated backtick identifier".to_string());
            }
            let name = self.sql[start..self.pos].to_string();
            self.pos += 1;
            Ok(name)
        } else {
            let start = self.pos;
            while self.pos < self.bytes.len()
                && (self.bytes[self.pos].is_ascii_alphanumeric()
                    || self.bytes[self.pos] == b'_'
                    || self.bytes[self.pos] == b'$'
                    || self.bytes[self.pos] == b'.')
            {
                self.pos += 1;
            }
            if self.pos == start {
                return Err(format!("expected identifier at position {}", self.pos));
            }
            Ok(self.sql[start..self.pos].to_string())
        }
    }

    fn parse_usize(&mut self) -> Result<usize, String> {
        let start = self.pos;
        while self.pos < self.bytes.len() && self.bytes[self.pos].is_ascii_digit() {
            self.pos += 1;
        }
        if self.pos == start {
            return Err(format!("expected number at position {}", self.pos));
        }
        self.sql[start..self.pos]
            .parse::<usize>()
            .map_err(|e| e.to_string())
    }

    fn parse_identifier_list(&mut self) -> Result<Vec<String>, String> {
        if self.peek_byte() != Some(b'(') {
            return Err("expected '(' for identifier list".to_string());
        }
        self.pos += 1;
        let mut cols = Vec::new();
        loop {
            self.skip_ws();
            cols.push(self.parse_identifier()?);
            self.skip_ws();
            match self.peek_byte() {
                Some(b',') => {
                    self.pos += 1;
                }
                Some(b')') => {
                    self.pos += 1;
                    break;
                }
                _ => return Err("expected ',' or ')' in column list".to_string()),
            }
        }
        Ok(cols)
    }

    /// Parse a charset name: a bare identifier (`utf8mb4`, `latin1`,
    /// `binary`) or a quoted string.
    fn parse_charset_name(&mut self) -> Result<String, String> {
        match self.peek_byte() {
            Some(b'\'') | Some(b'"') => self.parse_string_literal(),
            _ => self.parse_identifier(),
        }
    }

    /// Parse the `(col_or_@var [, ...])` target list. Each element is
    /// either `@ident` → `LoadTarget::UserVar`, or `ident` / `` `ident` ``
    /// → `LoadTarget::Column`.
    fn parse_target_list(&mut self) -> Result<Vec<LoadTarget>, String> {
        if self.peek_byte() != Some(b'(') {
            return Err("expected '(' for column list".to_string());
        }
        self.pos += 1;
        let mut targets = Vec::new();
        // Allow an empty list `()` (MySQL accepts it as "no columns").
        self.skip_ws();
        if self.peek_byte() == Some(b')') {
            self.pos += 1;
            return Ok(targets);
        }
        loop {
            self.skip_ws();
            if self.peek_byte() == Some(b'@') {
                self.pos += 1;
                let name = self.parse_identifier()?;
                targets.push(LoadTarget::UserVar(name));
            } else {
                let name = self.parse_identifier()?;
                targets.push(LoadTarget::Column(name));
            }
            self.skip_ws();
            match self.peek_byte() {
                Some(b',') => {
                    self.pos += 1;
                }
                Some(b')') => {
                    self.pos += 1;
                    break;
                }
                _ => return Err("expected ',' or ')' in column list".to_string()),
            }
        }
        Ok(targets)
    }

    /// Parse a single `SET`-clause right-hand side. `DEFAULT` (bare) maps
    /// to `Expr::Default(None)`; otherwise the expression substring is
    /// sliced out (respecting nested parens and quoted strings) up to the
    /// next top-level `,` or end-of-statement and handed to the full
    /// expression parser.
    fn parse_set_expr(&mut self) -> Result<Expr, String> {
        self.skip_ws();
        let start = self.pos;
        let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
        let mut cursor =
            crate::sql_lex::SqlTokenCursor::new(&self.sql[start..], mode, MYSQL_VERSION_ID);
        let mut depth = 0usize;
        self.pos = self.bytes.len();
        while let Some(token) = cursor.next().map_err(|err| err.to_string())? {
            use crate::sql_lex::SqlTokenKind;
            match token.kind {
                SqlTokenKind::Symbol(b',' | b';' | b')') if depth == 0 => {
                    self.pos = start + token.span.start;
                    break;
                }
                SqlTokenKind::Symbol(b'(') => depth += 1,
                SqlTokenKind::Symbol(b')') => depth = depth.saturating_sub(1),
                _ => {}
            }
        }
        let raw = self.sql[start..self.pos].trim();
        if raw.is_empty() {
            return Err(format!(
                "expected expression in SET clause at position {start}"
            ));
        }
        if raw.eq_ignore_ascii_case("DEFAULT") {
            return Ok(Expr::Default(None));
        }
        parse_expr_fragment(raw).map_err(|e| e.message)
    }
}

fn load_single_character(value: &str) -> Result<Option<char>, String> {
    let mut chars = value.chars();
    let first = chars.next();
    if chars.next().is_some() {
        return Err("Field separator argument is not what is expected".into());
    }
    Ok(first)
}

pub(super) fn parse_manual_auth_statement(sql: &str) -> Option<Statement> {
    // Head guard. Without it the account grammar would scan
    // the *entire* SQL payload (~1 MB for a sysbench multi-row INSERT) on
    // every COM_QUERY — visible in profiles at >10 % of CPU before this
    // gate was added. The supported auth keywords are:
    //   CREATE {USER|ROLE}, ALTER USER, DROP {USER|ROLE},
    //   RENAME USER, GRANT, REVOKE, SET {ROLE|PASSWORD|DEFAULT ROLE},
    //   SHOW {GRANTS|CREATE USER}.
    if !first_keyword_in(
        sql,
        &[
            b"CREATE",
            b"ALTER",
            b"DROP",
            b"RENAME",
            b"GRANT",
            b"REVOKE",
            b"SET",
            b"SHOW",
            b"INSTALL",
            b"UNINSTALL",
            b"CANCEL",
        ],
    ) {
        return None;
    }
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    if !manual_auth_route(sql, mode).ok()? {
        return None;
    }
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if let Some(statement) = parse_user_statement(trimmed).ok()? {
        return Some(statement);
    }
    if let Some(statement) = parse_account_statement(trimmed).ok()? {
        return Some(statement);
    }
    if let Some(statement) = parse_role_statement(trimmed).ok()? {
        return Some(statement);
    }
    parse_privilege_statement(trimmed).ok().flatten()
}

pub(super) fn parse_manual_lock_tables_statement(sql: &str) -> Option<Statement> {
    // Head guard — only LOCK / UNLOCK reach the full tokenisation. Without
    // this gate `mysql_tokens(trimmed)` would scan a 1 MB INSERT byte-by-byte
    // before the upper.first() match below rejected it.
    if !first_keyword_in(sql, &[b"LOCK", b"UNLOCK"]) {
        return None;
    }
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let tokens = mysql_tokens(trimmed);
    let upper = tokens
        .iter()
        .map(|t| t.to_ascii_uppercase())
        .collect::<Vec<_>>();
    match upper.as_slice() {
        [cmd, table] if cmd == "UNLOCK" && (table == "TABLE" || table == "TABLES") => {
            return Some(Statement::UnlockTables);
        }
        _ => {}
    }
    if upper.first().map(String::as_str) != Some("LOCK")
        || !matches!(upper.get(1).map(String::as_str), Some("TABLE" | "TABLES"))
    {
        return None;
    }
    let mut specs = Vec::new();
    let mut idx = 2;
    while idx < tokens.len() {
        let table = tokens.get(idx)?.clone();
        idx += 1;
        let mut alias = None;
        if upper.get(idx).map(String::as_str) == Some("AS") {
            idx += 1;
            alias = tokens.get(idx).cloned();
            idx += 1;
        } else if !matches!(
            upper.get(idx).map(String::as_str),
            Some("READ" | "LOW_PRIORITY" | "WRITE" | ",")
        ) {
            alias = tokens.get(idx).cloned();
            idx += 1;
        }
        let mut low_priority_write = false;
        let mode = match upper.get(idx).map(String::as_str) {
            Some("READ") => {
                idx += 1;
                if upper.get(idx).map(String::as_str) == Some("LOCAL") {
                    idx += 1;
                }
                LockTableMode::Read
            }
            Some("LOW_PRIORITY") => {
                idx += 1;
                if upper.get(idx).map(String::as_str) != Some("WRITE") {
                    return Some(Statement::Unsupported {
                        kind: "LOCK TABLES LOW_PRIORITY".to_string(),
                        sql: sql.to_string(),
                    });
                }
                idx += 1;
                low_priority_write = true;
                LockTableMode::Write
            }
            Some("WRITE") => {
                idx += 1;
                LockTableMode::Write
            }
            _ => return None,
        };
        specs.push(LockTableSpec {
            table,
            alias,
            mode,
            low_priority_write,
        });
        match upper.get(idx).map(String::as_str) {
            Some(",") => {
                idx += 1;
                if idx == tokens.len() {
                    return None;
                }
            }
            None => break,
            _ => {
                return Some(Statement::Unsupported {
                    kind: "LOCK TABLES syntax".to_string(),
                    sql: sql.to_string(),
                })
            }
        }
    }
    (!specs.is_empty()).then_some(Statement::LockTables(specs))
}

fn parse_admin_object_name_list(sql: &str) -> Option<Vec<String>> {
    use crate::sql_lex::{SqlTokenCursor, SqlTokenKind};

    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let mut cursor = SqlTokenCursor::new(sql, mode, MYSQL_VERSION_ID);
    let mut names = Vec::new();
    loop {
        let first = cursor.next().ok()??;
        let mut name = match first.kind {
            SqlTokenKind::Word => cursor.token_text(&first).to_string(),
            SqlTokenKind::QuotedIdentifier => cursor.decoded_quoted_text(&first)?,
            _ => return None,
        };
        loop {
            let is_dot = cursor
                .peek()
                .ok()?
                .is_some_and(|token| token.kind == SqlTokenKind::Symbol(b'.'));
            if !is_dot {
                break;
            }
            let _ = cursor.next().ok()??;
            let part = cursor.next().ok()??;
            let part = match part.kind {
                SqlTokenKind::Word => cursor.token_text(&part).to_string(),
                SqlTokenKind::QuotedIdentifier => cursor.decoded_quoted_text(&part)?,
                _ => return None,
            };
            name.push('.');
            name.push_str(&part);
        }
        names.push(name);
        match cursor.peek().ok()? {
            None => return Some(names),
            Some(token) if token.kind == SqlTokenKind::Symbol(b',') => {
                let _ = cursor.next().ok()??;
            }
            Some(_) => return None,
        }
    }
}

pub(super) fn reject_invalid_show_mydb_memory(sql: &str) -> Result<(), ParseError> {
    if !first_keyword_in(sql, &[b"SHOW"]) {
        return Ok(());
    }
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let cleaned = crate::sql_lex::mask_comments(sql, mode, MYSQL_VERSION_ID, false)
        .unwrap_or_else(|_| std::borrow::Cow::Borrowed(sql));
    let normalized = normalize_admin_spacing(cleaned.trim().trim_end_matches(';').trim(), mode)
        .ok_or_else(|| parse_error_at_zero("invalid administrative statement"))?;
    let upper = normalized.to_ascii_uppercase();
    if upper.starts_with("SHOW MYDB MEMORY ") && upper != "SHOW MYDB MEMORY SESSIONS" {
        return Err(parse_error_at_zero(
            "You have an error in your SQL syntax near SHOW MYDB MEMORY; expected end of statement or SESSIONS",
        ));
    }
    Ok(())
}

pub(super) const MYSQL_ADMIN_HEADS: &[&[u8]] = &[
    b"CREATE",
    b"ALTER",
    b"DROP",
    b"RENAME",
    b"GRANT",
    b"REVOKE",
    b"SET",
    b"SHOW",
    b"FLUSH",
    b"KILL",
    b"ANALYZE",
    b"CHECK",
    b"CHECKSUM",
    b"REPAIR",
    b"OPTIMIZE",
    b"CHANGE",
    b"START",
    b"STOP",
    b"RESET",
    b"PROMOTE",
    b"DEMOTE",
    b"RESUME",
    b"PURGE",
    b"UNDO",
    b"SHUTDOWN",
    b"RESTART",
    b"INSTALL",
    b"UNINSTALL",
    b"CANCEL",
    b"BACKUP",
];

/// Collapse trivia only. Quoted payload is object identity, not whitespace.
fn normalize_admin_spacing(sql: &str, mode: crate::sql_lex::SqlLexMode) -> Option<String> {
    let mut out = String::with_capacity(sql.len());
    let mut space = false;
    for segment in crate::sql_lex::segments(sql, mode, MYSQL_VERSION_ID) {
        let segment = segment.ok()?;
        let text = &sql[segment.range];
        if segment.kind == crate::sql_lex::SqlSegmentKind::Code {
            for ch in text.chars() {
                if ch.is_ascii_whitespace() {
                    space = true;
                } else {
                    if space && !out.is_empty() {
                        out.push(' ');
                    }
                    space = false;
                    out.push(ch);
                }
            }
        } else {
            if space && !out.is_empty() {
                out.push(' ');
            }
            space = false;
            out.push_str(text);
        }
    }
    Some(out)
}

pub(super) fn parse_manual_mysql_admin_statement(sql: &str) -> Option<Statement> {
    // Head guard — exits in O(1) on a 1 MB INSERT statement, rather than
    // running parse_manual_auth (mysql_tokens), then 3 admin probes
    // (each doing a 1 MB `to_ascii_uppercase`), then `split_whitespace
    // + join + to_ascii_uppercase` over the full payload. The set
    // below covers every admin statement this function recognises
    // (auth + replication + binlog + KILL / ANALYZE TABLE / FLUSH /
    // ALTER SYSTEM / SHOW ENGINE…).
    if !first_keyword_in(sql, MYSQL_ADMIN_HEADS) {
        return None;
    }
    // `first_keyword_in` deliberately looks past client metadata comments,
    // but the exact/prefix matches below must use the same logical statement.
    // Otherwise queries such as `/* ApplicationName=NineData */ SHOW ENGINES`
    // pass the head guard and then fall through as an unsupported sqlparser
    // statement because `upper` still begins with the comment text.
    let sql = strip_leading_sql_comments(sql);
    if let Some(statement) = parse_manual_auth_statement(sql) {
        return Some(statement);
    }
    if crate::replication::is_replication_admin_sql(sql) {
        return Some(Statement::ReplicationAdmin {
            sql: sql.trim().trim_end_matches(';').trim().to_string(),
        });
    }
    if crate::repl::is_mydb_repl_admin_sql(sql) {
        return Some(Statement::MydbReplicationAdmin {
            sql: sql.trim().trim_end_matches(';').trim().to_string(),
        });
    }
    if crate::binlog::is_binlog_admin_sql(sql) {
        return Some(Statement::BinlogAdmin {
            sql: sql.trim().trim_end_matches(';').trim().to_string(),
        });
    }
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let cleaned = crate::sql_lex::mask_comments(sql, mode, MYSQL_VERSION_ID, false)
        .unwrap_or_else(|_| std::borrow::Cow::Borrowed(sql));
    let normalized = normalize_admin_spacing(cleaned.trim().trim_end_matches(';').trim(), mode)?;
    let upper = normalized.to_ascii_uppercase();
    if let Some(raw_job_id) = upper.strip_prefix("CANCEL DDL JOB ") {
        return Some(match raw_job_id.parse::<u64>() {
            Ok(job_id) => Statement::CancelDdlJob(job_id),
            Err(_) => Statement::Unsupported {
                kind: "CANCEL DDL JOB requires an unsigned numeric job id".to_string(),
                sql: sql.to_string(),
            },
        });
    }
    match upper.as_str() {
        "SHUTDOWN" => return Some(Statement::Shutdown),
        "RESTART" => return Some(Statement::Restart),
        "SHOW ENGINES" | "SHOW STORAGE ENGINES" => return Some(Statement::ShowEngines),
        "SHOW ENGINE MYDB STATUS" => return Some(Statement::ShowEngineMydbStatus),
        "SHOW MYDB MEMORY" => return Some(Statement::ShowMydbMemory),
        "SHOW MYDB MEMORY SESSIONS" => return Some(Statement::ShowMydbMemorySessions),
        "SHOW PROCESSLIST" => return Some(Statement::ShowProcesslist { full: false }),
        "SHOW FULL PROCESSLIST" => return Some(Statement::ShowProcesslist { full: true }),
        "SHOW GRANTS" => return Some(Statement::ShowGrants),
        "FLUSH PRIVILEGES" => return Some(Statement::FlushPrivileges),
        "FLUSH USER_RESOURCES" | "FLUSH USER RESOURCES" => {
            return Some(Statement::FlushUserResources)
        }
        "FLUSH HOSTS" => return Some(Statement::FlushHosts),
        "FLUSH STATUS" => return Some(Statement::FlushStatus),
        "FLUSH QUERY CACHE" | "RESET QUERY CACHE" => return Some(Statement::FlushQueryCache),
        "FLUSH TABLES" => {
            return Some(Statement::FlushTables {
                tables: Vec::new(),
                with_read_lock: false,
                for_export: false,
            });
        }
        // NOTE: bare `FLUSH TABLES WITH READ LOCK` is intercepted earlier by
        // `binlog::is_binlog_admin_sql` (it owns the global read lock state),
        // so this arm is unreachable for that exact form. The table-list
        // variant `FLUSH TABLES t1, t2 [WITH READ LOCK]` is handled by the
        // prefix-match block below — that path is not yet supported and
        // returns 1235.
        "FLUSH LOGS" => {
            return Some(Statement::FlushLogs {
                kind: crate::engine::logging::FlushLogKind::All,
            })
        }
        "FLUSH GENERAL LOGS" | "FLUSH GENERAL LOG" => {
            return Some(Statement::FlushLogs {
                kind: crate::engine::logging::FlushLogKind::General,
            })
        }
        "FLUSH SLOW LOGS" | "FLUSH SLOW LOG" => {
            return Some(Statement::FlushLogs {
                kind: crate::engine::logging::FlushLogKind::Slow,
            })
        }
        "FLUSH ERROR LOGS" | "FLUSH ERROR LOG" => {
            return Some(Statement::FlushLogs {
                kind: crate::engine::logging::FlushLogKind::Error,
            })
        }
        "SHOW PRIVILEGES" => return Some(Statement::ShowPrivileges),
        "SHOW PLUGINS" => return Some(Statement::ShowPlugins),
        "ALTER SYSTEM CHECKPOINT" => return Some(Statement::AlterSystemCheckpoint),
        "ALTER SYSTEM FLUSH DATA_CACHE" => return Some(Statement::AlterSystemFlushDataCache),
        "ALTER SYSTEM FLUSH SQL_CACHE" => return Some(Statement::AlterSystemFlushSqlCache),
        _ => {}
    }

    // Generalised `SHOW ENGINE engine_name {STATUS | MUTEX}` — every shape
    // except the exact `SHOW ENGINE MYDB STATUS` caught above. Non-`MYDB`
    // engines and the `MUTEX` sub-command flow through here; the executor
    // routes `MYDB`/`MUTEX` and rejects unknown engines with 1286.
    // `SHOW {PROCEDURE|FUNCTION} CODE name` is a debug-build-only feature in
    // MySQL (needs `-DWITH_DEBUG`); on a production server it is a recognised
    // statement that is simply unavailable. Report it as parsed-but-unexecuted
    // (1235) rather than letting it fall through to a raw 1064 syntax error.
    if upper.starts_with("SHOW PROCEDURE CODE ") || upper.starts_with("SHOW FUNCTION CODE ") {
        let kind = if upper.starts_with("SHOW PROCEDURE CODE ") {
            "SHOW PROCEDURE CODE"
        } else {
            "SHOW FUNCTION CODE"
        };
        return Some(Statement::Unsupported {
            kind: format!("{kind} (requires a debug build)"),
            sql: sql.to_string(),
        });
    }

    if upper.starts_with("SHOW ENGINE ") {
        let kind = if upper.ends_with(" STATUS") {
            Some(ShowEngineKind::Status)
        } else if upper.ends_with(" MUTEX") {
            Some(ShowEngineKind::Mutex)
        } else {
            None
        };
        if let Some(kind) = kind {
            let tokens: Vec<&str> = normalized.split(' ').collect();
            // [SHOW, ENGINE, <engine_name>, STATUS|MUTEX]
            if tokens.len() == 4 {
                let engine = tokens[2].trim_matches('`').trim_matches('\'').to_string();
                return Some(Statement::ShowEngine { engine, kind });
            }
        }
    }

    if upper.starts_with("ALTER SYSTEM ") {
        return Some(Statement::Unsupported {
            kind: normalized,
            sql: sql.to_string(),
        });
    }

    // `FLUSH TABLES t1, t2 [WITH READ LOCK | FOR EXPORT]` — the table-list
    // forms. The bare `FLUSH TABLES` and `FLUSH TABLES WITH READ LOCK` are
    // already caught by the exact-match block above.
    if upper.starts_with("FLUSH TABLES ") {
        let rest = normalized["FLUSH TABLES ".len()..].trim();
        let upper_rest = rest.to_ascii_uppercase();
        let (table_part, with_read_lock, for_export) =
            if let Some(prefix) = upper_rest.strip_suffix(" WITH READ LOCK") {
                (rest[..prefix.len()].trim().to_string(), true, false)
            } else if let Some(prefix) = upper_rest.strip_suffix(" FOR EXPORT") {
                (rest[..prefix.len()].trim().to_string(), false, true)
            } else {
                (rest.to_string(), false, false)
            };
        let Some(tables) = parse_admin_object_name_list(&table_part) else {
            return Some(unsupported_admin(sql, "malformed FLUSH TABLES table list"));
        };
        return Some(Statement::FlushTables {
            tables,
            with_read_lock,
            for_export,
        });
    }

    if upper.starts_with("KILL ") {
        let mut parts = normalized.split_whitespace();
        let _ = parts.next();
        let first = parts.next()?;
        let (target, id_str) = match first.to_ascii_uppercase().as_str() {
            "CONNECTION" => (KillTarget::Connection, parts.next()?),
            "QUERY" => (KillTarget::Query, parts.next()?),
            _ => (KillTarget::Connection, first),
        };
        if parts.next().is_none() {
            if let Ok(connection_id) = id_str.parse::<u64>() {
                return Some(Statement::Kill {
                    target,
                    connection_id,
                });
            }
        }
    }

    if upper.starts_with("ANALYZE TABLE ") {
        let analyze_body = normalized["ANALYZE TABLE ".len()..].trim();
        if let Some(partition_pos) = find_top_level_keyword(analyze_body, "PARTITION") {
            let target = analyze_body[..partition_pos].trim();
            let suffix = analyze_body[partition_pos + "PARTITION".len()..].trim();
            let names = suffix
                .strip_prefix('(')
                .and_then(|rest| rest.strip_suffix(')'))
                .unwrap_or(suffix)
                .trim();
            if let Some(tables) = parse_admin_object_name_list(target)
                .filter(|tables| tables.len() == 1 && !names.is_empty())
            {
                let alter = format!("ALTER TABLE {target} ANALYZE PARTITION {names}");
                if let Ok(Statement::AlterTable(mut stmt)) = parse_statement(&alter) {
                    if stmt.actions.is_empty() && stmt.partition_actions.len() == 1 {
                        if let crate::parser_partition::PartitionAction::AnalyzePartitions(
                            targets,
                        ) = stmt.partition_actions.remove(0)
                        {
                            return Some(Statement::AnalyzeTable {
                                tables,
                                partitions: Some(targets),
                                histogram: None,
                            });
                        }
                    }
                }
                return Some(unsupported_admin(
                    sql,
                    "malformed ANALYZE TABLE PARTITION clause",
                ));
            }
            return Some(unsupported_admin(
                sql,
                "malformed ANALYZE TABLE PARTITION clause",
            ));
        }
        let update = find_top_level_keyword(analyze_body, "UPDATE");
        let drop = find_top_level_keyword(analyze_body, "DROP");
        let histogram_start = match (update, drop) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        if let Some(start) = histogram_start {
            let Some(tables) = parse_admin_object_name_list(analyze_body[..start].trim()) else {
                return Some(unsupported_admin(sql, "malformed ANALYZE TABLE target"));
            };
            if tables.len() != 1 {
                return Some(unsupported_admin(
                    sql,
                    "ANALYZE HISTOGRAM requires exactly one table",
                ));
            }
            let tokens = mysql_tokens(&analyze_body[start..]);
            let refs = tokens.iter().map(String::as_str).collect::<Vec<_>>();
            let Some(action) = parse_histogram_action(&refs) else {
                return Some(unsupported_admin(sql, "malformed ANALYZE HISTOGRAM clause"));
            };
            return Some(Statement::AnalyzeTable {
                tables,
                partitions: None,
                histogram: Some(action),
            });
        }
        if let Some(tables) = parse_admin_object_name_list(analyze_body) {
            return Some(Statement::AnalyzeTable {
                tables,
                partitions: None,
                histogram: None,
            });
        }
        // Anything else after the table name is an unimplemented option:
        // surface a clear error rather than a silent no-op.
        return Some(Statement::Unsupported {
            kind: "ANALYZE TABLE options".to_string(),
            sql: sql.to_string(),
        });
    }

    // OPTIMIZE has its own option grammar: LOCAL/NO_WRITE_TO_BINLOG
    // precede TABLE. CHECK/REPAIR modifiers must not be silently swallowed.
    if let Some(rest) = upper.strip_prefix("OPTIMIZE ") {
        let mut offset = "OPTIMIZE ".len();
        let mut no_write_to_binlog = false;
        for option in ["NO_WRITE_TO_BINLOG ", "LOCAL "] {
            if rest.starts_with(option) {
                offset += option.len();
                no_write_to_binlog = true;
                break;
            }
        }
        if !upper[offset..].starts_with("TABLE ") {
            return Some(unsupported_admin(sql, "malformed OPTIMIZE TABLE"));
        }
        let body = normalized[offset + "TABLE ".len()..].trim();
        let Some(tables) = parse_admin_object_name_list(body) else {
            return Some(unsupported_admin(
                sql,
                "malformed OPTIMIZE TABLE target or unsupported option",
            ));
        };
        return Some(Statement::OptimizeTable {
            tables,
            no_write_to_binlog,
        });
    }

    // P0-C1: CHECK / REPAIR TABLE <name1>, <name2>, ...
    // MySQL accepts a comma-separated table list and optional trailing
    // options (LOCAL / NO_WRITE_TO_BINLOG / EXTENDED / FAST / MEDIUM /
    // QUICK / CHANGED / FOR UPGRADE). We accept the list, swallow the
    // options and route to one row per table.
    if let Some(head) = ["CHECK TABLE ", "REPAIR TABLE "]
        .iter()
        .find(|h| upper.starts_with(*h))
    {
        let rest = normalized[head.len()..].trim();
        let trailing_options = [
            " QUICK",
            " FAST",
            " MEDIUM",
            " EXTENDED",
            " CHANGED",
            " FOR UPGRADE",
            " NO_WRITE_TO_BINLOG",
            " LOCAL",
        ];
        let mut body = rest.to_string();
        loop {
            let upper_body = body.to_ascii_uppercase();
            let mut stripped = false;
            for opt in &trailing_options {
                if let Some(remaining) = upper_body.strip_suffix(opt) {
                    if remaining.trim_end().ends_with(',') || remaining.trim().is_empty() {
                        continue;
                    }
                    body = body[..remaining.len()].trim_end().to_string();
                    stripped = true;
                    break;
                }
            }
            if !stripped {
                break;
            }
        }
        let Some(tables) = parse_admin_object_name_list(&body) else {
            return Some(Statement::Unsupported {
                kind: format!("malformed {} table list", head.trim()),
                sql: sql.to_string(),
            });
        };
        return Some(match *head {
            "CHECK TABLE " => Statement::CheckTable { tables },
            "REPAIR TABLE " => Statement::RepairTable { tables },
            _ => unreachable!(),
        });
    }

    // P1-C3: CHECKSUM TABLE <name1>, <name2>, ... [QUICK | EXTENDED]
    // Differs from CHECK/REPAIR/OPTIMIZE in the trailing-option vocabulary
    // (only QUICK and EXTENDED), so it gets its own fast path.
    if let Some(rest) = upper.strip_prefix("CHECKSUM TABLE ") {
        let _ = rest; // upper-cased view, not used below; we work off the
                      // case-preserving `normalized` slice.
        let body_view = normalized["CHECKSUM TABLE ".len()..].trim();
        let mut body = body_view.to_string();
        let mut quick = false;
        loop {
            let upper_body = body.to_ascii_uppercase();
            let mut stripped = false;
            for opt in [" QUICK", " EXTENDED"] {
                if let Some(remaining) = upper_body.strip_suffix(opt) {
                    if remaining.trim_end().ends_with(',') || remaining.trim().is_empty() {
                        continue;
                    }
                    if opt == " QUICK" {
                        quick = true;
                    }
                    body = body[..remaining.len()].trim_end().to_string();
                    stripped = true;
                    break;
                }
            }
            if !stripped {
                break;
            }
        }
        let Some(tables) = parse_admin_object_name_list(&body) else {
            return Some(Statement::Unsupported {
                kind: "malformed CHECKSUM TABLE table list".to_string(),
                sql: sql.to_string(),
            });
        };
        return Some(Statement::ChecksumTable { tables, quick });
    }

    // P2-B3 — TABLESPACE / UNDO TABLESPACE DDL.
    // mydb accepts the syntax + tracks names in an in-memory catalog
    // so mysqldump output replays cleanly; backing storage is the
    // regular per-table heap so the statement is metadata-only.
    if let Some(rest) = upper.strip_prefix("CREATE TABLESPACE ") {
        let _ = rest;
        let body = normalized["CREATE TABLESPACE ".len()..].trim();
        return Some(parse_tablespace_statement(
            body, sql, /*undo=*/ false, /*create=*/ true,
        ));
    }
    if let Some(_) = upper.strip_prefix("CREATE UNDO TABLESPACE ") {
        let body = normalized["CREATE UNDO TABLESPACE ".len()..].trim();
        return Some(parse_tablespace_statement(
            body, sql, /*undo=*/ true, /*create=*/ true,
        ));
    }
    if let Some(_) = upper.strip_prefix("DROP TABLESPACE ") {
        let body = normalized["DROP TABLESPACE ".len()..].trim();
        return Some(parse_tablespace_statement(
            body, sql, /*undo=*/ false, /*create=*/ false,
        ));
    }
    if let Some(_) = upper.strip_prefix("DROP UNDO TABLESPACE ") {
        let body = normalized["DROP UNDO TABLESPACE ".len()..].trim();
        return Some(parse_tablespace_statement(
            body, sql, /*undo=*/ true, /*create=*/ false,
        ));
    }
    if let Some(_) = upper.strip_prefix("ALTER TABLESPACE ") {
        let body = normalized["ALTER TABLESPACE ".len()..].trim().to_string();
        let tokens = mysql_tokens(&body);
        let name = tokens.first().cloned().unwrap_or_default();
        if name.is_empty() {
            return Some(Statement::Unsupported {
                kind: "ALTER TABLESPACE without name".to_string(),
                sql: sql.to_string(),
            });
        }
        if !validate_tablespace_options(&tokens[1..], true) {
            return Some(unsupported_admin(
                sql,
                "unknown or malformed ALTER TABLESPACE clause",
            ));
        }
        return Some(Statement::AlterTablespace { name, body });
    }
    if let Some(_) = upper.strip_prefix("ALTER UNDO TABLESPACE ") {
        let body = normalized["ALTER UNDO TABLESPACE ".len()..]
            .trim()
            .to_string();
        let tokens = mysql_tokens(&body);
        let name = tokens.first().cloned().unwrap_or_default();
        if name.is_empty() {
            return Some(Statement::Unsupported {
                kind: "ALTER UNDO TABLESPACE without name".to_string(),
                sql: sql.to_string(),
            });
        }
        if !validate_tablespace_options(&tokens[1..], true) {
            return Some(unsupported_admin(
                sql,
                "unknown or malformed ALTER UNDO TABLESPACE clause",
            ));
        }
        return Some(Statement::AlterTablespace { name, body });
    }

    // P2-C1 — RESOURCE GROUP DDL facade.
    if let Some(_) = upper.strip_prefix("CREATE RESOURCE GROUP ") {
        let body = normalized["CREATE RESOURCE GROUP ".len()..].trim();
        return Some(parse_resource_group_create(body, sql));
    }
    if let Some(_) = upper.strip_prefix("DROP RESOURCE GROUP ") {
        let body = normalized["DROP RESOURCE GROUP ".len()..].trim();
        let tokens = mysql_tokens(body);
        let name = tokens.first().cloned().unwrap_or_default();
        if name.is_empty() {
            return Some(Statement::Unsupported {
                kind: "DROP RESOURCE GROUP without name".to_string(),
                sql: sql.to_string(),
            });
        }
        let force = tokens
            .get(1)
            .is_some_and(|token| token.eq_ignore_ascii_case("FORCE"));
        if tokens.len() > if force { 2 } else { 1 } {
            return Some(unsupported_admin(
                sql,
                "unknown DROP RESOURCE GROUP trailing clause",
            ));
        }
        return Some(Statement::DropResourceGroup { name, force });
    }
    if let Some(_) = upper.strip_prefix("ALTER RESOURCE GROUP ") {
        let body = normalized["ALTER RESOURCE GROUP ".len()..]
            .trim()
            .to_string();
        let tokens = mysql_tokens(&body);
        let name = tokens.first().cloned().unwrap_or_default();
        if name.is_empty() {
            return Some(Statement::Unsupported {
                kind: "ALTER RESOURCE GROUP without name".to_string(),
                sql: sql.to_string(),
            });
        }
        if !validate_alter_resource_group_tokens(&tokens[1..]) {
            return Some(unsupported_admin(
                sql,
                "unknown or malformed ALTER RESOURCE GROUP clause",
            ));
        }
        return Some(Statement::AlterResourceGroup { name, body });
    }

    None
}

/// Parse `CREATE RESOURCE GROUP <name> TYPE = {SYSTEM|USER}
/// [VCPU = …] [THREAD_PRIORITY = N] [ENABLE|DISABLE]`. The tail is
/// scanned token-by-token so the order of clauses doesn't matter,
/// matching MySQL.
fn parse_resource_group_create(body: &str, sql: &str) -> Statement {
    let tokens = mysql_tokens(body);
    let name = tokens.first().cloned().unwrap_or_default();
    if name.is_empty() {
        return Statement::Unsupported {
            kind: "CREATE RESOURCE GROUP without name".to_string(),
            sql: sql.to_string(),
        };
    }
    let mut is_system = false;
    let mut type_seen = false;
    let mut vcpu: Option<String> = None;
    let mut thread_priority: Option<i64> = None;
    let mut enabled = true;
    let mut pos = 1usize;
    while pos < tokens.len() {
        let upper = tokens[pos].to_ascii_uppercase();
        pos += 1;
        match upper.as_str() {
            "TYPE" => {
                if tokens.get(pos).map(String::as_str) == Some("=") {
                    pos += 1;
                }
                let Some(value) = tokens.get(pos) else {
                    return unsupported_admin(sql, "CREATE RESOURCE GROUP TYPE requires a value");
                };
                pos += 1;
                if value.eq_ignore_ascii_case("SYSTEM") {
                    is_system = true;
                } else if value.eq_ignore_ascii_case("USER") {
                    is_system = false;
                } else {
                    return unsupported_admin(
                        sql,
                        "CREATE RESOURCE GROUP TYPE must be SYSTEM or USER",
                    );
                }
                type_seen = true;
            }
            "VCPU" => {
                if tokens.get(pos).map(String::as_str) == Some("=") {
                    pos += 1;
                }
                let Some(value) = tokens.get(pos).filter(|value| !value.is_empty()) else {
                    return unsupported_admin(sql, "CREATE RESOURCE GROUP VCPU requires a value");
                };
                pos += 1;
                vcpu = Some(value.clone());
            }
            "THREAD_PRIORITY" => {
                if tokens.get(pos).map(String::as_str) == Some("=") {
                    pos += 1;
                }
                let Some(value) = tokens.get(pos) else {
                    return unsupported_admin(
                        sql,
                        "CREATE RESOURCE GROUP THREAD_PRIORITY requires a value",
                    );
                };
                pos += 1;
                let Ok(value) = value.parse::<i64>() else {
                    return unsupported_admin(
                        sql,
                        "CREATE RESOURCE GROUP THREAD_PRIORITY must be an integer",
                    );
                };
                thread_priority = Some(value);
            }
            "ENABLE" => enabled = true,
            "DISABLE" => enabled = false,
            _ => {
                return unsupported_admin(
                    sql,
                    &format!("unknown CREATE RESOURCE GROUP clause `{}`", tokens[pos - 1]),
                )
            }
        }
    }
    if !type_seen {
        return Statement::Unsupported {
            kind: "CREATE RESOURCE GROUP requires TYPE = {SYSTEM|USER}".to_string(),
            sql: sql.to_string(),
        };
    }
    Statement::CreateResourceGroup {
        name,
        is_system,
        vcpu,
        thread_priority,
        enabled,
    }
}

fn unsupported_admin(sql: &str, kind: &str) -> Statement {
    Statement::Unsupported {
        kind: kind.to_string(),
        sql: sql.to_string(),
    }
}

fn validate_alter_resource_group_tokens(tokens: &[String]) -> bool {
    if tokens.is_empty() {
        return false;
    }
    let mut pos = 0usize;
    while pos < tokens.len() {
        let keyword = tokens[pos].as_str();
        pos += 1;
        if keyword.eq_ignore_ascii_case("ENABLE") || keyword.eq_ignore_ascii_case("DISABLE") {
            continue;
        }
        if !(keyword.eq_ignore_ascii_case("VCPU")
            || keyword.eq_ignore_ascii_case("THREAD_PRIORITY"))
        {
            return false;
        }
        if tokens.get(pos).map(String::as_str) == Some("=") {
            pos += 1;
        }
        let Some(value) = tokens.get(pos) else {
            return false;
        };
        if keyword.eq_ignore_ascii_case("THREAD_PRIORITY") && value.parse::<i64>().is_err() {
            return false;
        }
        pos += 1;
    }
    true
}

/// Parse `[CREATE|DROP] [UNDO] TABLESPACE <name> [ADD DATAFILE 'path']
/// [ENGINE=...] [...]`. Returns the matching Statement variant.
fn parse_tablespace_statement(body: &str, sql: &str, undo: bool, create: bool) -> Statement {
    let trimmed = body.trim().trim_end_matches(';').trim();
    let tokens = mysql_tokens(trimmed);
    let name = tokens.first().cloned().unwrap_or_default();
    if name.is_empty() {
        return Statement::Unsupported {
            kind: format!(
                "{} TABLESPACE without name",
                if create { "CREATE" } else { "DROP" }
            ),
            sql: sql.to_string(),
        };
    }
    if !create {
        if !validate_tablespace_options(&tokens[1..], false) {
            return unsupported_admin(sql, "unknown or malformed DROP TABLESPACE clause");
        }
        return Statement::DropTablespace { name, undo };
    }
    // CREATE: optionally extract ADD DATAFILE '<path>' so the catalog
    // can echo it back. We tolerate (and ignore) the rest of the
    // tail: ENGINE=, FILE_BLOCK_SIZE=, AUTOEXTEND_SIZE=, COMMENT etc.
    if !validate_tablespace_options(&tokens[1..], true) {
        return unsupported_admin(sql, "unknown or malformed CREATE TABLESPACE clause");
    }
    let datafile = tokens[1..].windows(3).find_map(|window| {
        (window[0].eq_ignore_ascii_case("ADD") && window[1].eq_ignore_ascii_case("DATAFILE"))
            .then(|| window[2].clone())
    });
    Statement::CreateTablespace {
        name,
        datafile,
        undo,
    }
}

fn validate_tablespace_options(tokens: &[String], create_or_alter: bool) -> bool {
    let mut pos = 0usize;
    while pos < tokens.len() {
        let keyword = tokens[pos].as_str();
        pos += 1;
        if keyword.eq_ignore_ascii_case("ADD") {
            if !create_or_alter
                || !tokens
                    .get(pos)
                    .is_some_and(|token| token.eq_ignore_ascii_case("DATAFILE"))
            {
                return false;
            }
            pos += 1;
            if tokens.get(pos).is_none() {
                return false;
            }
            pos += 1;
            continue;
        }
        if keyword.eq_ignore_ascii_case("RENAME") {
            if !create_or_alter
                || !tokens
                    .get(pos)
                    .is_some_and(|token| token.eq_ignore_ascii_case("TO"))
                || tokens.get(pos + 1).is_none()
            {
                return false;
            }
            pos += 2;
            continue;
        }
        let known_value_option = [
            "ENGINE",
            "FILE_BLOCK_SIZE",
            "AUTOEXTEND_SIZE",
            "MAX_SIZE",
            "EXTENT_SIZE",
            "INITIAL_SIZE",
            "COMMENT",
            "ENCRYPTION",
            "ENGINE_ATTRIBUTE",
            "SECONDARY_ENGINE_ATTRIBUTE",
        ]
        .iter()
        .any(|candidate| keyword.eq_ignore_ascii_case(candidate));
        if !known_value_option {
            return false;
        }
        if tokens.get(pos).map(String::as_str) == Some("=") {
            pos += 1;
        }
        if tokens.get(pos).is_none() {
            return false;
        }
        pos += 1;
    }
    true
}

/// MySQL database options, including ENCRYPTION (missing in sqlparser 0.61).
/// Keep the head probe bounded so other CREATE statements are not tokenized twice.
pub(super) fn try_parse_create_database(sql: &str) -> Result<Option<Statement>, ParseError> {
    use crate::sql_lex::{SqlTokenCursor, SqlTokenKind};
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let mut cursor = SqlTokenCursor::new(sql, mode, MYSQL_VERSION_ID);
    let mut alter = false;
    for keywords in [&["CREATE", "ALTER"][..], &["DATABASE", "SCHEMA"][..]] {
        let Some(token) = cursor.next().map_err(|err| parse_lex_error(sql, err))? else {
            return Ok(None);
        };
        if cursor.token_text(&token).eq_ignore_ascii_case("ALTER") {
            alter = true;
        }
        if token.kind != SqlTokenKind::Word
            || !keywords
                .iter()
                .any(|kw| cursor.token_text(&token).eq_ignore_ascii_case(kw))
        {
            return Ok(None);
        }
    }
    let dialect = MydbDialect::default();
    let parsed = (|| -> Result<Statement, ParserError> {
        let mut parser = SqlParser::new(&dialect).try_with_sql(sql)?;
        parser.expect_keyword(if alter {
            Keyword::ALTER
        } else {
            Keyword::CREATE
        })?;
        let _ = parser.parse_one_of_keywords(&[Keyword::DATABASE, Keyword::SCHEMA]);
        let if_not_exists =
            !alter && parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let name = if alter
            && matches!(parser.peek_token().token,
            Token::Word(ref word) if matches!(word.keyword, Keyword::DEFAULT | Keyword::CHARACTER | Keyword::CHARSET | Keyword::COLLATE | Keyword::ENCRYPTION | Keyword::READ))
        {
            String::new()
        } else {
            let name = parser.parse_identifier()?.value;
            if name.is_empty() {
                return parser.expected("a nonempty database identifier", parser.peek_token());
            }
            name
        };
        let mut default_charset = None;
        let mut default_collation = None;
        let mut encrypted = false;
        loop {
            let has_default = parser.parse_keyword(Keyword::DEFAULT);
            if parser.parse_keywords(&[Keyword::CHARACTER, Keyword::SET])
                || parser.parse_keyword(Keyword::CHARSET)
            {
                if default_charset.is_some() {
                    return parser.expected("a single CHARACTER SET option", parser.peek_token());
                }
                let _ = parser.consume_token(&Token::Eq);
                default_charset = Some(parser.parse_literal_string()?);
            } else if parser.parse_keyword(Keyword::COLLATE) {
                let _ = parser.consume_token(&Token::Eq);
                default_collation = Some(parser.parse_literal_string()?);
            } else if parser.parse_keyword(Keyword::ENCRYPTION) {
                let _ = parser.consume_token(&Token::Eq);
                let token = parser.next_token();
                let value = match &token.token {
                    Token::SingleQuotedString(value) | Token::DoubleQuotedString(value) => value,
                    _ => return parser.expected("ENCRYPTION 'Y' or 'N'", token),
                };
                if value.eq_ignore_ascii_case("Y") {
                    encrypted = true;
                } else if !value.eq_ignore_ascii_case("N") {
                    return parser.expected("ENCRYPTION 'Y' or 'N'", token);
                }
            } else if has_default {
                return parser.expected(
                    "CHARACTER SET, CHARSET, COLLATE or ENCRYPTION",
                    parser.peek_token(),
                );
            } else {
                break;
            }
        }
        let _ = parser.consume_token(&Token::SemiColon);
        parser.expect_token(&Token::EOF)?;
        // 'N' matches the storage engine's actual unencrypted behavior. Never
        // accept a request for encryption until storage implements it.
        if encrypted {
            return Ok(Statement::Unsupported {
                kind: "DATABASE ENCRYPTION='Y' (at-rest encryption)".into(),
                sql: sql.into(),
            });
        }
        if alter {
            if default_charset.is_none() && default_collation.is_none() {
                return parser.expected("CHARACTER SET or COLLATE", parser.peek_token());
            }
            return Ok(Statement::AlterDatabase {
                name,
                default_charset,
                default_collation,
            });
        }
        Ok(Statement::CreateDatabase {
            name,
            if_not_exists,
            default_charset,
            default_collation,
        })
    })();
    parsed
        .map(Some)
        .map_err(|err| ParseError::from_sqlparser(sql, err))
}

pub(super) fn parse_manual_show_create_database_statement(
    sql: &str,
) -> Result<Option<Statement>, ParseError> {
    let sql = trim_sql_statement(strip_leading_sql_comments(sql));
    let Some(mut idx) = consume_keyword_at(sql, 0, "SHOW") else {
        return Ok(None);
    };
    idx = match consume_keyword_at(sql, idx, "CREATE") {
        Some(idx) => idx,
        None => return Ok(None),
    };
    idx = if let Some(next) = consume_keyword_at(sql, idx, "DATABASE") {
        next
    } else if let Some(next) = consume_keyword_at(sql, idx, "SCHEMA") {
        next
    } else {
        return Ok(None);
    };
    let mut if_not_exists = false;
    if let Some(next) = consume_keywords_at(sql, idx, &["IF", "NOT", "EXISTS"]) {
        if_not_exists = true;
        idx = next;
    }
    let (name, after_name) = parse_routine_object_name(sql, idx)?;
    if !sql[after_name..].trim().is_empty() {
        return Err(parse_error_at_zero(
            "unexpected trailing tokens in SHOW CREATE DATABASE",
        ));
    }
    Ok(Some(Statement::ShowCreateDatabase {
        name,
        if_not_exists,
    }))
}

pub(super) fn parse_manual_show_diagnostics_statement(
    sql: &str,
) -> Result<Option<Statement>, ParseError> {
    let sql = trim_sql_statement(strip_leading_sql_comments(sql));
    let Some(mut idx) = consume_keyword_at(sql, 0, "SHOW") else {
        return Ok(None);
    };
    if let Some(after_count) = consume_count_star_at(sql, idx) {
        idx = after_count;
        let errors_only = if let Some(next) = consume_keyword_at(sql, idx, "WARNINGS") {
            idx = next;
            false
        } else if let Some(next) = consume_keyword_at(sql, idx, "ERRORS") {
            idx = next;
            true
        } else {
            return Ok(None);
        };
        if !sql[idx..].trim().is_empty() {
            return Err(parse_error_at_zero(
                "unexpected trailing tokens in SHOW COUNT diagnostics",
            ));
        }
        return Ok(Some(Statement::ShowCountDiagnostics { errors_only }));
    }

    let errors_only = if let Some(next) = consume_keyword_at(sql, idx, "WARNINGS") {
        idx = next;
        false
    } else if let Some(next) = consume_keyword_at(sql, idx, "ERRORS") {
        idx = next;
        true
    } else {
        return Ok(None);
    };
    let (limit, idx) = parse_optional_show_limit(sql, idx)?;
    if !sql[idx..].trim().is_empty() {
        return Err(parse_error_at_zero(
            "unexpected trailing tokens in SHOW diagnostics",
        ));
    }
    Ok(Some(Statement::ShowDiagnostics { errors_only, limit }))
}

fn consume_count_star_at(sql: &str, idx: usize) -> Option<usize> {
    let mut idx = consume_keyword_at(sql, idx, "COUNT")?;
    idx = skip_ws(sql, idx);
    if !sql[idx..].starts_with('(') {
        return None;
    }
    idx += 1;
    idx = skip_ws(sql, idx);
    if !sql[idx..].starts_with('*') {
        return None;
    }
    idx += 1;
    idx = skip_ws(sql, idx);
    if !sql[idx..].starts_with(')') {
        return None;
    }
    Some(idx + 1)
}

fn parse_optional_show_limit(
    sql: &str,
    idx: usize,
) -> Result<(Option<ShowLimit>, usize), ParseError> {
    let Some(mut idx) = consume_keyword_at(sql, idx, "LIMIT") else {
        return Ok((None, idx));
    };
    let (first, after_first) = parse_usize_literal_at(sql, idx)
        .ok_or_else(|| parse_error_at_zero("SHOW diagnostics LIMIT requires a row count"))?;
    idx = skip_ws(sql, after_first);
    if sql[idx..].starts_with(',') {
        let (row_count, after_row_count) =
            parse_usize_literal_at(sql, idx + 1).ok_or_else(|| {
                parse_error_at_zero("SHOW diagnostics LIMIT offset requires a row count")
            })?;
        return Ok((
            Some(ShowLimit {
                offset: first,
                row_count,
            }),
            after_row_count,
        ));
    }
    Ok((
        Some(ShowLimit {
            offset: 0,
            row_count: first,
        }),
        idx,
    ))
}

fn parse_usize_literal_at(sql: &str, idx: usize) -> Option<(usize, usize)> {
    let idx = skip_ws(sql, idx);
    let start = idx;
    let mut end = idx;
    for ch in sql[idx..].chars() {
        if !ch.is_ascii_digit() {
            break;
        }
        end += ch.len_utf8();
    }
    if end == start {
        return None;
    }
    sql[start..end].parse::<usize>().ok().map(|n| (n, end))
}

pub(super) fn parse_manual_show_index_statement(
    sql: &str,
) -> Result<Option<Statement>, ParseError> {
    let sql = trim_sql_statement(strip_leading_sql_comments(sql));
    let Some(mut idx) = consume_keyword_at(sql, 0, "SHOW") else {
        return Ok(None);
    };
    if let Some(next) = consume_keyword_at(sql, idx, "EXTENDED") {
        idx = next;
    }
    idx = if let Some(next) = consume_keyword_at(sql, idx, "INDEX") {
        next
    } else if let Some(next) = consume_keyword_at(sql, idx, "INDEXES") {
        next
    } else if let Some(next) = consume_keyword_at(sql, idx, "KEYS") {
        next
    } else {
        return Ok(None);
    };
    idx = consume_keyword_at(sql, idx, "FROM")
        .or_else(|| consume_keyword_at(sql, idx, "IN"))
        .ok_or_else(|| parse_error_at_zero("SHOW INDEX requires FROM table"))?;
    let (mut table, after_table) = parse_routine_object_name(sql, idx)?;
    idx = after_table;
    if let Some(next) =
        consume_keyword_at(sql, idx, "FROM").or_else(|| consume_keyword_at(sql, idx, "IN"))
    {
        let (database, after_db) = parse_routine_object_name(sql, next)?;
        if !table.contains('.') {
            table = format!("{database}.{table}");
        }
        idx = after_db;
    }
    let mut filter = None;
    if let Some(next) = consume_keyword_at(sql, idx, "WHERE") {
        let expr_sql = sql[next..].trim();
        if expr_sql.is_empty() {
            return Err(parse_error_at_zero(
                "SHOW INDEX WHERE requires an expression",
            ));
        }
        filter = Some(ShowFilter::Where(parse_expr_fragment(expr_sql)?));
        idx = sql.len();
    }
    if !sql[idx..].trim().is_empty() {
        return Err(parse_error_at_zero(
            "unexpected trailing tokens in SHOW INDEX",
        ));
    }
    Ok(Some(Statement::ShowIndex { table, filter }))
}

pub(super) fn parse_manual_metadata_show_statement(
    sql: &str,
) -> Result<Option<Statement>, ParseError> {
    let sql = trim_sql_statement(strip_leading_sql_comments(sql));
    let Some(mut idx) = consume_keyword_at(sql, 0, "SHOW") else {
        return Ok(None);
    };
    let Some(next) = consume_keyword_at(sql, idx, "TABLE") else {
        return Ok(None);
    };
    idx = next;
    let Some(next) = consume_keyword_at(sql, idx, "STATUS") else {
        return Ok(None);
    };
    idx = next;

    let mut database = None;
    if let Some(next) =
        consume_keyword_at(sql, idx, "FROM").or_else(|| consume_keyword_at(sql, idx, "IN"))
    {
        let (db_name, after_db) = parse_routine_object_name(sql, next)?;
        database = Some(db_name);
        idx = after_db;
    }

    let filter = parse_optional_show_like_or_where(sql, idx, "SHOW TABLE STATUS")?;

    Ok(Some(Statement::ShowTableStatus { database, filter }))
}

pub(super) fn parse_manual_show_local_scope_statement(
    sql: &str,
) -> Result<Option<Statement>, ParseError> {
    // sqlparser only normalises GLOBAL/SESSION on `SHOW {VARIABLES|STATUS}`;
    // MySQL also accepts `LOCAL` as a synonym for `SESSION`. Intercept that
    // shape here so the rest of the pipeline sees a regular SESSION request.
    let sql = trim_sql_statement(strip_leading_sql_comments(sql));
    let Some(idx) = consume_keyword_at(sql, 0, "SHOW") else {
        return Ok(None);
    };
    let Some(idx) = consume_keyword_at(sql, idx, "LOCAL") else {
        return Ok(None);
    };
    if let Some(after) = consume_keyword_at(sql, idx, "VARIABLES") {
        let filter = parse_optional_show_like_or_where(sql, after, "SHOW LOCAL VARIABLES")?;
        return Ok(Some(Statement::ShowVariables {
            scope: ShowScope::Session,
            filter,
        }));
    }
    if let Some(after) = consume_keyword_at(sql, idx, "STATUS") {
        let filter = parse_optional_show_like_or_where(sql, after, "SHOW LOCAL STATUS")?;
        return Ok(Some(Statement::ShowStatus {
            scope: ShowScope::Session,
            filter,
        }));
    }
    Ok(None)
}

fn parse_optional_show_like_or_where(
    sql: &str,
    mut idx: usize,
    label: &'static str,
) -> Result<Option<ShowFilter>, ParseError> {
    let mut filter = None;
    if let Some(next) = consume_keyword_at(sql, idx, "LIKE") {
        let (lit, after) = parse_sql_string_literal_at(sql, next).ok_or_else(|| {
            parse_error_at_zero(&format!("{label} LIKE requires a string literal"))
        })?;
        filter = Some(ShowFilter::Like(lit));
        idx = after;
    } else if let Some(next) = consume_keyword_at(sql, idx, "WHERE") {
        let expr_sql = sql[next..].trim();
        if expr_sql.is_empty() {
            return Err(parse_error_at_zero(&format!(
                "{label} WHERE requires an expression"
            )));
        }
        filter = Some(ShowFilter::Where(parse_expr_fragment(expr_sql)?));
        idx = sql.len();
    }
    if !sql[idx..].trim().is_empty() {
        return Err(parse_error_at_zero(&format!(
            "unexpected trailing tokens in {label}"
        )));
    }
    Ok(filter)
}

pub(super) fn parse_manual_show_open_tables_statement(
    sql: &str,
) -> Result<Option<Statement>, ParseError> {
    let sql = trim_sql_statement(strip_leading_sql_comments(sql));
    let Some(mut idx) = consume_keyword_at(sql, 0, "SHOW") else {
        return Ok(None);
    };
    let Some(next) = consume_keyword_at(sql, idx, "OPEN") else {
        return Ok(None);
    };
    idx = next;
    let Some(next) = consume_keyword_at(sql, idx, "TABLES") else {
        return Ok(None);
    };
    idx = next;

    let mut database = None;
    if let Some(next) =
        consume_keyword_at(sql, idx, "FROM").or_else(|| consume_keyword_at(sql, idx, "IN"))
    {
        let (db_name, after_db) = parse_routine_object_name(sql, next)?;
        database = Some(db_name);
        idx = after_db;
    }

    let filter = parse_optional_show_like_or_where(sql, idx, "SHOW OPEN TABLES")?;

    Ok(Some(Statement::ShowOpenTables { database, filter }))
}

/// `SET PERSIST var=value` / `SET PERSIST_ONLY var=value`. sqlparser 0.62
/// doesn't carry `PERSIST` in its `ContextModifier` enum, so we intercept
/// the keyword here, rewrite the statement as a plain `SET ...`, and let
/// sqlparser parse the assignment list. The resulting AST is post-processed
/// to mark each assignment's scope as Persist / PersistOnly.
pub(super) fn parse_manual_set_persist_statement(
    sql: &str,
) -> Result<Option<Statement>, ParseError> {
    // P3: cheap head gate before the full-body `to_ascii_uppercase()` — a plain
    // SELECT/INSERT must not pay a full-string allocation just to be rejected.
    if !first_keyword_is(sql, b"SET") {
        return Ok(None);
    }
    let body = trim_sql_statement(strip_leading_sql_comments(sql));
    // Need at least `SET PERSIST x=1` worth of bytes; bail without panic.
    if body.len() < "SET PERSIST x".len() {
        return Ok(None);
    }
    let head = body.trim_start();
    let (scope, keyword_end) =
        if let Some(end) = consume_keywords_at(head, 0, &["SET", "PERSIST_ONLY"]) {
            (SetScope::PersistOnly, end)
        } else if let Some(end) = consume_keywords_at(head, 0, &["SET", "PERSIST"]) {
            (SetScope::Persist, end)
        } else {
            return Ok(None);
        };
    let rest = head[keyword_end..].trim_start();
    if rest.is_empty() {
        return Err(parse_error_full_span(
            sql,
            "SET PERSIST requires a variable assignment",
        ));
    }
    let rewritten = format!("SET {rest}");
    let inner = parse_statement(&rewritten)?;
    match inner {
        Statement::Set(mut assignments) => {
            for assign in assignments.iter_mut() {
                if assign.scope.is_some() {
                    return Err(parse_error_full_span(
                        sql,
                        "SET PERSIST cannot be combined with SESSION/LOCAL/GLOBAL/PERSIST scope",
                    ));
                }
                assign.scope = Some(scope);
            }
            Ok(Some(Statement::Set(assignments)))
        }
        // `SET autocommit=…` normally short-circuits to its own AST
        // variant. PERSIST must route the same value through the
        // generic system-variable path so the on-disk file records it
        // by name. Re-synthesise the assignment list.
        Statement::SetAutocommit(value) => Ok(Some(Statement::Set(vec![SetAssignment {
            scope: Some(scope),
            name: "autocommit".to_string(),
            value: Expr::Literal(Literal::Int(if value { 1 } else { 0 })),
        }]))),
        // `SET TRANSACTION ...` cannot be persisted — MySQL rejects
        // PERSIST on transaction characteristics outright.
        Statement::SetTransactionIsolation { .. }
        | Statement::SetTransactionCharacteristics { .. } => Err(parse_error_full_span(
            sql,
            "PERSIST is not allowed on SET TRANSACTION",
        )),
        _ => Err(parse_error_full_span(
            sql,
            "SET PERSIST expects a system variable assignment",
        )),
    }
}

/// `RESET PERSIST [IF EXISTS] [var_name]` — MySQL 8.0.5+.
pub(super) fn parse_manual_reset_persist_statement(
    sql: &str,
) -> Result<Option<Statement>, ParseError> {
    // P3: cheap head gate before the full-body uppercase allocation.
    if !first_keyword_is(sql, b"RESET") {
        return Ok(None);
    }
    let body = trim_sql_statement(strip_leading_sql_comments(sql));
    let upper = body.to_ascii_uppercase();
    let Some(rest) = upper.strip_prefix("RESET PERSIST") else {
        return Ok(None);
    };
    // Resolve back to original-case slice for the variable name.
    let tail = &body["RESET PERSIST".len()..];
    let tail_trim = tail.trim();
    if tail_trim.is_empty() {
        // RESET PERSIST → clear everything.
        let _ = rest;
        return Ok(Some(Statement::ResetPersist {
            var: None,
            if_exists: false,
        }));
    }
    let (if_exists, name_raw) = if let Some(after) = tail_trim
        .strip_prefix("IF EXISTS")
        .or_else(|| tail_trim.strip_prefix("if exists"))
        .or_else(|| tail_trim.strip_prefix("If Exists"))
    {
        (true, after.trim())
    } else {
        // Case-insensitive prefix check.
        let head: String = tail_trim
            .chars()
            .take(9)
            .flat_map(char::to_uppercase)
            .collect();
        if head == "IF EXISTS" {
            (true, tail_trim[9..].trim())
        } else {
            (false, tail_trim)
        }
    };
    if name_raw.is_empty() {
        if if_exists {
            return Err(parse_error_full_span(
                sql,
                "RESET PERSIST IF EXISTS requires a variable name",
            ));
        }
        return Ok(Some(Statement::ResetPersist {
            var: None,
            if_exists: false,
        }));
    }
    let name = name_raw
        .trim_end_matches(';')
        .trim()
        .trim_matches('`')
        .to_string();
    if name.is_empty() {
        return Err(parse_error_full_span(sql, "RESET PERSIST: empty var name"));
    }
    Ok(Some(Statement::ResetPersist {
        var: Some(name),
        if_exists,
    }))
}

/// `SET [SESSION|GLOBAL] TRANSACTION characteristic [, characteristic]*`
/// where each characteristic is one of:
///   * `ISOLATION LEVEL {READ UNCOMMITTED|READ COMMITTED|REPEATABLE READ|SERIALIZABLE}`
///   * `READ ONLY`
///   * `READ WRITE`
///
/// Bare `SET TRANSACTION` (no SESSION/GLOBAL) affects only the *next*
/// transaction in this session (MySQL 8.0 semantics).
/// `SET RESOURCE GROUP grp [FOR tid [, tid]*]`. mydb has no resource-group
/// machinery; the dispatcher emits warning 1287 and returns OK.
pub(super) fn parse_manual_set_resource_group(sql: &str) -> Result<Option<Statement>, ParseError> {
    // P3: cheap head gate before the full-body uppercase allocation.
    if !first_keyword_is(sql, b"SET") {
        return Ok(None);
    }
    let body = trim_sql_statement(strip_leading_sql_comments(sql));
    let upper = body.to_ascii_uppercase();
    let Some(rest_upper) = upper.strip_prefix("SET RESOURCE GROUP ") else {
        return Ok(None);
    };
    let after_kw = body.len() - rest_upper.len();
    let tail = body[after_kw..].trim().trim_end_matches(';').trim();
    if tail.is_empty() {
        return Err(parse_error_full_span(
            sql,
            "SET RESOURCE GROUP requires a group name",
        ));
    }
    let upper_tail = tail.to_ascii_uppercase();
    let (name_raw, threads_part) = if let Some(idx) = upper_tail.find(" FOR ") {
        (tail[..idx].trim(), Some(tail[idx + 5..].trim()))
    } else {
        (tail, None)
    };
    let group = name_raw.trim_matches('`').trim_matches('\'').to_string();
    if group.is_empty() {
        return Err(parse_error_full_span(sql, "SET RESOURCE GROUP: empty name"));
    }
    let mut threads: Vec<u64> = Vec::new();
    if let Some(list) = threads_part {
        for piece in list.split(',') {
            let v = piece.trim().parse::<u64>().map_err(|_| {
                parse_error_full_span(sql, "SET RESOURCE GROUP FOR expects integer thread ids")
            })?;
            threads.push(v);
        }
    }
    Ok(Some(Statement::SetResourceGroup { group, threads }))
}

/// True when `sql` carries no executable statement — only whitespace,
/// statement separators (`;`), and MySQL comments (`-- …`, `# …`, `/* … */`).
///
/// MySQL returns a plain OK packet for such a query, and the `mysql` CLI
/// forwards the bare `--` banner lines that mysqldump emits verbatim to the
/// server (confirmed against MySQL 8.0's general log). mydb must therefore
/// treat a comment-only COM_QUERY as a successful no-op rather than raising a
/// 1064 parse error — otherwise `mysql < dump.sql` aborts on the very first
/// `--` line.
///
/// Version-gated comments (`/*! … */`) and optimizer hints (`/*+ … */`) carry
/// executable content, so a query that begins with one is *not* empty.
pub fn sql_is_effectively_empty(sql: &str) -> bool {
    // After whitespace and separators, any byte that cannot introduce a SQL
    // comment proves this is a real statement.  This keeps the exact lexical
    // path for comment-only/version-comment input while avoiding a complete
    // segment-map allocation for ordinary statements and large bulk INSERTs.
    if let Some(first) = sql
        .as_bytes()
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace() && *byte != b';')
    {
        if !matches!(first, b'-' | b'#' | b'/') {
            return false;
        }
    }
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let Ok(prepared) = crate::sql_lex::PreparedSql::new(sql, mode, MYSQL_VERSION_ID) else {
        // Unterminated comments must reach the parser and produce ER_PARSE_ERROR.
        return false;
    };
    let effective = prepared.effective();
    crate::sql_lex::effective_is_empty(effective, mode, MYSQL_VERSION_ID).unwrap_or(false)
}

fn strip_leading_sql_comments(sql: &str) -> &str {
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    match crate::sql_lex::skip_trivia(sql, 0, mode, MYSQL_VERSION_ID, false) {
        Ok(offset) => &sql[offset..],
        Err(_) => sql.trim_start(),
    }
}

/// Parses the MySQL XA control statements:
///   XA {START|BEGIN} xid [JOIN|RESUME]
///   XA END   xid [SUSPEND [FOR MIGRATE]]
///   XA PREPARE  xid
///   XA COMMIT   xid [ONE PHASE]
///   XA ROLLBACK xid
///   XA RECOVER  [CONVERT XID]
/// xid = gtrid_lit [, bqual_lit [, format_id_num]]
/// Returns `None` if the statement isn't an XA command. Returns
/// `Some(Statement::Unsupported)` if it's clearly XA syntax but a
/// branch mydb doesn't implement (e.g. JOIN/RESUME/SUSPEND).
pub(super) fn parse_xa_statement(sql: &str) -> Option<Statement> {
    // Head guard — running the sqlparser tokenizer on a 1 MB INSERT just
    // to discover it isn't an `XA …` command would dominate the parse
    // cost. The head check exits in O(1) bytes.
    if !first_keyword_is(sql, b"XA") {
        return None;
    }
    use sqlparser::tokenizer::{Token, Tokenizer};

    let dialect = MydbDialect::default();
    let tokens = Tokenizer::new(&dialect, sql).tokenize().ok()?;
    let mut tokens: Vec<Token> = tokens
        .into_iter()
        .filter(|token| !matches!(token, Token::Whitespace(_)))
        .collect();
    if matches!(tokens.last(), Some(Token::SemiColon)) {
        tokens.pop();
    }
    if tokens.is_empty() {
        return None;
    }
    let is_xa = matches!(
        &tokens[0],
        Token::Word(w) if w.value.eq_ignore_ascii_case("XA")
    );
    if !is_xa {
        return None;
    }
    if tokens.len() < 2 {
        return Some(unsupported_xa("XA without verb", sql));
    }
    let verb = match &tokens[1] {
        Token::Word(w) => w.value.to_ascii_uppercase(),
        _ => return Some(unsupported_xa("XA verb missing", sql)),
    };
    let rest = &tokens[2..];

    match verb.as_str() {
        "START" | "BEGIN" => {
            let (xid, tail) = match parse_xa_xid(rest, sql) {
                Ok(value) => value,
                Err(stmt) => return Some(stmt),
            };
            if let Some(tok) = tail.first() {
                if matches!(tok, Token::Word(w) if w.value.eq_ignore_ascii_case("JOIN") || w.value.eq_ignore_ascii_case("RESUME"))
                {
                    return Some(unsupported_xa("XA START JOIN/RESUME", sql));
                }
                return Some(unsupported_xa("unexpected tokens after XA START xid", sql));
            }
            Some(Statement::XaStart { xid })
        }
        "END" => {
            let (xid, tail) = match parse_xa_xid(rest, sql) {
                Ok(value) => value,
                Err(stmt) => return Some(stmt),
            };
            if let Some(tok) = tail.first() {
                if matches!(tok, Token::Word(w) if w.value.eq_ignore_ascii_case("SUSPEND")) {
                    return Some(unsupported_xa("XA END SUSPEND", sql));
                }
                return Some(unsupported_xa("unexpected tokens after XA END xid", sql));
            }
            Some(Statement::XaEnd { xid })
        }
        "PREPARE" => {
            let (xid, tail) = match parse_xa_xid(rest, sql) {
                Ok(value) => value,
                Err(stmt) => return Some(stmt),
            };
            if !tail.is_empty() {
                return Some(unsupported_xa(
                    "unexpected tokens after XA PREPARE xid",
                    sql,
                ));
            }
            Some(Statement::XaPrepare { xid })
        }
        "COMMIT" => {
            let (xid, tail) = match parse_xa_xid(rest, sql) {
                Ok(value) => value,
                Err(stmt) => return Some(stmt),
            };
            let one_phase = match tail {
                [Token::Word(a), Token::Word(b)]
                    if a.value.eq_ignore_ascii_case("ONE")
                        && b.value.eq_ignore_ascii_case("PHASE") =>
                {
                    true
                }
                [] => false,
                _ => return Some(unsupported_xa("unexpected tokens after XA COMMIT xid", sql)),
            };
            Some(Statement::XaCommit { xid, one_phase })
        }
        "ROLLBACK" => {
            let (xid, tail) = match parse_xa_xid(rest, sql) {
                Ok(value) => value,
                Err(stmt) => return Some(stmt),
            };
            if !tail.is_empty() {
                return Some(unsupported_xa(
                    "unexpected tokens after XA ROLLBACK xid",
                    sql,
                ));
            }
            Some(Statement::XaRollback { xid })
        }
        "RECOVER" => match rest {
            [] => Some(Statement::XaRecover { convert_xid: false }),
            [Token::Word(a), Token::Word(b)]
                if a.value.eq_ignore_ascii_case("CONVERT")
                    && b.value.eq_ignore_ascii_case("XID") =>
            {
                Some(Statement::XaRecover { convert_xid: true })
            }
            _ => Some(unsupported_xa("unexpected tokens after XA RECOVER", sql)),
        },
        other => Some(unsupported_xa(&format!("unknown XA verb {other}"), sql)),
    }
}

fn unsupported_xa(kind: &str, sql: &str) -> Statement {
    Statement::Unsupported {
        kind: kind.to_string(),
        sql: sql.to_string(),
    }
}

/// Decode a single `xid = gtrid_lit [, bqual_lit [, format_id_num]]` from
/// `tokens`; returns the parsed Xid and the leftover token slice.
fn parse_xa_xid<'a>(
    tokens: &'a [sqlparser::tokenizer::Token],
    sql: &str,
) -> Result<(Xid, &'a [sqlparser::tokenizer::Token]), Statement> {
    use sqlparser::tokenizer::Token;

    let mut iter = tokens.iter().enumerate();
    let (_, first) = iter
        .next()
        .ok_or_else(|| unsupported_xa("missing gtrid in XA xid", sql))?;
    let gtrid = decode_xid_literal(first)
        .ok_or_else(|| unsupported_xa("invalid gtrid literal in XA xid", sql))?;
    let mut bqual: Vec<u8> = Vec::new();
    let mut format_id: i64 = crate::xa::XID_FORMAT_ID_DEFAULT;
    let mut consumed: usize = 1;
    if let Some((_, Token::Comma)) = iter.next().map(|(i, t)| (i, t)) {
        consumed += 1;
        let (_, second) = iter
            .next()
            .ok_or_else(|| unsupported_xa("missing bqual after comma in XA xid", sql))?;
        bqual = decode_xid_literal(second)
            .ok_or_else(|| unsupported_xa("invalid bqual literal in XA xid", sql))?;
        consumed += 1;
        if let Some((_, Token::Comma)) = iter.next().map(|(i, t)| (i, t)) {
            consumed += 1;
            let (_, third) = iter
                .next()
                .ok_or_else(|| unsupported_xa("missing format_id after comma in XA xid", sql))?;
            format_id = decode_xid_format_id(third)
                .ok_or_else(|| unsupported_xa("invalid format_id literal in XA xid", sql))?;
            consumed += 1;
        }
    }
    // `iter.next()` may have consumed a non-comma token (the start of the
    // trailing keyword tail). Rebuild the slice off `consumed`.
    let xid = Xid::new(format_id, gtrid, bqual)
        .map_err(|err| unsupported_xa(&format!("invalid xid: {}", err.message()), sql))?;
    Ok((xid, &tokens[consumed..]))
}

fn decode_xid_literal(token: &sqlparser::tokenizer::Token) -> Option<Vec<u8>> {
    use sqlparser::tokenizer::Token;
    match token {
        Token::SingleQuotedString(s) | Token::DoubleQuotedString(s) => Some(s.as_bytes().to_vec()),
        Token::NationalStringLiteral(s) => Some(s.as_bytes().to_vec()),
        Token::HexStringLiteral(hex) => decode_hex_bytes(hex),
        _ => None,
    }
}

fn decode_xid_format_id(token: &sqlparser::tokenizer::Token) -> Option<i64> {
    use sqlparser::tokenizer::Token;
    match token {
        Token::Number(value, _) => value.parse::<i64>().ok(),
        Token::Minus => None,
        _ => None,
    }
}

pub(super) fn decode_hex_bytes(hex: &str) -> Option<Vec<u8>> {
    let s = hex.trim();
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2 + 1);
    // MySQL left-pads an odd-length hex literal with an implicit leading
    // zero: `0x123` == `0x0123` == the 2-byte value `0x01 0x23`. Emit the
    // dangling high nibble as its own byte (value 0x0X) before decoding
    // the remaining even-length tail as digit pairs.
    let mut i = if bytes.len() % 2 != 0 {
        out.push(hex_digit(bytes[0])?);
        1
    } else {
        0
    };
    while i < bytes.len() {
        let hi = hex_digit(bytes[i])?;
        let lo = hex_digit(bytes[i + 1])?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Some(out)
}

pub(super) fn decode_base64_bytes(encoded: &str) -> Option<Vec<u8>> {
    use base64::Engine;

    let bytes = encoded.as_bytes();
    if bytes
        .iter()
        .any(|byte| matches!(byte, b' ' | b'\n' | b'\r' | b'\t'))
    {
        let cleaned: Vec<u8> = bytes
            .iter()
            .copied()
            .filter(|byte| !matches!(byte, b' ' | b'\n' | b'\r' | b'\t'))
            .collect();
        base64::engine::general_purpose::STANDARD
            .decode(cleaned)
            .ok()
    } else {
        base64::engine::general_purpose::STANDARD.decode(bytes).ok()
    }
}

use crate::util::hex::digit as hex_digit;

/// Decode `b'1010'` payload (raw `'0'`/`'1'` chars, no `b'` markers) into a
/// big-endian byte sequence. `"1010"` → `[0x0A]`. `""` → `[]`. Pads leading
/// zero bits on the left so the bit count rounds up to a whole byte. Returns
/// `None` if the input contains any non-binary character.
pub(crate) fn decode_bit_string(bits: &str) -> Option<Vec<u8>> {
    let s = bits.trim();
    for c in s.chars() {
        if c != '0' && c != '1' {
            return None;
        }
    }
    if s.is_empty() {
        return Some(Vec::new());
    }
    let total_bits = s.len();
    let n_bytes = total_bits.div_ceil(8);
    let leading_zeros = n_bytes * 8 - total_bits;
    let mut out: Vec<u8> = vec![0; n_bytes];
    for (i, c) in s.bytes().enumerate() {
        if c == b'1' {
            let global_bit = leading_zeros + i;
            let byte_idx = global_bit / 8;
            let bit_idx = 7 - (global_bit % 8);
            out[byte_idx] |= 1 << bit_idx;
        }
    }
    Some(out)
}

/// Parse the tail of `START TRANSACTION`/`BEGIN` into a `Statement::Begin`.
/// `tail` is the already-uppercased, single-space-normalised remainder after
/// the leading keyword. Returns `None` when the tail is not a valid
/// characteristic list (unknown token / mutually exclusive access modes) so
/// the caller can surface an error instead of silently accepting.
pub(super) fn parse_manual_update_order_limit(sql: &str) -> Result<Option<Statement>, ParseError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    // P1-6 — accept a leading `WITH ... AS (...) UPDATE ...` prologue.  The
    // trailing ORDER BY / LIMIT belongs to the UPDATE body, so scanning for
    // them at depth 0 across the whole statement still finds the right
    // positions once the CTE prologue is treated as a nested paren scope.
    if find_dml_verb_after_optional_cte(trimmed, "UPDATE").is_none() {
        return Ok(None);
    }
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let order_range = crate::sql_lex::StructuralCursor::new(trimmed, mode, MYSQL_VERSION_ID)
        .find_top_level_keyword(b"ORDER BY")
        .map_err(|err| parse_lex_error(sql, err))?;
    let order_pos = order_range.as_ref().map(|range| range.start);
    let limit_pos = find_top_level_keyword(trimmed, "LIMIT");

    let (base_end, order_end) = match (order_pos, limit_pos) {
        (Some(o), Some(l)) if o < l => (o, l),
        (Some(o), None) => (o, trimmed.len()),
        _ => return Ok(None),
    };

    let base_sql = trimmed[..base_end].trim();
    let order_sql = trimmed[order_range.expect("ORDER BY range").end..order_end].trim();
    if base_sql.is_empty() || order_sql.is_empty() {
        return Ok(None);
    }
    let limit_sql = limit_pos.map(|lp| trimmed[lp + "LIMIT".len()..].trim());
    if let Some(limit_sql) = limit_sql {
        if limit_sql.is_empty() {
            return Ok(None);
        }
    }

    let mut statement = parse_statement(base_sql)?;
    let mut next_parameter = 0;
    visit::number_statement_placeholders(&mut statement, &mut next_parameter);
    // Reach through the optional Statement::WithCte { body: Update, .. }
    // wrapper so the manual ORDER BY / LIMIT still lands on the Update body.
    let update = match &mut statement {
        Statement::Update(u) => u,
        Statement::WithCte(with) => match with.body.as_mut() {
            Statement::Update(u) => u,
            _ => return Ok(None),
        },
        _ => return Ok(None),
    };
    update.order_by = parse_order_by_tail(order_sql)?;
    let mut parameters = visit::ParameterRebaser::new(next_parameter);
    for order in &mut update.order_by {
        visit::visit_expr_mut(&mut parameters, &mut order.expr);
    }
    next_parameter = parameters.next_index();
    if let Some(limit_sql) = limit_sql {
        let (mut limit, offset) = parse_limit_tail(limit_sql)?;
        if let Some(limit) = &mut limit {
            let mut parameters = visit::ParameterRebaser::new(next_parameter);
            visit::AstVisitorMut::enter_limit(&mut parameters, limit);
        }
        if offset.is_some() {
            return Err(parse_error_full_span(
                sql,
                "UPDATE LIMIT offset is not supported",
            ));
        }
        update.limit = limit;
    }
    Ok(Some(statement))
}

// Stored routine, trigger and event grammar.
pub(super) fn parse_manual_routine_statement(sql: &str) -> Result<Option<Statement>, ParseError> {
    let trimmed = trim_sql_statement(sql);
    if trimmed.is_empty() {
        return Ok(None);
    }
    if starts_with_keyword(trimmed, "DELIMITER") {
        return Err(parse_error_with_spec_at_zero(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "DELIMITER is a client script directive, not a server SQL statement",
        ));
    }
    if starts_with_keyword(trimmed, "CREATE") && looks_like_create_routine(trimmed) {
        return parse_create_routine_statement(sql, trimmed).map(Some);
    }
    if starts_with_keyword(trimmed, "CREATE") && looks_like_create_trigger(trimmed) {
        return parse_create_trigger_statement(sql, trimmed).map(Some);
    }
    if starts_with_keyword(trimmed, "CREATE") && looks_like_create_event(trimmed) {
        return parse_create_event_statement(sql, trimmed).map(Some);
    }
    if starts_with_keyword(trimmed, "ALTER") {
        if let Some(statement) = parse_alter_event_statement(sql, trimmed)? {
            return Ok(Some(statement));
        }
        return parse_alter_routine_statement(sql, trimmed);
    }
    if starts_with_keyword(trimmed, "DROP") {
        if let Some(statement) = parse_drop_trigger_statement(trimmed)? {
            return Ok(Some(statement));
        }
        if let Some(statement) = parse_drop_event_statement(trimmed)? {
            return Ok(Some(statement));
        }
        return parse_drop_routine_statement(sql, trimmed);
    }
    if starts_with_keyword(trimmed, "CALL") {
        return parse_call_statement(trimmed).map(|call| Some(Statement::Call(call)));
    }
    if starts_with_keyword(trimmed, "SHOW") {
        if let Some(statement) = parse_show_trigger_statement(trimmed)? {
            return Ok(Some(statement));
        }
        if let Some(statement) = parse_show_event_statement(trimmed)? {
            return Ok(Some(statement));
        }
        return parse_show_routine_statement(trimmed);
    }
    Ok(None)
}

fn looks_like_create_routine(sql: &str) -> bool {
    let Some(mut idx) = consume_keyword_at(sql, 0, "CREATE") else {
        return false;
    };
    if let Some(next) = consume_keyword_at(sql, idx, "DEFINER") {
        idx = skip_ws(sql, next);
        if sql[idx..].starts_with('=') {
            idx = skip_ws(sql, idx + 1);
        }
        return find_first_top_level_keyword(sql, idx, &["PROCEDURE", "FUNCTION"]).is_some();
    }
    consume_keyword_at(sql, idx, "PROCEDURE").is_some()
        || consume_keyword_at(sql, idx, "FUNCTION").is_some()
}

fn looks_like_create_trigger(sql: &str) -> bool {
    let Some(mut idx) = consume_keyword_at(sql, 0, "CREATE") else {
        return false;
    };
    if let Some(next) = consume_keyword_at(sql, idx, "DEFINER") {
        idx = skip_ws(sql, next);
        if sql[idx..].starts_with('=') {
            idx = skip_ws(sql, idx + 1);
        }
        return find_first_top_level_keyword(sql, idx, &["TRIGGER"]).is_some();
    }
    if let Some(next) = consume_keywords_at(sql, idx, &["IF", "NOT", "EXISTS"]) {
        idx = next;
    }
    consume_keyword_at(sql, idx, "TRIGGER").is_some()
}

fn looks_like_create_event(sql: &str) -> bool {
    let Some(mut idx) = consume_keyword_at(sql, 0, "CREATE") else {
        return false;
    };
    if let Some(next) = consume_keyword_at(sql, idx, "DEFINER") {
        idx = skip_ws(sql, next);
        if sql[idx..].starts_with('=') {
            idx = skip_ws(sql, idx + 1);
        }
        return find_first_top_level_keyword(sql, idx, &["EVENT"]).is_some();
    }
    if let Some(next) = consume_keywords_at(sql, idx, &["IF", "NOT", "EXISTS"]) {
        idx = next;
    }
    if let Some(next) = consume_keywords_at(sql, idx, &["OR", "REPLACE"]) {
        idx = next;
    }
    consume_keyword_at(sql, idx, "EVENT").is_some()
}

fn trim_sql_statement(sql: &str) -> &str {
    sql.trim().trim_end_matches(';').trim()
}

fn parse_create_routine_statement(_original_sql: &str, sql: &str) -> Result<Statement, ParseError> {
    let mut idx = consume_keyword_at(sql, 0, "CREATE").unwrap_or(0);
    let mut definer = None;
    if let Some(next) = consume_keyword_at(sql, idx, "DEFINER") {
        idx = skip_ws(sql, next);
        if sql[idx..].starts_with('=') {
            idx = skip_ws(sql, idx + 1);
        }
        let kind_pos = find_first_top_level_keyword(sql, idx, &["PROCEDURE", "FUNCTION"])
            .ok_or_else(|| parse_error_at_zero("expected PROCEDURE or FUNCTION after DEFINER"))?;
        let definer_sql = sql[idx..kind_pos].trim();
        definer = Some(parse_definer_account(definer_sql).ok_or_else(|| {
            parse_error_at_zero("expected MySQL definer account before routine kind")
        })?);
        idx = kind_pos;
    }
    let (kind, next) = if let Some(next) = consume_keyword_at(sql, idx, "PROCEDURE") {
        (RoutineKind::Procedure, next)
    } else if let Some(next) = consume_keyword_at(sql, idx, "FUNCTION") {
        (RoutineKind::Function, next)
    } else {
        return Err(parse_error_at_zero(
            "expected CREATE PROCEDURE or CREATE FUNCTION",
        ));
    };
    idx = next;
    let if_not_exists = if let Some(next) = consume_keywords_at(sql, idx, &["IF", "NOT", "EXISTS"])
    {
        idx = next;
        true
    } else {
        false
    };
    let (name, next) = parse_routine_object_name(sql, idx)?;
    idx = skip_ws(sql, next);
    let (param_sql, next) = consume_parenthesized(sql, idx)
        .ok_or_else(|| parse_error_at_zero("expected routine parameter list"))?;
    idx = next;
    let params = parse_routine_params(&param_sql, kind)?;
    let mut return_type = None;
    let mut return_unsigned = false;
    let mut return_type_sql = None;
    if kind == RoutineKind::Function {
        idx = consume_keyword_at(sql, idx, "RETURNS")
            .ok_or_else(|| parse_error_at_zero("CREATE FUNCTION requires RETURNS"))?;
        let body_or_char_pos = find_first_top_level_keyword(
            sql,
            idx,
            &[
                "LANGUAGE",
                "DETERMINISTIC",
                "NOT",
                "CONTAINS",
                "NO",
                "READS",
                "MODIFIES",
                "SQL",
                "COMMENT",
                "BEGIN",
                "RETURN",
                "IF",
                "CASE",
                "LOOP",
                "WHILE",
                "REPEAT",
                "SET",
                "SELECT",
                "CALL",
                "SIGNAL",
                "RESIGNAL",
                "PREPARE",
                "EXECUTE",
                "DEALLOCATE",
            ],
        )
        .ok_or_else(|| parse_error_at_zero("CREATE FUNCTION requires a body"))?;
        let type_sql = sql[idx..body_or_char_pos].trim();
        let (dt, unsigned) = parse_routine_data_type(type_sql)?;
        return_type = Some(dt);
        return_unsigned = unsigned;
        return_type_sql = Some(type_sql.to_string());
        idx = body_or_char_pos;
    }
    let (characteristics, body_sql) = parse_routine_characteristics_and_body(sql, idx)?;
    let body = parse_routine_body_sql(body_sql)?;
    Ok(Statement::CreateRoutine(RoutineDef {
        kind,
        name,
        if_not_exists,
        definer,
        params,
        return_type,
        return_unsigned,
        return_type_sql,
        characteristics,
        body,
    }))
}

fn parse_create_trigger_statement(original_sql: &str, sql: &str) -> Result<Statement, ParseError> {
    let mut idx = consume_keyword_at(sql, 0, "CREATE").unwrap_or(0);
    let mut definer = None;
    if let Some(next) = consume_keyword_at(sql, idx, "DEFINER") {
        idx = skip_ws(sql, next);
        if sql[idx..].starts_with('=') {
            idx = skip_ws(sql, idx + 1);
        }
        let trigger_pos = find_first_top_level_keyword(sql, idx, &["TRIGGER"])
            .ok_or_else(|| parse_error_at_zero("expected TRIGGER after DEFINER"))?;
        let definer_sql = sql[idx..trigger_pos].trim();
        definer =
            Some(parse_definer_account(definer_sql).ok_or_else(|| {
                parse_error_at_zero("expected MySQL definer account before TRIGGER")
            })?);
        idx = trigger_pos;
    }
    idx = consume_keyword_at(sql, idx, "TRIGGER")
        .ok_or_else(|| parse_error_at_zero("expected CREATE TRIGGER"))?;
    let mut if_not_exists = false;
    if let Some(next) = consume_keywords_at(sql, idx, &["IF", "NOT", "EXISTS"]) {
        if_not_exists = true;
        idx = next;
    }
    let (name, next) = parse_routine_object_name(sql, idx)?;
    idx = next;
    let (timing, next) = if let Some(next) = consume_keyword_at(sql, idx, "BEFORE") {
        (TriggerTiming::Before, next)
    } else if let Some(next) = consume_keyword_at(sql, idx, "AFTER") {
        (TriggerTiming::After, next)
    } else {
        return Err(parse_error_at_zero(
            "CREATE TRIGGER requires BEFORE or AFTER",
        ));
    };
    idx = next;
    let (event, next) = if let Some(next) = consume_keyword_at(sql, idx, "INSERT") {
        (TriggerEvent::Insert, next)
    } else if let Some(next) = consume_keyword_at(sql, idx, "UPDATE") {
        (TriggerEvent::Update, next)
    } else if let Some(next) = consume_keyword_at(sql, idx, "DELETE") {
        (TriggerEvent::Delete, next)
    } else {
        return Err(parse_error_at_zero(
            "CREATE TRIGGER requires INSERT, UPDATE, or DELETE",
        ));
    };
    idx = next;
    idx = consume_keyword_at(sql, idx, "ON")
        .ok_or_else(|| parse_error_at_zero("CREATE TRIGGER requires ON table"))?;
    let (table, next) = parse_routine_object_name(sql, idx)?;
    idx = consume_keywords_at(sql, next, &["FOR", "EACH", "ROW"])
        .ok_or_else(|| parse_error_at_zero("CREATE TRIGGER requires FOR EACH ROW"))?;
    let order = if let Some(next) = consume_keyword_at(sql, idx, "FOLLOWS") {
        let (other_trigger, after_name) = parse_routine_object_name(sql, next)?;
        idx = after_name;
        Some(TriggerOrder {
            kind: TriggerOrderKind::Follows,
            other_trigger,
        })
    } else if let Some(next) = consume_keyword_at(sql, idx, "PRECEDES") {
        let (other_trigger, after_name) = parse_routine_object_name(sql, next)?;
        idx = after_name;
        Some(TriggerOrder {
            kind: TriggerOrderKind::Precedes,
            other_trigger,
        })
    } else {
        None
    };
    let body_sql = sql[idx..].trim();
    let body = parse_routine_body_sql(body_sql)?;
    Ok(Statement::CreateTrigger(TriggerDef {
        name,
        definer,
        timing,
        event,
        table,
        order,
        body,
        create_sql: trim_sql_statement(original_sql).to_string(),
        if_not_exists,
    }))
}

fn parse_alter_routine_statement(
    original_sql: &str,
    sql: &str,
) -> Result<Option<Statement>, ParseError> {
    let mut idx = match consume_keyword_at(sql, 0, "ALTER") {
        Some(idx) => idx,
        None => return Ok(None),
    };
    let (kind, next) = if let Some(next) = consume_keyword_at(sql, idx, "PROCEDURE") {
        (RoutineKind::Procedure, next)
    } else if let Some(next) = consume_keyword_at(sql, idx, "FUNCTION") {
        (RoutineKind::Function, next)
    } else {
        return Ok(None);
    };
    idx = next;
    let (name, next) = parse_routine_object_name(sql, idx)?;
    let (characteristics, body) = parse_routine_characteristics_patch(sql, next)?;
    if !body.trim().is_empty() {
        return Err(ParseError::unsupported_at(
            original_sql,
            Span {
                start: next,
                end: original_sql.len(),
            },
            "ALTER PROCEDURE/FUNCTION only supports routine characteristics",
        ));
    }
    Ok(Some(Statement::AlterRoutine {
        kind,
        name,
        characteristics,
    }))
}

fn parse_routine_characteristics_patch(
    sql: &str,
    mut idx: usize,
) -> Result<(RoutineCharacteristicsPatch, &str), ParseError> {
    let mut patch = RoutineCharacteristicsPatch::default();
    loop {
        idx = skip_ws(sql, idx);
        if idx >= sql.len() {
            return Ok((patch, ""));
        }
        if let Some(next) = consume_keywords_at(sql, idx, &["LANGUAGE", "SQL"]) {
            idx = next;
        } else if consume_keyword_at(sql, idx, "LANGUAGE").is_some() {
            return Err(parse_error_at_zero("only LANGUAGE SQL is supported"));
        } else if let Some(next) = consume_keywords_at(sql, idx, &["NOT", "DETERMINISTIC"]) {
            patch.deterministic = Some(false);
            idx = next;
        } else if let Some(next) = consume_keyword_at(sql, idx, "DETERMINISTIC") {
            patch.deterministic = Some(true);
            idx = next;
        } else if let Some(next) = consume_keywords_at(sql, idx, &["CONTAINS", "SQL"]) {
            patch.sql_data_access = Some(RoutineSqlDataAccess::ContainsSql);
            idx = next;
        } else if let Some(next) = consume_keywords_at(sql, idx, &["NO", "SQL"]) {
            patch.sql_data_access = Some(RoutineSqlDataAccess::NoSql);
            idx = next;
        } else if let Some(next) = consume_keywords_at(sql, idx, &["READS", "SQL", "DATA"]) {
            patch.sql_data_access = Some(RoutineSqlDataAccess::ReadsSqlData);
            idx = next;
        } else if let Some(next) = consume_keywords_at(sql, idx, &["MODIFIES", "SQL", "DATA"]) {
            patch.sql_data_access = Some(RoutineSqlDataAccess::ModifiesSqlData);
            idx = next;
        } else if let Some(next) = consume_keywords_at(sql, idx, &["SQL", "SECURITY", "DEFINER"]) {
            patch.security = Some(RoutineSecurity::Definer);
            idx = next;
        } else if let Some(next) = consume_keywords_at(sql, idx, &["SQL", "SECURITY", "INVOKER"]) {
            patch.security = Some(RoutineSecurity::Invoker);
            idx = next;
        } else if let Some(next) = consume_keyword_at(sql, idx, "COMMENT") {
            let (comment, after) = parse_sql_string_literal_at(sql, next)
                .ok_or_else(|| parse_error_at_zero("routine COMMENT requires a string literal"))?;
            patch.comment = Some(comment);
            idx = after;
        } else {
            break;
        }
    }
    Ok((patch, sql[idx..].trim()))
}

fn parse_drop_routine_statement(
    _original_sql: &str,
    sql: &str,
) -> Result<Option<Statement>, ParseError> {
    let mut idx = match consume_keyword_at(sql, 0, "DROP") {
        Some(idx) => idx,
        None => return Ok(None),
    };
    let (kind, next) = if let Some(next) = consume_keyword_at(sql, idx, "PROCEDURE") {
        (RoutineKind::Procedure, next)
    } else if let Some(next) = consume_keyword_at(sql, idx, "FUNCTION") {
        (RoutineKind::Function, next)
    } else {
        return Ok(None);
    };
    idx = next;
    let mut if_exists = false;
    if let Some(next) = consume_keywords_at(sql, idx, &["IF", "EXISTS"]) {
        if_exists = true;
        idx = next;
    }
    let (name, after_name) = parse_routine_object_name(sql, idx)?;
    if !sql[after_name..].trim().is_empty() {
        return Err(parse_error_at_zero(
            "unexpected trailing tokens in DROP PROCEDURE/FUNCTION",
        ));
    }
    Ok(Some(Statement::DropRoutine {
        kind,
        name,
        if_exists,
    }))
}

fn parse_drop_trigger_statement(sql: &str) -> Result<Option<Statement>, ParseError> {
    let mut idx = match consume_keyword_at(sql, 0, "DROP") {
        Some(idx) => idx,
        None => return Ok(None),
    };
    let Some(next) = consume_keyword_at(sql, idx, "TRIGGER") else {
        return Ok(None);
    };
    idx = next;
    let mut if_exists = false;
    if let Some(next) = consume_keywords_at(sql, idx, &["IF", "EXISTS"]) {
        if_exists = true;
        idx = next;
    }
    let (name, end) = parse_routine_object_name(sql, idx)?;
    require_statement_end(sql, end)?;
    Ok(Some(Statement::DropTrigger { name, if_exists }))
}

fn parse_show_routine_statement(sql: &str) -> Result<Option<Statement>, ParseError> {
    let mut idx = match consume_keyword_at(sql, 0, "SHOW") {
        Some(idx) => idx,
        None => return Ok(None),
    };
    if let Some(next) = consume_keyword_at(sql, idx, "CREATE") {
        idx = next;
        let (kind, next) = if let Some(next) = consume_keyword_at(sql, idx, "PROCEDURE") {
            (RoutineKind::Procedure, next)
        } else if let Some(next) = consume_keyword_at(sql, idx, "FUNCTION") {
            (RoutineKind::Function, next)
        } else {
            return Ok(None);
        };
        let (name, _) = parse_routine_object_name(sql, next)?;
        return Ok(Some(Statement::ShowCreateRoutine { kind, name }));
    }
    let (kind, next) = if let Some(next) = consume_keyword_at(sql, idx, "PROCEDURE") {
        (RoutineKind::Procedure, next)
    } else if let Some(next) = consume_keyword_at(sql, idx, "FUNCTION") {
        (RoutineKind::Function, next)
    } else {
        return Ok(None);
    };
    idx = next;
    if consume_keyword_at(sql, idx, "STATUS").is_none() {
        return Ok(None);
    }
    idx = consume_keyword_at(sql, idx, "STATUS").unwrap();
    let mut filter = None;
    if let Some(next) = consume_keyword_at(sql, idx, "LIKE") {
        let (lit, after) = parse_sql_string_literal_at(sql, next)
            .ok_or_else(|| parse_error_at_zero("SHOW ROUTINE STATUS LIKE requires a string"))?;
        filter = Some(ShowFilter::Like(lit));
        idx = after;
    } else if let Some(next) = consume_keyword_at(sql, idx, "WHERE") {
        let expr_sql = sql[next..].trim();
        if expr_sql.is_empty() {
            return Err(parse_error_at_zero(
                "SHOW ROUTINE STATUS WHERE requires an expression",
            ));
        }
        filter = Some(ShowFilter::Where(parse_expr_fragment(expr_sql)?));
        idx = sql.len();
    }
    if !sql[idx..].trim().is_empty() {
        return Err(parse_error_at_zero(
            "unexpected trailing tokens in SHOW ROUTINE STATUS",
        ));
    }
    Ok(Some(Statement::ShowRoutineStatus { kind, filter }))
}

fn parse_show_trigger_statement(sql: &str) -> Result<Option<Statement>, ParseError> {
    let mut idx = match consume_keyword_at(sql, 0, "SHOW") {
        Some(idx) => idx,
        None => return Ok(None),
    };
    if let Some(next) = consume_keyword_at(sql, idx, "CREATE") {
        idx = next;
        let Some(next) = consume_keyword_at(sql, idx, "TRIGGER") else {
            return Ok(None);
        };
        let (name, _) = parse_routine_object_name(sql, next)?;
        return Ok(Some(Statement::ShowCreateTrigger { name }));
    }
    let Some(next) = consume_keyword_at(sql, idx, "TRIGGERS") else {
        return Ok(None);
    };
    idx = next;
    let mut database = None;
    if let Some(next) =
        consume_keyword_at(sql, idx, "FROM").or_else(|| consume_keyword_at(sql, idx, "IN"))
    {
        let (db_name, after_db) = parse_routine_object_name(sql, next)?;
        database = Some(db_name);
        idx = after_db;
    }
    let mut filter = None;
    if let Some(next) = consume_keyword_at(sql, idx, "LIKE") {
        let (lit, after) = parse_sql_string_literal_at(sql, next)
            .ok_or_else(|| parse_error_at_zero("SHOW TRIGGERS LIKE requires a string"))?;
        filter = Some(ShowFilter::Like(lit));
        idx = after;
    } else if let Some(next) = consume_keyword_at(sql, idx, "WHERE") {
        let expr_sql = sql[next..].trim();
        if expr_sql.is_empty() {
            return Err(parse_error_at_zero(
                "SHOW TRIGGERS WHERE requires an expression",
            ));
        }
        filter = Some(ShowFilter::Where(parse_expr_fragment(expr_sql)?));
        idx = sql.len();
    }
    if !sql[idx..].trim().is_empty() {
        return Err(parse_error_at_zero(
            "unexpected trailing tokens in SHOW TRIGGERS",
        ));
    }
    Ok(Some(Statement::ShowTriggers { database, filter }))
}

fn parse_create_event_statement(original_sql: &str, sql: &str) -> Result<Statement, ParseError> {
    let mut idx = consume_keyword_at(sql, 0, "CREATE").unwrap_or(0);
    let mut definer = None;
    if let Some(next) = consume_keyword_at(sql, idx, "DEFINER") {
        idx = skip_ws(sql, next);
        if sql[idx..].starts_with('=') {
            idx = skip_ws(sql, idx + 1);
        }
        let event_pos = find_first_top_level_keyword(sql, idx, &["EVENT"])
            .ok_or_else(|| parse_error_at_zero("expected EVENT after DEFINER"))?;
        let definer_sql = sql[idx..event_pos].trim();
        definer =
            Some(parse_definer_account(definer_sql).ok_or_else(|| {
                parse_error_at_zero("expected MySQL definer account before EVENT")
            })?);
        idx = event_pos;
    }
    let mut if_not_exists = false;
    let mut or_replace = false;
    if let Some(next) = consume_keywords_at(sql, idx, &["IF", "NOT", "EXISTS"]) {
        if_not_exists = true;
        idx = next;
    }
    if let Some(next) = consume_keywords_at(sql, idx, &["OR", "REPLACE"]) {
        or_replace = true;
        idx = next;
    }
    idx = consume_keyword_at(sql, idx, "EVENT")
        .ok_or_else(|| parse_error_at_zero("expected CREATE EVENT"))?;
    if !if_not_exists {
        if let Some(next) = consume_keywords_at(sql, idx, &["IF", "NOT", "EXISTS"]) {
            if_not_exists = true;
            idx = next;
        }
    }
    let (name, next) = parse_routine_object_name(sql, idx)?;
    idx = next;
    idx = consume_keywords_at(sql, idx, &["ON", "SCHEDULE"])
        .ok_or_else(|| parse_error_at_zero("CREATE EVENT requires ON SCHEDULE"))?;
    let (schedule, next) = parse_event_schedule(sql, idx)?;
    idx = next;
    let mut on_completion = None;
    let mut status = None;
    let mut comment = None;
    loop {
        idx = skip_ws(sql, idx);
        if let Some(next) = consume_keywords_at(sql, idx, &["ON", "COMPLETION", "NOT", "PRESERVE"])
        {
            on_completion = Some(EventOnCompletion::NotPreserve);
            idx = next;
            continue;
        }
        if let Some(next) = consume_keywords_at(sql, idx, &["ON", "COMPLETION", "PRESERVE"]) {
            on_completion = Some(EventOnCompletion::Preserve);
            idx = next;
            continue;
        }
        if let Some(next) = consume_keywords_at(sql, idx, &["DISABLE", "ON", "SLAVE"]) {
            status = Some(EventStatus::DisabledOnSlave);
            idx = next;
            continue;
        }
        if let Some(next) = consume_keyword_at(sql, idx, "DISABLE") {
            status = Some(EventStatus::Disabled);
            idx = next;
            continue;
        }
        if let Some(next) = consume_keyword_at(sql, idx, "ENABLE") {
            status = Some(EventStatus::Enabled);
            idx = next;
            continue;
        }
        if let Some(next) = consume_keyword_at(sql, idx, "COMMENT") {
            let (text, after) = parse_sql_string_literal_at(sql, next)
                .ok_or_else(|| parse_error_at_zero("EVENT COMMENT requires a string literal"))?;
            comment = Some(text);
            idx = after;
            continue;
        }
        break;
    }
    idx = consume_keyword_at(sql, idx, "DO")
        .ok_or_else(|| parse_error_at_zero("CREATE EVENT requires DO"))?;
    let body_sql = sql[idx..].trim();
    if body_sql.is_empty() {
        return Err(parse_error_at_zero("CREATE EVENT DO requires a body"));
    }
    // Validate the body so we surface syntax errors at DDL time.  The
    // executor re-parses it per fire.
    let _ = parse_routine_body_sql(body_sql)?;
    Ok(Statement::CreateEvent(EventDef {
        name,
        definer,
        schedule,
        on_completion,
        status,
        comment,
        body_sql: body_sql.to_string(),
        create_sql: trim_sql_statement(original_sql).to_string(),
        if_not_exists,
        or_replace,
    }))
}

fn parse_alter_event_statement(
    original_sql: &str,
    sql: &str,
) -> Result<Option<Statement>, ParseError> {
    let mut idx = match consume_keyword_at(sql, 0, "ALTER") {
        Some(idx) => idx,
        None => return Ok(None),
    };
    let mut definer = None;
    if let Some(next) = consume_keyword_at(sql, idx, "DEFINER") {
        idx = skip_ws(sql, next);
        if sql[idx..].starts_with('=') {
            idx = skip_ws(sql, idx + 1);
        }
        let event_pos = find_first_top_level_keyword(sql, idx, &["EVENT"]);
        let Some(event_pos) = event_pos else {
            return Ok(None);
        };
        let definer_sql = sql[idx..event_pos].trim();
        definer =
            Some(parse_definer_account(definer_sql).ok_or_else(|| {
                parse_error_at_zero("expected MySQL definer account before EVENT")
            })?);
        idx = event_pos;
    }
    let Some(after_event) = consume_keyword_at(sql, idx, "EVENT") else {
        return Ok(None);
    };
    idx = after_event;
    let (name, next) = parse_routine_object_name(sql, idx)?;
    idx = next;
    let mut schedule = None;
    let mut on_completion = None;
    let mut status = None;
    let mut comment = None;
    let mut rename_to = None;
    let mut body_sql = None;
    loop {
        idx = skip_ws(sql, idx);
        if idx >= sql.len() {
            break;
        }
        if let Some(next) = consume_keywords_at(sql, idx, &["ON", "SCHEDULE"]) {
            let (parsed, after) = parse_event_schedule(sql, next)?;
            schedule = Some(parsed);
            idx = after;
            continue;
        }
        if let Some(next) = consume_keywords_at(sql, idx, &["ON", "COMPLETION", "NOT", "PRESERVE"])
        {
            on_completion = Some(EventOnCompletion::NotPreserve);
            idx = next;
            continue;
        }
        if let Some(next) = consume_keywords_at(sql, idx, &["ON", "COMPLETION", "PRESERVE"]) {
            on_completion = Some(EventOnCompletion::Preserve);
            idx = next;
            continue;
        }
        if let Some(next) = consume_keywords_at(sql, idx, &["RENAME", "TO"]) {
            let (new_name, after) = parse_routine_object_name(sql, next)?;
            rename_to = Some(new_name);
            idx = after;
            continue;
        }
        if let Some(next) = consume_keywords_at(sql, idx, &["DISABLE", "ON", "SLAVE"]) {
            status = Some(EventStatus::DisabledOnSlave);
            idx = next;
            continue;
        }
        if let Some(next) = consume_keyword_at(sql, idx, "DISABLE") {
            status = Some(EventStatus::Disabled);
            idx = next;
            continue;
        }
        if let Some(next) = consume_keyword_at(sql, idx, "ENABLE") {
            status = Some(EventStatus::Enabled);
            idx = next;
            continue;
        }
        if let Some(next) = consume_keyword_at(sql, idx, "COMMENT") {
            let (text, after) = parse_sql_string_literal_at(sql, next)
                .ok_or_else(|| parse_error_at_zero("EVENT COMMENT requires a string literal"))?;
            comment = Some(text);
            idx = after;
            continue;
        }
        if let Some(next) = consume_keyword_at(sql, idx, "DO") {
            let body = sql[next..].trim();
            if body.is_empty() {
                return Err(parse_error_at_zero("ALTER EVENT DO requires a body"));
            }
            let _ = parse_routine_body_sql(body)?;
            body_sql = Some(body.to_string());
            idx = sql.len();
            continue;
        }
        break;
    }
    require_statement_end(sql, idx)?;
    Ok(Some(Statement::AlterEvent(AlterEventStmt {
        name,
        definer,
        schedule,
        on_completion,
        status,
        comment,
        rename_to,
        body_sql,
        create_sql: trim_sql_statement(original_sql).to_string(),
    })))
}

fn parse_drop_event_statement(sql: &str) -> Result<Option<Statement>, ParseError> {
    let Some(after_drop) = consume_keyword_at(sql, 0, "DROP") else {
        return Ok(None);
    };
    let Some(after_event) = consume_keyword_at(sql, after_drop, "EVENT") else {
        return Ok(None);
    };
    let mut idx = after_event;
    let mut if_exists = false;
    if let Some(next) = consume_keywords_at(sql, idx, &["IF", "EXISTS"]) {
        if_exists = true;
        idx = next;
    }
    let (name, end) = parse_routine_object_name(sql, idx)?;
    require_statement_end(sql, end)?;
    Ok(Some(Statement::DropEvent { name, if_exists }))
}

fn parse_show_event_statement(sql: &str) -> Result<Option<Statement>, ParseError> {
    let Some(mut idx) = consume_keyword_at(sql, 0, "SHOW") else {
        return Ok(None);
    };
    if let Some(next) = consume_keyword_at(sql, idx, "CREATE") {
        idx = next;
        let Some(next) = consume_keyword_at(sql, idx, "EVENT") else {
            return Ok(None);
        };
        let (name, _) = parse_routine_object_name(sql, next)?;
        return Ok(Some(Statement::ShowCreateEvent { name }));
    }
    let Some(next) = consume_keyword_at(sql, idx, "EVENTS") else {
        return Ok(None);
    };
    idx = next;
    let mut database = None;
    if let Some(next) =
        consume_keyword_at(sql, idx, "FROM").or_else(|| consume_keyword_at(sql, idx, "IN"))
    {
        let (db_name, after_db) = parse_routine_object_name(sql, next)?;
        database = Some(db_name);
        idx = after_db;
    }
    let mut filter = None;
    if let Some(next) = consume_keyword_at(sql, idx, "LIKE") {
        let (lit, after) = parse_sql_string_literal_at(sql, next)
            .ok_or_else(|| parse_error_at_zero("SHOW EVENTS LIKE requires a string"))?;
        filter = Some(ShowFilter::Like(lit));
        idx = after;
    } else if let Some(next) = consume_keyword_at(sql, idx, "WHERE") {
        let expr_sql = sql[next..].trim();
        if expr_sql.is_empty() {
            return Err(parse_error_at_zero(
                "SHOW EVENTS WHERE requires an expression",
            ));
        }
        filter = Some(ShowFilter::Where(parse_expr_fragment(expr_sql)?));
        idx = sql.len();
    }
    if !sql[idx..].trim().is_empty() {
        return Err(parse_error_at_zero(
            "unexpected trailing tokens in SHOW EVENTS",
        ));
    }
    Ok(Some(Statement::ShowEvents { database, filter }))
}

/// Parse `AT <expr>` or `EVERY n unit [STARTS <expr>] [ENDS <expr>]`.
/// Returns the schedule and the byte offset past the clause so the
/// outer parser can resume scanning the optional ON COMPLETION /
/// status / COMMENT / DO suffix.
fn parse_event_schedule(sql: &str, start: usize) -> Result<(EventSchedule, usize), ParseError> {
    let idx = skip_ws(sql, start);
    if let Some(next) = consume_keyword_at(sql, idx, "AT") {
        let end = find_event_schedule_terminator(sql, next);
        let expr_sql = sql[next..end].trim();
        if expr_sql.is_empty() {
            return Err(parse_error_at_zero("ON SCHEDULE AT requires an expression"));
        }
        // Validate the expression — we re-evaluate at execute time.
        let _ = parse_expr_fragment(expr_sql)?;
        return Ok((
            EventSchedule::At {
                at_expr: expr_sql.to_string(),
            },
            end,
        ));
    }
    if let Some(next) = consume_keyword_at(sql, idx, "EVERY") {
        let mut idx = next;
        let value_end = find_event_interval_value_end(sql, idx);
        let value_sql = sql[idx..value_end].trim();
        if value_sql.is_empty() {
            return Err(parse_error_at_zero("EVERY requires a value"));
        }
        let _ = parse_expr_fragment(value_sql)?;
        idx = value_end;
        idx = skip_ws(sql, idx);
        let unit_start = idx;
        while idx < sql.len() {
            let ch = sql[idx..].chars().next().unwrap();
            if ch.is_ascii_alphabetic() || ch == '_' {
                idx += ch.len_utf8();
            } else {
                break;
            }
        }
        if unit_start == idx {
            return Err(parse_error_at_zero(
                "EVERY requires a unit (SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, YEAR)",
            ));
        }
        let unit = sql[unit_start..idx].to_ascii_uppercase();
        let mut starts_expr = None;
        let mut ends_expr = None;
        loop {
            idx = skip_ws(sql, idx);
            if let Some(next) = consume_keyword_at(sql, idx, "STARTS") {
                let end = find_event_schedule_terminator(sql, next);
                let expr_sql = sql[next..end].trim();
                if expr_sql.is_empty() {
                    return Err(parse_error_at_zero("STARTS requires an expression"));
                }
                let _ = parse_expr_fragment(expr_sql)?;
                starts_expr = Some(expr_sql.to_string());
                idx = end;
                continue;
            }
            if let Some(next) = consume_keyword_at(sql, idx, "ENDS") {
                let end = find_event_schedule_terminator(sql, next);
                let expr_sql = sql[next..end].trim();
                if expr_sql.is_empty() {
                    return Err(parse_error_at_zero("ENDS requires an expression"));
                }
                let _ = parse_expr_fragment(expr_sql)?;
                ends_expr = Some(expr_sql.to_string());
                idx = end;
                continue;
            }
            break;
        }
        return Ok((
            EventSchedule::Every {
                interval_value: value_sql.to_string(),
                interval_unit: unit,
                starts_expr,
                ends_expr,
            },
            idx,
        ));
    }
    Err(parse_error_at_zero(
        "ON SCHEDULE requires AT <ts> or EVERY <n> <unit>",
    ))
}

fn find_event_schedule_terminator(sql: &str, start: usize) -> usize {
    find_first_top_level_keyword(
        sql,
        start,
        &[
            "STARTS", "ENDS", "ON", "COMMENT", "ENABLE", "DISABLE", "DO", "RENAME",
        ],
    )
    .unwrap_or(sql.len())
}

/// EVERY's numeric/interval-literal payload — we stop at the unit
/// keyword.  Recognises `INTERVAL n unit` shorthand as a single
/// fragment when used as a value (rare, but legal in MySQL).
fn find_event_interval_value_end(sql: &str, start: usize) -> usize {
    find_first_top_level_keyword(
        sql,
        start,
        &[
            "SECOND",
            "MINUTE",
            "HOUR",
            "DAY",
            "WEEK",
            "MONTH",
            "QUARTER",
            "YEAR",
            "MICROSECOND",
            "SECOND_MICROSECOND",
            "MINUTE_MICROSECOND",
            "MINUTE_SECOND",
            "HOUR_MICROSECOND",
            "HOUR_SECOND",
            "HOUR_MINUTE",
            "DAY_MICROSECOND",
            "DAY_SECOND",
            "DAY_MINUTE",
            "DAY_HOUR",
            "YEAR_MONTH",
        ],
    )
    .unwrap_or(sql.len())
}

fn parse_call_statement(sql: &str) -> Result<RoutineCall, ParseError> {
    let idx = consume_keyword_at(sql, 0, "CALL").unwrap_or(0);
    let (name, mut idx) = parse_routine_object_name(sql, idx)?;
    idx = skip_ws(sql, idx);
    let args = if idx < sql.len() && sql[idx..].starts_with('(') {
        let (arg_sql, after_args) = consume_parenthesized(sql, idx)
            .ok_or_else(|| parse_error_at_zero("CALL has an unterminated argument list"))?;
        idx = after_args;
        if arg_sql.trim().is_empty() {
            Vec::new()
        } else {
            parse_expr_list_fragment(&arg_sql)?
        }
    } else {
        Vec::new()
    };
    if !sql[idx..].trim().is_empty() {
        return Err(parse_error_at_zero("unexpected trailing tokens in CALL"));
    }
    Ok(RoutineCall { name, args })
}

pub fn parse_routine_body_sql(sql: &str) -> Result<RoutineBody, ParseError> {
    let trimmed = trim_sql_statement(sql);
    if trimmed.is_empty() {
        return Err(parse_error_at_zero("routine body is empty"));
    }
    let stmt = parse_routine_stmt(trimmed)?;
    let statements = match stmt {
        RoutineStmt::Block {
            label: None,
            statements,
        } => statements,
        other => vec![other],
    };
    Ok(RoutineBody {
        sql: trimmed.to_string(),
        statements,
    })
}

fn parse_routine_params(sql: &str, kind: RoutineKind) -> Result<Vec<RoutineParam>, ParseError> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    split_top_level(trimmed, ',')
        .into_iter()
        .map(|part| parse_routine_param(part.trim(), kind))
        .collect()
}

fn parse_routine_param(sql: &str, kind: RoutineKind) -> Result<RoutineParam, ParseError> {
    let tokens = mysql_tokens(sql);
    if tokens.is_empty() {
        return Err(parse_error_at_zero("empty routine parameter"));
    }
    let mut idx = 0;
    let mode = match tokens[idx].to_ascii_uppercase().as_str() {
        "IN" => {
            idx += 1;
            RoutineParamMode::In
        }
        "OUT" => {
            idx += 1;
            RoutineParamMode::Out
        }
        "INOUT" => {
            idx += 1;
            RoutineParamMode::InOut
        }
        _ => RoutineParamMode::In,
    };
    if kind == RoutineKind::Function && mode != RoutineParamMode::In {
        return Err(parse_error_at_zero(
            "stored function parameters cannot be OUT or INOUT",
        ));
    }
    let Some(name) = tokens.get(idx) else {
        return Err(parse_error_at_zero("routine parameter requires a name"));
    };
    idx += 1;
    let lex_mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let mut cursor = crate::sql_lex::SqlTokenCursor::new(sql, lex_mode, MYSQL_VERSION_ID);
    let mut end = 0;
    for _ in 0..idx {
        end = cursor
            .next()
            .map_err(|err| parse_lex_error(sql, err))?
            .ok_or_else(|| parse_error_at_zero("routine parameter requires a name"))?
            .span
            .end;
    }
    let data_type_sql = sql[end..].trim().to_string();
    if data_type_sql.trim().is_empty() {
        return Err(parse_error_at_zero(
            "routine parameter requires a data type",
        ));
    }
    let (data_type, unsigned) = parse_routine_data_type(&data_type_sql)?;
    Ok(RoutineParam {
        name: unquote_identifier(name),
        mode,
        data_type,
        unsigned,
        data_type_sql,
    })
}

fn parse_routine_data_type(sql: &str) -> Result<(DataType, bool), ParseError> {
    let ddl = format!("CREATE TABLE __mydb_routine_type (x {sql})");
    let statement = parse_statement(&ddl)?;
    let Statement::CreateTable(table) = statement else {
        return Err(parse_error_at_zero("expected data type"));
    };
    let Some(column) = table.columns.into_iter().next() else {
        return Err(parse_error_at_zero("expected data type"));
    };
    Ok((column.data_type, column.unsigned))
}

fn parse_routine_characteristics_and_body(
    sql: &str,
    mut idx: usize,
) -> Result<(RoutineCharacteristics, &str), ParseError> {
    let mut characteristics = RoutineCharacteristics::default();
    loop {
        idx = skip_ws(sql, idx);
        if idx >= sql.len() {
            return Ok((characteristics, ""));
        }
        if let Some(next) = consume_keywords_at(sql, idx, &["LANGUAGE", "SQL"]) {
            idx = next;
        } else if consume_keyword_at(sql, idx, "LANGUAGE").is_some() {
            return Err(parse_error_at_zero("only LANGUAGE SQL is supported"));
        } else if let Some(next) = consume_keywords_at(sql, idx, &["NOT", "DETERMINISTIC"]) {
            characteristics.deterministic = false;
            idx = next;
        } else if let Some(next) = consume_keyword_at(sql, idx, "DETERMINISTIC") {
            characteristics.deterministic = true;
            idx = next;
        } else if let Some(next) = consume_keywords_at(sql, idx, &["CONTAINS", "SQL"]) {
            characteristics.sql_data_access = RoutineSqlDataAccess::ContainsSql;
            idx = next;
        } else if let Some(next) = consume_keywords_at(sql, idx, &["NO", "SQL"]) {
            characteristics.sql_data_access = RoutineSqlDataAccess::NoSql;
            idx = next;
        } else if let Some(next) = consume_keywords_at(sql, idx, &["READS", "SQL", "DATA"]) {
            characteristics.sql_data_access = RoutineSqlDataAccess::ReadsSqlData;
            idx = next;
        } else if let Some(next) = consume_keywords_at(sql, idx, &["MODIFIES", "SQL", "DATA"]) {
            characteristics.sql_data_access = RoutineSqlDataAccess::ModifiesSqlData;
            idx = next;
        } else if let Some(next) = consume_keywords_at(sql, idx, &["SQL", "SECURITY", "DEFINER"]) {
            characteristics.security = RoutineSecurity::Definer;
            idx = next;
        } else if let Some(next) = consume_keywords_at(sql, idx, &["SQL", "SECURITY", "INVOKER"]) {
            characteristics.security = RoutineSecurity::Invoker;
            idx = next;
        } else if let Some(next) = consume_keyword_at(sql, idx, "COMMENT") {
            let (comment, after) = parse_sql_string_literal_at(sql, next)
                .ok_or_else(|| parse_error_at_zero("routine COMMENT requires a string literal"))?;
            characteristics.comment = comment;
            idx = after;
        } else {
            break;
        }
    }
    Ok((characteristics, sql[idx..].trim()))
}

#[inline(never)]
fn parse_routine_stmt(sql: &str) -> Result<RoutineStmt, ParseError> {
    let _depth = RecursionGuard::enter(RecursionKind::Routine)?;
    with_parser_stack(|| parse_routine_stmt_inner(sql))?
}

#[inline(never)]
fn parse_routine_stmt_inner(sql: &str) -> Result<RoutineStmt, ParseError> {
    let sql = trim_sql_statement(sql);
    // Statement splitting deliberately returns slices of the original SQL so
    // SHOW CREATE can retain comments.  Classification, however, must begin at
    // the first effective token: a comment between two routine statements is
    // trivia, not part of the following statement's head keyword.
    let sql = &sql[skip_ws(sql, 0)..];
    let (label, body) = split_routine_label(sql);
    if label.as_ref().is_some_and(|name| name.is_empty())
        || (label.is_some()
            && !["BEGIN", "LOOP", "WHILE", "REPEAT"]
                .iter()
                .any(|keyword| starts_with_keyword(body, keyword)))
    {
        return Err(parse_error_at_zero(
            "a nonempty label may only introduce BEGIN, LOOP, WHILE or REPEAT",
        ));
    }
    if starts_with_keyword(body, "BEGIN") {
        let inner = strip_compound_tail(
            consume_keyword_at(body, 0, "BEGIN").unwrap_or(0),
            body,
            "END",
            label.as_deref(),
        )?;
        return Ok(RoutineStmt::Block {
            label,
            statements: parse_routine_stmt_list(inner)?,
        });
    }
    if starts_with_keyword(body, "DECLARE") {
        return parse_routine_declare(body);
    }
    if starts_with_keyword(body, "SET") {
        let after_set = consume_keyword_at(body, 0, "SET").unwrap_or(0);
        let target = body[after_set..].trim_start();
        if target.starts_with("@@")
            || [
                "GLOBAL",
                "SESSION",
                "LOCAL",
                "PERSIST",
                "PERSIST_ONLY",
                "NAMES",
                "CHARACTER",
                "TRANSACTION",
            ]
            .iter()
            .any(|keyword| starts_with_keyword(target, keyword))
        {
            return Ok(RoutineStmt::Sql(body.to_string()));
        }
        return parse_routine_set(body);
    }
    if starts_with_keyword(body, "SELECT") && top_level_keyword_pos(body, "INTO").is_some() {
        return parse_routine_select_into(body);
    }
    if starts_with_keyword(body, "IF") {
        return parse_routine_if(body);
    }
    if starts_with_keyword(body, "CASE") {
        return parse_routine_case(body);
    }
    if starts_with_keyword(body, "LOOP") {
        let inner = strip_compound_tail(
            consume_keyword_at(body, 0, "LOOP").unwrap_or(0),
            body,
            "END LOOP",
            label.as_deref(),
        )?;
        return Ok(RoutineStmt::Loop {
            label,
            statements: parse_routine_stmt_list(inner)?,
        });
    }
    if starts_with_keyword(body, "WHILE") {
        return parse_routine_while(label, body);
    }
    if starts_with_keyword(body, "REPEAT") {
        return parse_routine_repeat(label, body);
    }
    if let Some(next) = consume_keyword_at(body, 0, "LEAVE") {
        return Ok(RoutineStmt::Leave(parse_single_identifier(
            body[next..].trim(),
        )?));
    }
    if let Some(next) = consume_keyword_at(body, 0, "ITERATE") {
        return Ok(RoutineStmt::Iterate(parse_single_identifier(
            body[next..].trim(),
        )?));
    }
    if let Some(next) = consume_keyword_at(body, 0, "RETURN") {
        return Ok(RoutineStmt::Return(parse_expr_fragment(
            body[next..].trim(),
        )?));
    }
    if let Some(next) = consume_keyword_at(body, 0, "OPEN") {
        return Ok(RoutineStmt::Open(parse_single_identifier(
            body[next..].trim(),
        )?));
    }
    if starts_with_keyword(body, "FETCH") {
        return parse_routine_fetch(body);
    }
    if let Some(next) = consume_keyword_at(body, 0, "CLOSE") {
        return Ok(RoutineStmt::Close(parse_single_identifier(
            body[next..].trim(),
        )?));
    }
    if starts_with_keyword(body, "SIGNAL") {
        return parse_routine_signal(body);
    }
    if starts_with_keyword(body, "RESIGNAL") {
        return parse_routine_resignal(body);
    }
    if starts_with_keyword(body, "CALL") {
        return Ok(RoutineStmt::Call(parse_call_statement(body)?));
    }
    if starts_with_keyword(body, "PREPARE") {
        return parse_routine_prepare(body);
    }
    if starts_with_keyword(body, "EXECUTE") {
        return parse_routine_execute(body);
    }
    if starts_with_keyword(body, "DEALLOCATE") {
        return parse_routine_deallocate(body);
    }
    if starts_with_keyword(body, "GET") {
        return parse_routine_get_diagnostics(body);
    }
    Ok(RoutineStmt::Sql(body.to_string()))
}

fn parse_diag_item(name: &str) -> Result<DiagItem, ParseError> {
    match name.trim().to_ascii_uppercase().as_str() {
        "ROW_COUNT" => Ok(DiagItem::RowCount),
        "NUMBER" => Ok(DiagItem::Number),
        "MYSQL_ERRNO" => Ok(DiagItem::MysqlErrno),
        "RETURNED_SQLSTATE" => Ok(DiagItem::ReturnedSqlState),
        "MESSAGE_TEXT" => Ok(DiagItem::MessageText),
        "CLASS_ORIGIN" => Ok(DiagItem::ClassOrigin),
        "SUBCLASS_ORIGIN" => Ok(DiagItem::SubclassOrigin),
        "CONSTRAINT_CATALOG" => Ok(DiagItem::ConstraintCatalog),
        "CONSTRAINT_SCHEMA" => Ok(DiagItem::ConstraintSchema),
        "CONSTRAINT_NAME" => Ok(DiagItem::ConstraintName),
        "CATALOG_NAME" => Ok(DiagItem::CatalogName),
        "SCHEMA_NAME" => Ok(DiagItem::SchemaName),
        "TABLE_NAME" => Ok(DiagItem::TableName),
        "COLUMN_NAME" => Ok(DiagItem::ColumnName),
        "CURSOR_NAME" => Ok(DiagItem::CursorName),
        other => Err(parse_error_at_zero(&format!(
            "unsupported GET DIAGNOSTICS item '{other}'"
        ))),
    }
}

fn parse_diag_items(sql: &str) -> Result<Vec<(RoutineTarget, DiagItem)>, ParseError> {
    split_top_level(sql.trim(), ',')
        .into_iter()
        .map(|part| {
            let (lhs, rhs) = split_assignment(part.trim())
                .ok_or_else(|| parse_error_at_zero("GET DIAGNOSTICS item requires ="))?;
            Ok((parse_routine_target(lhs.trim())?, parse_diag_item(rhs)?))
        })
        .collect()
}

pub(super) fn parse_routine_get_diagnostics(sql: &str) -> Result<RoutineStmt, ParseError> {
    let mut idx = consume_keyword_at(sql, 0, "GET").unwrap_or(0);
    let mut stacked = false;
    if let Some(next) = consume_keyword_at(sql, idx, "CURRENT") {
        idx = next;
    } else if let Some(next) = consume_keyword_at(sql, idx, "STACKED") {
        idx = next;
        stacked = true;
    }
    idx = consume_keyword_at(sql, idx, "DIAGNOSTICS")
        .ok_or_else(|| parse_error_at_zero("GET requires DIAGNOSTICS"))?;
    let rest = sql[idx..].trim();
    if let Some(after_cond) = consume_keyword_at(rest, 0, "CONDITION") {
        // `CONDITION <number> <target> = <item> [, ...]`. Everything up to the
        // first top-level `=` is `<number> <target>`; split off the trailing
        // target token so the remainder parses as a normal item list.
        let tail = rest[after_cond..].trim();
        let eq_pos = tail
            .find('=')
            .ok_or_else(|| parse_error_at_zero("GET DIAGNOSTICS CONDITION requires items"))?;
        let left = tail[..eq_pos].trim_end();
        let target_split = left
            .rfind(|c: char| c.is_whitespace())
            .map(|p| p + 1)
            .ok_or_else(|| {
                parse_error_at_zero("GET DIAGNOSTICS CONDITION requires a condition number")
            })?;
        let cond_number = parse_expr_fragment(left[..target_split].trim())?;
        let items_sql = format!("{}{}", &left[target_split..], &tail[eq_pos..]);
        let items = parse_diag_items(&items_sql)?;
        if items
            .iter()
            .any(|(_, item)| matches!(item, DiagItem::Number | DiagItem::RowCount))
        {
            return Err(parse_error_at_zero(
                "CONDITION requires condition-area diagnostics items",
            ));
        }
        Ok(RoutineStmt::GetDiagnostics {
            stacked,
            condition: Some(cond_number),
            items,
        })
    } else {
        let items = parse_diag_items(rest)?;
        if items.is_empty()
            || items
                .iter()
                .any(|(_, item)| !matches!(item, DiagItem::Number | DiagItem::RowCount))
        {
            return Err(parse_error_at_zero(
                "GET DIAGNOSTICS requires statement-area items",
            ));
        }
        Ok(RoutineStmt::GetDiagnostics {
            stacked,
            condition: None,
            items,
        })
    }
}

fn parse_routine_stmt_list(sql: &str) -> Result<Vec<RoutineStmt>, ParseError> {
    split_routine_statements(sql)
        .into_iter()
        .filter(|part| !sql_is_effectively_empty(part))
        .map(|part| parse_routine_stmt(part.trim()))
        .collect()
}

fn parse_routine_declare(sql: &str) -> Result<RoutineStmt, ParseError> {
    let body = sql[consume_keyword_at(sql, 0, "DECLARE").unwrap_or(0)..].trim();
    if let Some(span) = top_level_phrase_span(body, "CONDITION FOR") {
        let name = unquote_identifier(body[..span.start].trim());
        let condition = parse_routine_condition(body[span.end..].trim())?;
        return Ok(RoutineStmt::DeclareCondition { name, condition });
    }
    if let Some(span) = top_level_phrase_span(body, "CURSOR FOR") {
        let name = unquote_identifier(body[..span.start].trim());
        let select_sql = body[span.end..].trim().to_string();
        return Ok(RoutineStmt::DeclareCursor { name, select_sql });
    }
    if let Some(span) = top_level_phrase_span(body, "HANDLER FOR") {
        let action_sql = body[..span.start].trim().to_ascii_uppercase();
        let action = match action_sql.as_str() {
            "CONTINUE" => RoutineHandlerAction::Continue,
            "EXIT" => RoutineHandlerAction::Exit,
            "UNDO" => RoutineHandlerAction::Undo,
            _ => {
                return Err(parse_error_at_zero(
                    "handler action must be CONTINUE, EXIT or UNDO",
                ))
            }
        };
        let after_for = body[span.end..].trim();
        let mut conditions = Vec::new();
        let mut cursor = 0usize;
        loop {
            let (condition, next) = parse_routine_condition_prefix(after_for, cursor)?;
            conditions.push(condition);
            cursor = skip_ws(after_for, next);
            if after_for.as_bytes().get(cursor) == Some(&b',') {
                cursor += 1;
                continue;
            }
            break;
        }
        let statement_sql = after_for[cursor..].trim();
        if statement_sql.is_empty() {
            return Err(parse_error_at_zero(
                "DECLARE HANDLER requires a handler statement",
            ));
        }
        let statement = parse_routine_stmt(statement_sql)?;
        return Ok(RoutineStmt::DeclareHandler {
            action,
            conditions,
            statement: Box::new(statement),
        });
    }
    let default_pos = top_level_keyword_pos(body, "DEFAULT");
    let (decl_sql, default) = match default_pos {
        Some(pos) => (
            body[..pos].trim(),
            Some(parse_expr_fragment(body[pos + "DEFAULT".len()..].trim())?),
        ),
        None => (body.trim(), None),
    };
    let tokens = mysql_tokens(decl_sql);
    // Parse the name list before considering types: DATE and TIME are also
    // legal variable names, including in a comma-separated declaration.
    let mut type_idx = 1;
    while tokens.get(type_idx).is_some_and(|token| token == ",") {
        type_idx += 2;
    }
    if tokens.is_empty() || type_idx >= tokens.len() {
        return Err(parse_error_at_zero(
            "DECLARE variable requires a name and data type",
        ));
    }
    let names = tokens[..type_idx]
        .iter()
        .step_by(2)
        .map(|token| unquote_identifier(token))
        .collect::<Vec<_>>();
    let data_type_sql = tokens[type_idx..].join(" ");
    let (data_type, unsigned) = parse_routine_data_type(&data_type_sql)?;
    Ok(RoutineStmt::DeclareVar {
        names,
        data_type,
        unsigned,
        data_type_sql,
        default,
    })
}

/// Parse assignment expressions in one batch, preserving parameter order.
/// SET and SIGNAL share splitting and expression conversion, but validate
/// their own targets and error wording.
fn parse_routine_assignments<'a>(
    sql: &'a str,
    expected: &str,
) -> Result<impl Iterator<Item = (&'a str, Expr)>, ParseError> {
    let assignments = split_top_level(sql, ',')
        .into_iter()
        .map(|part| split_assignment(part.trim()).ok_or_else(|| parse_error_at_zero(expected)))
        .collect::<Result<Vec<_>, _>>()?;
    let length = assignments
        .iter()
        .map(|(_, rhs)| rhs.trim().len())
        .sum::<usize>()
        + assignments.len().saturating_sub(1) * 2;
    let mut values_sql = String::with_capacity(length);
    for (index, (_, rhs)) in assignments.iter().enumerate() {
        if index != 0 {
            values_sql.push_str(", ");
        }
        values_sql.push_str(rhs.trim());
    }
    let values = parse_expr_list_fragment(&values_sql)?;
    if values.len() != assignments.len() {
        return Err(parse_error_at_zero("assignment expression count mismatch"));
    }
    Ok(assignments
        .into_iter()
        .zip(values)
        .map(|((lhs, _), value)| (lhs.trim(), value)))
}

fn parse_routine_set(sql: &str) -> Result<RoutineStmt, ParseError> {
    let body = sql[consume_keyword_at(sql, 0, "SET").unwrap_or(0)..].trim();
    let assignments = parse_routine_assignments(body, "SET assignment requires = or :=")?
        .map(|(lhs, value)| Ok((parse_routine_target(lhs)?, value)))
        .collect::<Result<Vec<_>, ParseError>>()?;
    Ok(RoutineStmt::Set { assignments })
}

fn parse_routine_select_into(sql: &str) -> Result<RoutineStmt, ParseError> {
    let into = top_level_keyword_pos(sql, "INTO").unwrap();
    let after_into = into + "INTO".len();
    let target_end = find_first_top_level_keyword(
        sql,
        after_into,
        &[
            "FROM", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "FOR", "LOCK",
        ],
    )
    .unwrap_or(sql.len());
    let targets = split_top_level(sql[after_into..target_end].trim(), ',')
        .into_iter()
        .map(|part| parse_routine_target(part.trim()))
        .collect::<Result<Vec<_>, _>>()?;
    let mut select_sql = String::new();
    select_sql.push_str(sql[..into].trim_end());
    if target_end < sql.len() {
        select_sql.push(' ');
        select_sql.push_str(sql[target_end..].trim_start());
    }
    Ok(RoutineStmt::SelectInto {
        select_sql,
        targets,
    })
}

fn parse_routine_if(sql: &str) -> Result<RoutineStmt, ParseError> {
    let body_start = consume_keyword_at(sql, 0, "IF").unwrap_or(0);
    let body = strip_compound_tail(body_start, sql, "END IF", None)?;
    let then_pos = routine_expression_keyword_pos(body, "THEN")
        .ok_or_else(|| parse_error_at_zero("IF statement requires THEN"))?;
    let first_cond = parse_expr_fragment(body[..then_pos].trim())?;
    let mut branches = Vec::new();
    let mut remainder = body[then_pos + "THEN".len()..].trim();
    loop {
        let marker = find_if_marker(remainder);
        let (stmts_sql, marker_kind, after_marker) = match marker {
            Some((pos, kind, after)) => (&remainder[..pos], Some(kind), after),
            None => (remainder, None, remainder.len()),
        };
        if branches.is_empty() {
            branches.push((first_cond.clone(), parse_routine_stmt_list(stmts_sql)?));
        }
        match marker_kind {
            Some("ELSEIF") => {
                let tail = remainder[after_marker..].trim();
                let then_pos = routine_expression_keyword_pos(tail, "THEN")
                    .ok_or_else(|| parse_error_at_zero("ELSEIF requires THEN"))?;
                let cond = parse_expr_fragment(tail[..then_pos].trim())?;
                let after_then = then_pos + "THEN".len();
                let next_remainder = tail[after_then..].trim();
                let next_marker = find_if_marker(next_remainder);
                let branch_sql = next_marker
                    .map(|(pos, _, _)| &next_remainder[..pos])
                    .unwrap_or(next_remainder);
                branches.push((cond, parse_routine_stmt_list(branch_sql)?));
                if let Some((pos, _, _)) = next_marker {
                    remainder = next_remainder[pos..].trim();
                    continue;
                }
                return Ok(RoutineStmt::If {
                    branches,
                    else_branch: Vec::new(),
                });
            }
            Some("ELSE") => {
                let else_branch = parse_routine_stmt_list(remainder[after_marker..].trim())?;
                return Ok(RoutineStmt::If {
                    branches,
                    else_branch,
                });
            }
            None => {
                return Ok(RoutineStmt::If {
                    branches,
                    else_branch: Vec::new(),
                })
            }
            _ => unreachable!(),
        }
    }
}

fn parse_routine_case(sql: &str) -> Result<RoutineStmt, ParseError> {
    let body_start = consume_keyword_at(sql, 0, "CASE").unwrap_or(0);
    let body = strip_compound_tail(body_start, sql, "END CASE", None)?;
    let first_when = routine_expression_keyword_pos(body, "WHEN")
        .ok_or_else(|| parse_error_at_zero("CASE statement requires WHEN"))?;
    let operand_sql = body[..first_when].trim();
    let operand = if operand_sql.is_empty() {
        None
    } else {
        Some(parse_expr_fragment(operand_sql)?)
    };
    let mut branches = Vec::new();
    let mut else_branch = Vec::new();
    let mut rest = body[first_when..].trim();
    while starts_with_keyword(rest, "WHEN") {
        let after_when = consume_keyword_at(rest, 0, "WHEN").unwrap();
        let then_pos = routine_expression_keyword_pos(rest, "THEN")
            .ok_or_else(|| parse_error_at_zero("CASE WHEN requires THEN"))?;
        let cond = parse_expr_fragment(rest[after_when..then_pos].trim())?;
        let after_then = then_pos + "THEN".len();
        let marker = find_case_marker(&rest[after_then..]);
        let (stmts_sql, next_offset) = match marker {
            Some((pos, _, _)) => (&rest[after_then..after_then + pos], after_then + pos),
            None => (&rest[after_then..], rest.len()),
        };
        branches.push((cond, parse_routine_stmt_list(stmts_sql)?));
        rest = rest[next_offset..].trim();
    }
    if starts_with_keyword(rest, "ELSE") {
        let after_else = consume_keyword_at(rest, 0, "ELSE").unwrap();
        else_branch = parse_routine_stmt_list(rest[after_else..].trim())?;
    }
    Ok(RoutineStmt::Case {
        operand,
        branches,
        else_branch,
    })
}

fn parse_routine_while(label: Option<String>, sql: &str) -> Result<RoutineStmt, ParseError> {
    let after_while = consume_keyword_at(sql, 0, "WHILE").unwrap_or(0);
    let inner = strip_compound_tail(after_while, sql, "END WHILE", label.as_deref())?;
    let do_pos = top_level_keyword_pos(inner, "DO")
        .ok_or_else(|| parse_error_at_zero("WHILE statement requires DO"))?;
    Ok(RoutineStmt::While {
        label,
        condition: parse_expr_fragment(inner[..do_pos].trim())?,
        statements: parse_routine_stmt_list(inner[do_pos + "DO".len()..].trim())?,
    })
}

fn parse_routine_repeat(label: Option<String>, sql: &str) -> Result<RoutineStmt, ParseError> {
    let after_repeat = consume_keyword_at(sql, 0, "REPEAT").unwrap_or(0);
    let inner = strip_compound_tail(after_repeat, sql, "END REPEAT", label.as_deref())?;
    let until_pos = top_level_keyword_pos(inner, "UNTIL")
        .ok_or_else(|| parse_error_at_zero("REPEAT statement requires UNTIL"))?;
    Ok(RoutineStmt::Repeat {
        label,
        statements: parse_routine_stmt_list(inner[..until_pos].trim())?,
        until: parse_expr_fragment(inner[until_pos + "UNTIL".len()..].trim())?,
    })
}

fn parse_routine_fetch(sql: &str) -> Result<RoutineStmt, ParseError> {
    let after_fetch = consume_keyword_at(sql, 0, "FETCH").unwrap_or(0);
    let into = top_level_keyword_pos(sql, "INTO")
        .ok_or_else(|| parse_error_at_zero("FETCH requires INTO"))?;
    let mut cursor_sql = sql[after_fetch..into].trim();
    if let Some(after_next) = consume_keyword_at(cursor_sql, 0, "NEXT") {
        cursor_sql = cursor_sql[after_next..].trim_start();
    }
    if let Some(after_from) = consume_keyword_at(cursor_sql, 0, "FROM") {
        cursor_sql = cursor_sql[after_from..].trim_start();
    }
    if cursor_sql.is_empty() {
        return Err(parse_error_at_zero("FETCH requires a cursor name"));
    }
    let cursor = unquote_identifier(cursor_sql);
    let targets = split_top_level(sql[into + "INTO".len()..].trim(), ',')
        .into_iter()
        .map(|part| parse_routine_target(part.trim()))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RoutineStmt::Fetch { cursor, targets })
}

fn parse_routine_signal(sql: &str) -> Result<RoutineStmt, ParseError> {
    let after_signal = consume_keyword_at(sql, 0, "SIGNAL").unwrap_or(0);
    let (condition, idx) = parse_routine_condition_prefix(sql, after_signal)?;
    if !matches!(
        condition,
        RoutineCondition::SqlState(_) | RoutineCondition::Named(_)
    ) {
        return Err(parse_error_at_zero(
            "SIGNAL requires SQLSTATE or a declared condition name",
        ));
    }
    let (message_text, mysql_errno) = parse_signal_information_items(sql, idx)?;
    Ok(RoutineStmt::Signal {
        condition,
        message_text,
        mysql_errno,
    })
}

fn parse_routine_resignal(sql: &str) -> Result<RoutineStmt, ParseError> {
    let mut idx = consume_keyword_at(sql, 0, "RESIGNAL").unwrap_or(0);
    let condition =
        if skip_ws(sql, idx) < sql.len() && consume_keyword_at(sql, idx, "SET").is_none() {
            let (condition, end) = parse_routine_condition_prefix(sql, idx)?;
            if !matches!(
                condition,
                RoutineCondition::SqlState(_) | RoutineCondition::Named(_)
            ) {
                return Err(parse_error_at_zero(
                    "RESIGNAL requires SQLSTATE or a declared condition name",
                ));
            }
            idx = end;
            Some(condition)
        } else {
            None
        };
    let (message_text, mysql_errno) = parse_signal_information_items(sql, idx)?;
    Ok(RoutineStmt::Resignal {
        condition,
        message_text,
        mysql_errno,
    })
}

fn parse_signal_information_items(
    sql: &str,
    idx: usize,
) -> Result<(Option<Expr>, Option<Expr>), ParseError> {
    let mut message_text = None;
    let mut mysql_errno = None;
    let Some(next) = consume_keyword_at(sql, idx, "SET") else {
        if !sql[idx..].trim().is_empty() {
            return Err(parse_error_at_zero(
                "unexpected SIGNAL condition information",
            ));
        }
        return Ok((message_text, mysql_errno));
    };
    for (lhs, value) in parse_routine_assignments(sql[next..].trim(), "SIGNAL SET item requires =")?
    {
        let name = lhs.trim().to_ascii_uppercase();
        match name.as_str() {
            "MESSAGE_TEXT" => {
                if message_text.replace(value).is_some() {
                    return Err(parse_error_at_zero("duplicate MESSAGE_TEXT item"));
                }
            }
            "MYSQL_ERRNO" => {
                if mysql_errno.replace(value).is_some() {
                    return Err(parse_error_at_zero("duplicate MYSQL_ERRNO item"));
                }
            }
            _ => {
                return Err(parse_error_at_zero(
                    "unsupported SIGNAL condition information item",
                ));
            }
        }
    }
    Ok((message_text, mysql_errno))
}

fn parse_routine_prepare(sql: &str) -> Result<RoutineStmt, ParseError> {
    let after_prepare = consume_keyword_at(sql, 0, "PREPARE").unwrap_or(0);
    let from_pos = top_level_keyword_pos(sql, "FROM")
        .ok_or_else(|| parse_error_at_zero("PREPARE requires FROM"))?;
    Ok(RoutineStmt::Prepare {
        name: unquote_identifier(sql[after_prepare..from_pos].trim()),
        source: parse_expr_fragment(sql[from_pos + "FROM".len()..].trim())?,
    })
}

fn parse_single_identifier(sql: &str) -> Result<String, ParseError> {
    use crate::sql_lex::{SqlTokenCursor, SqlTokenKind};
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let mut cursor = SqlTokenCursor::new(sql, mode, MYSQL_VERSION_ID);
    let token = cursor
        .next()
        .map_err(|err| parse_lex_error(sql, err))?
        .ok_or_else(|| parse_error_at_zero("expected an identifier"))?;
    let name = match token.kind {
        SqlTokenKind::Word => cursor.token_text(&token).to_string(),
        SqlTokenKind::QuotedIdentifier => cursor.decoded_quoted_text(&token).unwrap(),
        _ => return Err(parse_error_at_zero("expected an identifier")),
    };
    if cursor
        .next()
        .map_err(|err| parse_lex_error(sql, err))?
        .is_some()
    {
        return Err(parse_error_at_zero("unexpected tokens after identifier"));
    }
    Ok(name)
}

fn require_statement_end(sql: &str, idx: usize) -> Result<(), ParseError> {
    if sql_is_effectively_empty(&sql[idx..]) {
        Ok(())
    } else {
        Err(parse_error_at_zero("unexpected trailing tokens"))
    }
}

fn parse_routine_execute(sql: &str) -> Result<RoutineStmt, ParseError> {
    let after_execute = consume_keyword_at(sql, 0, "EXECUTE").unwrap_or(0);
    let using_pos = top_level_keyword_pos(sql, "USING");
    let name_end = using_pos.unwrap_or(sql.len());
    let using = using_pos
        .map(|pos| {
            split_top_level(sql[pos + "USING".len()..].trim(), ',')
                .into_iter()
                .map(|part| parse_routine_target(part.trim()))
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();
    Ok(RoutineStmt::Execute {
        name: parse_single_identifier(sql[after_execute..name_end].trim())?,
        using,
    })
}

fn parse_routine_deallocate(sql: &str) -> Result<RoutineStmt, ParseError> {
    let mut idx = consume_keyword_at(sql, 0, "DEALLOCATE").unwrap_or(0);
    if let Some(next) = consume_keyword_at(sql, idx, "PREPARE") {
        idx = next;
    }
    Ok(RoutineStmt::Deallocate {
        name: parse_single_identifier(sql[idx..].trim())?,
    })
}

/// Parse the top-level MySQL SQL prepared-statement surface. sqlparser's
/// PREPARE grammar uses `AS`, so MySQL's `PREPARE name FROM ...` must be
/// intercepted before the generic parser.
pub(super) fn parse_manual_prepared_statement(sql: &str) -> Result<Option<Statement>, ParseError> {
    if starts_with_keyword(sql, "PREPARE") {
        let RoutineStmt::Prepare { name, source } = parse_routine_prepare(sql)? else {
            unreachable!("prepare parser returned another routine statement")
        };
        if name.is_empty() {
            return Err(parse_error_at_zero("PREPARE requires a statement name"));
        }
        return Ok(Some(Statement::Prepare { name, source }));
    }
    if starts_with_keyword(sql, "EXECUTE") {
        let RoutineStmt::Execute { name, using } = parse_routine_execute(sql)? else {
            unreachable!("execute parser returned another routine statement")
        };
        if name.is_empty() {
            return Err(parse_error_at_zero("EXECUTE requires a statement name"));
        }
        let using = using
            .into_iter()
            .map(|target| match target {
                RoutineTarget::UserVariable(name) => Ok(name),
                RoutineTarget::Local(name) => Err(parse_error_at_zero(format!(
                    "EXECUTE USING requires a user variable, found {name}"
                ))),
            })
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(Some(Statement::ExecutePrepared { name, using }));
    }
    if starts_with_keyword(sql, "DEALLOCATE") || starts_with_keyword(sql, "DROP") {
        let is_drop_prepare = consume_keyword_at(sql, 0, "DROP")
            .and_then(|idx| consume_keyword_at(sql, idx, "PREPARE"))
            .is_some();
        if starts_with_keyword(sql, "DROP") && !is_drop_prepare {
            return Ok(None);
        }
        let parsed = if is_drop_prepare {
            let mut idx = consume_keyword_at(sql, 0, "DROP").unwrap();
            idx = consume_keyword_at(sql, idx, "PREPARE").unwrap();
            RoutineStmt::Deallocate {
                name: unquote_identifier(sql[idx..].trim()),
            }
        } else {
            parse_routine_deallocate(sql)?
        };
        let RoutineStmt::Deallocate { name } = parsed else {
            unreachable!("deallocate parser returned another routine statement")
        };
        if name.is_empty() {
            return Err(parse_error_at_zero(
                "DEALLOCATE PREPARE requires a statement name",
            ));
        }
        return Ok(Some(Statement::DeallocatePrepared { name }));
    }
    Ok(None)
}

fn parse_routine_condition(sql: &str) -> Result<RoutineCondition, ParseError> {
    let trimmed = sql.trim();
    let (condition, end) = parse_routine_condition_prefix(trimmed, 0)?;
    if skip_ws(trimmed, end) != trimmed.len() {
        return Err(parse_error_at_zero(
            "invalid trailing handler condition text",
        ));
    }
    Ok(condition)
}

/// Parse one handler condition and return the byte offset immediately after
/// it.  Handler statements are arbitrary stored-program statements, so the
/// boundary must be derived from the condition grammar rather than from a
/// whitelist of possible statement-leading keywords.
fn parse_routine_condition_prefix(
    sql: &str,
    start: usize,
) -> Result<(RoutineCondition, usize), ParseError> {
    let idx = skip_ws(sql, start);
    if let Some(end) = consume_keyword_at(sql, idx, "SQLEXCEPTION") {
        return Ok((RoutineCondition::SqlException, end));
    }
    if let Some(end) = consume_keyword_at(sql, idx, "SQLWARNING") {
        return Ok((RoutineCondition::SqlWarning, end));
    }
    if let Some(after_not) = consume_keyword_at(sql, idx, "NOT") {
        let end = consume_keyword_at(sql, after_not, "FOUND")
            .ok_or_else(|| parse_error_at_zero("NOT handler condition requires FOUND"))?;
        return Ok((RoutineCondition::NotFound, end));
    }
    if let Some(mut next) = consume_keyword_at(sql, idx, "SQLSTATE") {
        if let Some(after_value) = consume_keyword_at(sql, next, "VALUE") {
            next = after_value;
        }
        let (state, end) = parse_sql_string_literal_at(sql, next)
            .ok_or_else(|| parse_error_at_zero("SQLSTATE requires string literal"))?;
        return Ok((RoutineCondition::SqlState(state), end));
    }

    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let mut tokens = crate::sql_lex::SqlTokenCursor::new(&sql[idx..], mode, MYSQL_VERSION_ID);
    let token = tokens
        .next()
        .map_err(|_| parse_error_at_zero("invalid handler condition"))?
        .ok_or_else(|| parse_error_at_zero("DECLARE HANDLER requires a condition"))?;
    let end = idx + token.span.end;
    match token.kind {
        crate::sql_lex::SqlTokenKind::Number => {
            let code = tokens
                .token_text(&token)
                .parse::<u16>()
                .map_err(|_| parse_error_at_zero("invalid MySQL handler error number"))?;
            Ok((RoutineCondition::MysqlError(code), end))
        }
        crate::sql_lex::SqlTokenKind::Word => Ok((
            RoutineCondition::Named(tokens.token_text(&token).to_string()),
            end,
        )),
        crate::sql_lex::SqlTokenKind::QuotedIdentifier => {
            let name = tokens
                .decoded_quoted_text(&token)
                .ok_or_else(|| parse_error_at_zero("invalid named handler condition"))?;
            Ok((RoutineCondition::Named(name), end))
        }
        _ => Err(parse_error_at_zero("invalid handler condition")),
    }
}

fn parse_routine_target(sql: &str) -> Result<RoutineTarget, ParseError> {
    let trimmed = sql.trim();
    if let Some(name) = trimmed.strip_prefix('@') {
        Ok(RoutineTarget::UserVariable(
            name.trim_matches('`').to_ascii_lowercase(),
        ))
    } else if !trimmed.is_empty() {
        Ok(RoutineTarget::Local(unquote_identifier(trimmed)))
    } else {
        Err(parse_error_at_zero("expected routine variable target"))
    }
}

pub(super) fn parse_definer_account(sql: &str) -> Option<AccountIdent> {
    parse_definer(sql).ok()
}

fn parse_routine_object_name(sql: &str, idx: usize) -> Result<(String, usize), ParseError> {
    let mut idx = skip_ws(sql, idx);
    let start = idx;
    let mut quote = None;
    while idx < sql.len() {
        let ch = sql[idx..].chars().next().unwrap();
        if let Some(q) = quote {
            idx += ch.len_utf8();
            if ch == q {
                quote = None;
            }
            continue;
        }
        match ch {
            '`' | '\'' | '"' => {
                quote = Some(ch);
                idx += ch.len_utf8();
            }
            '(' | ';' => break,
            c if c.is_whitespace() => break,
            _ => idx += ch.len_utf8(),
        }
    }
    if idx == start {
        return Err(parse_error_at_zero("expected routine name"));
    }
    Ok((normalize_object_name_sql(&sql[start..idx]), idx))
}

fn normalize_object_name_sql(sql: &str) -> String {
    split_top_level(sql, '.')
        .into_iter()
        .map(|part| unquote_identifier(part.trim()))
        .collect::<Vec<_>>()
        .join(".")
}

pub(super) fn unquote_identifier(input: &str) -> String {
    let trimmed = input.trim();
    if trimmed.len() >= 2 {
        let first = trimmed.as_bytes()[0] as char;
        let last = trimmed.as_bytes()[trimmed.len() - 1] as char;
        if (first == '`' || first == '\'' || first == '"') && last == first {
            return trimmed[1..trimmed.len() - 1]
                .replace(&format!("{first}{first}"), &first.to_string());
        }
    }
    trimmed.trim_matches('`').to_string()
}

fn consume_parenthesized(sql: &str, idx: usize) -> Option<(String, usize)> {
    let mut idx = skip_ws(sql, idx);
    if !sql[idx..].starts_with('(') {
        return None;
    }
    idx += 1;
    let start = idx;
    let mut depth = 1usize;
    let mut quote = None;
    while idx < sql.len() {
        let ch = sql[idx..].chars().next()?;
        if let Some(q) = quote {
            idx += ch.len_utf8();
            if ch == q {
                let next = sql[idx..].chars().next();
                if next == Some(q) {
                    idx += q.len_utf8();
                } else {
                    quote = None;
                }
            } else if ch == '\\' {
                if let Some(esc) = sql[idx..].chars().next() {
                    idx += esc.len_utf8();
                }
            }
            continue;
        }
        match ch {
            '\'' | '"' | '`' => {
                quote = Some(ch);
                idx += ch.len_utf8();
            }
            '(' => {
                depth += 1;
                idx += 1;
            }
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some((sql[start..idx].to_string(), idx + 1));
                }
                idx += 1;
            }
            _ => idx += ch.len_utf8(),
        }
    }
    None
}

pub(super) fn skip_ws(sql: &str, idx: usize) -> usize {
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    crate::sql_lex::skip_trivia(sql, idx, mode, MYSQL_VERSION_ID, false).unwrap_or_else(|_| {
        let mut idx = idx;
        while idx < sql.len() && sql.as_bytes()[idx].is_ascii_whitespace() {
            idx += 1;
        }
        idx
    })
}

pub(super) fn consume_keyword_at(sql: &str, idx: usize, keyword: &str) -> Option<usize> {
    let idx = skip_ws(sql, idx);
    let end = idx.checked_add(keyword.len())?;
    // `sql.get(idx..end)` is None both when `end` exceeds the length AND when
    // it lands inside a multibyte UTF-8 scalar — the old `sql[idx..end]` slice
    // panicked on inputs like `😀💩 SELECT 1` where a keyword-length offset cut
    // through an emoji (M4-found DoS). ASCII keyword ⇒ a match implies `end`
    // is a char boundary.
    match sql.get(idx..end) {
        Some(slice) if slice.eq_ignore_ascii_case(keyword) => {}
        _ => return None,
    }
    if idx > 0 {
        let prev = sql[..idx].chars().next_back()?;
        if is_ident_char(prev) {
            return None;
        }
    }
    if end < sql.len() {
        let next = sql[end..].chars().next()?;
        if is_ident_char(next) {
            return None;
        }
    }
    Some(end)
}

pub(super) fn consume_keywords_at(sql: &str, mut idx: usize, keywords: &[&str]) -> Option<usize> {
    for keyword in keywords {
        idx = consume_keyword_at(sql, idx, keyword)?;
    }
    Some(idx)
}

pub(super) fn is_ident_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_' || ch == '$'
}

pub(super) fn find_first_top_level_keyword(
    sql: &str,
    start: usize,
    keywords: &[&str],
) -> Option<usize> {
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    crate::sql_lex::StructuralCursor::new(sql, mode, MYSQL_VERSION_ID)
        .find_top_level_keywords(start, keywords)
        .ok()
        .flatten()
        .map(|span| span.start)
}

fn top_level_keyword_pos(sql: &str, keyword: &str) -> Option<usize> {
    find_first_top_level_keyword(sql, 0, &[keyword])
}

fn top_level_phrase_span(sql: &str, phrase: &str) -> Option<std::ops::Range<usize>> {
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    crate::sql_lex::StructuralCursor::new(sql, mode, MYSQL_VERSION_ID)
        .find_top_level_keyword(phrase.as_bytes())
        .ok()
        .flatten()
}

fn split_top_level(sql: &str, sep: char) -> Vec<&str> {
    if !sep.is_ascii() {
        return vec![sql];
    }
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    crate::sql_lex::StructuralCursor::split_top_level(sql, sep as u8, mode, MYSQL_VERSION_ID)
        .unwrap_or_else(|_| std::iter::once(0..sql.len()).collect())
        .into_iter()
        .map(|range| &sql[range])
        .collect()
}

fn split_assignment(sql: &str) -> Option<(&str, &str)> {
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let idx = crate::sql_lex::StructuralCursor::new(sql, mode, MYSQL_VERSION_ID)
        .find_top_level_byte(b'=')
        .ok()
        .flatten()?;
    if idx > 0 && sql.as_bytes()[idx - 1] == b':' {
        Some((&sql[..idx - 1], &sql[idx + 1..]))
    } else {
        Some((&sql[..idx], &sql[idx + 1..]))
    }
}

fn parse_sql_string_literal_at(sql: &str, idx: usize) -> Option<(String, usize)> {
    let idx = skip_ws(sql, idx);
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let mut cursor = crate::sql_lex::SqlTokenCursor::new(&sql[idx..], mode, MYSQL_VERSION_ID);
    let token = cursor.next().ok().flatten()?;
    if token.span.start != 0 || token.kind != crate::sql_lex::SqlTokenKind::String {
        return None;
    }
    let decoded = cursor.decoded_quoted_text(&token)?;
    Some((decoded, idx + token.span.end))
}

fn split_routine_label(sql: &str) -> (Option<String>, &str) {
    let trimmed = sql.trim();
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let mut cursor = crate::sql_lex::SqlTokenCursor::new(trimmed, mode, MYSQL_VERSION_ID);
    let Ok(Some(name)) = cursor.next() else {
        return (None, trimmed);
    };
    if !matches!(
        name.kind,
        crate::sql_lex::SqlTokenKind::Word | crate::sql_lex::SqlTokenKind::QuotedIdentifier
    ) {
        return (None, trimmed);
    }
    let Ok(Some(colon)) = cursor.next() else {
        return (None, trimmed);
    };
    if colon.kind != crate::sql_lex::SqlTokenKind::Symbol(b':')
        || trimmed.as_bytes().get(colon.span.end) == Some(&b'=')
    {
        return (None, trimmed);
    }
    let label = cursor
        .decoded_quoted_text(&name)
        .unwrap_or_else(|| cursor.token_text(&name).to_owned());
    (Some(label), trimmed[colon.span.end..].trim_start())
}

fn strip_compound_tail<'a>(
    body_start: usize,
    sql: &'a str,
    end_phrase: &str,
    label: Option<&str>,
) -> Result<&'a str, ParseError> {
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let mut cursor = crate::sql_lex::SqlTokenCursor::new(sql, mode, MYSQL_VERSION_ID);
    let words = end_phrase.split_whitespace();
    let word_count = words.clone().count();
    // Retain only the suffix needed for END [kind] [label]. Current grammar
    // forms fit on the stack, independent of the size of the routine body.
    let tail_capacity = word_count + usize::from(label.is_some());
    let mut tokens = smallvec::SmallVec::<[crate::sql_lex::SqlToken; 3]>::new();
    while let Some(token) = cursor
        .next()
        .map_err(|err| parse_error_at_zero(err.to_string()))?
    {
        if tokens.len() == tail_capacity {
            tokens.remove(0);
        }
        tokens.push(token);
    }
    let matches_tail = |tokens: &[crate::sql_lex::SqlToken]| {
        tokens.len().checked_sub(word_count).filter(|&start| {
            tokens[start..]
                .iter()
                .zip(words.clone())
                .all(|(token, word)| {
                    token.kind == crate::sql_lex::SqlTokenKind::Word
                        && cursor.token_text(token).eq_ignore_ascii_case(word)
                })
        })
    };
    // Prefer an unlabeled END phrase. A quoted opening label can itself
    // be named `loop`, and END LOOP may omit that optional closing label.
    let tail_start = matches_tail(&tokens)
        .or_else(|| {
            let (Some(label), Some(last)) = (label, tokens.last()) else {
                return None;
            };
            if matches!(
                last.kind,
                crate::sql_lex::SqlTokenKind::Word | crate::sql_lex::SqlTokenKind::QuotedIdentifier
            ) && unquote_identifier(cursor.token_text(last)).eq_ignore_ascii_case(label)
            {
                matches_tail(&tokens[..tokens.len() - 1])
            } else {
                None
            }
        })
        .ok_or_else(|| parse_error_at_zero(format!("compound statement requires {end_phrase}")))?;
    let end = tokens[tail_start].span.start;
    sql.get(body_start..end)
        .map(str::trim)
        .ok_or_else(|| parse_error_at_zero("invalid compound statement body"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RoutineCompoundKind {
    Begin,
    If,
    Case,
    Loop,
    While,
    Repeat,
}

fn routine_compound_suffix(word: &str) -> Option<RoutineCompoundKind> {
    [
        ("IF", RoutineCompoundKind::If),
        ("CASE", RoutineCompoundKind::Case),
        ("LOOP", RoutineCompoundKind::Loop),
        ("WHILE", RoutineCompoundKind::While),
        ("REPEAT", RoutineCompoundKind::Repeat),
    ]
    .into_iter()
    .find_map(|(keyword, kind)| word.eq_ignore_ascii_case(keyword).then_some(kind))
}

fn routine_statement_start_word(word: &str) -> bool {
    matches!(
        word,
        "BEGIN"
            | "DECLARE"
            | "SET"
            | "SELECT"
            | "IF"
            | "CASE"
            | "LOOP"
            | "WHILE"
            | "REPEAT"
            | "LEAVE"
            | "ITERATE"
            | "RETURN"
            | "OPEN"
            | "FETCH"
            | "CLOSE"
            | "SIGNAL"
            | "RESIGNAL"
            | "CALL"
            | "PREPARE"
            | "EXECUTE"
            | "DEALLOCATE"
            | "GET"
            | "INSERT"
            | "REPLACE"
            | "UPDATE"
            | "DELETE"
            | "CREATE"
            | "ALTER"
            | "DROP"
            | "TRUNCATE"
    )
}

pub(super) fn split_routine_statements(sql: &str) -> Vec<&str> {
    scan_routine_boundaries(sql, &[], true).0
}

// Statement splitting and branch detection share the same nesting rules.
// Expression scans start outside statement context so CASE is an expression.
fn scan_routine_boundaries<'a>(
    sql: &'a str,
    markers: &[&'static str],
    mut statement_start: bool,
) -> (Vec<&'a str>, Option<(usize, &'static str, usize)>) {
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    use crate::sql_lex::{SqlTokenCursor, SqlTokenKind};
    let mut tokens = SqlTokenCursor::new(sql, mode, MYSQL_VERSION_ID);
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut paren_depth = 0usize;
    let mut compound_stack = Vec::new();
    let mut expression_case_depth = 0usize;
    let mut declare_candidate = false;
    let mut handler_action_pending = false;
    let mut create_statement = false;
    let mut alter_statement = false;
    let mut routine_ddl_body_pending = false;
    let mut trigger_header_pending = false;
    // On a malformed final token, keep its original tail in the last part.
    // Protocol multi-statement execution must retain successfully split prefixes.
    while let Ok(Some(token)) = tokens.next() {
        let idx = token.span.start;
        match token.kind {
            SqlTokenKind::Symbol(b'(') => paren_depth += 1,
            SqlTokenKind::Symbol(b')') => paren_depth = paren_depth.saturating_sub(1),
            SqlTokenKind::Symbol(b';') if paren_depth == 0 => {
                if compound_stack.is_empty() {
                    parts.push(&sql[start..idx]);
                    start = idx + 1;
                }
                statement_start = true;
                expression_case_depth = 0;
                declare_candidate = false;
                handler_action_pending = false;
                create_statement = false;
                alter_statement = false;
                routine_ddl_body_pending = false;
                trigger_header_pending = false;
            }
            SqlTokenKind::Word if paren_depth == 0 => {
                let word_start = idx;
                let idx = token.span.end;
                // Only grammar keywords need folding. Longer/Unicode names
                // cannot match them and never require an allocated uppercase copy.
                let text = tokens.token_text(&token);
                let mut upper = [0u8; 16];
                let word = if text.len() <= upper.len() && text.is_ascii() {
                    for (out, byte) in upper.iter_mut().zip(text.bytes()) {
                        *out = byte.to_ascii_uppercase();
                    }
                    std::str::from_utf8(&upper[..text.len()]).unwrap()
                } else {
                    ""
                };

                if compound_stack.is_empty() && expression_case_depth == 0 {
                    if let Some(&marker) = markers.iter().find(|&&marker| marker == word) {
                        return (parts, Some((word_start, marker, idx)));
                    }
                }

                if word == "END" {
                    if expression_case_depth > 0 {
                        expression_case_depth -= 1;
                        statement_start = false;
                        continue;
                    }
                    let suffix = tokens
                        .peek()
                        .ok()
                        .flatten()
                        .filter(|token| token.kind == SqlTokenKind::Word)
                        .and_then(|token| routine_compound_suffix(&sql[token.span.clone()]));
                    match suffix {
                        Some(kind) if compound_stack.last().copied() == Some(kind) => {
                            compound_stack.pop();
                            let _ = tokens.next();
                        }
                        None if compound_stack.last().copied()
                            == Some(RoutineCompoundKind::Begin) =>
                        {
                            compound_stack.pop();
                        }
                        _ => {}
                    }
                    statement_start = false;
                    continue;
                }

                let next = tokens.peek().ok().flatten().cloned();
                let next_symbol = next.as_ref().and_then(|token| match token.kind {
                    SqlTokenKind::Symbol(symbol) => Some(symbol),
                    _ => None,
                });
                let is_label = if statement_start && next_symbol == Some(b':') {
                    let mut lookahead = tokens.clone();
                    let _ = lookahead.next();
                    !lookahead
                        .next()
                        .ok()
                        .flatten()
                        .is_some_and(|token| token.kind == SqlTokenKind::Symbol(b'='))
                } else {
                    false
                };
                if is_label {
                    continue;
                }

                if statement_start && word == "CREATE" {
                    create_statement = true;
                } else if statement_start && word == "ALTER" {
                    alter_statement = true;
                } else if (create_statement && matches!(word, "PROCEDURE" | "FUNCTION" | "EVENT"))
                    || (alter_statement && word == "EVENT")
                {
                    routine_ddl_body_pending = true;
                } else if create_statement && word == "TRIGGER" {
                    // INSERT/UPDATE/DELETE in the trigger header is the event,
                    // not the first body statement.  Arm body recognition only
                    // after the mandatory `FOR EACH ROW` clause.
                    trigger_header_pending = true;
                } else if trigger_header_pending && word == "ROW" {
                    trigger_header_pending = false;
                    routine_ddl_body_pending = true;
                }

                if statement_start && word == "DECLARE" {
                    declare_candidate = true;
                } else if declare_candidate && word == "HANDLER" {
                    handler_action_pending = true;
                }
                let handler_action_start =
                    handler_action_pending && routine_statement_start_word(word);
                if handler_action_start {
                    handler_action_pending = false;
                }
                let routine_ddl_body_start =
                    routine_ddl_body_pending && routine_statement_start_word(word);
                let procedural_start =
                    statement_start || handler_action_start || routine_ddl_body_start;

                let opener = if procedural_start && word == "BEGIN" {
                    let transaction_begin = next_symbol == Some(b';')
                        || next.as_ref().is_some_and(|token| {
                            token.kind == SqlTokenKind::Word
                                && tokens.token_text(token).eq_ignore_ascii_case("WORK")
                        });
                    (!transaction_begin).then_some(RoutineCompoundKind::Begin)
                } else if procedural_start && word == "IF" {
                    Some(RoutineCompoundKind::If)
                } else if procedural_start && word == "CASE" {
                    Some(RoutineCompoundKind::Case)
                } else if procedural_start && word == "LOOP" {
                    Some(RoutineCompoundKind::Loop)
                } else if procedural_start && word == "WHILE" {
                    Some(RoutineCompoundKind::While)
                } else if procedural_start && word == "REPEAT" && next_symbol != Some(b'(') {
                    Some(RoutineCompoundKind::Repeat)
                } else {
                    None
                };
                if routine_ddl_body_start {
                    routine_ddl_body_pending = false;
                }
                if let Some(kind) = opener {
                    compound_stack.push(kind);
                    statement_start = matches!(
                        kind,
                        RoutineCompoundKind::Begin
                            | RoutineCompoundKind::Loop
                            | RoutineCompoundKind::Repeat
                    );
                    continue;
                }

                if word == "CASE" {
                    expression_case_depth += 1;
                    statement_start = false;
                    continue;
                }
                if expression_case_depth == 0 {
                    match word {
                        "THEN"
                            if matches!(
                                compound_stack.last(),
                                Some(RoutineCompoundKind::If | RoutineCompoundKind::Case)
                            ) =>
                        {
                            statement_start = true;
                            continue;
                        }
                        "DO" if compound_stack.last().copied()
                            == Some(RoutineCompoundKind::While) =>
                        {
                            statement_start = true;
                            continue;
                        }
                        "ELSE"
                            if matches!(
                                compound_stack.last(),
                                Some(RoutineCompoundKind::If | RoutineCompoundKind::Case)
                            ) =>
                        {
                            statement_start = true;
                            continue;
                        }
                        "ELSEIF"
                            if compound_stack.last().copied() == Some(RoutineCompoundKind::If) =>
                        {
                            statement_start = false;
                            continue;
                        }
                        "WHEN"
                            if compound_stack.last().copied()
                                == Some(RoutineCompoundKind::Case) =>
                        {
                            statement_start = false;
                            continue;
                        }
                        _ => {}
                    }
                }
                statement_start = false;
            }
            _ => {}
        }
    }
    if start < sql.len() {
        parts.push(&sql[start..]);
    }
    (parts, None)
}

fn routine_expression_keyword_pos(sql: &str, keyword: &'static str) -> Option<usize> {
    scan_routine_boundaries(sql, &[keyword], false)
        .1
        .map(|(start, _, _)| start)
}

fn find_if_marker(sql: &str) -> Option<(usize, &'static str, usize)> {
    scan_routine_boundaries(sql, &["ELSEIF", "ELSE"], true).1
}

fn find_case_marker(sql: &str) -> Option<(usize, &'static str, usize)> {
    scan_routine_boundaries(sql, &["WHEN", "ELSE"], true).1
}

pub(super) fn sql_statement_kind(stmt: &SqlStatement) -> &'static str {
    match stmt {
        SqlStatement::Query(_) => "SELECT",
        SqlStatement::Insert(_) => "INSERT",
        SqlStatement::Update(_) => "UPDATE",
        SqlStatement::Delete(_) => "DELETE",
        SqlStatement::CreateView(_) => "CREATE VIEW",
        SqlStatement::AlterView { .. } => "ALTER VIEW",
        SqlStatement::CreateTable(_) => "CREATE TABLE",
        SqlStatement::CreateIndex(_) => "CREATE INDEX",
        SqlStatement::AlterTable(_) => "ALTER TABLE",
        SqlStatement::Analyze(_) => "ANALYZE",
        SqlStatement::Drop { .. } => "DROP",
        SqlStatement::StartTransaction { .. } => "START TRANSACTION",
        SqlStatement::Commit { .. } => "COMMIT",
        SqlStatement::Rollback { .. } => "ROLLBACK",
        SqlStatement::Set(_) => "SET",
        SqlStatement::ShowVariables { .. } => "SHOW VARIABLES",
        SqlStatement::ShowStatus { .. } => "SHOW STATUS",
        SqlStatement::ShowCollation { .. } => "SHOW COLLATION",
        _ => "SQL statement",
    }
}
