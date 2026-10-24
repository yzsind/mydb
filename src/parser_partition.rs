//! Manual parser for MySQL `PARTITION BY ...` clauses in CREATE TABLE
//! and partition-related actions in ALTER TABLE.
//!
//! `sqlparser` 0.61 does not model MySQL's full partition syntax (it
//! exposes `partition_by: Option<Box<Expr>>` for the PostgreSQL form
//! and a limited `AddPartitions`/`DropPartitions` AST), so we do our
//! own pre-parse: detect the `PARTITION BY` clause inside the
//! original SQL, parse it into [`PartitionDef`] / [`PartitionAction`],
//! strip the clause from the SQL, and hand the cleaned-up SQL to
//! sqlparser. Lexical errors within the partition fragment are rebased
//! to the original SQL before they leave this parser.
//!
//! Supported syntax — MySQL 8.0 manual sections 24.2 / 24.3 / 24.4:
//!
//! ```sql
//! PARTITION BY [LINEAR] HASH (expr) [PARTITIONS n]
//! PARTITION BY [LINEAR] KEY [ALGORITHM={1|2}] (col_list) [PARTITIONS n]
//! PARTITION BY RANGE (expr) (PARTITION pN VALUES LESS THAN (literal|MAXVALUE) ...)
//! PARTITION BY RANGE COLUMNS (col_list) (PARTITION pN VALUES LESS THAN (...) ...)
//! PARTITION BY LIST (expr) (PARTITION pN VALUES IN (literal_list) ...)
//! PARTITION BY LIST COLUMNS (col_list) (PARTITION pN VALUES IN ((tuple)*) ...)
//! ```
//!
//! Each PARTITION entry tolerates trailing `[STORAGE] ENGINE=...`,
//! `[DEFAULT] CHARACTER SET=...`, `COMMENT='...'`, `TABLESPACE=...`
//! options. Placement options are recorded and checked by the storage
//! layer before mutation; unsupported placement is rejected.
//!
//! Subpartitioning (`SUBPARTITION BY ...`) is parsed and stored; the
//! storage layer expands each top-level partition into leaf
//! subpartition storage ids for routing, scanning, and metadata.

use crate::types::Value;

/// Mirror of [`crate::storage::partitioning::PartitionMethod`] for the
/// pre-storage parser AST.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PartitionMethod {
    Range,
    RangeColumns,
    List,
    ListColumns,
    Hash,
    LinearHash,
    Key,
    LinearKey,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum PartitionLiteralDef {
    Value(Value),
    MaxValue,
    Expression(String),
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum PartitionBoundsDef {
    RangeLessThan(Vec<PartitionLiteralDef>),
    ListIn(Vec<Vec<PartitionLiteralDef>>),
    Hash,
}

/// Parsed per-(sub)partition physical placement options. Semantic validation
/// rejects options mydb cannot honor before any storage mutation.
#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct PartitionOptionsDef {
    pub data_directory: Option<String>,
    pub index_directory: Option<String>,
    pub tablespace: Option<String>,
    pub engine: Option<String>,
    pub max_rows: Option<u64>,
    pub min_rows: Option<u64>,
    pub nodegroup: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PartitionItemDef {
    pub name: String,
    pub bounds: PartitionBoundsDef,
    pub comment: String,
    pub options: PartitionOptionsDef,
    pub subpartitions: Vec<SubpartitionItemDef>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SubpartitionItemDef {
    pub name: String,
    pub comment: String,
    pub options: PartitionOptionsDef,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SubpartitionByDef {
    pub method: PartitionMethod,
    pub expr_sql: Option<String>,
    pub columns: Vec<String>,
    pub key_algorithm: Option<u8>,
    pub count: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PartitionDef {
    pub method: PartitionMethod,
    /// For RANGE/LIST/HASH: canonical SQL text of the partition
    /// expression (matches what we'll persist into
    /// `PartitionInfo::expr_sql`). Empty for COLUMNS / KEY variants.
    pub expr_sql: Option<String>,
    /// For RANGE COLUMNS / LIST COLUMNS / KEY: declared columns.
    pub columns: Vec<String>,
    /// Optional `ALGORITHM=N` for KEY variants.
    pub key_algorithm: Option<u8>,
    /// `PARTITIONS n` count for HASH/KEY; the explicit partitions in
    /// `partitions` always win when both are given.
    pub partition_count: Option<u32>,
    /// Explicit `(PARTITION pN ...)` definitions. For HASH/KEY this
    /// can be empty when only a count is given (the partitions are
    /// synthesised as `p0..p{n-1}`).
    pub partitions: Vec<PartitionItemDef>,
    /// Optional `SUBPARTITION BY` clause.
    pub subpartition_by: Option<SubpartitionByDef>,
}

/// Alter-table partition operations.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum PartitionAction {
    /// `ADD PARTITION (PARTITION pN VALUES ...)` for RANGE/LIST.
    AddPartitions(Vec<PartitionItemDef>),
    /// `ADD PARTITION PARTITIONS N` for HASH/KEY.
    AddPartitionCount(u32),
    /// `DROP PARTITION p1, p2`.
    DropPartitions(Vec<String>),
    /// `TRUNCATE PARTITION p1, p2` or `TRUNCATE PARTITION ALL`.
    TruncatePartitions(PartitionTargets),
    /// `ANALYZE PARTITION p1, p2` or `ALL`.
    AnalyzePartitions(PartitionTargets),
    /// `OPTIMIZE PARTITION ...` or `ALL`.
    OptimizePartitions(PartitionTargets),
    /// `CHECK PARTITION ...` or `ALL`.
    CheckPartitions(PartitionTargets),
    /// `REPAIR PARTITION ...` or `ALL`.
    RepairPartitions(PartitionTargets),
    /// `REBUILD PARTITION ...` or `ALL`.
    RebuildPartitions(PartitionTargets),
    /// `COALESCE PARTITION N` — HASH/KEY only, reduces partition count.
    CoalescePartitions(u32),
    /// `REORGANIZE PARTITION p_old1, p_old2, ... INTO (...)`.
    ReorganizePartitions {
        old_partitions: Vec<String>,
        new_partitions: Vec<PartitionItemDef>,
    },
    /// `EXCHANGE PARTITION p WITH TABLE t [WITH | WITHOUT VALIDATION]`.
    ExchangePartition {
        partition: String,
        table: String,
        with_validation: bool,
    },
    /// `REMOVE PARTITIONING`.
    RemovePartitioning,
    /// Convert / re-partition the table to a fresh partition layout
    /// (`PARTITION BY ...`). MySQL allows this as a single ALTER TABLE
    /// action; effectively a CREATE TABLE-style re-declaration.
    PartitionBy(PartitionDef),
}

/// Target list shape shared by ANALYZE/OPTIMIZE/CHECK/REPAIR/REBUILD/
/// TRUNCATE PARTITION. MySQL accepts either an explicit name list or
/// the `ALL` shorthand.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum PartitionTargets {
    All,
    Names(Vec<String>),
}

/// Partition grammar adapter over the shared, SQL-mode-aware lexer.
/// Keyword and quoted-identifier identity remains distinct throughout parsing.
#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Ident(String),
    QuotedIdent(String),
    Number(String),
    StringLit(String),
    LParen,
    RParen,
    Comma,
    Equal,
    /// Anything else we don't classify (operators inside expressions).
    Other(char),
    Eof,
}

#[derive(Debug)]
struct Tokeniser<'a> {
    chars: crate::sql_lex::SqlTokenCursor<'a>,
    src: &'a str,
    peeked: Option<(usize, Tok)>,
    next_pos: usize,
}

impl<'a> Tokeniser<'a> {
    fn new(src: &'a str) -> Self {
        let mode = crate::sql_lex::SqlLexMode::from_parser_bits(
            crate::parser::current_parser_sql_mode_bits(),
        );
        Self {
            chars: crate::sql_lex::SqlTokenCursor::new(src, mode, crate::parser::MYSQL_VERSION_ID),
            src,
            peeked: None,
            next_pos: 0,
        }
    }

    fn peek(&mut self) -> Result<&Tok, crate::parser::ParseError> {
        if self.peeked.is_none() {
            let (pos, tok) = self.next_inner()?;
            self.peeked = Some((pos, tok));
        }
        Ok(&self.peeked.as_ref().unwrap().1)
    }

    fn next(&mut self) -> Result<Tok, crate::parser::ParseError> {
        if let Some((pos, tok)) = self.peeked.take() {
            self.next_pos = pos;
            return Ok(tok);
        }
        let (pos, tok) = self.next_inner()?;
        self.next_pos = pos;
        Ok(tok)
    }

    fn position(&self) -> usize {
        self.next_pos
    }

    /// Position immediately *before* the next token (for slicing the
    /// source up to here without including the next token's text).
    fn position_before_peek(&mut self) -> Result<usize, crate::parser::ParseError> {
        if self.peeked.is_none() {
            let (pos, tok) = self.next_inner()?;
            self.peeked = Some((pos, tok));
        }
        Ok(self.peeked.as_ref().unwrap().0)
    }

    fn next_inner(&mut self) -> Result<(usize, Tok), crate::parser::ParseError> {
        let Some(token) = self
            .chars
            .next()
            .map_err(|err| crate::parser::ParseError::from_lex(self.src, err))?
        else {
            return Ok((self.src.len(), Tok::Eof));
        };
        let pos = token.span.start;
        let tok = match token.kind {
            crate::sql_lex::SqlTokenKind::Word => {
                Tok::Ident(self.chars.token_text(&token).to_string())
            }
            crate::sql_lex::SqlTokenKind::QuotedIdentifier => Tok::QuotedIdent(
                self.chars
                    .decoded_quoted_text(&token)
                    .expect("validated quoted identifier"),
            ),
            crate::sql_lex::SqlTokenKind::String => Tok::StringLit(
                self.chars
                    .decoded_quoted_text(&token)
                    .expect("validated string literal"),
            ),
            crate::sql_lex::SqlTokenKind::Number => {
                Tok::Number(self.chars.token_text(&token).to_string())
            }
            crate::sql_lex::SqlTokenKind::Placeholder => Tok::Other('?'),
            crate::sql_lex::SqlTokenKind::Symbol(b'(') => Tok::LParen,
            crate::sql_lex::SqlTokenKind::Symbol(b')') => Tok::RParen,
            crate::sql_lex::SqlTokenKind::Symbol(b',') => Tok::Comma,
            crate::sql_lex::SqlTokenKind::Symbol(b'=') => Tok::Equal,
            crate::sql_lex::SqlTokenKind::Symbol(symbol @ (b'-' | b'@')) => {
                let mut probe = self.chars.clone();
                let joined = probe
                    .next()
                    .map_err(|err| crate::parser::ParseError::from_lex(self.src, err))?
                    .filter(|next| {
                        next.span.start == token.span.end
                            && match symbol {
                                b'-' => next.kind == crate::sql_lex::SqlTokenKind::Number,
                                b'@' => next.kind == crate::sql_lex::SqlTokenKind::Word,
                                _ => false,
                            }
                    });
                if let Some(next) = joined {
                    self.chars = probe;
                    let text = self.src[token.span.start..next.span.end].to_string();
                    if symbol == b'-' {
                        Tok::Number(text)
                    } else {
                        Tok::Ident(text)
                    }
                } else {
                    Tok::Other(symbol as char)
                }
            }
            crate::sql_lex::SqlTokenKind::Symbol(_) => {
                Tok::Other(self.chars.token_text(&token).chars().next().unwrap_or('\0'))
            }
        };
        Ok((pos, tok))
    }
}

/// Lowercased equivalent of `Tok::Ident` for case-insensitive keyword
/// matching. Returns the empty string for non-Ident tokens.
fn ident_lower(t: &Tok) -> String {
    match t {
        Tok::Ident(s) => s.to_ascii_lowercase(),
        _ => String::new(),
    }
}

/// Find the byte offset where `PARTITION BY` (case-insensitive)
/// starts at the top level of the SQL, *outside* of strings / comments
/// / nested parentheses. Returns None if no such clause exists.
pub fn find_partition_by(sql: &str) -> Result<Option<usize>, crate::parser::ParseError> {
    find_top_level_keyword(sql, &["PARTITION", "BY"])
}

/// Scan `sql` for a sequence of top-level keywords (case-insensitive
/// idents separated only by whitespace/comments). Returns the byte
/// offset of the first keyword if matched, ignoring matches inside
/// strings/comments/nested parens.
fn find_top_level_keyword(
    sql: &str,
    words: &[&str],
) -> Result<Option<usize>, crate::parser::ParseError> {
    use crate::sql_lex::{SqlLexMode, SqlTokenCursor, SqlTokenKind};
    let mode = SqlLexMode::from_parser_bits(crate::parser::current_parser_sql_mode_bits());
    let mut tokens = SqlTokenCursor::new(sql, mode, crate::parser::MYSQL_VERSION_ID);
    let mut depth = 0_i32;
    while let Some(token) = tokens
        .next()
        .map_err(|err| crate::parser::ParseError::from_lex(sql, err))?
    {
        match token.kind {
            SqlTokenKind::Symbol(b'(') => depth += 1,
            SqlTokenKind::Symbol(b')') => depth -= 1,
            SqlTokenKind::Word
                if depth == 0 && tokens.token_text(&token).eq_ignore_ascii_case(words[0]) =>
            {
                let mut probe = tokens.clone();
                let mut matched = true;
                for word in &words[1..] {
                    if !probe
                        .eat_keyword(word.as_bytes())
                        .map_err(|err| crate::parser::ParseError::from_lex(sql, err))?
                    {
                        matched = false;
                        break;
                    }
                }
                if matched {
                    return Ok(Some(token.span.start));
                }
            }
            _ => {}
        }
    }
    Ok(None)
}

/// Try to detect and extract a PARTITION BY clause from a CREATE
/// TABLE statement. Returns `(stripped_sql, Some(PartitionDef))` on
/// success, `(sql, None)` if no PARTITION BY clause is present.
/// Errors propagate as `ParseError`.
pub fn extract_create_table_partition(
    sql: &str,
) -> Result<(String, Option<PartitionDef>), crate::parser::ParseError> {
    let Some(start) = find_partition_by(sql)? else {
        return Ok((sql.to_string(), None));
    };
    let (def, end) = (|| {
        let mut t = Tokeniser::new(&sql[start..]);
        expect_ident_ci(&mut t, "PARTITION")?;
        expect_ident_ci(&mut t, "BY")?;
        let def = parse_partition_def(&mut t)?;
        Ok((def, start + t.position_before_peek()?))
    })()
    .map_err(|err: crate::parser::ParseError| err.rebase(sql, start))?;
    let mut stripped = String::with_capacity(sql.len());
    stripped.push_str(&sql[..start]);
    stripped.push_str(&sql[end..]);
    Ok((
        stripped.trim_end().trim_end_matches(';').to_string(),
        Some(def),
    ))
}

fn expect_ident_ci(t: &mut Tokeniser<'_>, word: &str) -> Result<(), crate::parser::ParseError> {
    match t.next()? {
        Tok::Ident(s) if s.eq_ignore_ascii_case(word) => Ok(()),
        other => Err(crate::parser::ParseError::at_zero(format!(
            "expected `{}`, got {:?}",
            word, other
        ))),
    }
}

fn try_consume_ident_ci(
    t: &mut Tokeniser<'_>,
    word: &str,
) -> Result<bool, crate::parser::ParseError> {
    match t.peek()? {
        Tok::Ident(s) if s.eq_ignore_ascii_case(word) => {
            t.next()?;
            Ok(true)
        }
        _ => Ok(false),
    }
}

fn parse_partition_def(t: &mut Tokeniser<'_>) -> Result<PartitionDef, crate::parser::ParseError> {
    let linear = try_consume_ident_ci(t, "LINEAR")?;
    let kind_ident = match t.next()? {
        Tok::Ident(s) => s,
        other => {
            return Err(crate::parser::ParseError::at_zero(format!(
                "expected RANGE/LIST/HASH/KEY after PARTITION BY, got {:?}",
                other
            )));
        }
    };
    let kind_lower = kind_ident.to_ascii_lowercase();
    let method;
    let mut expr_sql: Option<String> = None;
    let mut columns: Vec<String> = Vec::new();
    let mut key_algorithm: Option<u8> = None;
    match kind_lower.as_str() {
        "range" => {
            if linear {
                return Err(crate::parser::ParseError::at_zero(
                    "LINEAR is not valid with RANGE",
                ));
            }
            if try_consume_ident_ci(t, "COLUMNS")? {
                method = PartitionMethod::RangeColumns;
                columns = parse_column_list(t)?;
            } else {
                method = PartitionMethod::Range;
                expr_sql = Some(parse_parenthesised_expr(t)?);
            }
        }
        "list" => {
            if linear {
                return Err(crate::parser::ParseError::at_zero(
                    "LINEAR is not valid with LIST",
                ));
            }
            if try_consume_ident_ci(t, "COLUMNS")? {
                method = PartitionMethod::ListColumns;
                columns = parse_column_list(t)?;
            } else {
                method = PartitionMethod::List;
                expr_sql = Some(parse_parenthesised_expr(t)?);
            }
        }
        "hash" => {
            method = if linear {
                PartitionMethod::LinearHash
            } else {
                PartitionMethod::Hash
            };
            expr_sql = Some(parse_parenthesised_expr(t)?);
        }
        "key" => {
            method = if linear {
                PartitionMethod::LinearKey
            } else {
                PartitionMethod::Key
            };
            if try_consume_ident_ci(t, "ALGORITHM")? {
                expect_punct(t, Tok::Equal)?;
                let n = parse_unsigned_int(t)?;
                if n != 1 && n != 2 {
                    return Err(crate::parser::ParseError::at_zero(
                        "KEY ALGORITHM must be 1 or 2",
                    ));
                }
                key_algorithm = Some(n as u8);
            }
            columns = parse_column_list(t)?;
        }
        _ => {
            return Err(crate::parser::ParseError::at_zero(format!(
                "unknown partition method `{}`",
                kind_ident
            )));
        }
    }

    let mut partition_count: Option<u32> = None;
    if try_consume_ident_ci(t, "PARTITIONS")? {
        partition_count = Some(parse_unsigned_int(t)?);
    }

    let mut subpartition_by: Option<SubpartitionByDef> = None;
    if try_consume_ident_ci(t, "SUBPARTITION")? {
        expect_ident_ci(t, "BY")?;
        let sub = parse_subpartition_by(t)?;
        subpartition_by = Some(sub);
    }

    let mut partitions: Vec<PartitionItemDef> = Vec::new();
    if matches!(t.peek()?, Tok::LParen) {
        partitions = parse_partition_list(t, method)?;
    }

    Ok(PartitionDef {
        method,
        expr_sql,
        columns,
        key_algorithm,
        partition_count,
        partitions,
        subpartition_by,
    })
}

fn parse_subpartition_by(
    t: &mut Tokeniser<'_>,
) -> Result<SubpartitionByDef, crate::parser::ParseError> {
    let linear = try_consume_ident_ci(t, "LINEAR")?;
    let kind = match t.next()? {
        Tok::Ident(s) => s,
        other => {
            return Err(crate::parser::ParseError::at_zero(format!(
                "expected HASH/KEY after SUBPARTITION BY, got {:?}",
                other
            )))
        }
    };
    let kind_lower = kind.to_ascii_lowercase();
    let mut expr_sql = None;
    let mut columns = Vec::new();
    let mut key_algorithm = None;
    let method = match kind_lower.as_str() {
        "hash" => {
            expr_sql = Some(parse_parenthesised_expr(t)?);
            if linear {
                PartitionMethod::LinearHash
            } else {
                PartitionMethod::Hash
            }
        }
        "key" => {
            if try_consume_ident_ci(t, "ALGORITHM")? {
                expect_punct(t, Tok::Equal)?;
                let n = parse_unsigned_int(t)?;
                if n != 1 && n != 2 {
                    return Err(crate::parser::ParseError::at_zero(
                        "KEY ALGORITHM must be 1 or 2",
                    ));
                }
                key_algorithm = Some(n as u8);
            }
            columns = parse_column_list(t)?;
            if linear {
                PartitionMethod::LinearKey
            } else {
                PartitionMethod::Key
            }
        }
        _ => {
            return Err(crate::parser::ParseError::at_zero(format!(
                "SUBPARTITION BY supports HASH/KEY only, got `{}`",
                kind
            )))
        }
    };
    let mut count = None;
    if try_consume_ident_ci(t, "SUBPARTITIONS")? {
        count = Some(parse_unsigned_int(t)?);
    }
    Ok(SubpartitionByDef {
        method,
        expr_sql,
        columns,
        key_algorithm,
        count,
    })
}

fn parse_column_list(t: &mut Tokeniser<'_>) -> Result<Vec<String>, crate::parser::ParseError> {
    expect_punct(t, Tok::LParen)?;
    let mut out = Vec::new();
    // D5: MySQL 8.0 accepts `PARTITION BY KEY() PARTITIONS n` — an empty
    // column list falls back to the PK (or first UNIQUE key) automatically.
    // The empty-list acceptance is only meaningful for KEY; RANGE COLUMNS
    // and LIST COLUMNS still enforce ≥1 column at the semantic layer.
    if matches!(t.peek()?, Tok::RParen) {
        t.next()?;
        return Ok(out);
    }
    loop {
        match t.next()? {
            Tok::Ident(s) | Tok::QuotedIdent(s) => out.push(s),
            other => {
                return Err(crate::parser::ParseError::at_zero(format!(
                    "expected column name, got {:?}",
                    other
                )))
            }
        }
        match t.next()? {
            Tok::Comma => continue,
            Tok::RParen => return Ok(out),
            other => {
                return Err(crate::parser::ParseError::at_zero(format!(
                    "expected `,` or `)` in column list, got {:?}",
                    other
                )))
            }
        }
    }
}

/// Reads `( expr )` and returns the raw SQL text inside the outer
/// parens.
fn parse_parenthesised_expr(t: &mut Tokeniser<'_>) -> Result<String, crate::parser::ParseError> {
    expect_punct(t, Tok::LParen)?;
    let start = t.position_before_peek()?;
    let mut depth = 1_i32;
    loop {
        match t.next()? {
            Tok::LParen => depth += 1,
            Tok::RParen => {
                depth -= 1;
                if depth == 0 {
                    let end = t.position();
                    let body = t.src[start..end].trim().to_string();
                    return Ok(body);
                }
            }
            Tok::Eof => {
                return Err(crate::parser::ParseError::at_zero(
                    "unbalanced parentheses in partition expression",
                ))
            }
            _ => {}
        }
    }
}

fn parse_partition_list(
    t: &mut Tokeniser<'_>,
    method: PartitionMethod,
) -> Result<Vec<PartitionItemDef>, crate::parser::ParseError> {
    expect_punct(t, Tok::LParen)?;
    let mut out = Vec::new();
    loop {
        let item = parse_partition_item(t, method)?;
        out.push(item);
        match t.next()? {
            Tok::Comma => continue,
            Tok::RParen => return Ok(out),
            other => {
                return Err(crate::parser::ParseError::at_zero(format!(
                    "expected `,` or `)` in partition list, got {:?}",
                    other
                )))
            }
        }
    }
}

fn parse_partition_item(
    t: &mut Tokeniser<'_>,
    method: PartitionMethod,
) -> Result<PartitionItemDef, crate::parser::ParseError> {
    expect_ident_ci(t, "PARTITION")?;
    let name = match t.next()? {
        Tok::Ident(s) | Tok::QuotedIdent(s) => s,
        other => {
            return Err(crate::parser::ParseError::at_zero(format!(
                "expected partition name, got {:?}",
                other
            )))
        }
    };
    let mut comment = String::new();
    let bounds = match method {
        PartitionMethod::Range | PartitionMethod::RangeColumns => {
            expect_ident_ci(t, "VALUES")?;
            expect_ident_ci(t, "LESS")?;
            expect_ident_ci(t, "THAN")?;
            // Either `MAXVALUE` bareword or `(literal_list)`.
            if try_consume_ident_ci(t, "MAXVALUE")? {
                PartitionBoundsDef::RangeLessThan(vec![PartitionLiteralDef::MaxValue])
            } else {
                let lits = parse_literal_tuple(t)?;
                PartitionBoundsDef::RangeLessThan(lits)
            }
        }
        PartitionMethod::List | PartitionMethod::ListColumns => {
            expect_ident_ci(t, "VALUES")?;
            expect_ident_ci(t, "IN")?;
            let groups = parse_list_in_values(t, matches!(method, PartitionMethod::ListColumns))?;
            PartitionBoundsDef::ListIn(groups)
        }
        PartitionMethod::Hash
        | PartitionMethod::LinearHash
        | PartitionMethod::Key
        | PartitionMethod::LinearKey => PartitionBoundsDef::Hash,
    };
    // Trailing options. COMMENT is captured separately; the physical
    // placement options (ENGINE / DATA|INDEX DIRECTORY / TABLESPACE /
    // MAX_ROWS / MIN_ROWS / NODEGROUP) are parsed into `options` so they
    // round-trip through SHOW CREATE TABLE and IS.PARTITIONS. Explicit
    // subpartition definitions, when present, are a parenthesized list
    // after these options.
    let mut subpartitions = Vec::new();
    let mut options = PartitionOptionsDef::default();
    loop {
        match t.peek()?.clone() {
            Tok::LParen => {
                subpartitions = parse_subpartition_list(t)?;
                continue;
            }
            Tok::Ident(s) => {
                let lower = s.to_ascii_lowercase();
                if lower == "comment" {
                    t.next()?;
                    if matches!(t.peek()?, Tok::Equal) {
                        t.next()?;
                    }
                    let lit = expect_string_literal(t)?;
                    comment = lit;
                    continue;
                }
                if parse_partition_option_into(t, &lower, &mut options)? {
                    continue;
                }
                break;
            }
            _ => break,
        }
    }
    Ok(PartitionItemDef {
        name,
        bounds,
        comment,
        options,
        subpartitions,
    })
}

/// Parse one physical placement option keyword (already peeked as
/// `lower`) into `out`, returning `true` if it was consumed. Handles the
/// `STORAGE ENGINE` / `DATA|INDEX DIRECTORY` two-word forms and the
/// optional `=` before the value.
fn parse_partition_option_into(
    t: &mut Tokeniser<'_>,
    lower: &str,
    out: &mut PartitionOptionsDef,
) -> Result<bool, crate::parser::ParseError> {
    // Which logical option this keyword introduces.
    enum Opt {
        Engine,
        DataDir,
        IndexDir,
        Tablespace,
        MaxRows,
        MinRows,
        Nodegroup,
        Storage, // `STORAGE ENGINE` prefix
        Data,    // `DATA DIRECTORY` prefix
        Index,   // `INDEX DIRECTORY` prefix
    }
    let opt = match lower {
        "engine" => Opt::Engine,
        "storage" => Opt::Storage,
        "data" => Opt::Data,
        "index" => Opt::Index,
        "tablespace" => Opt::Tablespace,
        "max_rows" => Opt::MaxRows,
        "min_rows" => Opt::MinRows,
        "nodegroup" => Opt::Nodegroup,
        _ => return Ok(false),
    };
    t.next()?; // consume the keyword
    let opt = match opt {
        Opt::Storage => {
            // `STORAGE ENGINE = ...`
            let _ = try_consume_ident_ci(t, "ENGINE")?;
            Opt::Engine
        }
        Opt::Data => {
            expect_ident_ci(t, "DIRECTORY")?;
            Opt::DataDir
        }
        Opt::Index => {
            expect_ident_ci(t, "DIRECTORY")?;
            Opt::IndexDir
        }
        other => other,
    };
    if let Tok::Equal = t.peek()? {
        t.next()?;
    }
    // Value: string / ident / number depending on the option.
    match opt {
        Opt::Engine => out.engine = Some(read_option_word(t, "ENGINE")?),
        Opt::DataDir => out.data_directory = Some(read_option_word(t, "DATA DIRECTORY")?),
        Opt::IndexDir => out.index_directory = Some(read_option_word(t, "INDEX DIRECTORY")?),
        Opt::Tablespace => out.tablespace = Some(read_option_word(t, "TABLESPACE")?),
        Opt::MaxRows => out.max_rows = Some(read_option_u64(t, "MAX_ROWS")?),
        Opt::MinRows => out.min_rows = Some(read_option_u64(t, "MIN_ROWS")?),
        Opt::Nodegroup => {
            let value = read_option_u64(t, "NODEGROUP")?;
            out.nodegroup = Some(u32::try_from(value).map_err(|_| {
                crate::parser::ParseError::at_zero("NODEGROUP value is out of range")
            })?);
        }
        Opt::Storage | Opt::Data | Opt::Index => unreachable!("normalized above"),
    }
    Ok(true)
}

/// Consume one option value token (string literal or bareword ident) and
/// return it as a `String`. Numbers are read as their textual form.
fn read_option_word(
    t: &mut Tokeniser<'_>,
    option: &str,
) -> Result<String, crate::parser::ParseError> {
    match t.peek()?.clone() {
        Tok::StringLit(s) => {
            t.next()?;
            Ok(s)
        }
        Tok::Ident(s) | Tok::QuotedIdent(s) => {
            t.next()?;
            Ok(s)
        }
        Tok::Number(n) => {
            t.next()?;
            Ok(n)
        }
        other => Err(crate::parser::ParseError::at_zero(format!(
            "{option} requires a value, found {other:?}"
        ))),
    }
}

fn read_option_u64(t: &mut Tokeniser<'_>, option: &str) -> Result<u64, crate::parser::ParseError> {
    let raw = match t.peek()?.clone() {
        Tok::Number(n) => {
            t.next()?;
            n
        }
        Tok::StringLit(s) => {
            t.next()?;
            s
        }
        other => {
            return Err(crate::parser::ParseError::at_zero(format!(
                "{option} requires an unsigned integer, found {other:?}"
            )))
        }
    };
    raw.parse::<u64>().map_err(|_| {
        crate::parser::ParseError::at_zero(format!("invalid unsigned integer '{raw}' for {option}"))
    })
}

fn parse_subpartition_list(
    t: &mut Tokeniser<'_>,
) -> Result<Vec<SubpartitionItemDef>, crate::parser::ParseError> {
    expect_punct(t, Tok::LParen)?;
    let mut out = Vec::new();
    loop {
        if matches!(t.peek()?, Tok::RParen) {
            t.next()?;
            return Ok(out);
        }
        out.push(parse_subpartition_item(t)?);
        match t.next()? {
            Tok::Comma => continue,
            Tok::RParen => return Ok(out),
            other => {
                return Err(crate::parser::ParseError::at_zero(format!(
                    "expected `,` or `)` in subpartition list, got {:?}",
                    other
                )))
            }
        }
    }
}

fn parse_subpartition_item(
    t: &mut Tokeniser<'_>,
) -> Result<SubpartitionItemDef, crate::parser::ParseError> {
    expect_ident_ci(t, "SUBPARTITION")?;
    let name = match t.next()? {
        Tok::Ident(s) | Tok::QuotedIdent(s) => s,
        other => {
            return Err(crate::parser::ParseError::at_zero(format!(
                "expected subpartition name, got {:?}",
                other
            )))
        }
    };
    let mut comment = String::new();
    let mut options = PartitionOptionsDef::default();
    loop {
        match t.peek()?.clone() {
            Tok::Ident(s) if s.eq_ignore_ascii_case("COMMENT") => {
                t.next()?;
                if matches!(t.peek()?, Tok::Equal) {
                    t.next()?;
                }
                comment = expect_string_literal(t)?;
            }
            Tok::Ident(s) => {
                let lower = s.to_ascii_lowercase();
                if !parse_partition_option_into(t, &lower, &mut options)? {
                    break;
                }
            }
            _ => break,
        }
    }
    Ok(SubpartitionItemDef {
        name,
        comment,
        options,
    })
}

fn parse_literal_tuple(
    t: &mut Tokeniser<'_>,
) -> Result<Vec<PartitionLiteralDef>, crate::parser::ParseError> {
    expect_punct(t, Tok::LParen)?;
    let mut out = Vec::new();
    loop {
        out.push(parse_one_literal(t)?);
        match t.next()? {
            Tok::Comma => continue,
            Tok::RParen => return Ok(out),
            other => {
                return Err(crate::parser::ParseError::at_zero(format!(
                    "expected `,` or `)` in literal list, got {:?}",
                    other
                )))
            }
        }
    }
}

/// Parses the `IN (...)` clause for LIST partitions. Single-column
/// LIST: `VALUES IN (1, 2, 3)`. LIST COLUMNS: `VALUES IN ((1,'a'),
/// (2,'b'))`.
fn parse_list_in_values(
    t: &mut Tokeniser<'_>,
    columns: bool,
) -> Result<Vec<Vec<PartitionLiteralDef>>, crate::parser::ParseError> {
    expect_punct(t, Tok::LParen)?;
    let mut out = Vec::new();
    if columns {
        // D5: MySQL accepts both `LIST COLUMNS(c) VALUES IN (1,2,3)`
        // (single-column, bare literals) and `LIST COLUMNS(c1,c2) VALUES
        // IN ((1,'a'),(2,'b'))` (multi-column tuples). Peek the first
        // token to decide: `(` starts a tuple entry, anything else is a
        // bare literal — treat as a one-element tuple.
        loop {
            let tuple = if matches!(t.peek()?, Tok::LParen) {
                parse_literal_tuple_inline(t)?
            } else {
                vec![parse_one_literal(t)?]
            };
            out.push(tuple);
            match t.next()? {
                Tok::Comma => continue,
                Tok::RParen => return Ok(out),
                other => {
                    return Err(crate::parser::ParseError::at_zero(format!(
                        "expected `,` or `)` in LIST COLUMNS values, got {:?}",
                        other
                    )))
                }
            }
        }
    } else {
        loop {
            let lit = parse_one_literal(t)?;
            out.push(vec![lit]);
            match t.next()? {
                Tok::Comma => continue,
                Tok::RParen => return Ok(out),
                other => {
                    return Err(crate::parser::ParseError::at_zero(format!(
                        "expected `,` or `)` in LIST values, got {:?}",
                        other
                    )))
                }
            }
        }
    }
}

/// `( literal, literal, ... )` — assumes the opening paren has not
/// yet been consumed (called from the LIST COLUMNS tuple loop).
fn parse_literal_tuple_inline(
    t: &mut Tokeniser<'_>,
) -> Result<Vec<PartitionLiteralDef>, crate::parser::ParseError> {
    expect_punct(t, Tok::LParen)?;
    let mut out = Vec::new();
    loop {
        out.push(parse_one_literal(t)?);
        match t.next()? {
            Tok::Comma => continue,
            Tok::RParen => return Ok(out),
            other => {
                return Err(crate::parser::ParseError::at_zero(format!(
                    "expected `,` or `)` in LIST COLUMNS tuple, got {:?}",
                    other
                )))
            }
        }
    }
}

fn parse_one_literal(
    t: &mut Tokeniser<'_>,
) -> Result<PartitionLiteralDef, crate::parser::ParseError> {
    let start = t.position_before_peek()?;
    if matches!(t.peek()?, Tok::Ident(name) if name.eq_ignore_ascii_case("MAXVALUE")) {
        t.next()?;
        return Ok(PartitionLiteralDef::MaxValue);
    }
    let mut depth = 0usize;
    loop {
        match t.peek()? {
            Tok::Comma | Tok::RParen if depth == 0 => break,
            Tok::LParen => depth += 1,
            Tok::RParen => depth -= 1,
            Tok::Eof => {
                return Err(crate::parser::ParseError::at_zero(
                    "unterminated partition value",
                ))
            }
            _ => {}
        }
        t.next()?;
    }
    let end = t.position_before_peek()?;
    let source = &t.src[start..end];
    let expr =
        crate::parser::parse_expr_fragment(source).map_err(|error| error.rebase(t.src, start))?;
    if let crate::parser::Expr::Identifier(name) = &expr {
        let mut error =
            crate::parser::ParseError::at_zero(format!("Unknown column '{name}' in 'field list'"));
        error.spec = crate::error::catalog::ER_BAD_FIELD_ERROR;
        return Err(error);
    }
    if let crate::parser::Expr::Literal(literal) = &expr {
        return Ok(PartitionLiteralDef::Value(
            crate::engine::expr::literal_value(literal),
        ));
    }
    if matches!(
        expr,
        crate::parser::Expr::Default(_) | crate::parser::Expr::DefaultQualified(_)
    ) {
        return Err(crate::parser::ParseError::at_zero(
            "DEFAULT is not allowed in partition values",
        ));
    }
    // Constant-function and type restrictions belong to partition DDL binding.
    // Canonical SQL preserves the creating session's lexical interpretation.
    let canonical = crate::parser::expr_to_catalog_sql(&expr)
        .map_err(|error| crate::parser::ParseError::at_zero(error.to_string()))?;
    Ok(PartitionLiteralDef::Expression(canonical))
}

fn expect_string_literal(t: &mut Tokeniser<'_>) -> Result<String, crate::parser::ParseError> {
    match t.next()? {
        Tok::StringLit(s) => Ok(s),
        other => Err(crate::parser::ParseError::at_zero(format!(
            "expected string literal, got {:?}",
            other
        ))),
    }
}

fn expect_punct(t: &mut Tokeniser<'_>, expected: Tok) -> Result<(), crate::parser::ParseError> {
    let got = t.next()?;
    if std::mem::discriminant(&got) == std::mem::discriminant(&expected) {
        return Ok(());
    }
    Err(crate::parser::ParseError::at_zero(format!(
        "expected {:?}, got {:?}",
        expected, got
    )))
}

fn parse_unsigned_int(t: &mut Tokeniser<'_>) -> Result<u32, crate::parser::ParseError> {
    match t.next()? {
        Tok::Number(n) => n
            .parse::<u32>()
            .map_err(|_| crate::parser::ParseError::at_zero(format!("invalid integer `{}`", n))),
        other => Err(crate::parser::ParseError::at_zero(format!(
            "expected unsigned integer, got {:?}",
            other
        ))),
    }
}

/// Detect ALTER TABLE partition actions in the SQL. If found, strip
/// the action clauses from the SQL (replacing them with a no-op
/// placeholder sqlparser can ignore) and return them as a list of
/// [`PartitionAction`].
///
/// We recognise the following case-insensitive heads after `ALTER
/// TABLE <name>`:
/// `ADD PARTITION`, `DROP PARTITION`, `TRUNCATE PARTITION`,
/// `ANALYZE PARTITION`, `OPTIMIZE PARTITION`, `CHECK PARTITION`,
/// `REPAIR PARTITION`, `REBUILD PARTITION`, `COALESCE PARTITION`,
/// `REORGANIZE PARTITION`, `EXCHANGE PARTITION`, `REMOVE PARTITIONING`,
/// and a top-level `PARTITION BY` (re-partition).
pub fn extract_alter_partition_actions(
    sql: &str,
) -> Result<(String, Vec<PartitionAction>), crate::parser::ParseError> {
    // Skip "ALTER TABLE <name>" by tokenising up to and past the name.
    let mut t = Tokeniser::new(sql);
    if !try_consume_ident_ci(&mut t, "ALTER")? {
        return Ok((sql.to_string(), Vec::new()));
    }
    if !try_consume_ident_ci(&mut t, "TABLE")? {
        return Ok((sql.to_string(), Vec::new()));
    }
    // ignore optional IF EXISTS and the table name (which may be
    // schema-qualified with a dot — we just need to skip until the
    // start of the action list).
    if try_consume_ident_ci(&mut t, "IF")? {
        let _ = try_consume_ident_ci(&mut t, "EXISTS")?;
    }
    // skip table name (and any backtick/quoted identifier)
    match t.next()? {
        Tok::Ident(_) | Tok::QuotedIdent(_) => {}
        _ => return Ok((sql.to_string(), Vec::new())),
    }
    // optional .name
    if let Tok::Other('.') = t.peek()? {
        t.next()?;
        match t.next()? {
            Tok::Ident(_) | Tok::QuotedIdent(_) => {}
            _ => return Ok((sql.to_string(), Vec::new())),
        }
    }

    let action_start = t.position_before_peek()?;
    let mut actions = Vec::new();
    let mut consumed_any = false;
    // D2: walk the whole comma-separated action list, extracting
    // partition actions from any position and preserving the ranges of
    // non-partition actions for sqlparser. Previously the loop broke on
    // the first non-partition action, so `ALTER TABLE t ADD COLUMN c
    // INT, DROP PARTITION p0` never saw the DROP PARTITION.
    let mut kept_action_ranges: Vec<(usize, usize)> = Vec::new();

    loop {
        // Skip stray commas (a leading comma before the very first
        // action shouldn't happen, but tolerate it defensively).
        if matches!(t.peek()?, Tok::Comma) {
            t.next()?;
        }
        if matches!(t.peek()?, Tok::Eof) {
            break;
        }
        let action_start_pos = t.position_before_peek()?;
        let head = t.peek()?.clone();
        let kw = ident_lower(&head);
        let action_or_none = match kw.as_str() {
            "add" => parse_add_partition(&mut t)?,
            "drop" => parse_drop_partition(&mut t)?,
            "truncate" => parse_simple_partition_targets(&mut t, |targets| {
                PartitionAction::TruncatePartitions(targets)
            })?,
            "analyze" => parse_simple_partition_targets(&mut t, |targets| {
                PartitionAction::AnalyzePartitions(targets)
            })?,
            "optimize" => parse_simple_partition_targets(&mut t, |targets| {
                PartitionAction::OptimizePartitions(targets)
            })?,
            "check" => parse_simple_partition_targets(&mut t, |targets| {
                PartitionAction::CheckPartitions(targets)
            })?,
            "repair" => parse_simple_partition_targets(&mut t, |targets| {
                PartitionAction::RepairPartitions(targets)
            })?,
            "rebuild" => parse_simple_partition_targets(&mut t, |targets| {
                PartitionAction::RebuildPartitions(targets)
            })?,
            "coalesce" => parse_coalesce(&mut t)?,
            "reorganize" => parse_reorganize(&mut t)?,
            "exchange" => parse_exchange(&mut t)?,
            "remove" => parse_remove_partitioning(&mut t)?,
            "partition" => parse_partition_by_action(&mut t)?,
            _ => None,
        };
        match action_or_none {
            Some(action) => {
                actions.push(action);
                consumed_any = true;
            }
            None => {
                // Not a partition action — skip past it (paren-balanced
                // until the next top-level comma or EOF) and record the
                // source range so we can pass it back to sqlparser.
                let action_end_pos = advance_past_alter_action(&mut t)?;
                kept_action_ranges.push((action_start_pos, action_end_pos));
            }
        }
        // Consume optional inter-action comma; keep going regardless.
        if matches!(t.peek()?, Tok::Comma) {
            t.next()?;
            if matches!(t.peek()?, Tok::Eof | Tok::Other(';')) {
                return Err(crate::parser::ParseError::at_zero(
                    "Expected ALTER action after comma",
                ));
            }
            continue;
        }
        if consumed_any {
            if matches!(t.peek()?, Tok::Other(';')) {
                t.next()?;
            }
            if !matches!(t.peek()?, Tok::Eof) {
                return Err(crate::parser::ParseError::at_zero(
                    "Unexpected trailing token after partition action",
                ));
            }
        }
        break;
    }

    if !consumed_any {
        return Ok((sql.to_string(), Vec::new()));
    }
    // Reconstruct the SQL: keep the "ALTER TABLE <name>" prefix, then
    // re-emit only the ranges that belong to non-partition actions,
    // comma-separated.
    let mut stripped = String::with_capacity(sql.len());
    stripped.push_str(&sql[..action_start]);
    let mut first = true;
    for (a, b) in &kept_action_ranges {
        if *a >= *b {
            continue;
        }
        let slice = sql[*a..*b].trim().trim_end_matches(',').trim();
        if slice.is_empty() {
            continue;
        }
        if first {
            stripped.push(' ');
            first = false;
        } else {
            stripped.push_str(", ");
        }
        stripped.push_str(slice);
    }
    let cleaned = stripped.trim().trim_end_matches(';').trim().to_string();
    // sqlparser requires ALTER TABLE to have at least one action. If
    // we stripped every non-partition action, append a no-op
    // `ALGORITHM=DEFAULT` so the statement parses; our executor
    // ignores algorithm hints by themselves.
    let needs_noop = first;
    let final_sql = if needs_noop {
        format!("{} ALGORITHM=DEFAULT", cleaned)
    } else {
        cleaned
    };
    Ok((final_sql, actions))
}

fn parse_add_partition(
    t: &mut Tokeniser<'_>,
) -> Result<Option<PartitionAction>, crate::parser::ParseError> {
    let saved = t.peeked.clone();
    let saved_chars = t.chars.clone();
    let saved_pos = t.next_pos;
    let _ = t.next()?; // ADD
    if !try_consume_ident_ci(t, "PARTITION")? {
        t.peeked = saved;
        t.chars = saved_chars;
        t.next_pos = saved_pos;
        return Ok(None);
    }
    // ADD PARTITION (PARTITION pN VALUES ...) or ADD PARTITION PARTITIONS N.
    let action = if matches!(t.peek()?, Tok::LParen) {
        // Parser is happy to receive RANGE or LIST shape; we pass
        // `Range` to keep the same `VALUES LESS THAN/IN` discovery
        // path inside the item parser — the item parser inspects the
        // VALUES keyword and produces the right bounds variant.
        PartitionAction::AddPartitions(parse_partition_list_inferred(t)?)
    } else if try_consume_ident_ci(t, "PARTITIONS")? {
        PartitionAction::AddPartitionCount(parse_unsigned_int(t)?)
    } else {
        return Err(crate::parser::ParseError::at_zero(
            "ADD PARTITION requires partition definitions or PARTITIONS count",
        ));
    };
    Ok(Some(action))
}

/// `parse_partition_list` but accepts both LESS THAN and IN bounds in
/// a single list — used by ADD PARTITION which doesn't repeat the
/// `RANGE`/`LIST` keyword.
fn parse_partition_list_inferred(
    t: &mut Tokeniser<'_>,
) -> Result<Vec<PartitionItemDef>, crate::parser::ParseError> {
    expect_punct(t, Tok::LParen)?;
    let mut out = Vec::new();
    loop {
        let item = parse_partition_item_inferred(t)?;
        out.push(item);
        match t.next()? {
            Tok::Comma => continue,
            Tok::RParen => return Ok(out),
            other => {
                return Err(crate::parser::ParseError::at_zero(format!(
                    "expected `,` or `)` in partition list, got {:?}",
                    other
                )))
            }
        }
    }
}

fn parse_partition_item_inferred(
    t: &mut Tokeniser<'_>,
) -> Result<PartitionItemDef, crate::parser::ParseError> {
    expect_ident_ci(t, "PARTITION")?;
    let name = match t.next()? {
        Tok::Ident(s) | Tok::QuotedIdent(s) => s,
        other => {
            return Err(crate::parser::ParseError::at_zero(format!(
                "expected partition name, got {:?}",
                other
            )))
        }
    };
    let mut comment = String::new();
    expect_ident_ci(t, "VALUES")?;
    let bounds = if try_consume_ident_ci(t, "LESS")? {
        expect_ident_ci(t, "THAN")?;
        if try_consume_ident_ci(t, "MAXVALUE")? {
            PartitionBoundsDef::RangeLessThan(vec![PartitionLiteralDef::MaxValue])
        } else {
            PartitionBoundsDef::RangeLessThan(parse_literal_tuple(t)?)
        }
    } else if try_consume_ident_ci(t, "IN")? {
        // Heuristic: if the first token after `(` is `(`, it's
        // LIST COLUMNS; otherwise it's plain LIST.
        let mut probe = Tokeniser {
            chars: t.chars.clone(),
            src: t.src,
            peeked: t.peeked.clone(),
            next_pos: t.next_pos,
        };
        let _ = probe.next()?;
        let is_tuple = matches!(probe.peek()?, Tok::LParen);
        PartitionBoundsDef::ListIn(parse_list_in_values(t, is_tuple)?)
    } else {
        return Err(crate::parser::ParseError::at_zero(
            "expected `LESS THAN ...` or `IN (...)` in PARTITION VALUES",
        ));
    };
    let mut options = PartitionOptionsDef::default();
    loop {
        match t.peek()?.clone() {
            Tok::Ident(s) if s.eq_ignore_ascii_case("COMMENT") => {
                t.next()?;
                if matches!(t.peek()?, Tok::Equal) {
                    t.next()?;
                }
                comment = expect_string_literal(t)?;
            }
            Tok::Ident(s) => {
                let lower = s.to_ascii_lowercase();
                if !parse_partition_option_into(t, &lower, &mut options)? {
                    break;
                }
            }
            _ => break,
        }
    }
    // D3: an ADD PARTITION whose target table has an implicit
    // subpartition scheme (SUBPARTITION BY HASH(...) without SUBPARTITIONS n)
    // requires the ADD to list its subpartitions explicitly, e.g.
    //   ADD PARTITION (
    //     PARTITION p2 VALUES LESS THAN (30)
    //       (SUBPARTITION p2sp0, SUBPARTITION p2sp1)
    //   )
    // Previously `parse_partition_item_inferred` returned
    // `subpartitions: Vec::new()`, causing `build_partition_info_from_def`
    // to see `count=0` and fail. Parse the optional trailing
    // `(SUBPARTITION ...)` block using the same helper as CREATE TABLE.
    let subpartitions = if matches!(t.peek()?, Tok::LParen) {
        parse_subpartition_list(t)?
    } else {
        Vec::new()
    };
    Ok(PartitionItemDef {
        name,
        bounds,
        comment,
        options,
        subpartitions,
    })
}

fn parse_drop_partition(
    t: &mut Tokeniser<'_>,
) -> Result<Option<PartitionAction>, crate::parser::ParseError> {
    let saved = t.peeked.clone();
    let saved_chars = t.chars.clone();
    let saved_pos = t.next_pos;
    let _ = t.next()?; // DROP
    if !try_consume_ident_ci(t, "PARTITION")? {
        t.peeked = saved;
        t.chars = saved_chars;
        t.next_pos = saved_pos;
        return Ok(None);
    }
    let names = parse_name_list(t)?;
    Ok(Some(PartitionAction::DropPartitions(names)))
}

fn parse_name_list(t: &mut Tokeniser<'_>) -> Result<Vec<String>, crate::parser::ParseError> {
    let mut out = Vec::new();
    loop {
        match t.next()? {
            Tok::Ident(s) | Tok::QuotedIdent(s) => out.push(s),
            other => {
                return Err(crate::parser::ParseError::at_zero(format!(
                    "expected partition name, got {:?}",
                    other
                )))
            }
        }
        match t.peek()? {
            Tok::Comma => {
                // D1: after the comma, look ahead one token. If the next
                // ident is a top-level ALTER action keyword (ADD, DROP,
                // MODIFY, LOCK, ALGORITHM, …), the comma is the ALTER
                // action separator — don't consume it and don't add the
                // keyword to the partition name list. Only real partition
                // names, including quoted action keywords, continue the loop.
                let saved_peeked = t.peeked.clone();
                let saved_chars = t.chars.clone();
                let saved_pos = t.next_pos;
                t.next()?; // consume comma
                if let Tok::Ident(s) = t.peek()? {
                    if is_alter_action_boundary_keyword(s) {
                        // Restore the comma so the caller sees a clean
                        // action separator.
                        t.peeked = saved_peeked;
                        t.chars = saved_chars;
                        t.next_pos = saved_pos;
                        return Ok(out);
                    }
                }
                continue;
            }
            _ => return Ok(out),
        }
    }
}

/// D2: skip past a non-partition ALTER action token stream so we can
/// preserve it for sqlparser. Consume tokens (paren-balanced) until we
/// hit a top-level comma or EOF; do NOT consume the trailing comma.
/// Returns the source position just past the last consumed token.
fn advance_past_alter_action(t: &mut Tokeniser<'_>) -> Result<usize, crate::parser::ParseError> {
    let mut depth: i32 = 0;
    loop {
        match t.peek()? {
            Tok::Eof => return t.position_before_peek(),
            Tok::LParen => {
                t.next()?;
                depth += 1;
            }
            Tok::RParen => {
                if depth == 0 {
                    // Unbalanced RParen at depth 0 — treat as end.
                    return t.position_before_peek();
                }
                t.next()?;
                depth -= 1;
            }
            Tok::Comma if depth == 0 => {
                return t.position_before_peek();
            }
            _ => {
                t.next()?;
            }
        }
    }
}

/// D1: keywords that unambiguously start a new ALTER TABLE action —
/// used to bound the list of partition names in `parse_name_list` so
/// `ALTER TABLE t DROP PARTITION p0, ADD COLUMN c INT` doesn't slurp
/// `ADD`/`COLUMN` into the partition name list. Matches the set of
/// leading keywords `extract_alter_partition_actions` and sqlparser's
/// ALTER TABLE action grammar recognise; not every one belongs to a
/// partition action but any of them ends the current one.
fn is_alter_action_boundary_keyword(s: &str) -> bool {
    matches!(
        s.to_ascii_uppercase().as_str(),
        "ADD"
            | "DROP"
            | "MODIFY"
            | "CHANGE"
            | "RENAME"
            | "ALTER"
            | "DISABLE"
            | "ENABLE"
            | "ORDER"
            | "DISCARD"
            | "IMPORT"
            | "TRUNCATE"
            | "COALESCE"
            | "REORGANIZE"
            | "ANALYZE"
            | "CHECK"
            | "REPAIR"
            | "OPTIMIZE"
            | "REBUILD"
            | "EXCHANGE"
            | "REMOVE"
            | "ALGORITHM"
            | "LOCK"
            | "CONVERT"
            | "FORCE"
            | "PARTITION"
    )
}

fn parse_simple_partition_targets<F: FnOnce(PartitionTargets) -> PartitionAction>(
    t: &mut Tokeniser<'_>,
    ctor: F,
) -> Result<Option<PartitionAction>, crate::parser::ParseError> {
    let saved = t.peeked.clone();
    let saved_chars = t.chars.clone();
    let saved_pos = t.next_pos;
    let _ = t.next()?; // verb
    if !try_consume_ident_ci(t, "PARTITION")? {
        t.peeked = saved;
        t.chars = saved_chars;
        t.next_pos = saved_pos;
        return Ok(None);
    }
    let targets = if try_consume_ident_ci(t, "ALL")? {
        PartitionTargets::All
    } else {
        PartitionTargets::Names(parse_name_list(t)?)
    };
    Ok(Some(ctor(targets)))
}

fn parse_coalesce(
    t: &mut Tokeniser<'_>,
) -> Result<Option<PartitionAction>, crate::parser::ParseError> {
    let saved = t.peeked.clone();
    let saved_chars = t.chars.clone();
    let saved_pos = t.next_pos;
    let _ = t.next()?; // COALESCE
    if !try_consume_ident_ci(t, "PARTITION")? {
        t.peeked = saved;
        t.chars = saved_chars;
        t.next_pos = saved_pos;
        return Ok(None);
    }
    let n = parse_unsigned_int(t)?;
    Ok(Some(PartitionAction::CoalescePartitions(n)))
}

fn parse_reorganize(
    t: &mut Tokeniser<'_>,
) -> Result<Option<PartitionAction>, crate::parser::ParseError> {
    let saved = t.peeked.clone();
    let saved_chars = t.chars.clone();
    let saved_pos = t.next_pos;
    let _ = t.next()?; // REORGANIZE
    if !try_consume_ident_ci(t, "PARTITION")? {
        t.peeked = saved;
        t.chars = saved_chars;
        t.next_pos = saved_pos;
        return Ok(None);
    }
    let old = parse_name_list(t)?;
    expect_ident_ci(t, "INTO")?;
    let news = parse_partition_list_inferred(t)?;
    Ok(Some(PartitionAction::ReorganizePartitions {
        old_partitions: old,
        new_partitions: news,
    }))
}

fn parse_exchange(
    t: &mut Tokeniser<'_>,
) -> Result<Option<PartitionAction>, crate::parser::ParseError> {
    let saved = t.peeked.clone();
    let saved_chars = t.chars.clone();
    let saved_pos = t.next_pos;
    let _ = t.next()?; // EXCHANGE
    if !try_consume_ident_ci(t, "PARTITION")? {
        t.peeked = saved;
        t.chars = saved_chars;
        t.next_pos = saved_pos;
        return Ok(None);
    }
    let pname = match t.next()? {
        Tok::Ident(s) | Tok::QuotedIdent(s) => s,
        other => {
            return Err(crate::parser::ParseError::at_zero(format!(
                "expected partition name, got {:?}",
                other
            )))
        }
    };
    expect_ident_ci(t, "WITH")?;
    expect_ident_ci(t, "TABLE")?;
    // D5: MySQL accepts a qualified `db.tbl` after WITH TABLE. Parse
    // an optional `.<ident>` continuation and re-join into the stored
    // table name so downstream lookup (which itself understands
    // `db.tbl`) resolves the exchange target correctly.
    let mut other_table = match t.next()? {
        Tok::Ident(s) | Tok::QuotedIdent(s) => s,
        other => {
            return Err(crate::parser::ParseError::at_zero(format!(
                "expected table name, got {:?}",
                other
            )))
        }
    };
    if matches!(t.peek()?, Tok::Other('.')) {
        t.next()?; // consume '.'
        let tail = match t.next()? {
            Tok::Ident(s) | Tok::QuotedIdent(s) => s,
            other => {
                return Err(crate::parser::ParseError::at_zero(format!(
                    "expected table name after `.`, got {:?}",
                    other
                )))
            }
        };
        other_table = format!("{other_table}.{tail}");
    }
    let mut with_validation = true;
    if try_consume_ident_ci(t, "WITH")? {
        expect_ident_ci(t, "VALIDATION")?;
        with_validation = true;
    } else if try_consume_ident_ci(t, "WITHOUT")? {
        expect_ident_ci(t, "VALIDATION")?;
        with_validation = false;
    }
    Ok(Some(PartitionAction::ExchangePartition {
        partition: pname,
        table: other_table,
        with_validation,
    }))
}

fn parse_remove_partitioning(
    t: &mut Tokeniser<'_>,
) -> Result<Option<PartitionAction>, crate::parser::ParseError> {
    let saved = t.peeked.clone();
    let saved_chars = t.chars.clone();
    let saved_pos = t.next_pos;
    let _ = t.next()?; // REMOVE
    if !try_consume_ident_ci(t, "PARTITIONING")? {
        t.peeked = saved;
        t.chars = saved_chars;
        t.next_pos = saved_pos;
        return Ok(None);
    }
    Ok(Some(PartitionAction::RemovePartitioning))
}

fn parse_partition_by_action(
    t: &mut Tokeniser<'_>,
) -> Result<Option<PartitionAction>, crate::parser::ParseError> {
    let saved = t.peeked.clone();
    let saved_chars = t.chars.clone();
    let saved_pos = t.next_pos;
    let _ = t.next()?; // PARTITION
    if !try_consume_ident_ci(t, "BY")? {
        t.peeked = saved;
        t.chars = saved_chars;
        t.next_pos = saved_pos;
        return Ok(None);
    }
    let def = parse_partition_def(t)?;
    Ok(Some(PartitionAction::PartitionBy(def)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_range_simple() {
        let sql = "CREATE TABLE t (id INT, year INT) PARTITION BY RANGE (year) (\n  PARTITION p0 VALUES LESS THAN (2020),\n  PARTITION p1 VALUES LESS THAN (2025),\n  PARTITION p2 VALUES LESS THAN MAXVALUE\n)";
        let (stripped, def) = extract_create_table_partition(sql).unwrap();
        let def = def.unwrap();
        assert_eq!(def.method, PartitionMethod::Range);
        assert_eq!(def.expr_sql.as_deref(), Some("year"));
        assert_eq!(def.partitions.len(), 3);
        assert_eq!(def.partitions[0].name, "p0");
        match &def.partitions[2].bounds {
            PartitionBoundsDef::RangeLessThan(v) => {
                assert!(matches!(v[0], PartitionLiteralDef::MaxValue));
            }
            _ => panic!(),
        }
        assert!(!stripped.contains("PARTITION"));
    }

    #[test]
    fn parse_hash_count_only() {
        let sql = "CREATE TABLE t (id INT) PARTITION BY HASH(id) PARTITIONS 4";
        let (_, def) = extract_create_table_partition(sql).unwrap();
        let def = def.unwrap();
        assert_eq!(def.method, PartitionMethod::Hash);
        assert_eq!(def.partition_count, Some(4));
        assert!(def.partitions.is_empty());
    }

    #[test]
    fn parse_list_columns() {
        let sql = "CREATE TABLE t (a INT, b VARCHAR(8)) PARTITION BY LIST COLUMNS(a,b) (PARTITION p0 VALUES IN ((1,'x'),(2,'y')), PARTITION p1 VALUES IN ((3,'z')))";
        let (_, def) = extract_create_table_partition(sql).unwrap();
        let def = def.unwrap();
        assert_eq!(def.method, PartitionMethod::ListColumns);
        assert_eq!(def.columns, vec!["a", "b"]);
        assert_eq!(def.partitions.len(), 2);
    }

    #[test]
    fn parse_key_with_algorithm() {
        let sql = "CREATE TABLE t (a INT) PARTITION BY KEY ALGORITHM=2 (a) PARTITIONS 8";
        let (_, def) = extract_create_table_partition(sql).unwrap();
        let def = def.unwrap();
        assert_eq!(def.method, PartitionMethod::Key);
        assert_eq!(def.key_algorithm, Some(2));
        assert_eq!(def.partition_count, Some(8));
    }

    #[test]
    fn parse_alter_drop_partition() {
        let sql = "ALTER TABLE t DROP PARTITION p0, p1";
        let (_, actions) = extract_alter_partition_actions(sql).unwrap();
        assert_eq!(actions.len(), 1);
        assert!(matches!(
            &actions[0],
            PartitionAction::DropPartitions(v) if v == &vec!["p0".to_string(), "p1".to_string()]
        ));
    }
}

#[cfg(test)]
mod token_contract_tests {
    use super::*;
    #[test]
    fn quoted_all_is_a_partition_name() {
        let (_, actions) =
            extract_alter_partition_actions("ALTER TABLE t TRUNCATE PARTITION `ALL`").unwrap();
        assert_eq!(
            actions,
            vec![PartitionAction::TruncatePartitions(
                PartitionTargets::Names(vec!["ALL".into()])
            )]
        );
    }
    #[test]
    fn quoted_action_words_remain_in_name_list() {
        let (_, actions) =
            extract_alter_partition_actions("ALTER TABLE t DROP PARTITION p0, `ADD`").unwrap();
        assert_eq!(
            actions,
            vec![PartitionAction::DropPartitions(vec![
                "p0".into(),
                "ADD".into()
            ])]
        );
    }
    #[test]
    fn lexical_errors_are_not_partition_eof() {
        for sql in [
            "ALTER TABLE t DROP PARTITION p0 /* unfinished",
            "ALTER TABLE t TRUNCATE PARTITION ALL '",
        ] {
            let err = extract_alter_partition_actions(sql).unwrap_err();
            assert!(err.span.start > 0, "{sql}: {err:?}");
        }
    }
}

#[cfg(test)]
mod source_contract_tests {
    use super::*;
    #[test]
    fn quoted_keywords_do_not_route_partition_grammar() {
        assert_eq!(find_partition_by("SELECT `PARTITION` BY").unwrap(), None);
        for sql in [
            "CREATE TABLE t(id INT) PARTITION BY `HASH`(id)",
            "CREATE TABLE t(id INT) PARTITION BY HASH(id) `PARTITIONS` 2",
        ] {
            assert!(crate::parser::parse_statement(sql).is_err(), "{sql}");
        }
        let (_, actions) = extract_alter_partition_actions(
            "/* header */ ALTER /* table */ TABLE `t` TRUNCATE PARTITION `ALL`",
        )
        .unwrap();
        assert_eq!(
            actions,
            vec![PartitionAction::TruncatePartitions(
                PartitionTargets::Names(vec!["ALL".into()])
            )]
        );
    }
    #[test]
    fn lexical_error_uses_original_create_coordinates() {
        let sql = "/* 中文 */\nCREATE TABLE t(id INT) PARTITION BY HASH(id) PARTITIONS 2 /* broken";
        let err = extract_create_table_partition(sql).unwrap_err();
        assert_eq!(err.span.start, sql.rfind("/*").unwrap());
        assert_eq!(err.line, 2);
        let sql = "ALTER TABLE t DROP PARTITION p0 '\u{1f600}";
        let err = extract_alter_partition_actions(sql).unwrap_err();
        assert_eq!(err.span.start, sql.find('\'').unwrap());
    }
}
