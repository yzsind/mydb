//! Lightweight MySQL lexical segmentation shared by the parser, protocol
//! layer, cache normalizer, and SQL prefix classifiers.
//!
//! This is intentionally not a SQL tokenizer. It only identifies quoted
//! regions and MySQL comment forms so byte-oriented structural scanners never
//! interpret comment text as executable SQL.

use std::borrow::Cow;
use std::fmt;
use std::ops::Range;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct SqlLexMode {
    pub ansi_quotes: bool,
    pub no_backslash_escapes: bool,
}

impl SqlLexMode {
    pub(crate) fn from_parser_bits(bits: u16) -> Self {
        Self {
            ansi_quotes: bits & crate::engine::api::SQL_MODE_ANSI_QUOTES != 0,
            no_backslash_escapes: bits & crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES != 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SqlSegmentKind {
    Code,
    Quoted {
        quote: u8,
    },
    LineComment,
    BlockComment,
    OptimizerHint,
    ExecutableComment {
        gate: Option<u32>,
        active: bool,
        body: Range<usize>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SqlSegment {
    pub range: Range<usize>,
    pub kind: SqlSegmentKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SqlLexError {
    pub offset: usize,
    pub message: &'static str,
}

impl SqlLexError {
    fn rebase(mut self, offset: usize) -> Self {
        self.offset += offset;
        self
    }
}

impl fmt::Display for SqlLexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} at byte {}", self.message, self.offset)
    }
}

impl std::error::Error for SqlLexError {}

pub(crate) struct SqlSegments<'a> {
    bytes: &'a [u8],
    pos: usize,
    mode: SqlLexMode,
    server_version: u32,
}

pub(crate) fn segments(sql: &str, mode: SqlLexMode, server_version: u32) -> SqlSegments<'_> {
    segments_bytes(sql.as_bytes(), mode, server_version)
}

pub(crate) fn segments_bytes(
    bytes: &[u8],
    mode: SqlLexMode,
    server_version: u32,
) -> SqlSegments<'_> {
    SqlSegments {
        bytes,
        pos: 0,
        mode,
        server_version,
    }
}

#[inline]
fn dash_starts_comment(bytes: &[u8], pos: usize) -> bool {
    bytes.get(pos) == Some(&b'-')
        && bytes.get(pos + 1) == Some(&b'-')
        && bytes
            .get(pos + 2)
            .is_none_or(|next| next.is_ascii_whitespace() || next.is_ascii_control())
}

pub(crate) fn is_dash_line_comment_at(sql: &str, pos: usize) -> bool {
    dash_starts_comment(sql.as_bytes(), pos)
}

#[inline]
fn hash_starts_comment(bytes: &[u8], pos: usize) -> bool {
    bytes.get(pos) == Some(&b'#')
        && !(pos > 0 && bytes[pos - 1] == b'<' && bytes.get(pos + 1) == Some(&b'>'))
}

fn parse_gate(body: &[u8]) -> (Option<u32>, usize) {
    // MySQL 8.0 consumes exactly five version digits; shorter numeric
    // prefixes are executable SQL, and any sixth digit belongs to the body.
    let Some(prefix) = body
        .get(..5)
        .filter(|prefix| prefix.iter().all(u8::is_ascii_digit))
    else {
        return (None, 0);
    };
    let version = prefix
        .iter()
        .fold(0u32, |value, digit| value * 10 + (digit - b'0') as u32);
    (Some(version), 5)
}

impl Iterator for SqlSegments<'_> {
    type Item = Result<SqlSegment, SqlLexError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.bytes.len() {
            return None;
        }
        let start = self.pos;
        let b = self.bytes[start];

        if matches!(b, b'\'' | b'"' | b'`') {
            let quote = b;
            let hex_literal = quote == b'\''
                && start > 0
                && matches!(self.bytes[start - 1], b'x' | b'X')
                && (start < 2
                    || !(self.bytes[start - 2].is_ascii_alphanumeric()
                        || self.bytes[start - 2] == b'_'));
            if hex_literal {
                let end = memchr::memchr(b'\'', &self.bytes[start + 1..])
                    .map(|offset| start + 1 + offset);
                if let Some(end) = end {
                    let digits = &self.bytes[start + 1..end];
                    if digits.len() % 2 != 0 || !digits.iter().all(u8::is_ascii_hexdigit) {
                        self.pos = end + 1;
                        return Some(Err(SqlLexError {
                            offset: start - 1,
                            message: "invalid hexadecimal literal",
                        }));
                    }
                }
            }
            let backslash_escapes = match quote {
                b'\'' => !self.mode.no_backslash_escapes,
                b'"' => !self.mode.ansi_quotes && !self.mode.no_backslash_escapes,
                b'`' => false,
                _ => false,
            };
            self.pos += 1;
            while self.pos < self.bytes.len() {
                // SQL payload strings are often hundreds of bytes long while
                // the only interesting bytes are the closing quote and an
                // occasional escape.  `memchr` uses the platform vector path;
                // keeping the state machine only for candidate bytes avoids a
                // scalar branch per payload byte without changing quote,
                // doubled-quote, or SQL-mode semantics.
                let relative = if backslash_escapes {
                    memchr::memchr2(quote, b'\\', &self.bytes[self.pos..])
                } else {
                    memchr::memchr(quote, &self.bytes[self.pos..])
                };
                let Some(relative) = relative else {
                    self.pos = self.bytes.len();
                    break;
                };
                self.pos += relative;
                if backslash_escapes && self.bytes[self.pos] == b'\\' {
                    self.pos = (self.pos + 2).min(self.bytes.len());
                    continue;
                }
                debug_assert_eq!(self.bytes[self.pos], quote);
                if self.bytes.get(self.pos + 1) == Some(&quote) {
                    self.pos += 2;
                    continue;
                }
                self.pos += 1;
                if quote == b'`' || (quote == b'"' && self.mode.ansi_quotes) {
                    if let Some(offset) = memchr::memchr(0, &self.bytes[start..self.pos]) {
                        return Some(Err(SqlLexError {
                            offset: start + offset,
                            message: "NUL is not allowed in an SQL identifier",
                        }));
                    }
                }
                return Some(Ok(SqlSegment {
                    range: start..self.pos,
                    kind: SqlSegmentKind::Quoted { quote },
                }));
            }
            return Some(Err(SqlLexError {
                offset: start,
                message: "unterminated quoted SQL token",
            }));
        }

        if dash_starts_comment(self.bytes, start) || hash_starts_comment(self.bytes, start) {
            self.pos += if b == b'#' { 1 } else { 2 };
            while self.pos < self.bytes.len() && self.bytes[self.pos] != b'\n' {
                self.pos += 1;
            }
            if self.pos < self.bytes.len() {
                self.pos += 1;
            }
            return Some(Ok(SqlSegment {
                range: start..self.pos,
                kind: SqlSegmentKind::LineComment,
            }));
        }

        if b == b'/' && self.bytes.get(start + 1) == Some(&b'*') {
            let mut close = start + 2;
            while close + 1 < self.bytes.len()
                && !(self.bytes[close] == b'*' && self.bytes[close + 1] == b'/')
            {
                close += 1;
            }
            if close + 1 >= self.bytes.len() {
                self.pos = self.bytes.len();
                return Some(Err(SqlLexError {
                    offset: start,
                    message: "unterminated block comment",
                }));
            }
            self.pos = close + 2;
            let kind = match self.bytes.get(start + 2) {
                Some(b'+') => SqlSegmentKind::OptimizerHint,
                Some(b'!') => {
                    let raw_body_start = start + 3;
                    let (gate, digit_len) = parse_gate(&self.bytes[raw_body_start..close]);
                    let body_start = raw_body_start + digit_len;
                    SqlSegmentKind::ExecutableComment {
                        gate,
                        active: gate.is_none_or(|v| v <= self.server_version),
                        body: body_start..close,
                    }
                }
                _ => SqlSegmentKind::BlockComment,
            };
            return Some(Ok(SqlSegment {
                range: start..self.pos,
                kind,
            }));
        }

        // Coalesce ordinary code until the next byte that could start a quote
        // or comment. Non-ASCII bytes are copied as part of the run and never
        // sliced individually.
        self.pos += 1;
        while self.pos < self.bytes.len() {
            let current = self.bytes[self.pos];
            if matches!(current, b'\'' | b'"' | b'`')
                || hash_starts_comment(self.bytes, self.pos)
                || dash_starts_comment(self.bytes, self.pos)
                || (current == b'/' && self.bytes.get(self.pos + 1) == Some(&b'*'))
            {
                break;
            }
            self.pos += 1;
        }
        Some(Ok(SqlSegment {
            range: start..self.pos,
            kind: SqlSegmentKind::Code,
        }))
    }
}

/// SQL with executable-comment delimiters masked in-place. The effective text
/// always has the same byte length and line layout as the original.
#[derive(Debug, Clone)]
pub(crate) struct PreparedSql<'a> {
    effective: Cow<'a, str>,
}

impl<'a> PreparedSql<'a> {
    pub(crate) fn new(
        sql: &'a str,
        mode: SqlLexMode,
        server_version: u32,
    ) -> Result<Self, SqlLexError> {
        let mut executable = Vec::new();
        for segment in segments(sql, mode, server_version) {
            let segment = segment?;
            if matches!(segment.kind, SqlSegmentKind::LineComment)
                && sql
                    .as_bytes()
                    .get(segment.range.start..segment.range.start + 2)
                    == Some(b"--")
                && sql
                    .as_bytes()
                    .get(segment.range.start + 2)
                    .is_some_and(|b| b.is_ascii_control() && !b.is_ascii_whitespace())
            {
                executable.push(segment);
            } else if matches!(segment.kind, SqlSegmentKind::ExecutableComment { .. }) {
                executable.push(segment);
            }
        }
        if executable.is_empty() {
            return Ok(Self {
                effective: Cow::Borrowed(sql),
            });
        }

        let mut bytes = sql.as_bytes().to_vec();
        for segment in executable {
            if matches!(segment.kind, SqlSegmentKind::LineComment) {
                mask_preserving_newlines(&mut bytes, segment.range);
                continue;
            }
            let SqlSegmentKind::ExecutableComment { active, body, .. } = segment.kind else {
                unreachable!();
            };
            if active {
                mask_preserving_newlines(&mut bytes, segment.range.start..body.start);
                mask_preserving_newlines(&mut bytes, body.end..segment.range.end);
            } else {
                mask_preserving_newlines(&mut bytes, segment.range);
            }
        }
        let effective = String::from_utf8(bytes).expect("same-length SQL masking preserves UTF-8");
        // An active executable comment can expose quoted identifiers that
        // the outer comment scan did not visit. String-literal NULs stay valid.
        if effective.as_bytes().contains(&0) {
            for segment in segments(&effective, mode, server_version) {
                segment?;
            }
        }
        Ok(Self {
            effective: Cow::Owned(effective),
        })
    }

    pub(crate) fn effective(&self) -> &str {
        self.effective.as_ref()
    }

    pub(crate) fn into_effective(self) -> Cow<'a, str> {
        self.effective
    }
}

fn mask_preserving_newlines(bytes: &mut [u8], range: Range<usize>) {
    for byte in &mut bytes[range] {
        if *byte != b'\n' && *byte != b'\r' {
            *byte = b' ';
        }
    }
}

pub(crate) fn mask_comments<'a>(
    sql: &'a str,
    mode: SqlLexMode,
    server_version: u32,
    preserve_optimizer_hints: bool,
) -> Result<Cow<'a, str>, SqlLexError> {
    let mut ranges = Vec::new();
    for segment in segments(sql, mode, server_version) {
        let segment = segment?;
        let mask = matches!(
            segment.kind,
            SqlSegmentKind::LineComment | SqlSegmentKind::BlockComment
        ) || (!preserve_optimizer_hints
            && matches!(segment.kind, SqlSegmentKind::OptimizerHint));
        if mask {
            ranges.push(segment.range);
        }
    }
    if ranges.is_empty() {
        return Ok(Cow::Borrowed(sql));
    }
    let mut bytes = sql.as_bytes().to_vec();
    for range in ranges {
        mask_preserving_newlines(&mut bytes, range);
    }
    Ok(Cow::Owned(
        String::from_utf8(bytes).expect("same-length SQL masking preserves UTF-8"),
    ))
}

/// Skip whitespace and ordinary comments at `start`. Optimizer hints are left
/// visible when `preserve_optimizer_hint` is true so a caller can parse the
/// one grammar position in which it is meaningful.
pub(crate) fn skip_trivia(
    sql: &str,
    mut start: usize,
    mode: SqlLexMode,
    server_version: u32,
    preserve_optimizer_hint: bool,
) -> Result<usize, SqlLexError> {
    loop {
        while start < sql.len() && sql.as_bytes()[start].is_ascii_whitespace() {
            start += 1;
        }
        if start >= sql.len() {
            return Ok(start);
        }
        // Finding the end of a Code segment scans the entire unquoted tail.
        // A token cursor calls this once per token, so doing that here makes
        // long numeric expressions/VALUES lists quadratic. Only comments and
        // quotes need segmentation; keep quoted-token error behavior intact.
        let bytes = sql.as_bytes();
        if !matches!(bytes[start], b'\'' | b'"' | b'`')
            && !hash_starts_comment(bytes, start)
            && !dash_starts_comment(bytes, start)
            && !(bytes[start] == b'/' && bytes.get(start + 1) == Some(&b'*'))
        {
            return Ok(start);
        }
        let Some(segment) = segments(&sql[start..], mode, server_version).next() else {
            return Ok(start);
        };
        let segment = segment.map_err(|err| err.rebase(start))?;
        let skippable = matches!(
            segment.kind,
            SqlSegmentKind::LineComment | SqlSegmentKind::BlockComment
        ) || (!preserve_optimizer_hint
            && matches!(segment.kind, SqlSegmentKind::OptimizerHint));
        if !skippable || segment.range.start != 0 {
            return Ok(start);
        }
        start += segment.range.end;
    }
}

pub(crate) fn code_ranges(
    sql: &str,
    mode: SqlLexMode,
    server_version: u32,
) -> Result<Vec<Range<usize>>, SqlLexError> {
    let mut out = Vec::new();
    for segment in segments(sql, mode, server_version) {
        let segment = segment?;
        if matches!(segment.kind, SqlSegmentKind::Code) {
            out.push(segment.range);
        }
    }
    Ok(out)
}

/// Count occurrences of `needle` that are SQL code rather than quoted text or
/// comments. Callers should pass the effective SQL produced by [`PreparedSql`]
/// when executable comments need to participate.
pub(crate) fn count_code_byte(
    sql: &str,
    needle: u8,
    mode: SqlLexMode,
    server_version: u32,
) -> Result<usize, SqlLexError> {
    let mut count = 0usize;
    for segment in segments(sql, mode, server_version) {
        let segment = segment?;
        if matches!(segment.kind, SqlSegmentKind::Code) {
            count += sql.as_bytes()[segment.range]
                .iter()
                .filter(|byte| **byte == needle)
                .count();
        }
    }
    Ok(count)
}

/// Test whether already-prepared SQL contains only comments, whitespace and
/// statement separators, in one lexical pass and without allocating ranges.
pub(crate) fn effective_is_empty(
    effective_sql: &str,
    mode: SqlLexMode,
    server_version: u32,
) -> Result<bool, SqlLexError> {
    for segment in segments(effective_sql, mode, server_version) {
        let segment = segment?;
        match segment.kind {
            SqlSegmentKind::Code => {
                if effective_sql.as_bytes()[segment.range]
                    .iter()
                    .any(|byte| !byte.is_ascii_whitespace() && *byte != b';')
                {
                    return Ok(false);
                }
            }
            SqlSegmentKind::Quoted { .. } | SqlSegmentKind::ExecutableComment { .. } => {
                return Ok(false);
            }
            SqlSegmentKind::LineComment
            | SqlSegmentKind::BlockComment
            | SqlSegmentKind::OptimizerHint => {}
        }
    }
    Ok(true)
}

/// Return the leading unquoted SQL words, skipping every inert comment form
/// and applying executable-comment version gates first. Scanning stops at the
/// first significant non-word token, which prevents `--x` or punctuation from
/// being mistaken for a command prefix.
pub(crate) fn leading_keyword_ranges(
    sql: &str,
    mode: SqlLexMode,
    server_version: u32,
    limit: usize,
) -> Result<Vec<Range<usize>>, SqlLexError> {
    let prepared = PreparedSql::new(sql, mode, server_version)?;
    let effective = prepared.effective();
    let mut out = Vec::with_capacity(limit);
    for segment in segments(effective, mode, server_version) {
        let segment = segment?;
        match segment.kind {
            SqlSegmentKind::LineComment
            | SqlSegmentKind::BlockComment
            | SqlSegmentKind::OptimizerHint => continue,
            SqlSegmentKind::Quoted { .. } | SqlSegmentKind::ExecutableComment { .. } => break,
            SqlSegmentKind::Code => {
                let bytes = effective.as_bytes();
                let mut pos = segment.range.start;
                while pos < segment.range.end {
                    while pos < segment.range.end && bytes[pos].is_ascii_whitespace() {
                        pos += 1;
                    }
                    if pos == segment.range.end {
                        break;
                    }
                    if !(bytes[pos].is_ascii_alphabetic() || bytes[pos] == b'_') {
                        return Ok(out);
                    }
                    let start = pos;
                    pos += 1;
                    while pos < segment.range.end
                        && (bytes[pos].is_ascii_alphanumeric() || matches!(bytes[pos], b'_' | b'$'))
                    {
                        pos += 1;
                    }
                    out.push(start..pos);
                    if out.len() == limit {
                        return Ok(out);
                    }
                }
            }
        }
    }
    Ok(out)
}

/// Return the first keyword range from SQL that has already passed through
/// [`PreparedSql`]. The common path is allocation-free and only scans leading
/// trivia plus the keyword itself.
pub(crate) fn first_keyword(
    effective_sql: &str,
    mode: SqlLexMode,
    server_version: u32,
) -> Result<Option<Range<usize>>, SqlLexError> {
    let start = skip_trivia(effective_sql, 0, mode, server_version, false)?;
    let bytes = effective_sql.as_bytes();
    if start >= bytes.len() || !(bytes[start].is_ascii_alphabetic() || bytes[start] == b'_') {
        return Ok(None);
    }
    let mut end = start + 1;
    while end < bytes.len()
        && (bytes[end].is_ascii_alphanumeric() || matches!(bytes[end], b'_' | b'$'))
    {
        end += 1;
    }
    Ok(Some(start..end))
}

/// Cursor for quote/comment-aware structural scans over effective SQL.
///
/// The cursor never exposes bytes inside quoted tokens or comments. Parenthesis
/// depth is maintained once, allowing several top-level searches to continue
/// without restarting independent lexical state machines.
pub(crate) struct StructuralCursor<'a> {
    sql: &'a str,
    mode: SqlLexMode,
    server_version: u32,
    pos: usize,
    depth: usize,
    code_end: usize,
}

impl<'a> StructuralCursor<'a> {
    pub(crate) fn new(effective_sql: &'a str, mode: SqlLexMode, server_version: u32) -> Self {
        Self {
            sql: effective_sql,
            mode,
            server_version,
            pos: 0,
            depth: 0,
            code_end: 0,
        }
    }

    pub(crate) fn find_top_level_keyword(
        &mut self,
        keyword: &[u8],
    ) -> Result<Option<Range<usize>>, SqlLexError> {
        if !keyword.contains(&b' ') {
            return self.find_top_level(|sql, pos| {
                keyword_matches_at(sql.as_bytes(), pos, keyword).then_some(keyword.len())
            });
        }
        let mode = self.mode;
        let server_version = self.server_version;
        self.find_top_level(|sql, pos| {
            let first = keyword.split(|byte| byte.is_ascii_whitespace()).next()?;
            if !keyword_matches_at(sql.as_bytes(), pos, first) {
                return None;
            }
            let mut cursor = SqlTokenCursor::new(&sql[pos..], mode, server_version);
            let mut end = 0;
            for word in keyword
                .split(|byte| byte.is_ascii_whitespace())
                .filter(|word| !word.is_empty())
            {
                let token = cursor.eat_keyword_token(word).ok()??;
                end = token.span.end;
            }
            Some(end)
        })
    }

    /// Find any requested keyword without materializing the top-level words.
    /// `start` restricts matches while retaining nesting state from the prefix.
    pub(crate) fn find_top_level_keywords(
        &mut self,
        start: usize,
        keywords: &[&str],
    ) -> Result<Option<Range<usize>>, SqlLexError> {
        self.find_top_level(|sql, pos| {
            if pos < start
                || !(sql.as_bytes()[pos].is_ascii_alphabetic() || sql.as_bytes()[pos] == b'_')
            {
                return None;
            }
            keywords
                .iter()
                .find(|word| keyword_matches_at(sql.as_bytes(), pos, word.as_bytes()))
                .map(|word| word.len())
        })
    }

    pub(crate) fn find_top_level_byte(&mut self, needle: u8) -> Result<Option<usize>, SqlLexError> {
        Ok(self
            .find_top_level(|sql, pos| (sql.as_bytes()[pos] == needle).then_some(1))?
            .map(|range| range.start))
    }

    pub(crate) fn split_top_level(
        effective_sql: &'a str,
        separator: u8,
        mode: SqlLexMode,
        server_version: u32,
    ) -> Result<Vec<Range<usize>>, SqlLexError> {
        let mut cursor = Self::new(effective_sql, mode, server_version);
        let mut ranges = Vec::new();
        let mut start = 0;
        while let Some(pos) = cursor.find_top_level_byte(separator)? {
            ranges.push(start..pos);
            start = pos + 1;
        }
        ranges.push(start..effective_sql.len());
        Ok(ranges)
    }

    fn find_top_level<F>(&mut self, mut matches: F) -> Result<Option<Range<usize>>, SqlLexError>
    where
        F: FnMut(&str, usize) -> Option<usize>,
    {
        while self.pos < self.sql.len() {
            if self.pos >= self.code_end {
                let Some(segment) =
                    segments(&self.sql[self.pos..], self.mode, self.server_version).next()
                else {
                    break;
                };
                let segment = segment.map_err(|err| err.rebase(self.pos))?;
                let end = self.pos + segment.range.end;
                if !matches!(segment.kind, SqlSegmentKind::Code) {
                    self.pos = end;
                    continue;
                }
                self.code_end = end;
            }
            let segment_start = self.pos;
            let segment_end = self.code_end;
            self.pos = segment_end;
            let bytes = self.sql.as_bytes();
            let mut pos = segment_start;
            while pos < segment_end {
                match bytes[pos] {
                    b'(' => self.depth += 1,
                    b')' => self.depth = self.depth.saturating_sub(1),
                    _ if self.depth == 0 => {
                        if let Some(len) = matches(self.sql, pos) {
                            self.pos = pos + len;
                            return Ok(Some(pos..pos + len));
                        }
                    }
                    _ => {}
                }
                pos += 1;
            }
        }
        Ok(None)
    }
}

fn keyword_matches_at(bytes: &[u8], pos: usize, keyword: &[u8]) -> bool {
    if keyword.is_empty() || pos + keyword.len() > bytes.len() {
        return false;
    }
    let before_ok = pos == 0 || !is_word_continue(bytes[pos - 1]);
    let after = pos + keyword.len();
    before_ok
        && (after == bytes.len() || !is_word_continue(bytes[after]))
        && bytes[pos..after].eq_ignore_ascii_case(keyword)
}

#[inline]
fn is_word_continue(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || !byte.is_ascii() || matches!(byte, b'_' | b'$')
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SqlTokenKind {
    Word,
    QuotedIdentifier,
    String,
    Number,
    Placeholder,
    Symbol(u8),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SqlToken {
    pub span: Range<usize>,
    pub kind: SqlTokenKind,
}

/// Span-based token cursor for the small hand-written MySQL grammars.
#[derive(Debug, Clone)]
pub(crate) struct SqlTokenCursor<'a> {
    sql: &'a str,
    mode: SqlLexMode,
    server_version: u32,
    pos: usize,
    peeked: Option<SqlToken>,
}

impl<'a> SqlTokenCursor<'a> {
    pub(crate) fn new(effective_sql: &'a str, mode: SqlLexMode, server_version: u32) -> Self {
        Self {
            sql: effective_sql,
            mode,
            server_version,
            pos: 0,
            peeked: None,
        }
    }

    pub(crate) fn peek(&mut self) -> Result<Option<&SqlToken>, SqlLexError> {
        if self.peeked.is_none() {
            self.peeked = self.lex_next()?;
        }
        Ok(self.peeked.as_ref())
    }

    pub(crate) fn next(&mut self) -> Result<Option<SqlToken>, SqlLexError> {
        if let Some(token) = self.peeked.take() {
            self.pos = token.span.end;
            return Ok(Some(token));
        }
        self.lex_next()
    }

    pub(crate) fn eat_keyword(&mut self, keyword: &[u8]) -> Result<bool, SqlLexError> {
        Ok(self.eat_keyword_token(keyword)?.is_some())
    }

    pub(crate) fn eat_keyword_token(
        &mut self,
        keyword: &[u8],
    ) -> Result<Option<SqlToken>, SqlLexError> {
        let sql = self.sql.as_bytes();
        let matches = self.peek()?.is_some_and(|token| {
            token.kind == SqlTokenKind::Word
                && sql[token.span.clone()].eq_ignore_ascii_case(keyword)
        });
        if matches {
            return self.next();
        }
        Ok(None)
    }

    pub(crate) fn token_text(&self, token: &SqlToken) -> &'a str {
        &self.sql[token.span.clone()]
    }

    pub(crate) fn decoded_quoted_text(&self, token: &SqlToken) -> Option<String> {
        if !matches!(
            token.kind,
            SqlTokenKind::QuotedIdentifier | SqlTokenKind::String
        ) {
            return None;
        }
        let raw = self.token_text(token);
        let quote = raw.as_bytes().first().copied()?;
        let body = raw.get(1..raw.len().checked_sub(1)?)?;
        let backslash_escapes = quote != b'`'
            && !(quote == b'"' && self.mode.ansi_quotes)
            && !self.mode.no_backslash_escapes;
        // Most quoted payloads contain no escapes or doubled delimiters.
        // Preserve their UTF-8 bytes with one copy instead of decoding and
        // pushing every character; the full state machine handles all others.
        let needs_decode = if backslash_escapes {
            memchr::memchr2(quote, b'\\', body.as_bytes()).is_some()
        } else {
            memchr::memchr(quote, body.as_bytes()).is_some()
        };
        if !needs_decode {
            return Some(body.to_owned());
        }
        let mut out = String::with_capacity(body.len());
        let mut chars = body.chars().peekable();
        while let Some(ch) = chars.next() {
            if backslash_escapes && ch == '\\' {
                match chars.next() {
                    Some('0') => out.push('\0'),
                    Some('b') => out.push('\u{0008}'),
                    Some('n') => out.push('\n'),
                    Some('r') => out.push('\r'),
                    Some('t') => out.push('\t'),
                    Some('Z') => out.push('\u{001a}'),
                    Some('\\') => out.push('\\'),
                    Some('\'') => out.push('\''),
                    Some('"') => out.push('"'),
                    Some(other) => {
                        if matches!(other, '%' | '_') {
                            out.push('\\');
                        }
                        out.push(other);
                    }
                    None => out.push('\\'),
                }
            } else if ch as u32 == quote as u32 && chars.peek().copied() == Some(ch) {
                out.push(ch);
                chars.next();
            } else {
                out.push(ch);
            }
        }
        Some(out)
    }

    #[inline]
    fn lex_next(&mut self) -> Result<Option<SqlToken>, SqlLexError> {
        let bytes = self.sql.as_bytes();
        let mut start = self.pos;
        while start < bytes.len() && bytes[start].is_ascii_whitespace() {
            start += 1;
        }
        // Ordinary tokens need no trivia scanner. In particular, quoted
        // tokens are validated below exactly once instead of first scanning
        // their entire payload inside skip_trivia and then scanning it again.
        if matches!(bytes.get(start), Some(b'#' | b'-' | b'/')) {
            start = skip_trivia(self.sql, start, self.mode, self.server_version, false)?;
        }
        if start >= bytes.len() {
            self.pos = start;
            return Ok(None);
        }
        let byte = bytes[start];
        let (end, kind) = if matches!(byte, b'\'' | b'"' | b'`') {
            let segment = segments(&self.sql[start..], self.mode, self.server_version)
                .next()
                .expect("non-empty quoted token")
                .map_err(|err| err.rebase(start))?;
            let quote = byte;
            let kind = match quote {
                b'`' => SqlTokenKind::QuotedIdentifier,
                b'"' if self.mode.ansi_quotes => SqlTokenKind::QuotedIdentifier,
                _ => SqlTokenKind::String,
            };
            (start + segment.range.end, kind)
        } else if byte.is_ascii_alphabetic() || matches!(byte, b'_' | b'$') || !byte.is_ascii() {
            // Non-ASCII continuation bytes stay in the same identifier. This
            // matches the upstream MySQL dialect and never slices UTF-8.
            let mut end = start + 1;
            while end < bytes.len() && is_word_continue(bytes[end]) {
                end += 1;
            }
            (end, SqlTokenKind::Word)
        } else if byte.is_ascii_digit()
            || (byte == b'.' && bytes.get(start + 1).is_some_and(u8::is_ascii_digit))
        {
            let mut end = start + 1;
            while end < bytes.len()
                && (bytes[end].is_ascii_alphanumeric() || matches!(bytes[end], b'.' | b'+' | b'-'))
            {
                end += 1;
            }
            (end, SqlTokenKind::Number)
        } else if byte == b'?' {
            (start + 1, SqlTokenKind::Placeholder)
        } else {
            (start + 1, SqlTokenKind::Symbol(byte))
        };
        self.pos = end;
        Ok(Some(SqlToken {
            span: start..end,
            kind,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VERSION: u32 = 80000;

    #[test]
    fn quoted_cursor_preserves_plain_payload_and_decodes_special_bytes() {
        for ansi_quotes in [false, true] {
            for no_backslash_escapes in [false, true] {
                let mode = SqlLexMode {
                    ansi_quotes,
                    no_backslash_escapes,
                };
                for body in ["plain", "中文 café", "a\0b\nc", ""] {
                    for quote in ["'", "\"", "`"] {
                        let source = format!("{quote}{body}{quote}");
                        let mut cursor = SqlTokenCursor::new(&source, mode, VERSION);
                        if body.contains('\0') && (quote == "`" || (quote == "\"" && ansi_quotes)) {
                            assert!(cursor.next().is_err());
                            continue;
                        }
                        let token = cursor.next().unwrap().unwrap();
                        assert_eq!(cursor.decoded_quoted_text(&token).as_deref(), Some(body));
                    }
                }
                for (source, escaped, literal) in [
                    ("'a\\nb'", "a\nb", "a\\nb"),
                    ("'a''b'", "a'b", "a'b"),
                    ("`a``b`", "a`b", "a`b"),
                ] {
                    let mut cursor = SqlTokenCursor::new(source, mode, VERSION);
                    let token = cursor.next().unwrap().unwrap();
                    assert_eq!(
                        cursor.decoded_quoted_text(&token).as_deref(),
                        Some(if no_backslash_escapes {
                            literal
                        } else {
                            escaped
                        })
                    );
                }
            }
        }
    }

    #[test]
    fn admin_normalization_matches_spacing_case_and_terminator_contract() {
        for head in ["", "show", "SHOW", "purge", "中文"] {
            for separator in [" ", "  ", "\t", "\n", "\u{2003}"] {
                for tail in ["", ";", ";;;", " ; ;", "  "] {
                    let sql = format!("{separator}{head}{separator}binary{separator}logs{separator}'Case  sensitive'{tail}{separator}");
                    let expected = sql
                        .trim()
                        .trim_end_matches(';')
                        .split_whitespace()
                        .collect::<Vec<_>>()
                        .join(" ")
                        .to_ascii_uppercase();
                    assert_eq!(normalize_admin_sql(&sql), expected);
                }
            }
        }
        let padding = " ".repeat(1024 * 1024);
        assert_eq!(
            normalize_admin_sql(&format!("SHOW{padding}MASTER STATUS;")),
            "SHOW MASTER STATUS"
        );
        assert_eq!(normalize_admin_sql(&padding), "");
    }

    #[test]
    fn trivia_prefix_preserves_comments_hints_quotes_and_errors() {
        for ansi_quotes in [false, true] {
            for no_backslash_escapes in [false, true] {
                let mode = SqlLexMode {
                    ansi_quotes,
                    no_backslash_escapes,
                };
                for prefix in ["", " \t", " /* gap */ ", "-- comment\n# next\n"] {
                    for code in ["1+2", "--x", "-", "/", "名", "'abc'", "`x`", "\"x\""] {
                        let sql = format!("{prefix}{code}");
                        assert_eq!(
                            skip_trivia(&sql, 0, mode, VERSION, false).unwrap(),
                            prefix.len()
                        );
                    }
                    for malformed in ["'", "`", "\"", "/* missing"] {
                        assert!(skip_trivia(
                            &format!("{prefix}{malformed}"),
                            0,
                            mode,
                            VERSION,
                            false
                        )
                        .is_err());
                    }
                }
                let hint = " /*+ INDEX(t i) */ 1";
                assert_eq!(skip_trivia(hint, 0, mode, VERSION, true).unwrap(), 1);
                assert_eq!(
                    skip_trivia(hint, 0, mode, VERSION, false).unwrap(),
                    hint.len() - 1
                );
                for gate in ["/*!80000 1 */", "/*!99999 1 */"] {
                    assert_eq!(skip_trivia(gate, 0, mode, VERSION, false).unwrap(), 0);
                }
            }
        }
    }

    #[test]
    fn token_cursor_handles_long_unquoted_values_list() {
        let sql = "(123,456),".repeat(10_000);
        let mut cursor = SqlTokenCursor::new(&sql, SqlLexMode::default(), VERSION);
        let mut tokens = 0;
        while cursor.next().unwrap().is_some() {
            tokens += 1;
        }
        assert_eq!(tokens, 60_000);
    }

    #[test]
    fn mysql_dash_comment_requires_whitespace() {
        let kinds = |sql| {
            segments(sql, SqlLexMode::default(), VERSION)
                .map(|s| s.unwrap().kind)
                .collect::<Vec<_>>()
        };
        assert_eq!(kinds("-- comment"), vec![SqlSegmentKind::LineComment]);
        assert_eq!(kinds("--\tcomment"), vec![SqlSegmentKind::LineComment]);
        assert_eq!(kinds("--"), vec![SqlSegmentKind::LineComment]);
        assert_eq!(kinds("1--2"), vec![SqlSegmentKind::Code]);
    }

    #[test]
    fn executable_comments_are_masked_without_moving_offsets() {
        let sql = "SELECT /*!80000 0xCAFE */ 1, /*!99999 2 */ 3";
        let prepared = PreparedSql::new(sql, SqlLexMode::default(), VERSION).unwrap();
        assert_eq!(prepared.effective().len(), sql.len());
        assert_eq!(
            prepared.effective().lines().count(),
            sql.lines().count(),
            "line layout must be stable"
        );
        assert!(prepared.effective().contains("0xCAFE"));
        assert!(!prepared.effective().contains("99999"));
        assert!(!prepared.effective().contains(" 2 "));
    }

    #[test]
    fn executable_marker_inside_literals_and_comments_is_inert() {
        for sql in [
            "SELECT '/*!80000 ? */'",
            "SELECT 1 -- /*!80000 ? */\n",
            "SELECT 1 # /*!80000 ? */\n",
            "SELECT /* plain /*!80000 ? */ 1",
        ] {
            let prepared = PreparedSql::new(sql, SqlLexMode::default(), VERSION).unwrap();
            assert!(matches!(prepared.effective, Cow::Borrowed(_)), "{sql}");
        }
    }

    #[test]
    fn no_backslash_escapes_changes_quote_boundary() {
        let sql = "'a\\' /*!80000 ? */";
        let default = PreparedSql::new(sql, SqlLexMode::default(), VERSION);
        assert!(default.is_err(), "default mode keeps the quote open");
        let mode = SqlLexMode {
            no_backslash_escapes: true,
            ..SqlLexMode::default()
        };
        let prepared = PreparedSql::new(sql, mode, VERSION).unwrap();
        assert!(prepared.effective().contains('?'));
    }

    #[test]
    fn ansi_quotes_and_utf8_doubled_quotes_are_mode_aware() {
        let ansi = SqlLexMode {
            ansi_quotes: true,
            no_backslash_escapes: false,
        };
        let sql = r#""a\" ?"#;
        assert!(count_code_byte(sql, b'?', SqlLexMode::default(), VERSION).is_err());
        assert_eq!(count_code_byte(sql, b'?', ansi, VERSION).unwrap(), 1);
        assert_eq!(
            count_code_byte("SELECT '中''?文', ?", b'?', SqlLexMode::default(), VERSION,).unwrap(),
            1
        );
    }

    #[test]
    fn unterminated_block_comment_is_an_error() {
        let err = PreparedSql::new("SELECT /* x", SqlLexMode::default(), VERSION).unwrap_err();
        assert_eq!(err.offset, 7);
    }

    #[test]
    fn first_keyword_uses_shared_mysql_trivia_rules() {
        for sql in [
            "  SELECT 1",
            "/* header */ SELECT 1",
            "-- header\nSELECT 1",
            "# header\nSELECT 1",
        ] {
            let range = first_keyword(sql, SqlLexMode::default(), VERSION)
                .unwrap()
                .unwrap();
            assert_eq!(&sql[range], "SELECT");
        }
        assert!(
            first_keyword("--not-comment", SqlLexMode::default(), VERSION)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn structural_cursor_ignores_nested_and_inert_separators() {
        let sql = "a, fn('x,y', (1, 2)), /* , */ b, `c,d`";
        let ranges =
            StructuralCursor::split_top_level(sql, b',', SqlLexMode::default(), VERSION).unwrap();
        let parts = ranges
            .into_iter()
            .map(|range| sql[range].trim())
            .collect::<Vec<_>>();
        assert_eq!(parts, ["a", "fn('x,y', (1, 2))", "/* , */ b", "`c,d`"]);

        let mut cursor = StructuralCursor::new(
            "WITH x AS (SELECT 'DELETE') DELETE FROM t",
            SqlLexMode::default(),
            VERSION,
        );
        let range = cursor.find_top_level_keyword(b"DELETE").unwrap().unwrap();
        assert_eq!(&cursor.sql[range], "DELETE");
    }

    #[test]
    fn token_cursor_decodes_quotes_using_sql_mode() {
        let mut cursor = SqlTokenCursor::new(
            "CREATE /* gap */ USER 'a''b' @ 'h\\n'",
            SqlLexMode::default(),
            VERSION,
        );
        assert!(cursor.eat_keyword(b"CREATE").unwrap());
        assert!(cursor.eat_keyword(b"USER").unwrap());
        let user = cursor.next().unwrap().unwrap();
        assert_eq!(cursor.decoded_quoted_text(&user).as_deref(), Some("a'b"));
        assert_eq!(
            cursor.next().unwrap().unwrap().kind,
            SqlTokenKind::Symbol(b'@')
        );
        let host = cursor.next().unwrap().unwrap();
        assert_eq!(cursor.decoded_quoted_text(&host).as_deref(), Some("h\n"));

        let mode = SqlLexMode {
            no_backslash_escapes: true,
            ..SqlLexMode::default()
        };
        let mut cursor = SqlTokenCursor::new("'h\\n'", mode, VERSION);
        let token = cursor.next().unwrap().unwrap();
        assert_eq!(cursor.decoded_quoted_text(&token).as_deref(), Some("h\\n"));
    }

    #[test]
    fn lexical_mode_matrix_keeps_inert_regions_structurally_inert() {
        for ansi_quotes in [false, true] {
            for no_backslash_escapes in [false, true] {
                let mode = SqlLexMode {
                    ansi_quotes,
                    no_backslash_escapes,
                };
                let sql = "SELECT '中'';?文', \"名;?\", `列;?`, (1, 2) /* ;? */ -- ;?\n# ;?\n";
                assert_eq!(count_code_byte(sql, b';', mode, VERSION).unwrap(), 0);
                assert_eq!(count_code_byte(sql, b'?', mode, VERSION).unwrap(), 0);
                let ranges = StructuralCursor::split_top_level(sql, b',', mode, VERSION).unwrap();
                assert_eq!(
                    ranges.len(),
                    4,
                    "ansi={ansi_quotes} no_backslash={no_backslash_escapes}"
                );
            }
        }
    }
}

#[cfg(test)]
mod coordinate_tests {
    use super::*;
    #[test]
    fn token_and_top_level_errors_use_full_source_offsets() {
        for suffix in [
            "/* unfinished",
            "'unfinished",
            "`unfinished",
            "\"unfinished",
        ] {
            let prefix = "SELECT '正常', id FROM t WHERE id = 1 ";
            let sql = format!("{prefix}{suffix}");
            let mut tokens = SqlTokenCursor::new(&sql, SqlLexMode::default(), 80000);
            let error = loop {
                match tokens.next() {
                    Ok(Some(_)) => {}
                    Ok(None) => panic!("expected lexical error"),
                    Err(error) => break error,
                }
            };
            assert_eq!(error.offset, prefix.len(), "{sql}");
            let mut code = StructuralCursor::new(&sql, SqlLexMode::default(), 80000);
            assert_eq!(
                code.find_top_level_byte(b';').unwrap_err().offset,
                prefix.len()
            );
        }
    }
}

/// Normalize administrative command spacing and case after command classification.
pub(crate) fn normalize_admin_sql(sql: &str) -> String {
    let sql = sql.trim().trim_end_matches(';');
    // Bound the startup allocation for inputs dominated by whitespace. Grow
    // normally for long arguments; no intermediate word vector or case copy.
    let mut normalized = String::with_capacity(sql.len().min(256));
    for word in sql.split_whitespace() {
        if !normalized.is_empty() {
            normalized.push(' ');
        }
        normalized.push_str(word);
    }
    normalized.make_ascii_uppercase();
    normalized
}
