//! Shared text-edit application for compatibility rewrites.

use std::borrow::Cow;
use std::ops::Range;

use super::*;

/// Carry an owned SQL buffer through a pass without cloning on a no-op.
/// Replaced input buffers are dropped here, rather than retained by a chain
/// of borrowed locals in the caller. Borrowed slices are copied deliberately:
/// only an identical full-input borrow can reuse the previous ownership.
fn map_sql<'a, T, E>(
    input: Cow<'a, str>,
    pass: impl for<'s> FnOnce(&'s str) -> Result<(Cow<'s, str>, T), E>,
) -> Result<(Cow<'a, str>, T), E> {
    let (output, metadata) = pass(&input)?;
    // A borrowed no-op changes neither text nor coordinates. Avoid touching
    // provenance TLS for the common case where a compatibility pass is idle.
    if !std::ptr::eq(output.as_ref(), input.as_ref()) && !take_explicit_rewrite_map(&input, &output)
    {
        record_rewrite_source(&input, &output);
    }
    let output = match output {
        Cow::Owned(output) => Cow::Owned(output),
        Cow::Borrowed(output) if std::ptr::eq(output, input.as_ref()) => input,
        Cow::Borrowed(output) => Cow::Owned(output.to_owned()),
    };
    Ok((output, metadata))
}

pub(super) fn rewrite_sql_with<'a, T>(
    input: Cow<'a, str>,
    pass: impl for<'s> FnOnce(&'s str) -> (Cow<'s, str>, T),
) -> (Cow<'a, str>, T) {
    map_sql(input, |sql| Ok::<_, std::convert::Infallible>(pass(sql)))
        .unwrap_or_else(|never| match never {})
}

pub(super) fn try_rewrite_sql_with<'a, T>(
    input: Cow<'a, str>,
    pass: impl for<'s> FnOnce(&'s str) -> Result<(Cow<'s, str>, T), ParseError>,
) -> Result<(Cow<'a, str>, T), ParseError> {
    map_sql(input, pass)
}

pub(super) fn rewrite_sql<'a>(
    input: Cow<'a, str>,
    pass: impl for<'s> FnOnce(&'s str) -> Cow<'s, str>,
) -> Cow<'a, str> {
    rewrite_sql_with(input, |sql| (pass(sql), ())).0
}

pub(super) fn try_rewrite_sql<'a>(
    input: Cow<'a, str>,
    pass: impl for<'s> FnOnce(&'s str) -> Result<Cow<'s, str>, ParseError>,
) -> Result<Cow<'a, str>, ParseError> {
    try_rewrite_sql_with(input, |sql| pass(sql).map(|sql| (sql, ()))).map(|(sql, ())| sql)
}
/// Attributes stripped before sqlparser and restored exactly once after AST
/// conversion. Keeping this state beside the rewrite primitives prevents
/// individual passes from mutating the converted statement independently.
pub(super) struct PostParseState {
    pub(super) ddl_query_hints: Vec<QueryHint>,
    pub(super) ddl_hint_warnings: Vec<String>,
    pub(super) window_frame_exclusions: Vec<WindowFrameExclusion>,
    pub(super) create_index_visible: Option<bool>,
    pub(super) create_index_fulltext: bool,
    pub(super) update_modifiers: UpdateModifiers,
    pub(super) delete_modifiers: DeleteModifiers,
    pub(super) partition_for_create: Option<crate::parser_partition::PartitionDef>,
    pub(super) partition_actions_for_alter: Vec<crate::parser_partition::PartitionAction>,
    pub(super) fulltext_parsers: Vec<FulltextOptions>,
    pub(super) create_table_pct_override: CreateTablePctOverride,
    pub(super) create_zone_options: Vec<(String, String)>,
    pub(super) inline_index_visibility: Vec<(String, bool)>,
    pub(super) inline_index_includes: Vec<Vec<String>>,
}

impl PostParseState {
    pub(super) fn apply(self, statement: &mut Statement, sql: &str) -> Result<(), ParseError> {
        match &mut *statement {
            Statement::CreateIndex(index) => {
                index.query_hints = self.ddl_query_hints;
                index.hint_warnings = self.ddl_hint_warnings;
            }
            Statement::AlterTable(alter) => {
                alter.query_hints = self.ddl_query_hints;
                alter.hint_warnings = self.ddl_hint_warnings;
            }
            _ => {}
        }
        apply_stripped_window_frame_exclusions(statement, &self.window_frame_exclusions)?;
        if let (Some(visible), Statement::CreateIndex(index)) =
            (self.create_index_visible, &mut *statement)
        {
            index.visible = visible;
        }
        if self.create_index_fulltext {
            if let Statement::CreateIndex(index) = &mut *statement {
                index
                    .column_prefix_lengths
                    .iter_mut()
                    .for_each(|prefix| *prefix = None);
                index
                    .column_descending
                    .iter_mut()
                    .for_each(|descending| *descending = false);
                if index.key_expressions.iter().any(Option::is_some) {
                    *statement = Statement::Unsupported {
                        kind: "FULLTEXT INDEX on expression".to_string(),
                        sql: sql.to_string(),
                    };
                    return Ok(());
                }
                index.index_type = "FULLTEXT".to_string();
            }
        }
        let dml = match &mut *statement {
            Statement::WithCte(with) => with.body.as_mut(),
            statement => statement,
        };
        match dml {
            Statement::Update(update) if self.update_modifiers.any() => {
                update.ignore = self.update_modifiers.ignore;
                update.low_priority = self.update_modifiers.low_priority;
            }
            Statement::Delete(delete) if self.delete_modifiers.any() => {
                delete.low_priority = self.delete_modifiers.low_priority;
                delete.quick = self.delete_modifiers.quick;
                delete.ignore = self.delete_modifiers.ignore;
            }
            _ => {}
        }
        if let Some(partition) = self.partition_for_create {
            if let Some(table) = create_table_target_mut(statement) {
                table.partition = Some(partition);
            }
        }
        if !self.partition_actions_for_alter.is_empty() {
            if let Statement::AlterTable(table) = &mut *statement {
                table.partition_actions = self.partition_actions_for_alter;
            }
        }
        if !self.fulltext_parsers.is_empty() {
            apply_fulltext_parsers(statement, self.fulltext_parsers)?;
        }
        if !self.create_table_pct_override.is_empty() {
            if let Some(table) = create_table_target_mut(statement) {
                if let Some(pctfree) = self.create_table_pct_override.pctfree {
                    table.pctfree = Some(pctfree);
                }
                if let Some(pctused) = self.create_table_pct_override.pctused {
                    table.pctused = Some(pctused);
                }
            }
        }
        if let Some(table) = create_table_target_mut(statement) {
            for entry in self.create_zone_options {
                table.table_options.retain(|(key, _)| key != &entry.0);
                table.table_options.push(entry);
            }
        }
        apply_inline_index_visibility(statement, &self.inline_index_visibility)?;
        apply_inline_index_includes(statement, &self.inline_index_includes)?;
        Ok(())
    }
}

/// Strip a statement-level optimizer hint from the canonical DDL position.
/// sqlparser 0.61 preserves optimizer hints only on SELECT/DML nodes, so a
/// comment between CREATE/ALTER and the following keyword otherwise reaches
/// the grammar as an unexpected token.  Keep this pass deliberately narrow:
/// ordinary comments and hints in any other position remain untouched.
pub(super) fn strip_ddl_optimizer_hint(
    sql: &str,
) -> Result<(Cow<'_, str>, Vec<QueryHint>, Vec<String>), ParseError> {
    let bytes = sql.as_bytes();
    let mut start = 0usize;
    while start < bytes.len() && bytes[start].is_ascii_whitespace() {
        start += 1;
    }
    let keyword_end = bytes[start..]
        .iter()
        .position(|byte| !byte.is_ascii_alphabetic())
        .map(|offset| start + offset)
        .unwrap_or(bytes.len());
    let keyword = &bytes[start..keyword_end];
    if !keyword.eq_ignore_ascii_case(b"CREATE") && !keyword.eq_ignore_ascii_case(b"ALTER") {
        return Ok((Cow::Borrowed(sql), Vec::new(), Vec::new()));
    }
    let mut hint_start = keyword_end;
    while hint_start < bytes.len() && bytes[hint_start].is_ascii_whitespace() {
        hint_start += 1;
    }
    if bytes.get(hint_start..hint_start.saturating_add(3)) != Some(b"/*+".as_slice()) {
        return Ok((Cow::Borrowed(sql), Vec::new(), Vec::new()));
    }
    let hint_end = bytes[hint_start + 3..]
        .windows(2)
        .position(|window| window == b"*/")
        .map(|offset| hint_start + 3 + offset)
        .ok_or_else(|| ParseError::at_zero("unterminated DDL optimizer hint"))?;

    // CREATE hints are accepted only for index statements. ALTER hints are
    // attached to AlterTable and the dispatcher diagnoses a non-index use.
    let mut next = hint_end + 2;
    while next < bytes.len() && bytes[next].is_ascii_whitespace() {
        next += 1;
    }
    let next_end = bytes[next..]
        .iter()
        .position(|byte| !byte.is_ascii_alphabetic() && *byte != b'_')
        .map(|offset| next + offset)
        .unwrap_or(bytes.len());
    let next_keyword = &bytes[next..next_end];
    if keyword.eq_ignore_ascii_case(b"CREATE")
        && !matches!(
            next_keyword,
            value if value.eq_ignore_ascii_case(b"INDEX")
                || value.eq_ignore_ascii_case(b"UNIQUE")
                || value.eq_ignore_ascii_case(b"FULLTEXT")
                || value.eq_ignore_ascii_case(b"SPATIAL")
        )
    {
        return Ok((Cow::Borrowed(sql), Vec::new(), Vec::new()));
    }
    if keyword.eq_ignore_ascii_case(b"ALTER") && !next_keyword.eq_ignore_ascii_case(b"TABLE") {
        return Ok((Cow::Borrowed(sql), Vec::new(), Vec::new()));
    }

    let hint_text = &sql[hint_start + 3..hint_end];
    let (hints, warnings) = parse_query_hints(hint_text);
    let range = hint_start..hint_end + 2;
    Ok((
        mask_ranges(sql, std::slice::from_ref(&range)),
        hints,
        warnings,
    ))
}

/// Mask non-overlapping source ranges in one allocation while preserving byte
/// offsets and line layout. Post-parse state restores the stripped attributes.
fn mask_ranges<'a>(sql: &'a str, ranges: &[Range<usize>]) -> Cow<'a, str> {
    if ranges.is_empty() {
        return Cow::Borrowed(sql);
    }
    debug_assert!(ranges.iter().all(|range| range.start <= range.end));
    debug_assert!(ranges.windows(2).all(|pair| pair[0].end <= pair[1].start));
    let mut bytes = sql.as_bytes().to_vec();
    for range in ranges {
        for byte in &mut bytes[range.clone()] {
            if !matches!(*byte, b'\n' | b'\r') {
                *byte = b' ';
            }
        }
    }
    Cow::Owned(String::from_utf8(bytes).expect("masking ASCII preserves UTF-8"))
}

// Expression compatibility rewrites.

/// Pre-parse step: pull MySQL partition clauses out of the SQL and
/// return them alongside a "cleaned" SQL that sqlparser can handle.
pub(super) fn strip_partition_clauses(
    sql: &str,
) -> Result<
    (
        std::borrow::Cow<'_, str>,
        Option<crate::parser_partition::PartitionDef>,
        Vec<crate::parser_partition::PartitionAction>,
    ),
    ParseError,
> {
    // Head guard. Only CREATE TABLE / CREATE TEMPORARY / ALTER TABLE need
    // partition stripping. For every other head the function used to
    // allocate a fresh 1 MB String just to return the input unchanged.
    if !first_keyword_in(sql, &[b"CREATE", b"ALTER"]) {
        return Ok((std::borrow::Cow::Borrowed(sql), None, Vec::new()));
    }
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let mut cursor = crate::sql_lex::SqlTokenCursor::new(sql, mode, MYSQL_VERSION_ID);
    let create = cursor
        .eat_keyword(b"CREATE")
        .map_err(|err| parse_lex_error(sql, err))?;
    let alter = !create
        && cursor
            .eat_keyword(b"ALTER")
            .map_err(|err| parse_lex_error(sql, err))?;
    if create {
        let _ = cursor
            .eat_keyword(b"TEMPORARY")
            .map_err(|err| parse_lex_error(sql, err))?;
    }
    if !cursor
        .eat_keyword(b"TABLE")
        .map_err(|err| parse_lex_error(sql, err))?
    {
        return Ok((std::borrow::Cow::Borrowed(sql), None, Vec::new()));
    }
    if create {
        let (stripped, part) = crate::parser_partition::extract_create_table_partition(sql)?;
        // `extract_create_table_partition` always allocates; preserve owned.
        return Ok((std::borrow::Cow::Owned(stripped), part, Vec::new()));
    }
    if alter {
        let (stripped, actions) = crate::parser_partition::extract_alter_partition_actions(sql)?;
        return Ok((std::borrow::Cow::Owned(stripped), None, actions));
    }
    Ok((std::borrow::Cow::Borrowed(sql), None, Vec::new()))
}

/// Whether `lower[i..]` starts with the lowercase keyword `kw` with word boundaries on both sides.
fn fulltext_word_ci(lower: &[u8], i: usize, kw: &[u8]) -> bool {
    rewrite_keyword_matches_at(lower, i, kw)
}

/// Options missing from sqlparser's inline FULLTEXT constraint AST.
#[derive(Default)]
pub(super) struct FulltextOptions {
    parser: String,
    comment: Option<String>,
}

/// Extract FULLTEXT options after the key list, preserving their index order.
/// The shared lexer keeps SQL comments, escaped strings and quoted identifiers
/// inert and decodes literals using the session SQL mode. Ordinary statements
/// retain the prescan fast exit and never allocate or scan here.
pub(super) fn strip_fulltext_parsers(
    sql: &str,
    has_fulltext: bool,
) -> Result<(Cow<'_, str>, Vec<FulltextOptions>), ParseError> {
    use crate::sql_lex::{SqlLexMode, SqlTokenCursor, SqlTokenKind};
    if !has_fulltext {
        return Ok((Cow::Borrowed(sql), Vec::new()));
    }
    let mode = SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let mut cursor = SqlTokenCursor::new(sql, mode, MYSQL_VERSION_ID);
    let mut tokens = Vec::new();
    while let Some(token) = cursor
        .next()
        .map_err(|err| super::entry::parse_lex_error(sql, err))?
    {
        tokens.push(token);
    }
    let word = |i: usize, expected: &str| {
        tokens.get(i).is_some_and(|token| {
            token.kind == SqlTokenKind::Word
                && sql[token.span.clone()].eq_ignore_ascii_case(expected)
        })
    };
    let mut options = Vec::new();
    let mut out: Option<Vec<u8>> = None;
    let mut i = 0;
    while i < tokens.len() {
        // FULLTEXT is a constraint head or follows CREATE/ADD. In particular,
        // a table/column called `fulltext` must not shift option association.
        let at_head = i > 0
            && (word(i - 1, "CREATE")
                || word(i - 1, "ADD")
                || matches!(tokens[i - 1].kind, SqlTokenKind::Symbol(b'(' | b',')));
        if !at_head || !word(i, "FULLTEXT") {
            i += 1;
            continue;
        }
        let mut j = i + 1;
        // Find this index's key list. Malformed heads remain for sqlparser
        // to reject; do not cross a definition boundary looking for a list.
        while j < tokens.len()
            && !matches!(
                tokens[j].kind,
                SqlTokenKind::Symbol(b'(' | b')' | b',' | b';')
            )
        {
            j += 1;
        }
        if tokens
            .get(j)
            .is_none_or(|t| t.kind != SqlTokenKind::Symbol(b'('))
        {
            i += 1;
            continue;
        }
        let mut depth = 1;
        j += 1;
        while j < tokens.len() && depth > 0 {
            match tokens[j].kind {
                SqlTokenKind::Symbol(b'(') => depth += 1,
                SqlTokenKind::Symbol(b')') => depth -= 1,
                _ => {}
            }
            j += 1;
        }
        let mut index_options = FulltextOptions::default();
        while j < tokens.len() && depth == 0 {
            let start = tokens[j].span.start;
            let literal_index = if word(j, "COMMENT") {
                j + 1
            } else if word(j, "WITH") && word(j + 1, "PARSER") {
                j + 2
            } else {
                break;
            };
            let Some(value) = tokens.get(literal_index) else {
                return Err(parse_error_full_span(
                    sql,
                    "missing FULLTEXT index option value",
                ));
            };
            if word(j, "COMMENT") {
                if value.kind != SqlTokenKind::String {
                    return Err(parse_error_full_span(
                        sql,
                        "FULLTEXT COMMENT requires a string literal",
                    ));
                }
                index_options.comment = cursor.decoded_quoted_text(value);
            } else {
                index_options.parser = match value.kind {
                    SqlTokenKind::Word => sql[value.span.clone()].to_ascii_lowercase(),
                    SqlTokenKind::QuotedIdentifier => cursor
                        .decoded_quoted_text(value)
                        .expect("quoted identifier")
                        .to_ascii_lowercase(),
                    _ => {
                        return Err(parse_error_full_span(
                            sql,
                            "WITH PARSER requires an identifier",
                        ))
                    }
                };
            }
            if !index_options.parser.is_empty() && index_options.parser != "ngram" {
                return Err(parse_error_full_span(
                    sql,
                    "FULLTEXT WITH PARSER: unsupported parser",
                ));
            }
            out.get_or_insert_with(|| sql.as_bytes().to_vec())[start..value.span.end].fill(b' ');
            j = literal_index + 1;
        }
        options.push(index_options);
        i = j;
    }
    let cleaned = match out {
        Some(bytes) => Cow::Owned(String::from_utf8(bytes).expect("only complete tokens replaced")),
        None => Cow::Borrowed(sql),
    };
    Ok((cleaned, options))
}

/// Preprocess: rewrite the standalone DDL `CREATE FULLTEXT INDEX ...` into `CREATE INDEX ...`.
/// sqlparser 0.61 recognizes the FULLTEXT keyword only inside CREATE TABLE / ALTER TABLE
/// inline constraints, not the standalone `CREATE FULLTEXT INDEX` statement (which errors
/// right after CREATE). Returns `true` on a hit; the caller then sets the converted
/// `CreateIndex.index_type` to `FULLTEXT`. Must run after [`strip_fulltext_parsers`]: that pass
/// aligns `WITH PARSER` names via `fulltext` word occurrences; deleting the keyword early makes it exit early and miss the parser name.
pub(super) fn strip_create_index_fulltext(
    sql: &str,
    has_fulltext: bool,
) -> (std::borrow::Cow<'_, str>, bool) {
    // Head guard plus pre-scanned `has_fulltext`. Without these
    // `to_ascii_lowercase` would allocate a full copy of the SQL on every
    // `parse_statement`.
    if !has_fulltext || !first_keyword_is(sql, b"CREATE") {
        return (std::borrow::Cow::Borrowed(sql), false);
    }
    let lower = sql.to_ascii_lowercase();
    let lb = lower.as_bytes();
    let b = sql.as_bytes();
    let mut i = 0;
    while i < b.len() && b[i].is_ascii_whitespace() {
        i += 1;
    }
    if !fulltext_word_ci(lb, i, b"create") {
        return (std::borrow::Cow::Borrowed(sql), false);
    }
    i += "create".len();
    let after_create = i;
    while i < b.len() && b[i].is_ascii_whitespace() {
        i += 1;
    }
    if i == after_create || !fulltext_word_ci(lb, i, b"fulltext") {
        return (std::borrow::Cow::Borrowed(sql), false);
    }
    let fulltext_start = i;
    i += "fulltext".len();
    let after_fulltext = i;
    while i < b.len() && b[i].is_ascii_whitespace() {
        i += 1;
    }
    // Only `INDEX` immediately following makes it `CREATE FULLTEXT INDEX`; otherwise return as-is.
    if i == after_fulltext || !fulltext_word_ci(lb, i, b"index") {
        return (std::borrow::Cow::Borrowed(sql), false);
    }
    // Drop the FULLTEXT word plus its trailing whitespace, keep CREATE and its whitespace → `CREATE INDEX ...`.
    let mut out = String::with_capacity(sql.len() - (i - fulltext_start));
    out.push_str(&sql[..fulltext_start]);
    out.push_str(&sql[i..]);
    (std::borrow::Cow::Owned(out), true)
}

/// Inject the parser names extracted by [`strip_fulltext_parsers`] into the fulltext
/// indexes of the statement (CREATE TABLE inline / ALTER TABLE ADD / standalone CREATE FULLTEXT INDEX), in FULLTEXT occurrence order.
fn create_table_target_mut(statement: &mut Statement) -> Option<&mut CreateTable> {
    match statement {
        Statement::CreateTable(table) => Some(table),
        Statement::CreateTableAsSelect { table, .. } => Some(table),
        _ => None,
    }
}

/// Visit inline index definitions in declaration order without collecting a
/// temporary Vec of mutable references. Standalone CREATE INDEX has its own
/// upstream grammar and is deliberately outside this scope.
fn for_each_inline_index(
    statement: &mut Statement,
    mut apply: impl FnMut(&mut IndexDef) -> Result<(), ParseError>,
) -> Result<(), ParseError> {
    if let Some(table) = create_table_target_mut(statement) {
        for index in &mut table.indexes {
            apply(index)?;
        }
    } else if let Statement::AlterTable(table) = statement {
        for action in &mut table.actions {
            if let AlterAction::AddIndex(index) = action {
                apply(index)?;
            }
        }
    }
    Ok(())
}

pub(super) fn apply_fulltext_parsers(
    statement: &mut Statement,
    parsers: Vec<FulltextOptions>,
) -> Result<(), ParseError> {
    let mut parsers = parsers.into_iter();
    let mut apply = |parser: &mut String, comment: &mut String| {
        if let Some(options) = parsers.next() {
            *parser = options.parser;
            if let Some(value) = options.comment {
                *comment = value;
            }
        }
    };
    if let Statement::CreateIndex(index) = statement {
        if index.index_type.eq_ignore_ascii_case("FULLTEXT") {
            apply(&mut index.fulltext_parser, &mut index.comment);
        }
        Ok(())
    } else {
        for_each_inline_index(statement, |index| {
            if index.is_fulltext() {
                apply(&mut index.fulltext_parser, &mut index.comment);
            }
            Ok(())
        })
    }
}

/// Modifier flags stripped from `DELETE [LOW_PRIORITY] [QUICK] [IGNORE] FROM ...`
/// before handing the SQL to sqlparser (which doesn't recognise these MySQL
/// modifiers between `DELETE` and `FROM`).
#[derive(Default, Clone, Copy)]
pub(super) struct DeleteModifiers {
    low_priority: bool,
    quick: bool,
    ignore: bool,
}

impl DeleteModifiers {
    fn any(&self) -> bool {
        self.low_priority || self.quick || self.ignore
    }
}

/// Existing rewrite spacing is retained so downstream source-offset handling
/// does not change while the common prefix scan is consolidated.
enum ModifierSpacing {
    PerKeyword,
    Compact,
}

fn strip_modifier_prefix<'a>(
    sql: &'a str,
    verb: &str,
    spacing: ModifierSpacing,
    mut take: impl FnMut(&str) -> Option<usize>,
) -> Cow<'a, str> {
    let Some(start) = find_dml_verb_after_optional_cte(sql, verb) else {
        return Cow::Borrowed(sql);
    };
    let verb_end = start + verb.len();
    let mut end = verb_end;
    let mut count = 0;
    loop {
        let rest = sql[end..].trim_start();
        let Some(length) = take(rest) else { break };
        end = sql.len() - rest.len() + length;
        count += 1;
    }
    if count == 0 {
        return Cow::Borrowed(sql);
    }
    let (spaces, tail) = match spacing {
        ModifierSpacing::PerKeyword => (count, &sql[end..]),
        ModifierSpacing::Compact => (1, sql[end..].trim_start()),
    };
    let mut out = String::with_capacity(sql.len());
    out.push_str(&sql[..verb_end]);
    for _ in 0..spaces {
        out.push(' ');
    }
    out.push_str(tail);
    Cow::Owned(out)
}

pub(super) fn strip_delete_modifiers(sql: &str) -> (Cow<'_, str>, DeleteModifiers) {
    let mut mods = DeleteModifiers::default();
    let rewritten = strip_modifier_prefix(sql, "DELETE", ModifierSpacing::Compact, |rest| {
        let (keyword, flag) = if starts_with_keyword(rest, "LOW_PRIORITY") {
            ("LOW_PRIORITY", &mut mods.low_priority)
        } else if starts_with_keyword(rest, "QUICK") {
            ("QUICK", &mut mods.quick)
        } else if starts_with_keyword(rest, "IGNORE") {
            ("IGNORE", &mut mods.ignore)
        } else {
            return None;
        };
        *flag = true;
        Some(keyword.len())
    });
    (rewritten, mods)
}

/// MySQL allows the `tbl_name.*` target suffix in multi-table DELETE:
///   `DELETE t1.*, t2.* FROM t1 JOIN t2 ON ...`
///   `DELETE FROM t1.*, t2.* USING t1 JOIN t2 ON ...`
/// The `.*` is purely cosmetic (a DELETE always removes whole rows), but
/// sqlparser 0.61 parses DELETE targets via `parse_object_name(false)`, which
/// rejects the `*`. We therefore strip the `.*` before the SQL reaches the
/// parser.
///
/// Safety: stripping is scoped to DELETE statements and, within one, to `.`→`*`
/// pairs at **paren depth 0**, outside string/backtick/comment context, and only
/// when the `*` is *terminal* (immediately followed by `,` / `;` / `FROM` /
/// `USING` / end). Those constraints make it impossible to touch:
///   - `a.*` inside a WHERE/ON subquery — it lives at depth ≥ 1;
///   - a `.*` inside a string/identifier/comment — those spans are skipped;
///   - a multiplication like `WHERE x = 5.*3` — its `*` is followed by an operand.
pub(super) fn strip_delete_target_wildcards(sql: &str) -> std::borrow::Cow<'_, str> {
    let trimmed = sql.trim_start();
    if !starts_with_keyword(trimmed, "DELETE") || !sql.contains(".*") {
        return std::borrow::Cow::Borrowed(sql);
    }
    let bytes = sql.as_bytes();
    // DELETE targets precede FROM, or lie between FROM and USING.
    // Once the table-reference list begins, wildcards are no longer targets.
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let mut cursor = crate::sql_lex::SqlTokenCursor::new(sql, mode, MYSQL_VERSION_ID);
    let _ = cursor.eat_keyword(b"DELETE");
    loop {
        if cursor.eat_keyword(b"LOW_PRIORITY").unwrap_or(false)
            || cursor.eat_keyword(b"QUICK").unwrap_or(false)
            || cursor.eat_keyword(b"IGNORE").unwrap_or(false)
        {
            continue;
        }
        break;
    }
    let keyword = if cursor.eat_keyword(b"FROM").unwrap_or(false) {
        "USING"
    } else {
        "FROM"
    };
    let Some(target_end) = find_first_top_level_keyword(sql, 0, &[keyword]) else {
        return std::borrow::Cow::Borrowed(sql);
    };
    let n = target_end;
    let mut depth: i32 = 0;
    let mut i = 0usize;
    // Byte ranges [start, end) to delete (the `.` ... `*` spans).
    let mut strips: Vec<(usize, usize)> = Vec::new();
    while i < n {
        let c = bytes[i];
        match c {
            b'\'' | b'"' | b'`' => {
                i = quoted_span_end(&bytes[..n], i);
            }
            b'-' if i + 1 < n && bytes[i + 1] == b'-' => {
                while i < n && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'#' => {
                while i < n && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if i + 1 < n && bytes[i + 1] == b'*' => {
                i += 2;
                while i + 1 < n && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    i += 1;
                }
                i = (i + 2).min(n);
            }
            b'(' => {
                depth += 1;
                i += 1;
            }
            b')' => {
                depth -= 1;
                i += 1;
            }
            b'.' if depth == 0 => {
                let mut j = i + 1;
                while j < n && bytes[j].is_ascii_whitespace() {
                    j += 1;
                }
                if j < n && bytes[j] == b'*' {
                    // Confirm the `*` terminates a target (next non-ws token is a
                    // separator / clause keyword / end), not a multiplication.
                    let mut k = j + 1;
                    while k < n && bytes[k].is_ascii_whitespace() {
                        k += 1;
                    }
                    let terminates = k >= n
                        || bytes[k] == b','
                        || bytes[k] == b';'
                        || starts_with_keyword(&sql[k..], "FROM")
                        || starts_with_keyword(&sql[k..], "USING");
                    if terminates {
                        strips.push((i, j + 1));
                        i = j + 1;
                        continue;
                    }
                }
                i += 1;
            }
            _ => {
                i += 1;
            }
        }
    }
    if strips.is_empty() {
        return std::borrow::Cow::Borrowed(sql);
    }
    let mut out = String::with_capacity(sql.len());
    let mut last = 0usize;
    for (s, e) in strips {
        out.push_str(&sql[last..s]);
        last = e;
    }
    out.push_str(&sql[last..]);
    std::borrow::Cow::Owned(out)
}

/// Flags stripped from `UPDATE [LOW_PRIORITY] [IGNORE] ...` heads. Both
/// modifiers are no-ops on mydb's MVCC engine but sqlparser 0.61 rejects the
/// LOW_PRIORITY keyword in this position, so we peel them off before parse
/// and reattach the flags to the converted `Update` statement.
#[derive(Default, Debug, Clone, Copy)]
pub(super) struct UpdateModifiers {
    low_priority: bool,
    ignore: bool,
}

impl UpdateModifiers {
    fn any(&self) -> bool {
        self.low_priority || self.ignore
    }
}

pub(super) fn strip_update_modifiers(sql: &str) -> (Cow<'_, str>, UpdateModifiers) {
    let mut mods = UpdateModifiers::default();
    let rewritten = strip_modifier_prefix(sql, "UPDATE", ModifierSpacing::PerKeyword, |rest| {
        let (keyword, flag) = if starts_with_keyword(rest, "LOW_PRIORITY") && !mods.low_priority {
            ("LOW_PRIORITY", &mut mods.low_priority)
        } else if starts_with_keyword(rest, "IGNORE") && !mods.ignore {
            ("IGNORE", &mut mods.ignore)
        } else {
            return None;
        };
        *flag = true;
        Some(keyword.len())
    });
    (rewritten, mods)
}

/// Apply only DML prefix modifiers before the shared rewrite-free route.
/// Any unsupported shape/error returns to the complete pipeline on the
/// original source. Large literal extraction retains its own digest path.
pub(super) fn try_parse_plain_modified_dml(sql: &str) -> Option<Statement> {
    if sql.len() >= 64 * 1024 {
        return None;
    }
    let (head, len) = extract_first_keyword(sql);
    match &head[..len] {
        b"UPDATE" => {
            let (normalized, modifiers) = strip_update_modifiers(sql);
            if !modifiers.any() {
                return None;
            }
            let _source = ParserRawSqlGuard::activate(&normalized);
            let Statement::Update(mut update) = try_parse_plain_update_fast(&normalized).ok()??
            else {
                return None;
            };
            update.ignore = modifiers.ignore;
            update.low_priority = modifiers.low_priority;
            Some(Statement::Update(update))
        }
        b"DELETE" => {
            let (normalized, modifiers) = strip_delete_modifiers(sql);
            if !modifiers.any() {
                return None;
            }
            let _source = ParserRawSqlGuard::activate(&normalized);
            let Statement::Delete(mut delete) =
                try_parse_plain_query_or_delete(&normalized).ok()??
            else {
                return None;
            };
            delete.ignore = modifiers.ignore;
            delete.low_priority = modifiers.low_priority;
            delete.quick = modifiers.quick;
            Some(Statement::Delete(delete))
        }
        _ => None,
    }
}

pub(super) fn strip_create_index_visibility(
    sql: &str,
) -> (std::borrow::Cow<'_, str>, Option<bool>) {
    // Head + presence guards. Without these the function allocated a full
    // 1 MB uppercased copy of the SQL on every parse just to confirm the
    // head was CREATE.
    if !first_keyword_is(sql, b"CREATE") || !contains_keyword_ci(sql.as_bytes(), b"INDEX") {
        return (std::borrow::Cow::Borrowed(sql), None);
    }
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let Ok(ranges) = crate::sql_lex::code_ranges(sql, mode, MYSQL_VERSION_ID) else {
        return (std::borrow::Cow::Borrowed(sql), None);
    };
    let bytes = sql.as_bytes();
    let mut token_end = None;
    'outer: for range in ranges.iter().rev() {
        for i in (range.start..range.end).rev() {
            if !bytes[i].is_ascii_whitespace() && bytes[i] != b';' {
                token_end = Some(i + 1);
                break 'outer;
            }
        }
    }
    let Some(token_end) = token_end else {
        return (std::borrow::Cow::Borrowed(sql), None);
    };
    for (keyword, visible) in [("INVISIBLE", false), ("VISIBLE", true)] {
        let Some(token_start) = token_end.checked_sub(keyword.len()) else {
            continue;
        };
        if keyword_at(sql, token_start, keyword) {
            let mut out = String::with_capacity(sql.len());
            out.push_str(&sql[..token_start]);
            out.push(' ');
            out.push_str(&sql[token_end..]);
            return (std::borrow::Cow::Owned(out), Some(visible));
        }
    }
    (std::borrow::Cow::Borrowed(sql), None)
}

/// ALTER/DROP CHECK are outside sqlparser's MySQL AST. Split structurally before
/// parsing its siblings so ALGORITHM/LOCK and compound actions are preserved.
fn parse_alter_check_actions(sql: &str) -> Result<Option<Statement>, ParseError> {
    if !sql
        .as_bytes()
        .windows(5)
        .any(|word| word.eq_ignore_ascii_case(b"CHECK"))
    {
        return Ok(None);
    }
    use crate::sql_lex::{SqlTokenCursor, SqlTokenKind, StructuralCursor};
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let tokens = |text: &str| {
        let mut cursor = SqlTokenCursor::new(text, mode, MYSQL_VERSION_ID);
        let mut out = Vec::new();
        while let Some(token) = cursor.next().map_err(|err| parse_lex_error(sql, err))? {
            let value = if token.kind == SqlTokenKind::QuotedIdentifier {
                cursor.decoded_quoted_text(&token).unwrap_or_default()
            } else {
                cursor.token_text(&token).to_string()
            };
            out.push((token, value));
        }
        Ok::<_, ParseError>(out)
    };
    let head = tokens(sql)?;
    if head.len() < 4
        || !head[0].1.eq_ignore_ascii_case("ALTER")
        || !head[1].1.eq_ignore_ascii_case("TABLE")
    {
        return Ok(None);
    }
    let mut end = 3;
    while head
        .get(end)
        .is_some_and(|t| t.0.kind == SqlTokenKind::Symbol(b'.'))
    {
        end += 2;
    }
    if end >= head.len() {
        return Ok(None);
    }
    let table_sql = &sql[head[2].0.span.start..head[end - 1].0.span.end];
    let tail = sql[head[end].0.span.start..].trim().trim_end_matches(';');
    let clauses = StructuralCursor::split_top_level(tail, b',', mode, MYSQL_VERSION_ID)
        .map_err(|err| parse_lex_error(sql, err))?
        .into_iter()
        .map(|range| &tail[range])
        .collect::<Vec<_>>();
    let mut parts = Vec::new();
    let mut has_check = false;
    for clause in &clauses {
        let words = tokens(clause)?;
        let is_check = words.len() >= 2
            && (words[0].1.eq_ignore_ascii_case("ALTER")
                || words[0].1.eq_ignore_ascii_case("DROP"))
            && words[1].1.eq_ignore_ascii_case("CHECK");
        has_check |= is_check;
        parts.push((words, is_check));
    }
    if !has_check {
        return Ok(None);
    }
    let mut result = AlterTable {
        table: String::new(),
        actions: Vec::new(),
        partition_actions: Vec::new(),
        query_hints: Vec::new(),
        hint_warnings: Vec::new(),
        algorithm: AlterAlgorithm::Default,
        lock: AlterLock::Default,
    };
    // Let the canonical parser decode qualified/quoted table identifiers.
    let Statement::AlterTable(identity) =
        parse_statement(&format!("ALTER TABLE {table_sql} LOCK=DEFAULT"))?
    else {
        unreachable!()
    };
    result.table = identity.table;
    for (clause, (words, is_check)) in clauses.iter().zip(parts) {
        if is_check && words[0].1.eq_ignore_ascii_case("DROP") {
            let (name, if_exists) = match words.as_slice() {
                [_, _, name] => (name, false),
                [_, _, if_word, exists, name]
                    if if_word.1.eq_ignore_ascii_case("IF")
                        && exists.1.eq_ignore_ascii_case("EXISTS") =>
                {
                    (name, true)
                }
                _ => {
                    return Err(parse_error_full_span(
                        sql,
                        "DROP CHECK expects [IF EXISTS] name",
                    ))
                }
            };
            if !matches!(
                name.0.kind,
                SqlTokenKind::Word | SqlTokenKind::QuotedIdentifier
            ) {
                return Err(parse_error_full_span(
                    sql,
                    "DROP CHECK expects a constraint identifier",
                ));
            }
            result.actions.push(AlterAction::DropCheck {
                name: name.1.clone(),
                if_exists,
                include_foreign_keys: false,
            });
        } else if is_check {
            let enforced = match words.as_slice() {
                [_, _, _, enforced] if enforced.1.eq_ignore_ascii_case("ENFORCED") => true,
                [_, _, _, not, enforced]
                    if not.1.eq_ignore_ascii_case("NOT")
                        && enforced.1.eq_ignore_ascii_case("ENFORCED") =>
                {
                    false
                }
                _ => {
                    return Err(parse_error_full_span(
                        sql,
                        "ALTER CHECK expects a name and [NOT] ENFORCED",
                    ))
                }
            };
            if !matches!(
                words[2].0.kind,
                SqlTokenKind::Word | SqlTokenKind::QuotedIdentifier
            ) {
                return Err(parse_error_full_span(
                    sql,
                    "ALTER CHECK expects a constraint identifier",
                ));
            }
            result.actions.push(AlterAction::AlterCheck {
                name: words[2].1.clone(),
                enforced,
            });
        } else {
            let Statement::AlterTable(other) =
                parse_statement(&format!("ALTER TABLE {table_sql} {clause}"))?
            else {
                return Err(parse_error_full_span(sql, "expected ALTER TABLE action"));
            };
            result.actions.extend(other.actions);
            result.partition_actions.extend(other.partition_actions);
            if words
                .first()
                .is_some_and(|t| t.1.eq_ignore_ascii_case("ALGORITHM"))
            {
                result.algorithm = other.algorithm;
            }
            if words
                .first()
                .is_some_and(|t| t.1.eq_ignore_ascii_case("LOCK"))
            {
                result.lock = other.lock;
            }
        }
    }
    Ok(Some(Statement::AlterTable(result)))
}
pub(super) fn parse_manual_alter_table_check_statement(
    sql: &str,
) -> Result<Option<Statement>, ParseError> {
    // Head guard — `split_whitespace().collect()` over a 1 MB INSERT is
    // pure waste when the statement isn't even an ALTER.
    if !first_keyword_is(sql, b"ALTER") {
        return Ok(None);
    }
    if let Some(statement) = parse_alter_check_actions(sql)? {
        return Ok(Some(statement));
    }
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let mut cursor = crate::sql_lex::SqlTokenCursor::new(trimmed, mode, MYSQL_VERSION_ID);
    let mut tokens = Vec::new();
    while let Some(token) = cursor.next().map_err(|err| parse_lex_error(sql, err))? {
        tokens.push(token);
    }
    let mut parts = tokens
        .iter()
        .map(|token| &trimmed[token.span.clone()])
        .collect::<Vec<_>>();
    if parts.len() < 4
        || !parts[0].eq_ignore_ascii_case("ALTER")
        || !parts[1].eq_ignore_ascii_case("TABLE")
    {
        return Ok(None);
    }
    let (table, table_end) = if parts.get(3) == Some(&".") && parts.len() >= 5 {
        (
            format!(
                "{}.{}",
                unquote_identifier(parts[2]),
                unquote_identifier(parts[4])
            ),
            4,
        )
    } else {
        (unquote_identifier(parts[2]), 2)
    };
    let tail = trimmed[tokens[table_end].span.end..].trim_start();
    if table_end == 4 {
        parts.drain(3..5);
    }
    if parts.len() == 8
        && parts[3].eq_ignore_ascii_case("RENAME")
        && parts[4].eq_ignore_ascii_case("INDEX")
        && parts[6].eq_ignore_ascii_case("TO")
    {
        return Ok(Some(Statement::AlterTable(AlterTable {
            table,
            query_hints: Vec::new(),
            hint_warnings: Vec::new(),
            actions: vec![AlterAction::RenameIndex {
                old_name: unquote_identifier(parts[5]),
                new_name: unquote_identifier(parts[7]),
            }],
            partition_actions: Vec::new(),
            algorithm: AlterAlgorithm::Default,
            lock: AlterLock::Default,
        })));
    }
    if let Some(statement) = parse_manual_alter_table_options(sql, &table, tail)? {
        return Ok(Some(statement));
    }
    if parts.len() < 5 {
        return Ok(None);
    }
    if parts.len() == 8
        && parts[3].eq_ignore_ascii_case("ALTER")
        && parts[4].eq_ignore_ascii_case("COLUMN")
        && parts[6].eq_ignore_ascii_case("SET")
        && (parts[7].eq_ignore_ascii_case("VISIBLE") || parts[7].eq_ignore_ascii_case("INVISIBLE"))
    {
        return Ok(Some(Statement::AlterTable(AlterTable {
            table,
            query_hints: Vec::new(),
            hint_warnings: Vec::new(),
            actions: vec![AlterAction::SetColumnVisibility {
                column: unquote_identifier(parts[5]),
                invisible: parts[7].eq_ignore_ascii_case("INVISIBLE"),
            }],
            partition_actions: Vec::new(),
            algorithm: AlterAlgorithm::Default,
            lock: AlterLock::Default,
        })));
    }
    if parts.len() == 7
        && parts[3].eq_ignore_ascii_case("ALTER")
        && parts[4].eq_ignore_ascii_case("INDEX")
    {
        let name = unquote_identifier(parts[5]);
        let visible = match parts[6].to_ascii_uppercase().as_str() {
            "VISIBLE" => true,
            "INVISIBLE" => false,
            _ => {
                return Err(parse_error_full_span(
                    sql,
                    "ALTER TABLE ALTER INDEX expects VISIBLE or INVISIBLE",
                ))
            }
        };
        return Ok(Some(Statement::AlterTable(AlterTable {
            table,
            query_hints: Vec::new(),
            hint_warnings: Vec::new(),
            actions: vec![AlterAction::AlterIndexVisibility { name, visible }],
            partition_actions: Vec::new(),
            algorithm: AlterAlgorithm::Default,
            lock: AlterLock::Default,
        })));
    }
    Ok(None)
}

fn parse_manual_alter_table_options(
    sql: &str,
    table: &str,
    tail: &str,
) -> Result<Option<Statement>, ParseError> {
    let upper = tail.to_ascii_uppercase();
    // D6: {DISCARD|IMPORT} TABLESPACE at table level, and their
    // partition-level counterparts (`{DISCARD|IMPORT} PARTITION p [, ...]
    // TABLESPACE`), plus the standalone WITH/WITHOUT VALIDATION clause.
    // These all round-trip cleanly to `Statement::Unsupported` which the
    // dispatcher returns as 1235 (not the generic catch-all path). Naming
    // the operation in the `kind` string lets clients grep the error
    // message unambiguously.
    if upper.starts_with("DISCARD TABLESPACE") {
        return Ok(Some(Statement::Unsupported {
            kind: format!("ALTER TABLE {table} DISCARD TABLESPACE"),
            sql: sql.to_string(),
        }));
    }
    if upper.starts_with("IMPORT TABLESPACE") {
        return Ok(Some(Statement::Unsupported {
            kind: format!("ALTER TABLE {table} IMPORT TABLESPACE"),
            sql: sql.to_string(),
        }));
    }
    if upper.starts_with("DISCARD PARTITION") {
        return Ok(Some(Statement::Unsupported {
            kind: format!("ALTER TABLE {table} DISCARD PARTITION ... TABLESPACE"),
            sql: sql.to_string(),
        }));
    }
    if upper.starts_with("IMPORT PARTITION") {
        return Ok(Some(Statement::Unsupported {
            kind: format!("ALTER TABLE {table} IMPORT PARTITION ... TABLESPACE"),
            sql: sql.to_string(),
        }));
    }
    if upper.starts_with("WITH VALIDATION") || upper.starts_with("WITHOUT VALIDATION") {
        return Ok(Some(Statement::Unsupported {
            kind: format!("ALTER TABLE {tail}"),
            sql: sql.to_string(),
        }));
    }
    if upper.starts_with("CONVERT TO CHARACTER SET") || upper.starts_with("CONVERT TO CHARSET") {
        let words: Vec<&str> = tail.split_whitespace().collect();
        // CONVERT TO CHARACTER SET <cs> [COLLATE <coll>]
        //            or CHARSET <cs> [COLLATE <coll>]
        let cs_idx = if words
            .get(2)
            .is_some_and(|w| w.eq_ignore_ascii_case("CHARACTER"))
        {
            4
        } else {
            3
        };
        let Some(charset) = words.get(cs_idx) else {
            return Err(parse_error_full_span(
                sql,
                "CONVERT TO CHARACTER SET expects a charset name",
            ));
        };
        let collation = match words.get(cs_idx + 1) {
            None => None,
            Some(word) if word.eq_ignore_ascii_case("COLLATE") => match words.get(cs_idx + 2) {
                Some(coll) => Some(coll.trim_matches('`').to_string()),
                None => {
                    return Err(parse_error_full_span(
                        sql,
                        "COLLATE expects a collation name",
                    ))
                }
            },
            Some(_) => {
                return Err(parse_error_full_span(
                    sql,
                    "unexpected token after CONVERT TO CHARACTER SET",
                ))
            }
        };
        if words.len() > cs_idx + if collation.is_some() { 3 } else { 1 } {
            return Err(parse_error_full_span(
                sql,
                "unexpected token after CONVERT TO CHARACTER SET",
            ));
        }
        return Ok(Some(Statement::AlterTable(AlterTable {
            table: table.to_string(),
            query_hints: Vec::new(),
            hint_warnings: Vec::new(),
            actions: vec![AlterAction::ConvertToCharset {
                charset: charset.trim_matches('`').to_string(),
                collation,
            }],
            partition_actions: Vec::new(),
            algorithm: AlterAlgorithm::Default,
            lock: AlterLock::Default,
        })));
    }
    // InnoDB-aligned accepted no-ops. DISABLE/ENABLE KEYS is MyISAM-era
    // (InnoDB answers Note 1031). FORCE instead uses the physical rebuild
    // plan below and must never be silently discarded. ORDER BY is
    // ignored when a clustered key orders the table
    // (mydb heaps are unordered — same Warning 1105 as InnoDB+PK).
    if upper.trim_end_matches(';').trim_end() == "FORCE" {
        return Ok(Some(Statement::AlterTable(AlterTable {
            table: table.to_string(),
            query_hints: Vec::new(),
            hint_warnings: Vec::new(),
            actions: vec![AlterAction::Rebuild],
            partition_actions: Vec::new(),
            algorithm: AlterAlgorithm::Default,
            lock: AlterLock::Default,
        })));
    }
    let noop_warning: Option<Option<(String, u16, String)>> =
        match upper.trim_end_matches(';').trim_end() {
            "DISABLE KEYS" | "ENABLE KEYS" => Some(Some((
                "Note".to_string(),
                1031,
                format!("Table storage engine for '{table}' doesn't have this option"),
            ))),
            other if other.starts_with("ORDER BY ") => Some(Some((
                "Warning".to_string(),
                1105,
                format!(
                    "ORDER BY ignored as there is a user-defined clustered index in the table '{table}'"
                ),
            ))),
            _ => None,
        };
    if let Some(warning) = noop_warning {
        return Ok(Some(Statement::AlterTable(AlterTable {
            table: table.to_string(),
            query_hints: Vec::new(),
            hint_warnings: Vec::new(),
            actions: vec![AlterAction::Noop { warning }],
            partition_actions: Vec::new(),
            algorithm: AlterAlgorithm::Default,
            lock: AlterLock::Default,
        })));
    }
    let option_like = [
        "DEFAULT ",
        "CHARSET",
        "CHARACTER SET",
        "COLLATE",
        "COMMENT",
        "ENGINE",
        "ROW_FORMAT",
        "TABLESPACE",
        "ENCRYPTION",
        "DATA DIRECTORY",
        "INDEX DIRECTORY",
        "PCTFREE",
        "PCTUSED",
        "KEY_BLOCK_SIZE",
        "STATS_PERSISTENT",
        "STATS_AUTO_RECALC",
        "STATS_SAMPLE_PAGES",
        "PACK_KEYS",
        "CHECKSUM",
        "TABLE_CHECKSUM",
        "DELAY_KEY_WRITE",
        "MAX_ROWS",
        "MIN_ROWS",
        "AVG_ROW_LENGTH",
        "CONNECTION",
        "INSERT_METHOD",
        "AUTOEXTEND_SIZE",
        "COMPRESSION",
        "ZONE_MAP",
        "ZONE_SIZE",
    ]
    .iter()
    .any(|kw| upper.starts_with(kw));
    if !option_like || upper.starts_with("AUTO_INCREMENT") {
        return Ok(None);
    }
    // PCTFREE / PCTUSED are Oracle-style table-storage keywords that
    // sqlparser's MySQL dialect doesn't recognise in CREATE TABLE
    // options. Strip them out (and any leading commas / SET prefix)
    // before handing the rest to sqlparser; capture the values into
    // `pctfree` / `pctused` directly.
    let (stripped_tail, mut generic) = strip_zone_map_options(tail, false)?;
    let (stripped_tail, pctfree, pctused) = strip_pctfree_pctused(&stripped_tail)?;
    let mut default_charset = None;
    let mut default_collation = None;
    let mut comment = None;
    let tail_is_empty = crate::sql_lex::effective_is_empty(
        &stripped_tail,
        crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits()),
        MYSQL_VERSION_ID,
    )
    .map_err(|err| parse_error_at_zero(&err.to_string()))?;
    if !tail_is_empty {
        let probe = format!(
            "CREATE TABLE __mydb_alter_options_probe (c INT) {}",
            stripped_tail.trim()
        );
        let dialect = MydbDialect::default();
        let statements = match adapter::parse_sql(&dialect, &probe) {
            Ok(statements) => statements,
            Err(_) => return Ok(None),
        };
        let Some(SqlStatement::CreateTable(table_stmt)) = statements.into_iter().next() else {
            return Ok(None);
        };
        let sqlparser::ast::CreateTableOptions::Plain(options) = table_stmt.table_options else {
            return Ok(None);
        };
        for option in options {
            match option {
                sqlparser::ast::SqlOption::KeyValue { key, value } => {
                    let key = key.value.to_ascii_uppercase();
                    match key.as_str() {
                        "DEFAULT CHARSET"
                        | "CHARSET"
                        | "DEFAULT CHARACTER SET"
                        | "CHARACTER SET" => {
                            default_charset = Some(sql_option_value_name(&value)?);
                        }
                        "DEFAULT COLLATE" | "COLLATE" => {
                            default_collation = Some(sql_option_value_name(&value)?);
                        }
                        other => {
                            match accept_generic_table_option(
                                other,
                                &sql_option_value_name(&value)?,
                            ) {
                                Ok(Some(entry)) => {
                                    generic.retain(|(k, _): &(String, String)| k != &entry.0);
                                    generic.push(entry);
                                }
                                Ok(None) => {
                                    return Ok(Some(Statement::Unsupported {
                                        kind: format!("ALTER TABLE option {other}"),
                                        sql: sql.to_string(),
                                    }));
                                }
                                Err(message) => {
                                    return Err(parse_error_at_zero(&message));
                                }
                            }
                        }
                    }
                }
                sqlparser::ast::SqlOption::Comment(def) => {
                    comment = Some(match def {
                        sqlparser::ast::CommentDef::WithEq(value)
                        | sqlparser::ast::CommentDef::WithoutEq(value) => value,
                    });
                }
                other => {
                    // sqlparser currently represents MySQL's ALTER-tail
                    // `ENGINE = name` as a non-KeyValue SqlOption on some
                    // token shapes. Re-parse that option through the same
                    // allow-list instead of treating its display form as an
                    // opaque unsupported clause.
                    let rendered = other.to_string();
                    if let Some((key, value)) = rendered.split_once('=') {
                        let key = key.trim().to_ascii_uppercase();
                        if key == "ENGINE" {
                            match accept_generic_table_option(&key, value.trim()) {
                                Ok(Some(entry)) => {
                                    generic.retain(|(k, _)| k != &entry.0);
                                    generic.push(entry);
                                    continue;
                                }
                                Ok(None) => {}
                                Err(message) => return Err(parse_error_at_zero(&message)),
                            }
                        }
                    }
                    return Ok(Some(Statement::Unsupported {
                        kind: format!("ALTER TABLE option {other}"),
                        sql: sql.to_string(),
                    }));
                }
            }
        }
    }
    let mut actions: Vec<AlterAction> = Vec::new();
    if default_charset.is_some()
        || default_collation.is_some()
        || comment.is_some()
        || !generic.is_empty()
    {
        actions.push(AlterAction::SetTableOptions {
            default_charset,
            default_collation,
            comment,
            generic,
        });
    }
    if pctfree.is_some() || pctused.is_some() {
        actions.push(AlterAction::SetStorageProperties { pctfree, pctused });
    }
    if actions.is_empty() {
        return Ok(None);
    }
    Ok(Some(Statement::AlterTable(AlterTable {
        table: table.to_string(),
        query_hints: Vec::new(),
        hint_warnings: Vec::new(),
        actions,
        partition_actions: Vec::new(),
        algorithm: AlterAlgorithm::Default,
        lock: AlterLock::Default,
    })))
}

/// `bytes[i..]` starts with `w` (case-insensitive), and `i+w.len()` sits at a word boundary.
fn matches_word_ci(bytes: &[u8], i: usize, w: &[u8]) -> bool {
    if i + w.len() > bytes.len() {
        return false;
    }
    for k in 0..w.len() {
        if bytes[i + k].to_ascii_lowercase() != w[k].to_ascii_lowercase() {
            return false;
        }
    }
    if i + w.len() < bytes.len() {
        let after = bytes[i + w.len()];
        if after.is_ascii_alphanumeric() || after == b'_' {
            return false;
        }
    }
    true
}

/// Normalize only the assignment delimiter of SET list items. RHS expressions
/// (including nested user-variable assignments) retain their native semantics.
pub(super) fn normalize_assignment_operator(sql: &str) -> Cow<'_, str> {
    use crate::sql_lex::{SqlTokenCursor, SqlTokenKind};
    if !sql.contains(":=") || !first_keyword_in(sql, &[b"SET", b"UPDATE", b"INSERT", b"REPLACE"]) {
        return Cow::Borrowed(sql);
    }
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let mut cursor = SqlTokenCursor::new(sql, mode, MYSQL_VERSION_ID);
    let insert = first_keyword_in(sql, &[b"INSERT", b"REPLACE"]);
    let mut depth = 0usize;
    let mut in_set = false;
    let mut lhs = false;
    let mut ranges = Vec::new();
    while let Ok(Some(token)) = cursor.next() {
        match token.kind {
            SqlTokenKind::Symbol(b'(') => depth += 1,
            SqlTokenKind::Symbol(b')') => depth = depth.saturating_sub(1),
            SqlTokenKind::Word if depth == 0 => {
                let word = cursor.token_text(&token);
                if word.eq_ignore_ascii_case("SET")
                    || (insert && word.eq_ignore_ascii_case("UPDATE"))
                {
                    in_set = true;
                    lhs = true;
                }
                if ["WHERE", "ORDER", "LIMIT", "SELECT", "RETURNING"]
                    .iter()
                    .any(|w| word.eq_ignore_ascii_case(w))
                {
                    in_set = false;
                }
            }
            SqlTokenKind::Symbol(b',') if depth == 0 && in_set => lhs = true,
            SqlTokenKind::Symbol(b'=') if depth == 0 && in_set => lhs = false,
            SqlTokenKind::Symbol(b':')
                if depth == 0
                    && in_set
                    && lhs
                    && sql.as_bytes().get(token.span.end) == Some(&b'=') =>
            {
                ranges.push(token.span);
                lhs = false;
            }
            _ => {}
        }
    }
    mask_ranges(sql, &ranges)
}

/// MySQL accepts an unquoted select-item alias that begins with a digit when
/// the token also contains identifier characters (`AS 1Eq`). sqlparser
/// tokenizes it as a number followed by an identifier. Quote that alias before
/// parsing; numeric literals in expressions and CAST type clauses are not
/// touched because the rewrite is anchored to the `AS` keyword.
pub(super) fn rewrite_digit_leading_alias(sql: &str) -> std::borrow::Cow<'_, str> {
    let bytes = sql.as_bytes();
    if !memchr::memchr2_iter(b'a', b'A', bytes).any(|pos| {
        bytes
            .get(pos + 1)
            .is_some_and(|byte| byte.eq_ignore_ascii_case(&b's'))
    }) {
        return std::borrow::Cow::Borrowed(sql);
    }
    let mut out = String::with_capacity(sql.len() + 8);
    let mut i = 0usize;
    let mut changed = false;
    while i < bytes.len() {
        if matches!(bytes[i], b'\'' | b'"' | b'`') {
            let start = i;
            i = quoted_span_end(bytes, i);
            out.push_str(&sql[start..i]);
            continue;
        }
        if !matches_word_ci(bytes, i, b"AS") {
            let ch = sql[i..].chars().next().expect("i is a char boundary");
            out.push(ch);
            i += ch.len_utf8();
            continue;
        }
        let as_end = i + 2;
        let mut alias = as_end;
        while alias < bytes.len() && bytes[alias].is_ascii_whitespace() {
            alias += 1;
        }
        let mut end = alias;
        while end < bytes.len() && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
            end += 1;
        }
        let candidate = &bytes[alias..end];
        let valid = candidate.first().is_some_and(u8::is_ascii_digit)
            && candidate.iter().any(u8::is_ascii_alphabetic);
        if valid {
            out.push_str(&sql[i..alias]);
            out.push('`');
            out.push_str(&sql[alias..end]);
            out.push('`');
            i = end;
            changed = true;
        } else {
            out.push_str(&sql[i..as_end]);
            i = as_end;
        }
    }
    if changed {
        std::borrow::Cow::Owned(out)
    } else {
        std::borrow::Cow::Borrowed(sql)
    }
}

/// MySQL permits the `AS` keyword to be omitted when a CTAS query starts with
/// a parenthesized query expression:
/// `CREATE TABLE dst (SELECT ...) UNION (SELECT ...)`.
/// A parenthesized column definition is left unchanged by requiring a query
/// keyword immediately after the opening parenthesis.
/// Lex only actual SQL tokens: comments and quoted payloads cannot act as modifiers.
fn ctas_tokens(sql: &str) -> Option<Vec<crate::sql_lex::SqlToken>> {
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let mut cursor = crate::sql_lex::SqlTokenCursor::new(sql, mode, MYSQL_VERSION_ID);
    let mut tokens = Vec::new();
    while let Some(token) = cursor.next().ok()? {
        tokens.push(token);
    }
    Some(tokens)
}

fn token_word(sql: &str, token: Option<&crate::sql_lex::SqlToken>, word: &str) -> bool {
    token.is_some_and(|t| {
        t.kind == crate::sql_lex::SqlTokenKind::Word
            && sql[t.span.clone()].eq_ignore_ascii_case(word)
    })
}

/// Index immediately after a qualified object name, including quoted components.
fn object_name_end(tokens: &[crate::sql_lex::SqlToken], mut pos: usize) -> Option<usize> {
    use crate::sql_lex::SqlTokenKind::{QuotedIdentifier, Symbol, Word};
    if !matches!(tokens.get(pos)?.kind, Word | QuotedIdentifier) {
        return None;
    }
    pos += 1;
    while tokens.get(pos).is_some_and(|t| t.kind == Symbol(b'.')) {
        if !matches!(tokens.get(pos + 1)?.kind, Word | QuotedIdentifier) {
            return None;
        }
        pos += 2;
    }
    Some(pos)
}

fn ctas_target_end(sql: &str, tokens: &[crate::sql_lex::SqlToken]) -> Option<usize> {
    if !token_word(sql, tokens.first(), "CREATE") {
        return None;
    }
    let mut pos = 1;
    if token_word(sql, tokens.get(pos), "TEMPORARY") {
        pos += 1;
    }
    if !token_word(sql, tokens.get(pos), "TABLE") {
        return None;
    }
    pos += 1;
    if token_word(sql, tokens.get(pos), "IF") {
        if !token_word(sql, tokens.get(pos + 1), "NOT")
            || !token_word(sql, tokens.get(pos + 2), "EXISTS")
        {
            return None;
        }
        pos += 3;
    }
    object_name_end(tokens, pos)
}

pub(super) fn rewrite_parenthesized_ctas(sql: &str) -> std::borrow::Cow<'_, str> {
    use crate::sql_lex::SqlTokenKind::Symbol;
    use std::borrow::Cow;
    if !first_keyword_in(sql, &[b"CREATE"]) {
        return Cow::Borrowed(sql);
    }
    let Some(tokens) = ctas_tokens(sql) else {
        return Cow::Borrowed(sql);
    };
    let Some(pos) = ctas_target_end(sql, &tokens) else {
        return Cow::Borrowed(sql);
    };
    if !tokens.get(pos).is_some_and(|t| t.kind == Symbol(b'('))
        || !["SELECT", "WITH", "TABLE", "VALUES"]
            .iter()
            .any(|word| token_word(sql, tokens.get(pos + 1), word))
    {
        return Cow::Borrowed(sql);
    }
    let open = tokens[pos].span.start;
    Cow::Owned(format!("{}AS {}", &sql[..open], &sql[open..]))
}

/// sqlparser expects a column list before PARTITION; MySQL accepts it after.
pub(super) fn rewrite_insert_partition_columns(sql: &str) -> std::borrow::Cow<'_, str> {
    use crate::sql_lex::SqlTokenKind::Symbol;
    use std::borrow::Cow;
    if !first_keyword_in(sql, &[b"INSERT", b"REPLACE"]) {
        return Cow::Borrowed(sql);
    }
    let Some(tokens) = ctas_tokens(sql) else {
        return Cow::Borrowed(sql);
    };
    let mut pos = 1;
    while ["LOW_PRIORITY", "DELAYED", "HIGH_PRIORITY", "IGNORE", "INTO"]
        .iter()
        .any(|w| token_word(sql, tokens.get(pos), w))
    {
        pos += 1;
    }
    let Some(pos) = object_name_end(&tokens, pos) else {
        return Cow::Borrowed(sql);
    };
    if !token_word(sql, tokens.get(pos), "PARTITION")
        || !tokens.get(pos + 1).is_some_and(|t| t.kind == Symbol(b'('))
    {
        return Cow::Borrowed(sql);
    }
    let Some(close) = (pos + 2..tokens.len()).find(|&i| tokens[i].kind == Symbol(b')')) else {
        return Cow::Borrowed(sql);
    };
    if !tokens
        .get(close + 1)
        .is_some_and(|t| t.kind == Symbol(b'('))
    {
        return Cow::Borrowed(sql);
    }
    let Some(end) = (close + 2..tokens.len()).find(|&i| tokens[i].kind == Symbol(b')')) else {
        return Cow::Borrowed(sql);
    };
    // Do not move a parenthesized source query.
    if ["SELECT", "WITH", "VALUES", "TABLE"]
        .iter()
        .any(|w| token_word(sql, tokens.get(close + 2), w))
    {
        return Cow::Borrowed(sql);
    }
    Cow::Owned(format!(
        "{}{} {}{}",
        &sql[..tokens[pos].span.start],
        &sql[tokens[close + 1].span.start..tokens[end].span.end],
        &sql[tokens[pos].span.start..tokens[close].span.end],
        &sql[tokens[end].span.end..]
    ))
}

/// Normalize two legacy MySQL ENUM/SET grammar forms that sqlparser does not
/// accept: hexadecimal members and the post-type `BINARY` attribute.
///
/// Hex members denote a byte string. The parser AST currently stores
/// ENUM/SET members as text, so decode hexadecimal bytes using the declared
/// column/table charset before producing the textual grammar form.
/// `BINARY` requests bytewise member comparison and is represented by the
/// standard binary collation.
pub(super) fn rewrite_enum_set_legacy_literals(sql: &str) -> std::borrow::Cow<'_, str> {
    let upper = sql.to_ascii_uppercase();
    if !upper.contains("ENUM") && !upper.contains("SET") {
        return std::borrow::Cow::Borrowed(sql);
    }
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut index = 0usize;
    let mut changed = false;
    while index < bytes.len() {
        if matches!(bytes[index], b'\'' | b'"' | b'`') {
            index = copy_quoted(sql, index, &mut out);
            continue;
        }
        let type_len = if is_word_at(sql, &upper, index, "ENUM") {
            4
        } else if is_word_at(sql, &upper, index, "SET") {
            3
        } else {
            let ch = sql[index..].chars().next().unwrap();
            out.push(ch);
            index += ch.len_utf8();
            continue;
        };
        let mut open = index + type_len;
        while open < bytes.len() && bytes[open].is_ascii_whitespace() {
            open += 1;
        }
        if bytes.get(open) != Some(&b'(') {
            out.push_str(&sql[index..index + type_len]);
            index += type_len;
            continue;
        }
        out.push_str(&sql[index..=open]);
        index = open + 1;
        let mut depth = 1usize;
        let mut expects_member = true;
        let mut member_charset = None;
        while index < bytes.len() && depth != 0 {
            if depth == 1 && bytes[index] == b'"' && !rewrite_ansi_quotes() {
                index = copy_double_quoted_as_single(sql, index, &mut out);
                expects_member = false;
                changed = true;
                continue;
            }
            if matches!(bytes[index], b'\'' | b'"' | b'`') {
                index = copy_quoted(sql, index, &mut out);
                expects_member = false;
                continue;
            }
            match bytes[index] {
                b'(' => {
                    depth += 1;
                    out.push('(');
                    index += 1;
                }
                b')' => {
                    depth -= 1;
                    out.push(')');
                    index += 1;
                }
                b',' if depth == 1 => {
                    out.push(',');
                    index += 1;
                    expects_member = true;
                }
                byte if byte.is_ascii_whitespace() => {
                    out.push(byte as char);
                    index += 1;
                }
                b'0' if depth == 1
                    && expects_member
                    && matches!(bytes.get(index + 1), Some(b'x') | Some(b'X')) =>
                {
                    let hex_start = index + 2;
                    let mut hex_end = hex_start;
                    while hex_end < bytes.len() && bytes[hex_end].is_ascii_hexdigit() {
                        hex_end += 1;
                    }
                    if hex_end == hex_start {
                        out.push('0');
                        index += 1;
                        continue;
                    }
                    let charset = member_charset.get_or_insert_with(|| enum_hex_charset(sql, open));
                    write_hex_enum_member(&sql[hex_start..hex_end], charset, &mut out);
                    index = hex_end;
                    expects_member = false;
                    changed = true;
                }
                _ => {
                    let ch = sql[index..].chars().next().unwrap();
                    out.push(ch);
                    index += ch.len_utf8();
                    expects_member = false;
                }
            }
        }
        let mut attribute = index;
        while attribute < bytes.len() && bytes[attribute].is_ascii_whitespace() {
            attribute += 1;
        }
        if is_word_at(sql, &upper, attribute, "BINARY") {
            out.push_str(&sql[index..attribute]);
            out.push_str("COLLATE binary");
            index = attribute + "BINARY".len();
            changed = true;
        }
    }
    if changed {
        std::borrow::Cow::Owned(out)
    } else {
        std::borrow::Cow::Borrowed(sql)
    }
}

pub(super) fn validate_enum_set_member_lengths(sql: &str) -> Result<(), ParseError> {
    let upper = sql.to_ascii_uppercase();
    if !upper.contains("ENUM") && !upper.contains("SET") {
        return Ok(());
    }
    let bytes = sql.as_bytes();
    let mut index = 0usize;
    while index < bytes.len() {
        if matches!(bytes[index], b'\'' | b'"' | b'`') {
            index = skip_quoted(sql, index);
            continue;
        }
        let type_len = if is_word_at(sql, &upper, index, "ENUM") {
            4
        } else if is_word_at(sql, &upper, index, "SET") {
            3
        } else {
            index += 1;
            continue;
        };
        let mut open = index + type_len;
        while open < bytes.len() && bytes[open].is_ascii_whitespace() {
            open += 1;
        }
        if bytes.get(open) != Some(&b'(') {
            index += type_len;
            continue;
        }
        index = open + 1;
        let mut depth = 1usize;
        while index < bytes.len() && depth != 0 {
            match bytes[index] {
                b'\'' => {
                    let (end, symbols) = quoted_symbol_count(sql, index);
                    if symbols > 255 {
                        return Err(ParseError {
                            spec: crate::error::catalog::ER_TOO_LONG_SET_ENUM_VALUE,
                            message:
                                "String is too long for ENUM/SET (should be no longer than 255)"
                                    .to_string(),
                            span: Span { start: index, end },
                            line: 1,
                            column: 1,
                            expected: Vec::new(),
                        });
                    }
                    index = end;
                }
                b'"' | b'`' => index = skip_quoted(sql, index),
                b'(' => {
                    depth += 1;
                    index += 1;
                }
                b')' => {
                    depth -= 1;
                    index += 1;
                }
                _ => index += 1,
            }
        }
    }
    Ok(())
}

/// MySQL accepts the SQL-standard optional `ROW` keyword in VALUES row
/// constructors (`VALUES ROW(1), ROW(2)`). sqlparser accepts the equivalent
/// `VALUES (1), (2)` form, so remove only ROW tokens that occur at the
/// top-level of an active VALUES list.
pub(super) fn rewrite_values_row_keyword(sql: &str) -> std::borrow::Cow<'_, str> {
    let upper = sql.to_ascii_uppercase();
    if !upper.contains("VALUES") || !upper.contains("ROW") {
        return std::borrow::Cow::Borrowed(sql);
    }
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut index = 0usize;
    let mut depth = 0usize;
    let mut values_base: Option<usize> = None;
    let mut expect_row = false;
    let mut after_row = false;
    let mut row_depth: Option<usize> = None;
    let mut quantified_values = false;
    let mut changed = false;
    while index < bytes.len() {
        if matches!(bytes[index], b'\'' | b'"' | b'`') {
            index = copy_quoted(sql, index, &mut out);
            continue;
        }
        if is_word_at(sql, &upper, index, "VALUES") {
            quantified_values = values_follows_quantifier(sql, index);
            if quantified_values {
                out.push_str("SELECT");
                changed = true;
            } else {
                out.push_str(&sql[index..index + "VALUES".len()]);
            }
            index += "VALUES".len();
            values_base = Some(depth);
            expect_row = true;
            after_row = false;
            continue;
        }
        if let Some(base) = values_base {
            if depth < base {
                values_base = None;
                expect_row = false;
                after_row = false;
                row_depth = None;
                quantified_values = false;
            } else if depth == base && expect_row && is_word_at(sql, &upper, index, "ROW") {
                let mut open = index + "ROW".len();
                while open < bytes.len() && bytes[open].is_ascii_whitespace() {
                    open += 1;
                }
                if bytes.get(open) == Some(&b'(') {
                    index += "ROW".len();
                    expect_row = false;
                    row_depth = Some(base + 1);
                    changed = true;
                    continue;
                }
            } else if depth == base && after_row && !bytes[index].is_ascii_whitespace() {
                if bytes[index] == b',' {
                    if quantified_values {
                        out.push_str(" UNION ALL SELECT ");
                        index += 1;
                        expect_row = true;
                        after_row = false;
                        continue;
                    }
                    expect_row = true;
                    after_row = false;
                } else if bytes[index] != b')' {
                    values_base = None;
                    expect_row = false;
                    after_row = false;
                    row_depth = None;
                    quantified_values = false;
                }
            }
        }
        match bytes[index] {
            b'(' => depth += 1,
            b')' => {
                let closes_row = row_depth == Some(depth);
                depth = depth.saturating_sub(1);
                if closes_row {
                    row_depth = None;
                    after_row = true;
                }
            }
            _ => {}
        }
        let ch = sql[index..].chars().next().unwrap();
        out.push(ch);
        index += ch.len_utf8();
    }
    if changed {
        std::borrow::Cow::Owned(out)
    } else {
        std::borrow::Cow::Borrowed(sql)
    }
}

fn values_follows_quantifier(sql: &str, values_start: usize) -> bool {
    let bytes = sql.as_bytes();
    let mut index = values_start;
    while index > 0 && bytes[index - 1].is_ascii_whitespace() {
        index -= 1;
    }
    if index == 0 || bytes[index - 1] != b'(' {
        return false;
    }
    index -= 1;
    while index > 0 && bytes[index - 1].is_ascii_whitespace() {
        index -= 1;
    }
    let end = index;
    while index > 0
        && ((bytes[index - 1] as char).is_ascii_alphanumeric() || bytes[index - 1] == b'_')
    {
        index -= 1;
    }
    matches!(
        sql[index..end].to_ascii_uppercase().as_str(),
        "ALL" | "ANY" | "SOME"
    )
}

fn quoted_symbol_count(raw: &str, start: usize) -> (usize, usize) {
    let bytes = raw.as_bytes();
    let quote = bytes[start];
    let mut index = start + 1;
    let mut symbols = 0usize;
    while index < bytes.len() {
        if bytes[index] == b'\\' {
            index += 1;
            if index < bytes.len() {
                let width = raw[index..].chars().next().map_or(1, char::len_utf8);
                index += width;
                symbols += 1;
            }
            continue;
        }
        if bytes[index] == quote {
            if bytes.get(index + 1) == Some(&quote) {
                index += 2;
                symbols += 1;
                continue;
            }
            return (index + 1, symbols);
        }
        let width = raw[index..].chars().next().map_or(1, char::len_utf8);
        index += width;
        symbols += 1;
    }
    (index, symbols)
}

fn enum_hex_charset(sql: &str, enum_open: usize) -> String {
    use crate::sql_lex::SqlTokenKind;
    let Some(tokens) = ctas_tokens(sql) else {
        return "utf8mb4".into();
    };
    let mut depth = 0usize;
    let mut enum_depth = None;
    let mut column_tail = false;
    let mut column_charset = None;
    let mut table_charset = None;
    for (i, token) in tokens.iter().enumerate() {
        match token.kind {
            SqlTokenKind::Symbol(b'(') => {
                depth += 1;
                if token.span.start == enum_open {
                    enum_depth = Some(depth);
                }
            }
            SqlTokenKind::Symbol(b')') => {
                if enum_depth == Some(depth) {
                    column_tail = true;
                    enum_depth = None;
                } else if column_tail {
                    column_tail = false;
                }
                depth = depth.saturating_sub(1);
            }
            SqlTokenKind::Symbol(b',') if column_tail => column_tail = false,
            _ => {}
        }
        let (offset, collation) = if token_word(sql, Some(token), "CHARSET") {
            (1, false)
        } else if token_word(sql, Some(token), "CHARACTER")
            && token_word(sql, tokens.get(i + 1), "SET")
        {
            (2, false)
        } else if token_word(sql, Some(token), "COLLATE") {
            (1, true)
        } else {
            continue;
        };
        let mut next = i + offset;
        if tokens
            .get(next)
            .is_some_and(|t| t.kind == SqlTokenKind::Symbol(b'='))
        {
            next += 1;
        }
        let Some(value) = tokens.get(next) else {
            continue;
        };
        let name = sql[value.span.clone()].trim_matches(['`', '\'', '"']);
        let charset = if collation {
            crate::charset::resolve_collation(name)
                .ok()
                .map(|c| c.charset.to_string())
        } else {
            Some(name.to_string())
        };
        if column_tail {
            column_charset = charset;
        } else if depth == 0 {
            table_charset = charset;
        }
    }
    column_charset
        .or(table_charset)
        .unwrap_or_else(|| "utf8mb4".into())
}

fn write_hex_enum_member(hex: &str, charset: &str, out: &mut String) {
    out.push('\'');
    let mut digits = hex.as_bytes();
    let padded = [b'0'; 1];
    let owned;
    if digits.len() % 2 != 0 {
        owned = [padded.as_slice(), digits].concat();
        digits = &owned;
    }
    let bytes = digits
        .chunks_exact(2)
        .map(|pair| {
            let high = (pair[0] as char).to_digit(16).unwrap() as u8;
            let low = (pair[1] as char).to_digit(16).unwrap() as u8;
            (high << 4) | low
        })
        .collect::<Vec<_>>();
    let text = crate::charset::decode_bytes(charset, &bytes)
        .unwrap_or_else(|_| bytes.iter().copied().map(char::from).collect());
    for ch in text.chars() {
        match ch {
            '\'' => out.push_str("''"),
            '\\' => out.push_str("\\\\"),
            ch => out.push(ch),
        }
    }
    out.push('\'');
}

fn rewrite_ansi_quotes() -> bool {
    current_parser_sql_mode_bits() & crate::engine::api::SQL_MODE_ANSI_QUOTES != 0
}

/// Copy a double-quoted string literal as the equivalent single-quoted one.
/// Without ANSI_QUOTES MySQL reads both delimiters as string literals, while
/// the upstream DDL grammar accepts only single quotes for ENUM/SET members
/// and COMMENT values. Escapes are copied verbatim; only the delimiter and
/// the quote characters it doubles change.
fn copy_double_quoted_as_single(raw: &str, start: usize, out: &mut String) -> usize {
    let end = skip_quoted(raw, start);
    if end < start + 2 || raw.as_bytes()[end - 1] != b'"' {
        out.push_str(&raw[start..end]);
        return end;
    }
    let backslash_escapes =
        current_parser_sql_mode_bits() & crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES == 0;
    let mut chars = raw[start + 1..end - 1].chars().peekable();
    out.push('\'');
    while let Some(ch) = chars.next() {
        match ch {
            '\\' if backslash_escapes => {
                out.push(ch);
                if let Some(escaped) = chars.next() {
                    out.push(escaped);
                }
            }
            '"' if chars.peek() == Some(&'"') => {
                chars.next();
                out.push('"');
            }
            '\'' => out.push_str("\'\'"),
            _ => out.push(ch),
        }
    }
    out.push('\'');
    end
}

/// `COMMENT "text"` (column, index, table and partition options) is a string
/// literal unless ANSI_QUOTES makes it an identifier.
pub(super) fn rewrite_double_quoted_comments(sql: &str) -> std::borrow::Cow<'_, str> {
    if rewrite_ansi_quotes() || !sql.as_bytes().contains(&b'"') {
        return std::borrow::Cow::Borrowed(sql);
    }
    let upper = sql.to_ascii_uppercase();
    if !upper.contains("COMMENT") {
        return std::borrow::Cow::Borrowed(sql);
    }
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut index = 0usize;
    let mut changed = false;
    while index < bytes.len() {
        if matches!(bytes[index], b'\'' | b'"' | b'`') {
            index = copy_quoted(sql, index, &mut out);
            continue;
        }
        if !is_word_at(sql, &upper, index, "COMMENT") {
            let ch = sql[index..].chars().next().unwrap();
            out.push(ch);
            index += ch.len_utf8();
            continue;
        }
        let mut value = index + "COMMENT".len();
        while value < bytes.len() && bytes[value].is_ascii_whitespace() {
            value += 1;
        }
        if bytes.get(value) == Some(&b'=') {
            value += 1;
            while value < bytes.len() && bytes[value].is_ascii_whitespace() {
                value += 1;
            }
        }
        out.push_str(&sql[index..value]);
        index = value;
        if bytes.get(value) == Some(&b'"') {
            index = copy_double_quoted_as_single(sql, value, &mut out);
            changed = true;
        }
    }
    if changed {
        std::borrow::Cow::Owned(out)
    } else {
        std::borrow::Cow::Borrowed(sql)
    }
}

fn copy_quoted(raw: &str, start: usize, out: &mut String) -> usize {
    let end = skip_quoted(raw, start);
    out.push_str(&raw[start..end]);
    end
}

fn is_word_at(raw: &str, upper: &str, idx: usize, word: &str) -> bool {
    upper.as_bytes().get(idx..idx + word.len()) == Some(word.as_bytes())
        && (idx == 0
            || !matches!(raw.as_bytes()[idx - 1] as char, ch if ch.is_ascii_alphanumeric() || ch == '_'))
        && raw
            .as_bytes()
            .get(idx + word.len())
            .map(|b| {
                let ch = *b as char;
                !(ch.is_ascii_alphanumeric() || ch == '_')
            })
            .unwrap_or(true)
}

fn scan_word(raw: &str, start: usize) -> usize {
    let bytes = raw.as_bytes();
    let mut i = start;
    while i < bytes.len() {
        let ch = bytes[i] as char;
        if ch.is_ascii_alphanumeric() || ch == '_' {
            i += 1;
        } else {
            break;
        }
    }
    i
}

fn matching_paren(raw: &str, open: usize) -> Option<usize> {
    let bytes = raw.as_bytes();
    let mut depth = 0i32;
    let mut i = open;
    while i < bytes.len() {
        let ch = bytes[i] as char;
        match ch {
            '\'' | '"' | '`' => i = skip_quoted(raw, i),
            '(' => {
                depth += 1;
                i += 1;
            }
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
                i += 1;
            }
            _ => i += 1,
        }
    }
    None
}

/// MySQL permits `ALTER TABLE t ADD (c1 INT, c2 VARCHAR(10))`. sqlparser's
/// MySQL dialect does not currently accept that shorthand, so expand the
/// parenthesized column list into ordinary comma-separated ADD COLUMN
/// actions. Quoted text and nested type/default expression parentheses are
/// preserved byte-for-byte.
pub(super) fn rewrite_alter_parenthesized_add(sql: &str) -> Cow<'_, str> {
    let trimmed = sql.trim_start();
    if !trimmed
        .get(.."ALTER".len())
        .is_some_and(|head| head.eq_ignore_ascii_case("ALTER"))
    {
        return Cow::Borrowed(sql);
    }
    let bytes = sql.as_bytes();
    let mut i = sql.len() - trimmed.len();
    let mut depth = 0i32;
    while i < bytes.len() {
        match bytes[i] as char {
            '\'' | '"' | '`' => {
                i = skip_quoted(sql, i);
            }
            '(' => {
                depth += 1;
                i += 1;
            }
            ')' => {
                depth -= 1;
                i += 1;
            }
            ch if depth == 0 && (ch.is_ascii_alphabetic() || ch == '_') => {
                let end = scan_word(sql, i);
                if sql[i..end].eq_ignore_ascii_case("ADD") {
                    let mut open = end;
                    while open < bytes.len() && bytes[open].is_ascii_whitespace() {
                        open += 1;
                    }
                    if let Some(next) = consume_keyword_at(sql, open, "COLUMN") {
                        open = skip_ws(sql, next);
                    }
                    if bytes.get(open) == Some(&b'(') {
                        let Some(close) = matching_paren(sql, open) else {
                            return Cow::Borrowed(sql);
                        };
                        let body = &sql[open + 1..close];
                        let mut parts = Vec::new();
                        let body_bytes = body.as_bytes();
                        let mut part_start = 0usize;
                        let mut cursor = 0usize;
                        let mut part_depth = 0i32;
                        while cursor < body_bytes.len() {
                            match body_bytes[cursor] as char {
                                '\'' | '"' | '`' => {
                                    cursor = skip_quoted(body, cursor);
                                }
                                '(' => {
                                    part_depth += 1;
                                    cursor += 1;
                                }
                                ')' => {
                                    part_depth -= 1;
                                    cursor += 1;
                                }
                                ',' if part_depth == 0 => {
                                    parts.push(body[part_start..cursor].trim());
                                    part_start = cursor + 1;
                                    cursor += 1;
                                }
                                _ => cursor += 1,
                            }
                        }
                        parts.push(body[part_start..].trim());
                        if parts.iter().any(|part| part.is_empty()) {
                            return Cow::Borrowed(sql);
                        }
                        let replacement = parts
                            .into_iter()
                            .map(|part| format!("ADD COLUMN {part}"))
                            .collect::<Vec<_>>()
                            .join(", ");
                        let mut out = String::with_capacity(
                            sql.len().saturating_sub(close + 1 - i) + replacement.len(),
                        );
                        out.push_str(&sql[..i]);
                        out.push_str(&replacement);
                        out.push_str(&sql[close + 1..]);
                        return Cow::Owned(out);
                    }
                }
                i = end;
            }
            _ => i += 1,
        }
    }
    Cow::Borrowed(sql)
}

pub(super) fn rewrite_ctas_conflict_policy(
    sql: &str,
) -> (std::borrow::Cow<'_, str>, CtasConflictPolicy) {
    use crate::sql_lex::SqlTokenKind::Symbol;
    use std::borrow::Cow;
    let unchanged = || (Cow::Borrowed(sql), CtasConflictPolicy::Error);
    if !first_keyword_in(sql, &[b"CREATE"]) {
        return unchanged();
    }
    let Some(tokens) = ctas_tokens(sql) else {
        return unchanged();
    };
    let Some(start) = ctas_target_end(sql, &tokens) else {
        return unchanged();
    };
    let mut depth = 0usize;
    for pos in start..tokens.len() {
        match tokens[pos].kind {
            Symbol(b'(') => depth += 1,
            Symbol(b')') => depth = depth.saturating_sub(1),
            _ if depth == 0 => {
                if ["AS", "SELECT", "WITH"]
                    .iter()
                    .any(|w| token_word(sql, tokens.get(pos), w))
                {
                    break;
                }
                let policy = if token_word(sql, tokens.get(pos), "IGNORE") {
                    CtasConflictPolicy::Ignore
                } else if token_word(sql, tokens.get(pos), "REPLACE") {
                    CtasConflictPolicy::Replace
                } else {
                    continue;
                };
                let next = pos + 1 + usize::from(token_word(sql, tokens.get(pos + 1), "AS"));
                if !["SELECT", "WITH", "TABLE", "VALUES"]
                    .iter()
                    .any(|w| token_word(sql, tokens.get(next), w))
                    && !tokens.get(next).is_some_and(|t| t.kind == Symbol(b'('))
                {
                    continue;
                }
                let mut out = sql.to_string();
                out.replace_range(
                    tokens[pos].span.clone(),
                    &" ".repeat(tokens[pos].span.len()),
                );
                return (Cow::Owned(out), policy);
            }
            _ => {}
        }
    }
    unchanged()
}

fn quoted_span_end(bytes: &[u8], start: usize) -> usize {
    let quote = bytes[start];
    // SQL mode affects escapes only. Ordinary quoted spans avoid a mode
    // lookup; escaped spans still read it once and retain both mode policies.
    let mut backslash = None;
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] == b'\\' && quote != b'`' {
            let escapes = *backslash.get_or_insert_with(|| {
                let mode = current_parser_sql_mode_bits();
                mode & crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES == 0
                    && (quote != b'"' || mode & crate::engine::api::SQL_MODE_ANSI_QUOTES == 0)
            });
            if escapes {
                i = (i + 2).min(bytes.len());
                continue;
            }
        }
        if bytes[i] == quote {
            i += 1;
            if bytes.get(i) == Some(&quote) {
                i += 1;
            } else {
                break;
            }
        } else {
            i += 1;
        }
    }
    i
}

fn skip_quoted(raw: &str, start: usize) -> usize {
    quoted_span_end(raw.as_bytes(), start)
}

pub(super) fn normalize_create_table_like_parenthesized(
    sql: std::borrow::Cow<'_, str>,
    has_like: bool,
) -> std::borrow::Cow<'_, str> {
    let raw = sql.as_ref();
    // Caller passes pre-scanned `has_like` (true only when head is CREATE
    // and `LIKE` appears in the SQL); for non-CREATE heads the pre-scan
    // already skipped the scan, so this becomes a constant-time check.
    if !has_like || !raw.as_bytes().contains(&b'(') {
        return sql;
    }
    match maybe_normalize_create_table_like_parenthesized(sql.as_ref()) {
        Some(normalized) => std::borrow::Cow::Owned(normalized),
        None => sql,
    }
}

fn maybe_normalize_create_table_like_parenthesized(sql: &str) -> Option<String> {
    use sqlparser::keywords::Keyword;
    use sqlparser::tokenizer::{Token, Tokenizer};

    fn is_keyword(token: &Token, keyword: Keyword) -> bool {
        matches!(token, Token::Word(word) if word.keyword == keyword)
    }

    fn is_object_name(tokens: &[Token]) -> bool {
        if tokens.is_empty() {
            return false;
        }
        let mut expect_word = true;
        for token in tokens {
            if expect_word {
                if !matches!(token, Token::Word(_)) {
                    return false;
                }
            } else if !matches!(token, Token::Period) {
                return false;
            }
            expect_word = !expect_word;
        }
        !expect_word
    }

    fn render_object_name(tokens: &[Token]) -> String {
        tokens
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("")
    }

    let dialect = MydbDialect::default();
    let tokens = Tokenizer::new(&dialect, sql).tokenize().ok()?;
    let tokens = tokens
        .into_iter()
        .filter(|token| !matches!(token, Token::Whitespace(_)))
        .collect::<Vec<_>>();

    let mut end = tokens.len();
    let mut trailing_semicolon = false;
    if end > 0 && matches!(tokens[end - 1], Token::SemiColon) {
        trailing_semicolon = true;
        end -= 1;
    }
    let tokens = &tokens[..end];
    if tokens.len() < 7 {
        return None;
    }

    let mut idx = 0;
    if !is_keyword(&tokens[idx], Keyword::CREATE) {
        return None;
    }
    idx += 1;
    if idx < tokens.len()
        && (is_keyword(&tokens[idx], Keyword::TEMP) || is_keyword(&tokens[idx], Keyword::TEMPORARY))
    {
        idx += 1;
    }
    if idx >= tokens.len() || !is_keyword(&tokens[idx], Keyword::TABLE) {
        return None;
    }
    idx += 1;
    if idx + 2 < tokens.len()
        && is_keyword(&tokens[idx], Keyword::IF)
        && is_keyword(&tokens[idx + 1], Keyword::NOT)
        && is_keyword(&tokens[idx + 2], Keyword::EXISTS)
    {
        idx += 3;
    }

    let destination_start = idx;
    while idx < tokens.len() && !matches!(tokens[idx], Token::LParen) {
        idx += 1;
    }
    if idx == destination_start || idx + 3 >= tokens.len() {
        return None;
    }
    let destination = &tokens[destination_start..idx];
    if !is_object_name(destination) {
        return None;
    }

    if !matches!(tokens[idx], Token::LParen) || !is_keyword(&tokens[idx + 1], Keyword::LIKE) {
        return None;
    }
    let source_start = idx + 2;
    let mut source_end = source_start;
    while source_end < tokens.len() && !matches!(tokens[source_end], Token::RParen) {
        source_end += 1;
    }
    if source_end == source_start || source_end >= tokens.len() || source_end + 1 != tokens.len() {
        return None;
    }
    let source = &tokens[source_start..source_end];
    if !is_object_name(source) {
        return None;
    }

    let mut normalized = tokens[..destination_start]
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(" ");
    normalized.push(' ');
    normalized.push_str(&render_object_name(destination));
    normalized.push_str(" LIKE ");
    normalized.push_str(&render_object_name(source));
    if trailing_semicolon {
        normalized.push(';');
    }
    Some(normalized)
}

// GROUP BY and window compatibility rewrites.
fn is_keyword_boundary(ch: Option<char>) -> bool {
    ch.is_none_or(|ch| !ch.is_ascii_alphanumeric() && ch != '_' && ch != '$')
}

fn keyword_at(sql: &str, idx: usize, keyword: &str) -> bool {
    let end = idx + keyword.len();
    let Some(candidate) = sql.get(idx..end) else {
        return false;
    };
    candidate.eq_ignore_ascii_case(keyword)
        && is_keyword_boundary(sql.get(..idx).and_then(|prefix| prefix.chars().last()))
        && is_keyword_boundary(sql.get(end..).and_then(|suffix| suffix.chars().next()))
}

/// MySQL accepts the comma-separated multi-table `UPDATE t1, t2, ... SET ...`
/// form (a cross join with the join predicate in WHERE), but sqlparser's
/// `parse_update` parses a single table reference and then expects `SET`,
/// failing at the first comma. Rewrite each depth-0 comma between `UPDATE` and
/// the first top-level `SET` keyword into ` CROSS JOIN `, which sqlparser and
/// the existing multi-table UPDATE executor both handle. Commas inside string /
/// backtick literals, parentheses (subqueries / derived tables), or after `SET`
/// are left untouched.
/// `INSERT INTO t VALUES ()` and `INSERT INTO t () VALUES ()` — the empty
/// all-defaults row and empty column list — are rejected by sqlparser (which
/// requires at least one expression / identifier inside `()`). MySQL treats an
/// empty `VALUES ()` row as "one row, every column takes its default".
/// Rewrite each empty depth-0 row after `VALUES`/`VALUE` into a
/// `(__MYDB_EMPTY_ROW__)` marker (recognised in `convert_insert`), and strip an
/// empty depth-0 column list before it. Empty parens at deeper nesting (e.g.
/// `NOW()`), inside literals, are left untouched.
pub(super) fn rewrite_empty_insert_rows(sql: &str) -> std::borrow::Cow<'_, str> {
    let rest = sql.trim_start();
    let is_ins = rest.len() >= 6 && rest.as_bytes()[..6].eq_ignore_ascii_case(b"INSERT");
    let is_repl = rest.len() >= 7 && rest.as_bytes()[..7].eq_ignore_ascii_case(b"REPLACE");
    if !is_ins && !is_repl {
        return std::borrow::Cow::Borrowed(sql);
    }
    if !sql.contains('(') {
        return std::borrow::Cow::Borrowed(sql);
    }
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'$';
    let mut out = String::with_capacity(sql.len() + 16);
    let mut depth: i32 = 0;
    let mut after_values = false;
    let mut after_source_keyword = false;
    let mut seen_top_level_group = false;
    let mut prev_ident = false;
    let mut changed = false;
    let mut chars = sql.char_indices().peekable();
    while let Some((idx, ch)) = chars.next() {
        match ch {
            '\'' | '"' | '`' => {
                let quote = ch;
                out.push(quote);
                prev_ident = false;
                while let Some((_, c)) = chars.next() {
                    out.push(c);
                    if c == quote {
                        if matches!(chars.peek(), Some((_, q)) if *q == quote) {
                            out.push(quote);
                            chars.next();
                            continue;
                        }
                        break;
                    }
                    if c == '\\' {
                        if let Some((_, esc)) = chars.next() {
                            out.push(esc);
                        }
                    }
                }
            }
            '(' if depth == 0 => {
                // Look ahead: is this group empty (only whitespace until `)`)?
                let mut look = chars.clone();
                let mut empty = false;
                while let Some((_, c)) = look.next() {
                    if c.is_whitespace() {
                        continue;
                    }
                    empty = c == ')';
                    break;
                }
                if empty {
                    // Consume up to and including the closing `)`.
                    for (_, c) in chars.by_ref() {
                        if c == ')' {
                            break;
                        }
                    }
                    if after_values {
                        let marker = extensions::register(sql, extensions::Extension::EmptyRow);
                        out.push('(');
                        out.push_str(&marker);
                        out.push(')');
                    } else if !after_source_keyword && !seen_top_level_group {
                        // Empty column list — drop it (keep a space so the
                        // surrounding tokens stay separated).
                        out.push(' ');
                    } else {
                        // A nullary function in INSERT SET / INSERT SELECT /
                        // ODKU is an expression, not an empty target-column
                        // list.
                        out.push_str("()");
                    }
                    changed = true;
                    seen_top_level_group = true;
                    prev_ident = false;
                } else {
                    depth += 1;
                    out.push('(');
                    seen_top_level_group = true;
                    prev_ident = false;
                }
            }
            '(' => {
                depth += 1;
                out.push('(');
                prev_ident = false;
            }
            ')' => {
                depth -= 1;
                out.push(')');
                prev_ident = false;
            }
            _ => {
                if depth == 0 && !after_source_keyword && !prev_ident && (ch == 'v' || ch == 'V') {
                    let rem = &sql[idx..];
                    let matched = if rem.len() >= 6
                        && rem.as_bytes()[..6].eq_ignore_ascii_case(b"VALUES")
                        && rem.as_bytes().get(6).is_none_or(|&b| !is_ident(b))
                    {
                        true
                    } else {
                        rem.len() >= 5
                            && rem.as_bytes()[..5].eq_ignore_ascii_case(b"VALUE")
                            && rem.as_bytes().get(5).is_none_or(|&b| !is_ident(b))
                    };
                    if matched {
                        after_values = true;
                        after_source_keyword = true;
                    }
                }
                if depth == 0
                    && !prev_ident
                    && (sql[idx..]
                        .get(..2)
                        .is_some_and(|word| word.eq_ignore_ascii_case("ON"))
                        || sql[idx..]
                            .get(..9)
                            .is_some_and(|word| word.eq_ignore_ascii_case("RETURNING")))
                {
                    after_values = false;
                }
                if depth == 0 && !prev_ident && matches!(ch, 's' | 'S') {
                    let rem = &sql[idx..];
                    let source = [b"SET".as_slice(), b"SELECT"].iter().any(|word| {
                        rem.len() >= word.len()
                            && rem.as_bytes()[..word.len()].eq_ignore_ascii_case(word)
                            && rem.as_bytes().get(word.len()).is_none_or(|&b| !is_ident(b))
                    });
                    if source {
                        after_source_keyword = true;
                    }
                }
                if depth == 0 && !prev_ident && matches!(ch, 'o' | 'O' | 'r' | 'R') {
                    let rem = &sql[idx..];
                    if [b"ON".as_slice(), b"RETURNING"].iter().any(|word| {
                        rem.len() >= word.len()
                            && rem.as_bytes()[..word.len()].eq_ignore_ascii_case(word)
                            && rem.as_bytes().get(word.len()).is_none_or(|&b| !is_ident(b))
                    }) {
                        after_values = false;
                    }
                }
                out.push(ch);
                prev_ident = ch.is_alphanumeric() || ch == '_' || ch == '$';
            }
        }
    }
    if changed {
        std::borrow::Cow::Owned(out)
    } else {
        std::borrow::Cow::Borrowed(sql)
    }
}

pub(super) fn rewrite_comma_update_targets(sql: &str) -> std::borrow::Cow<'_, str> {
    let leading = sql.len() - sql.trim_start().len();
    let rest = &sql[leading..];
    if rest.len() < 6 || !rest.as_bytes()[..6].eq_ignore_ascii_case(b"UPDATE") {
        return std::borrow::Cow::Borrowed(sql);
    }
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'$';
    if rest.as_bytes().get(6).is_some_and(|&b| is_ident(b)) {
        return std::borrow::Cow::Borrowed(sql);
    }
    if !sql.contains(',') {
        return std::borrow::Cow::Borrowed(sql);
    }

    let mut out = String::with_capacity(sql.len() + 16);
    let mut depth: i32 = 0;
    let mut reached_set = false;
    let mut prev_ident = false;
    let mut chars = sql.char_indices().peekable();
    while let Some((idx, ch)) = chars.next() {
        if reached_set {
            out.push(ch);
            continue;
        }
        match ch {
            '\'' | '"' | '`' => {
                let quote = ch;
                out.push(quote);
                prev_ident = false;
                while let Some((_, c)) = chars.next() {
                    out.push(c);
                    if c == quote {
                        if matches!(chars.peek(), Some((_, q)) if *q == quote) {
                            out.push(quote);
                            chars.next();
                            continue;
                        }
                        break;
                    }
                    if c == '\\' {
                        if let Some((_, esc)) = chars.next() {
                            out.push(esc);
                        }
                    }
                }
            }
            '(' => {
                depth += 1;
                out.push('(');
                prev_ident = false;
            }
            ')' => {
                depth -= 1;
                out.push(')');
                prev_ident = false;
            }
            ',' if depth == 0 => {
                out.push_str(" CROSS JOIN ");
                prev_ident = false;
            }
            _ => {
                if depth == 0 && !prev_ident && (ch == 's' || ch == 'S') {
                    let rem = &sql[idx..];
                    if rem.len() >= 3 && rem.as_bytes()[..3].eq_ignore_ascii_case(b"SET") {
                        let boundary = rem.as_bytes().get(3).is_none_or(|&b| !is_ident(b));
                        if boundary {
                            reached_set = true;
                            out.push(ch);
                            continue;
                        }
                    }
                }
                out.push(ch);
                prev_ident = ch.is_alphanumeric() || ch == '_' || ch == '$';
            }
        }
    }
    std::borrow::Cow::Owned(out)
}

/// Visit executable characters using the same SQL-mode-aware lexer as parsing.
/// Callers operate on PreparedSql's effective text, so versioned comments have
/// already been expanded. Quoted text and hints are never rewritten.
fn visit_code_chars(
    sql: &str,
    mut visit: impl FnMut(usize, char) -> Result<(), ParseError>,
) -> Result<(), ParseError> {
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    for segment in crate::sql_lex::segments(sql, mode, MYSQL_VERSION_ID) {
        let segment = segment.map_err(|error| parse_lex_error(sql, error))?;
        if matches!(segment.kind, crate::sql_lex::SqlSegmentKind::Code) {
            for (offset, ch) in sql[segment.range.clone()].char_indices() {
                visit(segment.range.start + offset, ch)?;
            }
        }
    }
    Ok(())
}

fn replace_sql_ranges<'a>(
    sql: &'a str,
    ranges: &[Range<usize>],
    replacement: &str,
) -> Cow<'a, str> {
    if ranges.is_empty() {
        return Cow::Borrowed(sql);
    }
    let edits = ranges
        .iter()
        .map(|range| TextEdit {
            range: range.clone(),
            replacement,
        })
        .collect::<Vec<_>>();
    rewrite_source_edits(sql, &edits).expect("validated disjoint rewrite ranges")
}

pub(super) fn rewrite_grouping_sets_keyword(sql: &str) -> Result<Cow<'_, str>, ParseError> {
    // Most statements cannot contain this extension. Do not lex them again.
    if !sql
        .as_bytes()
        .windows(8)
        .any(|word| word.eq_ignore_ascii_case(b"GROUPING"))
    {
        return Ok(Cow::Borrowed(sql));
    }
    let mut ranges = Vec::new();
    let mut skip_until = 0;
    visit_code_chars(sql, |idx, _| {
        if idx >= skip_until && keyword_at(sql, idx, "GROUPING") {
            let after = skip_ws(sql, idx + "GROUPING".len());
            if after > idx + "GROUPING".len() && keyword_at(sql, after, "SETS") {
                skip_until = after + "SETS".len();
                ranges.push(idx..skip_until);
            }
        }
        Ok(())
    })?;
    Ok(replace_sql_ranges(sql, &ranges, "GROUPING_SETS"))
}

pub(super) fn rewrite_empty_grouping_set_tuples(sql: &str) -> Result<Cow<'_, str>, ParseError> {
    if !sql
        .as_bytes()
        .windows(13)
        .any(|word| word.eq_ignore_ascii_case(b"GROUPING_SETS"))
    {
        return Ok(Cow::Borrowed(sql));
    }
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let mut ranges = Vec::new();
    let mut skip_until = 0;
    visit_code_chars(sql, |idx, _| {
        if idx < skip_until || !keyword_at(sql, idx, "GROUPING_SETS") {
            return Ok(());
        }
        let open = skip_ws(sql, idx + "GROUPING_SETS".len());
        if sql.as_bytes().get(open) != Some(&b'(') {
            return Ok(());
        }
        let mut cursor = crate::sql_lex::SqlTokenCursor::new(&sql[open..], mode, MYSQL_VERSION_ID);
        let mut depth = 0usize;
        let mut tuple_open = None;
        while let Some(token) = cursor.next().map_err(|mut error| {
            error.offset += open;
            parse_lex_error(sql, error)
        })? {
            use crate::sql_lex::SqlTokenKind;
            match token.kind {
                SqlTokenKind::Symbol(b'(') => {
                    tuple_open = (depth == 1).then_some(open + token.span.start);
                    depth += 1;
                }
                SqlTokenKind::Symbol(b')') => {
                    if depth == 2 {
                        if let Some(start) = tuple_open.take() {
                            // Preserve the former whitespace-only empty tuple contract.
                            let close = open + token.span.start;
                            if sql[start + 1..close].chars().all(char::is_whitespace) {
                                ranges.push(start..open + token.span.end);
                            }
                        }
                    }
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        skip_until = open + token.span.end;
                        break;
                    }
                }
                _ => tuple_open = None,
            }
        }
        Ok(())
    })?;
    if ranges.is_empty() {
        return Ok(Cow::Borrowed(sql));
    }
    let marker = extensions::register(sql, extensions::Extension::EmptyGroupingSet);
    Ok(replace_sql_ranges(sql, &ranges, &format!("({marker}())")))
}

pub(super) fn strip_window_frame_exclusions(
    sql: &str,
    has_exclude: bool,
) -> Result<(std::borrow::Cow<'_, str>, Vec<WindowFrameExclusion>), ParseError> {
    if !has_exclude {
        return Ok((std::borrow::Cow::Borrowed(sql), Vec::new()));
    }
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let mut cursor = crate::sql_lex::SqlTokenCursor::new(sql, mode, MYSQL_VERSION_ID);
    let mut ranges = Vec::new();
    let mut exclusions = Vec::new();
    let mut previous = String::new();
    let mut scopes = vec![(false, None, false)]; // window, frame, named-window definitions
    while let Some(token) = cursor.next().map_err(|err| parse_lex_error(sql, err))? {
        use crate::sql_lex::SqlTokenKind;
        let word = cursor.token_text(&token);
        match token.kind {
            SqlTokenKind::Symbol(b'(') => {
                let named = scopes.last().unwrap().2;
                scopes.push((
                    previous == "OVER" || (named && previous == "AS"),
                    None,
                    false,
                ));
            }
            SqlTokenKind::Symbol(b')') if scopes.len() > 1 => {
                scopes.pop();
            }
            SqlTokenKind::Word => {
                let scope = scopes.last_mut().unwrap();
                if word.eq_ignore_ascii_case("WINDOW") {
                    scope.2 = true;
                }
                if scope.0
                    && ["ROWS", "RANGE", "GROUPS"]
                        .iter()
                        .any(|key| word.eq_ignore_ascii_case(key))
                {
                    scope.1 = Some(exclusions.len());
                    exclusions.push(WindowFrameExclusion::NoOthers);
                }
            }
            _ => {}
        }
        let is_exclusion = scopes.last().unwrap().1.is_some()
            && token.kind == crate::sql_lex::SqlTokenKind::Word
            && cursor.token_text(&token).eq_ignore_ascii_case("EXCLUDE")
            && matches!(previous.as_str(), "ROW" | "PRECEDING" | "FOLLOWING");
        previous = cursor.token_text(&token).to_ascii_uppercase();
        if !is_exclusion {
            continue;
        }
        let Some(next) = cursor.next().map_err(|err| parse_lex_error(sql, err))? else {
            return Err(parse_error_full_span(sql, "invalid window EXCLUDE clause"));
        };
        let next_word =
            (next.kind == crate::sql_lex::SqlTokenKind::Word).then(|| cursor.token_text(&next));
        let (end, exclusion) = if next_word.is_some_and(|word| word.eq_ignore_ascii_case("NO")) {
            let Some(others) = cursor.next().map_err(|err| parse_lex_error(sql, err))? else {
                return Err(parse_error_full_span(
                    sql,
                    "invalid EXCLUDE NO OTHERS clause",
                ));
            };
            if others.kind != crate::sql_lex::SqlTokenKind::Word
                || !cursor.token_text(&others).eq_ignore_ascii_case("OTHERS")
            {
                return Err(parse_error_full_span(
                    sql,
                    "invalid EXCLUDE NO OTHERS clause",
                ));
            }
            (others.span.end, WindowFrameExclusion::NoOthers)
        } else if next_word.is_some_and(|word| word.eq_ignore_ascii_case("CURRENT")) {
            let Some(row) = cursor.next().map_err(|err| parse_lex_error(sql, err))? else {
                return Err(parse_error_full_span(
                    sql,
                    "invalid EXCLUDE CURRENT ROW clause",
                ));
            };
            if row.kind != crate::sql_lex::SqlTokenKind::Word
                || !cursor.token_text(&row).eq_ignore_ascii_case("ROW")
            {
                return Err(parse_error_full_span(
                    sql,
                    "invalid EXCLUDE CURRENT ROW clause",
                ));
            }
            (row.span.end, WindowFrameExclusion::CurrentRow)
        } else if next_word.is_some_and(|word| word.eq_ignore_ascii_case("GROUP")) {
            (next.span.end, WindowFrameExclusion::Group)
        } else if next_word.is_some_and(|word| word.eq_ignore_ascii_case("TIES")) {
            (next.span.end, WindowFrameExclusion::Ties)
        } else {
            return Err(parse_error_full_span(sql, "invalid window EXCLUDE clause"));
        };
        ranges.push(token.span.start..end);
        exclusions[scopes.last().unwrap().1.expect("frame index")] = exclusion;
    }
    Ok((rewrite::mask_ranges(sql, &ranges), exclusions))
}

fn sql_location_to_byte(sql: &str, loc: sqlparser::tokenizer::Location) -> usize {
    position_in_source(sql, loc.line, loc.column).unwrap_or(sql.len())
}

/// Clause keywords that terminate the SELECT projection list at paren depth 0.
/// Used by [`capture_select_item_text`] to find where a projection item ends.
fn is_select_tail_keyword(word: &str) -> bool {
    matches!(
        word.to_ascii_uppercase().as_str(),
        "FROM"
            | "WHERE"
            | "GROUP"
            | "HAVING"
            | "ORDER"
            | "LIMIT"
            | "OFFSET"
            | "WINDOW"
            | "UNION"
            | "EXCEPT"
            | "INTERSECT"
            | "INTO"
            | "FOR"
            | "PROCEDURE"
            | "LOCK"
    )
}

/// P2-C1: recover the original SQL text of an unaliased projection item so its
/// result-set column can be named the MySQL way (`SELECT 1+1` → column `1+1`,
/// `COUNT(*)` → `COUNT(*)`, `NOW()` → `NOW()`). `start` is the sqlparser span
/// start of the item's expression. We scan forward tracking parenthesis depth
/// and string / identifier quoting, stopping at the first top-level comma (next
/// select item), top-level clause keyword (FROM/WHERE/…), or end of input.
/// Leading unary operators (`-`, `+`, `~`) that sqlparser omits from the
/// expression span are recovered by a short backward scan. Returns the trimmed
/// slice, or None when it comes out empty / the location is out of range.
pub(super) fn capture_select_item_text(
    sql: &str,
    start: sqlparser::tokenizer::Location,
) -> Result<Option<String>, ParseError> {
    let Some(range) = capture_select_item_range(sql, start) else {
        return Ok(None);
    };
    // Small labels fit the per-node allowance; large labels can repeat a
    // large literal through nested query scopes and require explicit admission.
    if range.len() > 1024 {
        reserve_parser_expansion(range.len())?;
    }
    Ok(Some(sql[range].to_string()))
}

fn capture_select_item_range(
    sql: &str,
    start: sqlparser::tokenizer::Location,
) -> Option<Range<usize>> {
    let bytes = sql.as_bytes();
    let mut begin = sql_location_to_byte(sql, start);
    if begin >= sql.len() {
        return None;
    }
    // Prefixed string spans may omit the charset introducer.
    if matches!(bytes[begin], b'\'' | b'"' | b'X' | b'x' | b'B' | b'b') {
        let mut end = begin;
        while end > 0 && bytes[end - 1].is_ascii_whitespace() {
            end -= 1;
        }
        let mut prefix = end;
        while prefix > 0 && (bytes[prefix - 1].is_ascii_alphanumeric() || bytes[prefix - 1] == b'_')
        {
            prefix -= 1;
        }
        if prefix < end && bytes[prefix] == b'_' {
            begin = prefix;
        }
    }
    // Recover the item prefix the span drops. sqlparser gives specialised
    // nodes (CAST, CONVERT, TRIM, SUBSTRING, POSITION, CEIL/FLOOR, EXISTS,
    // INTERVAL, typed literals, NOT/BINARY/unary operators, a parenthesized
    // subquery, ...) a span that starts at an inner operand rather than at
    // the item, so a label captured from the span would be `t` for
    // `SUBSTRING(t, 1, 2)` and several items would share one name. Every
    // projection item starts right after a depth-zero `,`, a comment/hint, or
    // the SELECT keyword and its modifiers, so walk back to that boundary.
    begin = select_item_start(sql, begin);
    let mut i = begin;
    let mut depth: i32 = 0;
    let mut end = sql.len();
    while i < sql.len() {
        let c = bytes[i];
        match c {
            b'\'' | b'"' | b'`' => {
                i = skip_quoted(sql, i);
            }
            b'(' | b'[' => {
                depth += 1;
                i += 1;
            }
            b')' | b']' => {
                if depth == 0 {
                    end = i;
                    break;
                }
                depth -= 1;
                i += 1;
            }
            b',' | b';' if depth == 0 => {
                end = i;
                break;
            }
            _ if depth == 0 && (c.is_ascii_alphabetic() || c == b'_') => {
                let word_start = i;
                let mut k = i;
                while k < sql.len() && (bytes[k].is_ascii_alphanumeric() || bytes[k] == b'_') {
                    k += 1;
                }
                if is_select_tail_keyword(&sql[word_start..k]) {
                    end = word_start;
                    break;
                }
                i = k;
            }
            _ => {
                i += 1;
            }
        }
    }
    let raw = sql[begin..end].trim();
    if raw.is_empty() {
        None
    } else {
        let leading = sql[begin..end].len() - sql[begin..end].trim_start().len();
        Some(begin + leading..begin + leading + raw.len())
    }
}

/// Start of the projection item containing byte `pos`, found by walking back
/// over the item's prefix (keywords, function names, opening parentheses and
/// unary operators) to the preceding item boundary.
fn select_item_start(sql: &str, pos: usize) -> usize {
    const ITEM_BOUNDARY_KEYWORDS: [&str; 12] = [
        "SELECT",
        "ALL",
        "DISTINCT",
        "DISTINCTROW",
        "HIGH_PRIORITY",
        "STRAIGHT_JOIN",
        "SQL_SMALL_RESULT",
        "SQL_BIG_RESULT",
        "SQL_BUFFER_RESULT",
        "SQL_NO_CACHE",
        "SQL_CACHE",
        "SQL_CALC_FOUND_ROWS",
    ];
    let bytes = sql.as_bytes();
    let mut start = pos;
    let mut i = pos;
    while i > 0 {
        match bytes[i - 1] {
            c if c.is_ascii_whitespace() => i -= 1,
            b'(' | b'-' | b'+' | b'~' | b'!' => {
                i -= 1;
                start = i;
            }
            c if c.is_ascii_alphanumeric() || c == b'_' => {
                let word_end = i;
                while i > 0 && (bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_') {
                    i -= 1;
                }
                if ITEM_BOUNDARY_KEYWORDS
                    .iter()
                    .any(|keyword| sql[i..word_end].eq_ignore_ascii_case(keyword))
                {
                    break;
                }
                start = i;
            }
            // `,` ends the previous item; `*/` ends a hint or comment. Quotes
            // and closing brackets never precede an item's start inside it.
            _ => break,
        }
    }
    start
}

/// Retain expression labels while the wire decoder rewrites literal octets.
/// Only add aliases at parser-proven projection boundaries; execution keeps
/// the byte-preserving literals and never reparses values from display text.
pub(crate) fn preserve_decoded_projection_labels(original: &str, decoded: &mut String, mode: u16) {
    fn query_labels(
        original: &Query,
        decoded: &Query,
        source: &str,
        target: &str,
        edits: &mut Vec<(usize, String)>,
    ) {
        if let (Some(a), Some(b)) = (&original.with, &decoded.with) {
            for (a, b) in a.cte_tables.iter().zip(&b.cte_tables) {
                query_labels(&a.query, &b.query, source, target, edits);
            }
        }
        body_labels(&original.body, &decoded.body, source, target, edits);
    }
    fn table_labels(
        a: &TableFactor,
        b: &TableFactor,
        source: &str,
        target: &str,
        edits: &mut Vec<(usize, String)>,
    ) {
        if let (
            TableFactor::Derived { subquery: a, .. },
            TableFactor::Derived { subquery: b, .. },
        ) = (a, b)
        {
            query_labels(a, b, source, target, edits);
        }
    }
    fn body_labels(
        a: &SetExpr,
        b: &SetExpr,
        source: &str,
        target: &str,
        edits: &mut Vec<(usize, String)>,
    ) {
        match (a, b) {
            (SetExpr::Select(a), SetExpr::Select(b))
                if a.projection.len() == b.projection.len() =>
            {
                for (a, b) in a.projection.iter().zip(&b.projection) {
                    let (SqlSelectItem::UnnamedExpr(a), SqlSelectItem::UnnamedExpr(b)) = (a, b)
                    else {
                        continue;
                    };
                    let mut shape = a;
                    while let SqlExpr::Nested(inner) = shape {
                        shape = inner;
                    }
                    if matches!(shape, SqlExpr::Prefixed { .. }) {
                        continue;
                    }
                    let Ok(expr) = convert_expr(a) else { continue };
                    if crate::engine::expr::expr_name(&expr) != "expr" {
                        continue;
                    }
                    let (Some(a), Some(b)) = (
                        capture_select_item_range(source, a.span().start),
                        capture_select_item_range(target, b.span().start),
                    ) else {
                        continue;
                    };
                    if source[a.clone()] != target[b.clone()] {
                        edits.push((b.end, format!(" AS `{}`", source[a].replace('`', "``"))));
                    }
                }
                for (a, b) in a.from.iter().zip(&b.from) {
                    table_labels(&a.relation, &b.relation, source, target, edits);
                    for (a, b) in a.joins.iter().zip(&b.joins) {
                        table_labels(&a.relation, &b.relation, source, target, edits);
                    }
                }
            }
            (SetExpr::Query(a), SetExpr::Query(b)) => query_labels(a, b, source, target, edits),
            (
                SetExpr::SetOperation {
                    left: a, right: ar, ..
                },
                SetExpr::SetOperation {
                    left: b, right: br, ..
                },
            ) => {
                body_labels(a, b, source, target, edits);
                body_labels(ar, br, source, target, edits);
            }
            _ => {}
        }
    }
    let _mode = ParserSqlModeGuard::activate(mode);
    let (keyword, length) = extract_first_keyword(original);
    if matches!(
        &keyword[..length],
        b"INSERT" | b"REPLACE" | b"UPDATE" | b"DELETE" | b"CREATE" | b"ALTER"
    ) && !original
        .trim_end()
        .trim_end_matches(';')
        .as_bytes()
        .contains(&b';')
    {
        return;
    }
    let Ok(_memory) = ParserMemoryGuard::enter(decoded) else {
        return;
    };
    let _ = with_parser_stack(|| {
        let dialect = MydbDialect::default();
        let (Ok(a), Ok(b)) = (
            adapter::parse_sql(&dialect, original),
            adapter::parse_sql(&dialect, decoded),
        ) else {
            return;
        };
        if a.len() != b.len() {
            return;
        }
        let _source = ParserRawSqlGuard::activate(original);
        let mut edits = Vec::new();
        for (a, b) in a.iter().zip(&b) {
            if let (SqlStatement::Query(a), SqlStatement::Query(b)) = (a, b) {
                query_labels(a, b, original, decoded, &mut edits);
            }
        }
        edits.sort_unstable_by_key(|(position, _)| std::cmp::Reverse(*position));
        for (position, alias) in edits {
            decoded.insert_str(position, &alias);
        }
    });
}

fn apply_stripped_window_frame_exclusions(
    statement: &mut Statement,
    exclusions: &[WindowFrameExclusion],
) -> Result<(), ParseError> {
    if exclusions.is_empty() {
        return Ok(());
    }
    let mut pending = std::collections::VecDeque::from(exclusions.to_vec());
    let mut visitor = WindowFrameExclusionApplier {
        pending: &mut pending,
    };
    visit::visit_statement_mut(&mut visitor, statement);
    if pending.is_empty() {
        Ok(())
    } else {
        Err(parse_error_at_zero(
            "EXCLUDE clause used without a window frame",
        ))
    }
}

struct WindowFrameExclusionApplier<'a> {
    pending: &'a mut std::collections::VecDeque<WindowFrameExclusion>,
}

impl visit::AstVisitorMut for WindowFrameExclusionApplier<'_> {
    fn enter_window_frame(&mut self, frame: &mut WindowFrame) {
        if let Some(exclusion) = self.pending.pop_front() {
            frame.exclusion = exclusion;
        }
    }
}

// DDL attribute compatibility rewrites.
/// Stash for `CREATE TABLE … PCTFREE/PCTUSED` values rescued from the
/// raw SQL before sqlparser sees it.
#[derive(Default, Debug, Clone, Copy)]
pub(super) struct CreateTablePctOverride {
    pctfree: Option<u8>,
    pctused: Option<u8>,
}

/// Strip mydb's physical zone-map table options before sqlparser sees them.
/// Current sqlparser does not admit extension table options, so retaining the
/// values out-of-band is the same compatibility technique used for PCTFREE.
pub(super) fn strip_zone_map_options<'a>(
    sql: &'a str,
    require_table_body: bool,
) -> Result<(std::borrow::Cow<'a, str>, Vec<(String, String)>), ParseError> {
    if require_table_body {
        if !first_keyword_is(sql, b"CREATE") {
            return Ok((std::borrow::Cow::Borrowed(sql), Vec::new()));
        }
        let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
        let mut cursor = crate::sql_lex::SqlTokenCursor::new(sql, mode, MYSQL_VERSION_ID);
        cursor
            .eat_keyword(b"CREATE")
            .map_err(|err| parse_lex_error(sql, err))?;
        cursor
            .eat_keyword(b"TEMPORARY")
            .map_err(|err| parse_lex_error(sql, err))?;
        if !cursor
            .eat_keyword(b"TABLE")
            .map_err(|err| parse_lex_error(sql, err))?
        {
            return Ok((std::borrow::Cow::Borrowed(sql), Vec::new()));
        }
    }
    if !contains_keyword_ci(sql.as_bytes(), b"ZONE_MAP")
        && !contains_keyword_ci(sql.as_bytes(), b"ZONE_SIZE")
    {
        return Ok((std::borrow::Cow::Borrowed(sql), Vec::new()));
    }
    let bytes = sql.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut options = Vec::new();
    let mut i = 0usize;
    let mut depth = 0usize;
    let mut saw_body = false;
    let mut preceding_comma = None;
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let mut segments = crate::sql_lex::segments(sql, mode, MYSQL_VERSION_ID).peekable();
    while i < bytes.len() {
        // Use the shared lexer so comments, escaped quotes and SQL modes
        // cannot turn inert text into physical table options.
        while let Some(segment) = segments.peek() {
            let segment = segment
                .as_ref()
                .map_err(|err| parse_error_at_zero(&err.to_string()))?;
            if segment.range.end <= i {
                segments.next();
                continue;
            }
            break;
        }
        if let Some(Ok(segment)) = segments.peek() {
            if !matches!(segment.kind, crate::sql_lex::SqlSegmentKind::Code) {
                if matches!(segment.kind, crate::sql_lex::SqlSegmentKind::Quoted { .. }) {
                    preceding_comma = None;
                }
                out.extend_from_slice(&bytes[i..segment.range.end]);
                i = segment.range.end;
                continue;
            }
        }
        let b = bytes[i];
        // CTAS query text is not a table-option list, even when it contains
        // parentheses followed by an identifier named zone_map/zone_size.
        if require_table_body
            && depth == 0
            && (keyword_at(sql, i, "AS") || keyword_at(sql, i, "SELECT"))
        {
            out.extend_from_slice(&bytes[i..]);
            break;
        }
        if b == b'(' {
            preceding_comma = None;
            depth += 1;
            saw_body = true;
            out.push(b);
            i += 1;
            continue;
        }
        if b == b')' {
            preceding_comma = None;
            depth = depth.saturating_sub(1);
            out.push(b);
            i += 1;
            continue;
        }
        let eligible = depth == 0 && (!require_table_body || saw_body);
        let boundary = i == 0 || (!bytes[i - 1].is_ascii_alphanumeric() && bytes[i - 1] != b'_');
        let matched_key = if eligible
            && boundary
            && bytes[i..].len() >= 8
            && bytes[i..i + 8].eq_ignore_ascii_case(b"ZONE_MAP")
        {
            Some("ZONE_MAP")
        } else if eligible
            && boundary
            && bytes[i..].len() >= 9
            && bytes[i..i + 9].eq_ignore_ascii_case(b"ZONE_SIZE")
        {
            Some("ZONE_SIZE")
        } else {
            None
        };
        let Some(key) = matched_key else {
            if !b.is_ascii_whitespace() {
                preceding_comma = (b == b',').then_some(out.len());
            }
            out.push(b);
            i += 1;
            continue;
        };
        let end = i + key.len();
        if end < bytes.len() && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
            out.push(b);
            i += 1;
            continue;
        }
        let mut j = end;
        while j < bytes.len() && bytes[j].is_ascii_whitespace() {
            j += 1;
        }
        if j < bytes.len() && bytes[j] == b'=' {
            j += 1;
        }
        while j < bytes.len() && bytes[j].is_ascii_whitespace() {
            j += 1;
        }
        if bytes
            .get(j)
            .is_some_and(|b| matches!(b, b'\'' | b'"' | b'`'))
        {
            return Err(parse_error_at_zero(&format!(
                "{key} expects an unquoted value"
            )));
        }
        let value_start = j;
        while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
            j += 1;
        }
        if j == value_start {
            return Err(parse_error_at_zero(&format!("{key} expects a value")));
        }
        let raw = std::str::from_utf8(&bytes[value_start..j])
            .map_err(|_| parse_error_at_zero("zone-map option is not valid UTF-8"))?;
        let entry = accept_generic_table_option(key, raw)
            .map_err(|message| parse_error_at_zero(&message))?
            .ok_or_else(|| parse_error_at_zero(&format!("unsupported {key} value")))?;
        options.retain(|(existing, _): &(String, String)| existing != key);
        options.push(entry);
        // Consume a following comma. If the option was preceded by a comma,
        // remove that already-copied separator instead.
        while j < bytes.len() && bytes[j].is_ascii_whitespace() {
            j += 1;
        }
        if j < bytes.len() && bytes[j] == b',' {
            j += 1;
        } else if let Some(comma) = preceding_comma {
            out[comma] = b' ';
        }
        // Preserve all comment bytes and line breaks: trimming a newline
        // after a line comment would comment out the following table option.
        preceding_comma = None;
        out.push(b' ');
        i = j;
    }
    if options.is_empty() {
        return Ok((std::borrow::Cow::Borrowed(sql), options));
    }
    let cleaned = String::from_utf8(out)
        .map_err(|_| parse_error_at_zero("zone-map option strip produced invalid UTF-8"))?;
    Ok((std::borrow::Cow::Owned(cleaned), options))
}

impl CreateTablePctOverride {
    fn is_empty(&self) -> bool {
        self.pctfree.is_none() && self.pctused.is_none()
    }
}

/// Normalize MySQL's legacy `TEXT(M)` spelling to the concrete LOB family
/// selected by `M`. sqlparser accepts `BLOB(M)` but not `TEXT(M)`, while MySQL
/// treats both lengths as a request for the smallest fitting LOB type.
///
/// The rewrite is restricted to CREATE/ALTER statements and scans quoted
/// strings, identifiers and comments, so a literal such as `'TEXT(70000)'`
/// remains byte-for-byte unchanged.
pub(super) fn rewrite_legacy_text_lengths(
    sql: &str,
) -> Result<std::borrow::Cow<'_, str>, ParseError> {
    let (head, head_len) = extract_first_keyword(sql);
    if !matches!(&head[..head_len], b"CREATE" | b"ALTER") {
        return Ok(std::borrow::Cow::Borrowed(sql));
    }

    let bytes = sql.as_bytes();
    let is_ident = |byte: u8| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$');
    let mut replacements: Vec<(usize, usize, &'static str)> = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' | b'`' => {
                i = quoted_span_end(bytes, i);
                continue;
            }
            b'#' => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
                continue;
            }
            b'-' if crate::sql_lex::is_dash_line_comment_at(sql, i) => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
                continue;
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    i += 1;
                }
                i = (i + 2).min(bytes.len());
                continue;
            }
            byte if is_ident(byte) && (i == 0 || !is_ident(bytes[i - 1])) => {
                let word_start = i;
                while i < bytes.len() && is_ident(bytes[i]) {
                    i += 1;
                }
                let word = &bytes[word_start..i];
                let is_text = word.eq_ignore_ascii_case(b"TEXT");
                let has_legacy_width = is_text
                    || word.eq_ignore_ascii_case(b"BLOB")
                    || word.eq_ignore_ascii_case(b"INT")
                    || word.eq_ignore_ascii_case(b"INTEGER")
                    || word.eq_ignore_ascii_case(b"CHAR")
                    || word.eq_ignore_ascii_case(b"BINARY");
                if !has_legacy_width {
                    continue;
                }
                let mut open = i;
                while open < bytes.len() && bytes[open].is_ascii_whitespace() {
                    open += 1;
                }
                if bytes.get(open) != Some(&b'(') {
                    continue;
                }
                let mut number_start = open + 1;
                while number_start < bytes.len() && bytes[number_start].is_ascii_whitespace() {
                    number_start += 1;
                }
                let mut number_end = number_start;
                while number_end < bytes.len() && bytes[number_end].is_ascii_digit() {
                    number_end += 1;
                }
                if number_end == number_start {
                    continue;
                }
                let mut close = number_end;
                while close < bytes.len() && bytes[close].is_ascii_whitespace() {
                    close += 1;
                }
                if bytes.get(close) != Some(&b')') {
                    continue;
                }
                let length_text = &sql[number_start..number_end];
                let length = length_text.parse::<u64>().unwrap_or(u64::MAX);
                if length > u32::MAX as u64 {
                    return Err(ParseError {
                        spec: crate::error::catalog::ER_TOO_BIG_DISPLAYWIDTH,
                        message: format!(
                            "Display width out of range for '{}'",
                            String::from_utf8_lossy(word)
                        ),
                        span: Span {
                            start: number_start,
                            end: number_end,
                        },
                        line: 1,
                        column: number_start + 1,
                        expected: Vec::new(),
                    });
                }
                if !is_text {
                    i = close + 1;
                    continue;
                }
                let concrete = match length {
                    0..=255 => "TINYTEXT",
                    256..=65_535 => "TEXT",
                    65_536..=16_777_215 => "MEDIUMTEXT",
                    16_777_216..=4_294_967_295 => "LONGTEXT",
                    _ => unreachable!("u32 length was checked above"),
                };
                replacements.push((word_start, close + 1, concrete));
                i = close + 1;
                continue;
            }
            _ => i += 1,
        }
    }
    if replacements.is_empty() {
        return Ok(std::borrow::Cow::Borrowed(sql));
    }
    let edits = replacements
        .iter()
        .map(|(start, end, replacement)| TextEdit {
            range: *start..*end,
            replacement,
        })
        .collect::<Vec<_>>();
    rewrite_source_edits(sql, &edits)
}

/// Normalize MySQL's `ALTER TABLE ... DROP KEY name` spelling to the
/// equivalent `DROP INDEX name` understood by sqlparser.
///
/// Only word tokens in an ALTER statement are considered, so comments,
/// string literals and quoted identifiers containing `DROP KEY` are left
/// untouched.
pub(super) fn rewrite_alter_drop_key(sql: &str) -> Result<std::borrow::Cow<'_, str>, ParseError> {
    let (head, head_len) = extract_first_keyword(sql);
    if &head[..head_len] != b"ALTER" {
        return Ok(std::borrow::Cow::Borrowed(sql));
    }

    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let mut cursor = crate::sql_lex::SqlTokenCursor::new(sql, mode, MYSQL_VERSION_ID);
    let mut previous_was_drop = false;
    let mut replacements = Vec::new();
    while let Some(token) = cursor.next().map_err(|err| parse_lex_error(sql, err))? {
        if token.kind != crate::sql_lex::SqlTokenKind::Word {
            previous_was_drop = false;
            continue;
        }
        let word = cursor.token_text(&token);
        if previous_was_drop && word.eq_ignore_ascii_case("KEY") {
            replacements.push(token.span);
            previous_was_drop = false;
        } else {
            previous_was_drop = word.eq_ignore_ascii_case("DROP");
        }
    }
    if replacements.is_empty() {
        return Ok(std::borrow::Cow::Borrowed(sql));
    }

    let added = replacements.len() * ("INDEX".len() - "KEY".len());
    let mut rewritten = String::with_capacity(sql.len() + added);
    let mut copied = 0usize;
    for span in replacements {
        rewritten.push_str(&sql[copied..span.start]);
        rewritten.push_str("INDEX");
        copied = span.end;
    }
    rewritten.push_str(&sql[copied..]);
    Ok(std::borrow::Cow::Owned(rewritten))
}

/// Reject CAST/CONVERT character and binary display widths before sqlparser
/// converts them to its numeric AST.  In particular, widths with hundreds of
/// digits overflow sqlparser's integer representation and used to be silently
/// represented as an unspecified length.
///
/// This is deliberately context-aware: `CHAR(4294967296)` is a regular MySQL
/// function call and must not be mistaken for the target type in
/// `CAST(x AS CHAR(n))` or `CONVERT(x, CHAR(n))`.
pub(super) fn validate_cast_type_widths(sql: &str) -> Result<(), ParseError> {
    #[derive(Clone, Copy)]
    enum Frame {
        Other,
        Cast { seen_as: bool },
        Convert { seen_comma: bool },
    }

    fn is_ident(byte: u8) -> bool {
        byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$')
    }

    fn width_error(sql: &str, start: usize, end: usize, type_name: &[u8]) -> ParseError {
        let prefix = &sql[..start];
        let line = prefix.bytes().filter(|byte| *byte == b'\n').count() + 1;
        let column = prefix
            .rfind('\n')
            .map_or(start + 1, |newline| start.saturating_sub(newline));
        ParseError {
            spec: crate::error::catalog::ER_TOO_BIG_DISPLAYWIDTH,
            message: format!(
                "Display width out of range for '{}'",
                String::from_utf8_lossy(type_name)
            ),
            span: Span { start, end },
            line,
            column,
            expected: Vec::new(),
        }
    }

    let bytes = sql.as_bytes();
    let mut frames: Vec<Frame> = Vec::new();
    let mut pending_frame = None;
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' | b'`' => {
                i = quoted_span_end(bytes, i);
            }
            b'#' => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'-' if crate::sql_lex::is_dash_line_comment_at(sql, i) => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    i += 1;
                }
                i = (i + 2).min(bytes.len());
            }
            b'(' => {
                frames.push(pending_frame.take().unwrap_or(Frame::Other));
                i += 1;
            }
            b')' => {
                frames.pop();
                pending_frame = None;
                i += 1;
            }
            b',' => {
                if let Some(Frame::Convert { seen_comma }) = frames.last_mut() {
                    *seen_comma = true;
                }
                pending_frame = None;
                i += 1;
            }
            byte if is_ident(byte) && (i == 0 || !is_ident(bytes[i - 1])) => {
                let word_start = i;
                while i < bytes.len() && is_ident(bytes[i]) {
                    i += 1;
                }
                let word = &bytes[word_start..i];
                if word.eq_ignore_ascii_case(b"AS") {
                    if let Some(Frame::Cast { seen_as }) = frames.last_mut() {
                        *seen_as = true;
                    }
                }

                let is_cast_target = matches!(frames.last(), Some(Frame::Cast { seen_as: true }));
                let is_convert_target =
                    matches!(frames.last(), Some(Frame::Convert { seen_comma: true }));
                let is_width_type = word.eq_ignore_ascii_case(b"CHAR")
                    || word.eq_ignore_ascii_case(b"NCHAR")
                    || word.eq_ignore_ascii_case(b"BINARY");
                let mut open = i;
                while open < bytes.len() && bytes[open].is_ascii_whitespace() {
                    open += 1;
                }
                if is_width_type
                    && (is_cast_target || is_convert_target)
                    && bytes.get(open) == Some(&b'(')
                {
                    let mut number_start = open + 1;
                    while number_start < bytes.len() && bytes[number_start].is_ascii_whitespace() {
                        number_start += 1;
                    }
                    let mut number_end = number_start;
                    while number_end < bytes.len() && bytes[number_end].is_ascii_digit() {
                        number_end += 1;
                    }
                    if number_end > number_start {
                        let mut close = number_end;
                        while close < bytes.len() && bytes[close].is_ascii_whitespace() {
                            close += 1;
                        }
                        if bytes.get(close) == Some(&b')')
                            && sql[number_start..number_end].parse::<u32>().is_err()
                        {
                            return Err(width_error(sql, number_start, number_end, word));
                        }
                    }
                }

                pending_frame = if word.eq_ignore_ascii_case(b"CAST") {
                    Some(Frame::Cast { seen_as: false })
                } else if word.eq_ignore_ascii_case(b"CONVERT") {
                    Some(Frame::Convert { seen_comma: false })
                } else {
                    None
                };
            }
            byte if byte.is_ascii_whitespace() => i += 1,
            _ => {
                pending_frame = None;
                i += 1;
            }
        }
    }
    Ok(())
}

/// Remove index-level `KEY_BLOCK_SIZE [=] N` clauses (not in sqlparser
/// 0.61's index-option vocabulary) before parse. Table-level
/// occurrences are left alone: in CREATE TABLE they sit at paren depth
/// 0 where sqlparser parses them as a plain table option; the manual
/// ALTER-options pre-parse short-circuits before sqlparser, so any
/// occurrence reaching this rewrite inside an ALTER is index-level.
/// The clause is a physical-compression hint mydb genuinely ignores —
/// the same treatment InnoDB gives it outside ROW_FORMAT=COMPRESSED.
pub(super) fn strip_index_key_block_size(
    sql: &str,
    has_key_block_size: bool,
) -> std::borrow::Cow<'_, str> {
    if !has_key_block_size {
        return std::borrow::Cow::Borrowed(sql);
    }
    let bytes = sql.as_bytes();
    let (head_buf, head_len) = extract_first_keyword(sql);
    let alter_mode = &head_buf[..head_len] == b"ALTER";
    // CREATE TABLE keeps depth-0 occurrences (real table option parsed
    // by sqlparser); CREATE [UNIQUE|FULLTEXT|…] INDEX has no table
    // options, so everything is index-level there.
    let create_table_mode = !alter_mode && {
        let rest = sql[..].trim_start();
        let rest = rest[rest.find(char::is_whitespace).unwrap_or(rest.len())..].trim_start();
        let second: String = rest
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect::<String>()
            .to_ascii_uppercase();
        second == "TABLE" || second == "TEMPORARY"
    };
    let is_ident_byte = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'$';
    let mut out: Vec<u8> = bytes.to_vec();
    let mut changed = false;
    let mut depth: i32 = 0;
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' | b'`' => {
                i = quoted_span_end(bytes, i);
                continue;
            }
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ => {}
        }
        if is_ident_byte(bytes[i]) && (i == 0 || !is_ident_byte(bytes[i - 1])) {
            let start = i;
            while i < bytes.len() && is_ident_byte(bytes[i]) {
                i += 1;
            }
            let word = &bytes[start..i];
            if word.len() == 14
                && word.eq_ignore_ascii_case(b"KEY_BLOCK_SIZE")
                && (!create_table_mode || depth >= 1)
            {
                let mut j = i;
                while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                    j += 1;
                }
                if bytes.get(j) == Some(&b'=') {
                    j += 1;
                    while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                        j += 1;
                    }
                }
                let digits_start = j;
                while j < bytes.len() && bytes[j].is_ascii_digit() {
                    j += 1;
                }
                if j > digits_start {
                    for slot in &mut out[start..j] {
                        *slot = b' ';
                    }
                    changed = true;
                    i = j;
                }
            }
            continue;
        }
        i += 1;
    }
    if !changed {
        return std::borrow::Cow::Borrowed(sql);
    }
    // The rewrite only writes ASCII spaces over ASCII bytes, so the
    // result is valid UTF-8 by construction.
    std::borrow::Cow::Owned(String::from_utf8(out).expect("space-blanking preserves UTF-8"))
}

/// Strip `VISIBLE` / `INVISIBLE` from inline index definitions in
/// CREATE TABLE bodies and ALTER TABLE … ADD INDEX actions (sqlparser
/// 0.61 has no index-visibility vocabulary outside CREATE INDEX) and
/// bind each marker to its key list with a temporary sentinel column.
/// Conversion may insert inline UNIQUE / prefix PRIMARY indexes, so a
/// position in the converted index vector is not a stable identity. Column-level INVISIBLE is
/// left untouched — sqlparser parses that as `ColumnOption::Invisible`.
/// A PRIMARY KEY marked INVISIBLE is rejected like MySQL (error 3522).
/// Shared byte scanner for CREATE/ALTER index-definition options. Each caller
/// chooses first/last closing parenthesis semantics and owns its rewrite data.
#[derive(Clone, Copy, PartialEq)]
enum IndexSegmentKind {
    Unknown,
    Index,
    PrimaryKey,
    Other,
}

struct IndexDefinitionWord<'a> {
    text: &'a [u8],
    start: usize,
    end: usize,
    kind: IndexSegmentKind,
    key_close: Option<usize>,
}

struct IndexDefinitionScanner<'a> {
    bytes: &'a [u8],
    position: usize,
    depth: i32,
    base_depth: i32,
    alter: bool,
    first_close: bool,
    kind: IndexSegmentKind,
    pending_add: bool,
    pending_constraint_name: bool,
    key_close: Option<usize>,
}

impl<'a> IndexDefinitionScanner<'a> {
    fn new(sql: &'a str, first_close: bool) -> Self {
        let (head, len) = extract_first_keyword(sql);
        let alter = &head[..len] == b"ALTER";
        Self {
            bytes: sql.as_bytes(),
            position: 0,
            depth: 0,
            base_depth: if alter { 0 } else { 1 },
            alter,
            first_close,
            kind: IndexSegmentKind::Unknown,
            pending_add: !alter,
            pending_constraint_name: false,
            key_close: None,
        }
    }
    fn reset_segment(&mut self, pending_add: bool) {
        self.kind = IndexSegmentKind::Unknown;
        self.pending_add = pending_add;
        self.pending_constraint_name = false;
        self.key_close = None;
    }
    fn skip_to(&mut self, position: usize) {
        self.position = position;
    }
}

impl<'a> Iterator for IndexDefinitionScanner<'a> {
    type Item = IndexDefinitionWord<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        let bytes = self.bytes;
        let ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'$';
        while self.position < bytes.len() {
            let i = self.position;
            self.position += 1;
            match bytes[i] {
                b'\'' | b'"' | b'`' => {
                    self.position = quoted_span_end(bytes, i);
                    self.pending_constraint_name = false;
                    continue;
                }
                b'(' => {
                    self.depth += 1;
                    if !self.alter && self.depth == 1 {
                        self.reset_segment(true);
                    }
                    continue;
                }
                b')' => {
                    self.depth -= 1;
                    if self.depth == self.base_depth
                        && (!self.first_close || self.key_close.is_none())
                    {
                        self.key_close = Some(i);
                    }
                    continue;
                }
                b',' if self.depth == self.base_depth => {
                    self.reset_segment(!self.alter);
                    continue;
                }
                _ => {}
            }
            if !ident(bytes[i]) || (i != 0 && ident(bytes[i - 1])) {
                continue;
            }
            while self.position < bytes.len() && ident(bytes[self.position]) {
                self.position += 1;
            }
            if self.depth != self.base_depth {
                continue;
            }
            if self.pending_constraint_name {
                self.pending_constraint_name = false;
                continue;
            }
            let text = &bytes[i..self.position];
            let kind = self.kind;
            use IndexSegmentKind::*;
            match kind {
                Unknown | Other if self.alter && text.eq_ignore_ascii_case(b"ADD") => {
                    self.kind = Unknown;
                    self.pending_add = true;
                }
                Unknown if self.pending_add && text.eq_ignore_ascii_case(b"CONSTRAINT") => {
                    self.pending_constraint_name = true
                }
                Unknown
                    if self.pending_add
                        && [
                            b"UNIQUE".as_slice(),
                            b"INDEX",
                            b"KEY",
                            b"FULLTEXT",
                            b"SPATIAL",
                        ]
                        .iter()
                        .any(|word| text.eq_ignore_ascii_case(word)) =>
                {
                    self.kind = Index
                }
                Unknown if self.pending_add && text.eq_ignore_ascii_case(b"PRIMARY") => {
                    self.kind = PrimaryKey
                }
                Unknown => self.kind = Other,
                _ => {}
            }
            return Some(IndexDefinitionWord {
                text,
                start: i,
                end: self.position,
                kind,
                key_close: self.key_close,
            });
        }
        None
    }
}

pub(super) fn strip_inline_index_visibility(
    sql: &str,
    enabled: bool,
) -> Result<(std::borrow::Cow<'_, str>, Vec<(String, bool)>), ParseError> {
    if !enabled {
        return Ok((std::borrow::Cow::Borrowed(sql), Vec::new()));
    }
    let bytes = sql.as_bytes();
    let mut out: Vec<u8> = bytes.to_vec();
    let mut targets: Vec<(String, bool)> = Vec::new();
    let mut injections = Vec::new();
    for word in IndexDefinitionScanner::new(sql, true) {
        if word.kind == IndexSegmentKind::Index
            && (word.text.eq_ignore_ascii_case(b"INVISIBLE")
                || word.text.eq_ignore_ascii_case(b"VISIBLE"))
        {
            let visible = word.text.eq_ignore_ascii_case(b"VISIBLE");
            let close = word
                .key_close
                .ok_or_else(|| parse_error_at_zero("index visibility must follow its key list"))?;
            let seq = targets.len();
            let sentinel = extensions::register(sql, extensions::Extension::IndexVisibility);
            targets.push((sentinel, visible));
            injections.push((close, seq));
            out[word.start..word.end].fill(b' ');
        } else if word.kind == IndexSegmentKind::PrimaryKey
            && word.text.eq_ignore_ascii_case(b"INVISIBLE")
        {
            return Err(parse_error_with_spec_at_zero(
                crate::error::catalog::ER_PK_INDEX_CANT_BE_INVISIBLE,
                "A primary key index cannot be invisible",
            ));
        }
    }
    if targets.is_empty() {
        return Ok((std::borrow::Cow::Borrowed(sql), Vec::new()));
    }
    let mut rewritten = Vec::with_capacity(out.len() + injections.len() * 32);
    let mut previous = 0;
    for (position, seq) in injections {
        rewritten.extend_from_slice(&out[previous..position]);
        rewritten.extend_from_slice(format!(", `{}`", targets[seq].0).as_bytes());
        previous = position;
    }
    rewritten.extend_from_slice(&out[previous..]);
    let cleaned = String::from_utf8(rewritten).map_err(|_| {
        parse_error_at_zero("internal error: visibility strip produced invalid UTF-8")
    })?;
    Ok((std::borrow::Cow::Owned(cleaned), targets))
}

/// Resolve visibility by the sentinel carried by its original index.
fn apply_inline_index_visibility(
    statement: &mut Statement,
    targets: &[(String, bool)],
) -> Result<(), ParseError> {
    if targets.is_empty() {
        return Ok(());
    }
    let mut applied = vec![false; targets.len()];
    for_each_inline_index(statement, |index| {
        for (seq, (sentinel, visible)) in targets.iter().enumerate() {
            if let Some(pos) = index.columns.iter().rposition(|name| name == sentinel) {
                if applied[seq] {
                    return Err(parse_error_at_zero("ambiguous index visibility marker"));
                }
                remove_index_key_part(index, pos);
                index.visible = *visible;
                applied[seq] = true;
            }
        }
        Ok(())
    })?;
    if applied.iter().any(|value| !value) {
        return Err(parse_error_at_zero(
            "cannot attribute index VISIBLE/INVISIBLE marker",
        ));
    }
    Ok(())
}

/// Reserved key-part name used to thread an inline / ALTER `INCLUDE (...)`
/// clause through sqlparser, which only understands `INCLUDE` on a
/// standalone `CREATE INDEX`. [`strip_inline_index_includes`] removes the
/// clause and injects `\`__mydb_inc_<seq>\`` as the last key column of the
/// owning index; [`apply_inline_index_includes`] recognises that sentinel,
/// pops it back off the key, and moves the recorded columns into
/// `IndexDef::included_columns`. The `<seq>` is the index into the records
/// vector returned by the strip.
///
/// Pre-process `CREATE TABLE ... (KEY k (a) INCLUDE (b, c))` and
/// `ALTER TABLE ... ADD INDEX k (a) INCLUDE (b, c)`. sqlparser 0.62 rejects
/// `INCLUDE` in both positions (it only parses it on a standalone
/// `CREATE INDEX`), so we strip the clause out of the SQL text and inject a
/// sentinel key column so the surviving index round-trips through the
/// parser bound to its real definition. Returns the rewritten SQL plus one
/// `Vec<String>` of included column names per stripped clause, indexed by
/// the `<seq>` embedded in the sentinel name. Skips string / backtick
/// literals. No-op (borrowed) when `enabled` is false or nothing matched.
pub(super) fn strip_inline_index_includes(
    sql: &str,
    enabled: bool,
) -> Result<(std::borrow::Cow<'_, str>, Vec<Vec<String>>), ParseError> {
    if !enabled {
        return Ok((std::borrow::Cow::Borrowed(sql), Vec::new()));
    }
    let bytes = sql.as_bytes();
    let mut out: Vec<u8> = bytes.to_vec();
    let mut records: Vec<Vec<String>> = Vec::new();
    // (byte position of the key-list ')', seq) — where to splice the
    // sentinel key column.
    let mut injections: Vec<(usize, usize)> = Vec::new();
    let mut scanner = IndexDefinitionScanner::new(sql, false);
    while let Some(word) = scanner.next() {
        if !word.text.eq_ignore_ascii_case(b"INCLUDE") {
            continue;
        }
        if word.kind == IndexSegmentKind::PrimaryKey {
            return Err(parse_error_at_zero(
                "PRIMARY KEY cannot declare INCLUDE columns",
            ));
        }
        if word.kind != IndexSegmentKind::Index {
            continue;
        }
        let mut open = word.end;
        while open < bytes.len() && bytes[open].is_ascii_whitespace() {
            open += 1;
        }
        if bytes.get(open) != Some(&b'(') {
            continue;
        }
        let (cols, close) = parse_include_column_list(bytes, open)?;
        let key_close = word
            .key_close
            .ok_or_else(|| parse_error_at_zero("INCLUDE must follow an index key column list"))?;
        injections.push((key_close, records.len()));
        records.push(cols);
        out[word.start..=close].fill(b' ');
        scanner.skip_to(close + 1);
    }
    if injections.is_empty() {
        return Ok((std::borrow::Cow::Borrowed(sql), Vec::new()));
    }
    injections.sort_by_key(|&(pos, _)| pos);
    let mut result: Vec<u8> = Vec::with_capacity(out.len() + injections.len() * 24);
    let mut prev = 0usize;
    for (pos, seq) in injections {
        result.extend_from_slice(&out[prev..pos]);
        let marker = extensions::register(sql, extensions::Extension::IndexInclude(seq));
        result.extend_from_slice(format!(", `{marker}`").as_bytes());
        prev = pos;
    }
    result.extend_from_slice(&out[prev..]);
    let cleaned = String::from_utf8(result)
        .map_err(|_| parse_error_at_zero("internal error: INCLUDE strip produced invalid UTF-8"))?;
    Ok((std::borrow::Cow::Owned(cleaned), records))
}

/// Parse `( col1, col2, ... )` starting at `open` (which must index a
/// `(`), returning the column names and the byte index of the closing
/// `)`. Backtick-quoted identifiers are unquoted; nested parens / string
/// literals are tolerated defensively.
fn parse_include_column_list(
    bytes: &[u8],
    open: usize,
) -> Result<(Vec<String>, usize), ParseError> {
    let is_ident_byte = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'$';
    let mut cols: Vec<String> = Vec::new();
    let mut i = open + 1;
    let mut depth = 1i32;
    while i < bytes.len() {
        let b = bytes[i];
        match b {
            b'`' => {
                let start = i + 1;
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'`' {
                        if bytes.get(i + 1) == Some(&b'`') {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                let raw = &bytes[start..i.min(bytes.len())];
                let name = String::from_utf8_lossy(raw).replace("``", "`");
                if !name.is_empty() {
                    cols.push(name);
                }
                i += 1; // past closing backtick
            }
            b'(' => {
                depth += 1;
                i += 1;
            }
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Ok((cols, i));
                }
                i += 1;
            }
            b',' | b' ' | b'\t' | b'\n' | b'\r' => {
                i += 1;
            }
            _ if is_ident_byte(b) => {
                let start = i;
                while i < bytes.len() && is_ident_byte(bytes[i]) {
                    i += 1;
                }
                cols.push(String::from_utf8_lossy(&bytes[start..i]).into_owned());
            }
            _ => {
                // Unexpected token inside the INCLUDE list (prefix length,
                // expression, …) — INCLUDE columns are plain names.
                return Err(parse_error_at_zero(
                    "INCLUDE columns must be plain column names",
                ));
            }
        }
    }
    Err(parse_error_at_zero("unterminated INCLUDE column list"))
}

/// Re-attach inline / ALTER `INCLUDE (...)` columns stripped by
/// [`strip_inline_index_includes`]. Each affected index carries a sentinel
/// key column named `__mydb_inc_<seq>`; here we pop it back off the key
/// and attach the recorded columns to `IndexDef::included_columns`, rejecting the
/// combinations mydb forbids (UNIQUE / PRIMARY keys, non-BTREE indexes).
pub(super) fn apply_inline_index_includes(
    statement: &mut Statement,
    records: &[Vec<String>],
) -> Result<(), ParseError> {
    if records.is_empty() {
        return Ok(());
    }
    for_each_inline_index(statement, |index| {
        let Some((pos, seq)) =
            index
                .columns
                .iter()
                .enumerate()
                .find_map(|(pos, name)| match extensions::get(name) {
                    Some(extensions::Extension::IndexInclude(seq)) => Some((pos, seq)),
                    _ => None,
                })
        else {
            return Ok(());
        };
        let included = records
            .get(seq)
            .ok_or_else(|| parse_error_at_zero("internal error: dangling INCLUDE sentinel"))?;
        if index.unique {
            return Err(parse_error_at_zero(
                "UNIQUE INDEX cannot declare INCLUDE columns",
            ));
        }
        if !index.index_type.eq_ignore_ascii_case("BTREE") {
            return Err(parse_error_at_zero(
                "INCLUDE columns are only supported on BTREE indexes",
            ));
        }
        // Pop the sentinel out of every key-parallel vector.
        remove_index_key_part(index, pos);
        index.included_columns = included.clone();
        Ok(())
    })
}

fn remove_index_key_part(index: &mut IndexDef, pos: usize) {
    index.columns.remove(pos);
    remove_if_present(&mut index.column_prefix_lengths, pos);
    remove_if_present(&mut index.column_descending, pos);
    remove_if_present(&mut index.key_expressions, pos);
    remove_if_present(&mut index.key_expression_multi_valued, pos);
}

#[inline]
fn remove_if_present<T>(v: &mut Vec<T>, pos: usize) {
    if pos < v.len() {
        v.remove(pos);
    }
}

/// Remove NDB-era column attributes `COLUMN_FORMAT {FIXED|DYNAMIC|
/// DEFAULT}` and `STORAGE {DISK|MEMORY|DEFAULT}` before sqlparser sees
/// them. InnoDB parses and ignores both; mydb mirrors that. The strip
/// is value-gated — a bare `STORAGE` followed by anything else (e.g. a
/// column named `storage`) is left untouched.
pub(super) fn strip_column_format_storage(sql: &str, enabled: bool) -> std::borrow::Cow<'_, str> {
    if !enabled {
        return std::borrow::Cow::Borrowed(sql);
    }
    let bytes = sql.as_bytes();
    let is_ident_byte = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'$';
    let mut out: Vec<u8> = bytes.to_vec();
    let mut changed = false;
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' | b'`' => {
                i = quoted_span_end(bytes, i);
                continue;
            }
            _ => {}
        }
        if is_ident_byte(bytes[i]) && (i == 0 || !is_ident_byte(bytes[i - 1])) {
            let start = i;
            while i < bytes.len() && is_ident_byte(bytes[i]) {
                i += 1;
            }
            let word = &bytes[start..i];
            let allowed: &[&[u8]] = if word.eq_ignore_ascii_case(b"COLUMN_FORMAT") {
                &[b"FIXED", b"DYNAMIC", b"DEFAULT"]
            } else if word.eq_ignore_ascii_case(b"STORAGE") {
                &[b"DISK", b"MEMORY", b"DEFAULT"]
            } else {
                continue;
            };
            let mut j = i;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            let value_start = j;
            while j < bytes.len() && is_ident_byte(bytes[j]) {
                j += 1;
            }
            let value = &bytes[value_start..j];
            if allowed.iter().any(|v| value.eq_ignore_ascii_case(v)) {
                for slot in &mut out[start..j] {
                    *slot = b' ';
                }
                changed = true;
                i = j;
            }
            continue;
        }
        i += 1;
    }
    if !changed {
        return std::borrow::Cow::Borrowed(sql);
    }
    std::borrow::Cow::Owned(String::from_utf8(out).expect("space-blanking preserves UTF-8"))
}

pub(super) fn strip_create_table_pctfree_pctused(
    sql: &str,
    has_pctfree_or_pctused: bool,
) -> Result<(std::borrow::Cow<'_, str>, CreateTablePctOverride), ParseError> {
    // Caller passes the pre-scanned `has_pctfree_or_pctused`. For non-
    // CREATE heads it's always false (pre-scan skips the scan). The body
    // below walks every byte of `sql` while tracking quote state, which
    // used to dominate parse cost on every non-CREATE-TABLE statement
    // (~7 % on the sysbench multi-row INSERT pipeline).
    if !has_pctfree_or_pctused {
        return Ok((
            std::borrow::Cow::Borrowed(sql),
            CreateTablePctOverride::default(),
        ));
    }
    let bytes = sql.as_bytes();
    // CRITICAL: walk by byte index for fast PCTFREE matching, but
    // accumulate into `Vec<u8>` (NOT `String`) and finalise with
    // `String::from_utf8`. `out.push(b as char)` would turn a UTF-8
    // continuation byte, such as 0xE4 in U+4F60, into U+00E4 and re-encode
    // it to two bytes, doubling string-byte lengths everywhere and
    // breaking LENGTH on multibyte text and BLOB round trips.
    let mut out: Vec<u8> = Vec::with_capacity(sql.len());
    let mut pctfree: Option<u8> = None;
    let mut pctused: Option<u8> = None;
    let mut i = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let mut in_backtick = false;
    let mut depth = 0usize;
    let mut in_ctas_query = false;
    while i < bytes.len() {
        let b = bytes[i];
        // Track string-literal state so we never strip inside one.
        if !in_double && !in_backtick && b == b'\'' {
            // Backslash escape inside single-quoted literal.
            if in_single && i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                out.push(b'\'');
                out.push(b'\'');
                i += 2;
                continue;
            }
            in_single = !in_single;
            out.push(b'\'');
            i += 1;
            continue;
        }
        if !in_single && !in_backtick && b == b'"' {
            in_double = !in_double;
            out.push(b'"');
            i += 1;
            continue;
        }
        if !in_single && !in_double && b == b'`' {
            in_backtick = !in_backtick;
            out.push(b'`');
            i += 1;
            continue;
        }
        if in_single || in_double || in_backtick {
            out.push(b);
            i += 1;
            continue;
        }
        match b {
            b'(' => {
                depth += 1;
                out.push(b);
                i += 1;
                continue;
            }
            b')' => {
                depth = depth.saturating_sub(1);
                out.push(b);
                i += 1;
                continue;
            }
            _ => {}
        }
        // At a non-literal byte boundary, try matching PCTFREE/PCTUSED.
        let at_boundary = i == 0 || !bytes[i - 1].is_ascii_alphanumeric() && bytes[i - 1] != b'_';
        // A top-level AS begins the CTAS query. Identifiers and expressions
        // in the SELECT must never be mistaken for target table options.
        if depth == 0
            && at_boundary
            && i + 2 <= bytes.len()
            && bytes[i..i + 2].eq_ignore_ascii_case(b"AS")
            && (i + 2 == bytes.len()
                || (!bytes[i + 2].is_ascii_alphanumeric() && bytes[i + 2] != b'_'))
        {
            in_ctas_query = true;
        }
        if depth == 0 && !in_ctas_query && at_boundary {
            let mut matched = false;
            for (kw, slot) in [("PCTFREE", &mut pctfree), ("PCTUSED", &mut pctused)] {
                let end = i + kw.len();
                if end <= bytes.len()
                    && bytes[i..end].eq_ignore_ascii_case(kw.as_bytes())
                    && (end == bytes.len()
                        || !bytes[end].is_ascii_alphanumeric() && bytes[end] != b'_')
                {
                    let mut j = end;
                    while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                        j += 1;
                    }
                    let has_equals = j < bytes.len() && bytes[j] == b'=';
                    if has_equals {
                        j += 1;
                    }
                    while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                        j += 1;
                    }
                    // A table/column/query identifier named PCTFREE is not an
                    // option. Only claim the token when its value syntax has
                    // actually begun (`=` or an integer literal).
                    if !has_equals && (j >= bytes.len() || !bytes[j].is_ascii_digit()) {
                        continue;
                    }
                    let num_start = j;
                    while j < bytes.len() && bytes[j].is_ascii_digit() {
                        j += 1;
                    }
                    if j == num_start {
                        return Err(parse_error_at_zero(&format!(
                            "{kw} expects an integer literal between 0 and 50"
                        )));
                    }
                    let raw = std::str::from_utf8(&bytes[num_start..j]).map_err(|_| {
                        parse_error_at_zero("PCTFREE/PCTUSED value is not valid UTF-8")
                    })?;
                    let n: u32 = raw.parse().map_err(|_| {
                        parse_error_at_zero(&format!(
                            "{kw} expects an integer literal between 0 and 50"
                        ))
                    })?;
                    if n > 50 {
                        return Err(parse_error_at_zero(&format!(
                            "{kw}={n} out of range — must be between 0 and 50"
                        )));
                    }
                    *slot = Some(n as u8);
                    // Skip a trailing comma so the residue doesn't look
                    // like a dangling option separator.
                    while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                        j += 1;
                    }
                    if j < bytes.len() && bytes[j] == b',' {
                        j += 1;
                    }
                    i = j;
                    matched = true;
                    break;
                }
            }
            if matched {
                continue;
            }
        }
        out.push(b);
        i += 1;
    }
    let kept = String::from_utf8(out)
        .map_err(|_| parse_error_at_zero("internal: PCTFREE strip produced invalid UTF-8"))?;
    Ok((
        std::borrow::Cow::Owned(kept),
        CreateTablePctOverride { pctfree, pctused },
    ))
}

/// Scan an `ALTER TABLE … <tail>` body for `PCTFREE=N` / `PCTUSED=N`
/// clauses and remove them, returning the cleaned tail plus the
/// extracted values. Each clause may be preceded by `SET` and
/// separated from other options by an optional comma. The matcher is
/// permissive about whitespace, optional `=`, and trailing commas to
/// keep the rest of the option parser unaffected.
///
/// Examples:
///   "PCTFREE=20"             → ("",         Some(20), None)
///   "PCTFREE 20"             → ("",         Some(20), None)
///   "SET PCTFREE=10 PCTUSED=40" → ("SET ",   Some(10), Some(40))
///   "DEFAULT CHARSET=utf8mb4 PCTFREE=5" → ("DEFAULT CHARSET=utf8mb4 ", Some(5), None)
fn strip_pctfree_pctused(tail: &str) -> Result<(String, Option<u8>, Option<u8>), ParseError> {
    let mut pctfree: Option<u8> = None;
    let mut pctused: Option<u8> = None;
    // Lowercase-aware scan: walk byte by byte, on each "word boundary"
    // try to match PCTFREE / PCTUSED followed by optional `=`/whitespace
    // and a number literal. If matched, consume the match and skip the
    // copy into `kept`; otherwise copy the current byte through.
    let bytes = tail.as_bytes();
    let mut i = 0usize;
    // See strip_create_table_pctfree_pctused for why we must accumulate
    // bytes into `Vec<u8>` instead of pushing `b as char` through.
    let mut kept_bytes: Vec<u8> = Vec::with_capacity(tail.len());
    'outer: while i < bytes.len() {
        // Try a keyword match only when at a word boundary (start of
        // string or preceded by a non-identifier byte).
        let at_boundary = i == 0 || !bytes[i - 1].is_ascii_alphanumeric() && bytes[i - 1] != b'_';
        if at_boundary {
            for (kw, slot) in [("PCTFREE", &mut pctfree), ("PCTUSED", &mut pctused)] {
                let end = i + kw.len();
                if end <= bytes.len()
                    && bytes[i..end].eq_ignore_ascii_case(kw.as_bytes())
                    && (end == bytes.len()
                        || !bytes[end].is_ascii_alphanumeric() && bytes[end] != b'_')
                {
                    // Consume kw + optional ws + optional `=` + ws + integer.
                    let mut j = end;
                    while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                        j += 1;
                    }
                    if j < bytes.len() && bytes[j] == b'=' {
                        j += 1;
                    }
                    while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                        j += 1;
                    }
                    let num_start = j;
                    while j < bytes.len() && bytes[j].is_ascii_digit() {
                        j += 1;
                    }
                    if j == num_start {
                        return Err(parse_error_at_zero(&format!(
                            "{kw} expects an integer literal between 0 and 50"
                        )));
                    }
                    let raw = std::str::from_utf8(&bytes[num_start..j]).map_err(|_| {
                        parse_error_at_zero("PCTFREE/PCTUSED value is not valid UTF-8")
                    })?;
                    let n: u32 = raw.parse().map_err(|_| {
                        parse_error_at_zero(&format!(
                            "{kw} expects an integer literal between 0 and 50"
                        ))
                    })?;
                    if n > 50 {
                        return Err(parse_error_at_zero(&format!(
                            "{kw}={n} out of range — must be between 0 and 50"
                        )));
                    }
                    *slot = Some(n as u8);
                    // Also skip a trailing comma between options so the
                    // residue doesn't start with `,`.
                    while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                        j += 1;
                    }
                    if j < bytes.len() && bytes[j] == b',' {
                        j += 1;
                    }
                    i = j;
                    continue 'outer;
                }
            }
        }
        kept_bytes.push(bytes[i]);
        i += 1;
    }
    let kept = String::from_utf8(kept_bytes)
        .map_err(|_| parse_error_at_zero("internal: PCTFREE strip produced invalid UTF-8"))?;
    Ok((kept, pctfree, pctused))
}

// RETURNING compatibility rewrites.
/// Result of [`convert_alter_table_action`]: either a parsed
/// [`AlterAction`] or a bubble-up [`Statement::Unsupported`] for an
/// operation we don't yet handle. Pulling the per-action conversion
/// into its own function lets the multi-action collector loop over the
/// same code path that the single-action variant used to.
pub(super) fn rewrite_insert_target_alias(
    sql: &str,
) -> Result<std::borrow::Cow<'_, str>, ParseError> {
    let bytes = sql.as_bytes();
    let mut pos = bytes
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    if !returning_keyword_at(bytes, pos, b"insert") {
        return Ok(std::borrow::Cow::Borrowed(sql));
    }
    // Find INTO before the target. Modifiers between INSERT and INTO are
    // accepted; quoted text cannot occur in this header.
    let mut into = None;
    while pos < bytes.len() {
        if returning_keyword_at(bytes, pos, b"into") {
            into = Some(pos + 4);
            break;
        }
        pos += 1;
    }
    let Some(mut pos) = into else {
        return Ok(std::borrow::Cow::Borrowed(sql));
    };
    while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
        pos += 1;
    }
    // Consume schema-qualified target, honoring backtick/double-quoted parts.
    let mut quoted = None;
    while pos < bytes.len() {
        let b = bytes[pos];
        if let Some(q) = quoted {
            pos += 1;
            if b == q {
                quoted = None;
            }
            continue;
        }
        if matches!(b, b'`' | b'"') {
            quoted = Some(b);
            pos += 1;
            continue;
        }
        if b.is_ascii_whitespace() {
            break;
        }
        if b == b'(' {
            return Ok(std::borrow::Cow::Borrowed(sql));
        }
        pos += 1;
    }
    let remove_start = pos;
    while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
        pos += 1;
    }
    if !returning_keyword_at(bytes, pos, b"as") {
        return Ok(std::borrow::Cow::Borrowed(sql));
    }
    pos += 2;
    while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
        pos += 1;
    }
    let alias_start = pos;
    let alias = if matches!(bytes.get(pos), Some(b'`' | b'"')) {
        let q = bytes[pos];
        pos += 1;
        let start = pos;
        while pos < bytes.len() && bytes[pos] != q {
            pos += 1;
        }
        if pos >= bytes.len() {
            return Err(parse_error_at_zero("unterminated INSERT target alias"));
        }
        let alias = sql[start..pos].to_string();
        pos += 1;
        alias
    } else {
        while pos < bytes.len() && returning_ident_byte(bytes[pos]) {
            pos += 1;
        }
        if pos == alias_start {
            return Err(parse_error_at_zero("INSERT target alias is missing"));
        }
        sql[alias_start..pos].to_string()
    };
    let remove_end = pos;
    // Only rewrite when this statement actually has top-level RETURNING.
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let mut cursor =
        crate::sql_lex::SqlTokenCursor::new(&sql[remove_end..], mode, MYSQL_VERSION_ID);
    let mut depth = 0usize;
    let mut returning = None;
    while let Some(token) = cursor.next().map_err(|err| parse_lex_error(sql, err))? {
        use crate::sql_lex::SqlTokenKind;
        match token.kind {
            SqlTokenKind::Symbol(b'(') => depth += 1,
            SqlTokenKind::Symbol(b')') => depth = depth.saturating_sub(1),
            SqlTokenKind::Word
                if depth == 0 && cursor.token_text(&token).eq_ignore_ascii_case("RETURNING") =>
            {
                returning = Some(remove_end + token.span.end);
                break;
            }
            _ => {}
        }
    }
    let Some(ret_end) = returning else {
        return Ok(std::borrow::Cow::Borrowed(sql));
    };
    let name = extensions::register(sql, extensions::Extension::ReturningTarget(alias));
    let marker = format!(" NULL AS {name},");
    rewrite_source_edits(
        sql,
        &[
            TextEdit {
                range: remove_start..remove_end,
                replacement: "",
            },
            TextEdit {
                range: ret_end..ret_end,
                replacement: &marker,
            },
        ],
    )
}

/// sqlparser 0.61 understands a RETURNING projection but predates PostgreSQL
/// 18's `WITH (OLD AS ..., NEW AS ...)` namespace prefix. Rewrite only a
/// depth-zero RETURNING prefix into hidden marker projection items; the normal
/// sqlparser path still owns expression parsing and `convert_returning`
/// removes/decodes the markers. Quotes and comments are skipped so text inside
/// literals or nested queries is never touched.
pub(super) fn rewrite_returning_with_aliases(
    sql: &str,
) -> Result<std::borrow::Cow<'_, str>, ParseError> {
    let (head, head_len) = extract_first_keyword(sql);
    // These statement heads cannot contain an executable RETURNING clause.
    // Keep compound, WITH and EXPLAIN forms on the complete scanner path.
    if matches!(&head[..head_len], b"SELECT" | b"SET" | b"SHOW" | b"USE") {
        return Ok(std::borrow::Cow::Borrowed(sql));
    }
    // Most DML has no RETURNING clause. A substring precheck is sufficient
    // here: candidates still use the complete quote/comment-aware scanner.
    if !memchr::memchr2_iter(b'r', b'R', sql.as_bytes()).any(|pos| {
        sql.as_bytes()
            .get(pos..pos + b"returning".len())
            .is_some_and(|word| word.eq_ignore_ascii_case(b"returning"))
    }) {
        return Ok(std::borrow::Cow::Borrowed(sql));
    }
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    let mut depth = 0i32;
    let mut returning_end = None;
    while i < bytes.len() {
        let b = bytes[i];
        if matches!(b, b'\'' | b'"' | b'`') {
            i = quoted_span_end(bytes, i);
            continue;
        }
        if crate::sql_lex::is_dash_line_comment_at(sql, i) {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if b == b'#' {
            i += 1;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if b == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            i += 2;
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            i = (i + 2).min(bytes.len());
            continue;
        }
        if b == b'(' {
            depth += 1;
            i += 1;
            continue;
        }
        if b == b')' {
            depth -= 1;
            i += 1;
            continue;
        }
        if depth == 0 && returning_keyword_at(bytes, i, b"returning") {
            returning_end = Some(i + "returning".len());
            break;
        }
        i += 1;
    }
    let Some(ret_end) = returning_end else {
        return Ok(std::borrow::Cow::Borrowed(sql));
    };
    let mut pos = ret_end;
    while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
        pos += 1;
    }
    if !returning_keyword_at(bytes, pos, b"with") {
        return Ok(std::borrow::Cow::Borrowed(sql));
    }
    pos += 4;
    while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
        pos += 1;
    }
    if bytes.get(pos) != Some(&b'(') {
        return Err(parse_error_at_zero(
            "RETURNING WITH requires parenthesized OLD/NEW aliases",
        ));
    }
    let open = pos;
    pos += 1;
    let mut inner_quote = None;
    let mut close = None;
    while pos < bytes.len() {
        let b = bytes[pos];
        if let Some(q) = inner_quote {
            if b == q {
                inner_quote = None;
            }
        } else if matches!(b, b'"' | b'`') {
            inner_quote = Some(b);
        } else if b == b')' {
            close = Some(pos);
            break;
        }
        pos += 1;
    }
    let close = close.ok_or_else(|| parse_error_at_zero("unterminated RETURNING WITH clause"))?;
    let body = &sql[open + 1..close];
    let mut markers = Vec::new();
    for spec in body.split(',') {
        let words: Vec<&str> = spec.split_whitespace().collect();
        if words.len() != 3 || !words[1].eq_ignore_ascii_case("as") {
            return Err(parse_error_at_zero(
                "RETURNING WITH entries must be OLD AS alias or NEW AS alias",
            ));
        }
        let kind = if words[0].eq_ignore_ascii_case("old") {
            "old"
        } else if words[0].eq_ignore_ascii_case("new") {
            "new"
        } else {
            return Err(parse_error_at_zero(
                "RETURNING WITH accepts only OLD and NEW",
            ));
        };
        let alias = words[2].trim_matches(['`', '"']);
        if alias.is_empty() {
            return Err(parse_error_at_zero("RETURNING WITH alias cannot be empty"));
        }
        let extension = if kind == "old" {
            extensions::Extension::ReturningOld(alias.to_owned())
        } else {
            extensions::Extension::ReturningNew(alias.to_owned())
        };
        let marker = extensions::register(sql, extension);
        markers.push(format!("NULL AS {marker}"));
    }
    if markers.is_empty() {
        return Err(parse_error_at_zero("RETURNING WITH cannot be empty"));
    }
    let replacement = format!(" {}, ", markers.join(", "));
    rewrite_source_edits(
        sql,
        &[TextEdit {
            range: ret_end..close + 1,
            replacement: &replacement,
        }],
    )
}

fn returning_keyword_at(bytes: &[u8], pos: usize, keyword: &[u8]) -> bool {
    rewrite_keyword_matches_at(bytes, pos, keyword)
}

fn returning_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$'
}

#[inline]
fn rewrite_keyword_matches_at(bytes: &[u8], pos: usize, keyword: &[u8]) -> bool {
    let Some(end) = pos.checked_add(keyword.len()) else {
        return false;
    };
    !keyword.is_empty()
        && end <= bytes.len()
        && bytes[pos..end].eq_ignore_ascii_case(keyword)
        && (pos == 0 || !returning_ident_byte(bytes[pos - 1]))
        && (end == bytes.len() || !returning_ident_byte(bytes[end]))
}
