//! Token-based vector WITH clauses, bound to their owning index key list.
use super::*;
use crate::sql_lex::{SqlLexMode, SqlTokenCursor, SqlTokenKind};
use crate::storage::vector_config::{VectorIndexOptions, VectorOption};
use std::borrow::Cow;

pub(super) fn strip_vector_options(
    sql: &str,
) -> Result<(Cow<'_, str>, Vec<VectorIndexOptions>), ParseError> {
    if !(first_keyword_is(sql, b"CREATE") || first_keyword_is(sql, b"ALTER")) {
        return Ok((Cow::Borrowed(sql), Vec::new()));
    }
    let mode = SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let mut cursor = SqlTokenCursor::new(sql, mode, MYSQL_VERSION_ID);
    let mut tokens = Vec::new();
    while let Some(t) = cursor
        .next()
        .map_err(|e| super::entry::parse_lex_error(sql, e))?
    {
        tokens.push(t);
    }
    let word = |i: usize, w: &str| {
        tokens.get(i).is_some_and(|t| {
            t.kind == SqlTokenKind::Word && sql[t.span.clone()].eq_ignore_ascii_case(w)
        })
    };
    let symbol = |i: usize, b: u8| {
        tokens
            .get(i)
            .is_some_and(|t| t.kind == SqlTokenKind::Symbol(b))
    };
    let mut edits = Vec::new();
    let mut records = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut i = 0;
    while i < tokens.len() {
        if !word(i, "WITH") || !symbol(i + 1, b'(') {
            i += 1;
            continue;
        }
        let mut end = i;
        let mut algorithm = None;
        while end > 0 {
            let len = if end >= 2 && word(end - 2, "USING") {
                algorithm = Some(end - 1);
                2
            } else if end >= 2
                && word(end - 2, "COMMENT")
                && tokens[end - 1].kind == SqlTokenKind::String
            {
                2
            } else if word(end - 1, "VISIBLE") || word(end - 1, "INVISIBLE") {
                1
            } else if tokens[end - 1].kind == SqlTokenKind::Number {
                if end >= 3 && word(end - 3, "KEY_BLOCK_SIZE") && symbol(end - 2, b'=') {
                    3
                } else if end >= 2 && word(end - 2, "KEY_BLOCK_SIZE") {
                    2
                } else {
                    break;
                }
            } else {
                break;
            };
            end -= len;
        }
        let Some(algorithm) = algorithm else {
            i += 1;
            continue;
        };
        if !word(algorithm, "HNSW") && !word(algorithm, "IVF") {
            return Err(parse_error_full_span(
                sql,
                "WITH vector options require USING HNSW or IVF",
            ));
        }
        if end == 0 || !symbol(end - 1, b')') {
            return Err(parse_error_full_span(
                sql,
                "USING HNSW/IVF WITH must follow the index key list",
            ));
        }
        let key_close = tokens[end - 1].span.start;
        if !seen.insert(key_close) {
            return Err(parse_error_full_span(sql, "duplicate vector WITH clause"));
        }
        let mut options = VectorIndexOptions::default();
        let mut j = i + 2;
        if symbol(j, b')') {
            return Err(parse_error_full_span(sql, "vector WITH must not be empty"));
        }
        loop {
            let key = tokens
                .get(j)
                .ok_or_else(|| parse_error_full_span(sql, "unterminated vector WITH"))?;
            if key.kind != SqlTokenKind::Word {
                return Err(parse_error_full_span(
                    sql,
                    "WITH option requires an unquoted parameter name",
                ));
            }
            let name = sql[key.span.clone()].to_ascii_lowercase();
            if options.entries.iter().any(|v| v.name == name) {
                return Err(parse_error_full_span(
                    sql,
                    &format!("duplicate vector WITH option '{name}'"),
                ));
            }
            if !symbol(j + 1, b'=') {
                return Err(parse_error_full_span(sql, "WITH option requires '='"));
            }
            let value = tokens
                .get(j + 2)
                .ok_or_else(|| parse_error_full_span(sql, "missing vector WITH option value"))?;
            let numeric = value.kind == SqlTokenKind::Number;
            let text = match value.kind {
                SqlTokenKind::Word => sql[value.span.clone()].to_owned(),
                SqlTokenKind::String => cursor.decoded_quoted_text(value).expect("string token"),
                SqlTokenKind::Number
                    if sql[value.span.clone()].bytes().all(|b| b.is_ascii_digit()) =>
                {
                    sql[value.span.clone()].to_owned()
                }
                _ => {
                    return Err(parse_error_full_span(
                        sql,
                        "WITH values must be names, strings or unsigned decimal integers",
                    ))
                }
            };
            options.entries.push(VectorOption {
                name,
                value: text,
                numeric,
                offset: key.span.start,
            });
            j += 3;
            if symbol(j, b')') {
                break;
            }
            if !symbol(j, b',') {
                return Err(parse_error_full_span(
                    sql,
                    "WITH options require comma separators",
                ));
            }
            j += 1;
        }
        if word(j + 1, "WITH") {
            return Err(parse_error_full_span(sql, "duplicate vector WITH clause"));
        }
        // Validate before sqlparser lowers the owning index. Keep type/dimension
        // validation in DDL, where the source column is available.
        crate::storage::vector_config::VectorIndexConfig::from_options(
            &sql[tokens[algorithm].span.clone()],
            Some(&options),
            "",
        )
        .map_err(|e| {
            parse_error_with_spec_at_zero(
                e.spec()
                    .unwrap_or(crate::error::catalog::ER_NOT_SUPPORTED_YET),
                e.message(),
            )
        })?;
        let marker = extensions::register(sql, extensions::Extension::VectorOptions(records.len()));
        edits.push((key_close, key_close, format!(", `{marker}`")));
        edits.push((
            tokens[i].span.start,
            tokens[j].span.end,
            " ".repeat(tokens[j].span.end - tokens[i].span.start),
        ));
        records.push(options);
        i = j + 1;
    }
    if records.is_empty() {
        return Ok((Cow::Borrowed(sql), records));
    }
    edits.sort_by_key(|e| e.0);
    let mut out = String::with_capacity(sql.len() + records.len() * 32);
    let mut end = 0;
    for (start, stop, replacement) in edits {
        out.push_str(&sql[end..start]);
        out.push_str(&replacement);
        end = stop;
    }
    out.push_str(&sql[end..]);
    Ok((Cow::Owned(out), records))
}
pub(super) fn apply_vector_options(
    statement: &mut Statement,
    records: &[VectorIndexOptions],
) -> Result<(), ParseError> {
    let mut used = std::collections::HashSet::new();
    macro_rules! apply {
        ($index:expr) => {{
            let index = $index;
            if let Some((pos, seq)) =
                index
                    .columns
                    .iter()
                    .enumerate()
                    .find_map(|(p, n)| match extensions::get(n) {
                        Some(extensions::Extension::VectorOptions(s)) => Some((p, s)),
                        _ => None,
                    })
            {
                if !used.insert(seq) || index.vector_options.is_some() {
                    return Err(parse_error_at_zero("duplicate vector WITH binding"));
                }
                index.columns.remove(pos);
                if pos < index.column_prefix_lengths.len() {
                    index.column_prefix_lengths.remove(pos);
                }
                if pos < index.column_descending.len() {
                    index.column_descending.remove(pos);
                }
                if pos < index.key_expressions.len() {
                    index.key_expressions.remove(pos);
                }
                if pos < index.key_expression_multi_valued.len() {
                    index.key_expression_multi_valued.remove(pos);
                }
                index.vector_options = Some(
                    records
                        .get(seq)
                        .ok_or_else(|| parse_error_at_zero("invalid vector WITH binding"))?
                        .clone(),
                );
            }
        }};
    }
    match statement {
        Statement::CreateIndex(index) => apply!(index),
        _ => for_each_inline_index(statement, |index| {
            apply!(index);
            Ok(())
        })?,
    }
    if used.len() != records.len() {
        return Err(parse_error_at_zero(
            "WITH options were not attached to an index",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn vector_with_binds_all_ddl_forms() {
        let sqls = [
            "CREATE INDEX x ON t(v) USING HNSW WITH (m=16, ef_construction=64, quantization=sq8) COMMENT 'hello'",
            "CREATE TABLE t (v VECTOR(7) NOT NULL, INDEX x(v) USING HNSW WITH (m=16, ef_construction=64, quantization=sq8))",
            "ALTER TABLE t ADD INDEX x(v) USING HNSW WITH (m=16, ef_construction=64, quantization=sq8)",
        ];
        for sql in sqls {
            let stmt = parse_statement(sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
            match stmt {
                Statement::CreateIndex(i) => assert_eq!(i.vector_options.unwrap().entries.len(), 3),
                mut other => {
                    let mut count = 0;
                    for_each_inline_index(&mut other, |i| {
                        assert_eq!(i.columns, ["v"]);
                        assert_eq!(i.vector_options.as_ref().unwrap().entries.len(), 3);
                        count += 1;
                        Ok(())
                    })
                    .unwrap();
                    assert_eq!(count, 1);
                }
            }
        }
    }
    #[test]
    fn vector_with_after_other_index_options() {
        for options in [
            "USING HNSW COMMENT 'x' WITH (m=8)",
            "COMMENT 'x' USING HNSW WITH (m=8)",
            "USING HNSW INVISIBLE WITH (m=8)",
        ] {
            let sql = format!("CREATE INDEX ix ON t(v) {options}");
            let Statement::CreateIndex(index) =
                parse_statement(&sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"))
            else {
                panic!("{sql}");
            };
            assert_eq!(index.vector_options.unwrap().entries[0].name, "m");
        }
    }

    #[test]
    fn vector_with_zero_ef_emits_hint_warning() {
        let Statement::Select(select) = parse_statement("SELECT /*+ HNSW_EF(0) */ 1").unwrap()
        else {
            panic!("expected SELECT");
        };
        assert_eq!(select.hint_warnings, ["HNSW_EF: invalid argument \"0\""]);
    }

    #[test]
    fn vector_with_rejects_invalid_syntax() {
        for options in [
            "()",
            "(m=2,m=3)",
            "(m=-1)",
            "(m=1+1)",
            "(m=?)",
            "(m=0x10)",
            "(m=16,)",
            "(typo=1)",
            "(pq_m=1)",
            "(quantization=oops)",
            "(m=16) WITH (m=32)",
        ] {
            let sql = format!("CREATE INDEX x ON t(v) USING HNSW WITH {options}");
            assert!(parse_statement(&sql).is_err(), "{sql}");
        }
    }
    #[test]
    fn vector_with_multiple_indexes_and_comments() {
        let sql="ALTER TABLE t ADD INDEX x(v) USING HNSW /* a */ WITH (m=4, quantization='SQ4') COMMENT 'WITH (m=1)', ADD INDEX y(v) USING IVF WITH (nlist=7,quantization=bq)";
        let mut stmt = parse_statement(sql).unwrap();
        let mut names = Vec::new();
        for_each_inline_index(&mut stmt, |i| {
            names.push((
                i.name.clone(),
                i.vector_options.as_ref().unwrap().entries[0].name.clone(),
            ));
            Ok(())
        })
        .unwrap();
        assert_eq!(
            names,
            [
                (Some("x".into()), "m".into()),
                (Some("y".into()), "nlist".into())
            ]
        );
    }
}
