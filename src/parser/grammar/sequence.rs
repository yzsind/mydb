//! Sequence statements use the shared typed cursor; names and literals retain
//! lexical roles and all accepted statements consume their complete input.
use super::super::{ParseError, SequenceRestart, ShowFilter, Statement};
use super::cursor::Cursor;
use crate::sql_lex::SqlTokenKind;
use crate::storage::sequence::{BoundOpt, SeqOptions, SeqType};

fn name_parts(c: &mut Cursor<'_>) -> Result<Vec<String>, ParseError> {
    let mut parts = vec![c.identifier()?];
    while c.symbol(b'.')? {
        parts.push(c.identifier()?);
    }
    Ok(parts)
}
fn name(c: &mut Cursor<'_>) -> Result<String, ParseError> {
    let mut name = c.identifier()?;
    while c.symbol(b'.')? {
        name.push('.');
        name.push_str(&c.identifier()?);
    }
    Ok(name)
}

pub(in crate::parser) fn set_owner(
    options: &mut SeqOptions,
    mut parts: Vec<String>,
    none: bool,
) -> Result<(), String> {
    if none {
        options.owned_none = true;
        return Ok(());
    }
    if parts.len() < 2 {
        return Err("sequence OWNED BY requires table.column or NONE".into());
    }
    options.owned_column = parts.pop();
    options.owned_table = Some(if parts.len() == 1 {
        parts.pop().unwrap()
    } else {
        parts.join(".")
    });
    Ok(())
}

fn number(c: &mut Cursor<'_>) -> Result<i64, ParseError> {
    let negative = c.symbol(b'-')?;
    if !negative {
        let _ = c.symbol(b'+')?;
    }
    let token = c.peek()?.ok_or_else(|| c.expected("integer value"))?;
    let text = c.text(&token);
    if token.kind != SqlTokenKind::Number || !text.bytes().all(|b| b.is_ascii_digit()) {
        return Err(c.expected("integer value"));
    }
    let value = text
        .parse::<i128>()
        .ok()
        .and_then(|value| i64::try_from(if negative { -value } else { value }).ok())
        .ok_or_else(|| c.expected("integer in bigint range"))?;
    c.next()?;
    Ok(value)
}

fn option(c: &mut Cursor<'_>, options: &mut SeqOptions) -> Result<bool, ParseError> {
    if c.symbol(b',')? {
        return Ok(true);
    }
    if c.keyword(b"AS")? {
        let token = c.peek()?.ok_or_else(|| c.expected("sequence type"))?;
        if token.kind != SqlTokenKind::Word {
            return Err(c.expected("sequence type"));
        }
        options.as_type = Some(
            SeqType::parse(c.text(&token))
                .ok_or_else(|| c.expected("smallint, integer, or bigint"))?,
        );
        c.next()?;
    } else if c.keyword(b"INCREMENT")? {
        let _ = c.keyword(b"BY")?;
        options.increment = Some(number(c)?);
    } else if c.keyword(b"MINVALUE")? {
        options.min_value = BoundOpt::Value(number(c)?);
    } else if c.keyword(b"MAXVALUE")? {
        options.max_value = BoundOpt::Value(number(c)?);
    } else if c.keyword(b"CACHE")? {
        options.cache = Some(number(c)?);
    } else if c.keyword(b"START")? {
        let _ = c.keyword(b"WITH")?;
        options.start = Some(number(c)?);
    } else if c.keyword(b"CYCLE")? {
        options.cycle = Some(true);
    } else if c.keyword(b"NO")? {
        if c.keyword(b"MINVALUE")? {
            options.min_value = BoundOpt::NoBound;
        } else if c.keyword(b"MAXVALUE")? {
            options.max_value = BoundOpt::NoBound;
        } else if c.keyword(b"CYCLE")? {
            options.cycle = Some(false);
        } else {
            return Err(c.expected("MINVALUE, MAXVALUE, or CYCLE"));
        }
    } else if c.keyword(b"OWNED")? {
        c.expect_keyword(b"BY")?;
        let token = c
            .peek()?
            .ok_or_else(|| c.expected("table.column or NONE"))?;
        let bare_none =
            token.kind == SqlTokenKind::Word && c.text(&token).eq_ignore_ascii_case("NONE");
        let parts = name_parts(c)?;
        let none = bare_none && parts.len() == 1;
        set_owner(options, parts, none).map_err(|message| {
            ParseError::new(
                c.source(),
                super::super::Span {
                    start: token.span.start,
                    end: token.span.end,
                },
                message,
                vec![],
            )
        })?;
    } else {
        return Ok(false);
    }
    Ok(true)
}

fn done(c: &mut Cursor<'_>) -> Result<bool, ParseError> {
    Ok(c.peek()?
        .is_none_or(|token| token.kind == SqlTokenKind::Symbol(b';')))
}

pub(in crate::parser) fn try_parse_create_sequence(
    sql: &str,
) -> Result<Option<Statement>, ParseError> {
    let mut c = Cursor::new(sql);
    if !c.keyword(b"CREATE")? {
        return Ok(None);
    }
    let temporary = c.keyword(b"TEMPORARY")? || c.keyword(b"TEMP")?;
    let _ = c.keyword(b"UNLOGGED")?;
    if !c.keyword(b"SEQUENCE")? {
        return Ok(None);
    }
    let if_not_exists = c.keyword(b"IF")?;
    if if_not_exists {
        c.expect_keyword(b"NOT")?;
        c.expect_keyword(b"EXISTS")?;
    }
    let name = name(&mut c)?;
    let mut options = SeqOptions::default();
    while !done(&mut c)? {
        if !option(&mut c, &mut options)? {
            return Err(c.expected("sequence option"));
        }
    }
    c.finish()?;
    Ok(Some(Statement::CreateSequence {
        name,
        if_not_exists,
        temporary,
        options,
    }))
}

pub(in crate::parser) fn try_parse_alter_sequence(
    sql: &str,
) -> Result<Option<Statement>, ParseError> {
    let mut c = Cursor::new(sql);
    if !c.keyword(b"ALTER")? || !c.keyword(b"SEQUENCE")? {
        return Ok(None);
    }
    let if_exists = c.keyword(b"IF")?;
    if if_exists {
        c.expect_keyword(b"EXISTS")?;
    }
    let name = name(&mut c)?;
    let mut options = SeqOptions::default();
    let mut restart = None;
    let mut rename_to = None;
    while !done(&mut c)? {
        if option(&mut c, &mut options)? {
            continue;
        }
        if c.keyword(b"RESTART")? {
            let with = c.keyword(b"WITH")?;
            let numeric = c.peek()?.is_some_and(|token| {
                matches!(
                    token.kind,
                    SqlTokenKind::Number | SqlTokenKind::Symbol(b'+' | b'-')
                )
            });
            restart = Some(if with || numeric {
                SequenceRestart::To(number(&mut c)?)
            } else {
                SequenceRestart::ToStart
            });
        } else if c.keyword(b"RENAME")? {
            c.expect_keyword(b"TO")?;
            rename_to = Some(self::name(&mut c)?);
        } else if c.keyword(b"SET")? {
            return Ok(Some(Statement::Unsupported {
                kind: "ALTER SEQUENCE SET".into(),
                sql: sql.into(),
            }));
        } else if c.keyword(b"OWNER")? {
            return Ok(Some(Statement::Unsupported {
                kind: "ALTER SEQUENCE OWNER".into(),
                sql: sql.into(),
            }));
        } else {
            return Err(c.expected("sequence option, RESTART, or RENAME"));
        }
    }
    c.finish()?;
    Ok(Some(Statement::AlterSequence {
        name,
        if_exists,
        options,
        restart,
        rename_to,
    }))
}

pub(in crate::parser) fn try_parse_show_sequence(
    sql: &str,
) -> Result<Option<Statement>, ParseError> {
    let mut c = Cursor::new(sql);
    if !c.keyword(b"SHOW")? {
        return Ok(None);
    }
    if c.keyword(b"CREATE")? {
        if !c.keyword(b"SEQUENCE")? {
            return Ok(None);
        }
        let name = name(&mut c)?;
        c.finish()?;
        return Ok(Some(Statement::ShowCreateSequence { name }));
    }
    if !c.keyword(b"SEQUENCES")? {
        return Ok(None);
    }
    let database = if c.keyword(b"FROM")? || c.keyword(b"IN")? {
        Some(name(&mut c)?)
    } else {
        None
    };
    let filter = if c.keyword(b"LIKE")? {
        let token = c.peek()?.ok_or_else(|| c.expected("string pattern"))?;
        if token.kind != SqlTokenKind::String {
            return Err(c.expected("string pattern"));
        }
        let pattern = c.quoted_text(&token)?;
        c.next()?;
        Some(ShowFilter::Like(pattern))
    } else {
        None
    };
    c.finish()?;
    Ok(Some(Statement::ShowSequences { database, filter }))
}
