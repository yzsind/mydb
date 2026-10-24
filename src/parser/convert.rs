//! Conversion from sqlparser 0.61 AST into mydb public AST.

use super::entry::parse_error_with_spec_at_zero as numeric_spec_error;
use super::*;

/// LIKE escape-clause conversion with MySQL defaults: no ESCAPE clause
/// means `\` is the escape character; an explicit `ESCAPE ''` disables
/// escaping entirely.
/// Resolve the ESCAPE clause of a LIKE / NOT LIKE predicate to a single
/// escape character. Absent clause → default `\`.
///
/// MySQL 8.0 accepts any string / binary literal here and expects the
/// value to resolve to at most one character. B5 widens the previously
/// SingleQuoted-only path to every string literal shape the client
/// might use (double-quoted, national, triple-quoted, hex `X'…'`,
/// bit-string) and hard-errors on shapes that violate the "0 or 1
/// character" contract with error 1210 rather than silently reverting
/// to "no escape".
///
/// * Empty string → `Ok(None)`: no escape character in effect.
/// * Single-char string → `Ok(Some(ch))`.
/// * >1 characters or a placeholder / non-string value → parse error
///   with MySQL errno 1210 (Incorrect arguments to ESCAPE).
fn convert_like_escape(escape: Option<&sqlparser::ast::Value>) -> Result<Option<char>, ParseError> {
    let Some(value) = escape else {
        return Ok((super::entry::current_parser_sql_mode_bits()
            & crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES
            == 0)
            .then_some('\\'));
    };
    let text: std::borrow::Cow<'_, str> = match value {
        SqlValue::SingleQuotedString(s)
        | SqlValue::DoubleQuotedString(s)
        | SqlValue::TripleSingleQuotedString(s)
        | SqlValue::TripleDoubleQuotedString(s)
        | SqlValue::EscapedStringLiteral(s)
        | SqlValue::NationalStringLiteral(s) => s.as_str().into(),
        SqlValue::HexStringLiteral(s) => match decode_hex_bytes(s) {
            Some(bytes) => match String::from_utf8(bytes) {
                Ok(text) => text.into(),
                Err(_) => {
                    return Err(ParseError::at_zero(
                        "Incorrect arguments to ESCAPE (hex literal is not valid UTF-8)",
                    ))
                }
            },
            None => {
                return Err(ParseError::at_zero(format!(
                    "Incorrect arguments to ESCAPE (invalid hex literal: {s})",
                )))
            }
        },
        SqlValue::SingleQuotedByteStringLiteral(s) | SqlValue::DoubleQuotedByteStringLiteral(s) => {
            match decode_bit_string(s) {
                Some(bytes) => match String::from_utf8(bytes) {
                    Ok(text) => text.into(),
                    Err(_) => {
                        return Err(ParseError::at_zero(
                            "Incorrect arguments to ESCAPE (bit literal is not valid UTF-8)",
                        ))
                    }
                },
                None => {
                    return Err(ParseError::at_zero(format!(
                        "Incorrect arguments to ESCAPE (invalid bit literal: {s})",
                    )))
                }
            }
        }
        _ => {
            return Err(ParseError::at_zero(
                "Incorrect arguments to ESCAPE (expected a string literal)",
            ))
        }
    };
    if text.is_empty()
        && super::entry::current_parser_sql_mode_bits()
            & crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES
            != 0
    {
        let mut err = ParseError::at_zero("Incorrect arguments to ESCAPE");
        err.spec = crate::error::catalog::ER_WRONG_ARGUMENTS;
        return Err(err);
    }
    let mut chars = text.chars();
    let first = chars.next();
    if chars.next().is_some() {
        // MySQL 8.0.45 raises ER 1210 with SQLSTATE HY000 verbatim.
        return Err(ParseError::at_zero(
            "Incorrect arguments to ESCAPE (expected 0 or 1 characters)",
        ));
    }
    Ok(first)
}

pub(super) fn convert_statement(sql: &str, stmt: SqlStatement) -> Result<Statement, ParseError> {
    let unsupported = |kind: &str| {
        Ok(Statement::Unsupported {
            kind: kind.to_string(),
            sql: sql.to_string(),
        })
    };

    match stmt {
        SqlStatement::RenameTable(renames) => {
            let mut pairs = Vec::with_capacity(renames.len());
            for r in renames {
                pairs.push((
                    object_name_to_string(r.old_name)?,
                    object_name_to_string(r.new_name)?,
                ));
            }
            Ok(Statement::RenameTables { pairs })
        }
        SqlStatement::CreateTable(table) => convert_create_table(sql, table),
        SqlStatement::CreateSequence {
            temporary,
            if_not_exists,
            name,
            data_type,
            sequence_options,
            owned_by,
        } => {
            let name = object_name_to_string(name)?;
            let options =
                convert_sequence_options(data_type.as_ref(), &sequence_options, owned_by.as_ref())?;
            Ok(Statement::CreateSequence {
                name,
                if_not_exists,
                temporary,
                options,
            })
        }
        SqlStatement::CreateDatabase {
            db_name,
            if_not_exists,
            default_charset,
            default_collation,
            ..
        } => Ok(Statement::CreateDatabase {
            name: object_name_to_string(db_name)?,
            if_not_exists,
            default_charset,
            default_collation,
        }),
        SqlStatement::CreateSchema {
            schema_name,
            if_not_exists,
            ..
        } => Ok(Statement::CreateDatabase {
            name: schema_name_to_string(schema_name)?,
            if_not_exists,
            default_charset: None,
            default_collation: None,
        }),
        SqlStatement::AlterTable(table) => convert_alter_table(sql, table),
        SqlStatement::Drop {
            object_type,
            names,
            cascade,
            restrict,
            temporary,
            table,
            if_exists,
            ..
        } => match object_type {
            ObjectType::Table => Ok(Statement::DropTable {
                names: names
                    .into_iter()
                    .map(object_name_to_string)
                    .collect::<Result<Vec<_>, _>>()?,
                if_exists,
                temporary,
                drop_behavior: if cascade {
                    Some(TableDropBehavior::Cascade)
                } else if restrict {
                    Some(TableDropBehavior::Restrict)
                } else {
                    None
                },
            }),
            ObjectType::Database | ObjectType::Schema if names.len() == 1 => {
                let mut names = names;
                Ok(Statement::DropDatabase {
                    name: object_name_to_string(names.remove(0))?,
                    if_exists,
                })
            }
            ObjectType::Index if names.len() == 1 => {
                let mut names = names;
                let Some(table) = table else {
                    return unsupported("DROP INDEX without ON table");
                };
                Ok(Statement::DropIndex {
                    name: object_name_to_string(names.remove(0))?,
                    table: object_name_to_string(table)?,
                })
            }
            ObjectType::View => {
                let mut converted = Vec::with_capacity(names.len());
                for name in names {
                    converted.push(object_name_to_qualified(name)?);
                }
                Ok(Statement::DropView {
                    names: converted,
                    if_exists,
                })
            }
            ObjectType::Sequence => {
                let mut converted = Vec::with_capacity(names.len());
                for name in names {
                    converted.push(object_name_to_string(name)?);
                }
                Ok(Statement::DropSequence {
                    names: converted,
                    if_exists,
                })
            }
            _ => unsupported(&format!("DROP {object_type}")),
        },
        SqlStatement::CreateIndex(index) => {
            // Consume every upstream field explicitly. sqlparser accepts a
            // superset of MySQL here; silently dropping one of those fields
            // would turn a partial/conditional index into an ordinary index.
            let sqlparser::ast::CreateIndex {
                name,
                table_name,
                using,
                columns: index_columns,
                unique,
                concurrently,
                if_not_exists,
                include,
                nulls_distinct,
                with,
                predicate,
                index_options,
                alter_options,
            } = index;
            let Some(name) = name else {
                return unsupported("CREATE INDEX without name");
            };
            if concurrently {
                return unsupported("CREATE INDEX CONCURRENTLY");
            }
            if if_not_exists {
                return unsupported("CREATE INDEX IF NOT EXISTS");
            }
            if nulls_distinct.is_some() {
                return unsupported("CREATE INDEX NULLS [NOT] DISTINCT");
            }
            if !with.is_empty() {
                return unsupported("CREATE INDEX WITH storage parameters");
            }
            if predicate.is_some() {
                return unsupported("partial CREATE INDEX with WHERE predicate");
            }
            let mut algorithm = AlterAlgorithm::Default;
            let mut lock = AlterLock::Default;
            for option in alter_options {
                match option {
                    sqlparser::ast::AlterTableOperation::Algorithm { algorithm: a, .. } => {
                        algorithm = match a {
                            sqlparser::ast::AlterTableAlgorithm::Default => AlterAlgorithm::Default,
                            sqlparser::ast::AlterTableAlgorithm::Instant => AlterAlgorithm::Instant,
                            sqlparser::ast::AlterTableAlgorithm::Inplace => AlterAlgorithm::Inplace,
                            sqlparser::ast::AlterTableAlgorithm::Copy => AlterAlgorithm::Copy,
                        };
                    }
                    sqlparser::ast::AlterTableOperation::Lock { lock: l, .. } => {
                        lock = match l {
                            sqlparser::ast::AlterTableLock::Default => AlterLock::Default,
                            sqlparser::ast::AlterTableLock::None => AlterLock::None,
                            sqlparser::ast::AlterTableLock::Shared => AlterLock::Shared,
                            sqlparser::ast::AlterTableLock::Exclusive => AlterLock::Exclusive,
                        };
                    }
                    _ => return unsupported("unsupported CREATE INDEX option"),
                }
            }
            let columns = convert_index_columns(index_columns)?;
            let (index_type, comment) = convert_index_type_and_options(using, &index_options)?;
            // BTREE and vector (HNSW / IVF) indexes are creatable via
            // CREATE INDEX; SPATIAL/FULLTEXT use dedicated keyword paths.
            if index_type != "BTREE" && index_type != "HNSW" && index_type != "IVF" {
                return unsupported(&format!("CREATE INDEX USING {index_type}"));
            }
            let included_columns: Vec<String> = include.into_iter().map(ident_value).collect();
            if unique && !included_columns.is_empty() {
                return unsupported("UNIQUE INDEX cannot declare INCLUDE columns");
            }
            if name.0.len() != 1 {
                return Err(parse_error_at_zero(
                    "index name must not be schema-qualified",
                ));
            }
            Ok(Statement::CreateIndex(CreateIndex {
                name: object_name_to_string(name)?,
                table: object_name_to_string(table_name)?,
                algorithm,
                lock,
                query_hints: Vec::new(),
                hint_warnings: Vec::new(),
                columns: columns.names,
                column_prefix_lengths: columns.prefix_lengths,
                column_descending: columns.descending,
                key_expressions: columns.key_expressions,
                key_expression_multi_valued: columns.key_expression_multi_valued,
                unique,
                included_columns,
                index_type,
                visible: true,
                comment,
                // The FULLTEXT keyword is stripped in [`strip_create_index_fulltext`];
                // here we convert as a plain index first; the parser name is injected later by [`apply_fulltext_parsers`].
                fulltext_parser: String::new(),
            }))
        }
        SqlStatement::Analyze(analyze) => convert_analyze(sql, analyze),
        SqlStatement::Insert(insert) => convert_insert(sql, insert),
        SqlStatement::Query(query) => convert_query(sql, *query),
        SqlStatement::Explain {
            statement,
            format,
            analyze,
            ..
        } => {
            let target = convert_statement(sql, *statement)?;
            Ok(Statement::Explain {
                target: Box::new(target),
                format: convert_explain_format(format),
                analyze,
            })
        }
        SqlStatement::Set(set) => convert_set_statement(sql, set),
        SqlStatement::Update(update) => convert_update(sql, update),
        SqlStatement::Delete(delete) => convert_delete(sql, delete),
        SqlStatement::Truncate(truncate) => convert_truncate(sql, truncate),
        SqlStatement::CreateView(view) => convert_create_view(sql, view),
        SqlStatement::AlterView {
            name,
            columns,
            query,
            with_options: _,
        } => convert_alter_view(sql, name, columns, *query),
        SqlStatement::Use(use_expr) => convert_use(use_expr),
        SqlStatement::ShowDatabases {
            terse,
            history,
            show_options,
        }
        | SqlStatement::ShowSchemas {
            terse,
            history,
            show_options,
        } => {
            if terse {
                return unsupported("SHOW TERSE DATABASES");
            }
            if history {
                return unsupported("SHOW DATABASES HISTORY");
            }
            Ok(Statement::ShowDatabases {
                filter: show_options_filter(&show_options)?,
            })
        }
        SqlStatement::ShowTables {
            terse,
            history,
            extended: _,
            full,
            external,
            show_options,
        } => {
            // sqlparser exposes a handful of non-MySQL dialect flags here
            // (Snowflake `TERSE` / `HISTORY` / `EXTERNAL`). mydb only speaks
            // MySQL — reject them outright rather than silently accepting,
            // matching the project rule against silent no-ops.
            if terse {
                return unsupported("SHOW TERSE TABLES");
            }
            if history {
                return unsupported("SHOW TABLES HISTORY");
            }
            if external {
                return unsupported("SHOW EXTERNAL TABLES");
            }
            // EXTENDED is the MySQL flag that asks for hidden `#sql*` tables
            // left over from a failed ALTER TABLE. mydb never leaves those
            // around, so EXTENDED is a strict superset of the base output —
            // accepted and ignored, with the row shape unchanged.
            Ok(Statement::ShowTables {
                database: show_database(&show_options),
                full,
                filter: show_options_filter(&show_options)?,
            })
        }
        SqlStatement::ShowColumns {
            extended: _,
            full,
            show_options,
        } => {
            let Some(table) = show_table(&show_options) else {
                return unsupported("SHOW COLUMNS without table");
            };
            // EXTENDED asks MySQL to also list columns reserved for internal
            // use. mydb has no such hidden columns, so EXTENDED is accepted
            // and treated identically to the base form.
            Ok(Statement::ShowColumns {
                table,
                full,
                filter: show_options_filter(&show_options)?,
            })
        }
        SqlStatement::ShowCreate { obj_type, obj_name }
            if obj_type == ShowCreateObject::Table || obj_type == ShowCreateObject::View =>
        {
            // Both SHOW CREATE TABLE and SHOW CREATE VIEW funnel through the
            // same Statement variant; the executor decides at run time
            // whether the target name is a base table or a view and emits
            // the appropriate `CREATE TABLE`/`CREATE VIEW` text. This
            // matches MySQL's lenient behaviour where `SHOW CREATE TABLE v`
            // on a view also returns the view definition.
            Ok(Statement::ShowCreateTable {
                table: object_name_to_string(obj_name)?,
            })
        }
        SqlStatement::ShowCharset(show) => Ok(Statement::ShowCharset {
            filter: convert_show_filter(show.filter)?,
        }),
        SqlStatement::ShowCollation { filter } => Ok(Statement::ShowCollation {
            filter: convert_show_filter(filter)?,
        }),
        SqlStatement::ShowVariables {
            filter,
            global,
            session,
        } => Ok(Statement::ShowVariables {
            scope: show_scope(session, global),
            filter: convert_show_filter(filter)?,
        }),
        SqlStatement::ShowStatus {
            filter,
            global,
            session,
        } => Ok(Statement::ShowStatus {
            scope: show_scope(session, global),
            filter: convert_show_filter(filter)?,
        }),
        SqlStatement::ExplainTable {
            describe_alias,
            table_name,
            ..
        } if matches!(
            describe_alias,
            DescribeAlias::Desc | DescribeAlias::Describe | DescribeAlias::Explain
        ) =>
        {
            Ok(Statement::Describe {
                table: object_name_to_string(table_name)?,
            })
        }
        SqlStatement::Grant(_) | SqlStatement::Revoke(_) | SqlStatement::CreateUser(_) => {
            unsupported(sql_statement_kind(&stmt))
        }
        SqlStatement::Declare { stmts } => convert_session_declare_cursor(sql, stmts),
        SqlStatement::Fetch {
            name,
            direction,
            into,
            ..
        } => convert_session_fetch_cursor(sql, name, direction, into),
        SqlStatement::Close { cursor } => convert_session_close_cursor(sql, cursor),
        other => {
            let kind = sql_statement_kind(&other);
            unsupported(kind)
        }
    }
}

/// PostgreSQL-flavoured `DECLARE <name> CURSOR FOR <query>` at the
/// session level. We only accept the simple form — a single cursor
/// name, no SCROLL / WITH HOLD / BINARY / SENSITIVE modifiers, no
/// `data_type`/`assignment` (which would make this a PL/pgSQL variable
/// declaration). Anything else falls through to a clear unsupported
/// error so users don't think a fancy clause silently took effect.
fn convert_session_declare_cursor(
    sql: &str,
    stmts: Vec<sqlparser::ast::Declare>,
) -> Result<Statement, ParseError> {
    if stmts.len() != 1 {
        return Err(parse_error_full_span(
            sql,
            "only a single DECLARE clause per statement is supported",
        ));
    }
    let decl = stmts.into_iter().next().unwrap();
    if !matches!(decl.declare_type, Some(sqlparser::ast::DeclareType::Cursor)) {
        return Err(parse_error_full_span(
            sql,
            "only DECLARE ... CURSOR is supported outside stored routines",
        ));
    }
    if decl.data_type.is_some() || decl.assignment.is_some() {
        return Err(parse_error_full_span(
            sql,
            "DECLARE CURSOR does not accept a data type or default assignment",
        ));
    }
    if matches!(decl.scroll, Some(true))
        || matches!(decl.hold, Some(true))
        || matches!(decl.sensitive, Some(true))
    {
        return Err(parse_error_full_span(
            sql,
            "DECLARE CURSOR SCROLL/WITH HOLD/SENSITIVE modifiers are not supported",
        ));
    }
    let mut names = decl.names;
    if names.len() != 1 {
        return Err(parse_error_full_span(
            sql,
            "DECLARE CURSOR requires exactly one cursor name",
        ));
    }
    let name = names.remove(0).value;
    if name.is_empty() {
        return Err(parse_error_full_span(sql, "DECLARE CURSOR name is empty"));
    }
    let query = decl
        .for_query
        .ok_or_else(|| parse_error_full_span(sql, "DECLARE CURSOR requires FOR <select>"))?;
    let inner = QueryExpr::try_from(convert_query(sql, *query)?)?;
    Ok(Statement::DeclareSqlCursor {
        name,
        query: inner,
        sql: sql.to_string(),
    })
}

/// `FETCH [count|NEXT|FIRST|FORWARD count] [FROM] <name>` — pulls rows
/// out of a SQL cursor. We map FORWARD to a finite count and treat NEXT
/// / FIRST as count=1 (SQL standard). RELATIVE / ABSOLUTE / SCROLL,
/// FORWARD ALL, BACKWARD, and INTO are rejected — none of them make
/// sense for the forward-only, server-side streaming cursor we expose.
fn convert_session_fetch_cursor(
    sql: &str,
    name: sqlparser::ast::Ident,
    direction: sqlparser::ast::FetchDirection,
    into: Option<sqlparser::ast::ObjectName>,
) -> Result<Statement, ParseError> {
    use sqlparser::ast::FetchDirection;
    use sqlparser::ast::Value as SqlAstValue;
    if into.is_some() {
        return Err(parse_error_full_span(
            sql,
            "FETCH ... INTO @var is not supported; assign FETCH columns via SELECT INTO instead",
        ));
    }
    let count = match direction {
        FetchDirection::Next | FetchDirection::First | FetchDirection::Last => 1,
        FetchDirection::Count { limit } | FetchDirection::Forward { limit: Some(limit) } => {
            // sqlparser 0.61's FetchDirection carries a bare `Value`,
            // not a `ValueWithSpan`. Only an integer literal is meaningful
            // for FETCH count — anything else (placeholder, string,
            // expression) is rejected.
            match limit {
                SqlAstValue::Number(raw, _) => raw.parse::<u64>().map_err(|_| {
                    parse_error_full_span(sql, "FETCH count must be a non-negative integer")
                })?,
                _ => {
                    return Err(parse_error_full_span(
                        sql,
                        "FETCH count must be a numeric literal",
                    ))
                }
            }
        }
        FetchDirection::Forward { limit: None } => 1,
        _ => {
            return Err(parse_error_full_span(
                sql,
                "FETCH direction not supported (only NEXT / FIRST / count are accepted)",
            ))
        }
    };
    if count == 0 {
        return Err(parse_error_full_span(sql, "FETCH count must be at least 1"));
    }
    Ok(Statement::FetchSqlCursor {
        name: name.value,
        count,
    })
}

fn convert_session_close_cursor(
    sql: &str,
    cursor: sqlparser::ast::CloseCursor,
) -> Result<Statement, ParseError> {
    match cursor {
        sqlparser::ast::CloseCursor::Specific { name } => {
            Ok(Statement::CloseSqlCursor { name: name.value })
        }
        sqlparser::ast::CloseCursor::All => Err(parse_error_full_span(
            sql,
            "CLOSE ALL is not supported; close cursors by name",
        )),
    }
}

fn convert_set_statement(sql: &str, set: sqlparser::ast::Set) -> Result<Statement, ParseError> {
    match set {
        sqlparser::ast::Set::MultipleAssignments { assignments } => {
            let mut assignments = assignments
                .into_iter()
                .map(convert_set_assignment)
                .collect::<Result<Vec<_>, _>>()?;
            // MySQL applies a leading SET scope to the complete assignment
            // list (`SET GLOBAL a=1, b=2`). sqlparser attaches the modifier
            // only to the first assignment, so carry it to unqualified
            // siblings while retaining any explicit per-assignment scope.
            if let Some(inherited_scope) = assignments.iter().find_map(|item| item.scope) {
                for item in &mut assignments {
                    if item.scope.is_none() {
                        item.scope = Some(inherited_scope);
                    }
                }
            }
            Ok(Statement::Set(assignments))
        }
        sqlparser::ast::Set::SingleAssignment {
            scope,
            variable,
            values,
            ..
        } => {
            if values.len() != 1 {
                return Ok(Statement::Unsupported {
                    kind: "SET assignment list".to_string(),
                    sql: sql.to_string(),
                });
            }
            let name = object_name_to_string(variable)?;
            validate_set_qualifier(convert_set_scope(scope), &name)?;
            Ok(Statement::Set(vec![SetAssignment {
                scope: convert_set_scope(scope),
                name,
                value: convert_expr(&values.into_iter().next().unwrap())?,
            }]))
        }
        sqlparser::ast::Set::ParenthesizedAssignments { .. } => Ok(Statement::Unsupported {
            kind: "SET parenthesized assignments".to_string(),
            sql: sql.to_string(),
        }),
        sqlparser::ast::Set::SetNames {
            charset_name,
            collation_name,
        } => Ok(Statement::SetNames {
            charset: Some(ident_value(charset_name)),
            collation: collation_name,
        }),
        sqlparser::ast::Set::SetNamesDefault { .. } => Ok(Statement::SetNames {
            charset: None,
            collation: None,
        }),
        // PostgreSQL-style `SET TIME ZONE 'UTC'` reaches this arm — MySQL
        // uses `SET time_zone = 'UTC'` instead, which goes through the
        // regular SingleAssignment path. We reject the PG form explicitly
        // so the operator gets a hint instead of a generic "Unsupported".
        sqlparser::ast::Set::SetTimeZone { .. } => Err(parse_error_full_span(
            sql,
            "PostgreSQL-style `SET TIME ZONE ...` is not supported; use `SET time_zone = '...'`",
        )),
        // `SET TRANSACTION` (with characteristics) is parsed by our manual
        // fast-path before sqlparser sees it. Reaching this arm means the
        // fast-path didn't recognise the shape — emit a precise error.
        sqlparser::ast::Set::SetTransaction { .. } => Err(parse_error_full_span(
            sql,
            "unsupported SET TRANSACTION form; use `[GLOBAL|SESSION] TRANSACTION {ISOLATION LEVEL X | READ ONLY | READ WRITE}`",
        )),
        sqlparser::ast::Set::SetSessionAuthorization(..) => Err(parse_error_full_span(
            sql,
            "`SET SESSION AUTHORIZATION` is a PostgreSQL extension; not supported in MySQL mode",
        )),
        sqlparser::ast::Set::SetSessionParam(..) => Err(parse_error_full_span(
            sql,
            "`SET SESSION ...` parameter form not recognised by mydb",
        )),
        // SET ROLE has its own fast-path; reaching this arm is a bug.
        sqlparser::ast::Set::SetRole { .. } => Err(parse_error_full_span(
            sql,
            "unsupported SET ROLE form",
        )),
    }
}

fn validate_set_qualifier(scope: Option<SetScope>, name: &str) -> Result<(), ParseError> {
    if scope.is_some() && name.starts_with("@@") {
        return Err(parse_error_full_span(
            name,
            "SET scope cannot be combined with an @@ variable qualifier",
        ));
    }
    Ok(())
}

fn convert_set_assignment(assign: SqlSetAssignment) -> Result<SetAssignment, ParseError> {
    let name = object_name_to_string(assign.name)?;
    validate_set_qualifier(convert_set_scope(assign.scope), &name)?;
    Ok(SetAssignment {
        scope: convert_set_scope(assign.scope),
        name,
        value: convert_expr(&assign.value)?,
    })
}

fn convert_set_scope(scope: Option<sqlparser::ast::ContextModifier>) -> Option<SetScope> {
    match scope {
        Some(sqlparser::ast::ContextModifier::Session)
        | Some(sqlparser::ast::ContextModifier::Local) => Some(SetScope::Session),
        Some(sqlparser::ast::ContextModifier::Global) => Some(SetScope::Global),
        None => None,
    }
}

fn convert_analyze(sql: &str, analyze: Analyze) -> Result<Statement, ParseError> {
    let Some(table_name) = analyze.table_name else {
        return Ok(Statement::Unsupported {
            kind: "ANALYZE without table".to_string(),
            sql: sql.to_string(),
        });
    };
    if !analyze.has_table_keyword
        || analyze.partitions.is_some()
        || analyze.for_columns
        || !analyze.columns.is_empty()
        || analyze.cache_metadata
        || analyze.noscan
        || analyze.compute_statistics
    {
        return Ok(Statement::Unsupported {
            kind: "ANALYZE TABLE options".to_string(),
            sql: sql.to_string(),
        });
    }
    Ok(Statement::AnalyzeTable {
        tables: vec![object_name_to_string(table_name)?],
        partitions: None,
        histogram: None,
    })
}

fn convert_explain_format(format: Option<AnalyzeFormatKind>) -> Option<ExplainFormat> {
    let format = format?.to_string().to_ascii_uppercase();
    if format.contains("JSON") {
        Some(ExplainFormat::Json)
    } else if format.contains("TREE") {
        Some(ExplainFormat::Tree)
    } else if format.contains("TRADITIONAL") {
        Some(ExplainFormat::Traditional)
    } else {
        None
    }
}

fn convert_use(use_expr: Use) -> Result<Statement, ParseError> {
    let name = match use_expr {
        Use::Database(name) | Use::Schema(name) | Use::Object(name) => object_name_to_string(name)?,
        Use::Default => "test".to_string(),
        _ => {
            return parse_error_from_sql_span(
                sqlparser::tokenizer::Span::empty(),
                "unsupported USE statement",
                vec![],
            )
        }
    };
    Ok(Statement::UseDatabase(name))
}

fn convert_show_filter(
    filter: Option<SqlShowStatementFilter>,
) -> Result<Option<ShowFilter>, ParseError> {
    match filter {
        None => Ok(None),
        Some(SqlShowStatementFilter::Like(pattern)) => Ok(Some(ShowFilter::Like(pattern))),
        Some(SqlShowStatementFilter::Where(expr)) => {
            Ok(Some(ShowFilter::Where(convert_expr(&expr)?)))
        }
        Some(SqlShowStatementFilter::ILike(_)) | Some(SqlShowStatementFilter::NoKeyword(_)) => Err(
            parse_error_at_zero("SHOW ILIKE/raw filters are not supported by MySQL"),
        ),
    }
}

/// Preserve literal bytes: sqlparser's Display is not a MySQL SQL serializer.
fn raw_view_body(sql: &str) -> Result<String, ParseError> {
    use crate::sql_lex::{SqlLexMode, SqlTokenCursor, SqlTokenKind};
    let mode = SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let mut cursor = SqlTokenCursor::new(sql, mode, MYSQL_VERSION_ID);
    let mut saw_view = false;
    while let Some(token) = cursor
        .next()
        .map_err(|e| parse_error_at_zero(e.to_string()))?
    {
        if token.kind != SqlTokenKind::Word {
            continue;
        }
        let word = &sql[token.span.clone()];
        if word.eq_ignore_ascii_case("VIEW") {
            saw_view = true;
        }
        if saw_view && word.eq_ignore_ascii_case("AS") {
            return Ok(sql[token.span.end..]
                .trim()
                .trim_end_matches(';')
                .trim()
                .to_string());
        }
    }
    Err(parse_error_at_zero("view definition requires AS query"))
}

pub(super) fn convert_create_view(sql: &str, view: SqlCreateView) -> Result<Statement, ParseError> {
    // Full-field destructure (M5.18) + explicit guards (B6): reject view
    // modifiers mydb doesn't implement instead of silently accepting and
    // dropping them. `or_alter` (MsSql), `secure` (Snowflake), `options`
    // (WITH/OPTIONS), `cluster_by` (BigQuery) and `comment` (Snowflake) are not
    // MySQL 8.0 view syntax; surfacing them as Unsupported (1235) upholds the
    // "never a silent no-op" rule. A future sqlparser field fails to compile
    // here rather than being dropped.
    let SqlCreateView {
        or_alter,
        or_replace,
        materialized,
        secure,
        name,
        name_before_not_exists: _,
        columns,
        query,
        options,
        cluster_by,
        comment,
        with_no_schema_binding,
        if_not_exists,
        temporary,
        to,
        params,
    } = view;
    let options_present = !matches!(options, sqlparser::ast::CreateTableOptions::None);
    if materialized
        || temporary
        || to.is_some()
        || with_no_schema_binding
        || or_alter
        || secure
        || options_present
        || !cluster_by.is_empty()
        || comment.is_some()
    {
        return Ok(Statement::Unsupported {
            kind: "CREATE VIEW variant".to_string(),
            sql: sql.to_string(),
        });
    }
    let query = QueryExpr::try_from(convert_query(sql, *query)?)?;
    let view_sql = raw_view_body(sql)?;
    let columns = columns.iter().map(|c| c.name.value.clone()).collect();
    let (algorithm, definer, security_type) = convert_create_view_params(params)?;
    Ok(Statement::CreateView {
        name: object_name_to_qualified(name)?,
        sql: view_sql,
        query,
        or_replace,
        if_not_exists,
        algorithm,
        definer,
        security_type,
        columns,
        check_option: None,
    })
}

fn convert_alter_view(
    sql: &str,
    name: sqlparser::ast::ObjectName,
    columns: Vec<sqlparser::ast::Ident>,
    query: sqlparser::ast::Query,
) -> Result<Statement, ParseError> {
    let query = QueryExpr::try_from(convert_query(sql, query)?)?;
    let view_sql = raw_view_body(sql)?;
    let column_names: Vec<String> = columns.into_iter().map(|c| c.value).collect();
    Ok(Statement::AlterView {
        name: object_name_to_qualified(name)?,
        sql: view_sql,
        query,
        algorithm: None,
        definer: None,
        security_type: None,
        columns: column_names,
        check_option: None,
    })
}

fn convert_create_view_params(
    params: Option<sqlparser::ast::CreateViewParams>,
) -> Result<
    (
        Option<String>,
        Option<AccountIdent>,
        Option<RoutineSecurity>,
    ),
    ParseError,
> {
    let Some(params) = params else {
        return Ok((None, None, None));
    };
    let algorithm = params.algorithm.map(|algorithm| algorithm.to_string());
    let definer = params
        .definer
        .map(convert_view_definer)
        .transpose()?
        .flatten();
    let security = params.security.map(|security| match security {
        sqlparser::ast::CreateViewSecurity::Definer => RoutineSecurity::Definer,
        sqlparser::ast::CreateViewSecurity::Invoker => RoutineSecurity::Invoker,
    });
    Ok((algorithm, definer, security))
}

fn convert_view_definer(
    definer: sqlparser::ast::GranteeName,
) -> Result<Option<AccountIdent>, ParseError> {
    match definer {
        sqlparser::ast::GranteeName::UserHost { user, host } => Ok(Some(AccountIdent {
            user: user.value,
            host: host.value,
        })),
        sqlparser::ast::GranteeName::ObjectName(name) => {
            let name = object_name_to_string(name)?;
            if name.eq_ignore_ascii_case("CURRENT_USER")
                || name.eq_ignore_ascii_case("CURRENT_USER()")
            {
                Ok(None)
            } else {
                parse_definer_account(&name)
                    .map(Some)
                    .ok_or_else(|| parse_error_at_zero("invalid view definer"))
            }
        }
    }
}

fn convert_create_table(
    sql: &str,
    mut table: sqlparser::ast::CreateTable,
) -> Result<Statement, ParseError> {
    // Full-field audit: MySQL-mode conversion must never silently discard a
    // modifier added by sqlparser. Supported fields are consumed below;
    // dialect-specific fields are rejected explicitly, while ON COMMIT is a
    // retained mydb extension.
    let sqlparser::ast::CreateTable {
        or_replace,
        temporary: _,
        external,
        dynamic,
        global,
        if_not_exists: _,
        transient,
        volatile,
        iceberg,
        name: _,
        columns: _,
        constraints: _,
        hive_distribution,
        hive_formats,
        table_options: _,
        file_format,
        location,
        query: _,
        without_rowid,
        like: _,
        clone: _,
        version,
        comment,
        on_commit: _,
        on_cluster,
        primary_key,
        order_by,
        partition_by,
        cluster_by,
        clustered_by,
        inherits,
        partition_of: _,
        for_values,
        strict,
        copy_grants,
        enable_schema_evolution,
        change_tracking,
        data_retention_time_in_days,
        max_data_extension_time_in_days,
        default_ddl_collation,
        with_aggregation_policy,
        with_row_access_policy,
        with_tags,
        external_volume,
        base_location,
        catalog,
        catalog_sync,
        storage_serialization_policy,
        target_lag,
        warehouse,
        refresh_mode,
        initialize,
        require_user,
    } = &table;
    let unsupported_variant = *or_replace
        || *external
        || *dynamic
        || global.is_some()
        || *transient
        || *volatile
        || *iceberg
        || !matches!(
            hive_distribution,
            sqlparser::ast::HiveDistributionStyle::NONE
        )
        || hive_formats.is_some()
        || file_format.is_some()
        || location.is_some()
        || *without_rowid
        || version.is_some()
        || comment.is_some()
        || on_cluster.is_some()
        || primary_key.is_some()
        || order_by.is_some()
        || partition_by.is_some()
        || cluster_by.is_some()
        || clustered_by.is_some()
        || inherits.is_some()
        || for_values.is_some()
        || *strict
        || *copy_grants
        || enable_schema_evolution.is_some()
        || change_tracking.is_some()
        || data_retention_time_in_days.is_some()
        || max_data_extension_time_in_days.is_some()
        || default_ddl_collation.is_some()
        || with_aggregation_policy.is_some()
        || with_row_access_policy.is_some()
        || with_tags.is_some()
        || external_volume.is_some()
        || base_location.is_some()
        || catalog.is_some()
        || catalog_sync.is_some()
        || storage_serialization_policy.is_some()
        || target_lag.is_some()
        || warehouse.is_some()
        || refresh_mode.is_some()
        || initialize.is_some()
        || *require_user;
    if unsupported_variant {
        return Ok(Statement::Unsupported {
            kind: "CREATE TABLE variant".to_string(),
            sql: sql.to_string(),
        });
    }
    let temporary = table.temporary;
    let on_commit = table.on_commit.map(|on_commit| match on_commit {
        sqlparser::ast::OnCommit::PreserveRows => TempTableOnCommit::PreserveRows,
        sqlparser::ast::OnCommit::DeleteRows => TempTableOnCommit::DeleteRows,
        sqlparser::ast::OnCommit::Drop => TempTableOnCommit::Drop,
    });
    if on_commit.is_some() && !temporary {
        return Ok(Statement::Unsupported {
            kind: "ON COMMIT on non-temporary table".to_string(),
            sql: sql.to_string(),
        });
    }
    if let Some(query) = table.query.take() {
        let explicit_definition = !table.columns.is_empty() || !table.constraints.is_empty();
        // Run every CTAS target through the full plain CREATE conversion,
        // including the bare form. Previously the early return for a bare
        // CTAS discarded ENGINE/charset/table options before the executor
        // ever saw them.
        let inner = QueryExpr::try_from(convert_query(sql, *query)?)?;
        let converted = convert_create_table(sql, table)?;
        let Statement::CreateTable(table) = converted else {
            return Ok(converted);
        };
        return Ok(Statement::CreateTableAsSelect {
            table: Box::new(table),
            explicit_definition,
            conflict_policy: CtasConflictPolicy::Error,
            query: inner,
        });
    }
    if let Some(like) = table.like {
        if !table.columns.is_empty() || !table.constraints.is_empty() {
            return Ok(Statement::Unsupported {
                kind: "CREATE TABLE LIKE with column definitions".to_string(),
                sql: sql.to_string(),
            });
        }
        let like = match like {
            sqlparser::ast::CreateTableLikeKind::Plain(like)
            | sqlparser::ast::CreateTableLikeKind::Parenthesized(like) => like,
        };
        if like.defaults.is_some() {
            return Ok(Statement::Unsupported {
                kind: "CREATE TABLE LIKE defaults option".to_string(),
                sql: sql.to_string(),
            });
        }
        return Ok(Statement::CreateTableLike {
            name: object_name_to_string(table.name)?,
            if_not_exists: table.if_not_exists,
            source: object_name_to_string(like.name)?,
            temporary,
            on_commit,
        });
    }
    if table.clone.is_some() || table.partition_of.is_some() {
        return Ok(Statement::Unsupported {
            kind: "CREATE TABLE variant".to_string(),
            sql: sql.to_string(),
        });
    }

    let table_options = match create_table_options(sql, &table.table_options)? {
        Ok(options) => options,
        Err(stmt) => return Ok(stmt),
    };
    let mut primary_key = Vec::new();
    let mut primary_key_defined = false;
    let mut indexes = Vec::new();
    let mut columns = Vec::with_capacity(table.columns.len());
    let mut column_comments = Vec::new();
    let mut checks = Vec::new();
    let mut foreign_keys = Vec::new();
    for column in table.columns {
        let (column, comment) = convert_column_def_with_comment(sql, column)?;
        if let Some(comment) = comment {
            column_comments.push((column.name.clone(), comment));
        }
        if column.primary_key {
            if std::mem::replace(&mut primary_key_defined, true) {
                return Err(parse_error_with_spec_at_zero(
                    crate::error::catalog::ER_MULTIPLE_PRI_KEY,
                    "Multiple primary key defined",
                ));
            }
            primary_key.push(column.name.clone());
        }
        if column.unique {
            let columns = ConvertedIndexColumns::bare(vec![column.name.clone()]);
            indexes.push(IndexDef {
                // MySQL derives an unnamed inline UNIQUE index name from
                // the first key column, not from a private `_uniq` suffix.
                name: Some(column.name.clone()),
                columns: columns.names,
                column_prefix_lengths: columns.prefix_lengths,
                column_descending: columns.descending,
                key_expressions: columns.key_expressions,
                key_expression_multi_valued: columns.key_expression_multi_valued,
                unique: true,
                included_columns: Vec::new(),
                index_type: "BTREE".to_string(),
                visible: true,
                comment: String::new(),
                fulltext_parser: String::new(),
            });
        }
        columns.push(column);
    }
    for constraint in table.constraints {
        match constraint {
            TableConstraint::PrimaryKey(constraint) => {
                if constraint.characteristics.is_some() {
                    return Err(parse_error_at_zero(
                        "PRIMARY KEY constraint characteristics are not supported",
                    ));
                }
                if std::mem::replace(&mut primary_key_defined, true) {
                    return Err(parse_error_with_spec_at_zero(
                        crate::error::catalog::ER_MULTIPLE_PRI_KEY,
                        "Multiple primary key defined",
                    ));
                }
                let (index_type, comment) = convert_index_type_and_options(
                    constraint.index_type,
                    &constraint.index_options,
                )?;
                if index_type != "BTREE" {
                    return Err(parse_error_at_zero("PRIMARY KEY requires a BTREE index"));
                }
                let columns = convert_index_columns(constraint.columns)?;
                if columns.has_expressions() {
                    return Ok(Statement::Unsupported {
                        kind: "PRIMARY KEY on expression".to_string(),
                        sql: sql.to_string(),
                    });
                }
                if columns.has_prefix_or_desc() || !comment.is_empty() {
                    // MySQL supports prefix / DESC key parts on the PK
                    // (mandatory for BLOB/TEXT keys). Route through the
                    // generic index machinery, which preserves both —
                    // storage recognises the PRIMARY-named IndexDef.
                    indexes.push(IndexDef {
                        name: Some("PRIMARY".to_string()),
                        columns: columns.names,
                        column_prefix_lengths: columns.prefix_lengths,
                        column_descending: columns.descending,
                        key_expressions: columns.key_expressions,
                        key_expression_multi_valued: columns.key_expression_multi_valued,
                        unique: true,
                        included_columns: Vec::new(),
                        index_type,
                        visible: true,
                        comment,
                        fulltext_parser: String::new(),
                    });
                } else {
                    primary_key = columns.names;
                }
            }
            TableConstraint::Unique(constraint) => {
                if constraint.characteristics.is_some()
                    || !matches!(
                        constraint.nulls_distinct,
                        sqlparser::ast::NullsDistinctOption::None
                    )
                {
                    return Err(parse_error_at_zero("UNIQUE constraint characteristics / NULLS DISTINCT options are not supported"));
                }
                let columns = convert_index_columns(constraint.columns)?;
                let (index_type, comment) = convert_index_type_and_options(
                    constraint.index_type,
                    &constraint.index_options,
                )?;
                if index_type != "BTREE" {
                    return Ok(Statement::Unsupported {
                        kind: format!("UNIQUE INDEX USING {index_type}"),
                        sql: sql.to_string(),
                    });
                }
                indexes.push(IndexDef {
                    name: constraint.index_name.or(constraint.name).map(ident_value),
                    columns: columns.names,
                    column_prefix_lengths: columns.prefix_lengths,
                    column_descending: columns.descending,
                    key_expressions: columns.key_expressions,
                    key_expression_multi_valued: columns.key_expression_multi_valued,
                    unique: true,
                    included_columns: Vec::new(),
                    index_type,
                    visible: true,
                    comment,
                    fulltext_parser: String::new(),
                });
            }
            TableConstraint::Index(constraint) => {
                let columns = convert_index_columns(constraint.columns)?;
                let (index_type, comment) = convert_index_type_and_options(
                    constraint.index_type,
                    &constraint.index_options,
                )?;
                // BTREE and vector (HNSW / IVF) indexes are allowed as inline
                // table keys; other special index types use dedicated keyword paths.
                if index_type != "BTREE" && index_type != "HNSW" && index_type != "IVF" {
                    return Ok(Statement::Unsupported {
                        kind: format!("INDEX USING {index_type}"),
                        sql: sql.to_string(),
                    });
                }
                indexes.push(IndexDef {
                    name: constraint.name.map(ident_value),
                    columns: columns.names,
                    column_prefix_lengths: columns.prefix_lengths,
                    column_descending: columns.descending,
                    key_expressions: columns.key_expressions,
                    key_expression_multi_valued: columns.key_expression_multi_valued,
                    unique: false,
                    included_columns: Vec::new(),
                    index_type,
                    visible: true,
                    comment,
                    fulltext_parser: String::new(),
                });
            }
            TableConstraint::Check(constraint) => {
                checks.push(convert_check_constraint(constraint, None)?);
            }
            TableConstraint::FulltextOrSpatial(constraint) if constraint.fulltext => {
                let mut columns = convert_index_columns(constraint.columns)?;
                // MySQL accepts and ignores prefix lengths / ASC|DESC on FULLTEXT key parts
                // (a prefix is meaningless for an inverted index); we align with that behavior. Expression keys are still rejected.
                columns.prefix_lengths.iter_mut().for_each(|p| *p = None);
                columns.descending.iter_mut().for_each(|d| *d = false);
                if columns.has_expressions() {
                    return Ok(Statement::Unsupported {
                        kind: "FULLTEXT INDEX on expression".to_string(),
                        sql: sql.to_string(),
                    });
                }
                indexes.push(IndexDef {
                    name: constraint.opt_index_name.map(ident_value),
                    columns: columns.names,
                    column_prefix_lengths: columns.prefix_lengths,
                    column_descending: columns.descending,
                    key_expressions: columns.key_expressions,
                    key_expression_multi_valued: columns.key_expression_multi_valued,
                    unique: false,
                    included_columns: Vec::new(),
                    index_type: "FULLTEXT".to_string(),
                    visible: true,
                    comment: String::new(),
                    fulltext_parser: String::new(),
                });
            }
            TableConstraint::FulltextOrSpatial(constraint) => {
                // `!constraint.fulltext` ⇒ SPATIAL. A spatial index is over a
                // single geometry column with no prefix length / direction /
                // expression key.
                let columns = convert_index_columns(constraint.columns)?;
                if columns.names.len() != 1
                    || columns.has_prefix_or_desc()
                    || columns.has_expressions()
                {
                    return Ok(Statement::Unsupported {
                        kind: "SPATIAL INDEX must be a single plain geometry column".to_string(),
                        sql: sql.to_string(),
                    });
                }
                indexes.push(IndexDef {
                    name: constraint.opt_index_name.map(ident_value),
                    columns: columns.names,
                    column_prefix_lengths: columns.prefix_lengths,
                    column_descending: columns.descending,
                    key_expressions: columns.key_expressions,
                    key_expression_multi_valued: columns.key_expression_multi_valued,
                    unique: false,
                    included_columns: Vec::new(),
                    index_type: "SPATIAL".to_string(),
                    visible: true,
                    comment: String::new(),
                    fulltext_parser: String::new(),
                });
            }
            TableConstraint::ForeignKey(fk) => {
                foreign_keys.push(convert_foreign_key_constraint(fk)?);
            }
        }
    }

    Ok(Statement::CreateTable(CreateTable {
        name: object_name_to_string(table.name)?,
        if_not_exists: table.if_not_exists,
        temporary,
        on_commit,
        columns,
        column_comments,
        primary_key,
        indexes,
        checks,
        foreign_keys,
        auto_increment_start: table_options.auto_increment_start,
        default_charset: table_options.default_charset,
        default_collation: table_options.default_collation,
        table_comment: table_options.comment.unwrap_or_default(),
        // PCTFREE/PCTUSED are pre-stripped at the SQL-string level
        // (see `strip_create_table_pctfree_pctused`) and reattached
        // to the `Statement::CreateTable` after sqlparser returns.
        // They never reach this code path.
        pctfree: None,
        pctused: None,
        partition: None,
        table_options: table_options.generic,
        engine: table_options.engine,
    }))
}

/// Convert a sqlparser `AS type` + `Vec<SequenceOptions>` + `OWNED BY` into the
/// storage option bag. Defaults / validation happen later in the executor.
fn convert_sequence_options(
    data_type: Option<&SqlDataType>,
    options: &[SqlSequenceOptions],
    owned_by: Option<&ObjectName>,
) -> Result<crate::storage::sequence::SeqOptions, ParseError> {
    use crate::storage::sequence::BoundOpt;
    let mut opts = crate::storage::sequence::SeqOptions::default();
    if let Some(dt) = data_type {
        let Some(ty) = seq_type_from_sql(dt) else {
            return Err(parse_error_at_zero(
                "sequence type must be smallint, integer, or bigint",
            ));
        };
        opts.as_type = Some(ty);
    }
    for option in options {
        match option {
            SqlSequenceOptions::IncrementBy(expr, _) => {
                opts.increment = Some(seq_expr_to_i64(expr)?)
            }
            SqlSequenceOptions::MinValue(Some(expr)) => {
                opts.min_value = BoundOpt::Value(seq_expr_to_i64(expr)?)
            }
            SqlSequenceOptions::MinValue(None) => opts.min_value = BoundOpt::NoBound,
            SqlSequenceOptions::MaxValue(Some(expr)) => {
                opts.max_value = BoundOpt::Value(seq_expr_to_i64(expr)?)
            }
            SqlSequenceOptions::MaxValue(None) => opts.max_value = BoundOpt::NoBound,
            SqlSequenceOptions::StartWith(expr, _) => opts.start = Some(seq_expr_to_i64(expr)?),
            SqlSequenceOptions::Cache(expr) => opts.cache = Some(seq_expr_to_i64(expr)?),
            // sqlparser 0.61 inverts the flag: `CYCLE` => Cycle(false),
            // `NO CYCLE` => Cycle(true). Undo that here.
            SqlSequenceOptions::Cycle(b) => opts.cycle = Some(!*b),
        }
    }
    if let Some(obj) = owned_by {
        let none = matches!(obj.0.as_slice(), [ObjectNamePart::Identifier(ident)]
            if ident.quote_style.is_none() && ident.value.eq_ignore_ascii_case("none"));
        let parts = obj
            .0
            .iter()
            .map(|part| match part {
                ObjectNamePart::Identifier(ident) => Ok(ident.value.clone()),
                ObjectNamePart::Function(_) => {
                    Err(parse_error_at_zero("unsupported sequence owner name"))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        super::grammar::set_sequence_owner(&mut opts, parts, none).map_err(parse_error_at_zero)?;
    }
    Ok(opts)
}

fn convert_alter_table(
    sql: &str,
    table: sqlparser::ast::AlterTable,
) -> Result<Statement, ParseError> {
    let sqlparser::ast::AlterTable {
        name,
        if_exists,
        only,
        operations,
        location,
        on_cluster,
        table_type,
        end_token: _,
    } = table;
    if if_exists || only || location.is_some() || on_cluster.is_some() || table_type.is_some() {
        return Ok(Statement::Unsupported {
            kind: "ALTER TABLE variant".to_string(),
            sql: sql.to_string(),
        });
    }
    let table_name = object_name_to_string(name)?;
    // Pull off any ALGORITHM= and LOCK= clauses (each at most once;
    // additional copies are tolerated — we keep the last). The
    // remaining operations are kept in declaration order so the
    // dispatcher can batch homogeneous shapes (e.g. multiple ADD INDEX
    // onto one heap scan). MySQL accepts arbitrary ordering of the
    // algorithm/lock clauses relative to the mutations.
    let mut algorithm = AlterAlgorithm::Default;
    let mut lock = AlterLock::Default;
    let mutation_count = operations
        .iter()
        .filter(|op| {
            !matches!(
                op,
                sqlparser::ast::AlterTableOperation::Algorithm { .. }
                    | sqlparser::ast::AlterTableOperation::Lock { .. }
            )
        })
        .count();
    let mut actions = Vec::with_capacity(mutation_count);
    for op in operations {
        match op {
            sqlparser::ast::AlterTableOperation::Algorithm { algorithm: a, .. } => {
                algorithm = match a {
                    sqlparser::ast::AlterTableAlgorithm::Default => AlterAlgorithm::Default,
                    sqlparser::ast::AlterTableAlgorithm::Instant => AlterAlgorithm::Instant,
                    sqlparser::ast::AlterTableAlgorithm::Inplace => AlterAlgorithm::Inplace,
                    sqlparser::ast::AlterTableAlgorithm::Copy => AlterAlgorithm::Copy,
                };
            }
            sqlparser::ast::AlterTableOperation::Lock { lock: l, .. } => {
                lock = match l {
                    sqlparser::ast::AlterTableLock::Default => AlterLock::Default,
                    sqlparser::ast::AlterTableLock::None => AlterLock::None,
                    sqlparser::ast::AlterTableLock::Shared => AlterLock::Shared,
                    sqlparser::ast::AlterTableLock::Exclusive => AlterLock::Exclusive,
                };
            }
            mutation => match convert_alter_table_action(sql, mutation)? {
                ConvertedAlterAction::Action(action) => actions.push(action),
                ConvertedAlterAction::Actions(mut extra) => actions.append(&mut extra),
                ConvertedAlterAction::Unsupported(statement) => return Ok(statement),
            },
        }
    }
    // Partition-only ALTER has no remaining upstream mutation; its empty
    // action list is intentional and uses the same result construction.
    Ok(Statement::AlterTable(AlterTable {
        table: table_name,
        query_hints: Vec::new(),
        hint_warnings: Vec::new(),
        actions,
        partition_actions: Vec::new(),
        algorithm,
        lock,
    }))
}

/// Preserve ALTER action order while sharing comment and position lowering.
fn column_alter_actions(
    action: AlterAction,
    column: String,
    comment: Option<String>,
    position: Option<sqlparser::ast::MySQLColumnPosition>,
) -> ConvertedAlterAction {
    let position = convert_column_position(position);
    let mut actions =
        Vec::with_capacity(1 + usize::from(comment.is_some()) + usize::from(position.is_some()));
    actions.push(action);
    if let Some(comment) = comment {
        actions.push(AlterAction::SetColumnComment {
            column: column.clone(),
            comment,
        });
    }
    if let Some(position) = position {
        actions.push(AlterAction::MoveColumn { column, position });
    }
    ConvertedAlterAction::Actions(actions)
}

fn convert_column_position(
    position: Option<sqlparser::ast::MySQLColumnPosition>,
) -> Option<ColumnPosition> {
    match position {
        Some(sqlparser::ast::MySQLColumnPosition::First) => Some(ColumnPosition::First),
        Some(sqlparser::ast::MySQLColumnPosition::After(ident)) => {
            Some(ColumnPosition::After(ident_value(ident)))
        }
        None => None,
    }
}

fn convert_alter_table_action(
    sql: &str,
    mutation: sqlparser::ast::AlterTableOperation,
) -> Result<ConvertedAlterAction, ParseError> {
    let action = match mutation {
        sqlparser::ast::AlterTableOperation::AddColumn {
            column_def,
            if_not_exists,
            column_position,
            ..
        } => {
            if if_not_exists {
                return Ok(ConvertedAlterAction::Unsupported(Statement::Unsupported {
                    kind: "ALTER TABLE ADD COLUMN IF NOT EXISTS".to_string(),
                    sql: sql.to_string(),
                }));
            }
            let (column, comment) = convert_column_def_with_comment(sql, column_def)?;
            let column_name = column.name.clone();
            return Ok(column_alter_actions(
                AlterAction::AddColumn(column),
                column_name,
                comment,
                column_position,
            ));
        }
        sqlparser::ast::AlterTableOperation::AutoIncrement { value, .. } => {
            AlterAction::SetAutoIncrement(parse_auto_increment_value(&value.value)?)
        }
        sqlparser::ast::AlterTableOperation::AddConstraint { constraint, .. } => match constraint {
            TableConstraint::Unique(constraint) => {
                if constraint.characteristics.is_some()
                    || !matches!(
                        constraint.nulls_distinct,
                        sqlparser::ast::NullsDistinctOption::None
                    )
                {
                    return Err(parse_error_at_zero("UNIQUE constraint characteristics / NULLS DISTINCT options are not supported"));
                }
                let columns = convert_index_columns(constraint.columns)?;
                let (index_type, comment) = convert_index_type_and_options(
                    constraint.index_type,
                    &constraint.index_options,
                )?;
                AlterAction::AddIndex(IndexDef {
                    name: constraint.index_name.or(constraint.name).map(ident_value),
                    columns: columns.names,
                    column_prefix_lengths: columns.prefix_lengths,
                    column_descending: columns.descending,
                    key_expressions: columns.key_expressions,
                    key_expression_multi_valued: columns.key_expression_multi_valued,
                    unique: true,
                    included_columns: Vec::new(),
                    index_type,
                    visible: true,
                    comment,
                    fulltext_parser: String::new(),
                })
            }
            TableConstraint::PrimaryKey(constraint) => {
                if constraint.characteristics.is_some() {
                    return Err(parse_error_at_zero(
                        "PRIMARY KEY constraint characteristics are not supported",
                    ));
                }
                let (index_type, comment) = convert_index_type_and_options(
                    constraint.index_type,
                    &constraint.index_options,
                )?;
                if index_type != "BTREE" {
                    return Err(parse_error_at_zero("PRIMARY KEY requires a BTREE index"));
                }
                let columns = convert_index_columns(constraint.columns)?;
                if columns.has_expressions() {
                    return Ok(ConvertedAlterAction::Unsupported(Statement::Unsupported {
                        kind: "ALTER TABLE ADD PRIMARY KEY on expression".to_string(),
                        sql: sql.to_string(),
                    }));
                }
                AlterAction::AddIndex(IndexDef {
                    name: Some("PRIMARY".to_string()),
                    columns: columns.names,
                    column_prefix_lengths: columns.prefix_lengths,
                    column_descending: columns.descending,
                    key_expressions: columns.key_expressions,
                    key_expression_multi_valued: columns.key_expression_multi_valued,
                    unique: true,
                    included_columns: Vec::new(),
                    index_type,
                    visible: true,
                    comment,
                    fulltext_parser: String::new(),
                })
            }
            TableConstraint::Index(constraint) => {
                let columns = convert_index_columns(constraint.columns)?;
                let (index_type, comment) = convert_index_type_and_options(
                    constraint.index_type,
                    &constraint.index_options,
                )?;
                AlterAction::AddIndex(IndexDef {
                    name: constraint.name.map(ident_value),
                    columns: columns.names,
                    column_prefix_lengths: columns.prefix_lengths,
                    column_descending: columns.descending,
                    key_expressions: columns.key_expressions,
                    key_expression_multi_valued: columns.key_expression_multi_valued,
                    unique: false,
                    included_columns: Vec::new(),
                    index_type,
                    visible: true,
                    comment,
                    fulltext_parser: String::new(),
                })
            }
            TableConstraint::Check(constraint) => {
                AlterAction::AddCheck(convert_check_constraint(constraint, None)?)
            }
            TableConstraint::ForeignKey(fk) => {
                AlterAction::AddForeignKey(convert_foreign_key_constraint(fk)?)
            }
            // S6 — `ALTER TABLE … ADD FULLTEXT|SPATIAL …`. Mirrors the
            // CREATE TABLE handling of `FulltextOrSpatial`, mapping to an
            // AddIndex with the right index_type so the existing offline
            // build (BM25 / R-tree from the heap) runs. Previously this fell
            // through to Unsupported, and `apply_fulltext_parsers`' ALTER
            // branch (which looks for a fulltext AddIndex) was dead code.
            TableConstraint::FulltextOrSpatial(constraint) => {
                let mut columns = convert_index_columns(constraint.columns)?;
                let index_type = if constraint.fulltext {
                    // Prefix length / direction are meaningless for an
                    // inverted index; MySQL accepts and ignores them.
                    columns.prefix_lengths.iter_mut().for_each(|p| *p = None);
                    columns.descending.iter_mut().for_each(|d| *d = false);
                    if columns.has_expressions() {
                        return Ok(ConvertedAlterAction::Unsupported(Statement::Unsupported {
                            kind: "FULLTEXT INDEX on expression".to_string(),
                            sql: sql.to_string(),
                        }));
                    }
                    "FULLTEXT"
                } else {
                    if columns.names.len() != 1
                        || columns.has_prefix_or_desc()
                        || columns.has_expressions()
                    {
                        return Ok(ConvertedAlterAction::Unsupported(Statement::Unsupported {
                            kind: "SPATIAL INDEX must be a single plain geometry column"
                                .to_string(),
                            sql: sql.to_string(),
                        }));
                    }
                    "SPATIAL"
                };
                AlterAction::AddIndex(IndexDef {
                    name: constraint.opt_index_name.map(ident_value),
                    columns: columns.names,
                    column_prefix_lengths: columns.prefix_lengths,
                    column_descending: columns.descending,
                    key_expressions: columns.key_expressions,
                    key_expression_multi_valued: columns.key_expression_multi_valued,
                    unique: false,
                    included_columns: Vec::new(),
                    index_type: index_type.to_string(),
                    visible: true,
                    comment: String::new(),
                    fulltext_parser: String::new(),
                })
            } // NB: `TableConstraint` is matched exhaustively — a new
              // sqlparser variant will surface as a compile error here rather
              // than being silently dropped (per the project's destructure rule).
        },
        sqlparser::ast::AlterTableOperation::DropColumn {
            column_names,
            if_exists,
            drop_behavior,
            ..
        } if column_names.len() == 1 => {
            if if_exists || drop_behavior.is_some() {
                return Err(parse_error_full_span(
                    sql,
                    "DROP COLUMN IF EXISTS / CASCADE / RESTRICT is not supported",
                ));
            }
            AlterAction::DropColumn(ident_value(column_names.into_iter().next().unwrap()))
        }
        sqlparser::ast::AlterTableOperation::DropIndex { name } => {
            AlterAction::DropIndex(ident_value(name))
        }
        sqlparser::ast::AlterTableOperation::DropPrimaryKey { .. } => AlterAction::DropPrimaryKey,
        sqlparser::ast::AlterTableOperation::DropForeignKey { name, .. } => {
            AlterAction::DropForeignKey {
                name: ident_value(name),
            }
        }
        sqlparser::ast::AlterTableOperation::AlterColumn { column_name, op } => match op {
            sqlparser::ast::AlterColumnOperation::SetDefault { value } => {
                AlterAction::SetColumnDefault {
                    column: ident_value(column_name),
                    default: Some(convert_expr(&value)?),
                }
            }
            sqlparser::ast::AlterColumnOperation::DropDefault => AlterAction::SetColumnDefault {
                column: ident_value(column_name),
                default: None,
            },
            _ => {
                return Ok(ConvertedAlterAction::Unsupported(Statement::Unsupported {
                    kind: "ALTER TABLE ALTER COLUMN operation".to_string(),
                    sql: sql.to_string(),
                }))
            }
        },
        // DROP CHECK is decoded separately by the CHECK-clause rewriter.
        // Preserve DROP CONSTRAINT's permission to resolve foreign keys.
        sqlparser::ast::AlterTableOperation::DropConstraint {
            name, if_exists, ..
        } => AlterAction::DropCheck {
            name: ident_value(name),
            if_exists,
            include_foreign_keys: true,
        },
        sqlparser::ast::AlterTableOperation::ModifyColumn {
            col_name,
            data_type,
            options,
            column_position,
            ..
        } => {
            let (column, comment) = convert_column_parts(
                sql,
                col_name,
                data_type,
                options
                    .into_iter()
                    .map(|option| sqlparser::ast::ColumnOptionDef { name: None, option }),
            )?;
            let column_name = column.name.clone();
            return Ok(column_alter_actions(
                AlterAction::ModifyColumn(column),
                column_name,
                comment,
                column_position,
            ));
        }
        sqlparser::ast::AlterTableOperation::ChangeColumn {
            old_name,
            new_name,
            data_type,
            options,
            column_position,
            ..
        } => {
            let (column, comment) = convert_column_parts(
                sql,
                new_name,
                data_type,
                options
                    .into_iter()
                    .map(|option| sqlparser::ast::ColumnOptionDef { name: None, option }),
            )?;
            let column_name = column.name.clone();
            return Ok(column_alter_actions(
                AlterAction::ChangeColumn {
                    old_name: ident_value(old_name),
                    new_def: column,
                },
                column_name,
                comment,
                column_position,
            ));
        }
        sqlparser::ast::AlterTableOperation::RenameColumn {
            old_column_name,
            new_column_name,
            ..
        } => AlterAction::RenameColumn {
            old_name: ident_value(old_column_name),
            new_name: ident_value(new_column_name),
        },
        sqlparser::ast::AlterTableOperation::RenameTable { table_name } => {
            let name = match table_name {
                sqlparser::ast::RenameTableNameKind::As(name)
                | sqlparser::ast::RenameTableNameKind::To(name) => name,
            };
            AlterAction::RenameTable {
                new_name: object_name_to_string(name)?,
            }
        }
        _ => {
            return Ok(ConvertedAlterAction::Unsupported(Statement::Unsupported {
                kind: "ALTER TABLE operation".to_string(),
                sql: sql.to_string(),
            }))
        }
    };
    Ok(ConvertedAlterAction::Action(action))
}

fn convert_assignment(assignment: &sqlparser::ast::Assignment) -> Result<Assignment, ParseError> {
    let SqlAssignmentTarget::ColumnName(name) = &assignment.target else {
        return parse_error_from_sql_span(
            sqlparser::tokenizer::Span::empty(),
            "unsupported assignment target",
            vec![],
        );
    };
    let (column, table_qualifier) = assignment_column_with_qualifier(name)?;
    Ok(Assignment {
        column,
        value: convert_expr(&assignment.value)?,
        table_qualifier,
    })
}

pub(super) fn convert_insert(
    sql: &str,
    insert: sqlparser::ast::Insert,
) -> Result<Statement, ParseError> {
    // Irrefutable full-field destructure (M5.18): a future sqlparser upgrade
    // that adds an INSERT field then fails to compile here rather than silently
    // dropping it — this is the exact function the `priority` regression
    // lived in for a year. `optimizer_hint` is a pure perf hint (accept and
    // ignore, like UPDATE/DELETE); `into` / `has_table_keyword` are surface
    // syntax with no effect on our AST.
    let sqlparser::ast::Insert {
        insert_token: _,
        optimizer_hint,
        or,
        ignore,
        into: _,
        table,
        table_alias,
        columns,
        overwrite,
        source,
        assignments,
        partitioned,
        after_columns,
        has_table_keyword: _,
        on,
        returning,
        replace_into,
        priority,
        insert_alias,
        settings,
        format_clause,
    } = insert;
    let (query_hints, hint_warnings) = optimizer_hint
        .as_ref()
        .map(|hint| parse_query_hints(&hint.text))
        .unwrap_or_default();
    let table = match table {
        TableObject::TableName(name) => object_name_to_string(name)?,
        _ => {
            return Ok(Statement::Unsupported {
                kind: "INSERT target".to_string(),
                sql: sql.to_string(),
            })
        }
    };
    let mut returning = convert_returning(returning.as_deref())?;
    let target_alias = table_alias
        .map(ident_value)
        .or_else(|| returning.as_mut().and_then(|r| r.target_alias.take()));
    if or.is_some()
        || overwrite
        || !after_columns.is_empty()
        || settings.is_some()
        || format_clause.is_some()
    {
        return Ok(Statement::Unsupported {
            kind: "INSERT feature".to_string(),
            sql: sql.to_string(),
        });
    }
    // S1 — `INSERT [LOW_PRIORITY | DELAYED | HIGH_PRIORITY] ...` priority
    // modifiers. InnoDB ignores all three; we mirror that for LOW/HIGH and
    // record DELAYED so dispatch can surface the deprecation warning 1287.
    let delayed = matches!(priority, Some(sqlparser::ast::MysqlInsertPriority::Delayed));
    // M5.C2 — `INSERT INTO t PARTITION (p0, p1) VALUES (...)`. sqlparser
    // surfaces the partition list as a `Vec<Expr>` of bare identifiers; we
    // only accept identifier names here (the MySQL syntax doesn't allow
    // expressions anyway), and a non-identifier produces a clear error
    // rather than silently dropping the clause.
    let insert_partitions: Vec<String> = match &partitioned {
        None => Vec::new(),
        Some(list) => {
            let mut out = Vec::with_capacity(list.len());
            for expr in list {
                match expr {
                    SqlExpr::Identifier(ident) => out.push(ident.value.clone()),
                    _ => {
                        return Ok(Statement::Unsupported {
                            kind: "INSERT PARTITION non-identifier".to_string(),
                            sql: sql.to_string(),
                        });
                    }
                }
            }
            out
        }
    };
    let insert_alias = insert_alias.map(convert_insert_alias).transpose()?;
    let on_duplicate_key_update = match on {
        None => None,
        Some(sqlparser::ast::OnInsert::DuplicateKeyUpdate(assignments)) => Some(
            assignments
                .iter()
                .map(convert_assignment)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        _ => {
            return Ok(Statement::Unsupported {
                kind: "INSERT ON CONFLICT".to_string(),
                sql: sql.to_string(),
            })
        }
    };
    // MySQL rejects `REPLACE ... ON DUPLICATE KEY UPDATE` at parse time (1064):
    // the two conflict strategies are mutually exclusive. Without this guard the
    // executor would silently pick the ODKU branch and ignore REPLACE, violating
    // the "never silently no-op unsupported SQL" rule.
    if replace_into && on_duplicate_key_update.is_some() {
        return Err(ParseError::at_zero(
            "You have an error in your SQL syntax; \
             REPLACE cannot be combined with ON DUPLICATE KEY UPDATE",
        ));
    }
    let mut columns = if columns.is_empty() {
        None
    } else {
        Some(columns.into_iter().map(ident_value).collect())
    };
    // C1 — a `WITH … SELECT` INSERT source carries CTE definitions. We lift
    // them to wrap the whole INSERT (`WITH … INSERT INTO t SELECT …`), which
    // is semantically identical (the CTE is only visible to the source query)
    // and reuses `execute_with_cte`'s registry so the streaming source
    // resolves the CTE through the thread-local registry. Captured here, the
    // wrapper is applied to the finished `Insert` at the end.
    let mut cte_wrapper: Option<(Vec<CteDef>, bool)> = None;
    let source = if !assignments.is_empty() {
        if source.is_some() || columns.is_some() {
            return Ok(Statement::Unsupported {
                kind: "INSERT SET with source or column list".to_string(),
                sql: sql.to_string(),
            });
        }
        let mut set_columns = Vec::with_capacity(assignments.len());
        let mut values = Vec::with_capacity(assignments.len());
        for assignment in &assignments {
            let SqlAssignmentTarget::ColumnName(name) = &assignment.target else {
                return Ok(Statement::Unsupported {
                    kind: "INSERT SET assignment target".to_string(),
                    sql: sql.to_string(),
                });
            };
            set_columns.push(assignment_column_name(name)?);
            values.push(fold_const_string_to_vector(convert_expr(
                &assignment.value,
            )?));
        }
        columns = Some(set_columns);
        InsertSource::Values(vec![values])
    } else {
        let Some(source) = source else {
            return Ok(Statement::Unsupported {
                kind: "INSERT source".to_string(),
                sql: sql.to_string(),
            });
        };
        match source.body.as_ref() {
            SetExpr::Values(values) => {
                // Direct INSERT VALUES has no query tail. Validate the entire
                // upstream Query before lowering its rows.
                if source.with.is_some()
                    || source.order_by.is_some()
                    || source.limit_clause.is_some()
                    || source.fetch.is_some()
                    || !source.locks.is_empty()
                    || source.for_clause.is_some()
                    || source.settings.is_some()
                    || source.format_clause.is_some()
                    || !source.pipe_operators.is_empty()
                {
                    return Err(parse_error_at_zero(
                        "query modifiers are not allowed after INSERT VALUES",
                    ));
                }
                let mut rows = Vec::with_capacity(values.rows.len());
                let mut saw_empty_row = false;
                for row in &values.rows {
                    // `VALUES ()` was rewritten to the `(__MYDB_EMPTY_ROW__)`
                    // marker (see `rewrite_empty_insert_rows`): an empty
                    // all-defaults row. Represent it as an empty value list so
                    // the executor fills every column with its default.
                    if row_is_empty_default_marker(row) {
                        saw_empty_row = true;
                        rows.push(Vec::new());
                    } else {
                        rows.push(
                            convert_expr_list(row)?
                                .into_iter()
                                .map(fold_const_string_to_vector)
                                .collect(),
                        );
                    }
                }
                // With no explicit column list, an empty row means "every
                // column defaults": force an explicit empty column list so the
                // executor's `insert_column_indexes` yields zero targets and
                // the default-fill path runs (rather than reporting 1136).
                if saw_empty_row && columns.is_none() {
                    columns = Some(Vec::new());
                }
                InsertSource::Values(rows)
            }
            _ => {
                let body = match convert_query(sql, *source)? {
                    Statement::Unsupported { kind, .. } => {
                        return Ok(Statement::Unsupported {
                            kind,
                            sql: sql.to_string(),
                        })
                    }
                    Statement::WithCte(WithCte {
                        ctes,
                        recursive,
                        body,
                    }) => {
                        cte_wrapper = Some((ctes, recursive));
                        *body
                    }
                    body => body,
                };
                let Some(source) = insert_source_from_query_body(body) else {
                    return Ok(Statement::Unsupported {
                        kind: "INSERT source query".to_string(),
                        sql: sql.to_string(),
                    });
                };
                source
            }
        }
    };
    let insert_stmt = Statement::Insert(Insert {
        table,
        target_alias,
        columns,
        source,
        view_check_predicate: None,
        on_duplicate_key_update,
        insert_alias,
        ignore,
        replace: replace_into,
        delayed,
        partitions: insert_partitions,
        query_hints,
        hint_warnings,
        returning,
    });
    match cte_wrapper {
        Some((ctes, recursive)) => Ok(Statement::WithCte(WithCte {
            ctes,
            recursive,
            body: Box::new(insert_stmt),
        })),
        None => Ok(insert_stmt),
    }
}

fn convert_insert_alias(alias: sqlparser::ast::InsertAliases) -> Result<InsertAlias, ParseError> {
    Ok(InsertAlias {
        row_alias: object_name_to_string(alias.row_alias)?,
        column_aliases: alias
            .col_aliases
            .unwrap_or_default()
            .into_iter()
            .map(ident_value)
            .collect(),
    })
}

/// Header conversion owns only mydb metadata. The upstream body may then be
/// moved (top-level DML) or borrowed (query-only nested positions).
struct QueryTail {
    order_by: Vec<OrderBy>,
    limit: Option<LimitValue>,
    offset: Option<LimitValue>,
    for_update: bool,
    lock_clause: Option<LockClause>,
}

struct QueryHeader {
    ctes: Option<(bool, Vec<CteDef>)>,
    tail: Result<QueryTail, Statement>,
}

enum PreparedQuery {
    Ready(QueryHeader),
    Unsupported(Statement),
}

impl PreparedQuery {
    fn finish(
        self,
        convert_body: impl FnOnce(QueryTail) -> Result<Statement, ParseError>,
    ) -> Result<Statement, ParseError> {
        let header = match self {
            Self::Ready(header) => header,
            Self::Unsupported(statement) => return Ok(statement),
        };
        let body = match header.tail {
            Ok(tail) => convert_body(tail)?,
            Err(unsupported) => unsupported,
        };
        Ok(match header.ctes {
            Some((recursive, ctes)) => Statement::WithCte(WithCte {
                ctes,
                recursive,
                body: Box::new(body),
            }),
            None => body,
        })
    }
}

fn convert_query(sql: &str, query: Query) -> Result<Statement, ParseError> {
    let prepared = prepare_query(sql, &query)?;
    prepared.finish(|tail| match *query.body {
        SetExpr::Update(statement) | SetExpr::Delete(statement) | SetExpr::Insert(statement) => {
            convert_statement(sql, statement)
        }
        SetExpr::Query(inner) => {
            let statement = convert_query(sql, *inner)?;
            apply_tail_modifiers(
                sql,
                statement,
                tail.order_by,
                tail.limit,
                tail.offset,
                tail.for_update,
                tail.lock_clause,
            )
        }
        body => convert_set_expr(
            sql,
            &body,
            tail.order_by,
            tail.limit,
            tail.offset,
            tail.for_update,
            tail.lock_clause,
        ),
    })
}

fn convert_query_ref(sql: &str, query: &Query) -> Result<Statement, ParseError> {
    prepare_query(sql, query)?.finish(|tail| {
        convert_set_expr(
            sql,
            &query.body,
            tail.order_by,
            tail.limit,
            tail.offset,
            tail.for_update,
            tail.lock_clause,
        )
    })
}

fn prepare_query(sql: &str, query: &Query) -> Result<PreparedQuery, ParseError> {
    let Query {
        with: _,
        body: _,
        order_by: _,
        limit_clause: _,
        fetch: _,
        locks: _,
        for_clause: _,
        settings: _,
        format_clause: _,
        pipe_operators: _,
    } = query;
    if query.for_clause.is_some()
        || query.fetch.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return Ok(PreparedQuery::Unsupported(Statement::Unsupported {
            kind: "SELECT query feature".to_string(),
            sql: sql.to_string(),
        }));
    }
    let ctes = if let Some(with) = &query.with {
        let recursive = with.recursive;
        let mut ctes = Vec::with_capacity(with.cte_tables.len());
        let mut names = std::collections::HashSet::new();
        for cte in &with.cte_tables {
            let sqlparser::ast::Cte {
                alias,
                query: body,
                from,
                materialized,
                closing_paren_token: _,
            } = cte;
            if from.is_some() || materialized.is_some() {
                return Ok(PreparedQuery::Unsupported(Statement::Unsupported {
                    kind: "CTE annotation".into(),
                    sql: sql.into(),
                }));
            }
            let name = alias.name.value.clone();
            if !names.insert(name.to_ascii_lowercase()) {
                let mut err = parse_error_at_zero(format!("Not unique table/alias: '{name}'"));
                err.spec = crate::error::catalog::ER_NONUNIQ_TABLE;
                return Err(err);
            }
            let body_stmt = convert_query_ref(sql, body)?;
            let columns = alias.columns.iter().map(|c| c.name.value.clone()).collect();
            ctes.push(CteDef {
                name,
                columns,
                is_recursive: recursive,
                query: QueryExpr::try_from(body_stmt)?,
            });
        }
        Some((recursive, ctes))
    } else {
        None
    };
    Ok(PreparedQuery::Ready(QueryHeader {
        ctes,
        tail: convert_query_tail(sql, query)?,
    }))
}

fn convert_query_tail(
    sql: &str,
    query: &Query,
) -> Result<Result<QueryTail, Statement>, ParseError> {
    let (limit, offset) = match convert_limit_clause(query.limit_clause.as_ref()) {
        Ok(limit) => limit,
        Err(LimitClauseError::Invalid(msg)) => {
            return Err(parse_error_full_span(sql, msg));
        }
        Err(LimitClauseError::Unsupported) => {
            return Ok(Err(Statement::Unsupported {
                kind: "LIMIT clause".to_string(),
                sql: sql.to_string(),
            }))
        }
    };
    let order_by = match query.order_by.as_ref().map(|order| &order.kind) {
        None => Vec::new(),
        Some(OrderByKind::Expressions(items)) => convert_order_by(items)?,
        Some(OrderByKind::All(_)) => {
            return Ok(Err(Statement::Unsupported {
                kind: "ORDER BY ALL".to_string(),
                sql: sql.to_string(),
            }))
        }
    };
    let lock_clause = parse_lock_clause(query)?;
    let for_update = lock_clause.is_some();
    Ok(Ok(QueryTail {
        order_by,
        limit,
        offset,
        for_update,
        lock_clause,
    }))
}
fn convert_set_expr(
    sql: &str,
    set_expr: &SetExpr,
    order_by: Vec<OrderBy>,
    limit: Option<LimitValue>,
    offset: Option<LimitValue>,
    for_update: bool,
    lock_clause: Option<LockClause>,
) -> Result<Statement, ParseError> {
    with_parser_stack(|| {
        convert_set_expr_checked(
            sql,
            set_expr,
            order_by,
            limit,
            offset,
            for_update,
            lock_clause,
        )
    })?
}

#[inline(never)]
fn convert_set_expr_checked(
    sql: &str,
    set_expr: &SetExpr,
    order_by: Vec<OrderBy>,
    limit: Option<LimitValue>,
    offset: Option<LimitValue>,
    for_update: bool,
    lock_clause: Option<LockClause>,
) -> Result<Statement, ParseError> {
    match set_expr {
        SetExpr::Select(select) => convert_select(
            sql,
            select,
            order_by,
            limit,
            offset,
            for_update,
            lock_clause,
        ),
        SetExpr::Query(query) => {
            // A parenthesised query nested inside another query body, e.g.
            // `(SELECT ... UNION ALL SELECT ...) ORDER BY b LIMIT 5`. sqlparser
            // represents the parentheses as `SetExpr::Query(inner)` and hangs
            // the trailing ORDER BY/LIMIT/OFFSET/lock clause on the *outer*
            // query — which arrives here as our `order_by`/`limit`/`offset`/
            // `for_update`/`lock_clause` arguments. Converting the inner query
            // alone would silently drop those tail modifiers (a correctness bug
            // for `LIMIT`, which would otherwise return all rows). Re-attach
            // them to the converted inner statement.
            let inner = convert_query_ref(sql, query)?;
            apply_tail_modifiers(sql, inner, order_by, limit, offset, for_update, lock_clause)
        }
        SetExpr::Values(values) => convert_values_query(
            sql,
            values,
            order_by,
            limit,
            offset,
            for_update,
            lock_clause,
        ),
        SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {
            let set_op = match op {
                SetOperator::Union => SetOp::Union,
                SetOperator::Intersect => SetOp::Intersect,
                SetOperator::Except | SetOperator::Minus => SetOp::Except,
            };
            let all = match set_quantifier {
                SetQuantifier::All => true,
                SetQuantifier::Distinct | SetQuantifier::None => false,
                SetQuantifier::ByName
                | SetQuantifier::AllByName
                | SetQuantifier::DistinctByName => {
                    return Err(parse_error_with_spec_at_zero(
                        crate::error::catalog::ER_NOT_SUPPORTED_YET,
                        "BY NAME set operations are not supported",
                    ));
                }
            };
            let left = convert_set_expr(
                sql,
                left,
                Vec::new(),
                None,
                None,
                for_update,
                lock_clause.clone(),
            )?;
            let right =
                convert_set_expr(sql, right, Vec::new(), None, None, for_update, lock_clause)?;
            Ok(Statement::SetOperation(SetOperation {
                op: set_op,
                all,
                left: QueryExpr::try_from(left)?,
                right: QueryExpr::try_from(right)?,
                order_by,
                limit,
                offset,
            }))
        }
        SetExpr::Update(_) | SetExpr::Delete(_) | SetExpr::Insert(_) => {
            Err(parse_error_at_zero("subquery must be SELECT"))
        }
        SetExpr::Table(table) => {
            let Some(name) = &table.table_name else {
                return Ok(Statement::Unsupported {
                    kind: "TABLE statement without a table name".into(),
                    sql: sql.into(),
                });
            };
            let name = match &table.schema_name {
                Some(schema) => format!("{schema}.{name}"),
                None => name.clone(),
            };
            let relation = TableRef {
                name,
                alias: None,
                system_time: None,
                index_hints: Vec::new(),
                partitions: Vec::new(),
                column_aliases: Vec::new(),
                generate_series: None,
            };
            let mut select = Select {
                projection: vec![SelectItem::Wildcard],
                ..Select::default()
            };
            if !is_dual_table(&relation) {
                select.table = Some(relation.name.clone());
                select.from.push(relation);
            }
            select.order_by = match convert_projection_order(sql, order_by, &select.projection)? {
                Ok(order) => order,
                Err(unsupported) => return Ok(unsupported),
            };
            for order in &mut select.order_by {
                resolve_window_inherits_in_expr(&mut order.expr, &[])
                    .map_err(parse_error_at_zero)?;
            }
            select.limit = limit;
            select.offset = offset;
            select.for_update = for_update;
            select.lock_clause = lock_clause;
            Ok(Statement::Select(select))
        }
        _ => Ok(Statement::Unsupported {
            kind: "query set operation".to_string(),
            sql: sql.to_string(),
        }),
    }
}

fn convert_values_query(
    sql: &str,
    values: &sqlparser::ast::Values,
    order_by: Vec<OrderBy>,
    limit: Option<LimitValue>,
    offset: Option<LimitValue>,
    for_update: bool,
    lock_clause: Option<LockClause>,
) -> Result<Statement, ParseError> {
    if for_update || lock_clause.is_some() {
        return Ok(Statement::Unsupported {
            kind: "VALUES lock clause".to_string(),
            sql: sql.to_string(),
        });
    }
    let mut rows = Vec::with_capacity(values.rows.len());
    let mut width: Option<usize> = None;
    for row in &values.rows {
        let converted = convert_expr_list(&row)?;
        if let Some(expected) = width {
            if converted.len() != expected {
                return Err(parse_error_at_zero(
                    "VALUES rows must have the same number of columns",
                ));
            }
        } else {
            width = Some(converted.len());
        }
        rows.push(converted);
    }
    let columns = (0..width.unwrap_or(0))
        .map(|idx| format!("column_{idx}"))
        .collect();
    Ok(Statement::Select(Select {
        projection: vec![SelectItem::Wildcard],
        values: Some(ValuesSource { rows, columns }),
        order_by,
        limit,
        offset,
        ..Select::default()
    }))
}

/// Preserve the active source for every borrowed query conversion. Keeping
/// Select unboxed here avoids an allocate/unbox/rebox cycle in scalar slots.
fn convert_query_node(query: &Query) -> Result<Statement, ParseError> {
    with_parser_source(|source| convert_query_ref(source, query))
        .unwrap_or_else(|| convert_query_ref(&adapter::display_sql(query), query))
}

fn convert_query_to_select(query: &Query) -> Result<Select, ParseError> {
    match convert_query_node(query)? {
        Statement::Select(select) => Ok(select),
        query => Ok(wrap_setop_as_derived_select(QueryExpr::try_from(query)?)),
    }
}

/// Query-only ownership for FROM, CTE and other structured query slots.
pub(super) fn convert_query_to_subquery(query: &Query) -> Result<SubQuery, ParseError> {
    QueryExpr::try_from(convert_query_node(query)?)
}

fn convert_order_by(items: &[sqlparser::ast::OrderByExpr]) -> Result<Vec<OrderBy>, ParseError> {
    items
        .iter()
        .map(|item| {
            Ok(OrderBy {
                expr: convert_expr(&item.expr)?,
                asc: item.options.asc.unwrap_or(true),
                nulls_first: item.options.nulls_first,
            })
        })
        .collect()
}

fn convert_projection_order(
    sql: &str,
    order_by: Vec<OrderBy>,
    projection: &[SelectItem],
) -> Result<Result<Vec<OrderBy>, Statement>, ParseError> {
    match resolve_order_by_refs(order_by, projection) {
        Ok(order_by) => Ok(Ok(order_by)),
        Err(SelectListRefError::Allocation(error)) => Err(error),
        Err(SelectListRefError::OutOfRange(pos)) => {
            return Err(ParseError {
                spec: crate::error::catalog::ER_BAD_FIELD_ERROR,
                message: format!("Unknown column '{pos}' in 'order clause'"),
                span: Span { start: 0, end: 0 },
                line: 1,
                column: 1,
                expected: Vec::new(),
            });
        }
        Err(SelectListRefError::Wildcard(pos)) => {
            return Ok(Err(Statement::Unsupported {
                kind: format!("ORDER BY position {pos} with wildcard select list"),
                sql: sql.to_string(),
            }));
        }
    }
}

fn convert_select(
    sql: &str,
    select: &SqlSelect,
    order_by: Vec<OrderBy>,
    limit: Option<LimitValue>,
    offset: Option<LimitValue>,
    for_update: bool,
    lock_clause: Option<LockClause>,
) -> Result<Statement, ParseError> {
    // Set-operation siblings release the guard between branches; only query
    // blocks nested through subqueries and derived tables accumulate.
    let _nesting = RecursionGuard::enter(RecursionKind::Select)?;
    let SqlSelect {
        select_token: _,
        optimizer_hint: _,
        distinct: _,
        select_modifiers: _,
        top: _,
        top_before_distinct: _,
        projection: _,
        exclude: _,
        into: _,
        from: _,
        lateral_views: _,
        prewhere: _,
        selection: _,
        connect_by: _,
        group_by: _,
        cluster_by: _,
        distribute_by: _,
        sort_by: _,
        having: _,
        named_window: _,
        qualify: _,
        window_before_qualify: _,
        value_table_mode: _,
        flavor: _,
    } = select;
    if !select.connect_by.is_empty()
        || select.exclude.is_some()
        || select.top.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
    {
        return Ok(Statement::Unsupported {
            kind: "SELECT feature".to_string(),
            sql: sql.to_string(),
        });
    }
    if matches!(select.distinct, Some(sqlparser::ast::Distinct::On(_))) {
        return Err(parse_error_at_zero("DISTINCT ON is not supported"));
    }
    let named_windows = convert_named_windows(&select.named_window)?;
    let (table, from, derived, joins) = if select.from.is_empty() {
        (None, Vec::new(), None, Vec::new())
    } else {
        // First FROM item becomes the primary table; any additional
        // comma-separated items become implicit CROSS JOINs. This is the
        // SQL-standard rewrite and matches MySQL's behaviour. The WHERE
        // clause naturally provides the join predicates after the rewrite.
        // Required for unmodified TPC-H queries (Q2, Q5, Q7-Q11, Q15).
        let (base, derived_first, _base_lateral, mut joins) =
            match convert_table_with_joins(&select.from[0]) {
                Ok(t) => t,
                Err(error) => return error.into_statement_result(sql),
            };
        if select.from.len() == 1
            && derived_first.is_none()
            && joins.is_empty()
            && is_dual_table(&base)
        {
            (None, Vec::new(), None, Vec::new())
        } else {
            let primary_name = base.name.clone();
            for extra in &select.from[1..] {
                let (extra_base, extra_derived, extra_lateral, extra_joins) =
                    match convert_table_with_joins(extra) {
                        Ok(t) => t,
                        Err(error) => return error.into_statement_result(sql),
                    };
                // A comma-separated derived table (`FROM t, (SELECT ...) AS s`)
                // is rewritten as a CROSS JOIN whose right side carries the
                // derived subquery. The CTE-join path materialises both sides
                // and performs the join in memory.
                joins.push(Join {
                    table: extra_base,
                    join_type: JoinType::Cross,
                    condition: None,
                    using_columns: Vec::new(),
                    natural: false,
                    derived: extra_derived,
                    straight: false,
                    lateral: extra_lateral,
                });
                joins.extend(extra_joins);
            }
            // Preserve the existing convention that `from` holds only the
            // primary table (other tables live in `joins`); downstream code
            // such as the simple fast path and statistics paths rely on this.
            (Some(primary_name), vec![base], derived_first, joins)
        }
    };
    let mut projection = select
        .projection
        .iter()
        .map(convert_select_item)
        .collect::<Result<Vec<_>, ParseError>>()?;
    // P2-C1: MySQL names an unaliased expression column by its original SQL
    // text (`SELECT 1+1` → `1+1`, `COUNT(*)` → `COUNT(*)`). `expr_name`'s
    // generic fallback returns the placeholder "expr" for compound
    // expressions; for exactly those, recover the real name from the source
    // span and store it as a synthesised alias. Plain columns and literals
    // (which `expr_name` already names correctly, preserving column source
    // tracking) are left untouched.
    for (internal, sql_item) in projection.iter_mut().zip(select.projection.iter()) {
        if let SelectItem::Expr { expr, alias } = internal {
            if alias.is_none()
                && crate::engine::expr::expr_name(expr) == "expr"
                && !parser_fragment_mode()
            {
                if let SqlSelectItem::UnnamedExpr(sql_expr) = sql_item {
                    // sqlparser's specialised TRIM span starts at the inner
                    // trim operand (often the default/removal string), not at
                    // `TRIM(`. Its Display implementation retains the full
                    // direction/operand expression, so use that canonical
                    // spelling rather than letting a depth-zero FROM truncate
                    // every form to the same `' '` column name.
                    if matches!(sql_expr, SqlExpr::Trim { .. } | SqlExpr::Extract { .. }) {
                        *alias = Some(adapter::display_sql(sql_expr));
                    } else if let Some(raw) = capture_select_item_text(sql, sql_expr.span().start)?
                    {
                        *alias = Some(raw);
                    }
                }
            }
        }
    }
    let order_by = match convert_projection_order(sql, order_by, &projection)? {
        Ok(order) => order,
        Err(unsupported) => return Ok(unsupported),
    };
    let resolved_named =
        resolve_named_windows(named_windows).map_err(|msg| parse_error_at_zero(msg))?;
    let mut projection = projection;
    // Aggregate ownership is query-block scoped and must be decided after
    // binding.  Rejecting it here loses both scope and MySQL's argument/type
    // validation order (for example a conversion error inside a scalar
    // subquery predicate may precede aggregate placement diagnostics).
    let mut selection = select.selection.as_ref().map(convert_expr).transpose()?;
    let mut having = select.having.as_ref().map(convert_expr).transpose()?;
    let mut order_by = order_by;
    for item in &mut projection {
        if let SelectItem::Expr { expr, .. } = item {
            resolve_window_inherits_in_expr(expr, &resolved_named)
                .map_err(|msg| parse_error_at_zero(msg))?;
        }
    }
    if let Some(sel) = selection.as_mut() {
        resolve_window_inherits_in_expr(sel, &resolved_named)
            .map_err(|msg| parse_error_at_zero(msg))?;
    }
    if let Some(h) = having.as_mut() {
        resolve_window_inherits_in_expr(h, &resolved_named)
            .map_err(|msg| parse_error_at_zero(msg))?;
    }
    for o in &mut order_by {
        resolve_window_inherits_in_expr(&mut o.expr, &resolved_named)
            .map_err(|msg| parse_error_at_zero(msg))?;
    }
    let (query_hints, hint_warnings) = select
        .optimizer_hint
        .as_ref()
        .map(|h| parse_query_hints(&h.text))
        .unwrap_or_default();
    let (mut group_by, grouping_sets, group_by_modifier) = convert_group_by(&select.group_by)?;
    match resolve_group_by_ordinals(&mut group_by, &projection) {
        Ok(()) => {}
        Err(GroupByRefError::Ref(SelectListRefError::OutOfRange(pos))) => {
            return Err(ParseError {
                spec: crate::error::catalog::ER_BAD_FIELD_ERROR,
                message: format!("Unknown column '{pos}' in 'group statement'"),
                span: Span { start: 0, end: 0 },
                line: 1,
                column: 1,
                expected: Vec::new(),
            });
        }
        Err(GroupByRefError::Ref(SelectListRefError::Wildcard(_))) => {}
        Err(GroupByRefError::Ref(SelectListRefError::Allocation(error))) => return Err(error),
        Err(GroupByRefError::Aggregate(name)) => {
            return Err(ParseError {
                spec: crate::error::catalog::ER_WRONG_GROUP_FIELD,
                message: format!("Can't group on '{name}'"),
                span: Span { start: 0, end: 0 },
                line: 1,
                column: 1,
                expected: Vec::new(),
            });
        }
    }
    Ok(Statement::Select(Select {
        projection,
        table,
        from,
        values: None,
        json_table: None,
        derived,
        no_merge: false,
        straight_join: select
            .select_modifiers
            .as_ref()
            .is_some_and(|modifiers| modifiers.straight_join),
        joins,
        selection,
        group_by,
        grouping_sets,
        group_by_modifier,
        having,
        order_by,
        limit,
        offset,
        distinct: select.distinct.is_some(),
        for_update,
        lock_clause,
        sql_calc_found_rows: select
            .select_modifiers
            .as_ref()
            .map(|m| m.sql_calc_found_rows)
            .unwrap_or(false),
        named_windows: resolved_named,
        query_hints,
        hint_warnings,
    }))
}

/// Both WINDOW declarations and inline OVER clauses carry the same fields.
struct ConvertedWindowSpec {
    partition_by: Vec<Expr>,
    order_by: Vec<OrderBy>,
    frame: Option<WindowFrame>,
    inherits: Option<String>,
}

fn convert_window_spec(
    spec: &sqlparser::ast::WindowSpec,
) -> Result<ConvertedWindowSpec, ParseError> {
    Ok(ConvertedWindowSpec {
        partition_by: convert_expr_list(&spec.partition_by)?,
        order_by: convert_order_by(&spec.order_by)?,
        frame: spec
            .window_frame
            .as_ref()
            .map(convert_window_frame)
            .transpose()?,
        inherits: spec.window_name.as_ref().map(|name| name.value.clone()),
    })
}

fn convert_named_windows(
    items: &[sqlparser::ast::NamedWindowDefinition],
) -> Result<Vec<NamedWindowDef>, ParseError> {
    use sqlparser::ast::NamedWindowExpr;
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let name = item.0.value.clone();
        match &item.1 {
            NamedWindowExpr::NamedWindow(other) => {
                out.push(NamedWindowDef {
                    name,
                    partition_by: Vec::new(),
                    order_by: Vec::new(),
                    frame: None,
                    inherits: Some(other.value.clone()),
                });
            }
            NamedWindowExpr::WindowSpec(spec) => {
                let ConvertedWindowSpec {
                    partition_by,
                    order_by,
                    frame,
                    inherits,
                } = convert_window_spec(spec)?;
                out.push(NamedWindowDef {
                    name,
                    partition_by,
                    order_by,
                    frame,
                    inherits,
                });
            }
        }
    }
    Ok(out)
}

fn convert_window_frame(frame: &sqlparser::ast::WindowFrame) -> Result<WindowFrame, ParseError> {
    use sqlparser::ast::WindowFrameUnits as SqlFrameUnits;
    let units = match frame.units {
        SqlFrameUnits::Rows => WindowFrameUnits::Rows,
        SqlFrameUnits::Range => WindowFrameUnits::Range,
        SqlFrameUnits::Groups => WindowFrameUnits::Groups,
    };
    let start = convert_window_frame_bound(&frame.start_bound)?;
    let end = match &frame.end_bound {
        Some(b) => convert_window_frame_bound(b)?,
        None => WindowFrameBound::CurrentRow,
    };
    Ok(WindowFrame {
        units,
        start,
        end,
        exclusion: WindowFrameExclusion::NoOthers,
    })
}

fn convert_window_frame_bound(
    bound: &sqlparser::ast::WindowFrameBound,
) -> Result<WindowFrameBound, ParseError> {
    use sqlparser::ast::WindowFrameBound as SqlBound;
    Ok(match bound {
        SqlBound::CurrentRow => WindowFrameBound::CurrentRow,
        SqlBound::Preceding(None) => WindowFrameBound::UnboundedPreceding,
        SqlBound::Following(None) => WindowFrameBound::UnboundedFollowing,
        SqlBound::Preceding(Some(expr)) => {
            WindowFrameBound::Preceding(Box::new(convert_expr(expr)?))
        }
        SqlBound::Following(Some(expr)) => {
            WindowFrameBound::Following(Box::new(convert_expr(expr)?))
        }
    })
}

fn convert_truncate(
    sql: &str,
    truncate: sqlparser::ast::Truncate,
) -> Result<Statement, ParseError> {
    // Irrefutable destructure so any modifier a future sqlparser adds surfaces
    // at compile time rather than being silently dropped (cf. the `insert.priority`
    // lesson). MySQL 8.0's grammar is only `TRUNCATE [TABLE] tbl_name` — every
    // other clause below is a non-MySQL dialect extension and must be rejected
    // explicitly, never treated as a plain TRUNCATE (no silent no-op).
    let sqlparser::ast::Truncate {
        table_names,
        partitions,
        table: _table_keyword,
        if_exists,
        identity,
        cascade,
        on_cluster,
    } = truncate;
    let unsupported = |kind: &str| {
        Ok(Statement::Unsupported {
            kind: format!("TRUNCATE ... {kind}"),
            sql: sql.to_string(),
        })
    };
    if if_exists {
        return unsupported("IF EXISTS");
    }
    if identity.is_some() {
        // PostgreSQL RESTART/CONTINUE IDENTITY — MySQL always resets the counter.
        return unsupported("{RESTART|CONTINUE} IDENTITY");
    }
    if cascade.is_some() {
        // PostgreSQL CASCADE/RESTRICT — MySQL rejects truncating an FK-referenced
        // table (error 1701) rather than cascading.
        return unsupported("{CASCADE|RESTRICT}");
    }
    if on_cluster.is_some() {
        return unsupported("ON CLUSTER");
    }
    if partitions.is_some() {
        // MySQL prunes partitions via `ALTER TABLE ... TRUNCATE PARTITION`, not
        // `TRUNCATE ... PARTITION (...)`.
        return unsupported("PARTITION (use ALTER TABLE ... TRUNCATE PARTITION)");
    }
    if table_names.len() != 1 {
        return unsupported("multiple tables");
    }
    let sqlparser::ast::TruncateTableTarget {
        name,
        only,
        has_asterisk,
    } = table_names
        .into_iter()
        .next()
        .expect("length checked above");
    if only {
        // PostgreSQL `TRUNCATE ONLY t` (no-descendant-tables) — MySQL has no
        // inheritance so no such qualifier.
        return unsupported("ONLY");
    }
    if has_asterisk {
        // PostgreSQL `TRUNCATE t*` (include descendants).
        return unsupported("table-name `*`");
    }
    Ok(Statement::TruncateTable {
        name: object_name_to_string(name)?,
    })
}

fn convert_update(sql: &str, update: sqlparser::ast::Update) -> Result<Statement, ParseError> {
    // Irrefutable full-field destructure (M5.18): a future sqlparser upgrade
    // that adds an UPDATE field then fails to compile here instead of silently
    // dropping it — the `insert.priority` lesson. `optimizer_hint`
    // (`UPDATE /*+ ... */ ...`) is a pure performance hint in MySQL: it steers
    // plan choice but never changes the result. mydb's UPDATE executor doesn't
    // act on it yet, so we parse it for validation and accept-and-ignore,
    // mirroring LOW_PRIORITY — no longer a silent bare-field skip (B4).
    let sqlparser::ast::Update {
        update_token: _,
        optimizer_hint,
        table,
        assignments,
        from,
        selection,
        returning,
        or,
        limit,
    } = update;
    let (query_hints, hint_warnings) = optimizer_hint
        .as_ref()
        .map(|hint| parse_query_hints(&hint.text))
        .unwrap_or_default();
    if from.is_some() || or.is_some() {
        return Ok(Statement::Unsupported {
            kind: "UPDATE feature".to_string(),
            sql: sql.to_string(),
        });
    }
    let (target, joins) = if table.joins.is_empty() {
        let table = match convert_dml_target(&table) {
            Ok(table) => table,
            Err(kind) => {
                return Ok(Statement::Unsupported {
                    kind,
                    sql: sql.to_string(),
                })
            }
        };
        (table, Vec::new())
    } else {
        let (base_ref, base_derived, _base_lateral, joins) = match convert_table_with_joins(&table)
        {
            Ok(parts) => parts,
            Err(error) => return error.into_statement_result(sql),
        };
        if base_derived.is_some() {
            return Ok(Statement::Unsupported {
                kind: "UPDATE derived table".to_string(),
                sql: sql.to_string(),
            });
        }
        if let Some(kind) = unsupported_dml_join(DmlKind::Update, &base_ref, &joins) {
            return Ok(Statement::Unsupported {
                kind: kind.to_owned(),
                sql: sql.to_owned(),
            });
        }
        let plain_target = matches!(table.relation, TableFactor::Table { .. });
        let target = DmlTarget {
            name: base_ref.name,
            alias: if plain_target { base_ref.alias } else { None },
            partitions: if plain_target {
                base_ref.partitions
            } else {
                Vec::new()
            },
        };
        (target, joins)
    };
    let mut converted_assignments = Vec::with_capacity(assignments.len());
    for assignment in assignments {
        let SqlAssignmentTarget::ColumnName(name) = assignment.target else {
            return Ok(Statement::Unsupported {
                kind: "UPDATE assignment target".to_string(),
                sql: sql.to_string(),
            });
        };
        let (column, table_qualifier) = assignment_column_with_qualifier(name)?;
        converted_assignments.push(Assignment {
            column,
            value: convert_expr(&assignment.value)?,
            table_qualifier,
        });
    }
    Ok(Statement::Update(Update {
        table: target.name,
        assignments: converted_assignments,
        selection: selection.as_ref().map(convert_expr).transpose()?,
        view_check_predicate: None,
        order_by: Vec::new(),
        limit: convert_optional_limit(sql, limit.as_ref())?,
        ignore: false,
        low_priority: false,
        primary_alias: target.alias,
        joins,
        partitions: target.partitions,
        query_hints,
        hint_warnings,
        returning: convert_returning(returning.as_deref())?,
    }))
}

fn convert_delete(sql: &str, delete: sqlparser::ast::Delete) -> Result<Statement, ParseError> {
    // Irrefutable full-field destructure (M5.18): a future sqlparser upgrade
    // that adds a DELETE field then fails to compile here instead of silently
    // dropping it. `optimizer_hint` (`DELETE /*+ ... */ ...`) is a pure perf
    // hint in MySQL — parse-for-validation and accept-and-ignore, no longer a
    // silent bare-field skip (B5).
    let sqlparser::ast::Delete {
        delete_token: _,
        optimizer_hint,
        tables,
        from,
        using,
        selection,
        returning,
        order_by,
        limit,
    } = delete;
    let (query_hints, hint_warnings) = optimizer_hint
        .as_ref()
        .map(|hint| parse_query_hints(&hint.text))
        .unwrap_or_default();
    let returning = convert_returning(returning.as_deref())?;
    // Two multi-table DELETE shapes reduce to the same internal form:
    //   Form A:  DELETE t1, t2 FROM table_refs [WHERE ...]
    //   Form B:  DELETE FROM t1, t2 USING table_refs [WHERE ...]
    // sqlparser records the target list in `delete.tables` for form A but in
    // `delete.from` for form B (with the join tree parked in `delete.using`).
    // Normalise both into (targets, table_refs) and feed the shared lowering.
    let (target_names, table_refs, using_form): (Vec<String>, &[TableWithJoins], bool) =
        if let Some(using) = using.as_ref() {
            // Form B: `from` holds the target tables; `using` holds refs.
            let from_targets = match &from {
                sqlparser::ast::FromTable::WithFromKeyword(tables)
                | sqlparser::ast::FromTable::WithoutKeyword(tables) => tables,
            };
            let mut targets = Vec::with_capacity(from_targets.len());
            for target in from_targets {
                match delete_target_name(target) {
                    Ok(name) => targets.push(name),
                    Err(kind) => {
                        return Ok(Statement::Unsupported {
                            kind,
                            sql: sql.to_string(),
                        })
                    }
                }
            }
            (targets, using.as_slice(), true)
        } else {
            let from = match &from {
                sqlparser::ast::FromTable::WithFromKeyword(tables)
                | sqlparser::ast::FromTable::WithoutKeyword(tables) => tables,
            };
            let targets = tables
                .iter()
                .map(|name| object_name_to_string(name))
                .collect::<Result<Vec<_>, _>>()?;
            (targets, from.as_slice(), false)
        };
    if table_refs.is_empty() {
        return Ok(Statement::Unsupported {
            kind: "DELETE target".to_string(),
            sql: sql.to_string(),
        });
    }
    if !using_form
        && target_names.is_empty()
        && (table_refs.len() != 1 || table_refs.iter().any(|table| !table.joins.is_empty()))
    {
        return Err(ParseError::at_zero(
            "You have an error in your SQL syntax; multi-table DELETE requires an explicit target list",
        ));
    }
    // Multi-table when: an explicit target list is present (form A's `t1, t2`
    // or any USING form), more than one ref, or the single ref carries joins.
    // Single-table DELETE shape:
    //   DELETE FROM t [WHERE ...] [ORDER BY ... LIMIT ...]
    let is_multi_table = using_form
        || !target_names.is_empty()
        || table_refs.len() > 1
        || table_refs
            .first()
            .map(|t| !t.joins.is_empty())
            .unwrap_or(false);
    let (table, primary_alias, joins, targets, partitions) = if is_multi_table {
        // Combine `FROM t1, t2 JOIN t3 ON ...` into one (base, joins) list. The
        // executor treats comma joins as CROSS joins and relies on the WHERE
        // clause to provide the join condition.
        let (mut base_ref, base_derived, _base_lateral, mut joins) =
            match convert_table_with_joins(&table_refs[0]) {
                Ok(parts) => parts,
                Err(error) => return error.into_statement_result(sql),
            };
        if base_derived.is_some() {
            return Ok(Statement::Unsupported {
                kind: "DELETE derived table".to_string(),
                sql: sql.to_string(),
            });
        }
        for additional in &table_refs[1..] {
            let (extra_ref, extra_derived, _extra_lateral, extra_joins) =
                match convert_table_with_joins(additional) {
                    Ok(parts) => parts,
                    Err(error) => return error.into_statement_result(sql),
                };
            if extra_derived.is_some() {
                return Ok(Statement::Unsupported {
                    kind: "DELETE derived table".to_string(),
                    sql: sql.to_string(),
                });
            }
            joins.push(Join {
                table: extra_ref,
                join_type: JoinType::Cross,
                condition: None,
                using_columns: Vec::new(),
                natural: false,
                derived: None,
                straight: false,
                lateral: false,
            });
            joins.extend(extra_joins);
        }
        if let Some(kind) = unsupported_dml_join(DmlKind::Delete, &base_ref, &joins) {
            return Ok(Statement::Unsupported {
                kind: kind.to_owned(),
                sql: sql.to_owned(),
            });
        }
        let primary_alias = base_ref.alias.take();
        let partitions = std::mem::take(&mut base_ref.partitions);
        (
            base_ref.name,
            primary_alias,
            joins,
            target_names,
            partitions,
        )
    } else {
        let target = match convert_dml_target(&table_refs[0]) {
            Ok(table) => table,
            Err(kind) => {
                return Ok(Statement::Unsupported {
                    kind,
                    sql: sql.to_string(),
                })
            }
        };
        (
            target.name,
            target.alias,
            Vec::new(),
            Vec::new(),
            target.partitions,
        )
    };
    Ok(Statement::Delete(Delete {
        table,
        selection: selection.as_ref().map(convert_expr).transpose()?,
        order_by: convert_order_by(&order_by)?,
        limit: convert_optional_limit(sql, limit.as_ref())?,
        primary_alias,
        joins,
        targets,
        partitions,
        low_priority: false,
        quick: false,
        ignore: false,
        query_hints,
        hint_warnings,
        returning,
    }))
}

pub(super) fn convert_returning(
    returning: Option<&[SqlSelectItem]>,
) -> Result<Option<ReturningClause>, ParseError> {
    let Some(items) = returning else {
        return Ok(None);
    };
    if items.is_empty() {
        return Err(parse_error_at_zero(
            "RETURNING requires an output expression",
        ));
    }
    let mut old_alias = None;
    let mut new_alias = None;
    let mut target_alias = None;
    let mut projection = Vec::with_capacity(items.len());
    for item in items {
        if let SqlSelectItem::ExprWithAlias { alias, .. } = item {
            let slot_and_name = match extensions::get(&alias.value) {
                Some(extensions::Extension::ReturningOld(name)) => Some((&mut old_alias, name)),
                Some(extensions::Extension::ReturningNew(name)) => Some((&mut new_alias, name)),
                Some(extensions::Extension::ReturningTarget(name)) => {
                    Some((&mut target_alias, name))
                }
                _ => None,
            };
            if let Some((slot, name)) = slot_and_name {
                if slot.replace(name).is_some() {
                    return Err(parse_error_at_zero("duplicate RETURNING alias"));
                }
                continue;
            }
        }
        projection.push(convert_select_item(item)?);
    }
    if projection.is_empty() {
        return Err(parse_error_at_zero(
            "RETURNING requires an output expression",
        ));
    }
    if old_alias
        .as_ref()
        .zip(new_alias.as_ref())
        .is_some_and(|(a, b)| a.eq_ignore_ascii_case(b))
    {
        return Err(parse_error_at_zero(
            "OLD and NEW aliases in RETURNING WITH must be distinct",
        ));
    }
    // sqlparser's MySqlDialect currently tokenizes `alias.*` in a RETURNING
    // list as the bare identifier `alias`. In this namespace a bare OLD/NEW
    // row alias is not a scalar column, so restore the qualified wildcard.
    for item in &mut projection {
        if let SelectItem::Expr {
            expr: Expr::Identifier(name),
            alias: None,
        } = item
        {
            let is_row_alias = name.eq_ignore_ascii_case("old")
                || name.eq_ignore_ascii_case("new")
                || old_alias
                    .as_ref()
                    .is_some_and(|a| name.eq_ignore_ascii_case(a))
                || new_alias
                    .as_ref()
                    .is_some_and(|a| name.eq_ignore_ascii_case(a));
            if is_row_alias {
                *item = SelectItem::QualifiedWildcard(name.clone());
            }
        }
    }
    Ok(Some(ReturningClause {
        projection,
        old_alias,
        new_alias,
        target_alias,
    }))
}

fn convert_optional_limit(
    sql: &str,
    limit: Option<&SqlExpr>,
) -> Result<Option<LimitValue>, ParseError> {
    match limit {
        Some(limit) => convert_limit_value(limit).map(Some).map_err(|err| {
            let msg = match err {
                LimitValueError::Invalid(msg) => msg,
                LimitValueError::Unsupported => "LIMIT value".to_string(),
            };
            parse_error_full_span(sql, msg)
        }),
        None => Ok(None),
    }
}

fn convert_column_def_with_comment(
    sql: &str,
    column: sqlparser::ast::ColumnDef,
) -> Result<(ColumnDef, Option<String>), ParseError> {
    convert_column_parts(sql, column.name, column.data_type, column.options)
}

fn convert_column_parts(
    sql: &str,
    name: sqlparser::ast::Ident,
    mut data_type: SqlDataType,
    options: impl IntoIterator<Item = sqlparser::ast::ColumnOptionDef>,
) -> Result<(ColumnDef, Option<String>), ParseError> {
    if name.quote_style.is_none()
        && (name.value.eq_ignore_ascii_case("MOD") || name.value.eq_ignore_ascii_case("BOTH"))
    {
        return Err(parse_error_at_zero(
            "reserved column names MOD and BOTH must be quoted",
        ));
    }
    let long_alias = matches!(&data_type, SqlDataType::Custom(name, modifiers)
        if name.to_string().eq_ignore_ascii_case("LONG") && modifiers.is_empty());
    let year_alias = matches!(&data_type, SqlDataType::Custom(name, _)
        if name.to_string().eq_ignore_ascii_case("YEAR"));
    let national_prefix = matches!(&data_type, SqlDataType::Custom(name, modifiers)
        if modifiers.is_empty() && (name.to_string().eq_ignore_ascii_case("NATIONAL") || name.to_string().eq_ignore_ascii_case("NCHAR")));
    let national = matches!(&data_type, SqlDataType::Nvarchar(_))
        || matches!(&data_type, SqlDataType::Custom(name, _)
        if name.to_string().eq_ignore_ascii_case("NATIONAL") || name.to_string().eq_ignore_ascii_case("NCHAR"));
    let mut long_byte = false;
    let mut zerofill = false;
    let mut type_suffixes = 0usize;
    let mut precision_suffix = false;
    let mut signed_suffix = None;
    let mut signed_requested = false;
    let column_name = ident_value(name);
    let mut nullable = true;
    let mut primary_key = false;
    let mut unique = false;
    let mut auto_increment = false;
    let mut default = None;
    let mut on_update = None;
    let mut checks = Vec::new();
    let mut generated = None;
    let mut charset = national.then(|| "utf8mb3".to_owned());
    let mut collation = national.then(|| "utf8mb3_general_ci".to_owned());
    let mut comment = None;
    let mut invisible = false;
    let mut srid = None;
    for (option_index, option) in options.into_iter().enumerate() {
        match option.option {
            ColumnOption::Null => nullable = true,
            ColumnOption::NotNull => nullable = false,
            // MySQL 8.0.13+: `DEFAULT (<expr>)` (parenthesised) means evaluate
            // the expression at INSERT / ALTER ADD COLUMN backfill time, not
            // store a literal. Wrap the inner Expr in a sentinel function so
            // downstream layers (column_def_to_schema) can tell it apart from
            // plain `DEFAULT 42`. Inner literal default stays as-is.
            ColumnOption::Default(expr) => {
                if let SqlExpr::Nested(inner) = &expr {
                    default = Some(Expr::Function {
                        name: DEFAULT_EXPR_SENTINEL.to_string(),
                        args: vec![convert_expr(inner)?],
                        distinct: false,
                    });
                } else {
                    default = Some(convert_expr(&expr)?);
                }
            }
            ColumnOption::PrimaryKey(_) => {
                if primary_key {
                    return Err(parse_error_with_spec_at_zero(
                        crate::error::catalog::ER_MULTIPLE_PRI_KEY,
                        "Multiple primary key defined",
                    ));
                }
                primary_key = true;
                nullable = false;
            }
            ColumnOption::Unique(_) => unique = true,
            ColumnOption::DialectSpecific(tokens) => {
                if let Some(suffix) = super::adapter::decode_column_type_suffix(&tokens)? {
                    use super::adapter::TypeSuffix;
                    if matches!(suffix, TypeSuffix::Zerofill) {
                        zerofill = true;
                        // Retain support for a following signedness suffix when
                        // ZEROFILL is adjacent to the type.
                        if option_index == type_suffixes {
                            type_suffixes += 1;
                        }
                        continue;
                    }
                    if option_index != type_suffixes {
                        return Err(parse_error_at_zero(
                            "type suffix must precede column options",
                        ));
                    }
                    match suffix {
                        TypeSuffix::National {
                            initial,
                            varying,
                            length,
                        } if national_prefix && type_suffixes == 0 => {
                            let prefix_is_nchar = matches!(&data_type, SqlDataType::Custom(name, _) if name.to_string().eq_ignore_ascii_case("NCHAR"));
                            if (prefix_is_nchar
                                && !matches!(
                                    initial,
                                    sqlparser::keywords::Keyword::VARCHAR
                                        | sqlparser::keywords::Keyword::VARYING
                                ))
                                || (!prefix_is_nchar
                                    && initial == sqlparser::keywords::Keyword::VARYING)
                            {
                                return Err(parse_error_at_zero(
                                    "invalid national character type suffix",
                                ));
                            }
                            let length = length.map(|length| CharacterLength::IntegerLength {
                                length,
                                unit: None,
                            });
                            data_type = if varying {
                                SqlDataType::Varchar(length)
                            } else {
                                SqlDataType::Char(length)
                            };
                        }
                        TypeSuffix::Byte if long_alias && type_suffixes == 0 => long_byte = true,
                        TypeSuffix::Precision(info) if type_suffixes == 0 => {
                            data_type = match data_type {
                                SqlDataType::Real
                                    if super::entry::current_parser_sql_mode_bits()
                                        & crate::engine::api::SQL_MODE_REAL_AS_FLOAT
                                        != 0 =>
                                {
                                    SqlDataType::Float(info)
                                }
                                SqlDataType::Real | SqlDataType::DoublePrecision => {
                                    SqlDataType::Double(info)
                                }
                                _ => {
                                    return Err(parse_error_at_zero(
                                        "unexpected column type precision",
                                    ))
                                }
                            };
                            precision_suffix = true;
                        }
                        TypeSuffix::Unsigned
                            if signed_suffix.is_none()
                                && (year_alias
                                    || precision_suffix
                                    || (zerofill && !is_unsigned_data_type(&data_type))) =>
                        {
                            signed_suffix = Some(!year_alias)
                        }
                        TypeSuffix::Signed
                            if signed_suffix.is_none() && !is_unsigned_data_type(&data_type) =>
                        {
                            signed_requested = true;
                            signed_suffix = Some(false);
                        }
                        _ => return Err(parse_error_at_zero("unexpected column type suffix")),
                    }
                    type_suffixes += 1;
                    continue;
                }
                if tokens
                    .iter()
                    .any(|token| token.to_string().eq_ignore_ascii_case("AUTO_INCREMENT"))
                {
                    auto_increment = true;
                    nullable = false;
                }
            }
            ColumnOption::OnUpdate(expr) => {
                on_update = Some(convert_expr(&expr)?);
            }
            generated_option @ ColumnOption::Generated { .. } => {
                generated = Some(convert_generated_column_option(sql, generated_option)?);
            }
            ColumnOption::Check(mut constraint) => {
                if constraint.name.is_none() {
                    constraint.name = option.name;
                }
                checks.push(convert_check_constraint(
                    constraint,
                    Some(column_name.clone()),
                )?);
            }
            ColumnOption::CharacterSet(name) => {
                charset = Some(object_name_to_string(name)?);
            }
            ColumnOption::Collation(name) => {
                collation = Some(object_name_to_string(name)?);
            }
            ColumnOption::Comment(value) => {
                comment = Some(value);
            }
            ColumnOption::Invisible => {
                invisible = true;
            }
            ColumnOption::Srid(expr) => {
                set_column_srid(&mut srid, &expr)?;
            }
            // Inline column-level `REFERENCES other(col)` is parsed but
            // ignored — matching MySQL/InnoDB, which only enforces
            // table-level `FOREIGN KEY (...) REFERENCES ...` clauses.
            ColumnOption::ForeignKey(_) => {}
            _ => {
                return Err(ParseError::unsupported_at(
                    sql,
                    Span {
                        start: 0,
                        end: sql.len(),
                    },
                    "unsupported column option",
                ))
            }
        }
    }
    let serial = is_serial_type(&data_type);
    let (data_type, unsigned) = if serial {
        (DataType::BigInt, true)
    } else if long_alias {
        (
            if long_byte {
                DataType::Blob(crate::storage::schema::MEDIUM_LOB_MAX)
            } else {
                DataType::Text(crate::storage::schema::MEDIUM_LOB_MAX)
            },
            false,
        )
    } else {
        convert_data_type_with_unsigned(sql, data_type)?
    };
    if signed_requested
        && (unsigned
            || !matches!(
                data_type,
                DataType::TinyInt
                    | DataType::SmallInt
                    | DataType::MediumInt
                    | DataType::Int
                    | DataType::BigInt
                    | DataType::Float(_)
                    | DataType::Double(_)
                    | DataType::Decimal(_)
            ))
    {
        return Err(parse_error_at_zero("SIGNED requires a signed numeric type"));
    }
    if zerofill
        && !matches!(
            data_type,
            DataType::TinyInt
                | DataType::SmallInt
                | DataType::MediumInt
                | DataType::Int
                | DataType::BigInt
                | DataType::Bool
                | DataType::Decimal(_)
                | DataType::Float(_)
                | DataType::Double(_)
        )
    {
        return Err(ParseError::unsupported_at(
            sql,
            Span {
                start: 0,
                end: sql.len(),
            },
            "ZEROFILL only applies to numeric column types",
        ));
    }
    let unsigned = zerofill || signed_suffix.unwrap_or(unsigned);
    let data_type = with_column_srid(sql, data_type, srid)?;
    if serial {
        nullable = false;
        auto_increment = true;
        unique = true;
    }
    if invisible && primary_key {
        return Err(parse_error_with_spec_at_zero(
            crate::error::catalog::ER_PK_INDEX_CANT_BE_INVISIBLE,
            "A primary key index cannot be invisible",
        ));
    }
    Ok((
        ColumnDef {
            name: column_name,
            data_type,
            unsigned,
            zerofill,
            invisible,
            nullable,
            primary_key,
            unique,
            auto_increment,
            default,
            on_update,
            checks,
            generated,
            charset,
            collation,
        },
        comment,
    ))
}

fn set_column_srid(target: &mut Option<u32>, expr: &SqlExpr) -> Result<(), ParseError> {
    if target.is_some() {
        return Err(numeric_spec_error(
            crate::error::catalog::ER_DUP_FIELDNAME,
            "Duplicate SRID attribute",
        ));
    }
    let value = match expr {
        SqlExpr::Value(value) => match &value.value {
            SqlValue::Number(text, _) => text.parse::<u32>().ok(),
            _ => None,
        },
        _ => None,
    }
    .ok_or_else(|| {
        numeric_spec_error(
            crate::error::catalog::ER_WRONG_VALUE_FOR_TYPE,
            "SRID must be an unsigned 32-bit integer literal",
        )
    })?;
    *target = Some(value);
    Ok(())
}

fn with_column_srid(
    sql: &str,
    data_type: DataType,
    srid: Option<u32>,
) -> Result<DataType, ParseError> {
    match (data_type, srid) {
        (DataType::Geometry(subtype, _), Some(srid)) => Ok(DataType::Geometry(subtype, Some(srid))),
        (data_type @ DataType::Geometry(..), None) => Ok(data_type),
        (data_type, None) => Ok(data_type),
        (_, Some(_)) => Err(ParseError::unsupported_at(
            sql,
            Span {
                start: 0,
                end: sql.len(),
            },
            "SRID attribute is valid only for spatial columns",
        )),
    }
}

fn convert_generated_column_option(
    sql: &str,
    option: ColumnOption,
) -> Result<GeneratedColumnDef, ParseError> {
    let ColumnOption::Generated {
        generated_as,
        sequence_options,
        generation_expr,
        generation_expr_mode,
        ..
    } = option
    else {
        unreachable!("convert_generated_column_option called with non-generated option")
    };
    if sequence_options.is_some() || generation_expr.is_none() {
        return Err(ParseError::unsupported_at(
            sql,
            Span {
                start: 0,
                end: sql.len(),
            },
            "generated identity column",
        ));
    }
    let expr = generation_expr.expect("checked above");
    let stored = match (generated_as, generation_expr_mode) {
        (GeneratedAs::ExpStored, _) | (_, Some(GeneratedExpressionMode::Stored)) => true,
        (GeneratedAs::Always, None | Some(GeneratedExpressionMode::Virtual)) => false,
        _ => {
            return Err(ParseError::unsupported_at(
                sql,
                Span {
                    start: 0,
                    end: sql.len(),
                },
                "generated column option",
            ))
        }
    };
    Ok(GeneratedColumnDef {
        expr: convert_expr(&expr)?,
        stored,
    })
}

fn convert_check_constraint(
    constraint: sqlparser::ast::CheckConstraint,
    column: Option<String>,
) -> Result<CheckConstraintDef, ParseError> {
    Ok(CheckConstraintDef {
        name: constraint.name.map(ident_value),
        expr: convert_expr(&constraint.expr)?,
        enforced: constraint.enforced.unwrap_or(true),
        column,
    })
}

fn convert_referential_action(action: sqlparser::ast::ReferentialAction) -> ReferentialActionDef {
    use sqlparser::ast::ReferentialAction as RA;
    match action {
        RA::Restrict => ReferentialActionDef::Restrict,
        RA::Cascade => ReferentialActionDef::Cascade,
        RA::SetNull => ReferentialActionDef::SetNull,
        RA::NoAction => ReferentialActionDef::NoAction,
        RA::SetDefault => ReferentialActionDef::SetDefault,
    }
}

fn convert_foreign_key_constraint(
    fk: sqlparser::ast::ForeignKeyConstraint,
) -> Result<ForeignKeyDef, ParseError> {
    // Irrefutable destructure of every field: a future sqlparser upgrade that
    // adds a field to `ForeignKeyConstraint` becomes a compile error here
    // rather than a silent drop (the `insert.priority`-lost-for-a-year class
    // of bug). Fields mydb does not model are bound and explicitly handled.
    let sqlparser::ast::ForeignKeyConstraint {
        name,
        index_name,
        columns,
        foreign_table,
        referred_columns,
        on_delete,
        on_update,
        match_kind,
        characteristics,
    } = fk;
    // `MATCH FULL|PARTIAL|SIMPLE`: InnoDB parses but ignores the MATCH clause
    // (all references behave as MATCH SIMPLE), so accepting it here — rather
    // than erroring — mirrors MySQL 8.0. mydb likewise implements SIMPLE.
    let _ = match_kind;
    // `index_name` (the optional MySQL `FOREIGN KEY idx_name (...)` hint) and
    // `characteristics` (Postgres-style DEFERRABLE, unreachable from MySQL
    // grammar) are not modeled: the backing index name is chosen by
    // `fk_build_one`, and MySQL has no deferrable constraints.
    let _ = index_name;
    let _ = characteristics;
    Ok(ForeignKeyDef {
        name: name.map(ident_value),
        columns: columns.into_iter().map(ident_value).collect(),
        referenced_table: object_name_to_string(foreign_table)?,
        referenced_columns: referred_columns.into_iter().map(ident_value).collect(),
        on_delete: on_delete.map(convert_referential_action),
        on_update: on_update.map(convert_referential_action),
    })
}

fn convert_data_type_with_unsigned(
    sql: &str,
    data_type: SqlDataType,
) -> Result<(DataType, bool), ParseError> {
    let unsigned = is_unsigned_data_type(&data_type);
    let dt = match data_type {
        SqlDataType::TinyInt(width) | SqlDataType::TinyIntUnsigned(width) => {
            validate_integer_display_width(width)?;
            DataType::TinyInt
        }
        SqlDataType::UTinyInt => DataType::TinyInt,
        SqlDataType::SmallInt(width)
        | SqlDataType::SmallIntUnsigned(width)
        | SqlDataType::Int2(width) => {
            validate_integer_display_width(width)?;
            DataType::SmallInt
        }
        SqlDataType::USmallInt => DataType::SmallInt,
        SqlDataType::MediumInt(width) | SqlDataType::MediumIntUnsigned(width) => {
            validate_integer_display_width(width)?;
            DataType::MediumInt
        }
        SqlDataType::Int(width)
        | SqlDataType::IntUnsigned(width)
        | SqlDataType::Integer(width)
        | SqlDataType::IntegerUnsigned(width) => {
            validate_integer_display_width(width)?;
            DataType::Int
        }
        SqlDataType::BigInt(width) | SqlDataType::BigIntUnsigned(width) => {
            validate_integer_display_width(width)?;
            DataType::BigInt
        }
        SqlDataType::UBigInt => DataType::BigInt,
        SqlDataType::Bool | SqlDataType::Boolean => DataType::Bool,
        SqlDataType::Float(info) | SqlDataType::FloatUnsigned(info) => {
            convert_mysql_float_type(info)?
        }
        SqlDataType::Real | SqlDataType::RealUnsigned => {
            if super::entry::current_parser_sql_mode_bits()
                & crate::engine::api::SQL_MODE_REAL_AS_FLOAT
                != 0
            {
                DataType::Float(None)
            } else {
                DataType::Double(None)
            }
        }
        SqlDataType::Float4 | SqlDataType::Float32 => DataType::Float(None),
        SqlDataType::Double(info) | SqlDataType::DoubleUnsigned(info) => {
            DataType::Double(convert_float_number_info(info, false)?)
        }
        SqlDataType::DoublePrecision
        | SqlDataType::DoublePrecisionUnsigned
        | SqlDataType::Float8
        | SqlDataType::Float64 => DataType::Double(None),
        SqlDataType::Decimal(info)
        | SqlDataType::DecimalUnsigned(info)
        | SqlDataType::Numeric(info)
        | SqlDataType::Dec(info) => DataType::Decimal(convert_exact_number_info(info)?),
        SqlDataType::Char(len) | SqlDataType::Character(len) => {
            DataType::Char(convert_character_len(len)?)
        }
        SqlDataType::Varchar(len)
        | SqlDataType::Nvarchar(len)
        | SqlDataType::CharacterVarying(len)
        | SqlDataType::CharVarying(len) => DataType::VarChar(convert_character_len(len)?),
        SqlDataType::Binary(len) => DataType::Binary(convert_fixed_binary_len(len)?),
        SqlDataType::Varbinary(len) => DataType::VarBinary(convert_binary_len(len)?),
        SqlDataType::TinyText => DataType::Text(crate::storage::schema::TINY_LOB_MAX),
        SqlDataType::Text | SqlDataType::String(_) => {
            DataType::Text(crate::storage::schema::DEFAULT_LOB_MAX)
        }
        SqlDataType::MediumText => DataType::Text(crate::storage::schema::MEDIUM_LOB_MAX),
        SqlDataType::LongText => DataType::Text(crate::storage::schema::LONG_LOB_MAX),
        SqlDataType::TinyBlob => DataType::Blob(crate::storage::schema::TINY_LOB_MAX),
        SqlDataType::Blob(Some(length)) => {
            if length > u32::MAX as u64 {
                return Err(numeric_spec_error(
                    crate::error::catalog::ER_TOO_BIG_DISPLAYWIDTH,
                    "Display width out of range for BLOB (max = 4294967295)",
                ));
            }
            DataType::Blob(match length {
                0..=255 => crate::storage::schema::TINY_LOB_MAX,
                256..=65_535 => crate::storage::schema::DEFAULT_LOB_MAX,
                65_536..=16_777_215 => crate::storage::schema::MEDIUM_LOB_MAX,
                _ => crate::storage::schema::LONG_LOB_MAX,
            })
        }
        SqlDataType::Blob(None) | SqlDataType::Bytes(_) => {
            DataType::Blob(crate::storage::schema::DEFAULT_LOB_MAX)
        }
        SqlDataType::MediumBlob => DataType::Blob(crate::storage::schema::MEDIUM_LOB_MAX),
        SqlDataType::LongBlob => DataType::Blob(crate::storage::schema::LONG_LOB_MAX),
        SqlDataType::Date => DataType::Date,
        SqlDataType::Time(p, _) => DataType::Time {
            fsp: clamp_fsp(sql, p)?,
        },
        SqlDataType::Datetime(p) => DataType::DateTime {
            fsp: clamp_fsp(sql, p)?,
        },
        SqlDataType::Timestamp(p, _) => DataType::Timestamp {
            fsp: clamp_fsp(sql, p)?,
        },
        SqlDataType::JSON | SqlDataType::JSONB => DataType::Json,
        // MySQL geometry type names are not sqlparser keywords in type
        // position (except POINT/POLYGON, which the MySQL dialect routes to
        // `Custom` too), so they arrive as `Custom(name, _)`. Map the known
        // spatial names; anything else falls through to the unsupported arm.
        SqlDataType::Custom(name, modifiers) => {
            let name_str = name.to_string();
            if name_str.eq_ignore_ascii_case("VECTOR") {
                // `VECTOR(n)` — the dimension arrives as a string modifier.
                {
                    let (dim, datatype) = parse_vector_type(&modifiers, sql)?;
                    DataType::Vector(dim, datatype)
                }
            } else if name_str.eq_ignore_ascii_case("NCHAR") {
                let length = match modifiers.as_slice() {
                    [] => None,
                    [value] if value.eq_ignore_ascii_case("MAX") => None,
                    [value] => Some(CharacterLength::IntegerLength {
                        length: value.parse().map_err(|_| {
                            numeric_spec_error(
                                crate::error::catalog::ER_TOO_BIG_DISPLAYWIDTH,
                                "invalid NCHAR length",
                            )
                        })?,
                        unit: None,
                    }),
                    _ => return Err(parse_error_at_zero("NCHAR requires at most one length")),
                };
                DataType::Char(convert_character_len(length)?)
            } else if name_str.eq_ignore_ascii_case("YEAR") {
                validate_year_display_width(&modifiers, sql)?;
                DataType::Year
            } else {
                match geometry_subtype_from_name(&name_str) {
                    Some(subtype) => DataType::Geometry(subtype, None),
                    None => {
                        return Err(ParseError::unsupported_at(
                            sql,
                            Span {
                                start: 0,
                                end: sql.len(),
                            },
                            format!("unsupported data type {name}"),
                        ))
                    }
                }
            }
        }
        // Defensive: some sqlparser paths surface POINT/POLYGON as a
        // PostgreSQL-style geometric type.
        SqlDataType::GeometricType(kind) => {
            use crate::storage::schema::GeometrySubtype;
            let subtype = match kind {
                sqlparser::ast::GeometricTypeKind::Point => GeometrySubtype::Point,
                sqlparser::ast::GeometricTypeKind::Polygon => GeometrySubtype::Polygon,
                other => {
                    return Err(ParseError::unsupported_at(
                        sql,
                        Span {
                            start: 0,
                            end: sql.len(),
                        },
                        format!("unsupported geometric data type {other}"),
                    ))
                }
            };
            DataType::Geometry(subtype, None)
        }
        SqlDataType::Enum(members, bits) => {
            // sqlparser accepts ClickHouse-style `ENUM8('a', 'b')` (a
            // trailing bit-width) and `ENUM('a' = 1, 'b' = 2)` (named
            // value assignments). MySQL supports neither — silently
            // dropping either the width hint or the numeric assignment
            // would be misleading, so reject both explicitly.
            if bits.is_some() {
                return Err(ParseError::unsupported_at(
                    sql,
                    Span {
                        start: 0,
                        end: sql.len(),
                    },
                    "ENUM<bits> width hint is not supported (MySQL ENUM has no bit width)",
                ));
            }
            let mut names = Vec::with_capacity(members.len());
            for m in members {
                match m {
                    sqlparser::ast::EnumMember::Name(name) => names.push(name),
                    sqlparser::ast::EnumMember::NamedValue(_, _) => {
                        return Err(ParseError::unsupported_at(
                            sql,
                            Span { start: 0, end: sql.len() },
                            "ENUM('name' = <value>) is not supported (MySQL ENUM members do not carry numeric assignments)",
                        ));
                    }
                }
            }
            DataType::Enum(names)
        }
        SqlDataType::Set(values) => DataType::Set(values),
        SqlDataType::Bit(len) => DataType::Bit(len.map(|v| v as usize)),
        _ => {
            return Err(ParseError::unsupported_at(
                sql,
                Span {
                    start: 0,
                    end: sql.len(),
                },
                format!("unsupported data type {data_type}"),
            ))
        }
    };
    Ok((dt, unsigned))
}

// Numeric/boolean contexts interpret literal bytes as their encoded integer.
// Resolve provenance before compilation; ordinary binary columns remain strings.
fn numeric_literal_operand(expr: &mut Expr) {
    if let Expr::Literal(Literal::HexBytes(bytes) | Literal::BitBytes(bytes)) = expr {
        if bytes.len() <= 8 {
            let value = bytes
                .iter()
                .fold(0u64, |n, byte| (n << 8) | u64::from(*byte));
            *expr = Expr::Literal(Literal::UInt(value));
        }
    }
}

// sqlparser bounds parser recursion, but left-associated binary trees can
// still have arbitrary depth. Check before entering the large conversion
// frame, and retain the counter across nested queries/functions.
#[inline(never)]
pub(super) fn convert_expr(mut expr: &SqlExpr) -> Result<Expr, ParseError> {
    while let SqlExpr::Nested(inner) = expr {
        expr = inner;
    }
    if let SqlExpr::Value(value) = expr {
        return convert_value(&value.value, value.span);
    }
    let _depth = RecursionGuard::enter(RecursionKind::Expression)?;
    with_parser_stack(|| convert_expr_checked(expr))?
}

/// Direct literal children in these contexts acquire numeric provenance.
pub(super) fn has_numeric_literal_children(expr: &Expr) -> bool {
    match expr {
        Expr::Unary { .. }
        | Expr::Binary {
            op: BinaryOp::And | BinaryOp::Or | BinaryOp::Xor,
            ..
        } => true,
        Expr::Function { name, .. } => matches!(
            name.to_ascii_uppercase().as_str(),
            "ABS"
                | "SIGN"
                | "CEIL"
                | "CEILING"
                | "FLOOR"
                | "ROUND"
                | "TRUNCATE"
                | "LOG"
                | "LOG2"
                | "LOG10"
                | "LN"
                | "EXP"
                | "SQRT"
                | "POW"
                | "POWER"
                | "SIN"
                | "COS"
                | "TAN"
                | "ASIN"
                | "ACOS"
                | "ATAN"
                | "ATAN2"
                | "COT"
                | "RADIANS"
                | "DEGREES"
                | "SPACE"
                | "CHAR"
                | "__IS_TRUE"
        ),
        _ => false,
    }
}

#[inline(never)]
fn convert_expr_checked(expr: &SqlExpr) -> Result<Expr, ParseError> {
    let mut converted = convert_expr_inner(expr)?;
    if has_numeric_literal_children(&converted) {
        match &mut converted {
            Expr::Unary { expr, .. } => numeric_literal_operand(expr),
            Expr::Binary { left, right, .. } => {
                numeric_literal_operand(left);
                numeric_literal_operand(right);
            }
            Expr::Function { args, .. } => {
                for arg in args {
                    numeric_literal_operand(arg);
                }
            }
            _ => unreachable!("numeric literal context must have scalar children"),
        }
    }
    Ok(converted)
}

fn convert_expr_inner(expr: &SqlExpr) -> Result<Expr, ParseError> {
    // sqlparser builds ordinary `a AND b AND ...` / `a OR b OR ...` input as
    // a left-deep binary tree. Recursing through that spine is needlessly
    // expensive here: `convert_expr` has a large debug-build stack frame due
    // to the breadth of SqlExpr variants it converts, so a modest TPC-style
    // predicate can exhaust nextest's 2 MiB worker stack. Walk homogeneous
    // logical chains explicitly and rebuild the same left-to-right internal
    // tree. Nested/mixed operators remain leaves and use the regular converter.
    if let SqlExpr::BinaryOp { left, op, right } = expr {
        let continues_chain = |child: &SqlExpr| {
            matches!(
                (op, child),
                (
                    SqlBinaryOperator::And,
                    SqlExpr::BinaryOp {
                        op: SqlBinaryOperator::And,
                        ..
                    }
                ) | (
                    SqlBinaryOperator::Or,
                    SqlExpr::BinaryOp {
                        op: SqlBinaryOperator::Or,
                        ..
                    }
                )
            )
        };
        // Keep the overwhelmingly common two-operand case allocation-free.
        if continues_chain(left) || continues_chain(right) {
            return convert_logical_chain(expr, op);
        }
    }
    match expr {
        SqlExpr::Nested(expr) => convert_expr(expr),
        SqlExpr::Identifier(ident) => {
            if ident.quote_style.is_none()
                && (ident.value.eq_ignore_ascii_case("MOD")
                    || ident.value.eq_ignore_ascii_case("BOTH"))
            {
                return Err(parse_error_at_zero(
                    "reserved identifiers MOD and BOTH must be quoted",
                ));
            }
            let value = ident.value.clone();
            if ident.quote_style.is_none() && value.eq_ignore_ascii_case("DEFAULT") {
                return Ok(Expr::Default(None));
            }
            if let Some(name) = value.strip_prefix('@') {
                if ident.quote_style.is_none() && !name.starts_with('@') {
                    return Ok(Expr::UserVariable(name.to_ascii_lowercase()));
                }
            }
            if ident.quote_style.is_none() && value.starts_with("@@") {
                return system_variable_expr(&value);
            }
            Ok(Expr::Identifier(value))
        }
        SqlExpr::CompoundIdentifier(parts) => {
            let system_variable = parts
                .first()
                .is_some_and(|part| part.quote_style.is_none() && part.value.starts_with("@@"));
            let parts = parts.iter().cloned().map(ident_value).collect::<Vec<_>>();
            if system_variable {
                system_variable_expr(&parts.join("."))
            } else {
                Ok(Expr::CompoundIdentifier(parts))
            }
        }
        SqlExpr::Value(value) => convert_value(&value.value, value.span),
        SqlExpr::Tuple(exprs) => {
            if exprs.len() < 2 {
                return Err(parse_error_at_zero("ROW requires at least two expressions"));
            }
            Ok(Expr::Tuple(convert_expr_list(exprs)?))
        }
        SqlExpr::UnaryOp { op, expr } => match op {
            SqlUnaryOperator::Not | SqlUnaryOperator::BangNot => Ok(Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(convert_expr(expr)?),
            }),
            SqlUnaryOperator::Minus => Ok(Expr::Unary {
                op: UnaryOp::Neg,
                expr: Box::new(convert_expr(expr)?),
            }),
            SqlUnaryOperator::Plus => convert_expr(expr),
            SqlUnaryOperator::BitwiseNot => Ok(Expr::Unary {
                op: UnaryOp::BitNot,
                expr: Box::new(convert_expr(expr)?),
            }),
            _ => parse_error_from_sql_span(expr.span(), "unsupported unary operator", vec![]),
        },
        SqlExpr::BinaryOp { left, op, right } => {
            if matches!(op, SqlBinaryOperator::StringConcat)
                && current_parser_sql_mode_bits() & crate::engine::api::SQL_MODE_PIPES_AS_CONCAT
                    != 0
            {
                return Ok(Expr::Function {
                    name: "CONCAT".into(),
                    args: vec![convert_expr(left)?, convert_expr(right)?],
                    distinct: false,
                });
            }
            if let SqlBinaryOperator::Custom(operator) = op {
                if operator == super::adapter::DYNAMIC_LIKE {
                    reject_predicate_as_predicate_lhs(left)?;
                    let SqlExpr::Tuple(parts) = right.as_ref() else {
                        unreachable!("native LIKE operands")
                    };
                    return Ok(Expr::Function {
                        name: "__LIKE_DYNAMIC_ESCAPE".into(),
                        args: vec![
                            convert_expr(left)?,
                            convert_expr(&parts[0])?,
                            convert_expr(&parts[1])?,
                            convert_expr(&parts[2])?,
                        ],
                        distinct: false,
                    });
                }
                if operator == super::adapter::SOUNDS_LIKE_OP {
                    return convert_sounds_like(left, right);
                }
                let function = match operator.as_str() {
                    "<->" => "L2_DISTANCE",
                    "<#>" => "DOT_PRODUCT",
                    _ => {
                        return parse_error_from_sql_span(
                            expr.span(),
                            "unsupported binary operator",
                            vec![],
                        )
                    }
                };
                let distance = Expr::Function {
                    name: function.into(),
                    args: vec![convert_expr(left)?, convert_expr(right)?],
                    distinct: false,
                };
                return Ok(if operator == "<#>" {
                    Expr::Binary {
                        left: Box::new(Expr::Literal(Literal::Int(0))),
                        op: BinaryOp::Sub,
                        right: Box::new(distance),
                    }
                } else {
                    distance
                });
            }

            // Standard SQL date ± interval: rewrite to DATE_ADD / DATE_SUB
            // so the existing function evaluator handles it.
            if matches!(op, SqlBinaryOperator::Plus | SqlBinaryOperator::Minus)
                && is_sql_interval_expr(right)
            {
                let func_name = if matches!(op, SqlBinaryOperator::Minus) {
                    "DATE_SUB"
                } else {
                    "DATE_ADD"
                };
                return Ok(Expr::Function {
                    name: func_name.to_string(),
                    args: vec![convert_expr(left)?, convert_expr(right)?],
                    distinct: false,
                });
            }
            // Addition is commutative, so MySQL also accepts the INTERVAL on the
            // left: `INTERVAL 1 DAY + date` == `date + INTERVAL 1 DAY`. (Only
            // `+`; `INTERVAL - date` is not valid MySQL.) Rewrite to
            // DATE_ADD(date, interval).
            if matches!(op, SqlBinaryOperator::Plus)
                && is_sql_interval_expr(left)
                && !is_sql_interval_expr(right)
            {
                return Ok(Expr::Function {
                    name: "DATE_ADD".to_string(),
                    args: vec![convert_expr(right)?, convert_expr(left)?],
                    distinct: false,
                });
            }
            if matches!(
                op,
                SqlBinaryOperator::MyIntegerDivide | SqlBinaryOperator::DuckIntegerDivide
            ) {
                return Ok(Expr::Binary {
                    left: Box::new(convert_expr(left)?),
                    op: BinaryOp::IntDiv,
                    right: Box::new(convert_expr(right)?),
                });
            }
            // `@var := expr` — a MySQL user-variable assignment expression.
            // sqlparser parses `:=` as `BinaryOperator::Assignment` at `=`
            // precedence, so a chain `@a := @b := 5` arrives left-associative
            // as `((@a := @b) := 5)`; MySQL's `:=` is right-associative
            // (`@a := (@b := 5)`, all get 5). Re-thread the chain and emit the
            // internal `__set_user_var('name', expr)` function so the existing
            // `Expr::Function` machinery carries it (issue #86).
            if matches!(op, SqlBinaryOperator::Assignment) {
                return convert_user_var_assignment(left, right);
            }
            // MySQL does not allow an unparenthesized predicate result on the
            // left of another comparison (`1 IS TRUE = 1`). Parentheses turn
            // the inner predicate into a simple_expr and are intentionally
            // represented by SqlExpr::Nested, so they remain legal.
            if is_sql_comparison_op(op) && is_unparenthesized_mysql_truth_test(left) {
                return parse_error_from_sql_span(
                    left.span(),
                    "invalid unparenthesized predicate chain",
                    vec![],
                );
            }
            // Without whitespace sqlparser tokenizes MySQL `lhs<@var` as
            // PostgreSQL's `<@` operator followed by a bare identifier.  Put
            // back the user-variable boundary before generic conversion.
            if matches!(op, SqlBinaryOperator::ArrowAt) {
                let SqlExpr::Identifier(identifier) = right.as_ref() else {
                    return parse_error_from_sql_span(
                        right.span(),
                        "unsupported <@ operator",
                        vec![],
                    );
                };
                return Ok(Expr::Binary {
                    left: Box::new(convert_expr(left)?),
                    op: BinaryOp::Lt,
                    right: Box::new(Expr::UserVariable(identifier.value.to_ascii_lowercase())),
                });
            }
            let cop = convert_binary_op(op)?;
            let cleft = convert_expr(left)?;
            let cright = convert_expr(right)?;
            // sqlparser gives the JSON `->` / `->>` operators a lower
            // precedence than MySQL does, so `j -> '$.x' = 5` mis-parses as
            // `j -> ('$.x' = 5)`. In MySQL these bind very tightly and their
            // right operand is always a path literal, so any binary structure
            // there is a precedence artifact: re-thread the arrow down the
            // left spine of its right operand so it binds the path leaf.
            if matches!(cop, BinaryOp::Arrow | BinaryOp::LongArrow) {
                return Ok(rethread_json_arrow(cleft, cop, cright));
            }
            Ok(Expr::Binary {
                left: Box::new(cleft),
                op: cop,
                right: Box::new(cright),
            })
        }
        SqlExpr::MemberOf(member_of) => Ok(Expr::Function {
            name: "__MEMBER_OF".to_string(),
            args: vec![
                convert_expr(&member_of.value)?,
                convert_expr(&member_of.array)?,
            ],
            distinct: false,
        }),
        SqlExpr::IsNull(expr) => Ok(Expr::IsNull(Box::new(convert_expr(expr)?))),
        SqlExpr::IsNotNull(expr) => Ok(Expr::IsNotNull(Box::new(convert_expr(expr)?))),
        SqlExpr::IsTrue(expr) => truth_test_expr("__IS_TRUE", expr),
        SqlExpr::IsNotTrue(expr) => Ok(Expr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(truth_test_expr("__IS_TRUE", expr)?),
        }),
        SqlExpr::IsFalse(expr) => truth_test_expr("__IS_FALSE", expr),
        SqlExpr::IsNotFalse(expr) => Ok(Expr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(truth_test_expr("__IS_FALSE", expr)?),
        }),
        SqlExpr::IsUnknown(expr) => Ok(Expr::IsNull(Box::new(convert_expr(expr)?))),
        SqlExpr::IsNotUnknown(expr) => Ok(Expr::IsNotNull(Box::new(convert_expr(expr)?))),
        SqlExpr::InList {
            expr,
            list,
            negated,
        } => {
            reject_predicate_as_predicate_lhs(expr)?;
            Ok(Expr::InList {
                expr: Box::new(convert_expr(expr)?),
                list: convert_expr_list(list)?,
                negated: *negated,
            })
        }
        SqlExpr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            reject_predicate_as_predicate_lhs(expr)?;
            Ok(Expr::InSubquery {
                expr: Box::new(convert_expr(expr)?),
                subquery: Box::new(convert_query_to_select(subquery)?),
                negated: *negated,
            })
        }
        SqlExpr::Subquery(query) => Ok(Expr::Subquery(Box::new(convert_query_to_select(query)?))),
        SqlExpr::Exists { subquery, negated } => Ok(Expr::Exists {
            subquery: Box::new(convert_query_to_select(subquery)?),
            negated: *negated,
        }),
        SqlExpr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            reject_predicate_as_predicate_lhs(expr)?;

            // bool_pri comparison predicate: `2 = 1 BETWEEN 0 AND 1`
            // means `2 = (1 BETWEEN 0 AND 1)`. A flat Pratt table first
            // yields `(2 = 1) BETWEEN ...`; rotate the comparison's RHS into
            // the predicate while the SqlExpr::Nested boundary is available.
            if let SqlExpr::BinaryOp {
                left: cmp_left,
                op: cmp_op,
                right: cmp_right,
            } = expr.as_ref()
            {
                if is_sql_comparison_op(cmp_op) {
                    return Ok(Expr::Binary {
                        left: Box::new(convert_expr(cmp_left)?),
                        op: convert_binary_op(cmp_op)?,
                        right: Box::new(Expr::Between {
                            expr: Box::new(convert_expr(cmp_right)?),
                            low: Box::new(convert_expr(low)?),
                            high: Box::new(convert_expr(high)?),
                            negated: *negated,
                        }),
                    });
                }
            }

            // The high operand is a `predicate`, not a `bool_pri`. Thus a
            // following comparison belongs outside the complete BETWEEN:
            // `5 BETWEEN 0 AND 10 < 2` => `(5 BETWEEN 0 AND 10) < 2`.
            if let SqlExpr::BinaryOp {
                left: high_left,
                op: high_op,
                right: high_right,
            } = high.as_ref()
            {
                if is_sql_comparison_op(high_op) {
                    return Ok(Expr::Binary {
                        left: Box::new(Expr::Between {
                            expr: Box::new(convert_expr(expr)?),
                            low: Box::new(convert_expr(low)?),
                            high: Box::new(convert_expr(high_left)?),
                            negated: *negated,
                        }),
                        op: convert_binary_op(high_op)?,
                        right: Box::new(convert_expr(high_right)?),
                    });
                }
            }

            Ok(Expr::Between {
                expr: Box::new(convert_expr(expr)?),
                low: Box::new(convert_expr(low)?),
                high: Box::new(convert_expr(high)?),
                negated: *negated,
            })
        }
        SqlExpr::Function(function) => {
            if function.null_treatment == Some(sqlparser::ast::NullTreatment::IgnoreNulls)
                || !function.within_group.is_empty()
            {
                let mut err = parse_error_at_zero("IGNORE NULLS / WITHIN GROUP is not supported");
                err.spec = crate::error::catalog::ER_NOT_SUPPORTED_YET;
                return Err(err);
            }
            let name = object_name_to_string(&function.name)?;
            validate_function_syntax(function, &name)?;
            let mut args = convert_function_args(&function.args)?;
            if name.eq_ignore_ascii_case("DEFAULT") {
                if let [Expr::CompoundIdentifier(parts)] = args.as_slice() {
                    return Ok(Expr::DefaultQualified(parts.clone()));
                }
                let column = match args.as_slice() {
                    [expr] => expr_column_name_owned(expr),
                    _ => None,
                }
                .ok_or_else(|| parse_error_at_zero("DEFAULT requires exactly one column name"))?;
                return Ok(Expr::Default(Some(column)));
            }
            let upper_name = name.to_ascii_uppercase();
            validate_mysql_function_arity(
                &upper_name,
                args.len(),
                function_args_distinct(&function.args),
            )?;
            // sqlparser represents MySQL's explicit ROW(a, b, ...) syntax as
            // an ordinary function call. It is a row constructor, not a
            // stored-function lookup; normalize it to the same Tuple node
            // used for `(a, b, ...)` so comparison/coercion is shared.
            if upper_name == "ROW" {
                return Ok(Expr::Tuple(args));
            }
            // The leading unit/format of these functions is a bare grammar
            // keyword (`TIMESTAMPDIFF(DAY, a, b)`, `GET_FORMAT(DATE, 'ISO')`).
            // Make it a constant so binding, view analysis and the
            // ONLY_FULL_GROUP_BY / hidden-column rewrites never treat it as a
            // column reference.
            if matches!(
                upper_name.as_str(),
                "TIMESTAMPADD" | "TIMESTAMPDIFF" | "GET_FORMAT"
            ) {
                if let Some(Expr::Identifier(unit)) = args.first_mut() {
                    let unit = std::mem::take(unit);
                    args[0] = Expr::Literal(Literal::String(unit));
                }
            }
            if upper_name == "GROUP_CONCAT" || upper_name == "JSON_ARRAYAGG" {
                if upper_name == "JSON_ARRAYAGG"
                    && matches!(
                        &function.args,
                        FunctionArguments::List(list)
                            if list.clauses.iter().any(|clause| {
                                matches!(clause, FunctionArgumentClause::OrderBy(_))
                            })
                    )
                {
                    return parse_error_from_sql_span(
                        function.span(),
                        "JSON_ARRAYAGG does not accept an ORDER BY clause in MySQL 8.0",
                        vec![],
                    );
                }
                // MySQL 8.0 syntax:
                //   GROUP_CONCAT([DISTINCT] expr1 [, expr2 ...]
                //       [ORDER BY ord1 [ASC|DESC] [, ord2 ...]]
                //       [SEPARATOR str])
                //   JSON_ARRAYAGG(expr)
                //
                // Encoding into our plain `Expr::Function` (no AST field
                // changes — 200+ construction sites would otherwise need to
                // be touched):
                //   * Multi-column values `expr1, expr2, ...` (GROUP_CONCAT
                //     only) are wrapped in `CONCAT(expr1, expr2, ...)` so
                //     the slot still sees a single value expression.
                //   * The optional ORDER BY clause is wrapped in a
                //     synthetic internal `__gc_ordered` function that
                //     becomes args[0]:
                //       `__gc_ordered(value, ord1, "ASC"|"DESC", ord2, ...)`
                //     `AggregateSlot::from_expr` detects this wrapper,
                //     pulls the value back out, and stores the order
                //     spec on the slot. Other AST passes (qualification,
                //     decorrelation, expr_program) treat the wrapper as
                //     a normal function call, so column references
                //     inside `ord1`/`ord2`/… are qualified the same way
                //     as the value expression's column refs.
                //   * SEPARATOR keeps its existing convention: the
                //     literal becomes args[1] for GROUP_CONCAT only.
                let value_expr_count = args.len();
                let mut order_pairs: Vec<Expr> = Vec::new();
                let mut separator: Option<Expr> = None;
                if let FunctionArguments::List(list) = &function.args {
                    for clause in &list.clauses {
                        match clause {
                            FunctionArgumentClause::Separator(value) => {
                                separator = Some(convert_value(value, function.args.span())?);
                            }
                            FunctionArgumentClause::OrderBy(order_exprs) => {
                                for item in order_exprs {
                                    let asc = item.options.asc.unwrap_or(true);
                                    let mut order_expr = convert_expr(&item.expr)?;
                                    if matches!(
                                        &order_expr,
                                        Expr::Literal(Literal::Int(position))
                                            if *position <= 0
                                                || *position as usize > value_expr_count
                                    ) {
                                        let position = match order_expr {
                                            Expr::Literal(Literal::Int(position)) => position,
                                            _ => unreachable!("guarded ordinal"),
                                        };
                                        return Err(ParseError {
                                            spec: crate::error::catalog::ER_BAD_FIELD_ERROR,
                                            message: format!(
                                                "Unknown column '{position}' in 'order clause'"
                                            ),
                                            span: Span { start: 0, end: 0 },
                                            line: item.expr.span().start.line.max(1) as usize,
                                            column: item.expr.span().start.column.max(1) as usize,
                                            expected: Vec::new(),
                                        });
                                    }
                                    if let Expr::Literal(Literal::Int(position)) = &order_expr {
                                        if *position > 0 {
                                            order_expr = clone_parser_expression(
                                                &args[*position as usize - 1],
                                            )?;
                                        } else {
                                            unreachable!("non-positive ordinal rejected above");
                                        }
                                    }
                                    order_pairs.push(order_expr);
                                    // Direction token: base ASC/DESC plus an
                                    // optional explicit NULLS placement suffix
                                    // (mydb extension). Decoded by
                                    // `aggregate::unwrap_group_concat_ordered`.
                                    let dir = match (asc, item.options.nulls_first) {
                                        (true, None) => "ASC",
                                        (false, None) => "DESC",
                                        (true, Some(true)) => "ASC_NULLS_FIRST",
                                        (true, Some(false)) => "ASC_NULLS_LAST",
                                        (false, Some(true)) => "DESC_NULLS_FIRST",
                                        (false, Some(false)) => "DESC_NULLS_LAST",
                                    };
                                    order_pairs.push(Expr::Literal(Literal::String(dir.into())));
                                }
                            }
                            _ => {}
                        }
                    }
                }
                let value_expr = if args.is_empty() {
                    Expr::Literal(Literal::Null)
                } else if args.len() == 1 || upper_name == "JSON_ARRAYAGG" {
                    args.swap_remove(0)
                } else {
                    // GROUP_CONCAT with multiple value expressions.
                    Expr::Function {
                        name: "CONCAT".to_string(),
                        args: std::mem::take(&mut args),
                        distinct: false,
                    }
                };
                args.clear();
                if order_pairs.is_empty() {
                    args.push(value_expr);
                } else {
                    let mut wrapper_args = Vec::with_capacity(1 + order_pairs.len());
                    wrapper_args.push(value_expr);
                    wrapper_args.extend(order_pairs);
                    args.push(Expr::Function {
                        name: GROUP_CONCAT_ORDERED_FN.to_string(),
                        args: wrapper_args,
                        distinct: false,
                    });
                }
                if let Some(sep) = separator {
                    args.push(sep);
                }
            }
            if let Some(over) = &function.over {
                use sqlparser::ast::WindowType;
                match over {
                    WindowType::NamedWindow(window_name) => {
                        return Ok(Expr::WindowFunction {
                            name,
                            args,
                            spec: Box::new(crate::parser::WindowSpec {
                                partition_by: Vec::new(),
                                order_by: Vec::new(),
                                frame: None,
                                inherits: Some(window_name.value.clone()),
                            }),
                        });
                    }
                    WindowType::WindowSpec(spec) => {
                        let ConvertedWindowSpec {
                            partition_by,
                            order_by,
                            frame,
                            inherits,
                        } = convert_window_spec(spec)?;
                        return Ok(Expr::WindowFunction {
                            name,
                            args,
                            spec: Box::new(crate::parser::WindowSpec {
                                partition_by,
                                order_by,
                                frame,
                                inherits,
                            }),
                        });
                    }
                }
            }
            Ok(Expr::Function {
                name,
                args,
                distinct: function_args_distinct(&function.args),
            })
        }
        SqlExpr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            let mut args = vec![convert_expr(expr)?];
            if let Some(from) = substring_from {
                args.push(convert_expr(from)?);
            }
            if let Some(for_) = substring_for {
                args.push(convert_expr(for_)?);
            }
            Ok(Expr::Function {
                name: "SUBSTRING".to_string(),
                args,
                distinct: false,
            })
        }
        SqlExpr::Position { expr, r#in } => Ok(Expr::Function {
            name: "POSITION".to_string(),
            args: vec![convert_expr(expr)?, convert_expr(r#in)?],
            distinct: false,
        }),
        SqlExpr::Trim {
            expr,
            trim_where,
            trim_what,
            trim_characters,
        } => {
            let mut args = vec![convert_expr(expr)?];
            if let Some(what) = trim_what {
                args.push(convert_expr(what)?);
            } else if let Some(chars) = trim_characters {
                if chars.len() > 1 {
                    return parse_error_from_sql_span(
                        expr.span(),
                        "TRIM does not support multiple trim characters",
                        vec![],
                    );
                }
                if let Some(first) = chars.first() {
                    args.push(convert_expr(first)?);
                }
            }
            if let Some(where_) = trim_where {
                args.push(Expr::Literal(Literal::String(
                    match where_ {
                        TrimWhereField::Both => "BOTH",
                        TrimWhereField::Leading => "LEADING",
                        TrimWhereField::Trailing => "TRAILING",
                    }
                    .to_string(),
                )));
            }
            Ok(Expr::Function {
                name: "TRIM".to_string(),
                args,
                distinct: false,
            })
        }
        SqlExpr::Extract { field, expr, .. } => Ok(Expr::Function {
            name: "EXTRACT".to_string(),
            args: vec![
                Expr::Literal(Literal::String(field.to_string())),
                convert_expr(expr)?,
            ],
            distinct: false,
        }),
        SqlExpr::Ceil { expr, field } => {
            let mut args = vec![convert_expr(expr)?];
            if let CeilFloorKind::Scale(value) = field {
                args.push(convert_value(value, expr.span())?);
            }
            Ok(Expr::Function {
                name: "CEIL".to_string(),
                args,
                distinct: false,
            })
        }
        SqlExpr::Floor { expr, field } => {
            let mut args = vec![convert_expr(expr)?];
            if let CeilFloorKind::Scale(value) = field {
                args.push(convert_value(value, expr.span())?);
            }
            Ok(Expr::Function {
                name: "FLOOR".to_string(),
                args,
                distinct: false,
            })
        }
        SqlExpr::Like {
            negated,
            expr,
            pattern,
            escape_char,
            ..
        } => {
            reject_predicate_as_predicate_lhs(expr)?;
            Ok(Expr::Like {
                expr: Box::new(convert_expr(expr)?),
                pattern: Box::new(convert_expr(pattern)?),
                negated: *negated,
                escape_char: convert_like_escape(escape_char.as_ref())?,
            })
        }
        SqlExpr::ILike { negated, .. } => {
            // B4: MySQL 8.0 has no ILIKE (PostgreSQL extension).
            // sqlparser still recognises it, so the parser gets here
            // whenever a client types it. Silently rewriting it to
            // LIKE was hiding the case-insensitivity contract behind
            // the default collation — worked under `utf8mb4_0900_ai_ci`,
            // wrong under `utf8mb4_bin`. Report as syntax error per
            // CLAUDE.md's "unsupported → error, never no-op" rule.
            let verb = if *negated { "NOT ILIKE" } else { "ILIKE" };
            Err(ParseError::at_zero(format!(
                "You have an error in your SQL syntax; near '{verb}' — mydb targets \
                 MySQL 8.0 which does not implement ILIKE; use LOWER()/UPPER() with \
                 LIKE, an explicit COLLATE clause, or a `_ci` column collation."
            )))
        }
        SqlExpr::RLike {
            negated,
            expr,
            pattern,
            ..
        } => {
            reject_predicate_as_predicate_lhs(expr)?;
            let call = Expr::Function {
                name: "REGEXP_LIKE".to_string(),
                args: vec![convert_expr(expr)?, convert_expr(pattern)?],
                distinct: false,
            };
            if *negated {
                Ok(Expr::Unary {
                    op: UnaryOp::Not,
                    expr: Box::new(call),
                })
            } else {
                Ok(call)
            }
        }
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            let mut conds = Vec::with_capacity(conditions.len());
            let mut results = Vec::with_capacity(conditions.len());
            for case_when in conditions {
                conds.push(convert_expr(&case_when.condition)?);
                results.push(convert_expr(&case_when.result)?);
            }
            Ok(Expr::Case {
                operand: operand
                    .as_ref()
                    .map(|e| convert_expr(e).map(Box::new))
                    .transpose()?,
                conditions: conds.into_boxed_slice(),
                results,
                else_result: else_result
                    .as_ref()
                    .map(|e| convert_expr(e).map(Box::new))
                    .transpose()?,
            })
        }
        SqlExpr::AtTimeZone {
            timestamp,
            time_zone,
        } => Ok(Expr::Function {
            // Internal lowering keeps the ordinary function traversal,
            // fingerprinting, CHECK validation and evaluator fallback paths.
            name: "__AT_TIME_ZONE".to_string(),
            args: vec![convert_expr(timestamp)?, convert_expr(time_zone)?],
            distinct: false,
        }),
        SqlExpr::Cast {
            expr,
            data_type,
            kind,
            array,
            format,
        } => {
            if *kind != sqlparser::ast::CastKind::Cast || *array || format.is_some() {
                let mut error = parse_error_at_zero(
                    "TRY_CAST, SAFE_CAST, scalar ARRAY casts and CAST FORMAT are not supported",
                );
                error.spec = crate::error::catalog::ER_NOT_SUPPORTED_YET;
                return Err(error);
            }
            let mut args = vec![convert_expr(expr)?];
            let (type_name, extra) = convert_cast_data_type(data_type)?;
            args.push(Expr::Literal(Literal::String(type_name)));
            args.extend(extra);
            Ok(Expr::Function {
                name: "CAST".to_string(),
                args,
                distinct: false,
            })
        }
        SqlExpr::Convert {
            expr,
            data_type,
            charset,
            ..
        } => {
            let mut args = vec![convert_expr(expr)?];
            if let Some(dt) = data_type {
                if charset.is_some() {
                    return parse_error_from_sql_span(
                        expr.span(),
                        "CONVERT charset clause is not supported",
                        vec![],
                    );
                }
                let (type_name, extra) = convert_cast_data_type(dt)?;
                args.push(Expr::Literal(Literal::String(type_name)));
                args.extend(extra);
            } else if let Some(charset) = charset {
                args.push(Expr::Literal(Literal::String(object_name_to_string(
                    charset,
                )?)));
                return Ok(Expr::Function {
                    name: "CONVERT_USING".to_string(),
                    args,
                    distinct: false,
                });
            } else {
                return parse_error_from_sql_span(
                    expr.span(),
                    "CONVERT requires a target type",
                    vec![],
                );
            }
            Ok(Expr::Function {
                name: "CAST".to_string(),
                args,
                distinct: false,
            })
        }
        SqlExpr::AnyOp {
            left,
            compare_op,
            right,
            ..
        } => convert_quantified_comparison(left, compare_op, SubqueryQuantifier::Any, right),
        SqlExpr::AllOp {
            left,
            compare_op,
            right,
        } => convert_quantified_comparison(left, compare_op, SubqueryQuantifier::All, right),
        SqlExpr::Prefixed { prefix, value } => {
            // MySQL 8.0 charset introducers use exactly one leading `_`
            // followed by a known charset name (`_utf8mb4'x'`, `_binary'x'`,
            // …). Reject `__utf8'x'` / `___utf8'x'` (multiple underscores)
            // and `abc'x'` (non-introducer prefix) here rather than silently
            // stripping — a plain identifier prefix followed by a string is
            // a MySQL syntax error, and multi-underscore forms often
            // indicate a typo the user should see.
            let raw = prefix.value.as_str();
            if !raw.starts_with('_') || raw.starts_with("__") {
                return parse_error_from_sql_span(
                    prefix.span,
                    format!(
                        "invalid character set introducer '{raw}': must be a single leading underscore followed by a charset name",
                    ),
                    vec![],
                );
            }
            let charset = raw[1..].to_string();
            if charset.is_empty() {
                return parse_error_from_sql_span(
                    prefix.span,
                    "empty character set introducer",
                    vec![],
                );
            }
            Ok(Expr::Function {
                name: "CONVERT_USING".to_string(),
                args: vec![
                    convert_expr(value)?,
                    Expr::Literal(Literal::String(charset)),
                    // Preserve the SQL source category. A character-set
                    // introducer is still a literal (coercibility 4), while
                    // CONVERT(expr USING charset) produces an implicit
                    // string (coercibility 2). Both share the value kernel.
                    Expr::Literal(Literal::Bool(true)),
                ],
                distinct: false,
            })
        }
        SqlExpr::Collate { expr, collation } => Ok(Expr::Function {
            name: "COLLATE".to_string(),
            args: vec![
                convert_expr(expr)?,
                Expr::Literal(Literal::String(object_name_to_string(collation)?)),
            ],
            distinct: false,
        }),
        SqlExpr::Interval(Interval {
            value,
            leading_field,
            leading_precision,
            last_field,
            fractional_seconds_precision,
        }) => {
            if leading_precision.is_some()
                || last_field.is_some()
                || fractional_seconds_precision.is_some()
            {
                let mut error =
                    parse_error_at_zero("INTERVAL precision and unit ranges are not supported");
                error.spec = crate::error::catalog::ER_NOT_SUPPORTED_YET;
                return Err(error);
            }
            // MySQL 8.0 requires an explicit time-unit on INTERVAL
            // literals (`INTERVAL 1 DAY`, `INTERVAL '1:20' HOUR_MINUTE`).
            // The previous silent DAY fallback was reachable through
            // rare sqlparser paths that emit a bare INTERVAL with no
            // leading field; a defensive error here converts that into
            // a clean 1064 instead of silently interpreting the value
            // as days.
            let Some(field) = leading_field.as_ref() else {
                return parse_error_from_sql_span(
                    expr.span(),
                    "INTERVAL requires an explicit time unit (e.g. DAY, HOUR, MINUTE)",
                    vec![],
                );
            };
            let unit = field.to_string().to_ascii_uppercase();
            Ok(Expr::Function {
                name: "__INTERVAL".to_string(),
                args: vec![convert_expr(value)?, Expr::Literal(Literal::String(unit))],
                distinct: false,
            })
        }
        // Typed string literal forms such as `DATE '1998-12-01'`,
        // `TIMESTAMP '1998-12-01 00:00:00'`, `TIME '12:34:56'`. These appear
        // throughout the standard TPC-H queries. mydb stores DATE/TIMESTAMP
        // values comparably to ISO-8601 strings and coerces strings on the
        // fly when compared against typed columns, so the simplest correct
        // mapping is to emit the underlying string literal.
        SqlExpr::TypedString(ts) => {
            let s = ts.value.value.clone().into_string().ok_or_else(|| {
                parse_error_at_span(expr.span(), "TYPED literal requires a string value")
            })?;
            let valid = match ts.data_type {
                SqlDataType::Date => crate::storage::parse_date_to_days_bytes(s.as_bytes()).is_ok(),
                SqlDataType::Time(..) => crate::storage::parse_time_to_micros(&s).is_ok(),
                SqlDataType::Datetime(..) | SqlDataType::Timestamp(..) => {
                    crate::storage::parse_datetime_to_micros(&s).is_ok()
                }
                _ => true,
            };
            let allowed_invalid_date =
                matches!(ts.data_type, SqlDataType::Date | SqlDataType::Datetime(..))
                    && current_parser_sql_mode_bits()
                        & crate::engine::api::TEMPORAL_MODE_ALLOW_INVALID_DATES
                        != 0
                    && crate::util::temporal::parse_delimited_datetime_components(&s).is_some();
            if !valid && !allowed_invalid_date {
                let mut err = parse_error_at_span(
                    expr.span(),
                    format!("Incorrect {} value: '{}'", ts.data_type, s),
                );
                err.spec = crate::error::catalog::ER_WRONG_VALUE;
                return Err(err);
            }
            // MySQL temporal literals `DATE '…'` / `TIME '…'` / `DATETIME '…'`
            // / `TIMESTAMP '…'` are genuine temporal typed values, not plain
            // strings: `DATE '2024-05-31' + 0` is 20240531 and
            // `DATE '2020-01-01' = TIMESTAMP '2020-01-01 00:00:00'` is TRUE.
            // Lower them to a CAST so comparison / arithmetic / CAST see a real
            // DATE/TIME/DATETIME value; other typed strings keep string form.
            if matches!(ts.data_type, SqlDataType::Timestamp(..)) {
                // A TIMESTAMP typed literal denotes an instant.  Keeping it
                // distinct from DATETIME is required by AT TIME ZONE and by
                // session-zone conversion; ordinary CAST(TIMESTAMP AS ...)
                // deliberately still follows MySQL's DATETIME cast surface.
                Ok(Expr::Function {
                    name: "__TIMESTAMP_LITERAL".to_string(),
                    args: vec![Expr::Literal(Literal::String(s))],
                    distinct: false,
                })
            } else if matches!(
                ts.data_type,
                SqlDataType::Date | SqlDataType::Time(..) | SqlDataType::Datetime(..)
            ) {
                let (type_name, extra) = convert_cast_data_type(&ts.data_type)?;
                let mut args = vec![Expr::Literal(Literal::String(s))];
                args.push(Expr::Literal(Literal::String(type_name)));
                args.extend(extra);
                Ok(Expr::Function {
                    name: "CAST".to_string(),
                    args,
                    distinct: false,
                })
            } else {
                // MySQL 8.0 only accepts the four temporal typed literals
                // (DATE / TIME / DATETIME / TIMESTAMP). Other forms such as
                // INTEGER '42', BINARY '00', and CHAR with a CJK literal are syntax
                // errors — silently downgrading them to a plain string
                // literal would hide a class of user mistakes and violates
                // the project's "unsupported → explicit error" rule.
                // The user can spell an explicit CAST(<literal> AS <type>)
                // when that is what they meant.
                parse_error_from_sql_span(
                    expr.span(),
                    "typed literal not supported: only DATE, TIME, DATETIME, TIMESTAMP accept the `TYPE 'value'` form — use CAST(<literal> AS <type>) instead",
                    vec![],
                )
            }
        }
        SqlExpr::MatchAgainst {
            columns,
            match_value,
            opt_search_modifier,
        } => {
            let query = match convert_value(match_value, expr.span())? {
                query @ Expr::Literal(Literal::String(_)) | query @ Expr::Placeholder(_) => query,
                _ => {
                    return parse_error_from_sql_span(
                        expr.span(),
                        "MATCH ... AGAINST expects a string literal or parameter",
                        vec![],
                    )
                }
            };
            let mode = match opt_search_modifier {
                None | Some(sqlparser::ast::SearchModifier::InNaturalLanguageMode) => {
                    FtMode::NaturalLanguage
                }
                Some(sqlparser::ast::SearchModifier::InBooleanMode) => FtMode::Boolean,
                Some(sqlparser::ast::SearchModifier::InNaturalLanguageModeWithQueryExpansion)
                | Some(sqlparser::ast::SearchModifier::WithQueryExpansion) => {
                    return parse_error_from_sql_span(
                        expr.span(),
                        "MATCH ... AGAINST WITH QUERY EXPANSION is not supported",
                        vec![],
                    )
                }
            };
            // Normalize into the internal scalar function __match_against(mode, query, col1, col2, ...):
            // columns are passed as Identifier args so existing column-reference collection / column pruning handles them automatically.
            let mut args = Vec::with_capacity(columns.len() + 2);
            args.push(Expr::Literal(Literal::String(mode.as_str().to_string())));
            args.push(query);
            for name in columns {
                args.push(Expr::Identifier(match_against_column(name)?));
            }
            Ok(Expr::Function {
                name: FT_MATCH_FUNCTION.to_string(),
                args,
                distinct: false,
            })
        }
        _ => parse_error_from_sql_span(expr.span(), "unsupported expression", vec![]),
    }
}

/// Convert one homogeneous AND/OR tree without recursive descent through its
/// binary spine. The explicit work list also handles a right-deep tree while
/// retaining the operands' SQL order in a balanced tree. AND and OR are associative under SQL three-valued logic.
fn convert_logical_chain(expr: &SqlExpr, target: &SqlBinaryOperator) -> Result<Expr, ParseError> {
    let same_operator = |op: &SqlBinaryOperator| {
        matches!(
            (target, op),
            (SqlBinaryOperator::And, SqlBinaryOperator::And)
                | (SqlBinaryOperator::Or, SqlBinaryOperator::Or)
        )
    };
    let mut work = vec![expr];
    let mut operands = Vec::new();
    while let Some(current) = work.pop() {
        match current {
            SqlExpr::BinaryOp { left, op, right } if same_operator(op) => {
                work.push(right);
                work.push(left);
            }
            _ => operands.push(current),
        }
    }

    let internal_op = match target {
        SqlBinaryOperator::And => BinaryOp::And,
        SqlBinaryOperator::Or => BinaryOp::Or,
        _ => unreachable!("logical-chain converter only accepts AND/OR"),
    };
    let mut converted = Vec::with_capacity(operands.len());
    for operand in operands {
        let mut operand = convert_expr(operand)?;
        numeric_literal_operand(&mut operand);
        converted.push(operand);
    }
    Ok(
        super::boolean::balanced_boolean_chain(converted, internal_op)
            .expect("logical chain has operands"),
    )
}

fn system_variable_expr(raw: &str) -> Result<Expr, ParseError> {
    let Some(rest) = raw.strip_prefix("@@") else {
        return Err(parse_error_at_zero("expected @@ system variable"));
    };
    let lowered = rest.to_ascii_lowercase();
    let (scope, name) = if let Some(name) = lowered.strip_prefix("session.") {
        (SystemVariableScope::Session, name)
    } else if let Some(name) = lowered.strip_prefix("local.") {
        (SystemVariableScope::Local, name)
    } else if let Some(name) = lowered.strip_prefix("global.") {
        (SystemVariableScope::Global, name)
    } else {
        (SystemVariableScope::ImplicitSession, lowered.as_str())
    };
    if name.is_empty() {
        return Err(parse_error_at_zero("system variable name is empty"));
    }
    Ok(Expr::SystemVariable {
        scope,
        name: name.to_string(),
    })
}

pub(super) fn convert_value(
    value: &SqlValue,
    span: sqlparser::tokenizer::Span,
) -> Result<Expr, ParseError> {
    let literal = match value {
        SqlValue::Null => Literal::Null,
        SqlValue::Boolean(value) => Literal::Bool(*value),
        // sqlparser represents @`quoted name` as Placeholder after removing
        // quotes. A name beginning with @ is still a user variable here;
        // system variables arrive as identifiers, and parameters carry ?N.
        SqlValue::Placeholder(name) if name.starts_with('@') => {
            return Ok(Expr::UserVariable(name[1..].to_ascii_lowercase()))
        }
        SqlValue::Placeholder(name) => {
            return adapter::parameter_index(name).map(Expr::Placeholder)
        }
        SqlValue::Number(value, _) => parse_number_literal(value, span)?,
        // Hex literal `0xCAFE` (sqlparser strips the `0x` prefix; payload is
        // raw hex digits) and bit literal `b'1010'` (payload is `'0'`/`'1'`s).
        // Decode to bytes here and carry provenance via dedicated `Literal`
        // variants so arithmetic / bitwise eval can coerce them to BIGINT
        // UNSIGNED without auto-coercing generic BLOB column values.
        SqlValue::HexStringLiteral(s) => {
            if s.is_empty() && !raw_source_is_x_hex(span).unwrap_or(false) {
                return Err(parse_error_at_zero(
                    "0x requires at least one hexadecimal digit",
                ));
            }
            // MySQL 8.0 accepts an odd-length `0x…` (implicit leading
            // zero) but rejects an odd-length `X'…'` with a syntax
            // error. sqlparser flattens both into the same token, so we
            // sniff the original source at the token span to recover
            // provenance. When the raw text starts with `X`/`x` the
            // literal is the `X'…'` form.
            if s.len() % 2 != 0 {
                if raw_source_is_x_hex(span).unwrap_or(false) {
                    return parse_error_from_sql_span(
                        span,
                        format!("hex string literal must have even number of hex digits: X'{s}'"),
                        vec![],
                    );
                }
            }
            match decode_hex_bytes(s) {
                Some(bytes) => Literal::HexBytes(bytes),
                None => {
                    return parse_error_from_sql_span(
                        span,
                        format!("invalid hex literal: {s}"),
                        vec![],
                    );
                }
            }
        }
        SqlValue::SingleQuotedByteStringLiteral(s) | SqlValue::DoubleQuotedByteStringLiteral(s) => {
            match decode_bit_string(s) {
                Some(bytes) => Literal::BitBytes(bytes),
                None => {
                    return parse_error_from_sql_span(
                        span,
                        format!("invalid bit literal: {s}"),
                        vec![],
                    );
                }
            }
        }
        // Other string-literal variants: skip the catch-all
        // `value.clone().into_string()` which clones the entire SqlValue
        // enum just to extract the inner String. Direct String clones are
        // the same cost minus the enum-clone overhead.
        SqlValue::SingleQuotedString(s)
        | SqlValue::DoubleQuotedString(s)
        | SqlValue::TripleSingleQuotedString(s)
        | SqlValue::TripleDoubleQuotedString(s)
        | SqlValue::EscapedStringLiteral(s) => Literal::String(s.clone()),
        // `N'…'` is equivalent to `_utf8'…'` in MySQL 8.0 — the payload
        // must be interpreted as utf8 regardless of the client's
        // `character_set_client`. Lower it to the same CONVERT_USING
        // form the `_utf8'…'` introducer produces so downstream
        // charset-aware paths see one consistent shape.
        SqlValue::NationalStringLiteral(s) => {
            return Ok(Expr::Function {
                name: "CONVERT_USING".to_string(),
                args: vec![
                    Expr::Literal(Literal::String(s.clone())),
                    Expr::Literal(Literal::String("utf8mb3".to_string())),
                    Expr::Literal(Literal::Bool(true)),
                ],
                distinct: false,
            });
        }
        value => {
            if let Some(s) = value.clone().into_string() {
                Literal::String(s)
            } else {
                return parse_error_from_sql_span(span, "unsupported literal", vec![]);
            }
        }
    };
    Ok(Expr::Literal(literal))
}

fn validate_function_syntax(
    function: &sqlparser::ast::Function,
    name: &str,
) -> Result<(), ParseError> {
    let sqlparser::ast::Function {
        name: _,
        uses_odbc_syntax: _,
        parameters,
        args,
        filter,
        null_treatment,
        over,
        within_group: _,
    } = function;
    if !matches!(parameters, FunctionArguments::None) || filter.is_some() {
        return Err(parse_error_at_zero(
            "function parameters / FILTER are not supported",
        ));
    }
    let named = |names: &[&str]| {
        names
            .iter()
            .any(|candidate| name.eq_ignore_ascii_case(candidate))
    };
    if over.is_some() && named(&["ROW", "DEFAULT"]) {
        return Err(parse_error_at_zero(format!(
            "OVER is not allowed for {name}"
        )));
    }
    if null_treatment.is_some()
        && (over.is_none() || !named(&["FIRST_VALUE", "LAST_VALUE", "NTH_VALUE", "LAG", "LEAD"]))
    {
        return Err(parse_error_at_zero(format!(
            "NULL treatment is not allowed for {name}"
        )));
    }
    if let FunctionArguments::List(list) = args {
        let duplicate_supported = match list.duplicate_treatment {
            None => true,
            Some(DuplicateTreatment::Distinct) => named(&[
                "COUNT",
                "SUM",
                "AVG",
                "MIN",
                "MAX",
                "GROUP_CONCAT",
                "ST_COLLECT",
            ]),
            Some(DuplicateTreatment::All) => named(&[
                "COUNT",
                "SUM",
                "AVG",
                "MIN",
                "MAX",
                "BIT_AND",
                "BIT_OR",
                "BIT_XOR",
                "STD",
                "STDDEV",
                "STDDEV_POP",
                "STDDEV_SAMP",
                "VARIANCE",
                "VAR_POP",
                "VAR_SAMP",
                "JSON_ARRAYAGG",
                "JSON_OBJECTAGG",
                "ST_COLLECT",
            ]),
        };
        if !duplicate_supported {
            return Err(parse_error_at_zero(format!(
                "duplicate treatment is not allowed for {name}"
            )));
        }
        let wildcard = list.args.iter().any(|arg| {
            matches!(
                arg,
                FunctionArg::Unnamed(
                    FunctionArgExpr::Wildcard | FunctionArgExpr::QualifiedWildcard(_)
                )
            )
        });
        if wildcard
            && !(name.eq_ignore_ascii_case("COUNT")
                && matches!(
                    list.args.as_slice(),
                    [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)]
                ))
        {
            return Err(parse_error_at_zero(format!(
                "wildcard argument is not allowed for {name}"
            )));
        }
        if over.is_some() && list.duplicate_treatment == Some(DuplicateTreatment::Distinct) {
            return Err(parse_error_with_spec_at_zero(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!(
                    "This version of MySQL doesn't yet support '{name}(DISTINCT ...) OVER (...)'"
                ),
            ));
        }
        if list.duplicate_treatment == Some(DuplicateTreatment::Distinct)
            && (list.args.iter().any(|arg| {
                matches!(
                    arg,
                    FunctionArg::Unnamed(
                        FunctionArgExpr::Wildcard | FunctionArgExpr::QualifiedWildcard(_)
                    )
                )
            }) || name.eq_ignore_ascii_case("JSON_ARRAYAGG")
                || name.eq_ignore_ascii_case("JSON_OBJECTAGG"))
        {
            return Err(parse_error_at_zero(
                "DISTINCT is not allowed in this function argument list",
            ));
        }
        let mut order = false;
        let mut separator = false;
        for clause in &list.clauses {
            match clause {
                FunctionArgumentClause::OrderBy(_)
                    if name.eq_ignore_ascii_case("GROUP_CONCAT") && !order && !separator =>
                {
                    order = true
                }
                FunctionArgumentClause::Separator(_)
                    if name.eq_ignore_ascii_case("GROUP_CONCAT") && !separator =>
                {
                    separator = true
                }
                _ => {
                    return Err(parse_error_at_zero(format!(
                        "unsupported or repeated argument clause in {name}: {clause}"
                    )))
                }
            }
        }
    }
    Ok(())
}

fn convert_function_args(args: &FunctionArguments) -> Result<Vec<Expr>, ParseError> {
    match args {
        FunctionArguments::None => Ok(Vec::new()),
        FunctionArguments::List(list) => list
            .args
            .iter()
            .map(|arg| match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => convert_expr(expr),
                FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Ok(Expr::Wildcard),
                FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(name)) => {
                    Ok(Expr::CompoundIdentifier(vec![
                        object_name_to_string(name)?,
                        "*".to_string(),
                    ]))
                }
                _ => parse_error_from_sql_span(arg.span(), "unsupported function argument", vec![]),
            })
            .collect(),
        FunctionArguments::Subquery(query) => {
            parse_error_from_sql_span(query.span(), "unsupported function subquery", vec![])
        }
    }
}

fn convert_cast_data_type(data_type: &SqlDataType) -> Result<(String, Vec<Expr>), ParseError> {
    Ok(match data_type {
        SqlDataType::Signed | SqlDataType::SignedInteger => ("SIGNED".to_string(), vec![]),
        SqlDataType::Unsigned | SqlDataType::UnsignedInteger => ("UNSIGNED".to_string(), vec![]),
        SqlDataType::Int(_)
        | SqlDataType::Integer(_)
        | SqlDataType::BigInt(_)
        | SqlDataType::TinyInt(_)
        | SqlDataType::SmallInt(_)
        | SqlDataType::MediumInt(_) => ("SIGNED".to_string(), vec![]),
        SqlDataType::Char(len) | SqlDataType::Character(len) => {
            let mut extra = vec![];
            if let Some(CharacterLength::IntegerLength { length, .. }) = len {
                validate_cast_character_length(*length, 4, "CHAR")?;
                extra.push(Expr::Literal(Literal::Int(*length as i64)));
            }
            ("CHAR".to_string(), extra)
        }
        SqlDataType::Varchar(len) | SqlDataType::CharVarying(len) => {
            let mut extra = vec![];
            if let Some(CharacterLength::IntegerLength { length, .. }) = len {
                validate_cast_character_length(*length, 4, "CHAR")?;
                extra.push(Expr::Literal(Literal::Int(*length as i64)));
            }
            ("CHAR".to_string(), extra)
        }
        SqlDataType::Text
        | SqlDataType::TinyText
        | SqlDataType::MediumText
        | SqlDataType::LongText
        | SqlDataType::Clob(_) => ("CHAR".to_string(), vec![]),
        SqlDataType::Date => ("DATE".to_string(), vec![]),
        SqlDataType::Datetime(p) | SqlDataType::Timestamp(p, _) => {
            ("DATETIME".to_string(), cast_fsp_extra(*p))
        }
        SqlDataType::Time(p, _) => ("TIME".to_string(), cast_fsp_extra(*p)),
        SqlDataType::Decimal(info) | SqlDataType::Dec(info) | SqlDataType::Numeric(info) => {
            let mut extra = vec![];
            match info {
                ExactNumberInfo::PrecisionAndScale(p, s) => {
                    let (p, s) = validate_decimal_precision_scale(*p, *s)?;
                    extra.push(Expr::Literal(Literal::Int(p as i64)));
                    extra.push(Expr::Literal(Literal::Int(s as i64)));
                }
                ExactNumberInfo::Precision(p) => {
                    let (p, _) = validate_decimal_precision_scale(*p, 0)?;
                    extra.push(Expr::Literal(Literal::Int(p as i64)));
                }
                ExactNumberInfo::None => {}
            }
            ("DECIMAL".to_string(), extra)
        }
        SqlDataType::Double(_) | SqlDataType::DoublePrecision | SqlDataType::Real => {
            ("DOUBLE".to_string(), vec![])
        }
        SqlDataType::Float(_) | SqlDataType::Float4 => ("FLOAT".to_string(), vec![]),
        SqlDataType::Binary(len) => {
            let mut extra = vec![];
            if let Some(n) = len {
                validate_cast_binary_length(*n)?;
                extra.push(Expr::Literal(Literal::Int(*n as i64)));
            }
            ("BINARY".to_string(), extra)
        }
        SqlDataType::Varbinary(len) => {
            let mut extra = vec![];
            if let Some(bl) = len {
                let n = match bl {
                    sqlparser::ast::BinaryLength::IntegerLength { length } => *length as i64,
                    sqlparser::ast::BinaryLength::Max => i64::MAX,
                };
                extra.push(Expr::Literal(Literal::Int(n)));
            }
            ("BINARY".to_string(), extra)
        }
        SqlDataType::Blob(_)
        | SqlDataType::TinyBlob
        | SqlDataType::MediumBlob
        | SqlDataType::LongBlob => ("BINARY".to_string(), vec![]),
        SqlDataType::JSON | SqlDataType::JSONB => ("JSON".to_string(), vec![]),
        // MySQL 8.0 has no `CAST(x AS BOOLEAN)` target — it must raise the
        // canonical "unsupported CAST target type" error, not silently degrade
        // to CHAR via the catch-all below (project rule: no silent no-op).
        SqlDataType::Bool | SqlDataType::Boolean => ("BOOLEAN".to_string(), vec![]),
        // sqlparser 0.61 has no native YEAR / NCHAR / VECTOR cast types;
        // the MySQL dialect surfaces them as Custom identifiers. Pass the
        // name through — the evaluator owns the supported-target list and
        // raises the canonical "unsupported CAST target type" error for
        // anything it doesn't know.
        SqlDataType::Custom(name, args) => {
            let upper = name.to_string().to_ascii_uppercase();
            match upper.as_str() {
                // NCHAR / NATIONAL CHAR is utf8-CHAR in MySQL 8.0. Keep
                // any length argument the parser captured as `Custom` so
                // `CAST(x AS NCHAR(10))` truncates like `CHAR(10)`.
                "NCHAR" => {
                    let mut extra = vec![];
                    if let Some(first) = args.first() {
                        let n = first.parse::<u64>().map_err(|_| {
                            numeric_spec_error(
                                crate::error::catalog::ER_TOO_BIG_DISPLAYWIDTH,
                                "Display width out of range for NCHAR (max = 4294967295)",
                            )
                        })?;
                        validate_cast_character_length(n, 3, "NCHAR")?;
                        extra.push(Expr::Literal(Literal::Int(n as i64)));
                    }
                    ("CHAR".to_string(), extra)
                }
                "VECTOR" => {
                    let (dim, datatype) = parse_vector_type(args, "CAST AS VECTOR")?;
                    let extra = dim
                        .map(|d| {
                            vec![
                                Expr::Literal(Literal::Int(d as i64)),
                                Expr::Literal(Literal::String(datatype.name().into())),
                            ]
                        })
                        .unwrap_or_default();
                    ("VECTOR".into(), extra)
                }
                _ => (upper, vec![]),
            }
        }
        // MySQL 8.0 has a fixed set of CAST target types (see
        // https://dev.mysql.com/doc/refman/8.0/en/cast-functions.html).
        // Any sqlparser-supported type we haven't mapped above is not a
        // valid MySQL CAST target — historically we silently downgraded
        // to CHAR, which is a project-rule violation ("no silent no-op").
        // Reject with a 1064 (syntax) since the parser owns this branch.
        other => {
            return Err(ParseError::at_zero(format!(
                "unsupported CAST target type: {other:?}"
            )));
        }
    })
}

/// Validate CAST/CONVERT result widths before expression evaluation.
///
/// This deliberately runs in AST conversion rather than in `eval_expr`.
/// MySQL rejects an invalid target definition even when the source expression
/// is NULL and EXPLAIN never evaluates the expression. It also prevents a
/// hostile query from reaching an unchecked `usize` conversion or attempting
/// a multi-gigabyte allocation merely to discover that the target is invalid.
fn validate_cast_character_length(
    characters: u64,
    max_bytes_per_char: u64,
    target: &str,
) -> Result<(), ParseError> {
    if characters > u32::MAX as u64 {
        return Err(numeric_spec_error(
            crate::error::catalog::ER_TOO_BIG_DISPLAYWIDTH,
            format!("Display width out of range for {target} (max = 4294967295)"),
        ));
    }
    if characters > u32::MAX as u64 / max_bytes_per_char {
        return Err(ParseError {
            spec: crate::error::catalog::ER_DATA_OUT_OF_RANGE,
            message: format!("Data out of range for CAST(... AS {target}({characters}))"),
            span: Span { start: 0, end: 0 },
            line: 1,
            column: 1,
            expected: Vec::new(),
        });
    }
    Ok(())
}

fn validate_cast_binary_length(bytes: u64) -> Result<(), ParseError> {
    if bytes > u32::MAX as u64 {
        return Err(numeric_spec_error(
            crate::error::catalog::ER_TOO_BIG_DISPLAYWIDTH,
            "Display width out of range for BINARY (max = 4294967295)",
        ));
    }
    Ok(())
}

fn convert_quantified_comparison(
    left: &SqlExpr,
    compare_op: &SqlBinaryOperator,
    quantifier: SubqueryQuantifier,
    right: &SqlExpr,
) -> Result<Expr, ParseError> {
    let right = match right {
        SqlExpr::Subquery(query) => {
            QuantifiedComparisonRight::Subquery(Box::new(convert_query_to_select(query)?))
        }
        _ => {
            return parse_error_from_sql_span(
                right.span(),
                "ANY/ALL/SOME comparison requires a subquery",
                vec![],
            )
        }
    };
    Ok(Expr::QuantifiedComparison {
        left: Box::new(convert_expr(left)?),
        op: convert_binary_op(compare_op)?,
        quantifier,
        right,
    })
}

fn is_sql_comparison_op(op: &SqlBinaryOperator) -> bool {
    matches!(
        op,
        SqlBinaryOperator::Eq
            | SqlBinaryOperator::NotEq
            | SqlBinaryOperator::Lt
            | SqlBinaryOperator::LtEq
            | SqlBinaryOperator::Gt
            | SqlBinaryOperator::GtEq
            | SqlBinaryOperator::Spaceship
    )
}

/// SOUNDS LIKE is a predicate whose operands are bit expressions. Keep
/// comparison rotation here, before lowering to ordinary scalar calls.
fn convert_sounds_like(left: &SqlExpr, right: &SqlExpr) -> Result<Expr, ParseError> {
    reject_predicate_as_predicate_lhs(left)?;
    if let SqlExpr::BinaryOp {
        left: cmp_left,
        op,
        right: cmp_right,
    } = left
    {
        if is_sql_comparison_op(op) {
            return Ok(Expr::Binary {
                left: Box::new(convert_expr(cmp_left)?),
                op: convert_binary_op(op)?,
                right: Box::new(convert_sounds_like(cmp_right, right)?),
            });
        }
    }
    let soundex = |expr| Expr::Function {
        name: "SOUNDEX".into(),
        args: vec![expr],
        distinct: false,
    };
    Ok(Expr::Binary {
        left: Box::new(soundex(convert_expr(left)?)),
        op: BinaryOp::Eq,
        right: Box::new(soundex(convert_expr(right)?)),
    })
}

fn is_unparenthesized_mysql_predicate(expr: &SqlExpr) -> bool {
    matches!(
        expr,
        SqlExpr::InList { .. }
            | SqlExpr::InSubquery { .. }
            | SqlExpr::Between { .. }
            | SqlExpr::Like { .. }
            | SqlExpr::RLike { .. }
            | SqlExpr::IsNull(_)
            | SqlExpr::IsNotNull(_)
            | SqlExpr::IsTrue(_)
            | SqlExpr::IsNotTrue(_)
            | SqlExpr::IsFalse(_)
            | SqlExpr::IsNotFalse(_)
            | SqlExpr::IsUnknown(_)
            | SqlExpr::IsNotUnknown(_)
            | SqlExpr::MemberOf(_)
    ) || matches!(expr, SqlExpr::BinaryOp { op: SqlBinaryOperator::Custom(operator), .. }
        if operator == super::adapter::SOUNDS_LIKE_OP || operator == super::adapter::DYNAMIC_LIKE)
}

fn is_unparenthesized_mysql_truth_test(expr: &SqlExpr) -> bool {
    matches!(
        expr,
        SqlExpr::IsTrue(_)
            | SqlExpr::IsNotTrue(_)
            | SqlExpr::IsFalse(_)
            | SqlExpr::IsNotFalse(_)
            | SqlExpr::IsUnknown(_)
            | SqlExpr::IsNotUnknown(_)
    )
}

fn reject_predicate_as_predicate_lhs(expr: &SqlExpr) -> Result<(), ParseError> {
    if is_unparenthesized_mysql_predicate(expr) {
        return parse_error_from_sql_span(
            expr.span(),
            "invalid unparenthesized predicate chain",
            vec![],
        );
    }
    Ok(())
}

fn convert_binary_op(op: &SqlBinaryOperator) -> Result<BinaryOp, ParseError> {
    Ok(match op {
        SqlBinaryOperator::Or => BinaryOp::Or,
        SqlBinaryOperator::And => BinaryOp::And,
        SqlBinaryOperator::Eq => BinaryOp::Eq,
        SqlBinaryOperator::Spaceship => BinaryOp::NullSafeEq,
        SqlBinaryOperator::NotEq => BinaryOp::NotEq,
        SqlBinaryOperator::Lt => BinaryOp::Lt,
        SqlBinaryOperator::LtEq => BinaryOp::LtEq,
        SqlBinaryOperator::Gt => BinaryOp::Gt,
        SqlBinaryOperator::GtEq => BinaryOp::GtEq,
        SqlBinaryOperator::Plus => BinaryOp::Add,
        SqlBinaryOperator::Minus => BinaryOp::Sub,
        SqlBinaryOperator::Multiply => BinaryOp::Mul,
        SqlBinaryOperator::Divide => BinaryOp::Div,
        SqlBinaryOperator::Modulo => BinaryOp::Mod,
        SqlBinaryOperator::Xor => BinaryOp::Xor,
        SqlBinaryOperator::BitwiseAnd => BinaryOp::BitAnd,
        SqlBinaryOperator::BitwiseOr => BinaryOp::BitOr,
        SqlBinaryOperator::BitwiseXor => BinaryOp::BitXor,
        SqlBinaryOperator::PGBitwiseShiftLeft => BinaryOp::ShiftLeft,
        SqlBinaryOperator::PGBitwiseShiftRight => BinaryOp::ShiftRight,
        // Freeze `||` semantics at parse/PREPARE time. This is required by
        // MySQL prepared-statement semantics and also keeps persisted
        // canonical expressions independent of the executing session.
        SqlBinaryOperator::StringConcat => {
            if crate::parser::current_parser_sql_mode_bits()
                & crate::engine::api::SQL_MODE_PIPES_AS_CONCAT
                != 0
            {
                BinaryOp::Concat
            } else {
                BinaryOp::Or
            }
        }
        SqlBinaryOperator::Arrow => BinaryOp::Arrow,
        SqlBinaryOperator::LongArrow => BinaryOp::LongArrow,
        _ => {
            return parse_error_from_sql_span(
                sqlparser::tokenizer::Span::empty(),
                "unsupported binary operator",
                vec![],
            )
        }
    })
}

fn convert_select_item(item: &SqlSelectItem) -> Result<SelectItem, ParseError> {
    match item {
        SqlSelectItem::Wildcard(_) => Ok(SelectItem::Wildcard),
        SqlSelectItem::QualifiedWildcard(kind, _) => match kind {
            SelectItemQualifiedWildcardKind::ObjectName(name) => {
                Ok(SelectItem::QualifiedWildcard(object_name_to_string(name)?))
            }
            SelectItemQualifiedWildcardKind::Expr(expr) => {
                parse_error_from_sql_span(expr.span(), "unsupported qualified wildcard", vec![])
            }
        },
        SqlSelectItem::UnnamedExpr(expr) => Ok(SelectItem::Expr {
            expr: convert_expr(expr)?,
            alias: None,
        }),
        SqlSelectItem::ExprWithAlias { expr, alias } => Ok(SelectItem::Expr {
            expr: convert_expr(expr)?,
            alias: Some(alias.value.clone()),
        }),
    }
}

fn convert_expr_list(exprs: &[SqlExpr]) -> Result<Vec<Expr>, ParseError> {
    exprs.iter().map(convert_expr).collect()
}

/// Convert a `@var := expr` assignment (sqlparser `BinaryOperator::Assignment`,
/// left-associative at `=` precedence) into right-associative nested
/// `__set_user_var` functions, matching MySQL's `:=` semantics. A chain
/// `@a := @b := 5` (parsed `((@a := @b) := 5)`) becomes
/// `@a := (@b := 5)` so every variable receives the final value (issue #86).
fn convert_user_var_assignment(left: &SqlExpr, right: &SqlExpr) -> Result<Expr, ParseError> {
    // Collect the chain's variables walking down the left spine. The
    // outermost `right` is the value; each left-spine node contributes its
    // own right operand (an inner variable) and the leftmost leaf is the
    // first variable.
    let mut vars = Vec::new();
    let mut cursor = left;
    loop {
        match cursor {
            SqlExpr::Nested(inner) => cursor = inner,
            SqlExpr::BinaryOp {
                left: inner_left,
                op: SqlBinaryOperator::Assignment,
                right: inner_right,
            } => {
                vars.push(user_var_assign_target(inner_right)?);
                cursor = inner_left;
            }
            _ => {
                vars.push(user_var_assign_target(cursor)?);
                break;
            }
        }
    }
    // `vars` is ordered from the innermost (rightmost-typed) variable to the
    // outermost (leftmost) variable, which is exactly the order to nest:
    // innermost `__set(v, value)` first, then wrap outward.
    let mut expr = convert_expr(right)?;
    for name in vars {
        expr = Expr::Function {
            name: USER_VAR_ASSIGN_FUNCTION.to_string(),
            args: vec![Expr::Literal(Literal::String(name)), expr],
            distinct: false,
        };
    }
    Ok(expr)
}

pub(super) fn convert_limit_clause(
    limit_clause: Option<&LimitClause>,
) -> Result<(Option<LimitValue>, Option<LimitValue>), LimitClauseError> {
    match limit_clause {
        None => Ok((None, None)),
        Some(LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        }) if limit_by.is_empty() => Ok((
            limit
                .as_ref()
                .map(convert_limit_value)
                .transpose()
                .map_err(LimitClauseError::from)?,
            offset
                .as_ref()
                .map(|offset| convert_limit_value(&offset.value))
                .transpose()
                .map_err(LimitClauseError::from)?,
        )),
        Some(LimitClause::OffsetCommaLimit { offset, limit }) => {
            let mut count = convert_limit_value(limit).map_err(LimitClauseError::from)?;
            if let LimitValue::Placeholder(index) = count {
                count = LimitValue::OffsetCommaPlaceholder(index);
            }
            Ok((
                Some(count),
                Some(convert_limit_value(offset).map_err(LimitClauseError::from)?),
            ))
        }
        _ => Err(LimitClauseError::Unsupported),
    }
}

fn convert_limit_value(expr: &SqlExpr) -> Result<LimitValue, LimitValueError> {
    match expr {
        SqlExpr::Value(value) => match &value.value {
            SqlValue::Number(text, _) => parse_limit_number(text),
            SqlValue::Placeholder(name) => adapter::parameter_index(name)
                .map(LimitValue::Placeholder)
                .map_err(|err| LimitValueError::Invalid(err.message)),
            SqlValue::Null => Err(LimitValueError::Invalid(
                "LIMIT/OFFSET value must not be NULL".to_string(),
            )),
            _ => Err(LimitValueError::Invalid(
                "LIMIT/OFFSET value must be a non-negative integer".to_string(),
            )),
        },
        SqlExpr::UnaryOp {
            op: SqlUnaryOperator::Minus,
            ..
        } => Err(LimitValueError::Invalid(
            "LIMIT/OFFSET value must be a non-negative integer".to_string(),
        )),
        _ => Err(LimitValueError::Unsupported),
    }
}

enum DmlKind {
    Update,
    Delete,
}

/// Both DML forms share the supported join shapes and validation order.
/// RIGHT and NATURAL/USING normalization remains in the executor.
fn unsupported_dml_join(kind: DmlKind, target: &TableRef, joins: &[Join]) -> Option<&'static str> {
    let (temporal, derived, unsupported) = match kind {
        DmlKind::Update => (
            "FOR SYSTEM_TIME AS OF on an UPDATE target or source",
            "UPDATE join with derived table",
            "UPDATE SEMI/ANTI/FULL join",
        ),
        DmlKind::Delete => (
            "FOR SYSTEM_TIME AS OF on a DELETE target or source",
            "DELETE join with derived table",
            "DELETE supports only INNER / CROSS / LEFT / RIGHT joins",
        ),
    };
    if target.system_time.is_some() || joins.iter().any(|join| join.table.system_time.is_some()) {
        return Some(temporal);
    }
    for join in joins {
        if join.derived.is_some() {
            return Some(derived);
        }
        if !matches!(
            join.join_type,
            JoinType::Inner | JoinType::Cross | JoinType::Left | JoinType::Right
        ) {
            return Some(unsupported);
        }
    }
    None
}

/// Plain table modifiers must be rejected consistently in queries and DML.
/// Function-specific modifiers (for example generate_series ordinality) are
/// handled by their own table-factor conversion.
fn validate_plain_table_modifiers(
    ordinality: bool,
    json_path: bool,
    sample: bool,
) -> Result<(), String> {
    if ordinality || json_path || sample {
        Err("table ordinality, JSON path, and TABLESAMPLE are not supported".to_owned())
    } else {
        Ok(())
    }
}

/// The target metadata needed by single-table DML. Index hints retain their
/// existing handling; aliases and partitions are lowered exactly once.
struct DmlTarget {
    name: String,
    alias: Option<String>,
    partitions: Vec<String>,
}

fn convert_dml_target(table: &TableWithJoins) -> Result<DmlTarget, String> {
    if !table.joins.is_empty() {
        return Err("JOIN".to_string());
    }
    match &table.relation {
        TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            partitions,
            with_ordinality,
            json_path,
            sample,
            index_hints: _,
        } if args.is_none() && with_hints.is_empty() => {
            if version.is_some() {
                return Err("FOR SYSTEM_TIME AS OF is not allowed on a DML target".to_string());
            }
            validate_plain_table_modifiers(
                *with_ordinality,
                json_path.is_some(),
                sample.is_some(),
            )?;
            Ok(DmlTarget {
                name: object_name_to_string(name).map_err(|err| err.message)?,
                alias: alias.as_ref().map(table_alias_name),
                partitions: partitions.iter().map(|ident| ident.value.clone()).collect(),
            })
        }
        _ => Err("table reference".to_string()),
    }
}

/// Table lowering distinguishes invalid syntax from unsupported features so
/// nested joins and derived tables preserve the MySQL parse-error code.
enum TableConversionError {
    Unsupported(String),
    Syntax(ParseError),
}

impl From<String> for TableConversionError {
    fn from(kind: String) -> Self {
        Self::Unsupported(kind)
    }
}

impl From<ParseError> for TableConversionError {
    fn from(error: ParseError) -> Self {
        Self::Syntax(error)
    }
}

impl TableConversionError {
    fn into_statement_result(self, sql: &str) -> Result<Statement, ParseError> {
        match self {
            Self::Unsupported(kind) => Ok(Statement::Unsupported {
                kind,
                sql: sql.to_string(),
            }),
            Self::Syntax(error) => Err(error),
        }
    }
}

fn convert_table_with_joins(
    table: &TableWithJoins,
) -> Result<(TableRef, Option<SubQuery>, bool, Vec<Join>), TableConversionError> {
    let (base, derived, base_lateral) = convert_table_factor(&table.relation)?;
    let mut joins = Vec::new();
    for join in &table.joins {
        let (table, join_derived, lateral) = convert_table_factor(&join.relation)?;
        let (join_type, condition, using_columns, natural, straight) =
            convert_join_operator(&join.join_operator)?;
        joins.push(Join {
            table,
            join_type,
            straight,
            condition,
            using_columns,
            natural,
            derived: join_derived,
            lateral,
        });
    }
    Ok((base, derived, base_lateral, joins))
}

fn convert_table_factor(
    factor: &TableFactor,
) -> Result<(TableRef, Option<SubQuery>, bool), TableConversionError> {
    match factor {
        // PostgreSQL `generate_series(start, stop [, step])` table function.
        // sqlparser parses this as `TableFactor::Table { args: Some(..) }`; the
        // ordinary Table arm below is guarded on `args.is_none()`, so the series
        // generator is intercepted here. Arguments are carried verbatim and
        // evaluated as constants by the planner. Any other function-style table
        // reference (`FROM some_func(..)`) still falls through to the catch-all
        // `_` arm and is rejected (1235).
        TableFactor::Table {
            name,
            alias,
            args: Some(table_args),
            with_ordinality,
            with_hints,
            version,
            partitions,
            json_path,
            sample,
            index_hints,
        } if object_name_is_generate_series(name) => {
            if !with_hints.is_empty()
                || version.is_some()
                || !partitions.is_empty()
                || json_path.is_some()
                || sample.is_some()
                || !index_hints.is_empty()
            {
                return Err(
                    "generate_series table modifiers other than WITH ORDINALITY are not supported"
                        .to_owned()
                        .into(),
                );
            }
            let exprs = generate_series_args(table_args)?;
            if exprs.len() < 2 || exprs.len() > 3 {
                return Err(format!(
                    "generate_series requires 2 or 3 arguments, got {}",
                    exprs.len()
                )
                .into());
            }
            let alias_name = alias.as_ref().map(table_alias_name);
            let column_aliases = table_alias_columns(alias.as_ref());
            let mut iter = exprs.into_iter();
            let start = iter.next().expect("checked len >= 2");
            let stop = iter.next().expect("checked len >= 2");
            let step = iter.next();
            let spec = GenerateSeriesSpec {
                start,
                stop,
                step,
                with_ordinality: *with_ordinality,
                column_aliases,
            };
            Ok((
                TableRef {
                    name: alias_name
                        .clone()
                        .unwrap_or_else(|| "generate_series".to_string()),
                    alias: alias_name,
                    system_time: None,
                    index_hints: vec![],
                    partitions: vec![],
                    column_aliases: Vec::new(),
                    generate_series: Some(Box::new(spec)),
                },
                None,
                false,
            ))
        }
        TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            sample,
            index_hints,
        } if args.is_none() && with_hints.is_empty() => {
            validate_plain_table_modifiers(
                *with_ordinality,
                json_path.is_some(),
                sample.is_some(),
            )?;
            let system_time = match version {
                None => None,
                Some(sqlparser::ast::TableVersion::ForSystemTimeAsOf(expr)) => {
                    Some(SystemTimeAsOf {
                        expr: Box::new(convert_expr(expr)?),
                        binding_id: 0,
                    })
                }
                Some(_) => {
                    return Err("only FOR SYSTEM_TIME AS OF table versioning is supported"
                        .to_string()
                        .into())
                }
            };
            Ok((
                TableRef {
                    name: object_name_to_string(name)?,
                    alias: alias.as_ref().map(table_alias_name),
                    system_time,
                    index_hints: convert_index_hints(index_hints),
                    partitions: partitions.iter().map(|i| i.value.clone()).collect(),
                    column_aliases: Vec::new(),
                    generate_series: None,
                },
                None,
                false,
            ))
        }
        TableFactor::Derived {
            lateral,
            subquery,
            alias,
            sample: None,
        } => {
            let alias_name = alias
                .as_ref()
                .map(table_alias_name)
                .unwrap_or_else(|| "derived".to_string());
            let mut subquery = convert_query_to_subquery(subquery)?;
            let alias_columns = table_alias_columns(alias.as_ref());
            let mut deferred_aliases = Vec::new();
            if !alias_columns.is_empty() {
                // P2-C6b: column-list aliases on a derived table (`AS x(a, b)`)
                // rename the subquery's output columns. Supported for a VALUES
                // row constructor and for a plain SELECT with a statically
                // known projection width; a wildcard projection or a set
                // operation (where the width isn't resolvable at parse time)
                // is a clear error, never a silent drop.
                match subquery.as_select_mut() {
                    Some(inner) if inner.values.is_some() => {
                        let values = inner.values.as_mut().unwrap();
                        let width = values.columns.len();
                        if alias_columns.len() != width {
                            return Err(format!(
                                "derived table has {width} columns but {} column aliases",
                                alias_columns.len()
                            )
                            .into());
                        }
                        values.columns = alias_columns;
                    }
                    Some(inner) => {
                        if inner
                            .projection
                            .iter()
                            .any(|it| !matches!(it, SelectItem::Expr { .. }))
                        {
                            deferred_aliases = alias_columns.clone();
                        }
                        if deferred_aliases.is_empty() {
                            let width = inner.projection.len();
                            if alias_columns.len() != width {
                                return Err(format!(
                                    "derived table has {width} columns but {} column aliases",
                                    alias_columns.len()
                                )
                                .into());
                            }
                            for (item, new_name) in
                                inner.projection.iter_mut().zip(alias_columns.into_iter())
                            {
                                if let SelectItem::Expr { alias, .. } = item {
                                    *alias = Some(new_name);
                                }
                            }
                        }
                    }
                    None => {
                        deferred_aliases = alias_columns;
                    }
                }
            }
            Ok((
                TableRef {
                    name: alias_name.clone(),
                    alias: Some(alias_name),
                    system_time: None,
                    index_hints: vec![],
                    partitions: vec![],
                    column_aliases: deferred_aliases,
                    generate_series: None,
                },
                Some(subquery),
                *lateral,
            ))
        }
        TableFactor::JsonTable {
            json_expr,
            json_path,
            columns,
            alias,
        } => {
            let alias_name = alias
                .as_ref()
                .map(table_alias_name)
                .unwrap_or_else(|| "json_table".to_string());
            let source = JsonTableSource {
                expr: convert_expr(json_expr)?,
                path: json_table_path_value(json_path)?,
                columns: convert_json_table_columns(columns)?,
            };
            let select = Select {
                projection: vec![SelectItem::Wildcard],
                json_table: Some(source),
                ..Select::default()
            };
            Ok((
                TableRef {
                    name: alias_name.clone(),
                    alias: Some(alias_name),
                    system_time: None,
                    index_hints: vec![],
                    partitions: vec![],
                    column_aliases: Vec::new(),
                    generate_series: None,
                },
                Some(SubQuery::Select(Box::new(select))),
                // JSON_TABLE is implicitly lateral in MySQL 8.0: a table to
                // its left may be referenced without spelling LATERAL.
                true,
            ))
        }
        TableFactor::NestedJoin {
            table_with_joins,
            alias,
        } => {
            // Preserve a parenthesised join as its own query block instead of
            // flattening it into the surrounding join list.  Flattening is
            // only semantics-preserving for pure INNER/CROSS joins; for
            // LEFT/RIGHT joins it changes which rows are null-extended.
            //
            // The executor's derived-source bridge already provides the
            // required recursive materialisation boundary.  An unaliased
            // nested join keeps each leaf table qualifier visible to the
            // outer ON clause (MySQL extension); an explicit alias scopes the
            // whole group in the normal derived-table manner.
            let (base, base_derived, _base_lateral, joins) =
                convert_table_with_joins(table_with_joins)?;
            let nested = Select {
                projection: vec![SelectItem::Wildcard],
                table: Some(base.name.clone()),
                from: vec![base],
                derived: base_derived,
                no_merge: true,
                joins,
                ..Select::default()
            };
            let explicit_alias = alias.as_ref().map(table_alias_name);
            let internal_name = if explicit_alias.is_some() {
                "$nested_join_scoped"
            } else {
                "$nested_join_preserve"
            };
            Ok((
                TableRef {
                    name: internal_name.to_string(),
                    alias: explicit_alias,
                    system_time: None,
                    index_hints: Vec::new(),
                    partitions: Vec::new(),
                    column_aliases: Vec::new(),
                    generate_series: None,
                },
                Some(SubQuery::Select(Box::new(nested))),
                false,
            ))
        }
        _ => Err("table reference".to_string().into()),
    }
}

fn convert_json_table_columns(
    columns: &[sqlparser::ast::JsonTableColumn],
) -> Result<Vec<JsonTableColumnDef>, ParseError> {
    columns
        .iter()
        .map(|column| match column {
            sqlparser::ast::JsonTableColumn::ForOrdinality(name) => {
                Ok(JsonTableColumnDef::Ordinality {
                    name: name.value.clone(),
                })
            }
            sqlparser::ast::JsonTableColumn::Named(named) => {
                let (data_type, unsigned) =
                    convert_data_type_with_unsigned("", named.r#type.clone())?;
                Ok(JsonTableColumnDef::Named {
                    name: named.name.value.clone(),
                    data_type,
                    unsigned,
                    path: json_table_path_value(&named.path)?,
                    exists: named.exists,
                    on_empty: convert_json_table_on_response(named.on_empty.as_ref())?,
                    on_error: convert_json_table_on_response(named.on_error.as_ref())?,
                })
            }
            sqlparser::ast::JsonTableColumn::Nested(nested) => Ok(JsonTableColumnDef::Nested {
                path: json_table_path_value(&nested.path)?,
                columns: convert_json_table_columns(&nested.columns)?,
            }),
        })
        .collect()
}

fn convert_json_table_on_response(
    response: Option<&sqlparser::ast::JsonTableColumnErrorHandling>,
) -> Result<JsonTableOnResponse, ParseError> {
    Ok(match response {
        None | Some(sqlparser::ast::JsonTableColumnErrorHandling::Null) => {
            JsonTableOnResponse::Null
        }
        Some(sqlparser::ast::JsonTableColumnErrorHandling::Error) => JsonTableOnResponse::Error,
        Some(sqlparser::ast::JsonTableColumnErrorHandling::Default(value)) => {
            JsonTableOnResponse::Default(convert_value(value, sqlparser::tokenizer::Span::empty())?)
        }
    })
}

fn convert_index_hints(hints: &[TableIndexHints]) -> Vec<IndexHint> {
    hints
        .iter()
        .map(|h| {
            let hint_type = match h.hint_type {
                TableIndexHintType::Use => IndexHintType::Use,
                TableIndexHintType::Ignore => IndexHintType::Ignore,
                TableIndexHintType::Force => IndexHintType::Force,
            };
            let scope = match h.for_clause {
                None => IndexHintScope::All,
                Some(TableIndexHintForClause::Join) => IndexHintScope::Join,
                Some(TableIndexHintForClause::OrderBy) => IndexHintScope::OrderBy,
                Some(TableIndexHintForClause::GroupBy) => IndexHintScope::GroupBy,
            };
            let index_names = h.index_names.iter().map(|id| id.value.clone()).collect();
            IndexHint {
                hint_type,
                scope,
                index_names,
            }
        })
        .collect()
}

fn convert_join_operator(
    operator: &JoinOperator,
) -> Result<(JoinType, Option<Expr>, Vec<String>, bool, bool), TableConversionError> {
    let (join_type, constraint) = match operator {
        JoinOperator::Join(constraint)
        | JoinOperator::Inner(constraint)
        | JoinOperator::StraightJoin(constraint) => (JoinType::Inner, constraint),
        JoinOperator::Left(constraint) | JoinOperator::LeftOuter(constraint) => {
            (JoinType::Left, constraint)
        }
        JoinOperator::Right(constraint) | JoinOperator::RightOuter(constraint) => {
            (JoinType::Right, constraint)
        }
        JoinOperator::FullOuter(constraint) => (JoinType::Full, constraint),
        JoinOperator::CrossJoin(constraint) => (JoinType::Cross, constraint),
        _ => return Err("JOIN type".to_string().into()),
    };
    // sqlparser permits an unconstrained outer join, but MySQL requires
    // ON/USING unless it is NATURAL. WHERE filters the joined result and must
    // never be treated as a replacement for an outer join's match condition.
    if matches!(join_type, JoinType::Left | JoinType::Right | JoinType::Full)
        && matches!(constraint, JoinConstraint::None)
    {
        return Err(ParseError::at_zero(
            "Non-NATURAL outer JOIN requires ON or USING; WHERE is not a join condition",
        )
        .into());
    }
    let (condition, using_columns, natural) = match constraint {
        JoinConstraint::On(expr) => (Some(convert_expr(expr)?), Vec::new(), false),
        JoinConstraint::None => (None, Vec::new(), false),
        JoinConstraint::Using(columns) => (None, convert_join_using_columns(columns)?, false),
        JoinConstraint::Natural => (None, Vec::new(), true),
    };
    Ok((
        join_type,
        condition,
        using_columns,
        natural,
        matches!(operator, JoinOperator::StraightJoin(_)),
    ))
}

fn convert_join_using_columns(columns: &[ObjectName]) -> Result<Vec<String>, String> {
    let mut out = Vec::with_capacity(columns.len());
    let mut seen = std::collections::HashSet::new();
    for column in columns {
        let name = match column.0.as_slice() {
            [ObjectNamePart::Identifier(ident)] => &ident.value,
            parts
                if parts
                    .iter()
                    .any(|part| matches!(part, ObjectNamePart::Function(_))) =>
            {
                return Err("unsupported object name".to_owned())
            }
            _ => return Err("JOIN USING qualified column".to_owned()),
        };
        if columns.len() > 1 && !seen.insert(name.to_ascii_lowercase()) {
            return Err("JOIN USING duplicate column".to_string());
        }
        out.push(name.clone());
    }
    Ok(out)
}

fn convert_group_by(
    group_by: &sqlparser::ast::GroupByExpr,
) -> Result<(Vec<Expr>, Vec<Vec<usize>>, GroupByModifier), ParseError> {
    const MAX_GROUP_KEYS: usize = 64;
    const MAX_GROUPING_SETS: usize = 4096;

    fn push_group_key(
        keys: &mut Vec<Expr>,
        expr: &sqlparser::ast::Expr,
    ) -> Result<usize, ParseError> {
        let converted = convert_expr(expr)?;
        if let Some(idx) = keys.iter().position(|existing| existing == &converted) {
            return Ok(idx);
        }
        if keys.len() >= MAX_GROUP_KEYS {
            return parse_error_from_sql_span(
                sqlparser::tokenizer::Span::empty(),
                "GROUP BY supports at most 64 grouping expressions",
                vec![],
            );
        }
        keys.push(converted);
        Ok(keys.len() - 1)
    }

    fn is_empty_grouping_set_sentinel(expr: &sqlparser::ast::Expr) -> Result<bool, ParseError> {
        let function = match expr {
            sqlparser::ast::Expr::Nested(inner) => return is_empty_grouping_set_sentinel(inner),
            sqlparser::ast::Expr::Function(function) => function,
            _ => return Ok(false),
        };
        let name = object_name_to_string(&function.name)?;
        if !matches!(
            extensions::get(&name),
            Some(extensions::Extension::EmptyGroupingSet)
        ) {
            return Ok(false);
        }
        Ok(matches!(
            &function.args,
            FunctionArguments::List(list) if list.args.is_empty()
        ))
    }

    fn grouping_element_indices(
        keys: &mut Vec<Expr>,
        exprs: &[sqlparser::ast::Expr],
    ) -> Result<Vec<usize>, ParseError> {
        if exprs.len() == 1 && is_empty_grouping_set_sentinel(&exprs[0])? {
            return Ok(Vec::new());
        }
        exprs
            .iter()
            .map(|expr| push_group_key(keys, expr))
            .collect::<Result<Vec<_>, _>>()
    }

    fn append_set_product(
        current: &mut Vec<Vec<usize>>,
        addition: Vec<Vec<usize>>,
    ) -> Result<(), ParseError> {
        let count = current.len().saturating_mul(addition.len());
        if count > MAX_GROUPING_SETS {
            return Err(parse_error_at_zero(
                "GROUP BY supports at most 4096 grouping sets",
            ));
        }
        let width = current
            .iter()
            .map(Vec::len)
            .max()
            .unwrap_or(0)
            .saturating_add(addition.iter().map(Vec::len).max().unwrap_or(0));
        reserve_parser_expansion(count.saturating_mul(
            std::mem::size_of::<Vec<usize>>() + 2 * width * std::mem::size_of::<usize>(),
        ))?;
        let mut next = Vec::with_capacity(count);
        for prefix in current.iter() {
            for suffix in &addition {
                let mut set = prefix.clone();
                set.extend(suffix.iter().copied());
                next.push(set);
                if next.len() > MAX_GROUPING_SETS {
                    return parse_error_from_sql_span(
                        sqlparser::tokenizer::Span::empty(),
                        "GROUP BY supports at most 4096 grouping sets",
                        vec![],
                    );
                }
            }
        }
        *current = next;
        Ok(())
    }

    fn rollup_sets(elements: Vec<Vec<usize>>) -> Result<Vec<Vec<usize>>, ParseError> {
        let count = elements.len().saturating_add(1);
        if count > MAX_GROUPING_SETS {
            return Err(parse_error_at_zero(
                "ROLLUP expands to more than 4096 grouping sets",
            ));
        }
        let width = elements.iter().map(Vec::len).sum::<usize>();
        reserve_parser_expansion(count.saturating_mul(
            std::mem::size_of::<Vec<usize>>() + 2 * width * std::mem::size_of::<usize>(),
        ))?;
        let mut out = Vec::with_capacity(elements.len() + 1);
        for prefix_len in (0..=elements.len()).rev() {
            let mut set = Vec::new();
            for element in elements.iter().take(prefix_len) {
                set.extend(element.iter().copied());
            }
            out.push(set);
        }
        Ok(out)
    }

    fn cube_sets(elements: Vec<Vec<usize>>) -> Result<Vec<Vec<usize>>, ParseError> {
        let n = elements.len();
        if n >= usize::BITS as usize {
            return parse_error_from_sql_span(
                sqlparser::tokenizer::Span::empty(),
                "GROUP BY CUBE has too many dimensions",
                vec![],
            );
        }
        let count = 1usize << n;
        if count > MAX_GROUPING_SETS {
            return parse_error_from_sql_span(
                sqlparser::tokenizer::Span::empty(),
                "GROUP BY CUBE expands to more than 4096 grouping sets",
                vec![],
            );
        }
        let width = elements.iter().map(Vec::len).sum::<usize>();
        reserve_parser_expansion(count.saturating_mul(
            std::mem::size_of::<Vec<usize>>() + 2 * width * std::mem::size_of::<usize>(),
        ))?;
        let mut out = Vec::with_capacity(count);
        for mask in (0..count).rev() {
            let mut set = Vec::new();
            for (idx, element) in elements.iter().enumerate() {
                if (mask & (1usize << (n - idx - 1))) != 0 {
                    set.extend(element.iter().copied());
                }
            }
            out.push(set);
        }
        Ok(out)
    }

    fn convert_grouping_sets_expr(
        keys: &mut Vec<Expr>,
        expr: &sqlparser::ast::Expr,
    ) -> Result<Vec<Vec<usize>>, ParseError> {
        match expr {
            sqlparser::ast::Expr::GroupingSets(sets) => sets
                .iter()
                .map(|set| grouping_element_indices(keys, set))
                .collect::<Result<Vec<_>, _>>(),
            _ => parse_error_from_sql_span(
                sqlparser::tokenizer::Span::empty(),
                "GROUP BY GROUPING SETS modifier is malformed",
                vec![],
            ),
        }
    }

    fn function_grouping_args(
        args: &FunctionArguments,
    ) -> Result<Vec<Vec<sqlparser::ast::Expr>>, ParseError> {
        let FunctionArguments::List(list) = args else {
            return parse_error_from_sql_span(
                sqlparser::tokenizer::Span::empty(),
                "GROUP BY grouping function requires an argument list",
                vec![],
            );
        };
        list.args
            .iter()
            .map(|arg| match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(sqlparser::ast::Expr::Tuple(items))) => {
                    Ok(items.clone())
                }
                FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Ok(vec![expr.clone()]),
                _ => parse_error_from_sql_span(
                    arg.span(),
                    "unsupported GROUP BY grouping function argument",
                    vec![],
                ),
            })
            .collect()
    }

    fn apply_group_item(
        keys: &mut Vec<Expr>,
        sets: &mut Vec<Vec<usize>>,
        expr: &sqlparser::ast::Expr,
        modifier: &mut GroupByModifier,
    ) -> Result<(), ParseError> {
        match expr {
            sqlparser::ast::Expr::Rollup(elements) => {
                *modifier = GroupByModifier::Rollup;
                let converted = elements
                    .iter()
                    .map(|element| grouping_element_indices(keys, element))
                    .collect::<Result<Vec<_>, _>>()?;
                append_set_product(sets, rollup_sets(converted)?)
            }
            sqlparser::ast::Expr::Cube(elements) => {
                *modifier = GroupByModifier::Cube;
                let converted = elements
                    .iter()
                    .map(|element| grouping_element_indices(keys, element))
                    .collect::<Result<Vec<_>, _>>()?;
                append_set_product(sets, cube_sets(converted)?)
            }
            sqlparser::ast::Expr::GroupingSets(group_sets) => {
                *modifier = GroupByModifier::GroupingSets;
                let converted = group_sets
                    .iter()
                    .map(|set| grouping_element_indices(keys, set))
                    .collect::<Result<Vec<_>, _>>()?;
                append_set_product(sets, converted)
            }
            sqlparser::ast::Expr::Tuple(items) if items.is_empty() => {
                append_set_product(sets, vec![Vec::new()])
            }
            sqlparser::ast::Expr::Function(function) => {
                let name = object_name_to_string(&function.name)?;
                if name.eq_ignore_ascii_case("ROLLUP") {
                    *modifier = GroupByModifier::Rollup;
                    let converted = function_grouping_args(&function.args)?
                        .iter()
                        .map(|element| grouping_element_indices(keys, element))
                        .collect::<Result<Vec<_>, _>>()?;
                    append_set_product(sets, rollup_sets(converted)?)
                } else if name.eq_ignore_ascii_case("CUBE") {
                    *modifier = GroupByModifier::Cube;
                    let converted = function_grouping_args(&function.args)?
                        .iter()
                        .map(|element| grouping_element_indices(keys, element))
                        .collect::<Result<Vec<_>, _>>()?;
                    append_set_product(sets, cube_sets(converted)?)
                } else if name.eq_ignore_ascii_case("GROUPING_SETS")
                    || name.eq_ignore_ascii_case("GROUPING SETS")
                {
                    *modifier = GroupByModifier::GroupingSets;
                    let converted = function_grouping_args(&function.args)?
                        .iter()
                        .map(|set| grouping_element_indices(keys, set))
                        .collect::<Result<Vec<_>, _>>()?;
                    append_set_product(sets, converted)
                } else {
                    let idx = push_group_key(keys, expr)?;
                    for set in sets.iter_mut() {
                        set.push(idx);
                    }
                    Ok(())
                }
            }
            _ => {
                let idx = push_group_key(keys, expr)?;
                for set in sets.iter_mut() {
                    set.push(idx);
                }
                Ok(())
            }
        }
    }

    match group_by {
        sqlparser::ast::GroupByExpr::All(_) => parse_error_from_sql_span(
            sqlparser::tokenizer::Span::empty(),
            "unsupported GROUP BY ALL",
            vec![],
        ),
        sqlparser::ast::GroupByExpr::Expressions(exprs, modifiers) => {
            let mut group_by_modifier = GroupByModifier::None;
            let mut trailing_modifier = GroupByModifier::None;
            let mut group_by_keys = Vec::new();
            let mut grouping_sets = vec![Vec::new()];
            for e in exprs {
                apply_group_item(
                    &mut group_by_keys,
                    &mut grouping_sets,
                    e,
                    &mut group_by_modifier,
                )?;
            }
            for modifier in modifiers {
                match modifier {
                    sqlparser::ast::GroupByWithModifier::Rollup => {
                        if group_by_modifier != GroupByModifier::None {
                            return parse_error_from_sql_span(
                                sqlparser::tokenizer::Span::empty(),
                                "duplicate GROUP BY modifier",
                                vec![],
                            );
                        }
                        group_by_modifier = GroupByModifier::Rollup;
                        trailing_modifier = GroupByModifier::Rollup;
                    }
                    sqlparser::ast::GroupByWithModifier::Cube => {
                        if group_by_modifier != GroupByModifier::None {
                            return parse_error_from_sql_span(
                                sqlparser::tokenizer::Span::empty(),
                                "duplicate GROUP BY modifier",
                                vec![],
                            );
                        }
                        group_by_modifier = GroupByModifier::Cube;
                        trailing_modifier = GroupByModifier::Cube;
                    }
                    sqlparser::ast::GroupByWithModifier::GroupingSets(expr) => {
                        if group_by_modifier != GroupByModifier::None {
                            return parse_error_from_sql_span(
                                sqlparser::tokenizer::Span::empty(),
                                "duplicate GROUP BY modifier",
                                vec![],
                            );
                        }
                        let converted = convert_grouping_sets_expr(&mut group_by_keys, expr)?;
                        append_set_product(&mut grouping_sets, converted)?;
                        group_by_modifier = GroupByModifier::GroupingSets;
                    }
                    _ => {
                        return parse_error_from_sql_span(
                            sqlparser::tokenizer::Span::empty(),
                            "GROUP BY modifier is not supported",
                            vec![],
                        );
                    }
                }
            }
            let grouping_sets = if trailing_modifier == GroupByModifier::Rollup {
                rollup_sets((0..group_by_keys.len()).map(|idx| vec![idx]).collect())?
            } else if trailing_modifier == GroupByModifier::Cube {
                cube_sets((0..group_by_keys.len()).map(|idx| vec![idx]).collect())?
            } else if group_by_modifier == GroupByModifier::None {
                Vec::new()
            } else {
                grouping_sets
            };
            if grouping_sets.len() > MAX_GROUPING_SETS {
                return parse_error_from_sql_span(
                    sqlparser::tokenizer::Span::empty(),
                    "GROUP BY supports at most 4096 grouping sets",
                    vec![],
                );
            }
            Ok((group_by_keys, grouping_sets, group_by_modifier))
        }
    }
}

fn convert_index_columns(
    columns: Vec<sqlparser::ast::IndexColumn>,
) -> Result<ConvertedIndexColumns, ParseError> {
    let mut names = Vec::with_capacity(columns.len());
    let mut prefix_lengths = Vec::with_capacity(columns.len());
    let mut descending = Vec::with_capacity(columns.len());
    let mut key_expressions = Vec::with_capacity(columns.len());
    let mut key_expression_multi_valued = Vec::with_capacity(columns.len());
    for column in columns {
        key_expression_multi_valued.push(false);
        let desc = column.column.options.asc == Some(false);
        match column.column.expr {
            SqlExpr::Identifier(ident) => {
                names.push(ident_value(ident));
                prefix_lengths.push(None);
                descending.push(desc);
                key_expressions.push(None);
            }
            // `name(10)` (MySQL prefix-length syntax) parses as a
            // function call to sqlparser. We disambiguate by requiring
            // a single numeric literal argument; anything else is a
            // functional key part — but those parse as `Nested` (see
            // the next arm) because MySQL spells them `((LOWER(c)))`.
            SqlExpr::Function(function) => {
                let name = object_name_to_string(function.name)?;
                let prefix_len = index_prefix_len_from_function_args(&function.args)?;
                names.push(name);
                prefix_lengths.push(Some(prefix_len));
                descending.push(desc);
                key_expressions.push(None);
            }
            // MySQL 8.0 functional index: `((LOWER(c)))` lands as
            // `Nested(...)` because the outer parens are the column
            // list and the inner parens force sqlparser to treat the
            // body as an expression rather than a column reference.
            SqlExpr::Nested(mut inner) => {
                // ARRAY belongs to this exact functional key part. Only a
                // top-level CAST may define a multi-valued index; leave any
                // nested ARRAY marker intact so scalar validation rejects it.
                let mut root = inner.as_mut();
                while let SqlExpr::Nested(nested) = root {
                    root = nested.as_mut();
                }
                if let SqlExpr::Cast { array, .. } = root {
                    *key_expression_multi_valued.last_mut().unwrap() = std::mem::take(array);
                }
                let converted = convert_expr(&inner)?;
                let expr_sql = expr_to_catalog_sql(&converted).map_err(|err| {
                    let mut error = parse_error_at_zero(err.to_string());
                    error.spec = crate::error::catalog::ER_FUNCTIONAL_INDEX_REF_AUTO_INCREMENT;
                    error
                })?;
                if expr_sql.is_empty() {
                    return parse_error_from_sql_span(
                        inner.span(),
                        "functional index expression must not be empty",
                        vec![],
                    );
                }
                // We persist the canonical SQL text of the
                // expression; SHOW CREATE TABLE round-trips back to
                // `((<expr_sql>))`. Store it as the `columns[i]`
                // placeholder too so existing code that iterates
                // `columns` (e.g. cardinality stats, EXPLAIN labels)
                // sees a non-empty string instead of accidentally
                // matching a real column.
                names.push(expr_sql.clone());
                prefix_lengths.push(None);
                descending.push(desc);
                key_expressions.push(Some(expr_sql));
            }
            other => {
                return parse_error_from_sql_span(
                    other.span(),
                    "unsupported index expression",
                    vec![],
                );
            }
        }
    }
    Ok(ConvertedIndexColumns {
        names,
        prefix_lengths,
        descending,
        key_expressions,
        key_expression_multi_valued,
    })
}

fn convert_index_type_and_options(
    direct_type: Option<IndexType>,
    options: &[IndexOption],
) -> Result<(String, String), ParseError> {
    let mut index_type = direct_type
        .map(|ty| ty.to_string().to_ascii_uppercase())
        .unwrap_or_else(|| "BTREE".to_string());
    let mut comment = String::new();
    for option in options {
        match option {
            IndexOption::Using(ty) => {
                index_type = ty.to_string().to_ascii_uppercase();
            }
            IndexOption::Comment(text) => {
                comment = text.clone();
            }
        }
    }
    // InnoDB treats `USING HASH` as BTREE (documented, no warning);
    // mydb does the same so mysqldump files with MEMORY-era hints load.
    if index_type == "HASH" {
        index_type = "BTREE".to_string();
    }
    Ok((index_type, comment))
}

fn convert_exact_number_info(info: ExactNumberInfo) -> Result<Option<(u8, u8)>, ParseError> {
    match info {
        // MySQL defines bare DECIMAL/NUMERIC as DECIMAL(10,0).  Normalize at
        // the parser boundary so storage enforcement, metadata and ZEROFILL
        // width all observe the same declaration.
        ExactNumberInfo::None => Ok(Some((10, 0))),
        ExactNumberInfo::Precision(p) => Ok(Some(validate_decimal_precision_scale(p, 0)?)),
        ExactNumberInfo::PrecisionAndScale(p, s) => {
            Ok(Some(validate_decimal_precision_scale(p, s)?))
        }
    }
}

fn convert_float_number_info(
    info: ExactNumberInfo,
    float_precision_form: bool,
) -> Result<Option<(u8, u8)>, ParseError> {
    match info {
        ExactNumberInfo::None => Ok(None),
        ExactNumberInfo::Precision(p) => {
            let maximum = if float_precision_form { 53 } else { 255 };
            if p == 0 || p > maximum {
                // FLOAT(p) is the SQL bit-precision form. MySQL reports
                // ER_WRONG_FIELD_SPEC (1063), not the display-width error used
                // by oversized FLOAT(M,D)/DOUBLE(M,D) declarations.
                let spec = if float_precision_form {
                    crate::error::catalog::ER_WRONG_FIELD_SPEC
                } else {
                    crate::error::catalog::ER_TOO_BIG_DISPLAYWIDTH
                };
                return Err(numeric_spec_error(
                    spec,
                    format!("Display width out of range for FLOAT/DOUBLE (max = {maximum})"),
                ));
            }
            Ok(Some((p as u8, 0)))
        }
        ExactNumberInfo::PrecisionAndScale(p, s) => {
            if s < 0 {
                return Err(numeric_spec_error(
                    crate::error::catalog::ER_TOO_BIG_SCALE,
                    "Scale must not be negative",
                ));
            }
            let scale = s as u64;
            if scale > 30 {
                return Err(numeric_spec_error(
                    crate::error::catalog::ER_TOO_BIG_SCALE,
                    format!("Too big scale {scale} specified. Maximum is 30"),
                ));
            }
            if p > 255 {
                return Err(numeric_spec_error(
                    crate::error::catalog::ER_TOO_BIG_DISPLAYWIDTH,
                    "Display width out of range for FLOAT/DOUBLE (max = 255)",
                ));
            }
            if scale > p {
                return Err(numeric_spec_error(
                    crate::error::catalog::ER_M_BIGGER_THAN_D,
                    "For float(M,D), double(M,D) or decimal(M,D), M must be >= D",
                ));
            }
            Ok(Some((p as u8, scale as u8)))
        }
    }
}

fn convert_mysql_float_type(info: ExactNumberInfo) -> Result<DataType, ParseError> {
    match info {
        ExactNumberInfo::None => Ok(DataType::Float(None)),
        ExactNumberInfo::Precision(precision) => {
            let Some((precision, _)) =
                convert_float_number_info(ExactNumberInfo::Precision(precision), true)?
            else {
                unreachable!("FLOAT precision conversion must retain precision");
            };
            // FLOAT(p) uses SQL bit precision, not the deprecated display
            // width/scale form. MySQL stores p<=24 as FLOAT and p>=25 as
            // DOUBLE, and SHOW COLUMNS omits the original `(p)`.
            Ok(if precision <= 24 {
                DataType::Float(None)
            } else {
                DataType::Double(None)
            })
        }
        ExactNumberInfo::PrecisionAndScale(precision, scale) => Ok(DataType::Float(
            convert_float_number_info(ExactNumberInfo::PrecisionAndScale(precision, scale), false)?,
        )),
    }
}

fn validate_integer_display_width(width: Option<u64>) -> Result<(), ParseError> {
    if width.is_some_and(|width| width > 255) {
        return Err(numeric_spec_error(
            crate::error::catalog::ER_TOO_BIG_DISPLAYWIDTH,
            "Display width out of range for integer type (max = 255)",
        ));
    }
    Ok(())
}

fn convert_character_len(len: Option<CharacterLength>) -> Result<Option<usize>, ParseError> {
    match len {
        None => Ok(None),
        Some(CharacterLength::IntegerLength { length, .. }) => {
            if length > u32::MAX as u64 {
                return Err(numeric_spec_error(
                    crate::error::catalog::ER_TOO_BIG_DISPLAYWIDTH,
                    "Display width out of range for character type (max = 4294967295)",
                ));
            }
            Ok(Some(length as usize))
        }
        Some(CharacterLength::Max) => Ok(None),
    }
}

fn convert_binary_len(
    len: Option<sqlparser::ast::BinaryLength>,
) -> Result<Option<usize>, ParseError> {
    match len {
        None => Ok(None),
        Some(sqlparser::ast::BinaryLength::IntegerLength { length }) => {
            if length > u32::MAX as u64 {
                return Err(numeric_spec_error(
                    crate::error::catalog::ER_TOO_BIG_DISPLAYWIDTH,
                    "Display width out of range for binary type (max = 4294967295)",
                ));
            }
            Ok(Some(length as usize))
        }
        Some(sqlparser::ast::BinaryLength::Max) => Ok(None),
    }
}

fn convert_fixed_binary_len(len: Option<u64>) -> Result<Option<usize>, ParseError> {
    match len {
        Some(length) if length > u32::MAX as u64 => Err(numeric_spec_error(
            crate::error::catalog::ER_TOO_BIG_DISPLAYWIDTH,
            "Display width out of range for binary type (max = 4294967295)",
        )),
        Some(length) => Ok(Some(length as usize)),
        None => Ok(None),
    }
}

// Statement conversion support shared with compatibility passes.
pub(super) fn parse_histogram_action(tokens: &[&str]) -> Option<HistogramAction> {
    // Re-join so we can tokenize on commas / the WITH ... BUCKETS tail
    // independent of the original whitespace.
    let joined = tokens.join(" ");
    let upper = joined.to_ascii_uppercase();
    let is_update = upper.starts_with("UPDATE HISTOGRAM ON ");
    let is_drop = upper.starts_with("DROP HISTOGRAM ON ");
    if !is_update && !is_drop {
        return None;
    }
    let prefix_len = if is_update {
        "UPDATE HISTOGRAM ON ".len()
    } else {
        "DROP HISTOGRAM ON ".len()
    };
    let mut remainder = &joined[prefix_len..];

    // Split off an optional `WITH N BUCKETS` tail (UPDATE only).
    let mut buckets: Option<usize> = None;
    if let Some(with_pos) = remainder.to_ascii_uppercase().find(" WITH ") {
        let (cols_part, with_part) = remainder.split_at(with_pos);
        let with_upper = with_part.to_ascii_uppercase();
        // Expect ` WITH <N> BUCKETS`.
        let with_tokens: Vec<&str> = with_part.split_whitespace().collect();
        if with_tokens.len() == 3
            && with_tokens[0].eq_ignore_ascii_case("WITH")
            && with_tokens[2].eq_ignore_ascii_case("BUCKETS")
        {
            buckets = with_tokens[1].parse::<usize>().ok();
            if buckets.is_none() {
                return None; // malformed bucket count
            }
        } else if with_upper.contains("BUCKETS") {
            // Recognizable but malformed WITH ... BUCKETS.
            return None;
        } else {
            return None;
        }
        remainder = cols_part;
    }

    let columns: Vec<String> = remainder
        .split(',')
        .map(|c| c.trim().trim_matches('`').to_string())
        .filter(|c| !c.is_empty())
        .collect();
    if columns.is_empty() {
        return None;
    }
    if is_update {
        // DROP HISTOGRAM does not accept WITH N BUCKETS.
        Some(HistogramAction::Update { columns, buckets })
    } else {
        if buckets.is_some() {
            return None;
        }
        Some(HistogramAction::Drop { columns })
    }
}

fn schema_name_to_string(name: sqlparser::ast::SchemaName) -> Result<String, ParseError> {
    match name {
        sqlparser::ast::SchemaName::Simple(name)
        | sqlparser::ast::SchemaName::NamedAuthorization(name, _) => object_name_to_string(name),
        sqlparser::ast::SchemaName::UnnamedAuthorization(ident) => Ok(ident_value(ident)),
    }
}

fn show_database(options: &ShowStatementOptions) -> Option<String> {
    let show_in = options.show_in.as_ref()?;
    match show_in.parent_type {
        Some(ShowStatementInParentType::Database)
        | Some(ShowStatementInParentType::Schema)
        | None => show_in
            .parent_name
            .clone()
            .and_then(|name| object_name_to_string(name).ok()),
        _ => None,
    }
}

fn show_table(options: &ShowStatementOptions) -> Option<String> {
    let show_in = options.show_in.as_ref()?;
    match show_in.parent_type {
        Some(ShowStatementInParentType::Table) | None => show_in
            .parent_name
            .clone()
            .and_then(|name| object_name_to_string(name).ok()),
        _ => None,
    }
}

fn show_options_filter(options: &ShowStatementOptions) -> Result<Option<ShowFilter>, ParseError> {
    if options.starts_with.is_some() || options.limit.is_some() || options.limit_from.is_some() {
        return Err(parse_error_at_zero(
            "SHOW STARTS WITH/LIMIT filters are not supported by MySQL",
        ));
    }
    match options.filter_position.clone() {
        Some(ShowStatementFilterPosition::Infix(filter))
        | Some(ShowStatementFilterPosition::Suffix(filter)) => convert_show_filter(Some(filter)),
        None => Ok(None),
    }
}

fn show_scope(session: bool, global: bool) -> ShowScope {
    if global && !session {
        ShowScope::Global
    } else {
        ShowScope::Session
    }
}

#[derive(Default)]
struct ParsedCreateTableOptions {
    auto_increment_start: Option<u64>,
    default_charset: Option<String>,
    default_collation: Option<String>,
    comment: Option<String>,
    /// The requested storage engine name (`ENGINE=InnoDB` / `MyISAM` /
    /// …). mydb is single-engine, so this is metadata only: recorded for
    /// SHOW CREATE TABLE / SHOW TABLE STATUS round-trip while every table
    /// physically uses mydb's MVCC engine. `None` = no ENGINE clause
    /// (keeps SHOW CREATE byte-identical for the common case).
    engine: Option<String>,
    /// Accepted metadata-only options (ROW_FORMAT, MAX_ROWS, …) in
    /// declaration order; see `accept_generic_table_option`.
    generic: Vec<(String, String)>,
}

/// MySQL table options mydb accepts: validated,
/// persisted on the catalog, echoed by SHOW CREATE TABLE and
/// information_schema. Most have no physical behaviour — the same
/// treatment InnoDB gives them; ZONE_MAP / ZONE_SIZE are physical exceptions.
/// (PACK_KEYS / CHECKSUM /
/// DELAY_KEY_WRITE are MyISAM-era, STATS_* / MAX_ROWS are hints).
/// Returns `Ok(Some((KEY, value)))` canonicalised, `Ok(None)` when the
/// key is not in the accepted set (caller decides unsupported), or a
/// 1064-style message when the key is known but the value is invalid.
pub(super) fn accept_generic_table_option(
    key: &str,
    raw_value: &str,
) -> Result<Option<(String, String)>, String> {
    let value = raw_value.trim();
    let upper_value = value.to_ascii_uppercase();
    let enum_value = |allowed: &[&str]| -> Result<String, String> {
        if allowed.contains(&upper_value.as_str()) {
            Ok(upper_value.clone())
        } else {
            Err(format!("invalid {key} value '{value}'"))
        }
    };
    let uint_value = || -> Result<String, String> {
        value
            .parse::<u64>()
            .map(|n| n.to_string())
            .map_err(|_| format!("invalid {key} value '{value}'"))
    };
    let canonical = match key {
        "ENGINE" => match upper_value.as_str() {
            // HEAP is MySQL's historical alias for MEMORY. mydb's storage
            // implementation is shared, but preserving the canonical engine
            // name keeps SHOW/INFORMATION_SCHEMA metadata deterministic.
            "HEAP" | "MEMORY" => "MEMORY".to_string(),
            "INNODB" => "INNODB".to_string(),
            _ => return Ok(None),
        },
        "ZONE_MAP" => enum_value(&["ON", "OFF", "DEFAULT"])?,
        "ZONE_SIZE" => {
            if upper_value == "DEFAULT" {
                "DEFAULT".to_string()
            } else {
                let (digits, multiplier) = match upper_value.as_bytes().last().copied() {
                    Some(b'K') => (&upper_value[..upper_value.len() - 1], 1024_u64),
                    Some(b'M') => (&upper_value[..upper_value.len() - 1], 1024_u64 * 1024),
                    _ => (upper_value.as_str(), 1_u64),
                };
                let bytes = digits
                    .parse::<u64>()
                    .ok()
                    .and_then(|n| n.checked_mul(multiplier))
                    .ok_or_else(|| format!("invalid {key} value '{value}'"))?;
                if !(64 * 1024..=64 * 1024 * 1024).contains(&bytes)
                    || bytes % (16 * 1024) != 0
                    || !(bytes / (16 * 1024)).is_power_of_two()
                {
                    return Err(format!(
                        "invalid {key} value '{value}': expected a power-of-two multiple of 16K between 64K and 64M"
                    ));
                }
                bytes.to_string()
            }
        }
        "ROW_FORMAT" => enum_value(&[
            "DEFAULT",
            "DYNAMIC",
            "FIXED",
            "COMPRESSED",
            "REDUNDANT",
            "COMPACT",
        ])?,
        "KEY_BLOCK_SIZE" | "STATS_SAMPLE_PAGES" | "MAX_ROWS" | "MIN_ROWS" | "AVG_ROW_LENGTH"
        | "AUTOEXTEND_SIZE" => {
            if key == "STATS_SAMPLE_PAGES" && upper_value == "DEFAULT" {
                "DEFAULT".to_string()
            } else {
                uint_value()?
            }
        }
        "STATS_PERSISTENT" | "STATS_AUTO_RECALC" | "PACK_KEYS" | "CHECKSUM" | "TABLE_CHECKSUM"
        | "DELAY_KEY_WRITE" => enum_value(&["0", "1", "DEFAULT"])?,
        "INSERT_METHOD" => enum_value(&["NO", "FIRST", "LAST"])?,
        "COMPRESSION" => enum_value(&["ZLIB", "LZ4", "NONE"])?,
        "CONNECTION" => value.to_string(),
        // `CREATE TABLE ... TABLESPACE ts` — accept the declared name as
        // metadata (mydb has no per-tablespace physical storage) so
        // mysqldump / migration output round-trips instead of erroring.
        "TABLESPACE" => value.to_string(),
        "ENCRYPTION" => {
            // mydb has no table-level encryption (TDE). Accept InnoDB's
            // no-encryption default 'N'/'n' as a metadata-only no-op so
            // mysqldump / CREATE round-trips; 'Y' stays honestly
            // unsupported (Ok(None) -> Statement::Unsupported), since
            // silently ignoring a request for at-rest encryption would
            // be a data-safety lie.
            match upper_value.as_str() {
                "N" => "N".to_string(),
                "Y" => return Ok(None),
                _ => return Err(format!("invalid {key} value '{value}'")),
            }
        }
        _ => return Ok(None),
    };
    let key = if key == "TABLE_CHECKSUM" {
        "CHECKSUM"
    } else {
        key
    };
    Ok(Some((key.to_string(), canonical)))
}

/// Render an accepted generic option for SHOW CREATE TABLE — string-
/// valued options are single-quoted, the rest stay bare.
pub fn render_table_option(key: &str, value: &str) -> String {
    match key {
        "CONNECTION" | "COMPRESSION" | "ENCRYPTION" => {
            format!("{key}='{}'", value.replace('\'', "''"))
        }
        _ => format!("{key}={value}"),
    }
}

fn create_table_options(
    sql: &str,
    options: &sqlparser::ast::CreateTableOptions,
) -> Result<Result<ParsedCreateTableOptions, Statement>, ParseError> {
    let sqlparser::ast::CreateTableOptions::Plain(options) = options else {
        return Ok(Ok(ParsedCreateTableOptions::default()));
    };
    let mut parsed = ParsedCreateTableOptions::default();
    for option in options {
        match option {
            sqlparser::ast::SqlOption::KeyValue { key, value } => {
                let key = key.value.to_ascii_uppercase();
                match key.as_str() {
                    "AUTO_INCREMENT" => {
                        parsed.auto_increment_start = Some(parse_auto_increment_expr(value)?);
                    }
                    "DEFAULT CHARSET" | "CHARSET" | "DEFAULT CHARACTER SET" | "CHARACTER SET" => {
                        parsed.default_charset = Some(sql_option_value_name(value)?);
                    }
                    "DEFAULT COLLATE" | "COLLATE" => {
                        parsed.default_collation = Some(sql_option_value_name(value)?);
                    }
                    other => {
                        match accept_generic_table_option(other, &sql_option_value_name(value)?) {
                            Ok(Some(entry)) => {
                                parsed.generic.retain(|(k, _)| k != &entry.0);
                                parsed.generic.push(entry);
                            }
                            Ok(None) => {
                                return Ok(Err(Statement::Unsupported {
                                    kind: format!("CREATE TABLE option {other}"),
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
            sqlparser::ast::SqlOption::Comment(comment) => {
                parsed.comment = Some(match comment {
                    sqlparser::ast::CommentDef::WithEq(value)
                    | sqlparser::ast::CommentDef::WithoutEq(value) => value.clone(),
                });
            }
            sqlparser::ast::SqlOption::TableSpace(ts) => {
                // `CREATE TABLE ... TABLESPACE ts [STORAGE {DISK|MEMORY}]`
                // — accept the declared name as metadata (mydb has no
                // per-tablespace physical storage) so mysqldump /
                // migration output round-trips instead of erroring. The
                // optional STORAGE clause is likewise accepted + ignored.
                parsed.generic.retain(|(k, _)| k != "TABLESPACE");
                parsed
                    .generic
                    .push(("TABLESPACE".to_string(), ts.name.clone()));
            }
            sqlparser::ast::SqlOption::NamedParenthesizedList(list)
                if list.key.value.eq_ignore_ascii_case("ENGINE") =>
            {
                let engine = list
                    .name
                    .as_ref()
                    .map(|name| name.value.as_str())
                    .unwrap_or("");
                if !list.values.is_empty() {
                    return Ok(Err(Statement::Unsupported {
                        kind: "CREATE TABLE parameterized ENGINE".to_string(),
                        sql: sql.to_string(),
                    }));
                }
                // mydb is single-engine. Accept ANY engine name (InnoDB,
                // MyISAM, MEMORY, …) as metadata: recorded for SHOW CREATE
                // round-trip, physically served by mydb's MVCC engine —
                // the same accept+ignore treatment metadata-only options
                // get. Rejecting a specific engine name would cascade-fail
                // every upstream test that declares e.g. ENGINE=MyISAM.
                if !engine.is_empty() {
                    parsed.engine = Some(engine.to_string());
                }
            }
            other => {
                return Ok(Err(Statement::Unsupported {
                    kind: format!("CREATE TABLE option {other}"),
                    sql: sql.to_string(),
                }));
            }
        }
    }
    Ok(Ok(parsed))
}

pub(super) fn sql_option_value_name(expr: &SqlExpr) -> Result<String, ParseError> {
    match expr {
        SqlExpr::Identifier(ident) => Ok(ident.value.clone()),
        SqlExpr::CompoundIdentifier(parts) => {
            join_name_parts(parts.iter().map(|ident| Ok(ident.value.as_str())))
        }
        SqlExpr::Value(value) => match &value.value {
            SqlValue::SingleQuotedString(value)
            | SqlValue::DoubleQuotedString(value)
            | SqlValue::EscapedStringLiteral(value) => Ok(value.clone()),
            _ => Ok(value.value.to_string()),
        },
        other => Ok(other.to_string()),
    }
}

fn parse_auto_increment_expr(expr: &SqlExpr) -> Result<u64, ParseError> {
    match expr {
        SqlExpr::Value(value) => parse_auto_increment_value(&value.value),
        SqlExpr::UnaryOp {
            op: SqlUnaryOperator::Plus,
            expr,
        } => parse_auto_increment_expr(expr),
        _ => Err(parse_error_at_zero(
            "AUTO_INCREMENT value must be a positive integer literal",
        )),
    }
}

/// Resolve a `[smallint|integer|bigint]` sequence backing type from a
/// sqlparser data type (display-width / unsigned suffixes are ignored).
fn seq_type_from_sql(dt: &SqlDataType) -> Option<crate::storage::sequence::SeqType> {
    let rendered = dt.to_string();
    let base = rendered.split('(').next().unwrap_or("").trim();
    crate::storage::sequence::SeqType::parse(base)
}

/// Evaluate a signed integer literal used as a sequence option value
/// (`INCREMENT`, `MINVALUE`, `START`, ...). Handles a leading unary sign.
fn seq_expr_to_i64(expr: &SqlExpr) -> Result<i64, ParseError> {
    match expr {
        SqlExpr::Value(value) => seq_value_to_i64(&value.value, false),
        SqlExpr::UnaryOp {
            op: SqlUnaryOperator::Plus,
            expr,
        } => seq_expr_to_i64(expr),
        SqlExpr::UnaryOp {
            op: SqlUnaryOperator::Minus,
            expr,
        } => match &**expr {
            SqlExpr::Value(value) => seq_value_to_i64(&value.value, true),
            _ => Err(parse_error_at_zero(
                "sequence option value must be an integer literal",
            )),
        },
        _ => Err(parse_error_at_zero(
            "sequence option value must be an integer literal",
        )),
    }
}

fn seq_value_to_i64(value: &SqlValue, negative: bool) -> Result<i64, ParseError> {
    let SqlValue::Number(text, _) = value else {
        return Err(parse_error_at_zero(
            "sequence option value must be an integer literal",
        ));
    };
    if text.contains('.') {
        return Err(parse_error_at_zero(
            "sequence option value must be an integer literal",
        ));
    }
    // Parse the magnitude as i128 so `MINVALUE -9223372036854775808` (i64::MIN)
    // round-trips before the final i64 range check.
    let magnitude: i128 = text
        .parse()
        .map_err(|_| parse_error_at_zero("sequence option value is out of range for bigint"))?;
    let signed = if negative { -magnitude } else { magnitude };
    i64::try_from(signed)
        .map_err(|_| parse_error_at_zero("sequence option value is out of range for bigint"))
}

/// Result of converting one ALTER TABLE action.
enum ConvertedAlterAction {
    Action(AlterAction),
    Actions(Vec<AlterAction>),
    Unsupported(Statement),
}

/// True when a VALUES row is exactly the `__MYDB_EMPTY_ROW__` sentinel emitted
/// by [`rewrite_empty_insert_rows`] for an empty `VALUES ()` all-defaults row.
fn row_is_empty_default_marker(row: &[SqlExpr]) -> bool {
    matches!(
        row,
        [SqlExpr::Identifier(ident)] if matches!(extensions::get(&ident.value), Some(extensions::Extension::EmptyRow))
    )
}

/// Preserve the query expression; INSERT's sink does not require a SELECT
/// wrapper around set operations or nested lexical WITH scopes.
fn insert_source_from_query_body(stmt: Statement) -> Option<InsertSource> {
    QueryExpr::try_from(stmt).ok().map(InsertSource::Query)
}

fn parse_lock_clause(query: &Query) -> Result<Option<LockClause>, ParseError> {
    use sqlparser::ast::{LockType, NonBlock};
    let Some(first) = query.locks.first() else {
        return Ok(None);
    };
    if query.locks.len() > 1 && query.locks.iter().any(|lock| lock.of.is_none()) {
        return Err(parse_error_at_zero(
            "multiple locking clauses require distinct OF targets",
        ));
    }
    let mode = |lock: &sqlparser::ast::LockClause| match lock.lock_type {
        LockType::Update => LockClauseMode::ForUpdate,
        LockType::Share => LockClauseMode::ForShare,
    };
    let wait = |lock: &sqlparser::ast::LockClause| match lock.nonblock {
        Some(NonBlock::Nowait) => LockWaitMode::NoWait,
        Some(NonBlock::SkipLocked) => LockWaitMode::SkipLocked,
        None => LockWaitMode::Default,
    };
    let mut targets = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for lock in &query.locks {
        if let Some(name) = &lock.of {
            let table = object_name_to_string(name)?;
            if !seen.insert(table.to_ascii_lowercase()) {
                return Err(parse_error_at_zero(format!(
                    "duplicate locking target '{table}'"
                )));
            }
            targets.push(LockTarget {
                table,
                mode: mode(lock),
                wait: wait(lock),
            });
        }
    }
    Ok(Some(LockClause {
        mode: mode(first),
        wait: wait(first),
        targets,
    }))
}

/// Re-attach trailing ORDER BY / LIMIT / OFFSET / lock modifiers that belonged
/// to a parenthesised-query wrapper (`SetExpr::Query`) onto the converted inner
/// statement. Only the empty slot is filled: if the inner statement already
/// carries its own tail modifiers, the two nesting levels cannot be flattened
/// onto a single node, so we surface a clear "unsupported" error rather than
/// silently dropping one level (mydb never silently no-ops unsupported SQL).
fn apply_tail_modifiers(
    sql: &str,
    stmt: Statement,
    order_by: Vec<OrderBy>,
    limit: Option<LimitValue>,
    offset: Option<LimitValue>,
    for_update: bool,
    lock_clause: Option<LockClause>,
) -> Result<Statement, ParseError> {
    // Nothing to attach — leave the inner statement untouched.
    if order_by.is_empty()
        && limit.is_none()
        && offset.is_none()
        && !for_update
        && lock_clause.is_none()
    {
        return Ok(stmt);
    }
    // Preserve separate query levels whenever the inner query already owns
    // a tail. The derived bridge binds outer ordinals after output expansion.
    let inner_has_tail = match &stmt {
        Statement::Select(select) => {
            !select.order_by.is_empty()
                || select.limit.is_some()
                || select.offset.is_some()
                || select.for_update
                || select.lock_clause.is_some()
        }
        Statement::SetOperation(set) => {
            !set.order_by.is_empty()
                || set.limit.is_some()
                || set.offset.is_some()
                || for_update
                || lock_clause.is_some()
        }
        Statement::WithCte(_) => true,
        _ => return Err(parse_error_full_span(sql, "expected a parenthesized query")),
    };
    if inner_has_tail {
        let query = QueryExpr::try_from(stmt)?;
        let name = extensions::register(sql, extensions::Extension::QueryTail);
        return Ok(Statement::Select(Select {
            projection: vec![SelectItem::Wildcard],
            table: Some(name.clone()),
            from: vec![TableRef {
                name: name.clone(),
                alias: Some(name),
                system_time: None,
                index_hints: Vec::new(),
                partitions: Vec::new(),
                column_aliases: Vec::new(),
                generate_series: None,
            }],
            derived: Some(query),
            no_merge: true,
            order_by,
            limit,
            offset,
            for_update,
            lock_clause,
            ..Select::default()
        }));
    }
    match stmt {
        Statement::Select(mut select) => {
            select.order_by = order_by;
            select.limit = limit;
            select.offset = offset;
            select.for_update = for_update;
            select.lock_clause = lock_clause;
            Ok(Statement::Select(select))
        }
        Statement::SetOperation(mut set) => {
            set.order_by = order_by;
            set.limit = limit;
            set.offset = offset;
            Ok(Statement::SetOperation(set))
        }
        _ => unreachable!("query type checked above"),
    }
}

/// Build the synthetic `SELECT * FROM (<set operation>)` wrapper used to carry
/// a `UNION`/`INTERSECT`/`EXCEPT` subquery into an expression position (where
/// the surrounding code expects a plain [`Select`]). The set operation is
/// materialised through the derived-table bridge exactly like a FROM-clause
/// derived set operation.
fn wrap_setop_as_derived_select(set_op: QueryExpr) -> Select {
    Select {
        projection: vec![SelectItem::Wildcard],
        derived: Some(set_op),
        ..Select::default()
    }
}

/// Flatten named-window inherits chains. A named window may inherit from another;
/// MySQL forbids the parent from also inheriting and forbids cycles. After this
/// returns, every `NamedWindowDef.inherits` is `None` and the entry holds the
/// fully merged spec.
fn resolve_named_windows(mut items: Vec<NamedWindowDef>) -> Result<Vec<NamedWindowDef>, String> {
    if items.is_empty() || (items.len() == 1 && items[0].inherits.is_none()) {
        return Ok(items);
    }
    let mut by_name = std::collections::HashMap::with_capacity(items.len());
    let mut inherited = 0;
    for (index, window) in items.iter().enumerate() {
        if by_name
            .insert(window.name.to_ascii_lowercase(), index)
            .is_some()
        {
            return Err(format!("duplicate named window '{}'", window.name));
        }
        inherited += usize::from(window.inherits.is_some());
    }
    // Validate against the original definitions before mutating anything.
    // Otherwise an earlier merge could hide a forbidden multi-level chain.
    let mut merges = Vec::with_capacity(inherited);
    for (index, window) in items.iter().enumerate() {
        if let Some(parent_name) = &window.inherits {
            let parent_index = *by_name
                .get(&parent_name.to_ascii_lowercase())
                .ok_or_else(|| format!("named window '{}' is not defined", parent_name))?;
            let parent = &items[parent_index];
            if parent.inherits.is_some() {
                return Err(format!(
                    "named window '{}' cannot inherit from another inheriting window",
                    window.name
                ));
            }
            validate_window_inherit(parent, window)?;
            merges.push((index, parent_index));
        }
    }
    for (index, parent_index) in merges {
        // Every parent is non-inheriting and therefore remains unchanged.
        let partition_by = items[parent_index].partition_by.clone();
        let order_by = items[index]
            .order_by
            .is_empty()
            .then(|| items[parent_index].order_by.clone());
        let child = &mut items[index];
        child.partition_by = partition_by;
        if let Some(order_by) = order_by {
            child.order_by = order_by;
        }
        child.inherits = None;
    }
    Ok(items)
}

fn validate_window_inherit(parent: &NamedWindowDef, child: &NamedWindowDef) -> Result<(), String> {
    if !child.partition_by.is_empty() {
        return Err(format!(
            "window '{}' inheriting from '{}' cannot redefine PARTITION BY",
            child.name, parent.name
        ));
    }
    if !parent.order_by.is_empty() && !child.order_by.is_empty() {
        return Err(format!(
            "window '{}' inherits ORDER BY from '{}' but also defines ORDER BY",
            child.name, parent.name
        ));
    }
    if parent.frame.is_some() {
        return Err(format!(
            "window '{}' cannot inherit from '{}' because the parent defines a frame",
            child.name, parent.name
        ));
    }
    Ok(())
}
/// Resolve `Expr::WindowFunction { inherits: Some(name), .. }` references against
/// the resolved named-window list. Mutates expressions in place.
fn resolve_window_inherits_in_expr(
    expr: &mut Expr,
    named: &[NamedWindowDef],
) -> Result<(), String> {
    use super::visit::{visit_expr_mut, AstVisitorMut};
    struct Resolve<'a> {
        named: &'a [NamedWindowDef],
        result: Result<(), String>,
    }
    impl Resolve<'_> {
        fn resolve(&self, expr: &mut Expr) -> Result<(), String> {
            let Expr::WindowFunction { spec, .. } = expr else {
                return Ok(());
            };
            let crate::parser::WindowSpec {
                partition_by,
                order_by,
                frame,
                inherits,
                ..
            } = spec.as_mut();

            if let Some(parent_name) = inherits.take() {
                let parent = self
                    .named
                    .iter()
                    .find(|w| w.name.eq_ignore_ascii_case(&parent_name))
                    .ok_or_else(|| format!("named window '{}' is not defined", parent_name))?;
                if !partition_by.is_empty() {
                    return Err(format!(
                        "OVER ({} ...) cannot redefine PARTITION BY",
                        parent.name
                    ));
                }
                *partition_by = parent.partition_by.clone();
                if !parent.order_by.is_empty() && !order_by.is_empty() {
                    return Err(format!(
                        "OVER ({} ...) cannot override ORDER BY inherited from named window",
                        parent.name
                    ));
                }
                if order_by.is_empty() {
                    *order_by = parent.order_by.clone();
                }
                if frame.is_none() {
                    *frame = parent.frame.clone();
                }
            }
            Ok(())
        }
    }
    impl AstVisitorMut for Resolve<'_> {
        fn exit_expr(&mut self, expr: &mut Expr) {
            if self.result.is_ok() {
                self.result = self.resolve(expr);
            }
        }
        fn walk_expr_children(&self, expr: &Expr) -> bool {
            self.result.is_ok()
                && !matches!(
                    expr,
                    Expr::Subquery(_)
                        | Expr::Exists { .. }
                        | Expr::InSubquery { .. }
                        | Expr::QuantifiedComparison { .. }
                        | Expr::Grouping(_)
                        | Expr::Tuple(_)
                )
        }
        fn walk_window_frame_children(&self, _: &WindowFrame) -> bool {
            // Frame offsets have separate validation; preserve this pass's scope.
            false
        }
    }
    let mut resolver = Resolve {
        named,
        result: Ok(()),
    };
    visit_expr_mut(&mut resolver, expr);
    resolver.result
}

fn assignment_column_with_qualifier(
    name: impl std::borrow::Borrow<ObjectName>,
) -> Result<(String, Option<String>), ParseError> {
    match name.borrow().0.as_slice() {
        [ObjectNamePart::Identifier(column)] => Ok((column.value.clone(), None)),
        [ObjectNamePart::Identifier(table), ObjectNamePart::Identifier(column)] => {
            Ok((column.value.clone(), Some(table.value.clone())))
        }
        _ => Err(parse_error_at_zero(
            "UPDATE assignment target must be column or table.column",
        )),
    }
}

fn is_unsigned_data_type(data_type: &SqlDataType) -> bool {
    matches!(
        data_type,
        SqlDataType::TinyIntUnsigned(_)
            | SqlDataType::UTinyInt
            | SqlDataType::SmallIntUnsigned(_)
            | SqlDataType::USmallInt
            | SqlDataType::IntUnsigned(_)
            | SqlDataType::IntegerUnsigned(_)
            | SqlDataType::MediumIntUnsigned(_)
            | SqlDataType::BigIntUnsigned(_)
            | SqlDataType::UBigInt
            | SqlDataType::FloatUnsigned(_)
            | SqlDataType::DoubleUnsigned(_)
            | SqlDataType::DoublePrecisionUnsigned
            | SqlDataType::RealUnsigned
            | SqlDataType::DecimalUnsigned(_)
    )
}

/// Map a MySQL geometry type name (case-insensitive, backtick-tolerant) to its
/// subtype, or `None` if it is not a spatial type.
fn geometry_subtype_from_name(name: &str) -> Option<crate::storage::schema::GeometrySubtype> {
    use crate::storage::schema::GeometrySubtype as G;
    let n = name.trim().trim_matches('`').to_ascii_uppercase();
    Some(match n.as_str() {
        "GEOMETRY" => G::Geometry,
        "POINT" => G::Point,
        "LINESTRING" => G::LineString,
        "POLYGON" => G::Polygon,
        "MULTIPOINT" => G::MultiPoint,
        "MULTILINESTRING" => G::MultiLineString,
        "MULTIPOLYGON" => G::MultiPolygon,
        "GEOMETRYCOLLECTION" | "GEOMCOLLECTION" => G::GeometryCollection,
        _ => return None,
    })
}

/// Clamp a sqlparser fractional-seconds precision to MySQL 8.0's legal
/// range (0..=6). `None` defaults to 0. Errors out of range so the user
/// sees a clear "fsp 7 is not in [0, 6]" instead of silently rounding.
fn clamp_fsp(sql: &str, p: Option<u64>) -> Result<u8, ParseError> {
    let raw = p.unwrap_or(0);
    if raw > 6 {
        let mut err = ParseError::unsupported_at(
            sql,
            Span {
                start: 0,
                end: sql.len(),
            },
            format!("fractional seconds precision {raw} out of range [0, 6]"),
        );
        err.spec = crate::error::catalog::ER_TOO_BIG_PRECISION;
        return Err(err);
    }
    Ok(raw as u8)
}

/// MySQL `SERIAL` pseudo-type: an alias for
/// `BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE`. sqlparser 0.59 has no
/// SERIAL keyword, so it surfaces as a bare `Custom` type name with no
/// modifiers. The column-def builders detect it before type conversion and
/// inject the implied attributes.
fn is_serial_type(data_type: &SqlDataType) -> bool {
    matches!(data_type, SqlDataType::Custom(name, args)
        if args.is_empty() && name.to_string().eq_ignore_ascii_case("SERIAL"))
}

/// Parse the dimension modifier of `VECTOR(n)`. `n` arrives as a string in the
/// sqlparser `Custom` modifier list; an absent modifier means an unconstrained
/// `VECTOR`. Validates `1 <= n <= MAX_DIM`.
/// MySQL 8.0 only permits `YEAR` (no display width) or the deprecated
/// `YEAR(4)`. Every other explicit width — `YEAR(0)`, `YEAR(2)`, `YEAR(255)`,
/// `YEAR(4294967295)` … — is rejected with ER_INVALID_YEAR_COLUMN_LENGTH
/// (errno 1818, "Supports only YEAR or YEAR(4) column."). `YEAR(2)` in
/// particular was removed in 8.0. The display width arrives as a string
/// modifier on the `Custom("YEAR", …)` type; no modifier means bare `YEAR`.
fn validate_year_display_width(modifiers: &[String], sql: &str) -> Result<(), ParseError> {
    let Some(raw) = modifiers.first() else {
        return Ok(());
    };
    if raw.trim() == "4" {
        return Ok(());
    }
    Err(ParseError {
        spec: crate::error::catalog::ER_INVALID_YEAR_COLUMN_LENGTH,
        message: "Supports only YEAR or YEAR(4) column.".to_string(),
        span: Span {
            start: 0,
            end: sql.len(),
        },
        line: 1,
        column: 1,
        expected: Vec::new(),
    })
}

fn parse_vector_type(
    modifiers: &[String],
    sql: &str,
) -> Result<(Option<u32>, crate::vector::VectorDataType), ParseError> {
    use crate::vector::VectorDataType;
    let span = Span {
        start: 0,
        end: sql.len(),
    };
    let fail = |message: String| ParseError::unsupported_at(sql, span, message);
    if modifiers.is_empty() {
        return Ok((None, VectorDataType::Float32));
    }
    if modifiers.len() > 2 {
        return Err(fail(
            "VECTOR accepts dimension and optional datatype".into(),
        ));
    }
    let datatype = if modifiers.len() == 2 {
        VectorDataType::parse(&modifiers[1])
            .ok_or_else(|| fail(format!("invalid VECTOR datatype '{}'", modifiers[1])))?
    } else {
        VectorDataType::Float32
    };
    let dim: u32 = modifiers[0]
        .trim()
        .parse()
        .map_err(|_| fail(format!("invalid VECTOR dimension '{}'", modifiers[0])))?;
    if dim == 0 || dim > datatype.max_dimension() {
        return Err(fail(format!(
            "VECTOR dimension {dim} out of range [1, {}] for {datatype}",
            datatype.max_dimension()
        )));
    }
    Ok((Some(dim), datatype))
}

/// Extract the column name from the column reference of `MATCH (cols)`. Phase 1
/// supports only simple/qualified column identifiers, taking the last segment as the column name (index-side comparison uses the bare column name).
fn match_against_column(name: &sqlparser::ast::ObjectName) -> Result<String, ParseError> {
    name.0
        .last()
        .and_then(|part| part.as_ident())
        .map(|ident| ident.value.clone())
        .ok_or_else(|| ParseError::at_zero("MATCH column must be a simple identifier"))
}

fn truth_test_expr(name: &str, expr: &SqlExpr) -> Result<Expr, ParseError> {
    Ok(Expr::Function {
        name: name.to_string(),
        args: vec![convert_expr(expr)?],
        distinct: false,
    })
}

fn expr_column_name_owned(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(name) => Some(name.clone()),
        Expr::CompoundIdentifier(parts) => parts.last().cloned(),
        _ => None,
    }
}

pub(super) fn parse_number_literal(
    text: &str,
    span: sqlparser::tokenizer::Span,
) -> Result<Literal, ParseError> {
    // MySQL distinguishes exact from approximate numeric literals:
    //   * an `e`/`E` exponent  -> approximate DOUBLE (f64)
    //   * a bare decimal point -> exact DECIMAL (keep the text, never an f64)
    //   * an integer overflowing u64 -> exact DECIMAL (MySQL promotes oversized
    //     integer literals to DECIMAL rather than erroring)
    // Previously any `.`/`e` literal became an f64, so a high-precision value
    // like `123456789012345678.99` was truncated to ~15-17 significant digits
    // before it ever reached the exact DecimalCompact encoder.
    if text.contains('e') || text.contains('E') {
        let value: f64 = text
            .parse()
            .map_err(|_| parse_error_at_span(span, format!("malformed float literal '{text}'")))?;
        if !value.is_finite() {
            let mut err =
                parse_error_at_span(span, format!("float literal '{text}' is out of range"));
            // MySQL parses the token shape successfully, then rejects the
            // non-finite DOUBLE literal as ER_ILLEGAL_VALUE_FOR_TYPE rather
            // than reporting a syntax error.
            err.spec = crate::error::catalog::ER_ILLEGAL_VALUE_FOR_TYPE;
            return Err(err);
        }
        return Ok(Literal::Float(value));
    }
    if text.contains('.') {
        // Validate the textual form, but discard the lossy f64 — keeping the
        // text is the whole point.
        if text.parse::<f64>().map(|v| v.is_finite()).unwrap_or(false) {
            return Ok(Literal::Decimal(text.to_string()));
        }
        return Err(parse_error_at_span(
            span,
            format!("malformed decimal literal '{text}'"),
        ));
    }
    if let Ok(value) = text.parse::<i64>() {
        return Ok(Literal::Int(value));
    }
    if let Ok(value) = text.parse::<u64>() {
        return Ok(Literal::UInt(value));
    }
    if text
        .bytes()
        .all(|b| b.is_ascii_digit() || b == b'-' || b == b'+')
    {
        // Oversized integer literal: MySQL promotes it to an exact DECIMAL
        // rather than erroring.
        Ok(Literal::Decimal(text.to_string()))
    } else {
        Err(parse_error_at_span(
            span,
            format!("malformed integer literal '{text}'"),
        ))
    }
}

fn is_sql_interval_expr(expr: &SqlExpr) -> bool {
    match expr {
        SqlExpr::Interval(_) => true,
        SqlExpr::Nested(inner) => is_sql_interval_expr(inner),
        SqlExpr::Function(function) => function.name.to_string().eq_ignore_ascii_case("__INTERVAL"),
        _ => false,
    }
}

/// Pass a `CAST(x AS DATETIME(N))` / `TIME(N)` fractional-seconds precision
/// through to the evaluator as an extra integer argument. Clamped to MySQL's
/// 0..=6 range; absent precision produces no extra arg (fsp 0).
fn cast_fsp_extra(p: Option<u64>) -> Vec<Expr> {
    match p {
        Some(n) => vec![Expr::Literal(Literal::Int(n.min(6) as i64))],
        None => vec![],
    }
}

fn function_args_distinct(args: &FunctionArguments) -> bool {
    match args {
        FunctionArguments::List(list) => {
            list.duplicate_treatment == Some(DuplicateTreatment::Distinct)
        }
        _ => false,
    }
}

/// Re-associate a JSON `->` / `->>` operator so it binds the path leaf at
/// the bottom-left of its (possibly mis-grouped) right operand, fixing the
/// precedence mismatch between sqlparser and MySQL. Recurses down the left
/// spine so chained shapes like `j -> '$.x' = 5 AND ...` rotate correctly at
/// any depth.
fn rethread_json_arrow(left: Expr, op: BinaryOp, right: Expr) -> Expr {
    match right {
        Expr::Binary {
            left: rl,
            op: rop,
            right: rr,
        } => Expr::Binary {
            left: Box::new(rethread_json_arrow(left, op, *rl)),
            op: rop,
            right: rr,
        },
        leaf => Expr::Binary {
            left: Box::new(left),
            op,
            right: Box::new(leaf),
        },
    }
}

/// Why ORDER BY / GROUP BY select-list references resolve here and not
/// later: every downstream consumer (planner Sort/TopK, vectorized bridge,
/// the in-memory CTE/join executor, fast paths) evaluates the clause exprs
/// against rows as ordinary expressions. A bare integer literal would
/// evaluate to a constant key — a silent no-op sort / single-bucket group —
/// so positional references must be substituted before the `Select` fans
/// out to any of those paths.
enum SelectListRefError {
    Allocation(ParseError),
    /// Position outside 1..=select-list width (MySQL 1054).
    OutOfRange(u64),
    /// Positional reference combined with a wildcard select item: the
    /// select-list width is unknown until execution, so the reference
    /// cannot be resolved at parse time.
    Wildcard(u64),
}

fn resolve_order_by_refs(
    order_by: Vec<OrderBy>,
    projection: &[SelectItem],
) -> Result<Vec<OrderBy>, SelectListRefError> {
    let has_wildcard = projection.iter().any(|item| {
        matches!(
            item,
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_)
        )
    });
    order_by
        .into_iter()
        .map(|mut item| {
            match &item.expr {
                Expr::Identifier(name) => {
                    if let Some(expr) = projection_alias_expr(projection, name) {
                        item.expr = clone_parser_expression(expr)
                            .map_err(SelectListRefError::Allocation)?;
                    }
                }
                // A plain integer literal in ORDER BY is a positional
                // reference into the select list (MySQL semantics). Only
                // raw number tokens reach here: `-1` stays a Unary minus
                // expression, so it is never mistaken for a position.
                Expr::Literal(Literal::Int(pos)) if *pos >= 0 && !has_wildcard => {
                    item.expr = ordinal_projection_expr(*pos as u64, projection, has_wildcard)?;
                }
                Expr::Literal(Literal::UInt(pos)) if !has_wildcard => {
                    item.expr = ordinal_projection_expr(*pos, projection, has_wildcard)?;
                }
                _ => {}
            }
            Ok(item)
        })
        .collect()
}

fn ordinal_projection_expr(
    pos: u64,
    projection: &[SelectItem],
    has_wildcard: bool,
) -> Result<Expr, SelectListRefError> {
    if has_wildcard {
        return Err(SelectListRefError::Wildcard(pos));
    }
    if pos >= 1 {
        if let Some(SelectItem::Expr { expr, .. }) = projection.get(pos as usize - 1) {
            return clone_parser_expression(expr).map_err(SelectListRefError::Allocation);
        }
    }
    Err(SelectListRefError::OutOfRange(pos))
}

/// GROUP BY positional-reference outcome. Splitting this from
/// `SelectListRefError` keeps the aggregate case (`GROUP BY 1` pointing at
/// `COUNT(*)` — MySQL 1056, not 1054) expressible without widening the
/// shared enum that ORDER BY also uses.
enum GroupByRefError {
    Ref(SelectListRefError),
    /// The referenced select item is an aggregate call; grouping on it is
    /// ill-formed (MySQL ER_WRONG_GROUP_FIELD). Carries the display name
    /// (alias if present, else the expression text).
    Aggregate(String),
}

/// Substitute `GROUP BY N` positional references with the N-th select
/// expression. Runs after `convert_group_by`, so grouping-set index lists
/// (which point into `group_by` by position) stay valid — only the exprs
/// they reference change. The planner's `resolve_group_ref_expr` still
/// handles aliases and `GROUPING(...)` arguments at plan time; by the time
/// it runs, ordinals from SQL text are already gone.
fn resolve_group_by_ordinals(
    group_by: &mut [Expr],
    projection: &[SelectItem],
) -> Result<(), GroupByRefError> {
    let has_wildcard = projection.iter().any(|item| {
        matches!(
            item,
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_)
        )
    });
    for expr in group_by.iter_mut() {
        let pos = match expr {
            Expr::Literal(Literal::Int(pos)) if *pos >= 0 => *pos as u64,
            Expr::Literal(Literal::UInt(pos)) => *pos,
            _ => continue,
        };
        let resolved =
            ordinal_projection_expr(pos, projection, has_wildcard).map_err(GroupByRefError::Ref)?;
        // Top-level aggregate select items cannot be grouped on. Deeper
        // nesting (`COUNT(*) + 1`) is left to the planner's existing
        // aggregate validation rather than a full expression walk here.
        if let Expr::Function { name, .. } = &resolved {
            if is_aggregate_function(name) {
                let display = match projection.get(pos as usize - 1) {
                    Some(SelectItem::Expr {
                        alias: Some(alias), ..
                    }) => alias.clone(),
                    _ => expr_to_sql(&resolved),
                };
                return Err(GroupByRefError::Aggregate(display));
            }
        }
        *expr = resolved;
    }
    Ok(())
}

/// Single source of truth for "is this function name an aggregate" —
/// shared by the planner (re-exported as `planner::is_aggregate_function`)
/// and the GROUP BY ordinal validation above.
pub fn is_aggregate_function(name: &str) -> bool {
    matches!(
        name.to_ascii_uppercase().as_str(),
        "COUNT"
            | "SUM"
            | "AVG"
            | "MIN"
            | "MAX"
            | "GROUP_CONCAT"
            | "JSON_ARRAYAGG"
            | "JSON_OBJECTAGG"
            | "BIT_AND"
            | "BIT_OR"
            | "BIT_XOR"
            | "STD"
            | "STDDEV"
            | "STDDEV_POP"
            | "STDDEV_SAMP"
            | "VARIANCE"
            | "VAR_POP"
            | "VAR_SAMP"
            | "ST_COLLECT"
            | "__HIDDEN_FIRST_VALUE"
    )
}

/// Native aggregate/window syntax is arity-checked by MySQL's parser.  Keep
/// those calls out of the executor when their shape is impossible; otherwise
/// accumulators historically ignored extra arguments (for example
/// `SUM(1, 2)`) or manufactured an empty aggregate for `COUNT()`.
fn validate_mysql_function_arity(
    name: &str,
    arity: usize,
    distinct: bool,
) -> Result<(), ParseError> {
    let valid = match name {
        // MOD is a keyword function with exactly two expr productions in
        // MySQL's grammar, rather than a generic variadic native call.
        "MOD" => arity == 2 && !distinct,
        "ROW" => arity >= 2 && !distinct,
        "COUNT" => arity == 1 || (distinct && arity > 1),
        "GROUP_CONCAT" => arity >= 1,
        "JSON_OBJECTAGG" | "NTH_VALUE" => arity == 2,
        "LAG" | "LEAD" => (1..=3).contains(&arity),
        "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "PERCENT_RANK" | "CUME_DIST" => arity == 0,
        "NTILE" | "FIRST_VALUE" | "LAST_VALUE" => arity == 1,
        "SUM" | "AVG" | "MIN" | "MAX" | "JSON_ARRAYAGG" | "BIT_AND" | "BIT_OR" | "BIT_XOR"
        | "STD" | "STDDEV" | "STDDEV_POP" | "STDDEV_SAMP" | "VARIANCE" | "VAR_POP" | "VAR_SAMP"
        | "ST_COLLECT" | "ANY_VALUE" => arity == 1,
        "GROUPING" => arity >= 1,
        _ => true,
    };
    if valid {
        Ok(())
    } else {
        Err(ParseError::at_zero(format!(
            "incorrect parameter count in the call to native function '{name}'"
        )))
    }
}

/// Alias/ordinal lowering can duplicate a large tree many times from short
/// source references. Reserve that retained output before cloning it.
fn clone_parser_expression(expr: &Expr) -> Result<Expr, ParseError> {
    struct Size(usize);
    impl visit::AstVisitor for Size {
        fn enter_expr(&mut self, expr: &Expr) {
            self.0 = self.0.saturating_add(2 * std::mem::size_of::<Expr>());
            self.0 = self.0.saturating_add(match expr {
                Expr::Identifier(s)
                | Expr::UserVariable(s)
                | Expr::Default(Some(s))
                | Expr::Literal(Literal::String(s) | Literal::Decimal(s)) => s.len(),
                Expr::Literal(Literal::HexBytes(b) | Literal::BitBytes(b)) => b.len(),
                Expr::CompoundIdentifier(parts) => {
                    parts.iter().map(String::len).sum::<usize>()
                        + parts.len() * std::mem::size_of::<String>()
                }
                Expr::Function { name, .. } => name.len(),
                Expr::WindowFunction { name, .. } => name.len() + std::mem::size_of::<WindowSpec>(),
                Expr::BoundValue(value) => value.heap_capacity(),
                _ => 0,
            });
        }
        fn enter_select(&mut self, select: &Select) {
            self.0 = self.0.saturating_add(2 * std::mem::size_of::<Select>());
            for item in &select.projection {
                if let SelectItem::Expr {
                    alias: Some(name), ..
                } = item
                {
                    self.0 = self.0.saturating_add(name.len());
                }
            }
            self.0 = self.0.saturating_add(
                select
                    .grouping_sets
                    .iter()
                    .map(|set| {
                        2 * set.len() * std::mem::size_of::<usize>()
                            + std::mem::size_of::<Vec<usize>>()
                    })
                    .sum::<usize>(),
            );
        }
    }
    let mut size = Size(0);
    with_parser_stack(|| visit::visit_expr(&mut size, expr))?;
    if size.0 > 1024 {
        reserve_parser_expansion(size.0)?;
    }
    with_parser_stack(|| expr.clone())
}

fn projection_alias_expr<'a>(projection: &'a [SelectItem], name: &str) -> Option<&'a Expr> {
    projection.iter().find_map(|item| match item {
        SelectItem::Expr {
            expr,
            alias: Some(alias),
        } if alias.eq_ignore_ascii_case(name) => Some(expr),
        _ => None,
    })
}

/// Extract the lowercased user-variable name from an assignment target,
/// erroring if the left-hand side is not a `@var` reference.
fn user_var_assign_target(expr: &SqlExpr) -> Result<String, ParseError> {
    match convert_expr(expr)? {
        Expr::UserVariable(name) => Ok(name),
        _ => Err(parse_error_at_zero(
            "assignment operator ':=' requires a user variable (@var) on the left-hand side",
        )),
    }
}

/// Constant-fold bulk-ingest functions whose sole argument is a string literal:
/// `STRING_TO_VECTOR` / `TO_VECTOR` become native VECTOR bytes and `FROM_BASE64`
/// becomes binary bytes.
///
/// Applied only to INSERT … VALUES (and INSERT … SET) row values — the
/// perf-critical bulk-vector ingest path. Folding here (rather than globally in
/// [`convert_expr`]) keeps `SHOW CREATE` / generated-column / DEFAULT-expression
/// round-trips rendering the original function text. On any parse failure the
/// expression is returned unchanged so the eval-time error (and its exact MySQL
/// message) is preserved. This mirrors the fast path's
/// [`InsertCursor::parse_string_to_vector`] so both produce byte-identical ASTs.
pub(super) fn fold_const_string_to_vector(expr: Expr) -> Expr {
    if let Expr::Function {
        name,
        args,
        distinct: false,
    } = &expr
    {
        if args.len() == 1
            && (name.eq_ignore_ascii_case("STRING_TO_VECTOR")
                || name.eq_ignore_ascii_case("TO_VECTOR"))
        {
            if let Expr::Literal(Literal::String(s)) = &args[0] {
                if let Ok(bytes) = crate::engine::vector::codec::parse_text(s) {
                    return Expr::BoundValue(crate::types::Value::Vector(bytes.into()));
                }
            }
        }
        if args.len() == 1 && name.eq_ignore_ascii_case("FROM_BASE64") {
            if let Expr::Literal(Literal::String(s)) = &args[0] {
                if let Some(bytes) = super::manual::decode_base64_bytes(s) {
                    return Expr::BoundValue(crate::types::Value::Bytes(bytes));
                }
            }
        }
    }
    expr
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum LimitValueError {
    /// Value is malformed (negative, fractional, NULL, out of range, etc.).
    /// Surfaced to the client as a parse error (1064).
    Invalid(String),
    /// Expression form is not yet recognized for LIMIT/OFFSET (column ref, function call, ...).
    /// Surfaced as Statement::Unsupported (1235).
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum LimitClauseError {
    Invalid(String),
    Unsupported,
}

impl From<LimitValueError> for LimitClauseError {
    fn from(err: LimitValueError) -> Self {
        match err {
            LimitValueError::Invalid(msg) => LimitClauseError::Invalid(msg),
            LimitValueError::Unsupported => LimitClauseError::Unsupported,
        }
    }
}

fn parse_limit_number(text: &str) -> Result<LimitValue, LimitValueError> {
    if text.contains('.') {
        return Err(LimitValueError::Invalid(format!(
            "LIMIT/OFFSET value must be a non-negative integer, got '{text}'"
        )));
    }
    text.parse::<usize>().map(LimitValue::Rows).map_err(|_| {
        LimitValueError::Invalid(format!("LIMIT/OFFSET value '{text}' is out of range"))
    })
}

/// Extract the bare table name of a USING-form DELETE target (`DELETE FROM t1,
/// t2 USING ...`). The target is a plain table reference (no joins, no
/// subquery); it names a table/alias that appears in the USING clause.
fn delete_target_name(table: &TableWithJoins) -> Result<String, String> {
    if !table.joins.is_empty() {
        return Err("DELETE target with joins".to_string());
    }
    match &table.relation {
        TableFactor::Table { name, args, .. } if args.is_none() => {
            object_name_to_string(name).map_err(|err| err.message)
        }
        _ => Err("DELETE target".to_string()),
    }
}

/// True for an unqualified, case-insensitive `generate_series` table-function
/// name. Qualified names (`schema.generate_series`) are not treated as the
/// builtin.
fn object_name_is_generate_series(name: &ObjectName) -> bool {
    matches!(name.0.as_slice(), [ObjectNamePart::Identifier(ident)]
        if ident.value.eq_ignore_ascii_case("generate_series"))
}

fn table_alias_columns(alias: Option<&sqlparser::ast::TableAlias>) -> Vec<String> {
    alias
        .map(|alias| {
            alias
                .columns
                .iter()
                .map(|column| column.name.value.clone())
                .collect()
        })
        .unwrap_or_default()
}

/// Extract positional argument expressions from a table-function call,
/// rejecting named arguments and wildcards (not valid for `generate_series`).
fn generate_series_args(table_args: &TableFunctionArgs) -> Result<Vec<Expr>, TableConversionError> {
    let TableFunctionArgs { args, settings } = table_args;
    if settings.is_some() {
        return Err("generate_series SETTINGS are not supported"
            .to_owned()
            .into());
    }
    let mut exprs = Vec::with_capacity(args.len());
    for arg in args {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                exprs.push(convert_expr(expr)?);
            }
            _ => {
                return Err("generate_series arguments must be positional expressions"
                    .to_string()
                    .into())
            }
        }
    }
    Ok(exprs)
}

fn json_table_path_value(value: &SqlValue) -> Result<String, ParseError> {
    value
        .clone()
        .into_string()
        .ok_or_else(|| parse_error_at_zero("JSON_TABLE path must be a string literal"))
}

fn is_dual_table(table: &TableRef) -> bool {
    table.name.eq_ignore_ascii_case("dual") && table.alias.is_none() && table.index_hints.is_empty()
}

pub(super) fn parse_query_hints(text: &str) -> (Vec<QueryHint>, Vec<String>) {
    let mut hints = Vec::new();
    let mut warnings: Vec<String> = Vec::new();
    let text = text.trim();
    if text.is_empty() {
        return (hints, warnings);
    }
    let mut pos = 0;
    let bytes = text.as_bytes();
    while pos < bytes.len() {
        while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }
        if pos >= bytes.len() {
            break;
        }
        let name_start = pos;
        while pos < bytes.len() && bytes[pos] != b'(' && !bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }
        let name = &text[name_start..pos];
        while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }
        let args = if pos < bytes.len() && bytes[pos] == b'(' {
            pos += 1; // skip '('
            let args_start = pos;
            let mut depth = 1u32;
            let mut quote = None;
            while pos < bytes.len() && depth > 0 {
                if let Some(q) = quote {
                    if bytes[pos] == b'\\' && q != b'`' {
                        pos = (pos + 2).min(bytes.len());
                        continue;
                    }
                    if bytes[pos] == q {
                        if bytes.get(pos + 1) == Some(&q) {
                            pos += 2;
                            continue;
                        }
                        quote = None;
                    }
                    pos += 1;
                    continue;
                }
                if matches!(bytes[pos], b'\'' | b'"' | b'`') {
                    quote = Some(bytes[pos]);
                } else if bytes[pos] == b'(' {
                    depth += 1;
                } else if bytes[pos] == b')' {
                    depth -= 1;
                }
                if depth > 0 {
                    pos += 1;
                }
            }
            if depth != 0 || quote.is_some() {
                warnings.push(format!("{name}: unterminated argument list"));
                break;
            }
            let args_str = text[args_start..pos].trim();
            if pos < bytes.len() {
                pos += 1; // skip ')'
            }
            split_hint_arguments(args_str)
        } else {
            vec![]
        };

        let upper = name.to_ascii_uppercase();
        match upper.as_str() {
            // MySQL accepts INDEX(table index...) as the original spelling
            // of the query-block index hint; USE_INDEX is its newer explicit
            // synonym. Both carry identical planner semantics.
            "USE_INDEX" | "INDEX" => {
                if args.len() >= 2 {
                    hints.push(QueryHint::UseIndex {
                        table: args[0].clone(),
                        indexes: args[1..].to_vec(),
                    });
                } else {
                    warnings.push(format!("{upper}: requires (table, index...)"));
                }
            }
            "NO_INDEX" => {
                if args.len() == 2 {
                    hints.push(QueryHint::NoIndex {
                        table: args[0].clone(),
                        index: args[1].clone(),
                    });
                } else {
                    warnings.push("NO_INDEX: requires (table, index)".to_string());
                }
            }
            "INDEX_MERGE" => {
                if !args.is_empty() {
                    hints.push(QueryHint::IndexMerge {
                        table: args[0].clone(),
                        indexes: args[1..].to_vec(),
                    });
                } else {
                    warnings.push("INDEX_MERGE: requires (table [index...])".to_string());
                }
            }
            "NO_INDEX_MERGE" => {
                if !args.is_empty() {
                    hints.push(QueryHint::NoIndexMerge {
                        table: args[0].clone(),
                        indexes: args[1..].to_vec(),
                    });
                } else {
                    warnings.push("NO_INDEX_MERGE: requires (table [index...])".to_string());
                }
            }
            "FULL_SCAN" => {
                if args.len() == 1 {
                    hints.push(QueryHint::FullScan {
                        table: args[0].clone(),
                    });
                } else {
                    warnings.push("FULL_SCAN: requires at least one argument".to_string());
                }
            }
            "ZONE_MAP" => {
                if args.len() == 1 {
                    hints.push(QueryHint::ZoneMap {
                        table: args[0].clone(),
                    });
                } else {
                    warnings.push("ZONE_MAP: requires (table)".to_string());
                }
            }
            "NO_ZONE_MAP" => {
                if args.len() == 1 {
                    hints.push(QueryHint::NoZoneMap {
                        table: args[0].clone(),
                    });
                } else {
                    warnings.push("NO_ZONE_MAP: requires (table)".to_string());
                }
            }
            "HASH_JOIN" => {
                if !args.is_empty() {
                    hints.push(QueryHint::HashJoin { tables: args });
                } else {
                    warnings.push("HASH_JOIN: requires at least one argument".to_string());
                }
            }
            "NO_HASH_JOIN" => {
                if !args.is_empty() {
                    hints.push(QueryHint::NoHashJoin { tables: args });
                } else {
                    warnings.push("NO_HASH_JOIN: requires at least one argument".to_string());
                }
            }
            "NL_JOIN" => {
                if !args.is_empty() {
                    hints.push(QueryHint::NlJoin { tables: args });
                } else {
                    warnings.push("NL_JOIN: requires at least one argument".to_string());
                }
            }
            "INDEX_NLJ" => {
                if !args.is_empty() {
                    hints.push(QueryHint::IndexNlj { tables: args });
                } else {
                    warnings.push("INDEX_NLJ: requires at least one argument".to_string());
                }
            }
            "ORDERED" => {
                if args.is_empty() {
                    hints.push(QueryHint::Ordered);
                } else {
                    warnings.push("ORDERED: no arguments expected".to_string());
                }
            }
            "INDEX_FFS" => {
                if matches!(args.len(), 1 | 2) {
                    hints.push(QueryHint::IndexFfs {
                        table: args[0].clone(),
                        index: args.get(1).cloned(),
                    });
                } else {
                    warnings.push("INDEX_FFS: requires at least one argument".to_string());
                }
            }
            "PARALLEL" => {
                let degree = match args.first() {
                    Some(s) => match s.parse::<u32>() {
                        Ok(d) => d,
                        Err(_) => {
                            warnings.push(format!("PARALLEL: invalid degree \"{}\"", s));
                            0
                        }
                    },
                    None => 0,
                };
                if args.len() > 1 {
                    warnings.push("PARALLEL: extra arguments ignored".to_string());
                }
                hints.push(QueryHint::Parallel { degree });
            }
            "NO_PARALLEL" => {
                if args.is_empty() {
                    hints.push(QueryHint::NoParallel);
                } else {
                    warnings.push("NO_PARALLEL: no arguments expected".to_string());
                }
            }
            "MAX_EXECUTION_TIME" => {
                let ms = match args.first() {
                    Some(s) => match s.trim().parse::<u64>() {
                        Ok(n) => n,
                        Err(_) => {
                            warnings.push(format!("MAX_EXECUTION_TIME: invalid argument \"{s}\""));
                            0
                        }
                    },
                    None => {
                        warnings.push("MAX_EXECUTION_TIME: missing argument".to_string());
                        0
                    }
                };
                if args.len() > 1 {
                    warnings.push("MAX_EXECUTION_TIME: extra arguments ignored".to_string());
                }
                hints.push(QueryHint::MaxExecutionTime { ms });
            }
            "SET_VAR" => {
                if args.len() != 1 {
                    warnings.push("SET_VAR: requires (name=value)".to_string());
                } else if let Some((name, value)) = args[0].split_once('=') {
                    let name = name.trim().to_ascii_lowercase();
                    let value = value
                        .trim()
                        .trim_matches('\'')
                        .trim_matches('"')
                        .to_string();
                    if matches!(
                        name.as_str(),
                        "select_into_buffer_size"
                            | "select_into_disk_sync"
                            | "select_into_disk_sync_delay"
                    ) {
                        hints.push(QueryHint::SetVar { name, value });
                    } else {
                        warnings.push(format!("unknown hint \"SET_VAR\""));
                    }
                } else {
                    warnings.push("SET_VAR: requires (name=value)".to_string());
                }
            }
            "HNSW_EF" => {
                let ef = match args.first() {
                    Some(s) => match s.trim().parse::<u64>() {
                        Ok(n) => n,
                        Err(_) => {
                            warnings.push(format!("HNSW_EF: invalid argument \"{s}\""));
                            0
                        }
                    },
                    None => {
                        warnings.push("HNSW_EF: missing argument".to_string());
                        0
                    }
                };
                if args.len() > 1 {
                    warnings.push("HNSW_EF: extra arguments ignored".to_string());
                }
                hints.push(QueryHint::HnswEf { ef });
            }
            "IVF_NPROBE" => {
                let nprobe = match args.first() {
                    Some(s) => match s.trim().parse::<u64>() {
                        Ok(n) => n,
                        Err(_) => {
                            warnings.push(format!("IVF_NPROBE: invalid argument \"{s}\""));
                            0
                        }
                    },
                    None => {
                        warnings.push("IVF_NPROBE: missing argument".to_string());
                        0
                    }
                };
                if args.len() > 1 {
                    warnings.push("IVF_NPROBE: extra arguments ignored".to_string());
                }
                hints.push(QueryHint::IvfNprobe { nprobe });
            }
            "NO_VECTOR_INDEX" => {
                if !args.is_empty() {
                    warnings.push("NO_VECTOR_INDEX: no arguments expected".to_string());
                }
                hints.push(QueryHint::NoVectorIndex);
            }
            _ => {
                warnings.push(format!("unknown hint \"{}\"", upper));
            }
        }
    }
    dedupe_query_hints(hints, warnings)
}

fn split_hint_arguments(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut start = None;
    let mut quote = None;
    let bytes = text.as_bytes();
    let mut pos = 0usize;
    while pos < bytes.len() {
        let byte = bytes[pos];
        if let Some(q) = quote {
            if byte == b'\\' && q != b'`' {
                pos = (pos + 2).min(bytes.len());
                continue;
            }
            if byte == q {
                if bytes.get(pos + 1) == Some(&q) {
                    pos += 2;
                    continue;
                }
                quote = None;
            }
            pos += 1;
            continue;
        }
        if matches!(byte, b'\'' | b'"' | b'`') {
            start.get_or_insert(pos);
            quote = Some(byte);
        } else if byte == b',' || byte.is_ascii_whitespace() {
            if let Some(begin) = start.take() {
                out.push(unquote_identifier(text[begin..pos].trim()));
            }
        } else {
            start.get_or_insert(pos);
        }
        pos += 1;
    }
    if let Some(begin) = start {
        out.push(unquote_identifier(text[begin..].trim()));
    }
    out.into_iter().filter(|arg| !arg.is_empty()).collect()
}

fn dedupe_query_hints(
    hints: Vec<QueryHint>,
    mut warnings: Vec<String>,
) -> (Vec<QueryHint>, Vec<String>) {
    let mut accepted = Vec::with_capacity(hints.len());
    let mut seen = std::collections::HashSet::new();
    for hint in hints {
        let key = match &hint {
            QueryHint::UseIndex { table, .. }
            | QueryHint::NoIndex { table, .. }
            | QueryHint::IndexMerge { table, .. }
            | QueryHint::NoIndexMerge { table, .. }
            | QueryHint::FullScan { table }
            | QueryHint::IndexFfs { table, .. } => format!("ACCESS:{}", table.to_ascii_lowercase()),
            QueryHint::ZoneMap { table } | QueryHint::NoZoneMap { table } => {
                format!("ZONE_MAP:{}", table.to_ascii_lowercase())
            }
            QueryHint::HashJoin { tables }
            | QueryHint::NoHashJoin { tables }
            | QueryHint::NlJoin { tables }
            | QueryHint::IndexNlj { tables } => {
                let mut targets = tables
                    .iter()
                    .map(|table| table.to_ascii_lowercase())
                    .collect::<Vec<_>>();
                targets.sort_unstable();
                format!("JOIN_METHOD:{}", targets.join(","))
            }
            QueryHint::Ordered => "ORDERED".to_string(),
            QueryHint::Parallel { .. } | QueryHint::NoParallel => "PARALLEL".to_string(),
            QueryHint::MaxExecutionTime { .. } => "MAX_EXECUTION_TIME".to_string(),
            QueryHint::SetVar { name, .. } => {
                format!("SET_VAR:{}", name.to_ascii_lowercase())
            }
            QueryHint::HnswEf { .. } => "HNSW_EF".to_string(),
            QueryHint::IvfNprobe { .. } => "IVF_NPROBE".to_string(),
            QueryHint::NoVectorIndex => "NO_VECTOR_INDEX".to_string(),
        };
        if seen.insert(key.clone()) {
            accepted.push(hint);
        } else if matches!(hint, QueryHint::NoParallel) && key == "PARALLEL" {
            // NO_PARALLEL is a safety override, not a first/last-wins tuning
            // choice.  Keep its documented precedence even when PARALLEL
            // appeared first in the same optimizer comment.
            if let Some(existing) = accepted
                .iter_mut()
                .find(|existing| matches!(existing, QueryHint::Parallel { .. }))
            {
                *existing = QueryHint::NoParallel;
            }
            warnings.push("PARALLEL: conflicting hint ignored; NO_PARALLEL wins".to_string());
        } else {
            warnings.push(format!(
                "{key}: duplicate or conflicting hint ignored (first wins)"
            ));
        }
    }
    (accepted, warnings)
}

fn table_alias_name(alias: &TableAlias) -> String {
    alias.name.value.clone()
}

fn assignment_column_name(name: &ObjectName) -> Result<String, ParseError> {
    let mut last = "";
    for part in &name.0 {
        match part {
            ObjectNamePart::Identifier(ident) => last = &ident.value,
            ObjectNamePart::Function(_) => {
                return parse_error_from_sql_span(
                    sqlparser::tokenizer::Span::empty(),
                    "unsupported object name",
                    vec![],
                )
            }
        }
    }
    Ok(last.to_owned())
}

fn index_prefix_len_from_function_args(args: &FunctionArguments) -> Result<usize, ParseError> {
    let FunctionArguments::List(list) = args else {
        return Err(parse_error_at_zero("index prefix length"));
    };
    if list.args.len() != 1 {
        return Err(parse_error_at_zero("index prefix length"));
    }
    let FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::Value(value))) = &list.args[0] else {
        return Err(parse_error_at_zero("index prefix length"));
    };
    let SqlValue::Number(text, _) = &value.value else {
        return Err(parse_error_at_zero("index prefix length"));
    };
    let len = text
        .parse::<usize>()
        .map_err(|_| parse_error_at_zero("index prefix length"))?;
    if len == 0 {
        return Err(parse_error_at_zero(
            "index prefix length must be greater than zero",
        ));
    }
    Ok(len)
}

const DECIMAL_MAX_PRECISION: u64 = 65;
const DECIMAL_MAX_SCALE: u64 = 30;

fn validate_decimal_precision_scale(p: u64, s: i64) -> Result<(u8, u8), ParseError> {
    if s < 0 {
        return Err(numeric_spec_error(
            crate::error::catalog::ER_TOO_BIG_SCALE,
            format!("DECIMAL scale {s} must not be negative"),
        ));
    }
    let s = s as u64;
    if s > DECIMAL_MAX_SCALE {
        return Err(numeric_spec_error(
            crate::error::catalog::ER_TOO_BIG_SCALE,
            format!("Too big scale {s} specified. Maximum is {DECIMAL_MAX_SCALE}"),
        ));
    }
    if p > DECIMAL_MAX_PRECISION {
        return Err(numeric_spec_error(
            crate::error::catalog::ER_TOO_BIG_PRECISION,
            format!("Too-big precision {p} specified. Maximum is {DECIMAL_MAX_PRECISION}"),
        ));
    }
    if s > p {
        return Err(numeric_spec_error(
            crate::error::catalog::ER_M_BIGGER_THAN_D,
            "For float(M,D), double(M,D) or decimal(M,D), M must be >= D",
        ));
    }
    // MySQL retains the historical DECIMAL(0,0) spelling and normalizes it
    // to the default DECIMAL(10,0) definition.
    if p == 0 {
        return Ok((10, 0));
    }
    Ok((p as u8, s as u8))
}

/// Build the existing dot-separated internal name in one allocation. Parts
/// remain borrowed; invalid object-name forms fail before allocation.
fn join_name_parts<'a>(
    parts: impl Iterator<Item = Result<&'a str, ParseError>> + Clone,
) -> Result<String, ParseError> {
    let capacity = parts
        .clone()
        .enumerate()
        .try_fold(0, |len, (index, part)| {
            Ok::<_, ParseError>(len + part?.len() + usize::from(index != 0))
        })?;
    let mut name = String::with_capacity(capacity);
    for (index, part) in parts.enumerate() {
        if index != 0 {
            name.push('.');
        }
        name.push_str(part?);
    }
    Ok(name)
}

fn object_name_to_qualified(name: ObjectName) -> Result<QualifiedName, ParseError> {
    let mut parts = name.0.into_iter();
    let first = match parts.next() {
        Some(ObjectNamePart::Identifier(ident)) => ident.value,
        _ => return Err(parse_error_at_zero("expected catalog object name")),
    };
    let (database, object) = match parts.next() {
        Some(ObjectNamePart::Identifier(ident)) => (Some(first), ident.value),
        None => (None, first),
        _ => return Err(parse_error_at_zero("expected catalog object name")),
    };
    if parts.next().is_some() {
        return Err(parse_error_at_zero("expected [database.]object"));
    }
    QualifiedName::new(database, object).map_err(parse_error_at_zero)
}

fn object_name_to_string(name: impl std::borrow::Borrow<ObjectName>) -> Result<String, ParseError> {
    join_name_parts(name.borrow().0.iter().map(|part| match part {
        ObjectNamePart::Identifier(ident) => Ok(ident.value.as_str()),
        ObjectNamePart::Function(_) => parse_error_from_sql_span(
            sqlparser::tokenizer::Span::empty(),
            "unsupported object name",
            vec![],
        ),
    }))
}

fn ident_value(ident: Ident) -> String {
    ident.value
}

pub(crate) fn parse_catalog_expr(sql: &str) -> Result<Expr, ParseError> {
    let _mode = ParserSqlModeGuard::activate(0);
    parse_expr_fragment(sql)
}

pub fn parse_expr_fragment(sql: &str) -> Result<Expr, ParseError> {
    let mut projection = parse_projection_fragment(sql)?;
    if projection.len() != 1 {
        return Err(parse_error_full_span(
            sql,
            "expected exactly one expression",
        ));
    }
    let SelectItem::Expr { expr, .. } = projection.pop().unwrap() else {
        unreachable!()
    };
    Ok(expr)
}

pub(super) fn parse_expr_list_fragment(sql: &str) -> Result<Vec<Expr>, ParseError> {
    Ok(parse_projection_fragment(sql)?
        .into_iter()
        .map(|item| {
            let SelectItem::Expr { expr, .. } = item else {
                unreachable!()
            };
            expr
        })
        .collect())
}

fn parse_projection_fragment(sql: &str) -> Result<Vec<SelectItem>, ParseError> {
    // SELECT conversion normally synthesizes a display alias from an
    // unaliased expression's source text. Disable only that presentation
    // feature here, so a real user alias remains distinguishable from `1+2`.
    let _fragment = ParserFragmentGuard::activate();
    let (projection_sql, source_map) = edit_source(
        sql,
        &[TextEdit {
            range: 0..0,
            replacement: "SELECT ",
        }],
    )?;
    let statement = parse_statement(&projection_sql).map_err(|err| source_map.error(sql, err))?;
    let Statement::Select(select) = statement else {
        return Err(parse_error_at_zero("expected expression"));
    };
    // This API is used for persisted CHECK/generated/partition expressions.
    // Accepting a SELECT tail here would silently truncate source such as
    // `1, 2`, `1 FROM t` or `1 WHERE false` to the first expression.
    let Select {
        projection,
        table,
        from,
        values,
        json_table,
        derived,
        no_merge,
        straight_join,
        joins,
        selection,
        group_by,
        grouping_sets,
        group_by_modifier,
        having,
        order_by,
        limit,
        offset,
        distinct,
        for_update,
        lock_clause,
        sql_calc_found_rows,
        named_windows,
        query_hints,
        hint_warnings,
    } = select;
    let clean_tail = table.is_none()
        && from.is_empty()
        && values.is_none()
        && json_table.is_none()
        && derived.is_none()
        && !no_merge
        && !straight_join
        && joins.is_empty()
        && selection.is_none()
        && group_by.is_empty()
        && grouping_sets.is_empty()
        && group_by_modifier == GroupByModifier::None
        && having.is_none()
        && order_by.is_empty()
        && limit.is_none()
        && offset.is_none()
        && !distinct
        && !for_update
        && lock_clause.is_none()
        && !sql_calc_found_rows
        && named_windows.is_empty()
        && query_hints.is_empty()
        && hint_warnings.is_empty();
    if !clean_tail || projection.is_empty() {
        return Err(parse_error_full_span(
            sql,
            "expected exactly one expression",
        ));
    }
    if projection
        .iter()
        .any(|item| !matches!(item, SelectItem::Expr { alias: None, .. }))
    {
        return Err(parse_error_full_span(
            sql,
            "expression aliases and wildcard projections are not allowed",
        ));
    }
    Ok(projection)
}

fn parse_auto_increment_value(value: &SqlValue) -> Result<u64, ParseError> {
    let SqlValue::Number(text, _) = value else {
        return Err(parse_error_at_zero(
            "AUTO_INCREMENT value must be a positive integer literal",
        ));
    };
    if text.contains('.') {
        return Err(parse_error_at_zero(
            "AUTO_INCREMENT value must be a positive integer literal",
        ));
    }
    let value = text.parse::<u64>().map_err(|_| {
        parse_error_at_zero("AUTO_INCREMENT value must be a positive integer literal")
    })?;
    // S28 — `AUTO_INCREMENT = 0` is accepted (a no-op at the storage layer:
    // the counter can never be lowered below its current max), matching MySQL,
    // rather than rejected at parse time.
    Ok(value)
}
