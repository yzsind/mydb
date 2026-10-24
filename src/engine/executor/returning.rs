//! PostgreSQL-style DML RETURNING projection.

use super::*;

#[derive(Debug, Clone, Copy)]
pub(super) enum DefaultImage {
    Old,
    New,
}

pub(super) struct ReturningCollector {
    projector: Option<ReturningProjector>,
    rows: Vec<Vec<Value>>,
}

struct ReturningProjector {
    eval_table: TableSchema,
    columns: Vec<Column>,
    program: super::select::TableProjectionProgram,
    base_width: usize,
    null_row: Vec<Value>,
    default_image: DefaultImage,
}

impl ReturningCollector {
    pub(super) fn new(
        clause: Option<&crate::parser::ReturningClause>,
        table: &TableSchema,
        default_image: DefaultImage,
        target_alias: Option<&str>,
    ) -> Result<Self, QueryError> {
        let Some(clause) = clause else {
            return Ok(Self {
                projector: None,
                rows: Vec::new(),
            });
        };
        let projector = ReturningProjector::new(clause, table, default_image, target_alias)?;
        Ok(Self {
            projector: Some(projector),
            rows: Vec::new(),
        })
    }

    pub(super) fn push(
        &mut self,
        old: Option<&[Value]>,
        new: Option<&[Value]>,
    ) -> Result<(), QueryError> {
        if let Some(projector) = &self.projector {
            self.rows.push(projector.project(old, new)?);
        }
        Ok(())
    }

    pub(super) fn finish(self, fallback: QueryResponse) -> QueryResponse {
        match self.projector {
            Some(projector) => QueryResponse::ResultSet(ResultSet {
                columns: projector.columns,
                rows: self.rows,
            }),
            None => fallback,
        }
    }
}

pub(super) fn describe_returning(
    clause: Option<&crate::parser::ReturningClause>,
    table: &TableSchema,
    default_image: DefaultImage,
    target_alias: Option<&str>,
) -> Result<Vec<Column>, QueryError> {
    let collector = ReturningCollector::new(clause, table, default_image, target_alias)?;
    Ok(collector.finish(QueryResponse::Ok).columns())
}

impl ReturningProjector {
    fn new(
        clause: &crate::parser::ReturningClause,
        table: &TableSchema,
        default_image: DefaultImage,
        target_alias: Option<&str>,
    ) -> Result<Self, QueryError> {
        if clause.projection.iter().any(|item| match item {
            SelectItem::Expr { expr, .. } => returning_expr_forbidden(expr),
            _ => false,
        }) {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "aggregate and window functions are not allowed in RETURNING",
            ));
        }

        let old_name = clause.old_alias.as_deref().unwrap_or("old");
        let new_name = clause.new_alias.as_deref().unwrap_or("new");
        let hidden_old = clause.old_alias.is_some();
        let hidden_new = clause.new_alias.is_some();
        let target_name = target_alias.unwrap_or(table.name.as_str());

        let mut eval_table = table.clone();
        let base_width = eval_table.columns.len();
        let old_columns: Vec<_> = table
            .columns
            .iter()
            .enumerate()
            .map(|(idx, col)| {
                let mut col = col.clone();
                col.name = format!("__mydb_returning_old_{idx}");
                col
            })
            .collect();
        let new_columns: Vec<_> = table
            .columns
            .iter()
            .enumerate()
            .map(|(idx, col)| {
                let mut col = col.clone();
                col.name = format!("__mydb_returning_new_{idx}");
                col
            })
            .collect();
        eval_table.columns.extend(old_columns);
        eval_table.columns.extend(new_columns);
        eval_table.build_column_index();

        let mut projection = Vec::new();
        for item in &clause.projection {
            match item {
                SelectItem::Wildcard => {
                    let prefix = match default_image {
                        DefaultImage::Old => "__mydb_returning_old_",
                        DefaultImage::New => "__mydb_returning_new_",
                    };
                    for (idx, col) in table.iter_select_columns() {
                        projection.push(SelectItem::Expr {
                            expr: Expr::Identifier(format!("{prefix}{idx}")),
                            alias: Some(col.name.clone()),
                        });
                    }
                }
                SelectItem::QualifiedWildcard(qualifier) => {
                    let q = qualifier.rsplit('.').next().unwrap_or(qualifier);
                    let image = if q.eq_ignore_ascii_case(old_name) {
                        Some(DefaultImage::Old)
                    } else if q.eq_ignore_ascii_case(new_name) {
                        Some(DefaultImage::New)
                    } else if q.eq_ignore_ascii_case(target_name)
                        || (target_alias.is_none() && q.eq_ignore_ascii_case(&table.name))
                    {
                        Some(default_image)
                    } else {
                        None
                    };
                    let Some(image) = image else {
                        return Err(error(
                            crate::error::catalog::ER_BAD_FIELD_ERROR,
                            format!("Unknown table '{}' in RETURNING", qualifier),
                        ));
                    };
                    let prefix = match image {
                        DefaultImage::Old => "__mydb_returning_old_",
                        DefaultImage::New => "__mydb_returning_new_",
                    };
                    for (idx, col) in table.iter_select_columns() {
                        projection.push(SelectItem::Expr {
                            expr: Expr::Identifier(format!("{prefix}{idx}")),
                            alias: Some(col.name.clone()),
                        });
                    }
                }
                SelectItem::Expr {
                    expr: Expr::Identifier(name),
                    alias: None,
                } if name.eq_ignore_ascii_case(target_name)
                    || (target_alias.is_none() && name.eq_ignore_ascii_case(&table.name)) =>
                {
                    let prefix = match default_image {
                        DefaultImage::Old => "__mydb_returning_old_",
                        DefaultImage::New => "__mydb_returning_new_",
                    };
                    for (idx, col) in table.iter_select_columns() {
                        projection.push(SelectItem::Expr {
                            expr: Expr::Identifier(format!("{prefix}{idx}")),
                            alias: Some(col.name.clone()),
                        });
                    }
                }
                SelectItem::Expr { expr, alias } => {
                    let display_alias = alias.clone().or_else(|| match expr {
                        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
                            Some(parts.join("."))
                        }
                        _ => None,
                    });
                    let mut expr = expr.clone();
                    rewrite_image_refs(
                        &mut expr,
                        table,
                        old_name,
                        new_name,
                        hidden_old,
                        hidden_new,
                        target_name,
                        default_image,
                    )?;
                    projection.push(SelectItem::Expr {
                        expr,
                        alias: display_alias,
                    });
                }
            }
        }
        let select = Select {
            projection,
            ..Select::default()
        };
        let mut columns = single_table_result_columns(&select, &eval_table, None)?;
        for column in &mut columns {
            let image_index = column
                .source
                .org_name
                .strip_prefix("__mydb_returning_old_")
                .or_else(|| column.source.org_name.strip_prefix("__mydb_returning_new_"))
                .and_then(|suffix| suffix.parse::<usize>().ok());
            if let Some(source) = image_index.and_then(|index| table.columns.get(index)) {
                column.source.org_name = source.name.clone();
            }
        }
        let program =
            super::select::TableProjectionProgram::compile_once(&select.projection, &eval_table);
        let null_row = vec![Value::Null; base_width];
        Ok(Self {
            eval_table,
            columns,
            program,
            base_width,
            null_row,
            default_image,
        })
    }

    fn project(
        &self,
        old: Option<&[Value]>,
        new: Option<&[Value]>,
    ) -> Result<Vec<Value>, QueryError> {
        let old = old.unwrap_or(&self.null_row);
        let new = new.unwrap_or(&self.null_row);
        let default = match self.default_image {
            DefaultImage::Old => old,
            DefaultImage::New => new,
        };
        let mut values = Vec::with_capacity(self.base_width * 3);
        values.extend_from_slice(default);
        values.extend_from_slice(old);
        values.extend_from_slice(new);
        self.program.project_row(&self.eval_table, &values)
    }
}

fn returning_expr_forbidden(expr: &Expr) -> bool {
    match expr {
        Expr::WindowFunction { .. } => true,
        Expr::Function { name, args, .. } => {
            planner::is_aggregate_function(name) || args.iter().any(returning_expr_forbidden)
        }
        Expr::Unary { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::GroupKeyRef { expr, .. } => returning_expr_forbidden(expr),
        Expr::Binary { left, right, .. } => {
            returning_expr_forbidden(left) || returning_expr_forbidden(right)
        }
        Expr::InList { expr, list, .. } => {
            returning_expr_forbidden(expr) || list.iter().any(returning_expr_forbidden)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            returning_expr_forbidden(expr)
                || returning_expr_forbidden(low)
                || returning_expr_forbidden(high)
        }
        Expr::Like { expr, pattern, .. } => {
            returning_expr_forbidden(expr) || returning_expr_forbidden(pattern)
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand.as_deref().is_some_and(returning_expr_forbidden)
                || conditions.iter().any(returning_expr_forbidden)
                || results.iter().any(returning_expr_forbidden)
                || else_result.as_deref().is_some_and(returning_expr_forbidden)
        }
        Expr::Tuple(items) => items.iter().any(returning_expr_forbidden),
        _ => false,
    }
}

#[allow(clippy::too_many_arguments)]
fn rewrite_image_refs(
    expr: &mut Expr,
    table: &TableSchema,
    old_name: &str,
    new_name: &str,
    hidden_old: bool,
    hidden_new: bool,
    target_name: &str,
    default_image: DefaultImage,
) -> Result<(), QueryError> {
    match expr {
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let qualifier = &parts[parts.len() - 2];
            let column = parts.last().cloned().unwrap_or_default();
            let image = if qualifier.eq_ignore_ascii_case(old_name) {
                Some(DefaultImage::Old)
            } else if qualifier.eq_ignore_ascii_case(new_name) {
                Some(DefaultImage::New)
            } else if (!hidden_old && qualifier.eq_ignore_ascii_case("old"))
                || (!hidden_new && qualifier.eq_ignore_ascii_case("new"))
            {
                Some(if qualifier.eq_ignore_ascii_case("old") {
                    DefaultImage::Old
                } else {
                    DefaultImage::New
                })
            } else if (hidden_old && qualifier.eq_ignore_ascii_case("old"))
                || (hidden_new && qualifier.eq_ignore_ascii_case("new"))
            {
                return Err(error(
                    crate::error::catalog::ER_BAD_FIELD_ERROR,
                    format!("Unknown table '{}' in RETURNING", qualifier),
                ));
            } else if qualifier.eq_ignore_ascii_case(target_name)
                || (target_name.eq_ignore_ascii_case(&table.name)
                    && qualifier.eq_ignore_ascii_case(&table.name))
            {
                Some(default_image)
            } else {
                None
            };
            let Some(image) = image else {
                return Err(error(
                    crate::error::catalog::ER_BAD_FIELD_ERROR,
                    format!("Unknown table '{}' in RETURNING", qualifier),
                ));
            };
            let idx = table.column_index(&column)?;
            let prefix = match image {
                DefaultImage::Old => "__mydb_returning_old_",
                DefaultImage::New => "__mydb_returning_new_",
            };
            *expr = Expr::Identifier(format!("{prefix}{idx}"));
        }
        Expr::Unary { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::GroupKeyRef { expr, .. } => rewrite_image_refs(
            expr,
            table,
            old_name,
            new_name,
            hidden_old,
            hidden_new,
            target_name,
            default_image,
        )?,
        Expr::Binary { left, right, .. } => {
            rewrite_image_refs(
                left,
                table,
                old_name,
                new_name,
                hidden_old,
                hidden_new,
                target_name,
                default_image,
            )?;
            rewrite_image_refs(
                right,
                table,
                old_name,
                new_name,
                hidden_old,
                hidden_new,
                target_name,
                default_image,
            )?;
        }
        Expr::InList { expr, list, .. } => {
            rewrite_image_refs(
                expr,
                table,
                old_name,
                new_name,
                hidden_old,
                hidden_new,
                target_name,
                default_image,
            )?;
            for item in list {
                rewrite_image_refs(
                    item,
                    table,
                    old_name,
                    new_name,
                    hidden_old,
                    hidden_new,
                    target_name,
                    default_image,
                )?;
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            for item in [expr, low, high] {
                rewrite_image_refs(
                    item,
                    table,
                    old_name,
                    new_name,
                    hidden_old,
                    hidden_new,
                    target_name,
                    default_image,
                )?;
            }
        }
        Expr::Like { expr, pattern, .. } => {
            rewrite_image_refs(
                expr,
                table,
                old_name,
                new_name,
                hidden_old,
                hidden_new,
                target_name,
                default_image,
            )?;
            rewrite_image_refs(
                pattern,
                table,
                old_name,
                new_name,
                hidden_old,
                hidden_new,
                target_name,
                default_image,
            )?;
        }
        Expr::Function { args, .. } | Expr::Tuple(args) => {
            for item in args {
                rewrite_image_refs(
                    item,
                    table,
                    old_name,
                    new_name,
                    hidden_old,
                    hidden_new,
                    target_name,
                    default_image,
                )?;
            }
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(v) = operand {
                rewrite_image_refs(
                    v,
                    table,
                    old_name,
                    new_name,
                    hidden_old,
                    hidden_new,
                    target_name,
                    default_image,
                )?;
            }
            for v in conditions.iter_mut().chain(results.iter_mut()) {
                rewrite_image_refs(
                    v,
                    table,
                    old_name,
                    new_name,
                    hidden_old,
                    hidden_new,
                    target_name,
                    default_image,
                )?;
            }
            if let Some(v) = else_result {
                rewrite_image_refs(
                    v,
                    table,
                    old_name,
                    new_name,
                    hidden_old,
                    hidden_new,
                    target_name,
                    default_image,
                )?;
            }
        }
        _ => {}
    }
    Ok(())
}
