//! Stable final-page selection after complete window evaluation.
use super::*;

pub(super) struct Order {
    fields: Vec<crate::engine::executor::sort::SortField>,
    keys: Vec<Expr>,
    positions: Vec<Option<usize>>,
    schema: crate::storage::TableSchema,
}

impl Order {
    pub(super) fn bind(
        columns: &[Column],
        order_by: &[OrderBy],
        order_key_map: &[(Expr, String)],
    ) -> Self {
        let final_schema = synthetic_set_op_schema(&columns);
        let output_index: std::collections::HashMap<String, usize> = columns
            .iter()
            .enumerate()
            .map(|(idx, col)| (col.name.to_ascii_lowercase(), idx))
            .collect();
        let order_keys: Vec<Expr> = order_by
            .iter()
            .map(|item| {
                let mut e = item.expr.clone();
                rebind_expr_to_output_columns(&mut e, &order_key_map);
                e
            })
            .collect();
        let direct_order_positions: Vec<Option<usize>> = order_keys
            .iter()
            .enumerate()
            .map(|(index, expr)| {
                // Qualified expressions retain their projection slot even
                // when two output columns have the same display name.
                if let Some(position) = order_key_map
                    .iter()
                    .position(|(source, _)| source == &order_by[index].expr)
                {
                    return Some(position);
                }
                match expr {
                    Expr::Literal(crate::parser::Literal::Int(n)) if *n > 0 => {
                        Some(*n as usize - 1)
                    }
                    Expr::Literal(crate::parser::Literal::UInt(n)) if *n > 0 => {
                        Some(*n as usize - 1)
                    }
                    _ => crate::planner::expr_column_name(expr)
                        .and_then(|name| output_index.get(&name.to_ascii_lowercase()).copied()),
                }
            })
            .collect();
        let mut fields = crate::engine::executor::sort::fields_from_order_by(&order_by);
        for (index, (field, expr)) in fields.iter_mut().zip(&order_keys).enumerate() {
            field.collation_id = direct_order_positions[index]
                .and_then(|pos| columns.get(pos))
                .and_then(|c| c.source.collation_id)
                .or_else(|| {
                    crate::engine::expr::expr_collation(expr, &final_schema)
                        .map(|info| info.collation_id)
                });
        }

        Self {
            fields,
            keys: order_keys,
            positions: direct_order_positions,
            schema: final_schema,
        }
    }

    fn key(&self, row: &[Value]) -> Vec<Value> {
        self.keys
            .iter()
            .enumerate()
            .map(|(idx, e)| {
                self.positions[idx]
                    .and_then(|pos| row.get(pos).cloned())
                    .unwrap_or_else(|| {
                        crate::engine::expr::eval_expr(e, &self.schema, row).unwrap_or(Value::Null)
                    })
            })
            .collect()
    }

    pub(super) fn select_before_projection(
        &self,
        rows: &mut Vec<Vec<Value>>,
        projection: &[WindowFinalCol],
        window: (usize, usize),
    ) -> bool {
        // Pure slot copies have no diagnostics or side effects to preserve on
        // discarded rows. Computed projections must still evaluate every row.
        if !projection
            .iter()
            .all(|p| matches!(p, WindowFinalCol::Index(_)))
        {
            return false;
        }
        let Some(positions) = self
            .positions
            .iter()
            .map(|position| {
                let WindowFinalCol::Index(source) = projection.get((*position)?)? else {
                    return None;
                };
                Some(*source)
            })
            .collect::<Option<Vec<_>>>()
        else {
            return false;
        };
        crate::engine::executor::sort::sort_window_by_cached_keys(
            rows,
            &self.fields,
            window.0,
            window.1,
            |row| {
                positions
                    .iter()
                    .map(|&p| row.get(p).cloned().unwrap_or(Value::Null))
                    .collect()
            },
        );
        true
    }

    pub(super) fn apply(&self, rows: &mut Vec<Vec<Value>>, window: Option<(usize, usize)>) {
        if let Some((offset, limit)) = window {
            crate::engine::executor::sort::sort_window_by_cached_keys(
                rows,
                &self.fields,
                offset,
                limit,
                |row| self.key(row),
            );
        } else {
            crate::engine::executor::sort::sort_by_cached_keys(rows, &self.fields, |row| {
                self.key(row)
            });
        }
    }
}
