//! Reusable scalar kernels for a recursive delta that cannot expand rows.

use super::*;
use crate::engine::expr_program::{EvalScratch, ExprCompiler, RowSource};

#[cfg(test)]
pub(crate) static RECURSIVE_STEP_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

struct MappedRow<'a> {
    values: &'a [Value],
    indices: &'a [usize],
}

impl RowSource for MappedRow<'_> {
    fn value_ref(&self, index: usize) -> Option<&Value> {
        self.values.get(*self.indices.get(index)?)
    }

    fn len(&self) -> usize {
        self.indices.len()
    }
}

pub(in crate::engine::executor) struct RecursiveStep<'a> {
    nodes: Vec<&'a PhysicalPlan>,
    scan_predicate: Option<super::super::plan::CteRowPredicate>,
    filters: Vec<Option<Arc<ExprProgram>>>,
    indices: Vec<usize>,
    projection: Vec<VecResultProjection>,
    pub(in crate::engine::executor) columns: Vec<crate::types::Column>,
    recording: bool,
}

impl<'a> RecursiveStep<'a> {
    /// Only unary, row-preserving nodes can reuse a single delta row. Joins,
    /// subqueries, set operators and scalar escapes retain the batch graph.
    /// This is a physical capability proof, independent of SQL literals,
    /// arithmetic operators, recursion depth and the final consumer.
    pub(in crate::engine::executor) fn compile(
        select: &crate::parser::Select,
        plan: &'a PhysicalPlan,
        name: &str,
    ) -> Result<Option<Self>, QueryError> {
        let mut nodes = Vec::new();
        let mut node = plan;
        let (columns, filter, projected) = loop {
            nodes.push(node);
            match node {
                PhysicalPlan::Projection {
                    input,
                    distinct: false,
                    ..
                }
                | PhysicalPlan::Filter { input, .. } => node = input,
                PhysicalPlan::CteScan {
                    name: source,
                    columns,
                    filter,
                    projected_columns,
                    ..
                } if source.eq_ignore_ascii_case(name) => {
                    break (columns, filter, projected_columns)
                }
                _ => return Ok(None),
            }
        };
        let projection = final_projection(plan).unwrap_or(&select.projection);
        if projection.iter().any(|item| {
            matches!(item, SelectItem::Expr { expr, .. }
            if crate::parser::visit::value_expr_contains_subquery(expr))
        }) {
            return Ok(None);
        }
        preflight_runtime_parameters(plan)?;
        let Some((template, lowered, _)) = preflight_result_projection(plan, projection, None)
        else {
            return Ok(None);
        };
        if lowered.iter().any(|item| match item {
            VecResultProjection::Scalar(_) => true,
            VecResultProjection::Expr(program) => program.capability().legacy_escape,
            _ => false,
        }) {
            return Ok(None);
        }
        let Some(output) = query_projection_template(projection, &template, &lowered) else {
            return Ok(None);
        };
        let scan_predicate = match filter {
            Some(expr) => match super::super::plan::compile_cte_row_predicate(expr, columns) {
                Some(predicate) => Some(predicate),
                None => return Ok(None),
            },
            None => None,
        };
        let indices = projected
            .as_ref()
            .map_or_else(|| (0..columns.len()).collect(), |indices| indices.to_vec());
        if indices.len() != template.len() {
            return Ok(None);
        }
        let schema = template_to_batch_schema(&template);
        let input = expr_batch::batch_schema_to_input(&schema);
        nodes.reverse();
        let mut filters = Vec::with_capacity(nodes.len());
        for node in &nodes {
            let program = match node {
                PhysicalPlan::Filter { predicate, .. } => {
                    if crate::parser::visit::value_expr_contains_subquery(predicate) {
                        return Ok(None);
                    }
                    let Ok(program) = ExprCompiler::compile_cached(predicate, &input) else {
                        return Ok(None);
                    };
                    if program.capability().legacy_escape {
                        return Ok(None);
                    }
                    Some(program)
                }
                _ => None,
            };
            filters.push(program);
        }
        #[cfg(test)]
        RECURSIVE_STEP_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(Some(Self {
            nodes,
            scan_predicate,
            filters,
            indices,
            projection: lowered,
            columns: output
                .iter()
                .map(|field| crate::types::Column {
                    name: field.column.clone(),
                    column_type: field.column_type,
                    source: field.source.clone(),
                })
                .collect(),
            recording: analyze_stats::is_recording(),
        }))
    }

    pub(in crate::engine::executor) fn evaluate(
        &self,
        values: &[Value],
        output: &mut Vec<Value>,
    ) -> Result<bool, QueryError> {
        output.clear();
        let start = self.recording.then(std::time::Instant::now);
        let source = MappedRow {
            values,
            indices: &self.indices,
        };
        let mut scratch = EvalScratch::default();
        let mut passed = match &self.scan_predicate {
            Some(predicate) => super::super::plan::eval_cte_row_predicate(predicate, values)?,
            None => true,
        };
        for (index, (node, filter)) in self.nodes.iter().zip(&self.filters).enumerate() {
            if passed {
                if let Some(program) = filter {
                    passed = match program.eval_simple_predicate_source(&source) {
                        Some(result) => result?,
                        None => crate::engine::expr::truthy(
                            &program.eval_source(&source, &mut scratch)?,
                        ),
                    };
                }
            }
            if index + 1 == self.nodes.len() && passed {
                for (item, column) in self.projection.iter().zip(&self.columns) {
                    let value = match item {
                        VecResultProjection::Column(index) => {
                            source.value_ref(*index).cloned().ok_or_else(|| {
                                crate::error::catalog::ER_BAD_FIELD_ERROR.error("unknown column")
                            })?
                        }
                        VecResultProjection::Expr(program) => {
                            program.eval_source(&source, &mut scratch)?
                        }
                        VecResultProjection::Scalar(_) => {
                            unreachable!("scalar escape rejected before execution")
                        }
                    };
                    output.push(match (column.column_type, value) {
                        (ColumnType::Float, Value::Double(number)) => Value::Float(number as f32),
                        (ColumnType::Decimal { .. }, value) => {
                            crate::engine::expr::decimal_result_value(&value, column.column_type)
                                .into_owned()
                        }
                        (_, value) => value,
                    });
                }
            }
            if let Some(start) = start {
                analyze_stats::record(
                    node,
                    start.elapsed().as_nanos(),
                    u64::from(passed),
                    Default::default(),
                );
            }
        }
        Ok(passed)
    }
}
