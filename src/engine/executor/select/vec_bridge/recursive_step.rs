//! Reusable scalar kernels for a recursive delta that cannot expand rows.

use super::*;
use crate::engine::expr_program::{BoundIntegerBinary, EvalScratch, ExprCompiler, RowSource};

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
    integer_kernels: Vec<Option<BoundIntegerBinary>>,
    _kernel_charge: mem_budget::ScopedCharge,
    pub(in crate::engine::executor) columns: Vec<crate::types::Column>,
    recording: bool,
}

impl<'a> RecursiveStep<'a> {
    /// A direct reference to the same recursive field already belongs to the
    /// anchor's assignment domain. Rechecking it cannot change the value.
    pub(in crate::engine::executor) fn identity_assignments(&self) -> Vec<bool> {
        self.projection.iter().enumerate().map(|(output, item)| {
            matches!(item, VecResultProjection::Column(input) if self.indices.get(*input) == Some(&output))
        }).collect()
    }

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
        let mut integer_kernels = Vec::new();
        let mut kernel_charge = mem_budget::ScopedCharge::new(0);
        for (index, item) in lowered.iter().enumerate() {
            let kernel = match item {
                VecResultProjection::Expr(program) => program.bind_integer_binary(),
                _ => None,
            };
            if kernel.is_some() && integer_kernels.capacity() == 0 {
                let bytes = lowered
                    .len()
                    .saturating_mul(std::mem::size_of::<Option<BoundIntegerBinary>>());
                if !kernel_charge.add_within(bytes, kernel_charge.remaining()) {
                    break;
                }
                integer_kernels =
                    mem_budget::try_vec_with_capacity(lowered.len(), "recursive integer kernels")?;
                integer_kernels.resize_with(index, || None);
            }
            if integer_kernels.capacity() != 0 {
                integer_kernels.push(kernel);
            }
        }
        #[cfg(test)]
        RECURSIVE_STEP_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(Some(Self {
            nodes,
            scan_predicate,
            filters,
            indices,
            projection: lowered,
            integer_kernels,
            _kernel_charge: kernel_charge,
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
        if !self.recording {
            if !passed {
                return Ok(false);
            }
            for program in self.filters.iter().flatten() {
                if !Self::evaluate_filter(program, &source, &mut scratch)? {
                    return Ok(false);
                }
            }
            self.project(&source, output, &mut scratch)?;
            return Ok(true);
        }
        for (index, (node, filter)) in self.nodes.iter().zip(&self.filters).enumerate() {
            if passed {
                if let Some(program) = filter {
                    passed = Self::evaluate_filter(program, &source, &mut scratch)?;
                }
            }
            if index + 1 == self.nodes.len() && passed {
                self.project(&source, output, &mut scratch)?;
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

    fn evaluate_filter(
        program: &ExprProgram,
        source: &MappedRow<'_>,
        scratch: &mut EvalScratch,
    ) -> Result<bool, QueryError> {
        match program.eval_simple_predicate_source(source) {
            Some(result) => result,
            None => program
                .eval_source(source, scratch)
                .map(|value| crate::engine::expr::truthy(&value)),
        }
    }

    #[inline]
    fn project(
        &self,
        source: &MappedRow<'_>,
        output: &mut Vec<Value>,
        scratch: &mut EvalScratch,
    ) -> Result<(), QueryError> {
        for (index, (item, column)) in self.projection.iter().zip(&self.columns).enumerate() {
            let value = match item {
                VecResultProjection::Column(index) => {
                    source.value_ref(*index).cloned().ok_or_else(|| {
                        crate::error::catalog::ER_BAD_FIELD_ERROR.error("unknown column")
                    })?
                }
                VecResultProjection::Expr(program) => {
                    match self
                        .integer_kernels
                        .get(index)
                        .and_then(Option::as_ref)
                        .and_then(|kernel| kernel.evaluate(source))
                    {
                        Some(value) => value,
                        None => program.eval_source(source, scratch)?,
                    }
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
        Ok(())
    }
}
