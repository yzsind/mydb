//! Ordered temporal value semantics over prebound batch operands. Speculation
//! is restricted to pure temporal/format functions and commits only if the
//! original evaluator produced neither an error nor any diagnostic.
use super::*;
use crate::engine::executor::memory_footprint::AllocatedBytes;
use std::borrow::Cow;

pub(super) struct TemporalValue<'a> {
    name: &'a str,
    nodes: Cow<'a, [ExprNode]>,
    syntax: Option<&'a [Expr]>,
    width: usize,
    domain: Domain,
}

mod calendar;
mod fast;
mod integer;

enum Input<'b> {
    Constant(Cow<'b, Value>),
    Column(Cow<'b, ColumnVector>, Option<ColumnType>),
}

impl<'a> TemporalValue<'a> {
    pub(super) fn bind(node: &'a ExprNode, schema: &BatchSchema) -> Option<Self> {
        let (name, nodes, syntax) = match node {
            ExprNode::Function {
                kernel:
                    FunctionKernel::LegacySession(name)
                    | FunctionKernel::LegacyDeterministic(name @ ("FROM_DAYS" | "SEC_TO_TIME")),
                args,
                ..
            } => (*name, Cow::Borrowed(args.as_slice()), None),
            ExprNode::Function {
                kernel: FunctionKernel::UnixTimestamp,
                args,
                ..
            } if args.len() == 1 => {
                let ExprNode::Column(index) = &args[0] else {
                    return None;
                };
                if !matches!(
                    schema.fields().get(*index)?.column_type,
                    ColumnType::Date | ColumnType::DateTime | ColumnType::Timestamp
                ) {
                    return None;
                }
                ("UNIX_TIMESTAMP", Cow::Borrowed(args.as_slice()), None)
            }
            ExprNode::Function {
                kernel: FunctionKernel::Datediff,
                args,
                ..
            } => ("DATEDIFF", Cow::Borrowed(args.as_slice()), None),
            ExprNode::LegacyEscape {
                expr,
                schema: table,
                ..
            } => {
                let Expr::Function {
                    name,
                    args,
                    distinct: false,
                } = expr.as_ref()
                else {
                    return None;
                };
                if ![
                    "ADDTIME",
                    "SUBTIME",
                    "TIMESTAMPADD",
                    "TIMESTAMPDIFF",
                    "MAKETIME",
                    "FROM_UNIXTIME",
                ]
                .iter()
                .any(|known| name.eq_ignore_ascii_case(known))
                {
                    return None;
                }
                if !bounded_bound_literals(args) {
                    return None;
                }
                let mut input = None;
                let mut nodes = Vec::with_capacity(args.len());
                for (index, arg) in args.iter().enumerate() {
                    if index == 0
                        && (name.eq_ignore_ascii_case("TIMESTAMPADD")
                            || name.eq_ignore_ascii_case("TIMESTAMPDIFF"))
                    {
                        // The unit is syntax, never a value evaluated by the
                        // original function. Retain its original AST below.
                        nodes.push(ExprNode::Const(Value::Null));
                    } else {
                        let node = match arg {
                            Expr::Identifier(name) => {
                                ExprNode::Column(table.find_column_index(name)?)
                            }
                            Expr::Literal(value) => ExprNode::Const(literal_value(value)),
                            Expr::BoundValue(value) => ExprNode::Const(value.clone()),
                            Expr::Placeholder(index) => ExprNode::Placeholder(*index),
                            _ => compile_node(
                                arg,
                                input.get_or_insert_with(|| InputSchema::from_table(table)),
                            )
                            .map(fold_bound_integer_sign)
                            .ok()?,
                        };
                        nodes.push(node);
                    }
                }
                (name.as_str(), Cow::Owned(nodes), Some(args.as_slice()))
            }
            _ => return None,
        };
        if name.eq_ignore_ascii_case("MAKETIME")
            && !nodes.iter().all(|node| cheap_time_operand(node, schema))
        {
            return None;
        }
        let (arity, domain) = if [
            "ADDTIME",
            "SUBTIME",
            "MAKEDATE",
            "PERIOD_ADD",
            "PERIOD_DIFF",
            "TIMEDIFF",
            "TIME_FORMAT",
            "DATEDIFF",
        ]
        .iter()
        .any(|known| name.eq_ignore_ascii_case(known))
        {
            (
                2,
                if name.eq_ignore_ascii_case("TIMEDIFF") {
                    Domain::Temporal
                } else if name.starts_with("PERIOD_") || name.eq_ignore_ascii_case("DATEDIFF") {
                    Domain::Integer(i64::MIN, i64::MAX)
                } else {
                    Domain::Text
                },
            )
        } else if ["TIMESTAMPADD", "TIMESTAMPDIFF", "MAKETIME", "CONVERT_TZ"]
            .iter()
            .any(|known| name.eq_ignore_ascii_case(known))
        {
            (
                3,
                if name.eq_ignore_ascii_case("TIMESTAMPDIFF") {
                    Domain::Integer(i64::MIN, i64::MAX)
                } else if name.eq_ignore_ascii_case("MAKETIME") {
                    Domain::Temporal
                } else {
                    Domain::Text
                },
            )
        } else if name.eq_ignore_ascii_case("FROM_UNIXTIME") && (1..=2).contains(&nodes.len()) {
            (nodes.len(), Domain::Text)
        } else if name.eq_ignore_ascii_case("UNIX_TIMESTAMP") {
            (1, Domain::Exact)
        } else if name.eq_ignore_ascii_case("FROM_DAYS") || name.eq_ignore_ascii_case("SEC_TO_TIME")
        {
            (1, Domain::Temporal)
        } else if name.eq_ignore_ascii_case("FORMAT_BYTES")
            || name.eq_ignore_ascii_case("FORMAT_PICO_TIME")
        {
            (1, Domain::Text)
        } else {
            return None;
        };
        if nodes.len() != arity || !nodes.iter().all(|arg| {
            constant(arg).is_some() || matches!(arg, ExprNode::Column(index) if schema.fields().get(*index).is_some_and(|field| field.column_type == ColumnType::Time))
                || contract(arg, schema).is_some()
        }) { return None; }
        if nodes
            .iter()
            .filter_map(constant)
            .any(|value| value.allocated_bytes() > 4096)
        {
            return None;
        }
        let format = if name.eq_ignore_ascii_case("TIME_FORMAT") {
            Some(1)
        } else if name.eq_ignore_ascii_case("FROM_UNIXTIME") && arity == 2 {
            Some(1)
        } else {
            None
        };
        let width = match format {
            Some(index) => {
                let format = text(&nodes[index])?;
                if format.len() > 4096 {
                    return None;
                }
                if name.eq_ignore_ascii_case("TIME_FORMAT") {
                    let mut width = 1usize;
                    let mut chars = format.chars();
                    while let Some(ch) = chars.next() {
                        let count = if ch != '%' {
                            ch.len_utf8()
                        } else {
                            match chars.next() {
                                Some('H' | 'k') => 10,
                                Some('T') => 16,
                                Some('r') => 11,
                                Some('f') => 6,
                                Some('Y') => 4,
                                Some('h' | 'I' | 'l' | 'i' | 's' | 'S' | 'p' | 'y' | 'm' | 'd') => {
                                    2
                                }
                                Some(other) => other.len_utf8(),
                                None => 1,
                            }
                        };
                        width = width.checked_add(count)?;
                    }
                    width.max(32)
                } else {
                    format.len().checked_mul(64)?.checked_add(64)?
                }
            }
            None if name.eq_ignore_ascii_case("FORMAT_BYTES")
                || name.eq_ignore_ascii_case("FORMAT_PICO_TIME") =>
            {
                if contract(&nodes[0], schema)
                    .is_some_and(|c| matches!(c.domain, Domain::Integer(..) | Domain::Unsigned))
                {
                    48
                } else {
                    512
                }
            }
            None => {
                if name.eq_ignore_ascii_case("UNIX_TIMESTAMP") {
                    std::mem::size_of::<Value>()
                } else if matches!(domain, Domain::Integer(..) | Domain::Temporal) {
                    8
                } else {
                    32
                }
            }
        };
        Some(Self {
            name,
            nodes,
            syntax,
            width,
            domain,
        })
    }

    pub(super) fn contract(&self) -> Contract {
        Contract {
            domain: self.domain,
            parallel: false,
        }
    }

    pub(super) fn workspace(&self, batch: &Batch) -> Option<(usize, usize)> {
        let output = batch
            .num_rows()
            .checked_mul(self.width.checked_add(8)?)?
            .checked_add(512)?;
        let mut work = output.checked_mul(2)?.checked_add(
            batch
                .num_rows()
                .checked_mul(std::mem::size_of::<Value>() * 2)?,
        )?;
        for node in self.nodes.iter() {
            if let Some(value) = constant(node) {
                work = work.checked_add(value.allocated_bytes().checked_mul(4)?)?;
            } else {
                let (_, child) = workspace(node, batch)?;
                work = work.checked_add(child)?;
            }
        }
        if self.name.eq_ignore_ascii_case("TIME_FORMAT") {
            // Parsed tokens own per-specifier metadata as well as literal
            // bytes. Reserve their upper bound even for a one-row batch.
            let format = text(&self.nodes[1])?;
            work = work.checked_add(format.len().checked_mul(64)?)?;
        }
        Some((output, work))
    }

    pub(super) fn evaluate(&self, batch: &Batch) -> Option<ColumnVector> {
        let mut inputs = Vec::with_capacity(self.nodes.len());
        for node in self.nodes.iter() {
            inputs.push(match constant(node) {
                Some(value) => Input::Constant(value),
                None => Input::Column(
                    operand(node, batch)?,
                    match node {
                        ExprNode::Column(index) => {
                            Some(batch.schema().fields().get(*index)?.column_type)
                        }
                        _ => None,
                    },
                ),
            });
        }
        if self.name.eq_ignore_ascii_case("UNIX_TIMESTAMP") {
            return calendar::evaluate(self, &inputs, batch);
        }
        if let Some(column) = calendar::evaluate(self, &inputs, batch) {
            return Some(column);
        }
        if let Some(column) = integer::evaluate(self, &inputs, batch) {
            return Some(column);
        }
        if let Some(column) = fast::evaluate(self, &inputs, batch) {
            return Some(column);
        }
        let values_only: Vec<_> = (0..inputs.len())
            .map(|_| Expr::BoundValue(Value::Null))
            .collect();
        let args = self.syntax.unwrap_or(&values_only);
        let diagnostics = crate::engine::api::StatementWarningSink::default();
        let output = crate::engine::api::with_statement_warning_sink(diagnostics.clone(), || {
            let mut output = Vec::with_capacity(batch.num_rows());
            for row in 0..batch.num_rows() {
                if row & 63 == 0 {
                    crate::engine::cancel::check_cancelled()?;
                    if !diagnostics.lock().is_empty() {
                        return Ok(None);
                    }
                }
                let value = crate::engine::expr::eval_function(self.name, args, |arg| {
                    let index = args
                        .iter()
                        .position(|source| std::ptr::eq(source, arg))
                        .expect("function evaluates its own argument");
                    Ok(match &inputs[index] {
                        Input::Constant(value) => value.as_ref().clone(),
                        Input::Column(column, Some(kind)) => column.typed_value_at(row, *kind),
                        Input::Column(column, None) => column.value_at(row),
                    })
                })?;
                output.push(value);
            }
            Ok::<_, QueryError>(Some(output))
        })
        .ok()??;
        if !diagnostics.lock().is_empty() {
            return None;
        }
        Some(ColumnVector::from_values(output))
    }
}
