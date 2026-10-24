//! sources execution and preparation.

use super::*;

pub(super) struct ValuesBatchStream<'a> {
    pub(super) db: &'a Arc<Database>,
    pub(super) txn: TxnView,
    pub(super) rows: &'a [Vec<Expr>],
    pub(super) next: usize,
    pub(super) schema: Arc<BatchSchema>,
    pub(super) eval_ctx: crate::engine::executor::select::eval::EvalQueryContext,
}

impl BatchOperator for ValuesBatchStream<'_> {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }

    fn properties(&self) -> BatchProperties {
        BatchProperties::UNORDERED_SINGLE
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        if self.next >= self.rows.len() {
            return Ok(None);
        }
        let end = self
            .next
            .saturating_add(DEFAULT_BATCH_ROWS)
            .min(self.rows.len());
        let empty = PlanRow::default();
        let mut values = Vec::with_capacity(end - self.next);
        for raw in &self.rows[self.next..end] {
            crate::engine::cancel::check_cancelled()?;
            let mut row = Vec::with_capacity(raw.len());
            for expr in raw {
                row.push(
                    crate::engine::executor::select::eval::eval_plan_expr_with_db(
                        self.db,
                        self.txn,
                        expr,
                        &empty,
                        &mut self.eval_ctx,
                    )?,
                );
            }
            values.push(row);
        }
        self.next = end;
        Ok(value_rows_to_batches(self.schema.clone(), values).pop())
    }
}

pub(super) struct JsonTableBatchStream {
    pub(super) source: crate::parser::JsonTableSource,
    pub(super) schema: Arc<BatchSchema>,
    pub(super) roots: Option<Vec<Vec<u8>>>,
    pub(super) root_index: usize,
    pub(super) pending: std::collections::VecDeque<Vec<Value>>,
}

impl BatchOperator for JsonTableBatchStream {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }

    fn properties(&self) -> BatchProperties {
        BatchProperties::UNORDERED_SINGLE
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        crate::engine::cancel::check_cancelled()?;
        if self.roots.is_none() {
            let value = crate::engine::expr::eval_const(&self.source.expr)?;
            self.roots = Some(if matches!(value, Value::Null) {
                Vec::new()
            } else {
                let doc = crate::engine::executor::select::json_table_doc_arg(&value)?;
                crate::engine::executor::select::json_table_eval_path(&doc, &self.source.path)?
            });
        }
        let roots = self.roots.as_ref().expect("JSON roots initialized");
        let mut rows = Vec::with_capacity(DEFAULT_BATCH_ROWS);
        while rows.len() < DEFAULT_BATCH_ROWS {
            if let Some(row) = self.pending.pop_front() {
                rows.push(row);
                continue;
            }
            let Some(root) = roots.get(self.root_index) else {
                break;
            };
            self.root_index += 1;
            self.pending.extend(
                crate::engine::executor::select::json_table_rows_for_columns(
                    root,
                    &self.source.columns,
                    self.root_index as u64,
                )?,
            );
        }
        if rows.is_empty() {
            Ok(None)
        } else {
            Ok(value_rows_to_batches(self.schema.clone(), rows).pop())
        }
    }
}

pub(super) fn try_build_values_batch<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
) -> Result<Option<VecBatchInput<'a>>, QueryError> {
    let PhysicalPlan::ValuesScan {
        rows,
        columns,
        filter,
        ..
    } = plan
    else {
        return Ok(None);
    };
    let template = synthetic_source_template("values", columns);
    let schema = template_to_batch_schema(&template);
    let mut stream: Box<dyn BatchOperator + 'a> = Box::new(ValuesBatchStream {
        db,
        txn,
        rows,
        next: 0,
        schema: schema.clone(),
        eval_ctx: crate::engine::executor::select::eval::EvalQueryContext::default(),
    });
    if let Some(predicate) = filter {
        let program = expr_batch::BatchExprProgram::compile(predicate, &schema)?;
        let properties = stream.properties();
        stream = if let Some(program) = program {
            Box::new(FilterBatchStream {
                upstream: stream,
                program,
                schema,
                properties,
                analyze_plan: Some(plan),
                analyze_recorded: false,
            })
        } else {
            Box::new(ScalarFilterBatchStream {
                db,
                txn,
                upstream: stream,
                predicate: Cow::Borrowed(predicate),
                template: template.clone(),
                schema,
                properties,
                eval_ctx: crate::engine::executor::select::eval::EvalQueryContext::default(),
                analyze_plan: Some(plan),
                scalar_rows: 0,
                analyze_recorded: false,
            })
        };
    }
    Ok(Some(VecBatchInput { stream, template }))
}

pub(super) fn try_build_json_table_batch<'a>(
    plan: &'a PhysicalPlan,
) -> Result<Option<VecBatchInput<'a>>, QueryError> {
    let PhysicalPlan::JsonTableScan {
        source,
        relation,
        columns,
        filter,
        ..
    } = plan
    else {
        return Ok(None);
    };
    let template = synthetic_source_template(relation, columns);
    let schema = template_to_batch_schema(&template);
    let mut stream: Box<dyn BatchOperator + 'a> = Box::new(JsonTableBatchStream {
        source: source.clone(),
        schema: schema.clone(),
        roots: None,
        root_index: 0,
        pending: std::collections::VecDeque::new(),
    });
    if let Some(predicate) = filter {
        let Some(program) = expr_batch::BatchExprProgram::compile(predicate, &schema)? else {
            return Ok(None);
        };
        let properties = stream.properties();
        stream = Box::new(FilterBatchStream {
            upstream: stream,
            program,
            schema,
            properties,
            analyze_plan: Some(plan),
            analyze_recorded: false,
        });
    }
    Ok(Some(VecBatchInput { stream, template }))
}

pub(super) struct SystemBatchStream<'a> {
    pub(super) db: &'a Arc<Database>,
    pub(super) name: &'a str,
    pub(super) registry: crate::engine::session_registry::SessionRegistry,
    pub(super) request: crate::engine::information_schema::SystemScanRequest,
    pub(super) projected_columns: Vec<usize>,
    pub(super) schema: Arc<BatchSchema>,
    pub(super) output: Option<std::vec::IntoIter<Batch>>,
    pub(super) memory_charge: Option<crate::engine::executor::mem_budget::ControlledMemoryCharge>,
}

impl BatchOperator for SystemBatchStream<'_> {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }

    fn properties(&self) -> BatchProperties {
        BatchProperties::UNORDERED_SINGLE
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        crate::engine::cancel::check_cancelled()?;
        if self.output.is_none() {
            let governed = crate::engine::executor::select::system_table_rows(
                self.db,
                self.name,
                &self.registry,
                &self.request,
            )?;
            let (rows, charge) = governed.into_parts();
            let rows = rows
                .into_iter()
                .map(|row| {
                    self.projected_columns
                        .iter()
                        .map(|index| row.get(*index).cloned().unwrap_or(Value::Null))
                        .collect()
                })
                .collect();
            self.output = Some(value_rows_into_batches(self.schema.clone(), rows).into_iter());
            self.memory_charge = Some(charge);
        }
        Ok(self.output.as_mut().and_then(Iterator::next))
    }
}

pub(in crate::engine::executor::select) fn system_scan_request(
    filter: Option<&Expr>,
) -> crate::engine::information_schema::SystemScanRequest {
    fn value(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Literal(crate::parser::Literal::String(value)) => Some(value.clone()),
            Expr::BoundValue(value) => Some(value.to_display_string()),
            Expr::Placeholder(index) => {
                crate::engine::params::current_param(*index).map(|value| value.to_display_string())
            }
            _ => None,
        }
    }
    fn column(expr: &Expr) -> Option<&str> {
        match expr {
            Expr::Identifier(column) => Some(column),
            Expr::CompoundIdentifier(parts) => parts.last().map(String::as_str),
            _ => None,
        }
    }
    fn visit(expr: &Expr, request: &mut crate::engine::information_schema::SystemScanRequest) {
        match expr {
            Expr::Binary {
                left,
                op: crate::parser::BinaryOp::And,
                right,
            } => {
                visit(left, request);
                visit(right, request);
            }
            Expr::Binary {
                left,
                op: crate::parser::BinaryOp::Eq,
                right,
            } => {
                let pair = column(left)
                    .zip(value(right))
                    .or_else(|| column(right).zip(value(left)));
                if let Some((column, value)) = pair {
                    let normalized = column.to_ascii_lowercase();
                    request.equalities.push((
                        normalized.clone(),
                        crate::types::Value::String(value.clone()),
                    ));
                    match normalized.as_str() {
                        "table_schema" => request.table_schema = Some(value),
                        "table_name" => request.table_name = Some(value),
                        "column_name" => request.column_name = Some(value),
                        "storage_id" => request.storage_id = value.parse().ok(),
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
    let mut request = crate::engine::information_schema::SystemScanRequest::default();
    if let Some(filter) = filter {
        visit(filter, &mut request);
    }
    request
}

pub(super) fn try_build_system_batch<'a>(
    db: &'a Arc<Database>,
    plan: &'a PhysicalPlan,
) -> Result<Option<VecBatchInput<'a>>, QueryError> {
    let PhysicalPlan::SystemScan {
        name,
        relation,
        columns,
        filter,
        projected_columns,
        ..
    } = plan
    else {
        return Ok(None);
    };
    let registry =
        crate::engine::executor::select::current_session_registry().ok_or_else(|| {
            crate::storage::error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "system scan opened without a statement SessionRegistry",
            )
        })?;
    let projected = projected_columns
        .as_deref()
        .map(|projection| projection.to_vec())
        .unwrap_or_else(|| (0..columns.len()).collect());
    let output_columns = projected
        .iter()
        .filter_map(|index| columns.get(*index).cloned())
        .collect::<Vec<_>>();
    let template = synthetic_source_template(relation, &output_columns);
    let schema = template_to_batch_schema(&template);
    let mut stream: Box<dyn BatchOperator + 'a> = Box::new(SystemBatchStream {
        db,
        name,
        registry,
        request: system_scan_request(filter.as_ref()),
        projected_columns: projected,
        schema: schema.clone(),
        output: None,
        memory_charge: None,
    });
    if let Some(predicate) = filter {
        let Some(program) = expr_batch::BatchExprProgram::compile(predicate, &schema)? else {
            return Ok(None);
        };
        let properties = stream.properties();
        stream = Box::new(FilterBatchStream {
            upstream: stream,
            program,
            schema,
            properties,
            analyze_plan: Some(plan),
            analyze_recorded: false,
        });
    }
    Ok(Some(VecBatchInput { stream, template }))
}
