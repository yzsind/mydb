//! Ordered scalar residuals read source rows without gathering pair payloads.
use super::*;

impl BatchExprProgram {
    pub(crate) fn scalar_pairs_eligible(&self, schema: &BatchSchema) -> bool {
        matches!(&self.root, BatchPredicateNode::Leaf {
            program, kernel: PredicateKernel::Scalar,
        } if !program.has_column_contract(schema))
    }

    /// Only a whole-program scalar leaf can use this lane. Native/split
    /// predicates keep their existing column kernels and scheduling. Admission
    /// and capability refusal happen before the first expression is evaluated.
    pub(crate) fn eval_scalar_pairs(
        &self,
        build: &[ColumnVector],
        probe: &Batch,
        build_rows: &[u32],
        probe_rows: &[u32],
        build_first: bool,
        schema: &Arc<BatchSchema>,
    ) -> Result<Option<Bitmap>, QueryError> {
        let BatchPredicateNode::Leaf {
            program,
            kernel: PredicateKernel::Scalar,
        } = &self.root
        else {
            return Ok(None);
        };
        if build_rows.len() != probe_rows.len() {
            return Ok(None);
        }
        let (first, first_rows, second, second_rows) = if build_first {
            (build, build_rows, probe.columns(), probe_rows)
        } else {
            (probe.columns(), probe_rows, build, build_rows)
        };
        let columns = first.len() + second.len();
        if columns != schema.len() {
            return Ok(None);
        }
        let references = program.referenced_columns();
        // Source payload bytes bound both cached rows. Include an optional
        // deferred truthiness array to preserve the scalar filter's diagnostic
        // order even when a dynamic conditional returns character values.
        let mut bytes = columns
            .saturating_mul(std::mem::size_of::<Value>())
            .saturating_add(
                build_rows
                    .len()
                    .saturating_mul(std::mem::size_of::<(usize, Value)>()),
            )
            .saturating_add(build_rows.len().div_ceil(8))
            .saturating_add(256);
        for &index in references {
            let column = if index < first.len() {
                first.get(index)
            } else {
                second.get(index - first.len())
            };
            let Some(column) = column else {
                return Ok(None);
            };
            // These layouts need a row walk to size owned heap cells. Do not
            // rescan a potentially large build for every bounded pair morsel;
            // retain the ordinary gathered lane for such physical vectors.
            if matches!(
                column,
                ColumnVector::Generic { .. } | ColumnVector::DecimalWide { .. }
            ) {
                return Ok(None);
            }
            bytes = bytes.saturating_add(column.memory_bytes());
        }
        let mut charge = probe.memory_charge();
        if !charge.add_within(bytes, charge.remaining()) {
            return Ok(None);
        }
        let mut values =
            crate::engine::executor::mem_budget::try_vec_with_capacity(columns, "scalar pair row")?;
        values.resize(columns, Value::Null);
        let mut truth = Bitmap::new_all_invalid(build_rows.len());
        let mut deferred = Vec::new();
        let (mut last_first, mut last_second) = (None, None);
        self.fallback_rows.fetch_add(
            build_rows.len() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        with_eval_scratch(|scratch| {
            for position in 0..build_rows.len() {
                if position & crate::engine::cancel::SCALAR_ROW_CANCEL_MASK == 0 {
                    crate::engine::cancel::check_cancelled()?;
                }
                let (a, b) = (first_rows[position], second_rows[position]);
                for &index in references {
                    let (column, row, changed) = if index < first.len() {
                        (&first[index], a, last_first != Some(a))
                    } else {
                        (&second[index - first.len()], b, last_second != Some(b))
                    };
                    if changed {
                        values[index] =
                            column.typed_value_at(row as usize, schema.fields()[index].column_type);
                    }
                }
                last_first = Some(a);
                last_second = Some(b);
                let value = program.eval_row(&RowView::from_values(&values), scratch)?;
                if crate::engine::expr::truthiness_may_warn(&value) {
                    if deferred.capacity() == 0 {
                        deferred = crate::engine::executor::mem_budget::try_vec_with_capacity(
                            build_rows.len(),
                            "scalar pair truthiness",
                        )?;
                    }
                    deferred.push((position, value));
                } else {
                    truth.set(position, crate::engine::expr::truthy(&value));
                }
            }
            Ok(())
        })?;
        for (position, value) in deferred {
            truth.set(position, crate::engine::expr::truthy(&value));
        }
        Ok(Some(truth))
    }
}

#[cfg(test)]
mod tests;
