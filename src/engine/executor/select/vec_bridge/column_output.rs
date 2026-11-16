//! Immutable column-only output binding for an already validated compiled plan.
use super::*;

#[derive(Debug)]
pub(crate) struct ColumnOutputLayout {
    input: Vec<PlanField>,
    output: Vec<PlanField>,
    columns: Vec<usize>,
    schema: Arc<BatchSchema>,
    _charge: Arc<mem_budget::ControlledMemoryCharge>,
}

thread_local! {
    static CURRENT: std::cell::RefCell<Option<(usize, Arc<ColumnOutputLayout>)>> =
        const { std::cell::RefCell::new(None) };
}

pub(crate) struct ColumnOutputGuard(Option<(usize, Arc<ColumnOutputLayout>)>);

impl Drop for ColumnOutputGuard {
    fn drop(&mut self) {
        CURRENT.with(|slot| *slot.borrow_mut() = self.0.take());
    }
}

pub(crate) fn enter_column_output_layout(
    plan: &PhysicalPlan,
    layout: Option<&Arc<ColumnOutputLayout>>,
) -> ColumnOutputGuard {
    ColumnOutputGuard(CURRENT.with(|slot| {
        slot.replace(layout.map(|layout| (plan as *const _ as usize, Arc::clone(layout))))
    }))
}

pub(super) fn current_column_output_layout(plan: &PhysicalPlan) -> Option<Arc<ColumnOutputLayout>> {
    CURRENT.with(|slot| {
        slot.borrow().as_ref().and_then(|(key, layout)| {
            (*key == plan as *const _ as usize).then(|| Arc::clone(layout))
        })
    })
}

pub(crate) fn compile_column_output_layout(plan: &PhysicalPlan) -> Option<Arc<ColumnOutputLayout>> {
    let projection = final_projection(plan)?;
    if hash_aggregate_node(plan).is_some()
        || !projection.iter().all(|item| match item {
            SelectItem::Expr { expr, .. } => {
                matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
            }
            _ => true,
        })
        || plan.any(|node| {
            matches!(
                node,
                PhysicalPlan::SetOp { .. }
                    | PhysicalPlan::WindowAgg { .. }
                    | PhysicalPlan::SystemScan { .. }
                    | PhysicalPlan::RecursiveUnion { .. }
            )
        })
    {
        return None;
    }
    let (input, lowered, row_major) = preflight_result_projection(plan, projection, None)?;
    if row_major || input.iter().any(|field| field.source.dynamic_parameter) {
        return None;
    }
    let columns = lowered
        .iter()
        .map(|item| match item {
            VecResultProjection::Column(column) => Some(*column),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()?;
    let output = query_projection_template(projection, &input, &lowered)?;
    let schema = build_batch_schema(&output);
    let bytes = schema
        .heap_capacity_bytes()
        .saturating_mul(2)
        .saturating_add((input.capacity() + output.capacity()) * std::mem::size_of::<PlanField>())
        .saturating_add(columns.capacity() * std::mem::size_of::<usize>())
        .saturating_add(512);
    let charge = mem_budget::ControlledMemoryCharge::try_new_optional(
        mem_budget::ControlledMemoryCategory::DerivedCache,
        bytes,
    )?;
    let charge = Arc::new(charge);
    let schema = Arc::new(schema.with_metadata_charge(Arc::clone(&charge)));
    Some(Arc::new(ColumnOutputLayout {
        input,
        output,
        columns,
        schema,
        _charge: charge,
    }))
}

impl ColumnOutputLayout {
    pub(super) fn template(&self) -> Vec<PlanField> {
        self.output.clone()
    }

    pub(super) fn parts(
        &self,
    ) -> (
        Vec<PlanField>,
        Vec<VecResultProjection>,
        bool,
        Vec<PlanField>,
        Arc<BatchSchema>,
    ) {
        (
            self.input.clone(),
            self.columns
                .iter()
                .copied()
                .map(VecResultProjection::Column)
                .collect(),
            false,
            self.output.clone(),
            Arc::clone(&self.schema),
        )
    }
}
