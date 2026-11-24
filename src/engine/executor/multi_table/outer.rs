//! Prefix-tree enumeration for joins with a preserved right operand. The
//! existing left/inner streaming enumerator remains the common fast path.
use super::*;

type Visitor<'a> = dyn FnMut(&mut [Value], &mut [usize]) -> Result<(), QueryError> + 'a;

fn set_row(
    ctx: &MultiContext,
    depth: usize,
    index: usize,
    combined: &mut [Value],
    indices: &mut [usize],
) -> Result<(), QueryError> {
    let p = &ctx.participants[depth];
    let row = &p.rows[index];
    indices[depth] = index;
    combined[p.base..p.base + row.values.len()].clone_from_slice(&row.values);
    if p.rowid {
        combined[p.base + p.schema.columns.len()] = Value::UInt(crate::storage::rowid::for_row(
            &p.schema,
            crate::storage::DmlRowRef {
                storage_id: row.storage_id,
                row_id: row.row_id,
            },
        )?);
    }
    Ok(())
}

pub(super) fn enumerate(
    ctx: &MultiContext,
    types: &[JoinType],
    conditions: &[Option<BoundMultiExpr>],
    combined: &mut [Value],
    indices: &mut [usize],
    visit: &mut Visitor<'_>,
) -> Result<(), QueryError> {
    prefix(
        ctx,
        types,
        conditions,
        ctx.participants.len(),
        combined,
        indices,
        visit,
    )
}

fn prefix(
    ctx: &MultiContext,
    types: &[JoinType],
    conditions: &[Option<BoundMultiExpr>],
    count: usize,
    combined: &mut [Value],
    indices: &mut [usize],
    visit: &mut Visitor<'_>,
) -> Result<(), QueryError> {
    crate::engine::cancel::check_cancelled()?;
    let depth = count - 1;
    let p = &ctx.participants[depth];
    if depth == 0 {
        for index in 0..p.rows.len() {
            set_row(ctx, depth, index, combined, indices)?;
            visit(combined, indices)?;
        }
        return Ok(());
    }
    let matches = |combined: &[Value]| -> Result<bool, QueryError> {
        conditions[depth].as_ref().map_or(Ok(true), |condition| {
            eval_in_context(condition, ctx, combined).map(|value| truthy(&value))
        })
    };
    if types[depth] == JoinType::Right {
        // Enumerate the left prefix once, recording only which right rows
        // matched. Rebuilding that prefix for every right row would turn a
        // selective N-way chain into an unnecessary Cartesian recomputation.
        let _memory = super::super::mem_budget::ControlledMemoryCharge::try_new(
            super::super::mem_budget::ControlledMemoryCategory::Executor,
            p.rows.len(),
        )?;
        let mut matched = vec![false; p.rows.len()];
        prefix(
            ctx,
            types,
            conditions,
            depth,
            combined,
            indices,
            &mut |combined, indices| {
                for (index, matched) in matched.iter_mut().enumerate() {
                    crate::engine::cancel::check_cancelled()?;
                    set_row(ctx, depth, index, combined, indices)?;
                    if matches(combined)? {
                        *matched = true;
                        visit(combined, indices)?;
                    }
                }
                Ok(())
            },
        )?;
        combined[..p.base].fill(Value::Null);
        indices[..depth].fill(OUTER_NULL_PARTICIPANT);
        for (index, matched) in matched.into_iter().enumerate() {
            if !matched {
                set_row(ctx, depth, index, combined, indices)?;
                visit(combined, indices)?;
            }
        }
    } else {
        prefix(
            ctx,
            types,
            conditions,
            depth,
            combined,
            indices,
            &mut |combined, indices| {
                let mut matched = false;
                for index in 0..p.rows.len() {
                    crate::engine::cancel::check_cancelled()?;
                    set_row(ctx, depth, index, combined, indices)?;
                    if matches(combined)? {
                        matched = true;
                        visit(combined, indices)?;
                    }
                }
                if !matched && types[depth] == JoinType::Left {
                    combined[p.base..p.base + p.width].fill(Value::Null);
                    indices[depth] = OUTER_NULL_PARTICIPANT;
                    visit(combined, indices)?;
                }
                Ok(())
            },
        )?;
    }
    Ok(())
}
