//! Short-circuit indexed existence checks without materializing every match.
use super::*;

/// Only positive existence is known before exhausting a key, except that a
/// NULL comparison operand can never satisfy ordinary inequality.
fn known_exists(summary: &NeqBuildSummary, value: &Value) -> Option<bool> {
    if matches!(value, Value::Null) {
        Some(false)
    } else if summary.non_null > 0 && (!summary.all_same || summary.first != ValueKey::from(value))
    {
        Some(true)
    } else {
        None
    }
}

fn known_conjunction(
    inner: Option<bool>,
    outer: Option<bool>,
    inner_semi: bool,
    outer_semi: bool,
) -> Option<bool> {
    if inner.is_some_and(|found| found != inner_semi)
        || outer.is_some_and(|found| found != outer_semi)
    {
        Some(false)
    } else if inner == Some(inner_semi) && outer == Some(outer_semi) {
        Some(true)
    } else {
        None
    }
}

pub(super) fn read_summaries(
    db: &Arc<Database>,
    txn: TxnView,
    spec: &FusedNeqBatchSpec,
    batch: &crate::engine::executor::vec::Batch,
    visible: &[usize],
    keys: &[IndexKey],
    groups: &[smallvec::SmallVec<[usize; 1]>],
) -> Result<(Vec<NeqBuildSummary>, Vec<NeqBuildSummary>), QueryError> {
    let handle = db.cached_index_handle(spec.table.schema.id, &spec.index.name)?;
    let (mut entries, _charge) =
        lookup_inlj_entries_parallel(db, &handle, &spec.table.schema, &spec.index, keys, txn)?;
    // Pending index repairs can return a different entry order. Group by key
    // and heap position explicitly; no ordering assumption escapes storage.
    entries.sort_unstable_by_key(|&(key, row)| (key, row));
    let mut ranges = vec![(0usize, 0usize, 0usize); keys.len()];
    for (position, &(key, _)) in entries.iter().enumerate() {
        let range = &mut ranges[key as usize];
        if range.0 == range.1 {
            range.0 = position;
            range.2 = position;
        }
        range.1 = position + 1;
    }
    let mut inner = vec![empty_neq_build_summary(); keys.len()];
    let mut outer = vec![empty_neq_build_summary(); keys.len()];
    if entries.is_empty() {
        record_inlj_probe_batch(keys.len() as u64, 0, 0);
        return Ok((inner, outer));
    }
    let heap = db.cached_heap_handle(spec.table.schema.id)?;
    let wave_rows = (spec.work_bytes_per_row.saturating_mul(visible.len()) / 2)
        .min(16 * 1024 * 1024)
        .checked_div(spec.candidate_row_bytes.max(1))
        .unwrap_or(1)
        .clamp(1, 32_768);
    let mut read_ids = Vec::new();
    loop {
        crate::engine::cancel::check_cancelled()?;
        let mut requests = Vec::with_capacity(keys.len().saturating_mul(2));
        for (key, range) in ranges.iter_mut().enumerate() {
            if range.0 == range.1 {
                continue;
            }
            let decided = groups[key].iter().all(|&ordinal| {
                let row = visible[ordinal];
                known_conjunction(
                    known_exists(
                        &inner[key],
                        &batch.columns()[spec.inner.left_idx].value_at(row),
                    ),
                    known_exists(
                        &outer[key],
                        &batch.columns()[spec.outer.left_idx].value_at(row),
                    ),
                    spec.inner_is_semi,
                    spec.outer_is_semi,
                )
                .is_some()
            });
            if decided {
                range.0 = range.1;
                continue;
            }
            // Start with two candidates. Grow unresolved keys geometrically
            // so negative proofs with large fanout still use bulk MRR rather
            // than performing one tiny storage call per pair of matches.
            let room = wave_rows.saturating_sub(requests.len());
            if room == 0 {
                break;
            }
            let count = (range.0 - range.2).saturating_add(2).min(room);
            let end = range.0.saturating_add(count).min(range.1);
            requests.extend_from_slice(&entries[range.0..end]);
            range.0 = end;
        }
        if requests.is_empty() {
            break;
        }
        requests.sort_unstable_by_key(|&(_, row)| row);
        let mut ids: Vec<RowId> = requests.iter().map(|&(_, row)| row).collect();
        ids.dedup();
        read_ids.extend_from_slice(&ids);
        let mut rows =
            read_inlj_probe_rows_parallel(db, &heap, &spec.table, &ids, txn, &spec.projection)?;
        rows.sort_unstable_by_key(|pair| pair.0);
        let positions = map_inlj_entries_to_right_positions(&requests, &rows)?;
        for (&(key, _), position) in requests.iter().zip(positions) {
            if position < 0 {
                continue;
            }
            let values = &rows[position as usize].1;
            update_fused_neq_summary(&spec.inner, &mut inner[key as usize], values)?;
            update_fused_neq_summary(&spec.outer, &mut outer[key as usize], values)?;
        }
    }
    read_ids.sort_unstable();
    read_ids.dedup();
    record_inlj_probe_batch(
        keys.len() as u64,
        entries.len() as u64,
        read_ids.len() as u64,
    );
    Ok((inner, outer))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn existence_proofs_hold_for_every_unread_suffix_and_null_combination() {
        let inputs = [
            Value::Null,
            Value::Int(1),
            Value::Int(2),
            Value::UInt(u64::MAX),
        ];
        for prefix in [
            vec![],
            vec![Value::Null],
            vec![Value::Int(1)],
            vec![Value::Int(1), Value::Int(2)],
        ] {
            let mut summary = empty_neq_build_summary();
            for value in &prefix {
                update_neq_build_summary(&mut summary, value);
            }
            for left in &inputs {
                for right in &inputs {
                    for inner_semi in [false, true] {
                        for outer_semi in [false, true] {
                            let known = known_conjunction(
                                known_exists(&summary, left),
                                known_exists(&summary, right),
                                inner_semi,
                                outer_semi,
                            );
                            for suffix in [
                                vec![],
                                vec![Value::Null],
                                vec![Value::Int(1)],
                                vec![Value::Int(2)],
                                vec![Value::UInt(u64::MAX)],
                            ] {
                                let exists = |probe: &Value| {
                                    !matches!(probe, Value::Null)
                                        && prefix.iter().chain(&suffix).any(|v| {
                                            !matches!(v, Value::Null)
                                                && ValueKey::from(v) != ValueKey::from(probe)
                                        })
                                };
                                let oracle =
                                    exists(left) == inner_semi && exists(right) == outer_semi;
                                if let Some(proof) = known {
                                    assert_eq!(proof, oracle);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
