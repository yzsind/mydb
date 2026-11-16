//! Phase 3E — incremental window-function evaluation.
//!
//! Computes `ROW_NUMBER`, `RANK`, `DENSE_RANK`, and incremental
//! `SUM`/`COUNT`/`AVG`/`MIN`/`MAX` over `ROWS BETWEEN` frames on a
//! sorted input. The implementation deliberately stays narrow: the
//! legacy `compute_window_partitions` in `plan.rs` still owns RANGE
//! frames, exotic frame bounds, and the full window function catalog.
//! The bridge routes the easy cases here; everything else falls back.
//!
//! The input is a fully materialised partition (sorted by the
//! `PARTITION BY` columns then the `ORDER BY` columns). Output is a
//! `Vec<Value>` of the same length as the partition: one window value
//! per input row.
//!
//! Frame increments:
//! - `SUM`/`COUNT` slide via add-on-enter / subtract-on-leave.
//! - `MIN`/`MAX` over a monotonic window use a monotonic deque so each
//!   row costs amortised O(1).
//! - `AVG` is `SUM / COUNT` finalised per row.
//!
//! These shapes cover the majority of analytic workloads (TPC-H Q15-
//! style rolling sums, dashboard moving averages, etc.) and pay back
//! the full-scan cost of the legacy "rescan the whole frame per row"
//! implementation.

use std::collections::VecDeque;

use crate::types::{QueryError, Value};

/// Window aggregation function. The set is intentionally small; the
/// bridge layer is responsible for translating an `Expr::WindowFunction`
/// into one of these or returning `Ok(None)` for the legacy path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFn {
    RowNumber,
    Rank,
    DenseRank,
    Sum,
    Count,
    Avg,
    Min,
    Max,
}

/// `ROWS BETWEEN ...` frame description in row units. Negative values
/// not supported (callers should reject and fall back).
#[derive(Debug, Clone, Copy)]
pub struct RowsFrame {
    /// Distance from the current row to the start of the frame
    /// (`None` = unbounded preceding, so the frame starts at row 0).
    pub preceding: Option<usize>,
    /// Distance from the current row to the end of the frame
    /// (`None` = unbounded following, so the frame ends at the last
    /// partition row).
    pub following: Option<usize>,
}

impl RowsFrame {
    /// MySQL default frame when an `ORDER BY` is present and no frame
    /// is specified: `RANGE UNBOUNDED PRECEDING TO CURRENT ROW`. The
    /// `ROWS` approximation used here matches the legacy fallback's
    /// default — see `plan.rs::execute_window_agg`.
    #[cfg(test)]
    pub fn order_default() -> Self {
        Self {
            preceding: None,
            following: Some(0),
        }
    }

    /// Whole-partition frame, used when there's no `ORDER BY`.
    pub fn whole_partition() -> Self {
        Self {
            preceding: None,
            following: None,
        }
    }
}

/// Compute window-function output for a single partition.
///
/// `column_values` is the column over which the aggregation is performed
/// (ignored for `RowNumber`, used by all others). `order_keys` is a
/// parallel slice giving each row's full sort key — used by `Rank` and
/// `DenseRank` to detect peer groups. If `order_keys` is empty the
/// `Rank` / `DenseRank` functions degrade to `RowNumber`.
#[cfg(test)]
pub fn run_window_partition(
    func: WindowFn,
    frame: RowsFrame,
    column_values: &[Value],
    order_keys: &[Vec<Value>],
) -> Result<Vec<Value>, QueryError> {
    run_window_partition_with_collations(func, frame, column_values, order_keys, &[])
}

pub fn run_window_partition_with_collations(
    func: WindowFn,
    frame: RowsFrame,
    column_values: &[Value],
    order_keys: &[Vec<Value>],
    order_collations: &[Option<u16>],
) -> Result<Vec<Value>, QueryError> {
    let n = column_values.len();
    let mut out: Vec<Value> = Vec::with_capacity(n);

    match func {
        WindowFn::RowNumber => {
            for i in 0..n {
                if i % 256 == 0 {
                    crate::engine::cancel::check_cancelled()?;
                }
                out.push(Value::Int((i + 1) as i64));
            }
        }
        WindowFn::Rank => {
            // SQL standard RANK: ties share the rank, and the next
            // distinct value jumps past the tied count.
            let mut current_rank: i64 = 1;
            for i in 0..n {
                if i % 256 == 0 {
                    crate::engine::cancel::check_cancelled()?;
                }
                if i > 0 && !order_keys_eq(order_keys, order_collations, i - 1, i)? {
                    current_rank = (i + 1) as i64;
                }
                out.push(Value::Int(current_rank));
            }
        }
        WindowFn::DenseRank => {
            let mut current_rank: i64 = 1;
            for i in 0..n {
                if i % 256 == 0 {
                    crate::engine::cancel::check_cancelled()?;
                }
                if i > 0 && !order_keys_eq(order_keys, order_collations, i - 1, i)? {
                    current_rank += 1;
                }
                out.push(Value::Int(current_rank));
            }
        }
        WindowFn::Sum | WindowFn::Count | WindowFn::Avg => {
            // Sliding aggregate. The frame at row `i` is `[lo..=hi]`.
            //
            // SUM / AVG accumulate through `NumericAggState` — the same
            // single-source kernel the legacy row path uses — so an
            // exact (integer / decimal) argument yields the byte-identical
            // `DecimalCompact` / `Int` the legacy
            // `evaluate_window_per_partition` produces (and AVG keeps the
            // `div_precision_increment` scale via `finalize_avg_value`).
            // The slide adds rows entering the frame and subtracts rows
            // leaving it; for an `Exact` state that is precise i128
            // arithmetic, so the result equals a fresh per-frame
            // re-accumulation. The dispatch fast path only routes
            // *exact, uniform-scale* arguments here — float SUM/AVG stays
            // on the legacy path because f64 add/subtract would drift.
            use crate::engine::executor::select::aggregate::{
                accumulate_numeric_state, finalize_avg_value, numeric_state_to_value,
                subtract_numeric_state, NumericAggState,
            };
            let mut state = NumericAggState::default();
            let mut count: u64 = 0;
            let mut prev_lo: usize = 0;
            let mut prev_hi: i64 = -1;
            for i in 0..n {
                if i % 256 == 0 {
                    crate::engine::cancel::check_cancelled()?;
                }
                let (lo, hi) = frame_bounds(i, n, frame);
                let new_lo = lo;
                let new_hi = hi as i64;
                // Remove rows that left the bottom of the frame.
                while prev_lo < new_lo && prev_lo as i64 <= prev_hi {
                    if prev_lo % 256 == 0 {
                        crate::engine::cancel::check_cancelled()?;
                    }
                    let v = &column_values[prev_lo];
                    if !matches!(v, Value::Null) {
                        subtract_numeric_state(&mut state, v)?;
                        count -= 1;
                    }
                    prev_lo += 1;
                }
                // Add rows above prev_hi up to new_hi.
                let mut add_from = (prev_hi + 1) as usize;
                if prev_hi < new_lo as i64 - 1 {
                    // Frame jumped past the previous range; reset state.
                    state = NumericAggState::default();
                    count = 0;
                    add_from = new_lo;
                    prev_lo = new_lo;
                }
                while add_from <= new_hi as usize {
                    if add_from % 256 == 0 {
                        crate::engine::cancel::check_cancelled()?;
                    }
                    let v = &column_values[add_from];
                    if !matches!(v, Value::Null) {
                        accumulate_numeric_state(&mut state, v)?;
                        count += 1;
                    }
                    add_from += 1;
                }
                prev_hi = new_hi;

                // When the frame empties (count == 0) reset the state so
                // the next non-empty frame re-derives its scale from its
                // own values — matching the legacy fresh-accumulation, and
                // avoiding a stale `Exact{magnitude:0, scale}` lingering.
                if count == 0 {
                    state = NumericAggState::default();
                }

                let value = match func {
                    WindowFn::Sum => {
                        if count == 0 {
                            Value::Null
                        } else {
                            numeric_state_to_value(&state)
                        }
                    }
                    WindowFn::Count => Value::Int(count as i64),
                    WindowFn::Avg => finalize_avg_value(&state, count),
                    _ => unreachable!(),
                };
                out.push(value);
            }
        }
        WindowFn::Min => {
            run_min_max_window(frame, column_values, true, &mut out)?;
        }
        WindowFn::Max => {
            run_min_max_window(frame, column_values, false, &mut out)?;
        }
    }
    Ok(out)
}

fn run_min_max_window(
    frame: RowsFrame,
    column_values: &[Value],
    want_min: bool,
    out: &mut Vec<Value>,
) -> Result<(), QueryError> {
    let n = column_values.len();
    let mut deque: VecDeque<usize> = VecDeque::new();
    let mut added_until: usize = 0;

    for i in 0..n {
        if i % 256 == 0 {
            crate::engine::cancel::check_cancelled()?;
        }
        let (lo, hi) = frame_bounds(i, n, frame);
        while added_until <= hi {
            if added_until % 256 == 0 {
                crate::engine::cancel::check_cancelled()?;
            }
            let v = &column_values[added_until];
            if !matches!(v, Value::Null) {
                while let Some(&back_idx) = deque.back() {
                    let back = &column_values[back_idx];
                    let ord = crate::engine::expr::compare_values(back, v)?;
                    let dominated = if want_min {
                        ord == std::cmp::Ordering::Greater
                    } else {
                        ord == std::cmp::Ordering::Less
                    };
                    if dominated {
                        deque.pop_back();
                    } else {
                        break;
                    }
                }
                deque.push_back(added_until);
            }
            added_until += 1;
        }
        while deque.front().is_some_and(|&idx| idx < lo) {
            deque.pop_front();
        }
        out.push(
            deque
                .front()
                .map(|&idx| column_values[idx].clone())
                .unwrap_or(Value::Null),
        );
    }
    Ok(())
}

fn order_keys_eq(
    keys: &[Vec<Value>],
    collations: &[Option<u16>],
    a: usize,
    b: usize,
) -> Result<bool, QueryError> {
    if keys.is_empty() {
        return Ok(true); // Without ORDER BY every row in the partition is a peer.
    }
    for (column, (left, right)) in keys[a].iter().zip(&keys[b]).enumerate() {
        // `compare_values` deliberately gives NULL a synthetic ordering for
        // ORDER BY and therefore returns Less even for NULL/NULL. Peer
        // equality is different: two NULL sort keys are peers.
        match (left, right) {
            (Value::Null, Value::Null) => continue,
            (Value::Null, _) | (_, Value::Null) => return Ok(false),
            _ => {}
        }
        let ordering = crate::engine::expr::with_compare_collation(
            collations.get(column).copied().flatten(),
            || crate::engine::expr::compare_values(left, right),
        )?;
        if ordering != std::cmp::Ordering::Equal {
            return Ok(false);
        }
    }
    Ok(true)
}

fn frame_bounds(i: usize, n: usize, frame: RowsFrame) -> (usize, usize) {
    let lo = match frame.preceding {
        None => 0,
        Some(d) => i.saturating_sub(d),
    };
    let hi = match frame.following {
        None => n - 1,
        Some(d) => (i + d).min(n - 1),
    };
    (lo, hi)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ints(v: &[i64]) -> Vec<Value> {
        v.iter().map(|x| Value::Int(*x)).collect()
    }

    #[test]
    fn row_number_assigns_1_through_n() {
        let col = ints(&[10, 20, 30]);
        let out =
            run_window_partition(WindowFn::RowNumber, RowsFrame::whole_partition(), &col, &[])
                .unwrap();
        assert_eq!(out, vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
    }

    #[test]
    fn rank_handles_ties_correctly() {
        // Values: [10, 10, 20, 20, 30]; expected ranks 1,1,3,3,5
        let col = ints(&[10, 10, 20, 20, 30]);
        let keys: Vec<Vec<Value>> = col.iter().map(|v| vec![v.clone()]).collect();
        let out = run_window_partition(WindowFn::Rank, RowsFrame::whole_partition(), &col, &keys)
            .unwrap();
        assert_eq!(
            out,
            vec![
                Value::Int(1),
                Value::Int(1),
                Value::Int(3),
                Value::Int(3),
                Value::Int(5),
            ]
        );
    }

    #[test]
    fn dense_rank_packs_groups() {
        let col = ints(&[10, 10, 20, 30, 30]);
        let keys: Vec<Vec<Value>> = col.iter().map(|v| vec![v.clone()]).collect();
        let out = run_window_partition(
            WindowFn::DenseRank,
            RowsFrame::whole_partition(),
            &col,
            &keys,
        )
        .unwrap();
        assert_eq!(
            out,
            vec![
                Value::Int(1),
                Value::Int(1),
                Value::Int(2),
                Value::Int(3),
                Value::Int(3),
            ]
        );
    }

    #[test]
    fn rank_treats_null_sort_keys_as_peers() {
        let col = ints(&[0, 0, 1]);
        let keys = vec![vec![Value::Null], vec![Value::Null], vec![Value::Int(1)]];
        let out = run_window_partition(WindowFn::Rank, RowsFrame::whole_partition(), &col, &keys)
            .unwrap();
        assert_eq!(out, ints(&[1, 1, 3]));
    }

    #[test]
    fn rank_peer_equality_uses_column_collation() {
        let col = ints(&[0, 0, 0]);
        let keys = vec![
            vec![Value::String("A".into())],
            vec![Value::String("a".into())],
            vec![Value::String("b".into())],
        ];
        let ci = crate::charset::collation_id("utf8mb4_0900_ai_ci");
        let binary = crate::charset::collation_id("utf8mb4_bin");
        let ci_out = run_window_partition_with_collations(
            WindowFn::Rank,
            RowsFrame::whole_partition(),
            &col,
            &keys,
            &[Some(ci)],
        )
        .unwrap();
        let binary_out = run_window_partition_with_collations(
            WindowFn::Rank,
            RowsFrame::whole_partition(),
            &col,
            &keys,
            &[Some(binary)],
        )
        .unwrap();
        assert_eq!(ci_out, ints(&[1, 1, 3]));
        assert_eq!(binary_out, ints(&[1, 2, 3]));
    }

    #[test]
    fn running_sum_unbounded_preceding_current_row() {
        // ORDER BY default: cumulative sum. SUM now runs through
        // `NumericAggState`, so an integer argument yields the exact
        // `DecimalCompact(_, 0)` the legacy row path produces (scale 0).
        let col = ints(&[1, 2, 3, 4, 5]);
        let out =
            run_window_partition(WindowFn::Sum, RowsFrame::order_default(), &col, &[]).unwrap();
        assert_eq!(
            out,
            vec![
                Value::DecimalCompact(1, 0),
                Value::DecimalCompact(3, 0),
                Value::DecimalCompact(6, 0),
                Value::DecimalCompact(10, 0),
                Value::DecimalCompact(15, 0),
            ]
        );
    }

    #[test]
    fn sliding_sum_three_row_window() {
        // ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING — 3-row sliding window.
        // The slide subtracts rows leaving the frame; for an exact state
        // that is precise i128 arithmetic, so it equals a fresh
        // per-frame re-accumulation.
        let col = ints(&[1, 2, 3, 4, 5]);
        let frame = RowsFrame {
            preceding: Some(1),
            following: Some(1),
        };
        let out = run_window_partition(WindowFn::Sum, frame, &col, &[]).unwrap();
        // Frames: [0..2]=3, [0..3]=6, [1..4]=9, [2..5]=12, [3..5]=9
        assert_eq!(
            out,
            vec![
                Value::DecimalCompact(3, 0),
                Value::DecimalCompact(6, 0),
                Value::DecimalCompact(9, 0),
                Value::DecimalCompact(12, 0),
                Value::DecimalCompact(9, 0),
            ]
        );
    }

    #[test]
    fn sliding_count_skips_nulls() {
        let col = vec![Value::Int(1), Value::Null, Value::Int(3), Value::Null];
        let frame = RowsFrame {
            preceding: Some(1),
            following: Some(1),
        };
        let out = run_window_partition(WindowFn::Count, frame, &col, &[]).unwrap();
        // Frames: [0..2]={1,null}=1, [0..3]={1,null,3}=2, [1..4]={null,3,null}=1, [2..4]={3,null}=1
        assert_eq!(
            out,
            vec![Value::Int(1), Value::Int(2), Value::Int(1), Value::Int(1),]
        );
    }

    #[test]
    fn sliding_min_and_max() {
        let col = ints(&[5, 1, 4, 1, 5, 9, 2, 6, 5]);
        let frame = RowsFrame {
            preceding: Some(2),
            following: Some(0),
        };
        let min_out = run_window_partition(WindowFn::Min, frame, &col, &[]).unwrap();
        let max_out = run_window_partition(WindowFn::Max, frame, &col, &[]).unwrap();
        // 3-row trailing window: min[i] for i=0..8
        assert_eq!(min_out, ints(&[5, 1, 1, 1, 1, 1, 2, 2, 2]));
        assert_eq!(max_out, ints(&[5, 5, 5, 4, 5, 9, 9, 9, 6]));
    }
}

#[cfg(test)]
#[test]
fn vector_window_kernels_observe_cancelled_partition() {
    let token = crate::engine::cancel::QueryCancelToken::noop();
    let _scope = crate::engine::cancel::enter(token.clone());
    token.cancel_local();
    let values = vec![Value::Int(1); 1024];
    let keys = vec![vec![Value::Int(1)]; 1024];
    for function in [
        WindowFn::RowNumber,
        WindowFn::Rank,
        WindowFn::DenseRank,
        WindowFn::Sum,
        WindowFn::Count,
        WindowFn::Avg,
        WindowFn::Min,
        WindowFn::Max,
    ] {
        assert_eq!(
            run_window_partition(function, RowsFrame::whole_partition(), &values, &keys)
                .unwrap_err()
                .code(),
            1317,
            "{function:?}"
        );
    }
}
