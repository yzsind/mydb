//! Parallel in-place sort utility.
//!
//! `parallel_sort_unstable` splits a slice into roughly equal chunks,
//! sorts each on its own thread via `std::thread::scope`, then merges
//! the sorted runs back into a fresh `Vec` and writes the result back
//! into the input slice with `swap_nonoverlapping`. Output buffer
//! costs one extra allocation the size of the input — acceptable for
//! the index-build / external-sort use cases that drive this path,
//! since the alternative is a fully serial `sort_unstable`.
//!
//! `degree=1` (or any input below the parallel threshold) falls
//! through to the standard library `sort_unstable`. The merge always
//! deduplicates adjacent equal values, matching `sort_unstable + dedup`
//! semantics so the helper is a drop-in for the index-build sorter.
//!
//! Generic over any `T: Ord + Send`; the helper is invoked with both
//! `IndexEntry` (for the generic index path) and narrow integer-key
//! triples like `(i64, RowId)` / `((i64, i64), RowId)` from the
//! specialised paths.

use std::cmp::Ordering;
use std::ptr;

/// Below this, parallelism overhead would dominate; just call
/// `sort_unstable` directly. Tuned by observation: a 6 M-element
/// shuffle drops from ~600 ms to ~200 ms on 4 cores; below ~256 K the
/// thread fan-out + merge costs more than the saving.
const PARALLEL_SORT_THRESHOLD: usize = 256 * 1024;

/// Sort `data` in place in ascending order, optionally deduplicating
/// equal-adjacent runs. `degree` caps the worker thread count; values
/// `<= 1` and inputs below [`PARALLEL_SORT_THRESHOLD`] short-circuit
/// to the serial path so callers can pass through their global
/// `MYDB_PARALLEL_DEGREE` without special-casing tiny inputs.
pub fn parallel_sort_unstable_dedup<T>(data: &mut Vec<T>, degree: usize)
where
    T: Ord + Send,
{
    if degree <= 1 || data.len() < PARALLEL_SORT_THRESHOLD {
        data.sort_unstable();
        data.dedup();
        return;
    }
    let effective = degree
        .min(data.len().div_ceil(PARALLEL_SORT_THRESHOLD / 2))
        .max(2);
    let chunk_size = data.len().div_ceil(effective);
    std::thread::scope(|scope| {
        for chunk in data.chunks_mut(chunk_size) {
            scope.spawn(|| chunk.sort_unstable());
        }
    });
    // Build (chunk_start, chunk_end) ranges to drive pairwise merge rounds.
    let mut ranges: Vec<(usize, usize)> = Vec::with_capacity(effective);
    let mut offset = 0usize;
    while offset < data.len() {
        let end = (offset + chunk_size).min(data.len());
        ranges.push((offset, end));
        offset = end;
    }
    pairwise_merge_dedup(data, ranges);
}

/// Comparator-driven counterpart used by encoded arenas whose sortable item
/// is a compact ordinal rather than the key bytes themselves.
pub fn parallel_sort_unstable_dedup_by<T, F>(data: &mut Vec<T>, degree: usize, compare: F)
where
    T: Send,
    F: Fn(&T, &T) -> Ordering + Sync,
{
    if degree <= 1 || data.len() < PARALLEL_SORT_THRESHOLD {
        data.sort_unstable_by(&compare);
        data.dedup_by(|right, left| compare(left, right).is_eq());
        return;
    }
    let effective = degree
        .min(data.len().div_ceil(PARALLEL_SORT_THRESHOLD / 2))
        .max(2);
    let chunk_size = data.len().div_ceil(effective);
    std::thread::scope(|scope| {
        for chunk in data.chunks_mut(chunk_size) {
            let compare = &compare;
            scope.spawn(move || chunk.sort_unstable_by(compare));
        }
    });
    let mut ranges = Vec::with_capacity(effective);
    let mut offset = 0usize;
    while offset < data.len() {
        let end = (offset + chunk_size).min(data.len());
        ranges.push((offset, end));
        offset = end;
    }
    pairwise_merge_dedup_by(data, ranges, &compare);
}

fn pairwise_merge_dedup_by<T, F>(data: &mut Vec<T>, mut ranges: Vec<(usize, usize)>, compare: &F)
where
    F: Fn(&T, &T) -> Ordering,
{
    let mut scratch = Vec::with_capacity(data.len());
    while ranges.len() > 1 {
        let source = data.as_ptr();
        let mut next_ranges = Vec::with_capacity(ranges.len().div_ceil(2));
        for pair in ranges.chunks(2) {
            let output_start = scratch.len();
            match pair {
                [left, right] => unsafe {
                    merge_two_runs_dedup_by(source, *left, *right, &mut scratch, compare);
                },
                [single] => unsafe {
                    move_run_dedup_by(source, *single, &mut scratch, compare);
                },
                _ => unreachable!(),
            }
            next_ranges.push((output_start, scratch.len()));
        }
        unsafe { data.set_len(0) };
        std::mem::swap(data, &mut scratch);
        ranges = next_ranges;
    }
    data.dedup_by(|right, left| compare(left, right).is_eq());
}

unsafe fn merge_two_runs_dedup_by<T, F>(
    source: *const T,
    left: (usize, usize),
    right: (usize, usize),
    output: &mut Vec<T>,
    compare: &F,
) where
    F: Fn(&T, &T) -> Ordering,
{
    let (mut left_pos, left_end) = left;
    let (mut right_pos, right_end) = right;
    while left_pos < left_end && right_pos < right_end {
        let take_left =
            unsafe { !compare(&*source.add(left_pos), &*source.add(right_pos)).is_gt() };
        let value = if take_left {
            let value = unsafe { ptr::read(source.add(left_pos)) };
            left_pos += 1;
            value
        } else {
            let value = unsafe { ptr::read(source.add(right_pos)) };
            right_pos += 1;
            value
        };
        push_dedup_by(output, value, compare);
    }
    while left_pos < left_end {
        let value = unsafe { ptr::read(source.add(left_pos)) };
        left_pos += 1;
        push_dedup_by(output, value, compare);
    }
    while right_pos < right_end {
        let value = unsafe { ptr::read(source.add(right_pos)) };
        right_pos += 1;
        push_dedup_by(output, value, compare);
    }
}

unsafe fn move_run_dedup_by<T, F>(
    source: *const T,
    run: (usize, usize),
    output: &mut Vec<T>,
    compare: &F,
) where
    F: Fn(&T, &T) -> Ordering,
{
    for position in run.0..run.1 {
        push_dedup_by(output, unsafe { ptr::read(source.add(position)) }, compare);
    }
}

fn push_dedup_by<T, F>(output: &mut Vec<T>, value: T, compare: &F)
where
    F: Fn(&T, &T) -> Ordering,
{
    if output
        .last()
        .is_some_and(|previous| compare(previous, &value).is_eq())
    {
        drop(value);
    } else {
        output.push(value);
    }
}

/// Merge adjacent sorted runs in logarithmic rounds. Each round moves every
/// live element exactly once into one scratch Vec, then swaps that allocation
/// with the caller's Vec. Compared with a BinaryHeap k-way merge this removes
/// O(log degree) heap maintenance and, importantly, avoids the final full-Vec
/// move back into the source allocation. Source slots are read exactly once
/// with `ptr::read`; their length is reset before the allocation is reused.
fn pairwise_merge_dedup<T: Ord>(data: &mut Vec<T>, mut ranges: Vec<(usize, usize)>) {
    let mut scratch = Vec::with_capacity(data.len());
    while ranges.len() > 1 {
        let source = data.as_ptr();
        let mut next_ranges = Vec::with_capacity(ranges.len().div_ceil(2));
        for pair in ranges.chunks(2) {
            let output_start = scratch.len();
            match pair {
                [left, right] => unsafe {
                    merge_two_runs_dedup(source, *left, *right, &mut scratch);
                },
                [single] => unsafe {
                    move_run_dedup(source, *single, &mut scratch);
                },
                _ => unreachable!(),
            }
            next_ranges.push((output_start, scratch.len()));
        }
        // SAFETY: both merge helpers moved every element from every source
        // range exactly once. No source element may be dropped again.
        unsafe {
            data.set_len(0);
        }
        std::mem::swap(data, &mut scratch);
        debug_assert!(scratch.is_empty());
        ranges = next_ranges;
    }
    data.dedup();
}

unsafe fn merge_two_runs_dedup<T: Ord>(
    source: *const T,
    left: (usize, usize),
    right: (usize, usize),
    output: &mut Vec<T>,
) {
    let (mut left_pos, left_end) = left;
    let (mut right_pos, right_end) = right;
    while left_pos < left_end && right_pos < right_end {
        let take_left = unsafe { &*source.add(left_pos) <= &*source.add(right_pos) };
        let value = if take_left {
            let value = unsafe { ptr::read(source.add(left_pos)) };
            left_pos += 1;
            value
        } else {
            let value = unsafe { ptr::read(source.add(right_pos)) };
            right_pos += 1;
            value
        };
        push_dedup(output, value);
    }
    while left_pos < left_end {
        let value = unsafe { ptr::read(source.add(left_pos)) };
        left_pos += 1;
        push_dedup(output, value);
    }
    while right_pos < right_end {
        let value = unsafe { ptr::read(source.add(right_pos)) };
        right_pos += 1;
        push_dedup(output, value);
    }
}

unsafe fn move_run_dedup<T: Ord>(source: *const T, run: (usize, usize), output: &mut Vec<T>) {
    for position in run.0..run.1 {
        push_dedup(output, unsafe { ptr::read(source.add(position)) });
    }
}

#[inline]
fn push_dedup<T: Ord>(output: &mut Vec<T>, value: T) {
    if output.last() == Some(&value) {
        drop(value);
    } else {
        output.push(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serial_path_sorts_and_dedups() {
        let mut v = vec![5i32, 3, 9, 1, 5, 3, 7];
        parallel_sort_unstable_dedup(&mut v, 1);
        assert_eq!(v, vec![1, 3, 5, 7, 9]);
    }

    #[test]
    fn small_input_falls_through_to_serial() {
        let mut v = vec![3i32, 1, 2, 1];
        parallel_sort_unstable_dedup(&mut v, 8);
        assert_eq!(v, vec![1, 2, 3]);
    }

    #[test]
    fn large_input_parallel_path_matches_serial() {
        // Generate enough shuffled values to cross the parallel threshold
        // without turning the default unit suite into a microbenchmark.
        let mut rng_state: u64 = 0xdead_beef;
        let len = PARALLEL_SORT_THRESHOLD + 16 * 1024;
        let mut data: Vec<u64> = (0..len)
            .map(|_| {
                rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
                rng_state
            })
            .collect();
        let mut serial = data.clone();
        serial.sort_unstable();
        serial.dedup();
        parallel_sort_unstable_dedup(&mut data, 4);
        assert_eq!(data, serial);
    }

    #[test]
    fn duplicates_collapse_across_chunks() {
        // Several copies of one value spread across all chunks must
        // collapse to one. Guards against off-by-one in the
        // last_pushed bookkeeping.
        let mut v: Vec<i32> = (0..400_000).flat_map(|i| [i / 100, i / 100]).collect();
        let mut serial = v.clone();
        serial.sort_unstable();
        serial.dedup();
        parallel_sort_unstable_dedup(&mut v, 4);
        assert_eq!(v, serial);
    }
}
