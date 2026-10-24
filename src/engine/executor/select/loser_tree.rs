//! Tournament loser tree for k-way merge.
//!
//! Used by:
//! - [`super::external_sort::merge_runs`] — k-way merge across sorted
//!   spill runs (fan-in = `MYDB_SORT_MERGE_FANIN`, default 32).
//! - [`super::parallel::k_way_merge_sorted`] — merge per-worker outputs
//!   of `ParallelSort` (one input per worker, fan-in = degree).
//!
//! Why not `BinaryHeap`?
//!
//! `BinaryHeap` pop + push for a single output row costs roughly
//! `2 log m` comparisons (sift down + sift up) AND `2 log m`
//! element-sized swaps. With wide heap entries (`Vec<Value>` keys plus
//! a `PlanRow` payload, typically ~56+ bytes each), those swaps dominate
//! the merge loop for big runs.
//!
//! A loser tree keeps each run's current head pinned in a `Vec<Option<E>>`
//! and only moves *indices* during the tournament: per output row the
//! winner walks from one leaf to the root in exactly `⌈log₂ m⌉`
//! comparisons, with no payload movement at all. That cuts the
//! comparison count in half and removes ~`log m` heavy swaps per row.
//!
//! Stability
//! ---------
//!
//! `Ord` on the input alone has no tie-break. Callers that need a
//! stable merge (e.g. preserving the order of equal-keyed rows across
//! runs) must encode the run / source index into the key tuple — that's
//! what both call sites here do.
//!
//! Algorithm
//! ---------
//!
//! Conceptually a complete binary tree with `m` leaves and `m - 1`
//! internal nodes. Each internal node stores the leaf-index of the
//! *loser* of the subtree's tournament; the winner bubbles upward.
//! `tree[0]` ends up holding the global winner. After we emit the
//! winner and refill its leaf, only the path from that leaf to the
//! root is replayed — `O(log m)` comparisons.
//!
//! We collapse leaves and internal nodes into a single `Vec<usize>` of
//! length `m`. Leaf `i` has internal parent index `(i + m) / 2`. (For
//! `m` not a power of two the layout still works because each leaf's
//! parent chain is bounded by `⌈log₂ m⌉`.)
//!
//! An exhausted run is represented by `heads[i] = None`. Comparison
//! treats `None` as `+∞`, so an exhausted leaf always loses to a
//! present leaf and thus never floats to the top.

use std::cmp::Ordering;

const NO_LOSER: usize = usize::MAX;

/// K-way merge tournament. `K` is the comparable key, `T` is an
/// arbitrary payload moved alongside the key (it never participates in
/// comparison, so callers can put rows / IDs / anything else here).
pub(crate) struct LoserTree<K: Ord, T> {
    heads: Vec<Option<(K, T)>>,
    tree: Vec<usize>,
    m: usize,
}

impl<K: Ord, T> LoserTree<K, T> {
    /// Build the tournament from per-run initial heads. `heads[i]` is
    /// the first element of run `i`, or `None` if run `i` is empty.
    pub(crate) fn new(heads: Vec<Option<(K, T)>>) -> Self {
        let m = heads.len();
        let tree = vec![NO_LOSER; m.max(1)];
        let mut t = Self { heads, tree, m };
        // Replay each leaf up to its root path; processing left-to-right
        // means the first leaf seen at a node populates it without a
        // real comparison, and the second leaf plays the proper match.
        for leaf in 0..m {
            t.replay(leaf);
        }
        t
    }

    /// Return the current winner leaf index, or `None` if every run is
    /// exhausted.
    #[inline]
    pub(crate) fn winner_leaf(&self) -> Option<usize> {
        if self.m == 0 {
            return None;
        }
        let leaf = self.tree[0];
        if leaf >= self.m {
            return None;
        }
        if self.heads[leaf].is_none() {
            return None;
        }
        Some(leaf)
    }

    /// Pop the current winner: returns the `(key, payload)` plus the
    /// leaf index that emitted it (caller refills that leaf via
    /// [`Self::refill`]).
    pub(crate) fn pop_winner(&mut self) -> Option<(K, T, usize)> {
        let leaf = self.winner_leaf()?;
        let (k, t) = self.heads[leaf].take()?;
        Some((k, t, leaf))
    }

    /// Provide the next head for `leaf` (or `None` to mark the run
    /// exhausted) and replay the tournament path from that leaf.
    pub(crate) fn refill(&mut self, leaf: usize, head: Option<(K, T)>) {
        debug_assert!(leaf < self.m);
        debug_assert!(
            self.heads[leaf].is_none(),
            "loser_tree::refill called before pop_winner consumed the slot",
        );
        self.heads[leaf] = head;
        self.replay(leaf);
    }

    /// Walk from `leaf` up to the root, settling each internal node's
    /// loser. After the walk the global winner is in `tree[0]`.
    ///
    /// At each internal node we have two candidates: the leaf we're
    /// carrying up, and `tree[node]` (the previously stored loser at
    /// that node — but during build it can be `NO_LOSER`, meaning no
    /// other subtree has played yet, in which case we just deposit our
    /// candidate and stop).
    fn replay(&mut self, leaf: usize) {
        debug_assert!(leaf < self.m);
        let m = self.m;
        let mut curr = leaf;
        let mut node = (leaf + m) / 2;
        while node > 0 {
            let other = self.tree[node];
            if other == NO_LOSER {
                // Build phase: first arrival at this node — store the
                // candidate as the tentative winner and stop. The
                // sibling will arrive in its own replay pass.
                self.tree[node] = curr;
                return;
            }
            // The match itself: smaller key wins. `None` heads are
            // treated as `+∞`, so a present leaf always beats an
            // exhausted one.
            let winner = match (&self.heads[curr], &self.heads[other]) {
                (None, None) => curr.min(other),
                (None, Some(_)) => other,
                (Some(_), None) => curr,
                (Some((ka, _)), Some((kb, _))) => match ka.cmp(kb) {
                    Ordering::Less => curr,
                    Ordering::Greater => other,
                    Ordering::Equal => curr.min(other),
                },
            };
            let loser = if winner == curr { other } else { curr };
            self.tree[node] = loser;
            curr = winner;
            node /= 2;
        }
        self.tree[0] = curr;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Merge a fan-in via the loser tree and compare against the
    /// natural-order sort of the concatenated inputs.
    fn merge_and_collect(runs: Vec<Vec<i64>>) -> Vec<i64> {
        // Each leaf carries (key, payload) where payload tracks the run
        // index so we can refill from the right iterator on each pop.
        let mut iters: Vec<std::vec::IntoIter<i64>> =
            runs.into_iter().map(|r| r.into_iter()).collect();
        let initial: Vec<Option<(i64, usize)>> = iters
            .iter_mut()
            .enumerate()
            .map(|(i, it)| it.next().map(|v| (v, i)))
            .collect();
        let mut tree = LoserTree::new(initial);
        let mut out = Vec::new();
        while let Some((k, run_idx, leaf)) = tree.pop_winner() {
            out.push(k);
            let next = iters[run_idx].next().map(|v| (v, run_idx));
            tree.refill(leaf, next);
        }
        out
    }

    #[test]
    fn empty_runs_yield_empty_output() {
        assert!(merge_and_collect(vec![]).is_empty());
        assert!(merge_and_collect(vec![vec![]]).is_empty());
        assert!(merge_and_collect(vec![vec![], vec![], vec![]]).is_empty());
    }

    #[test]
    fn single_run_passes_through_in_order() {
        let out = merge_and_collect(vec![vec![1, 5, 9, 13]]);
        assert_eq!(out, vec![1, 5, 9, 13]);
    }

    #[test]
    fn two_runs_interleave_correctly() {
        let out = merge_and_collect(vec![vec![1, 3, 5, 7], vec![2, 4, 6, 8]]);
        assert_eq!(out, vec![1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn many_runs_with_unequal_lengths() {
        let out = merge_and_collect(vec![
            vec![1, 4, 9],
            vec![2],
            vec![3, 5, 7, 11, 13],
            vec![],
            vec![6, 8, 10, 12],
        ]);
        assert_eq!(out, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]);
    }

    #[test]
    fn ties_break_by_smaller_run_index_matches_binary_heap() {
        // Two runs producing identical first keys. Tie rule: smaller
        // run index wins. After we pop the winner and the same run
        // *still* has the smallest-tied key, it stays the winner — so
        // run 0 drains first, then run 1. This mirrors `BinaryHeap`'s
        // behaviour (popping (k, run0) and refilling with (k, run0)
        // keeps that entry at the heap top vs (k, run1)).
        let mut iters: Vec<std::vec::IntoIter<i64>> =
            vec![vec![5, 5].into_iter(), vec![5, 5].into_iter()];
        let initial: Vec<Option<(i64, usize)>> = iters
            .iter_mut()
            .enumerate()
            .map(|(i, it)| it.next().map(|v| (v, i)))
            .collect();
        let mut tree = LoserTree::new(initial);
        let mut order = Vec::new();
        while let Some((_k, run_idx, leaf)) = tree.pop_winner() {
            order.push(run_idx);
            let next = iters[run_idx].next().map(|v| (v, run_idx));
            tree.refill(leaf, next);
        }
        assert_eq!(order, vec![0, 0, 1, 1]);
    }

    #[test]
    fn ties_across_runs_keep_each_run_in_original_order() {
        // Three runs that all start with the same first key but end
        // with different second keys. Ensures that ties resolve to
        // smaller-run-first AND that once a run advances past the tied
        // key the next run picks up correctly.
        let mut iters: Vec<std::vec::IntoIter<i64>> = vec![
            vec![5, 7].into_iter(),
            vec![5, 8].into_iter(),
            vec![5, 6].into_iter(),
        ];
        let initial: Vec<Option<(i64, usize)>> = iters
            .iter_mut()
            .enumerate()
            .map(|(i, it)| it.next().map(|v| (v, i)))
            .collect();
        let mut tree = LoserTree::new(initial);
        let mut out: Vec<(i64, usize)> = Vec::new();
        while let Some((k, run_idx, leaf)) = tree.pop_winner() {
            out.push((k, run_idx));
            let next = iters[run_idx].next().map(|v| (v, run_idx));
            tree.refill(leaf, next);
        }
        // First three are the 5s in run-idx order (stable tie-break);
        // then the remaining seconds in numeric order.
        assert_eq!(out, vec![(5, 0), (5, 1), (5, 2), (6, 2), (7, 0), (8, 1)]);
    }

    #[test]
    fn matches_binary_heap_for_randomized_runs() {
        // Compare against std's stable sort on the flat concatenation.
        let mut rng: u64 = 0xC0DE_FACE;
        let mut next = || {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            rng
        };
        for run_count in [1, 2, 3, 5, 8, 16, 32] {
            let mut runs: Vec<Vec<i64>> = Vec::with_capacity(run_count);
            let mut flat: Vec<i64> = Vec::new();
            for _ in 0..run_count {
                let len = (next() % 50) as usize;
                let mut r: Vec<i64> = (0..len).map(|_| (next() % 200) as i64 - 100).collect();
                r.sort();
                flat.extend(&r);
                runs.push(r);
            }
            let merged = merge_and_collect(runs);
            let mut expected = flat;
            expected.sort();
            assert_eq!(merged, expected, "fan_in={run_count}");
        }
    }

    #[test]
    fn fan_in_one_special_case() {
        // m=1 edge case: the loser tree degenerates to a single leaf;
        // tree[0] just tracks that leaf.
        let initial: Vec<Option<(i64, ())>> = vec![Some((42, ()))];
        let mut tree = LoserTree::new(initial);
        assert_eq!(tree.winner_leaf(), Some(0));
        let (k, _, leaf) = tree.pop_winner().unwrap();
        assert_eq!(k, 42);
        tree.refill(leaf, None);
        assert!(tree.pop_winner().is_none());
    }
}
