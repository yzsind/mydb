//! Resumable ANN state. No heap visibility or SQL semantics live in this layer.
use super::{graph::VectorGraph, hnsw_core::Scored, selection::RowSelection};
use crate::engine::vector::workspace::VectorQueryCharge;
use crate::util::fast_hash::{ah_hash_set, AhHashSet};
use crate::{storage::RowId, types::QueryError, vector::VectorValue};
use std::{cmp::Reverse, collections::BinaryHeap, sync::Arc};

pub(crate) struct CursorWork {
    pub(crate) visited: u64,
    pub(crate) distances: u64,
    pub(crate) predicate_skips: u64,
    pub(crate) connectors: u64,
}
/// Mapped SQ8 node addresses are immutable for the lifetime of a cursor.
struct MappedFrontier {
    nodes: BinaryHeap<Reverse<Scored<(u64, u64)>>>,
    deferred: usize,
    defer_rows: bool,
    stride: usize,
    visited: Vec<u64>,
}

impl MappedFrontier {
    const NO_ROW: u64 = u64::MAX;

    #[inline]
    fn unpack_row(packed: u64) -> RowId {
        RowId::new((packed >> 16) as u32, packed as u16)
    }

    #[inline]
    fn visit(&mut self, id: u64) -> Option<bool> {
        let slot = (id & u32::MAX as u64) as usize;
        if slot >= self.stride {
            return None;
        }
        let index = ((id >> 32) as usize)
            .checked_mul(self.stride)?
            .checked_add(slot)?;
        let word = self.visited.get_mut(index / 64)?;
        let mask = 1 << (index % 64);
        let fresh = *word & mask == 0;
        *word |= mask;
        Some(fresh)
    }
}

pub(crate) struct HnswCursorState {
    pub(crate) frontier: BinaryHeap<Reverse<Scored<u64>>>,
    pub(crate) visited: AhHashSet<u64>,
    pub(crate) pending: BinaryHeap<Reverse<Scored<RowId>>>,
    mapped: Option<MappedFrontier>,
    pub(crate) expanded: usize,
    pub(crate) charge: VectorQueryCharge,
    allocation_credit: usize,
}
impl HnswCursorState {
    fn new() -> Result<Self, QueryError> {
        Ok(Self {
            frontier: BinaryHeap::new(),
            visited: ah_hash_set(),
            pending: BinaryHeap::new(),
            mapped: None,
            expanded: 0,
            charge: VectorQueryCharge::try_new_recoverable(0)?,
            allocation_credit: 0,
        })
    }
    /// Book bounded scratch slabs before mutation, amortizing the shared
    /// query/process ledger across discoveries. Tight budgets can still admit
    /// the minimum required allocation instead of paying for unused credit.
    fn charge_allocation(&mut self, bytes: usize) -> Result<(), QueryError> {
        if self.allocation_credit < bytes {
            let required = bytes - self.allocation_credit;
            let reservation = required.max(16 * 1024);
            match self.charge.try_grow_recoverable(reservation) {
                Ok(()) => self.allocation_credit += reservation,
                Err(err)
                    if reservation > required && err.is(crate::error::catalog::ER_OUTOFMEMORY) =>
                {
                    self.charge.try_grow_recoverable(required)?;
                    self.allocation_credit += required;
                }
                Err(err) => return Err(err),
            }
        }
        self.allocation_credit -= bytes;
        Ok(())
    }
    pub(crate) fn push_pending(&mut self, row: RowId, distance: f64) -> Result<(), QueryError> {
        self.charge_allocation(48)?;
        self.pending.push(Reverse(Scored {
            id: row,
            dist: distance,
        }));
        Ok(())
    }
    pub(crate) fn discover(
        &mut self,
        id: u64,
        distance: f64,
        rows: impl IntoIterator<Item = RowId>,
        sid: u64,
        selection: Option<&RowSelection>,
    ) -> Result<(), QueryError> {
        self.charge_allocation(160)?;
        self.visited.insert(id);
        self.frontier.push(Reverse(Scored { id, dist: distance }));
        for row in rows {
            if selection.is_none_or(|s| s.contains(sid, row)) {
                self.push_pending(row, distance)?;
            }
        }
        Ok(())
    }
    pub(crate) fn start_mapped(
        &mut self,
        geometry: Option<(usize, usize)>,
    ) -> Result<(), QueryError> {
        let mut mapped = MappedFrontier {
            nodes: BinaryHeap::new(),
            deferred: 0,
            defer_rows: true,
            stride: 0,
            visited: Vec::new(),
        };
        if let Some((stride, slots)) = geometry {
            let words = slots.div_ceil(64);
            let bytes = words.saturating_mul(std::mem::size_of::<u64>());
            // Large or sparse files retain the query-local hash set. Keep the
            // bitmap optional so a tight budget still admits small searches.
            if bytes <= 256 * 1024 {
                match self.charge.try_grow_recoverable(bytes) {
                    Ok(()) => {
                        if mapped.visited.try_reserve_exact(words).is_ok() {
                            mapped.visited.resize(words, 0);
                            mapped.stride = stride;
                        } else {
                            self.charge.shrink(bytes);
                        }
                    }
                    Err(err) if err.is(crate::error::catalog::ER_OUTOFMEMORY) => {}
                    Err(err) => return Err(err),
                }
            }
        }
        self.mapped = Some(mapped);
        Ok(())
    }

    /// Mark a node before fetching its code. An uninitialized slot stays
    /// uninitialized until this mapping is revoked and the cursor invalidated.
    #[inline]
    pub(crate) fn visit_mapped(&mut self, id: u64) -> bool {
        self.mapped
            .as_mut()
            .expect("mapped cursor")
            .visit(id)
            .unwrap_or_else(|| self.visited.insert(id))
    }

    pub(crate) fn discover_mapped(
        &mut self,
        id: u64,
        distance: f64,
        row: Option<RowId>,
    ) -> Result<(), QueryError> {
        self.charge_allocation(160)?;
        let mapped = self.mapped.as_mut().expect("mapped cursor");
        let mut packed = MappedFrontier::NO_ROW;
        if let Some(row) = row {
            if mapped.defer_rows {
                packed = (u64::from(row.page_id) << 16) | u64::from(row.slot_id);
                mapped.deferred += 1;
            } else {
                self.push_pending(row, distance)?;
            }
        }
        self.mapped.as_mut().unwrap().nodes.push(Reverse(Scored {
            id: (id, packed),
            dist: distance,
        }));
        Ok(())
    }

    pub(crate) fn pop_mapped(&mut self) -> Result<Option<u64>, QueryError> {
        let mapped = self.mapped.as_mut().expect("mapped cursor");
        let Some(Reverse(next)) = mapped.nodes.pop() else {
            return Ok(None);
        };
        let (id, packed) = next.id;
        if mapped.defer_rows && packed != MappedFrontier::NO_ROW {
            mapped.deferred -= 1;
            self.push_pending(MappedFrontier::unpack_row(packed), next.dist)?;
        }
        Ok(Some(id))
    }

    pub(crate) fn pending_len(&self) -> usize {
        self.pending.len() + self.mapped.as_ref().map_or(0, |mapped| mapped.deferred)
    }

    pub(crate) fn frontier_distance(&self) -> Option<f64> {
        if let Some(mapped) = &self.mapped {
            return mapped.nodes.peek().map(|score| score.0.dist);
        }
        self.frontier.peek().map(|score| score.0.dist)
    }

    fn flush_deferred_rows(&mut self) -> Result<(), QueryError> {
        let count = self.mapped.as_ref().map_or(0, |mapped| mapped.deferred);
        if count == 0 {
            return Ok(());
        }
        self.charge_allocation(count.saturating_mul(48))?;
        let mapped = self.mapped.as_mut().expect("mapped cursor");
        self.pending
            .extend(mapped.nodes.iter().filter_map(|Reverse(node)| {
                let packed = node.id.1;
                (packed != MappedFrontier::NO_ROW).then(|| {
                    Reverse(Scored {
                        id: MappedFrontier::unpack_row(packed),
                        dist: node.dist,
                    })
                })
            }));
        mapped.deferred = 0;
        mapped.defer_rows = false;
        Ok(())
    }

    pub(crate) fn take(&mut self, limit: usize) -> Vec<(RowId, f64)> {
        let mut out = Vec::with_capacity(limit.min(self.pending.len()));
        for _ in 0..limit {
            let Some(Reverse(score)) = self.pending.pop() else {
                break;
            };
            out.push((score.id, score.dist));
        }
        out
    }

    /// Count candidates strictly closer than the frontier. Ties can still
    /// change the emitted batch. Stop counting as soon as it is covered.
    fn covers_frontier(&self, limit: usize) -> bool {
        let Some(next_distance) = self.frontier_distance() else {
            return true;
        };
        // Deferred rows have their scores in the frontier and therefore
        // cannot be strictly closer than its minimum. Only promoted rows
        // can prove that this batch is ready.
        self.pending.len() >= limit
            && (next_distance.is_nan()
                || self
                    .pending
                    .iter()
                    .filter(|score| score.0.dist < next_distance)
                    .take(limit)
                    .count()
                    == limit)
    }
}

pub(crate) struct VectorSearchCursor {
    pub(crate) graph: Arc<VectorGraph>,
    pub(crate) query: VectorValue,
    pub(crate) decoded: Vec<f32>,
    pub(crate) prepared: Option<super::quantization::QuantQuery>,
    pub(crate) native_prepared: Option<super::distance::PreparedNativeDistance>,
    pub(crate) paged_navigation: Option<super::hnsw_paged::PagedCursorNavigation>,
    #[cfg(test)]
    pub(crate) raw_navigation: bool,
    pub(crate) hnsw: HnswCursorState,
    pub(crate) probes: Vec<usize>,
    pub(crate) probe_position: usize,
    pub(crate) probe_offset: usize,
    pub(crate) resume_work: bool,
    pub(crate) max_navigation: usize,
    pub(crate) navigation_incomplete: bool,
    pub(crate) scan_position: usize,
    pub(crate) scan_ids: Vec<u64>,
    pub(crate) epoch: u64,
    pub(crate) initialized: bool,
    pub(crate) sid: u64,
    pub(crate) selection: Option<Arc<RowSelection>>,
    pub(crate) allow_predicate_navigation: bool,
    pub(crate) work: CursorWork,
}
impl VectorSearchCursor {
    pub(crate) fn new(
        graph: Arc<VectorGraph>,
        query: &VectorValue,
        sid: u64,
        selection: Option<Arc<RowSelection>>,
    ) -> Result<Self, QueryError> {
        graph.ensure_valid()?;
        if !graph.is_empty() && graph.dim() != query.dimension as usize {
            return Err(
                crate::error::catalog::MYDB_ER_VECTOR_DIMENSION_MISMATCH.error(format!(
                    "query vector has {} dimensions but index has {}",
                    query.dimension,
                    graph.dim()
                )),
            );
        }
        let mut hnsw = HnswCursorState::new()?;
        hnsw.charge
            .try_grow_recoverable(query.payload.len().saturating_mul(5) + 256)?;
        let decoded = if matches!(
            graph.as_ref(),
            VectorGraph::Memory(_) | VectorGraph::Paged(_)
        ) {
            super::to_f32(query)
        } else {
            Vec::new()
        };
        Ok(Self {
            graph,
            query: query.clone(),
            decoded,
            prepared: None,
            native_prepared: None,
            paged_navigation: None,
            #[cfg(test)]
            raw_navigation: false,
            hnsw,
            probes: Vec::new(),
            probe_position: 0,
            probe_offset: 0,
            resume_work: false,
            max_navigation: usize::MAX,
            navigation_incomplete: false,
            scan_position: 0,
            scan_ids: Vec::new(),
            epoch: 0,
            initialized: false,
            sid,
            selection,
            allow_predicate_navigation: false,
            work: CursorWork {
                visited: 0,
                distances: 0,
                predicate_skips: 0,
                connectors: 0,
            },
        })
    }
    /// None requests heap completion, including a change to the pinned graph.
    pub(crate) fn next_batch(
        &mut self,
        effort: usize,
        limit: usize,
        bounded_refinement: bool,
    ) -> Result<Option<Vec<(RowId, f64)>>, QueryError> {
        let graph = Arc::clone(&self.graph);
        graph.ensure_valid()?;
        let mut navigation_effort = effort.max(self.hnsw.expanded).min(self.max_navigation);
        self.navigation_incomplete = false;
        let valid = loop {
            crate::engine::cancel::check_cancelled()?;
            self.resume_work = false;
            let valid = match graph.as_ref() {
                VectorGraph::NativeHnsw(g) => g.advance_cursor(self, navigation_effort)?,
                VectorGraph::NativeIvf(g) => g.advance_cursor(self, effort)?,
                VectorGraph::Memory(g) => g.advance_cursor(self, navigation_effort)?,
                VectorGraph::Paged(g) => g.advance_cursor(self, navigation_effort)?,
            };
            if !valid {
                break valid;
            }
            if self.navigation_incomplete {
                break true;
            }
            if !self.resume_work {
                if matches!(graph.as_ref(), VectorGraph::NativeIvf(_))
                    || self.hnsw.covers_frontier(limit)
                {
                    break true;
                }
                if navigation_effort >= self.max_navigation {
                    self.navigation_incomplete = true;
                    break true;
                }
                navigation_effort = if bounded_refinement {
                    // Recheck convergence after each short slice when the
                    // candidate pool is close to completion.
                    self.hnsw.expanded.saturating_add(64)
                } else {
                    navigation_effort
                        .saturating_mul(2)
                        .max(self.hnsw.expanded.saturating_add(1))
                }
                .min(self.max_navigation);
            }
            // Backend guards have been released before the next work slice.
            std::thread::yield_now();
        };
        graph.ensure_valid()?;
        if valid && self.navigation_incomplete {
            // A capped single-pass caller may still consume its candidates.
            self.hnsw.flush_deferred_rows()?;
        }
        Ok(valid.then(|| self.hnsw.take(limit)))
    }
    pub(crate) fn exhausted(&self) -> bool {
        self.hnsw.frontier_distance().is_none()
            && self.hnsw.pending_len() == 0
            && (self.probes.is_empty() || self.probe_position >= self.probes.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::vector::{
        hnsw::HnswIndex, ivf::IvfParams, native_hnsw::NativeHnsw, native_ivf::NativeIvf, Metric,
    };

    #[test]
    fn filtered_frontier_waits_for_ties_and_incomplete_batches() {
        let mut state = HnswCursorState::new().unwrap();
        for (id, distance) in [2.0, 3.0, 0.5, 1.0, 2.0].into_iter().enumerate() {
            state
                .push_pending(RowId::new(id as u32, 0), distance)
                .unwrap();
        }
        state.frontier.push(Reverse(Scored { id: 9, dist: 2.0 }));
        assert!(state.covers_frontier(2));
        assert!(!state.covers_frontier(3));
        assert!(!state.covers_frontier(6));
        state.frontier.clear();
        assert!(state.covers_frontier(6));
    }

    #[test]
    fn mapped_frontier_promotes_rows_and_flushes_capped_candidates_once() {
        let mut state = HnswCursorState::new().unwrap();
        state.start_mapped(Some((2, 4))).unwrap();
        let first = 1 << 32;
        let rows = [
            (first, 3.0, Some(RowId::new(12, 0))),
            (first + 1, 1.0, Some(RowId::new(10, 0))),
            (first + 2, 2.0, Some(RowId::new(11, 0))),
            (first + 3, 0.5, None),
        ];
        for (id, distance, row) in rows {
            assert!(state.visit_mapped(id));
            assert!(!state.visit_mapped(id));
            state.discover_mapped(id, distance, row).unwrap();
        }
        assert_eq!(state.pending_len(), 3);
        assert_eq!(state.pop_mapped().unwrap(), Some(first + 3));
        assert_eq!(state.pop_mapped().unwrap(), Some(first + 1));
        assert!(state.covers_frontier(1));
        assert!(!state.covers_frontier(2));
        assert_eq!(state.take(1), vec![(RowId::new(10, 0), 1.0)]);
        state.flush_deferred_rows().unwrap();
        assert_eq!(state.pop_mapped().unwrap(), Some(first + 2));
        assert_eq!(
            state.take(4),
            vec![(RowId::new(11, 0), 2.0), (RowId::new(12, 0), 3.0)]
        );
        assert_eq!(state.pending_len(), 0);
    }

    #[test]
    fn filtered_vector_cursor_reservations_preserve_tight_budgets_and_release() {
        use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
        let budget = Arc::new(MemBudget::new(450));
        with_query_budget(budget.clone(), || {
            let mut state = HnswCursorState::new().unwrap();
            for id in 0..2 {
                state
                    .discover(id, id as f64, [RowId::new(id as u32, 0)], 1, None)
                    .unwrap();
            }
            // Optional unused credit must not reject an otherwise admissible
            // discovery. A genuine limit miss must precede state mutation.
            let err = state
                .discover(2, 2.0, [RowId::new(2, 0)], 1, None)
                .unwrap_err();
            assert!(err.is(crate::error::catalog::ER_OUTOFMEMORY));
            assert_eq!(state.visited.len(), 2);
            assert_eq!(state.frontier.len(), 2);
            assert_eq!(
                state.take(3),
                vec![(RowId::new(0, 0), 0.0), (RowId::new(1, 0), 1.0)]
            );
            assert!(budget.used() > 0);
            assert!(budget.peak_used() <= 450);
        });
        assert_eq!(budget.used(), 0);

        let budget = Arc::new(MemBudget::new(256 * 1024));
        with_query_budget(budget.clone(), || {
            let mut state = HnswCursorState::new().unwrap();
            for id in (0..1000).rev() {
                state
                    .discover(id, id as f64, [RowId::new(id as u32, 0)], 1, None)
                    .unwrap();
            }
            let first = state.take(400);
            let second = state.take(1000);
            assert_eq!(first.len(), 400);
            assert_eq!(second.len(), 600);
            assert!(first
                .into_iter()
                .chain(second)
                .enumerate()
                .all(|(i, row)| { row == (RowId::new(i as u32, 0), i as f64) }));
            assert!(budget.used() > 0);
            assert!(budget.peak_used() <= 256 * 1024);
        });
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn filtered_vector_cursor_resumes_without_repeating_work_and_detects_mutations() {
        const ROWS: u32 = 1152;
        for kind in 0..4 {
            let (_paged_dir, paged_file) = crate::storage::source_audit_vector_file();
            let graph = Arc::new(match kind {
                0 => VectorGraph::Memory(HnswIndex::new(Metric::L2, 8, 64)),
                1 => VectorGraph::NativeHnsw(
                    NativeHnsw::new(
                        crate::vector::VectorDataType::Float16,
                        2,
                        Metric::L2,
                        8,
                        64,
                        None,
                    )
                    .unwrap(),
                ),
                2 => VectorGraph::NativeIvf(
                    NativeIvf::new(
                        crate::vector::VectorDataType::Float32,
                        2,
                        Metric::L2,
                        IvfParams {
                            nlist: 4,
                            ..Default::default()
                        },
                        None,
                    )
                    .unwrap(),
                ),
                _ => VectorGraph::Paged(super::super::hnsw_paged::PagedHnsw::new(paged_file)),
            });
            for i in 0..ROWS {
                let v = super::super::codec::from_f32(&[i as f32, 1.0]);
                let v = if kind == 1 {
                    super::super::codec::coerce_value(
                        crate::types::Value::Vector(v),
                        crate::vector::VectorDataType::Float16,
                        Some(2),
                    )
                    .unwrap()
                } else {
                    v
                };
                graph.insert(RowId::new(i, 0), v).unwrap();
            }
            if let VectorGraph::NativeIvf(g) = graph.as_ref() {
                g.train().unwrap();
            }
            let bad_query = super::super::codec::from_f32(&[1.0]);
            let failure = VectorSearchCursor::new(graph.clone(), &bad_query, 7, None)
                .err()
                .expect("dimension mismatch");
            assert!(failure.is(crate::error::catalog::MYDB_ER_VECTOR_DIMENSION_MISMATCH));
            let query = super::super::codec::from_f32(&[35.125, 1.0]);
            let mut selection = RowSelection::new().unwrap();
            let ids: Vec<_> = (0..ROWS)
                .filter(|i| i % 3 == 0)
                .map(|i| RowId::new(i, 0))
                .collect();
            selection.append(7, &ids).unwrap();
            selection.seal().unwrap();
            let selection = Arc::new(selection);
            let mut cursor =
                VectorSearchCursor::new(graph.clone(), &query, 7, Some(selection.clone())).unwrap();
            let effort = if kind == 2 { 4 } else { ROWS as usize };
            let first = cursor.next_batch(effort, 3, false).unwrap().unwrap();
            let distances = cursor.work.distances;
            let visits = cursor.work.visited;
            let second = cursor.next_batch(effort, 3, false).unwrap().unwrap();
            assert_eq!(cursor.work.distances, distances);
            assert_eq!(cursor.work.visited, visits);
            assert_eq!(first.len(), 3);
            assert_eq!(second.len(), 3);
            for (id, _) in first.iter().chain(&second) {
                assert!(selection.contains(7, *id));
            }
            assert!(first
                .iter()
                .all(|(id, _)| second.iter().all(|(other, _)| id != other)));
            let mut narrow =
                VectorSearchCursor::new(graph.clone(), &query, 7, Some(selection.clone())).unwrap();
            let nearest = narrow.next_batch(1, 32, true).unwrap().unwrap();
            let mut reranked = nearest.clone();
            for (id, distance) in &mut reranked {
                *distance = super::super::distance::ordering_distance(
                    &[35.125, 1.0],
                    &[id.page_id as f32, 1.0],
                    Metric::L2,
                );
            }
            reranked.sort_unstable_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
            assert_eq!(
                reranked
                    .iter()
                    .take(3)
                    .map(|(id, _)| *id)
                    .collect::<Vec<_>>(),
                vec![RowId::new(36, 0), RowId::new(33, 0), RowId::new(39, 0)]
            );
            if kind != 2 {
                assert!(
                    narrow
                        .hnsw
                        .frontier_distance()
                        .is_none_or(|next| next > nearest.last().unwrap().1),
                    "a count of discovered matches is not a converged candidate pool"
                );
            }
            graph.mark_deleted_by_row_id(RowId::new(0, 0)).unwrap();
            assert!(cursor.next_batch(effort, 3, false).unwrap().is_none());
        }
    }
}
