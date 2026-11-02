//! Resumable ANN state. No heap visibility or SQL semantics live in this layer.
use super::{graph::VectorGraph, hnsw_core::Scored, selection::RowSelection};
use crate::engine::vector::workspace::VectorQueryCharge;
use crate::{storage::RowId, types::QueryError, vector::VectorValue};
use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashSet},
    sync::Arc,
};

pub(crate) struct CursorWork {
    pub(crate) visited: u64,
    pub(crate) distances: u64,
}
pub(crate) struct HnswCursorState {
    pub(crate) frontier: BinaryHeap<Reverse<Scored<u64>>>,
    pub(crate) visited: HashSet<u64>,
    pub(crate) pending: BinaryHeap<Reverse<Scored<RowId>>>,
    pub(crate) expanded: usize,
    pub(crate) charge: VectorQueryCharge,
}
impl HnswCursorState {
    fn new() -> Result<Self, QueryError> {
        Ok(Self {
            frontier: BinaryHeap::new(),
            visited: HashSet::new(),
            pending: BinaryHeap::new(),
            expanded: 0,
            charge: VectorQueryCharge::try_new_recoverable(0)?,
        })
    }
    pub(crate) fn discover(
        &mut self,
        id: u64,
        distance: f64,
        rows: impl IntoIterator<Item = RowId>,
        sid: u64,
        selection: Option<&RowSelection>,
    ) -> Result<(), QueryError> {
        self.charge.try_grow_recoverable(160)?;
        self.visited.insert(id);
        self.frontier.push(Reverse(Scored { id, dist: distance }));
        for row in rows {
            if selection.is_none_or(|s| s.contains(sid, row)) {
                self.charge.try_grow_recoverable(48)?;
                self.pending.push(Reverse(Scored {
                    id: row,
                    dist: distance,
                }));
            }
        }
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
}

pub(crate) struct VectorSearchCursor {
    pub(crate) graph: Arc<VectorGraph>,
    pub(crate) query: VectorValue,
    pub(crate) decoded: Vec<f32>,
    pub(crate) prepared: Option<super::quantization::QuantQuery>,
    pub(crate) native_prepared: Option<super::distance::PreparedNativeDistance>,
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
            work: CursorWork {
                visited: 0,
                distances: 0,
            },
        })
    }
    /// None means the pinned graph changed; the executor completes on the heap.
    pub(crate) fn next_batch(
        &mut self,
        effort: usize,
        limit: usize,
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
            if !self.resume_work {
                if matches!(graph.as_ref(), VectorGraph::NativeIvf(_))
                    || self.hnsw.frontier.is_empty()
                    || !self.frontier_can_improve(limit)?
                {
                    break true;
                }
                if navigation_effort >= self.max_navigation {
                    self.navigation_incomplete = true;
                    break true;
                }
                navigation_effort = navigation_effort
                    .saturating_mul(2)
                    .max(self.hnsw.expanded.saturating_add(1))
                    .min(self.max_navigation);
            }
            // Backend guards have been released before the next work slice.
            std::thread::yield_now();
        };
        graph.ensure_valid()?;
        Ok(valid.then(|| self.hnsw.take(limit)))
    }
    /// A count of discovered matches is not a convergence proof. In
    /// particular a filtered result radius can extend well beyond the
    /// unfiltered navigation radius. Retain the frontier until it cannot
    /// improve this batch's candidate pool, or the work budget is reached.
    fn frontier_can_improve(&self, limit: usize) -> Result<bool, QueryError> {
        if limit == 0 {
            return Ok(false);
        }
        if self.hnsw.pending.len() < limit {
            return Ok(true);
        }
        let _scratch = VectorQueryCharge::try_new_recoverable(
            limit.saturating_mul(std::mem::size_of::<Scored<RowId>>()),
        )?;
        let mut best = BinaryHeap::with_capacity(limit);
        for score in &self.hnsw.pending {
            let score = Scored {
                id: score.0.id,
                dist: score.0.dist,
            };
            if best.len() < limit {
                best.push(score);
            } else if best.peek().is_some_and(|worst| score < *worst) {
                *best.peek_mut().unwrap() = score;
            }
        }
        Ok(self
            .hnsw
            .frontier
            .peek()
            .is_some_and(|next| best.peek().is_some_and(|worst| next.0.dist <= worst.dist)))
    }
    pub(crate) fn exhausted(&self) -> bool {
        self.hnsw.frontier.is_empty()
            && self.hnsw.pending.is_empty()
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
            let first = cursor.next_batch(effort, 3).unwrap().unwrap();
            let distances = cursor.work.distances;
            let visits = cursor.work.visited;
            let second = cursor.next_batch(effort, 3).unwrap().unwrap();
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
            let nearest = narrow.next_batch(1, 3).unwrap().unwrap();
            assert_eq!(
                nearest.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
                vec![RowId::new(36, 0), RowId::new(33, 0), RowId::new(39, 0)]
            );
            if kind != 2 {
                assert!(
                    narrow
                        .hnsw
                        .frontier
                        .peek()
                        .is_none_or(|next| { next.0.dist > nearest.last().unwrap().1 }),
                    "a count of discovered matches is not a converged candidate pool"
                );
            }
            graph.mark_deleted_by_row_id(RowId::new(0, 0)).unwrap();
            assert!(cursor.next_batch(effort, 3).unwrap().is_none());
        }
    }
}
