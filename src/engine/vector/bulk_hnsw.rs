//! Capacity-bounded HNSW construction, independent of persistent payloads.
//! Shared construction codes and dense adjacency avoid random edits to final
//! vector pages. Original/declared payloads are streamed only during publication.

use super::distance::{dot_i8, normalize_in_place, prefetch_read, quantize_into, Metric};
use super::hnsw_core::{random_level, select_diverse, Scored};
use crate::engine::executor::select::grace::SpillTmpDir;
use crate::engine::executor::spill_quota::TrackedSpillWriter;
use crate::storage::RowId;
use crate::types::QueryError;
use parking_lot::Mutex;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::io::Write;
use std::path::Path;

struct CodeSpill {
    writer: Option<TrackedSpillWriter>,
    directory: Option<SpillTmpDir>,
    mapping: Option<super::spill_rows::MappedSpillBytes>,
}
fn spill_io(error: std::io::Error) -> QueryError {
    crate::engine::executor::spill_quota::query_error_from_io(&error).unwrap_or_else(|| {
        crate::storage::error(
            crate::error::catalog::ER_GET_ERRNO,
            format!("HNSW construction code spill: {error}"),
        )
    })
}
use std::sync::atomic::{AtomicU16, AtomicU32, AtomicUsize, Ordering};

pub(crate) struct BulkHnsw {
    dim: usize,
    m: usize,
    ef: usize,
    metric: Metric,
    calibration: (f32, f32, f32),
    codes: Vec<i8>,
    code_spill: Option<CodeSpill>,
    encoding: Vec<i8>,
    norms: Vec<i32>,
    rows: Vec<RowId>,
    levels: Vec<u8>,
    offsets: Vec<u32>,
    edges: Vec<AtomicU32>,
    counts: Vec<AtomicU16>,
    locks: Vec<Mutex<()>>,
    entry: u32,
    expected: usize,
}

struct Scratch {
    visited: Vec<u8>,
    generation: u8,
    frontier: BinaryHeap<Reverse<Scored<u32>>>,
    results: BinaryHeap<Scored<u32>>,
    neighbors: Vec<u32>,
}
impl Scratch {
    fn new(rows: usize, ef: usize, m: usize) -> Self {
        Self {
            visited: vec![0; rows],
            generation: 0,
            frontier: BinaryHeap::with_capacity(ef * m),
            results: BinaryHeap::with_capacity(ef + 1),
            neighbors: Vec::with_capacity(m * 4),
        }
    }
    fn begin(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        if self.generation == 0 {
            self.visited.fill(0);
            self.generation = 1;
        }
        self.frontier.clear();
        self.results.clear();
    }
    fn visit(&mut self, id: u32) -> bool {
        let stamp = &mut self.visited[id as usize];
        if *stamp == self.generation {
            false
        } else {
            *stamp = self.generation;
            true
        }
    }
}

impl BulkHnsw {
    /// Counts actual deterministic layers, including temporary reverse-edge headroom.
    pub(crate) fn estimated_bytes(
        rows: usize,
        dim: usize,
        m: usize,
        threads: usize,
        ef: usize,
    ) -> Option<usize> {
        if rows == 0 || rows > u32::MAX as usize || m < 2 || m > 16383 {
            return None;
        }
        let ef = ef.max(m);
        let ml = 1.0 / (m as f64).ln();
        let upper = (0..rows).try_fold(0usize, |total, id| {
            total.checked_add(random_level(
                id as u64,
                ml,
                super::hnsw_paged::VEC_MAX_LAYER,
            ))
        })?;
        let layers = rows.checked_add(upper)?;
        if layers > u32::MAX as usize {
            return None;
        }
        let edges = rows
            .checked_mul(m.checked_mul(3)?)?
            .checked_add(upper.checked_mul(m.checked_mul(2)?)?)?;
        rows.checked_mul(dim.checked_add(32)?)?
            .checked_add(edges.checked_mul(4)?)?
            .checked_add(layers.checked_mul(2)?)?
            .checked_add(
                threads.max(1).checked_mul(
                    rows.checked_add(ef.checked_mul(m)?.checked_mul(64)?)?
                        .checked_add(1024 * 1024)?,
                )?,
            )?
            .checked_add(16 * 1024 * 1024)
    }

    /// Caller owns a reservation covering estimated_bytes for this shape.
    pub(crate) fn new(
        rows: usize,
        dim: usize,
        metric: Metric,
        m: usize,
        ef: usize,
        calibration: (f32, f32, f32),
        spill_data_dir: Option<&Path>,
    ) -> Result<Self, QueryError> {
        let code_spill = if let Some(data_dir) = spill_data_dir {
            let directory = SpillTmpDir::create_kind(data_dir, "vector-build")?;
            let writer = TrackedSpillWriter::create_new(
                &directory.path().join("codes"),
                directory.charger(),
                4 * 1024 * 1024,
            )?;
            Some(CodeSpill {
                writer: Some(writer),
                directory: Some(directory),
                mapping: None,
            })
        } else {
            None
        };
        Ok(Self {
            dim,
            m,
            ef: ef.max(m),
            metric,
            calibration,
            codes: Vec::with_capacity(if code_spill.is_some() { 0 } else { rows * dim }),
            code_spill,
            encoding: vec![0; dim],
            norms: Vec::with_capacity(rows),
            rows: Vec::with_capacity(rows),
            levels: Vec::with_capacity(rows),
            offsets: Vec::with_capacity(rows),
            edges: Vec::new(),
            counts: Vec::new(),
            locks: Vec::new(),
            entry: 0,
            expected: rows,
        })
    }
    pub(crate) fn push(&mut self, row: RowId, vector: &[f32]) -> Result<(), QueryError> {
        if vector.len() != self.dim || self.rows.len() >= self.expected {
            return Err(super::codec::vector_err(
                "HNSW construction input changed between passes",
            ));
        }
        let mut normalized = Vec::new();
        let source = if self.metric == Metric::Cosine {
            normalized.extend_from_slice(vector);
            normalize_in_place(&mut normalized);
            normalized.as_slice()
        } else {
            vector
        };
        quantize_into(
            source,
            self.calibration.0,
            self.calibration.1,
            &mut self.encoding,
        );
        self.norms.push(dot_i8(&self.encoding, &self.encoding));
        if let Some(spill) = &mut self.code_spill {
            // SAFETY: i8 and u8 have identical layouts and every bit pattern is
            // valid. The writer copies these immutable bytes before returning.
            let bytes = unsafe {
                std::slice::from_raw_parts(self.encoding.as_ptr().cast::<u8>(), self.encoding.len())
            };
            spill
                .writer
                .as_mut()
                .expect("unsealed construction codes")
                .write_all(bytes)
                .map_err(spill_io)?;
        } else {
            self.codes.extend_from_slice(&self.encoding);
        }
        self.rows.push(row);
        Ok(())
    }
    fn seal_codes(&mut self) -> Result<(), QueryError> {
        if let Some(spill) = &mut self.code_spill {
            let expected = self.rows.len() * self.dim;
            if spill
                .writer
                .take()
                .expect("construction codes sealed once")
                .finish()?
                != expected as u64
            {
                return Err(super::codec::vector_err(
                    "HNSW construction spill length mismatch",
                ));
            }
            let directory = spill.directory.take().expect("construction code owner");
            let file = crate::platform::file::File::open(directory.path().join("codes"))
                .map_err(spill_io)?;
            let mapping = super::spill_rows::MappedSpillBytes::open(file, directory)?;
            if mapping.len() != expected {
                return Err(super::codec::vector_err(
                    "HNSW construction spill changed before mapping",
                ));
            }
            mapping.advise_random();
            spill.mapping = Some(mapping);
        }
        Ok(())
    }
    #[inline]
    fn code(&self, id: u32) -> &[i8] {
        let offset = id as usize * self.dim;
        if let Some(spill) = &self.code_spill {
            let bytes = &spill.mapping.as_ref().expect("sealed construction codes")
                [offset..offset + self.dim];
            // SAFETY: the completed mapping is immutable, i8 accepts every
            // byte representation, and this borrow cannot outlive the owner.
            unsafe { std::slice::from_raw_parts(bytes.as_ptr().cast::<i8>(), bytes.len()) }
        } else {
            &self.codes[offset..offset + self.dim]
        }
    }
    #[inline]
    fn distance(&self, a: u32, b: u32) -> f64 {
        (i64::from(self.norms[a as usize]) + i64::from(self.norms[b as usize])
            - 2 * i64::from(dot_i8(self.code(a), self.code(b)))) as f64
    }
    fn edge_offset(&self, id: u32, layer: usize) -> usize {
        let id = id as usize;
        id * self.m * 3
            + (self.offsets[id] as usize - id) * self.m * 2
            + if layer == 0 {
                0
            } else {
                self.m * 3 + (layer - 1) * self.m * 2
            }
    }
    fn temporary_capacity(&self, layer: usize) -> usize {
        if layer == 0 {
            self.m * 3
        } else {
            self.m * 2
        }
    }
    fn neighbors(&self, id: u32, layer: usize, output: &mut Vec<u32>) {
        output.clear();
        if (self.levels[id as usize] as usize) < layer {
            return;
        }
        let _guard = self.locks[id as usize].lock();
        let offset = self.edge_offset(id, layer);
        let count = self.counts[self.offsets[id as usize] as usize + layer].load(Ordering::Relaxed)
            as usize;
        output.extend(
            self.edges[offset..offset + count]
                .iter()
                .map(|edge| edge.load(Ordering::Relaxed)),
        );
    }
    fn store_locked(&self, id: u32, layer: usize, neighbors: &[u32]) {
        let offset = self.edge_offset(id, layer);
        debug_assert!(neighbors.len() <= self.temporary_capacity(layer));
        for (slot, &neighbor) in self.edges[offset..offset + neighbors.len()]
            .iter()
            .zip(neighbors)
        {
            slot.store(neighbor, Ordering::Relaxed);
        }
        self.counts[self.offsets[id as usize] as usize + layer]
            .store(neighbors.len() as u16, Ordering::Relaxed);
    }
    fn select(&self, ranked: &[Scored<u32>], maximum: usize) -> Vec<u32> {
        select_diverse(ranked, maximum, |a, b| {
            Ok::<_, std::convert::Infallible>(self.distance(a, b))
        })
        .unwrap()
        .into_iter()
        .map(|candidate| candidate.id)
        .collect()
    }
    fn prune(&self, id: u32, neighbors: &[u32], maximum: usize) -> Vec<u32> {
        let mut ranked: Vec<_> = neighbors
            .iter()
            .copied()
            .filter(|&other| other != id)
            .map(|other| Scored {
                id: other,
                dist: self.distance(id, other),
            })
            .collect();
        ranked.sort_unstable();
        ranked.dedup_by_key(|candidate| candidate.id);
        self.select(&ranked, maximum)
    }
    fn add_edges(&self, id: u32, layer: usize, additions: &[u32]) {
        let _guard = self.locks[id as usize].lock();
        let offset = self.edge_offset(id, layer);
        let count = self.counts[self.offsets[id as usize] as usize + layer].load(Ordering::Relaxed)
            as usize;
        let mut neighbors: Vec<_> = self.edges[offset..offset + count]
            .iter()
            .map(|edge| edge.load(Ordering::Relaxed))
            .collect();
        for &addition in additions {
            if addition != id && !neighbors.contains(&addition) {
                neighbors.push(addition);
            }
        }
        if neighbors.len() > self.temporary_capacity(layer) {
            neighbors = self.prune(id, &neighbors, if layer == 0 { self.m * 2 } else { self.m });
        }
        self.store_locked(id, layer, &neighbors);
    }
    fn greedy(&self, query: u32, mut current: u32, layer: usize, scratch: &mut Scratch) -> u32 {
        let mut best = self.distance(query, current);
        loop {
            self.neighbors(current, layer, &mut scratch.neighbors);
            let before = current;
            for &candidate in &scratch.neighbors {
                let distance = self.distance(query, candidate);
                if distance < best {
                    best = distance;
                    current = candidate;
                }
            }
            if before == current {
                return current;
            }
        }
    }
    fn search(
        &self,
        query: u32,
        entry: u32,
        layer: usize,
        scratch: &mut Scratch,
    ) -> Vec<Scored<u32>> {
        scratch.begin();
        scratch.visit(entry);
        let first = Scored {
            id: entry,
            dist: self.distance(query, entry),
        };
        scratch.frontier.push(Reverse(first));
        scratch.results.push(first);
        while let Some(Reverse(candidate)) = scratch.frontier.pop() {
            let worst = scratch.results.peek().unwrap().dist;
            if candidate.dist > worst && scratch.results.len() >= self.ef {
                break;
            }
            self.neighbors(candidate.id, layer, &mut scratch.neighbors);
            for index in 0..scratch.neighbors.len() {
                let id = scratch.neighbors[index];
                if !scratch.visit(id) {
                    continue;
                }
                if let Some(&ahead) = scratch.neighbors.get(index + 2) {
                    prefetch_read(self.code(ahead).as_ptr());
                }
                let scored = Scored {
                    id,
                    dist: self.distance(query, id),
                };
                if scratch.results.len() < self.ef || scored < *scratch.results.peek().unwrap() {
                    scratch.frontier.push(Reverse(scored));
                    scratch.results.push(scored);
                    if scratch.results.len() > self.ef {
                        scratch.results.pop();
                    }
                    // Discard queued nodes that are no longer among the best
                    // visited candidates. This bounds stale frontier storage
                    // even on adversarial duplicate/tie-heavy inputs.
                    if scratch.frontier.len() >= self.ef.saturating_mul(4) {
                        let worst = *scratch.results.peek().unwrap();
                        scratch.frontier.retain(|Reverse(value)| *value <= worst);
                    }
                }
            }
        }
        let mut ranked = scratch.results.drain().collect::<Vec<_>>();
        ranked.sort_unstable();
        ranked
    }
    fn link(&self, id: u32, scratch: &mut Scratch) {
        if id == self.entry {
            return;
        }
        let level = self.levels[id as usize] as usize;
        let mut current = self.entry;
        for layer in (level + 1..=self.levels[self.entry as usize] as usize).rev() {
            current = self.greedy(id, current, layer, scratch);
        }
        for layer in (0..=level).rev() {
            let ranked = self.search(id, current, layer, scratch);
            let selected = self.select(
                &ranked
                    .iter()
                    .copied()
                    .filter(|candidate| candidate.id != id)
                    .collect::<Vec<_>>(),
                self.m,
            );
            self.add_edges(id, layer, &selected);
            for &neighbor in &selected {
                self.add_edges(neighbor, layer, &[id]);
            }
            if let Some(best) = ranked.first() {
                current = best.id;
            }
        }
    }
    pub(crate) fn build(&mut self, threads: usize) -> Result<(), QueryError> {
        if self.rows.len() != self.expected {
            return Err(super::codec::vector_err(
                "HNSW construction row count changed",
            ));
        }
        self.seal_codes()?;
        let ml = 1.0 / (self.m as f64).ln();
        let mut layers = 0usize;
        for id in 0..self.rows.len() {
            let level = random_level(id as u64, ml, super::hnsw_paged::VEC_MAX_LAYER);
            if level > self.levels.get(self.entry as usize).copied().unwrap_or(0) as usize {
                self.entry = id as u32;
            }
            self.levels.push(level as u8);
            self.offsets.push(layers as u32);
            layers += level + 1;
        }
        let edge_count = self.rows.len() * self.m * 3 + (layers - self.rows.len()) * self.m * 2;
        self.edges = (0..edge_count).map(|_| AtomicU32::new(0)).collect();
        self.counts = (0..layers).map(|_| AtomicU16::new(0)).collect();
        self.locks = (0..self.rows.len()).map(|_| Mutex::new(())).collect();
        let cursor = AtomicUsize::new(0);
        let cancellation = crate::engine::cancel::CancellationSnapshot::capture();
        std::thread::scope(|scope| -> Result<(), QueryError> {
            let mut handles = Vec::new();
            let graph = &*self;
            for _ in 0..threads.max(1).min(self.rows.len()) {
                let cursor = &cursor;
                let cancellation = &cancellation;
                handles.push(scope.spawn(move || -> Result<(), QueryError> {
                    let _scope = cancellation.enter();
                    let mut scratch = Scratch::new(graph.rows.len(), graph.ef, graph.m);
                    loop {
                        let first = cursor.fetch_add(16, Ordering::Relaxed);
                        if first >= graph.rows.len() {
                            break;
                        }
                        crate::engine::cancel::check_cancelled()?;
                        for id in first..(first + 16).min(graph.rows.len()) {
                            graph.link(id as u32, &mut scratch);
                        }
                    }
                    Ok(())
                }));
            }
            let mut first_error = None;
            for worker in handles {
                match worker.join() {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        first_error.get_or_insert(error);
                    }
                    Err(_) => {
                        first_error.get_or_insert_with(|| {
                            super::codec::vector_err("HNSW construction worker panicked")
                        });
                    }
                }
            }
            first_error.map_or(Ok(()), Err)
        })?;
        let cursor = AtomicUsize::new(0);
        std::thread::scope(|scope| -> Result<(), QueryError> {
            let mut handles = Vec::new();
            for _ in 0..threads.max(1).min(self.rows.len()) {
                let graph = &*self;
                let cursor = &cursor;
                let cancellation = &cancellation;
                handles.push(scope.spawn(move || -> Result<(), QueryError> {
                    let _scope = cancellation.enter();
                    loop {
                        let id = cursor.fetch_add(1, Ordering::Relaxed);
                        if id >= graph.rows.len() {
                            break;
                        }
                        if id % 64 == 0 {
                            crate::engine::cancel::check_cancelled()?;
                        }
                        for layer in 0..=graph.levels[id] as usize {
                            let mut neighbors = Vec::new();
                            graph.neighbors(id as u32, layer, &mut neighbors);
                            let maximum = if layer == 0 { graph.m * 2 } else { graph.m };
                            let selected = graph.prune(id as u32, &neighbors, maximum);
                            let _guard = graph.locks[id].lock();
                            graph.store_locked(id as u32, layer, &selected);
                        }
                    }
                    Ok(())
                }));
            }
            let mut first_error = None;
            for worker in handles {
                match worker.join() {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        first_error.get_or_insert(error);
                    }
                    Err(_) => {
                        first_error.get_or_insert_with(|| {
                            super::codec::vector_err("HNSW final prune worker panicked")
                        });
                    }
                }
            }
            first_error.map_or(Ok(()), Err)
        })?;
        crate::engine::cancel::check_cancelled()?;
        self.repair_connectivity()?;
        crate::engine::cancel::check_cancelled()?;
        Ok(())
    }
    fn repair_connectivity(&self) -> Result<(), QueryError> {
        let mut seen = vec![false; self.rows.len()];
        let mut pending = Vec::with_capacity(self.rows.len());
        let mut neighbors = Vec::new();
        let visit =
            |root: u32, seen: &mut [bool], pending: &mut Vec<u32>, neighbors: &mut Vec<u32>| {
                seen[root as usize] = true;
                pending.push(root);
                while let Some(id) = pending.pop() {
                    self.neighbors(id, 0, neighbors);
                    for &other in neighbors.iter() {
                        if !seen[other as usize] {
                            seen[other as usize] = true;
                            pending.push(other);
                        }
                    }
                }
            };
        visit(self.entry, &mut seen, &mut pending, &mut neighbors);
        if seen.iter().all(|&visited| visited) {
            return Ok(());
        }
        for id in 0..self.rows.len() {
            self.neighbors(id as u32, 0, &mut neighbors);
            if neighbors.len() >= self.m * 2 {
                let selected = self.prune(id as u32, &neighbors, self.m * 2 - 1);
                self.store_locked(id as u32, 0, &selected);
            }
        }
        seen.fill(false);
        visit(self.entry, &mut seen, &mut pending, &mut neighbors);
        let mut anchor = self.entry;
        for id in 0..self.rows.len() {
            if id % 64 == 0 {
                crate::engine::cancel::check_cancelled()?;
            }
            if !seen[id] {
                self.neighbors(anchor, 0, &mut neighbors);
                neighbors.push(id as u32);
                self.store_locked(anchor, 0, &neighbors);
                visit(id as u32, &mut seen, &mut pending, &mut neighbors);
                anchor = id as u32;
            }
        }
        Ok(())
    }
    pub(crate) fn release_codes(&mut self) {
        self.codes = Vec::new();
        self.norms = Vec::new();
        self.code_spill.take();
        self.encoding = Vec::new();
    }
    pub(crate) fn len(&self) -> usize {
        self.rows.len()
    }
    pub(crate) fn row(&self, id: usize) -> RowId {
        self.rows[id]
    }
    pub(crate) fn level(&self, id: usize) -> usize {
        self.levels[id] as usize
    }
    pub(crate) fn entry(&self) -> Option<(u32, usize)> {
        (!self.rows.is_empty()).then(|| (self.entry, self.level(self.entry as usize)))
    }
    pub(crate) fn copy_links(&self, id: usize, links: &mut Vec<Vec<u32>>) {
        links.resize_with(self.level(id) + 1, Vec::new);
        links.truncate(self.level(id) + 1);
        for (layer, output) in links.iter_mut().enumerate() {
            self.neighbors(id as u32, layer, output);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn graph(rows: usize, dim: usize, duplicates: bool) -> BulkHnsw {
        let mut graph = BulkHnsw::new(rows, dim, Metric::L2, 8, 96, (0.0, 1.0, 1.0), None).unwrap();
        let mut seed = 17u64;
        for id in 0..rows {
            let vector: Vec<f32> = (0..dim)
                .map(|_| {
                    seed ^= seed << 13;
                    seed ^= seed >> 7;
                    seed ^= seed << 17;
                    if duplicates {
                        0.0
                    } else {
                        ((seed % 201) as i32 - 100) as f32
                    }
                })
                .collect();
            graph
                .push(
                    RowId {
                        page_id: (id / 5) as u32,
                        slot_id: (id % 5) as u16,
                    },
                    &vector,
                )
                .unwrap();
        }
        graph
    }

    #[test]
    fn bounded_topology_preserves_rows_levels_degrees_and_recall() {
        let mut graph = graph(2048, 33, false);
        graph.build(4).unwrap();
        let mut neighbors = Vec::new();
        for id in 0..graph.len() {
            assert_eq!(
                graph.row(id),
                RowId {
                    page_id: (id / 5) as u32,
                    slot_id: (id % 5) as u16
                }
            );
            assert_eq!(
                graph.level(id),
                random_level(
                    id as u64,
                    1.0 / 8f64.ln(),
                    super::super::hnsw_paged::VEC_MAX_LAYER
                )
            );
            for level in 0..=graph.level(id) {
                graph.neighbors(id as u32, level, &mut neighbors);
                assert!(neighbors.len() <= if level == 0 { 16 } else { 8 });
                assert!(neighbors
                    .iter()
                    .all(|&other| other as usize != id && graph.level(other as usize) >= level));
                let mut unique = neighbors.clone();
                unique.sort_unstable();
                unique.dedup();
                assert_eq!(unique.len(), neighbors.len());
            }
        }
        let mut scratch = Scratch::new(graph.len(), graph.ef, graph.m);
        let mut covered = 0;
        for query in (0..graph.len()).step_by(97) {
            let mut truth: Vec<_> = (0..graph.len())
                .map(|id| Scored {
                    id: id as u32,
                    dist: graph.distance(query as u32, id as u32),
                })
                .collect();
            truth.sort_unstable();
            let actual = graph.search(query as u32, graph.entry, 0, &mut scratch);
            covered += truth[..10]
                .iter()
                .filter(|expected| actual[..10].contains(expected))
                .count();
        }
        assert!(covered >= 190, "top-10 matches across 22 probes: {covered}");
    }

    #[test]
    fn duplicate_codes_still_publish_every_row_in_a_connected_graph() {
        let mut graph = graph(512, 13, true);
        graph.build(4).unwrap();
        let mut seen = vec![false; graph.len()];
        let mut pending = vec![graph.entry];
        let mut neighbors = Vec::new();
        while let Some(id) = pending.pop() {
            if seen[id as usize] {
                continue;
            }
            seen[id as usize] = true;
            graph.neighbors(id, 0, &mut neighbors);
            pending.extend(neighbors.iter().copied());
        }
        assert!(seen.into_iter().all(|visited| visited));
        let mut links = Vec::new();
        for id in 0..graph.len() {
            graph.copy_links(id, &mut links);
            assert!(links[0].len() <= graph.m * 2);
        }
    }

    #[test]
    fn mapped_codes_match_resident_topology_and_release_the_spill() {
        let directory = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let mut resident = graph(1024, 33, false);
        let mut mapped = BulkHnsw::new(
            1024,
            33,
            Metric::L2,
            8,
            96,
            (0.0, 1.0, 1.0),
            Some(directory.path()),
        )
        .unwrap();
        for id in 0..resident.len() {
            let row: Vec<f32> = resident
                .code(id as u32)
                .iter()
                .map(|&code| code as f32)
                .collect();
            mapped.push(resident.row(id), &row).unwrap();
        }
        resident.build(1).unwrap();
        mapped.build(1).unwrap();
        let mut expected = Vec::new();
        let mut actual = Vec::new();
        for id in 0..resident.len() {
            assert_eq!(
                mapped.distance(id as u32, 1023),
                resident.distance(id as u32, 1023)
            );
            resident.copy_links(id, &mut expected);
            mapped.copy_links(id, &mut actual);
            assert_eq!(actual, expected);
        }
        mapped.release_codes();
        assert_eq!(
            std::fs::read_dir(crate::storage::naming::system_tmp_dir(directory.path()))
                .unwrap()
                .count(),
            0
        );
    }

    #[test]
    fn construction_inherits_statement_cancellation() {
        let mut graph = graph(256, 17, false);
        let _deadline =
            crate::engine::cancel::install_statement_deadline(std::time::Duration::from_millis(1));
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert_eq!(graph.build(4).unwrap_err().code(), 3024);
    }

    #[test]
    fn reservation_covers_dense_storage_and_rejects_overflow() {
        let rows = 4096;
        let mut graph = graph(rows, 1, true);
        graph.build(1).unwrap();
        let bytes = BulkHnsw::estimated_bytes(rows, 1, 8, 1, 96).unwrap();
        let actual = graph.codes.capacity()
            + graph.rows.capacity() * std::mem::size_of::<RowId>()
            + graph.norms.capacity() * 4
            + graph.levels.capacity()
            + graph.offsets.capacity() * 4
            + graph.edges.capacity() * 4
            + graph.counts.capacity() * 2
            + graph.locks.capacity() * std::mem::size_of::<Mutex<()>>();
        assert!(actual < bytes, "allocated={actual}, reserved={bytes}");
        assert!(BulkHnsw::estimated_bytes(usize::MAX, 768, 16, 16, 64).is_none());
    }
}
