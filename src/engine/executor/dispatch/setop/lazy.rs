//! Unordered consumers drain one reduced partition without an output-file round trip.
use super::*;
use crate::engine::executor::{
    mem_budget::ScopedCharge,
    select::grace::{SpillTmpDir, ValueRowReader},
    work_area::WorkAreaHandle,
};
use std::sync::atomic::Ordering::Relaxed;

pub(super) struct PartitionedSetOutput {
    entries: Option<std::vec::IntoIter<PackedSetEntry>>,
    reader: Option<ValueRowReader>,
    current_run: Option<std::path::PathBuf>,
    runs: std::vec::IntoIter<std::path::PathBuf>,
    paths: std::vec::IntoIter<std::path::PathBuf>,
    pub(super) dir: SpillTmpDir,
    collations: Vec<Option<u16>>,
    op: SetOp,
    all: bool,
    ncols: usize,
    replay_cap: usize,
    state_bytes: usize,
    run_bytes: usize,
    partitions_seen: u64,
    peak: usize,
    next_run: usize,
    charge: ScopedCharge,
    decode_charge: ScopedCharge,
    work: WorkAreaHandle,
}
fn record(metrics: &SetOpReplayMetrics, peak: usize, partitions_seen: u64) {
    SETOP_SPILL_BYTES_WRITTEN.fetch_add(metrics.bytes_written, Relaxed);
    SETOP_SPILL_BYTES_READ.fetch_add(metrics.bytes_read, Relaxed);
    SETOP_SPILL_PARTITIONS.fetch_add(metrics.partitions, Relaxed);
    SETOP_SPILL_REPARTITIONS.fetch_add(metrics.repartitions, Relaxed);
    SETOP_SPILL_MAX_DEPTH.fetch_max(metrics.max_depth, Relaxed);
    SETOP_SPILL_MERGE_PASSES.fetch_add(metrics.merge_passes, Relaxed);
    SETOP_SPILL_FILE_OPENS.fetch_add(metrics.file_opens, Relaxed);
    crate::engine::executor::select::analyze_stats::add_pending_setop_spill(
        metrics.bytes_written,
        partitions_seen,
        metrics.file_opens,
        peak as u64,
        metrics.repartitions,
        metrics.max_depth,
        metrics.merge_passes,
    );
}
impl PartitionedSetOutput {
    #[cfg(test)]
    pub(super) fn first_path(&self) -> Option<&std::path::Path> {
        self.paths.as_slice().first().map(|p| p.as_path())
    }
    pub(super) fn next(
        &mut self,
        mut buffer: Vec<Value>,
        inline_strings: bool,
    ) -> Result<Option<(u64, u64, Vec<Value>)>, QueryError> {
        // The caller has moved the preceding row into its output batch.
        self.decode_charge
            .release_bytes(self.decode_charge.charged());
        loop {
            crate::engine::cancel::check_cancelled()?;
            if let Some(entries) = self.entries.as_mut() {
                if let Some((ordinal, repeats, row)) = entries.next() {
                    return row
                        .decode(self.ncols, buffer, inline_strings, &mut self.decode_charge)
                        .map(|row| Some((ordinal, repeats, row)));
                }
                self.entries = None;
                self.charge.release_bytes(self.state_bytes);
                self.work.release(self.state_bytes);
                self.state_bytes = 0;
            }
            if let Some(reader) = self.reader.as_mut() {
                let row = if inline_strings {
                    reader.next_inline_reuse(std::mem::take(&mut buffer))?
                } else {
                    reader.next()?
                };
                if let Some(mut row) = row {
                    let ordinal = setop_u64(&row, 0)?;
                    let repeats = setop_u64(&row, 1)?;
                    if repeats == 0 {
                        return Err(crate::error::catalog::ER_UNKNOWN_ERROR
                            .error("invalid zero set-operation output count"));
                    }
                    row.drain(..2);
                    return Ok(Some((ordinal, repeats, row)));
                }
                self.reader = None;
                if let Some(path) = self.current_run.take() {
                    SETOP_SPILL_BYTES_READ
                        .fetch_add(path.metadata().map_or(0, |m| m.len()), Relaxed);
                    crate::engine::executor::spill_quota::remove_spill_file(
                        &path,
                        &self.dir.charger(),
                    )?;
                }
            }
            if let Some(path) = self.runs.next() {
                self.reader = Some(ValueRowReader::open(&path, self.dir.charger())?.ok_or_else(
                    || {
                        crate::error::catalog::ER_UNKNOWN_ERROR
                            .error("missing set-operation output run")
                    },
                )?);
                self.current_run = Some(path);
                SETOP_SPILL_FILE_OPENS.fetch_add(1, Relaxed);
                continue;
            }
            self.runs = Vec::new().into_iter();
            self.charge.release_bytes(self.run_bytes);
            self.run_bytes = 0;
            let Some(path) = self.paths.next() else {
                return Ok(None);
            };
            let mut direct = None;
            let mut runs = Vec::new();
            let mut metrics = SetOpReplayMetrics::default();
            let tag = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("partition");
            reduce_setop_spill_partition_into(
                &self.collations,
                self.op,
                self.all,
                self.ncols,
                &path,
                tag,
                0,
                self.replay_cap,
                &self.dir,
                &mut runs,
                &mut self.next_run,
                &self.work,
                &mut self.charge,
                &mut self.peak,
                &mut metrics,
                Some(&mut direct),
            )?;
            self.partitions_seen = self.partitions_seen.saturating_add(metrics.partitions);
            record(&metrics, self.peak, self.partitions_seen);
            self.run_bytes = runs.capacity() * std::mem::size_of::<std::path::PathBuf>()
                + runs.iter().map(|p| p.capacity()).sum::<usize>();
            self.charge.add_force(self.run_bytes)?;
            self.runs = runs.into_iter();
            if let Some((entries, bytes)) = direct {
                self.state_bytes = bytes;
                self.entries = Some(entries.into_iter());
            }
        }
    }
}
#[allow(clippy::too_many_arguments)]
pub(super) fn start(
    collations: &[Option<u16>],
    op: SetOp,
    all: bool,
    ncols: usize,
    dir: SpillTmpDir,
    partitions: usize,
    replay_cap: usize,
    mut charge: ScopedCharge,
    work: WorkAreaHandle,
    peak: usize,
    input_bytes: u64,
    input_opens: u64,
) -> Result<IncrementalSetOutput, QueryError> {
    let metadata = std::mem::size_of::<PartitionedSetOutput>()
        + collations.len() * std::mem::size_of::<Option<u16>>()
        + partitions
            * (std::mem::size_of::<std::path::PathBuf>() + dir.path().as_os_str().len() + 32);
    charge.add_force(metadata)?;
    let mut paths = Vec::with_capacity(partitions);
    for p in 0..partitions {
        let path = dir.path().join(format!("setop-{p}"));
        if !dir.charger().absent_partition(&path)? {
            paths.push(path);
        }
    }
    SETOP_SPILL_COUNT.fetch_add(1, Relaxed);
    record(
        &SetOpReplayMetrics {
            bytes_written: input_bytes,
            partitions: partitions as u64,
            file_opens: input_opens,
            ..Default::default()
        },
        peak,
        partitions as u64,
    );
    Ok(IncrementalSetOutput {
        inner: IncrementalSetOutputInner::Partitioned(Box::new(PartitionedSetOutput {
            entries: None,
            reader: None,
            current_run: None,
            runs: Vec::new().into_iter(),
            paths: paths.into_iter(),
            dir,
            collations: collations.to_vec(),
            op,
            all,
            ncols,
            replay_cap,
            state_bytes: 0,
            run_bytes: 0,
            partitions_seen: partitions as u64,
            peak,
            next_run: 0,
            charge,
            decode_charge: ScopedCharge::new(0),
            work,
        })),
        repeated: None,
        _memory_charge: None,
        _work_area: None,
    })
}
