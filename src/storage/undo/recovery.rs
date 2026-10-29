//! Restore WAL-backed undo without requiring unlogged allocation predecessors.
use super::*;

impl UndoManager {
    pub(super) fn repair_recovery_segments_from_wal(
        &mut self,
        mut frames: Vec<(u64, &[u8])>,
    ) -> Result<(), QueryError> {
        // Allocation is ordered within a shard, but transaction-local WAL can
        // become durable out of order. A missing predecessor therefore does
        // not invalidate the complete later frame supplied by WAL. Keep the
        // contiguous fast path; relocate only retained frames beyond a gap.
        frames.sort_by_key(|(ptr, _)| *ptr);
        let mut ends = self.segment_file_bytes.clone();
        let mut contiguous = Vec::new();
        let mut relocate = Vec::new();
        let mut retired = HashMap::<usize, u32>::new();
        for (ptr, bytes) in frames {
            let seg = undo_ptr_segment(ptr);
            let offset = undo_ptr_offset(ptr);
            let end = ends.entry(seg).or_insert(UNDO_SEGMENT_HEADER_LEN);
            if offset < *end {
                continue;
            }
            if offset == *end {
                *end = offset + UNDO_FRAME_HEADER_LEN as u64 + bytes.len() as u64;
                contiguous.push((ptr, bytes));
            } else {
                retired
                    .entry(shard_of_segment(seg) as usize)
                    .and_modify(|seq| *seq = (*seq).max(seg >> SHARD_BITS))
                    .or_insert(seg >> SHARD_BITS);
                if self.records.contains_key(&ptr) && self.canonical_ptr(ptr) == ptr {
                    relocate.push((ptr, bytes));
                }
            }
        }
        self.repair_segments_from_wal(contiguous)?;
        if relocate.is_empty() {
            return Ok(());
        }
        // Retire affected allocation lanes before publishing aliases. Filling
        // a physical gap later could otherwise reuse a relocated logical id.
        for (shard_idx, high_seq) in retired {
            if let Some(shard) = self.write_shards.get(shard_idx) {
                let mut shard = shard.lock();
                shard.next_seg_seq = shard.next_seg_seq.max(high_seq);
                next_recovery_sequence(shard.next_seg_seq)?;
                shard.rotate()?;
                shard.flush_all()?;
            }
        }
        self.relocate_recovery_frames(relocate)
    }

    fn relocate_recovery_frames(&mut self, frames: Vec<(u64, &[u8])>) -> Result<(), QueryError> {
        let disk_max = self
            .segment_file_bytes
            .keys()
            .filter(|id| shard_of_segment(**id) == COMPACTION_SHARD_LANE)
            .map(|id| id >> SHARD_BITS)
            .max()
            .unwrap_or(0);
        let base = next_recovery_sequence(disk_max.max(self.compaction_seq_high))?;
        let plan = CompactionPlan {
            candidates: Vec::new(),
            size_before: 0,
            moves: Vec::new(),
            out_base_seq: base,
            segment_bytes: self.segment_bytes,
            dir: self.undo_dir.clone().expect("persistent undo recovery"),
        };
        let mut built = BuiltCompaction::default();
        let mut seq = base;
        let mut segment = make_segment_id(COMPACTION_SHARD_LANE, seq);
        let mut buffer = Vec::new();
        write_segment_header_into(&mut buffer, segment, &plan.dir)?;
        let mut remap = Vec::with_capacity(frames.len());
        for (old, payload) in frames {
            let frame_len = UNDO_FRAME_HEADER_LEN + payload.len();
            if buffer.len() as u64 > UNDO_SEGMENT_HEADER_LEN
                && (buffer.len() + frame_len) as u64 > self.segment_bytes
            {
                Self::flush_compaction_segment(
                    &plan,
                    segment,
                    std::mem::take(&mut buffer),
                    &mut built,
                )?;
                seq = next_recovery_sequence(seq)?;
                segment = make_segment_id(COMPACTION_SHARD_LANE, seq);
                write_segment_header_into(&mut buffer, segment, &plan.dir)?;
            }
            let new = make_undo_ptr(segment, buffer.len() as u64)?;
            let start = buffer.len();
            buffer.resize(start + UNDO_FRAME_HEADER_LEN, 0);
            buffer.extend_from_slice(payload);
            let encoded = &mut buffer[start + UNDO_FRAME_HEADER_LEN..];
            Self::patch_encoded_id(encoded, new)?;
            let header = undo_frame_header(encoded);
            buffer[start..start + UNDO_FRAME_HEADER_LEN].copy_from_slice(&header);
            remap.push((old, new));
        }
        Self::flush_compaction_segment(&plan, segment, buffer, &mut built)?;
        let mut pending = Vec::with_capacity(built.out_segments.len());
        for output in &built.out_segments {
            pending.push(output.seg_id);
            self.segment_file_bytes
                .insert(output.seg_id, output.file_len);
        }
        // The original WAL bytes are copied once, one bounded segment at a
        // time. Directory records keep their metadata-only/resident state;
        // startup does not materialize historical row images to move them.
        for &(old, new) in &remap {
            let mut record = self
                .records
                .remove(&old)
                .expect("retained WAL recovery frame");
            record.id = new;
            self.next_undo_id = self.next_undo_id.max(new.saturating_add(1));
            self.records.insert(new, record);
        }
        self.compaction_seq_high = seq;
        self.register_forwardings(&remap);
        self.refresh_metrics();
        // Publish an explicit roll-forward intent before any rename. A crash
        // between output renames must not expose orphan copies without their
        // original-id aliases, nor aliases whose targets cannot be completed.
        crate::platform::fs::sync_path(&plan.dir).map_err(io_err)?;
        self.persist_manifest_with_recovery_segments(&pending)?;
        #[cfg(test)]
        if FAIL_AFTER_RECOVERY_INTENT.with(|flag| flag.replace(false)) {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "induced recovery publication interruption",
            ));
        }
        self.finish_recovery_segment_publication(&pending)?;
        self.compensation_manifest_dirty
            .store(true, Ordering::Release);
        self.persist_manifest()?;
        Ok(())
    }

    pub(super) fn finish_recovery_segment_publication(
        &self,
        pending: &[u32],
    ) -> Result<(), QueryError> {
        if pending.is_empty() {
            return Ok(());
        }
        let dir = self.undo_dir.as_ref().expect("persistent undo recovery");
        if crate::storage::image_io::immutable_path(dir) {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "native snapshot undo requires recovery publication",
            ));
        }
        for &id in pending {
            if shard_of_segment(id) != COMPACTION_SHARD_LANE {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "invalid undo recovery output segment",
                ));
            }
            let path = self.segment_path(id)?;
            if !path.try_exists().map_err(io_err)? {
                fs::rename(path.with_extension("seg.compacting"), &path).map_err(io_err)?;
            }
        }
        crate::platform::fs::sync_path(dir).map_err(io_err)?;
        Ok(())
    }
}

#[cfg(test)]
thread_local! {
    pub(super) static FAIL_AFTER_RECOVERY_INTENT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub(super) fn next_recovery_sequence(sequence: u32) -> Result<u32, QueryError> {
    sequence
        .checked_add(1)
        .filter(|next| *next <= (u32::MAX >> SHARD_BITS))
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "undo recovery segment id exhausted",
            )
        })
}
