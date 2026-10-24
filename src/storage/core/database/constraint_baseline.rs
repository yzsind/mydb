// Immutable, sorted snapshot counts. Full validation writes only sequential
// streams; random updates are confined to keys touched by concurrent DML.
struct ConstraintFkBaseline {
    records: crate::engine::executor::spill_quota::LeasedSpillFile,
    offsets: crate::engine::executor::spill_quota::LeasedSpillFile,
    _tmp: crate::engine::executor::select::grace::SpillTmpDir,
    len: u64,
    max_key_bytes: usize,
}

impl ConstraintFkBaseline {
    fn build(
        db: &Database,
        entries: impl Iterator<Item = Result<IndexEntry, QueryError>>,
        jobs: &[Arc<AlterCopyJob>],
    ) -> Result<(Self, usize), QueryError> {
        let tmp = crate::engine::executor::select::grace::SpillTmpDir::create_kind(
            db.data_dir(),
            "constraint",
        )?;
        let records = tmp.charger().lease_file(
            OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(tmp.path().join("baseline"))
                .map_err(io_err)?,
        )?;
        let offsets = tmp.charger().lease_file(
            OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(tmp.path().join("offsets"))
                .map_err(io_err)?,
        )?;
        let mut record_writer =
            crate::engine::executor::spill_quota::TrackedSpillWriter::from_raw_file(
                records.try_clone().map_err(io_err)?,
                tmp.charger(),
                64 * 1024,
            )?;
        let mut offset_writer =
            crate::engine::executor::spill_quota::TrackedSpillWriter::from_raw_file(
                offsets.try_clone().map_err(io_err)?,
                tmp.charger(),
                64 * 1024,
            )?;
        let mut previous: Option<IndexKey> = None;
        let mut counts = (0u64, 0u64);
        let mut len = 0u64;
        let mut end = 0u64;
        let mut orphans = 0usize;
        let mut max_key_bytes = 0;
        let mut encoded = Vec::new();
        let mut emit = |key: &IndexKey, counts: (u64, u64)| -> Result<(), QueryError> {
            crate::engine::cancel::check_cancelled()?;
            encoded.clear();
            encode_index_key_into(key, &mut encoded)?;
            max_key_bytes = max_key_bytes.max(encoded.len());
            offset_writer
                .write_all(&end.to_le_bytes())
                .map_err(io_err)?;
            record_writer
                .write_all(&(encoded.len() as u32).to_le_bytes())
                .map_err(io_err)?;
            record_writer.write_all(&encoded).map_err(io_err)?;
            record_writer
                .write_all(&counts.0.to_le_bytes())
                .map_err(io_err)?;
            record_writer
                .write_all(&counts.1.to_le_bytes())
                .map_err(io_err)?;
            end += 20 + encoded.len() as u64;
            len += 1;
            orphans += usize::from(counts.0 != 0 && counts.1 == 0);
            Ok(())
        };
        for (position, entry) in entries.enumerate() {
            if position % 4096 == 0 {
                crate::engine::cancel::check_cancelled()?;
                for job in jobs {
                    Database::check_alter_copy_cancelled(job)?;
                }
            }
            let entry = entry?;
            if previous.as_ref().is_some_and(|key| *key != entry.key) {
                emit(previous.as_ref().unwrap(), counts)?;
                counts = (0, 0);
            }
            if previous.as_ref() != Some(&entry.key) {
                previous = Some(entry.key);
            }
            counts = adjusted_fk_counts(counts, entry.row_id.slot_id & 0x8000 != 0, true)?;
        }
        if let Some(key) = previous {
            emit(&key, counts)?;
        }
        record_writer.finish()?;
        offset_writer.finish()?;
        Ok((
            Self {
                records,
                offsets,
                _tmp: tmp,
                len,
                max_key_bytes,
            },
            orphans,
        ))
    }

    fn get(&self, key: &IndexKey) -> Result<(u64, u64), QueryError> {
        #[cfg(unix)]
        use std::os::unix::fs::FileExt;
        let mut low = 0;
        let mut high = self.len;
        let mut buffer = Vec::new();
        while low < high {
            crate::engine::cancel::check_cancelled()?;
            let mid = low + (high - low) / 2;
            let mut offset = [0; 8];
            self.offsets
                .read_exact_at(&mut offset, mid * 8)
                .map_err(io_err)?;
            let offset = u64::from_le_bytes(offset);
            let mut length = [0; 4];
            self.records
                .read_exact_at(&mut length, offset)
                .map_err(io_err)?;
            let length = u32::from_le_bytes(length) as usize;
            if length > self.max_key_bytes {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "invalid FK baseline key length",
                ));
            }
            buffer.resize(length + 16, 0);
            self.records
                .read_exact_at(&mut buffer, offset + 4)
                .map_err(io_err)?;
            record_spill_bytes_read((12 + buffer.len()) as u64);
            let mut pos = 0;
            let stored = decode_index_key_from(&buffer[..length], &mut pos)?;
            match stored.cmp(key) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid,
                std::cmp::Ordering::Equal => {
                    return Ok((
                        u64::from_le_bytes(buffer[length..length + 8].try_into().unwrap()),
                        u64::from_le_bytes(buffer[length + 8..length + 16].try_into().unwrap()),
                    ))
                }
            }
        }
        Ok((0, 0))
    }
}

impl Database {
    fn build_constraint_fk_baselines(
        &self,
        validation: &mut OnlineConstraintValidation,
        snapshot: TxnView,
        jobs: &[Arc<AlterCopyJob>],
    ) -> Result<(), QueryError> {
        for state in &mut validation.fks {
            let plan = ConstraintBaselinePlan {
                child: &validation.child,
                child_columns: &state.fk.child_col_idx,
                parent: &state.parent,
                parent_columns: &state.parent_columns,
                parent_index: &state.index,
                referenced_columns: &state.fk.referenced_columns,
            };
            let mut inputs = Vec::new();
            for (table, columns, parent) in [
                (plan.child, plan.child_columns, false),
                (plan.parent, plan.parent_columns, true),
            ] {
                for storage in self.scan_storage_ids(table) {
                    inputs.push(ConstraintBaselineInput {
                        table,
                        columns,
                        parent,
                        storage,
                        pages: self.heap_page_count(storage),
                        next: AtomicU64::new(0),
                    });
                }
            }
            let pages: u64 = inputs.iter().map(|input| input.pages).sum();
            let mut degree = constraint_merge_requested_degree().min((pages / 64).max(1) as usize);
            let reservation = loop {
                let bytes = if degree == 1 {
                    12 * 1024 * 1024
                } else {
                    degree * 8 * 1024 * 1024
                };
                if validation.memory.try_grow(bytes) {
                    break Some(bytes);
                }
                if degree == 1 {
                    break None;
                }
                degree = (degree / 2).max(1);
            };
            let Some(reservation) = reservation else {
                continue;
            };
            CONSTRAINT_BASELINE_WORKERS_PEAK.fetch_max(degree as u64, Ordering::Relaxed);
            let result = (|| {
                let sorted = if degree == 1 {
                    vec![plan.scan(self, &inputs, snapshot, jobs, 0, 1)?]
                } else {
                    CONSTRAINT_BASELINE_PARALLEL_RUNS.fetch_add(1, Ordering::Relaxed);
                    let cancellation = crate::engine::cancel::CancellationSnapshot::capture();
                    let budget = crate::engine::executor::mem_budget::current_budget();
                    std::thread::scope(|scope| {
                        let mut handles = Vec::with_capacity(degree);
                        let mut failure = None;
                        for worker in 0..degree {
                            let inputs = &inputs;
                            let cancellation = cancellation.clone();
                            let budget = budget.clone();
                            match std::thread::Builder::new()
                                .name(format!("mydb-fk-snapshot-{worker}"))
                                .stack_size(2 * 1024 * 1024)
                                .spawn_scoped(scope, move || {
                                    let _cancel = cancellation.enter();
                                    let work =
                                        || plan.scan(self, inputs, snapshot, jobs, worker, degree);
                                    match budget {
                                        Some(budget) => {
                                            crate::engine::executor::mem_budget::with_query_budget(
                                                budget, work,
                                            )
                                        }
                                        None => work(),
                                    }
                                }) {
                                Ok(handle) => handles.push(handle),
                                Err(err) => {
                                    failure = Some(error(
                                        crate::error::catalog::ER_UNKNOWN_ERROR,
                                        format!("cannot start FK snapshot worker: {err}"),
                                    ));
                                    break;
                                }
                            }
                        }
                        let mut outputs = Vec::with_capacity(handles.len());
                        for handle in handles {
                            match handle.join() {
                                Ok(Ok(output)) => outputs.push(output),
                                Ok(Err(err)) => {
                                    if failure.is_none() {
                                        failure = Some(err);
                                    }
                                }
                                Err(_) => {
                                    if failure.is_none() {
                                        failure = Some(error(
                                            crate::error::catalog::ER_UNKNOWN_ERROR,
                                            "FK snapshot worker panicked",
                                        ));
                                    }
                                }
                            }
                        }
                        match failure {
                            Some(err) => Err(err),
                            None => Ok(outputs),
                        }
                    })?
                };
                // Do not add a second tournament to the serial path.
                let entries: Box<dyn Iterator<Item = Result<IndexEntry, QueryError>>> =
                    if sorted.len() == 1 {
                        ConstraintBaselineMerge::stream(sorted.into_iter().next().unwrap())?
                    } else {
                        Box::new(ConstraintBaselineMerge::new(sorted)?)
                    };
                ConstraintFkBaseline::build(self, entries, jobs)
            })();
            validation.memory.shrink(reservation);
            let (baseline, orphans) = result?;
            let scratch = (baseline.max_key_bytes + 128)
                .saturating_mul(4)
                .max(state.scratch_bytes);
            validation.memory.grow(scratch - state.scratch_bytes)?;
            state.scratch_bytes = scratch;
            state.baseline = Some(baseline);
            CONSTRAINT_BASELINE_BUILDS.fetch_add(1, Ordering::Relaxed);
            state.orphan_groups = orphans;
        }
        Ok(())
    }
}

struct ConstraintBaselineInput<'a> {
    table: &'a TableSchema,
    columns: &'a [usize],
    parent: bool,
    storage: u64,
    pages: u64,
    next: AtomicU64,
}

#[derive(Clone, Copy)]
struct ConstraintBaselinePlan<'a> {
    child: &'a TableSchema,
    child_columns: &'a [usize],
    parent: &'a TableSchema,
    parent_columns: &'a [usize],
    parent_index: &'a IndexSchema,
    referenced_columns: &'a [String],
}

impl ConstraintBaselinePlan<'_> {
    fn scan(
        &self,
        db: &Database,
        inputs: &[ConstraintBaselineInput<'_>],
        snapshot: TxnView,
        jobs: &[Arc<AlterCopyJob>],
        worker: usize,
        degree: usize,
    ) -> Result<SortedIndexEntries, QueryError> {
        let sort_bytes = if degree == 1 {
            8 * 1024 * 1024
        } else {
            4 * 1024 * 1024
        };
        let mut sorter = IndexEntryExternalSorter::with_exact_run_bytes(sort_bytes);
        // Interleaved identities are globally unique across workers/storage
        // partitions. Sorting must not collapse equal keys from distinct rows.
        let mut ordinal = worker as u64;
        let mut previous_parent = false;
        for input in inputs {
            if input.parent != previous_parent {
                ordinal = worker as u64;
                previous_parent = input.parent;
            }
            loop {
                let start = input.next.fetch_add(32, Ordering::Relaxed);
                if start >= input.pages {
                    break;
                }
                for job in jobs {
                    Database::check_alter_copy_cancelled(job)?;
                }
                crate::engine::cancel::check_cancelled()?;
                let mut scanned = 0;
                db.scan_rows_columns_storage_streaming_range(
                    input.table,
                    input.storage,
                    snapshot,
                    input.columns,
                    start,
                    (start + 32).min(input.pages),
                    |values| {
                        scanned += 1;
                        if values.iter().any(|v| matches!(v, Value::Null)) {
                            return Ok(());
                        }
                        let key = fk_build_key(
                            self.parent,
                            self.parent_index,
                            self.referenced_columns,
                            values,
                        )?;
                        if ordinal >> 15 > u32::MAX as u64 {
                            return Err(error(
                                crate::error::catalog::ER_GET_ERRNO,
                                "FK snapshot row ordinal overflow",
                            ));
                        }
                        let row_id = RowId {
                            page_id: (ordinal >> 15) as u32,
                            slot_id: (ordinal & 0x7fff) as u16
                                | if input.parent { 0x8000 } else { 0 },
                        };
                        ordinal += degree as u64;
                        // Parallelize independent runs, never spawn another
                        // sorting team inside each scan worker.
                        sorter.push_with_sort_degree(
                            db.data_dir(),
                            IndexEntry { key, row_id },
                            Some(1),
                        )
                    },
                )?;
                CONSTRAINT_SCANNED_ROWS.fetch_add(scanned, Ordering::Relaxed);
                if let Some(job) = jobs.iter().find(|job| job.table_id == input.table.id) {
                    Database::advance_alter_copy_job_copied_by(job, scanned);
                }
            }
        }
        sorter.finish_with_degree(db.data_dir(), 1)
    }
}

struct ConstraintBaselineMerge {
    inputs: Vec<Box<dyn Iterator<Item = Result<IndexEntry, QueryError>>>>,
    heads: RunHeadTournament<IndexEntry>,
}
impl ConstraintBaselineMerge {
    fn stream(
        source: SortedIndexEntries,
    ) -> Result<Box<dyn Iterator<Item = Result<IndexEntry, QueryError>>>, QueryError> {
        Ok(match source {
            SortedIndexEntries::Memory(entries) => Box::new(entries.into_iter().map(Ok)),
            SortedIndexEntries::Spilled(runs) => Box::new(runs.into_merger()?),
        })
    }

    fn new(sorted: Vec<SortedIndexEntries>) -> Result<Self, QueryError> {
        let mut inputs: Vec<Box<dyn Iterator<Item = Result<IndexEntry, QueryError>>>> = Vec::new();
        let mut heads = Vec::new();
        for source in sorted {
            let mut input = Self::stream(source)?;
            heads.push(input.next().transpose()?);
            inputs.push(input);
        }
        Ok(Self {
            inputs,
            heads: RunHeadTournament::new(heads),
        })
    }
}
impl Iterator for ConstraintBaselineMerge {
    type Item = Result<IndexEntry, QueryError>;
    fn next(&mut self) -> Option<Self::Item> {
        let winner = self.heads.winner()?;
        let next = match self.inputs[winner].next().transpose() {
            Ok(next) => next,
            Err(err) => return Some(Err(err)),
        };
        self.heads.replace(winner, next).map(Ok)
    }
}

#[cfg(test)]
mod constraint_baseline_tests {
    use super::*;

    #[test]
    fn fk_baseline_lookup_and_overlay_tombstones_survive_spill() {
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::engine::StorageQueryEngine::open(dir.path()).unwrap();
        engine
            .execute("CREATE TABLE p(id INT PRIMARY KEY)")
            .unwrap();
        engine
            .execute("CREATE TABLE c(id INT PRIMARY KEY,pid INT,FOREIGN KEY(pid) REFERENCES p(id))")
            .unwrap();
        let db = engine.database();
        let parent = db.table("test.p").unwrap();
        let fk = db.table("test.c").unwrap().foreign_keys[0].clone();
        let index = parent.indexes[0].clone();
        let key = |v| IndexKey::from(vec![KeyPart::Int(v)]);
        let entries = vec![
            IndexEntry {
                key: key(1),
                row_id: RowId {
                    page_id: 0,
                    slot_id: 0,
                },
            },
            IndexEntry {
                key: key(1),
                row_id: RowId {
                    page_id: 0,
                    slot_id: 1,
                },
            },
            IndexEntry {
                key: key(1),
                row_id: RowId {
                    page_id: 0,
                    slot_id: 0x8000,
                },
            },
            IndexEntry {
                key: key(2),
                row_id: RowId {
                    page_id: 0,
                    slot_id: 2,
                },
            },
            IndexEntry {
                key: key(3),
                row_id: RowId {
                    page_id: 0,
                    slot_id: 0x8001,
                },
            },
        ];
        let (baseline, orphans) =
            ConstraintFkBaseline::build(db, entries.into_iter().map(Ok), &[]).unwrap();
        assert_eq!(orphans, 1);
        for (value, expected) in [
            (0, (0, 0)),
            (1, (2, 1)),
            (2, (1, 0)),
            (3, (0, 1)),
            (4, (0, 0)),
        ] {
            assert_eq!(baseline.get(&key(value)).unwrap(), expected);
        }
        let mut memory = ConstraintValidationMemory::new().unwrap();
        memory.grow(16 * 1024).unwrap();
        let mut state = OnlineFkValidation {
            fk,
            parent,
            index,
            parent_columns: vec![0],
            counts: crate::util::fast_hash::ah_hash_map(),
            bucket_bytes: 0,
            payload_bytes: 0,
            scratch_bytes: 16 * 1024,
            data_dir: dir.path().to_path_buf(),
            spill: None,
            baseline: Some(baseline),
            orphan_groups: orphans,
        };
        // Multiple replay watermarks can delete all versions of a key and
        // later insert it again. Never resurrect its immutable (2,1) counts.
        state.adjust(key(1), false, false, &mut memory).unwrap();
        state.adjust(key(1), false, false, &mut memory).unwrap();
        state.adjust(key(1), true, false, &mut memory).unwrap();
        state.spill(&mut memory).unwrap();
        state.adjust(key(1), false, true, &mut memory).unwrap();
        assert_eq!(state.orphan_groups, 2);
        state.adjust(key(1), true, true, &mut memory).unwrap();
        assert_eq!(state.orphan_groups, 1);
        state.adjust(key(2), true, true, &mut memory).unwrap();
        assert_eq!(state.orphan_groups, 0);
        // Previously untouched parent-only baseline key must seed before
        // the first child insertion, including after overlay spill.
        state.adjust(key(3), false, true, &mut memory).unwrap();
        assert_eq!(state.orphan_groups, 0);
        state.adjust(key(3), true, false, &mut memory).unwrap();
        assert_eq!(state.orphan_groups, 1);
    }
}
