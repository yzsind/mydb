// FULLTEXT backfill shares the encoded run sorter and the ordinary B-tree
// format. Each scan worker owns its tokenizer and bounded sorter: no per-token
// lock or unbounded producer queue is needed. Dictionary records stay packed
// through sorting and page construction; high-cardinality/oversized inputs use
// the same bounded external merge as other encoded indexes.

fn fulltext_build_program(index: &IndexSchema) -> Arc<CompiledIndexKeyProgram> {
    Arc::new(CompiledIndexKeyProgram {
        index: index.clone(),
        direct_parts: None,
        decode_schema: vec![ParamTarget::String, ParamTarget::Bytes],
        fixed_ops: None,
        fixed_signed_widths: None,
        physical_lane: EncodedPhysicalLane::Generic,
    })
}

fn fulltext_build_pending(
    tokenizer: &dyn crate::storage::fulltext::Tokenizer,
    cfg: &crate::storage::fulltext::payload::FtPayloadConfig,
    values: &[Value],
    mut append: impl FnMut(EncodedPendingIndexRecord) -> Result<(), QueryError>,
) -> Result<(), QueryError> {
    use std::borrow::Cow;
    // Owned fallback text must outlive borrowed token slices, including the
    // string representation accepted by the existing key-building oracle.
    let texts = values
        .iter()
        .map(|value| match value {
            Value::String(text) => Some(Cow::Borrowed(text.as_str())),
            Value::SmallString(text) => Some(Cow::Borrowed(
                std::str::from_utf8(text).expect("valid SmallString UTF-8"),
            )),
            Value::Null => None,
            other => Some(Cow::Owned(other.to_display_string())),
        })
        .collect::<Vec<_>>();
    // Inline short position lists avoid one allocation per (term, document).
    let mut positions = hashbrown::HashMap::<
        Cow<'_, str>,
        smallvec::SmallVec<[u32; 2]>,
        ahash::RandomState,
    >::with_capacity_and_hasher(16, ahash::RandomState::new());
    let mut doc_len = 0u64;
    let mut next_pos = 0u64;
    for (column, text) in texts.iter().enumerate() {
        let Some(text) = text else { continue };
        if column > 0 {
            next_pos = next_pos.saturating_add(u64::from(cfg.position_gap));
        }
        let previous_len = doc_len;
        tokenizer.visit_tokens(text, &mut |token| {
            positions.entry(token).or_default().push(next_pos as u32);
            next_pos = next_pos.saturating_add(1);
            doc_len = doc_len.saturating_add(1);
        });
        let last_position =
            (cfg.positions && doc_len != previous_len).then(|| next_pos.saturating_sub(1));
        crate::storage::fulltext::payload::validate_document_extent(doc_len, last_position)?;
    }
    let mut payload = Vec::new();
    for (token, positions) in positions {
        payload.clear();
        crate::storage::fulltext::payload::write_varint(&mut payload, doc_len);
        crate::storage::fulltext::payload::write_varint(&mut payload, positions.len() as u64);
        if cfg.positions {
            let mut previous = 0u32;
            for position in positions {
                crate::storage::fulltext::payload::write_varint(
                    &mut payload,
                    u64::from(position.wrapping_sub(previous)),
                );
                previous = position;
            }
        }
        let mut order_key = smallvec::SmallVec::new();
        for bytes in [token.as_bytes(), payload.as_slice()] {
            order_key.push(NON_NULL_MARKER);
            for &byte in bytes {
                order_key.push(byte);
                if byte == 0 {
                    order_key.push(0xff);
                }
            }
            order_key.extend_from_slice(&VARLEN_TERMINATOR);
        }
        append(EncodedPendingIndexRecord {
            order_key,
            materialized_key: None,
            has_null: false,
        })?;
    }
    Ok(())
}

fn combine_fulltext_sorters(
    mut sorters: Vec<EncodedIndexSorter>,
    data_dir: &Path,
    degree: usize,
    memory_bytes: usize,
) -> Result<EncodedSortedRecords, QueryError> {
    if sorters.len() == 1 {
        return sorters.pop().unwrap().finish(data_dir);
    }
    // Dynamic page scheduling can leave a worker below the normal sampling
    // threshold, or completely idle. Sample its final partial batch rather
    // than forcing all other resident workers to spill because of skew.
    for sorter in &mut sorters {
        if sorter.runs.is_empty() {
            sorter.maybe_promote_variable_dictionary_after(0)?;
        }
    }
    let mut resident_peak = 0usize;
    let resident = sorters.iter().all(|sorter| {
        if let EncodedSortStorage::Interned {
            records,
            dictionary_bytes,
            keys,
            ..
        } = &sorter.storage
        {
            // Source capacities coexist with the exact-size destination during
            // remapping. Include both dictionaries and the id/rank maps.
            resident_peak = resident_peak.saturating_add(
                (records.capacity() + records.len())
                    .saturating_mul(16)
                    .saturating_add(dictionary_bytes.saturating_mul(2))
                    .saturating_add(keys.len().saturating_mul(32)),
            );
        }
        sorter.runs.is_empty() && matches!(&sorter.storage, EncodedSortStorage::Interned { .. })
    }) && resident_peak <= memory_bytes;
    if !resident {
        // Flush concurrently before transferring the runs into one RAII owner.
        // The merger consolidates to a bounded fan-in, regardless of run count.
        std::thread::scope(|scope| -> Result<(), QueryError> {
            let handles = sorters
                .iter_mut()
                .map(|sorter| scope.spawn(move || sorter.flush_run(data_dir)))
                .collect::<Vec<_>>();
            for handle in handles {
                handle.join().map_err(|_| fulltext_build_worker_panic())??;
            }
            Ok(())
        })?;
        let tmp = IndexSortTmpDir::create(data_dir)?;
        let mut runs = Vec::new();
        for sorter in sorters {
            for run in &sorter.runs {
                let destination = tmp.path.join(format!("fulltext-run-{}", runs.len()));
                rename_owned_file(run, &destination).map_err(io_err)?;
                runs.push(destination);
            }
        }
        let runs = consolidate_fulltext_sort_runs(&tmp.path, runs, degree)?;
        return Ok(EncodedSortedRecords::Spilled {
            tmp,
            runs,
            fixed128: None,
        });
    }

    let mut keys = Vec::new();
    let mut lookup = EncodedInternedLookup::with_hasher(ahash::RandomState::new());
    let mut workers = Vec::with_capacity(sorters.len());
    let mut total = 0usize;
    for sorter in sorters {
        let EncodedSortStorage::Interned {
            keys: local,
            records,
            ..
        } = sorter.storage
        else {
            unreachable!()
        };
        let mut mapping = Vec::with_capacity(local.len());
        for key in local {
            mapping.push(EncodedIndexSorter::intern_pending_key(
                &mut keys,
                &mut lookup,
                EncodedPendingIndexRecord {
                    order_key: key.order_key.into(),
                    materialized_key: key.materialized_key,
                    has_null: key.has_null,
                },
            )?);
        }
        total = total.checked_add(records.len()).ok_or_else(|| {
            error(
                crate::error::catalog::ER_OUTOFMEMORY,
                "FULLTEXT posting count overflow",
            )
        })?;
        workers.push((records, mapping));
    }
    drop(lookup);
    let mut order = (0..keys.len()).collect::<Vec<_>>();
    order.sort_unstable_by(|left, right| keys[*left].order_key.cmp(&keys[*right].order_key));
    let mut ranks = vec![0u32; keys.len()];
    for (rank, id) in order.into_iter().enumerate() {
        ranks[id] = rank as u32;
    }
    let mut records = vec![0u128; total];
    std::thread::scope(|scope| {
        let mut remaining = records.as_mut_slice();
        let mut handles = Vec::with_capacity(workers.len());
        for (local, mapping) in workers {
            let (target, tail) = remaining.split_at_mut(local.len());
            remaining = tail;
            let ranks = &ranks;
            handles.push(scope.spawn(move || {
                for (target, record) in target.iter_mut().zip(local) {
                    let id = mapping[interned_record_key_id(record) as usize];
                    *target = pack_interned_record(
                        ranks[id as usize],
                        id,
                        interned_record_row_id(record),
                    );
                }
            }));
        }
        for handle in handles {
            handle.join().map_err(|_| fulltext_build_worker_panic())?;
        }
        Ok::<_, QueryError>(())
    })?;
    parallel_sort_fixed128_in_place(&mut records, degree);
    Ok(EncodedSortedRecords::Interned { keys, records })
}

fn fulltext_build_worker_panic() -> QueryError {
    error(
        crate::error::catalog::ER_UNKNOWN_ERROR,
        "FULLTEXT index-build worker panicked",
    )
}

fn consolidate_fulltext_sort_runs(
    tmp: &Path,
    mut runs: Vec<PathBuf>,
    degree: usize,
) -> Result<Vec<PathBuf>, QueryError> {
    // Divide descriptor headroom across workers. Each group owns at most
    // sixteen 64 KiB readers and one writer, covered by worker scratch.
    let fan_in = (index_merge_fan_in() / degree.max(1)).clamp(2, 16);
    let cancel = crate::engine::cancel::CancellationSnapshot::capture();
    let mut pass = 0usize;
    while runs.len() > 16 {
        let groups = runs.chunks(fan_in).collect::<Vec<_>>();
        let outputs = (0..groups.len())
            .map(|group| tmp.join(format!("fulltext-merge-{pass}-{group}")))
            .collect::<Vec<_>>();
        let next = AtomicUsize::new(0);
        std::thread::scope(|scope| -> Result<(), QueryError> {
            let mut handles = Vec::new();
            for _ in 0..degree.min(groups.len()).max(1) {
                let groups = &groups;
                let outputs = &outputs;
                let next = &next;
                let cancel = cancel.clone();
                handles.push(scope.spawn(move || -> Result<(), QueryError> {
                    let _worker = enter_index_build_worker();
                    let _cancel = cancel.enter();
                    loop {
                        crate::engine::cancel::check_cancelled()?;
                        let group = next.fetch_add(1, Ordering::Relaxed);
                        let Some(paths) = groups.get(group) else {
                            return Ok(());
                        };
                        merge_sort_run_group::<EncodedIndexRunCodec>(paths, &outputs[group])?;
                    }
                }));
            }
            for handle in handles {
                handle.join().map_err(|_| fulltext_build_worker_panic())??;
            }
            Ok(())
        })?;
        runs = outputs;
        pass += 1;
    }
    Ok(runs)
}

impl Database {
    fn rebuild_fulltext_index_parallel(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        options: IndexBuildOptions,
    ) -> Result<(), QueryError> {
        use crate::engine::executor::mem_budget::{
            ControlledMemoryCategory, ControlledMemoryCharge,
        };
        const WORKER_SCRATCH: usize = 4 * 1024 * 1024;
        let columns = index
            .columns
            .iter()
            .map(|name| table.column_index(name))
            .collect::<Result<Vec<_>, _>>()?;
        let page_count = self.with_heap_read(table.id, |heap| Ok(heap.page_count_relaxed()))?;
        let requested = options
            .effective_degree
            .max(1)
            .min(page_count.max(1) as usize);
        let task_pages = encoded_scan_task_pages(page_count, requested);
        let requested = requested.min(page_count.div_ceil(task_pages).max(1) as usize);
        let program = fulltext_build_program(index);
        let requested_bytes =
            crate::engine::executor::mem_budget::query_mem_limit_bytes().max(16 * 1024 * 1024);
        let (_lease, _charge, bytes) =
            reserve_offline_index_build_scratch(self, requested_bytes, 16 * 1024 * 1024)?;
        INDEX_BUILD_SCRATCH_GRANTED_PEAK.fetch_max(bytes as u64, Ordering::Relaxed);
        // Reduce the worker count before dividing the admitted envelope. Each
        // worker needs scan/document scratch and at least a 4 MiB sort run.
        let degree = requested.min((bytes / (WORKER_SCRATCH + 4 * 1024 * 1024)).max(1));
        let budget = (bytes - degree * WORKER_SCRATCH) / degree;
        let next_page = AtomicU64::new(0);
        let task_pages = encoded_scan_task_pages(page_count, degree);
        let cancel = crate::engine::cancel::CancellationSnapshot::capture();
        let default_meta = crate::storage::schema::FulltextMeta::default();
        let meta = index.fulltext.as_ref().unwrap_or(&default_meta);
        let cfg = crate::storage::fulltext::payload::config_from_meta(meta);
        // Long ngrams can retain quadratically many token bytes in one row.
        // Reserve their worst-case expansion before invoking the tokenizer.
        let document_factor = if meta.parser == "ngram" {
            let n = meta
                .ngram_size
                .map(|n| n as usize)
                .unwrap_or_else(|| crate::storage::fulltext::tokenizer::global_config().ngram_size);
            256usize.saturating_add(n.saturating_mul(8))
        } else {
            256
        };
        let started = Instant::now();
        let outputs = self.with_heap_read(table.id, |heap| {
            std::thread::scope(
                |scope| -> Result<Vec<(EncodedIndexSorter, u64)>, QueryError> {
                    let mut handles = Vec::with_capacity(degree);
                    for worker in 0..degree {
                        let next_page = &next_page;
                        let columns = &columns;
                        let program = program.clone();
                        let cancel = cancel.clone();
                        let cfg = &cfg;
                        handles.push(
                            std::thread::Builder::new()
                                .name(format!("mydb-fulltext-scan-{worker}"))
                                .spawn_scoped(scope, move || -> Result<_, QueryError> {
                                    let _worker = enter_index_build_worker();
                                    let _cancel = cancel.enter();
                                    // LOB readers are thread-local; the parent's
                                    // with_heap_read scope does not reach workers.
                                    heap.with_lob(|| {
                                        let tokenizer =
                                            crate::storage::fulltext::build_tokenizer_from_meta(
                                                meta,
                                            );
                                        let mut sorter = EncodedIndexSorter::new(program, budget)
                                            .with_sort_degree(1);
                                        let mut rows = 0u64;
                                        loop {
                                            crate::engine::cancel::check_cancelled()?;
                                            let start =
                                                next_page.fetch_add(task_pages, Ordering::Relaxed);
                                            if start >= page_count {
                                                break;
                                            }
                                            heap.scan_mvcc_columns_direct_range_impl(
                                                columns,
                                                start,
                                                start.saturating_add(task_pages).min(page_count),
                                                |row| {
                                                    if row.header.is_deleted() {
                                                        return Ok(false);
                                                    }
                                                    let document_bytes = row
                                                        .values
                                                        .iter()
                                                        .map(|value| match value {
                                                            Value::String(text) => text.len(),
                                                            Value::SmallString(text) => text.len(),
                                                            _ => 64,
                                                        })
                                                        .sum::<usize>()
                                                        .saturating_mul(document_factor);
                                                    let extra = document_bytes
                                                        .saturating_sub(WORKER_SCRATCH / 2);
                                                    let _wide = if extra > 0 {
                                                        Some(ControlledMemoryCharge::try_new(
                                                        ControlledMemoryCategory::StorageScratch,
                                                        extra,
                                                    )?)
                                                    } else {
                                                        None
                                                    };
                                                    fulltext_build_pending(
                                                        tokenizer.as_ref(),
                                                        cfg,
                                                        row.values,
                                                        |pending| {
                                                            sorter.push(
                                                                self.data_dir(),
                                                                pending,
                                                                row.row_id,
                                                            )
                                                        },
                                                    )?;
                                                    rows += 1;
                                                    Ok(false)
                                                },
                                            )?;
                                        }
                                        Ok((sorter, rows))
                                    })
                                })
                                .map_err(|err| {
                                    error(
                                        crate::error::catalog::ER_GET_ERRNO,
                                        format!("failed to spawn FULLTEXT scan worker: {err}"),
                                    )
                                })?,
                        );
                    }
                    let mut output = Vec::with_capacity(degree);
                    for handle in handles {
                        output.push(handle.join().map_err(|_| fulltext_build_worker_panic())??);
                    }
                    Ok(output)
                },
            )
        })?;
        INDEX_BUILD_SCAN_US.fetch_add(started.elapsed().as_micros() as u64, Ordering::Relaxed);
        INDEX_BUILD_ROWS.fetch_add(
            outputs.iter().map(|(_, rows)| rows).sum(),
            Ordering::Relaxed,
        );
        INDEX_BUILD_HEAP_SCANS.fetch_add(1, Ordering::Relaxed);
        let sort_started = Instant::now();
        let sorted = combine_fulltext_sorters(
            outputs.into_iter().map(|(sorter, _)| sorter).collect(),
            self.data_dir(),
            degree,
            bytes - degree * WORKER_SCRATCH,
        )?;
        INDEX_BUILD_SORT_US.fetch_add(sort_started.elapsed().as_micros() as u64, Ordering::Relaxed);
        self.remove_index_cache_entry(table.id, &index.name)?;
        let path = self.index_path_for_table_schema(table, table.id, &index.name);
        self.buffer_pool
            .discard_file(&Arc::new(BufferPool::index_file_key(path.clone())))?;
        let file = create_rebuilt_from_encoded_sorted_with_degree(
            self, path, index, program, sorted, &mut None, degree,
        )?;
        file.flush()?;
        self.remove_index_cache_entry(table.id, &index.name)?;
        INDEX_BUILD_REQUESTED_WORKERS.fetch_add(options.requested_degree as u64, Ordering::Relaxed);
        INDEX_BUILD_EFFECTIVE_WORKERS.fetch_add(degree as u64, Ordering::Relaxed);
        if degree > 1 {
            INDEX_BUILD_PARALLEL_BUILDS.fetch_add(1, Ordering::Relaxed);
        } else {
            INDEX_BUILD_SERIAL_BUILDS.fetch_add(1, Ordering::Relaxed);
        }
        if degree < options.requested_degree {
            INDEX_BUILD_DEGREE_DOWNGRADES.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }
}

#[cfg(test)]
mod fulltext_build_tests {
    use super::*;
    use crate::engine::executor::StorageQueryEngine;

    #[test]
    fn fulltext_parallel_build_reads_external_documents_and_reopens() {
        for durability in [DurabilityMode::Relaxed, DurabilityMode::Strict] {
            let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
            let mut config = StorageConfig::from_env();
            config.durability = durability;
            config.buffer_pool_pages = 512;
            let open = || {
                StorageQueryEngine::open_with_storage_config_without_background_workers(
                    dir.path(),
                    config.clone(),
                )
                .unwrap()
            };
            let engine = open();
            engine
                .execute("CREATE TABLE lob_docs(id INT PRIMARY KEY,body TEXT)")
                .unwrap();
            let body = format!("alpha {} omega", "padding ".repeat(3000));
            let rows = (0..64)
                .map(|id| format!("({id},'{body}')"))
                .collect::<Vec<_>>()
                .join(",");
            engine
                .execute(&format!("INSERT INTO lob_docs VALUES {rows}"))
                .unwrap();
            engine
                .execute("CREATE /*+ PARALLEL(4) */ FULLTEXT INDEX ft ON lob_docs(body)")
                .unwrap();
            let verify = |engine: &StorageQueryEngine| {
                let crate::engine::api::QueryResponse::ResultSet(result) = engine
                    .execute("SELECT id FROM lob_docs WHERE MATCH(body) AGAINST('\"alpha padding\"' IN BOOLEAN MODE) ORDER BY id")
                    .unwrap()
                    .materialize()
                    .unwrap()
                else {
                    panic!("expected full-text results");
                };
                assert_eq!(
                    result.rows,
                    (0..64).map(|id| vec![Value::Int(id)]).collect::<Vec<_>>()
                );
            };
            verify(&engine);
            drop(engine);
            verify(&open());
        }
    }

    fn fixture() -> (tempfile::TempDir, StorageQueryEngine) {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let mut config = StorageConfig::from_env();
        config.durability = DurabilityMode::Relaxed;
        config.buffer_pool_pages = 512;
        let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
            dir.path(),
            config,
        )
        .unwrap();
        engine
            .execute("CREATE TABLE ft_fixture(a TEXT,b TEXT,FULLTEXT KEY ft(a,b))")
            .unwrap();
        (dir, engine)
    }

    #[test]
    fn fulltext_encoded_tokens_match_canonical_keys_for_metadata_and_column_gaps() {
        let (_dir, engine) = fixture();
        let table = engine.database().table("ft_fixture").unwrap();
        for parser in ["", "ngram"] {
            for stored_positions in [false, true] {
                let mut index = table
                    .indexes
                    .iter()
                    .find(|index| index.is_fulltext())
                    .unwrap()
                    .clone();
                let meta = index.fulltext.as_mut().unwrap();
                meta.parser = parser.into();
                meta.positions = Some(stored_positions);
                meta.position_gap = Some(137);
                meta.ngram_size = Some(2);
                let tokenizer = crate::storage::fulltext::build_tokenizer_from_meta(meta);
                let cfg = crate::storage::fulltext::payload::config_from_meta(meta);
                let program = fulltext_build_program(&index);
                for values in [
                    vec![
                        Value::String("Alpha alpha_beta THE beta ALPHA".into()),
                        Value::String("beta gamma".into()),
                    ],
                    vec![
                        Value::Null,
                        Value::String("ÉCOLE İSTANBUL 数据库全文检索 全文，索引".into()),
                    ],
                    vec![Value::String("alpha".into()), Value::Null],
                    vec![
                        Value::String(String::new()),
                        Value::String("alpha beta".into()),
                    ],
                    vec![Value::String("a".repeat(90)), Value::String("a b c".into())],
                    vec![Value::Int(123456), Value::String("123456".into())],
                ] {
                    let mut actual = Vec::new();
                    fulltext_build_pending(tokenizer.as_ref(), &cfg, &values, |pending| {
                        actual.push(
                            program.materialize_key(
                                &program.attach_row_id(pending, RowId::new(1, 2)),
                            )?,
                        );
                        Ok(())
                    })
                    .unwrap();
                    let mut expected = table.fulltext_index_keys(&index, &values).unwrap();
                    actual.sort();
                    expected.sort();
                    assert_eq!(actual, expected, "{parser}/{stored_positions}: {values:?}");
                }
            }
        }
    }

    #[test]
    fn fulltext_encoded_position_limits_match_dml_before_emitting_records() {
        let (_dir, engine) = fixture();
        let table = engine.database().table("ft_fixture").unwrap();
        for stored_positions in [false, true] {
            for gap in [u32::MAX - 1, u32::MAX] {
                let mut index = table
                    .indexes
                    .iter()
                    .find(|index| index.is_fulltext())
                    .unwrap()
                    .clone();
                let meta = index.fulltext.as_mut().unwrap();
                meta.position_gap = Some(gap);
                meta.positions = Some(stored_positions);
                let tokenizer = crate::storage::fulltext::build_tokenizer_from_meta(meta);
                let cfg = crate::storage::fulltext::payload::config_from_meta(meta);
                let program = fulltext_build_program(&index);
                for text in ["beta", "beta beta"] {
                    let values = vec![Value::String("alpha".into()), Value::String(text.into())];
                    let mut actual = Vec::new();
                    let result =
                        fulltext_build_pending(tokenizer.as_ref(), &cfg, &values, |pending| {
                            actual.push(program.materialize_key(
                                &program.attach_row_id(pending, RowId::new(1, 2)),
                            )?);
                            Ok(())
                        });
                    let expected = table.fulltext_index_keys(&index, &values);
                    if stored_positions && (gap == u32::MAX || text == "beta beta") {
                        assert_eq!(result.unwrap_err().code(), 1690);
                        assert_eq!(expected.unwrap_err().code(), 1690);
                        assert!(actual.is_empty());
                    } else {
                        result.unwrap();
                        let mut expected = expected.unwrap();
                        actual.sort();
                        expected.sort();
                        assert_eq!(actual, expected);
                    }
                }
            }
        }
    }

    #[test]
    fn fulltext_skewed_and_idle_scan_workers_keep_partial_dictionary_runs_resident() {
        let (dir, engine) = fixture();
        let table = engine.database().table("ft_fixture").unwrap();
        let index = table
            .indexes
            .iter()
            .find(|index| index.is_fulltext())
            .unwrap();
        let program = fulltext_build_program(index);
        let budget = 16 * 1024 * 1024;
        let mut sorters = (0..3)
            .map(|_| EncodedIndexSorter::new(program.clone(), budget))
            .collect::<Vec<_>>();
        let key = IndexKey::from(vec![
            KeyPart::String("alpha".into()),
            KeyPart::Bytes(vec![1, 1, 0]),
        ]);
        for i in 0..70_001u32 {
            let worker = usize::from(i == 70_000);
            sorters[worker]
                .push(
                    dir.path(),
                    program.encode_key(&key).unwrap(),
                    RowId::new(i, 0),
                )
                .unwrap();
        }
        assert!(sorters.iter().all(|sorter| sorter.runs.is_empty()));
        let sorted = combine_fulltext_sorters(sorters, dir.path(), 3, budget * 3).unwrap();
        assert!(matches!(&sorted, EncodedSortedRecords::Interned { .. }));
        let rows = sorted
            .into_record_iter(program)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(rows.len(), 70_001);
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(row.row_id, RowId::new(i as u32, 0));
        }
    }

    #[test]
    fn fulltext_parallel_resident_and_multipass_spill_preserve_entries_and_reopen() {
        let (dir, engine) = fixture();
        let db = engine.database();
        let table = db.table("ft_fixture").unwrap();
        let index = table
            .indexes
            .iter()
            .find(|index| index.is_fulltext())
            .unwrap();
        let program = fulltext_build_program(index);
        for budget in [1024 * 1024, 16 * 1024 * 1024] {
            let mut sorters = (0..2)
                .map(|_| EncodedIndexSorter::new(program.clone(), budget).with_sort_degree(1))
                .collect::<Vec<_>>();
            let mut expected = Vec::new();
            for i in (0..270_000u32).rev() {
                let key = IndexKey::from(vec![
                    KeyPart::String(format!("term{:03}", i % 97)),
                    KeyPart::Bytes(vec![3, 1, (i % 3) as u8]),
                ]);
                let row_id = RowId::new(i / 101 + 1, (i % 101) as u16);
                sorters[i as usize % 2]
                    .push(dir.path(), program.encode_key(&key).unwrap(), row_id)
                    .unwrap();
                expected.push((key, row_id));
            }
            expected.sort();
            let sorted = combine_fulltext_sorters(sorters, dir.path(), 2, budget * 2).unwrap();
            assert_eq!(
                matches!(&sorted, EncodedSortedRecords::Spilled { .. }),
                budget == 1024 * 1024
            );
            let path = dir.path().join(format!("ft-{budget}.idx"));
            let file = create_rebuilt_from_encoded_sorted_with_degree(
                db,
                path.clone(),
                index,
                program.clone(),
                sorted,
                &mut None,
                2,
            )
            .unwrap();
            assert_eq!(
                file.range_entries_chunk(None, None, None, 300_000).unwrap(),
                expected
            );
            drop(file);
            db.buffer_pool
                .discard_file(&Arc::new(BufferPool::index_file_key(path.clone())))
                .unwrap();
            let reopened = IndexFile::open_with_durability(
                path,
                db.log.clone(),
                DurabilityMode::Relaxed,
                db.buffer_pool.clone(),
            )
            .unwrap();
            assert_eq!(
                reopened
                    .range_entries_chunk(None, None, None, 300_000)
                    .unwrap(),
                expected
            );
        }
    }
}
