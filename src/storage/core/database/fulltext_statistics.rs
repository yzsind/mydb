// Exact scoring counters are ordinary MVCC rows. Their WAL, undo, savepoints,
// XA and physical replication therefore share the data transaction's decision.
// Signed transaction shards avoid a single corpus-wide write hotspot: a delete
// may debit a different shard from its insert, while snapshot sums stay exact.
const FULLTEXT_COUNTER_SHARDS: u64 = 64;

fn fulltext_counter_write_latches() -> &'static [parking_lot::Mutex<()>; 64] {
    static LATCHES: std::sync::OnceLock<[parking_lot::Mutex<()>; 64]> = std::sync::OnceLock::new();
    LATCHES.get_or_init(|| std::array::from_fn(|_| parking_lot::Mutex::new(())))
}

pub(crate) struct FulltextCorpusStatistics {
    pub(crate) documents: u64,
    pub(crate) total_tokens: u64,
    pub(crate) frequencies: HashMap<String, u64>,
    #[cfg(test)]
    pub(crate) counter_rows_read: u64,
}

fn fulltext_statistics_error(message: impl std::fmt::Display) -> QueryError {
    error(
        crate::error::catalog::ER_GET_ERRNO,
        format!("FULLTEXT statistics: {message}"),
    )
}

fn fulltext_corpus_definition(
    table: &TableSchema,
    index: &IndexSchema,
) -> Result<Vec<u8>, QueryError> {
    let mut columns = index
        .columns
        .iter()
        .map(|name| table.column_index(name).map(|p| table.columns[p].column_id))
        .collect::<Result<Vec<_>, _>>()?;
    columns.sort_unstable();
    let mut meta = index.fulltext.clone().unwrap_or_default();
    meta.corpus_id = 0;
    meta.snapshot_origin_txn_id = 0;
    serde_json::to_vec(&(columns, meta)).map_err(fulltext_statistics_error)
}

fn fulltext_document_statistics(
    table: &TableSchema,
    index: &IndexSchema,
    values: &[Value],
) -> Result<(i64, std::collections::BTreeSet<String>), QueryError> {
    use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};
    let meta = index
        .fulltext
        .as_ref()
        .ok_or_else(|| fulltext_statistics_error("missing tokenizer"))?;
    let tokenizer = crate::storage::fulltext::build_tokenizer_from_meta(meta);
    let mut length = 0i64;
    let mut terms = std::collections::BTreeSet::new();
    let mut charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
    for name in &index.columns {
        let position = table.column_index(name)?;
        let text: std::borrow::Cow<'_, str> = match values.get(position) {
            None | Some(Value::Null) => continue,
            Some(Value::String(s)) => std::borrow::Cow::Borrowed(s),
            Some(Value::SmallString(s)) => std::borrow::Cow::Borrowed(
                std::str::from_utf8(s).map_err(fulltext_statistics_error)?,
            ),
            Some(v) => std::borrow::Cow::Owned(v.to_display_string()),
        };
        let _scratch = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::StorageScratch,
            text.len().saturating_mul(32).saturating_add(256),
        )?;
        for token in tokenizer.tokenize(&text) {
            length = length
                .checked_add(1)
                .ok_or_else(|| fulltext_statistics_error("document length overflow"))?;
            if !terms.contains(&token) {
                charge.add_non_spillable(96usize.saturating_add(token.len()))?;
                terms.insert(token);
            }
        }
    }
    Ok((length, terms))
}

fn fulltext_temp_counter_key(owner: u64) -> String {
    table_key("__mydb_temp", &format!("ft_counters_{owner}"))
}

impl Database {
    #[cfg(test)]
    pub(crate) fn install_fulltext_copy_statistics_test_hook(&self) -> Arc<DdlBackfillTestHook> {
        let hook = Arc::new(DdlBackfillTestHook::new());
        *self.fulltext_copy_statistics_test_hook.lock().unwrap() = Some(hook.clone());
        hook
    }

    fn fulltext_counter_table(&self, owner: u64) -> Result<Arc<TableSchema>, QueryError> {
        let schema = crate::storage::data_dictionary::dd_table_schema(
            crate::storage::data_dictionary::DD_FULLTEXT_COUNTERS,
        )
        .expect("FULLTEXT counter schema");
        if !is_temp_table_id(owner) {
            return Ok(schema);
        }
        let key = fulltext_temp_counter_key(owner);
        let mut tables = self
            .temp_tables
            .write()
            .map_err(|_| fulltext_statistics_error("temporary table lock poisoned"))?;
        if let Some(schema) = tables.get(&key) {
            return Ok(schema.clone());
        }
        let id = self.next_temp_table_id.fetch_add(1, Ordering::AcqRel);
        let registration =
            crate::storage::temp_quota::register_auxiliary(&self.data_dir, id, owner)?;
        let mut schema = schema.as_ref().clone();
        schema.id = id;
        schema.object_id = id;
        schema.database = "__mydb_temp".into();
        schema.name = format!("ft_counters_{owner}");
        let schema = Arc::new(schema);
        // Temporary paths depend only on the physical id; initialize before
        // exposing the descriptor to another worker of the same transaction.
        let path = self.table_path(id);
        if let Err(err) = self.create_temp_empty_file(path.clone()) {
            let _ = self.remove_temp_file(path);
            return Err(err);
        }
        tables.insert(key, schema.clone());
        registration.commit();
        Ok(schema)
    }

    fn row_derivatives_undo_head(
        &self,
        txn_id: u64,
        table: &TableSchema,
    ) -> Result<Option<u64>, QueryError> {
        if !table
            .indexes
            .iter()
            .any(|index| index.is_vector() || index.is_fulltext())
        {
            return Ok(None);
        }
        self.txns_read()?.undo_head(txn_id)
    }

    fn maintain_row_delete(
        &self,
        txn_id: u64,
        undo_id: u64,
        table: &TableSchema,
        old_values: &[Value],
        row_id: RowId,
        storage_id: u64,
    ) -> Result<(), QueryError> {
        self.maintain_fulltext_statistics(
            txn_id,
            table,
            storage_id,
            std::iter::once((Some(old_values), None)),
        )?;
        self.defer_vector_delete(txn_id, undo_id, table, old_values, row_id, storage_id)
    }

    fn maintain_row_update(
        &self,
        txn_id: u64,
        undo_id: u64,
        table: &TableSchema,
        row_id: RowId,
        storage_id: u64,
        old_values: Option<&[Value]>,
        new_values: &[Value],
    ) -> Result<(), QueryError> {
        self.maintain_fulltext_statistics(
            txn_id,
            table,
            storage_id,
            std::iter::once((old_values, Some(new_values))),
        )?;
        self.defer_vector_update(
            txn_id, undo_id, table, row_id, storage_id, old_values, new_values,
        )
    }

    fn maintain_row_insert_batch(
        &self,
        txn_id: u64,
        table: &TableSchema,
        storage_id: u64,
        rows: &[Vec<Value>],
        row_ids: &[RowId],
    ) -> Result<(), QueryError> {
        self.maintain_fulltext_statistics(
            txn_id,
            table,
            storage_id,
            rows.iter().map(|row| (None, Some(row.as_slice()))),
        )?;
        self.defer_vector_insert_batch(txn_id, table, storage_id, rows, row_ids)
    }

    fn fulltext_counter_prefix(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        storage_id: u64,
    ) -> Result<Vec<Value>, QueryError> {
        let corpus_id = index.fulltext.as_ref().map_or(0, |meta| meta.corpus_id);
        if corpus_id == 0 {
            return Err(fulltext_statistics_error("missing corpus identity"));
        }
        Ok(vec![
            Value::UInt(table.id),
            Value::UInt(table.active_generation),
            Value::UInt(corpus_id),
            Value::Bytes(fulltext_corpus_definition(table, index)?),
            Value::UInt(storage_id),
        ])
    }

    fn apply_fulltext_counter_deltas(
        &self,
        txn_id: u64,
        prefix: &[Value],
        deltas: std::collections::BTreeMap<String, (i64, i64)>,
    ) -> Result<(), QueryError> {
        if deltas
            .values()
            .all(|&(count, length)| count == 0 && length == 0)
        {
            return Ok(());
        }
        // Transaction locks are reentrant across workers of the same
        // transaction. Serialize its counter RMW even on parallel storage
        // entry points; different transaction shards remain independent.
        let latch = ((self as *const Database as usize >> 6) ^ txn_id as usize) & 63;
        let _writer = fulltext_counter_write_latches()[latch].lock();
        let Some(Value::UInt(owner)) = prefix.first() else {
            return Err(fulltext_statistics_error("invalid corpus owner"));
        };
        let counters = self.fulltext_counter_table(*owner)?;
        let primary = &counters.indexes[0];
        let txn = self.session_statement_view(txn_id, IsolationLevel::ReadCommitted, None);
        let row = |token: &str, count, length| {
            let mut row = prefix.to_vec();
            row.extend([
                Value::Bytes(token.as_bytes().to_vec()),
                Value::UInt(txn_id % FULLTEXT_COUNTER_SHARDS),
                Value::Int(count),
                Value::Int(length),
            ]);
            row
        };
        // Reserve the shard even if N and total length are unchanged. This
        // also serializes first insertion of a previously unseen term.
        let header = row("", 0, 0);
        let header_key = counters.index_key(primary, &header)?;
        self.lock_unique_keys_nonblocking(txn_id, vec![(counters.id, "PRIMARY", header_key)])?;
        let _suppress_binlog = crate::binlog::SessionSqlLogBinGuard::new(false);
        for (token, (count, length)) in deltas {
            if count == 0 && length == 0 {
                continue;
            }
            let mut values = row(&token, count, length);
            let key = counters.index_key(primary, &values)?;
            let ids = self.lookup_index(&counters, primary, key)?;
            if ids.len() > 1 {
                return Err(fulltext_statistics_error("duplicate counter"));
            }
            if let Some(&id) = ids.first() {
                self.lock_row_nonblocking(txn_id, counters.id, id)?;
                let old = self
                    .read_mvcc_row(counters.id, id)?
                    .ok_or_else(|| fulltext_statistics_error("missing counter row"))?;
                if old.header.is_deleted() {
                    return Err(fulltext_statistics_error("deleted counter row"));
                }
                for (pos, delta) in [(7, count), (8, length)] {
                    let Some(Value::Int(previous)) = old.values.get(pos) else {
                        return Err(fulltext_statistics_error("invalid counter type"));
                    };
                    values[pos] = Value::Int(
                        previous
                            .checked_add(delta)
                            .ok_or_else(|| fulltext_statistics_error("counter overflow"))?,
                    );
                }
                self.update_row_in_place(
                    txn,
                    &counters,
                    counters.id,
                    id,
                    old.header,
                    &old.values,
                    values,
                    Some(&[7, 8]),
                )?;
            } else {
                self.insert_row_in_table(txn, &counters, values)?;
            }
        }
        Ok(())
    }

    fn maintain_fulltext_statistics<'a>(
        &self,
        txn_id: u64,
        table: &TableSchema,
        storage_id: u64,
        changes: impl Clone + Iterator<Item = (Option<&'a [Value]>, Option<&'a [Value]>)>,
    ) -> Result<(), QueryError> {
        let mut corpora = std::collections::BTreeSet::new();
        for index in table.indexes.iter().filter(|index| index.is_fulltext()) {
            let prefix = self.fulltext_counter_prefix(table, index, storage_id)?;
            let definition = fulltext_corpus_definition(table, index)?;
            let corpus_id = index.fulltext.as_ref().unwrap().corpus_id;
            if !corpora.insert((corpus_id, definition)) {
                continue;
            }
            let mut deltas = std::collections::BTreeMap::<String, (i64, i64)>::new();
            let mut charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
            for (old, new) in changes.clone() {
                if let (Some(old), Some(new)) = (old, new) {
                    if index.columns.iter().all(|name| {
                        table
                            .column_index(name)
                            .is_ok_and(|p| old.get(p) == new.get(p))
                    }) {
                        continue;
                    }
                }
                for (values, sign) in [(old, -1i64), (new, 1i64)] {
                    let Some(values) = values else { continue };
                    let (length, terms) = fulltext_document_statistics(table, index, values)?;
                    let header = deltas.entry(String::new()).or_default();
                    header.0 = header
                        .0
                        .checked_add(sign)
                        .ok_or_else(|| fulltext_statistics_error("delta overflow"))?;
                    header.1 = header
                        .1
                        .checked_add(sign * length)
                        .ok_or_else(|| fulltext_statistics_error("length delta overflow"))?;
                    for token in terms {
                        if !deltas.contains_key(&token) {
                            charge.add_non_spillable(112usize.saturating_add(token.len()))?;
                        }
                        let count = &mut deltas.entry(token).or_default().0;
                        *count = count
                            .checked_add(sign)
                            .ok_or_else(|| fulltext_statistics_error("frequency delta overflow"))?;
                    }
                }
            }
            self.apply_fulltext_counter_deltas(txn_id, &prefix, deltas)?;
        }
        Ok(())
    }

    pub(crate) fn fulltext_index_covers_snapshot(
        &self,
        index: &IndexSchema,
        txn: TxnView,
    ) -> Result<bool, QueryError> {
        let origin = index
            .fulltext
            .as_ref()
            .map_or(0, |meta| meta.snapshot_origin_txn_id);
        if origin == 0 {
            return Ok(true);
        }
        if origin == u64::MAX {
            return Ok(false);
        }
        self.row_version_visible(RowHeader::visible(origin), txn)
    }

    pub(crate) fn fulltext_statistics_undo_head(
        &self,
        txn_id: u64,
    ) -> Result<Option<u64>, QueryError> {
        if txn_id == 0 {
            return Ok(None);
        }
        let txns = self.txns_read()?;
        Ok(txns
            .slot(txn_id)
            .filter(|slot| slot.status.is_in_flight())
            .and_then(|slot| slot.undo_head.get()))
    }

    fn fulltext_owned_rows(
        &self,
        txn: TxnView,
        storage_ids: &[u64],
        charge: &mut crate::engine::executor::mem_budget::ScopedCharge,
    ) -> Result<std::collections::BTreeSet<(u64, RowId)>, QueryError> {
        let mut rows = std::collections::BTreeSet::new();
        if txn.dirty_read {
            return Ok(rows);
        }
        let mut ptr = self.fulltext_statistics_undo_head(txn.txn_id)?;
        let mut previous_depth = None;
        while let Some(id) = ptr {
            crate::engine::cancel::check_cancelled()?;
            let metadata = self
                .undo
                .read()
                .row_metadata(id)
                .ok_or_else(|| undo_corruption(id, "missing FULLTEXT transaction metadata"))?;
            if metadata.txn_id != txn.txn_id
                || metadata.chain_depth == 0
                || previous_depth.is_some_and(|depth| metadata.chain_depth >= depth)
            {
                return Err(undo_corruption(id, "invalid FULLTEXT transaction chain"));
            }
            previous_depth = Some(metadata.chain_depth);
            ptr = metadata.prev_undo_ptr;
            if !storage_ids.contains(&metadata.table_id) {
                continue;
            }
            let mut insert = |row_id| -> Result<(), QueryError> {
                let identity = (metadata.table_id, row_id);
                if !rows.contains(&identity) {
                    charge.add_non_spillable(64)?;
                    rows.insert(identity);
                }
                Ok(())
            };
            if metadata.insert_batch {
                // Only INSERT batches need their row-id vector. UPDATE and
                // DELETE traversal never pages in potentially large text.
                let record = self
                    .load_undo_owned(id)?
                    .ok_or_else(|| undo_corruption(id, "missing FULLTEXT insert batch"))?;
                let UndoKind::InsertBatch { row_ids } = record.kind else {
                    return Err(undo_corruption(id, "invalid FULLTEXT insert batch"));
                };
                for row_id in row_ids {
                    insert(row_id)?;
                }
            } else {
                insert(metadata.row_id)?;
            }
        }
        Ok(rows)
    }

    /// Point/range probes read at most 64 MVCC counters per requested term
    /// and physical partition. No user heap is opened for scoring statistics.
    pub(crate) fn fulltext_corpus_statistics(
        &self,
        txn: TxnView,
        table: &TableSchema,
        index: &IndexSchema,
        terms: &[String],
        explicit_storage_ids: Option<&[u64]>,
    ) -> Result<FulltextCorpusStatistics, QueryError> {
        if !self.fulltext_index_covers_snapshot(index, txn)? {
            return Err(fulltext_statistics_error(
                "corpus does not cover this snapshot; visible documents are required",
            ));
        }
        let counters = self.fulltext_counter_table(table.id)?;
        let primary = &counters.indexes[0];
        let mut documents = 0i128;
        let mut total_tokens = 0i128;
        let mut frequencies = terms
            .iter()
            .map(|t| (t.clone(), 0i128))
            .collect::<HashMap<_, _>>();
        let storage_ids = explicit_storage_ids
            .map(<[u64]>::to_vec)
            .unwrap_or_else(|| table.storage_ids());
        let mut own_charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
        let own_rows = self.fulltext_owned_rows(txn, &storage_ids, &mut own_charge)?;
        // Current-read writes can touch rows created or changed after an RR
        // snapshot. An aggregate counter's own version must not make those
        // concurrent contributions visible. Read the original snapshot and
        // overlay only this transaction's final visible document versions.
        let baseline = if own_rows.is_empty() {
            txn
        } else {
            TxnView {
                txn_id: 0,
                read_only: true,
                ..txn
            }
        };
        #[cfg(test)]
        let counter_rows_read = std::cell::Cell::new(0u64);
        let read = |prefix: &[Value], token: &str| -> Result<(i128, i128), QueryError> {
            let mut values = prefix.to_vec();
            values.push(Value::Bytes(token.as_bytes().to_vec()));
            let key = Self::dd_selector_key(&counters, primary, &values)?;
            let entries = self.prefix_index_entries_visible(&counters, primary, key, baseline)?;
            if entries.len() > FULLTEXT_COUNTER_SHARDS as usize {
                return Err(fulltext_statistics_error("invalid shard count"));
            }
            let (mut count, mut length) = (0i128, 0i128);
            for (_, id) in entries {
                #[cfg(test)]
                counter_rows_read.set(counter_rows_read.get() + 1);
                let Some(row) = self.read_row(&counters, id, baseline)? else {
                    continue;
                };
                let (Some(Value::Int(c)), Some(Value::Int(l))) =
                    (row.values.get(7), row.values.get(8))
                else {
                    return Err(fulltext_statistics_error("invalid persisted counter"));
                };
                count += i128::from(*c);
                length += i128::from(*l);
            }
            Ok((count, length))
        };
        for storage_id in storage_ids {
            let prefix = self.fulltext_counter_prefix(table, index, storage_id)?;
            let (count, length) = read(&prefix, "")?;
            documents += count;
            total_tokens += length;
            for (term, frequency) in &mut frequencies {
                *frequency += read(&prefix, term)?.0;
            }
        }
        for (storage_id, row_id) in own_rows {
            let before = self.read_row_at(table, storage_id, row_id, baseline)?;
            let after = self.read_row_at(table, storage_id, row_id, txn)?;
            if let (Some(before), Some(after)) = (&before, &after) {
                if index.columns.iter().all(|name| {
                    table
                        .column_index(name)
                        .is_ok_and(|p| before.values.get(p) == after.values.get(p))
                }) {
                    continue;
                }
            }
            let _rows_charge =
                crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                    crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                    before
                        .iter()
                        .chain(after.iter())
                        .map(|row| {
                            crate::engine::executor::mem_budget::estimate_row_bytes(&row.values)
                        })
                        .sum(),
                )?;
            for (row, sign) in [(before, -1i128), (after, 1i128)] {
                let Some(row) = row else {
                    continue;
                };
                let (length, tokens) = fulltext_document_statistics(table, index, &row.values)?;
                documents += sign;
                total_tokens += sign * i128::from(length);
                for (term, frequency) in &mut frequencies {
                    if tokens.contains(term) {
                        *frequency += sign;
                    }
                }
            }
        }
        let convert =
            |n| u64::try_from(n).map_err(|_| fulltext_statistics_error("invalid counter sum"));
        let documents = convert(documents)?;
        let total_tokens = convert(total_tokens)?;
        let frequencies = frequencies
            .into_iter()
            .map(|(token, count)| {
                let count = convert(count)?;
                if count > documents {
                    return Err(fulltext_statistics_error(
                        "document frequency exceeds corpus size",
                    ));
                }
                Ok((token, count))
            })
            .collect::<Result<HashMap<_, _>, QueryError>>()?;
        if documents == 0 && total_tokens != 0 {
            return Err(fulltext_statistics_error("tokens in empty corpus"));
        }
        Ok(FulltextCorpusStatistics {
            documents,
            total_tokens,
            frequencies,
            #[cfg(test)]
            counter_rows_read: counter_rows_read.get(),
        })
    }

    // EXCHANGE changes logical ownership without replaying DML. Move the
    // existing counter families with its catalog transaction, rather than
    // retokenizing either heap or leaving scoring to repair a cold query.
    fn transfer_fulltext_statistics(
        &self,
        txn: TxnView,
        source: &TableSchema,
        source_storage_id: u64,
        target: &TableSchema,
        target_storage_id: u64,
    ) -> Result<(), QueryError> {
        let mut seen = std::collections::BTreeSet::new();
        for index in target.indexes.iter().filter(|index| index.is_fulltext()) {
            let definition = fulltext_corpus_definition(target, index)?;
            let corpus_id = index.fulltext.as_ref().unwrap().corpus_id;
            if !seen.insert((corpus_id, definition.clone())) {
                continue;
            }
            let source_index = source
                .indexes
                .iter()
                .filter(|index| index.is_fulltext())
                .find(|index| {
                    fulltext_corpus_definition(source, index).is_ok_and(|value| value == definition)
                })
                .ok_or_else(|| fulltext_statistics_error("EXCHANGE corpus mismatch"))?;
            let source_prefix =
                self.fulltext_counter_prefix(source, source_index, source_storage_id)?;
            let target_prefix = self.fulltext_counter_prefix(target, index, target_storage_id)?;
            let mut rows = self.catalog_rows_for_selector_at(
                crate::storage::data_dictionary::DD_FULLTEXT_COUNTERS,
                CatalogRowSelector::PrefixValues(source_prefix),
                txn,
            )?;
            let mut charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
            for row in &mut rows {
                charge.add_non_spillable(
                    crate::engine::executor::mem_budget::estimate_row_bytes(row)
                        .saturating_add(128),
                )?;
                row[..target_prefix.len()].clone_from_slice(&target_prefix);
            }
            self.apply_catalog_row_replacement(
                txn,
                &CatalogRowReplacement {
                    dd_table_id: crate::storage::data_dictionary::DD_FULLTEXT_COUNTERS,
                    selector: CatalogRowSelector::PrefixValues(target_prefix),
                    rows,
                },
            )?;
        }
        Ok(())
    }

    // Establish a private COPY corpus before replay and publication barriers.
    // Replay debits/credits this baseline in the same transaction, using the
    // future public identity rather than the private heap's physical id.
    fn backfill_copied_fulltext_statistics(
        &self,
        txn: TxnView,
        table: &TableSchema,
        physical_id: u64,
        counter_storage_id: u64,
    ) -> Result<(), QueryError> {
        if !table.indexes.iter().any(|index| index.is_fulltext()) {
            return Ok(());
        }
        #[cfg(test)]
        if let Some(hook) = self
            .fulltext_copy_statistics_test_hook
            .lock()
            .unwrap()
            .take()
        {
            hook.pause();
        }
        let pages = self.with_heap_read(physical_id, |heap| heap.page_count())?;
        for lo in (0..pages).step_by(16) {
            crate::engine::cancel::check_cancelled()?;
            let rows =
                self.scan_rows_storage_range(table, physical_id, txn, lo, (lo + 16).min(pages))?;
            self.maintain_fulltext_statistics(
                txn.txn_id,
                table,
                counter_storage_id,
                rows.iter().map(|row| (None, Some(row.values.as_slice()))),
            )?;
        }
        Ok(())
    }

    // Offline FULLTEXT DDL holds the source write barrier. Initialize once
    // before publication, independent of the posting predicate. Rebuilds and
    // sibling indexes reuse the already maintained corpus. Failed builds can
    // leave unreachable counter identities, never a published partial corpus.
    fn ensure_fulltext_statistics(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
    ) -> Result<(), QueryError> {
        if !index.is_fulltext() {
            return Ok(());
        }
        // Published corpus counters are authoritative MVCC data, including
        // an empty corpus with no header rows. Rebuilding postings must not
        // allocate local transactions in a sealed source transaction domain.
        if index.state != IndexState::Building {
            return Ok(());
        }
        let definition = fulltext_corpus_definition(table, index)?;
        let shared_public = table.indexes.iter().any(|other| {
            other.is_fulltext()
                && other.state == IndexState::Public
                && other.fulltext.as_ref().map(|meta| meta.corpus_id)
                    == index.fulltext.as_ref().map(|meta| meta.corpus_id)
                && fulltext_corpus_definition(table, other)
                    .is_ok_and(|other| other == definition)
        });
        if shared_public {
            return Ok(());
        }
        let counters = self.fulltext_counter_table(table.id)?;
        let txn = if is_temp_table_id(table.id) {
            self.begin_temp_txn()?
        } else {
            self.begin_txn()?
        };
        // The table write barrier excludes active source writers. Match the
        // posting builder's complete committed contents even while another
        // table temporarily holds back the global visible-SCN frontier.
        let build_view = TxnView {
            snapshot_scn: u64::MAX,
            ..txn
        };
        let result = (|| {
            for storage_id in table.storage_ids() {
                let prefix = self.fulltext_counter_prefix(table, index, storage_id)?;
                // A crash before index publication can leave committed
                // private counters and an unreserved offline index id.
                // Reinitialize that unreachable family on every new build.
                self.apply_catalog_row_replacement(
                    txn,
                    &CatalogRowReplacement {
                        dd_table_id: counters.id,
                        selector: CatalogRowSelector::PrefixValues(prefix.clone()),
                        rows: Vec::new(),
                    },
                )?;
                let mut corpus_table = table.clone();
                corpus_table.indexes = vec![index.clone()];
                let pages = self.with_heap_read(storage_id, |heap| heap.page_count())?;
                for lo in (0..pages).step_by(16) {
                    let rows = self.scan_rows_storage_range(
                        table,
                        storage_id,
                        build_view,
                        lo,
                        (lo + 16).min(pages),
                    )?;
                    self.maintain_fulltext_statistics(
                        txn.txn_id,
                        &corpus_table,
                        storage_id,
                        rows.iter().map(|row| (None, Some(row.values.as_slice()))),
                    )?;
                }
            }
            self.commit_txn(txn.txn_id)?;
            #[cfg(test)]
            if std::env::var_os("MYDB_FT_STATISTICS_TEST_CRASH_AFTER_BUILD").is_some() {
                std::process::exit(87);
            }
            Ok(())
        })();
        if result.is_err() {
            let _ = self.rollback_txn(txn.txn_id);
        }
        result
    }
}
