/// Knobs the executor can flip to skip work that's only safe to skip
/// under specific INSERT shapes (see
/// [`Database::insert_rows_autoinc_fast_detailed`] for the proof). The
/// default value preserves the full insert pipeline used by general
/// DML.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct InsertHints {
    /// Rows were already coerced and had generated columns materialized by
    /// `prepare_insert_rows_for_batch`. Constraint/unique checks still run in
    /// the consuming insert; only duplicate CPU work is skipped.
    pub rows_prepared: bool,
    /// Skip recording an exclusive row lock for each freshly inserted
    /// row. Safe when (a) the rows are invisible to every other
    /// snapshot until commit (MVCC) and (b) no follow-up statement in
    /// this txn will UPDATE/DELETE them (autocommit single statement,
    /// no FK cascade).
    pub skip_row_lock: bool,
}

#[cfg(test)]
mod audit_1029_tests {
    use super::*;

    #[test]
    fn audit_1029_partial_lob_externalization_reclaims_rows_and_columns() {
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::engine::StorageQueryEngine::open(dir.path()).unwrap();
        engine.execute("CREATE TABLE lob_failure(b LONGBLOB, j JSON)").unwrap();
        let db = engine.db_handle();
        let table = db.table("test.lob_failure").unwrap();
        let invalid = vec![Value::Bytes(vec![42; 20000]), Value::String("invalid".repeat(5000))];
        assert!(db.externalize_row_lobs(table.id, &table.row_codec, &invalid).is_err());
        let (pages, free) = db.lob_space_stats(table.id).unwrap();
        assert!(pages > 1);
        assert_eq!(free, pages - 1);
        let valid = vec![Value::Bytes(vec![43; 20000]), Value::String(format!("\"{}\"", "x".repeat(20000)))];
        let txn = db.begin_txn().unwrap();
        assert!(db.insert_mvcc_batch_externalized(txn.txn_id, table.id, &table.row_codec, &[], &[valid, invalid]).is_err());
        db.rollback_txn(txn.txn_id).unwrap();
        let (pages, free) = db.lob_space_stats(table.id).unwrap();
        assert_eq!(free, pages - 1);
        engine.shutdown().unwrap();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BulkInsertMode {
    User,
    PartitionShadow,
}

enum DmlCandidates {
    RowIds(Option<Vec<RowId>>),
    WithOrigin(Option<Vec<crate::storage::DmlRowRef>>),
}

impl DmlCandidates {
    fn for_unpartitioned(self, storage_id: u64) -> Option<Vec<RowId>> {
        match self {
            Self::RowIds(candidates) => candidates,
            Self::WithOrigin(candidates) => candidates.map(|rows| {
                rows.into_iter()
                    .filter(|row| row.storage_id == storage_id)
                    .map(|row| row.row_id)
                    .collect()
            }),
        }
    }

    fn for_partitioned(self) -> Option<Vec<crate::storage::DmlRowRef>> {
        match self {
            // A partition-local RowId is ambiguous without its storage id.
            // Legacy/internal callers remain correct by falling back to the
            // pruned full scan; executor paths use `WithOrigin` below.
            Self::RowIds(_) => None,
            Self::WithOrigin(candidates) => candidates,
        }
    }
}

impl Database {
    fn record_non_temp_table_modifications(
        &self,
        txn_id: u64,
        table: &TableSchema,
        count: u64,
        live_row_delta: i64,
    ) -> Result<(), QueryError> {
        if !is_temp_table_id(table.id) {
            // The caller already owns the schema-validated table id. Going
            // back through `qualified_name` + normalization + a catalog read
            // allocated and re-resolved that same identity for every changed
            // row, even though the sidecar counter is keyed by id.
            self.record_table_modifications_by_id(table.id, count);
            self.record_pending_live_row_delta(txn_id, table.id, live_row_delta);
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub fn insert_row(
        &self,
        txn: TxnView,
        table_name: &str,
        values: Vec<Value>,
    ) -> Result<RowId, QueryError> {
        let table = self.table_by_key(table_name).ok_or_else(|| {
            error(
                crate::error::catalog::ER_NO_SUCH_TABLE,
                format!("Table '{table_name}' doesn't exist"),
            )
        })?;
        let row_id = self.insert_row_in_table(txn, &table, values)?;
        Ok(row_id)
    }

    pub fn insert_rows(
        &self,
        txn: TxnView,
        table_name: &str,
        rows: Vec<Vec<Value>>,
    ) -> Result<Vec<RowId>, QueryError> {
        Ok(self.insert_rows_detailed(txn, table_name, rows)?.row_ids)
    }

    /// Decide which of `values`' variable-length columns to store
    /// off-page and allocate their overflow chains, returning a per-
    /// column external mask (parallel to `values`). The mask is empty
    /// when nothing is externalized (the common case), so the row
    /// encodes byte-for-byte as before. `values` is left unchanged: the
    /// heap encoder ignores the inline bytes of an externalized column,
    /// while index keys / binlog still see the full value.
    fn externalize_row_lobs(
        &self,
        table_id: u64,
        codec: &RowCodec,
        values: &[Value],
    ) -> Result<Vec<Option<LobRef>>, QueryError> {
        // INSERT path: reuse is empty, so every masked chain is newly
        // allocated; the caller frees the whole mask on failure, so the
        // newly-allocated list is redundant here.
        let (mask, _newly_allocated) =
            self.externalize_row_lobs_with_reuse(table_id, codec, values, &[])?;
        Ok(mask)
    }

    /// Same conservative admission test for ordinary externalization and
    /// sparse-undo full-row rewrites. A true result cannot create LOB owners.
    fn row_lobs_stay_inline(codec: &RowCodec, values: &[Value]) -> bool {
        let mut maximum = 0usize;
        let mut total = 0usize;
        for value in values.iter().take(codec.columns.len()) {
            let len = match value {
                Value::Bytes(bytes) | Value::Json(bytes) | Value::Geometry(bytes) => bytes.len(),
                Value::Vector(bytes) => bytes.len(),
                Value::String(text) => text.len(),
                _ => 0,
            };
            maximum = maximum.max(len);
            total = total.saturating_add(len);
        }
        maximum <= lob_inline_max()
            && total.saturating_add(18) < PAGE_SIZE - PAGE_HEADER_SIZE - SLOT_SIZE
    }

    /// M1(LOB) — reuse-aware variant of `externalize_row_lobs`: for every column judged
    /// to need externalization, if `reuse[col]` carries an old chain (the caller has proven
    /// the column's new bytes equal the old chain's bytes byte for byte), reuse that chain
    /// directly — no re-allocation, no overflow-page rewrite — and record one reuse
    /// statistic; otherwise allocate a new chain as usual. `reuse` may be shorter than
    /// `values` (missing slots count as no reuse); an empty slice degrades to the original rewrite behavior — the `MYDB_LOB_REUSE_UNCHANGED=off` differential-oracle path.
    /// Returns `(mask, newly_allocated)`. `mask` is the per-column
    /// external descriptor for the heap write; `newly_allocated` lists
    /// **only the chains this call freshly allocated** — never the reused
    /// old chains carried in via `reuse`. BUG-2
    /// (docs/lob-management-audit-2026-07.md): a mid-operation failure
    /// (heap write / undo cap) must free only the newly-allocated chains;
    /// freeing the reused old chains — which the unchanged live row still
    /// references — is a use-after-free. So the allocator itself reports
    /// exactly what it allocated, and rollback/failure paths free that set
    /// rather than the whole mask.
    fn externalize_row_lobs_with_reuse(
        &self,
        table_id: u64,
        codec: &RowCodec,
        values: &[Value],
        reuse: &[Option<LobRef>],
    ) -> Result<(Vec<Option<LobRef>>, Vec<LobRef>), QueryError> {
        let n = values.len().min(codec.columns.len());
        let threshold = lob_inline_max();
        let has_forced_external = reuse.iter().take(n).any(|e| e.is_some());
        if !has_forced_external && Self::row_lobs_stay_inline(codec, values) {
            return Ok((Vec::new(), Vec::new()));
        }
        let is_lob_col = |i: usize| {
            matches!(
                codec.columns[i].kind,
                ColumnKind::VarString
                    | ColumnKind::VarBytes
                    | ColumnKind::Json
                    | ColumnKind::Geometry
                    | ColumnKind::Vector { .. }
            )
        };
        let lob_byte_len = |i: usize| match &values[i] {
            Value::Bytes(b) => b.len(),
            Value::String(s) => s.len(),
            Value::Json(b) | Value::Geometry(b) => b.len(),
            Value::Vector(b) => b.len(),
            _ => 0,
        };
        let mut externalized = vec![false; values.len()];
        // M1(LOB) — columns whose reuse slot carries an old chain are always treated as
        // "already externalized": their slot in values may be a Null placeholder (the kept
        // chain when rollback restores the old version), which the byte-size rule cannot detect — force-keep them.
        for i in 0..n {
            if matches!(reuse.get(i), Some(Some(_))) {
                externalized[i] = true;
            }
        }
        // Rule (a): every LOB-typed value over the inline threshold.
        for i in 0..n {
            if is_lob_col(i) && lob_byte_len(i) > threshold {
                externalized[i] = true;
            }
        }
        // Rule (b): externalize the largest remaining LOB column until
        // the row fits one page (or none remain — heap then rejects it).
        loop {
            let est = estimate_value_section_len(values, codec, &externalized);
            if est + PAGE_HEADER_SIZE + SLOT_SIZE <= PAGE_SIZE {
                break;
            }
            let candidate = (0..n)
                .filter(|&i| !externalized[i] && is_lob_col(i) && lob_byte_len(i) > 0)
                .max_by_key(|&i| lob_byte_len(i));
            match candidate {
                Some(i) => externalized[i] = true,
                None => break,
            }
        }
        if !externalized.iter().any(|&e| e) {
            return Ok((Vec::new(), Vec::new()));
        }
        let mut mask = vec![None; values.len()];
        // BUG-2 — chains this call allocates (excludes reused old chains).
        let mut newly_allocated: Vec<LobRef> = Vec::new();
        let allocation = (|| -> Result<(), QueryError> {
        for i in 0..n {
            if externalized[i] {
                // M1(LOB) — attach the kept/reused old chain: the caller (UPDATE reuse
                // or rollback kept-chain) has already ensured the chain matches the column's
                // bytes, so reuse it directly, skipping allocation + overflow-page rewrite.
                // The reuse count is recorded where `compute_lob_reuse` decides (rollback's
                // kept-chain retention must not count as reuse), not counted again here.
                if let Some(Some(old_lob)) = reuse.get(i) {
                    mask[i] = Some(*old_lob);
                    continue;
                }
                let bytes = match codec.columns[i].kind {
                    ColumnKind::VarString => value_to_string_bytes(&values[i]).into(),
                    ColumnKind::Json => json_coerce_to_jsonb(&values[i])?,
                    ColumnKind::Geometry => geometry_coerce_to_internal(&values[i])?.into(),
                    ColumnKind::Vector { dim, datatype } => {
                        vector_coerce_to_internal(&values[i], datatype, dim)?.into()
                    }
                    _ => value_to_bytes(&values[i]).into(),
                };
                // M5(LOB compression) — text/json/bytes/geometry are compressible; vector
                // is already-compact f32 little-endian that lz4 cannot shrink (only adds CPU) → skip.
                let compressible = !matches!(codec.columns[i].kind, ColumnKind::Vector { .. });
                let lob = self.with_overflow_mut(table_id, |ovf| {
                    ovf.allocate_chain_compressible(&bytes, compressible)
                })?;
                // M6(LOB) — record the bytes allocated for one externalized chain (raw bytes).
                self.dml_stats
                    .record_lob_chain_externalized(bytes.len() as u64);
                mask[i] = Some(lob);
                newly_allocated.push(lob);
            }
        }
            Ok(())
        })();
        if let Err(err) = allocation {
            self.free_lob_chains(table_id, &newly_allocated);
            return Err(err);
        }
        Ok((mask, newly_allocated))
    }

    /// Free the overflow chains recorded in a batch of external masks —
    /// used to undo externalization when a heap insert fails before the
    /// row (and its rollback undo record) exists to own the chains.
    fn free_external_masks(&self, table_id: u64, masks: &[Vec<Option<LobRef>>]) {
        let mut freed: u64 = 0;
        for mask in masks {
            for lob in mask.iter().flatten() {
                if self
                    .with_overflow_mut(table_id, |ovf| ovf.free_chain(*lob))
                    .is_ok()
                {
                    freed = freed.saturating_add(1);
                }
            }
        }
        self.dml_stats.record_lob_leak_recovered(freed);
    }

    /// Free a list of LOB chains already known to be unreferenced
    /// (used by M1.A2 batch UPDATE staging on prepare/write failure).
    /// Each freed chain bumps `dml_lob_leaks_recovered` — the metric
    /// distinguishes "engine reclaimed an in-flight allocation" from
    /// a quiet leak that the recovery path would silently absorb.
    pub(crate) fn free_lob_chains(&self, table_id: u64, chains: &[LobRef]) {
        let mut freed: u64 = 0;
        for lob in chains {
            if self
                .with_overflow_mut(table_id, |ovf| ovf.free_chain(*lob))
                .is_ok()
            {
                freed = freed.saturating_add(1);
            }
        }
        self.dml_stats.record_lob_leak_recovered(freed);
    }

    /// Capture the old version's off-page chains into an UPDATE undo
    /// record (descriptors only, externalized old columns nulled in the
    /// before-image) and externalize the new version's large columns.
    /// Returns `(undo_id, new_external)` for the caller's heap write. The
    /// old chains stay alive for snapshots seeing the old version (freed
    /// at purge); the new chains belong solely to the new version.
    fn prepare_update_lob(
        &self,
        txn_id: u64,
        storage_id: u64,
        codec: &RowCodec,
        row_id: RowId,
        old_header: RowHeader,
        old_values: &[Value],
        new_values: &[Value],
        changed_columns: Option<&[usize]>,
    ) -> Result<(u64, Vec<Option<LobRef>>, Vec<LobRef>), QueryError> {
        let before_external = self.read_row_external_refs(storage_id, row_id)?;
        // Undo shape depends on ownership and changed columns, not on whether
        // the heap payload can be overwritten at equal width.
        if undo_delta_rewrite_enabled()
            && before_external.is_empty()
            && Self::row_lobs_stay_inline(codec, new_values)
        {
            if let Some(cols) = changed_columns.filter(|cols| !cols.is_empty()) {
                if cols
                    .iter()
                    .all(|&col| col <= u16::MAX as usize && col < old_values.len())
                    && !self.row_has_externalizable_changes(codec, old_values, new_values, cols)
                {
                    let undo_id = self.push_undo_ext(
                        txn_id,
                        UndoKind::UpdateDelta {
                            before_cols: cols.iter().map(|&col| col as u16).collect(),
                        },
                        storage_id,
                        row_id,
                        Some(old_header),
                        Some(cols.iter().map(|&col| old_values[col].clone()).collect()),
                        Vec::new(),
                    )?;
                    return Ok((undo_id, Vec::new(), Vec::new()));
                }
            }
        }
        // M1(LOB) — compute the "reuse-if-unchanged" plan: for columns externalized in the
        // old version whose new bytes are byte-for-byte equal to the old chain, the new
        // version reuses the old chain (reuse_mask carries the old LobRef) and the column
        // moves from owned before_external to borrowed_external. Historical versions still
        // read the same immutable chain, but purge never frees borrowed chains; rollback transfers chain ownership from the current row back to the old row — no payload copy, double free, or leak.
        let (reuse_mask, kept_before_external) =
            self.compute_lob_reuse(storage_id, codec, &before_external, new_values)?;
        let borrowed_external: Vec<(usize, LobRef)> = before_external
            .iter()
            .filter(|(column, _)| reuse_mask.get(*column).is_some_and(Option::is_some))
            .copied()
            .collect();
        // Both owned and borrowed descriptors reconstruct the old version;
        // neither needs a payload-sized inline copy in undo.
        let null_cols: std::collections::HashSet<usize> = kept_before_external
            .iter()
            .chain(&borrowed_external)
            .map(|(i, _)| *i)
            .collect();
        let undo_values: Vec<Value> = if null_cols.is_empty() {
            old_values.to_vec()
        } else {
            old_values
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    if null_cols.contains(&i) {
                        Value::Null
                    } else {
                        v.clone()
                    }
                })
                .collect()
        };
        let undo_id = self.push_undo_ext_borrowed(
            txn_id,
            UndoKind::Update,
            storage_id,
            row_id,
            Some(old_header),
            Some(undo_values),
            kept_before_external,
            borrowed_external,
        )?;
        let (new_external, newly_allocated) =
            self.externalize_row_lobs_with_reuse(storage_id, codec, new_values, &reuse_mask)?;
        Ok((undo_id, new_external, newly_allocated))
    }

    /// M1(LOB) — compares the old version's externalized chain bytes against the new value
    /// bytes, producing:
    ///   * `reuse_mask` (parallel to values): byte-equal columns carry their old `LobRef`,
    ///     so `externalize_row_lobs_with_reuse` reuses them directly without rewriting;
    ///   * `kept_before_external`: the chains remaining in `before_external` after dropping
    ///     reused columns — still owned by undo (freed at purge); the reused columns'
    ///     chains are owned by the new row version instead and must never go into undo, or
    ///     purge would wrongly free chains the current row still references. With GUC
    /// `MYDB_LOB_REUSE_UNCHANGED=off`, returns empty reuse + the original `before_external` — fully equivalent to the old "always rewrite" path (differential oracle).
    fn compute_lob_reuse(
        &self,
        storage_id: u64,
        codec: &RowCodec,
        before_external: &[(usize, LobRef)],
        new_values: &[Value],
    ) -> Result<(Vec<Option<LobRef>>, Vec<(usize, LobRef)>), QueryError> {
        if before_external.is_empty() || !lob_reuse_unchanged_enabled() {
            return Ok((Vec::new(), before_external.to_vec()));
        }
        let mut reuse_mask: Vec<Option<LobRef>> = vec![None; new_values.len()];
        let mut kept: Vec<(usize, LobRef)> = Vec::with_capacity(before_external.len());
        for (col, old_lob) in before_external {
            let col = *col;
            // The column's new-value bytes (same coercion set as the externalization path, keeping the comparison consistent).
            let new_bytes = match new_values.get(col) {
                Some(value) => match codec.columns.get(col).map(|c| &c.kind) {
                    Some(ColumnKind::VarString) => value_to_string_bytes(value).into(),
                    Some(ColumnKind::Json) => json_coerce_to_jsonb(value)?,
                    Some(ColumnKind::Geometry) => geometry_coerce_to_internal(value)?.into(),
                    Some(ColumnKind::Vector { dim, datatype }) => {
                        vector_coerce_to_internal(value, *datatype, *dim)?.into()
                    }
                    Some(ColumnKind::VarBytes) => value_to_bytes(value).into(),
                    // Column type not externalizable (theoretically never appears in
                    // before_external) — conservatively skip reuse.
                    _ => {
                        kept.push((col, *old_lob));
                        continue;
                    }
                },
                None => {
                    kept.push((col, *old_lob));
                    continue;
                }
            };
            // Short-circuit on length mismatch to avoid a pointless full-chain read. A NULL
            // coercion stops the column being externalized; the new bytes are empty (or change length) and naturally fall into "no reuse".
            if new_bytes.len() as u64 != old_lob.total_len {
                kept.push((col, *old_lob));
                continue;
            }
            // Byte-for-byte comparison: read the old chain (read-only, no write lock). Reuse only if equal.
            let old_bytes = self.with_overflow_read(storage_id, |ovf| ovf.read_chain(*old_lob))?;
            if old_bytes.as_slice() == new_bytes.as_ref() {
                // M1(LOB) — decided to reuse: record one reuse statistic (saves writing the whole chain).
                self.dml_stats.record_lob_chain_reused(old_lob.total_len);
                reuse_mask[col] = Some(*old_lob);
            } else {
                kept.push((col, *old_lob));
            }
        }
        Ok((reuse_mask, kept))
    }

    /// Capture the old version's off-page chains into a DELETE undo
    /// record and produce the tombstone values with externalized columns
    /// nulled (DELETE never copies LOB bytes; the pre-image lives in the
    /// undo `before_external`). Returns `(undo_id, tombstone_values)`.
    fn prepare_delete_lob(
        &self,
        txn_id: u64,
        storage_id: u64,
        row_id: RowId,
        old_header: RowHeader,
        current_values: &[Value],
    ) -> Result<(u64, Vec<Value>), QueryError> {
        let before_external = self.read_row_external_refs(storage_id, row_id)?;
        let mut tombstone = current_values.to_vec();
        for (i, _) in &before_external {
            if let Some(slot) = tombstone.get_mut(*i) {
                *slot = Value::Null;
            }
        }
        let undo_id = self.push_undo_ext(
            txn_id,
            UndoKind::Delete,
            storage_id,
            row_id,
            Some(old_header),
            Some(tombstone.clone()),
            before_external,
        )?;
        Ok((undo_id, tombstone))
    }

    /// Externalize each row's over-threshold LOB columns, then run the
    /// heap batch insert with the resulting masks. On heap-insert
    /// failure the freshly allocated chains are freed (no row exists yet
    /// to own them via rollback). The single entry point every INSERT
    /// path uses so externalization is never accidentally skipped.
    fn insert_mvcc_batch_externalized(
        &self,
        txn_id: u64,
        table_id: u64,
        codec: &RowCodec,
        headers: &[RowHeader],
        rows: &[Vec<Value>],
    ) -> Result<Vec<RowId>, QueryError> {
        self.preflight_insert_undo_admission(table_id, rows.len())?;
        let mut externals = Vec::with_capacity(rows.len());
        for values in rows {
            match self.externalize_row_lobs(table_id, codec, values) {
                Ok(mask) => externals.push(mask),
                Err(err) => { self.free_external_masks(table_id, &externals); return Err(err); }
            }
        }
        match self.with_heap_mut(table_id, |heap| {
            heap.insert_mvcc_batch(txn_id, headers, rows, &externals)
        }) {
            Ok(inserted) => {
                // Merge once per prepared batch after final RowIds are known;
                // the zone id is a property of the forwarding root page.
                self.merge_zone_map_rows(txn_id, table_id, &inserted, rows)?;
                Ok(inserted)
            }
            Err(e) => {
                self.free_external_masks(table_id, &externals);
                Err(e)
            }
        }
    }

    pub fn insert_rows_detailed(
        &self,
        txn: TxnView,
        table_name: &str,
        rows: Vec<Vec<Value>>,
    ) -> Result<InsertRowsResult, QueryError> {
        self.insert_rows_detailed_with_hints(txn, table_name, rows, InsertHints::default())
    }

    /// Autocommit inserts can omit row locks for newly allocated, invisible
    /// rows. Unique-key locks and probes are still required: an explicit-key
    /// writer can target a reserved AUTO_INCREMENT value concurrently.
    pub fn insert_rows_autoinc_fast_detailed(
        &self,
        txn: TxnView,
        table_name: &str,
        rows: Vec<Vec<Value>>,
    ) -> Result<InsertRowsResult, QueryError> {
        self.insert_rows_detailed_with_hints(
            txn,
            table_name,
            rows,
            InsertHints {
                rows_prepared: false,
                skip_row_lock: true,
            },
        )
    }

    /// Prepare a full-width INSERT batch once so ODKU's conflict probe and
    /// eventual batch writer compare exactly the same coerced/generated values.
    /// AUTO_INCREMENT values must be explicit and positive. Check that after
    /// coercion as well as at executor admission so this API cannot allocate
    /// IDs ahead of source-order conflict handling.
    pub(crate) fn prepare_insert_rows_for_batch(
        &self,
        txn: TxnView,
        table_name: &str,
        rows: Vec<Vec<Value>>,
    ) -> Result<PreparedInsertBatch, QueryError> {
        let table = self.table_by_key(table_name).ok_or_else(|| {
            error(
                crate::error::catalog::ER_NO_SUCH_TABLE,
                format!("Table '{table_name}' doesn't exist"),
            )
        })?;
        self.check_dml_allowed_for_table(txn, table.id)?;

        let has_generated_columns = table
            .columns
            .iter()
            .any(|column| column.generated_expr.is_some());
        let mut prepared_rows = Vec::with_capacity(rows.len());
        for mut values in rows {
            coerce_row(&table, &mut values)?;
            if has_generated_columns {
                materialize_generated_columns(&table, &mut values, None)?;
            }
            if table.columns.iter().zip(&values).any(|(column, value)| {
                column.auto_increment
                    && !matches!(value, Value::Int(n) if *n > 0)
                    && !matches!(value, Value::UInt(n) if *n > 0)
            }) {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "prepared batch insert requires explicit positive AUTO_INCREMENT values",
                ));
            }
            prepared_rows.push(values);
        }
        self.check_not_null_constraints_batch(&table, &prepared_rows)?;
        self.check_check_constraints_batch(&table, &prepared_rows)?;
        Ok(PreparedInsertBatch {
            table_id: table.id,
            rows: prepared_rows,
        })
    }

    /// Consume a conflict-free prefix produced by
    /// `prepare_insert_rows_for_batch` through the canonical heap/index/undo/WAL
    /// pipeline. Ordinary tables skip repeated preparation; partitioned tables
    /// retain the canonical routed writer and its constraint checks. The caller's
    /// conflict locator has already checked both batch-local and stored UNIQUE
    /// keys. The consuming writer locks and rechecks storage, so a concurrent
    /// winner surfaces 1062 before index insertion; the executor rolls the segment back to
    /// its statement savepoint and retries it through the legacy oracle.
    pub(crate) fn insert_prepared_rows_detailed(
        &self,
        txn: TxnView,
        table_name: &str,
        prepared: Vec<Vec<Value>>,
    ) -> Result<InsertRowsResult, QueryError> {
        self.insert_rows_detailed_with_hints(
            txn,
            table_name,
            prepared,
            InsertHints {
                rows_prepared: true,
                ..InsertHints::default()
            },
        )
    }

    fn insert_rows_detailed_with_hints(
        &self,
        txn: TxnView,
        table_name: &str,
        rows: Vec<Vec<Value>>,
        hints: InsertHints,
    ) -> Result<InsertRowsResult, QueryError> {
        let table = self.table_by_key(table_name).ok_or_else(|| {
            error(
                crate::error::catalog::ER_NO_SUCH_TABLE,
                format!("Table '{table_name}' doesn't exist"),
            )
        })?;
        self.check_dml_allowed_for_table(txn, table.id)?;
        if table.is_partitioned() {
            return self.insert_rows_partitioned_detailed(txn, &table, rows);
        }
        // M2.2 - Acquire IX table lock to prevent concurrent DDL.
        self.acquire_table_write_binding(txn.txn_id, &table)?;
        let result = (|| {
            let mut rows = rows;
            // Generated values are strictly increasing within this batch.
            // Prove this from the source rows and every unique index; it only
            // removes batch-local hashing, never cross-transaction locking.
            let generated_unique_batch = rows.len() > 1
                && hints.skip_row_lock
                && table.auto_increment_column_index().is_some_and(|column| {
                    rows.iter()
                        .all(|row| matches!(row.get(column), Some(Value::Null)))
                        && table
                            .indexes
                            .iter()
                            .filter(|index| index.enforces_uniqueness())
                            .all(|index| {
                                !index.has_expression_parts()
                                    && index.columns.len() == 1
                                    && index.columns[0]
                                        .eq_ignore_ascii_case(&table.columns[column].name)
                            })
                });
            let first_insert_id = self.assign_auto_increment_values(
                txn.txn_id,
                txn.connection_id,
                &table,
                &mut rows,
            )?;
            // Lift table-level "does any X apply?" out of the per-row
            // loop. On a bulk INSERT these flags are constant; reading
            // them once amortises the cost of checking generated/unique
            // metadata over the entire batch.
            let has_generated_columns = table
                .columns
                .iter()
                .any(|column| column.generated_expr.is_some());
            let has_any_indexes = !table.indexes.is_empty();
            // Materialise the unique-index list once so the inner loop
            // iterates a tight `&[&IndexSchema]` instead of re-filtering
            // `table.indexes` per row.
            let unique_indexes: smallvec::SmallVec<[&crate::storage::schema::IndexSchema; 4]> =
                table
                    .indexes
                    .iter()
                    .filter(|index| index.enforces_uniqueness())
                    .collect();
            let has_unique_indexes = !unique_indexes.is_empty();
            // Parallel to `unique_indexes`: a Vec<HashSet> keyed by
            // position avoids the `BTreeMap<String, …>` lookup + string
            // hash that the per-row loop used to perform. Each HashSet
            // is pre-sized to the batch so the typical bulk INSERT
            // never triggers a `resize_rehash`.
            // Even a batch with distinct generated keys can race with another
            // transaction writing explicit keys. Always lock and probe storage.
            let need_unique_checks = has_unique_indexes;
            let need_batch_unique_keys =
                need_unique_checks && rows.len() > 1 && !generated_unique_batch;
            let mut batch_unique_keys_vec: Vec<crate::util::fast_hash::AhHashSet<IndexKey>> =
                if need_batch_unique_keys {
                    unique_indexes
                        .iter()
                        .map(|_| crate::util::fast_hash::ah_hash_set_with_capacity(rows.len()))
                        .collect()
                } else {
                    Vec::new()
                };
            for values in &mut rows {
                if !hints.rows_prepared {
                    coerce_row(&table, values)?;
                    if has_generated_columns {
                        materialize_generated_columns(&table, values, None)?;
                    }
                }
                if need_batch_unique_keys {
                    for (slot, index) in unique_indexes.iter().enumerate() {
                        let key = table.index_key(index, &values)?;
                        if key.0.iter().any(|part| matches!(part, KeyPart::Null)) && !index.primary
                        {
                            continue;
                        }
                        if !batch_unique_keys_vec[slot].insert(key) {
                            return Err(error(
                                crate::error::catalog::ER_DUP_ENTRY,
                                format!("duplicate entry for key '{}'", index.name),
                            ));
                        }
                    }
                }
            }
            let prepared_rows = rows;
            // M4.B6 — batch constraint checks. Lifting the
            // NOT NULL / CHECK passes out of the per-row prep loop
            // amortises the small-but-hot fixed costs (column-set
            // filter, vtable indirection on `Value::Null` matching,
            // `Vec::iter` setup) over the whole batch, and the row
            // loop above shrinks to the work that actually has to
            // be per-row (coerce, generated columns, unique-key
            // build). Error order matches the legacy path: row N
            // is rejected before row N+1 for the same reason.
            self.check_not_null_constraints_batch(&table, &prepared_rows)?;
            self.check_check_constraints_batch(&table, &prepared_rows)?;
            // Hold the unique-key locks before probing: a committed competitor
            // must not invalidate a successful probe before the heap write.
            if need_unique_checks {
                self.lock_unique_keys_for_rows(txn.txn_id, &table, &prepared_rows)?;
            }
            if need_unique_checks && (prepared_rows.len() == 1 || generated_unique_batch) {
                // A single row cannot duplicate itself. Keep exactly the same
                // index conflict proof and key locking as the batch path,
                // without building per-index HashSets and a string-keyed map.
                {
                    for index in &unique_indexes {
                        let key = table.index_key(index, &prepared_rows[0])?;
                        if key.0.iter().any(|part| matches!(part, KeyPart::Null)) && !index.primary
                        {
                            continue;
                        }
                        if !self.batch_min_unique_key_is_after_index(&table, index, &key)? {
                            // A competing explicit key may have committed before
                            // our locks were acquired. Recheck every generated
                            // row unless the whole batch is beyond the index.
                            if prepared_rows.len() == 1 {
                                self.check_unique_constraint(&table, txn, None, index, key)?;
                            } else {
                                for values in &prepared_rows {
                                    self.check_unique_constraint(
                                        &table,
                                        txn,
                                        None,
                                        index,
                                        table.index_key(index, values)?,
                                    )?;
                                }
                            }
                        }
                    }
                }
            } else if need_batch_unique_keys {
                // Reuse the `Vec<HashSet>` form via a thin shim so the
                // existing `check_batch_unique_constraints` /
                // `lock_unique_keys_for_rows` keep their `BTreeMap`-shaped
                // input for now; the per-batch overhead here is amortised
                // by the much bigger per-row loop savings above.
                let batch_unique_keys: BTreeMap<
                    String,
                    crate::util::fast_hash::AhHashSet<IndexKey>,
                > = unique_indexes
                    .iter()
                    .zip(batch_unique_keys_vec.drain(..))
                    .map(|(idx, keys)| (idx.name.clone(), keys))
                    .collect();
                {
                    self.check_batch_unique_constraints(
                        &table,
                        txn,
                        &prepared_rows,
                        &batch_unique_keys,
                    )?;
                }
            }
            let row_headers = vec![RowHeader::visible(txn.txn_id); prepared_rows.len()];
            let inserted = self.insert_mvcc_batch_externalized(
                txn.txn_id,
                table.id,
                &table.row_codec,
                &row_headers,
                &prepared_rows,
            )?;
            // M1.A3 — Atomicity of heap write, row-lock acquisition,
            // and insert-batch undo append. If any of
            // these post-heap steps fails we must physically remove the
            // already-written heap rows (and their freshly allocated LOB
            // chains via `rollback_inserted_rows`) before the error
            // propagates: the txn rollback path follows the undo chain,
            // and these rows are not yet on it.
            if let Err(err) = (|| -> Result<(), QueryError> {
                if !hints.skip_row_lock {
                    self.lock_rows_nonblocking(txn.txn_id, table.id, inserted.iter().copied())?;
                }
                self.push_insert_batch_undo_deferred(txn.txn_id, table.id, &inserted)?;
                Ok(())
            })() {
                // rollback_inserted_rows reads external refs, deletes the
                // heap slot, drops every index entry, and frees the
                // chains — exact mirror of the not-yet-committed insert.
                self.rollback_inserted_rows(txn.txn_id, table.id, &inserted)?;
                self.dml_stats
                    .record_rollback_inserted_rows(inserted.len() as u64);
                return Err(err);
            }
            if has_any_indexes {
                self.insert_index_batches_deferred_for_rows(
                    txn.txn_id,
                    &table,
                    &prepared_rows,
                    &inserted,
                )?;
            }
            self.defer_vector_insert_batch(
                txn.txn_id,
                &table,
                table.id,
                &prepared_rows,
                &inserted,
            )?;
            let row_ids = inserted;
            // Foreign-key child-side existence check runs *after* the batch
            // is inserted so a row referencing an earlier row in the same
            // statement (self-referential FK) sees it. No-op when the table
            // has no outgoing FKs (always true for partitioned tables, which
            // cannot carry foreign keys). Locks matched parents to bar orphans.
            if !table.foreign_keys.is_empty() {
                self.fk_check_child_references_batch(&table, txn, &prepared_rows)?;
            }
            self.record_alter_copy_touched_rows(
                txn.txn_id,
                table.id,
                table.id,
                row_ids.iter().copied(),
            );
            self.record_non_temp_table_modifications(
                txn.txn_id,
                &table,
                row_ids.len() as u64,
                row_ids.len().min(i64::MAX as usize) as i64,
            )?;
            crate::binlog::record_write_rows(txn.txn_id, &table, prepared_rows)?;
            self.dml_stats.record_inserts(row_ids.len() as u64);
            Ok(InsertRowsResult {
                storage_ids: Vec::new(),
                row_ids,
                first_insert_id,
            })
        })();
        result
    }

    /// Partition-aware INSERT path. Routes each prepared row to the
    /// matching partition's storage_id (via `route_row_to_partition`),
    /// groups rows by partition, then runs the heap/index insert per
    /// partition. AUTO_INCREMENT, NOT NULL / CHECK / unique-batch
    /// dedup, and table-level intent locks behave the same as the
    /// unpartitioned path; only the heap/index/visibility/undo writes
    /// are scoped to the chosen partition's `storage_id`.
    pub(in crate::storage::core) fn insert_rows_partitioned_detailed(
        &self,
        txn: TxnView,
        table: &std::sync::Arc<TableSchema>,
        rows: Vec<Vec<Value>>,
    ) -> Result<InsertRowsResult, QueryError> {
        self.insert_rows_partitioned_detailed_with_mode(
            txn,
            table,
            rows,
            false,
            BulkInsertMode::User,
        )
    }

    fn insert_rows_partitioned_detailed_with_mode(
        &self,
        txn: TxnView,
        table: &std::sync::Arc<TableSchema>,
        rows: Vec<Vec<Value>>,
        skip_unique_check: bool,
        mode: BulkInsertMode,
    ) -> Result<InsertRowsResult, QueryError> {
        self.acquire_table_write_binding(txn.txn_id, &table)?;
        let result = (|| {
            let mut rows = rows;
            let first_insert_id = if mode == BulkInsertMode::User {
                self.assign_auto_increment_values(txn.txn_id, txn.connection_id, table, &mut rows)?
            } else {
                0
            };
            let mut batch_unique_keys = (!skip_unique_check).then(|| {
                table
                    .indexes
                    .iter()
                    .filter(|index| index.enforces_uniqueness())
                    .map(|index| (index.name.clone(), crate::util::fast_hash::ah_hash_set()))
                    .collect::<BTreeMap<_, crate::util::fast_hash::AhHashSet<IndexKey>>>()
            });

            // Phase 1: coerce/check each row, compute its partition.
            #[derive(Clone)]
            struct Prepared {
                orig_idx: usize,
                storage_id: u64,
                values: Vec<Value>,
            }
            let mut prepared: Vec<Prepared> = Vec::with_capacity(rows.len());
            for (orig_idx, mut values) in rows.into_iter().enumerate() {
                coerce_row(table, &mut values)?;
                materialize_generated_columns(table, &mut values, None)?;
                for index in table
                    .indexes
                    .iter()
                    .filter(|index| index.enforces_uniqueness() && !skip_unique_check)
                {
                    let key = table.index_key(index, &values)?;
                    if key.0.iter().any(|part| matches!(part, KeyPart::Null)) && !index.primary {
                        continue;
                    }
                    let seen = batch_unique_keys
                        .as_mut()
                        .expect("unique checks enabled")
                        .get_mut(&index.name)
                        .expect("unique index batch should exist");
                    if !seen.insert(key) {
                        return Err(error(
                            crate::error::catalog::ER_DUP_ENTRY,
                            format!("duplicate entry for key '{}'", index.name),
                        ));
                    }
                }
                let routed = table.route_row_to_partition(&values)?;
                prepared.push(Prepared {
                    orig_idx,
                    storage_id: routed.storage_id,
                    values,
                });
            }

            let value_payload_capacity = |value: &Value| match value {
                Value::Decimal(value) | Value::String(value) => value.capacity(),
                Value::Bytes(value) | Value::Json(value) | Value::Geometry(value) => {
                    value.capacity()
                }
                Value::Vector(value) => value.capacity(),
                _ => 0,
            };
            let prepared_bytes = prepared
                .capacity()
                .saturating_mul(std::mem::size_of::<Prepared>())
                .saturating_add(
                    prepared
                        .iter()
                        .map(|row| {
                            row.values
                                .capacity()
                                .saturating_mul(std::mem::size_of::<Value>())
                                .saturating_add(
                                    row.values.iter().map(value_payload_capacity).sum::<usize>(),
                                )
                        })
                        .sum::<usize>(),
                );
            // `snapshot_values` clones every value once and the leaf buckets
            // subsequently retain the original rows. Charge both resident
            // copies by capacity for the full statement lifetime.
            let _partition_batch_charge =
                crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                    crate::engine::executor::mem_budget::ControlledMemoryCategory::PartitionDmlBatch,
                    prepared_bytes
                        .saturating_mul(2)
                        .saturating_add(
                            prepared
                                .len()
                                .saturating_mul(std::mem::size_of::<RowId>()),
                        ),
                )?;

            // Snapshot of prepared values for the table-level unique
            // constraint check (which probes each row's routed partition).
            let snapshot_values: Vec<Vec<Value>> =
                prepared.iter().map(|p| p.values.clone()).collect();
            // M4.B6 — batch NOT NULL / CHECK before the unique probe
            // and the per-partition heap writes. Same error ordering
            // as the row-by-row path the partition phase used to do
            // inline.
            self.check_not_null_constraints_batch(table, &snapshot_values)?;
            self.check_check_constraints_batch(table, &snapshot_values)?;
            if let Some(batch_unique_keys) = batch_unique_keys.as_ref() {
                self.lock_unique_keys_for_rows(txn.txn_id, table, &snapshot_values)?;
                self.check_batch_unique_constraints(
                    table,
                    txn,
                    &snapshot_values,
                    batch_unique_keys,
                )?;
            }

            // Phase 2: build an O(1) storage-id -> leaf-ordinal map and move
            // rows into preallocated leaf buckets. This avoids the old stable
            // sort and per-group full-row clones while retaining input-order
            // RowIds.
            let leaf_storage_ids = table
                .partitioning
                .as_ref()
                .expect("partitioned insert must have partition metadata")
                .storage_ids();
            let mut leaf_ordinals =
                crate::util::fast_hash::ah_hash_map_with_capacity(leaf_storage_ids.len());
            for (ordinal, storage_id) in leaf_storage_ids.iter().copied().enumerate() {
                leaf_ordinals.insert(storage_id, ordinal);
            }
            let mut buckets: Vec<Vec<Prepared>> =
                (0..leaf_storage_ids.len()).map(|_| Vec::new()).collect();
            let row_count = prepared.len();
            for row in prepared {
                let ordinal = *leaf_ordinals
                    .get(&row.storage_id)
                    .expect("partition router returned an unknown physical leaf");
                buckets[ordinal].push(row);
            }
            let mut row_ids: Vec<RowId> = vec![RowId::new(0, 0); row_count];
            let mut storage_ids = vec![0; row_count];

            let mut total_inserted: usize = 0;
            for (storage_id, bucket) in leaf_storage_ids.into_iter().zip(buckets) {
                if bucket.is_empty() {
                    continue;
                }
                let mut group_values: Vec<Vec<Value>> = Vec::with_capacity(bucket.len());
                let mut group_orig: Vec<usize> = Vec::with_capacity(bucket.len());
                for row in bucket {
                    group_orig.push(row.orig_idx);
                    group_values.push(row.values);
                }
                let row_headers = vec![RowHeader::visible(txn.txn_id); group_values.len()];
                let inserted = self.insert_mvcc_batch_externalized(
                    txn.txn_id,
                    storage_id,
                    &table.row_codec,
                    &row_headers,
                    &group_values,
                )?;
                // M1.A3 — same atomicity wrap as the unpartitioned
                // path: any failure between the heap write and the
                // undo append must physically reclaim the rows or the
                // txn rollback path can't reach them via the undo
                // chain.
                if let Err(err) = (|| -> Result<(), QueryError> {
                    self.lock_rows_nonblocking(txn.txn_id, storage_id, inserted.iter().copied())?;
                    self.push_insert_batch_undo_deferred(txn.txn_id, storage_id, &inserted)?;
                    Ok(())
                })() {
                    self.rollback_inserted_rows(txn.txn_id, storage_id, &inserted)?;
                    self.dml_stats
                        .record_rollback_inserted_rows(inserted.len() as u64);
                    return Err(err);
                }
                // Per-partition index entries.
                let mut index_entries = table
                    .indexes
                    .iter()
                    .map(|index| (index.name.clone(), Vec::new()))
                    .collect::<BTreeMap<_, Vec<(IndexKey, RowId)>>>();
                for (values, row_id) in group_values.iter().zip(inserted.iter()) {
                    for index in &table.indexes {
                        let entries = index_entries
                            .get_mut(&index.name)
                            .expect("index entry batch should exist");
                        for key in table.index_keys(index, values)? {
                            entries.push((key, *row_id));
                        }
                    }
                }
                self.insert_index_batches_deferred_at(
                    txn.txn_id,
                    storage_id,
                    table,
                    index_entries,
                )?;
                // M5.5 — keep any warm HNSW graph for this partition in
                // sync. The graph map is keyed by (table_id, storage_id,
                // index_name); RowIds are only unique within one
                // partition, so each partition has its own graph.
                if mode == BulkInsertMode::User {
                    self.defer_vector_insert_batch(
                        txn.txn_id,
                        table,
                        storage_id,
                        &group_values,
                        &inserted,
                    )?;
                }
                for (orig, rid) in group_orig.iter().zip(inserted.iter()) {
                    row_ids[*orig] = *rid;
                    storage_ids[*orig] = storage_id;
                }
                self.record_alter_copy_touched_rows(
                    txn.txn_id,
                    table.id,
                    storage_id,
                    inserted.iter().copied(),
                );
                total_inserted += inserted.len();
            }

            if mode == BulkInsertMode::User {
                self.record_non_temp_table_modifications(
                    txn.txn_id,
                    table,
                    total_inserted as u64,
                    total_inserted.min(i64::MAX as usize) as i64,
                )?;
                crate::binlog::record_write_rows(txn.txn_id, table, snapshot_values)?;
                self.dml_stats.record_inserts(total_inserted as u64);
            }
            Ok(InsertRowsResult {
                storage_ids,
                row_ids,
                first_insert_id,
            })
        })();
        result
    }

    pub fn insert_rows_bulk(
        &self,
        txn: TxnView,
        table_name: &str,
        rows: Vec<Vec<Value>>,
        skip_unique_check: bool,
    ) -> Result<Vec<RowId>, QueryError> {
        Ok(self
            .insert_rows_bulk_detailed(txn, table_name, rows, skip_unique_check)?
            .row_ids)
    }

    pub fn insert_rows_bulk_detailed(
        &self,
        txn: TxnView,
        table_name: &str,
        rows: Vec<Vec<Value>>,
        skip_unique_check: bool,
    ) -> Result<InsertRowsResult, QueryError> {
        let table = self.table_by_key(table_name).ok_or_else(|| {
            error(
                crate::error::catalog::ER_NO_SUCH_TABLE,
                format!("Table '{table_name}' doesn't exist"),
            )
        })?;
        self.check_dml_allowed_for_table(txn, table.id)?;
        self.insert_rows_bulk_detailed_for(txn, table, rows, skip_unique_check)
    }

    /// Bulk insert against an explicitly-supplied [`TableSchema`] rather
    /// than one resolved from the catalog by name. The partition-reorg
    /// build-then-swap path (§2.1) uses this to route rows into *shadow*
    /// partition storage ids described by a candidate schema that is not
    /// yet installed in the catalog — routing is driven entirely by
    /// `table.partitioning`, and heap/index access is by storage id, so
    /// the target files can be filled before the catalog commit point.
    /// The caller is responsible for any `check_dml_allowed_for_table`
    /// gating (skipped here because reorg runs internally under a DDL
    /// lock).
    pub(in crate::storage::core) fn insert_rows_bulk_detailed_for(
        &self,
        txn: TxnView,
        table: std::sync::Arc<TableSchema>,
        rows: Vec<Vec<Value>>,
        skip_unique_check: bool,
    ) -> Result<InsertRowsResult, QueryError> {
        self.insert_rows_bulk_detailed_for_mode(
            txn,
            table,
            rows,
            skip_unique_check,
            BulkInsertMode::User,
        )
    }

    /// Physical copy for partition DDL. These rows already exist logically,
    /// so the copy must not assign AUTO_INCREMENT values or publish logical
    /// row statistics, binlog events, and incremental vector updates.
    pub(in crate::storage::core) fn insert_rows_shadow_bulk_detailed_for(
        &self,
        txn: TxnView,
        table: std::sync::Arc<TableSchema>,
        rows: Vec<Vec<Value>>,
    ) -> Result<InsertRowsResult, QueryError> {
        self.insert_rows_bulk_detailed_for_mode(
            txn,
            table,
            rows,
            true,
            BulkInsertMode::PartitionShadow,
        )
    }

    fn insert_rows_bulk_detailed_for_mode(
        &self,
        txn: TxnView,
        table: std::sync::Arc<TableSchema>,
        rows: Vec<Vec<Value>>,
        skip_unique_check: bool,
        mode: BulkInsertMode,
    ) -> Result<InsertRowsResult, QueryError> {
        // Partitioned tables route through the partition-aware path
        // — `table.id` has no heap, only each partition's storage_id
        // does. `skip_unique_check` is honoured by the partition
        // path's prepare loop (which checks unique batch dedup but
        // skips on this flag).
        if table.is_partitioned() {
            return self.insert_rows_partitioned_detailed_with_mode(
                txn,
                &table,
                rows,
                skip_unique_check,
                mode,
            );
        }
        self.acquire_table_write_binding(txn.txn_id, &table)?;
        let result = (|| {
            let mut rows = rows;
            let first_insert_id = if mode == BulkInsertMode::User {
                self.assign_auto_increment_values(txn.txn_id, txn.connection_id, &table, &mut rows)?
            } else {
                0
            };
            let mut row_ids = Vec::with_capacity(rows.len());
            let mut prepared_rows = Vec::with_capacity(rows.len());
            if !skip_unique_check {
                let mut batch_unique_keys = table
                    .indexes
                    .iter()
                    .filter(|index| index.enforces_uniqueness())
                    .map(|index| (index.name.clone(), crate::util::fast_hash::ah_hash_set()))
                    .collect::<BTreeMap<_, crate::util::fast_hash::AhHashSet<IndexKey>>>();
                for mut values in rows {
                    coerce_row(&table, &mut values)?;
                    materialize_generated_columns(&table, &mut values, None)?;
                    for index in table
                        .indexes
                        .iter()
                        .filter(|index| index.enforces_uniqueness())
                    {
                        let key = table.index_key(index, &values)?;
                        if key.0.iter().any(|part| matches!(part, KeyPart::Null)) && !index.primary
                        {
                            continue;
                        }
                        let seen = batch_unique_keys
                            .get_mut(&index.name)
                            .expect("unique index batch should exist");
                        if !seen.insert(key) {
                            return Err(error(
                                crate::error::catalog::ER_DUP_ENTRY,
                                format!("duplicate entry for key '{}'", index.name),
                            ));
                        }
                    }
                    prepared_rows.push(values);
                }
                // M4.B6 — batch NOT NULL / CHECK; see canonical
                // INSERT path for rationale.
                self.check_not_null_constraints_batch(&table, &prepared_rows)?;
                self.check_check_constraints_batch(&table, &prepared_rows)?;
                self.lock_unique_keys_for_rows(txn.txn_id, &table, &prepared_rows)?;
                self.check_batch_unique_constraints(
                    &table,
                    txn,
                    &prepared_rows,
                    &batch_unique_keys,
                )?;
            } else {
                for mut values in rows {
                    coerce_row(&table, &mut values)?;
                    materialize_generated_columns(&table, &mut values, None)?;
                    prepared_rows.push(values);
                }
                // M4.B6 — skip_unique_check still needs NOT NULL /
                // CHECK enforcement on the prepared rows.
                self.check_not_null_constraints_batch(&table, &prepared_rows)?;
                self.check_check_constraints_batch(&table, &prepared_rows)?;
            }
            let row_headers = vec![RowHeader::visible(txn.txn_id); prepared_rows.len()];
            let inserted = self.insert_mvcc_batch_externalized(
                txn.txn_id,
                table.id,
                &table.row_codec,
                &row_headers,
                &prepared_rows,
            )?;
            // M1.A3 — bulk path: same atomicity wrap as the canonical
            // INSERT entry point.
            if let Err(err) = (|| -> Result<(), QueryError> {
                self.lock_rows_nonblocking(txn.txn_id, table.id, inserted.iter().copied())?;
                self.push_insert_batch_undo_deferred(txn.txn_id, table.id, &inserted)?;
                Ok(())
            })() {
                self.rollback_inserted_rows(txn.txn_id, table.id, &inserted)?;
                self.dml_stats
                    .record_rollback_inserted_rows(inserted.len() as u64);
                return Err(err);
            }
            self.insert_index_batches_deferred_for_rows(
                txn.txn_id,
                &table,
                &prepared_rows,
                &inserted,
            )?;
            if mode == BulkInsertMode::User {
                self.defer_vector_insert_batch(
                    txn.txn_id,
                    &table,
                    table.id,
                    &prepared_rows,
                    &inserted,
                )?;
            }
            row_ids.extend(inserted);
            // Foreign-key child-side existence check runs *after* the batch
            // is inserted so a row referencing an earlier row in the same
            // statement (self-referential FK) sees it. No-op when the table
            // has no outgoing FKs (always true for partitioned tables, which
            // cannot carry foreign keys). Locks matched parents to bar orphans.
            if !table.foreign_keys.is_empty() {
                self.fk_check_child_references_batch(&table, txn, &prepared_rows)?;
            }
            if mode == BulkInsertMode::User {
                self.record_alter_copy_touched_rows(
                    txn.txn_id,
                    table.id,
                    table.id,
                    row_ids.iter().copied(),
                );
                self.record_non_temp_table_modifications(
                    txn.txn_id,
                    &table,
                    row_ids.len() as u64,
                    row_ids.len().min(i64::MAX as usize) as i64,
                )?;
                crate::binlog::record_write_rows(txn.txn_id, &table, prepared_rows)?;
                self.dml_stats.record_inserts(row_ids.len() as u64);
            }
            Ok(InsertRowsResult {
                storage_ids: Vec::new(),
                row_ids,
                first_insert_id,
            })
        })();
        result
    }

    pub fn insert_pre_encoded_rows_heap_only_detailed(
        &self,
        txn: TxnView,
        table_name: &str,
        payloads: Vec<Vec<u8>>,
    ) -> Result<InsertRowsResult, QueryError> {
        let table = self.table_by_key(table_name).ok_or_else(|| {
            error(
                crate::error::catalog::ER_NO_SUCH_TABLE,
                format!("Table '{table_name}' doesn't exist"),
            )
        })?;
        self.check_dml_allowed_for_table(txn, table.id)?;
        // Pre-encoded row payloads can't be partition-routed without
        // decoding them first — the row bytes hide the column values
        // the partition function needs to see. Reject for partitioned
        // tables; LOAD DATA's encoded fast-path falls back to the
        // value-based path for these.
        if table.is_partitioned() {
            return Err(error(crate::error::catalog::ER_ILLEGAL_HA,
                "pre-encoded heap-only inserts are not supported on partitioned tables — use the value-based path",
            ));
        }
        if crate::binlog::enabled() {
            return Err(error(crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "pre-encoded heap-only inserts are not supported while binary logging is enabled; use the value-based path",
            ));
        }
        self.acquire_table_write_binding(txn.txn_id, &table)?;
        let result = (|| {
            self.preflight_insert_undo_admission(table.id, payloads.len())?;
            let inserted = self.with_heap_mut(table.id, |heap| {
                heap.insert_mvcc_batch_pre_encoded(txn.txn_id, payloads)
            })?;
            // The fast LOAD path deliberately keeps compact encoded payloads
            // opaque to the writer.  Until its projection batch is supplied,
            // mark just the touched zones Unknown rather than re-decoding rows
            // or risking an incomplete interval.
            // M1.A3 — pre-encoded LOAD DATA path: same atomicity wrap.
            if let Err(err) = (|| -> Result<(), QueryError> {
                self.push_insert_batch_undo_deferred(txn.txn_id, table.id, &inserted)?;
                Ok(())
            })() {
                self.rollback_inserted_rows(txn.txn_id, table.id, &inserted)?;
                self.dml_stats
                    .record_rollback_inserted_rows(inserted.len() as u64);
                return Err(err);
            }
            self.record_alter_copy_touched_rows(
                txn.txn_id,
                table.id,
                table.id,
                inserted.iter().copied(),
            );
            self.record_non_temp_table_modifications(
                txn.txn_id,
                &table,
                inserted.len() as u64,
                inserted.len().min(i64::MAX as usize) as i64,
            )?;
            self.dml_stats.record_inserts(inserted.len() as u64);
            Ok(InsertRowsResult {
                storage_ids: Vec::new(),
                row_ids: inserted,
                first_insert_id: 0,
            })
        })();
        result
    }

    pub fn insert_rows_heap_only_detailed(
        &self,
        txn: TxnView,
        table_name: &str,
        rows: Vec<Vec<Value>>,
    ) -> Result<InsertRowsResult, QueryError> {
        let table = self.table_by_key(table_name).ok_or_else(|| {
            error(
                crate::error::catalog::ER_NO_SUCH_TABLE,
                format!("Table '{table_name}' doesn't exist"),
            )
        })?;
        self.check_dml_allowed_for_table(txn, table.id)?;
        // Partitioned tables go through the partition-aware insert
        // path, which routes each row to the matching partition.
        // The LOAD DATA fast path's "heap-only / build indexes later"
        // optimization doesn't trivially compose with partition
        // routing (each partition needs its own index build), so we
        // accept a small overhead by going through the full insert
        // path. Future work: write to each partition's heap+index in
        // parallel from a single LOAD batch.
        if table.is_partitioned() {
            return self.insert_rows_partitioned_detailed(txn, &table, rows);
        }
        self.acquire_table_write_binding(txn.txn_id, &table)?;
        let result = (|| {
            let mut rows = rows;
            let first_insert_id = self.assign_auto_increment_values(
                txn.txn_id,
                txn.connection_id,
                &table,
                &mut rows,
            )?;
            let mut prepared_rows = Vec::with_capacity(rows.len());
            for mut values in rows {
                coerce_row(&table, &mut values)?;
                materialize_generated_columns(&table, &mut values, None)?;
                prepared_rows.push(values);
            }
            // M4.B6 — heap-only LOAD DATA fast path still owes the
            // user a NOT NULL / CHECK guarantee; do it in batch.
            self.check_not_null_constraints_batch(&table, &prepared_rows)?;
            self.check_check_constraints_batch(&table, &prepared_rows)?;

            let row_headers = vec![RowHeader::visible(txn.txn_id); prepared_rows.len()];
            let inserted = self.insert_mvcc_batch_externalized(
                txn.txn_id,
                table.id,
                &table.row_codec,
                &row_headers,
                &prepared_rows,
            )?;
            // M1.A3 — heap-only fast path shares the same atomicity
            // window: if undo append fails, the already-written heap
            // rows must be removed so they cannot escape the txn's
            // undo chain.
            if let Err(err) = (|| -> Result<(), QueryError> {
                self.push_insert_batch_undo_deferred(txn.txn_id, table.id, &inserted)?;
                Ok(())
            })() {
                self.rollback_inserted_rows(txn.txn_id, table.id, &inserted)?;
                self.dml_stats
                    .record_rollback_inserted_rows(inserted.len() as u64);
                return Err(err);
            }
            self.record_alter_copy_touched_rows(
                txn.txn_id,
                table.id,
                table.id,
                inserted.iter().copied(),
            );
            self.record_non_temp_table_modifications(
                txn.txn_id,
                &table,
                inserted.len() as u64,
                inserted.len().min(i64::MAX as usize) as i64,
            )?;
            crate::binlog::record_write_rows(txn.txn_id, &table, prepared_rows)?;
            self.dml_stats.record_inserts(inserted.len() as u64);
            Ok(InsertRowsResult {
                storage_ids: Vec::new(),
                row_ids: inserted,
                first_insert_id,
            })
        })();
        result
    }

    pub fn update_rows<F, G>(
        &self,
        txn: TxnView,
        table_name: &str,
        candidates: Option<Vec<RowId>>,
        predicate: F,
        apply_updates: G,
    ) -> Result<usize, QueryError>
    where
        F: FnMut(&[Value]) -> Result<bool, QueryError>,
        G: FnMut(&[Value]) -> Result<Vec<Value>, QueryError>,
    {
        self.update_rows_with_changed_columns(
            txn,
            table_name,
            candidates,
            None,
            predicate,
            apply_updates,
        )
    }

    pub fn update_rows_with_changed_columns<F, G>(
        &self,
        txn: TxnView,
        table_name: &str,
        candidates: Option<Vec<RowId>>,
        changed_columns: Option<&[usize]>,
        predicate: F,
        apply_updates: G,
    ) -> Result<usize, QueryError>
    where
        F: FnMut(&[Value]) -> Result<bool, QueryError>,
        G: FnMut(&[Value]) -> Result<Vec<Value>, QueryError>,
    {
        self.update_rows_with_changed_columns_inner(
            txn,
            table_name,
            DmlCandidates::RowIds(candidates),
            changed_columns,
            None,
            predicate,
            apply_updates,
            false,
        )
    }

    fn update_rows_with_changed_columns_inner<F, G>(
        &self,
        txn: TxnView,
        table_name: &str,
        candidates: DmlCandidates,
        changed_columns: Option<&[usize]>,
        noop_compare_columns: Option<&[usize]>,
        mut predicate: F,
        mut apply_updates: G,
        ignore_check_errors: bool,
    ) -> Result<usize, QueryError>
    where
        F: FnMut(&[Value]) -> Result<bool, QueryError>,
        G: FnMut(&[Value]) -> Result<Vec<Value>, QueryError>,
    {
        let table = self.table_by_key(table_name).ok_or_else(|| {
            error(
                crate::error::catalog::ER_NO_SUCH_TABLE,
                format!("Table '{table_name}' doesn't exist"),
            )
        })?;
        self.check_dml_allowed_for_table(txn, table.id)?;
        if table.is_partitioned() {
            return self.update_rows_partitioned(
                txn,
                &table,
                candidates.for_partitioned(),
                changed_columns,
                noop_compare_columns,
                &mut predicate,
                &mut apply_updates,
                ignore_check_errors,
            );
        }
        let candidates = candidates.for_unpartitioned(table.id);
        let generated_changed_columns =
            expand_changed_columns_for_generated(&table, changed_columns);
        let effective_changed_columns = generated_changed_columns.as_deref().or(changed_columns);
        // Foreign-key parent actions require per-row publication.
        if !table.fk_referenced_by.is_empty() {
            self.acquire_table_write_binding(txn.txn_id, &table)?;
            let rows = self.dml_candidate_rows(&table, txn, candidates)?;
            let mut count = 0;
            for row in rows {
                crate::engine::cancel::check_cancelled()?;
                if !predicate(&row.values)? {
                    continue;
                }
                if txn.txn_id != 0 {
                    self.lock_rows_nonblocking(txn.txn_id, table.id, std::iter::once(row.row_id))?;
                }
                let Some(mut current) = self.read_mvcc_row(table.id, row.row_id)? else {
                    continue;
                };
                self.check_dml_write_allowed(txn, table.id, &current)?;
                if current.header.is_deleted() {
                    continue;
                }
                Self::pad_full_row_to_schema(&mut current.values, &table);
                // Snapshot visibility is only a candidate filter. Re-evaluate
                // the predicate against the locked current version.
                if !predicate(&current.values)? {
                    continue;
                }
                let updated = match self.update_row_by_id_with_changed_columns_noop(
                    txn,
                    &table,
                    row.row_id,
                    changed_columns,
                    noop_compare_columns,
                    |vals| apply_updates(vals),
                ) {
                    Ok(updated) => updated,
                    Err(err) if ignore_check_errors && ignorable_update_error(&err) => {
                        crate::engine::api::push_statement_condition(
                            crate::error::ConditionLevel::Warning,
                            &err,
                        );
                        continue;
                    }
                    Err(err) => return Err(err),
                };
                if updated {
                    count += 1;
                }
            }
            return Ok(count);
        }
        self.acquire_table_write_binding(txn.txn_id, &table)?;
        let result = (|| {
            let rows = self.dml_candidate_rows(&table, txn, candidates)?;
            let mut updates = Vec::new();
            // Constraint probes only see keys already in the index. Reserve
            // new keys from earlier rows in this batch before any heap write.
            let batch_unique_indexes = if rows.len() > 1 {
                table
                    .indexes
                    .iter()
                    .enumerate()
                    .filter(|(_, index)| {
                        index.enforces_uniqueness()
                            && index_may_be_affected_by_columns(
                                &table,
                                index,
                                effective_changed_columns,
                            )
                    })
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            let mut batch_unique_keys = batch_unique_indexes
                .iter()
                .map(|_| crate::util::fast_hash::AhHashSet::<IndexKey>::default())
                .collect::<Vec<_>>();
            // Row locks and unique-key locks protect this statement-local
            // overlay until the accepted batch is published or rolled back.
            let mut released_unique_keys: Vec<crate::util::fast_hash::AhHashSet<RowId>> = (0
                ..table.indexes.len())
                .map(|_| Default::default())
                .collect();
            for row in rows {
                crate::engine::cancel::check_cancelled()?;
                if !predicate(&row.values)? {
                    continue;
                }
                // `row_id` is already known, so acquire the write lock before
                // materialising the row. The previous read-lock-reread shape
                // decoded every UPDATE candidate twice and, in the prepared
                // inline path, decoded a contended row before returning the
                // internal would-block marker. Row ids are not reused, so a
                // concurrently removed candidate is handled by the post-lock
                // read without making the early lock unsafe.
                if txn.txn_id != 0 {
                    self.lock_rows_nonblocking(txn.txn_id, table.id, std::iter::once(row.row_id))?;
                }
                let mut current = match self.read_mvcc_row(table.id, row.row_id)? {
                    Some(current) => current,
                    None => continue,
                };
                self.check_dml_write_allowed(txn, table.id, &current)?;
                if current.header.is_deleted() {
                    continue;
                }
                // The raw heap read returns the row at its original
                // stored width. Pad it out to the latest schema so the
                // UPDATE expression callback (which indexes by column
                // position) and the resulting heap re-write both see
                // the full Public-column complement. Pre-INSTANT-ADD
                // tables are unaffected because pad is a no-op when
                // `values.len() == columns.len()`.
                Self::pad_full_row_to_schema(&mut current.values, &table);
                // UPDATE is a locking current read. A concurrent writer may
                // have changed the row while this statement waited.
                if !predicate(&current.values)? {
                    continue;
                }
                let base_values = &current.values;
                let mut new_values = apply_updates(base_values)?;
                coerce_updated_row(&table, &mut new_values, changed_columns)?;
                materialize_generated_columns(&table, &mut new_values, effective_changed_columns)?;
                // MySQL no-op detection: matched but unchanged rows are
                // skipped (no write, no ON UPDATE refresh) — see
                // `update_row_by_id_with_changed_columns_noop`.
                if let Some(cols) = noop_compare_columns {
                    if row_unchanged_on(base_values, &new_values, cols) {
                        continue;
                    }
                }
                if let Err(err) = self.check_update_constraints_with_released_keys(
                    &table,
                    txn,
                    row.row_id,
                    base_values,
                    &new_values,
                    effective_changed_columns,
                    Some(&released_unique_keys),
                ) {
                    if ignore_check_errors && ignorable_update_error(&err) {
                        crate::engine::api::push_statement_condition(
                            crate::error::ConditionLevel::Warning,
                            &err,
                        );
                        continue;
                    }
                    return Err(err);
                }
                if !batch_unique_indexes.is_empty() {
                    let mut pending = Vec::new();
                    let mut duplicate = None;
                    for (set_pos, (_, index)) in batch_unique_indexes.iter().enumerate() {
                        let key = table.index_key(index, &new_values)?;
                        let old_key = table.index_key(index, base_values)?;
                        if key == old_key {
                            continue;
                        }
                        if !index.primary && key.0.iter().any(|part| matches!(part, KeyPart::Null))
                        {
                            pending.push((set_pos, None));
                            continue;
                        }
                        if batch_unique_keys[set_pos].contains(&key) {
                            duplicate = Some(duplicate_entry_error(&table, index, &key));
                            break;
                        }
                        pending.push((set_pos, Some(key)));
                    }
                    if let Some(err) = duplicate {
                        if ignore_check_errors {
                            crate::engine::api::push_statement_condition(
                                crate::error::ConditionLevel::Warning,
                                &err,
                            );
                            continue;
                        }
                        return Err(err);
                    }
                    for (set_pos, key) in pending {
                        if let Some(key) = key {
                            batch_unique_keys[set_pos].insert(key);
                        }
                        let index_pos = batch_unique_indexes[set_pos].0;
                        released_unique_keys[index_pos].insert(row.row_id);
                    }
                }
                updates.push(DmlUpdate {
                    row_id: row.row_id,
                    header: current.header,
                    old_values: current.values,
                    new_values,
                });
            }
            self.lock_update_rows(txn.txn_id, &table, &updates, effective_changed_columns)?;
            // M1.A2 — Track every chain `prepare_update_lob` allocates
            // for new row versions. Ownership transfers to the heap
            // row only after `write_heap_rows` lands; if either the
            // remaining `prepare_update_lob` calls or the batch heap
            // write fails partway, the txn rollback path runs the undo
            // `Update` arm which derives "new chain = current heap
            // external \ before_external" — but for not-yet-written
            // rows `current_ext` still matches `before_external`, so
            // it would leak. Free them explicitly here instead.
            let mut staged_new_chains: Vec<LobRef> = Vec::new();
            let patch_eligible = update_delta_enabled()
                && effective_changed_columns
                    .map(|c| !c.is_empty())
                    .unwrap_or(false);
            let mut patched_rows = vec![false; updates.len()];
            let patch_degree = crate::planner::default_parallel_degree().max(1) as usize;
            // Linear duplicate detection; the former prefix scan was O(N²)
            // even when sparse patches were disabled.
            let distinct_patch_rows = if patch_eligible && updates.len() >= 64 && patch_degree > 1 {
                let _row_ids_charge = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                    crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                    updates.len().saturating_mul(std::mem::size_of::<RowId>() + 1).saturating_mul(4),
                )?;
                let mut patch_row_ids = crate::util::fast_hash::AhHashSet::with_capacity_and_hasher(
                    updates.len(),
                    Default::default(),
                );
                let mut distinct = true;
                for update in &updates {
                    crate::engine::cancel::check_cancelled()?;
                    if !patch_row_ids.insert(update.row_id) {
                        distinct = false;
                        break;
                    }
                }
                distinct
            } else {
                false
            };
            if patch_eligible && updates.len() >= 64 && patch_degree > 1 && distinct_patch_rows {
                // Locks and semantic checks are complete. Sparse heap/WAL/undo
                // patches on distinct rows can use the configured fixed worker
                // topology; governor pressure never changes that topology.
                // This status/result scratch is non-spillable and returns a
                // clear HY001 instead of crossing MYDB_MEMORY_TARGET.
                let status_bytes = patched_rows
                    .capacity()
                    .saturating_mul(std::mem::size_of::<bool>())
                    .saturating_add(
                        updates
                            .len()
                            .saturating_mul(std::mem::size_of::<(usize, bool)>()),
                    )
                    .saturating_add(patch_degree.saturating_mul(512));
                let _patch_charge =
                    crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                        crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                        status_bytes,
                    )?;
                let chunk_rows = updates.len().div_ceil(patch_degree);
                let table_ref = &table;
                let cancellation = crate::engine::cancel::CancellationSnapshot::capture();
                let worker_results = std::thread::scope(|scope| {
                    let mut workers = Vec::with_capacity(patch_degree);
                    for (chunk_index, chunk) in updates.chunks(chunk_rows).enumerate() {
                        let cancellation = cancellation.clone();
                        workers.push(scope.spawn(move || {
                            let _cancel_scope = cancellation.enter();
                            let start = chunk_index * chunk_rows;
                            let mut results = Vec::with_capacity(chunk.len());
                            for (offset, update) in chunk.iter().enumerate() {
                                crate::engine::cancel::check_cancelled()?;
                                let patched = if !self.row_has_externalizable_changes(
                                    &table_ref.row_codec,
                                    &update.old_values,
                                    &update.new_values,
                                    effective_changed_columns.expect("patch columns"),
                                ) {
                                    self.try_in_place_update_delta(
                                        txn,
                                        table_ref,
                                        table_ref.id,
                                        update.row_id,
                                        update.header,
                                        &update.old_values,
                                        &update.new_values,
                                        effective_changed_columns.expect("patch columns"),
                                    )?
                                } else {
                                    false
                                };
                                results.push((start + offset, patched));
                            }
                            Ok::<_, QueryError>(results)
                        }));
                    }
                    let mut results = Vec::with_capacity(workers.len());
                    for worker in workers {
                        results.push(worker.join().map_err(|_| {
                            error(
                                crate::error::catalog::ER_UNKNOWN_ERROR,
                                "parallel sparse-update worker panicked",
                            )
                        })??);
                    }
                    Ok::<_, QueryError>(results)
                })?;
                for worker in worker_results {
                    for (index, patched) in worker {
                        patched_rows[index] = patched;
                    }
                }
            }
            // Bound the non-interruptible undo/LOB/heap publication unit.
            // Never cancel halfway through a heap batch: staged LOB ownership
            // transfers only when that batch has finished.
            for (chunk_index, chunk) in updates.chunks(256).enumerate() {
                crate::engine::cancel::check_cancelled()?;
                let prepare_result = (|| -> Result<Vec<HeapWrite>, QueryError> {
                    let mut heap_writes = Vec::with_capacity(chunk.len());
                    for (offset, update) in chunk.iter().enumerate() {
                        let update_index = chunk_index * 256 + offset;
                        // Phase 2e: attempt the column-level patch path
                        // per row. Rows that succeed are written in place
                        // immediately (heap + WAL + UpdateDelta undo);
                        // rows that bail out fall through to the legacy
                        // prepare_update_lob + batch heap write below.
                        if patched_rows[update_index] {
                            continue;
                        }
                        if patch_eligible
                            && !self.row_has_externalizable_changes(
                                &table.row_codec,
                                &update.old_values,
                                &update.new_values,
                                effective_changed_columns.unwrap(),
                            )
                        {
                            let cols = effective_changed_columns.unwrap();
                            if self.try_in_place_update_delta(
                                txn,
                                &table,
                                table.id,
                                update.row_id,
                                update.header,
                                &update.old_values,
                                &update.new_values,
                                cols,
                            )? {
                                // Row was patched in place; nothing more
                                // to do for this entry. Continue to the
                                // next update without adding to heap_writes.
                                continue;
                            }
                        }
                        let (undo_id, new_external, newly_allocated) = self.prepare_update_lob(
                            txn.txn_id,
                            table.id,
                            &table.row_codec,
                            update.row_id,
                            update.header,
                            &update.old_values,
                            &update.new_values,
                            effective_changed_columns,
                        )?;
                        // BUG-2 — stage only the freshly-allocated chains for
                        // failure cleanup. Reused old chains in `new_external`
                        // are still referenced by the (not-yet-overwritten)
                        // live row; freeing them on a bail-out is a UAF.
                        staged_new_chains.extend_from_slice(&newly_allocated);
                        heap_writes.push(HeapWrite {
                            row_id: update.row_id,
                            header: RowHeader {
                                flags: 0,
                                last_txn_id: txn.txn_id,
                                undo_ptr: Some(undo_id),
                            },
                            values: update.new_values.clone(),
                            external: new_external,
                        });
                    }
                    Ok(heap_writes)
                })();
                let heap_writes = match prepare_result {
                    Ok(writes) => writes,
                    Err(err) => {
                        self.free_lob_chains(table.id, &staged_new_chains);
                        return Err(err);
                    }
                };
                if let Err(err) = self.write_heap_rows(txn.txn_id, table.id, &heap_writes) {
                    self.free_lob_chains(table.id, &staged_new_chains);
                    return Err(err);
                }
                staged_new_chains.clear();
            }
            crate::engine::cancel::check_cancelled()?;
            // Covers both rows patched in place above and rows emitted through
            // the batch heap writer. Only changed/generated columns contribute
            // new bounds; UPDATE increments stale rather than live-row count.
            let zone_row_ids = updates.iter().map(|u| u.row_id).collect::<Vec<_>>();
            let zone_rows = updates
                .iter()
                .map(|u| u.new_values.as_slice())
                .collect::<Vec<_>>();
            self.merge_zone_map_updated_rows(
                txn.txn_id,
                table.id,
                &zone_row_ids,
                &zone_rows,
                effective_changed_columns,
            )?;
            // Ownership transferred to the heap rows — staging cleared.
            staged_new_chains.clear();
            self.update_changed_index_batches_deferred(
                txn.txn_id,
                &table,
                &updates,
                effective_changed_columns,
            )?;
            // ANN structures are derived, non-MVCC state. Publish replacements
            // only after commit so rollback/savepoint/XA rollback cannot leave
            // a permanent tombstone. One statement is an atomic rollback unit,
            // therefore its final undo head is a valid reachability marker for
            // every row changed by this batch.
            if let Some(vector_undo_id) = self.deferred_ann_undo_head(txn.txn_id, &table)? {
                for update in &updates {
                    self.defer_vector_update(
                        txn.txn_id,
                        vector_undo_id,
                        &table,
                        update.row_id,
                        table.id,
                        Some(&update.old_values),
                        &update.new_values,
                    )?;
                }
            }
            // B7 — batch the per-row AI cache hook into a single
            // pass that touches the per-table counter + compact redo at
            // most once for the entire UPDATE. Replaces the per-row
            // `cache_auto_increment_from_updated_values` loop.
            self.cache_auto_increment_from_updates_batch(
                &table,
                &updates,
                effective_changed_columns,
            )?;
            self.record_alter_copy_touched_rows(
                txn.txn_id,
                table.id,
                table.id,
                updates.iter().map(|update| update.row_id),
            );
            self.record_non_temp_table_modifications(txn.txn_id, &table, updates.len() as u64, 0)?;
            if crate::binlog::should_record_table_rows(&table) {
                crate::binlog::record_update_rows(
                    txn.txn_id,
                    &table,
                    updates
                        .iter()
                        .map(|update| (update.old_values.clone(), update.new_values.clone()))
                        .collect(),
                )?;
            }
            self.dml_stats.record_updates(updates.len() as u64);
            crate::engine::cancel::check_cancelled()?;
            Ok(updates.len())
        })();
        result
    }

    pub fn delete_rows<F>(
        &self,
        txn: TxnView,
        table_name: &str,
        candidates: Option<Vec<RowId>>,
        predicate: F,
    ) -> Result<usize, QueryError>
    where
        F: FnMut(&[Value]) -> Result<bool, QueryError>,
    {
        self.delete_rows_inner(
            txn,
            table_name,
            DmlCandidates::RowIds(candidates),
            predicate,
        )
    }

    pub fn delete_rows_with_origin<F>(
        &self,
        txn: TxnView,
        table_name: &str,
        candidates: Option<Vec<crate::storage::DmlRowRef>>,
        predicate: F,
    ) -> Result<usize, QueryError>
    where
        F: FnMut(&[Value]) -> Result<bool, QueryError>,
    {
        self.delete_rows_inner(
            txn,
            table_name,
            DmlCandidates::WithOrigin(candidates),
            predicate,
        )
    }

    fn delete_rows_inner<F>(
        &self,
        txn: TxnView,
        table_name: &str,
        candidates: DmlCandidates,
        mut predicate: F,
    ) -> Result<usize, QueryError>
    where
        F: FnMut(&[Value]) -> Result<bool, QueryError>,
    {
        let table = self.table_by_key(table_name).ok_or_else(|| {
            error(
                crate::error::catalog::ER_NO_SUCH_TABLE,
                format!("Table '{table_name}' doesn't exist"),
            )
        })?;
        self.check_dml_allowed_for_table(txn, table.id)?;
        if table.is_partitioned() {
            return self.delete_rows_partitioned(
                txn,
                &table,
                candidates.for_partitioned(),
                &mut predicate,
            );
        }
        let candidates = candidates.for_unpartitioned(table.id);
        // When other tables reference this one via FK, route each row
        // through `delete_row_by_id` so parent-side enforcement (RESTRICT /
        // CASCADE / SET NULL) and the orphan-blocking row lock run per row.
        // The fast batch path below stays untouched for the common case of
        // a table no FK points at.
        if !table.fk_referenced_by.is_empty() {
            self.acquire_table_write_binding(txn.txn_id, &table)?;
            let rows = self.dml_candidate_rows(&table, txn, candidates)?;
            let mut count = 0;
            for row in rows {
                crate::engine::cancel::check_cancelled()?;
                if !predicate(&row.values)? {
                    continue;
                }
                if txn.txn_id != 0 {
                    self.lock_rows_nonblocking(txn.txn_id, table.id, std::iter::once(row.row_id))?;
                }
                let Some(mut current) = self.read_mvcc_row(table.id, row.row_id)? else {
                    continue;
                };
                self.check_dml_write_allowed(txn, table.id, &current)?;
                if current.header.is_deleted() {
                    continue;
                }
                Self::pad_full_row_to_schema(&mut current.values, &table);
                if !predicate(&current.values)? {
                    continue;
                }
                if self.delete_row_by_id(txn, &table, row.row_id)? {
                    count += 1;
                }
            }
            return Ok(count);
        }
        self.acquire_table_write_binding(txn.txn_id, &table)?;
        let rows = self.dml_candidate_rows(&table, txn, candidates)?;
        let mut deletes = Vec::new();
        for row in rows {
            crate::engine::cancel::check_cancelled()?;
            if !predicate(&row.values)? {
                continue;
            }
            if txn.txn_id != 0 {
                self.lock_rows_nonblocking(txn.txn_id, table.id, std::iter::once(row.row_id))?;
            }
            let Some(mut current) = self.read_mvcc_row(table.id, row.row_id)? else {
                continue;
            };
            self.check_dml_write_allowed(txn, table.id, &current)?;
            if current.header.is_deleted() {
                continue;
            }
            Self::pad_full_row_to_schema(&mut current.values, &table);
            if !predicate(&current.values)? {
                continue;
            }
            deletes.push(DmlDelete {
                row_id: row.row_id,
                header: current.header,
                values: current.values,
            });
        }
        self.lock_delete_rows(txn.txn_id, &table, &deletes)?;
        let mut heap_writes = Vec::with_capacity(deletes.len());
        for chunk in deletes.chunks(256) {
            crate::engine::cancel::check_cancelled()?;
            let start = heap_writes.len();
            for delete in chunk {
                let (undo_id, tombstone) = self.prepare_delete_lob(
                    txn.txn_id,
                    table.id,
                    delete.row_id,
                    delete.header,
                    &delete.values,
                )?;
                heap_writes.push(HeapWrite {
                    row_id: delete.row_id,
                    header: RowHeader {
                        flags: MVCC_FLAG_DELETED,
                        last_txn_id: txn.txn_id,
                        undo_ptr: Some(undo_id),
                    },
                    values: tombstone,
                    external: Vec::new(),
                });
            }
            self.write_heap_rows(txn.txn_id, table.id, &heap_writes[start..])?;
        }
        crate::engine::cancel::check_cancelled()?;
        let deleted_row_ids = deletes
            .iter()
            .map(|delete| delete.row_id)
            .collect::<Vec<_>>();
        self.note_zone_map_deletes(txn.txn_id, table.id, &deleted_row_ids)?;
        self.delete_index_batches_deferred(txn.txn_id, &table, &deletes)?;
        // Graph tombstones are commit-deferred: unlike the heap/index paths
        // they have no MVCC undo, so applying one here would survive ROLLBACK.
        for (delete, write) in deletes.iter().zip(&heap_writes) {
            if let Some(undo_id) = write.header.undo_ptr {
                self.defer_vector_delete(
                    txn.txn_id,
                    undo_id,
                    &table,
                    &delete.values,
                    delete.row_id,
                    table.id,
                )?;
            }
        }
        self.record_alter_copy_touched_rows(
            txn.txn_id,
            table.id,
            table.id,
            deletes.iter().map(|delete| delete.row_id),
        );
        self.record_non_temp_table_modifications(
            txn.txn_id,
            &table,
            deletes.len() as u64,
            -(deletes.len().min(i64::MAX as usize) as i64),
        )?;
        if crate::binlog::should_record_table_rows(&table) {
            crate::binlog::record_delete_rows(
                txn.txn_id,
                &table,
                deletes.iter().map(|delete| delete.values.clone()).collect(),
            )?;
        }
        self.dml_stats.record_deletes(deletes.len() as u64);
        crate::engine::cancel::check_cancelled()?;
        Ok(deletes.len())
    }

    pub fn delete_row_by_id(
        &self,
        txn: TxnView,
        table: &TableSchema,
        row_id: RowId,
    ) -> Result<bool, QueryError> {
        self.check_dml_allowed_for_table(txn, table.id)?;
        self.acquire_table_write_binding(txn.txn_id, &table)?;
        let storage_id = self.locate_row_storage_id(table, row_id)?;
        self.delete_row_by_id_at(txn, table, storage_id, row_id)
    }

    /// Like [`delete_row_by_id`], but with the owning partition's
    /// `storage_id` supplied by the caller. Partitioned-table `row_id`s
    /// collide across partitions, so multi-table DELETE (which scans each
    /// partition and remembers the origin) must delete the exact partition
    /// row rather than let `locate_row_storage_id` probe and possibly hit a
    /// same-numbered row in the wrong partition.
    pub fn delete_row_by_id_at(
        &self,
        txn: TxnView,
        table: &TableSchema,
        storage_id: u64,
        row_id: RowId,
    ) -> Result<bool, QueryError> {
        self.delete_row_by_id_at_if(txn, table, storage_id, row_id, |_| Ok(true))
    }

    /// Delete an explicitly identified row only if `predicate` still matches
    /// after any lock wait and the latest committed/current version has been
    /// re-read. This closes the DML LIMIT under-fill/wrong-row window caused
    /// by stale ordered-index candidates.
    pub fn delete_row_by_id_at_if<F>(
        &self,
        txn: TxnView,
        table: &TableSchema,
        storage_id: u64,
        row_id: RowId,
        mut predicate: F,
    ) -> Result<bool, QueryError>
    where
        F: FnMut(&[Value]) -> Result<bool, QueryError>,
    {
        self.check_dml_allowed_for_table(txn, table.id)?;
        self.acquire_table_write_binding(txn.txn_id, &table)?;
        // Lock the known row id before decoding its payload. Besides removing
        // the normal read-lock-reread duplication, this makes a contended
        // nonblocking prepared DELETE fail before row materialisation.
        let held_before =
            txn.txn_id != 0 && self.txn_holds_row_lock(txn.txn_id, storage_id, row_id);
        if txn.txn_id != 0 {
            self.acquire_row_lock_blocking(txn.txn_id, storage_id, row_id, LockMode::Exclusive)?;
        }
        let Some(current) = self.read_mvcc_row(storage_id, row_id)? else {
            if txn.txn_id != 0 && !held_before {
                self.release_new_point_row_lock(txn.txn_id, storage_id, row_id);
            }
            return Ok(false);
        };
        if current.header.is_deleted() {
            if txn.txn_id != 0 && !held_before {
                self.release_new_point_row_lock(txn.txn_id, storage_id, row_id);
            }
            return Ok(false);
        }
        let mut predicate_values = current.values.clone();
        Self::pad_full_row_to_schema(&mut predicate_values, table);
        if !predicate(&predicate_values)? {
            if txn.txn_id != 0 && !held_before {
                self.release_new_point_row_lock(txn.txn_id, storage_id, row_id);
            }
            return Ok(false);
        }
        let index_values = &current.values;
        self.lock_unique_keys_for_values(txn.txn_id, table, index_values)?;
        // Foreign-key parent-side enforcement. Take an exclusive row lock
        // first (only when this table is referenced) so a concurrent child
        // INSERT/UPDATE holding a shared lock on this row blocks us until
        // it commits — closing the orphan window. Then apply RESTRICT /
        // CASCADE / SET NULL to referencing children before removing the row.
        if !table.fk_referenced_by.is_empty() {
            if txn.txn_id != 0 {
                self.acquire_row_lock_blocking(
                    txn.txn_id,
                    storage_id,
                    row_id,
                    LockMode::Exclusive,
                )?;
            }
            self.fk_enforce_parent_delete(table, txn, &current.values)?;
        }
        // Capture the old version's off-page chain descriptors (no
        // materialization) so undo owns them and purge can free them.
        // The tombstone and the undo before-image store the externalized
        // columns as NULL — DELETE never copies or re-inlines LOB bytes;
        // old snapshots read the pre-image via `before_external`.
        let before_external = self.read_row_external_refs(storage_id, row_id)?;
        let mut stored_values = current.values.clone();
        for (i, _) in &before_external {
            if let Some(slot) = stored_values.get_mut(*i) {
                *slot = Value::Null;
            }
        }
        let undo_id = self.push_undo_ext(
            txn.txn_id,
            UndoKind::Delete,
            storage_id,
            row_id,
            Some(current.header),
            Some(stored_values.clone()),
            before_external,
        )?;
        self.publish_wal_before_eager_heap_write(txn.txn_id)?;
        self.with_heap_mut(storage_id, |heap| {
            heap.write_mvcc(
                txn.txn_id,
                row_id,
                RowHeader {
                    flags: MVCC_FLAG_DELETED,
                    last_txn_id: txn.txn_id,
                    undo_ptr: Some(undo_id),
                },
                &stored_values,
            )
        })?;
        self.note_zone_map_deletes(txn.txn_id, storage_id, &[row_id])?;
        self.delete_indexes_deferred_at(txn.txn_id, storage_id, table, index_values, row_id)?;
        self.defer_vector_delete(txn.txn_id, undo_id, table, index_values, row_id, storage_id)?;
        self.record_alter_copy_touched_rows(
            txn.txn_id,
            table.id,
            storage_id,
            std::iter::once(row_id),
        );
        self.record_non_temp_table_modifications(txn.txn_id, table, 1, -1)?;
        crate::binlog::record_delete_rows(txn.txn_id, table, vec![current.values])?;
        self.dml_stats.record_deletes(1);
        Ok(true)
    }

    /// Find the partition's `storage_id` that owns `row_id`. For an
    /// unpartitioned table this returns `table.id`. For a partitioned
    /// table we probe each partition's heap; the first one that has a
    /// live row at the given row_id wins. The probe is cheap because
    /// each `read_mvcc_row` only fetches one page through the buffer
    /// pool. Worst case is O(partitions) per row.
    pub(crate) fn locate_row_storage_id(
        &self,
        table: &TableSchema,
        row_id: RowId,
    ) -> Result<u64, QueryError> {
        if !table.is_partitioned() {
            return Ok(table.id);
        }
        let storage_ids = table.partitioning.as_ref().unwrap().storage_ids();
        // When the planner has installed a partition prune set,
        // restrict the probe to only those storage_ids. RowId is
        // ambiguous across partitions (each partition starts row_id
        // numbering at (0,1,0)), so probing partitions outside the
        // prune set can find a row with the same row_id but a
        // different logical identity — exactly the bug this exists
        // to avoid.
        let allowed: Option<Vec<u64>> =
            crate::engine::executor::partition_router::current_pruned_partitions();
        for storage_id in storage_ids {
            if let Some(ref allowed) = allowed {
                if !allowed.contains(&storage_id) {
                    continue;
                }
            }
            if self.read_mvcc_row(storage_id, row_id)?.is_some() {
                return Ok(storage_id);
            }
        }
        // No partition found — fall back to `table.id`; the caller will
        // treat the missing row as a no-op (matches the row-gone case).
        Ok(table.id)
    }

    pub fn update_row_by_id<F>(
        &self,
        txn: TxnView,
        table: &TableSchema,
        row_id: RowId,
        apply_updates: F,
    ) -> Result<bool, QueryError>
    where
        F: FnMut(&[Value]) -> Result<Vec<Value>, QueryError>,
    {
        self.update_row_by_id_with_changed_columns(txn, table, row_id, None, apply_updates)
    }

    /// Batch counterpart of [`update_rows_with_changed_columns`] with
    /// MySQL no-op detection — see
    /// [`update_row_by_id_with_changed_columns_noop`] for the semantics of
    /// `noop_compare_columns`.
    pub fn update_rows_with_changed_columns_noop<F, G>(
        &self,
        txn: TxnView,
        table_name: &str,
        candidates: Option<Vec<RowId>>,
        changed_columns: Option<&[usize]>,
        noop_compare_columns: Option<&[usize]>,
        predicate: F,
        apply_updates: G,
    ) -> Result<usize, QueryError>
    where
        F: FnMut(&[Value]) -> Result<bool, QueryError>,
        G: FnMut(&[Value]) -> Result<Vec<Value>, QueryError>,
    {
        self.update_rows_with_changed_columns_inner(
            txn,
            table_name,
            DmlCandidates::RowIds(candidates),
            changed_columns,
            noop_compare_columns,
            predicate,
            apply_updates,
            false,
        )
    }

    /// Account for logical point UPDATEs that a homogeneous multi-statement
    /// batch coalesced into an already-recorded physical row rewrite.  The
    /// zone map deliberately keeps the single physical mutation: intermediate
    /// uncommitted values are never visible to another transaction and do not
    /// create additional stale envelopes.  DML/status and ANALYZE thresholds,
    /// however, count every successful child statement just as the scalar
    /// execution path does.
    pub(crate) fn record_coalesced_point_updates(
        &self,
        txn_id: u64,
        table: &TableSchema,
        additional_updates: u64,
    ) -> Result<(), QueryError> {
        if additional_updates == 0 {
            return Ok(());
        }
        self.record_non_temp_table_modifications(txn_id, table, additional_updates, 0)?;
        self.dml_stats.record_updates(additional_updates);
        Ok(())
    }

    /// Origin-preserving batch UPDATE used by executor index candidates.
    /// Partition heaps allocate RowIds independently, so carrying the storage
    /// id is required to avoid both missed rows and cross-partition aliases.
    #[allow(clippy::too_many_arguments)]
    pub fn update_rows_with_changed_columns_noop_at<F, G>(
        &self,
        txn: TxnView,
        table_name: &str,
        candidates: Option<Vec<crate::storage::DmlRowRef>>,
        changed_columns: Option<&[usize]>,
        noop_compare_columns: Option<&[usize]>,
        predicate: F,
        apply_updates: G,
        ignore_check_errors: bool,
    ) -> Result<usize, QueryError>
    where
        F: FnMut(&[Value]) -> Result<bool, QueryError>,
        G: FnMut(&[Value]) -> Result<Vec<Value>, QueryError>,
    {
        self.update_rows_with_changed_columns_inner(
            txn,
            table_name,
            DmlCandidates::WithOrigin(candidates),
            changed_columns,
            noop_compare_columns,
            predicate,
            apply_updates,
            ignore_check_errors,
        )
    }

    pub fn update_row_by_id_with_changed_columns<F>(
        &self,
        txn: TxnView,
        table: &TableSchema,
        row_id: RowId,
        changed_columns: Option<&[usize]>,
        apply_updates: F,
    ) -> Result<bool, QueryError>
    where
        F: FnMut(&[Value]) -> Result<Vec<Value>, QueryError>,
    {
        self.update_row_by_id_with_changed_columns_noop(
            txn,
            table,
            row_id,
            changed_columns,
            None,
            apply_updates,
        )
    }

    /// Like [`update_row_by_id_with_changed_columns`], but with MySQL
    /// no-op detection: when `noop_compare_columns` is `Some(cols)` and
    /// none of `cols` differ between the stored row and the candidate new
    /// row (compared *after* coercion + generated-column materialisation),
    /// the row is left untouched and `Ok(false)` is returned. This is the
    /// single source of truth for the "Changed" side of MySQL's
    /// `Rows matched: N Changed: M` accounting and for suppressing
    /// `ON UPDATE CURRENT_TIMESTAMP` refresh on rows that don't actually
    /// change. Callers that must always write (FK cascade, internal
    /// rewrites) pass `None`, preserving the historical behaviour.
    pub fn update_row_by_id_with_changed_columns_noop<F>(
        &self,
        txn: TxnView,
        table: &TableSchema,
        row_id: RowId,
        changed_columns: Option<&[usize]>,
        noop_compare_columns: Option<&[usize]>,
        apply_updates: F,
    ) -> Result<bool, QueryError>
    where
        F: FnMut(&[Value]) -> Result<Vec<Value>, QueryError>,
    {
        self.update_row_by_id_with_changed_columns_noop_at(
            txn,
            table,
            None,
            row_id,
            changed_columns,
            noop_compare_columns,
            apply_updates,
        )
    }

    /// Like [`update_row_by_id_with_changed_columns_noop`], but with the
    /// owning partition's `storage_id` supplied by the caller
    /// (`storage_id_override`). Multi-table UPDATE on a partitioned table
    /// must pass the origin partition so a `row_id` that collides across
    /// partitions is written in the correct partition — `None` falls back
    /// to the ambiguous `locate_row_storage_id` probe.
    #[allow(clippy::too_many_arguments)]
    pub fn update_row_by_id_with_changed_columns_noop_at<F>(
        &self,
        txn: TxnView,
        table: &TableSchema,
        storage_id_override: Option<u64>,
        row_id: RowId,
        changed_columns: Option<&[usize]>,
        noop_compare_columns: Option<&[usize]>,
        apply_updates: F,
    ) -> Result<bool, QueryError>
    where
        F: FnMut(&[Value]) -> Result<Vec<Value>, QueryError>,
    {
        self.update_row_by_id_with_changed_columns_noop_at_with_affected_indexes::<_, false>(
            txn,
            table,
            storage_id_override,
            row_id,
            changed_columns,
            noop_compare_columns,
            None,
            apply_updates,
            None,
        )
    }

    /// Point write with normalized images for AFTER triggers / RETURNING.
    /// Images are absent for a vanished row; an unchanged row returns OLD twice.
    /// The apply callback runs once, under the transaction row lock and outside
    /// heap/index handles. Ordinary and prepared writes do not allocate images.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn update_row_with_images_at<F>(
        &self,
        txn: TxnView,
        table: &TableSchema,
        storage_id: Option<u64>,
        row_id: RowId,
        changed_columns: Option<&[usize]>,
        noop_compare_columns: Option<&[usize]>,
        apply: F,
    ) -> Result<(bool, Option<(Vec<Value>, Vec<Value>)>), QueryError>
    where
        F: FnMut(&[Value]) -> Result<Vec<Value>, QueryError>,
    {
        let mut images = None;
        let changed = self
            .update_row_by_id_with_changed_columns_noop_at_with_affected_indexes::<_, true>(
                txn,
                table,
                storage_id,
                row_id,
                changed_columns,
                noop_compare_columns,
                None,
                apply,
                Some(&mut images),
            )?;
        Ok((changed, images))
    }

    /// Prepared exact-point UPDATE variant. `affected_index_positions` was
    /// resolved from the same schema version as `table`; callers are guarded
    /// by prepared-plan schema invalidation. Ordinary UPDATE paths keep using
    /// the conservative runtime dependency resolver above.
    pub fn update_prepared_row_by_id_with_changed_columns_noop<F>(
        &self,
        txn: TxnView,
        table: &TableSchema,
        row_id: RowId,
        changed_columns: Option<&[usize]>,
        noop_compare_columns: Option<&[usize]>,
        affected_index_positions: &[usize],
        apply_updates: F,
    ) -> Result<bool, QueryError>
    where
        F: FnMut(&[Value]) -> Result<Vec<Value>, QueryError>,
    {
        self.update_row_by_id_with_changed_columns_noop_at_with_affected_indexes::<_, false>(
            txn,
            table,
            None,
            row_id,
            changed_columns,
            noop_compare_columns,
            Some(affected_index_positions),
            apply_updates,
            None,
        )
    }

    /// Allocation-lean prepared point UPDATE for the common case where no
    /// unique key, parent-FK key, partition key, vector key, or binlog image
    /// depends on the changed columns. Ordinary nonunique indexes are maintained
    /// from the projected row, which includes all their key components.
    /// The decoded row buffer is owned here,
    /// so mutate it directly and retain only the changed columns' before-image
    /// for sparse undo. `Ok(None)` means the conservative preconditions or the
    /// raw heap patch did not hold; the caller must execute the ordinary path.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_update_prepared_row_by_id_in_place_delta_noop<F>(
        &self,
        txn: TxnView,
        table: &TableSchema,
        row_id: RowId,
        changed_columns: Option<&[usize]>,
        noop_compare_columns: Option<&[usize]>,
        affected_index_positions: &[usize],
        required_columns: &[usize],
        decode_plan: Option<&RowDecodePlan>,
        apply_updates: F,
    ) -> Result<Option<bool>, QueryError>
    where
        F: FnMut(&mut [Value]) -> Result<(), QueryError>,
    {
        self.try_update_prepared_row_by_id_in_place_delta_noop_if(
            txn,
            table,
            row_id,
            changed_columns,
            noop_compare_columns,
            affected_index_positions,
            required_columns,
            decode_plan,
            None,
            apply_updates,
        )
    }

    /// Evaluate a range predicate against the locked current projected row.
    /// A failed patch falls back with the already evaluated values, so volatile
    /// assignments and warning-producing expressions are never evaluated twice.
    #[allow(clippy::too_many_arguments, clippy::type_complexity)]
    pub(crate) fn try_update_prepared_row_by_id_in_place_delta_noop_if<F>(
        &self,
        txn: TxnView,
        table: &TableSchema,
        row_id: RowId,
        changed_columns: Option<&[usize]>,
        noop_compare_columns: Option<&[usize]>,
        affected_index_positions: &[usize],
        required_columns: &[usize],
        decode_plan: Option<&RowDecodePlan>,
        mut predicate: Option<&mut dyn FnMut(&[Value]) -> Result<bool, QueryError>>,
        mut apply_updates: F,
    ) -> Result<Option<bool>, QueryError>
    where
        F: FnMut(&mut [Value]) -> Result<(), QueryError>,
    {
        let generated_changed_columns =
            expand_changed_columns_for_generated(table, changed_columns);
        let effective_changed_columns = generated_changed_columns.as_deref().or(changed_columns);
        let Some(effective_changed_columns) =
            effective_changed_columns.filter(|cols| !cols.is_empty())
        else {
            return Ok(None);
        };
        if !update_delta_enabled()
            || table.is_partitioned()
            || affected_index_positions.iter().any(|&position| {
                table.indexes.get(position).is_none_or(|index| {
                    index.unique
                        || index.primary
                        || index.is_fulltext()
                        || index.has_multi_valued_parts()
                        || index.is_spatial()
                        || index.is_vector()
                        || index.key_expressions.iter().any(Option::is_some)
                })
            })
            || crate::binlog::should_record_table_rows(table)
            || !table.check_constraints.is_empty()
            // An outgoing FK only needs the parent existence check when one
            // of its child columns changes.  Treating the mere presence of
            // an FK as a blocker forced unrelated counter/attribute updates
            // to decode and clone the complete row.  The sparse row contains
            // every changed column, so an unaffected FK is semantically a
            // no-op here; affected (including multi-column) FKs retain the
            // full-row path, which supplies all remaining key columns.
            || table
                .foreign_keys
                .iter()
                .any(|fk| fk.affected_by_changed(effective_changed_columns))
            || table.columns.iter().any(|column| column.generated_expr.is_some())
        {
            return Ok(None);
        }
        if Self::fk_parent_update_may_affect_references(table, Some(effective_changed_columns))
            || self.row_has_externalizable_changes(
                &table.row_codec,
                &[],
                &[],
                effective_changed_columns,
            )
        {
            return Ok(None);
        }

        self.check_dml_allowed_for_table(txn, table.id)?;
        self.acquire_table_write_binding(txn.txn_id, &table)?;
        let storage_id = self.locate_row_storage_id(table, row_id)?;
        let held_before = predicate.is_none()
            || txn.txn_id == 0
            || self.txn_holds_row_lock(txn.txn_id, storage_id, row_id);
        if txn.txn_id != 0 {
            self.acquire_row_lock_blocking(txn.txn_id, storage_id, row_id, LockMode::Exclusive)?;
        }
        let Some((header, mut projected_values, stored_count)) = self
            .read_mvcc_row_columns_with_plan(storage_id, row_id, required_columns, decode_plan)?
        else {
            return Ok(Some(false));
        };
        if header.is_deleted() {
            return Ok(Some(false));
        }
        // A logical None can be a physically reserved zero undo slot. Check
        // the encoded shape rather than rejecting every first UPDATE.
        if header.undo_ptr.is_none()
            && !self.with_heap_read(storage_id, |heap| heap.has_reserved_undo_slot(row_id))?
        {
            return Ok(None);
        }
        Self::backfill_projected_with_defaults(
            &mut projected_values,
            required_columns,
            stored_count,
            table,
        );
        // Most point updates touch ordinary OLTP tables narrower than 24
        // columns. Keep this expression-addressable scratch row on the worker
        // stack; genuinely wide schemas retain SmallVec's ordinary heap
        // spill. The persistent sparse before-image below is still owned and
        // governed by the undo record for its full transaction lifetime.
        let mut current_values: smallvec::SmallVec<[Value; 24]> =
            std::iter::repeat_n(Value::Null, table.columns.len()).collect();
        for (column, value) in required_columns
            .iter()
            .copied()
            .zip(projected_values.into_iter())
        {
            if let Some(slot) = current_values.get_mut(column) {
                *slot = value;
            }
        }

        if let Some(predicate) = predicate.as_mut() {
            if !predicate(&current_values)? {
                if txn.txn_id != 0 && !held_before {
                    self.release_new_point_row_lock(txn.txn_id, storage_id, row_id);
                }
                return Ok(Some(false));
            }
        }

        let before_values_sparse: smallvec::SmallVec<[Value; 8]> = effective_changed_columns
            .iter()
            .map(|&col| current_values.get(col).cloned().unwrap_or(Value::Null))
            .collect();
        let index_before_values =
            (!affected_index_positions.is_empty()).then(|| current_values.clone());
        apply_updates(&mut current_values)?;
        coerce_updated_row(table, &mut current_values, changed_columns)?;
        materialize_generated_columns(table, &mut current_values, Some(effective_changed_columns))?;

        if let Some(cols) = noop_compare_columns {
            let unchanged = cols.iter().all(|col| {
                effective_changed_columns
                    .iter()
                    .position(|candidate| candidate == col)
                    .and_then(|pos| before_values_sparse.get(pos))
                    .zip(current_values.get(*col))
                    .is_some_and(|(before, after)| before == after)
            });
            if unchanged {
                return Ok(Some(false));
            }
        }

        // With no affected unique index, check_update_constraints never needs an old
        // unique key. Passing the new row for both slices preserves the shared
        // NOT NULL / CHECK / child-FK gates without materialising a full old
        // row solely for an argument that will not be read.
        self.check_update_constraints(
            table,
            txn,
            row_id,
            &current_values,
            &current_values,
            Some(effective_changed_columns),
        )?;
        let patched = self.try_in_place_update_delta_with_before(
            txn,
            table,
            storage_id,
            row_id,
            header,
            &current_values,
            effective_changed_columns,
            before_values_sparse.into_vec(),
        )?;
        if !patched {
            return self
                .update_prepared_row_by_id_with_changed_columns_noop(
                    txn,
                    table,
                    row_id,
                    changed_columns,
                    noop_compare_columns,
                    affected_index_positions,
                    |old| {
                        let mut values = old.to_vec();
                        for &column in effective_changed_columns {
                            values[column] = current_values[column].clone();
                        }
                        Ok(values)
                    },
                )
                .map(Some);
        }

        #[cfg(test)]
        crate::storage::record_prepared_point_sparse_patch_hit_for_test();

        self.merge_zone_map_updated_rows(
            txn.txn_id,
            storage_id,
            &[row_id],
            &[current_values.as_slice()],
            Some(effective_changed_columns),
        )?;
        if let Some(before) = index_before_values.as_ref() {
            self.update_changed_indexes_deferred_at(
                txn.txn_id,
                storage_id,
                table,
                before,
                &current_values,
                row_id,
                Some(effective_changed_columns),
                Some(affected_index_positions),
            )?;
        }
        self.cache_auto_increment_from_updated_values(
            table,
            &current_values,
            Some(effective_changed_columns),
        )?;
        self.record_alter_copy_touched_rows(
            txn.txn_id,
            table.id,
            storage_id,
            std::iter::once(row_id),
        );
        self.record_non_temp_table_modifications(txn.txn_id, table, 1, 0)?;
        self.dml_stats.record_updates(1);
        Ok(Some(true))
    }

    #[allow(clippy::too_many_arguments)]
    fn update_row_by_id_with_changed_columns_noop_at_with_affected_indexes<
        F,
        const CAPTURE_IMAGES: bool,
    >(
        &self,
        txn: TxnView,
        table: &TableSchema,
        storage_id_override: Option<u64>,
        row_id: RowId,
        changed_columns: Option<&[usize]>,
        noop_compare_columns: Option<&[usize]>,
        affected_index_positions: Option<&[usize]>,
        mut apply_updates: F,
        mut images: Option<&mut Option<(Vec<Value>, Vec<Value>)>>,
    ) -> Result<bool, QueryError>
    where
        F: FnMut(&[Value]) -> Result<Vec<Value>, QueryError>,
    {
        self.check_dml_allowed_for_table(txn, table.id)?;
        self.acquire_table_write_binding(txn.txn_id, &table)?;
        let generated_changed_columns =
            expand_changed_columns_for_generated(table, changed_columns);
        let effective_changed_columns = generated_changed_columns.as_deref().or(changed_columns);
        // For partitioned tables, locate which partition owns this
        // row_id. The executor remembers only `row_id`, not the
        // partition; we search all partitions for a matching entry.
        // For unpartitioned tables this collapses to `table.id`.
        let storage_id = match storage_id_override {
            Some(sid) => sid,
            None => self.locate_row_storage_id(table, row_id)?,
        };
        let result = (|| {
            // Point UPDATE already resolved the immutable RowId. Lock first,
            // then decode the latest version once. This also makes the
            // prepared inline retry gate trip before row decoding.
            if txn.txn_id != 0 {
                self.acquire_row_lock_blocking(
                    txn.txn_id,
                    storage_id,
                    row_id,
                    LockMode::Exclusive,
                )?;
            }
            let Some(mut current) = self.read_mvcc_row(storage_id, row_id)? else {
                return Ok(false);
            };
            if current.header.is_deleted() {
                return Ok(false);
            }
            // Pad the heap row out to the latest schema before handing
            // it to the UPDATE expression callback — see the matching
            // comment in `update_rows_with_changed_columns` for why.
            Self::pad_full_row_to_schema(&mut current.values, table);
            let base_values = &current.values;
            let mut new_values = apply_updates(base_values)?;
            coerce_updated_row(table, &mut new_values, changed_columns)?;
            materialize_generated_columns(table, &mut new_values, effective_changed_columns)?;
            // MySQL no-op detection: a row whose user-set columns all
            // coerce to their stored values is "matched but not changed"
            // — skip the write entirely (no heap/undo/index/binlog churn,
            // no ON UPDATE CURRENT_TIMESTAMP refresh) and report unchanged.
            if let Some(cols) = noop_compare_columns {
                if row_unchanged_on(base_values, &new_values, cols) {
                    if CAPTURE_IMAGES {
                        *images.as_deref_mut().expect("image capture requested") =
                            Some((base_values.to_vec(), base_values.to_vec()));
                    }
                    return Ok(false);
                }
            }
            // For partitioned tables: if the new partition key routes
            // the row to a different partition, do delete+insert across
            // partitions instead of in-place rewrite.
            if table.is_partitioned() {
                let routed = table.route_row_to_partition(&new_values)?;
                let target_sid = routed.storage_id;
                if target_sid != storage_id {
                    // Validate and lock the destination unique keys before
                    // deleting the source. The bulk index insert below does
                    // not perform duplicate detection.
                    self.lock_unique_keys_for_values(txn.txn_id, table, &new_values)?;
                    self.check_update_constraints(
                        table,
                        txn,
                        row_id,
                        base_values,
                        &new_values,
                        effective_changed_columns,
                    )?;
                    // C2: emit a single UPDATE_ROWS event for the migration
                    // instead of the DELETE_ROWS + WRITE_ROWS pair the
                    // untuned path produced. Downstream MySQL replicas and
                    // CDC consumers see one event = one logical UPDATE,
                    // matching what a native MySQL cross-partition move
                    // emits. The individual delete/insert calls' internal
                    // event recording is suppressed by the guard for the
                    // duration of the migration.
                    if CAPTURE_IMAGES {
                        *images.as_deref_mut().expect("image capture requested") =
                            Some((base_values.to_vec(), new_values.clone()));
                    }
                    let before_image = base_values.to_vec();
                    let after_image = new_values.clone();
                    {
                        let _suppress = crate::binlog::SessionSqlLogBinGuard::new(false);
                        self.delete_row_at(txn, table, storage_id, row_id, base_values)?;
                        self.insert_one_row_at(txn, table, target_sid, new_values)?;
                    }
                    self.cache_auto_increment_from_updated_values(
                        table,
                        &after_image,
                        effective_changed_columns,
                    )?;
                    if crate::binlog::should_record_table_rows(table) {
                        crate::binlog::record_update_rows(
                            txn.txn_id,
                            table,
                            vec![(before_image, after_image)],
                        )?;
                    }
                    self.record_non_temp_table_modifications(txn.txn_id, table, 1, 0)?;
                    self.dml_stats.record_updates(1);
                    return Ok(true);
                }
            }
            self.check_update_constraints(
                table,
                txn,
                row_id,
                base_values,
                &new_values,
                effective_changed_columns,
            )?;
            // Foreign-key parent-side: lock the row exclusively first (only
            // when referenced) so a concurrent child holding a shared lock
            // blocks us — no orphans. The cascade itself runs *after* the
            // new value is written (below) so ON UPDATE CASCADE children
            // re-validate against the already-updated parent key.
            let parent_fk_update_may_affect_refs =
                Self::fk_parent_update_may_affect_references(table, effective_changed_columns);
            if parent_fk_update_may_affect_refs && txn.txn_id != 0 {
                self.acquire_row_lock_blocking(
                    txn.txn_id,
                    storage_id,
                    row_id,
                    LockMode::Exclusive,
                )?;
            }
            // Phase 2e fast path: try the column-delta in-place patch
            // when the GUC is on and the change is amenable (no LOB,
            // no NULL transitions, all-fixed-width changed columns,
            // MVCC header already at full width). On success, the
            // heap is patched + WAL HeapUpdateLogical is logged + a
            // sparse UndoKind::UpdateDelta is pushed — we skip the
            // full-row legacy block and jump straight to the post-
            // write continuation (indexes / binlog / stats / cascades)
            // shared by both paths.
            let used_patch_path = if update_delta_enabled()
                && effective_changed_columns
                    .map(|c| !c.is_empty())
                    .unwrap_or(false)
                && !self.row_has_externalizable_changes(
                    &table.row_codec,
                    base_values,
                    &new_values,
                    effective_changed_columns.unwrap(),
                ) {
                let cols = effective_changed_columns.unwrap();
                self.try_in_place_update_delta(
                    txn,
                    table,
                    storage_id,
                    row_id,
                    current.header,
                    base_values,
                    &new_values,
                    cols,
                )?
            } else {
                false
            };
            if !used_patch_path {
                // Legacy full-row UPDATE: capture old LOB chains,
                // push an UndoKind::Update with the entire before-
                // image (LOB columns nulled), externalize new LOBs,
                // and re-encode the whole row through write_mvcc_ext.
                let before_external = self.read_row_external_refs(storage_id, row_id)?;
                // M1(LOB) — compute the "reuse-if-unchanged" plan. Reused columns move out of
                // the owned before_external (their chains are owned by the new row version) and
                // are kept in undo as borrowed descriptors. See the same-named comment in prepare_update_lob.
                let (reuse_mask, kept_before_external) = self.compute_lob_reuse(
                    storage_id,
                    &table.row_codec,
                    &before_external,
                    &new_values,
                )?;
                let borrowed_external: Vec<(usize, LobRef)> = before_external
                    .iter()
                    .filter(|(column, _)| reuse_mask.get(*column).is_some_and(Option::is_some))
                    .copied()
                    .collect();
                // P1-3 (gated, default off): the undo FORMAT decision is
                // independent of the heap write shape — even a full row
                // rewrite only needs the changed columns' before-values
                // for MVCC reconstruction and rollback (both splice the
                // patch onto the newer version / current row). Restricted
                // to rows with no off-page columns: a full Update undo
                // takes ownership of the old version's LOB chains, which
                // a sparse record cannot represent without leaking them.
                let sparse_cols: Option<&[usize]> = if undo_delta_rewrite_enabled()
                    && before_external.is_empty()
                    && effective_changed_columns
                        .map(|c| !c.is_empty())
                        .unwrap_or(false)
                    && !self.row_has_externalizable_changes(
                        &table.row_codec,
                        base_values,
                        &new_values,
                        effective_changed_columns.unwrap(),
                    ) {
                    effective_changed_columns
                } else {
                    None
                };
                let undo_id = if let Some(cols) = sparse_cols {
                    let before_cols_u16: Vec<u16> = cols
                        .iter()
                        .map(|c| u16::try_from(*c).unwrap_or(u16::MAX))
                        .collect();
                    let before_values_sparse: Vec<Value> = cols
                        .iter()
                        .map(|c| current.values.get(*c).cloned().unwrap_or(Value::Null))
                        .collect();
                    self.push_undo_ext(
                        txn.txn_id,
                        UndoKind::UpdateDelta {
                            before_cols: before_cols_u16,
                        },
                        storage_id,
                        row_id,
                        Some(current.header),
                        Some(before_values_sparse),
                        Vec::new(),
                    )?
                } else {
                    // Both owned and borrowed descriptors can reconstruct the old version, so both
                    // kinds of external columns keep only a Null placeholder in before_values.
                    let null_cols: std::collections::HashSet<usize> = kept_before_external
                        .iter()
                        .chain(&borrowed_external)
                        .map(|(i, _)| *i)
                        .collect();
                    let undo_values: Vec<Value> = if null_cols.is_empty() {
                        current.values.clone()
                    } else {
                        current
                            .values
                            .iter()
                            .enumerate()
                            .map(|(i, v)| {
                                if null_cols.contains(&i) {
                                    Value::Null
                                } else {
                                    v.clone()
                                }
                            })
                            .collect()
                    };
                    self.push_undo_ext_borrowed(
                        txn.txn_id,
                        UndoKind::Update,
                        storage_id,
                        row_id,
                        Some(current.header),
                        Some(undo_values),
                        kept_before_external,
                        borrowed_external.clone(),
                    )?
                };
                // The new version reuses byte-identical old chains (M1) and
                // freshly allocates the rest — every chain still has exactly
                // one owner (reused chains belong to the new version; undo
                // only borrows their immutable descriptors).
                let (new_external, newly_allocated) = self.externalize_row_lobs_with_reuse(
                    storage_id,
                    &table.row_codec,
                    &new_values,
                    &reuse_mask,
                )?;
                let write_result = self
                    .publish_wal_before_eager_heap_write(txn.txn_id)
                    .and_then(|_| {
                        self.with_heap_mut(storage_id, |heap| {
                            heap.write_mvcc_ext(
                                txn.txn_id,
                                row_id,
                                RowHeader {
                                    flags: 0,
                                    last_txn_id: txn.txn_id,
                                    undo_ptr: Some(undo_id),
                                },
                                &new_values,
                                &new_external,
                            )
                        })
                    });
                if let Err(e) = write_result {
                    // BUG-2 — free only the freshly-allocated chains; the
                    // reused old chains are still owned by the live row.
                    self.free_lob_chains(storage_id, &newly_allocated);
                    return Err(e);
                }
            }
            self.merge_zone_map_updated_rows(
                txn.txn_id,
                storage_id,
                &[row_id],
                &[new_values.as_slice()],
                effective_changed_columns,
            )?;
            self.update_changed_indexes_deferred_at(
                txn.txn_id,
                storage_id,
                table,
                base_values,
                &new_values,
                row_id,
                effective_changed_columns,
                affected_index_positions,
            )?;
            // Publish the derived ANN replacement only after commit. The heap
            // undo record is the lifetime marker used by savepoint pruning.
            if let Some(vector_undo_id) = self.deferred_ann_undo_head(txn.txn_id, table)? {
                self.defer_vector_update(
                    txn.txn_id,
                    vector_undo_id,
                    table,
                    row_id,
                    storage_id,
                    Some(base_values),
                    &new_values,
                )?;
            }
            // Parent-side cascade now that the new key is durable in this
            // txn: ON UPDATE CASCADE rewrites children to the new value,
            // SET NULL nulls them, RESTRICT/NO ACTION reject (rolled back).
            if parent_fk_update_may_affect_refs {
                self.fk_enforce_parent_update(table, txn, base_values, &new_values)?;
            }
            self.cache_auto_increment_from_updated_values(
                table,
                &new_values,
                effective_changed_columns,
            )?;
            self.record_alter_copy_touched_rows(
                txn.txn_id,
                table.id,
                storage_id,
                std::iter::once(row_id),
            );
            self.record_non_temp_table_modifications(txn.txn_id, table, 1, 0)?;
            if CAPTURE_IMAGES {
                *images.as_deref_mut().expect("image capture requested") =
                    Some((base_values.to_vec(), new_values.clone()));
            }
            if crate::binlog::should_record_table_rows(table) {
                crate::binlog::record_update_rows(
                    txn.txn_id,
                    table,
                    // Transfer the completed FULL images without cloning their payloads.
                    vec![(current.values, new_values)],
                )?;
            }
            self.dml_stats.record_updates(1);
            Ok(true)
        })();
        result
    }

    /// Partition-aware UPDATE. Iterates each partition, applies the
    /// predicate to rows from that partition, and updates in place
    /// against that partition's heap/indexes. When the partition
    /// column itself is updated to a value that routes the row to a
    /// different partition, the row is deleted from its current
    /// partition and re-inserted into the new one as a single logical
    /// "row migration" — matching MySQL's behaviour for partition-key
    /// updates.
    fn update_rows_partitioned<F, G>(
        &self,
        txn: TxnView,
        table: &std::sync::Arc<TableSchema>,
        candidates: Option<Vec<crate::storage::DmlRowRef>>,
        changed_columns: Option<&[usize]>,
        noop_compare_columns: Option<&[usize]>,
        predicate: &mut F,
        apply_updates: &mut G,
        ignore_check_errors: bool,
    ) -> Result<usize, QueryError>
    where
        F: FnMut(&[Value]) -> Result<bool, QueryError>,
        G: FnMut(&[Value]) -> Result<Vec<Value>, QueryError>,
    {
        self.acquire_table_write_binding(txn.txn_id, &table)?;
        let generated_changed_columns =
            expand_changed_columns_for_generated(table, changed_columns);
        let effective_changed_columns = generated_changed_columns.as_deref().or(changed_columns);
        let result = (|| {
            let mut total_updated: usize = 0;
            let mut rows = self.dml_candidate_rows_with_origin_for_write(table, txn, candidates)?;
            // Stable physical order reduces deadlock cycles for multi-row
            // statements without changing SQL ordering (ORDER/LIMIT uses the
            // point-write path before reaching this batch implementation).
            rows.sort_unstable_by_key(|row| row.row_ref);
            // Deferred migrations are not visible to index probes yet. Keep
            // their destination keys reserved for both later migrations and
            // in-place updates. Unaffected indexes need no extra key encoding.
            let batch_unique_indexes = table
                .indexes
                .iter()
                .filter(|index| {
                    rows.len() > 1
                        && index.enforces_uniqueness()
                        && index_may_be_affected_by_columns(table, index, effective_changed_columns)
                })
                .collect::<Vec<_>>();
            let mut migration_keys = batch_unique_indexes
                .iter()
                .map(|_| crate::util::fast_hash::AhHashSet::<(u64, IndexKey)>::default())
                .collect::<Vec<_>>();
            let mut migrations: Vec<(u64, RowId, Vec<Value>, Vec<Value>)> = Vec::new();
            for row in rows {
                crate::engine::cancel::check_cancelled()?;
                if !predicate(&row.values)? {
                    continue;
                }
                let storage_id = row.row_ref.storage_id;
                if txn.txn_id != 0 {
                    self.lock_rows_nonblocking(
                        txn.txn_id,
                        storage_id,
                        std::iter::once(row.row_ref.row_id),
                    )?;
                }
                let Some(mut current) = self.read_mvcc_row(storage_id, row.row_ref.row_id)? else {
                    continue;
                };
                self.check_dml_write_allowed(txn, storage_id, &current)?;
                if current.header.is_deleted() {
                    continue;
                }
                Self::pad_full_row_to_schema(&mut current.values, table);
                if !predicate(&current.values)? {
                    continue;
                }
                let mut new_values = apply_updates(&current.values)?;
                coerce_row(table, &mut new_values)?;
                materialize_generated_columns(table, &mut new_values, None)?;
                // MySQL no-op detection: skip matched-but-unchanged
                // rows (no write, no ON UPDATE refresh).
                if let Some(cols) = noop_compare_columns {
                    if row_unchanged_on(&current.values, &new_values, cols) {
                        continue;
                    }
                }
                if let Err(err) = self.check_update_constraints(
                    table,
                    txn,
                    row.row_ref.row_id,
                    &current.values,
                    &new_values,
                    effective_changed_columns,
                ) {
                    if ignore_check_errors && ignorable_update_error(&err) {
                        crate::engine::api::push_statement_condition(
                            crate::error::ConditionLevel::Warning,
                            &err,
                        );
                        continue;
                    }
                    return Err(err);
                }
                let routed = table.route_row_to_partition(&new_values)?;
                let target_storage_id = routed.storage_id;
                let mut pending = Vec::new();
                let mut duplicate = None;
                for (pos, index) in batch_unique_indexes.iter().enumerate() {
                    // If nothing is deferred, an in-place write is fully
                    // covered by the existing index probe above.
                    if target_storage_id == storage_id && migration_keys[pos].is_empty() {
                        continue;
                    }
                    let key = table.index_key(index, &new_values)?;
                    if !index.primary && key.0.iter().any(|part| matches!(part, KeyPart::Null)) {
                        continue;
                    }
                    let entry = (target_storage_id, key);
                    if migration_keys[pos].contains(&entry) {
                        duplicate = Some(duplicate_entry_error(table, index, &entry.1));
                        break;
                    }
                    if target_storage_id != storage_id {
                        pending.push((pos, entry));
                    }
                }
                if let Some(err) = duplicate {
                    if ignore_check_errors {
                        crate::engine::api::push_statement_condition(
                            crate::error::ConditionLevel::Warning,
                            &err,
                        );
                        continue;
                    }
                    return Err(err);
                }
                // Publish only after every unique index accepts this row:
                // an IGNORE-rejected row must not reserve any other key.
                for (pos, entry) in pending {
                    migration_keys[pos].insert(entry);
                }
                self.cache_auto_increment_from_updated_values(
                    table,
                    &new_values,
                    effective_changed_columns,
                )?;
                if target_storage_id == storage_id {
                    self.update_row_in_place(
                        txn,
                        table,
                        storage_id,
                        row.row_ref.row_id,
                        current.header,
                        &current.values,
                        new_values,
                        effective_changed_columns,
                    )?;
                    total_updated += 1;
                } else {
                    migrations.push((
                        storage_id,
                        row.row_ref.row_id,
                        current.values.clone(),
                        new_values,
                    ));
                }
            }
            for (src_sid, row_id, old_values, new_values) in migrations {
                crate::engine::cancel::check_cancelled()?;
                let routed = table.route_row_to_partition(&new_values)?;
                let dst_sid = routed.storage_id;
                let record_rows = crate::binlog::should_record_table_rows(table);
                let after_image = record_rows.then(|| new_values.clone());
                {
                    let _suppress = crate::binlog::SessionSqlLogBinGuard::new(false);
                    self.delete_row_at(txn, table, src_sid, row_id, &old_values)?;
                    self.insert_one_row_at(txn, table, dst_sid, new_values)?;
                }
                if let Some(after_image) = after_image {
                    crate::binlog::record_update_rows(
                        txn.txn_id,
                        table,
                        vec![(old_values, after_image)],
                    )?;
                }
                total_updated += 1;
            }
            self.record_non_temp_table_modifications(txn.txn_id, table, total_updated as u64, 0)?;
            self.dml_stats.record_updates(total_updated as u64);
            Ok(total_updated)
        })();
        result
    }

    /// Partition-aware DELETE. Iterates each partition; rows that
    /// match the predicate are tombstoned in that partition's heap
    /// and its indexes.
    fn delete_rows_partitioned<F>(
        &self,
        txn: TxnView,
        table: &std::sync::Arc<TableSchema>,
        candidates: Option<Vec<crate::storage::DmlRowRef>>,
        predicate: &mut F,
    ) -> Result<usize, QueryError>
    where
        F: FnMut(&[Value]) -> Result<bool, QueryError>,
    {
        self.acquire_table_write_binding(txn.txn_id, &table)?;
        let mut total_deleted: usize = 0;
        let mut rows = self.dml_candidate_rows_with_origin_for_write(table, txn, candidates)?;
        rows.sort_unstable_by_key(|row| row.row_ref);
        for row in rows {
            crate::engine::cancel::check_cancelled()?;
            if !predicate(&row.values)? {
                continue;
            }
            let row_ref = row.row_ref;
            if txn.txn_id != 0 {
                self.lock_rows_nonblocking(
                    txn.txn_id,
                    row_ref.storage_id,
                    std::iter::once(row_ref.row_id),
                )?;
            }
            let Some(mut current) = self.read_mvcc_row(row_ref.storage_id, row_ref.row_id)? else {
                continue;
            };
            self.check_dml_write_allowed(txn, row_ref.storage_id, &current)?;
            if current.header.is_deleted() {
                continue;
            }
            Self::pad_full_row_to_schema(&mut current.values, table);
            if !predicate(&current.values)? {
                continue;
            }
            self.delete_row_at(
                txn,
                table,
                row_ref.storage_id,
                row_ref.row_id,
                &current.values,
            )?;
            total_deleted += 1;
        }
        self.record_non_temp_table_modifications(
            txn.txn_id,
            table,
            total_deleted as u64,
            -(total_deleted.min(i64::MAX as usize) as i64),
        )?;
        self.dml_stats.record_deletes(total_deleted as u64);
        Ok(total_deleted)
    }

    /// In-place UPDATE inside a single partition's heap/indexes.
    /// Used by the partition-aware UPDATE path when the new partition
    /// key still routes the row to the same partition.
    #[allow(clippy::too_many_arguments)]
    fn update_row_in_place(
        &self,
        txn: TxnView,
        table: &TableSchema,
        storage_id: u64,
        row_id: RowId,
        old_header: RowHeader,
        old_values: &[Value],
        new_values: Vec<Value>,
        effective_changed_columns: Option<&[usize]>,
    ) -> Result<(), QueryError> {
        self.record_alter_copy_touched_rows(
            txn.txn_id,
            table.id,
            storage_id,
            std::iter::once(row_id),
        );
        // Phase 2e: when enabled and the row qualifies for an
        // in-place column patch (no LOB activity, only fixed-width
        // changed columns, MVCC header bytes are the same size
        // before and after), reserve the undo id, attempt the
        // patch, and emit a column-level UndoKind::UpdateDelta +
        // WalPayload::HeapUpdateLogical. If the patch path can't
        // apply, undo the reservation and fall through to the
        // existing full-row write below.
        if update_delta_enabled()
            && effective_changed_columns
                .map(|cols| !cols.is_empty())
                .unwrap_or(false)
        {
            let cols = effective_changed_columns.expect("checked above");
            let has_lob = self.row_has_externalizable_changes(
                &table.row_codec,
                old_values,
                &new_values,
                cols,
            );
            if !has_lob {
                let patched = self.try_in_place_update_delta(
                    txn,
                    table,
                    storage_id,
                    row_id,
                    old_header,
                    old_values,
                    &new_values,
                    cols,
                )?;
                if patched {
                    // Path succeeded: indexes / binlog / visibility
                    // map are handled by the helper below.
                    self.merge_zone_map_updated_rows(
                        txn.txn_id,
                        storage_id,
                        &[row_id],
                        &[new_values.as_slice()],
                        effective_changed_columns,
                    )?;
                    self.update_changed_indexes_deferred_at(
                        txn.txn_id,
                        storage_id,
                        table,
                        old_values,
                        &new_values,
                        row_id,
                        effective_changed_columns,
                        None,
                    )?;
                    if let Some(undo_id) = self.deferred_ann_undo_head(txn.txn_id, table)? {
                        self.defer_vector_update(
                            txn.txn_id,
                            undo_id,
                            table,
                            row_id,
                            storage_id,
                            Some(old_values),
                            &new_values,
                        )?;
                    }
                    if crate::binlog::should_record_table_rows(table) {
                        crate::binlog::record_update_rows(
                            txn.txn_id,
                            table,
                            vec![(old_values.to_vec(), new_values)],
                        )?;
                    }
                    return Ok(());
                }
                // Fell back — continue with the legacy full-row path.
            }
        }
        let (undo_id, new_external, newly_allocated) = self.prepare_update_lob(
            txn.txn_id,
            storage_id,
            &table.row_codec,
            row_id,
            old_header,
            old_values,
            &new_values,
            effective_changed_columns,
        )?;
        let write_result = self
            .publish_wal_before_eager_heap_write(txn.txn_id)
            .and_then(|_| {
                self.with_heap_mut(storage_id, |heap| {
                    heap.write_mvcc_ext(
                        txn.txn_id,
                        row_id,
                        RowHeader {
                            flags: 0,
                            last_txn_id: txn.txn_id,
                            undo_ptr: Some(undo_id),
                        },
                        &new_values,
                        &new_external,
                    )
                })
            });
        if let Err(e) = write_result {
            // BUG-2 — free only freshly-allocated chains; reused old
            // chains are still owned by the live row.
            self.free_lob_chains(storage_id, &newly_allocated);
            return Err(e);
        }
        self.merge_zone_map_updated_rows(
            txn.txn_id,
            storage_id,
            &[row_id],
            &[new_values.as_slice()],
            effective_changed_columns,
        )?;
        self.update_changed_indexes_deferred_at(
            txn.txn_id,
            storage_id,
            table,
            old_values,
            &new_values,
            row_id,
            effective_changed_columns,
            None,
        )?;
        self.defer_vector_update(
            txn.txn_id,
            undo_id,
            table,
            row_id,
            storage_id,
            Some(old_values),
            &new_values,
        )?;
        if crate::binlog::should_record_table_rows(table) {
            crate::binlog::record_update_rows(
                txn.txn_id,
                table,
                vec![(old_values.to_vec(), new_values)],
            )?;
        }
        Ok(())
    }

    /// Conservative LOB-changes detector for the column-delta UPDATE
    /// path. Returns true if any of the `changed` columns is either a
    /// varlen kind or larger than the LOB inline threshold in old or
    /// new state — in either case the in-place patch path bails out
    /// and the caller falls back to the full re-encode that already
    /// handles externalization correctly. False positives only cost
    /// the fast path; they cannot cause data corruption.
    fn row_has_externalizable_changes(
        &self,
        codec: &RowCodec,
        old_values: &[Value],
        new_values: &[Value],
        changed: &[usize],
    ) -> bool {
        for &col in changed {
            let Some(column) = codec.columns.get(col) else {
                return true;
            };
            if matches!(
                column.kind,
                ColumnKind::VarString
                    | ColumnKind::VarBytes
                    | ColumnKind::Json
                    | ColumnKind::Geometry
                    | ColumnKind::Vector { .. }
                    | ColumnKind::Decimal { .. }
            ) {
                return true;
            }
            // FixedBytes / VectorFixed can be patched, but only if
            // the value bytes don't change length. The fixed-column
            // byte-range helper already verifies width, so we don't
            // need to second-guess it here.
            let _ = (old_values, new_values);
        }
        false
    }

    /// Internal helper: drive the in-place patch path end-to-end.
    /// Returns `true` on success (caller continues with index and
    /// binlog maintenance), or `false` if the patch
    /// path fell back without making any change.
    ///
    /// `old_header` is the row's MVCC header BEFORE this UPDATE — it
    /// becomes the `before_header` recorded in the UpdateDelta undo
    /// so the visibility walker and rollback path can reconstruct
    /// the pre-UPDATE version. Passing the new header here would
    /// confuse the visibility walker into treating its own write as
    /// visible from older snapshots.
    #[allow(clippy::too_many_arguments)]
    fn try_in_place_update_delta(
        &self,
        txn: TxnView,
        table: &TableSchema,
        storage_id: u64,
        row_id: RowId,
        old_header: RowHeader,
        old_values: &[Value],
        new_values: &[Value],
        changed_cols: &[usize],
    ) -> Result<bool, QueryError> {
        // Reserve an undo id up front so the row's new undo_ptr
        // points at a real record. We push the undo BEFORE the
        // heap patch so the heap write's logical WAL record can
        // record the undo_lsn as the WAL HeapUpdateLogical's
        // undo_lsn field.
        let before_values_sparse: Vec<Value> = changed_cols
            .iter()
            .map(|c| old_values.get(*c).cloned().unwrap_or(Value::Null))
            .collect();
        self.try_in_place_update_delta_with_before(
            txn,
            table,
            storage_id,
            row_id,
            old_header,
            new_values,
            changed_cols,
            before_values_sparse,
        )
    }

    /// Variant for a caller that already retained the changed columns' old
    /// values before mutating its owned row buffer. This is the prepared
    /// point-UPDATE path: passing the sparse before-image avoids cloning the
    /// rest of a wide row solely to build UpdateDelta undo.
    #[allow(clippy::too_many_arguments)]
    fn try_in_place_update_delta_with_before(
        &self,
        txn: TxnView,
        _table: &TableSchema,
        storage_id: u64,
        row_id: RowId,
        old_header: RowHeader,
        new_values: &[Value],
        changed_cols: &[usize],
        before_values_sparse: Vec<Value>,
    ) -> Result<bool, QueryError> {
        debug_assert_eq!(changed_cols.len(), before_values_sparse.len());
        // Release the heap handle/page guards before reserving or appending
        // undo: preparation must not introduce a heap -> undo lock order.
        let heap = self.heap_handle(storage_id)?;
        let prepared = heap
            .read()
            .prepare_mvcc_patch(row_id, new_values, changed_cols)?;
        let Some(prepared) = prepared else {
            return Ok(false);
        };
        let before_cols_u16: Vec<u16> = changed_cols
            .iter()
            .map(|c| u16::try_from(*c).unwrap_or(u16::MAX))
            .collect();
        let undo_id = self.push_undo_ext(
            txn.txn_id,
            UndoKind::UpdateDelta {
                before_cols: before_cols_u16,
            },
            storage_id,
            row_id,
            Some(old_header),
            Some(before_values_sparse),
            Vec::new(),
        )?;
        let result = heap
            .read()
            .apply_prepared_mvcc_patch(txn.txn_id, undo_id, prepared);
        result
    }

    /// DELETE one row at a specific partition's storage_id.
    fn delete_row_at(
        &self,
        txn: TxnView,
        table: &TableSchema,
        storage_id: u64,
        row_id: RowId,
        old_values: &[Value],
    ) -> Result<(), QueryError> {
        self.record_alter_copy_touched_rows(
            txn.txn_id,
            table.id,
            storage_id,
            std::iter::once(row_id),
        );
        let current = self
            .read_mvcc_row(storage_id, row_id)?
            .ok_or_else(|| error(crate::error::catalog::ER_KEY_NOT_FOUND, "row not found"))?;
        let (undo_id, tombstone) = self.prepare_delete_lob(
            txn.txn_id,
            storage_id,
            row_id,
            current.header,
            &current.values,
        )?;
        self.with_heap_mut(storage_id, |heap| {
            heap.write_mvcc(
                txn.txn_id,
                row_id,
                RowHeader {
                    flags: MVCC_FLAG_DELETED,
                    last_txn_id: txn.txn_id,
                    undo_ptr: Some(undo_id),
                },
                &tombstone,
            )
        })?;
        // Publish old-key visibility before removing partition-local entries.
        self.delete_indexes_deferred_at(txn.txn_id, storage_id, table, old_values, row_id)?;
        self.defer_vector_delete(txn.txn_id, undo_id, table, old_values, row_id, storage_id)?;
        if crate::binlog::should_record_table_rows(table) {
            crate::binlog::record_delete_rows(txn.txn_id, table, vec![old_values.to_vec()])?;
        }
        Ok(())
    }

    /// INSERT one row into a specific partition's storage_id. Used
    /// during cross-partition UPDATE migration; bypasses the batch
    /// machinery since at most one row is involved.
    fn insert_one_row_at(
        &self,
        txn: TxnView,
        table: &TableSchema,
        storage_id: u64,
        values: Vec<Value>,
    ) -> Result<RowId, QueryError> {
        let row_headers = vec![RowHeader::visible(txn.txn_id)];
        let inserted = self.insert_mvcc_batch_externalized(
            txn.txn_id,
            storage_id,
            &table.row_codec,
            &row_headers,
            std::slice::from_ref(&values),
        )?;
        let row_id = inserted[0];
        self.record_alter_copy_touched_rows(
            txn.txn_id,
            table.id,
            storage_id,
            std::iter::once(row_id),
        );
        self.lock_rows_nonblocking(txn.txn_id, storage_id, std::iter::once(row_id))?;
        self.push_insert_batch_undo_deferred(txn.txn_id, storage_id, &inserted)?;
        let mut index_entries = table
            .indexes
            .iter()
            .map(|index| (index.name.clone(), Vec::new()))
            .collect::<BTreeMap<_, Vec<(IndexKey, RowId)>>>();
        for index in &table.indexes {
            let entries = index_entries
                .get_mut(&index.name)
                .expect("index entry batch should exist");
            for key in table.index_keys(index, &values)? {
                entries.push((key, row_id));
            }
        }
        self.insert_index_batches_deferred_at(txn.txn_id, storage_id, table, index_entries)?;
        self.defer_vector_insert_batch(
            txn.txn_id,
            table,
            storage_id,
            std::slice::from_ref(&values),
            &inserted,
        )?;
        crate::binlog::record_write_rows(txn.txn_id, table, vec![values])?;
        Ok(row_id)
    }

    /// M3(space shrink) — opportunistically shrink a table's `.lob` file: return
    /// trailing contiguous free pages to the OS. Called from the `OPTIMIZE TABLE` trigger
    /// point. Controlled by GUCs `MYDB_LOB_SHRINK_DISABLE` / `MYDB_LOB_SHRINK_MIN_FREE_PAGES`.
    /// Cheap no-op when the table has no `.lob` (never externalized). Returns the pages freed.
    /// Uses the synthetic DDL txn_id (like purge/rollback — a maintenance write).
    pub(crate) fn shrink_table_lobs(&self, table_id: u64) -> Result<u64, QueryError> {
        if lob_shrink_disabled() {
            return Ok(0);
        }
        let min_pages = lob_shrink_min_free_pages();
        let shrunk = self.with_overflow_mut(table_id, |ovf| ovf.shrink_to_fit(min_pages))?;
        if shrunk > 0 {
            self.dml_stats.record_lob_pages_shrunk(shrunk);
        }
        Ok(shrunk)
    }

    #[cfg(test)]
    /// M3/M6 — read a (page_count, free_count) snapshot of a table's `.lob` (observability
    /// / test verification). Returns (0, 0) when the table has no `.lob`.
    pub(crate) fn lob_space_stats(&self, table_id: u64) -> Result<(u64, u64), QueryError> {
        self.with_overflow_read(table_id, |ovf| Ok((ovf.page_count(), ovf.free_count())))
    }

    /// Validate each published physical B-tree under its structural latch.
    /// Vector graphs have their own storage format and are not B-trees.
    pub(crate) fn check_table_index_integrity(&self, table: &TableSchema) -> Vec<String> {
        let mut problems = Vec::new();
        for sid in table.storage_ids() {
            for index in table
                .iter_public_indexes()
                .filter(|index| !index.is_vector())
            {
                let result = (|| {
                    let handle = self.index_handle(sid, &index.name)?;
                    let file = handle.write();
                    file.validate_structure()
                })();
                if let Err(err) = result {
                    problems.push(format!(
                        "storage {sid} index `{}`: {}",
                        index.name,
                        err.message()
                    ));
                }
            }
        }
        problems
    }

    /// M6 CHECK TABLE (docs/lob-management-audit-2026-07.md) — LOB
    /// integrity check for one table. Returns a list of human-readable
    /// problems (empty = healthy). Two layers:
    ///   1. **Structural** per storage id (partitions each have their own
    ///      `.lob`): free-list is cycle-free and every physical page is a
    ///      well-formed overflow page (`validate_structure`).
    ///   2. **Row-driven** chain walk: only when the table actually has
    ///      externalized data (or a declared LOB column), materialize every
    ///      live row — this reads each referenced chain, so a dangling /
    ///      broken / cyclic chain surfaces as a 1030 corruption. Reuses the
    ///      tested scan path rather than reimplementing row iteration.
    pub(crate) fn check_table_lob_integrity(&self, table: &TableSchema) -> Vec<String> {
        let mut problems = Vec::new();
        let storage_ids = table.storage_ids();
        let mut any_lob_pages = false;
        for sid in &storage_ids {
            match self.with_overflow_read(*sid, |ovf| {
                ovf.validate_structure()?;
                Ok(ovf.page_count())
            }) {
                // >1 page means real data pages exist beyond the meta page.
                Ok(page_count) => {
                    if page_count > 1 {
                        any_lob_pages = true;
                    }
                }
                Err(e) => problems.push(format!("storage {sid} `.lob` structure: {}", e.message())),
            }
        }
        if any_lob_pages || table_has_externalizable_column(table) {
            let qname = format!("{}.{}", table.database, table.name);
            if let Err(e) = self.scan_rows(
                &qname,
                TxnView::read_only(u64::MAX, crate::storage::IsolationLevel::ReadCommitted),
            ) {
                problems.push(format!("row/LOB chain scan: {}", e.message()));
            }
        }
        problems
    }
}

/// M6 — does the table declare a column whose type is always externalized
/// off-page (TEXT/BLOB/JSON/GEOMETRY family, or a variable-length VECTOR)?
/// Used to decide whether CHECK TABLE runs the (heavier) row-driven LOB
/// chain walk. `VECTOR(n)` (fixed dim) lives inline and is excluded.
fn table_has_externalizable_column(table: &TableSchema) -> bool {
    table.columns.iter().any(|c| {
        matches!(
            c.data_type,
            DataTypeKind::Text { .. }
                | DataTypeKind::Blob { .. }
                | DataTypeKind::Json
                | DataTypeKind::Geometry { .. }
                | DataTypeKind::Vector { dim: None, .. }
        )
    })
}

// ===========================================================================
// M7 — LOB GUCs are runtime-tunable. They used to be `OnceLock`s (a runtime SET had no
// effect); now they are "`AtomicU64` + one-time env seed at startup": getters
// ensure-seed then read the atomic; setters just store. env stays the startup default
// (legacy env-override semantics, matching existing mydb_* GUCs like
// `mydb_buffer_pool_pages`); `SET GLOBAL mydb_lob_*` changes take effect immediately.
// Booleans are stored as 0/1. These getters/setters are registered as `mydb_lob_*`
// sysvars by `src/engine/sysvar/builtin/mydb_storage.rs`, supporting SET GLOBAL / SHOW VARIABLES / @@global.
// ===========================================================================

/// A runtime-tunable u64 GUC: atomic value + one-time env seed.
struct LobRuntimeKnob {
    value: std::sync::atomic::AtomicU64,
    seeded: std::sync::Once,
    env: &'static str,
    default: u64,
    /// Parses the env string into u64 (booleans use a bool_to_u64-style parser).
    parse: fn(&str) -> Option<u64>,
}

impl LobRuntimeKnob {
    const fn new(env: &'static str, default: u64, parse: fn(&str) -> Option<u64>) -> Self {
        Self {
            value: std::sync::atomic::AtomicU64::new(0),
            seeded: std::sync::Once::new(),
            env,
            default,
            parse,
        }
    }
    fn ensure_seeded(&self) {
        self.seeded.call_once(|| {
            let v = std::env::var(self.env)
                .ok()
                .and_then(|s| (self.parse)(&s))
                .unwrap_or(self.default);
            self.value.store(v, std::sync::atomic::Ordering::Relaxed);
        });
    }
    fn get(&self) -> u64 {
        self.ensure_seeded();
        self.value.load(std::sync::atomic::Ordering::Relaxed)
    }
    fn set(&self, v: u64) {
        self.ensure_seeded(); // prevent a later seed from overwriting a runtime-set value
        self.value.store(v, std::sync::atomic::Ordering::Relaxed);
    }
}

/// True when none of `cols` differ between the stored row `base` and the
/// candidate `new` row — i.e. the UPDATE leaves every user-set column at
/// its existing stored value, so MySQL counts the row as matched but not
/// changed. Compared *after* coercion so `SET c = c` (and literals that
/// coerce to the stored value) register as unchanged. Uses `Value`'s
/// stored-equality (`String`/`SmallString` compare equal, `Null == Null`);
/// any representation mismatch conservatively reports "changed", which
/// only forgoes the optimisation, never miscounts a real change as a no-op.
/// Per-row constraint violations that MySQL's `UPDATE IGNORE` downgrades
/// to a warning and skips (leaving the row unchanged) instead of aborting
/// the statement. Mirrors INSERT IGNORE's data-error set, plus:
/// * 1062 — a SET that moves a row onto an existing unique/primary key
///   (duplicate-key); UPDATE has no ON DUPLICATE fallback so IGNORE skips.
/// * 1451 — FK parent-side RESTRICT (updating a referenced key).
/// `check_update_constraints` raises all of these per row *before* the
/// heap write, so they can be attributed to (and skipped for) one row.
/// Non-ignorable errors (syntax, missing table, lock-wait, deadlock, …)
/// still propagate.
pub(crate) fn ignorable_update_error(error: &QueryError) -> bool {
    // 1526 — "table has no partition for value" (INSERT/UPDATE onto a
    //        RANGE/LIST partitioned table whose key doesn't fit any
    //        partition). MySQL 8.0 with IGNORE downgrades this to a
    //        warning and skips the row.
    // 1748 — "found row not matching the given partition set" (row
    //        would end up outside the PARTITION(...) targeting clause).
    //        Also downgraded to warning+skip under IGNORE in MySQL 8.0.
    error.is_any(&[
        crate::error::catalog::ER_BAD_NULL_ERROR,
        crate::error::catalog::ER_DUP_ENTRY,
        crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
        crate::error::catalog::WARN_DATA_TRUNCATED,
        crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
        crate::error::catalog::ER_ROW_IS_REFERENCED_2,
        crate::error::catalog::ER_NO_REFERENCED_ROW_2,
        crate::error::catalog::ER_NO_PARTITION_FOR_GIVEN_VALUE,
        crate::error::catalog::ER_ROW_DOES_NOT_MATCH_GIVEN_PARTITION_SET,
        crate::error::catalog::ER_VIEW_CHECK_FAILED,
        crate::error::catalog::ER_CHECK_CONSTRAINT_VIOLATED,
    ])
}

fn row_unchanged_on(base: &[Value], new: &[Value], cols: &[usize]) -> bool {
    cols.iter().all(|&i| match (base.get(i), new.get(i)) {
        (Some(a), Some(b)) => a == b,
        _ => false,
    })
}

fn parse_u64(s: &str) -> Option<u64> {
    s.trim().parse::<u64>().ok()
}
fn parse_bool_u64(s: &str) -> Option<u64> {
    match s.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "on" | "yes" => Some(1),
        "0" | "false" | "off" | "no" => Some(0),
        _ => None,
    }
}
fn parse_compression_u64(s: &str) -> Option<u64> {
    // lz4 → 1, off/anything else → 0.
    Some(if s.trim().eq_ignore_ascii_case("lz4") {
        1
    } else {
        0
    })
}

static LOB_INLINE_MAX_KNOB: LobRuntimeKnob =
    LobRuntimeKnob::new("MYDB_LOB_INLINE_MAX", 4096, parse_u64);
static LOB_REUSE_UNCHANGED_KNOB: LobRuntimeKnob =
    LobRuntimeKnob::new("MYDB_LOB_REUSE_UNCHANGED", 1, parse_bool_u64);
static LOB_SHRINK_DISABLE_KNOB: LobRuntimeKnob =
    LobRuntimeKnob::new("MYDB_LOB_SHRINK_DISABLE", 0, parse_bool_u64);
static LOB_SHRINK_MIN_FREE_PAGES_KNOB: LobRuntimeKnob =
    LobRuntimeKnob::new("MYDB_LOB_SHRINK_MIN_FREE_PAGES", 64, parse_u64);
static LOB_EXTENT_MIN_PAGES_KNOB: LobRuntimeKnob =
    LobRuntimeKnob::new("MYDB_LOB_EXTENT_MIN_PAGES", 4, parse_u64);
static LOB_PREFETCH_DISABLE_KNOB: LobRuntimeKnob =
    LobRuntimeKnob::new("MYDB_LOB_PREFETCH_DISABLE", 0, parse_bool_u64);
static LOB_COMPRESSION_KNOB: LobRuntimeKnob =
    LobRuntimeKnob::new("MYDB_LOB_COMPRESSION", 0, parse_compression_u64);
static LOB_COMPRESS_MIN_BYTES_KNOB: LobRuntimeKnob =
    LobRuntimeKnob::new("MYDB_LOB_COMPRESS_MIN_BYTES", 1024, parse_u64);

// ---- getters (runtime reads; called from storage-engine hot paths) ----

/// M7 — `mydb_lob_inline_max`: inline vs overflow threshold (bytes). Runtime-tunable.
pub(crate) fn lob_inline_max_runtime() -> usize {
    LOB_INLINE_MAX_KNOB.get() as usize
}
/// M1 — `mydb_lob_reuse_unchanged`.
pub(crate) fn lob_reuse_unchanged_enabled() -> bool {
    LOB_REUSE_UNCHANGED_KNOB.get() != 0
}
/// M3 — `mydb_lob_shrink_disable`.
pub(crate) fn lob_shrink_disabled() -> bool {
    LOB_SHRINK_DISABLE_KNOB.get() != 0
}
/// M3 — `mydb_lob_shrink_min_free_pages` (floor 1).
pub(crate) fn lob_shrink_min_free_pages() -> u64 {
    LOB_SHRINK_MIN_FREE_PAGES_KNOB.get().max(1)
}
/// M4 — `mydb_lob_extent_min_pages` (0 disables).
pub(crate) fn lob_extent_min_pages() -> u64 {
    LOB_EXTENT_MIN_PAGES_KNOB.get()
}
/// M4 — `mydb_lob_prefetch_disable`.
pub(crate) fn lob_prefetch_disabled() -> bool {
    LOB_PREFETCH_DISABLE_KNOB.get() != 0
}
/// M5 — `mydb_lob_compression` (true when lz4).
pub(crate) fn lob_compression_lz4() -> bool {
    LOB_COMPRESSION_KNOB.get() != 0
}
/// M5 — `mydb_lob_compress_min_bytes`.
pub(crate) fn lob_compress_min_bytes() -> usize {
    LOB_COMPRESS_MIN_BYTES_KNOB.get() as usize
}

// ---- sysvar getter/setter adapters (registered by mydb_storage.rs) ----
// getters return u64 (booleans = 0/1, compression lz4 = 1); setters take u64.

pub(crate) fn sysvar_lob_inline_max_get() -> u64 {
    LOB_INLINE_MAX_KNOB.get()
}
pub(crate) fn sysvar_lob_inline_max_set(v: u64) {
    LOB_INLINE_MAX_KNOB.set(v);
}
pub(crate) fn sysvar_lob_reuse_unchanged_get() -> u64 {
    LOB_REUSE_UNCHANGED_KNOB.get()
}
pub(crate) fn sysvar_lob_reuse_unchanged_set(v: u64) {
    LOB_REUSE_UNCHANGED_KNOB.set(if v != 0 { 1 } else { 0 });
}
pub(crate) fn sysvar_lob_shrink_disable_get() -> u64 {
    LOB_SHRINK_DISABLE_KNOB.get()
}
pub(crate) fn sysvar_lob_shrink_disable_set(v: u64) {
    LOB_SHRINK_DISABLE_KNOB.set(if v != 0 { 1 } else { 0 });
}
pub(crate) fn sysvar_lob_shrink_min_free_pages_get() -> u64 {
    LOB_SHRINK_MIN_FREE_PAGES_KNOB.get()
}
pub(crate) fn sysvar_lob_shrink_min_free_pages_set(v: u64) {
    LOB_SHRINK_MIN_FREE_PAGES_KNOB.set(v);
}
pub(crate) fn sysvar_lob_extent_min_pages_get() -> u64 {
    LOB_EXTENT_MIN_PAGES_KNOB.get()
}
pub(crate) fn sysvar_lob_extent_min_pages_set(v: u64) {
    LOB_EXTENT_MIN_PAGES_KNOB.set(v);
}
pub(crate) fn sysvar_lob_prefetch_disable_get() -> u64 {
    LOB_PREFETCH_DISABLE_KNOB.get()
}
pub(crate) fn sysvar_lob_prefetch_disable_set(v: u64) {
    LOB_PREFETCH_DISABLE_KNOB.set(if v != 0 { 1 } else { 0 });
}
pub(crate) fn sysvar_lob_compression_get() -> u64 {
    LOB_COMPRESSION_KNOB.get()
}
pub(crate) fn sysvar_lob_compression_set(v: u64) {
    LOB_COMPRESSION_KNOB.set(if v != 0 { 1 } else { 0 });
}
pub(crate) fn sysvar_lob_compress_min_bytes_get() -> u64 {
    LOB_COMPRESS_MIN_BYTES_KNOB.get()
}
pub(crate) fn sysvar_lob_compress_min_bytes_set(v: u64) {
    LOB_COMPRESS_MIN_BYTES_KNOB.set(v);
}

#[cfg(test)]
pub(crate) fn set_lob_reuse_unchanged_for_test(on: bool) {
    LOB_REUSE_UNCHANGED_KNOB.set(if on { 1 } else { 0 });
}
