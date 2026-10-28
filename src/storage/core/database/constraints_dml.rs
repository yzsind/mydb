// Bound speculative leaf walks by input size, independently of key type or
// table cardinality. Sparse batches must not scan the whole existing index.
const UNIQUE_SCAN_ENTRIES_PER_KEY: usize = 4;

fn duplicate_entry_error(table: &TableSchema, index: &IndexSchema, key: &IndexKey) -> QueryError {
    let value = key
        .0
        .iter()
        .map(|part| match part {
            KeyPart::Null => "NULL".to_string(),
            KeyPart::Int(value) => value.to_string(),
            KeyPart::UInt(value) => value.to_string(),
            KeyPart::Decimal { value, scale } => {
                crate::util::decimal::format_decimal_parts(*value, *scale)
            }
            KeyPart::DecimalWide { value, scale } => {
                crate::util::decimal::format_big_decimal_parts(value, *scale)
            }
            KeyPart::Bytes(value) => String::from_utf8_lossy(value).into_owned(),
            KeyPart::String(value) | KeyPart::CollatedString { value, .. } => value.clone(),
            KeyPart::Date(value) => value.to_string(),
            KeyPart::Time(value) | KeyPart::DateTime(value) => value.to_string(),
            KeyPart::Year(value) => value.to_string(),
            KeyPart::Float32(value) => crate::storage::core::f32_from_order_u32(*value).to_string(),
            KeyPart::Float64(value) => crate::storage::core::f64_from_order_u64(*value).to_string(),
            KeyPart::Param { idx, .. } => format!("?{idx}"),
        })
        .collect::<Vec<_>>()
        .join("-");
    crate::error::messages::duplicate_entry(&value, &format!("{}.{}", table.name, index.name))
}

impl Database {
    pub(crate) fn materialize_generated_columns_for_insert(
        &self,
        table: &TableSchema,
        values: &mut [Value],
    ) -> Result<(), QueryError> {
        materialize_generated_columns(table, values, None)
    }

    fn check_constraints(
        &self,
        table: &TableSchema,
        txn: TxnView,
        current: Option<RowId>,
        values: &[Value],
    ) -> Result<(), QueryError> {
        self.check_not_null_constraints(table, values)?;
        self.check_check_constraints(table, values, None)?;
        self.lock_unique_keys_for_values(txn.txn_id, table, values)?;
        for index in table
            .indexes
            .iter()
            .filter(|index| index.enforces_uniqueness())
        {
            let key = table.index_key(index, values)?;
            if key.0.iter().any(|part| matches!(part, KeyPart::Null)) && !index.primary {
                continue;
            }
            self.check_unique_constraint(table, txn, current, index, key)?;
        }
        Ok(())
    }

    fn check_update_constraints(
        &self,
        table: &TableSchema,
        txn: TxnView,
        current: RowId,
        old_values: &[Value],
        new_values: &[Value],
        changed_columns: Option<&[usize]>,
    ) -> Result<(), QueryError> {
        self.check_update_constraints_with_released_keys(
            table,
            txn,
            current,
            old_values,
            new_values,
            changed_columns,
            None,
        )
    }

    fn check_update_constraints_with_released_keys(
        &self,
        table: &TableSchema,
        txn: TxnView,
        current: RowId,
        old_values: &[Value],
        new_values: &[Value],
        changed_columns: Option<&[usize]>,
        released_keys: Option<&[crate::util::fast_hash::AhHashSet<RowId>]>,
    ) -> Result<(), QueryError> {
        self.check_updated_not_null_constraints(table, new_values, changed_columns)?;
        self.check_check_constraints(table, new_values, changed_columns)?;
        self.lock_changed_unique_keys_for_update(
            txn.txn_id,
            table,
            old_values,
            new_values,
            changed_columns,
            None,
        )?;
        // Foreign-key child-side existence check (no-op when the table has
        // no outgoing FKs). Gated on the changed columns for UPDATE.
        self.fk_check_child_references(table, txn, new_values, changed_columns)?;
        let mut unique_scope = None;
        for (index_pos, index) in table
            .indexes
            .iter()
            .enumerate()
            .filter(|(_, index)| index.enforces_uniqueness())
        {
            if !index_may_be_affected_by_columns(table, index, changed_columns) {
                continue;
            }
            let old_key = table.index_key(index, old_values)?;
            let new_key = table.index_key(index, new_values)?;
            if old_key == new_key {
                continue;
            }
            if new_key.0.iter().any(|part| matches!(part, KeyPart::Null)) && !index.primary {
                continue;
            }
            // Predicate/ODKU pruning describes the source, not the new
            // key. Unique indexes include the partition key; probe only its
            // destination leaf. Install lazily so non-key updates do not
            // evaluate the partition function or allocate another scope.
            if table.is_partitioned() && unique_scope.is_none() {
                unique_scope = Some(
                    crate::engine::executor::partition_router::push_pruned_partitions(vec![
                        table.route_row_to_partition(new_values)?.storage_id,
                    ]),
                );
            }
            // Row ids are local to partition heaps. Since the new key differs
            // from this row's old key, it cannot conflict with itself; never
            // skip a same-numbered row in another partition.
            let current = table.partitioning.is_none().then_some(current);
            let released_rows = released_keys.and_then(|keys| keys.get(index_pos));
            self.check_unique_constraint_excluding(
                table,
                txn,
                current,
                index,
                new_key,
                released_rows,
            )?;
        }
        Ok(())
    }

    pub(crate) fn check_check_constraints(
        &self,
        table: &TableSchema,
        values: &[Value],
        changed_columns: Option<&[usize]>,
    ) -> Result<(), QueryError> {
        self.check_hnsw_index_dimensions(table, values, changed_columns)?;
        for check in table
            .check_constraints
            .iter()
            .filter(|check| check.enforced)
        {
            if let Some(changed_columns) = changed_columns {
                if !check.references_any_column(changed_columns, table) {
                    continue;
                }
            }
            if !check_constraint_passes(table, check, values)? {
                return Err(check_constraint_error(&check.name));
            }
        }
        Ok(())
    }

    /// Public wrapper over the `coerce_row` single source so the LOAD DATA
    /// parallel-encode worker can apply the same sql_mode-aware coercion
    /// (integer range clamp / 1264, string length / 1406, column charset
    /// validation, temporal, DECIMAL rescale) the serial path gets inside
    /// `insert_rows_heap_only_detailed`. Runs on the worker thread; the
    /// LOAD fast path is strict-only, and an unset sql_mode thread-local
    /// defaults to strict, so behaviour matches the session.
    pub(crate) fn coerce_row_for_load(
        &self,
        table: &TableSchema,
        values: &mut [Value],
    ) -> Result<(), QueryError> {
        coerce_row(table, values)
    }

    pub(crate) fn check_not_null_constraints(
        &self,
        table: &TableSchema,
        values: &[Value],
    ) -> Result<(), QueryError> {
        for (idx, column) in table.columns.iter().enumerate() {
            if column.state != ColumnState::Dropped
                && !column.nullable
                && matches!(values[idx], Value::Null)
            {
                return Err(crate::error::messages::bad_null(&column.name));
            }
        }
        Ok(())
    }

    /// M4.B6 — Batch NOT NULL check for a full INSERT batch.
    ///
    /// The single-row path walks `table.columns` once per row and
    /// re-derives the (small) set of non-nullable column indexes on
    /// every iteration. For a 10 000-row batch over a 50-column
    /// table that's 500 000 column accesses + 500 000 nullable
    /// branches; on a hot path that adds up.
    ///
    /// Here we lift the "which columns are non-nullable?" decision
    /// out of the row loop and walk every row over a tight,
    /// cache-friendly slice of column indexes. Behaviourally
    /// identical to calling `check_not_null_constraints` per row:
    /// the *first* row × column pair that violates a NOT NULL
    /// constraint is reported in the same order (row outer,
    /// column inner), so error messages match the legacy path.
    pub(crate) fn check_not_null_constraints_batch(
        &self,
        table: &TableSchema,
        rows: &[Vec<Value>],
    ) -> Result<(), QueryError> {
        if rows.is_empty() {
            return Ok(());
        }
        // Borrow a pre-collected `Vec<usize>` of non-nullable column
        // positions — cheap to build once, free to reuse across rows.
        let mut non_nullable: smallvec::SmallVec<[usize; 16]> = smallvec::SmallVec::new();
        for (idx, column) in table.columns.iter().enumerate() {
            if column.state != ColumnState::Dropped && !column.nullable {
                non_nullable.push(idx);
            }
        }
        if non_nullable.is_empty() {
            return Ok(());
        }
        for row in rows {
            for &idx in &non_nullable {
                if matches!(row.get(idx), Some(Value::Null) | None) {
                    return Err(crate::error::messages::bad_null(&table.columns[idx].name));
                }
            }
        }
        Ok(())
    }

    /// M4.B6 — Batch CHECK check for a full INSERT batch.
    ///
    /// Pre-filters to *enforced* CHECK constraints so the per-row
    /// loop doesn't keep re-iterating the entire `check_constraints`
    /// vector and re-applying `.filter(|c| c.enforced)`. Order of
    /// reporting matches the per-row path (row outer, check inner)
    /// so an INSERT that would have failed on row N, check K still
    /// fails on the same row + check.
    ///
    /// Subqueries / aggregates inside a CHECK constraint compile to
    /// `CompiledTableExpr::Legacy(expr)` and fall through to the
    /// legacy `eval_expr_legacy` per row — no change of semantics,
    /// no risk of incorrect skipping.
    pub(crate) fn check_check_constraints_batch(
        &self,
        table: &TableSchema,
        rows: &[Vec<Value>],
    ) -> Result<(), QueryError> {
        if rows.is_empty() {
            return Ok(());
        }
        if table
            .columns
            .iter()
            .any(|c| matches!(c.data_type, DataTypeKind::Vector { dim: None, .. }))
        {
            for row in rows {
                self.check_hnsw_index_dimensions(table, row, None)?;
            }
        }
        let active: smallvec::SmallVec<[&CheckConstraintSchema; 4]> = table
            .check_constraints
            .iter()
            .filter(|c| c.enforced)
            .collect();
        if active.is_empty() {
            return Ok(());
        }
        for row in rows {
            for check in &active {
                if !check_constraint_passes(table, check, row)? {
                    return Err(check_constraint_error(&check.name));
                }
            }
        }
        Ok(())
    }

    fn check_updated_not_null_constraints(
        &self,
        table: &TableSchema,
        values: &[Value],
        changed_columns: Option<&[usize]>,
    ) -> Result<(), QueryError> {
        let Some(changed_columns) = changed_columns else {
            return self.check_not_null_constraints(table, values);
        };
        for idx in changed_columns.iter().copied() {
            let Some(column) = table.columns.get(idx) else {
                continue;
            };
            if column.state != ColumnState::Dropped
                && !column.nullable
                && matches!(values[idx], Value::Null)
            {
                return Err(crate::error::messages::bad_null(&column.name));
            }
        }
        Ok(())
    }

    fn check_batch_unique_constraints(
        &self,
        table: &TableSchema,
        txn: TxnView,
        rows: &[Vec<Value>],
        batch_unique_keys: &BTreeMap<String, crate::util::fast_hash::AhHashSet<IndexKey>>,
    ) -> Result<(), QueryError> {
        for index in table
            .indexes
            .iter()
            .filter(|index| index.enforces_uniqueness())
        {
            let Some(keys) = batch_unique_keys.get(&index.name) else {
                continue;
            };
            if keys.is_empty() {
                continue;
            }
            // B5 — the rightmost-leaf fast path and the merge-join both probe
            // the `table.id` index handle, which does not exist for a
            // partitioned table (each partition has its own index). Using them
            // there silently skips the whole conflict check. Route partitioned
            // tables through the per-row `check_unique_constraint`, which uses
            // the partition-aware `lookup_index_entries`.
            if !table.is_partitioned()
                && self.batch_unique_keys_are_after_index(table, index, keys)?
            {
                continue;
            }
            // Large batches share internal-page routing across sorted exact
            // probes. A bounded merge scan remains the fallback for repair or
            // unusually many stale row versions per unique key. Small and
            // partitioned batches keep the existing point/partition checks.
            const MERGE_JOIN_THRESHOLD: usize = 256;
            if !table.is_partitioned() && keys.len() >= MERGE_JOIN_THRESHOLD {
                self.check_unique_constraint_via_merge(table, txn, index, rows)?;
                continue;
            }
            for values in rows {
                let _partition = if table.is_partitioned() {
                    Some(
                        crate::engine::executor::partition_router::push_pruned_partitions(vec![
                            table.route_row_to_partition(values)?.storage_id,
                        ]),
                    )
                } else {
                    None
                };
                let key = table.index_key(index, values)?;
                if key.0.iter().any(|part| matches!(part, KeyPart::Null)) && !index.primary {
                    continue;
                }
                self.check_unique_constraint(table, txn, None, index, key)?;
            }
        }
        Ok(())
    }

    /// Return the earliest source-row ordinal that would take an ODKU conflict
    /// branch. The probe covers both duplicate keys inside `rows` and live keys
    /// already present in the index, while reusing the canonical MVCC/race
    /// decision in `check_unique_conflict_at_row`.
    ///
    /// Partitioned tables probe each routed leaf in source order; ordinary
    /// tables route sorted probes together, with a bounded scan fallback.
    pub(crate) fn first_unique_conflict_ordinal(
        &self,
        table: &TableSchema,
        txn: TxnView,
        rows: &[Vec<Value>],
    ) -> Result<Option<usize>, QueryError> {
        if table.is_partitioned() {
            let mut seen = table
                .indexes
                .iter()
                .filter(|index| index.enforces_uniqueness())
                .map(|index| (index, crate::util::fast_hash::ah_hash_set()))
                .collect::<Vec<_>>();
            for (ordinal, values) in rows.iter().enumerate() {
                let sid = table.route_row_to_partition(values)?.storage_id;
                let _partition =
                    crate::engine::executor::partition_router::push_pruned_partitions(vec![sid]);
                for (index, keys) in &mut seen {
                    let key = table.index_key(index, values)?;
                    if !index.primary && key.0.iter().any(|part| matches!(part, KeyPart::Null)) {
                        continue;
                    }
                    if !keys.insert(key.clone()) {
                        return Ok(Some(ordinal));
                    }
                    match self.check_unique_constraint(table, txn, None, index, key) {
                        Err(err) if err.is(crate::error::catalog::ER_DUP_ENTRY) => {
                            return Ok(Some(ordinal))
                        }
                        Err(err) => return Err(err),
                        Ok(()) => {}
                    }
                }
            }
            return Ok(None);
        }
        let mut earliest: Option<usize> = None;

        for index in table
            .indexes
            .iter()
            .filter(|index| index.enforces_uniqueness())
        {
            let mut key_ordinals =
                crate::util::fast_hash::ah_hash_map_with_capacity::<IndexKey, usize>(rows.len());
            let mut min_key: Option<IndexKey> = None;
            let mut max_key: Option<IndexKey> = None;
            for (ordinal, values) in rows.iter().enumerate() {
                let key = table.index_key(index, values)?;
                if key.0.iter().any(|part| matches!(part, KeyPart::Null)) && !index.primary {
                    continue;
                }
                match key_ordinals.entry(key) {
                    hashbrown::hash_map::Entry::Vacant(entry) => {
                        let key = entry.key();
                        if min_key.as_ref().is_none_or(|current| key < current) {
                            min_key = Some(key.clone());
                        }
                        if max_key.as_ref().is_none_or(|current| key > current) {
                            max_key = Some(key.clone());
                        }
                        entry.insert(ordinal);
                    }
                    hashbrown::hash_map::Entry::Occupied(_) => {
                        earliest = Some(earliest.map_or(ordinal, |prior| prior.min(ordinal)));
                    }
                }
            }
            if key_ordinals.is_empty() {
                continue;
            }
            if earliest == Some(0) {
                return Ok(earliest);
            }

            let min_key_ref = min_key.as_ref().expect("non-empty key map");
            if self.batch_min_unique_key_is_after_index(table, index, min_key_ref)? {
                continue;
            }

            const MERGE_JOIN_THRESHOLD: usize = 256;
            if key_ordinals.len() < MERGE_JOIN_THRESHOLD {
                self.probe_unique_conflict_ordinals(
                    table,
                    txn,
                    index,
                    &key_ordinals,
                    &mut earliest,
                )?;
                continue;
            }

            // Route distinct probes together instead of first scanning an
            // unrelated interval and then descending from the root per key.
            let mut sorted_keys: Vec<_> = key_ordinals.keys().cloned().collect();
            sorted_keys.sort_unstable();
            if let Some(matches) = self.probe_unique_keys_routed(table, txn, index, &sorted_keys)? {
                for (key_index, row_id) in matches {
                    let key = &sorted_keys[key_index as usize];
                    let ordinal = key_ordinals[key];
                    if earliest.is_some_and(|prior| ordinal >= prior) {
                        continue;
                    }
                    match self.check_unique_conflict_at_row(
                        table, txn, None, index, key, table.id, row_id,
                    ) {
                        Err(err) if err.is(crate::error::catalog::ER_DUP_ENTRY) => {
                            earliest = Some(earliest.map_or(ordinal, |prior| prior.min(ordinal)));
                        }
                        Err(err) => return Err(err),
                        Ok(()) => {}
                    }
                    if earliest == Some(0) {
                        return Ok(earliest);
                    }
                }
                continue;
            }

            let low = min_key.as_ref().map(|key| IndexBound {
                key: key.clone(),
                inclusive: true,
            });
            let high = max_key.as_ref().map(|key| IndexBound {
                key: key.clone(),
                inclusive: true,
            });
            const CHUNK: usize = 1024;
            let mut scanned = 0usize;
            let scan_budget = key_ordinals
                .len()
                .saturating_mul(UNIQUE_SCAN_ENTRIES_PER_KEY);
            let mut resume: Option<(IndexKey, RowId)> = None;
            loop {
                let chunk = self.range_index_entries_chunk(
                    table,
                    index,
                    low.clone(),
                    high.clone(),
                    resume.clone(),
                    CHUNK,
                )?;
                if chunk.is_empty() {
                    break;
                }
                scanned = scanned.saturating_add(chunk.len());
                let mut last_entry = None;
                for (index_key, row_id) in chunk {
                    last_entry = Some((index_key.clone(), row_id));
                    let Some(ordinal) = key_ordinals.get(&index_key).copied() else {
                        continue;
                    };
                    self.check_unique_key_reservation(
                        txn.txn_id,
                        table.id,
                        &index.name,
                        &index_key,
                    )?;
                    match self.check_unique_conflict_at_row(
                        table, txn, None, index, &index_key, table.id, row_id,
                    ) {
                        Err(err) if err.is(crate::error::catalog::ER_DUP_ENTRY) => {
                            earliest = Some(earliest.map_or(ordinal, |prior| prior.min(ordinal)));
                        }
                        Err(err) => return Err(err),
                        Ok(()) => {}
                    }
                    if earliest == Some(0) {
                        return Ok(earliest);
                    }
                }
                resume = last_entry;
                if scanned >= scan_budget {
                    self.probe_unique_conflict_ordinals(
                        table,
                        txn,
                        index,
                        &key_ordinals,
                        &mut earliest,
                    )?;
                    break;
                }
            }
        }
        Ok(earliest)
    }

    /// Bounded current-read candidates for a sorted, concrete, unpartitioned
    /// unique-key batch. Locks and heap/MVCC validation remain authoritative;
    /// only the index traversal is shared across probes.
    fn probe_unique_keys_routed(
        &self,
        table: &TableSchema,
        txn: TxnView,
        index: &IndexSchema,
        keys: &[IndexKey],
    ) -> Result<Option<Vec<(u32, RowId)>>, QueryError> {
        if table.is_partitioned() || self.has_pending_index_repairs() {
            return Ok(None);
        }
        for key in keys {
            self.check_unique_key_reservation(
                txn.txn_id,
                table.id,
                &index.name,
                key,
            )?;
        }
        self.with_index_read(table.id, &index.name, |file| {
            file.lookup_entries_many_sorted_keyed_bounded(
                keys,
                true,
                false,
                keys.len().saturating_mul(UNIQUE_SCAN_ENTRIES_PER_KEY),
            )
        })
    }

    fn probe_unique_conflict_ordinals(
        &self,
        table: &TableSchema,
        txn: TxnView,
        index: &IndexSchema,
        key_ordinals: &crate::util::fast_hash::AhHashMap<IndexKey, usize>,
        earliest: &mut Option<usize>,
    ) -> Result<(), QueryError> {
        for (key, ordinal) in key_ordinals {
            if earliest.is_some_and(|prior| *ordinal >= prior) {
                continue;
            }
            self.check_unique_key_reservation(
                txn.txn_id,
                table.id,
                &index.name,
                key,
            )?;
            for (storage_id, row_id) in
                self.lookup_index_row_locations(table, index, key.clone())?
            {
                match self
                    .check_unique_conflict_at_row(table, txn, None, index, key, storage_id, row_id)
                {
                    Err(err) if err.is(crate::error::catalog::ER_DUP_ENTRY) => {
                        *earliest = Some(earliest.map_or(*ordinal, |prior| prior.min(*ordinal)));
                        break;
                    }
                    Err(err) => return Err(err),
                    Ok(()) => {}
                }
            }
            if *earliest == Some(0) {
                break;
            }
        }
        Ok(())
    }

    /// M3.B9 — Sorted-merge UNIQUE check.
    ///
    /// Sort the batch's unique keys, then range-scan the index over
    /// `[min, max]` with a streaming cursor. The two sorted sequences
    /// advance in step: for every key the index emits, we skip ahead
    /// in the batch until we hit a key that is ≥ the index key, then
    /// either it matches (potential conflict — full MVCC / lock check
    /// at the matching row_id) or it is strictly greater (no
    /// conflict; keep scanning the index).
    ///
    /// The MVCC + lock semantics are identical to per-row
    /// `check_unique_constraint`: we factor them into
    /// `check_unique_conflict_at_row` and reuse from both paths so
    /// they cannot drift.
    fn check_unique_constraint_via_merge(
        &self,
        table: &TableSchema,
        txn: TxnView,
        index: &IndexSchema,
        rows: &[Vec<Value>],
    ) -> Result<(), QueryError> {
        let mut sorted_keys: Vec<IndexKey> = Vec::with_capacity(rows.len());
        for values in rows {
            let key = table.index_key(index, values)?;
            if key.0.iter().any(|part| matches!(part, KeyPart::Null)) && !index.primary {
                continue;
            }
            sorted_keys.push(key);
        }
        if sorted_keys.is_empty() {
            return Ok(());
        }
        sorted_keys.sort_unstable();
        sorted_keys.dedup();

        if let Some(matches) = self.probe_unique_keys_routed(table, txn, index, &sorted_keys)? {
            for (key_index, row_id) in matches {
                self.check_unique_conflict_at_row(
                    table,
                    txn,
                    None,
                    index,
                    &sorted_keys[key_index as usize],
                    table.id,
                    row_id,
                )?;
            }
            return Ok(());
        }

        let low_key = sorted_keys.first().cloned();
        let high_key = sorted_keys.last().cloned();
        let low = low_key.map(|key| IndexBound {
            key,
            inclusive: true,
        });
        let high = high_key.map(|key| IndexBound {
            key,
            inclusive: true,
        });

        const CHUNK: usize = 1024;
        let mut scanned = 0usize;
        let scan_budget = sorted_keys
            .len()
            .saturating_mul(UNIQUE_SCAN_ENTRIES_PER_KEY);
        let mut batch_ptr: usize = 0;
        let mut resume: Option<(IndexKey, RowId)> = None;
        loop {
            if batch_ptr >= sorted_keys.len() {
                return Ok(());
            }
            let chunk = self.range_index_entries_chunk(
                table,
                index,
                low.clone(),
                high.clone(),
                resume.clone(),
                CHUNK,
            )?;
            if chunk.is_empty() {
                return Ok(());
            }
            scanned = scanned.saturating_add(chunk.len());
            let mut last_entry = None;
            for (idx_key, idx_row_id) in chunk {
                last_entry = Some((idx_key.clone(), idx_row_id));
                while batch_ptr < sorted_keys.len() && sorted_keys[batch_ptr] < idx_key {
                    batch_ptr += 1;
                }
                if batch_ptr >= sorted_keys.len() {
                    return Ok(());
                }
                if sorted_keys[batch_ptr] == idx_key {
                    // Acquire the unique-key lock (same as per-row
                    // path), then validate the conflict against the
                    // exact row_id the index pointed at — no need to
                    // re-descend the B-tree.
                    self.check_unique_key_reservation(
                        txn.txn_id,
                        table.id,
                        &index.name,
                        &idx_key,
                    )?;
                    // Merge-join only runs for unpartitioned tables (gated in
                    // `check_batch_unique_constraints`), so the owning heap is
                    // `table.id`.
                    self.check_unique_conflict_at_row(
                        table, txn, None, index, &idx_key, table.id, idx_row_id,
                    )?;
                    // Keep `batch_ptr` on this key. Deferred index
                    // maintenance can leave old and new row_ids beside
                    // each other for the same unique key; a stale
                    // tombstone may be harmless while a later entry is
                    // the live conflict we must catch.
                }
            }
            resume = last_entry;
            if scanned >= scan_budget {
                // Keep the current key: a chunk can end between stale and live
                // entries for that same key. Exact probes retain MVCC semantics.
                for key in &sorted_keys[batch_ptr..] {
                    self.check_unique_constraint(table, txn, None, index, key.clone())?;
                }
                return Ok(());
            }
        }
    }

    /// MVCC + lock check that a candidate `key` does not conflict with
    /// the row at `row_id`. Mirrors the inner loop of
    /// `check_unique_constraint` so the per-row and merge-join paths
    /// stay in lock-step. `current` is the UPDATE caller's own row
    /// (skipped from the conflict check); INSERT passes `None`.
    #[allow(clippy::too_many_arguments)]
    fn check_unique_conflict_at_row(
        &self,
        table: &TableSchema,
        txn: TxnView,
        current: Option<RowId>,
        index: &IndexSchema,
        key: &IndexKey,
        // Partition heap that owns `row_id`. For unpartitioned tables this is
        // `table.id`; for partitioned tables it MUST be the origin partition
        // (from `lookup_index_entries`) — reading via `table.id` would hit a
        // non-existent heap and silently report "no conflict".
        storage_id: u64,
        row_id: RowId,
    ) -> Result<(), QueryError> {
        if Some(row_id) == current {
            return Ok(());
        }
        let Some(latest) = self.read_mvcc_row(storage_id, row_id)? else {
            return Ok(());
        };
        if table.index_key(index, &latest.values)? == *key {
            let writer_status = self.txns_read()?.status(latest.header.last_txn_id);
            match writer_status {
                Some(TxnStatus::Active | TxnStatus::Prepared)
                    if latest.header.last_txn_id != txn.txn_id =>
                {
                    self.check_row_lock_nonblocking(txn.txn_id, table.id, row_id)?;
                    return Err(crate::storage::core::lock_wait_pending_error());
                }
                Some(TxnStatus::Committed) | None
                    if !latest.header.is_deleted() && latest.header.last_txn_id != txn.txn_id =>
                {
                    // Uniqueness is a current-read constraint, including in
                    // RR after waiting for another writer. A committed live
                    // key is a duplicate regardless of our snapshot age. Real
                    // deadlocks remain the lock manager's responsibility.
                    return Err(duplicate_entry_error(table, index, key));
                }
                _ => {}
            }
            if latest.header.last_txn_id == txn.txn_id && !latest.header.is_deleted() {
                return Err(duplicate_entry_error(table, index, key));
            }
        }
        Ok(())
    }

    fn batch_unique_keys_are_after_index(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        keys: &crate::util::fast_hash::AhHashSet<IndexKey>,
    ) -> Result<bool, QueryError> {
        let Some(min_key) = keys.iter().min() else {
            return Ok(true);
        };
        self.batch_min_unique_key_is_after_index(table, index, min_key)
    }

    fn batch_min_unique_key_is_after_index(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        min_key: &IndexKey,
    ) -> Result<bool, QueryError> {
        // A pending visibility/index repair may contribute a key that is not
        // present in the physical rightmost leaf yet. The full lookup paths
        // merge those repair entries; the max-key shortcut cannot, so disable
        // it until repairs drain.
        if self.has_pending_index_repairs() {
            return Ok(false);
        }
        // Read-only rightmost-leaf probe: `last_key_is_before()` is `&self` navigation,
        // identical in shape to the lookups that already run under the shared
        // `with_index_read` handle lock. Taking the EXCLUSIVE `with_index_mut`
        // here serialised every concurrent INSERT's unique pre-check on the
        // index handle (e.g. all 20 TPC-C terminals appending to `order_line`),
        // blocking the shared `with_index_dml` insert navigators behind a
        // writer for a pure read. The shared lock is sufficient: B-tree
        // structural changes are serialised internally, so a concurrent split
        // cannot tear this rightmost navigation.
        self.with_index_read(table.id, &index.name, |index_file| {
            index_file.last_key_is_before(min_key)
        })
    }

    fn check_unique_constraint(
        &self,
        table: &TableSchema,
        txn: TxnView,
        current: Option<RowId>,
        index: &IndexSchema,
        key: IndexKey,
    ) -> Result<(), QueryError> {
        self.check_unique_constraint_excluding(table, txn, current, index, key, None)
    }

    fn check_unique_constraint_excluding(
        &self,
        table: &TableSchema,
        txn: TxnView,
        current: Option<RowId>,
        index: &IndexSchema,
        key: IndexKey,
        released_rows: Option<&crate::util::fast_hash::AhHashSet<RowId>>,
    ) -> Result<(), QueryError> {
        self.check_unique_key_reservation(
            txn.txn_id,
            table.id,
            &index.name,
            &key,
        )?;
        for (storage_id, row_id) in self.lookup_index_row_locations(table, index, key.clone())? {
            // The earlier row is locked and its accepted update releases this
            // old key. Batch-local new-key reservations remain authoritative.
            if storage_id == table.id && released_rows.is_some_and(|rows| rows.contains(&row_id)) {
                continue;
            }
            self.check_unique_conflict_at_row(
                table, txn, current, index, &key, storage_id, row_id,
            )?;
        }
        Ok(())
    }
}
