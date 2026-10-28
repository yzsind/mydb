static CONSTRAINT_LOCK_HOLD_US: AtomicU64 = AtomicU64::new(0);
static CONSTRAINT_LOCK_HOLD_MAX_US: AtomicU64 = AtomicU64::new(0);
static CONSTRAINT_SCANNED_ROWS: AtomicU64 = AtomicU64::new(0);
static CONSTRAINT_REPLAYED_ROWS: AtomicU64 = AtomicU64::new(0);
static CONSTRAINT_FINAL_TAIL_MAX: AtomicU64 = AtomicU64::new(0);
static CONSTRAINT_INDEX_MERGE_ACCEPTED: AtomicU64 = AtomicU64::new(0);
static CONSTRAINT_INDEX_MERGE_FALLBACKS: AtomicU64 = AtomicU64::new(0);
static CONSTRAINT_INDEX_MERGE_ENTRIES: AtomicU64 = AtomicU64::new(0);
static CONSTRAINT_INDEX_MERGE_PARALLEL_RUNS: AtomicU64 = AtomicU64::new(0);
static CONSTRAINT_INDEX_MERGE_WORKERS_PEAK: AtomicU64 = AtomicU64::new(0);
static CONSTRAINT_BASELINE_PARALLEL_RUNS: AtomicU64 = AtomicU64::new(0);
static CONSTRAINT_BASELINE_WORKERS_PEAK: AtomicU64 = AtomicU64::new(0);
static CONSTRAINT_BASELINE_BUILDS: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
thread_local! {
    static CONSTRAINT_LAST_INDEX_MERGE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

struct ConstraintHoldTimer(Instant);
impl Drop for ConstraintHoldTimer {
    fn drop(&mut self) {
        let us = self.0.elapsed().as_micros().min(u64::MAX as u128) as u64;
        CONSTRAINT_LOCK_HOLD_US.fetch_add(us, Ordering::Relaxed);
        CONSTRAINT_LOCK_HOLD_MAX_US.fetch_max(us, Ordering::Relaxed);
    }
}

// Online CHECK/FK validation uses the same transaction-qualified dirty-row
// capture as online COPY. Candidate constraints are private until publication;
// business writes keep their existing semantics, including rollback/savepoints.
type ConstraintRow = (u64, RowId);

#[cfg(test)]
thread_local! {
    static CONSTRAINT_VALIDATION_LIMIT: std::cell::Cell<usize> = const { std::cell::Cell::new(usize::MAX) };
}

struct ConstraintValidationMemory {
    limit_bytes: usize,
    charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
    spill_caches: Vec<std::rc::Weak<RefCell<ConstraintSpillCache>>>,
}
impl ConstraintValidationMemory {
    fn new() -> Result<Self, QueryError> {
        let limit_bytes =
            crate::engine::executor::mem_budget::query_mem_limit_bytes().min(256 * 1024 * 1024);
        #[cfg(test)]
        let limit_bytes = CONSTRAINT_VALIDATION_LIMIT.with(|limit| limit.get().min(limit_bytes));
        Ok(Self {
            limit_bytes,
            spill_caches: Vec::new(),
            charge: crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::DdlOnlineLog,
                0,
            )?,
        })
    }
    fn reclaim_spill_caches(&mut self) -> Result<(), QueryError> {
        for cache in &self.spill_caches {
            if let Some(cache) = cache.upgrade() {
                let released = cache.borrow_mut().release()?;
                self.charge.shrink(released);
            }
        }
        Ok(())
    }
    fn grow(&mut self, bytes: usize) -> Result<(), QueryError> {
        if self.try_grow(bytes) {
            return Ok(());
        }
        // Retry with error propagation: optional admission must not hide a
        // writeback failure or let caching prevent mandatory validation work.
        self.reclaim_spill_caches()?;
        if self.charge.bytes().saturating_add(bytes) > self.limit_bytes {
            return Err(error(
                crate::error::catalog::ER_OUTOFMEMORY,
                "online constraint validation exceeded its memory budget",
            ));
        }
        self.charge.try_grow(bytes)
    }
    fn try_grow(&mut self, bytes: usize) -> bool {
        if self.charge.bytes().saturating_add(bytes) <= self.limit_bytes
            && self.charge.try_grow_optional(bytes)
        {
            return true;
        }
        self.reclaim_spill_caches().is_ok()
            && self.charge.bytes().saturating_add(bytes) <= self.limit_bytes
            && self.charge.try_grow_optional(bytes)
    }
    fn shrink(&mut self, bytes: usize) {
        self.charge.shrink(bytes);
    }
}

fn constraint_key_payload_bytes(key: &IndexKey) -> usize {
    key.0.iter().map(key_part_payload_capacity).sum::<usize>()
        + if key.0.spilled() {
            key.0.capacity() * std::mem::size_of::<KeyPart>()
        } else {
            0
        }
}

struct OnlineCheckValidation {
    check: CheckConstraintSchema,
    invalid: BTreeSet<ConstraintRow>,
}

struct OnlineFkValidation {
    fk: ForeignKeySchema,
    parent: Arc<TableSchema>,
    index: IndexSchema,
    parent_columns: Vec<usize>,
    // Canonical parent-typed keys use the ordinary FK collation/coercion.
    counts: crate::util::fast_hash::AhHashMap<IndexKey, (u64, u64)>,
    bucket_bytes: usize,
    payload_bytes: usize,
    scratch_bytes: usize,
    data_dir: PathBuf,
    spill: Option<OnlineFkDiskCounts>,
    baseline: Option<ConstraintFkBaseline>,
    orphan_groups: usize,
}
impl OnlineFkValidation {
    fn replace(
        &mut self,
        old: Option<IndexKey>,
        new: Option<IndexKey>,
        parent: bool,
        memory: &mut ConstraintValidationMemory,
    ) -> Result<(), QueryError> {
        if old == new {
            return Ok(());
        }
        if let Some(key) = old {
            self.adjust(key, parent, false, memory)?;
        }
        if let Some(key) = new {
            self.adjust(key, parent, true, memory)?;
        }
        Ok(())
    }

    fn spill(&mut self, memory: &mut ConstraintValidationMemory) -> Result<(), QueryError> {
        if self.spill.is_some() {
            return Ok(());
        }
        let mut disk = OnlineFkDiskCounts::new(
            &self.data_dir,
            self.counts.hasher().clone(),
            self.scratch_bytes,
        )?;
        disk.enable_migration_cache(memory);
        for (key, counts) in self.counts.drain() {
            crate::engine::cancel::check_cancelled()?;
            disk.insert(&key, disk.hash(&key), counts)?;
        }
        self.counts = crate::util::fast_hash::ah_hash_map();
        // Flush before changing the direct-mapped cache geometry. The map's
        // reservation remained live throughout migration; only now can its
        // capacity be transferred to the long-lived spill cache.
        let migration_bytes = disk.cache.borrow_mut().release()?;
        memory.shrink(migration_bytes);
        // Reuse the released map and optionally admit unused query capacity.
        // This stays inside both existing limits; mandatory state can reclaim
        // the cache before attempting its own allocation.
        let released = self.bucket_bytes + self.payload_bytes;
        let mut extra = memory.limit_bytes.saturating_sub(memory.charge.bytes());
        while extra != 0 && !memory.charge.try_grow_optional(extra) {
            extra /= 2;
        }
        let cached = disk.enable_cache(released + extra);
        if cached != 0 {
            memory
                .spill_caches
                .push(std::rc::Rc::downgrade(&disk.cache));
        }
        memory.shrink(released + extra - cached);
        self.bucket_bytes = 0;
        self.payload_bytes = 0;
        self.spill = Some(disk);
        Ok(())
    }

    fn adjust(
        &mut self,
        key: IndexKey,
        parent: bool,
        add: bool,
        memory: &mut ConstraintValidationMemory,
    ) -> Result<(), QueryError> {
        // Reserve scratch before the map can fill the budget. Four times the
        // encoded upper bound covers encoding, collision decoding and allocator
        // rounding for arbitrary key arity and variable-width values.
        let payload = constraint_key_payload_bytes(&key);
        let scratch = (payload + (key.0.len() + 1) * std::mem::size_of::<KeyPart>() + 128)
            .saturating_mul(4)
            .max(16 * 1024);
        if scratch > self.scratch_bytes {
            let growth = scratch - self.scratch_bytes;
            if !memory.try_grow(growth) {
                if !self.counts.is_empty() {
                    self.spill(memory)?;
                }
                memory.grow(growth)?;
            }
            self.scratch_bytes = scratch;
            if let Some(disk) = &mut self.spill {
                if disk.buffer.capacity() < scratch / 4 {
                    disk.buffer.reserve_exact(scratch / 4 - disk.buffer.len());
                }
            }
        }
        if self.spill.is_none() && !self.counts.contains_key(&key) {
            if !add && self.baseline.is_none() {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "online FK replay lost its prior key state",
                ));
            }
            if self.counts.len() == self.counts.capacity() {
                let stride = std::mem::size_of::<(IndexKey, (u64, u64))>() + 16;
                let reservation = self.bucket_bytes.max(8 * stride) * 2;
                if !memory.try_grow(reservation) {
                    self.spill(memory)?;
                } else {
                    self.counts.try_reserve(1).map_err(|_| {
                        error(
                            crate::error::catalog::ER_OUTOFMEMORY,
                            "online FK key map allocation failed",
                        )
                    })?;
                    let bytes =
                        (self.counts.capacity() * 8).div_ceil(7).next_power_of_two() * stride;
                    let growth = bytes.saturating_sub(self.bucket_bytes);
                    memory.shrink(reservation.saturating_sub(growth));
                    self.bucket_bytes = self.bucket_bytes.max(bytes);
                }
            }
            if self.spill.is_none() {
                if memory.try_grow(payload) {
                    self.payload_bytes += payload;
                    let initial = self
                        .baseline
                        .as_ref()
                        .map(|base| base.get(&key))
                        .transpose()?
                        .unwrap_or((0, 0));
                    self.counts.insert(key.clone(), initial);
                } else {
                    self.spill(memory)?;
                }
            }
        }
        let (old, new) = if let Some(disk) = &mut self.spill {
            if let Some(baseline) = &self.baseline {
                let hash = disk.hash(&key);
                if disk.find(&key, hash)?.is_none() {
                    disk.insert(&key, hash, baseline.get(&key)?)?;
                }
            }
            disk.update(&key, parent, add)?
        } else {
            let counts = self.counts.get_mut(&key).unwrap();
            let old = *counts;
            *counts = adjusted_fk_counts(old, parent, add)?;
            let new = *counts;
            // A zero overlay is a tombstone overriding the immutable baseline.
            if new == (0, 0) && self.baseline.is_none() {
                let (removed, _) = self.counts.remove_entry(&key).unwrap();
                let bytes = constraint_key_payload_bytes(&removed);
                memory.shrink(bytes);
                self.payload_bytes -= bytes;
            }
            (old, new)
        };
        if old.0 > 0 && old.1 == 0 {
            self.orphan_groups -= 1;
        }
        if new.0 > 0 && new.1 == 0 {
            self.orphan_groups += 1;
        }
        Ok(())
    }
}

struct OnlineConstraintValidation {
    child: Arc<TableSchema>,
    checks: Vec<OnlineCheckValidation>,
    fks: Vec<OnlineFkValidation>,
    columns: Vec<usize>,
    column_errors: BTreeMap<ConstraintRow, QueryError>,
    watermark: Option<TxnView>,
    memory: ConstraintValidationMemory,
}
impl OnlineConstraintValidation {
    fn replace_column_values(
        &mut self,
        location: ConstraintRow,
        values: Option<&[Value]>,
        projected: bool,
    ) -> Result<(), QueryError> {
        if let Some(previous) = self.column_errors.remove(&location) {
            self.memory.shrink(96 + previous.message().len());
        }
        let Some(values) = values else { return Ok(()) };
        for (ordinal, position) in self.columns.iter().copied().enumerate() {
            let column = &self.child.columns[position];
            let value = values
                .get(if projected { ordinal } else { position })
                .unwrap_or(&Value::Null);
            let violation = if !column.nullable && matches!(value, Value::Null) {
                Some(crate::error::messages::bad_null(&column.name))
            } else {
                coerce_column_value_for_schema(value.clone(), column).err()
            };
            if let Some(err) = violation {
                self.memory.grow(96 + err.message().len())?;
                self.column_errors.insert(location, err);
                break;
            }
        }
        Ok(())
    }

    fn uses_table(&self, table_id: u64) -> bool {
        table_id == self.child.id || self.fks.iter().any(|fk| fk.parent.id == table_id)
    }

    fn replace(
        &mut self,
        db: &Database,
        table_id: u64,
        location: ConstraintRow,
        previous_values: Option<&[Value]>,
        values: Option<&[Value]>,
        initial: bool,
    ) -> Result<(), QueryError> {
        if table_id == self.child.id {
            self.replace_column_values(location, values, false)?;
            for state in &mut self.checks {
                let invalid = match values {
                    Some(values) => !check_constraint_passes(&self.child, &state.check, values)?,
                    None => false,
                };
                if invalid && !state.invalid.contains(&location) {
                    self.memory.grow(96)?;
                    state.invalid.insert(location);
                } else if !invalid && state.invalid.remove(&location) {
                    self.memory.shrink(96);
                }
            }
        }
        for state in &mut self.fks {
            if initial && state.baseline.is_some() {
                continue;
            }
            if table_id == self.child.id {
                let key = values
                    .map(|values| {
                        db.fk_child_reference_key(&state.fk, &state.parent, &state.index, values)
                    })
                    .transpose()?
                    .flatten();
                let old_key = previous_values
                    .map(|values| {
                        db.fk_child_reference_key(&state.fk, &state.parent, &state.index, values)
                    })
                    .transpose()?
                    .flatten();
                state.replace(old_key, key, false, &mut self.memory)?;
            }
            if table_id == state.parent.id {
                let key = values
                    .map(|values| {
                        let key_values: Vec<Value> = state
                            .parent_columns
                            .iter()
                            .map(|&idx| values[idx].clone())
                            .collect();
                        if key_values.iter().any(|v| matches!(v, Value::Null)) {
                            return Ok(None);
                        }
                        fk_build_key(
                            &state.parent,
                            &state.index,
                            &state.fk.referenced_columns,
                            &key_values,
                        )
                        .map(Some)
                    })
                    .transpose()?
                    .flatten();
                let old_key = previous_values
                    .map(|values| {
                        let key_values: Vec<Value> = state
                            .parent_columns
                            .iter()
                            .map(|&idx| values[idx].clone())
                            .collect();
                        if key_values.iter().any(|v| matches!(v, Value::Null)) {
                            return Ok(None);
                        }
                        fk_build_key(
                            &state.parent,
                            &state.index,
                            &state.fk.referenced_columns,
                            &key_values,
                        )
                        .map(Some)
                    })
                    .transpose()?
                    .flatten();
                state.replace(old_key, key, true, &mut self.memory)?;
            }
        }
        Ok(())
    }
    fn finish(&self) -> Result<(), QueryError> {
        if let Some(err) = self.column_errors.values().next() {
            return Err(err.clone());
        }
        for state in &self.checks {
            if !state.invalid.is_empty() {
                return Err(check_constraint_error(&state.check.name));
            }
        }
        for state in &self.fks {
            if state.orphan_groups != 0 {
                return Err(fk_child_violation_err(&self.child, &state.fk));
            }
        }
        Ok(())
    }
}

impl Database {
    #[cfg(test)]
    pub(crate) fn with_constraint_validation_limit_for_test<T>(
        &self,
        limit: usize,
        f: impl FnOnce() -> T,
    ) -> T {
        struct Reset(usize);
        impl Drop for Reset {
            fn drop(&mut self) {
                CONSTRAINT_VALIDATION_LIMIT.with(|v| v.set(self.0));
            }
        }
        let _reset = Reset(CONSTRAINT_VALIDATION_LIMIT.with(|v| v.replace(limit)));
        f()
    }

    pub(crate) fn online_constraint_perf_snapshot(&self) -> [u64; 13] {
        [
            CONSTRAINT_LOCK_HOLD_US.load(Ordering::Relaxed),
            CONSTRAINT_LOCK_HOLD_MAX_US.load(Ordering::Relaxed),
            CONSTRAINT_SCANNED_ROWS.load(Ordering::Relaxed),
            CONSTRAINT_REPLAYED_ROWS.load(Ordering::Relaxed),
            CONSTRAINT_FINAL_TAIL_MAX.load(Ordering::Relaxed),
            CONSTRAINT_INDEX_MERGE_ACCEPTED.load(Ordering::Relaxed),
            CONSTRAINT_INDEX_MERGE_FALLBACKS.load(Ordering::Relaxed),
            CONSTRAINT_INDEX_MERGE_ENTRIES.load(Ordering::Relaxed),
            CONSTRAINT_BASELINE_BUILDS.load(Ordering::Relaxed),
            CONSTRAINT_INDEX_MERGE_PARALLEL_RUNS.load(Ordering::Relaxed),
            CONSTRAINT_INDEX_MERGE_WORKERS_PEAK.load(Ordering::Relaxed),
            CONSTRAINT_BASELINE_PARALLEL_RUNS.load(Ordering::Relaxed),
            CONSTRAINT_BASELINE_WORKERS_PEAK.load(Ordering::Relaxed),
        ]
    }

    /// Shared by statement MDL, capability checks and dispatch. Constraint-only
    /// compound ALTERs publish one candidate; unrelated action mixes stay offline.
    pub(crate) fn constraint_alter_supported(actions: &[crate::parser::AlterAction]) -> bool {
        use crate::parser::AlterAction as A;
        !actions.is_empty()
            && actions.iter().all(|a| {
                matches!(
                    a,
                    A::AddCheck(_)
                        | A::AlterCheck { .. }
                        | A::DropCheck { .. }
                        | A::AddForeignKey(_)
                        | A::DropForeignKey { .. }
                )
            })
    }

    pub(crate) fn constraint_alter_dependencies(
        &self,
        table_name: &str,
        actions: &[crate::parser::AlterAction],
    ) -> Vec<String> {
        let (database, _) = split_table_name(DEFAULT_DATABASE, table_name);
        let mut names = BTreeSet::from([table_name.to_string()]);
        if let Ok(source) = self.table(table_name) {
            let column_change = actions.iter().any(|action| {
                matches!(
                    action,
                    crate::parser::AlterAction::ModifyColumn(_)
                        | crate::parser::AlterAction::ChangeColumn { .. }
                )
            });
            if column_change {
                names.extend(
                    source
                        .fk_referenced_by
                        .iter()
                        .map(|edge| edge.child_table_key.clone()),
                );
            }
            for fk in &source.foreign_keys {
                if column_change
                    || actions.iter().any(|action| match action {
                        crate::parser::AlterAction::DropForeignKey { name }
                        | crate::parser::AlterAction::DropCheck {
                            name,
                            include_foreign_keys: true,
                            ..
                        } => fk.name.eq_ignore_ascii_case(name),
                        _ => false,
                    })
                {
                    names.insert(table_key(&fk.referenced_database, &fk.referenced_table));
                }
            }
        }
        for action in actions {
            if let crate::parser::AlterAction::AddForeignKey(fk) = action {
                let (db, table) = split_table_name(&database, &fk.referenced_table);
                names.insert(table_key(&db, &table));
            }
        }
        names.into_iter().collect()
    }

    fn constraint_candidate(
        &self,
        source: &TableSchema,
        actions: &[crate::parser::AlterAction],
    ) -> Result<
        (
            TableSchema,
            Vec<IndexSchema>,
            BTreeSet<String>,
            BTreeSet<String>,
        ),
        QueryError,
    > {
        use crate::parser::AlterAction as A;
        // fk_build_one inspects the catalog's parent columns/indexes. Resolve
        // just those parents before taking its read guard (never hydrate while
        // holding a catalog guard).
        for action in actions {
            if let A::AddForeignKey(fk) = action {
                let (database, table) = split_table_name(&source.database, &fk.referenced_table);
                self.fk_table_descriptor(&table_key(&database, &table))?;
            }
        }
        let catalog = self.catalog_read()?;
        let mut candidate = source.clone();
        let mut indexes = Vec::new();
        let mut checks_to_validate = BTreeSet::new();
        let mut fks_to_validate = BTreeSet::new();
        for action in actions {
            match action {
                A::ModifyColumn(def) | A::ChangeColumn { new_def: def, .. } => {
                    let name = match action {
                        A::ChangeColumn { old_name, .. } => old_name,
                        _ => &def.name,
                    };
                    let position = candidate.column_index(name)?;
                    let mut column = column_def_to_schema(def.clone())?;
                    preserve_primary_key_not_null(&candidate, position, &mut column);
                    apply_column_charset_for_alter(
                        &mut column,
                        &candidate.columns[position],
                        &candidate.charset,
                        &candidate.collation,
                    )?;
                    if !column.name.eq_ignore_ascii_case(name)
                        || !def.checks.is_empty()
                        || column_change_requires_physical_copy(
                            &candidate.columns[position],
                            &column,
                        )
                    {
                        return Err(error(crate::error::catalog::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON, "online column validation requires unchanged physical encoding and column identity"));
                    }
                    self.reject_generated_storedness_change(&candidate, position, &column)?;
                    self.reject_functional_index_type_change(&candidate, position, &column)?;
                    self.reject_foreign_key_column_change(&candidate, position, &column)?;
                    self.reject_vector_index_column_change(&candidate, position, &column)?;
                    apply_column_def_preserving_identity(&mut candidate.columns[position], column);
                }
                A::MoveColumn { column, position } => {
                    move_column_in_candidate(&mut candidate, column, position)?
                }
                A::AddCheck(def) => {
                    let checks = build_check_constraints_for_table(
                        &candidate,
                        vec![def.clone()],
                        Some(&candidate.check_constraints),
                        None,
                    )?;
                    checks_to_validate.extend(checks.iter().map(|check| check.name.clone()));
                    candidate.check_constraints.extend(checks);
                }
                A::AlterCheck { name, enforced } => {
                    let check = candidate
                        .check_constraints
                        .iter_mut()
                        .find(|c| c.name == *name)
                        .ok_or_else(|| {
                            error(
                                crate::error::catalog::ER_CONSTRAINT_NOT_FOUND,
                                format!("Check constraint '{name}' does not exist."),
                            )
                        })?;
                    check.enforced = *enforced;
                    if *enforced {
                        checks_to_validate.insert(check.name.clone());
                    }
                }
                A::DropCheck {
                    name,
                    if_exists,
                    include_foreign_keys,
                } => {
                    if let Some(pos) = candidate
                        .check_constraints
                        .iter()
                        .position(|c| c.name == *name)
                    {
                        candidate.check_constraints.remove(pos);
                    } else if let Some(pos) = candidate
                        .foreign_keys
                        .iter()
                        .position(|fk| *include_foreign_keys && fk.name.eq_ignore_ascii_case(name))
                    {
                        candidate.foreign_keys.remove(pos);
                    } else if !if_exists {
                        return Err(error(
                            crate::error::catalog::ER_CONSTRAINT_NOT_FOUND,
                            format!("Check constraint '{name}' does not exist."),
                        ));
                    }
                }
                A::DropForeignKey { name } => {
                    let pos = candidate
                        .foreign_keys
                        .iter()
                        .position(|fk| fk.name.eq_ignore_ascii_case(name))
                        .ok_or_else(|| {
                            error(
                                crate::error::catalog::ER_CANT_DROP_FIELD_OR_KEY,
                                format!("Can't DROP FOREIGN KEY '{name}'; check that it exists"),
                            )
                        })?;
                    candidate.foreign_keys.remove(pos);
                }
                A::AddForeignKey(def) => {
                    if let Some(name) = &def.name {
                        if candidate
                            .foreign_keys
                            .iter()
                            .any(|fk| fk.name.eq_ignore_ascii_case(name))
                        {
                            return Err(error(
                                crate::error::catalog::ER_FK_DUP_NAME,
                                format!("Duplicate foreign key constraint name '{name}'"),
                            ));
                        }
                    }
                    let mut used = candidate
                        .foreign_keys
                        .iter()
                        .map(|fk| fk.name.clone())
                        .collect();
                    let mut names = candidate
                        .indexes
                        .iter()
                        .chain(indexes.iter())
                        .map(|i| normalize_name(&i.name))
                        .collect();
                    let (fk, index) = fk_build_one(
                        &catalog,
                        &candidate,
                        &indexes,
                        def.clone(),
                        &mut used,
                        &mut 1,
                        &mut names,
                    )?;
                    fks_to_validate.insert(normalize_name(&fk.name));
                    candidate.foreign_keys.push(fk);
                    if let Some(index) = index {
                        indexes.push(index);
                    }
                }
                _ => unreachable!("constraint capability checked"),
            }
        }
        let names = check_constraint_names_in_schema(
            &catalog,
            &source.database,
            Some(&source.qualified_name()),
        );
        ensure_check_constraint_names_available(&names, &candidate.check_constraints)?;
        if actions
            .iter()
            .any(|action| matches!(action, A::ModifyColumn(_) | A::ChangeColumn { .. }))
        {
            candidate.publish_new_row_layout();
            validate_table_column_identifiers(&candidate)?;
            validate_bit_widths(&candidate)?;
            validate_row_fits_page(&candidate)?;
            validate_and_prepare_generated_columns(&mut candidate)?;
            ensure_table_keeps_visible_column(&candidate)?;
        }
        candidate.build_column_index();
        indexes.retain(|index| {
            candidate
                .foreign_keys
                .iter()
                .any(|fk| fk.backing_index == index.name)
        });
        Ok((candidate, indexes, checks_to_validate, fks_to_validate))
    }

    fn constraint_barrier(
        &self,
        names: &[String],
        owner: u64,
        cleanup: bool,
    ) -> Result<(), QueryError> {
        let started = Instant::now();
        let timeout = crate::storage::txn::metadata_wait_timeout();
        loop {
            if !cleanup {
                self.ensure_operational()?;
                crate::engine::cancel::check_cancelled()?;
                for name in names {
                    if let Ok(table) = self.table(name) {
                        if let Some(job) = self.active_alter_copy_job(table.id) {
                            Self::check_alter_copy_cancelled(&job)?;
                        }
                    }
                }
            }
            let requests = names
                .iter()
                .map(|name| {
                    let (db, table) = split_table_name(DEFAULT_DATABASE, name);
                    MdlRequest {
                        key: MdlObjectKey::table(&db, &table),
                        lock_type: MdlLockType::SharedNoWrite,
                    }
                })
                .collect();
            match self.acquire_mdl(
                owner,
                Some(owner),
                MdlDuration::Transaction,
                requests,
                "ddl/online-constraint/barrier",
            ) {
                Ok(()) => return Ok(()),
                Err(err) if is_lock_wait_pending_error(&err) => {
                    self.mdl.clear_pending_for_owner(owner);
                    self.notify_non_row_lock_waiters();
                    if !cleanup && started.elapsed() >= timeout {
                        return Err(error(
                            crate::error::catalog::ER_LOCK_WAIT_TIMEOUT,
                            "online constraint timed out waiting for metadata locks",
                        ));
                    }
                    self.wait_for_ddl_signal(Duration::from_millis(10))?;
                }
                Err(err) => return Err(err),
            }
        }
    }

    fn release_constraint_barrier(
        &self,
        ids: &[u64],
        txn: u64,
        owner: u64,
        allow_dml: bool,
    ) -> Result<(), QueryError> {
        for id in ids {
            self.table_locks_sharded
                .downgrade_exclusive_to_shared_mode(
                    txn,
                    *id,
                    if allow_dml {
                        TableLockMode::IntentShared
                    } else {
                        TableLockMode::Shared
                    },
                )?;
        }
        self.notify_non_row_lock_waiters();
        self.release_mdl_duration(owner, MdlDuration::Transaction);
        Ok(())
    }

    fn replay_constraint_rows(
        &self,
        tables: &[Arc<TableSchema>],
        jobs: &[Arc<AlterCopyJob>],
        validation: &mut OnlineConstraintValidation,
        deadline: Option<Instant>,
    ) -> Result<(), QueryError> {
        // One read watermark for the entire cohort, including both FK sides.
        let read = self.begin_read_only_txn()?;
        let _read = OnlineIndexTxnGuard {
            db: self,
            txn_id: read.txn_id,
        };
        let mut bytes = 0usize;
        for (table, job) in tables.iter().zip(jobs) {
            Self::check_alter_copy_cancelled(job)?;
            for (storage, row_id) in self.take_online_copy_ready_rows(job, read)?.rows {
                CONSTRAINT_REPLAYED_ROWS.fetch_add(1, Ordering::Relaxed);
                check_online_copy_deadline(deadline)?;
                let row = self
                    .read_mvcc_row(storage, row_id)?
                    .map(|r| self.visible_row(r, read))
                    .transpose()?
                    .flatten();
                let row = row.map(|mut r| {
                    Self::pad_full_row_to_schema(&mut r.values, table);
                    r
                });
                if let Some(row) = &row {
                    bytes = bytes.saturating_add(
                        crate::engine::executor::mem_budget::estimate_row_bytes(&row.values),
                    );
                }
                if deadline.is_some() && bytes > 4 * 1024 * 1024 {
                    return Err(error(
                        crate::error::catalog::ER_LOCK_WAIT_TIMEOUT,
                        "online constraint final tail exceeded 4 MiB; retry the ALTER",
                    ));
                }
                let previous = if validation.fks.is_empty() {
                    None
                } else {
                    self.read_mvcc_row(storage, row_id)?
                        .map(|row| {
                            self.visible_row(
                                row,
                                validation.watermark.expect("scan watermark installed"),
                            )
                        })
                        .transpose()?
                        .flatten()
                        .map(|mut row| {
                            Self::pad_full_row_to_schema(&mut row.values, table);
                            row
                        })
                };
                validation.replace(
                    self,
                    table.id,
                    (storage, row_id),
                    previous.as_ref().map(|r| r.values.as_slice()),
                    row.as_ref().map(|r| r.values.as_slice()),
                    false,
                )?;
            }
            Self::set_alter_copy_job_replay_watermark(job, read.snapshot_scn);
        }
        validation.watermark = Some(read);
        Ok(())
    }

    pub(crate) fn alter_constraints_online(
        self: &Arc<Self>,
        table_name: &str,
        actions: &[crate::parser::AlterAction],
        owner: u64,
        lock: crate::parser::AlterLock,
    ) -> Result<(), QueryError> {
        #[cfg(test)]
        CONSTRAINT_LAST_INDEX_MERGE.with(|used| used.set(false));
        #[cfg(test)]
        CONSTRAINT_LAST_MERGE_WORKERS.with(|n| n.set(0));
        let source = self.table(table_name)?;
        let publishes_columns = actions.iter().any(|action| {
            matches!(
                action,
                crate::parser::AlterAction::ModifyColumn(_)
                    | crate::parser::AlterAction::ChangeColumn { .. }
            )
        });
        if table_ddl_schema_is_read_only(&source.database) {
            return Err(error(
                crate::error::catalog::ER_DBACCESS_DENIED_ERROR,
                format!("access denied for database '{}'", source.database),
            ));
        }
        let (mut candidate, indexes, checks_to_validate, fks_to_validate) =
            self.constraint_candidate(&source, actions)?;
        let names = self.constraint_alter_dependencies(table_name, actions);
        let _mdl = OnlineIndexMdlGuard { db: self, owner };
        self.constraint_barrier(&names, owner, false)?;
        let mut hold = Some(ConstraintHoldTimer(Instant::now()));
        let mut tables = names
            .iter()
            .filter_map(|name| self.table(name).ok())
            .collect::<Vec<_>>();
        tables.sort_unstable_by_key(|t| t.id);
        tables.dedup_by_key(|t| t.id);
        let mut ids = tables
            .iter()
            .flat_map(|t| t.storage_ids().into_iter().chain([t.id]))
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids.dedup();
        let lock_txn = self.begin_read_only_txn()?;
        let _lock = OnlineIndexTxnGuard {
            db: self,
            txn_id: lock_txn.txn_id,
        };
        self.try_online_index_data_barrier_mode(lock_txn.txn_id, &ids, true)?;
        let actual = self.table(table_name)?;
        if actual.schema_version != source.schema_version || actual.id != source.id {
            return Err(error(
                crate::error::catalog::ER_TABLE_DEF_CHANGED,
                "constraint source changed during preparation",
            ));
        }
        let mut descriptors = Vec::new();
        for table in &tables {
            self.pin_online_index_descriptor(table)?;
            descriptors.push(OnlineIndexDescriptorGuard {
                db: self,
                table_id: table.id,
            });
        }
        let mut targets = Vec::new();
        let mut index_names = Vec::new();
        let mut jobs = Vec::new();
        // Keep potentially table-sized validation allocations outside the
        // barrier scope: freeing them must not extend publication's lock hold.
        let mut validation_state = None;
        let mut publication_started = false;
        let result = (|| {
            for index in indexes {
                let (_, table, name) =
                    self.prepare_index_build(table_name, index, Some(lock_txn.txn_id), true, true)?;
                index_names.push(name.clone());
                let index = table
                    .indexes
                    .iter()
                    .find(|i| i.name == name)
                    .unwrap()
                    .clone();
                let capture = self.index_handle(table.id, &name)?;
                let log = Arc::new(OnlineIndexBuildLog::new());
                {
                    let mut handle = capture.write();
                    handle.flush()?;
                    handle.online_build_log = Some(log.clone());
                    handle.retain_buffered_pages_on_drop();
                }
                targets.push(OnlineIndexTarget {
                    table,
                    index,
                    log,
                    _capture: capture,
                    file: None,
                    validation_initialized: false,
                    null_key_groups: 0,
                    duplicate_key_groups: 0,
                });
            }
            let prepared = self.table(table_name)?;
            for fk in &mut candidate.foreign_keys {
                fk.backing_index_id = prepared
                    .indexes
                    .iter()
                    .find(|i| i.name.eq_ignore_ascii_case(&fk.backing_index))
                    .ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_CANNOT_ADD_FOREIGN,
                            "missing FK backing index",
                        )
                    })?
                    .index_id;
            }
            for table in &mut tables {
                if table.id == source.id {
                    *table = prepared.clone();
                }
                for id in table.storage_ids().into_iter().chain([table.id]) {
                    self.table_locks_sharded
                        .set_schema_floor(id, table.schema_version);
                }
            }
            let checks = candidate
                .check_constraints
                .iter()
                .filter(|check| check.enforced && checks_to_validate.contains(&check.name))
                .cloned()
                .map(|check| OnlineCheckValidation {
                    check,
                    invalid: BTreeSet::new(),
                })
                .collect::<Vec<_>>();
            let mut fks = Vec::new();
            let mut validation_memory = ConstraintValidationMemory::new()?;
            if fk_checks_enabled() {
                for fk in &candidate.foreign_keys {
                    if !fks_to_validate.contains(&normalize_name(&fk.name)) {
                        continue;
                    }
                    let parent = tables
                        .iter()
                        .find(|t| {
                            t.database == fk.referenced_database && t.name == fk.referenced_table
                        })
                        .cloned()
                        .ok_or_else(|| {
                            error(
                                crate::error::catalog::ER_FK_CANNOT_OPEN_PARENT,
                                "FK parent disappeared",
                            )
                        })?;
                    let index = fk_find_referenced_index(&parent, &fk.referenced_columns)
                        .cloned()
                        .ok_or_else(|| {
                            error(
                                crate::error::catalog::ER_FK_NO_INDEX_PARENT,
                                "FK parent index disappeared",
                            )
                        })?;
                    let parent_columns = fk
                        .referenced_columns
                        .iter()
                        .map(|c| parent.column_index(c))
                        .collect::<Result<_, _>>()?;
                    // All constraints need progress scratch before any map can
                    // consume the shared budget during the initial scan.
                    validation_memory.grow(16 * 1024)?;
                    fks.push(OnlineFkValidation {
                        fk: fk.clone(),
                        parent,
                        index,
                        parent_columns,
                        counts: crate::util::fast_hash::ah_hash_map(),
                        bucket_bytes: 0,
                        payload_bytes: 0,
                        scratch_bytes: 16 * 1024,
                        data_dir: self.data_dir().to_path_buf(),
                        spill: None,
                        baseline: None,
                        orphan_groups: 0,
                    });
                }
            }
            validation_state = Some(OnlineConstraintValidation {
                child: Arc::new(candidate.clone()),
                checks,
                fks,
                columns: source
                    .columns
                    .iter()
                    .zip(&candidate.columns)
                    .enumerate()
                    .filter_map(|(position, (old, new))| {
                        column_change_requires_validation(old, new).then_some(position)
                    })
                    .collect(),
                column_errors: BTreeMap::new(),
                watermark: None,
                memory: validation_memory,
            });
            let validation = validation_state.as_mut().unwrap();
            let needs_scan = !validation.checks.is_empty()
                || !validation.fks.is_empty()
                || !validation.columns.is_empty();
            let validation_tables = tables
                .iter()
                .filter(|table| validation.uses_table(table.id))
                .cloned()
                .collect::<Vec<_>>();
            if needs_scan {
                for table in &validation_tables {
                    if self.active_alter_copy_job(table.id).is_some() {
                        return Err(error(
                            crate::error::catalog::ER_LOCK_WAIT_TIMEOUT,
                            "another online DDL is active",
                        ));
                    }
                    let operation = if validation.columns.is_empty() {
                        "CONSTRAINT"
                    } else {
                        "VALIDATE_COLUMN"
                    };
                    let job = self.register_ddl_job(table, operation, true);
                    Self::set_alter_copy_job_state(&job, "VALIDATING");
                    jobs.push(job);
                }
            }
            // Capture is installed with writers drained; the snapshot pins undo
            // and physical row identities until the final replay completes.
            // Raw index walks are only candidates. Remember repair state while
            // writers are drained; certification below also drains writers and
            // verifies the *unconsumed* transaction-qualified capture logs.
            let merge_allowed = validation.columns.is_empty()
                && !self.constraint_merge_has_repairs(validation, &targets);
            let snapshot = self.begin_read_only_txn()?;
            let _snapshot_pin = self.register_statement_snapshot(snapshot.snapshot_scn);
            validation.watermark = Some(snapshot);
            let _snapshot = OnlineIndexTxnGuard {
                db: self,
                txn_id: snapshot.txn_id,
            };
            if needs_scan || !targets.is_empty() {
                self.release_constraint_barrier(
                    &ids,
                    lock_txn.txn_id,
                    owner,
                    matches!(
                        lock,
                        crate::parser::AlterLock::None | crate::parser::AlterLock::Default
                    ),
                )?;
                drop(hold.take());
                #[cfg(test)]
                self.pause_ddl_backfill_for_test();
                if !targets.is_empty() {
                    self.build_online_index_targets(&mut targets, IndexBuildOptions::default())?;
                }
                let merge_result = if merge_allowed && needs_scan {
                    self.try_constraint_index_merge(validation, &targets, &jobs)?
                } else {
                    None
                };
                // Keep the interleaving hook between candidate scanning and
                // certification, including on the fallback path.
                #[cfg(test)]
                if let Some(hook) = self
                    .online_index_validation_test_hook
                    .lock()
                    .unwrap()
                    .clone()
                {
                    hook.pause();
                }
                let mut merge_certified = false;
                if let Some(orphan_fk) = merge_result {
                    self.constraint_barrier(&names, owner, false)?;
                    hold = Some(ConstraintHoldTimer(Instant::now()));
                    self.try_online_index_data_barrier_mode(lock_txn.txn_id, &ids, true)?;
                    if constraint_capture_is_empty(&jobs)?
                        && !self.constraint_merge_has_repairs(validation, &targets)
                        && targets.iter().all(|target| {
                            let log = target.log.state.lock();
                            !log.failed && log.entries.is_empty()
                        })
                    {
                        // No dependency has changed since the initial barrier.
                        // Both success and violation now describe the original
                        // snapshot, even for a private Building index.
                        merge_certified = true;
                        CONSTRAINT_INDEX_MERGE_ACCEPTED.fetch_add(1, Ordering::Relaxed);
                        #[cfg(test)]
                        CONSTRAINT_LAST_INDEX_MERGE.with(|used| used.set(true));
                        if let Some(position) = orphan_fk {
                            return Err(fk_child_violation_err(
                                &validation.child,
                                &validation.fks[position].fk,
                            ));
                        }
                    } else {
                        CONSTRAINT_INDEX_MERGE_FALLBACKS.fetch_add(1, Ordering::Relaxed);
                        self.release_constraint_barrier(
                            &ids,
                            lock_txn.txn_id,
                            owner,
                            matches!(
                                lock,
                                crate::parser::AlterLock::None | crate::parser::AlterLock::Default
                            ),
                        )?;
                        drop(hold.take());
                    }
                }
                if !merge_certified {
                    self.build_constraint_fk_baselines(validation, snapshot, &jobs)?;
                    if needs_scan {
                        for (table, job) in validation_tables.iter().zip(&jobs) {
                            let needs_heap = (table.id == validation.child.id
                                && (!validation.checks.is_empty()
                                    || !validation.columns.is_empty()))
                                || validation.fks.iter().any(|state| {
                                    state.baseline.is_none()
                                        && (table.id == validation.child.id
                                            || table.id == state.parent.id)
                                });
                            if !needs_heap {
                                Self::set_alter_copy_job_state(job, "CATCH_UP");
                                continue;
                            }
                            for storage in self.scan_storage_ids(table) {
                                let pages = self.heap_page_count(storage);
                                for page in (0..pages).step_by(32) {
                                    Self::check_alter_copy_cancelled(job)?;
                                    crate::engine::cancel::check_cancelled()?;
                                    if !validation.columns.is_empty()
                                        && validation.checks.is_empty()
                                        && validation.fks.is_empty()
                                    {
                                        let columns = validation.columns.clone();
                                        let mut count = 0;
                                        self.scan_rows_columns_storage_streaming_range_with_ids(
                                            table,
                                            storage,
                                            snapshot,
                                            &columns,
                                            page,
                                            (page + 32).min(pages),
                                            |row_id, values| {
                                                validation.replace_column_values(
                                                    (storage, row_id),
                                                    Some(values),
                                                    true,
                                                )?;
                                                count += 1;
                                                Ok(())
                                            },
                                        )?;
                                        Self::advance_alter_copy_job_copied_by(job, count);
                                        CONSTRAINT_SCANNED_ROWS.fetch_add(count, Ordering::Relaxed);
                                        continue;
                                    }
                                    let rows = self.scan_rows_storage_range(
                                        table,
                                        storage,
                                        snapshot,
                                        page,
                                        (page + 32).min(pages),
                                    )?;
                                    let count = rows.len() as u64;
                                    for row in rows {
                                        validation.replace(
                                            self,
                                            table.id,
                                            (storage, row.row_id),
                                            None,
                                            Some(&row.values),
                                            true,
                                        )?;
                                    }
                                    Self::advance_alter_copy_job_copied_by(job, count);
                                    CONSTRAINT_SCANNED_ROWS.fetch_add(count, Ordering::Relaxed);
                                }
                            }
                            Self::set_alter_copy_job_state(job, "CATCH_UP");
                        }
                    }
                    let mut converged = false;
                    for _ in 0..64 {
                        crate::engine::cancel::check_cancelled()?;
                        self.replay_constraint_rows(&validation_tables, &jobs, validation, None)?;
                        self.replay_online_index_targets(&mut targets, false)?;
                        for target in &targets {
                            target.file.as_ref().unwrap().flush()?;
                        }
                        self.constraint_barrier(&names, owner, false)?;
                        hold = Some(ConstraintHoldTimer(Instant::now()));
                        self.try_online_index_data_barrier_mode(lock_txn.txn_id, &ids, true)?;
                        let tail: usize = jobs
                            .iter()
                            .map(|j| j.changes.lock().unwrap_or_else(|p| p.into_inner()).len())
                            .sum();
                        let index_tail: usize = targets
                            .iter()
                            .map(|t| t.log.state.lock().entries.len())
                            .sum();
                        let index_bytes: usize = targets
                            .iter()
                            .map(|t| t.log.state.lock().charge.bytes())
                            .sum();
                        if tail <= 1024 && index_tail <= 4096 && index_bytes <= 1024 * 1024 {
                            CONSTRAINT_FINAL_TAIL_MAX.fetch_max(tail as u64, Ordering::Relaxed);
                            converged = true;
                            break;
                        }
                        self.release_constraint_barrier(
                            &ids,
                            lock_txn.txn_id,
                            owner,
                            matches!(
                                lock,
                                crate::parser::AlterLock::None | crate::parser::AlterLock::Default
                            ),
                        )?;
                        drop(hold.take());
                    }
                    if !converged {
                        return Err(error(
                            crate::error::catalog::ER_LOCK_WAIT_TIMEOUT,
                            "online constraint could not converge within its final-tail budget",
                        ));
                    }
                    self.replay_constraint_rows(
                        &validation_tables,
                        &jobs,
                        validation,
                        Some(online_copy_deadline()),
                    )?;
                    if jobs
                        .iter()
                        .any(|j| j.changes.lock().unwrap_or_else(|p| p.into_inner()).len() != 0)
                    {
                        return Err(error(
                            crate::error::catalog::ER_LOCK_WAIT_TIMEOUT,
                            "online constraint has unresolved transactions",
                        ));
                    }
                    self.replay_online_index_targets(&mut targets, false)?;
                }
            }
            validation.finish()?;
            for job in &jobs {
                let mut progress = job.progress.lock().unwrap_or_else(|p| p.into_inner());
                Self::check_alter_copy_cancelled(job)?;
                progress.state = "FINALIZING".to_string();
            }
            crate::engine::cancel::check_cancelled()?;
            for table in &tables {
                let current = self.table(&table.qualified_name())?;
                if current.id != table.id
                    || current.schema_version != table.schema_version
                    || current.active_generation != table.active_generation
                {
                    return Err(error(
                        crate::error::catalog::ER_TABLE_DEF_CHANGED,
                        format!(
                            "constraint dependency changed for '{}': id {} -> {}, schema version {} -> {}, generation {} -> {}",
                            table.qualified_name(),
                            table.id, current.id,
                            table.schema_version, current.schema_version,
                            table.active_generation, current.active_generation,
                        ),
                    ));
                }
            }
            let mut inbound_updates = BTreeMap::<u64, Vec<InboundFk>>::new();
            for parent in &tables {
                let mut edges = Vec::new();
                for fk in &candidate.foreign_keys {
                    if table_key(&fk.referenced_database, &fk.referenced_table)
                        != parent.qualified_name()
                    {
                        continue;
                    }
                    edges.push(InboundFk {
                        child_table_id: source.id,
                        child_table_key: source.qualified_name(),
                        fk_name: fk.name.clone(),
                        on_delete: fk.on_delete,
                        on_update: fk.on_update,
                        referenced_col_idx: fk
                            .referenced_columns
                            .iter()
                            .map(|name| parent.column_index(name))
                            .collect::<Result<_, _>>()?,
                        child_col_idx: fk.child_col_idx.clone(),
                        child_backing_index: fk.backing_index.clone(),
                        child_backing_index_id: fk.backing_index_id,
                    });
                }
                inbound_updates.insert(parent.id, edges);
            }
            for target in &mut targets {
                target.file.as_ref().unwrap().flush()?;
            }
            // One catalog transaction publishes the constraints AND their trees.
            // Building intents survive a crash and are discarded by index recovery.
            {
                let mut catalog = self.catalog_write()?;
                let schema_names = check_constraint_names_in_schema(
                    &catalog,
                    &source.database,
                    Some(&source.qualified_name()),
                );
                ensure_check_constraint_names_available(
                    &schema_names,
                    &candidate.check_constraints,
                )?;
                publication_started = true;
                for target in &mut targets {
                    self.index_cache.write().insert(
                        (target.table.id, normalize_name(&target.index.name)),
                        Arc::new(parking_lot::RwLock::new(target.file.take().unwrap())),
                    );
                }
                let table =
                    Arc::make_mut(catalog.tables.get_mut(&source.qualified_name()).unwrap());
                if publishes_columns {
                    let boundary = self.next_scn_atomic.load(Ordering::Acquire);
                    for (old, new) in source.columns.iter().zip(&mut candidate.columns) {
                        if old.nullable && !new.nullable {
                            new.not_null_from_scn = boundary;
                            self.read_definition_highwater
                                .fetch_max(boundary, Ordering::Release);
                        }
                    }
                    table.columns = candidate.columns;
                    table.row_layouts = candidate.row_layouts;
                    table.active_layout_id = candidate.active_layout_id;
                }
                table.check_constraints = candidate.check_constraints;
                table.foreign_keys = candidate.foreign_keys;
                for index in &mut table.indexes {
                    if index_names.contains(&index.name) {
                        index.state = IndexState::Public;
                        index.readable_from_scn = self.next_scn_atomic.load(Ordering::Acquire);
                        self.read_definition_highwater
                            .fetch_max(index.readable_from_scn, Ordering::Release);
                    }
                }
                table.build_column_index();
                table.bump_schema_version();
                for dependency in &tables {
                    if publishes_columns && dependency.id != source.id {
                        continue;
                    }
                    let table = Arc::make_mut(
                        catalog
                            .tables
                            .get_mut(&dependency.qualified_name())
                            .unwrap(),
                    );
                    // Rebuilding the whole live catalog would erase inbound
                    // edges belonging to cold child headers. Replace only this
                    // child's edges and preserve every other relationship.
                    table
                        .fk_referenced_by
                        .retain(|edge| edge.child_table_id != source.id);
                    table
                        .fk_referenced_by
                        .extend(inbound_updates.remove(&table.id).unwrap_or_default());
                    if table.id != source.id {
                        table.bump_schema_version();
                    }
                    for id in table.storage_ids().into_iter().chain([table.id]) {
                        self.table_locks_sharded
                            .set_schema_floor(id, table.schema_version);
                    }
                    self.note_catalog_table(table.id);
                }
            }
            for table in &tables {
                self.sync_online_index_descriptor(table.id)?;
            }
            if publishes_columns {
                for id in source.storage_ids().into_iter().chain([source.id]) {
                    self.refresh_heap_codec(id)?;
                }
            }
            self.commit_ddl_catalog_txn()?;
            Ok(())
        })();
        for job in &jobs {
            self.unregister_alter_copy_job(job.table_id, result.as_ref().err());
        }
        if result.is_err() && !publication_started {
            // prepare_index_build may have persisted its intent before failing.
            if let Ok(current) = self.table(table_name) {
                for index in &current.indexes {
                    if index.state == IndexState::Building
                        && !source
                            .indexes
                            .iter()
                            .any(|old| old.index_id == index.index_id)
                        && !index_names.contains(&index.name)
                    {
                        index_names.push(index.name.clone());
                    }
                }
            }
            if !index_names.is_empty() {
                let _cancel =
                    crate::engine::cancel::enter(crate::engine::cancel::QueryCancelToken::noop());
                let _deadline = crate::engine::cancel::install_statement_deadline(Duration::ZERO);
                let cleanup =
                    crate::engine::executor::mem_budget::with_critical_memory_reservations(|| {
                        self.constraint_barrier(&names, owner, true)?;
                        self.try_online_index_data_barrier_mode(lock_txn.txn_id, &ids, true)?;
                        for target in &mut targets {
                            target.file.take();
                        }
                        self.discard_online_index_candidates(table_name, &index_names)
                    });
                if let Err(err) = cleanup {
                    self.quarantine_mvcc(format!("constraint cleanup failed: {}", err.message()));
                    return Err(err);
                }
            }
        }
        if publication_started {
            if let Err(err) = &result {
                self.quarantine_mvcc(format!(
                    "constraint publication failed; recovery required: {}",
                    err.message()
                ));
            }
        }
        drop(_lock);
        drop(_mdl);
        drop(hold.take());
        drop(validation_state);
        result
    }
}

// A rebuildable, collision-checked disk hash table. Only the bucket heads and
// individual records are read; neither cardinality nor replay history pins RAM.
// Counts stay mutable so the final publication barrier never sorts/scans a
// whole spill file. Hashing and equality are the same as the in-memory map,
// including collation and DECIMAL semantics; serialized bytes are not compared.
struct OnlineFkDiskCounts {
    file: Arc<crate::engine::executor::spill_quota::LeasedSpillFile>,
    tmp: crate::engine::executor::select::grace::SpillTmpDir,
    end: u64,
    charged_end: u64,
    bucket_offset: u64,
    buckets: u64,
    entries: u64,
    rehash: Option<ConstraintRehash>,
    hasher: ahash::RandomState,
    buffer: Vec<u8>,
    cache: std::rc::Rc<RefCell<ConstraintSpillCache>>,
}

struct ConstraintRehash {
    offset: u64,
    next_bucket: u64,
}

// Direct-mapped, write-back pages for a disposable spill file. Eviction is
// the only required writeback: no reader outlives this owner, and recovery
// never reads this file. Cache tags include the full file page offset.
struct ConstraintSpillPage {
    offset: u64,
    dirty: bool,
    bytes: [u8; 4096],
}

struct ConstraintSpillCache {
    file: Arc<crate::engine::executor::spill_quota::LeasedSpillFile>,
    charger: crate::engine::executor::spill_quota::SpillCharger,
    pages: Vec<ConstraintSpillPage>,
}

impl ConstraintSpillCache {
    fn flush_and_invalidate(&mut self) -> Result<(), QueryError> {
        #[cfg(unix)]
        use std::os::unix::fs::FileExt;
        for (position, page) in self.pages.iter_mut().enumerate() {
            if position % 256 == 0 {
                crate::engine::cancel::check_cancelled()?;
            }
            if page.dirty {
                let reservation = self
                    .charger
                    .reserve_physical_write(page.bytes.len() as u64)?;
                self.file
                    .write_all_at(&page.bytes, page.offset)
                    .map_err(io_err)?;
                drop(reservation);
                record_spill_bytes_written(4096);
                page.dirty = false;
            }
            page.offset = u64::MAX;
        }
        Ok(())
    }

    fn release(&mut self) -> Result<usize, QueryError> {
        self.flush_and_invalidate()?;
        let bytes = self.pages.capacity() * std::mem::size_of::<ConstraintSpillPage>();
        self.pages = Vec::new();
        Ok(bytes)
    }
}

impl OnlineFkDiskCounts {
    const BUCKETS: u64 = 1 << 18;
    const HEADER: usize = 40;

    fn new(
        data_dir: &Path,
        hasher: ahash::RandomState,
        scratch: usize,
    ) -> Result<Self, QueryError> {
        let tmp = crate::engine::executor::select::grace::SpillTmpDir::create_kind(
            data_dir,
            "constraint",
        )?;
        let path = tmp.path().join("counts");
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(io_err)?;
        // No WAL/recovery state: cancellation and process death close the
        // unlinked file. The directory owner marker is cleaned normally.
        fs::remove_file(path).map_err(io_err)?;
        let end = Self::BUCKETS * 8;
        let reservation = tmp.charger().reserve_write(end, end)?;
        file.set_len(end).map_err(io_err)?;
        drop(reservation);
        let file = Arc::new(tmp.charger().lease_file(file)?);
        let cache_file = file.clone();
        let cache_charger = tmp.charger();
        Ok(Self {
            file,
            tmp,
            end,
            charged_end: end,
            bucket_offset: 0,
            buckets: Self::BUCKETS,
            entries: 0,
            rehash: None,
            hasher,
            buffer: Vec::with_capacity(scratch / 4),
            cache: std::rc::Rc::new(RefCell::new(ConstraintSpillCache {
                file: cache_file,
                charger: cache_charger,
                pages: Vec::new(),
            })),
        })
    }

    fn hash(&self, key: &IndexKey) -> u64 {
        std::hash::BuildHasher::hash_one(&self.hasher, key)
    }

    fn enable_migration_cache(&mut self, memory: &mut ConstraintValidationMemory) {
        // Map growth can be refused while smaller allocations still fit.
        // Buffer bucket heads and appends during the initial drain as well:
        // without this cache every key needs a read and three tiny writes.
        // Admission is optional and includes the still-live map reservation.
        let mut budget = memory
            .limit_bytes
            .saturating_sub(memory.charge.bytes())
            .min(4 * 1024 * 1024);
        while budget != 0 && !memory.charge.try_grow_optional(budget) {
            budget /= 2;
        }
        let cached = self.enable_cache(budget);
        memory.shrink(budget - cached);
    }

    fn enable_cache(&mut self, budget: usize) -> usize {
        let pages = budget / std::mem::size_of::<ConstraintSpillPage>();
        let mut state = self.cache.borrow_mut();
        let cache = &mut state.pages;
        debug_assert!(cache.is_empty());
        if cache.try_reserve_exact(pages).is_err() {
            return 0;
        }
        cache.resize_with(pages, || ConstraintSpillPage {
            offset: u64::MAX,
            dirty: false,
            bytes: [0; 4096],
        });
        cache.capacity() * std::mem::size_of::<ConstraintSpillPage>()
    }

    fn cached_page<'a>(
        &self,
        cache: &'a mut [ConstraintSpillPage],
        offset: u64,
    ) -> Result<&'a mut ConstraintSpillPage, QueryError> {
        #[cfg(unix)]
        use std::os::unix::fs::FileExt;
        let page_offset = offset / 4096 * 4096;
        let slot = (offset / 4096 % cache.len() as u64) as usize;
        let page = &mut cache[slot];
        if page.offset != page_offset {
            if page.dirty {
                let reservation = self
                    .tmp
                    .charger()
                    .reserve_physical_write(page.bytes.len() as u64)?;
                self.file
                    .write_all_at(&page.bytes, page.offset)
                    .map_err(io_err)?;
                drop(reservation);
                record_spill_bytes_written(4096);
                page.dirty = false;
            }
            // Appends can address an unwritten page or the zero-filled tail
            // of a partial page. Interrupted reads must not create false EOF.
            page.offset = u64::MAX;
            page.bytes.fill(0);
            let mut read = 0;
            while read < page.bytes.len() {
                match self
                    .file
                    .read_at(&mut page.bytes[read..], page_offset + read as u64)
                {
                    Ok(0) => break,
                    Ok(n) => read += n,
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) => return Err(io_err(e)),
                }
            }
            record_spill_bytes_read(read as u64);
            page.offset = page_offset;
        }
        Ok(page)
    }

    fn read(&self, mut bytes: &mut [u8], mut offset: u64) -> Result<(), QueryError> {
        #[cfg(unix)]
        use std::os::unix::fs::FileExt;
        if offset
            .checked_add(bytes.len() as u64)
            .is_none_or(|end| end > self.end)
        {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "FK spill read exceeds file extent",
            ));
        }
        let mut state = self.cache.borrow_mut();
        let cache = &mut state.pages;
        if cache.is_empty() {
            self.file.read_exact_at(bytes, offset).map_err(io_err)?;
            record_spill_bytes_read(bytes.len() as u64);
            return Ok(());
        }
        while !bytes.is_empty() {
            let page = self.cached_page(cache, offset)?;
            let start = (offset % 4096) as usize;
            let n = bytes.len().min(4096 - start);
            bytes[..n].copy_from_slice(&page.bytes[start..start + n]);
            bytes = &mut bytes[n..];
            offset += n as u64;
        }
        Ok(())
    }

    fn write(&self, mut bytes: &[u8], mut offset: u64) -> Result<(), QueryError> {
        #[cfg(unix)]
        use std::os::unix::fs::FileExt;
        let mut state = self.cache.borrow_mut();
        let cache = &mut state.pages;
        if cache.is_empty() {
            let reservation = self
                .tmp
                .charger()
                .reserve_physical_write(bytes.len() as u64)?;
            self.file.write_all_at(bytes, offset).map_err(io_err)?;
            drop(reservation);
            record_spill_bytes_written(bytes.len() as u64);
            return Ok(());
        }
        while !bytes.is_empty() {
            let page = self.cached_page(cache, offset)?;
            let start = (offset % 4096) as usize;
            let n = bytes.len().min(4096 - start);
            page.bytes[start..start + n].copy_from_slice(&bytes[..n]);
            page.dirty = true;
            bytes = &bytes[n..];
            offset += n as u64;
        }
        Ok(())
    }

    fn find(&mut self, key: &IndexKey, hash: u64) -> Result<Option<(u64, (u64, u64))>, QueryError> {
        let mut head = [0; 8];
        self.read(&mut head, self.bucket_for_hash(hash))?;
        let mut pos = u64::from_le_bytes(head);
        while pos != 0 {
            let mut header = [0; Self::HEADER];
            self.read(&mut header, pos)?;
            let number =
                |offset| u64::from_le_bytes(header[offset..offset + 8].try_into().unwrap());
            if number(8) == hash {
                let len = usize::try_from(number(32)).map_err(|_| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "invalid FK spill key length",
                    )
                })?;
                if len > self.buffer.capacity() {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "FK spill key exceeds reserved scratch",
                    ));
                }
                self.buffer.resize(len, 0);
                let mut buffer = std::mem::take(&mut self.buffer);
                let result = self.read(&mut buffer, pos + Self::HEADER as u64);
                self.buffer = buffer;
                result?;
                let mut offset = 0;
                let stored = decode_index_key_from(&self.buffer, &mut offset)?;
                if stored == *key {
                    return Ok(Some((pos, (number(16), number(24)))));
                }
            }
            pos = number(0);
        }
        Ok(None)
    }

    // Grow the on-disk directory geometrically, without increasing the RAM
    // cache. A fixed bucket count makes N distinct keys require O(N²/B)
    // chain reads. Stored hashes suffice to rebuild links; keys and counts
    // remain in place, preserving collation-aware equality and replay state.
    fn grow_directory(&mut self) -> Result<(), QueryError> {
        let buckets = self.buckets.checked_mul(2).ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "FK spill bucket overflow",
            )
        })?;
        let offset = self.end;
        let end = buckets
            .checked_mul(8)
            .and_then(|n| offset.checked_add(n))
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "FK spill file offset overflow",
                )
            })?;
        self.reserve_extent(end)?;
        // Cached appends may have written a full page beyond the logical EOF.
        // Truncate that padding before extending a zero-filled directory, and
        // invalidate cached tail pages so they cannot hide the new zeroes.
        self.cache.borrow_mut().flush_and_invalidate()?;
        let reservation = self.tmp.charger().reserve_physical_write(end - offset)?;
        self.file.set_len(offset).map_err(io_err)?;
        self.file.set_len(end).map_err(io_err)?;
        drop(reservation);
        self.end = end;
        self.rehash = Some(ConstraintRehash {
            offset: self.bucket_offset,
            next_bucket: 0,
        });
        self.bucket_offset = offset;
        self.buckets = buckets;
        Ok(())
    }

    fn bucket_for_hash(&self, hash: u64) -> u64 {
        if let Some(rehash) = &self.rehash {
            let old_bucket = hash % (self.buckets / 2);
            if old_bucket >= rehash.next_bucket {
                return rehash.offset + old_bucket * 8;
            }
        }
        self.bucket_offset + (hash % self.buckets) * 8
    }

    // Split one old chain per insertion. This finishes before the next
    // doubling is needed, without a table-sized rehash in final DDL replay.
    // Since bucket counts are powers of two, an old chain has just two
    // destinations; keep their heads on the stack rather than probing the
    // new directory separately for every record.
    fn migrate_bucket(&mut self) -> Result<(), QueryError> {
        let Some(rehash) = &self.rehash else {
            return Ok(());
        };
        let bucket = rehash.next_bucket;
        let old_buckets = self.buckets / 2;
        let mut head = [0; 8];
        self.read(&mut head, rehash.offset + bucket * 8)?;
        let mut pos = u64::from_le_bytes(head);
        let mut heads = [0u64; 2];
        while pos != 0 {
            crate::engine::cancel::check_cancelled()?;
            let mut links = [0; 16];
            self.read(&mut links, pos)?;
            let next = u64::from_le_bytes(links[..8].try_into().unwrap());
            let hash = u64::from_le_bytes(links[8..].try_into().unwrap());
            let side = usize::from(hash & old_buckets != 0);
            self.write(&heads[side].to_le_bytes(), pos)?;
            heads[side] = pos;
            pos = next;
        }
        self.write(&heads[0].to_le_bytes(), self.bucket_offset + bucket * 8)?;
        self.write(
            &heads[1].to_le_bytes(),
            self.bucket_offset + (bucket + old_buckets) * 8,
        )?;
        if bucket + 1 == old_buckets {
            self.rehash = None;
        } else {
            self.rehash.as_mut().unwrap().next_bucket += 1;
        }
        Ok(())
    }

    fn reserve_extent(&mut self, required: u64) -> Result<(), QueryError> {
        if required > self.charged_end {
            // Amortize quota/free-space probes over bounded extents.
            let growth = (required - self.charged_end).max(64 * 1024).div_ceil(4096) * 4096;
            self.tmp.charger().charge_buffered_extent(growth)?;
            self.charged_end += growth;
        }
        Ok(())
    }

    fn insert(&mut self, key: &IndexKey, hash: u64, counts: (u64, u64)) -> Result<(), QueryError> {
        if self.rehash.is_none() && self.entries >= self.buckets * 2 {
            self.grow_directory()?;
        }
        self.migrate_bucket()?;
        let bucket = self.bucket_for_hash(hash);
        let mut head = [0; 8];
        self.read(&mut head, bucket)?;
        self.buffer.clear();
        encode_index_key_into(key, &mut self.buffer)?;
        let mut header = [0; Self::HEADER];
        header[..8].copy_from_slice(&head);
        header[8..16].copy_from_slice(&hash.to_le_bytes());
        header[16..24].copy_from_slice(&counts.0.to_le_bytes());
        header[24..32].copy_from_slice(&counts.1.to_le_bytes());
        header[32..].copy_from_slice(&(self.buffer.len() as u64).to_le_bytes());
        let bytes = Self::HEADER as u64 + self.buffer.len() as u64;
        let required = self.end.checked_add(bytes).ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "FK spill file offset overflow",
            )
        })?;
        self.reserve_extent(required)?;
        self.write(&header, self.end)?;
        self.write(&self.buffer, self.end + Self::HEADER as u64)?;
        self.write(&self.end.to_le_bytes(), bucket)?;
        self.end += bytes;
        self.entries += 1;
        Ok(())
    }

    fn update(
        &mut self,
        key: &IndexKey,
        parent: bool,
        add: bool,
    ) -> Result<((u64, u64), (u64, u64)), QueryError> {
        let hash = self.hash(key);
        let found = self.find(key, hash)?;
        let old = found.map_or((0, 0), |(_, counts)| counts);
        let new = adjusted_fk_counts(old, parent, add)?;
        if let Some((pos, _)) = found {
            let mut bytes = [0; 16];
            bytes[..8].copy_from_slice(&new.0.to_le_bytes());
            bytes[8..].copy_from_slice(&new.1.to_le_bytes());
            self.write(&bytes, pos + 16)?;
        } else {
            self.insert(key, hash, new)?;
        }
        Ok((old, new))
    }
}

fn adjusted_fk_counts(
    mut counts: (u64, u64),
    parent: bool,
    add: bool,
) -> Result<(u64, u64), QueryError> {
    let count = if parent { &mut counts.1 } else { &mut counts.0 };
    *count = if add {
        count.checked_add(1)
    } else {
        count.checked_sub(1)
    }
    .ok_or_else(|| {
        error(
            crate::error::catalog::ER_GET_ERRNO,
            "online FK replay reference count overflow/underflow",
        )
    })?;
    Ok(counts)
}

#[cfg(test)]
mod fk_disk_count_tests {
    use super::*;

    #[test]
    #[ignore = "manual fixed-cache FK spill scaling benchmark"]
    fn fk_spill_scaling_benchmark() {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        for n in [120_000, 480_000, 960_000, 1_920_000] {
            let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
            let mut disk = OnlineFkDiskCounts::new(
                dir.path(),
                ahash::RandomState::with_seeds(1, 2, 3, 4),
                16 * 1024,
            )
            .unwrap();
            disk.enable_cache(8 * 1024 * 1024);
            let start = Instant::now();
            for i in 0..n {
                let key = IndexKey::from(vec![
                    KeyPart::Int(i),
                    KeyPart::Int(i / 3000),
                    KeyPart::Int(1),
                ]);
                disk.update(&key, false, true).unwrap();
            }
            for i in 0..n {
                let key = IndexKey::from(vec![
                    KeyPart::Int(i),
                    KeyPart::Int(i / 3000),
                    KeyPart::Int(1),
                ]);
                assert_eq!(disk.update(&key, true, true).unwrap(), ((1, 0), (1, 1)));
            }
            eprintln!(
                "fk_spill keys={n} cache_bytes=8388608 elapsed_ms={}",
                start.elapsed().as_millis()
            );
        }
    }

    #[test]
    fn disk_directory_growth_preserves_collisions_and_replay_without_more_cache() {
        let dir = tempfile::tempdir().unwrap();
        for pages in [0, 1, 3] {
            let mut disk = OnlineFkDiskCounts::new(
                dir.path(),
                ahash::RandomState::with_seeds(1, 2, 3, 4),
                128 * 1024,
            )
            .unwrap();
            // Exercise several doublings with a small fixture; the initial
            // allocated directory remains zero-filled beyond these buckets.
            disk.buckets = 4;
            let budget = pages * std::mem::size_of::<ConstraintSpillPage>();
            assert_eq!(disk.enable_cache(budget), budget);
            let key = |i| {
                IndexKey::from(vec![
                    KeyPart::Int(i),
                    KeyPart::Bytes(vec![42; if i % 7 == 0 { 9000 } else { 19 }]),
                ])
            };
            for i in 0..257 {
                assert_eq!(disk.update(&key(i), false, true).unwrap(), ((0, 0), (1, 0)));
            }
            assert!(disk.buckets >= 128);
            assert_eq!(
                disk.cache.borrow().pages.capacity() * std::mem::size_of::<ConstraintSpillPage>(),
                budget
            );
            for i in (0..257).rev() {
                assert_eq!(disk.update(&key(i), true, true).unwrap(), ((1, 0), (1, 1)));
                assert_eq!(
                    disk.update(&key(i), false, false).unwrap(),
                    ((1, 1), (0, 1))
                );
                assert_eq!(disk.update(&key(i), true, false).unwrap(), ((0, 1), (0, 0)));
            }
            // Full hash collisions must still compare keys, across growth.
            // 129 colliding records span more than the three-page cache.
            for i in 257..386 {
                disk.insert(&key(i), 7, (i as u64, 2)).unwrap();
            }
            disk.cache.borrow_mut().flush_and_invalidate().unwrap();
            for i in 257..386 {
                assert_eq!(disk.find(&key(i), 7).unwrap().unwrap().1, (i as u64, 2));
            }
            for i in 0..257 {
                assert_eq!(disk.update(&key(i), false, true).unwrap(), ((0, 0), (1, 0)));
            }
            assert!(disk.find(&key(387), 7).unwrap().is_none());
        }
    }

    #[test]
    fn migration_cache_preserves_counts_with_full_and_exhausted_budgets() {
        let dir = tempfile::tempdir().unwrap();
        for available in [
            0,
            2 * std::mem::size_of::<ConstraintSpillPage>(),
            4 * 1024 * 1024,
        ] {
            let mut memory = ConstraintValidationMemory::new().unwrap();
            let live_map_bytes = 128 * 1024;
            memory.limit_bytes = live_map_bytes + available;
            memory.grow(live_map_bytes).unwrap();
            let mut disk =
                OnlineFkDiskCounts::new(dir.path(), ahash::RandomState::new(), 128 * 1024).unwrap();
            disk.enable_migration_cache(&mut memory);
            assert!(memory.charge.bytes() <= memory.limit_bytes);
            assert_eq!(disk.cache.borrow().pages.is_empty(), available == 0);
            let key = |i| IndexKey::from(vec![KeyPart::Int(i), KeyPart::Bytes(vec![42; 9000])]);
            // Force hash collisions and records spanning multiple cache pages.
            for i in 0..37 {
                disk.insert(&key(i), 7, (i as u64 + 1, 1)).unwrap();
            }
            if available == 4 * 1024 * 1024 {
                // All records are still buffered; migration issued no tiny
                // append writes before the explicit writeback below.
                assert_eq!(
                    disk.file.metadata().unwrap().len(),
                    OnlineFkDiskCounts::BUCKETS * 8
                );
            }
            let released = disk.cache.borrow_mut().release().unwrap();
            memory.shrink(released);
            assert_eq!(memory.charge.bytes(), live_map_bytes);
            // The next cache has a different geometry, just as after migration.
            let budget = 3 * std::mem::size_of::<ConstraintSpillPage>();
            disk.enable_cache(budget);
            for i in 0..37 {
                assert_eq!(disk.find(&key(i), 7).unwrap().unwrap().1, (i as u64 + 1, 1));
            }
            assert!(disk.find(&key(100), 7).unwrap().is_none());
        }
    }

    #[test]
    fn spill_cache_yields_budget_to_mandatory_validation_state() {
        let dir = tempfile::tempdir().unwrap();
        let mut memory = ConstraintValidationMemory::new().unwrap();
        memory.limit_bytes = 64 * 1024;
        let mut disk =
            OnlineFkDiskCounts::new(dir.path(), ahash::RandomState::new(), 16 * 1024).unwrap();
        let cache_bytes = 4 * std::mem::size_of::<ConstraintSpillPage>();
        memory.grow(cache_bytes).unwrap();
        assert_eq!(disk.enable_cache(cache_bytes), cache_bytes);
        memory
            .spill_caches
            .push(std::rc::Rc::downgrade(&disk.cache));
        let key = IndexKey::from(vec![KeyPart::Int(42)]);
        disk.update(&key, false, true).unwrap();
        // Forces dirty pages to disk and releases the optional reservation.
        memory.grow(60 * 1024).unwrap();
        assert_eq!(memory.charge.bytes(), 60 * 1024);
        assert!(disk.cache.borrow().pages.is_empty());
        assert_eq!(disk.update(&key, true, true).unwrap(), ((1, 0), (1, 1)));
        assert!(memory.grow(8 * 1024).is_err());
    }

    #[test]
    fn spill_page_cache_matches_uncached_counts_across_evictions() {
        let dir = tempfile::tempdir().unwrap();
        let hasher = ahash::RandomState::new();
        let mut cached = OnlineFkDiskCounts::new(dir.path(), hasher.clone(), 128 * 1024).unwrap();
        let mut direct = OnlineFkDiskCounts::new(dir.path(), hasher, 128 * 1024).unwrap();
        // Seed before enabling the cache, just as migration from the map does.
        let key = |i: usize| {
            IndexKey::from(vec![
                KeyPart::UInt(i as u64),
                KeyPart::Bytes(vec![i as u8; if i % 31 == 0 { 9000 } else { 19 }]),
            ])
        };
        for i in 0..257 {
            assert_eq!(
                cached.update(&key(i), false, true).unwrap(),
                direct.update(&key(i), false, true).unwrap()
            );
        }
        let budget = 3 * std::mem::size_of::<ConstraintSpillPage>();
        assert_eq!(cached.enable_cache(budget), budget);
        // Records and keys cross page boundaries, with many dirty collisions.
        for round in 0..4 {
            for i in (0..257).map(|n| (n * 113) % 257) {
                let parent = round % 2 == 0;
                let add = round < 2;
                assert_eq!(
                    cached.update(&key(i), parent, add).unwrap(),
                    direct.update(&key(i), parent, add).unwrap()
                );
            }
        }
        for i in 0..257 {
            let k = key(i);
            assert_eq!(cached.find(&k, cached.hash(&k)).unwrap().unwrap().1, (1, 0));
        }
    }

    #[test]
    fn disk_count_hash_collisions_compare_full_keys_and_release_files() {
        let dir = tempfile::tempdir().unwrap();
        let mut disk =
            OnlineFkDiskCounts::new(dir.path(), ahash::RandomState::new(), 16 * 1024).unwrap();
        let path = disk.tmp.path().to_path_buf();
        disk.enable_cache(std::mem::size_of::<ConstraintSpillPage>());
        let key = |value: &str| {
            IndexKey::from(vec![
                KeyPart::CollatedString {
                    collation_id: crate::charset::collation_id("utf8mb4_general_ci"),
                    value: value.into(),
                },
                KeyPart::Decimal {
                    value: 125,
                    scale: 2,
                },
            ])
        };
        let a = key("AbC");
        let b = key("DEF");
        disk.insert(&a, 7, (3, 1)).unwrap();
        disk.insert(&b, 7, (5, 0)).unwrap();
        assert_eq!(disk.find(&key("abc"), 7).unwrap().unwrap().1, (3, 1));
        assert_eq!(disk.find(&key("def"), 7).unwrap().unwrap().1, (5, 0));
        assert!(disk.find(&key("missing"), 7).unwrap().is_none());
        let c = key("counts");
        assert!(disk.update(&c, true, false).is_err());
        assert_eq!(disk.update(&c, false, true).unwrap(), ((0, 0), (1, 0)));
        assert_eq!(disk.update(&c, true, true).unwrap(), ((1, 0), (1, 1)));
        assert_eq!(disk.update(&c, false, false).unwrap(), ((1, 1), (0, 1)));
        assert_eq!(disk.update(&c, true, false).unwrap(), ((0, 1), (0, 0)));
        assert_eq!(disk.update(&c, true, true).unwrap(), ((0, 0), (0, 1)));
        drop(disk);
        assert!(!path.exists());
    }
}
