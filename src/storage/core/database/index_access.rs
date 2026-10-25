/// Resolve any deferred `KeyPart::Param` parts in `key` against the
/// thread-local parameter context, then truncate each *just-resolved*
/// part to the index's per-column prefix length. The planner already
/// truncates concrete literals at plan time (see
/// `constraint_key_part_for_index`); only deferred-Param parts need
/// runtime truncation, which we apply selectively so a key that was
/// fully concrete at plan time stays bit-identical here.
///
/// Returns `(resolved_key, unsatisfiable)`. `unsatisfiable` is `true` when a
/// deferred parameter binds to NULL. Every predicate the planner pushes into
/// an index access is non-null-safe — `col = ?`, `col < ?`, `col BETWEEN ?
/// AND ?`, `col LIKE ?` — and SQL `=`/`<`/`>` against NULL is UNKNOWN, so the
/// access can never match a stored row (`WHERE col = ?` with `? = NULL`
/// returns the empty set, like MySQL). `col IS NULL` produces a *concrete*
/// `KeyPart::Null` (never a `Param`), so it is unaffected and keeps matching
/// stored NULLs. Single-key / range-scan callers must return no rows when
/// this is `true`; batched-INLJ probe callers only ever pass concrete keys,
/// so they never observe it and can ignore the flag.
fn resolve_index_key(index: &IndexSchema, key: IndexKey) -> Result<(IndexKey, bool), QueryError> {
    if !key.contains_param() {
        return Ok((key, false));
    }
    let has_prefix = index.has_prefix_columns();
    // IndexKey stores up to four parts inline. Keep deferred-parameter
    // resolution inline as well: constructing an intermediate Vec forced one
    // allocator round-trip for every prepared point/range probe even though
    // the overwhelmingly common primary/unique key has one or two parts.
    let mut parts = smallvec::SmallVec::<[KeyPart; 4]>::with_capacity(key.0.len());
    let mut unsatisfiable = false;
    for (pos, part) in key.0.into_iter().enumerate() {
        let was_param = matches!(part, KeyPart::Param { .. });
        let mut resolved = part.resolve_param()?;
        // A parameter that binds to NULL makes this non-null-safe index
        // predicate UNKNOWN for every stored entry — the access is
        // unsatisfiable. (`IS NULL` is a concrete `KeyPart::Null`, never a
        // Param, so it does not land here.)
        if was_param && matches!(resolved, KeyPart::Null) {
            unsatisfiable = true;
        }
        if has_prefix && was_param {
            resolved = index.apply_prefix_to_key_part(pos, resolved);
        }
        parts.push(resolved);
    }
    Ok((IndexKey::from(parts), unsatisfiable))
}

#[derive(Debug, Clone)]
pub(crate) struct FulltextDiagnosticPosting {
    pub word: String,
    pub doc_id: u64,
    pub positions: Vec<u32>,
}

/// Resolve deferred params in an optional range bound. Returns
/// `(resolved_bound, unsatisfiable)`; see [`resolve_index_key`] for the
/// meaning of `unsatisfiable`. An absent bound is never unsatisfiable.
fn resolve_index_bound(
    index: &IndexSchema,
    bound: Option<IndexBound>,
) -> Result<(Option<IndexBound>, bool), QueryError> {
    match bound {
        Some(b) => {
            if let Some(KeyPart::Param {
                idx,
                target:
                    crate::storage::ParamTarget::LikePattern {
                        collation_id,
                        binary,
                        char_semantics,
                        upper,
                        escape,
                        prefix_len: _,
                    },
            }) = b.key.0.last()
            {
                crate::engine::status_counters::record_like_runtime_reclassify();
                let idx = *idx;
                let collation_id = *collation_id;
                let binary = *binary;
                let char_semantics = *char_semantics;
                let upper = *upper;
                let escape = *escape;
                let prior = IndexKey::from(b.key.0[..b.key.0.len() - 1].to_vec());
                let (prior, prior_unsat) = resolve_index_key(index, prior)?;
                if prior_unsat {
                    return Ok((None, true));
                }
                let value = crate::engine::params::current_param(idx).ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_WRONG_ARGUMENTS,
                        "parameter is not bound",
                    )
                })?;
                if matches!(value, crate::types::Value::Null) {
                    return Ok((None, true));
                }
                let pattern = match value {
                    crate::types::Value::String(s) | crate::types::Value::Decimal(s) => s,
                    crate::types::Value::Bytes(bytes) => match String::from_utf8(bytes) {
                        Ok(s) => s,
                        Err(_) => {
                            return Ok((
                                (!prior.0.is_empty()).then_some(IndexBound {
                                    key: prior,
                                    inclusive: true,
                                }),
                                false,
                            ))
                        }
                    },
                    other => other.to_display_string(),
                };
                let shape = crate::engine::expr::analyze_like_sargable_pattern(&pattern, escape);
                let Some(shape) = shape else {
                    return Ok((
                        (!prior.0.is_empty()).then_some(IndexBound {
                            key: prior,
                            inclusive: true,
                        }),
                        false,
                    ));
                };
                let base_target = if binary {
                    crate::storage::ParamTarget::Bytes
                } else if let Some(collation_id) = collation_id {
                    if char_semantics {
                        crate::storage::ParamTarget::CollatedCharString { collation_id }
                    } else {
                        crate::storage::ParamTarget::CollatedString { collation_id }
                    }
                } else if char_semantics {
                    crate::storage::ParamTarget::CharString
                } else {
                    crate::storage::ParamTarget::String
                };
                let (literal, is_prefix) = match shape {
                    crate::engine::expr::LikeSargablePattern::Literal(s) => (s, false),
                    crate::engine::expr::LikeSargablePattern::Prefix(s) => (s, true),
                };
                let raw_value = if binary {
                    crate::types::Value::Bytes(literal.into_bytes())
                } else {
                    crate::types::Value::String(literal)
                };
                let pos = prior.0.len();
                let concrete = KeyPart::for_target(raw_value, base_target)?;
                let low_part = index.apply_prefix_to_key_part(pos, concrete);
                if !upper {
                    let mut parts = prior.0.to_vec();
                    parts.push(low_part);
                    return Ok((
                        Some(IndexBound {
                            key: IndexKey::from(parts),
                            inclusive: true,
                        }),
                        false,
                    ));
                }
                if !is_prefix {
                    let mut parts = prior.0.to_vec();
                    parts.push(low_part);
                    return Ok((
                        Some(IndexBound {
                            key: IndexKey::from(parts),
                            inclusive: true,
                        }),
                        false,
                    ));
                }
                let next_value = match &low_part {
                    KeyPart::Bytes(bytes) => {
                        let mut next = bytes.clone();
                        let mut found = false;
                        while let Some(last) = next.last_mut() {
                            if *last != u8::MAX {
                                *last += 1;
                                found = true;
                                break;
                            }
                            next.pop();
                        }
                        found.then_some(crate::types::Value::Bytes(next))
                    }
                    KeyPart::CollatedString {
                        collation_id,
                        value,
                    } => crate::engine::expr::like_prefix_successor_for_collation(
                        value,
                        Some(crate::charset::collation_name_for_id(*collation_id)),
                    ),
                    KeyPart::String(value) => crate::engine::expr::like_prefix_successor(value),
                    _ => None,
                };
                let Some(next_value) = next_value else {
                    return Ok((
                        (!prior.0.is_empty()).then_some(IndexBound {
                            key: prior,
                            inclusive: true,
                        }),
                        false,
                    ));
                };
                let high_part = index
                    .apply_prefix_to_key_part(pos, KeyPart::for_target(next_value, base_target)?);
                let mut parts = prior.0.to_vec();
                parts.push(high_part);
                return Ok((
                    Some(IndexBound {
                        key: IndexKey::from(parts),
                        inclusive: false,
                    }),
                    false,
                ));
            }
            let (key, unsatisfiable) = resolve_index_key(index, b.key)?;
            Ok((
                Some(IndexBound {
                    key,
                    inclusive: b.inclusive,
                }),
                unsatisfiable,
            ))
        }
        None => Ok((None, false)),
    }
}

/// Map materialised `(IndexKey, row id)` entries back to their probe-key
/// index within `sorted_keys` (sorted ascending, all keys the same
/// eq-prefix length). Used only by the rare in-flight-repair fallback of
/// the keyed batched probe.
fn map_index_entries_to_key_indices(
    sorted_keys: &[IndexKey],
    entries: &[(IndexKey, RowId)],
) -> Vec<(u32, RowId)> {
    let Some(prefix_len) = sorted_keys.first().map(|key| key.0.len()) else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(entries.len());
    for (entry_key, row_id) in entries {
        if entry_key.0.len() < prefix_len {
            continue;
        }
        if let Ok(idx) =
            sorted_keys.binary_search_by(|key| key.0.as_slice().cmp(&entry_key.0[..prefix_len]))
        {
            out.push((idx as u32, *row_id));
        }
    }
    out
}

/// Decode a SPATIAL index key (four order-preserving `Float64` parts) back
/// into the geometry MBR it encodes. Returns `None` for any non-spatial key.
fn decode_mbr_key(key: &IndexKey) -> Option<crate::engine::geo::Mbr> {
    if key.0.len() < 4 {
        return None;
    }
    let f = |kp: &KeyPart| match kp {
        KeyPart::Float64(u) => Some(f64_from_order_u64(*u)),
        _ => None,
    };
    Some(crate::engine::geo::Mbr::new(
        f(&key.0[0])?,
        f(&key.0[1])?,
        f(&key.0[2])?,
        f(&key.0[3])?,
    ))
}

/// `MYDB_INDEX_PAGE_MAX_TRX` env var. Default `on`: allow covering /
/// index-only scans to short-circuit visibility filtering via a leaf's
/// `page_max_trx_id` upper bound. Set to `off` / `0` / `false` to
/// disable the shortcut and force the heap-header path on
/// every chunk — kept as a differential oracle so any regression can
/// be diffed against the pre-Phase-2 behavior.
pub(crate) fn index_page_max_trx_shortcut_enabled() -> bool {
    use std::sync::OnceLock;
    static FLAG: OnceLock<bool> = OnceLock::new();
    *FLAG.get_or_init(|| {
        crate::util::env::var("MYDB_INDEX_PAGE_MAX_TRX")
            .map(|v| {
                !matches!(
                    v.trim().to_ascii_lowercase().as_str(),
                    "0" | "off" | "false" | "no" | "disable" | "disabled"
                )
            })
            .unwrap_or(true)
    })
}

impl Database {
    pub(crate) fn temporal_history_index_degraded(&self) -> bool {
        self.temporal_history.history_index_degraded()
    }

    pub(crate) fn temporal_history_index_stats(
        &self,
        table_id: u64,
        index_name: &str,
    ) -> (usize, usize) {
        self.temporal_history
            .history_index_stats(table_id, index_name)
    }

    /// Query the R-tree spatial index: build an in-memory R-tree from the
    /// spatial index's persisted MBR B-tree entries and return every row whose
    /// stored MBR intersects `query`. This is a correct *superset* for all
    /// spatial-index-driving predicates — the caller re-checks the exact
    /// predicate against each candidate's heap row. The durable storage is the
    /// MBR B-tree (crash-safe via the WAL); the R-tree is the 2-D search
    /// structure built from it.
    pub(crate) fn spatial_search(
        &self,
        table_id: u64,
        index_name: &str,
        query: crate::engine::geo::Mbr,
    ) -> Result<Vec<RowId>, QueryError> {
        let cache_key = (table_id, normalize_name(index_name));
        let registered_generation = self
            .index_mutation_generations
            .read()
            .get(&cache_key)
            .copied();
        // Publish interest before scanning the durable index. A mutation
        // completed before registration is included in the scan; one that
        // completes afterwards must invalidate its captured generation.
        let generation = registered_generation.unwrap_or_else(|| {
            *self
                .index_mutation_generations
                .write()
                .entry(cache_key.clone())
                .or_insert(0)
        });
        if let Some(cache) = self.spatial_rtree_cache.read().get(&cache_key).cloned() {
            if cache.generation.load(Ordering::Acquire) == generation {
                return Ok(cache.tree.read().search(&query));
            }
        }

        let entries = self.with_index_read(table_id, index_name, |idx| idx.entries())?;
        let generation_after_scan = self
            .index_mutation_generations
            .read()
            .get(&cache_key)
            .copied()
            .unwrap_or(0);
        let direct_search = || {
            entries
                .iter()
                .filter_map(|(key, row)| {
                    decode_mbr_key(key)
                        .filter(|mbr| mbr.intersects(&query))
                        .map(|_| *row)
                })
                .collect()
        };
        if generation_after_scan != generation {
            return Ok(direct_search());
        }

        let initial_charge = entries.capacity().saturating_mul(128).saturating_add(4096);
        let Some(mut charge) =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::DerivedCache,
                initial_charge,
            )
        else {
            return Ok(direct_search());
        };
        let tree = rtree::RTree::from_entries(
            entries
                .iter()
                .filter_map(|(key, row)| decode_mbr_key(key).map(|m| (m, *row))),
        );
        let actual_capacity = tree.memory_capacity_bytes();
        if actual_capacity > charge.bytes()
            && charge.try_grow(actual_capacity - charge.bytes()).is_err()
        {
            return Ok(direct_search());
        }
        let result = tree.search(&query);
        self.spatial_rtree_cache.write().insert(
            cache_key,
            Arc::new(SpatialRtreeCache {
                generation: AtomicU64::new(generation),
                tree: parking_lot::RwLock::new(tree),
                _charge: charge,
            }),
        );
        Ok(result)
    }

    /// Add old spatial keys needed by this snapshot after probing the physical
    /// R-tree. Keep repairs out of the shared cache: their visibility depends
    /// on the reader's snapshot. A repair version change during the probe
    /// requires a fresh physical probe while the repair directory is pinned.
    pub(crate) fn spatial_search_visible(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        query: crate::engine::geo::Mbr,
        txn: TxnView,
    ) -> Result<Vec<RowId>, QueryError> {
        self.check_index_read_view(table, index, txn)?;
        let version_before = self.index_repair_version.load(Ordering::Acquire);
        let mut ids = self.spatial_search(table.id, &index.name, query)?;
        let version_after = self.index_repair_version.load(Ordering::Acquire);
        if !txn.historical && version_before == version_after && version_after & 1 == 0 {
            return Ok(ids);
        }

        let txns = self.txns_read()?;
        if self.index_repair_version.load(Ordering::Acquire) != version_before {
            ids = self.spatial_search(table.id, &index.name, query)?;
        }
        let previous_len = ids.len();
        let key_matches =
            |key: &IndexKey| decode_mbr_key(key).is_some_and(|mbr| mbr.intersects(&query));
        ids.extend(txns.index_visibility_repair_row_ids(
            txn,
            table.id,
            &index.name,
            key_matches,
            |txn_id| {
                self.temporal_history
                    .txn_commit(txn_id)
                    .map(|entry| entry.commit_scn)
            },
        ));
        if txn.historical {
            self.record_temporal_history_index_hit();
            ids.extend(
                self.temporal_history
                    .history_index_entries(txn, table.id, &index.name, key_matches)
                    .into_iter()
                    .map(|(_, row_id)| row_id),
            );
        }
        if ids.len() != previous_len {
            ids.sort_unstable();
            ids.dedup();
        }
        Ok(ids)
    }

    /// Apply a committed DELETE to an already-warm R-tree. Deletes may leave
    /// stale candidates while a multi-row commit is being drained, but can
    /// never omit a live row, so publishing the current generation remains a
    /// correct superset. INSERT/UPDATE deltas are invalidated instead: without
    /// associating every delta with its durable B-tree generation, publishing
    /// them incrementally could race another commit and cause false negatives.
    pub(crate) fn update_warm_spatial_cache(
        &self,
        storage_id: u64,
        index_name: &str,
        row_id: RowId,
        old_key: Option<&IndexKey>,
        new_key: Option<&IndexKey>,
    ) {
        let cache_key = (storage_id, normalize_name(index_name));
        let Some(cache) = self.spatial_rtree_cache.read().get(&cache_key).cloned() else {
            return;
        };
        let old_mbr = old_key.and_then(decode_mbr_key);
        let new_mbr = new_key.and_then(decode_mbr_key);
        if new_mbr.is_some() {
            let mut caches = self.spatial_rtree_cache.write();
            if caches
                .get(&cache_key)
                .is_some_and(|current| Arc::ptr_eq(current, &cache))
            {
                caches.remove(&cache_key);
            }
            return;
        }
        let Some(old_mbr) = old_mbr else {
            return;
        };

        let cached_generation = cache.generation.load(Ordering::Acquire);
        let generation = self
            .index_mutation_generations
            .read()
            .get(&cache_key)
            .copied()
            .unwrap_or(0);
        let next_generation = cached_generation.wrapping_add(1).max(1);
        if generation != cached_generation && generation != next_generation {
            let mut caches = self.spatial_rtree_cache.write();
            if caches
                .get(&cache_key)
                .is_some_and(|current| Arc::ptr_eq(current, &cache))
            {
                caches.remove(&cache_key);
            }
            return;
        }

        cache.tree.write().delete(old_mbr, row_id);
        let generation_after_delete = self
            .index_mutation_generations
            .read()
            .get(&cache_key)
            .copied()
            .unwrap_or(0);
        if generation_after_delete == generation {
            cache.generation.store(generation, Ordering::Release);
        } else {
            let mut caches = self.spatial_rtree_cache.write();
            if caches
                .get(&cache_key)
                .is_some_and(|current| Arc::ptr_eq(current, &cache))
            {
                caches.remove(&cache_key);
            }
        }
    }

    /// Whether any pending index-visibility-repair entries exist that
    /// need to be merged into raw index reads. Hot-path SELECT-side
    /// readers (covering scans, COUNT pushdown) consult this so they
    /// can skip the txns-read lock and the repair-merge step entirely
    /// in the common case (no in-flight DDL/DML repairs).
    pub fn has_pending_index_repairs(&self) -> bool {
        self.index_repair_version.load(Ordering::Acquire) & 1 != 0
    }

    /// Changes whenever a repair key is registered or purged. A reader that
    /// observed the same value before and after a lookup saw no new repairs.
    pub(crate) fn index_repair_version(&self) -> u64 {
        self.index_repair_version.load(Ordering::Acquire)
    }

    /// Per-index check used after the global atomic says some repair
    /// exists. Unrelated tables/indexes retain page visibility shortcuts.
    pub(crate) fn has_pending_index_repairs_for(&self, table_id: u64, index_name: &str) -> bool {
        // Lock-free: the per-index scope counts registered keys exactly; a
        // colliding index can only make this conservatively true.
        self.has_pending_index_repairs() && self.index_repair_epochs.has_keys(table_id, index_name)
    }

    /// Run a physical read followed by its repair merge again whenever a
    /// rolled-back writer removed retired keys of this index meanwhile: the
    /// read may predate the writer's restore while the merge no longer sees
    /// its retired keys, losing those rows.
    pub(crate) fn retry_if_rollback_overlapped<T>(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        mut read: impl FnMut() -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        loop {
            let epoch = self.index_repair_rollback_epoch(table.id, &index.name);
            let value = read()?;
            if self.index_repair_rollback_epoch(table.id, &index.name) == epoch {
                return Ok(value);
            }
            crate::engine::cancel::check_cancelled()?;
        }
    }

    /// `(retired keys registered, rollback epoch)` of one index, sampled
    /// together before a physical read.
    pub(crate) fn index_repair_activity(&self, table_id: u64, index_name: &str) -> (bool, u64) {
        let epoch = self.index_repair_rollback_epoch(table_id, index_name);
        (self.has_pending_index_repairs_for(table_id, index_name), epoch)
    }

    /// Moves whenever a rolled-back writer removes retired keys of this
    /// index scope. A physical read is only covered by a repair refresh
    /// taken after it when this value did not move across both.
    pub(crate) fn index_repair_rollback_epoch(&self, table_id: u64, index_name: &str) -> u64 {
        self.index_repair_epochs.rollback_epoch(table_id, index_name)
    }

    /// Writer state for an index-repair verdict. Repair writers are always
    /// published in the lock-free slot-status mirror before they can register
    /// a retired key, so the transaction directory is consulted only on a
    /// mirror miss (a collected slot, or a commit SCN beyond the mirror).
    fn index_repair_writer_state(
        &self,
        writer: u64,
    ) -> Result<Option<(TxnStatus, Option<u64>)>, QueryError> {
        if let Some((status, commit_scn)) = self.slot_status_cache.lookup(writer) {
            return Ok(Some((
                status,
                (status == TxnStatus::Committed).then_some(commit_scn),
            )));
        }
        Ok(self
            .txns_read()?
            .slot(writer)
            .map(|slot| (slot.status, slot.commit_scn)))
    }

    /// Keep the repair candidates whose retired keys `txn` must still see.
    /// Candidates come from a registry snapshot that is already released:
    /// a writer finishing afterwards resolves to the same verdict, because
    /// a commit after the snapshot carries a newer SCN, a rollback restores
    /// the physical entry, and purge only drops entries no snapshot needs.
    fn live_index_repair_entries(
        &self,
        txn: TxnView,
        candidates: Vec<crate::storage::txn::IndexRepairCandidate>,
    ) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
        let mut live = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            if candidate.writer == txn.txn_id {
                continue;
            }
            let durable_commit_scn = |id| {
                self.temporal_history
                    .txn_commit(id)
                    .map(|entry| entry.commit_scn)
            };
            let state = self.index_repair_writer_state(candidate.writer)?;
            if !crate::storage::txn::repair_entry_live_for(
                txn,
                candidate.writer,
                state,
                durable_commit_scn,
            ) {
                continue;
            }
            let creator_state = if candidate.creator == 0 {
                None
            } else {
                self.index_repair_writer_state(candidate.creator)?
            };
            if crate::storage::txn::repair_key_created_visible_to(
                txn,
                candidate.creator,
                creator_state,
                durable_commit_scn,
            ) {
                live.push((candidate.key, candidate.row_id));
            }
        }
        Ok(live)
    }

    #[inline]
    pub(crate) fn record_index_page_max_trx_shortcut_hit(&self) {
        self.index_page_max_trx_shortcut_hits
            .fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn record_index_page_max_trx_shortcut_miss(&self) {
        self.index_page_max_trx_shortcut_misses
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Bulk shortcut: if the leaf's `page_max_trx_id` proves every entry
    /// is visible to `view` (and no compensated deletes are pending),
    /// return an `all-true` mask of length `chunk_len` without touching
    /// the heap-header path. Bumps the hit/miss counters as a
    /// side effect. Callers combine this with
    /// `filter_visible_row_ids_mask_with_handle` (or its entry variants)
    /// on a `None` return.
    #[cfg(test)]
    #[inline]
    pub(crate) fn try_index_page_max_trx_shortcut(
        &self,
        page_max_trx_id: u64,
        view: TxnView,
        chunk_len: usize,
    ) -> Option<Vec<bool>> {
        if self.has_pending_index_repairs() {
            return None;
        }
        if !self.index_page_all_visible(page_max_trx_id, view) {
            return None;
        }
        Some(vec![true; chunk_len])
    }

    #[inline]
    pub(crate) fn try_index_page_max_trx_shortcut_for(
        &self,
        table_id: u64,
        index_name: &str,
        page_max_trx_id: u64,
        view: TxnView,
        chunk_len: usize,
    ) -> Option<Vec<bool>> {
        if self.has_pending_index_repairs_for(table_id, index_name) {
            return None;
        }
        if !self.index_page_all_visible(page_max_trx_id, view) {
            return None;
        }
        Some(vec![true; chunk_len])
    }

    /// InnoDB-style PAGE_MAX_TRX_ID visibility shortcut.
    ///
    /// A leaf whose `page_max_trx_id` is at or below the contiguous GC
    /// watermark (`min_retained_txn_id`) has the property that every writer whose
    /// entry survives on the leaf committed *before* every current
    /// snapshot's oldest active txn — so every surviving entry is
    /// definitely visible to `view` without any heap probe. The
    /// proof is derived from the index write path and the contiguous GC
    /// watermark, so it requires no `ANALYZE TABLE` to become valid.
    ///
    /// Corner cases:
    /// * `page_max_trx_id == 0`: bulk-loaded / empty page — trivially
    ///   visible to any reader (the writer is `txn_id_for_ddl()` and
    ///   DDL commits before the table becomes queryable).
    /// * `!has_pending_index_repairs()` is required by the caller:
    ///   without it, an entry that a visible-to-view later writer
    ///   marked-for-deletion could still be physically on the leaf
    ///   (mydb defers index deletes through the compensation table).
    ///   Callers combine both gates.
    /// * Falls back to `false` when the shortcut cannot fire, letting
    ///   the caller run the full heap-header path.
    ///
    /// See `doc_old/design/index-page-max-trx-2026-07.md`.
    #[inline]
    pub(crate) fn index_page_all_visible(&self, page_max_trx_id: u64, view: TxnView) -> bool {
        // Offline rebuilds contain physical rows of surviving XA branches.
        // Their zero bulk-load bound cannot prove transaction visibility.
        if self.xa_prepared_active.load(Ordering::Acquire)!=0{return false;}
        if view.historical {
            return page_max_trx_id == 0;
        }
        if !index_page_max_trx_shortcut_enabled() {
            return false;
        }
        if page_max_trx_id == 0 {
            self.record_index_page_max_trx_shortcut_hit();
            return true;
        }
        let watermark = self.min_retained_txn_id.load(Ordering::Relaxed);
        // Transaction-id order is not commit-SCN order. In particular, a
        // lower-id transaction can publish a higher commit SCN while an
        // earlier SCN allocated by a higher-id transaction still leaves a
        // hole in the commit frontier. Therefore an "oldest active txn id"
        // bound cannot prove page visibility. The GC watermark below is safe
        // only because slot GC advances it over a *contiguous* prefix.
        if watermark > 0 && page_max_trx_id <= watermark {
            self.record_index_page_max_trx_shortcut_hit();
            return true;
        }
        self.record_index_page_max_trx_shortcut_miss();
        false
    }

    /// Diagnostic / test helper: return the catalog `id` of the table
    /// whose unqualified name matches `name`, ignoring case. Lookup is
    /// O(tables); intended for tests, not the hot SELECT path.
    #[cfg(test)]
    pub(crate) fn table_id_for_test(&self, name: &str) -> Option<u64> {
        let cat = self.catalog.read().ok()?;
        cat.tables
            .values()
            .find(|t| t.name.eq_ignore_ascii_case(name))
            .map(|t| t.id)
    }

    /// Like [`lookup_index`], but each matching `row_id` is paired with the
    /// `storage_id` of the partition it was found in. Unique-constraint
    /// checking needs this: a bare `row_id` from a partitioned index is
    /// ambiguous across partitions, so the conflict validation must read the
    /// row from the exact partition heap (not `table.id`, which has no heap
    /// for a partitioned table — the root cause of missed 1062s). For an
    /// unpartitioned table the pair is always `(table.id, row_id)`.
    pub fn lookup_index_row_locations(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        key: IndexKey,
    ) -> Result<Vec<(u64, RowId)>, QueryError> {
        let (key, unsatisfiable) = resolve_index_key(index, key)?;
        if unsatisfiable {
            return Ok(Vec::new());
        }
        if table.is_partitioned() {
            let candidate_sids: Vec<u64> =
                match crate::engine::executor::partition_router::current_pruned_partitions() {
                    Some(pruned) => {
                        let all = table.storage_ids();
                        pruned.into_iter().filter(|s| all.contains(s)).collect()
                    }
                    None => table.storage_ids(),
                };
            let mut out: Vec<(u64, RowId)> = Vec::new();
            for sid in candidate_sids {
                let ids = self.with_index_read(sid, &index.name, |index_file| {
                    index_file.get(&key, !index.unique)
                })?;
                out.extend(ids.into_iter().map(|rid| (sid, rid)));
            }
            return Ok(out);
        }
        let ids = self.with_index_read(table.id, &index.name, |index_file| {
            index_file.get(&key, !index.unique)
        })?;
        Ok(ids.into_iter().map(|rid| (table.id, rid)).collect())
    }

    pub fn lookup_index(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        key: IndexKey,
    ) -> Result<Vec<RowId>, QueryError> {
        let (key, unsatisfiable) = resolve_index_key(index, key)?;
        if unsatisfiable {
            return Ok(Vec::new());
        }
        if table.is_partitioned() {
            // RowId is per-partition; if the active partition prune
            // set narrows the search to one partition, the returned
            // row_ids are unambiguous. Otherwise we visit every
            // partition's local index — which is correct, but the
            // ambiguous row_ids must be disambiguated by the caller
            // via `locate_row_storage_id`.
            let candidate_sids: Vec<u64> =
                match crate::engine::executor::partition_router::current_pruned_partitions() {
                    Some(pruned) => {
                        let all = table.storage_ids();
                        pruned.into_iter().filter(|s| all.contains(s)).collect()
                    }
                    None => table.storage_ids(),
                };
            let mut out: Vec<RowId> = Vec::new();
            for sid in candidate_sids {
                let ids = self.with_index_read(sid, &index.name, |index_file| {
                    index_file.get(&key, !index.unique)
                })?;
                out.extend(ids);
            }
            return Ok(out);
        }
        self.with_index_read(table.id, &index.name, |index_file| {
            index_file.get(&key, !index.unique)
        })
    }

    pub fn lookup_index_visible(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        key: IndexKey,
        txn: TxnView,
    ) -> Result<Vec<RowId>, QueryError> {
        self.check_index_read_view(table, index, txn)?;
        let (key, unsatisfiable) = resolve_index_key(index, key)?;
        if unsatisfiable {
            return Ok(Vec::new());
        }
        let mut ids = self.lookup_index(table, index, key.clone())?;
        let before = ids.len();
        self.add_exact_index_visibility_repair_ids(table, index, txn, &key, &mut ids)?;
        if ids.len() != before {
            dedup_row_ids(&mut ids);
        }
        Ok(ids)
    }


    /// A unique point lookup must merge old keys before choosing a row.
    /// Version validation coordinates the repair snapshot and physical probe,
    /// including registration and rollback while the probe is in progress.
    pub fn lookup_unique_index_candidate_one(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        key: IndexKey,
        txn: TxnView,
    ) -> Result<Option<RowId>, QueryError> {
        self.check_index_read_view(table, index, txn)?;
        debug_assert!(index.unique || index.primary);
        debug_assert!(!table.is_partitioned());
        let (key, unsatisfiable) = resolve_index_key(index, key)?;
        if unsatisfiable {
            return Ok(None);
        }
        let (candidate, repairs, recheck) =
            self.probe_unique_index_candidate(table, index, &key, txn, || {
                self.with_index_read(table.id, &index.name, |file| file.get_unique_one(&key))
            })?;
        if !recheck {
            return Ok(candidate);
        }
        let mut ids = candidate.into_iter().collect::<Vec<_>>();
        ids.extend(repairs.into_iter().map(|(_, id)| id));
        if txn.historical {
            self.record_temporal_history_index_hit();
            ids.extend(
                self.temporal_history
                    .history_index_entries_exact(txn, table.id, &index.name, &key)
                    .into_iter()
                    .map(|(_, id)| id),
            );
        }
        // A new physical owner of the same unique key may be invisible to
        // this snapshot. It must not hide the old repaired owner. A visible
        // row also needs a key recheck after reconstructing its MVCC version.
        for id in ids {
            if let Some(row) = self.read_row(table, id, txn)? {
                if table.index_key(index, &row.values)? == key {
                    return Ok(Some(id));
                }
            }
        }
        Ok(None)
    }

    /// Consume each possible owner of a unique key through one MVCC read.
    /// When supplied, the key must be checked against the visible version,
    /// including when repairs exist only for other keys: a new index entry
    /// can point to a version whose old key is different. None is the same
    /// no-repairs proof used by lookup_unique_index_candidate_one.
    pub(crate) fn read_unique_index_candidate<T>(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        key: IndexKey,
        txn: TxnView,
        mut read: impl FnMut(RowId, Option<&IndexKey>) -> Result<Option<T>, QueryError>,
    ) -> Result<Option<T>, QueryError> {
        self.check_index_read_view(table, index, txn)?;
        debug_assert!(index.unique || index.primary);
        debug_assert!(!table.is_partitioned());
        let (key, unsatisfiable) = resolve_index_key(index, key)?;
        if unsatisfiable {
            return Ok(None);
        }
        let (candidate, repairs, recheck) =
            self.probe_unique_index_candidate(table, index, &key, txn, || {
                self.with_index_read(table.id, &index.name, |file| file.get_unique_one(&key))
            })?;
        if txn.historical {
            self.record_temporal_history_index_hit();
        }
        for id in candidate
            .into_iter()
            .chain(repairs.into_iter().map(|(_, id)| id))
        {
            if let Some(row) = read(id, recheck.then_some(&key))? {
                return Ok(Some(row));
            }
        }
        if txn.historical {
            for (_, id) in
                self.temporal_history
                    .history_index_entries_exact(txn, table.id, &index.name, &key)
            {
                if let Some(row) = read(id, Some(&key))? {
                    return Ok(Some(row));
                }
            }
        }
        Ok(None)
    }

    /// Snapshot old-key candidates under a short registry-shard lease, then
    /// probe the physical index without holding any latch across page I/O or
    /// topology waits. A version change retries the entire pair: checking
    /// only emptiness would miss registration followed by rollback (ABA).
    fn probe_unique_index_candidate(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        key: &IndexKey,
        txn: TxnView,
        mut probe: impl FnMut() -> Result<Option<RowId>, QueryError>,
    ) -> Result<(Option<RowId>, Vec<(IndexKey, RowId)>, bool), QueryError> {
        self.ensure_operational()?;
        let epoch = self.index_repair_epochs.version(table.id, &index.name);
        let mut repair_key = None;
        for _ in 0..2 {
            let version = epoch.load(Ordering::Acquire);
            // An odd scope version means this index (or a colliding one) has
            // retired keys, so a physical hit must be rechecked against the
            // key. Only an exact-key registration by another writer needs
            // writer-state resolution; unrelated retired keys never send a
            // point read through the transaction directory.
            let recheck = txn.historical || version & 1 != 0;
            let repairs = if recheck {
                let repair_key = repair_key.get_or_insert_with(|| {
                    crate::storage::txn::IndexRepairKey {
                        table_id: table.id,
                        index_name: normalize_name(&index.name),
                        key: key.clone(),
                    }
                });
                let candidates = self
                    .index_repair_state
                    .exact_candidates_for(repair_key, |writer| writer != txn.txn_id);
                if candidates.is_empty() {
                    Vec::new()
                } else {
                    self.live_index_repair_entries(txn, candidates)?
                }
            } else {
                Vec::new()
            };
            let candidate = probe()?;
            std::sync::atomic::fence(Ordering::Acquire);
            if epoch.load(Ordering::Acquire) == version {
                return Ok((candidate, repairs, recheck));
            }
        }
        // Sustained changes to this index (or a colliding scope) must not
        // starve a reader. Fall back to a coordinated probe: holding the key's
        // registry shard shared excludes any writer from retiring this key
        // (and then removing its physical entry) until the probe finishes.
        let txns = self.txns_read()?;
        let repair_key = repair_key.unwrap_or_else(|| crate::storage::txn::IndexRepairKey {
            table_id: table.id,
            index_name: normalize_name(&index.name),
            key: key.clone(),
        });
        let (candidate, candidates) = self.index_repair_state.with_key_registration_excluded(
            &repair_key,
            |writer| writer != txn.txn_id,
            &mut probe,
        );
        let candidate = candidate?;
        let recheck = txn.historical || self.index_repair_epochs.has_keys(table.id, &index.name);
        let repairs = txns.live_repair_entries(txn, candidates, |id| {
            self.temporal_history
                .txn_commit(id)
                .map(|entry| entry.commit_scn)
        });
        Ok((candidate, repairs, recheck))
    }

    pub fn lookup_index_for_write(
        &self,
        txn: TxnView,
        table: &TableSchema,
        index: &IndexSchema,
        key: IndexKey,
    ) -> Result<Vec<RowId>, QueryError> {
        self.check_primary_read_view(table, txn)?;
        // Write-path probe keys are concrete row values, never deferred
        // params, so the unsatisfiable flag is always false here.
        let (key, _) = resolve_index_key(index, key)?;
        if !index.unique {
            return self.lookup_index(table, index, key);
        }
        if index.unique {
            self.lock_manager().check_unique_key_lock(
                txn.txn_id,
                table.id,
                &index.name,
                &key,
                &self.lock_owner_status(),
            )?;
        }
        let ids = self.lookup_index(table, index, key.clone())?;
        if !ids.is_empty() {
            return Ok(ids);
        }
        self.recheck_unique_index_miss(txn.txn_id, table.id, &index.name, &key, || {
            self.lookup_index(table, index, key.clone())
        })
    }

    pub fn lookup_unique_index_for_write_one(
        &self,
        txn: TxnView,
        table: &TableSchema,
        index: &IndexSchema,
        key: IndexKey,
    ) -> Result<Option<RowId>, QueryError> {
        self.check_primary_read_view(table, txn)?;
        debug_assert!(index.unique || index.primary);
        if table.is_partitioned() {
            let mut ids = self.lookup_index_for_write(txn, table, index, key)?;
            return Ok(ids.pop());
        }
        let (key, _) = resolve_index_key(index, key)?;
        if index.unique {
            // A point write must serialize with an existing owner before it
            // trusts the unique-index result. Wait at this exact storage
            // operation instead of surfacing the internal pending sentinel to
            // the statement dispatcher: the latter deep-clones/requalifies
            // and reruns the complete SQL envelope on every unrelated global
            // lock notification. Hot exact DELETE/UPDATE workloads can see
            // dozens of such wakeups before their key becomes available.
            // The blocking helper preserves deadlock detection, cancellation
            // and innodb_lock_wait_timeout while redoing only the key probe.
            self.check_unique_key_lock_blocking(txn.txn_id, table.id, &index.name, &key)?;
        }
        let candidate = self.with_index_read(table.id, &index.name, |index_file| {
            index_file.get_unique_one(&key)
        })?;
        if !index.unique || candidate.is_some() {
            return Ok(candidate);
        }
        self.recheck_unique_index_miss(txn.txn_id, table.id, &index.name, &key, || {
            self.with_index_read(table.id, &index.name, |index_file| {
                index_file.get_unique_one(&key)
            })
        })
    }

    /// Sorted multi-key counterpart of [`Self::lookup_unique_index_for_write_one`].
    /// Homogeneous point-DML batches already own concrete, de-duplicated keys
    /// in index order; retain scalar unique-lock semantics per key, then route
    /// all probes through one index handle/read guard so shared internal pages
    /// are decoded once instead of once per statement.
    pub(crate) fn lookup_unique_index_for_write_many(
        &self,
        txn: TxnView,
        table: &TableSchema,
        index: &IndexSchema,
        keys: &[IndexKey],
    ) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
        self.check_primary_read_view(table, txn)?;
        debug_assert!(index.unique || index.primary);
        debug_assert!(!table.is_partitioned());
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut resolved = Vec::with_capacity(keys.len());
        for key in keys {
            let (key, _) = resolve_index_key(index, key.clone())?;
            if index.unique {
                self.check_unique_key_lock_blocking(txn.txn_id, table.id, &index.name, &key)?;
            }
            resolved.push(key);
        }
        debug_assert!(resolved.windows(2).all(|pair| pair[0] < pair[1]));
        self.with_index_read(table.id, &index.name, |index_file| {
            index_file.lookup_entries_many_sorted(&resolved, true, true)
        })
    }

    #[cfg(test)]
    pub fn try_lookup_index_for_claim_delete(
        &self,
        txn: TxnView,
        table: &TableSchema,
        index: &IndexSchema,
        key: IndexKey,
    ) -> Result<Option<Vec<RowId>>, QueryError> {
        self.check_primary_read_view(table, txn)?;
        // Write-path probe keys are concrete; unsatisfiable is always false.
        let (key, _) = resolve_index_key(index, key)?;
        if index.unique {
            self.lock_manager().check_unique_key_lock(
                txn.txn_id,
                table.id,
                &index.name,
                &key,
                &self.lock_owner_status(),
            )?;
        }
        self.lookup_index(&table, index, key).map(Some)
    }


    pub fn prefix_index(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        key: IndexKey,
    ) -> Result<Vec<RowId>, QueryError> {
        let (key, unsatisfiable) = resolve_index_key(index, key)?;
        if unsatisfiable {
            return Ok(Vec::new());
        }
        if table.is_partitioned() {
            let mut out: Vec<RowId> = Vec::new();
            for sid in crate::engine::executor::partition_router::in_scope_storage_ids(table) {
                let ids = self.with_index_read(sid, &index.name, |index_file| {
                    index_file.prefix(&key, !index.unique)
                })?;
                out.extend(ids);
            }
            return Ok(out);
        }
        self.with_index_read(table.id, &index.name, |index_file| {
            index_file.prefix(&key, !index.unique)
        })
    }

    pub fn prefix_index_visible(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        key: IndexKey,
        txn: TxnView,
    ) -> Result<Vec<RowId>, QueryError> {
        self.check_index_read_view(table, index, txn)?;
        let (key, unsatisfiable) = resolve_index_key(index, key)?;
        if unsatisfiable {
            return Ok(Vec::new());
        }
        self.retry_if_rollback_overlapped(table, index, || {
            let mut ids = self.prefix_index(table, index, key.clone())?;
            let before = ids.len();
            self.add_index_visibility_repair_ids(table, index, txn, &mut ids, |candidate| {
                index_key_starts_with(candidate, &key)
            })?;
            if ids.len() != before {
                dedup_row_ids(&mut ids);
            }
            Ok(ids)
        })
    }

    pub fn range_index_bounds(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        low: Option<IndexBound>,
        high: Option<IndexBound>,
    ) -> Result<Vec<RowId>, QueryError> {
        let (low, low_unsat) = resolve_index_bound(index, low)?;
        let (high, high_unsat) = resolve_index_bound(index, high)?;
        if low_unsat || high_unsat {
            return Ok(Vec::new());
        }
        if table.is_partitioned() {
            // Honour the active prune context: a RowId is only unique
            // within one partition, so a caller that fans out per
            // partition (single-storage_id prune) gets an unambiguous
            // result here. With no prune context this visits every
            // partition (correct for covering consumers that union
            // index keys; ambiguous for RowId→heap consumers, which is
            // exactly why the executor installs the fan-out).
            let mut out: Vec<RowId> = Vec::new();
            for sid in crate::engine::executor::partition_router::in_scope_storage_ids(table) {
                let ids = self.with_index_read(sid, &index.name, |index_file| {
                    index_file.range(low.as_ref(), high.as_ref(), !index.unique)
                })?;
                out.extend(ids);
            }
            return Ok(out);
        }
        self.with_index_read(table.id, &index.name, |index_file| {
            index_file.range(low.as_ref(), high.as_ref(), !index.unique)
        })
    }

    /// Resolve the `storage_id` whose local B-tree the single-partition
    /// index cursors should open. Unpartitioned tables read `table.id`.
    /// Partitioned tables read the one in-scope partition selected by
    /// the active prune context (installed by the executor's
    /// per-partition fan-out). Returns `None` for a partitioned table
    /// with zero in-scope partitions (empty scan) and, defensively,
    /// picks the first when the context is ambiguous — callers of the
    /// forward/reverse cursors always run under a single-partition
    /// fan-out for partitioned tables, so "ambiguous" does not arise on
    /// the correctness-critical paths.
    fn index_cursor_storage_id(&self, table: &TableSchema) -> Option<u64> {
        if !table.is_partitioned() {
            return Some(table.id);
        }
        crate::engine::executor::partition_router::in_scope_storage_ids(table)
            .into_iter()
            .next()
    }

    /// Hot-path counterpart that reuses a pre-cached index handle.
    /// Skips the per-call `index_cache` HashMap + name normalize path
    /// that `with_index_read` would otherwise pay. Used by INLJ to
    /// amortise handle resolution across the entire outer loop.
    pub(crate) fn range_index_bounds_with_handle(
        &self,
        index_handle: &IndexHandle,
        index: &IndexSchema,
        low: Option<IndexBound>,
        high: Option<IndexBound>,
    ) -> Result<Vec<RowId>, QueryError> {
        let (low, low_unsat) = resolve_index_bound(index, low)?;
        let (high, high_unsat) = resolve_index_bound(index, high)?;
        if low_unsat || high_unsat {
            return Ok(Vec::new());
        }
        let index_file = index_handle.read();
        index_file.range(low.as_ref(), high.as_ref(), !index.unique)
    }

    pub fn range_index_bounds_visible(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        low: Option<IndexBound>,
        high: Option<IndexBound>,
        txn: TxnView,
    ) -> Result<Vec<RowId>, QueryError> {
        self.check_index_read_view(table, index, txn)?;
        let (low, low_unsat) = resolve_index_bound(index, low)?;
        let (high, high_unsat) = resolve_index_bound(index, high)?;
        if low_unsat || high_unsat {
            return Ok(Vec::new());
        }
        let mut ids = self.range_index_bounds(table, index, low.clone(), high.clone())?;
        let before = ids.len();
        self.add_bounded_index_visibility_repair_ids(
            table,
            index,
            txn,
            low.as_ref(),
            high.as_ref(),
            &mut ids,
        )?;
        if ids.len() != before {
            dedup_row_ids(&mut ids);
        }
        Ok(ids)
    }



    fn key_index_entries_with_bound<const PREFIX: bool>(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        key: IndexKey,
    ) -> Result<(Vec<(IndexKey, RowId)>, u64), QueryError> {
        let (key, unsatisfiable) = resolve_index_key(index, key)?;
        if unsatisfiable {
            return Ok((Vec::new(), 0));
        }
        if table.is_partitioned() {
            let mut out: Vec<(IndexKey, RowId)> = Vec::new();
            let mut page_max_trx_id = 0;
            for sid in crate::engine::executor::partition_router::in_scope_storage_ids(table) {
                let (entries, bound) = self.with_index_read(sid, &index.name, |index_file| {
                    if PREFIX {
                        index_file.prefix_entries_with_bound(&key, !index.unique)
                    } else {
                        index_file.get_entries_with_bound(&key, !index.unique)
                    }
                })?;
                out.extend(entries);
                page_max_trx_id = page_max_trx_id.max(bound);
            }
            return Ok((out, page_max_trx_id));
        }
        self.with_index_read(table.id, &index.name, |index_file| {
            if PREFIX {
                index_file.prefix_entries_with_bound(&key, !index.unique)
            } else {
                index_file.get_entries_with_bound(&key, !index.unique)
            }
        })
    }

    fn filter_and_dedup_repaired_index_entries(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        txn: TxnView,
        entries: &mut Vec<(IndexKey, RowId)>,
    ) -> Result<(), QueryError> {
        // A changed row can have its new physical key AND an old repair key
        // in this range. Deduplicating RowId first arbitrarily kept the new
        // key, which the later MVCC check rejected, losing the visible row.
        let mut visible = self.filter_visible_index_entries_mask(table, index, entries, txn)?
            .into_iter();
        entries.retain(|_| visible.next().expect("one visibility result per entry"));
        entries.sort_by_key(|entry| entry.1);
        entries.dedup_by_key(|entry| entry.1);
        Ok(())
    }

    fn visible_key_index_entries_with_bound<const PREFIX: bool>(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        key: IndexKey,
        txn: TxnView,
    ) -> Result<(Vec<(IndexKey, RowId)>, Option<u64>), QueryError> {
        self.check_index_read_view(table, index, txn)?;
        let (key, unsatisfiable) = resolve_index_key(index, key)?;
        if unsatisfiable {
            return Ok((Vec::new(), Some(0)));
        }
        let repairs_before = self.has_pending_index_repairs();
        let (mut entries, page_max_trx_id) =
            self.key_index_entries_with_bound::<PREFIX>(table, index, key.clone())?;
        let before = entries.len();
        if PREFIX {
            self.add_index_visibility_repair_entries(
                table,
                index,
                txn,
                &mut entries,
                |candidate| index_key_starts_with(candidate, &key),
            )?;
        } else {
            self.add_exact_index_visibility_repair_entries(table, index, txn, &key, &mut entries)?;
        }
        let repairs_after = self.has_pending_index_repairs();
        if entries.len() != before
            || ((repairs_before || repairs_after) && index.has_multi_valued_parts())
        {
            self.filter_and_dedup_repaired_index_entries(table, index, txn, &mut entries)?;
        }
        let bound = (!repairs_before && !repairs_after && entries.len() == before)
            .then_some(page_max_trx_id);
        Ok((entries, bound))
    }

    pub fn lookup_index_entries_visible(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        key: IndexKey,
        txn: TxnView,
    ) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
        self.check_index_read_view(table, index, txn)?;
        self.lookup_index_entries_visible_with_bound(table, index, key, txn)
            .map(|(entries, _page_max_trx_id)| entries)
    }

    pub(crate) fn lookup_index_entries_visible_with_bound(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        key: IndexKey,
        txn: TxnView,
    ) -> Result<(Vec<(IndexKey, RowId)>, Option<u64>), QueryError> {
        self.visible_key_index_entries_with_bound::<false>(table, index, key, txn)
    }



    pub fn prefix_index_entries_visible(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        key: IndexKey,
        txn: TxnView,
    ) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
        self.check_index_read_view(table, index, txn)?;
        self.prefix_index_entries_visible_with_bound(table, index, key, txn)
            .map(|(entries, _page_max_trx_id)| entries)
    }

    pub(crate) fn prefix_index_entries_visible_with_bound(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        key: IndexKey,
        txn: TxnView,
    ) -> Result<(Vec<(IndexKey, RowId)>, Option<u64>), QueryError> {
        self.visible_key_index_entries_with_bound::<true>(table, index, key, txn)
    }

    /// Snapshot the MVCC compensation entries for one composite-index
    /// prefix. A streaming prefix cursor merges this small ordered side set
    /// with the persisted leaf walk instead of materialising the complete
    /// prefix merely because an in-flight DELETE/UPDATE temporarily removed
    /// an old key from the B-tree.
    pub(crate) fn prefix_index_visibility_repair_entries(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        prefix: &IndexKey,
        txn: TxnView,
    ) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
        self.check_index_read_view(table, index, txn)?;
        debug_assert!(!txn.historical);
        let mut entries = Vec::new();
        self.add_index_visibility_repair_entries(table, index, txn, &mut entries, |candidate| {
            index_key_starts_with(candidate, prefix)
        })?;
        entries.sort_unstable_by(|left, right| {
            left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1))
        });
        entries.dedup();
        Ok(entries)
    }

    /// Snapshot MVCC compensation entries inside one ordered range. Native
    /// range cursors merge this usually tiny side stream with the persisted
    /// B-tree walk, avoiding whole-range materialization whenever concurrent
    /// DML temporarily retires an old index key.
    pub(crate) fn range_index_visibility_repair_entries(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        low: Option<IndexBound>,
        high: Option<IndexBound>,
        txn: TxnView,
    ) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
        self.check_index_read_view(table, index, txn)?;
        debug_assert!(!txn.historical);
        let (low, low_unsat) = resolve_index_bound(index, low)?;
        let (high, high_unsat) = resolve_index_bound(index, high)?;
        if low_unsat || high_unsat {
            return Ok(Vec::new());
        }
        let mut entries = Vec::new();
        self.add_bounded_index_visibility_repair_entries(
            table,
            index,
            txn,
            low.as_ref(),
            high.as_ref(),
            &mut entries,
        )?;
        entries.sort_unstable_by(|left, right| {
            left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1))
        });
        entries.dedup();
        Ok(entries)
    }

    pub fn range_index_bounds_entries(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        low: Option<IndexBound>,
        high: Option<IndexBound>,
    ) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
        let (low, low_unsat) = resolve_index_bound(index, low)?;
        let (high, high_unsat) = resolve_index_bound(index, high)?;
        if low_unsat || high_unsat {
            return Ok(Vec::new());
        }
        if table.is_partitioned() {
            let mut out = Vec::new();
            for storage_id in crate::engine::executor::partition_router::in_scope_storage_ids(table)
            {
                let mut entries = self.with_index_read(storage_id, &index.name, |index_file| {
                    index_file.range_entries(low.as_ref(), high.as_ref(), !index.unique)
                })?;
                out.append(&mut entries);
            }
            return Ok(out);
        }
        // Concurrent DML keeps the global visibility-repair gate active, so
        // vector and scalar range scans frequently need this materialized
        // base set before merging repair entries.  The old `range_entries`
        // path decoded every record on each touched leaf, even when a bounded
        // probe selected only 100 slots.  Drain the raw range cursor instead:
        // it preserves key/RowId order and bounds while decoding only selected
        // slots.  The returned Vec has the same lifetime/accounting contract
        // as before; the cursor owns only a short-lived cached index handle.
        self.range_index_entries_cursor(table, index, low, high)?
            .collect_remaining()
    }

    /// Hot-path variant of `range_index_bounds_entries_visible` that
    /// reuses a pre-cached index handle. The visibility-repair branch
    /// is rare (only set during in-flight DDL/DML index repair) and
    /// still falls back to the `txns_read()` lock via
    /// `add_index_visibility_repair_entries`. The common case — no
    /// repairs pending — is now a single `index_handle.read()` per
    /// outer row.
    pub(crate) fn range_index_bounds_entries_visible_with_handle(
        &self,
        index_handle: &IndexHandle,
        table: &TableSchema,
        index: &IndexSchema,
        low: Option<IndexBound>,
        high: Option<IndexBound>,
        txn: TxnView,
    ) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
        self.check_index_read_view(table, index, txn)?;
        let (low, low_unsat) = resolve_index_bound(index, low)?;
        let (high, high_unsat) = resolve_index_bound(index, high)?;
        if low_unsat || high_unsat {
            return Ok(Vec::new());
        }
        self.retry_if_rollback_overlapped(table, index, || {
            let mut entries = {
                let index_file = index_handle.read();
                index_file.range_entries(low.as_ref(), high.as_ref(), !index.unique)?
            };
            let before = entries.len();
            self.add_bounded_index_visibility_repair_entries(
                table,
                index,
                txn,
                low.as_ref(),
                high.as_ref(),
                &mut entries,
            )?;
            if entries.len() != before {
                self.filter_and_dedup_repaired_index_entries(table, index, txn, &mut entries)?;
            }
            Ok(entries)
        })
    }

    /// Batch equality-prefix lookup used by batched INLJ. The caller
    /// supplies de-duplicated keys in index order; this reuses one index
    /// read guard across all probes and keeps visibility-repair handling
    /// equivalent to the scalar lookup APIs. Full-length keys use exact
    /// lookup; shorter keys use prefix lookup, matching the scalar INLJ
    /// bound construction for composite indexes.
    pub(crate) fn lookup_index_entries_many_visible_with_handle(
        &self,
        index_handle: &IndexHandle,
        table: &TableSchema,
        index: &IndexSchema,
        keys: &[IndexKey],
        txn: TxnView,
    ) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
        self.check_index_read_view(table, index, txn)?;
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        // Batched-INLJ probe keys are concrete outer-row values, never
        // deferred params, so the unsatisfiable flag is always false here.
        let mut keys = keys
            .iter()
            .cloned()
            .map(|key| resolve_index_key(index, key).map(|(k, _)| k))
            .collect::<Result<Vec<_>, _>>()?;
        keys.sort_unstable();
        keys.dedup();
        let exact = keys
            .first()
            .is_some_and(|key| key.0.len() >= index.columns.len());
        let mut entries = {
            let index_file = index_handle.read();
            index_file.lookup_entries_many_sorted(&keys, exact, !index.unique)?
        };
        let before = entries.len();
        if self.has_pending_index_repairs() {
            self.add_index_visibility_repair_entries(
                table,
                index,
                txn,
                &mut entries,
                |candidate| {
                    keys.iter().any(|key| {
                        if exact {
                            candidate == key
                        } else {
                            index_key_starts_with(candidate, key)
                        }
                    })
                },
            )?;
        }
        if entries.len() != before {
            self.filter_and_dedup_repaired_index_entries(table, index, txn, &mut entries)?;
        }
        Ok(entries)
    }

    /// Anti/Semi short-circuit batched probe for callers that already own
    /// sorted, de-duplicated, concrete keys. For each input key, returns
    /// whether the index has at least one visible (MVCC-committed) row.
    ///
    /// Bulk DDL validation builds exactly that representation while it
    /// streams the child heap. Keeping this entry point avoids cloning and
    /// sorting a second million-key vector before the B-tree merge probe.
    /// The visibility and pending-repair handling remains identical to the
    /// regular batched INLJ path above.
    pub(crate) fn keys_with_visible_match_sorted_with_handle(
        &self,
        index_handle: &IndexHandle,
        table: &TableSchema,
        index: &IndexSchema,
        keys: &[IndexKey],
        txn: TxnView,
    ) -> Result<Vec<bool>, QueryError> {
        self.check_index_read_view(table, index, txn)?;
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        debug_assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
        let exact = keys
            .first()
            .is_some_and(|key| key.0.len() >= index.columns.len());
        // Pending-index-repair fallback: degraded path. Reuse the full
        // materialised lookup (which already merges repair entries in)
        // and reduce to "key has any visible row" by row-id presence.
        if self.has_pending_index_repairs() {
            let entries = self.lookup_index_entries_many_visible_with_handle(
                index_handle,
                table,
                index,
                &keys,
                txn,
            )?;
            let heap = self.cached_heap_handle(table.id)?;
            let mask = self.filter_visible_index_entries_mask_with_handle(
                &heap, table, index, &entries, txn,
            )?;
            let mut matched = vec![false; keys.len()];
            let prefix_len = keys.first().map(|k| k.0.len()).unwrap_or(0);
            for ((entry_key, _row_id), is_visible) in entries.iter().zip(mask.iter()) {
                if !*is_visible {
                    continue;
                }
                if entry_key.0.len() < prefix_len {
                    continue;
                }
                if let Ok(idx) =
                    keys.binary_search_by(|key| key.0.as_slice().cmp(&entry_key.0[..prefix_len]))
                {
                    matched[idx] = true;
                }
            }
            return Ok(matched);
        }
        // Prefer the leaf-level PAGE_MAX_TRX_ID proof before consulting a
        // RowId.  Static/read-mostly indexes then answer existence entirely
        // from their B-tree leaves; pages above the retained watermark keep
        // using the exact heap/undo visibility oracle below.
        let heap = self.cached_heap_handle(table.id)?;
        let table_ref = table;
        let mut page_all_visible =
            |page_max_trx_id: u64| -> bool { self.index_page_all_visible(page_max_trx_id, txn) };
        let mut accept = |key: &IndexKey, row_id: RowId| -> Result<bool, QueryError> {
            if let Some(visible) =
                self.is_row_id_visible_buffered_in_heap(&heap.read(), row_id, txn)?
            {
                return Ok(visible);
            }
            self.is_index_entry_version_visible(table_ref, index, key, row_id, txn)
        };
        let index_file = index_handle.read();
        let matched = index_file.keys_with_visible_match_routed(
            &keys,
            exact,
            &mut page_all_visible,
            &mut accept,
        )?;
        drop(index_file);
        Ok(matched)
    }

    /// Sequentially walk every leaf of an inner index, calling
    /// `visit(&entry.key.0[..prefix_len])` on each entry. Designed for INLJ probe
    /// bloom construction: the caller takes the read lock from `index_handle` and we forward to [`IndexFile::for_each_leaf_key_prefix`].
    ///
    /// Parameters and return semantics: see [`IndexFile::for_each_leaf_key_prefix`] —
    /// `max_pages` is a hard cap on leaf pages; on hitting it `Ok(false)` is returned and the caller must discard the half-built bloom.
    /// This API applies no visibility filtering, which suits bloom (it only needs no false
    /// negatives); visibility-requiring callers should keep using [`Self::lookup_index_entries_many_visible_with_handle`].
    pub(crate) fn for_each_index_leaf_key_prefix_with_handle<F>(
        &self,
        index_handle: &IndexHandle,
        prefix_len: usize,
        max_pages: usize,
        visit: F,
    ) -> Result<bool, QueryError>
    where
        F: FnMut(&[KeyPart]),
    {
        let index_file = index_handle.read();
        index_file.for_each_leaf_key_prefix(prefix_len, max_pages, visit)
    }

    /// Keyed variant of [`Self::lookup_index_entries_many_visible_with_handle`]
    /// for the non-covering batched INLJ. `keys` must already be sorted
    /// and distinct. Returns `(probe-key index, row id)` so the caller can
    /// map a match straight back to its outer rows without re-deriving or
    /// hashing the entry's `IndexKey` — no per-entry `IndexKey` is kept.
    pub(crate) fn lookup_index_entries_many_keyed_with_handle(
        &self,
        index_handle: &IndexHandle,
        table: &TableSchema,
        index: &IndexSchema,
        keys: &[IndexKey],
        txn: TxnView,
    ) -> Result<Vec<(u32, RowId)>, QueryError> {
        self.check_index_read_view(table, index, txn)?;
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        // Resolving deferred params is a positional, order-preserving map
        // (and a no-op for the concrete keys the batched INLJ builds), so
        // resolved key positions stay aligned with the caller's array.
        let resolved = keys
            .iter()
            .cloned()
            .map(|key| resolve_index_key(index, key).map(|(k, _)| k))
            .collect::<Result<Vec<_>, _>>()?;
        let exact = resolved
            .first()
            .is_some_and(|key| key.0.len() >= index.columns.len());
        if self.has_pending_index_repairs() {
            // Rare in-flight-repair path: reuse the materialised lookup
            // and map each entry back onto its probe-key index.
            let entries = self.lookup_index_entries_many_visible_with_handle(
                index_handle,
                table,
                index,
                keys,
                txn,
            )?;
            return Ok(map_index_entries_to_key_indices(keys, &entries));
        }
        let index_file = index_handle.read();
        index_file.lookup_entries_many_sorted_keyed(&resolved, exact, !index.unique)
    }

    /// Bounded keyed lookup used by optional secondary-index intersections.
    /// `None` means that the lookup exceeded `max_entries`; callers must fall
    /// back to their correctness-preserving candidate path.
    pub(crate) fn lookup_index_entries_many_keyed_bounded_with_handle(
        &self,
        index_handle: &IndexHandle,
        table: &TableSchema,
        index: &IndexSchema,
        keys: &[IndexKey],
        txn: TxnView,
        max_entries: usize,
    ) -> Result<Option<Vec<(u32, RowId)>>, QueryError> {
        self.check_index_read_view(table, index, txn)?;
        if keys.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let resolved = keys
            .iter()
            .cloned()
            .map(|key| resolve_index_key(index, key).map(|(k, _)| k))
            .collect::<Result<Vec<_>, _>>()?;
        let exact = resolved
            .first()
            .is_some_and(|key| key.0.len() >= index.columns.len());
        if self.has_pending_index_repairs() {
            let entries = self.lookup_index_entries_many_visible_with_handle(
                index_handle,
                table,
                index,
                keys,
                txn,
            )?;
            if entries.len() > max_entries {
                return Ok(None);
            }
            return Ok(Some(map_index_entries_to_key_indices(keys, &entries)));
        }
        let index_file = index_handle.read();
        index_file.lookup_entries_many_sorted_keyed_bounded(
            &resolved,
            exact,
            !index.unique,
            max_entries,
        )
    }

    /// Single-pass COUNT over an index range, never materialising the
    /// full `(key, row_id)` set. Walks leaves once via the next-leaf
    /// chain (so no per-chunk tree re-descent), then per leaf:
    ///   * applies the in-bounds + dedup-by-row-id filter
    ///   * batches the visibility check inside one heap read-guard
    ///     acquisition with a sliding-window heap prefetch
    ///   * if `count_col_pos` is `Some`, skips entries whose key part
    ///     at that index slot is `KeyPart::Null` — the streaming
    ///     equivalent of `COUNT(col)` over a covering index.
    /// Pending index-visibility-repair entries (rare, only set during
    /// in-flight DDL/DML index repair) are added at the end behind a
    /// `HashSet` so they don't double-count with the leaf-walk output.
    pub fn count_index_entries_visible(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        low: Option<IndexBound>,
        high: Option<IndexBound>,
        txn: TxnView,
        count_col_pos: Option<usize>,
    ) -> Result<i64, QueryError> {
        self.check_index_read_view(table, index, txn)?;
        let (low, low_unsat) = resolve_index_bound(index, low)?;
        let (high, high_unsat) = resolve_index_bound(index, high)?;
        if low_unsat || high_unsat {
            return Ok(0);
        }
        let unique = index.unique;

        // Repair entries (typically empty; cheap when none pending).
        let mut repair_entries: Vec<(IndexKey, RowId)> = Vec::new();
        if self.has_pending_index_repairs_for(table.id, &index.name) {
            self.add_bounded_index_visibility_repair_entries(
                table,
                index,
                txn,
                low.as_ref(),
                high.as_ref(),
                &mut repair_entries,
            )?;
        }

        let mut count: i64 = 0;
        // We only need the dedup HashSet when the leaf walk could
        // overlap with repair entries (extremely rare). Otherwise the
        // single-pass `last_row_id` carry across leaves is sufficient.
        let track_seen = !repair_entries.is_empty();
        let mut counted: std::collections::HashSet<RowId> = std::collections::HashSet::new();
        let mut last_row_id: Option<RowId> = None;
        let mut entry_buf: Vec<(IndexKey, RowId)> = Vec::with_capacity(512);

        self.with_index_read(table.id, &index.name, |index_file| {
            let mut leaf_id = match low.as_ref() {
                Some(b) => index_file.find_leaf_for_key_lower_bound(&b.key)?,
                None => index_file.leftmost_leaf()?,
            };
            loop {
                let leaf_page = index_file.read_page(leaf_id)?;
                let leaf = match leaf_page.as_ref() {
                    IndexPage::Leaf(l) => l,
                    _ => {
                        return Err(error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "corrupt index: expected leaf page",
                        ));
                    }
                };
                if let Some(next) = leaf.next_leaf {
                    index_file.prefetch_next_leaves(next);
                }

                let mut past_high = false;
                entry_buf.clear();
                for entry in &leaf.entries {
                    if index_key_past_high_bound(&entry.key, high.as_ref()) {
                        past_high = true;
                        break;
                    }
                    if !index_key_in_bounds(&entry.key, low.as_ref(), high.as_ref()) {
                        continue;
                    }
                    if !unique && last_row_id == Some(entry.row_id) {
                        continue;
                    }
                    last_row_id = Some(entry.row_id);
                    entry_buf.push((entry.key.clone(), entry.row_id));
                }

                if !entry_buf.is_empty() {
                    // InnoDB-style PAGE_MAX_TRX_ID shortcut: every entry on this
                    // leaf is visible if the leaf's writer upper-bound is
                    // resolved and repairs are drained. Skips the whole
                    // per-entry heap-header probe for covering COUNT scans.
                    let page_bound = leaf.page_max_trx_id();
                    let mask = self
                        .try_index_page_max_trx_shortcut_for(
                            table.id,
                            &index.name,
                            page_bound,
                            txn,
                            entry_buf.len(),
                        )
                        .map(Ok::<_, QueryError>)
                        .unwrap_or_else(|| {
                            self.filter_visible_index_entries_mask(table, index, &entry_buf, txn)
                        })?;
                    for ((key, row_id), visible) in entry_buf.iter().zip(mask.iter()) {
                        if !visible {
                            continue;
                        }
                        if let Some(pos) = count_col_pos {
                            if matches!(key.0.get(pos), Some(KeyPart::Null)) {
                                continue;
                            }
                        }
                        if track_seen && !counted.insert(*row_id) {
                            continue;
                        }
                        count += 1;
                    }
                }

                if past_high {
                    break;
                }
                let Some(next) = leaf.next_leaf else {
                    break;
                };
                leaf_id = next;
            }
            Ok(())
        })?;

        if !repair_entries.is_empty() {
            let mask =
                self.filter_visible_index_entries_mask(table, index, &repair_entries, txn)?;
            for ((key, row_id), visible) in repair_entries.iter().zip(mask.iter()) {
                if !visible {
                    continue;
                }
                if let Some(pos) = count_col_pos {
                    if matches!(key.0.get(pos), Some(KeyPart::Null)) {
                        continue;
                    }
                }
                if !counted.insert(*row_id) {
                    continue;
                }
                count += 1;
            }
        }

        Ok(count)
    }

    /// Count distinct, non-NULL values of the complete leading index
    /// column in one ordered leaf walk.
    ///
    /// The planner only selects this path when the aggregate argument is
    /// the non-prefix first column of `index`. B-tree order then makes all
    /// equal SQL keys contiguous (including collation-aware string keys),
    /// so normal execution retains one `KeyPart` instead of a
    /// cardinality-sized hash set. Visibility is resolved per leaf using
    /// `page_max_trx_id`, with the heap-header path as the correctness
    /// fallback. Pending repair keys are the only hash-backed state; that
    /// rare set is maintained by the write/repair subsystem rather than
    /// growing with the scanned relation.
    pub fn count_distinct_index_prefix_visible(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        low: Option<IndexBound>,
        high: Option<IndexBound>,
        txn: TxnView,
    ) -> Result<i64, QueryError> {
        self.check_index_read_view(table, index, txn)?;
        let (low, low_unsat) = resolve_index_bound(index, low)?;
        let (high, high_unsat) = resolve_index_bound(index, high)?;
        if low_unsat || high_unsat {
            return Ok(0);
        }

        // Integer-shaped leading keys have a raw leaf decoder that reads
        // only `[value, RowId]` from each slot. Use it before the generic
        // KeyPart path so common INT/BIGINT/date/time DISTINCT scans avoid
        // reconstructing an IndexKey (and its SmallVec) for every row.
        // Pending repairs stay on the generic path below because it merges
        // repaired full keys with the physical ordered stream.
        let lossless_i64 = index
            .columns
            .first()
            .and_then(|name| {
                table
                    .columns
                    .iter()
                    .find(|column| column.name.eq_ignore_ascii_case(name))
            })
            .is_some_and(|column| {
                use crate::types::ColumnType;
                match column.data_type.column_type() {
                    ColumnType::Tiny
                    | ColumnType::Short
                    | ColumnType::MediumInt
                    | ColumnType::Long
                    | ColumnType::Date
                    | ColumnType::Time
                    | ColumnType::DateTime
                    | ColumnType::Timestamp
                    | ColumnType::Year => true,
                    ColumnType::LongLong => !column.unsigned,
                    _ => false,
                }
            });
        if lossless_i64 && !self.has_pending_index_repairs_for(table.id, &index.name) {
            let leaf_ids = self.range_index_leaf_ids(table, index, low.clone(), high.clone())?;
            let heap = self.cached_heap_handle(table.id)?;
            let mut last_visible_value: Option<i64> = None;
            let mut count = 0i64;
            self.visit_index_lossless_i64_leaf_entry_chunks_with_bound(
                table,
                index,
                &leaf_ids,
                None,
                low,
                high,
                0,
                |entries, page_max_trx_id| {
                    let visible = self
                        .try_index_page_max_trx_shortcut_for(
                            table.id,
                            &index.name,
                            page_max_trx_id,
                            txn,
                            entries.len(),
                        )
                        .map(Ok::<_, QueryError>)
                        .unwrap_or_else(|| {
                            self.filter_visible_index_i64_entries_mask_with_handle(
                                &heap, table, index, entries, 0, txn,
                            )
                        })?;
                    for ((value, _row_id), is_visible) in entries.iter().zip(visible.into_iter()) {
                        if !is_visible {
                            continue;
                        }
                        let Some(value) = *value else {
                            continue;
                        };
                        if last_visible_value == Some(value) {
                            continue;
                        }
                        last_visible_value = Some(value);
                        count += 1;
                    }
                    Ok(())
                },
            )?;
            return Ok(count);
        }

        // A repair can contribute a key not present in the physical leaf
        // stream. Keep only distinct, visible repair prefixes, then remove
        // each one observed during the ordered walk. This preserves O(1)
        // state in the common no-repair case and avoids a table-sized set
        // even while a repair is pending.
        let mut repair_keys: std::collections::HashSet<KeyPart> = std::collections::HashSet::new();
        if self.has_pending_index_repairs_for(table.id, &index.name) {
            let mut repair_entries: Vec<(IndexKey, RowId)> = Vec::new();
            self.add_bounded_index_visibility_repair_entries(
                table,
                index,
                txn,
                low.as_ref(),
                high.as_ref(),
                &mut repair_entries,
            )?;
            if !repair_entries.is_empty() {
                let visible =
                    self.filter_visible_index_entries_mask(table, index, &repair_entries, txn)?;
                for ((key, _row_id), is_visible) in repair_entries.iter().zip(visible.into_iter()) {
                    if !is_visible {
                        continue;
                    }
                    if let Some(part) = key.0.first() {
                        if !matches!(part, KeyPart::Null) {
                            repair_keys.insert(part.clone());
                        }
                    }
                }
            }
        }

        let heap = self.cached_heap_handle(table.id)?;
        let mut cursor =
            self.range_index_entries_cursor(table, index, low.clone(), high.clone())?;
        let mut last_visible_key: Option<KeyPart> = None;
        let mut count = 0i64;
        cursor.visit_entries_with_bound(|entries, page_max_trx_id| {
            let visible = self
                .try_index_page_max_trx_shortcut_for(
                    table.id,
                    &index.name,
                    page_max_trx_id,
                    txn,
                    entries.len(),
                )
                .map(Ok::<_, QueryError>)
                .unwrap_or_else(|| {
                    self.filter_visible_index_entries_refs_mask_with_handle(
                        &heap, table, index, entries, txn,
                    )
                })?;
            for ((key, _row_id), is_visible) in entries.iter().zip(visible.into_iter()) {
                if !is_visible {
                    continue;
                }
                let Some(part) = key.0.first() else {
                    continue;
                };
                if matches!(part, KeyPart::Null) {
                    continue;
                }
                if last_visible_key.as_ref().is_some_and(|last| last == part) {
                    continue;
                }
                repair_keys.remove(part);
                last_visible_key = Some(part.clone());
                count += 1;
            }
            Ok(())
        })?;
        Ok(count + repair_keys.len() as i64)
    }

    pub fn range_index_bounds_entries_visible(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        low: Option<IndexBound>,
        high: Option<IndexBound>,
        txn: TxnView,
    ) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
        self.check_index_read_view(table, index, txn)?;
        let (low, low_unsat) = resolve_index_bound(index, low)?;
        let (high, high_unsat) = resolve_index_bound(index, high)?;
        if low_unsat || high_unsat {
            return Ok(Vec::new());
        }
        let entries = self.retry_if_rollback_overlapped(table, index, || {
            let mut entries =
                self.range_index_bounds_entries(table, index, low.clone(), high.clone())?;
            let before = entries.len();
            self.add_bounded_index_visibility_repair_entries(
                table,
                index,
                txn,
                low.as_ref(),
                high.as_ref(),
                &mut entries,
            )?;
            if entries.len() != before {
                self.filter_and_dedup_repaired_index_entries(table, index, txn, &mut entries)?;
            }
            Ok(entries)
        })?;
        Ok(entries)
    }

    pub fn range_index_entries_chunk(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        low: Option<IndexBound>,
        high: Option<IndexBound>,
        resume_after: Option<(IndexKey, RowId)>,
        chunk_size: usize,
    ) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
        let (low, low_unsat) = resolve_index_bound(index, low)?;
        let (high, high_unsat) = resolve_index_bound(index, high)?;
        if low_unsat || high_unsat {
            return Ok(Vec::new());
        }
        self.with_index_read(table.id, &index.name, |index_file| {
            index_file.range_entries_chunk(
                low.as_ref(),
                high.as_ref(),
                resume_after.as_ref(),
                chunk_size,
            )
        })
    }

    /// Forward-only cursor for range scans. The cursor owns a cached
    /// index handle and keeps the current leaf/slot position so chunked
    /// consumers can keep walking the leaf chain without repeating the
    /// root descent on every refill.
    pub(crate) fn range_index_entries_cursor(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        low: Option<IndexBound>,
        high: Option<IndexBound>,
    ) -> Result<IndexRangeCursor, QueryError> {
        let (low, low_unsat) = resolve_index_bound(index, low)?;
        let (high, high_unsat) = resolve_index_bound(index, high)?;
        let unique = index.unique;
        // Partitioned tables keep one physical B-tree per partition; a
        // forward cursor walks a single partition's local index, chosen
        // by the active prune context (the executor fans out per
        // partition). A partitioned table with no in-scope partition
        // yields an empty cursor.
        let Some(storage_id) = self.index_cursor_storage_id(table) else {
            let handle = self.cached_index_handle(table.id, &index.name)?;
            return Ok(IndexRangeCursor::empty(handle, unique));
        };
        let handle = self.cached_index_handle(storage_id, &index.name)?;
        if low_unsat || high_unsat {
            return Ok(IndexRangeCursor::empty(handle, unique));
        }
        IndexRangeCursor::new(handle, unique, low, high)
    }

    /// Ordered cursor for a reader's snapshot. Writers physically delete a
    /// retired key before they commit, so a scan that reaches that position
    /// afterwards merges the snapshot-visible retired keys back in, and the
    /// moved rows' newer keys are suppressed. Historical reads keep their
    /// own temporal-history merge.
    pub(crate) fn range_index_snapshot_cursor(
        self: &Arc<Self>,
        table: &TableSchema,
        index: &IndexSchema,
        low: Option<IndexBound>,
        high: Option<IndexBound>,
        txn: TxnView,
    ) -> Result<IndexRangeCursor, QueryError> {
        let cursor = self.range_index_entries_cursor(table, index, low, high)?;
        Ok(match self.snapshot_repair_feed(table, index, txn) {
            Some(feed) => cursor.with_repair_feed(feed),
            None => cursor,
        })
    }

    /// Reverse counterpart of [`Self::range_index_snapshot_cursor`].
    pub(crate) fn range_index_snapshot_reverse_cursor(
        self: &Arc<Self>,
        table: &TableSchema,
        index: &IndexSchema,
        low: Option<IndexBound>,
        high: Option<IndexBound>,
        txn: TxnView,
    ) -> Result<IndexRangeReverseCursor, QueryError> {
        let cursor = self.range_index_entries_reverse_cursor(table, index, low, high)?;
        Ok(match self.snapshot_repair_feed(table, index, txn) {
            Some(feed) => cursor.with_repair_feed(feed),
            None => cursor,
        })
    }

    fn snapshot_repair_feed(
        self: &Arc<Self>,
        table: &TableSchema,
        index: &IndexSchema,
        txn: TxnView,
    ) -> Option<Box<dyn IndexRepairFeed>> {
        (!txn.historical).then(|| {
            Box::new(SnapshotIndexRepairFeed {
                db: Arc::clone(self),
                table_id: table.id,
                index_name: index.name.clone(),
                txn,
                seen_version: None,
            }) as Box<dyn IndexRepairFeed>
        })
    }

    /// Reverse counterpart of `range_index_entries_cursor`: one root
    /// descent at construction plus one `prev_leaf` descent per leaf
    /// transition, instead of a full descent + resume-key rescan per
    /// chunk.
    pub(crate) fn range_index_entries_reverse_cursor(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        low: Option<IndexBound>,
        high: Option<IndexBound>,
    ) -> Result<IndexRangeReverseCursor, QueryError> {
        let (low, low_unsat) = resolve_index_bound(index, low)?;
        let (high, high_unsat) = resolve_index_bound(index, high)?;
        let unique = index.unique;
        // Same per-partition resolution as the forward cursor.
        let Some(storage_id) = self.index_cursor_storage_id(table) else {
            let handle = self.cached_index_handle(table.id, &index.name)?;
            return Ok(IndexRangeReverseCursor::empty(handle, unique));
        };
        let handle = self.cached_index_handle(storage_id, &index.name)?;
        if low_unsat || high_unsat {
            return Ok(IndexRangeReverseCursor::empty(handle, unique));
        }
        IndexRangeReverseCursor::new(handle, unique, low, high)
    }

    /// Keep the returned range alive until every borrowed leaf slice has been
    /// consumed. Asynchronous workers must retain its shared lease explicitly.
    pub fn range_index_leaf_ids(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        low: Option<IndexBound>,
        high: Option<IndexBound>,
    ) -> Result<IndexLeafRange, QueryError> {
        let (low, low_unsat) = resolve_index_bound(index, low)?;
        let (high, high_unsat) = resolve_index_bound(index, high)?;
        if low_unsat || high_unsat {
            return Ok(IndexLeafRange {
                ids: Vec::new(),
                lease: None,
            });
        }
        let handle = self.index_handle(table.id, &index.name)?;
        let handle_guard = handle.read_arc();
        let structure = handle_guard.structure_lock.read_arc();
        let ids = handle_guard.range_leaf_ids_locked(low.as_ref(), high.as_ref())?;
        Ok(IndexLeafRange {
            ids,
            lease: Some(Arc::new(IndexRangeLease {
                _structure: structure,
                _handle: Some(handle_guard),
            })),
        })
    }

    /// Leaf that owns `(key, row_id)`. Callers hold the scan's
    /// [`IndexRangeLease`], which freezes the topology, so the answer stays
    /// valid for that scan; a recursive read never queues behind a writer
    /// that is itself waiting for the lease.
    pub(crate) fn index_leaf_for_entry(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        key: &IndexKey,
        row_id: RowId,
    ) -> Result<u64, QueryError> {
        let handle = self.index_handle(table.id, &index.name)?;
        let file = handle.read_recursive();
        let _structure = file.structure_lock.read_recursive();
        file.leaf_for_entry(key, row_id)
    }

    /// Exact ordinal selection from snapshot-visible covering-index entries,
    /// using cached all-visible spans or a bounded per-leaf MVCC proof.
    /// `None` requests the repair-aware streaming fallback. Callers commit
    /// volatile state only after success, never after a declined attempt.
    pub(crate) fn select_all_visible_index_range_entry_by_ordinal<S>(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        low: Option<IndexBound>,
        high: Option<IndexBound>,
        txn: TxnView,
        choose_ordinal: S,
    ) -> Result<Option<(Option<(IndexKey, RowId)>, usize)>, QueryError>
    where
        S: FnOnce(usize) -> usize,
    {
        self.check_index_read_view(table, index, txn)?;
        self.select_all_visible_index_range_entry_by_filtered_ordinal(
            table,
            index,
            low,
            high,
            &[],
            txn,
            choose_ordinal,
        )
    }

    pub(crate) fn select_all_visible_index_range_entry_by_filtered_ordinal<S>(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        low: Option<IndexBound>,
        high: Option<IndexBound>,
        filter_terms: &[(usize, crate::storage::KeyPart)],
        txn: TxnView,
        choose_ordinal: S,
    ) -> Result<Option<(Option<(IndexKey, RowId)>, usize)>, QueryError>
    where
        S: FnOnce(usize) -> usize,
    {
        self.check_index_read_view(table, index, txn)?;
        let (low, low_unsat) = resolve_index_bound(index, low)?;
        let (high, high_unsat) = resolve_index_bound(index, high)?;
        if low_unsat || high_unsat {
            return Ok(Some((None, 0)));
        }
        let mut choose = Some(choose_ordinal);
        if !self.has_pending_index_repairs_for(table.id, &index.name) {
            let result = self.with_index_read(table.id, &index.name, |index_file| {
                if filter_terms.is_empty() {
                    return index_file.select_range_leaf_entry_by_ordinal_if(
                        low.as_ref(),
                        high.as_ref(),
                        |bound| self.index_page_all_visible(bound, txn),
                        |count| choose.take().unwrap()(count),
                    );
                }
                index_file.select_range_leaf_entry_by_filtered_ordinal_if(
                    low.as_ref(),
                    high.as_ref(),
                    filter_terms,
                    |bound| self.index_page_all_visible(bound, txn),
                    |count| choose.take().unwrap()(count),
                )
            })?;
            if result.is_some() || choose.is_none() {
                return Ok(result);
            }
        }
        // Old keys visible to this snapshot must be merged in index order.
        // Keep that case on the repair-aware scan. Retained repairs for other
        // ranges or already-visible commits do not disable the whole index.
        if txn.historical
            || txn.dirty_read
            || index.has_multi_valued_parts()
            || self.has_snapshot_index_repairs_in_range(
                table,
                index,
                low.as_ref(),
                high.as_ref(),
                txn,
            )
        {
            return Ok(None);
        }
        // Prove a contiguous writer prefix against this snapshot, rather than
        // requiring background slot GC to have physically removed every slot.
        // In-flight and rolled-back writers remain conservative barriers.
        let prefix = {
            let txns = self.txns.read();
            let mut prefix = self.min_retained_txn_id.load(Ordering::Acquire);
            for (&id, slot) in txns.slots.range((
                std::ops::Bound::Excluded(prefix),
                std::ops::Bound::Unbounded,
            )) {
                if slot.status != TxnStatus::Committed
                    || !slot.commit_scn.is_some_and(|scn| scn <= txn.snapshot_scn)
                {
                    prefix = id.saturating_sub(1);
                    break;
                }
                prefix = id;
            }
            prefix
        };
        let heap = self.cached_heap_handle(table.id)?;
        let result = self.with_index_read(table.id, &index.name, |index_file| {
            index_file.select_range_entry_by_mvcc_ordinal(
                low.as_ref(),
                high.as_ref(),
                filter_terms,
                |bound| index_page_max_trx_shortcut_enabled() && bound <= prefix,
                |key, row_id| {
                    if let Some(visible) =
                        self.is_row_id_visible_buffered_in_heap(&heap.read(), row_id, txn)?
                    {
                        return Ok(visible);
                    }
                    // A visible heap version alone is insufficient: an index
                    // entry may already carry a newer writer's key.
                    let Some(row) = self.read_row(table, row_id, txn)? else {
                        return Ok(false);
                    };
                    Ok(table.index_key(index, &row.values)? == *key)
                },
                choose.take().unwrap(),
            )
        })?;
        // A writer may have retired an old key between taking the repair
        // snapshot and entering the index. Reject the attempt without
        // committing RAND state if such a compensation is now required.
        if self.has_snapshot_index_repairs_in_range(table, index, low.as_ref(), high.as_ref(), txn)
        {
            return Ok(None);
        }
        Ok(result)
    }

    fn has_snapshot_index_repairs_in_range(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        low: Option<&IndexBound>,
        high: Option<&IndexBound>,
        txn: TxnView,
    ) -> bool {
        if !self.has_pending_index_repairs() {
            return false;
        }
        if !self.index_repair_epochs.has_keys(table.id, &index.name) {
            return false;
        }
        let candidates = self
            .index_repair_state
            .candidates(table.id, &index.name, low, high, |_| true);
        if candidates.is_empty() {
            return false;
        }
        let txns = self.txns.read();
        candidates.iter().any(|entry| {
            entry.writer != txn.txn_id
                && txns.slot(entry.writer).map_or_else(
                    || {
                        self.temporal_history
                            .txn_commit(entry.writer)
                            .is_some_and(|commit| commit.commit_scn > txn.snapshot_scn)
                    },
                    |slot| match slot.status {
                        TxnStatus::Active | TxnStatus::Prepared => true,
                        TxnStatus::Committed => {
                            slot.commit_scn.is_some_and(|scn| scn > txn.snapshot_scn)
                        }
                        TxnStatus::RolledBack => false,
                    },
                )
        })
    }

    pub(crate) fn index_page_id_upper_bound(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
    ) -> Result<u64, QueryError> {
        self.with_index_read(table.id, &index.name, |index_file| {
            Ok(index_file.next_page_id())
        })
    }

    #[cfg(test)]
    pub fn range_index_entries_for_leaf_ids(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        leaf_ids: &[u64],
        low: Option<IndexBound>,
        high: Option<IndexBound>,
    ) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
        let (low, low_unsat) = resolve_index_bound(index, low)?;
        let (high, high_unsat) = resolve_index_bound(index, high)?;
        if low_unsat || high_unsat {
            return Ok(Vec::new());
        }
        self.with_index_leaf_range_read(table.id, &index.name, |index_file| {
            index_file.range_entries_for_leaf_ids(leaf_ids, low.as_ref(), high.as_ref())
        })
    }

    /// Key-free variant of `range_index_entries_for_leaf_ids` for
    /// non-covering consumers (the parallel range-scan workers feed the
    /// result straight into a heap lookup) — skips the per-entry
    /// `IndexKey` clone entirely.
    pub fn range_index_row_ids_for_leaf_ids(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        leaf_ids: &[u64],
        low: Option<IndexBound>,
        high: Option<IndexBound>,
    ) -> Result<Vec<RowId>, QueryError> {
        let (low, low_unsat) = resolve_index_bound(index, low)?;
        let (high, high_unsat) = resolve_index_bound(index, high)?;
        if low_unsat || high_unsat {
            return Ok(Vec::new());
        }
        self.with_index_leaf_range_read(table.id, &index.name, |index_file| {
            index_file.range_row_ids_for_leaf_ids(leaf_ids, low.as_ref(), high.as_ref())
        })
    }

    /// Visit a bounded set of B-tree leaf pages in index order, passing
    /// small entry batches to `visitor`. This is the streaming primitive
    /// used by covering index aggregate pushdown: callers can update
    /// aggregate state per batch without materialising the full range.
    ///
    /// `visitor` receives **borrowed** `&IndexKey`s pointing into the
    /// currently-pinned leaf page's `entries`. The borrow is valid only
    /// for the duration of the call; callers must extract whatever they
    /// need (typically a `Value` derived from a `KeyPart`) and not store
    /// the reference. `entry.key.clone()` per row was previously ~15 %
    /// of worker CPU on the TPC-H lineitem covering-aggregate profile
    /// (one `Vec<KeyPart>` allocation per index entry under macOS' system
    /// allocator). The chunk buffer is per-leaf so the borrow lifetime
    /// is always shorter than the pinned leaf page Arc.
    #[allow(dead_code)]
    pub(crate) fn visit_index_leaf_entry_chunks<F>(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        leaf_ids: &[u64],
        seed_leaf_id: Option<u64>,
        low: Option<IndexBound>,
        high: Option<IndexBound>,
        mut visitor: F,
    ) -> Result<(), QueryError>
    where
        F: FnMut(&[(&IndexKey, RowId)]) -> Result<(), QueryError>,
    {
        self.visit_index_leaf_entry_chunks_with_bound(
            table,
            index,
            leaf_ids,
            seed_leaf_id,
            low,
            high,
            |entries, _page_max_trx_id| visitor(entries),
        )
    }

    /// Repair state for one parallel leaf slice of a snapshot read; pass it to
    /// the `*_for_snapshot` leaf visitors below.
    pub(crate) fn snapshot_leaf_repairs(
        self: &Arc<Self>,
        table: &TableSchema,
        index: &IndexSchema,
        low: Option<IndexBound>,
        high: Option<IndexBound>,
        txn: TxnView,
    ) -> SnapshotLeafRepairs {
        SnapshotLeafRepairs {
            feed: (!txn.historical).then(|| SnapshotIndexRepairFeed {
                db: Arc::clone(self),
                table_id: table.id,
                index_name: index.name.clone(),
                txn,
                seen_version: None,
            }),
            low,
            high,
            by_leaf: HashMap::new(),
            row_ids: HashSet::new(),
            seen: HashSet::new(),
            returned_row_ids: HashSet::new(),
            buffered: false,
        }
    }

    /// [`Self::visit_index_leaf_entry_chunks_with_bound`] for a snapshot
    /// whose writers may retire keys while the slice is read. Visitors must
    /// check entry visibility per key, which also discards moved rows'
    /// newer keys.
    ///
    /// While the index has no retired keys the slice streams directly. If a
    /// writer retires one during that stream, the streamed entries cannot be
    /// reconciled: the call returns `false` and the caller must discard
    /// everything the visitor produced and call again. Later calls buffer
    /// each leaf and add the retired keys it owns but did not contain (bound
    /// `u64::MAX`, so no visibility shortcut applies).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn visit_index_leaf_entry_chunks_for_snapshot<F>(
        &self,
        repairs: &mut SnapshotLeafRepairs,
        table: &TableSchema,
        index: &IndexSchema,
        leaf_ids: &[u64],
        seed_leaf_id: Option<u64>,
        low: Option<IndexBound>,
        high: Option<IndexBound>,
        mut visitor: F,
    ) -> Result<bool, QueryError>
    where
        F: FnMut(&[(&IndexKey, RowId)], u64) -> Result<(), QueryError>,
    {
        let Some(feed) = repairs.feed.as_ref() else {
            self.visit_index_leaf_entry_chunks_with_bound(
                table,
                index,
                leaf_ids,
                seed_leaf_id,
                low,
                high,
                visitor,
            )?;
            return Ok(true);
        };
        if !repairs.buffered {
            let version = self.index_repair_version();
            if !feed.active() {
                self.visit_index_leaf_entry_chunks_with_bound(
                    table,
                    index,
                    leaf_ids,
                    seed_leaf_id,
                    low.clone(),
                    high.clone(),
                    &mut visitor,
                )?;
                if !feed.active() && self.index_repair_version() == version {
                    return Ok(true);
                }
                repairs.buffered = true;
                return Ok(false);
            }
            repairs.buffered = true;
        }
        let mut seed = seed_leaf_id;
        for &leaf_id in leaf_ids {
            // Buffer the leaf: a retired key read here while still physical
            // must not be returned again from the directory.
            let (mut entries, mut bound, owned) = loop {
                let epoch = self.index_repair_rollback_epoch(table.id, &index.name);
                let mut entries = Vec::new();
                let mut bound = 0;
                self.visit_index_leaf_entry_chunks_with_bound(
                    table,
                    index,
                    std::slice::from_ref(&leaf_id),
                    seed,
                    low.clone(),
                    high.clone(),
                    |chunk, page_max_trx_id| {
                        bound = page_max_trx_id;
                        entries
                            .extend(chunk.iter().map(|(key, row_id)| ((*key).clone(), *row_id)));
                        Ok(())
                    },
                )?;
                if let Some(owned) = repairs.owned_entries(self, table, index, leaf_id, epoch)? {
                    break (entries, bound, owned);
                }
            };
            seed = Some(leaf_id);
            if let Some(owned) = owned {
                let physical: HashSet<(&IndexKey, RowId)> =
                    entries.iter().map(|(key, row_id)| (key, *row_id)).collect();
                let missing: Vec<(IndexKey, RowId)> = owned
                    .into_iter()
                    .filter(|(key, row_id)| !physical.contains(&(key, *row_id)))
                    .collect();
                if !missing.is_empty() {
                    // Owned keys lie inside this leaf's routing interval;
                    // sorting keeps the slice in index order for streaming
                    // consumers.
                    entries.extend(missing);
                    entries.sort_unstable_by(|left, right| {
                        left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1))
                    });
                    bound = u64::MAX;
                }
            }
            if !entries.is_empty() {
                let borrowed: Vec<(&IndexKey, RowId)> =
                    entries.iter().map(|(key, row_id)| (key, *row_id)).collect();
                visitor(&borrowed, bound)?;
            }
        }
        Ok(true)
    }

    /// [`Self::range_index_row_ids_for_leaf_ids`] for a snapshot. RowId
    /// consumers fetch the visible row without checking its key, so a row
    /// with a retired key is returned exactly once, by the leaf owning it.
    pub(crate) fn range_index_row_ids_for_leaf_ids_for_snapshot(
        &self,
        repairs: &mut SnapshotLeafRepairs,
        table: &TableSchema,
        index: &IndexSchema,
        leaf_ids: &[u64],
        low: Option<IndexBound>,
        high: Option<IndexBound>,
    ) -> Result<Vec<RowId>, QueryError> {
        if repairs.feed.is_none() {
            return self.range_index_row_ids_for_leaf_ids(table, index, leaf_ids, low, high);
        }
        let mut row_ids = Vec::new();
        for &leaf_id in leaf_ids {
            loop {
                let epoch = self.index_repair_rollback_epoch(table.id, &index.name);
                let mut leaf_row_ids = self.range_index_row_ids_for_leaf_ids(
                    table,
                    index,
                    std::slice::from_ref(&leaf_id),
                    low.clone(),
                    high.clone(),
                )?;
                if repairs.merge_row_ids(self, table, index, leaf_id, &mut leaf_row_ids, epoch)? {
                    row_ids.extend(leaf_row_ids);
                    break;
                }
            }
        }
        Ok(row_ids)
    }

    /// Keyed RowId variant for consumers that filter on the index key first
    /// (index condition pushdown) and then fetch rows without a key check.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn visit_index_leaf_entries_once_per_row_for_snapshot<F>(
        &self,
        repairs: &mut SnapshotLeafRepairs,
        table: &TableSchema,
        index: &IndexSchema,
        leaf_ids: &[u64],
        seed_leaf_id: Option<u64>,
        low: Option<IndexBound>,
        high: Option<IndexBound>,
        mut visitor: F,
    ) -> Result<(), QueryError>
    where
        F: FnMut(&[(&IndexKey, RowId)]) -> Result<(), QueryError>,
    {
        if repairs.feed.is_none() {
            return self.visit_index_leaf_entry_chunks(
                table,
                index,
                leaf_ids,
                seed_leaf_id,
                low,
                high,
                visitor,
            );
        }
        let mut seed = seed_leaf_id;
        for &leaf_id in leaf_ids {
            let entries = loop {
                let epoch = self.index_repair_rollback_epoch(table.id, &index.name);
                let mut entries = Vec::new();
                self.visit_index_leaf_entry_chunks(
                    table,
                    index,
                    std::slice::from_ref(&leaf_id),
                    seed,
                    low.clone(),
                    high.clone(),
                    |chunk| {
                        entries
                            .extend(chunk.iter().map(|(key, row_id)| ((*key).clone(), *row_id)));
                        Ok(())
                    },
                )?;
                if repairs
                    .merge_entries_once_per_row(self, table, index, leaf_id, &mut entries, epoch)?
                {
                    break entries;
                }
            };
            seed = Some(leaf_id);
            let borrowed: Vec<(&IndexKey, RowId)> =
                entries.iter().map(|(key, row_id)| (key, *row_id)).collect();
            visitor(&borrowed)?;
        }
        Ok(())
    }

    /// Variant of [`visit_index_leaf_entry_chunks`] that also passes the
    /// current leaf's `page_max_trx_id` upper bound to the visitor.
    /// Covering aggregates use it to short-circuit visibility filtering
    /// on leaves whose bound proves every entry is visible to the read
    /// view (see `Database::try_index_page_max_trx_shortcut`).
    pub(crate) fn visit_index_leaf_entry_chunks_with_bound<F>(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        leaf_ids: &[u64],
        seed_leaf_id: Option<u64>,
        low: Option<IndexBound>,
        high: Option<IndexBound>,
        mut visitor: F,
    ) -> Result<(), QueryError>
    where
        F: FnMut(&[(&IndexKey, RowId)], u64) -> Result<(), QueryError>,
    {
        const ENTRY_CHUNK: usize = 512;
        let (low, low_unsat) = resolve_index_bound(index, low)?;
        let (high, high_unsat) = resolve_index_bound(index, high)?;
        if low_unsat || high_unsat {
            return Ok(());
        }
        self.with_index_leaf_range_read(table.id, &index.name, |index_file| {
            let mut last_row_id: Option<RowId> = None;
            if !index.unique {
                if let Some(seed_leaf_id) = seed_leaf_id {
                    let seed_page = index_file.read_page(seed_leaf_id)?;
                    let seed_leaf = match seed_page.as_ref() {
                        IndexPage::Leaf(leaf) => leaf,
                        _ => {
                            return Err(error(
                                crate::error::catalog::ER_GET_ERRNO,
                                "corrupt index: expected leaf page",
                            ));
                        }
                    };
                    for entry in seed_leaf.entries.iter().rev() {
                        if index_key_in_bounds(&entry.key, low.as_ref(), high.as_ref()) {
                            last_row_id = Some(entry.row_id);
                            break;
                        }
                    }
                }
            }

            for &leaf_id in leaf_ids {
                let leaf_page = index_file.read_page_with_intent(leaf_id, AccessIntent::Scan)?;
                let leaf = match leaf_page.as_ref() {
                    IndexPage::Leaf(leaf) => leaf,
                    _ => {
                        return Err(error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "corrupt index: expected leaf page",
                        ));
                    }
                };
                if let Some(next) = leaf.next_leaf {
                    index_file.prefetch_next_leaves(next);
                }
                let (start, end, terminal) =
                    leaf_in_bounds_span(&leaf.entries, low.as_ref(), high.as_ref());
                let page_bound = leaf.page_max_trx_id();
                let mut entry_buf: Vec<(&IndexKey, RowId)> = Vec::with_capacity(ENTRY_CHUNK);
                for entry in &leaf.entries[start..end] {
                    if !index.unique && last_row_id == Some(entry.row_id) {
                        continue;
                    }
                    last_row_id = Some(entry.row_id);
                    entry_buf.push((&entry.key, entry.row_id));
                    if entry_buf.len() >= ENTRY_CHUNK {
                        visitor(&entry_buf, page_bound)?;
                        entry_buf.clear();
                    }
                }
                if !entry_buf.is_empty() {
                    visitor(&entry_buf, page_bound)?;
                }
                if terminal {
                    return Ok(());
                }
            }
            Ok(())
        })
    }

    /// Raw-page counterpart to `visit_index_leaf_entry_chunks_with_bound`
    /// for a single indexed column whose SQL type maps losslessly to `i64`.
    /// The schema eligibility check remains at the executor call site; this
    /// storage primitive validates the on-page shape while decoding.
    pub(crate) fn visit_index_lossless_i64_leaf_entry_chunks_with_bound<F>(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        leaf_ids: &[u64],
        seed_leaf_id: Option<u64>,
        low: Option<IndexBound>,
        high: Option<IndexBound>,
        key_position: usize,
        visitor: F,
    ) -> Result<(), QueryError>
    where
        F: FnMut(&[(Option<i64>, RowId)], u64) -> Result<(), QueryError>,
    {
        let (low, low_unsat) = resolve_index_bound(index, low)?;
        let (high, high_unsat) = resolve_index_bound(index, high)?;
        if low_unsat || high_unsat {
            return Ok(());
        }
        self.with_index_leaf_range_read(table.id, &index.name, |index_file| {
            index_file.visit_lossless_i64_leaf_entry_chunks(
                leaf_ids,
                seed_leaf_id,
                low.as_ref(),
                high.as_ref(),
                index.unique,
                key_position,
                visitor,
            )
        })
    }

    /// Visit index leaf page ranges with a resume bound that
    /// also passes each leaf's `page_max_trx_id` bound. See
    /// [`visit_index_leaf_entry_chunks_with_bound`] for the covering
    /// aggregate use-case that benefits from it.
    pub(crate) fn visit_index_leaf_page_range_entry_chunks_with_bound<F>(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        page_lo: u64,
        page_hi: u64,
        low: Option<IndexBound>,
        high: Option<IndexBound>,
        mut visitor: F,
    ) -> Result<(), QueryError>
    where
        F: FnMut(&[(&IndexKey, RowId)], u64) -> Result<(), QueryError>,
    {
        const ENTRY_CHUNK: usize = 512;
        let (low, low_unsat) = resolve_index_bound(index, low)?;
        let (high, high_unsat) = resolve_index_bound(index, high)?;
        if low_unsat || high_unsat {
            return Ok(());
        }
        self.with_index_read(table.id, &index.name, |index_file| {
            let upper = index_file.next_page_id();
            let page_lo = page_lo.max(INDEX_EMPTY_ROOT).min(upper);
            let page_hi = page_hi.max(page_lo).min(upper);
            // Big linear leaf-sweep: tag pages as Scan so the buffer
            // pool keeps them in the cold sublist. Otherwise an N-page
            // covering-index aggregate (e.g. TPC-H Q18 sf=5 over
            // `idx_orderkey_qty`, ~68K leaves) promotes every leaf to
            // `young` and evicts genuinely hot pages (customer/orders
            // PRIMARY) that the outer joins still need, which makes the
            // covering plan ~2.5x slower than the heap-scan baseline.
            //
            // Cold-cache prefetch: the synchronous `read_page_with_intent`
            // below would issue one 16 KiB pread per leaf. On the cold
            // pass over a multi-GB covering index that single-thread
            // syscall stream is the dominant wall-clock cost. Seed the
            // async I/O pool with a `prefetch_window`-sized batch up
            // front, and top it up as the cursor advances. Each
            // prefetch_page call is a non-blocking queue push.
            let window = index_file.buffer_pool.effective_prefetch_window() as u64;
            if window > 0 {
                index_file.prefetch_page_range(page_lo, page_hi);
            }
            for page_id in page_lo..page_hi {
                // Keep the prefetch cursor `window` pages ahead of the
                // read cursor.
                if window > 0 {
                    let prefetch_at = page_id.saturating_add(window);
                    if prefetch_at < page_hi {
                        index_file
                            .buffer_pool
                            .prefetch_page(index_file.page_key(prefetch_at));
                    }
                }
                let page = index_file.read_page_with_intent(page_id, AccessIntent::Scan)?;
                let leaf = match page.as_ref() {
                    IndexPage::Leaf(leaf) => leaf,
                    IndexPage::Internal(_) | IndexPage::Free(_) => continue,
                };
                // Per-leaf in-bounds span (see
                // `visit_index_leaf_entry_chunks`): interior leaves
                // run zero per-entry key comparisons. `terminal` is
                // ignored — this sweep visits leaves in page-id order,
                // not key order, so a high-bound cut in one leaf says
                // nothing about the next.
                let (start, end, _terminal) =
                    leaf_in_bounds_span(&leaf.entries, low.as_ref(), high.as_ref());
                let page_bound = leaf.page_max_trx_id();
                // Per-leaf chunk buffer holding borrowed key refs into
                // this leaf. See `visit_index_leaf_entry_chunks` for
                // the rationale — same per-leaf allocation pattern, no
                // `entry.key.clone()` on the hot path.
                let mut entry_buf: Vec<(&IndexKey, RowId)> = Vec::with_capacity(ENTRY_CHUNK);
                for entry in &leaf.entries[start..end] {
                    entry_buf.push((&entry.key, entry.row_id));
                    if entry_buf.len() >= ENTRY_CHUNK {
                        visitor(&entry_buf, page_bound)?;
                        entry_buf.clear();
                    }
                }
                if !entry_buf.is_empty() {
                    visitor(&entry_buf, page_bound)?;
                }
            }
            Ok(())
        })
    }

    /// Raw-header prefix for an unbounded COUNT(*) page-range worker.
    /// Leaves proven all-visible contribute their encoded entry count without
    /// decoding keys or RowIds.  On the first inconclusive leaf the caller
    /// resumes through the normal MVCC-aware covering-index path, so this is a
    /// general optimization rather than a benchmark-specific row-count cache.
    pub(crate) fn count_visible_index_leaf_prefix_page_range(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        page_lo: u64,
        page_hi: u64,
        txn: TxnView,
    ) -> Result<(u64, u64), QueryError> {
        self.check_index_read_view(table, index, txn)?;
        if self.has_pending_index_repairs_for(table.id, &index.name) {
            return Ok((0, page_lo.max(INDEX_EMPTY_ROOT)));
        }
        self.with_index_read(table.id, &index.name, |index_file| {
            index_file.count_leaf_entries_page_range_while(page_lo, page_hi, |page_bound| {
                self.index_page_all_visible(page_bound, txn)
            })
        })
    }

    pub(crate) fn visit_visible_index_numeric_prefix_page_range<F>(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        page_lo: u64,
        page_hi: u64,
        txn: TxnView,
        key_position: usize,
        data_type: &DataTypeKind,
        visitor: F,
    ) -> Result<u64, QueryError>
    where
        F: FnMut(&[Value]) -> Result<(), QueryError>,
    {
        self.check_index_read_view(table, index, txn)?;
        // A non-unique scan must deduplicate physical row IDs across leaves.
        // Preserve that existing path, as well as pending MVCC repairs.
        if !index.unique || self.has_pending_index_repairs_for(table.id, &index.name) {
            return Ok(page_lo.max(INDEX_EMPTY_ROOT));
        }
        self.with_index_read(table.id, &index.name, |index_file| {
            index_file.visit_numeric_leaf_prefix_page_range(
                page_lo,
                page_hi,
                key_position,
                data_type,
                |bound| self.index_page_all_visible(bound, txn),
                visitor,
            )
        })
    }

    fn add_index_visibility_repair_ids(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        txn: TxnView,
        ids: &mut Vec<RowId>,
        mut key_matches: impl FnMut(&IndexKey) -> bool,
    ) -> Result<(), QueryError> {
        self.check_index_read_view(table, index, txn)?;
        if txn.historical {
            self.record_temporal_history_index_hit();
        }
        // Hot SELECT path: skip the txns read lock entirely when there are
        // no pending index repairs anywhere. This is the common case for
        // read-mostly workloads — every SELECT was previously paying for
        // an `RwLock<TransactionManager>::read()` even though the lookup
        // would always return an empty list.
        let has_live_repairs = self.has_pending_index_repairs();
        if !txn.historical && !has_live_repairs {
            return Ok(());
        }
        if has_live_repairs && !txn.historical {
            let candidates = self.index_repair_state.candidates(
                table.id,
                &index.name,
                None,
                None,
                &mut key_matches,
            );
            ids.extend(
                self.live_index_repair_entries(txn, candidates)?
                    .into_iter()
                    .map(|(_, row_id)| row_id),
            );
        }
        if txn.historical {
            ids.extend(
                self.temporal_history
                    .history_index_entries(txn, table.id, &index.name, key_matches)
                    .into_iter()
                    .map(|(_, row_id)| row_id),
            );
        }
        Ok(())
    }

    fn add_exact_index_visibility_repair_ids(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        txn: TxnView,
        key: &IndexKey,
        ids: &mut Vec<RowId>,
    ) -> Result<(), QueryError> {
        self.check_index_read_view(table, index, txn)?;
        let mut entries = Vec::new();
        self.add_exact_index_visibility_repair_entries(table, index, txn, key, &mut entries)?;
        ids.extend(entries.into_iter().map(|(_, row_id)| row_id));
        Ok(())
    }

    fn add_bounded_index_visibility_repair_ids(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        txn: TxnView,
        low: Option<&IndexBound>,
        high: Option<&IndexBound>,
        ids: &mut Vec<RowId>,
    ) -> Result<(), QueryError> {
        self.check_index_read_view(table, index, txn)?;
        if txn.historical {
            self.record_temporal_history_index_hit();
        }
        let has_live_repairs = self.has_pending_index_repairs();
        if !txn.historical && !has_live_repairs {
            return Ok(());
        }
        if has_live_repairs && !txn.historical {
            let candidates =
                self.index_repair_state
                    .candidates(table.id, &index.name, low, high, |_| true);
            ids.extend(
                self.live_index_repair_entries(txn, candidates)?
                    .into_iter()
                    .map(|(_, row_id)| row_id),
            );
        }
        if txn.historical {
            ids.extend(self.temporal_history.history_index_row_ids_bounds(
                txn,
                table.id,
                &index.name,
                low,
                high,
            ));
        }
        Ok(())
    }

    fn add_bounded_index_visibility_repair_entries(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        txn: TxnView,
        low: Option<&IndexBound>,
        high: Option<&IndexBound>,
        entries: &mut Vec<(IndexKey, RowId)>,
    ) -> Result<(), QueryError> {
        self.check_index_read_view(table, index, txn)?;
        if txn.historical {
            self.record_temporal_history_index_hit();
        }
        let has_live_repairs = self.has_pending_index_repairs();
        if !txn.historical && !has_live_repairs {
            return Ok(());
        }
        if has_live_repairs && !txn.historical {
            let candidates =
                self.index_repair_state
                    .candidates(table.id, &index.name, low, high, |_| true);
            entries.extend(self.live_index_repair_entries(txn, candidates)?);
        }
        if txn.historical {
            entries.extend(self.temporal_history.history_index_entries_bounds(
                txn,
                table.id,
                &index.name,
                low,
                high,
            ));
        }
        Ok(())
    }

    fn add_exact_index_visibility_repair_entries(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        txn: TxnView,
        key: &IndexKey,
        entries: &mut Vec<(IndexKey, RowId)>,
    ) -> Result<(), QueryError> {
        self.check_index_read_view(table, index, txn)?;
        if txn.historical {
            self.record_temporal_history_index_hit();
        }
        let has_live_repairs = self.has_pending_index_repairs();
        if !txn.historical && !has_live_repairs {
            return Ok(());
        }
        if has_live_repairs && !txn.historical {
            let candidates =
                self.index_repair_state
                    .exact_candidates(table.id, &index.name, key);
            entries.extend(self.live_index_repair_entries(txn, candidates)?);
        }
        if txn.historical {
            entries.extend(self.temporal_history.history_index_entries_exact(
                txn,
                table.id,
                &index.name,
                key,
            ));
        }
        Ok(())
    }

    fn add_index_visibility_repair_entries(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        txn: TxnView,
        entries: &mut Vec<(IndexKey, RowId)>,
        mut key_matches: impl FnMut(&IndexKey) -> bool,
    ) -> Result<(), QueryError> {
        self.check_index_read_view(table, index, txn)?;
        if txn.historical {
            self.record_temporal_history_index_hit();
        }
        let has_live_repairs = self.has_pending_index_repairs();
        if !txn.historical && !has_live_repairs {
            return Ok(());
        }
        if has_live_repairs && !txn.historical {
            let candidates = self.index_repair_state.candidates(
                table.id,
                &index.name,
                None,
                None,
                &mut key_matches,
            );
            entries.extend(self.live_index_repair_entries(txn, candidates)?);
        }
        if txn.historical {
            entries.extend(self.temporal_history.history_index_entries(
                txn,
                table.id,
                &index.name,
                key_matches,
            ));
        }
        Ok(())
    }

    /// Even faster than [`Self::fulltext_scan_token`] when the caller
    /// only needs the *set* of row_ids for a token (NL union with
    /// `need_score=false`, boolean set membership). Skips payload decode
    /// entirely, emits a compact `Vec<RowId>`. For a `df ≈ 22k` token
    /// this halves per-posting work again vs. the "decode only tf/doc_len"
    /// path.
    pub(crate) fn fulltext_diagnostic_postings(
        &self,
        table: &crate::storage::schema::TableSchema,
        index: &crate::storage::schema::IndexSchema,
    ) -> Result<Vec<FulltextDiagnosticPosting>, QueryError> {
        use crate::storage::fulltext::payload::FtPayload;

        let mut rows = Vec::new();
        let storage_ids = if table.is_partitioned() {
            table.storage_ids()
        } else {
            vec![table.id]
        };
        for storage_id in storage_ids {
            self.with_index_read(storage_id, &index.name, |index_file| {
                for (key, row_id) in index_file.range_entries(None, None, !index.unique)? {
                    let Some(KeyPart::String(word)) = key.0.first() else {
                        continue;
                    };
                    let payload = match key.0.get(1) {
                        Some(KeyPart::Bytes(payload)) => payload.as_slice(),
                        _ => &[][..],
                    };
                    let decoded = FtPayload::decode(payload, crate::storage::fulltext::payload::stored_positions(index))?;
                    let local = (u64::from(row_id.page_id) << 16) | u64::from(row_id.slot_id);
                    let doc_id = ((storage_id & 0xffff) << 48) | (local & 0x0000_ffff_ffff_ffff);
                    rows.push(FulltextDiagnosticPosting {
                        word: word.clone(),
                        doc_id,
                        positions: decoded.positions,
                    });
                }
                Ok(())
            })?;
        }
        rows.sort_by(|left, right| {
            left.word
                .cmp(&right.word)
                .then_with(|| left.doc_id.cmp(&right.doc_id))
                .then_with(|| left.positions.cmp(&right.positions))
        });
        Ok(rows)
    }

    pub(crate) fn fulltext_scan_token_ids(
        &self,
        table: &crate::storage::schema::TableSchema,
        index: &crate::storage::schema::IndexSchema,
        token: &str,
        txn: TxnView,
    ) -> Result<Vec<RowId>, QueryError> {
        if self.has_pending_index_repairs_for(table.id, &index.name) {
            let batch = self.fulltext_scan_token_generic(table, index, token, false, txn)?;
            return Ok(batch.postings.into_iter().map(|p| p.row_id).collect());
        }
        let mut out: Vec<RowId> = Vec::new();
        let mut scan_one = |sid: u64| -> Result<(), QueryError> {
            self.with_index_read(sid, &index.name, |index_file| {
                index_file.fulltext_scan_token(token, |row_id, payload| {
                    crate::storage::fulltext::payload::FtPayload::decode_scoring_only(payload, crate::storage::fulltext::payload::stored_positions(index))?;
                    out.push(row_id);
                    Ok(())
                })
            })
        };
        if table.is_partitioned() {
            for sid in table.storage_ids() {
                scan_one(sid)?;
            }
        } else {
            scan_one(table.id)?;
        }
        if self.has_pending_index_repairs_for(table.id, &index.name) {
            let batch = self.fulltext_scan_token_generic(table, index, token, false, txn)?;
            return Ok(batch
                .postings
                .into_iter()
                .map(|posting| posting.row_id)
                .collect());
        }
        Ok(out)
    }

    /// Fast FULLTEXT posting scan for one exact token — decodes payload
    /// varints inline off B-tree leaves, skipping the `IndexKey` clone the
    /// generic `prefix_index_entries` path pays per posting. Used by the
    /// FT executor to convert `df ≈ 22k` postings without ~44k allocations
    /// (a `String` clone of the already-known token + a `Vec<u8>` clone of
    /// the payload bytes per posting).
    ///
    /// `want_positions` gates retention in the positions arena. NL scoring
    /// and prefix membership retain only `(doc_len, tf)`; all paths still
    /// validate the complete payload. Phrase adjacency retains positions too.
    ///
    /// Pending index-repair fallback: when in-flight DDL/DML has staged
    /// repair entries this defers to the generic materialising path
    /// (repairs are rare and the cost is limited to that window).
    pub(crate) fn fulltext_scan_token(
        &self,
        table: &crate::storage::schema::TableSchema,
        index: &crate::storage::schema::IndexSchema,
        token: &str,
        want_positions: bool,
        txn: TxnView,
    ) -> Result<crate::storage::fulltext::FtPostingBatch, QueryError> {
        use crate::storage::fulltext::payload::FtPayload;
        use crate::storage::fulltext::{FtPosting, FtPostingBatch};

        // Rare fallback: pending repairs need the generic path so the
        // repair entries can be merged in with the same visibility model
        // as the generic scan.
        if self.has_pending_index_repairs_for(table.id, &index.name) {
            return self.fulltext_scan_token_generic(table, index, token, want_positions, txn);
        }

        let mut batch = FtPostingBatch::default();
        let mut scan_one = |sid: u64| -> Result<(), QueryError> {
            self.with_index_read(sid, &index.name, |index_file| {
                index_file.fulltext_scan_token(token, |row_id, payload| {
                    if want_positions {
                        let (doc_len, tf, pos_start, pos_len) =
                            FtPayload::decode_into_arena(payload, &mut batch.positions_arena, crate::storage::fulltext::payload::stored_positions(index))?;
                        batch.postings.push(FtPosting {
                            row_id,
                            doc_len,
                            tf,
                            pos_start,
                            pos_len,
                        });
                    } else {
                        let (doc_len, tf) = FtPayload::decode_scoring_only(payload, crate::storage::fulltext::payload::stored_positions(index))?;
                        batch.postings.push(FtPosting {
                            row_id,
                            doc_len,
                            tf,
                            pos_start: 0,
                            pos_len: 0,
                        });
                    }
                    Ok(())
                })
            })
        };
        if table.is_partitioned() {
            for sid in table.storage_ids() {
                scan_one(sid)?;
            }
        } else {
            scan_one(table.id)?;
        }
        if self.has_pending_index_repairs_for(table.id, &index.name) {
            return self.fulltext_scan_token_generic(table, index, token, want_positions, txn);
        }
        Ok(batch)
    }

    /// Certify complete posting keys, including payloads, against the visible
    /// row. Prefix queries must retain every matching token of that row;
    /// deduplicating by RowId alone loses term frequencies and positions.
    fn filter_fulltext_entries_visible(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        txn: TxnView,
        entries: &mut Vec<(IndexKey, RowId)>,
    ) -> Result<(), QueryError> {
        let mut visible = self
            .filter_visible_index_entries_mask(table, index, entries, txn)?
            .into_iter();
        entries.retain(|_| visible.next().expect("one visibility result per posting"));
        entries.sort_unstable();
        entries.dedup();
        Ok(())
    }

    /// Generic materialising fallback for [`Self::fulltext_scan_token`]
    /// used only when in-flight index repairs are staged.
    fn fulltext_scan_token_generic(
        &self,
        table: &crate::storage::schema::TableSchema,
        index: &crate::storage::schema::IndexSchema,
        token: &str,
        want_positions: bool,
        txn: TxnView,
    ) -> Result<crate::storage::fulltext::FtPostingBatch, QueryError> {
        use crate::storage::fulltext::payload::FtPayload;
        use crate::storage::fulltext::{FtPosting, FtPostingBatch};
        use crate::storage::schema::{IndexKey, KeyPart};

        let key = IndexKey::from(vec![KeyPart::String(token.to_string())]);
        let entries = self.retry_if_rollback_overlapped(table, index, || {
            let (mut entries, _) =
                self.key_index_entries_with_bound::<true>(table, index, key.clone())?;
            self.add_index_visibility_repair_entries(
                table,
                index,
                txn,
                &mut entries,
                |candidate| index_key_starts_with(candidate, &key),
            )?;
            self.filter_fulltext_entries_visible(table, index, txn, &mut entries)?;
            Ok(entries)
        })?;
        let mut batch = FtPostingBatch::default();
        for (entry_key, row_id) in entries {
            let payload = match entry_key.0.get(1) {
                Some(KeyPart::Bytes(bytes)) => bytes.as_slice(),
                _ => &[][..],
            };
            let (doc_len, tf, pos_start, pos_len) = if want_positions {
                FtPayload::decode_into_arena(payload, &mut batch.positions_arena, crate::storage::fulltext::payload::stored_positions(index))?
            } else {
                let (doc_len, tf) = FtPayload::decode_scoring_only(payload, crate::storage::fulltext::payload::stored_positions(index))?;
                (doc_len, tf, 0, 0)
            };
            batch.postings.push(FtPosting {
                row_id,
                doc_len,
                tf,
                pos_start,
                pos_len,
            });
        }
        Ok(batch)
    }

    /// Fast FULLTEXT prefix (`word*`) posting scan grouped by concrete
    /// token. Same allocation savings vs. `range_index_bounds_entries`
    /// as [`Self::fulltext_scan_token`].
    ///
    /// Returns `(token, FtPostingBatch)` pairs in first-encounter order —
    /// each token gets its own `FtPostingBatch` so downstream scoring can
    /// treat each expansion as its own BM25 term (matching MySQL/Lucene:
    /// a wildcard hit contributes tf/df from the actual token it matched).
    pub(crate) fn fulltext_scan_prefix(
        &self,
        table: &crate::storage::schema::TableSchema,
        index: &crate::storage::schema::IndexSchema,
        prefix: &str,
        txn: TxnView,
    ) -> Result<Vec<(String, crate::storage::fulltext::FtPostingBatch)>, QueryError> {
        use crate::storage::fulltext::payload::FtPayload;
        use crate::storage::fulltext::{FtPosting, FtPostingBatch};

        if self.has_pending_index_repairs_for(table.id, &index.name) {
            return self.fulltext_scan_prefix_generic(table, index, prefix, txn);
        }

        let prefix_upper = crate::storage::fulltext::boolean::prefix_successor(prefix);
        // by_token preserves first-encounter order (via `order` vector).
        let mut order: Vec<String> = Vec::new();
        let mut by_token: std::collections::HashMap<String, FtPostingBatch> =
            std::collections::HashMap::new();

        let mut scan_one = |sid: u64| -> Result<(), QueryError> {
            self.with_index_read(sid, &index.name, |index_file| {
                index_file.fulltext_scan_prefix(
                    prefix,
                    prefix_upper.as_deref(),
                    |token, row_id, payload| {
                        let batch = if by_token.contains_key(token) {
                            by_token.get_mut(token).unwrap()
                        } else {
                            order.push(token.to_string());
                            by_token.entry(token.to_string()).or_default()
                        };
                        let (doc_len, tf) = FtPayload::decode_scoring_only(payload, crate::storage::fulltext::payload::stored_positions(index))?;
                        batch.postings.push(FtPosting {
                            row_id,
                            doc_len,
                            tf,
                            pos_start: 0,
                            pos_len: 0,
                        });
                        Ok(())
                    },
                )
            })
        };
        if table.is_partitioned() {
            for sid in table.storage_ids() {
                scan_one(sid)?;
            }
        } else {
            scan_one(table.id)?;
        }
        if self.has_pending_index_repairs_for(table.id, &index.name) {
            return self.fulltext_scan_prefix_generic(table, index, prefix, txn);
        }
        Ok(order
            .into_iter()
            .map(|t| {
                let batch = by_token.remove(&t).unwrap_or_default();
                (t, batch)
            })
            .collect())
    }

    fn fulltext_scan_prefix_generic(
        &self,
        table: &crate::storage::schema::TableSchema,
        index: &crate::storage::schema::IndexSchema,
        prefix: &str,
        txn: TxnView,
    ) -> Result<Vec<(String, crate::storage::fulltext::FtPostingBatch)>, QueryError> {
        use crate::storage::fulltext::payload::FtPayload;
        use crate::storage::fulltext::{FtPosting, FtPostingBatch};
        use crate::storage::schema::{IndexKey, KeyPart};

        let low = Some(IndexBound {
            key: IndexKey::from(vec![KeyPart::String(prefix.to_string())]),
            inclusive: true,
        });
        let high =
            crate::storage::fulltext::boolean::prefix_successor(prefix).map(|succ| IndexBound {
                key: IndexKey::from(vec![KeyPart::String(succ)]),
                inclusive: false,
            });
        let entries = self.retry_if_rollback_overlapped(table, index, || {
            let mut entries =
                self.range_index_bounds_entries(table, index, low.clone(), high.clone())?;
            self.add_bounded_index_visibility_repair_entries(
                table,
                index,
                txn,
                low.as_ref(),
                high.as_ref(),
                &mut entries,
            )?;
            self.filter_fulltext_entries_visible(table, index, txn, &mut entries)?;
            Ok(entries)
        })?;
        let mut order: Vec<String> = Vec::new();
        let mut by_token: std::collections::HashMap<String, FtPostingBatch> =
            std::collections::HashMap::new();
        for (entry_key, row_id) in entries {
            let token = match entry_key.0.first() {
                Some(KeyPart::String(t)) => t.clone(),
                _ => continue,
            };
            if !token.starts_with(prefix) {
                continue;
            }
            let payload = match entry_key.0.get(1) {
                Some(KeyPart::Bytes(bytes)) => bytes.as_slice(),
                _ => &[][..],
            };
            let batch = if let Some(existing) = by_token.get_mut(&token) {
                existing
            } else {
                order.push(token.clone());
                by_token.entry(token).or_default()
            };
            let (doc_len, tf) = FtPayload::decode_scoring_only(payload, crate::storage::fulltext::payload::stored_positions(index))?;
            batch.postings.push(FtPosting {
                row_id,
                doc_len,
                tf,
                pos_start: 0,
                pos_len: 0,
            });
        }
        Ok(order
            .into_iter()
            .map(|t| {
                let batch = by_token.remove(&t).unwrap_or_default();
                (t, batch)
            })
            .collect())
    }
}

/// Retired keys of one index visible to one reader snapshot.
struct SnapshotIndexRepairFeed {
    db: Arc<Database>,
    table_id: u64,
    index_name: String,
    txn: TxnView,
    seen_version: Option<u64>,
}

impl IndexRepairFeed for SnapshotIndexRepairFeed {
    fn active(&self) -> bool {
        self.db
            .has_pending_index_repairs_for(self.table_id, &self.index_name)
    }

    fn rollback_epoch(&self) -> u64 {
        self.db
            .index_repair_rollback_epoch(self.table_id, &self.index_name)
    }

    fn fetch_if_changed(
        &mut self,
        low: Option<&IndexBound>,
        high: Option<&IndexBound>,
    ) -> Result<Option<Vec<(IndexKey, RowId)>>, QueryError> {
        // Read the version first: a registration racing the lookup changes
        // it again, so the next call repeats the lookup.
        let version = self.db.index_repair_version();
        if self.seen_version == Some(version) {
            return Ok(None);
        }
        let mut entries = if self.active() {
            let candidates = self.db.index_repair_state.candidates(
                self.table_id,
                &self.index_name,
                low,
                high,
                |_| true,
            );
            self.db.live_index_repair_entries(self.txn, candidates)?
        } else {
            Vec::new()
        };
        entries.sort_unstable_by(|left, right| {
            left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1))
        });
        entries.dedup();
        self.seen_version = Some(version);
        Ok(Some(entries))
    }
}

/// Retired keys of one parallel leaf slice under one reader snapshot. The
/// shared range lease freezes the B-tree topology, so each retired key has
/// exactly one owning leaf and only the worker reading that leaf emits it.
pub(crate) struct SnapshotLeafRepairs {
    feed: Option<SnapshotIndexRepairFeed>,
    low: Option<IndexBound>,
    high: Option<IndexBound>,
    by_leaf: HashMap<u64, Vec<(IndexKey, RowId)>>,
    /// RowIds with a visible retired key; their physical keys are newer.
    row_ids: HashSet<RowId>,
    seen: HashSet<(IndexKey, RowId)>,
    returned_row_ids: HashSet<RowId>,
    /// Streaming is only safe while the index has no retired keys.
    buffered: bool,
}

impl SnapshotLeafRepairs {
    /// Refresh after a physical read of `leaf`; false while the index has
    /// never had retired keys during this scan.
    fn refresh(
        &mut self,
        db: &Database,
        table: &TableSchema,
        index: &IndexSchema,
    ) -> Result<bool, QueryError> {
        let Some(feed) = self.feed.as_mut() else {
            return Ok(false);
        };
        if self.seen.is_empty() && !feed.active() {
            return Ok(false);
        }
        if let Some(entries) = feed.fetch_if_changed(self.low.as_ref(), self.high.as_ref())? {
            // A row updated several times has one retired key per update;
            // keep all and let visibility select the snapshot's own version.
            for (key, row_id) in entries {
                self.row_ids.insert(row_id);
                if self.seen.insert((key.clone(), row_id)) {
                    let leaf = db.index_leaf_for_entry(table, index, &key, row_id)?;
                    self.by_leaf.entry(leaf).or_default().push((key, row_id));
                }
            }
        }
        Ok(true)
    }

    /// Retired keys owned by `leaf`, refreshed after a read of it that
    /// started at rollback epoch `epoch_before`. The outer `None` asks the
    /// caller to re-read the leaf: a rolled-back writer removed retired keys
    /// meanwhile, so neither the read nor the refresh may hold them.
    fn owned_entries(
        &mut self,
        db: &Database,
        table: &TableSchema,
        index: &IndexSchema,
        leaf: u64,
        epoch_before: u64,
    ) -> Result<Option<Option<Vec<(IndexKey, RowId)>>>, QueryError> {
        let active = self.refresh(db, table, index)?;
        if db.index_repair_rollback_epoch(table.id, &index.name) != epoch_before {
            return Ok(None);
        }
        if !active {
            return Ok(Some(None));
        }
        Ok(Some(
            self.by_leaf.remove(&leaf).filter(|owned| !owned.is_empty()),
        ))
    }

    /// False when the leaf must be re-read (see [`Self::owned_entries`]).
    fn merge_row_ids(
        &mut self,
        db: &Database,
        table: &TableSchema,
        index: &IndexSchema,
        leaf: u64,
        physical: &mut Vec<RowId>,
        epoch_before: u64,
    ) -> Result<bool, QueryError> {
        let Some(owned) = self.owned_entries(db, table, index, leaf, epoch_before)? else {
            return Ok(false);
        };
        let Some(owned) = owned else {
            if !self.row_ids.is_empty() {
                physical.retain(|row_id| !self.row_ids.contains(row_id));
            }
            return Ok(true);
        };
        physical.retain(|row_id| !self.row_ids.contains(row_id));
        physical.extend(
            owned
                .into_iter()
                .map(|(_, row_id)| row_id)
                .filter(|row_id| self.returned_row_ids.insert(*row_id)),
        );
        Ok(true)
    }

    /// False when the leaf must be re-read (see [`Self::owned_entries`]).
    fn merge_entries_once_per_row(
        &mut self,
        db: &Database,
        table: &TableSchema,
        index: &IndexSchema,
        leaf: u64,
        physical: &mut Vec<(IndexKey, RowId)>,
        epoch_before: u64,
    ) -> Result<bool, QueryError> {
        let Some(owned) = self.owned_entries(db, table, index, leaf, epoch_before)? else {
            return Ok(false);
        };
        if !self.row_ids.is_empty() {
            physical.retain(|entry| !self.row_ids.contains(&entry.1));
        }
        if let Some(owned) = owned {
            let before = physical.len();
            physical.extend(
                owned
                    .into_iter()
                    .filter(|entry| self.returned_row_ids.insert(entry.1)),
            );
            if physical.len() != before {
                physical.sort_unstable_by(|left, right| {
                    left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1))
                });
            }
        }
        Ok(true)
    }
}
