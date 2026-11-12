//! Large integer domains can intersect without retaining either complete RID
//! list. A bounded Bloom summarizes the first index; only second-index hits
//! enter the RID buffer. Every original row predicate remains authoritative.
use super::*;
use crate::engine::executor::select::bloom::{BloomFilter, ExactIntKeySet};

struct Domain {
    index: IndexSchema,
    keys: Arc<ExactIntKeySet>,
    column: usize,
    entries: usize,
}

pub(super) struct IntersectionChoice {
    first: Domain,
    second: Domain,
    max_ids: usize,
    bytes: usize,
    workspace: usize,
    first_buffer_ids: usize,
}

/// Exact RID bits retain the physical locality lost by hashing each RID.
/// Sparse/high-slot address spaces decline before allocating the bitmap.
struct RidBitmap {
    first_page: u32,
    last_page: u32,
    stride: usize,
    words: Vec<u64>,
}
impl RidBitmap {
    fn layout(ids: &[RowId]) -> Option<(u32, u32, usize, usize)> {
        let first = ids.iter().map(|id| id.page_id).min()?;
        let last = ids.iter().map(|id| id.page_id).max()?;
        let stride = (usize::from(ids.iter().map(|id| id.slot_id).max()?) + 1)
            .checked_next_power_of_two()?;
        let bits = usize::try_from(u64::from(last) - u64::from(first) + 1)
            .ok()?
            .checked_mul(stride)?;
        Some((first, last, stride, bits.checked_add(63)? / 64))
    }
    fn build(ids: &[RowId], layout: (u32, u32, usize, usize)) -> Option<Self> {
        let (first_page, last_page, stride, len) = layout;
        let mut words = Vec::new();
        words.try_reserve_exact(len).ok()?;
        words.resize(len, 0);
        for id in ids {
            let bit = (id.page_id - first_page) as usize * stride + usize::from(id.slot_id);
            words[bit / 64] |= 1 << (bit % 64);
        }
        Some(Self {
            first_page,
            last_page,
            stride,
            words,
        })
    }
    fn contains(&self, id: RowId) -> bool {
        if id.page_id < self.first_page
            || id.page_id > self.last_page
            || usize::from(id.slot_id) >= self.stride
        {
            return false;
        }
        let bit = (id.page_id - self.first_page) as usize * self.stride + usize::from(id.slot_id);
        self.words[bit / 64] & (1 << (bit % 64)) != 0
    }
}
enum RidFilter {
    Bitmap(RidBitmap),
    Bloom(BloomFilter),
}
impl RidFilter {
    fn contains(&self, id: RowId) -> bool {
        match self {
            Self::Bitmap(bits) => bits.contains(id),
            Self::Bloom(bits) => bits.contains_int(rid_key(id)),
        }
    }
}

fn index_keys(keys: &ExactIntKeySet, unsigned: bool) -> Option<Vec<crate::storage::IndexKey>> {
    let mut out = Vec::with_capacity(keys.len());
    let mut valid = true;
    keys.for_each_key(|key| {
        let part = if unsigned {
            u64::try_from(key).ok().map(crate::storage::KeyPart::UInt)
        } else {
            i64::try_from(key).ok().map(crate::storage::KeyPart::Int)
        };
        if let Some(part) = part {
            out.push(crate::storage::IndexKey::from(vec![part]));
        } else {
            valid = false;
        }
    });
    out.sort_unstable();
    out.dedup();
    valid.then_some(out)
}

#[inline]
fn rid_key(id: RowId) -> i128 {
    (i128::from(id.page_id) << 16) | i128::from(id.slot_id)
}

fn intersection_cardinality(rows: u64, pages: u64, first: usize, second: usize) -> Option<usize> {
    if first.saturating_add(second) > usize::try_from(rows / 4).unwrap_or(usize::MAX) {
        return None;
    }
    let matched = (first as f64 * second as f64 / rows.max(1) as f64).ceil() as usize;
    // RIDs are not distinct pages. Admit bounded locality discovery and keep
    // the mandatory half-heap proof on actual sorted pages before any output.
    (matched <= usize::try_from(pages.saturating_mul(2)).unwrap_or(usize::MAX)
        && matched <= usize::try_from(rows / 128).unwrap_or(usize::MAX))
    .then_some(matched)
}

impl RuntimeIndexScan<'_> {
    pub(super) fn intersection_choice(&self) -> Option<IntersectionChoice> {
        if !self.allowed
            || self.opened
            || self.filters.len() < 2
            || self.table.schema.is_partitioned()
            || self.table.system_time.is_some()
            || self.txn.historical
            || self.txn.dirty_read
            || self.db.has_pending_index_repairs()
        {
            return None;
        }
        let budget = mem_budget::current_budget()?;
        let workspace = (budget.limit() / 3).min(256 << 20);
        let stats = &self.table.schema.stats;
        let rows = stats
            .row_count
            .saturating_add(self.db.table_modification_delta(self.table.schema.id));
        // A cache-resident native scan is already cheap. This optional index
        // walk targets a heap that necessarily exceeds host/cgroup memory.
        if stats
            .heap_page_count
            .saturating_mul(crate::storage::PAGE_SIZE as u64)
            <= mem_budget::effective_memory_limit_bytes() as u64
        {
            return None;
        }
        let mut domains = Vec::new();
        for runtime_filter in &self.filters {
            super::super::super::super::plan::compile_runtime_filter_probe_projected(
                runtime_filter,
                &self.table.schema,
                &self.columns,
            )?;
            let RuntimeFilter::ExactIntKey {
                keys,
                null_safe,
                filter,
                ..
            } = runtime_filter
            else {
                continue;
            };
            if keys.len() != 1
                || null_safe.iter().any(|value| *value)
                || filter.len() == 0
                || filter.len() > workspace / 256
            {
                continue;
            }
            let indices = super::super::super::super::plan::runtime_filter_schema_indices(
                runtime_filter,
                &self.table,
            )?;
            let column = indices[0];
            if !matches!(
                self.table.schema.columns[column].data_type,
                crate::storage::DataTypeKind::TinyInt
                    | crate::storage::DataTypeKind::SmallInt
                    | crate::storage::DataTypeKind::MediumInt
                    | crate::storage::DataTypeKind::Int
                    | crate::storage::DataTypeKind::BigInt
            ) {
                continue;
            }
            for index in self.table.schema.iter_queryable_indexes() {
                if index.has_prefix_columns()
                    || index.has_expression_parts()
                    || index.has_multi_valued_parts()
                    || index.is_fulltext()
                    || index.is_spatial()
                    || index.is_vector()
                    || !index.columns.first().is_some_and(|name| {
                        name.eq_ignore_ascii_case(&self.table.schema.columns[column].name)
                    })
                {
                    continue;
                }
                let Some(ndv) = self.table.schema.index_prefix_cardinality(&index.name, 1) else {
                    continue;
                };
                let Some(entries) =
                    super::super::super::super::plan::estimate_runtime_keyset_index_entries(
                        rows,
                        ndv,
                        filter.len(),
                    )
                else {
                    continue;
                };
                // Even a materializable domain can touch many heap pages.
                // When another exact domain rejects most of those RIDs,
                // intersect before fetching rather than decoding the larger
                // candidate list only to discard it in the parent.
                if entries > (rows / 4) as usize {
                    continue;
                }
                domains.push(Domain {
                    index: index.clone(),
                    keys: filter.clone(),
                    column,
                    entries,
                });
                break;
            }
        }
        let mut best: Option<IntersectionChoice> = None;
        for first in 0..domains.len() {
            for second in first + 1..domains.len() {
                let (a, b) = (&domains[first], &domains[second]);
                if a.column == b.column {
                    continue;
                }
                let Some(matches) =
                    intersection_cardinality(rows, stats.heap_page_count, a.entries, b.entries)
                else {
                    continue;
                };
                if matches > a.entries.min(b.entries) / 4 {
                    continue;
                }
                // Summarize the smaller input to bound bitmap cache traffic.
                // The larger input can reject most RIDs after its first bit.
                let (a, b) = if a.entries <= b.entries {
                    (a, b)
                } else {
                    (b, a)
                };
                // A false positive pays for a heap fetch, not just a hash
                // probe. Keep at least twenty bits per estimated RID while
                // retaining the same complete workspace ceiling.
                let bloom_bytes = BloomFilter::estimated_byte_size(a.entries.saturating_mul(2));
                let overhead = a
                    .keys
                    .len()
                    .saturating_add(b.keys.len())
                    .saturating_mul(256)
                    .saturating_add(2 << 20);
                let Some(available) = workspace.checked_sub(bloom_bytes.saturating_add(overhead))
                else {
                    continue;
                };
                // The standard four-probe layout has <= 1.2% false positives
                // at ten bits/key. Reserve conservative false-positive RIDs.
                let expected = matches.saturating_add(b.entries / 64).saturating_add(4096);
                let max_ids = expected
                    .saturating_mul(2)
                    .min(available / (2 * std::mem::size_of::<RowId>() + 8));
                if max_ids < expected
                    || best.as_ref().is_some_and(|old| {
                        old.first.entries + old.second.entries <= a.entries + b.entries
                    })
                {
                    continue;
                }
                let copy = |domain: &Domain| Domain {
                    index: domain.index.clone(),
                    keys: domain.keys.clone(),
                    column: domain.column,
                    entries: domain.entries,
                };
                let base_bytes =
                    bloom_bytes + overhead + max_ids * (2 * std::mem::size_of::<RowId>() + 8);
                let first_buffer_ids = a
                    .entries
                    .saturating_mul(2)
                    .min(workspace.saturating_sub(base_bytes) / std::mem::size_of::<RowId>());
                let first_buffer_ids = if first_buffer_ids >= a.entries / 4 {
                    first_buffer_ids
                } else {
                    0
                };
                let first_bytes = first_buffer_ids.saturating_mul(std::mem::size_of::<RowId>());
                best = Some(IntersectionChoice {
                    first: copy(a),
                    second: copy(b),
                    max_ids,
                    bytes: base_bytes + first_bytes,
                    workspace,
                    first_buffer_ids,
                });
            }
        }
        best
    }

    pub(super) fn start_intersection(
        &mut self,
        choice: IntersectionChoice,
    ) -> Result<(), QueryError> {
        let Some(mut workspace) = mem_budget::ControlledMemoryCharge::try_new_optional(
            mem_budget::ControlledMemoryCategory::Executor,
            choice.bytes,
        ) else {
            return Ok(());
        };
        let Some(first_keys) = index_keys(
            &choice.first.keys,
            self.table.schema.columns[choice.first.column].unsigned,
        ) else {
            return Ok(());
        };
        let Some(second_keys) = index_keys(
            &choice.second.keys,
            self.table.schema.columns[choice.second.column].unsigned,
        ) else {
            return Ok(());
        };
        let version = self.db.index_repair_version();
        let bloom_bytes = BloomFilter::estimated_byte_size(choice.first.entries.saturating_mul(2));
        let mut first_ids = Vec::new();
        if choice.first_buffer_ids > 0
            && first_ids
                .try_reserve_exact(choice.first_buffer_ids)
                .is_err()
        {
            return Ok(());
        }
        let mut bloom = (choice.first_buffer_ids == 0)
            .then(|| BloomFilter::new_int(choice.first.entries.saturating_mul(2)));
        let Some(first_count) = self.db.visit_index_prefix_row_ids_bounded(
            &self.table.schema,
            &choice.first.index,
            &first_keys,
            self.txn,
            choice.first.entries.saturating_mul(2),
            |id| {
                if let Some(bloom) = bloom.as_mut() {
                    bloom.insert_int(rid_key(id));
                } else if first_ids.len() < choice.first_buffer_ids {
                    first_ids.push(id);
                } else {
                    // Statistics may overestimate the domain. Buffer only
                    // available workspace and retain a one-pass Bloom fallback
                    // if the actual domain crosses that smaller reservation.
                    let mut bits = BloomFilter::new_int(choice.first.entries.saturating_mul(2));
                    for buffered in &first_ids {
                        bits.insert_int(rid_key(*buffered));
                    }
                    bits.insert_int(rid_key(id));
                    first_ids = Vec::new();
                    bloom = Some(bits);
                }
                true
            },
        )?
        else {
            return Ok(());
        };
        let filter = if let Some(bloom) = bloom {
            RidFilter::Bloom(bloom)
        } else {
            let bitmap = RidBitmap::layout(&first_ids).and_then(|layout| {
                let bytes = layout.3.checked_mul(std::mem::size_of::<u64>())?;
                let extra = bytes.saturating_sub(bloom_bytes);
                if extra > choice.workspace.saturating_sub(choice.bytes)
                    || !workspace.try_grow_optional(extra)
                {
                    return None;
                }
                let result = RidBitmap::build(&first_ids, layout);
                if result.is_none() {
                    workspace.shrink(extra);
                }
                result
            });
            match bitmap {
                Some(bitmap) => RidFilter::Bitmap(bitmap),
                None => {
                    let mut bloom = BloomFilter::new_int(choice.first.entries.saturating_mul(2));
                    for id in &first_ids {
                        bloom.insert_int(rid_key(*id));
                    }
                    RidFilter::Bloom(bloom)
                }
            }
        };
        drop(first_ids);
        let mut ids = Vec::new();
        if ids.try_reserve_exact(choice.max_ids).is_err() {
            return Ok(());
        }
        let Some(second_count) = self.db.visit_index_prefix_row_ids_bounded(
            &self.table.schema,
            &choice.second.index,
            &second_keys,
            self.txn,
            choice.second.entries.saturating_mul(2),
            |id| {
                if filter.contains(id) {
                    if ids.len() == choice.max_ids {
                        return false;
                    }
                    ids.push(id);
                }
                true
            },
        )?
        else {
            return Ok(());
        };
        if version != self.db.index_repair_version() {
            return Ok(());
        }
        ids.sort_unstable();
        ids.dedup();
        let pages = ids
            .iter()
            .map(|id| id.page_id)
            .fold((None, 0u64), |(last, count), id| {
                (Some(id), count + u64::from(last != Some(id)))
            })
            .1;
        if pages > (self.table.schema.stats.heap_page_count / 2).max(1) {
            return Ok(());
        }
        analyze_stats::add_range_scan_work((first_count + second_count) as u64, 0, 0);
        analyze_stats::add_runtime_index_candidates(
            (first_keys.len() + second_keys.len()) as u64,
            ids.len() as u64,
            false,
        );
        drop(filter);
        let indexed = vec_index_scan::IndexBatchStream::from_row_ids(
            &self.db,
            self.txn,
            &self.table,
            &choice.second.index,
            ids,
            Some(&self.columns),
        )?;
        drop(workspace);
        if let Some(indexed) = indexed {
            self.probes = self
                .filters
                .iter()
                .map(|filter| {
                    super::super::super::super::plan::compile_runtime_filter_probe_projected(
                        filter,
                        &self.table.schema,
                        &self.columns,
                    )
                })
                .collect();
            self.adopt_indexed(indexed, pages)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_rid_bitmap_keeps_page_gaps_slots_and_full_page_id_range() {
        for ids in [
            vec![RowId::new(3, 0), RowId::new(3, 5), RowId::new(5, 2)],
            vec![RowId::new(u32::MAX - 1, 0), RowId::new(u32::MAX, u16::MAX)],
        ] {
            let bitmap = RidBitmap::build(&ids, RidBitmap::layout(&ids).unwrap()).unwrap();
            for id in &ids {
                assert!(bitmap.contains(*id));
            }
            assert!(!bitmap.contains(RowId::new(0, 0)));
            assert!(!bitmap.contains(RowId::new(ids[0].page_id, 1)));
        }
        assert!(RidBitmap::layout(&[]).is_none());
    }

    #[test]
    fn intersection_estimates_admit_bounded_locality_discovery() {
        assert_eq!(
            intersection_cardinality(200_000_000, 3_000_000, 20_000_000, 10_000_000),
            Some(1_000_000)
        );
        assert_eq!(
            intersection_cardinality(200_000_000, 1_000_000, 20_000_000, 10_000_000),
            Some(1_000_000)
        );
        assert!(intersection_cardinality(200_000_000, 200_000, 20_000_000, 10_000_000).is_none());
        assert!(intersection_cardinality(200_000_000, 3_000_000, 50_000_000, 50_000_000).is_none());
    }

    #[test]
    fn two_unmaterializable_index_domains_read_only_their_intersection() {
        use crate::engine::executor::select::bloom::ExactIntKeySetBuilder;
        use crate::engine::StorageQueryEngine;
        let dir = tempfile::tempdir().unwrap();
        let engine = StorageQueryEngine::open(dir.path()).unwrap();
        engine
            .execute(
                "CREATE TABLE index_pairs (k1 INT,k2 INT,v INT,INDEX by_k1(k1),INDEX by_k2(k2))",
            )
            .unwrap();
        engine.execute("INSERT INTO index_pairs SELECT MOD(n,31),MOD(n,29),n FROM generate_series(1,1000) g(n)").unwrap();
        engine
            .execute("INSERT INTO index_pairs VALUES(NULL,1,-1),(1,NULL,-2),(NULL,NULL,-3)")
            .unwrap();
        engine.execute("ANALYZE TABLE index_pairs").unwrap();
        let crate::engine::api::QueryResponse::ResultSet(expected) = engine.execute("SELECT k1,k2,v FROM index_pairs WHERE k1 BETWEEN 0 AND 15 AND k2 BETWEEN 0 AND 15 ORDER BY v").unwrap().materialize().unwrap() else { panic!("rows") };
        let db = engine.db_for_tests();
        db.set_undo_retention_ms(0).unwrap();
        db.gc_committed_txn_slots();
        let mut schema = db.table("index_pairs").unwrap();
        let stats = &mut Arc::make_mut(&mut schema).stats;
        stats.row_count = 100_000_000;
        stats.heap_page_count = (mem_budget::effective_memory_limit_bytes() as u64
            / crate::storage::PAGE_SIZE as u64)
            .saturating_add(1)
            .max(2_000_000);
        for stats in stats.indexes.values_mut() {
            stats.prefix_distinct_counts = vec![1000];
        }
        let table = PlanTable::new(schema.id, schema, Some("p".into()));
        let columns = vec![0, 1, 2];
        let schema = build_scan_batch_schema(&table, &columns);
        let budget = Arc::new(mem_budget::MemBudget::new(64 << 20));
        mem_budget::with_query_budget(budget.clone(), || {
            let filters = ["k1", "k2"].map(|name| {
                let mut builder = ExactIntKeySetBuilder::with_capacity(16).unwrap();
                for value in 0..16 {
                    builder.insert(value);
                }
                RuntimeFilter::ExactIntKey {
                    keys: vec![Expr::CompoundIdentifier(vec!["p".into(), name.into()])],
                    null_safe: vec![false],
                    filter: Arc::new(builder.finish().unwrap()),
                    rejected: Arc::new(std::sync::OnceLock::new()),
                }
            });
            let mut scan = RuntimeIndexScan {
                db: db.clone(),
                txn: TxnView::read_only(
                    db.visible_scn(),
                    crate::storage::IsolationLevel::ReadCommitted,
                ),
                table,
                columns,
                schema: schema.clone(),
                original: Box::new(MaterializedBatchOperator::new(schema, Vec::new())),
                filters: filters.to_vec(),
                indexed: None,
                probes: Vec::new(),
                opened: false,
                allowed: true,
                residual: None,
            };
            assert!(scan.choice().is_none());
            assert!(scan.intersection_choice().is_some());
            scan.start().unwrap();
            assert!(
                scan.indexed.is_some(),
                "the streamed intersection must be adopted"
            );
            let mut actual = Vec::new();
            while let Some(batch) = scan.next_batch().unwrap() {
                actual.extend(batch.visible_indices().map(|row| batch.row(row)));
            }
            actual.sort_by_key(|row| match row[2] {
                Value::Int(value) => value,
                _ => panic!("int"),
            });
            assert_eq!(actual, expected.rows);
        });
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn streamed_prefix_candidates_match_composite_lookup_and_decline_repairs() {
        use crate::engine::StorageQueryEngine;
        let dir = tempfile::tempdir().unwrap();
        let engine = StorageQueryEngine::open(dir.path()).unwrap();
        engine
            .execute("CREATE TABLE visit_candidates (a INT,b INT, INDEX ab(a,b))")
            .unwrap();
        engine.execute("INSERT INTO visit_candidates SELECT MOD(n,10),MOD(n,7) FROM generate_series(1,1000) g(n)").unwrap();
        engine
            .execute("UPDATE visit_candidates SET b=b+100 WHERE a=1 AND b=1 LIMIT 1")
            .unwrap();
        let db = engine.db_for_tests();
        let schema = db.table("visit_candidates").unwrap();
        let index = schema
            .indexes
            .iter()
            .find(|index| index.name == "ab")
            .unwrap();
        let keys = [1, 2]
            .map(|key| crate::storage::IndexKey::from(vec![crate::storage::KeyPart::Int(key)]));
        let txn = TxnView::read_only(
            db.visible_scn(),
            crate::storage::IsolationLevel::ReadCommitted,
        );
        let mut calls = 0;
        assert!(db
            .visit_index_prefix_row_ids_bounded(&schema, index, &keys, txn, 1000, |_| {
                calls += 1;
                true
            })
            .unwrap()
            .is_none());
        assert_eq!(calls, 0);
        db.set_undo_retention_ms(0).unwrap();
        db.gc_committed_txn_slots();
        let handle = db.cached_index_handle(schema.id, &index.name).unwrap();
        let expected = db
            .lookup_index_entries_many_keyed_bounded_with_handle(
                &handle, &schema, index, &keys, txn, 1000, true,
            )
            .unwrap()
            .unwrap();
        let mut actual = Vec::new();
        assert_eq!(
            db.visit_index_prefix_row_ids_bounded(&schema, index, &keys, txn, 1000, |id| {
                actual.push(id);
                true
            })
            .unwrap(),
            Some(200)
        );
        actual.sort_unstable();
        let mut expected = expected.into_iter().map(|(_, id)| id).collect::<Vec<_>>();
        expected.sort_unstable();
        assert_eq!(actual, expected);
        assert!(db
            .visit_index_prefix_row_ids_bounded(&schema, index, &keys, txn, 199, |_| true)
            .unwrap()
            .is_none());
        assert!(db
            .visit_index_prefix_row_ids_bounded(&schema, index, &keys, txn, 1000, |_| false)
            .unwrap()
            .is_none());
        let mut historical = txn;
        historical.historical = true;
        assert!(db
            .visit_index_prefix_row_ids_bounded(&schema, index, &keys, historical, 1000, |_| true)
            .unwrap()
            .is_none());
        let mut changed = false;
        assert!(
            db.visit_index_prefix_row_ids_bounded(&schema, index, &keys, txn, 1000, |_| {
                if !changed {
                    changed = true;
                    engine
                        .execute("UPDATE visit_candidates SET b=b+1 WHERE a=1 LIMIT 1")
                        .unwrap();
                }
                true
            })
            .unwrap()
            .is_none(),
            "publication during the callback invalidates every partial RID"
        );
    }

    #[test]
    fn intersection_row_ids_and_unsigned_keys_preserve_full_ranges() {
        use crate::engine::executor::select::bloom::ExactIntKeySetBuilder;
        assert_ne!(
            rid_key(RowId::new(u32::MAX, 0)),
            rid_key(RowId::new(0, u16::MAX))
        );
        let mut builder = ExactIntKeySetBuilder::with_capacity(2).unwrap();
        builder.insert(i128::from(u64::MAX));
        builder.insert(0);
        let keys = builder.finish().unwrap();
        assert_eq!(index_keys(&keys, true).unwrap().len(), 2);
        assert!(index_keys(&keys, false).is_none());
    }
}
