//! Block-balanced row sampling: one visible root per block per round.
//! Only the first round touches heap pages; later rounds replay compact
//! inventories, selecting their cutoff before ordering selected identities.
use super::*;
use crate::engine::executor::select::analyze_stats;
mod order;
mod store;
use order::{Order, Ordered, Record};
use store::Store;

pub(super) struct RoundScan {
    scan: Option<RefScan>,
    work: Arc<ScanWork>,
    store: Option<Store>,
    ordered: Option<Ordered>,
    sample: Resolved,
    pub full_percent: bool,
    total_pages: u64,
    histogram: Vec<u64>,
    histogram_charge: ScopedCharge,
    first_rows: u64,
    remaining: u64,
    path: std::path::PathBuf,
    finished: bool,
    delivered: bool,
    pub stats: analyze_stats::SampleRuntimeStats,
    key: Option<crate::planner::PlanNodeId>,
}
impl RoundScan {
    pub fn new(scan: RefScan, key: Option<crate::planner::PlanNodeId>) -> Self {
        let path = scan.db.data_dir().to_owned();
        let sample = scan.tasks.sample;
        let total_pages = scan.tasks.total;
        let full_percent = scan.tasks.full_percent;
        let workers = if total_pages == 0 {
            0
        } else {
            scan.workers.len().max(1) as u32
        };
        Self {
            work: scan.work.clone(),
            scan: Some(scan),
            store: (!full_percent).then(|| Store::new(&path)),
            ordered: None,
            sample,
            full_percent,
            total_pages,
            histogram: Vec::new(),
            histogram_charge: ScopedCharge::new(0),
            first_rows: 0,
            remaining: if sample.unit == SampleUnit::Rows {
                sample.rows
            } else {
                u64::MAX
            },
            path,
            finished: false,
            delivered: false,
            stats: analyze_stats::SampleRuntimeStats {
                workers,
                sequential_percent: full_percent,
                ..Default::default()
            },
            key,
        }
    }
    fn close_scan(&mut self) {
        self.scan.take();
        use std::sync::atomic::Ordering::Relaxed;
        self.stats.root_pages = self.work.pages.load(Relaxed);
        self.stats.nonempty_pages = self.work.nonempty.load(Relaxed);
        self.stats.root_checks = self.work.roots.load(Relaxed);
    }
    fn take_store(&mut self) -> Option<Store> {
        let store = self.store.take();
        if let Some(store) = &store {
            self.stats.directory_peak_bytes =
                self.stats.directory_peak_bytes.max(store.peak as u64);
            self.stats.spill_bytes += store.spilled;
        }
        store
    }
    fn prepare_later_rounds(&mut self) -> Result<(), QueryError> {
        self.close_scan();
        let mut store = self.take_store().unwrap();
        store.seal()?;
        let total = self
            .histogram
            .iter()
            .enumerate()
            .try_fold(0u64, |sum, (m, count)| {
                sum.checked_add(
                    (m as u64)
                        .checked_mul(*count)
                        .ok_or_else(|| invalid("root count overflow"))?,
                )
                .ok_or_else(|| invalid("root count overflow"))
            })?;
        let mut need = self.remaining.min(total - self.first_rows);
        if need == 0 {
            self.finished = true;
            return Ok(());
        }
        let mut active = self.first_rows;
        let mut last_round = 1usize;
        loop {
            active -= self.histogram.get(last_round).copied().unwrap_or(0);
            if need <= active {
                break;
            }
            need -= active;
            last_round += 1;
            if last_round >= self.histogram.len() {
                return Err(invalid("invalid round directory"));
            }
        }
        self.stats.rounds = last_round as u64 + 1;
        // Locate the last partial round in its own seeded page order. An
        // inverse first-round permutation gives O(1) directory addressing.
        let mut last_ordinal = 0;
        for ordinal in 0..self.total_pages {
            if ordinal % 256 == 0 {
                crate::engine::cancel::check_cancelled()?;
            }
            let page = permute(
                ordinal,
                self.total_pages,
                round_key(self.sample.seed, last_round as u64),
            );
            let index =
                permute_direction(page, self.total_pages, round_key(self.sample.seed, 0), true);
            if store.entry(index)?.count as usize > last_round {
                need -= 1;
                if need == 0 {
                    last_ordinal = ordinal;
                    break;
                }
            }
        }
        if need != 0 {
            return Err(invalid("incomplete final sampling round"));
        }
        let mut order = Order::new(&self.path);
        let mut slots = Vec::new();
        let mut charge = ScopedCharge::new(0);
        for index in 0..self.total_pages {
            crate::engine::cancel::check_cancelled()?;
            let entry = store.entry(index)?;
            let m = u64::from(entry.count);
            if m <= 1 {
                continue;
            }
            let page = permute(index, self.total_pages, round_key(self.sample.seed, 0));
            store.slots(entry, &mut slots, &mut charge)?;
            let key = root_key(self.sample.seed, entry.storage, u64::from(entry.page));
            for round in 1..m.min(last_round as u64 + 1) {
                let ordinal = permute_direction(
                    page,
                    self.total_pages,
                    round_key(self.sample.seed, round),
                    true,
                );
                if round == last_round as u64 && ordinal > last_ordinal {
                    continue;
                }
                let slot = slots[permute(round, m, key) as usize];
                order.push(Record {
                    round,
                    ordinal,
                    storage: entry.storage,
                    row: pack(RowId::new(entry.page, slot)),
                })?;
            }
        }
        drop(slots);
        drop(charge);
        drop(store);
        self.stats.directory_peak_bytes = self.stats.directory_peak_bytes.max(order.peak as u64);
        let (ordered, spilled) = order.finish()?;
        self.stats.spill_bytes += spilled;
        self.ordered = Some(ordered);
        Ok(())
    }
    pub fn next(&mut self, max_rows: usize) -> Result<Option<RefChunk>, QueryError> {
        if self.finished {
            return Ok(None);
        }
        if self.full_percent {
            let chunk = self.scan.as_mut().unwrap().next()?;
            if let Some(chunk) = &chunk {
                self.stats.visible_roots += chunk.rows.len() as u64;
            } else {
                self.finished = true;
                self.close_scan();
            }
            return Ok(chunk);
        }
        let mut rows = Vec::new();
        let mut charge = ScopedCharge::new(0);
        let target = max_rows
            .min(2048)
            .max(1)
            .min(usize::try_from(self.remaining).unwrap_or(usize::MAX));
        if target == 0 {
            self.finished = true;
            self.close_scan();
            self.take_store();
            return Ok(None);
        }
        mem_budget::try_reserve_vec_hard(&mut rows, target, &mut charge, "sample round output")?;
        let mut probed = 0;
        while rows.len() < target {
            crate::engine::cancel::check_cancelled()?;
            if let Some(ordered) = &mut self.ordered {
                match ordered.next()? {
                    Some(row) => {
                        rows.push(row);
                        self.remaining -= 1;
                    }
                    None => {
                        self.finished = true;
                        break;
                    }
                }
            } else if let Some(scan) = &mut self.scan {
                match scan.next()? {
                    Some(chunk) => {
                        probed += 1;
                        let count = chunk.rows.len();
                        self.stats.visible_roots += count as u64;
                        self.stats.rounds = 1;
                        if self.histogram.len() <= count {
                            let extra = count + 1 - self.histogram.len();
                            mem_budget::try_reserve_vec_hard(
                                &mut self.histogram,
                                extra,
                                &mut self.histogram_charge,
                                "sample round histogram",
                            )?;
                            self.histogram.resize(count + 1, 0);
                        }
                        self.histogram[count] += 1;
                        self.store
                            .as_mut()
                            .unwrap()
                            .append(chunk.task, &chunk.rows)?;
                        if count != 0 {
                            let key =
                                root_key(self.sample.seed, chunk.task.storage_id, chunk.task.lo);
                            rows.push(chunk.rows.at(permute(0, count as u64, key) as usize));
                            self.first_rows += 1;
                            self.remaining -= 1;
                        }
                    }
                    None => {
                        // Publish a completed first-round prefix before doing
                        // potentially blocking inventory replay and merge.
                        if !rows.is_empty() {
                            break;
                        }
                        self.prepare_later_rounds()?;
                        if self.finished {
                            break;
                        }
                    }
                }
            } else {
                self.finished = true;
                break;
            }
            if self.remaining == 0 {
                self.finished = true;
                self.close_scan();
                self.take_store();
                break;
            }
            let probe_window = if self.delivered {
                target.saturating_mul(4).max(64)
            } else {
                64
            };
            if probed >= probe_window && !rows.is_empty() {
                break;
            }
        }
        if rows.is_empty() {
            return Ok(None);
        }
        self.delivered = true;
        Ok(Some(RefChunk {
            rows: RowRefs::Heap(rows),
            _charge: charge,
            task: Task {
                storage_id: 0,
                lo: 0,
                hi: 0,
            },
        }))
    }
}
impl Drop for RoundScan {
    fn drop(&mut self) {
        self.close_scan();
        self.take_store();
        if let Some(key) = self.key {
            analyze_stats::add_sample_runtime_key(key, self.stats);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oracle(pages: &[Vec<DmlRowRef>], sample: Resolved) -> Vec<DmlRowRef> {
        let threshold = probability_threshold(sample.percent);
        let selected: Vec<Vec<_>> = pages
            .iter()
            .map(|rows| {
                rows.iter()
                    .copied()
                    .filter(|row| {
                        sample.unit == SampleUnit::Rows
                            || u128::from(priority(sample.seed, *row)) < threshold
                    })
                    .collect()
            })
            .collect();
        let mut result = Vec::new();
        let limit = if sample.unit == SampleUnit::Rows {
            sample.rows
        } else {
            u64::MAX
        };
        for round in 0..selected.iter().map(Vec::len).max().unwrap_or(0) as u64 {
            for ordinal in 0..pages.len() as u64 {
                let page = permute(ordinal, pages.len() as u64, round_key(sample.seed, round));
                let rows = &selected[page as usize];
                if let Some(first) = rows.first().filter(|_| round < rows.len() as u64) {
                    let index = permute(
                        round,
                        rows.len() as u64,
                        root_key(
                            sample.seed,
                            first.storage_id,
                            u64::from(first.row_id.page_id),
                        ),
                    );
                    if result.len() as u64 == limit {
                        return result;
                    }
                    result.push(rows[index as usize]);
                }
            }
        }
        result
    }

    #[test]
    fn tablesample_block_rounds_match_oracle_across_memory_and_workers() {
        use crate::engine::executor::StorageQueryEngine;
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
            dir.path(),
            crate::engine::executor::tests::memory_test_storage_config(),
        )
        .unwrap();
        engine
            .execute("CREATE TABLE round_sample(id BIGINT, payload VARCHAR(300))")
            .unwrap();
        engine.execute("INSERT INTO round_sample SELECT n,REPEAT('x',n%250) FROM generate_series(1,30000) g(n)").unwrap();
        engine
            .execute("DELETE FROM round_sample WHERE id%11=0")
            .unwrap();
        let db = engine.database();
        let schema = db.table("round_sample").unwrap();
        let table = PlanTable::new(schema.id, schema.clone(), None);
        let txn = TxnView::read_only(
            db.visible_scn(),
            crate::storage::IsolationLevel::ReadCommitted,
        );
        let count = db.heap_page_count_for_storage(schema.id).unwrap();
        let mut pages = vec![Vec::new(); count as usize];
        db.scan_visible_root_ids(
            &schema,
            schema.id,
            txn,
            0,
            count,
            |_| true,
            |row_id| {
                pages[row_id.page_id as usize].push(DmlRowRef {
                    storage_id: schema.id,
                    row_id,
                });
                Ok(())
            },
        )
        .unwrap();
        for page in &mut pages {
            page.sort_unstable();
        }
        let nonempty = pages.iter().filter(|p| !p.is_empty()).count() as u64;
        for (unit, amount) in [
            (SampleUnit::Rows, 0),
            (SampleUnit::Rows, 2),
            (SampleUnit::Rows, nonempty + 1),
            (SampleUnit::Rows, 25000),
            (SampleUnit::Rows, 30001),
            (SampleUnit::Percent, 0),
            (SampleUnit::Percent, 1),
            (SampleUnit::Percent, 25),
            (SampleUnit::Percent, 100),
        ] {
            let sample = Resolved {
                method: SampleMethod::Bernoulli,
                unit,
                rows: amount,
                percent: amount as f64,
                threshold: probability_threshold(amount as f64),
                seed: 42,
            };
            let expected = oracle(&pages, sample);
            for (bytes, degree) in [
                (64 * 1024 * 1024, 1),
                (64 * 1024 * 1024, 4),
                (1024 * 1024, 1),
                (1024 * 1024, 4),
            ] {
                let budget = Arc::new(mem_budget::MemBudget::new(bytes));
                mem_budget::with_query_budget(budget.clone(), || {
                    let scan = RefScan::new(db.clone(), table.clone(), txn, sample, degree, false)
                        .unwrap();
                    let mut scan = RoundScan::new(scan, None);
                    let mut actual = Vec::new();
                    while let Some(chunk) = scan.next(127).unwrap() {
                        actual.extend(chunk.rows.iter());
                    }
                    assert_eq!(
                        actual, expected,
                        "unit={unit:?}, amount={amount}, budget={bytes}, degree={degree}"
                    );
                    if unit == SampleUnit::Rows && amount == 2 {
                        assert_ne!(actual[0].row_id.page_id, actual[1].row_id.page_id);
                        assert!(scan.stats.root_pages <= 8, "{}", scan.stats.root_pages);
                    }
                    if bytes == 1024 * 1024 && unit == SampleUnit::Rows && amount == 25000 {
                        assert!(scan.stats.spill_bytes > 0);
                    }
                });
                assert_eq!(budget.used(), 0);
            }
        }
        assert_eq!(
            crate::engine::executor::spill_quota::global_spilled_bytes(),
            0
        );
    }
}
