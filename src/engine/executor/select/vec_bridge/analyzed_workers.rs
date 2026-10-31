//! Preserve source observations when an adjacent consumer owns scan workers.
use super::*;
use crate::engine::executor::vec::stream::ParallelBatchWorker;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[derive(Default)]
struct Totals {
    first: Option<u128>,
    active: u128,
    rows: u64,
    batches: u64,
    workers: u32,
}
struct Observation {
    key: Option<crate::planner::PlanNodeId>,
    collector: Option<analyze_stats::CollectorHandle>,
    io: Arc<crate::storage::QueryBufferStats>,
    totals: parking_lot::Mutex<Totals>,
    visits: Option<(crate::planner::PlanNodeId, Arc<AtomicU64>)>,
    mode: crate::engine::executor::vec::config::VecMode,
}
impl Drop for Observation {
    fn drop(&mut self) {
        let Some(key) = self.key else {
            return;
        };
        let totals = self.totals.get_mut();
        if totals.workers == 0 {
            return;
        }
        let (cache_reads, physical_reads) = crate::storage::query_buffer_stats_counts(&self.io);
        analyze_stats::with_collector_handle_if_present(self.collector.clone(), || {
            let _mode = crate::engine::executor::vec::config::enter_mode(self.mode);
            {
                analyze_stats::record_key_timing(
                    key,
                    totals.first.unwrap_or(totals.active),
                    totals.active,
                    0,
                    totals.rows,
                    analyze_stats::NodeIoStats {
                        cache_reads,
                        physical_reads,
                    },
                    true,
                );
                analyze_stats::add_vector_runtime_key(key, totals.batches, 0, totals.workers);
                if let Some((key, counter)) = &self.visits {
                    analyze_stats::add_partitions_key(*key, counter.load(Ordering::Relaxed));
                }
            }
        });
    }
}
struct Worker {
    inner: Box<dyn ParallelBatchWorker>,
    observation: Arc<Observation>,
}
impl ParallelBatchWorker for Worker {
    fn take_coordinator_pull(&mut self, abort: Arc<AtomicBool>) -> Option<Box<dyn BatchOperator>> {
        Some(Box::new(CoordinatorObservation {
            inner: self.inner.take_coordinator_pull(abort)?,
            observation: self.observation.clone(),
            active: 0,
            rows: 0,
            batches: 0,
            first: None,
            started: false,
        }))
    }

    fn run(
        self: Box<Self>,
        consume: &mut dyn FnMut(Batch) -> Result<(), QueryError>,
        abort: &AtomicBool,
    ) -> Result<(), QueryError> {
        let Self { inner, observation } = *self;
        let started = std::time::Instant::now();
        let mut consumer_ns = 0u128;
        let mut rows = 0u64;
        let mut batches = 0u64;
        let mut first = None;
        let result =
            crate::storage::with_shared_operator_buffer_stats(observation.io.clone(), || {
                inner.run(
                    &mut |batch| {
                        first.get_or_insert(started.elapsed().as_nanos());
                        rows += batch.visible_rows() as u64;
                        batches += 1;
                        let consume_start = std::time::Instant::now();
                        let result = consume(batch);
                        consumer_ns += consume_start.elapsed().as_nanos();
                        result
                    },
                    abort,
                )
            });
        let active = started.elapsed().as_nanos().saturating_sub(consumer_ns);
        let mut total = observation.totals.lock();
        if let Some(first) = first {
            total.first = Some(total.first.map_or(first, |previous| previous.min(first)));
        }
        total.active = total.active.max(active);
        total.rows += rows;
        total.batches += batches;
        total.workers += 1;
        drop(total);
        result
    }
}

struct CoordinatorObservation {
    inner: Box<dyn BatchOperator>,
    observation: Arc<Observation>,
    active: u128,
    rows: u64,
    batches: u64,
    first: Option<u128>,
    started: bool,
}

impl BatchOperator for CoordinatorObservation {
    fn schema(&self) -> &Arc<BatchSchema> {
        self.inner.schema()
    }
    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        self.started = true;
        let started = std::time::Instant::now();
        let result =
            crate::storage::with_shared_operator_buffer_stats(self.observation.io.clone(), || {
                self.inner.next_batch()
            });
        self.active += started.elapsed().as_nanos();
        if let Ok(Some(batch)) = &result {
            let rows = batch.visible_rows();
            if rows != 0 {
                self.first.get_or_insert(self.active);
                self.rows += rows as u64;
                self.batches += 1;
            }
        }
        result
    }
}

impl Drop for CoordinatorObservation {
    fn drop(&mut self) {
        if !self.started {
            return;
        }
        let mut total = self.observation.totals.lock();
        if let Some(first) = self.first {
            total.first = Some(total.first.map_or(first, |previous| previous.min(first)));
        }
        total.active = total.active.max(self.active);
        total.rows += self.rows;
        total.batches += self.batches;
        total.workers += 1;
    }
}
pub(super) fn wrap(
    workers: Vec<Box<dyn ParallelBatchWorker>>,
    plan: &PhysicalPlan,
    visits: Option<(crate::planner::PlanNodeId, Arc<AtomicU64>)>,
) -> Vec<Box<dyn ParallelBatchWorker>> {
    let observation = Arc::new(Observation {
        key: analyze_stats::plan_key(plan),
        collector: analyze_stats::current_collector_handle(),
        io: Arc::new(Default::default()),
        totals: Default::default(),
        visits,
        mode: crate::engine::executor::vec::config::effective_mode(),
    });
    workers
        .into_iter()
        .map(|inner| {
            Box::new(Worker {
                inner,
                observation: observation.clone(),
            }) as Box<dyn ParallelBatchWorker>
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coordinator_observation_counts_one_worker_and_selected_rows_on_drop() {
        let observation = Arc::new(Observation {
            key: None,
            collector: None,
            io: Arc::new(Default::default()),
            totals: Default::default(),
            visits: None,
            mode: crate::engine::executor::vec::config::VecMode::On,
        });
        let schema = Arc::new(BatchSchema::new(vec![]));
        let batches = vec![
            Batch::with_row_count(schema.clone(), vec![], 0),
            Batch::with_row_count(schema.clone(), vec![], 4)
                .with_selection(Selection::Indices(vec![2, 0, 2])),
        ];
        let mut input = CoordinatorObservation {
            inner: Box::new(MaterializedBatchOperator::new(schema, batches)),
            observation: observation.clone(),
            active: 0,
            rows: 0,
            batches: 0,
            first: None,
            started: false,
        };
        assert_eq!(input.next_batch().unwrap().unwrap().visible_rows(), 0);
        assert_eq!(input.next_batch().unwrap().unwrap().visible_rows(), 3);
        assert!(input.next_batch().unwrap().is_none());
        assert!(input.next_batch().unwrap().is_none());
        drop(input);
        let total = observation.totals.lock();
        assert_eq!((total.rows, total.batches, total.workers), (3, 1, 1));
        assert!(total.first.is_some());
        assert!(total.active >= total.first.unwrap());
    }
}
