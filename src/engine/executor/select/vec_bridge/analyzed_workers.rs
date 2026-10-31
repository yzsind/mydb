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
