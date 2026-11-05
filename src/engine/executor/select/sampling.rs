//! Physical sampling over MVCC-visible root heap identities. Predicates are
//! deliberately evaluated by consumers, after the sample has been selected.
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{mpsc, Arc};

use crate::engine::executor::mem_budget::{self, ScopedCharge};
use crate::engine::executor::vec::batch::{
    Batch, ColumnAccumulator, ColumnVector, DEFAULT_BATCH_ROWS,
};
use crate::engine::executor::vec::schema::{BatchField, BatchSchema};
use crate::engine::executor::vec::stream::BatchOperator;
use crate::parser::{Expr, SampleMethod, SampleUnit, Select, Statement, TableSampleSpec};
use crate::planner::{PhysicalPlan, PlanTable};
use crate::storage::{Database, DmlRowRef, RowId, TxnView};
use crate::types::{ColumnType, QueryError, Value};

pub(crate) static SAMPLE_WORKER_TASKS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static SAMPLE_PAGES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[derive(Clone, Copy, Debug)]
pub(crate) struct Resolved {
    method: SampleMethod,
    unit: SampleUnit,
    percent: f64,
    rows: u64,
    seed: u64,
}
pub(crate) type Bindings = Arc<HashMap<u32, Resolved>>;
thread_local! { static CURRENT: RefCell<Option<Bindings>> = const { RefCell::new(None) }; }
pub(crate) struct Scope(Option<Bindings>);
impl Drop for Scope {
    fn drop(&mut self) {
        CURRENT.with(|slot| *slot.borrow_mut() = self.0.take());
    }
}
pub(crate) fn capture() -> Option<Bindings> {
    CURRENT.with(|slot| slot.borrow().clone())
}
pub(crate) fn enter(bindings: Option<Bindings>) -> Scope {
    Scope(CURRENT.with(|slot| std::mem::replace(&mut *slot.borrow_mut(), bindings)))
}
pub(crate) fn describe(table: &PlanTable) -> String {
    table.sample.as_ref().expect("sampling spec").to_sql()
}
/// Costing reuses the statement binding rather than evaluating a CAST again
/// and duplicating its warnings on a cold plan-cache execution.
pub(crate) fn planning_amount(spec: &TableSampleSpec) -> Option<f64> {
    if let Some(resolved) = capture().and_then(|bindings| bindings.get(&spec.binding_id).copied()) {
        return Some(match resolved.unit {
            SampleUnit::Rows => resolved.rows as f64,
            SampleUnit::Percent => resolved.percent,
        });
    }
    let value = match &spec.amount {
        Expr::Literal(literal) => crate::engine::expr::literal_value(literal),
        Expr::BoundValue(value) => value.clone(),
        Expr::Placeholder(index) => crate::engine::params::current_param(*index)?,
        _ => return None,
    };
    numeric_amount(&value).ok()
}
fn invalid(message: impl Into<String>) -> QueryError {
    crate::error::catalog::ER_WRONG_ARGUMENTS.error(format!("TABLESAMPLE: {}", message.into()))
}
fn constant(expr: &Expr) -> bool {
    use crate::parser::{BinaryOp, Literal, UnaryOp};
    match expr {
        Expr::Literal(
            Literal::Int(_) | Literal::UInt(_) | Literal::Decimal(_) | Literal::Float(_),
        ) => true,
        Expr::BoundValue(value) => numeric_amount(value).is_ok(),
        Expr::Placeholder(_) => crate::engine::expr::eval_const_runtime(expr)
            .is_ok_and(|value| numeric_amount(&value).is_ok()),
        Expr::Unary {
            op: UnaryOp::Neg,
            expr,
        } => constant(expr),
        Expr::Binary {
            left,
            op:
                BinaryOp::Add
                | BinaryOp::Sub
                | BinaryOp::Mul
                | BinaryOp::Div
                | BinaryOp::IntDiv
                | BinaryOp::Mod,
            right,
        } => constant(left) && constant(right),
        Expr::Function {
            name,
            args,
            distinct: false,
        } if name.eq_ignore_ascii_case("CAST") && args.len() >= 2 => {
            matches!(&args[1], Expr::Literal(Literal::String(target))
                if matches!(target.as_str(), "SIGNED" | "UNSIGNED" | "DECIMAL" | "FLOAT" | "DOUBLE"))
                && (constant(&args[0])
                    || matches!(
                        args[0],
                        Expr::Literal(Literal::String(_))
                            | Expr::BoundValue(_)
                            | Expr::Placeholder(_)
                    ))
                && args[2..].iter().all(constant)
        }
        _ => false,
    }
}
pub(crate) fn numeric_amount(value: &Value) -> Result<f64, QueryError> {
    let n = match value {
        Value::Int(n) => *n as f64,
        Value::UInt(n) => *n as f64,
        Value::Float(n) => f64::from(*n),
        Value::Double(n) => *n,
        Value::Decimal(_)
        | Value::DecimalCompact(..)
        | Value::Decimal128(..)
        | Value::DecimalWide(_) => crate::storage::value_to_string(value)
            .parse::<f64>()
            .map_err(|_| invalid("invalid numeric value"))?,
        _ => return Err(invalid("arguments must be non-NULL numeric values")),
    };
    if !n.is_finite() {
        return Err(invalid("arguments must be finite"));
    }
    Ok(n)
}
fn evaluate(expr: &Expr) -> Result<Value, QueryError> {
    if !constant(expr) {
        return Err(invalid(
            "arguments require constants, arithmetic, numeric CAST or ? parameters",
        ));
    }
    crate::engine::expr::eval_const_runtime(expr).map_err(|err| invalid(err.message()))
}
fn resolve(spec: &TableSampleSpec) -> Result<Resolved, QueryError> {
    let value = evaluate(&spec.amount)?;
    let amount = numeric_amount(&value)?;
    let rows = if spec.unit == SampleUnit::Rows {
        match &value {
            Value::UInt(n) => *n,
            Value::Int(n) if *n >= 0 => *n as u64,
            Value::Decimal(_)
            | Value::DecimalCompact(..)
            | Value::Decimal128(..)
            | Value::DecimalWide(_) => {
                let text = crate::storage::value_to_string(&value);
                let (whole, fraction) = text.split_once('.').unwrap_or((&text, ""));
                if fraction.bytes().any(|b| b != b'0') {
                    return Err(invalid("ROWS must be a nonnegative integer"));
                }
                whole
                    .parse::<u64>()
                    .map_err(|_| invalid("ROWS exceeds the nonnegative u64 range"))?
            }
            _ if amount >= 0.0 && amount < 18446744073709551616.0 && amount.fract() == 0.0 => {
                amount as u64
            }
            _ => {
                return Err(invalid(
                    "ROWS must be a nonnegative integer in the u64 range",
                ))
            }
        }
    } else {
        if !(0.0..=100.0).contains(&amount) {
            return Err(invalid("PERCENT must be between 0 and 100"));
        }
        if matches!(
            value,
            Value::Decimal(_)
                | Value::DecimalCompact(..)
                | Value::Decimal128(..)
                | Value::DecimalWide(_)
        ) {
            let (coefficient, scale) = crate::util::decimal::parse_exact_decimal_big(
                &crate::storage::value_to_string(&value),
            )
            .ok_or_else(|| invalid("invalid percentage"))?;
            if coefficient
                > num_bigint::BigInt::from(100) * num_bigint::BigInt::from(10).pow(u32::from(scale))
            {
                return Err(invalid("PERCENT must be between 0 and 100"));
            }
        }
        0
    };
    let seed = match &spec.seed {
        Some(expr) => {
            let value = evaluate(expr)?;
            let n = numeric_amount(&value)?;
            // Hash normalized exact coefficients when available, preserving
            // integer seeds above 2^53 and equality across decimal scales.
            if let Some((mut coefficient, mut scale)) =
                crate::util::decimal::parse_exact_decimal_big(&crate::storage::value_to_string(
                    &value,
                ))
            {
                use num_traits::Zero;
                while scale > 0 && (&coefficient % 10u8).is_zero() {
                    coefficient /= 10u8;
                    scale -= 1;
                }
                coefficient
                    .to_signed_bytes_le()
                    .into_iter()
                    .fold(mix(u64::from(scale)), |hash, byte| {
                        mix(hash ^ u64::from(byte))
                    })
            } else {
                mix(if n == 0.0 { 0 } else { n.to_bits() })
            }
        }
        None => rand::random(),
    };
    Ok(Resolved {
        method: spec.method,
        unit: spec.unit,
        percent: amount,
        rows,
        seed,
    })
}

struct Collect(Vec<(crate::parser::TableRef, Box<TableSampleSpec>)>);
impl crate::parser::visit::AstVisitor for Collect {
    fn enter_select(&mut self, select: &Select) {
        for table in select
            .from
            .iter()
            .chain(select.joins.iter().map(|j| &j.table))
        {
            if let Some(sample) = &table.sample {
                self.0.push((table.clone(), sample.clone()));
            }
        }
    }
}
pub(crate) fn preflight_statement(
    db: &Arc<Database>,
    statement: &Statement,
) -> Result<Option<Bindings>, QueryError> {
    let mut collect = Collect(Vec::new());
    crate::parser::visit::visit_statement(&mut collect, statement);
    resolve_collected(db, collect)
}

fn resolve_collected(db: &Arc<Database>, collect: Collect) -> Result<Option<Bindings>, QueryError> {
    if collect.0.is_empty() {
        return Ok(None);
    }
    let mut bindings = capture().as_deref().cloned().unwrap_or_default();
    for (table, spec) in collect.0 {
        if table.generate_series.is_some()
            || db.is_system_table_name(crate::storage::DEFAULT_DATABASE, &table.name)
            || crate::engine::executor::cte::lookup_cte(&table.name).is_some()
        {
            return Err(crate::error::catalog::ER_NOT_SUPPORTED_YET
                .error("TABLESAMPLE requires a user base table"));
        }
        let schema = db.table(&table.name).map_err(|err| {
            if db
                .view_definition(crate::storage::DEFAULT_DATABASE, &table.name)
                .is_some()
            {
                crate::error::catalog::ER_NOT_SUPPORTED_YET
                    .error("TABLESAMPLE requires a user base table")
            } else {
                err
            }
        })?;
        if schema.system {
            return Err(crate::error::catalog::ER_NOT_SUPPORTED_YET
                .error("TABLESAMPLE requires a user base table"));
        }
        if !table.index_hints.is_empty() {
            return Err(crate::error::catalog::ER_NOT_SUPPORTED_YET
                .error("index hints cannot qualify a TABLESAMPLE source"));
        }
        bindings.insert(spec.binding_id, resolve(&spec)?);
    }
    Ok(Some(Arc::new(bindings)))
}
pub(super) fn preflight(
    db: &Arc<Database>,
    select: &Select,
) -> Result<Option<Bindings>, QueryError> {
    if capture().is_some() {
        return Ok(None);
    }
    let mut collect = Collect(Vec::new());
    crate::parser::visit::visit_select(&mut collect, select);
    resolve_collected(db, collect)
}

#[inline]
fn mix(mut n: u64) -> u64 {
    n = (n ^ (n >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    n = (n ^ (n >> 27)).wrapping_mul(0x94d049bb133111eb);
    n ^ (n >> 31)
}
#[inline]
fn priority(seed: u64, row: DmlRowRef) -> u64 {
    mix(mix(seed ^ row.storage_id ^ 0x726f775f73616d70) ^ pack(row.row_id))
}
fn page_priority(seed: u64, storage_id: u64, page: u64) -> u64 {
    mix(mix(seed ^ storage_id ^ 0x706167655f73616d) ^ page)
}
fn pack(row: RowId) -> u64 {
    (u64::from(row.page_id) << 16) | u64::from(row.slot_id)
}
fn unpack(row: u64) -> RowId {
    RowId::new((row >> 16) as u32, row as u16)
}
fn selected(hash: u64, percent: f64) -> bool {
    percent >= 100.0
        || (percent > 0.0 && u128::from(hash) < (percent / 100.0 * 18446744073709551616.0) as u128)
}
/// Six-round Feistel permutation on the next power-of-four domain, with
/// cycle walking to obtain a bijection on [0,n) without a page-number array.
fn permute(mut ordinal: u64, n: u64, seed: u64) -> u64 {
    if n <= 1 {
        return 0;
    }
    let half = (64 - (n - 1).leading_zeros()).div_ceil(2);
    let mask = (1u64 << half) - 1;
    loop {
        let (mut left, mut right) = (ordinal >> half, ordinal & mask);
        for round in 0u64..6 {
            (left, right) = (
                right,
                left ^ (mix(right ^ seed.wrapping_add(round.wrapping_mul(0x9e3779b97f4a7c15)))
                    & mask),
            );
        }
        ordinal = (left << half) | right;
        if ordinal < n {
            return ordinal;
        }
    }
}

#[derive(Clone, Copy)]
struct Task {
    storage_id: u64,
    lo: u64,
    hi: u64,
}
struct Tasks {
    heaps: Vec<(u64, u64)>,
    total: u64,
    ordinal: u64,
    heap: usize,
    page: u64,
    sample: Resolved,
    morsel_pages: u64,
}
impl Tasks {
    fn next(&mut self) -> Option<Task> {
        if self.sample.unit == SampleUnit::Rows && self.sample.method == SampleMethod::System {
            if self.ordinal == self.total {
                return None;
            }
            let page = permute(self.ordinal, self.total, self.sample.seed);
            self.ordinal += 1;
            let i = self.heaps.partition_point(|(_, end)| *end <= page);
            let lo = page - if i == 0 { 0 } else { self.heaps[i - 1].1 };
            return Some(Task {
                storage_id: self.heaps[i].0,
                lo,
                hi: lo + 1,
            });
        }
        while self.heap < self.heaps.len() {
            let (storage_id, end) = self.heaps[self.heap];
            let pages = end
                - if self.heap == 0 {
                    0
                } else {
                    self.heaps[self.heap - 1].1
                };
            if self.page == pages {
                self.heap += 1;
                self.page = 0;
                continue;
            }
            let lo = self.page;
            self.page += 1;
            if self.sample.method == SampleMethod::System && self.sample.unit == SampleUnit::Percent
            {
                if !selected(
                    page_priority(self.sample.seed, storage_id, lo),
                    self.sample.percent,
                ) {
                    continue;
                }
                while self.page < pages
                    && self.page - lo < self.morsel_pages
                    && selected(
                        page_priority(self.sample.seed, storage_id, self.page),
                        self.sample.percent,
                    )
                {
                    self.page += 1;
                }
            } else {
                self.page = pages.min(lo + self.morsel_pages);
            }
            return Some(Task {
                storage_id,
                lo,
                hi: self.page,
            });
        }
        None
    }
}
struct RefChunk {
    rows: Vec<DmlRowRef>,
    _charge: ScopedCharge,
}
fn scan_refs(
    db: &Arc<Database>,
    table: &PlanTable,
    txn: TxnView,
    task: Task,
    sample: Resolved,
) -> Result<RefChunk, QueryError> {
    let mut rows = Vec::new();
    let mut charge = ScopedCharge::new(0);
    db.scan_rows_columns_storage_streaming_range_with_ids(
        &table.schema,
        task.storage_id,
        txn,
        &[],
        task.lo,
        task.hi,
        |row_id, _| {
            crate::engine::cancel::check_cancelled()?;
            let row = DmlRowRef {
                storage_id: task.storage_id,
                row_id,
            };
            if sample.method == SampleMethod::Bernoulli
                && sample.unit == SampleUnit::Percent
                && !selected(priority(sample.seed, row), sample.percent)
            {
                return Ok(());
            }
            if rows.len() == rows.capacity() {
                let additional = rows.capacity().max(64);
                mem_budget::try_reserve_vec_hard(
                    &mut rows,
                    additional,
                    &mut charge,
                    "sample row references",
                )?;
            }
            rows.push(row);
            Ok(())
        },
    )?;
    // Historical versions are deferred by ordinary scans; root-slot sorting
    // makes truncation independent of that callback order.
    if sample.method == SampleMethod::System && sample.unit == SampleUnit::Rows {
        rows.sort_unstable();
    }
    SAMPLE_PAGES.fetch_add(task.hi - task.lo, std::sync::atomic::Ordering::Relaxed);
    Ok(RefChunk {
        rows,
        _charge: charge,
    })
}
struct Lane {
    send: Option<mpsc::SyncSender<Task>>,
    receive: mpsc::Receiver<Result<RefChunk, QueryError>>,
}
struct RefScan {
    db: Arc<Database>,
    table: PlanTable,
    txn: TxnView,
    tasks: Tasks,
    lanes: Vec<Lane>,
    workers: Vec<std::thread::JoinHandle<()>>,
    context: super::execution_context::ExecutionContext,
    _pressure: Option<crate::util::exec_permit::ParallelReadPressureGuard>,
    next_lane: usize,
    remaining: Option<u64>,
    finished: bool,
}
impl RefScan {
    fn new(
        db: Arc<Database>,
        table: PlanTable,
        txn: TxnView,
        sample: Resolved,
        degree: usize,
    ) -> Result<Self, QueryError> {
        let mut total = 0u64;
        let mut heaps = Vec::new();
        let mut ids = table
            .explicit_partition_storage_ids
            .clone()
            .unwrap_or_else(|| table.schema.storage_ids());
        ids.sort_unstable();
        ids.dedup();
        let zero = (sample.unit == SampleUnit::Rows && sample.rows == 0)
            || (sample.unit == SampleUnit::Percent && sample.percent == 0.0);
        if !zero {
            for storage_id in ids {
                total = total
                    .checked_add(db.heap_page_count_for_storage(storage_id)?)
                    .ok_or_else(|| invalid("page count overflow"))?;
                heaps.push((storage_id, total));
            }
        }
        let context =
            super::execution_context::ExecutionContext::capture(txn).fork_worker_cancellation();
        let mut scan = Self {
            db,
            table,
            txn,
            tasks: Tasks {
                heaps,
                total,
                ordinal: 0,
                heap: 0,
                page: 0,
                sample,
                morsel_pages: mem_budget::current_budget().map_or(32, |budget| {
                    (budget.limit() / degree.max(1) / crate::storage::PAGE_SIZE / 8).clamp(1, 32)
                        as u64
                }),
            },
            lanes: Vec::new(),
            workers: Vec::new(),
            context,
            _pressure: None,
            next_lane: 0,
            remaining: (sample.unit == SampleUnit::Rows && sample.method == SampleMethod::System)
                .then_some(sample.rows),
            finished: zero,
        };
        let degree = degree
            .min(usize::try_from(total).unwrap_or(usize::MAX))
            .max(1);
        if degree > 1 && !zero {
            scan._pressure = scan.context.enter_parallel_read_pressure()?;
            for _ in 0..degree {
                let Some(task) = scan.tasks.next() else { break };
                let (send, receive_task) = mpsc::sync_channel::<Task>(1);
                let (send_result, receive) = mpsc::sync_channel(1);
                let db = scan.db.clone();
                let table = scan.table.clone();
                let context = scan.context.clone();
                let worker = std::thread::Builder::new()
                    .name("mydb-sample-scan".into())
                    .spawn(move || {
                        context.run(&db, || {
                            while let Ok(task) = receive_task.recv() {
                                if context.cancellation().check().is_err() {
                                    break;
                                }
                                SAMPLE_WORKER_TASKS
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                let result = scan_refs(&db, &table, txn, task, sample);
                                let failed = result.is_err();
                                if send_result.send(result).is_err() || failed {
                                    break;
                                }
                            }
                        });
                    })
                    .map_err(|e| invalid(format!("cannot start sampling worker: {e}")))?;
                scan.workers.push(worker);
                send.send(task)
                    .map_err(|_| invalid("sampling worker stopped"))?;
                scan.lanes.push(Lane {
                    send: Some(send),
                    receive,
                });
            }
        }
        Ok(scan)
    }
    fn next(&mut self) -> Result<Option<RefChunk>, QueryError> {
        if self.finished {
            return Ok(None);
        }
        if let Some(error) = self.context.budget().terminal_oom() {
            return Err(error);
        }
        self.context.cancellation().check()?;
        crate::engine::cancel::check_cancelled()?;
        let chunk = if self.lanes.is_empty() {
            let Some(task) = self.tasks.next() else {
                self.finished = true;
                return Ok(None);
            };
            scan_refs(&self.db, &self.table, self.txn, task, self.tasks.sample)?
        } else {
            let lane = &mut self.lanes[self.next_lane];
            if lane.send.is_none() {
                self.finished = true;
                return Ok(None);
            }
            let result = lane.receive.recv().map_err(|_| {
                self.context
                    .budget()
                    .terminal_oom()
                    .or_else(|| self.context.cancellation().check().err())
                    .unwrap_or_else(|| invalid("sampling worker terminated unexpectedly"))
            })?;
            let chunk = result?;
            if let Some(task) = self.tasks.next() {
                lane.send
                    .as_ref()
                    .unwrap()
                    .send(task)
                    .map_err(|_| invalid("sampling worker stopped"))?;
            } else {
                lane.send.take();
            }
            self.next_lane = (self.next_lane + 1) % self.lanes.len();
            chunk
        };
        let mut chunk = chunk;
        if let Some(remaining) = &mut self.remaining {
            chunk
                .rows
                .truncate((*remaining).min(chunk.rows.len() as u64) as usize);
            *remaining -= chunk.rows.len() as u64;
            if *remaining == 0 {
                self.finished = true;
            }
        }
        Ok(Some(chunk))
    }
}
impl Drop for RefScan {
    fn drop(&mut self) {
        self.context.cancellation().cancel_local();
        self.lanes.clear();
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

mod output;
pub(in crate::engine::executor) use output::selected_refs;
pub(super) use output::{open, SampleBatchStream};

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn permutations_are_complete() {
        for n in [0, 1, 2, 3, 4, 5, 17, 63, 64, 65, 257, 1021, 4097] {
            for seed in [0, 1, 42, u64::MAX] {
                let mut pages = (0..n).map(|i| permute(i, n, seed)).collect::<Vec<_>>();
                pages.sort_unstable();
                assert_eq!(pages, (0..n).collect::<Vec<_>>());
            }
        }
    }

    #[test]
    fn tablesample_hashes_have_expected_row_and_page_inclusion() {
        let mut row_counts = [0usize; 100];
        let mut page_counts = [0usize; 100];
        for seed in 0..1024 {
            for i in 0..100 {
                let row = DmlRowRef {
                    storage_id: 17,
                    row_id: RowId::new(i as u32, 3),
                };
                row_counts[i] += usize::from(selected(priority(seed, row), 10.0));
                page_counts[i] += usize::from(selected(page_priority(seed, 17, i as u64), 10.0));
            }
        }
        for count in row_counts.into_iter().chain(page_counts) {
            assert!((44..=161).contains(&count), "{count}");
        }
    }

    #[test]
    fn tablesample_identity_components_cannot_cancel_each_other() {
        for seed in [0, 1, 42, u64::MAX] {
            assert_ne!(page_priority(seed, 1, 1), page_priority(seed, 2, 2));
            let a = DmlRowRef {
                storage_id: 65536,
                row_id: RowId::new(1, 0),
            };
            let b = DmlRowRef {
                storage_id: 131072,
                row_id: RowId::new(2, 0),
            };
            assert_ne!(priority(seed, a), priority(seed, b));
        }
    }

    #[test]
    fn tablesample_cancel_disconnects_prefetched_workers_and_releases_charges() {
        use crate::engine::executor::StorageQueryEngine;
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
            dir.path(),
            crate::engine::executor::tests::memory_test_storage_config(),
        )
        .unwrap();
        engine
            .execute("CREATE TABLE cancel_sample(id INT, s VARCHAR(128))")
            .unwrap();
        engine.execute("INSERT INTO cancel_sample SELECT n,REPEAT('x',100) FROM generate_series(1,4096) g(n)").unwrap();
        let db = engine.database();
        let schema = db.table("cancel_sample").unwrap();
        let table = PlanTable::new(schema.id, schema, None);
        let txn = TxnView::read_only(
            db.visible_scn(),
            crate::storage::IsolationLevel::ReadCommitted,
        );
        for oom in [false, true] {
            let budget = Arc::new(mem_budget::MemBudget::new(4 * 1024 * 1024));
            let token = crate::engine::cancel::QueryCancelToken::noop();
            let _token = crate::engine::cancel::enter(token.clone());
            mem_budget::with_query_budget(budget.clone(), || {
                let sample = Resolved {
                    method: SampleMethod::System,
                    unit: SampleUnit::Percent,
                    percent: 100.0,
                    rows: 0,
                    seed: 42,
                };
                let mut scan = RefScan::new(db.clone(), table.clone(), txn, sample, 4).unwrap();
                assert!(!scan.workers.is_empty());
                if oom {
                    budget.latch_terminal_oom(
                        crate::error::catalog::ER_OUTOFMEMORY.error("injected sampling worker OOM"),
                    );
                } else {
                    token.cancel_local();
                }
                match scan.next() {
                    Err(error) => assert_eq!(error.code(), if oom { 1037 } else { 1317 }),
                    _ => panic!("cancelled scan continued"),
                }
                drop(scan);
            });
            assert_eq!(budget.used(), 0);
        }
    }
}
