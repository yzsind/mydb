//! Physical sampling over MVCC-visible root heap identities. Predicates are
//! deliberately evaluated by consumers, after the sample has been selected.
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{mpsc, Arc};

use crate::engine::executor::mem_budget::{self, ScopedCharge};
use crate::engine::executor::vec::batch::{Batch, ColumnAccumulator, DEFAULT_BATCH_ROWS};
use crate::engine::executor::vec::schema::{BatchField, BatchSchema};
use crate::engine::executor::vec::stream::BatchOperator;
use crate::parser::{Expr, SampleMethod, SampleUnit, Select, Statement, TableSampleSpec};
use crate::planner::{PhysicalPlan, PlanTable};
use crate::storage::{Database, DmlRowRef, RowId, TxnView, VisibleRootScanScratch};
use crate::types::{QueryError, Value};

#[cfg(test)]
pub(crate) static SAMPLE_WORKER_TASKS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
#[cfg(test)]
pub(crate) static SAMPLE_PAGES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[derive(Clone, Copy, Debug)]
pub(crate) struct Resolved {
    method: SampleMethod,
    unit: SampleUnit,
    percent: f64,
    threshold: u128,
    rows: u64,
    seed: u64,
}
pub(crate) type Bindings = Arc<HashMap<u32, Resolved>>;
thread_local! { static FULL_PERCENT: std::cell::Cell<bool> = const {std::cell::Cell::new(false)}; }
thread_local! { static CURRENT: RefCell<Option<Bindings>> = const { RefCell::new(None) }; }
/// Full aggregations observe membership, not the sampler's output order.
/// LIMIT hints still take precedence when opening a sampled descendant.
pub(super) struct FullPercentScope(bool);
pub(super) fn enter_full_percent_consumer() -> FullPercentScope {
    FullPercentScope(FULL_PERCENT.with(|slot| slot.replace(true)))
}
impl Drop for FullPercentScope {
    fn drop(&mut self) {
        FULL_PERCENT.with(|slot| slot.set(self.0));
    }
}
pub(super) fn full_percent_consumer_scope(plan: &PhysicalPlan) -> Option<FullPercentScope> {
    if matches!(plan, PhysicalPlan::Limit { .. })
        || matches!(plan,PhysicalPlan::TopWindow {order_by,..} if order_by.is_empty())
    {
        return full_percent_allowed()
            .then(|| FullPercentScope(FULL_PERCENT.with(|slot| slot.replace(false))));
    }
    matches!(
        plan,
        PhysicalPlan::HashAggregate { .. }
            | PhysicalPlan::StreamingAggregate { .. }
            | PhysicalPlan::ParallelHashAggregate { .. }
            | PhysicalPlan::HashRollupAggregate { .. }
            | PhysicalPlan::HashGroupingSetsAggregate { .. }
            | PhysicalPlan::StreamingRollupAggregate { .. }
            | PhysicalPlan::StreamingGroupingSetsAggregate { .. }
            | PhysicalPlan::ParallelRollupAggregate { .. }
            | PhysicalPlan::ParallelGroupingSetsAggregate { .. }
    )
    .then(enter_full_percent_consumer)
}
fn full_percent_allowed() -> bool {
    FULL_PERCENT.with(std::cell::Cell::get)
}
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
        threshold: probability_threshold(amount),
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
#[cfg(test)]
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
#[cfg(test)]
fn selected(hash: u64, percent: f64) -> bool {
    u128::from(hash) < probability_threshold(percent)
}
fn probability_threshold(percent: f64) -> u128 {
    (percent / 100.0 * 18446744073709551616.0) as u128
}
fn round_key(seed: u64, round: u64) -> u64 {
    mix(seed ^ mix(round) ^ 0x706167655f726e64)
}
fn root_key(seed: u64, storage_id: u64, page: u64) -> u64 {
    mix(mix(seed ^ storage_id ^ 0x726f775f726e64) ^ page)
}
/// Six-round Feistel permutation on the next power-of-four domain, with
/// cycle walking to obtain a bijection on [0,n) without a page-number array.
fn permute(ordinal: u64, n: u64, seed: u64) -> u64 {
    permute_direction(ordinal, n, seed, false)
}
fn permute_direction(mut ordinal: u64, n: u64, seed: u64, inverse: bool) -> u64 {
    if n <= 1 {
        return 0;
    }
    let half = (64 - (n - 1).leading_zeros()).div_ceil(2);
    let mask = (1u64 << half) - 1;
    loop {
        let (mut left, mut right) = (ordinal >> half, ordinal & mask);
        for step in 0u64..6 {
            let round = if inverse { 5 - step } else { step };
            let key = seed.wrapping_add(round.wrapping_mul(0x9e3779b97f4a7c15));
            if inverse {
                (left, right) = (right ^ (mix(left ^ key) & mask), left);
            } else {
                (left, right) = (right, left ^ (mix(right ^ key) & mask));
            }
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
    full_percent: bool,
}
impl Tasks {
    fn next(&mut self) -> Option<Task> {
        if (self.sample.method == SampleMethod::Bernoulli && !self.full_percent)
            || self.sample.unit == SampleUnit::Rows
        {
            if self.ordinal == self.total {
                return None;
            }
            let seed = if self.sample.method == SampleMethod::Bernoulli {
                round_key(self.sample.seed, 0)
            } else {
                self.sample.seed
            };
            let page = permute(self.ordinal, self.total, seed);
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
                if u128::from(page_priority(self.sample.seed, storage_id, lo))
                    >= self.sample.threshold
                {
                    continue;
                }
                while self.page < pages
                    && self.page - lo < self.morsel_pages
                    && u128::from(page_priority(self.sample.seed, storage_id, self.page))
                        < self.sample.threshold
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
/// Inline up to four identities from one root page. Common page/storage
/// components are stored once, keeping this variant no larger than a Vec.
/// Sparse percentage pages therefore need no per-page allocation/admission.
enum RowRefs {
    Inline {
        first: DmlRowRef,
        slots: [u16; 3],
        len: u8,
    },
    Heap(Vec<DmlRowRef>),
}
#[inline(always)]
fn push_reference(
    rows: &mut Vec<DmlRowRef>,
    row: DmlRowRef,
    charge: &mut ScopedCharge,
) -> Result<(), QueryError> {
    if rows.len() == rows.capacity() {
        let additional = rows.capacity().max(64);
        mem_budget::try_reserve_vec_hard(rows, additional, charge, "sample row references")?;
    }
    rows.push(row);
    Ok(())
}
impl RowRefs {
    fn inline() -> Self {
        Self::Inline {
            first: DmlRowRef {
                storage_id: 0,
                row_id: RowId::new(0, 0),
            },
            slots: [0; 3],
            len: 0,
        }
    }
    fn len(&self) -> usize {
        match self {
            Self::Inline { len, .. } => usize::from(*len),
            Self::Heap(rows) => rows.len(),
        }
    }
    fn at(&self, index: usize) -> DmlRowRef {
        match self {
            Self::Heap(rows) => rows[index],
            Self::Inline { first, slots, len } => {
                assert!(index < usize::from(*len));
                if index == 0 {
                    *first
                } else {
                    DmlRowRef {
                        storage_id: first.storage_id,
                        row_id: RowId::new(first.row_id.page_id, slots[index - 1]),
                    }
                }
            }
        }
    }
    fn iter(&self) -> impl ExactSizeIterator<Item = DmlRowRef> + '_ {
        (0..self.len()).map(|i| self.at(i))
    }
    fn push(&mut self, row: DmlRowRef, charge: &mut ScopedCharge) -> Result<(), QueryError> {
        if let Self::Inline { first, slots, len } = self {
            if *len == 0 {
                *first = row;
                *len = 1;
                return Ok(());
            }
            if *len < 4
                && first.storage_id == row.storage_id
                && first.row_id.page_id == row.row_id.page_id
            {
                slots[usize::from(*len) - 1] = row.row_id.slot_id;
                *len += 1;
                return Ok(());
            }
            let mut heap = Vec::new();
            *charge = ScopedCharge::new(0);
            mem_budget::try_reserve_vec_hard(&mut heap, 64, charge, "sample row references")?;
            heap.extend(self.iter());
            *self = Self::Heap(heap);
        }
        let Self::Heap(rows) = self else {
            unreachable!()
        };
        push_reference(rows, row, charge)
    }
    fn sort_unstable(&mut self) {
        match self {
            Self::Heap(rows) => rows.sort_unstable(),
            Self::Inline { first, slots, len } if *len > 1 => {
                let mut ids = [first.row_id.slot_id, slots[0], slots[1], slots[2]];
                ids[..usize::from(*len)].sort_unstable();
                first.row_id.slot_id = ids[0];
                slots.copy_from_slice(&ids[1..]);
            }
            _ => {}
        }
    }
    fn truncate(&mut self, n: usize) {
        match self {
            Self::Inline { len, .. } => *len = usize::from(*len).min(n) as u8,
            Self::Heap(rows) => rows.truncate(n),
        }
    }
    fn into_vec(self, charge: &mut ScopedCharge) -> Result<Vec<DmlRowRef>, QueryError> {
        match self {
            Self::Heap(rows) => Ok(rows),
            inline => {
                let mut heap = Vec::new();
                *charge = ScopedCharge::new(0);
                mem_budget::try_reserve_vec_hard(
                    &mut heap,
                    inline.len(),
                    charge,
                    "sample reference handoff",
                )?;
                heap.extend(inline.iter());
                Ok(heap)
            }
        }
    }
}
struct RefChunk {
    rows: RowRefs,
    _charge: ScopedCharge,
    task: Task,
}
#[derive(Default)]
struct ScanWork {
    enabled: bool,
    pages: std::sync::atomic::AtomicU64,
    nonempty: std::sync::atomic::AtomicU64,
    roots: std::sync::atomic::AtomicU64,
}
fn scan_refs(
    db: &Arc<Database>,
    table: &PlanTable,
    txn: TxnView,
    task: Task,
    sample: Resolved,
    work: &ScanWork,
    scratch: &mut Option<VisibleRootScanScratch>,
) -> Result<RefChunk, QueryError> {
    if work.enabled {
        work.pages
            .fetch_add(task.hi - task.lo, std::sync::atomic::Ordering::Relaxed);
    }
    let mut rows = if sample.method == SampleMethod::Bernoulli && sample.unit == SampleUnit::Percent
    {
        RowRefs::inline()
    } else {
        RowRefs::Heap(Vec::new())
    };
    let mut charge =
        if sample.method == SampleMethod::Bernoulli && sample.unit == SampleUnit::Percent {
            ScopedCharge::for_budget(None)
        } else {
            ScopedCharge::new(0)
        };
    let threshold = sample.threshold;
    let prefix = mix(sample.seed ^ task.storage_id ^ 0x726f775f73616d70);
    let mut visit = |row_id| {
        let row = DmlRowRef {
            storage_id: task.storage_id,
            row_id,
        };
        rows.push(row, &mut charge)
    };
    let mut examined = 0u64;
    if sample.method == SampleMethod::Bernoulli {
        if scratch.is_none() {
            *scratch = Some(db.visible_root_scan_scratch(&table.schema, txn)?);
        }
        db.scan_visible_root_ids_with_scratch(
            &table.schema,
            task.storage_id,
            task.lo,
            task.hi,
            scratch.as_mut().unwrap(),
            |row_id| {
                examined += 1;
                sample.unit != SampleUnit::Percent
                    || u128::from(mix(prefix ^ pack(row_id))) < threshold
            },
            &mut visit,
        )?;
    } else {
        let RowRefs::Heap(heap_rows) = &mut rows else {
            unreachable!()
        };
        db.scan_rows_columns_storage_streaming_range_with_ids(
            &table.schema,
            task.storage_id,
            txn,
            &[],
            task.lo,
            task.hi,
            |row_id, _| {
                crate::engine::cancel::check_cancelled()?;
                push_reference(
                    heap_rows,
                    DmlRowRef {
                        storage_id: task.storage_id,
                        row_id,
                    },
                    &mut charge,
                )
            },
        )?;
    }
    // Historical versions are deferred by ordinary scans; root-slot sorting
    // makes truncation independent of that callback order.
    if sample.method == SampleMethod::Bernoulli || sample.unit == SampleUnit::Rows {
        rows.sort_unstable();
    }
    #[cfg(test)]
    SAMPLE_PAGES.fetch_add(task.hi - task.lo, std::sync::atomic::Ordering::Relaxed);
    if work.enabled {
        let mut previous = None;
        let mut pages = 0u64;
        for row in rows.iter() {
            let page = (row.storage_id, row.row_id.page_id);
            if previous != Some(page) {
                pages += 1;
                previous = Some(page);
            }
        }
        work.nonempty
            .fetch_add(pages, std::sync::atomic::Ordering::Relaxed);
        work.roots
            .fetch_add(examined, std::sync::atomic::Ordering::Relaxed);
    }
    Ok(RefChunk {
        rows,
        _charge: charge,
        task,
    })
}
enum Work {
    One(Task),
    Many(Vec<Task>, ScopedCharge),
}
enum Completed {
    One(RefChunk),
    Many(Vec<RefChunk>, ScopedCharge),
}
fn scan_work(
    db: &Arc<Database>,
    table: &PlanTable,
    txn: TxnView,
    work: Work,
    sample: Resolved,
    counters: &ScanWork,
    scratch: &mut Option<VisibleRootScanScratch>,
) -> Result<Completed, QueryError> {
    match work {
        Work::One(task) => {
            #[cfg(test)]
            SAMPLE_WORKER_TASKS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(Completed::One(scan_refs(
                db, table, txn, task, sample, counters, scratch,
            )?))
        }
        Work::Many(tasks, _charge) => {
            let mut chunks = Vec::new();
            let mut charge = ScopedCharge::new(0);
            mem_budget::try_reserve_vec_hard(
                &mut chunks,
                tasks.len(),
                &mut charge,
                "sample completed page window",
            )?;
            for task in tasks {
                crate::engine::cancel::check_cancelled()?;
                #[cfg(test)]
                SAMPLE_WORKER_TASKS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                chunks.push(scan_refs(db, table, txn, task, sample, counters, scratch)?);
            }
            Ok(Completed::Many(chunks, charge))
        }
    }
}
struct Lane {
    send: Option<mpsc::SyncSender<Work>>,
    receive: mpsc::Receiver<Result<Completed, QueryError>>,
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
    batch_pages: usize,
    pending: std::vec::IntoIter<RefChunk>,
    pending_charge: ScopedCharge,
    remaining: Option<u64>,
    finished: bool,
    work: Arc<ScanWork>,
    scratch: Option<VisibleRootScanScratch>,
}
impl RefScan {
    fn new(
        db: Arc<Database>,
        table: PlanTable,
        txn: TxnView,
        sample: Resolved,
        degree: usize,
        full_percent: bool,
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
                full_percent,
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
            batch_pages: 1,
            pending: Vec::new().into_iter(),
            pending_charge: ScopedCharge::new(0),
            remaining: (sample.unit == SampleUnit::Rows && sample.method == SampleMethod::System)
                .then_some(sample.rows),
            finished: zero,
            work: Arc::new(ScanWork {
                enabled: cfg!(test) || super::analyze_stats::is_recording(),
                ..Default::default()
            }),
            scratch: None,
        };
        let degree = if sample.method == SampleMethod::Bernoulli
            && sample.unit == SampleUnit::Rows
            && sample.rows <= 4
        {
            1
        } else {
            degree
        };
        let degree = degree.min(mem_budget::current_budget().map_or(degree, |budget| {
            (budget.limit() / crate::storage::PAGE_SIZE / 16).max(1)
        }));
        let degree = degree
            .min(usize::try_from(total).unwrap_or(usize::MAX))
            .max(1);
        if degree > 1
            && sample.method == SampleMethod::Bernoulli
            && super::vec_bridge::scan_batch_rows() == DEFAULT_BATCH_ROWS
        {
            let budget_pages = mem_budget::current_budget().map_or(16, |budget| {
                (budget.limit() / degree / crate::storage::PAGE_SIZE / 32).max(1)
            });
            let goal_pages = if sample.unit == SampleUnit::Rows {
                usize::try_from(sample.rows / degree as u64 / 4)
                    .unwrap_or(usize::MAX)
                    .max(1)
            } else {
                16
            };
            scan.batch_pages = 16.min(budget_pages).min(goal_pages).min(
                usize::try_from(total / degree as u64)
                    .unwrap_or(usize::MAX)
                    .max(1),
            );
        }
        if degree > 1 && !zero {
            scan._pressure = scan.context.enter_parallel_read_pressure()?;
            for _ in 0..degree {
                let Some(task) = scan.next_work()? else { break };
                let (send, receive_task) = mpsc::sync_channel::<Work>(1);
                let (send_result, receive) = mpsc::sync_channel(1);
                let db = scan.db.clone();
                let table = scan.table.clone();
                let context = scan.context.clone();
                let work = scan.work.clone();
                let worker = crate::util::thread_spawn::Builder::new()
                    .name("mydb-sample-scan".into())
                    .spawn(move || {
                        context.run(&db, || {
                            let mut scratch = None;
                            while let Ok(task) = receive_task.recv() {
                                if context.cancellation().check().is_err() {
                                    break;
                                }
                                let result =
                                    scan_work(&db, &table, txn, task, sample, &work, &mut scratch);
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
    fn next_work(&mut self) -> Result<Option<Work>, QueryError> {
        let Some(first) = self.tasks.next() else {
            return Ok(None);
        };
        if self.batch_pages == 1 {
            return Ok(Some(Work::One(first)));
        }
        let mut tasks = Vec::new();
        let mut charge = ScopedCharge::new(0);
        mem_budget::try_reserve_vec_hard(
            &mut tasks,
            self.batch_pages,
            &mut charge,
            "sample page window",
        )?;
        tasks.push(first);
        for _ in 1..self.batch_pages {
            let Some(task) = self.tasks.next() else { break };
            tasks.push(task);
        }
        Ok(Some(Work::Many(tasks, charge)))
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
        let chunk = if let Some(chunk) = self.pending.next() {
            chunk
        } else if self.lanes.is_empty() {
            let Some(task) = self.tasks.next() else {
                self.finished = true;
                return Ok(None);
            };
            scan_refs(
                &self.db,
                &self.table,
                self.txn,
                task,
                self.tasks.sample,
                &self.work,
                &mut self.scratch,
            )?
        } else {
            let lane_index = self.next_lane;
            let lane = &self.lanes[lane_index];
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
            let chunk = match result? {
                Completed::One(chunk) => chunk,
                Completed::Many(chunks, charge) => {
                    self.pending = chunks.into_iter();
                    self.pending_charge = charge;
                    self.pending.next().expect("nonempty sample page window")
                }
            };
            if let Some(task) = self.next_work()? {
                self.lanes[lane_index]
                    .send
                    .as_ref()
                    .unwrap()
                    .send(task)
                    .map_err(|_| invalid("sampling worker stopped"))?;
            } else {
                self.lanes[lane_index].send.take();
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
mod rounds;
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
                for ordinal in 0..n {
                    assert_eq!(
                        permute_direction(permute(ordinal, n, seed), n, seed, true),
                        ordinal
                    );
                }
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
        for (oom, method) in [
            (false, SampleMethod::System),
            (true, SampleMethod::System),
            (false, SampleMethod::Bernoulli),
            (true, SampleMethod::Bernoulli),
        ] {
            let budget = Arc::new(mem_budget::MemBudget::new(4 * 1024 * 1024));
            let token = crate::engine::cancel::QueryCancelToken::noop();
            let _token = crate::engine::cancel::enter(token.clone());
            mem_budget::with_query_budget(budget.clone(), || {
                let sample = Resolved {
                    method,
                    unit: SampleUnit::Percent,
                    percent: 100.0,
                    threshold: probability_threshold(100.0),
                    rows: 0,
                    seed: 42,
                };
                let mut scan =
                    RefScan::new(db.clone(), table.clone(), txn, sample, 4, false).unwrap();
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
