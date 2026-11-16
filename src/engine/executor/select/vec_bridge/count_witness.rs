//! Discard redundant rows after an exact monotone grouped-count witness.
use super::*;
use crate::parser::BinaryOp;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

pub(crate) static ACTIVATIONS: AtomicU64 = AtomicU64::new(0);
pub(crate) static CANDIDATES: AtomicU64 = AtomicU64::new(0);
pub(crate) static ROWS_SKIPPED: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
pub(crate) static HITS: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
pub(super) struct Rule {
    groups: Vec<Expr>,
    projected: usize,
    threshold: u16,
}
thread_local! {
    static RULE: std::cell::RefCell<Option<(usize,Rule)>> = const { std::cell::RefCell::new(None) };
}
pub(super) fn with_rule<T>(rule: Option<(usize, Rule)>, run: impl FnOnce() -> T) -> T {
    if rule.is_none() {
        return run();
    }
    struct Restore(Option<(usize, Rule)>);
    impl Drop for Restore {
        fn drop(&mut self) {
            RULE.with(|rule| *rule.borrow_mut() = self.0.take());
        }
    }
    let _restore = Restore(RULE.with(|slot| slot.replace(rule)));
    run()
}
pub(super) fn current(plan: &PhysicalPlan) -> Option<Rule> {
    RULE.with(|slot| {
        slot.borrow()
            .as_ref()
            .filter(|(key, _)| *key == plan as *const _ as usize)
            .map(|(_, rule)| rule.clone())
    })
}
fn count_star(expr: &Expr) -> bool {
    matches!(expr,Expr::Function {name,args,distinct:false} if name.eq_ignore_ascii_case("COUNT") && matches!(args.as_slice(),[Expr::Wildcard]))
}
fn bound(expr: &Expr) -> Option<i128> {
    match expr {
        Expr::Literal(crate::parser::Literal::Int(v)) | Expr::BoundValue(Value::Int(v)) => {
            Some(i128::from(*v))
        }
        Expr::Literal(crate::parser::Literal::UInt(v)) | Expr::BoundValue(Value::UInt(v)) => {
            Some(i128::from(*v))
        }
        Expr::Placeholder(index) => {
            crate::engine::params::with_current_param(*index, |value| match value {
                Some(Value::Int(v)) => Some(i128::from(*v)),
                Some(Value::UInt(v)) => Some(i128::from(*v)),
                _ => None,
            })
        }
        _ => None,
    }
}
/// Only a complete query-output boundary may use this rule. Looking through
/// Sort/Limit or another consumer could expose a hidden COUNT or fine key.
/// The surviving witness has the original COUNT threshold; DISTINCT makes
/// every additional qualifying fine group of that projected key redundant.
pub(super) fn prove(plan: &PhysicalPlan) -> Option<(usize, Rule)> {
    if crate::util::env::var("MYDB_GROUP_COUNT_WITNESS")
        .is_ok_and(|v| matches!(v.to_ascii_lowercase().as_str(), "off" | "0" | "false"))
    {
        return None;
    }
    let plan = match plan {
        PhysicalPlan::Materialize { input, .. } => return prove(input),
        _ => plan,
    };
    let PhysicalPlan::Projection {
        input,
        projection,
        distinct: true,
        ..
    } = plan
    else {
        return None;
    };
    let [SelectItem::Expr {
        expr: Expr::GroupKeyRef { index, .. },
        ..
    }] = projection.as_slice()
    else {
        return None;
    };
    let aggregate = input.ordinary_aggregate_input()?;
    if aggregate.group_by.len() < 2
        || aggregate.group_by.len() > 8
        || *index >= aggregate.group_by.len()
        || aggregate
            .group_by
            .iter()
            .any(|expr| !matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_)))
        || !matches!(aggregate.aggregates,[expr] if count_star(expr))
    {
        return None;
    }
    let Expr::Binary { left, op, right } = aggregate.having? else {
        return None;
    };
    let (value, inclusive) = if count_star(left) {
        match op {
            BinaryOp::Gt => (bound(right)?, false),
            BinaryOp::GtEq => (bound(right)?, true),
            _ => return None,
        }
    } else if count_star(right) {
        match op {
            BinaryOp::Lt => (bound(left)?, false),
            BinaryOp::LtEq => (bound(left)?, true),
            _ => return None,
        }
    } else {
        return None;
    };
    let threshold = (if inclusive {
        value
    } else {
        value.checked_add(1)?
    })
    .max(1);
    if threshold > 1024 {
        return None;
    }
    CANDIDATES.fetch_add(1, Relaxed);
    Some((
        input.as_ref() as *const _ as usize,
        Rule {
            groups: aggregate.group_by.to_vec(),
            projected: *index,
            threshold: threshold as u16,
        },
    ))
}

const MAX_KEY_BYTES: usize = 256;
const CANDIDATES_PER_PROJECTED_KEY: usize = 3;
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum FixedKey {
    Null,
    Number(i128),
    Date(i32),
    Time(i64),
    DateTime(i64),
    Timestamp(i64),
}
impl FixedKey {
    fn read(column: &ColumnVector, row: usize) -> Self {
        if column.is_null(row) {
            return Self::Null;
        }
        match column {
            ColumnVector::Bool { values, .. } => Self::Number(i128::from(values.get(row))),
            ColumnVector::I64 { data, .. } => Self::Number(i128::from(data[row])),
            ColumnVector::U64 { data, .. } => Self::Number(i128::from(data[row])),
            ColumnVector::Date32 { data, .. } => Self::Date(data[row]),
            ColumnVector::Time64 { data, .. } => Self::Time(data[row]),
            ColumnVector::DateTime64 { data, .. } => Self::DateTime(data[row]),
            ColumnVector::Timestamp64 { data, .. } => Self::Timestamp(data[row]),
            ColumnVector::Null { .. } => Self::Null,
            _ => unreachable!("witness lanes were checked before access"),
        }
    }
}
struct Witness {
    fine_key: Box<[FixedKey]>,
    count: u16,
}
struct State {
    fine: hashbrown::HashMap<FixedKey, Vec<Witness>, ahash::RandomState>,
    admitted: hashbrown::HashSet<FixedKey, ahash::RandomState>,
    max_groups: usize,
    threshold: u16,
    _charge: mem_budget::ScopedCharge,
}
impl State {
    fn new(threshold: u16, group_count: usize) -> Option<Self> {
        let budget = mem_budget::current_budget()?;
        let allowance = (budget.limit() / 16).min(32 << 20).min(budget.remaining());
        // Keep a few exact fine candidates: a short first fine group must
        // not hide a later qualifying group. Charge the actual tuple width,
        // vector storage and geometric hash capacities before retaining any.
        if !(1..=8).contains(&group_count) {
            return None;
        }
        debug_assert!(std::mem::size_of::<FixedKey>() * 8 <= MAX_KEY_BYTES);
        let tuple = std::mem::size_of::<FixedKey>().checked_mul(group_count)?;
        let payload = tuple
            .checked_add(std::mem::size_of::<Witness>())?
            .checked_mul(CANDIDATES_PER_PROJECTED_KEY)?;
        let per_key = (3usize * 64)
            .checked_add(payload)?
            .checked_add(3 * 48 + 2 * 32)?;
        let groups = (allowance.saturating_sub(1024) / per_key).min(65536);
        if groups < 128 {
            return None;
        }
        let bytes = groups.checked_mul(per_key)?.checked_add(1024)?;
        let mut charge = mem_budget::ScopedCharge::new(0);
        if !charge.add_within(bytes, allowance) {
            return None;
        }
        let mut fine = hashbrown::HashMap::with_hasher(ahash::RandomState::new());
        let mut admitted = hashbrown::HashSet::with_hasher(ahash::RandomState::new());
        fine.try_reserve(groups).ok()?;
        admitted.try_reserve(groups).ok()?;
        Some(Self {
            fine,
            admitted,
            max_groups: groups,
            threshold,
            _charge: charge,
        })
    }
    fn skip(&mut self, batch: &Batch, row: usize, groups: &[usize], projected: usize) -> bool {
        let coarse_key = FixedKey::read(&batch.columns()[groups[projected]], row);
        if self.admitted.contains(&coarse_key) {
            return true;
        }
        if self.admitted.len() >= self.max_groups {
            return false;
        }
        let mut full_key = [FixedKey::Null; 8];
        for (slot, &column) in groups.iter().enumerate() {
            full_key[slot] = FixedKey::read(&batch.columns()[column], row);
        }
        let full_key = &full_key[..groups.len()];
        // Ignoring an untracked fine group only misses an optimization; the
        // original aggregate receives every row until an exact witness exists.
        let count = if let Some(candidates) = self.fine.get_mut(&coarse_key) {
            if let Some(witness) = candidates
                .iter_mut()
                .find(|w| w.fine_key.as_ref() == full_key)
            {
                witness.count = (witness.count + 1).min(self.threshold);
                witness.count
            } else if candidates.len() < CANDIDATES_PER_PROJECTED_KEY {
                candidates.push(Witness {
                    fine_key: full_key.into(),
                    count: 1,
                });
                1
            } else {
                return false;
            }
        } else {
            if self.fine.len() >= self.max_groups {
                self.fine.clear();
            }
            let mut candidates = Vec::with_capacity(CANDIDATES_PER_PROJECTED_KEY);
            candidates.push(Witness {
                fine_key: full_key.into(),
                count: 1,
            });
            self.fine.insert(coarse_key, candidates);
            1
        };
        if count >= self.threshold {
            self.admitted.insert(coarse_key);
            self.fine.remove(&coarse_key);
        }
        // Forward the threshold-crossing row itself so the original aggregate
        // retains an exact qualifying fine group for every admitted key.
        false
    }
}
struct WitnessInput<'a> {
    upstream: Box<dyn BatchOperator + 'a>,
    schema: Arc<BatchSchema>,
    groups: Vec<usize>,
    projected: usize,
    state: Option<State>,
}
fn native(column: &ColumnVector) -> bool {
    matches!(
        column,
        ColumnVector::Null { .. }
            | ColumnVector::Bool { .. }
            | ColumnVector::I64 { .. }
            | ColumnVector::U64 { .. }
            | ColumnVector::Date32 { .. }
            | ColumnVector::Time64 { .. }
            | ColumnVector::DateTime64 { .. }
            | ColumnVector::Timestamp64 { .. }
    )
}
pub(super) fn wrap<'a>(input: VecBatchInput<'a>, rule: Rule) -> VecBatchInput<'a> {
    let Some(groups) = rule
        .groups
        .iter()
        .map(|expr| expr_column_index_in_template(expr, &input.template))
        .collect::<Option<Vec<_>>>()
    else {
        return input;
    };
    if groups.iter().any(|&index| {
        let field = &input.stream.schema().fields()[index];
        !matches!(
            field.column_type,
            ColumnType::Tiny
                | ColumnType::Short
                | ColumnType::MediumInt
                | ColumnType::Long
                | ColumnType::LongLong
                | ColumnType::Date
                | ColumnType::Time
                | ColumnType::DateTime
                | ColumnType::Timestamp
                | ColumnType::Year
        ) || field.source.flags
            & (crate::mysql::protocol::ENUM_FLAG | crate::mysql::protocol::SET_FLAG)
            != 0
    }) {
        return input;
    }
    #[cfg(test)]
    HITS.fetch_add(1, Relaxed);
    let schema = input.stream.schema().clone();
    // Reserve optional evidence before the aggregate opens its work area and
    // the upstream joins. A late reservation can lose all remaining budget.
    let state = State::new(rule.threshold, groups.len());
    if state.is_some() {
        ACTIVATIONS.fetch_add(1, Relaxed);
    }
    VecBatchInput {
        template: input.template,
        stream: Box::new(WitnessInput {
            upstream: input.stream,
            schema,
            groups,
            projected: rule.projected,
            state,
        }),
    }
}
impl BatchOperator for WitnessInput<'_> {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }
    fn properties(&self) -> BatchProperties {
        self.upstream.properties()
    }
    fn install_runtime_filter(&mut self, filter: crate::planner::RuntimeFilter) -> bool {
        self.upstream.install_runtime_filter(filter)
    }
    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        loop {
            let Some(batch) = self.upstream.next_batch()? else {
                return Ok(None);
            };
            // A late physical lane change disables optional suppression. Past
            // discarded rows already have a retained qualifying witness.
            if self
                .groups
                .iter()
                .any(|&column| !native(&batch.columns()[column]))
            {
                self.state = None;
            }
            let Some(state) = self.state.as_mut() else {
                return Ok(Some(batch));
            };
            let mut selected = Vec::with_capacity(batch.visible_rows());
            let mut skipped = 0;
            for row in batch.visible_indices() {
                if row % 1024 == 0 {
                    crate::engine::cancel::check_cancelled()?;
                }
                if state.skip(&batch, row, &self.groups, self.projected) {
                    skipped += 1;
                } else {
                    selected.push(row as u32);
                }
            }
            ROWS_SKIPPED.fetch_add(skipped, Relaxed);
            if selected.len() == batch.visible_rows() {
                return Ok(Some(batch));
            }
            if !selected.is_empty() {
                return Ok(Some(batch.with_selection(Selection::Indices(selected))));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn count_witness_can_prove_a_later_fine_group() {
        mem_budget::with_query_budget(Arc::new(mem_budget::MemBudget::new(16 << 20)), || {
            let schema = Arc::new(BatchSchema::new(
                (0..2)
                    .map(|i| BatchField {
                        table: "t".into(),
                        alias: None,
                        name: format!("k{i}"),
                        column_type: ColumnType::LongLong,
                        source: Default::default(),
                    })
                    .collect(),
            ));
            let batch = Batch::new(
                schema,
                vec![
                    ColumnVector::from_values(vec![Value::Int(1); 8]),
                    ColumnVector::from_values(
                        [0, 1, 1, 1, 1, 1, 2, 3]
                            .into_iter()
                            .map(Value::Int)
                            .collect(),
                    ),
                ],
            );
            let mut state = State::new(5, 2).unwrap();
            let skipped = (0..8)
                .map(|row| state.skip(&batch, row, &[0, 1], 0))
                .collect::<Vec<_>>();
            assert_eq!(
                skipped,
                vec![false, false, false, false, false, false, true, true]
            );
        });
    }

    #[test]
    fn count_witness_bounded_epochs_preserve_exact_coarse_key_set() {
        let budget = Arc::new(mem_budget::MemBudget::new(16 << 20));
        mem_budget::with_query_budget(budget.clone(), || {
            let schema = Arc::new(BatchSchema::new(
                (0..2)
                    .map(|i| BatchField {
                        table: "t".into(),
                        alias: None,
                        name: format!("k{i}"),
                        column_type: ColumnType::LongLong,
                        source: Default::default(),
                    })
                    .collect(),
            ));
            for threshold in [1, 2, 5, 1024] {
                for cap in [1, 3, 19, 100] {
                    for seed in [0u64, 23, 1987] {
                        let mut state = State::new(threshold, 2).unwrap();
                        state.max_groups = cap;
                        let mut random = seed;
                        let mut rows = (0..6000)
                            .map(|i| {
                                random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                                (
                                    if i % 11 == 0 {
                                        None
                                    } else {
                                        Some(((random >> 32) % 19) as i64 - 9)
                                    },
                                    ((random >> 16) % 23) as i64,
                                )
                            })
                            .collect::<Vec<_>>();
                        // Include a late qualifying NULL key and a skewed fine
                        // group crossing even the largest supported threshold.
                        rows.extend(std::iter::repeat_n((None, 999), 1100));
                        let batch = Batch::new(
                            schema.clone(),
                            vec![
                                ColumnVector::from_values(
                                    rows.iter()
                                        .map(|(k, _)| k.map(Value::Int).unwrap_or(Value::Null))
                                        .collect(),
                                ),
                                ColumnVector::from_values(
                                    rows.iter().map(|(_, d)| Value::Int(*d)).collect(),
                                ),
                            ],
                        );
                        let mut original = std::collections::HashMap::new();
                        let mut retained = std::collections::HashMap::new();
                        for (row, key) in rows.iter().enumerate() {
                            *original.entry(*key).or_insert(0usize) += 1;
                            if !state.skip(&batch, row, &[0, 1], 0) {
                                *retained.entry(*key).or_insert(0usize) += 1;
                            }
                        }
                        let coarse =
                            |counts: std::collections::HashMap<(Option<i64>, i64), usize>| {
                                counts
                                    .into_iter()
                                    .filter(|(_, n)| *n >= threshold as usize)
                                    .map(|((k, _), _)| k)
                                    .collect::<std::collections::HashSet<_>>()
                            };
                        assert_eq!(
                            coarse(original),
                            coarse(retained),
                            "threshold={threshold} cap={cap} seed={seed}"
                        );
                        assert!(state.fine.len() <= cap && state.admitted.len() <= cap);
                    }
                }
            }
        });
        mem_budget::with_query_budget(budget.clone(), || {
            let schema = Arc::new(BatchSchema::new(
                (0..2)
                    .map(|i| BatchField {
                        table: "t".into(),
                        alias: None,
                        name: format!("k{i}"),
                        column_type: ColumnType::LongLong,
                        source: Default::default(),
                    })
                    .collect(),
            ));
            let rows = (0..7)
                .flat_map(|_| (0..10).flat_map(|day| (0..1000).map(move |key| (key, day))))
                .collect::<Vec<_>>();
            let batch = Batch::new(
                schema,
                vec![
                    ColumnVector::from_values(rows.iter().map(|(k, _)| Value::Int(*k)).collect()),
                    ColumnVector::from_values(rows.iter().map(|(_, d)| Value::Int(*d)).collect()),
                ],
            );
            let mut state = State::new(5, 2).unwrap();
            state.max_groups = 2000;
            let skipped = (0..rows.len())
                .filter(|&row| state.skip(&batch, row, &[0, 1], 0))
                .count();
            assert_eq!(state.admitted.len(), 1000);
            assert_eq!(
                skipped, 29000,
                "interleaved fine groups must retain one witness per coarse key"
            );
        });
        assert_eq!(budget.used(), 0);
        mem_budget::with_query_budget(Arc::new(mem_budget::MemBudget::new(4096)), || {
            assert!(State::new(2, 2).is_none())
        });
    }
}

#[cfg(test)]
mod query_shape_tests {
    use super::*;
    #[test]
    fn count_witness_accepts_joined_group_key_aliases_after_cte_rewrites() {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let engine = crate::engine::StorageQueryEngine::open(dir.path()).unwrap();
        for sql in [
            "CREATE TABLE item(i_item_sk INT PRIMARY KEY,i_item_desc VARCHAR(64))",
            "CREATE TABLE date_dim(d_date_sk INT PRIMARY KEY,d_date DATE,d_year INT,d_moy INT)",
            "CREATE TABLE store_sales(ss_item_sk INT,ss_sold_date_sk INT,ss_customer_sk INT,ss_quantity INT,ss_sales_price DECIMAL(7,2))",
            "CREATE TABLE customer(c_customer_sk INT PRIMARY KEY)",
            "CREATE TABLE catalog_sales(cs_item_sk INT,cs_bill_customer_sk INT,cs_sold_date_sk INT,cs_quantity INT,cs_list_price DECIMAL(7,2))",
            "CREATE TABLE web_sales(ws_item_sk INT,ws_bill_customer_sk INT,ws_sold_date_sk INT,ws_quantity INT,ws_list_price DECIMAL(7,2))",
        ] {engine.execute(sql).unwrap();}
        let sql = include_str!("../../../../../scripts/tpcds/queries/q23.sql")
            .split(';')
            .next()
            .unwrap();
        let crate::parser::Statement::WithCte(mut with) =
            crate::parser::parse_statement(sql).unwrap()
        else {
            panic!()
        };
        crate::engine::executor::cte::apply_pre_materialization_rewrites(
            engine.db_for_tests(),
            "test",
            &mut with.ctes,
            &mut with.body,
            true,
        );
        let cte = with
            .ctes
            .iter()
            .find(|c| c.name == "frequent_ss_items")
            .unwrap();
        let plan = crate::planner::plan_query_physical(engine.db_for_tests(), &cte.query)
            .unwrap()
            .unwrap();
        assert!(prove(&plan).is_some(), "{plan:#?}");
        for sql in [
            "INSERT INTO item VALUES(1,'example')",
            "INSERT INTO date_dim VALUES(1,'2000-01-02',2000,1)",
            "INSERT INTO customer VALUES(1)",
            "INSERT INTO catalog_sales VALUES(1,1,1,1,1.2500)",
            "INSERT INTO web_sales VALUES(1,1,1,1,1.2500)",
        ] {
            engine.execute(sql).unwrap();
        }
        engine
            .execute(&format!(
                "INSERT INTO store_sales VALUES{}",
                std::iter::repeat_n("(1,1,1,1,1.2500)", 600)
                    .collect::<Vec<_>>()
                    .join(",")
            ))
            .unwrap();
        for auto in ["off", "all"] {
            let run = |mode| {
                crate::util::env::with_vars(
                    &[
                        ("MYDB_GROUP_COUNT_WITNESS", Some(mode)),
                        ("MYDB_AUTO_PARAMETERIZE", Some(auto)),
                        ("MYDB_CTE_PARALLEL_MATERIALIZE", Some("off")),
                        ("MYDB_CTE_INLINE_IN_SUBQUERY_MULTI", Some("off")),
                    ],
                    || {
                        let crate::engine::api::QueryResponse::ResultSet(rs) =
                            engine.execute(sql).unwrap()
                        else {
                            panic!("rows")
                        };
                        rs.rows
                    },
                )
            };
            let expected = run("off");
            let skipped = ROWS_SKIPPED.load(Relaxed);
            let candidates = CANDIDATES.load(Relaxed);
            let activations = ACTIVATIONS.load(Relaxed);
            assert_eq!(run("on"), expected);
            assert!(
                ROWS_SKIPPED.load(Relaxed) > skipped,
                "auto={auto}, candidates={}, activations={}",
                CANDIDATES.load(Relaxed) - candidates,
                ACTIVATIONS.load(Relaxed) - activations
            );
            for format in ["TREE", "JSON"] {
                let before = ROWS_SKIPPED.load(Relaxed);
                let candidates = CANDIDATES.load(Relaxed);
                let active = ACTIVATIONS.load(Relaxed);
                let response = crate::util::env::with_vars(
                    &[
                        ("MYDB_GROUP_COUNT_WITNESS", Some("on")),
                        ("MYDB_AUTO_PARAMETERIZE", Some(auto)),
                        ("MYDB_CTE_INLINE_IN_SUBQUERY_MULTI", Some("off")),
                    ],
                    || engine.execute(&format!("EXPLAIN ANALYZE FORMAT={format} {sql}")),
                )
                .unwrap();
                assert!(matches!(
                    response,
                    crate::engine::api::QueryResponse::ResultSet(_)
                ));
                assert!(
                    ROWS_SKIPPED.load(Relaxed) > before,
                    "ANALYZE {format} missed: candidates={} activations={}",
                    CANDIDATES.load(Relaxed) - candidates,
                    ACTIVATIONS.load(Relaxed) - active
                );
            }
        }
    }
}
