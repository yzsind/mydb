//! Dictionary codes replace repeated collated text in DISTINCT set state.
//! Dictionaries own exact representatives; canonical codes only test equality.
use super::*;
use crate::engine::executor::select::grace::{SpillTmpDir, ValueRowReader, ValueRowWriter};
use crate::engine::executor::{mem_budget::ScopedCharge, memory_footprint};
use std::{collections::VecDeque, hash::Hash};

/// Optional state has a bounded query share and participates in work areas.
struct Charge {
    query: ScopedCharge,
    work: crate::engine::executor::work_area::WorkAreaHandle,
    peak: usize,
}
impl Charge {
    fn new() -> Self {
        Self {
            query: ScopedCharge::new(0),
            work: crate::engine::executor::work_area::WorkAreaHandle::register(),
            peak: 0,
        }
    }
    fn add_within(&mut self, bytes: usize, cap: usize) -> bool {
        use crate::engine::executor::work_area::SpillHint;
        let mut hint = self.work.try_charge(bytes);
        if matches!(hint, SpillHint::Spill) {
            hint = self.work.try_borrow_for_pipeline_drain();
        }
        if matches!(hint, SpillHint::Spill) || !self.query.add_within(bytes, cap) {
            self.work.release(bytes);
            return false;
        }
        self.peak = self.peak.max(self.query.charged());
        true
    }
    fn release_bytes(&mut self, bytes: usize) {
        self.query.release_bytes(bytes);
        self.work.release(bytes);
    }
}

type Map<K, V> = hashbrown::HashMap<K, V, ahash::RandomState>;
type Key = crate::engine::expr::InlineKeyBuf;

#[derive(Clone, Copy)]
struct Code {
    canonical: u32,
    raw: u32,
}
#[derive(Clone, Copy)]
struct Entry {
    alternate: u32,
    seen: u8,
}

fn reserve<K: Eq + Hash, V>(map: &mut Map<K, V>, charge: &mut Charge, cap: usize) -> bool {
    if map.len() < map.capacity() {
        return true;
    }
    let old = memory_footprint::hash_table_capacity_bytes::<(K, V)>(map.capacity());
    let next = memory_footprint::hash_table_capacity_bytes::<(K, V)>(if map.capacity() == 0 {
        3
    } else {
        map.capacity().saturating_mul(2)
    });
    if !charge.add_within(next, cap) {
        return false;
    }
    if map.try_reserve(1).is_err() {
        charge.release_bytes(next);
        return false;
    }
    let actual = memory_footprint::hash_table_capacity_bytes::<(K, V)>(map.capacity());
    charge.release_bytes(old + next - actual);
    true
}
fn reserve_vec<T>(values: &mut Vec<T>, charge: &mut Charge, cap: usize) -> bool {
    if values.len() < values.capacity() {
        return true;
    }
    let old = values.capacity() * std::mem::size_of::<T>();
    let count = values.capacity().max(4).saturating_mul(2);
    let next = count.saturating_mul(std::mem::size_of::<T>());
    if !charge.add_within(next, cap) {
        return false;
    }
    if values.try_reserve_exact(count - values.len()).is_err() {
        charge.release_bytes(next);
        return false;
    }
    let actual = values.capacity() * std::mem::size_of::<T>();
    if actual > next && !charge.add_within(actual - next, cap) {
        return false;
    }
    charge.release_bytes(old + next.saturating_sub(actual));
    true
}

struct Dictionary {
    text: Map<String, Code>,
    integer: Map<i64, u32>,
    canonical: Map<Key, u32>,
    values: Vec<Value>,
    collation: Option<u16>,
    text_probes: usize,
}
impl Dictionary {
    fn new(collation: Option<u16>) -> Self {
        Self {
            text: Map::default(),
            integer: Map::default(),
            canonical: Map::default(),
            values: Vec::new(),
            collation,
            text_probes: 0,
        }
    }
    fn intern(
        &mut self,
        column: &ColumnVector,
        row: usize,
        charge: &mut Charge,
        cap: usize,
    ) -> Option<Code> {
        // Zero is reserved for NULL, independently of every physical lane.
        if column.is_null(row) {
            return Some(Code {
                canonical: 0,
                raw: 0,
            });
        }
        match column {
            ColumnVector::Utf8 { .. } => {
                let text = column.valid_utf8_at(row)?;
                self.text_probes = self.text_probes.saturating_add(1);
                if let Some(code) = self.text.get(text) {
                    return Some(*code);
                }
                if text.len().saturating_mul(16) > cap.saturating_sub(charge.query.charged()) {
                    return None;
                }
                let raw = u32::try_from(self.values.len()).ok()?.checked_add(1)?;
                let value = Value::String(text.into());
                let key = crate::engine::executor::dispatch::set_row_key(
                    &[self.collation],
                    std::slice::from_ref(&value),
                );
                let canonical = self.canonical.get(&key).copied().unwrap_or(raw);
                if !reserve(&mut self.text, charge, cap)
                    || !reserve_vec(&mut self.values, charge, cap)
                    || (canonical == raw && !reserve(&mut self.canonical, charge, cap))
                    || !charge.add_within(
                        text.len()
                            .saturating_mul(2)
                            .saturating_add(if key.spilled() { key.capacity() } else { 0 }),
                        cap,
                    )
                {
                    return None;
                }
                if canonical == raw {
                    self.canonical.insert(key, raw);
                }
                let code = Code { canonical, raw };
                self.values.push(value);
                self.text.insert(text.into(), code);
                Some(code)
            }
            ColumnVector::Date32 { data, .. } => {
                self.intern_integer(i64::from(data[row]), Value::Date(data[row]), charge, cap)
            }
            ColumnVector::I64 { data, .. } => {
                self.intern_integer(data[row], Value::Int(data[row]), charge, cap)
            }
            ColumnVector::Time64 { data, .. } => {
                self.intern_integer(data[row], Value::Time(data[row]), charge, cap)
            }
            ColumnVector::DateTime64 { data, .. } => {
                self.intern_integer(data[row], Value::DateTime(data[row]), charge, cap)
            }
            ColumnVector::Timestamp64 { data, .. } => {
                self.intern_integer(data[row], Value::Timestamp(data[row]), charge, cap)
            }
            _ => None,
        }
    }
    fn intern_integer(
        &mut self,
        key: i64,
        value: Value,
        charge: &mut Charge,
        cap: usize,
    ) -> Option<Code> {
        let id = if let Some(id) = self.integer.get(&key) {
            *id
        } else {
            let id = u32::try_from(self.values.len()).ok()?.checked_add(1)?;
            if !reserve(&mut self.integer, charge, cap)
                || !reserve_vec(&mut self.values, charge, cap)
            {
                return None;
            }
            self.integer.insert(key, id);
            self.values.push(value);
            id
        };
        Some(Code {
            canonical: id,
            raw: id,
        })
    }
    fn value(&self, code: u32) -> Value {
        if code == 0 {
            Value::Null
        } else {
            self.values[code as usize - 1].clone()
        }
    }
}

struct State {
    tables: Vec<Map<u128, Entry>>,
    hash: ahash::RandomState,
    dictionaries: Vec<Dictionary>,
    alternates: Vec<u128>,
    charge: Charge,
    cap: usize,
    input_rows: u64,
}
impl State {
    fn representative(&self, key: u128, entry: Entry) -> Vec<Value> {
        let raw = if entry.alternate == 0 {
            key
        } else {
            self.alternates[entry.alternate as usize - 1]
        };
        self.dictionaries
            .iter()
            .enumerate()
            .map(|(i, dict)| dict.value((raw >> (i * 32)) as u32))
            .collect()
    }
    fn insert(
        &mut self,
        batch: &Batch,
        row: usize,
        side: usize,
        build_side: usize,
        op: crate::parser::SetOp,
    ) -> bool {
        let mut key = 0u128;
        let mut raw = 0u128;
        for (i, dict) in self.dictionaries.iter_mut().enumerate() {
            let Some(code) = dict.intern(&batch.columns()[i], row, &mut self.charge, self.cap)
            else {
                return false;
            };
            if dict.text_probes >= 4096 && dict.text.len() > dict.text_probes.saturating_mul(7) / 8
            {
                return false;
            }
            key |= u128::from(code.canonical) << (i * 32);
            raw |= u128::from(code.raw) << (i * 32);
        }
        let shard = (self.hash.hash_one(key) as usize) & (self.tables.len() - 1);
        let entry = self.tables[shard].get(&key).copied();
        if entry.is_none() && side != build_side && op != crate::parser::SetOp::Union {
            return true;
        }
        if let Some(entry) = entry {
            if entry.seen & (1 << side) != 0 {
                return true;
            }
        }
        // INTERSECT can build the smaller right side, but the first LEFT
        // representative always wins, including collation-equivalent casing.
        let replace = entry.is_none() || (side == 0 && entry.is_some_and(|e| e.seen & 1 == 0));
        let alternate = if replace && raw != key {
            if self.alternates.len() >= u32::MAX as usize
                || !reserve_vec(&mut self.alternates, &mut self.charge, self.cap)
            {
                return false;
            }
            self.alternates.push(raw);
            self.alternates.len() as u32
        } else if replace {
            0
        } else {
            entry.unwrap().alternate
        };
        if let Some(entry) = self.tables[shard].get_mut(&key) {
            entry.seen |= 1 << side;
            entry.alternate = alternate;
        } else {
            if !reserve(&mut self.tables[shard], &mut self.charge, self.cap) {
                return false;
            }
            self.tables[shard].insert(
                key,
                Entry {
                    alternate,
                    seen: 1 << side,
                },
            );
        }
        true
    }
}

pub(super) struct Output {
    state: State,
    iterators: VecDeque<hashbrown::hash_map::IntoIter<u128, Entry>>,
    schema: Arc<BatchSchema>,
    op: crate::parser::SetOp,
}
impl Output {
    pub(super) fn metrics(&self) -> (u64, u64) {
        (self.state.charge.peak as u64, self.state.input_rows)
    }
    pub(super) fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        let mut rows = Vec::with_capacity(DEFAULT_BATCH_ROWS);
        while rows.len() < DEFAULT_BATCH_ROWS {
            let Some(iter) = self.iterators.front_mut() else {
                break;
            };
            let Some((key, entry)) = iter.next() else {
                self.iterators.pop_front();
                continue;
            };
            if match self.op {
                crate::parser::SetOp::Union => true,
                crate::parser::SetOp::Intersect => entry.seen == 3,
                crate::parser::SetOp::Except => entry.seen == 1,
            } {
                rows.push(self.state.representative(key, entry));
            }
        }
        crate::engine::cancel::check_cancelled()?;
        Ok(value_rows_into_batches(self.schema.clone(), rows)
            .into_iter()
            .next())
    }
}

struct Replay<'a> {
    schema: Arc<BatchSchema>,
    reader: Option<ValueRowReader>,
    _dir: Arc<SpillTmpDir>,
    prefix: Option<Batch>,
    rest: Option<Box<dyn BatchOperator + 'a>>,
}
impl BatchOperator for Replay<'_> {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }
    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        if let Some(reader) = self.reader.as_mut() {
            let mut rows = Vec::with_capacity(DEFAULT_BATCH_ROWS);
            while rows.len() < DEFAULT_BATCH_ROWS {
                match reader.next()? {
                    Some(row) => rows.push(row),
                    None => {
                        self.reader = None;
                        break;
                    }
                }
            }
            if !rows.is_empty() {
                return Ok(value_rows_into_batches(self.schema.clone(), rows)
                    .into_iter()
                    .next());
            }
        }
        if let Some(batch) = self.prefix.take() {
            return Ok(Some(batch));
        }
        match self.rest.as_mut() {
            Some(rest) => rest.next_batch(),
            None => Ok(None),
        }
    }
}

fn fallback<'a>(
    mut state: State,
    inputs: &mut [&mut Option<Box<dyn BatchOperator + 'a>>; 2],
    current: (usize, Batch),
    schemas: &[Arc<BatchSchema>; 2],
    data_dir: &std::path::Path,
) -> Result<(), QueryError> {
    let dir = Arc::new(SpillTmpDir::create_kind(data_dir, "setop")?);
    let paths = [
        dir.path().join("dictionary-left"),
        dir.path().join("dictionary-right"),
    ];
    let mut writers = [
        ValueRowWriter::create(&paths[0], schemas[0].len(), dir.charger())?,
        ValueRowWriter::create(&paths[1], schemas[1].len(), dir.charger())?,
    ];
    for table in &state.tables {
        for (&key, &entry) in table {
            let row = state.representative(key, entry);
            for (side, writer) in writers.iter_mut().enumerate() {
                if entry.seen & (1 << side) != 0 {
                    writer.append(&row)?;
                }
            }
        }
    }
    let mut bytes = 0;
    for writer in writers {
        bytes += writer.finish()?;
    }
    use std::sync::atomic::Ordering::Relaxed;
    crate::engine::executor::dispatch::SETOP_SPILL_COUNT.fetch_add(1, Relaxed);
    crate::engine::executor::dispatch::SETOP_SPILL_BYTES_WRITTEN.fetch_add(bytes, Relaxed);
    crate::engine::executor::dispatch::SETOP_SPILL_PARTITIONS.fetch_add(2, Relaxed);
    crate::engine::executor::dispatch::SETOP_SPILL_FILE_OPENS.fetch_add(4, Relaxed);
    analyze_stats::add_pending_setop_spill(bytes, 2, 4, state.charge.peak as u64, 0, 0, 0);
    state.tables.clear();
    drop(state);
    let (current_side, batch) = current;
    let mut batch = Some(batch);
    for side in 0..2 {
        *inputs[side] = Some(Box::new(Replay {
            schema: schemas[side].clone(),
            reader: ValueRowReader::open(&paths[side], dir.charger())?,
            _dir: dir.clone(),
            prefix: if side == current_side {
                batch.take()
            } else {
                None
            },
            rest: inputs[side].take(),
        }));
    }
    Ok(())
}

struct Prefix<'a> {
    schema: Arc<BatchSchema>,
    first: Option<Batch>,
    rest: Box<dyn BatchOperator + 'a>,
}
impl BatchOperator for Prefix<'_> {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }
    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        if let Some(first) = self.first.take() {
            Ok(Some(first))
        } else {
            self.rest.next_batch()
        }
    }
}

/// Avoid building a second index over almost-unique text. The retained first
/// batch remains replayable, so refusal never re-executes a SELECT expression.
fn low_cardinality_text(batch: &Batch, schema: &BatchSchema) -> bool {
    let sample_rows = batch.visible_rows().min(4096);
    if sample_rows < 4096 {
        return true;
    }
    let mut charge = ScopedCharge::new(0);
    let cap = mem_budget::current_budget().map_or(256 << 10, |b| b.limit() / 64);
    if !charge.add_within(sample_rows.saturating_mul(64), cap) {
        return false;
    }
    for (i, field) in schema.fields().iter().enumerate() {
        if field.column_type != ColumnType::VarString {
            continue;
        }
        let mut keys = hashbrown::HashSet::<&str, ahash::RandomState>::default();
        if keys.try_reserve(sample_rows).is_err() {
            return false;
        }
        let mut non_null = 0;
        for row in batch.visible_indices().take(sample_rows) {
            if batch.columns()[i].is_null(row) {
                continue;
            }
            let Some(value) = batch.columns()[i].valid_utf8_at(row) else {
                return false;
            };
            keys.insert(value);
            non_null += 1;
        }
        if non_null >= 1024 && keys.len() > non_null * 7 / 8 {
            return false;
        }
    }
    true
}

pub(super) fn prepare<'a>(
    left: &mut Option<Box<dyn BatchOperator + 'a>>,
    right: &mut Option<Box<dyn BatchOperator + 'a>>,
    schema: Arc<BatchSchema>,
    op: crate::parser::SetOp,
    build_right: bool,
    data_dir: &std::path::Path,
) -> Result<Option<Output>, QueryError> {
    if schema.len() == 0
        || schema.len() > 4
        || !schema
            .fields()
            .iter()
            .any(|f| f.column_type == ColumnType::VarString)
        || schema.fields().iter().any(|f| {
            !matches!(
                f.column_type,
                ColumnType::VarString
                    | ColumnType::Tiny
                    | ColumnType::Short
                    | ColumnType::MediumInt
                    | ColumnType::Long
                    | ColumnType::LongLong
                    | ColumnType::Date
                    | ColumnType::Time
                    | ColumnType::DateTime
                    | ColumnType::Timestamp
            )
        })
    {
        return Ok(None);
    }
    let schemas = [
        left.as_ref().unwrap().schema().clone(),
        right.as_ref().unwrap().schema().clone(),
    ];
    if schemas.iter().any(|s| {
        s.len() != schema.len()
            || s.fields()
                .iter()
                .zip(schema.fields())
                .any(|(a, b)| !SetOpBatchOperator::can_passthrough_field(a, b))
    }) {
        return Ok(None);
    }
    let first = usize::from(op == crate::parser::SetOp::Intersect && build_right);
    {
        let source = if first == 0 { &mut *left } else { &mut *right };
        if let Some(batch) = source.as_mut().unwrap().next_batch()? {
            let suitable = low_cardinality_text(&batch, &schema);
            *source = Some(Box::new(Prefix {
                schema: source.as_ref().unwrap().schema().clone(),
                first: Some(batch),
                rest: source.take().unwrap(),
            }));
            if !suitable {
                return Ok(None);
            }
        }
    }
    let cap = mem_budget::current_budget().map_or(32 << 20, |b| b.limit().saturating_mul(3) / 4);
    let mut state = State {
        tables: (0..64).map(|_| Map::default()).collect(),
        hash: ahash::RandomState::new(),
        dictionaries: schema
            .fields()
            .iter()
            .map(|f| Dictionary::new(f.source.collation_id))
            .collect(),
        alternates: Vec::new(),
        charge: Charge::new(),
        cap,
        input_rows: 0,
    };
    // Keep hash growth overlap bounded to one shard rather than a second
    // complete table at the memory watermark.
    if !state.charge.add_within(
        64 * std::mem::size_of::<Map<u128, Entry>>()
            + schema.len() * std::mem::size_of::<Dictionary>(),
        cap,
    ) {
        return Ok(None);
    }
    let mut inputs = [left, right];
    for side in [first, 1 - first] {
        loop {
            let Some(batch) = inputs[side].as_mut().unwrap().next_batch()? else {
                *inputs[side] = None;
                break;
            };
            crate::engine::cancel::check_cancelled()?;
            state.input_rows = state.input_rows.saturating_add(batch.visible_rows() as u64);
            for row in batch.visible_indices() {
                if !state.insert(&batch, row, side, first, op) {
                    fallback(state, &mut inputs, (side, batch), &schemas, data_dir)?;
                    return Ok(None);
                }
            }
        }
    }
    let iterators = std::mem::take(&mut state.tables)
        .into_iter()
        .map(Map::into_iter)
        .collect();
    Ok(Some(Output {
        state,
        iterators,
        schema,
        op,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    fn schema() -> Arc<BatchSchema> {
        Arc::new(BatchSchema::new(vec![
            BatchField {
                table: "t".into(),
                alias: None,
                name: "s".into(),
                column_type: ColumnType::VarString,
                source: crate::types::ColumnSource {
                    collation_id: Some(255),
                    ..Default::default()
                },
            },
            BatchField {
                table: "t".into(),
                alias: None,
                name: "d".into(),
                column_type: ColumnType::Date,
                source: Default::default(),
            },
        ]))
    }
    fn sorted(mut rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
        for row in &mut rows {
            for value in row {
                if let Value::SmallString(text) = value {
                    *value = Value::String(String::from_utf8(text.as_slice().to_vec()).unwrap());
                }
            }
        }
        rows.sort_by_key(|row| format!("{row:?}"));
        rows
    }
    fn run(
        left_rows: Vec<Vec<Value>>,
        right_rows: Vec<Vec<Value>>,
        op: crate::parser::SetOp,
        build_right: bool,
        bytes: usize,
        expect_native: bool,
    ) {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let collations = [Some(255), None];
        let expected = crate::engine::executor::dispatch::combine_set_rows_spillable_charged(
            &collations,
            op,
            false,
            left_rows.clone(),
            right_rows.clone(),
            2,
            dir.path(),
        )
        .unwrap()
        .0;
        let budget = Arc::new(MemBudget::new(bytes));
        with_query_budget(budget.clone(), || {
            let schema = schema();
            let input = |rows| -> Option<Box<dyn BatchOperator>> {
                Some(Box::new(MaterializedBatchOperator::new(
                    schema.clone(),
                    value_rows_into_batches(schema.clone(), rows),
                )))
            };
            let mut left = input(left_rows);
            let mut right = input(right_rows);
            let native = prepare(
                &mut left,
                &mut right,
                schema.clone(),
                op,
                build_right,
                dir.path(),
            )
            .unwrap();
            assert_eq!(native.is_some(), expect_native);
            let mut actual = Vec::new();
            if let Some(mut output) = native {
                while let Some(batch) = output.next_batch().unwrap() {
                    actual.extend(batch.visible_indices().map(|r| batch.row(r)));
                }
            } else {
                let drain = |input: &mut Option<Box<dyn BatchOperator>>| {
                    let mut rows = Vec::new();
                    while let Some(batch) = input.as_mut().unwrap().next_batch().unwrap() {
                        rows.extend(batch.visible_indices().map(|r| batch.row(r)));
                    }
                    rows
                };
                actual = crate::engine::executor::dispatch::combine_set_rows_spillable_charged(
                    &collations,
                    op,
                    false,
                    drain(&mut left),
                    drain(&mut right),
                    2,
                    dir.path(),
                )
                .unwrap()
                .0;
            }
            assert_eq!(sorted(actual), sorted(expected));
        });
        assert_eq!(budget.used(), 0);
        assert_eq!(
            std::fs::read_dir(dir.path().join("tmp"))
                .map(|i| i.count())
                .unwrap_or(0),
            0
        );
    }
    #[test]
    fn dictionary_sets_preserve_first_tuple_representatives_and_nulls() {
        let row = |s: &str, day| vec![Value::String(s.into()), Value::Date(day)];
        let left = vec![
            row("A", 1),
            row("á", 2),
            row("a", 2),
            row("B", 3),
            vec![Value::Null, Value::Date(4)],
            row("A", 1),
        ];
        let right = vec![
            row("a", 1),
            row("A", 2),
            row("c", 3),
            vec![Value::Null, Value::Date(4)],
            row("a", 1),
        ];
        for op in [
            crate::parser::SetOp::Union,
            crate::parser::SetOp::Intersect,
            crate::parser::SetOp::Except,
        ] {
            for build_right in [false, true] {
                run(left.clone(), right.clone(), op, build_right, 8 << 20, true);
            }
        }
    }
    #[test]
    fn dictionary_budget_fallback_replays_both_sides_without_losing_rows() {
        let rows = |start, end| {
            (start..end)
                .map(|n| vec![Value::String(format!("value-{}", n % 32)), Value::Date(n)])
                .collect::<Vec<_>>()
        };
        for op in [
            crate::parser::SetOp::Union,
            crate::parser::SetOp::Intersect,
            crate::parser::SetOp::Except,
        ] {
            for build_right in [false, true] {
                run(
                    rows(0, 500),
                    rows(250, 750),
                    op,
                    build_right,
                    32 << 10,
                    false,
                );
            }
        }
    }
    #[test]
    fn dictionary_full_width_integer_and_runtime_lane_fallback_are_exact() {
        let schema = schema();
        // A physically unsupported lane must replay its entire batch. This
        // also exercises a prefix already deduplicated before refusal.
        let rows = (0..12)
            .map(|n| vec![Value::String(format!("s{n}")), Value::Date(n)])
            .collect::<Vec<_>>();
        let mut left: Option<Box<dyn BatchOperator>> =
            Some(Box::new(MaterializedBatchOperator::new(
                schema.clone(),
                value_rows_into_batches(schema.clone(), rows.clone()),
            )));
        let right_rows = vec![vec![Value::String("s0".into()), Value::UInt(u64::MAX)]];
        let mut right: Option<Box<dyn BatchOperator>> =
            Some(Box::new(MaterializedBatchOperator::new(
                schema.clone(),
                value_rows_into_batches(schema.clone(), right_rows.clone()),
            )));
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        assert!(prepare(
            &mut left,
            &mut right,
            schema,
            crate::parser::SetOp::Union,
            false,
            dir.path()
        )
        .unwrap()
        .is_none());
        let mut actual = Vec::new();
        for input in [&mut left, &mut right] {
            while let Some(batch) = input.as_mut().unwrap().next_batch().unwrap() {
                actual.extend(batch.visible_indices().map(|r| batch.row(r)));
            }
        }
        assert_eq!(
            sorted(actual),
            sorted(rows.into_iter().chain(right_rows).collect())
        );
    }
}

#[cfg(test)]
mod soft_watermark_tests {
    use super::*;
    #[test]
    fn pipeline_reservations_above_soft_watermark_do_not_disable_small_dictionaries() {
        let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let budget = Arc::new(mem_budget::MemBudget::new(64 << 20));
        mem_budget::with_query_budget(budget.clone(), || {
            let _parent = mem_budget::force_charge_scoped(80 << 20).unwrap();
            let schema = Arc::new(BatchSchema::new(vec![
                BatchField {
                    table: "t".into(),
                    alias: None,
                    name: "s".into(),
                    column_type: ColumnType::VarString,
                    source: Default::default(),
                },
                BatchField {
                    table: "t".into(),
                    alias: None,
                    name: "d".into(),
                    column_type: ColumnType::Date,
                    source: Default::default(),
                },
            ]));
            let rows = (0..4096)
                .map(|n| vec![Value::String(format!("s{}", n % 32)), Value::Date(n)])
                .collect();
            let mut left: Option<Box<dyn BatchOperator>> =
                Some(Box::new(MaterializedBatchOperator::new(
                    schema.clone(),
                    value_rows_into_batches(schema.clone(), rows),
                )));
            let mut right: Option<Box<dyn BatchOperator>> = Some(Box::new(
                MaterializedBatchOperator::new(schema.clone(), Vec::new()),
            ));
            let mut output = prepare(
                &mut left,
                &mut right,
                schema,
                crate::parser::SetOp::Union,
                false,
                root.path(),
            )
            .unwrap()
            .expect("a shared WorkArea grant remains usable above the pipeline's soft watermark");
            let mut rows = 0;
            while let Some(batch) = output.next_batch().unwrap() {
                rows += batch.visible_rows();
            }
            assert_eq!(rows, 4096);
        });
        assert_eq!(budget.used(), 0);
    }
}
