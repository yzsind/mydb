//! Composable, diagnostic-free column kernels. A decline is side-effect free:
//! the caller retains the ordered scalar oracle for the complete expression.
use super::*;
use crate::engine::executor::vec::expr_batch::{
    transform_utf8_column, try_temporal_extract_column,
};
use crate::engine::executor::vec::{
    batch::{Batch, ColumnVector},
    bitmap::Bitmap,
    schema::BatchSchema,
};
use crate::engine::expr::date_format::DateFormatPlan;
use chrono::{Datelike, NaiveDate};

#[derive(Clone, Copy)]
enum Domain {
    Integer(i64, i64),
    Text,
    Binary,
    Temporal,
    Null,
}
struct Contract {
    domain: Domain,
    parallel: bool,
}

impl ExprProgram {
    /// Value-only kernels retain the same scalar fallback and diagnostic order.
    pub(crate) fn has_column_contract(&self, schema: &BatchSchema) -> bool {
        contract(&self.root, schema).is_some()
    }
    /// Stronger than "compiled": every value in the declared domain must be
    /// free of SQL warnings/errors before scheduling work out of row order.
    pub(crate) fn column_parallel_safe(&self, schema: &BatchSchema) -> bool {
        contract(&self.root, schema).is_some_and(|c| c.parallel)
    }
    /// Reservation for a retained output, independent of speculative workspace limits.
    pub(crate) fn column_retained_bound(&self, batch: &Batch) -> Option<usize> {
        contract(&self.root, batch.schema())?;
        workspace(&self.root, batch)?.0.checked_mul(2)
    }
    pub(crate) fn column_output_bound(&self, batch: &Batch) -> Option<usize> {
        contract(&self.root, batch.schema())?;
        let (payload, work) = workspace(&self.root, batch)?;
        let limit = crate::engine::executor::vec::evaluated_batch::byte_target().saturating_mul(2);
        let capacity = payload.checked_mul(2)?;
        (work <= limit && capacity <= limit).then_some(capacity)
    }
    pub(crate) fn try_eval_column(&self, batch: &Batch) -> Option<ColumnVector> {
        contract(&self.root, batch.schema())?;
        let column = if batch.selection().is_some() {
            evaluate_selected(&self.root, batch)?
        } else {
            let (payload, work) = workspace(&self.root, batch)?;
            let work = if matches!(self.root, ExprNode::Column(_)) {
                payload
            } else {
                work
            };
            if work > crate::engine::executor::vec::evaluated_batch::byte_target().saturating_mul(2)
            {
                return None;
            }
            let mut scratch = batch.memory_charge();
            if !scratch.add_within(work.checked_mul(2)?, usize::MAX) {
                return None;
            }
            evaluate(&self.root, batch)?
        };
        let rows = batch.visible_rows() as u64;
        record_column_eval_rows(rows);
        Some(column)
    }
}

fn constant(node: &ExprNode) -> Option<std::borrow::Cow<'_, Value>> {
    match node {
        ExprNode::Const(value) => Some(std::borrow::Cow::Borrowed(value)),
        ExprNode::Placeholder(index) => {
            crate::engine::params::current_param(*index).map(std::borrow::Cow::Owned)
        }
        _ => None,
    }
}
fn integer(node: &ExprNode) -> Option<i64> {
    match constant(node)?.as_ref() {
        Value::Int(n) => Some(*n),
        Value::UInt(n) => i64::try_from(*n).ok(),
        _ => None,
    }
}
fn text(node: &ExprNode) -> Option<std::borrow::Cow<'_, str>> {
    match constant(node)? {
        std::borrow::Cow::Borrowed(Value::String(s)) => Some(std::borrow::Cow::Borrowed(s)),
        std::borrow::Cow::Borrowed(Value::SmallString(s)) => {
            std::str::from_utf8(s).ok().map(std::borrow::Cow::Borrowed)
        }
        std::borrow::Cow::Owned(Value::String(s)) => Some(std::borrow::Cow::Owned(s)),
        std::borrow::Cow::Owned(Value::SmallString(s)) => String::from_utf8(s.to_vec())
            .ok()
            .map(std::borrow::Cow::Owned),
        _ => None,
    }
}
fn temporal(kernel: FunctionKernel) -> Option<TemporalExtractKind> {
    Some(match kernel {
        FunctionKernel::Year => TemporalExtractKind::Year,
        FunctionKernel::Month => TemporalExtractKind::Month,
        FunctionKernel::Day => TemporalExtractKind::Day,
        FunctionKernel::Hour => TemporalExtractKind::Hour,
        FunctionKernel::Minute => TemporalExtractKind::Minute,
        FunctionKernel::Second => TemporalExtractKind::Second,
        FunctionKernel::Quarter => TemporalExtractKind::Quarter,
        _ => return None,
    })
}
fn temporal_column(
    kernel: FunctionKernel,
    args: &[ExprNode],
) -> Option<(TemporalExtractKind, usize)> {
    if let Some(kind) = temporal(kernel) {
        let [ExprNode::Column(column)] = args else {
            return None;
        };
        return Some((kind, *column));
    }
    if let (FunctionKernel::LegacyDeterministic("EXTRACT"), [part, ExprNode::Column(column)]) =
        (kernel, args)
    {
        let part = text(part)?;
        for (name, kind) in [
            ("YEAR", TemporalExtractKind::Year),
            ("MONTH", TemporalExtractKind::Month),
            ("DAY", TemporalExtractKind::Day),
            ("HOUR", TemporalExtractKind::Hour),
            ("MINUTE", TemporalExtractKind::Minute),
            ("SECOND", TemporalExtractKind::Second),
            ("QUARTER", TemporalExtractKind::Quarter),
        ] {
            if part.eq_ignore_ascii_case(name) {
                return Some((kind, *column));
            }
        }
    }
    None
}

fn transform(kernel: FunctionKernel) -> Option<StringTransformKind> {
    Some(match kernel {
        FunctionKernel::Lower => StringTransformKind::Lower,
        FunctionKernel::Upper => StringTransformKind::Upper,
        FunctionKernel::Ltrim => StringTransformKind::Ltrim,
        FunctionKernel::Rtrim => StringTransformKind::Rtrim,
        FunctionKernel::Trim => StringTransformKind::Trim,
        FunctionKernel::Reverse => StringTransformKind::Reverse,
        _ => return None,
    })
}
fn comparison(op: BinaryOp) -> bool {
    matches!(
        op,
        BinaryOp::Eq
            | BinaryOp::NotEq
            | BinaryOp::Lt
            | BinaryOp::LtEq
            | BinaryOp::Gt
            | BinaryOp::GtEq
    )
}

fn contract(node: &ExprNode, schema: &BatchSchema) -> Option<Contract> {
    let (domain, parallel) = match node {
        ExprNode::Column(index) => {
            let field = schema.fields().get(*index)?;
            let domain = match field.column_type {
                ColumnType::Tiny
                | ColumnType::Short
                | ColumnType::MediumInt
                | ColumnType::Long
                | ColumnType::LongLong
                    if field.source.flags & crate::mysql::protocol::UNSIGNED_FLAG == 0 =>
                {
                    Domain::Integer(i64::MIN, i64::MAX)
                }
                ColumnType::Date | ColumnType::DateTime | ColumnType::Timestamp => Domain::Temporal,
                ColumnType::VarString | ColumnType::Blob => {
                    if field.source.flags & crate::mysql::protocol::BINARY_FLAG != 0
                        || field.source.collation_id == Some(crate::charset::BINARY_COLLATION_ID)
                    {
                        Domain::Binary
                    } else {
                        Domain::Text
                    }
                }
                _ => return None,
            };
            (domain, true)
        }
        ExprNode::Const(_) | ExprNode::Placeholder(_) => (
            match constant(node)?.as_ref() {
                Value::Int(n) => Domain::Integer(*n, *n),
                Value::Null => Domain::Null,
                Value::String(_) | Value::SmallString(_) => Domain::Text,
                Value::Bytes(_) => Domain::Binary,
                _ => return None,
            },
            true,
        ),
        ExprNode::SubstringConst { expr, .. } => {
            let child = contract(expr, schema)?;
            if !matches!(child.domain, Domain::Text | Domain::Binary) {
                return None;
            }
            (child.domain, child.parallel)
        }
        ExprNode::Binary {
            left,
            op,
            right,
            semantics,
        } if (matches!(op, BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul) || comparison(*op))
            && !semantics.unsigned_subtraction =>
        {
            let (l, r) = (contract(left, schema)?, contract(right, schema)?);
            let (Domain::Integer(a, b), Domain::Integer(c, d)) = (l.domain, r.domain) else {
                return None;
            };
            if comparison(*op) {
                return Some(Contract {
                    domain: Domain::Integer(0, 1),
                    parallel: l.parallel && r.parallel,
                });
            }
            let apply = |x: i64, y: i64| match op {
                BinaryOp::Add => x.checked_add(y),
                BinaryOp::Sub => x.checked_sub(y),
                _ => x.checked_mul(y),
            };
            let range: Option<Vec<_>> = [(a, c), (a, d), (b, c), (b, d)]
                .into_iter()
                .map(|(x, y)| apply(x, y))
                .collect();
            match range {
                Some(range) => (
                    Domain::Integer(*range.iter().min()?, *range.iter().max()?),
                    l.parallel && r.parallel,
                ),
                None => (Domain::Integer(i64::MIN, i64::MAX), false),
            }
        }
        ExprNode::Function { kernel, args, .. } => {
            if let Some((kind, index)) = temporal_column(*kernel, args) {
                if !matches!(
                    schema.fields().get(index)?.column_type,
                    ColumnType::Date | ColumnType::DateTime | ColumnType::Timestamp
                ) {
                    return None;
                }
                // Timestamp conversion can reject values outside the selected
                // zone's calendar; keep that diagnostic on the coordinator.
                let parallel = schema.fields()[index].column_type != ColumnType::Timestamp;
                let max = match kind {
                    TemporalExtractKind::Year => 9999,
                    TemporalExtractKind::Month => 12,
                    TemporalExtractKind::Day => 31,
                    TemporalExtractKind::Hour => 23,
                    TemporalExtractKind::Quarter => 4,
                    _ => 59,
                };
                (Domain::Integer(0, max), parallel)
            } else if transform(*kernel).is_some() && args.len() == 1 {
                let child = contract(&args[0], schema)?;
                if !matches!(child.domain, Domain::Text | Domain::Binary) {
                    return None;
                }
                if matches!(child.domain, Domain::Binary)
                    && matches!(
                        kernel,
                        FunctionKernel::Trim | FunctionKernel::Ltrim | FunctionKernel::Rtrim
                    )
                {
                    return None;
                }
                (child.domain, child.parallel)
            } else {
                match (kernel, args.as_slice()) {
                    (FunctionKernel::Date, [ExprNode::Column(index)])
                        if schema.fields().get(*index)?.column_type == ColumnType::Date =>
                    {
                        (Domain::Temporal, true)
                    }
                    (
                        FunctionKernel::Abs
                        | FunctionKernel::Floor
                        | FunctionKernel::Ceil
                        | FunctionKernel::Sign,
                        [arg],
                    ) => {
                        let child = contract(arg, schema)?;
                        let Domain::Integer(min, max) = child.domain else {
                            return None;
                        };
                        let range = match kernel {
                            FunctionKernel::Sign => Domain::Integer(-1, 1),
                            FunctionKernel::Abs => Domain::Integer(
                                0,
                                min.checked_abs()
                                    .unwrap_or(i64::MAX)
                                    .max(max.checked_abs().unwrap_or(i64::MAX)),
                            ),
                            _ => child.domain,
                        };
                        (
                            range,
                            child.parallel
                                && (!matches!(kernel, FunctionKernel::Abs) || min != i64::MIN),
                        )
                    }
                    (FunctionKernel::Length | FunctionKernel::CharLength, [arg]) => {
                        let child = contract(arg, schema)?;
                        if !matches!(child.domain, Domain::Text | Domain::Binary) {
                            return None;
                        }
                        (Domain::Integer(0, u32::MAX as i64), child.parallel)
                    }
                    (
                        FunctionKernel::Hex
                        | FunctionKernel::Md5
                        | FunctionKernel::Sha
                        | FunctionKernel::Sha1,
                        [arg],
                    ) => {
                        let child = contract(arg, schema)?;
                        if !matches!(child.domain, Domain::Text | Domain::Binary | Domain::Null) {
                            return None;
                        }
                        (Domain::Text, child.parallel)
                    }
                    (FunctionKernel::Sha2, [arg, length])
                        if integer(length)
                            .and_then(crate::engine::expr::digest::HashAlgorithm::from_sha2_length)
                            .is_some() =>
                    {
                        let child = contract(arg, schema)?;
                        if !matches!(child.domain, Domain::Text | Domain::Binary | Domain::Null) {
                            return None;
                        }
                        (Domain::Text, child.parallel)
                    }
                    (FunctionKernel::Concat, args) if !args.is_empty() => {
                        let mut binary = false;
                        for arg in args {
                            match contract(arg, schema)?.domain {
                                Domain::Text | Domain::Null => {}
                                Domain::Binary => binary = true,
                                _ => return None,
                            }
                        }
                        (if binary { Domain::Binary } else { Domain::Text }, false)
                    }
                    (FunctionKernel::Substring, args)
                        if (2..=3).contains(&args.len())
                            && integer(&args[1]).is_some()
                            && (args.len() == 2 || integer(&args[2]).is_some()) =>
                    {
                        let child = contract(&args[0], schema)?;
                        if !matches!(child.domain, Domain::Text | Domain::Binary) {
                            return None;
                        }
                        (child.domain, child.parallel)
                    }
                    (FunctionKernel::Left | FunctionKernel::Right, [arg, count])
                        if integer(count).is_some() =>
                    {
                        let child = contract(arg, schema)?;
                        if !matches!(child.domain, Domain::Text | Domain::Binary) {
                            return None;
                        }
                        (child.domain, child.parallel)
                    }
                    (FunctionKernel::Replace, [arg, from, to])
                        if text(from).is_some() && text(to).is_some() =>
                    {
                        let child = contract(arg, schema)?;
                        if !matches!(child.domain, Domain::Text) {
                            return None;
                        }
                        (Domain::Text, child.parallel)
                    }
                    (FunctionKernel::DateFormat, [arg, format]) if text(format).is_some() => {
                        if !matches!(contract(arg, schema)?.domain, Domain::Temporal) {
                            return None;
                        }
                        (Domain::Text, false)
                    }
                    _ => return None,
                }
            }
        }
        _ => return None,
    };
    Some(Contract { domain, parallel })
}

/// Bound speculative owned buffers from actual lanes, including intermediate
/// columns. A small numeric result must not admit an enormous string child.
fn workspace(node: &ExprNode, batch: &Batch) -> Option<(usize, usize)> {
    let numeric = batch.num_rows().checked_mul(16)?;
    let offsets = batch.num_rows().checked_add(1)?.checked_mul(8)?;
    match node {
        ExprNode::Column(index) => Some((batch.columns().get(*index)?.memory_bytes(), 0)),
        ExprNode::Const(_) | ExprNode::Placeholder(_) => {
            let value = constant(node)?;
            let bytes = match value.as_ref() {
                Value::Null => 0,
                Value::Int(_) => numeric,
                Value::String(s) => s
                    .len()
                    .checked_mul(batch.num_rows())?
                    .checked_add(offsets)?,
                Value::SmallString(s) => s
                    .len()
                    .checked_mul(batch.num_rows())?
                    .checked_add(offsets)?,
                Value::Bytes(s) => s
                    .len()
                    .checked_mul(batch.num_rows())?
                    .checked_add(offsets)?,
                _ => return None,
            };
            Some((bytes, bytes))
        }
        ExprNode::Binary { left, right, .. } => {
            let (_, left) = workspace(left, batch)?;
            let (_, right) = workspace(right, batch)?;
            Some((numeric, left.checked_add(right)?.checked_add(numeric)?))
        }
        ExprNode::SubstringConst { expr, .. } => {
            let (bytes, work) = workspace(expr, batch)?;
            let bytes = bytes.checked_add(offsets)?;
            Some((bytes, work.checked_add(bytes)?))
        }
        ExprNode::Function { kernel, args, .. } => {
            if temporal_column(*kernel, args).is_some() {
                return Some((numeric, numeric.checked_mul(2)?));
            }
            if matches!(kernel, FunctionKernel::Concat) {
                let mut bytes = offsets;
                let mut work = 0usize;
                for arg in args {
                    let (payload, child) = workspace(arg, batch)?;
                    bytes = bytes.checked_add(payload)?;
                    work = work.checked_add(child)?;
                }
                return Some((bytes, work.checked_add(bytes)?));
            }
            let (input, work) = workspace(args.first()?, batch)?;
            let bytes = match kernel {
                FunctionKernel::Hex => input.checked_mul(2)?.checked_add(offsets)?,
                FunctionKernel::Md5
                | FunctionKernel::Sha
                | FunctionKernel::Sha1
                | FunctionKernel::Sha2 => hash_algorithm(*kernel, args)?
                    .hex_len()
                    .checked_mul(batch.num_rows())?
                    .checked_add(offsets)?,
                FunctionKernel::Length
                | FunctionKernel::CharLength
                | FunctionKernel::Abs
                | FunctionKernel::Sign
                | FunctionKernel::Ceil
                | FunctionKernel::Floor => numeric,
                FunctionKernel::DateFormat => {
                    let format = text(&args[1])?;
                    let output = (DateFormatPlan::max_format_bytes(&format) as usize)
                        .checked_mul(batch.num_rows())?
                        .checked_add(offsets)?;
                    return Some((
                        output,
                        work.checked_add(output)?
                            .checked_add(numeric)?
                            .checked_add(format.len().checked_mul(32)?)?,
                    ));
                }
                FunctionKernel::Lower | FunctionKernel::Upper => {
                    input.checked_mul(3)?.checked_add(offsets)?
                }
                FunctionKernel::Replace => input
                    .checked_mul(text(&args[2])?.len().max(1))?
                    .checked_add(offsets)?,
                _ => input.checked_add(offsets)?,
            };
            Some((bytes, work.checked_add(bytes)?))
        }
        _ => None,
    }
}

/// Compact only visible rows; invisible values never enter speculative kernels.
fn evaluate_selected(node: &ExprNode, batch: &Batch) -> Option<ColumnVector> {
    let rows = batch.num_rows();
    let input_bytes = batch.visible_indices().try_fold(0usize, |bytes, row| {
        batch.columns().iter().try_fold(bytes, |bytes, column| {
            bytes.checked_add(column.owned_value_bytes(row))
        })
    })?;
    let estimate = input_bytes
        .checked_mul(2)?
        .checked_add(rows.checked_mul(16)?)?;
    if estimate > crate::engine::executor::vec::evaluated_batch::byte_target() {
        return None;
    }
    let mut scratch = batch.memory_charge();
    if !scratch.add_within(estimate, usize::MAX) {
        return None;
    }
    let indices = batch
        .visible_indices()
        .map(|row| u32::try_from(row).ok())
        .collect::<Option<Vec<_>>>()?;
    let columns = batch
        .columns()
        .iter()
        .map(|column| column.take_indices_exact(&indices))
        .collect();
    let compact = Batch::with_row_count(batch.schema().clone(), columns, indices.len());
    let (_, work) = workspace(node, &compact)?;
    if work > crate::engine::executor::vec::evaluated_batch::byte_target().saturating_mul(2) {
        return None;
    }
    if !scratch.add_within(work.checked_mul(2)?, usize::MAX) {
        return None;
    }
    let result = evaluate(node, &compact)?;
    let mut positions = vec![usize::MAX; rows];
    for (position, row) in indices.into_iter().enumerate() {
        positions[row as usize] = position;
    }
    let mut valid = Bitmap::new_all_invalid(rows);
    for (row, &position) in positions.iter().enumerate() {
        if position != usize::MAX && !result.is_null(position) {
            valid.set_true(row);
        }
    }
    let nulls = Some(valid);
    macro_rules! scatter_fixed {
        ($data:expr,$variant:ident) => {{
            let data = positions
                .iter()
                .map(|&position| {
                    if position == usize::MAX {
                        Default::default()
                    } else {
                        $data[position]
                    }
                })
                .collect();
            Some(ColumnVector::$variant { data, nulls })
        }};
    }
    match result {
        ColumnVector::I64 { data, .. } => scatter_fixed!(data, I64),
        ColumnVector::Date32 { data, .. } => scatter_fixed!(data, Date32),
        ColumnVector::DateTime64 { data, .. } => scatter_fixed!(data, DateTime64),
        ColumnVector::Timestamp64 { data, .. } => scatter_fixed!(data, Timestamp64),
        ColumnVector::Null { .. } => Some(ColumnVector::Null { len: rows }),
        column @ (ColumnVector::Utf8 { .. } | ColumnVector::Bytes { .. }) => {
            let mut data = Vec::new();
            let mut offsets = Vec::with_capacity(rows + 1);
            offsets.push(0);
            for (row, &position) in positions.iter().enumerate() {
                if nulls.as_ref()?.get(row) {
                    data.extend_from_slice(bytes_at(&column, position)?);
                }
                offsets.push(u32::try_from(data.len()).ok()?);
            }
            Some(if matches!(column, ColumnVector::Bytes { .. }) {
                ColumnVector::Bytes {
                    data,
                    offsets,
                    nulls,
                }
            } else {
                ColumnVector::Utf8 {
                    data,
                    offsets,
                    nulls,
                }
            })
        }
        _ => None,
    }
}

fn operand<'a>(node: &ExprNode, batch: &'a Batch) -> Option<std::borrow::Cow<'a, ColumnVector>> {
    if let ExprNode::Column(index) = node {
        return batch.columns().get(*index).map(std::borrow::Cow::Borrowed);
    }
    evaluate(node, batch).map(std::borrow::Cow::Owned)
}

fn evaluate(node: &ExprNode, batch: &Batch) -> Option<ColumnVector> {
    let rows = batch.num_rows();
    match node {
        ExprNode::Column(index) => batch.columns().get(*index).cloned(),
        ExprNode::Const(_) | ExprNode::Placeholder(_) => {
            let value = constant(node)?;
            let value = value.as_ref();
            Some(match value {
                Value::Int(n) => ColumnVector::I64 {
                    data: vec![*n; rows],
                    nulls: None,
                },
                Value::Null => ColumnVector::Null { len: rows },
                Value::String(_) | Value::SmallString(_) | Value::Bytes(_) => {
                    let bytes = crate::engine::expr::value_bytes_ref(value);
                    let mut data = Vec::with_capacity(bytes.len().checked_mul(rows)?);
                    let mut offsets = Vec::with_capacity(rows + 1);
                    offsets.push(0);
                    for _ in 0..rows {
                        data.extend_from_slice(&bytes);
                        offsets.push(u32::try_from(data.len()).ok()?);
                    }
                    if matches!(value, Value::Bytes(_)) {
                        ColumnVector::Bytes {
                            data,
                            offsets,
                            nulls: None,
                        }
                    } else {
                        ColumnVector::Utf8 {
                            data,
                            offsets,
                            nulls: None,
                        }
                    }
                }
                _ => return None,
            })
        }
        ExprNode::SubstringConst { expr, start, len } => {
            slice(operand(expr, batch)?.as_ref(), *start, *len)
        }
        ExprNode::Binary {
            left, op, right, ..
        } => {
            let left_column = operand(left, batch)?;
            let right_column = operand(right, batch)?;
            let (
                ColumnVector::I64 {
                    data: left,
                    nulls: ln,
                },
                ColumnVector::I64 {
                    data: right,
                    nulls: rn,
                },
            ) = (left_column.as_ref(), right_column.as_ref())
            else {
                return None;
            };
            let nulls = crate::engine::executor::vec::expr_batch::merge_nulls(
                ln.as_ref(),
                rn.as_ref(),
                rows,
            );
            let mut data = Vec::with_capacity(rows);
            for (row, (l, r)) in left.iter().copied().zip(right.iter().copied()).enumerate() {
                data.push(if nulls.as_ref().is_some_and(|valid| !valid.get(row)) {
                    0
                } else {
                    match op {
                        BinaryOp::Add => l.checked_add(r)?,
                        BinaryOp::Sub => l.checked_sub(r)?,
                        BinaryOp::Mul => l.checked_mul(r)?,
                        BinaryOp::Eq => i64::from(l == r),
                        BinaryOp::NotEq => i64::from(l != r),
                        BinaryOp::Lt => i64::from(l < r),
                        BinaryOp::LtEq => i64::from(l <= r),
                        BinaryOp::Gt => i64::from(l > r),
                        BinaryOp::GtEq => i64::from(l >= r),
                        _ => return None,
                    }
                });
            }
            Some(ColumnVector::I64 { data, nulls })
        }
        ExprNode::Function { kernel, args, .. } => {
            if let Some((kind, index)) = temporal_column(*kernel, args) {
                return try_temporal_extract_column(kind, batch, index);
            }
            if let Some(kind) = transform(*kernel) {
                let column = operand(&args[0], batch)?;
                return match column.as_ref() {
                    ColumnVector::Utf8 {
                        offsets,
                        data,
                        nulls,
                    } => transform_utf8_column(kind, offsets, data, nulls.clone()),
                    ColumnVector::Bytes {
                        data,
                        offsets,
                        nulls,
                    } => {
                        if matches!(
                            kind,
                            StringTransformKind::Trim
                                | StringTransformKind::Ltrim
                                | StringTransformKind::Rtrim
                        ) {
                            return None;
                        }
                        let mut data = data.clone();
                        if matches!(kind, StringTransformKind::Reverse) {
                            for pair in offsets.windows(2) {
                                data[pair[0] as usize..pair[1] as usize].reverse();
                            }
                        }
                        Some(ColumnVector::Bytes {
                            data,
                            offsets: offsets.clone(),
                            nulls: nulls.clone(),
                        })
                    }
                    ColumnVector::Null { len } => Some(ColumnVector::Null { len: *len }),
                    _ => None,
                };
            }
            match kernel {
                FunctionKernel::Date => {
                    let column = operand(&args[0], batch)?;
                    if !matches!(
                        column.as_ref(),
                        ColumnVector::Date32 { .. } | ColumnVector::Null { .. }
                    ) {
                        return None;
                    }
                    Some(column.into_owned())
                }
                FunctionKernel::Abs
                | FunctionKernel::Floor
                | FunctionKernel::Ceil
                | FunctionKernel::Sign => {
                    let column = operand(&args[0], batch)?;
                    let ColumnVector::I64 { data, nulls } = column.as_ref() else {
                        return None;
                    };
                    let (mut data, nulls) = (data.clone(), nulls.clone());
                    for (row, value) in data.iter_mut().enumerate() {
                        if nulls.as_ref().is_some_and(|v| !v.get(row)) {
                            continue;
                        }
                        *value = match kernel {
                            FunctionKernel::Abs => value.checked_abs()?,
                            FunctionKernel::Sign => value.signum(),
                            _ => *value,
                        };
                    }
                    Some(ColumnVector::I64 { data, nulls })
                }
                FunctionKernel::Hex => {
                    let column = operand(args.first()?, batch)?;
                    hex_column(column.as_ref())
                }
                FunctionKernel::Md5
                | FunctionKernel::Sha
                | FunctionKernel::Sha1
                | FunctionKernel::Sha2 => {
                    let column = operand(&args[0], batch)?;
                    hash_column(hash_algorithm(*kernel, args)?, column.as_ref())
                }
                FunctionKernel::Length | FunctionKernel::CharLength => {
                    let col = operand(&args[0], batch)?;
                    let mut data = Vec::with_capacity(rows);
                    for row in 0..rows {
                        let bytes = bytes_at(&col, row)?;
                        data.push(
                            if matches!(kernel, FunctionKernel::CharLength)
                                && matches!(col.as_ref(), ColumnVector::Utf8 { .. })
                            {
                                std::str::from_utf8(bytes).ok()?.chars().count() as i64
                            } else {
                                bytes.len() as i64
                            },
                        );
                    }
                    Some(ColumnVector::I64 {
                        data,
                        nulls: validity(&col),
                    })
                }
                FunctionKernel::Concat => concat(args, batch),
                FunctionKernel::Substring => slice(
                    operand(&args[0], batch)?.as_ref(),
                    integer(&args[1])?,
                    if args.len() == 3 {
                        Some(integer(&args[2])?)
                    } else {
                        None
                    },
                ),
                FunctionKernel::Left | FunctionKernel::Right => {
                    let count = integer(&args[1])?.max(0);
                    let column = operand(&args[0], batch)?;
                    if matches!(kernel, FunctionKernel::Left) {
                        slice(&column, 1, Some(count))
                    } else if count == 0 {
                        slice(&column, 0, Some(0))
                    } else {
                        right(&column, count)
                    }
                }
                FunctionKernel::Replace => {
                    let column = operand(&args[0], batch)?;
                    let (from, to) = (text(&args[1])?, text(&args[2])?);
                    map_text(&column, |value, out| {
                        if from.is_empty() {
                            out.push_str(value);
                        } else {
                            let mut rest = value;
                            while let Some((head, tail)) = rest.split_once(from.as_ref()) {
                                out.push_str(head);
                                out.push_str(&to);
                                rest = tail;
                            }
                            out.push_str(rest);
                        }
                    })
                }
                FunctionKernel::DateFormat => {
                    format_dates(operand(&args[0], batch)?.as_ref(), &text(&args[1])?)
                }
                _ => None,
            }
        }
        _ => None,
    }
}

fn validity(column: &ColumnVector) -> Option<Bitmap> {
    if matches!(column, ColumnVector::Null { .. }) {
        Some(Bitmap::new_all_invalid(column.len()))
    } else {
        column.nulls().cloned()
    }
}

fn bytes_at(column: &ColumnVector, row: usize) -> Option<&[u8]> {
    match column {
        ColumnVector::Utf8 { offsets, data, .. } | ColumnVector::Bytes { offsets, data, .. } => {
            data.get(offsets[row] as usize..offsets[row + 1] as usize)
        }
        ColumnVector::Null { .. } => Some(&[]),
        _ => None,
    }
}
fn map_text(
    column: &ColumnVector,
    mut apply: impl FnMut(&str, &mut String),
) -> Option<ColumnVector> {
    let ColumnVector::Utf8 {
        offsets,
        data,
        nulls,
    } = column
    else {
        return None;
    };
    let text = simdutf8::basic::from_utf8(&data).ok()?;
    let mut output = String::with_capacity(data.len());
    let mut positions = Vec::with_capacity(offsets.len());
    positions.push(0);
    for (row, pair) in offsets.windows(2).enumerate() {
        if nulls.as_ref().is_none_or(|v| v.get(row)) {
            apply(text.get(pair[0] as usize..pair[1] as usize)?, &mut output);
        }
        positions.push(u32::try_from(output.len()).ok()?);
    }
    Some(ColumnVector::Utf8 {
        data: output.into_bytes(),
        offsets: positions,
        nulls: nulls.clone(),
    })
}
fn slice(column: &ColumnVector, start: i64, len: Option<i64>) -> Option<ColumnVector> {
    let binary = matches!(column, ColumnVector::Bytes { .. });
    let mut output = Vec::new();
    let mut offsets = Vec::with_capacity(column.len() + 1);
    offsets.push(0);
    for row in 0..column.len() {
        if !column.is_null(row) {
            let bytes = bytes_at(&column, row)?;
            if binary {
                let n = bytes.len() as i128;
                let pos = if start > 0 {
                    i128::from(start) - 1
                } else if start < 0 {
                    n + i128::from(start)
                } else {
                    n
                };
                if pos >= 0 && pos < n {
                    let end = (pos + len.map_or(n, |l| i128::from(l.max(0)))).min(n);
                    output.extend_from_slice(&bytes[pos as usize..end as usize]);
                }
            } else {
                let value = std::str::from_utf8(bytes).ok()?;
                let n = value.chars().count() as i128;
                let pos = if start > 0 {
                    i128::from(start) - 1
                } else if start < 0 {
                    n + i128::from(start)
                } else {
                    n
                };
                if pos >= 0 && pos < n {
                    let take = len.map_or(usize::MAX, |l| {
                        usize::try_from(l.max(0)).unwrap_or(usize::MAX)
                    });
                    for ch in value.chars().skip(pos as usize).take(take) {
                        let mut buffer = [0; 4];
                        output.extend_from_slice(ch.encode_utf8(&mut buffer).as_bytes());
                    }
                }
            }
        }
        offsets.push(u32::try_from(output.len()).ok()?);
    }
    let nulls = validity(&column);
    Some(if binary {
        ColumnVector::Bytes {
            data: output,
            offsets,
            nulls,
        }
    } else {
        ColumnVector::Utf8 {
            data: output,
            offsets,
            nulls,
        }
    })
}
fn hex_column(column: &ColumnVector) -> Option<ColumnVector> {
    let mut data = String::with_capacity(column.memory_bytes().checked_mul(2)?);
    let mut offsets = Vec::with_capacity(column.len() + 1);
    offsets.push(0);
    for row in 0..column.len() {
        if row & 63 == 0 {
            crate::engine::cancel::check_cancelled().ok()?;
        }
        if !column.is_null(row) {
            crate::util::hex::append_upper(&mut data, bytes_at(column, row)?);
        }
        offsets.push(u32::try_from(data.len()).ok()?);
    }
    Some(ColumnVector::Utf8 {
        data: data.into_bytes(),
        offsets,
        nulls: validity(column),
    })
}

fn hash_algorithm(
    kernel: FunctionKernel,
    args: &[ExprNode],
) -> Option<crate::engine::expr::digest::HashAlgorithm> {
    use crate::engine::expr::digest::HashAlgorithm;
    match kernel {
        FunctionKernel::Md5 => Some(HashAlgorithm::Md5),
        FunctionKernel::Sha | FunctionKernel::Sha1 => Some(HashAlgorithm::Sha1),
        FunctionKernel::Sha2 => HashAlgorithm::from_sha2_length(integer(args.get(1)?)?),
        _ => None,
    }
}

fn hash_column(
    algorithm: crate::engine::expr::digest::HashAlgorithm,
    column: &ColumnVector,
) -> Option<ColumnVector> {
    let mut data = Vec::with_capacity(column.len().checked_mul(algorithm.hex_len())?);
    let mut offsets = Vec::with_capacity(column.len() + 1);
    offsets.push(0);
    for row in 0..column.len() {
        if row & 63 == 0 {
            crate::engine::cancel::check_cancelled().ok()?;
        }
        if !column.is_null(row) {
            algorithm
                .append_hex(bytes_at(column, row)?, &mut data)
                .ok()?;
        }
        offsets.push(u32::try_from(data.len()).ok()?);
    }
    Some(ColumnVector::Utf8 {
        data,
        offsets,
        nulls: validity(column),
    })
}

fn right(column: &ColumnVector, count: i64) -> Option<ColumnVector> {
    let binary = matches!(column, ColumnVector::Bytes { .. });
    let mut data = Vec::new();
    let mut offsets = Vec::with_capacity(column.len() + 1);
    offsets.push(0);
    for row in 0..column.len() {
        if !column.is_null(row) {
            let bytes = bytes_at(&column, row)?;
            let start = if binary {
                bytes
                    .len()
                    .saturating_sub(usize::try_from(count).unwrap_or(usize::MAX))
            } else {
                let value = std::str::from_utf8(bytes).ok()?;
                let skip = value
                    .chars()
                    .count()
                    .saturating_sub(usize::try_from(count).unwrap_or(usize::MAX));
                value
                    .char_indices()
                    .nth(skip)
                    .map_or(bytes.len(), |(pos, _)| pos)
            };
            data.extend_from_slice(&bytes[start..]);
        }
        offsets.push(u32::try_from(data.len()).ok()?);
    }
    let nulls = validity(&column);
    Some(if binary {
        ColumnVector::Bytes {
            data,
            offsets,
            nulls,
        }
    } else {
        ColumnVector::Utf8 {
            data,
            offsets,
            nulls,
        }
    })
}

fn concat(args: &[ExprNode], batch: &Batch) -> Option<ColumnVector> {
    let columns = args
        .iter()
        .map(|arg| operand(arg, batch))
        .collect::<Option<Vec<_>>>()?;
    let binary = columns
        .iter()
        .any(|c| matches!(c.as_ref(), ColumnVector::Bytes { .. }));
    let rows = batch.num_rows();
    let mut nulls = Bitmap::new_all_valid(rows);
    let mut data = Vec::new();
    let mut offsets = Vec::with_capacity(rows + 1);
    offsets.push(0);
    let packet = crate::engine::sysvar::builtin::connection::max_allowed_packet_bytes();
    for row in 0..rows {
        if columns.iter().any(|col| col.is_null(row)) {
            nulls.set(row, false);
        } else {
            let size = columns.iter().try_fold(0usize, |size, col| {
                size.checked_add(bytes_at(col, row)?.len())
            })?;
            if size as u64 > packet {
                return None;
            }
            for col in &columns {
                data.extend_from_slice(bytes_at(col, row)?);
            }
        }
        offsets.push(u32::try_from(data.len()).ok()?);
    }
    let nulls = (nulls.count_ones() != rows).then_some(nulls);
    Some(if binary {
        ColumnVector::Bytes {
            data,
            offsets,
            nulls,
        }
    } else {
        ColumnVector::Utf8 {
            data,
            offsets,
            nulls,
        }
    })
}
fn format_dates(column: &ColumnVector, format: &str) -> Option<ColumnVector> {
    let rows = column.len();
    let plan = DateFormatPlan::new(format);
    let locale = crate::engine::expr::session_time_locale();
    let mut local = Vec::new();
    if let ColumnVector::Timestamp64 { data, nulls } = &column {
        local.resize(rows, 0);
        crate::engine::expr::fill_session_timestamp_local_datetime_micros(
            &mut local,
            data,
            (0..rows).filter(|&r| nulls.as_ref().is_none_or(|v| v.get(r))),
        )?;
    }
    let mut output = String::with_capacity(rows.checked_mul(plan.max_bytes() as usize)?);
    let mut offsets = Vec::with_capacity(rows + 1);
    offsets.push(0);
    let mut previous: Option<(i64, std::ops::Range<usize>)> = None;
    for row in 0..rows {
        if !column.is_null(row) {
            let key = match column {
                ColumnVector::Date32 { data, .. } => i64::from(data[row]),
                ColumnVector::DateTime64 { data, .. } | ColumnVector::Timestamp64 { data, .. } => {
                    data[row]
                }
                _ => return None,
            };
            if let Some((last, range)) = &previous {
                if *last == key {
                    output.extend_from_within(range.clone());
                    offsets.push(u32::try_from(output.len()).ok()?);
                    continue;
                }
            }
            let begin = output.len();
            let dt = match &column {
                ColumnVector::Date32 { data, .. } => {
                    if data[row] == crate::types::ZERO_DATE
                        || crate::types::decode_tagged_date(data[row]).is_some()
                    {
                        return None;
                    }
                    NaiveDate::from_ymd_opt(1970, 1, 1)?
                        .checked_add_signed(chrono::Duration::days(i64::from(data[row])))?
                        .and_hms_opt(0, 0, 0)?
                }
                ColumnVector::DateTime64 { data, .. } => {
                    if data[row] == crate::types::ZERO_DATETIME
                        || crate::types::decode_tagged_datetime(data[row]).is_some()
                    {
                        return None;
                    }
                    chrono::DateTime::from_timestamp_micros(data[row])?.naive_utc()
                }
                ColumnVector::Timestamp64 { data, .. } => {
                    if data[row] == crate::types::ZERO_DATETIME {
                        return None;
                    }
                    chrono::DateTime::from_timestamp_micros(local[row])?.naive_utc()
                }
                _ => return None,
            };
            if !(0..=9999).contains(&dt.year()) {
                return None;
            }
            plan.append(dt, locale, &mut output);
            previous = Some((key, begin..output.len()));
        }
        offsets.push(u32::try_from(output.len()).ok()?);
    }
    Some(ColumnVector::Utf8 {
        data: output.into_bytes(),
        offsets,
        nulls: validity(&column),
    })
}

#[cfg(test)]
mod tests;
