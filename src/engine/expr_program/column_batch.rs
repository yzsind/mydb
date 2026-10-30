//! Composable, diagnostic-free column kernels. A decline is side-effect free:
//! the caller retains the ordered scalar oracle for the complete expression.
mod boolean_read;
mod conditional;
mod crypto_read;
mod format_read;
mod integer_string;
mod json;
mod numeric_read;
mod producer;
mod regexp_read;
mod string_read;
mod temporal_read;
mod temporal_shift;
mod unsigned_read;
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
use crate::engine::expr::network::{self, NetworkFunction};
use boolean_read::BooleanRead;
use chrono::{Datelike, NaiveDate};
use conditional::Conditional;
use crypto_read::CryptoRead;
use format_read::FormatRead;
use integer_string::IntegerString;
use json::JsonRead;
use numeric_read::NumericRead;
use producer::Producer;
use regexp_read::RegexpRead;
use string_read::StringRead;
use temporal_read::TemporalRead;
use temporal_shift::TemporalShift;
use unsigned_read::UnsignedRead;

#[derive(Clone, Copy)]
enum Domain {
    Integer(i64, i64),
    Unsigned,
    Exact,
    Approximate,
    Text,
    Binary,
    Temporal,
    Json,
    Null,
}
struct Contract {
    domain: Domain,
    parallel: bool,
}

#[cfg(test)]
thread_local! {
    static CONTRACT_BIND_VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
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
        let (payload, _) = workspace(&self.root, batch)?;
        let limit = crate::engine::executor::vec::evaluated_batch::byte_target().saturating_mul(2);
        let capacity = payload.checked_mul(2)?;
        (capacity <= limit).then_some(capacity)
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
                evaluate_in_chunks(&self.root, batch, self.referenced_columns(), payload, work)?
            } else {
                let mut scratch = batch.memory_charge();
                if !scratch.add_within(work.checked_mul(2)?, usize::MAX) {
                    return None;
                }
                evaluate(&self.root, batch)?
            }
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
    let read = |value: &Value| match value {
        Value::Int(n) => Some(*n),
        Value::UInt(n) => i64::try_from(*n).ok(),
        _ => None,
    };
    match node {
        ExprNode::Const(value) => read(value),
        ExprNode::Placeholder(index) => {
            crate::engine::params::with_current_param(*index, |value| read(value?))
        }
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

/// Per-row octet bound used only to prove absence of packet warnings before
/// moving a text expression into scan workers. Unknown bounds stay ordered.
fn string_bound(node: &ExprNode, schema: &BatchSchema) -> Option<u64> {
    if matches!(contract(node, schema)?.domain, Domain::Integer(..)) {
        return Some(20);
    }
    match node {
        ExprNode::Column(index) => schema.fields().get(*index)?.source.length.map(u64::from),
        ExprNode::Const(_) | ExprNode::Placeholder(_) => Some(match constant(node)?.as_ref() {
            Value::String(s) => s.len() as u64,
            Value::SmallString(s) => s.len() as u64,
            Value::Bytes(b) => b.len() as u64,
            Value::Null => 0,
            _ => return None,
        }),
        ExprNode::Function {
            kernel: FunctionKernel::LegacyDeterministic("CAST"),
            args,
            ..
        } => string_bound(&args[0], schema),
        ExprNode::Function {
            kernel: FunctionKernel::Lpad | FunctionKernel::Rpad,
            args,
            ..
        } => u64::try_from(integer(&args[1])?).ok()?.checked_mul(4),
        _ => None,
    }
}

fn contract(node: &ExprNode, schema: &BatchSchema) -> Option<Contract> {
    #[cfg(test)]
    CONTRACT_BIND_VISITS.with(|visits| visits.set(visits.get() + 1));
    if let Some(read) = NumericRead::bind(node, schema) {
        return Some(read.contract());
    }
    if let Some(read) = FormatRead::bind(node, schema) {
        return Some(read.contract());
    }
    if let Some(read) = BooleanRead::bind(node, schema) {
        return Some(read.contract());
    }
    if let Some(read) = Conditional::bind(node, schema) {
        return Some(read.contract());
    }
    if let Some(read) = CryptoRead::bind(node, schema) {
        return Some(read.contract());
    }
    if matches!(node, ExprNode::Function { .. }) {
        if let Some(read) = UnsignedRead::bind(node, schema) {
            return Some(read.contract());
        }
    }
    if let Some(read) = IntegerString::bind(node, schema) {
        return Some(read.contract());
    }
    if let Some(read) = RegexpRead::bind(node, schema) {
        return Some(read.contract());
    }
    if let Some(read) = StringRead::bind(node, schema) {
        return Some(read.contract());
    }
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
                ColumnType::Tiny
                | ColumnType::Short
                | ColumnType::MediumInt
                | ColumnType::Long
                | ColumnType::LongLong => Domain::Unsigned,
                ColumnType::Date | ColumnType::DateTime | ColumnType::Timestamp => Domain::Temporal,
                ColumnType::Json => Domain::Json,
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
                Value::UInt(_) => Domain::Unsigned,
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
            op: BinaryOp::Arrow | BinaryOp::LongArrow,
            ..
        } => {
            return Some(JsonRead::bind(node, schema)?.contract());
        }
        ExprNode::Binary {
            left, right, op, ..
        } if unsigned_read::bitwise(*op) => {
            let (a, b) = (contract(left, schema)?, contract(right, schema)?);
            let numeric = |d| matches!(d, Domain::Integer(..) | Domain::Unsigned | Domain::Null);
            if !numeric(a.domain) || !numeric(b.domain) {
                return None;
            }
            (Domain::Unsigned, a.parallel && b.parallel)
        }
        ExprNode::Binary {
            left,
            op,
            right,
            semantics,
        } if (matches!(
            op,
            BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Mod
        ) || comparison(*op))
            && !semantics.unsigned_subtraction =>
        {
            let (l, r) = (contract(left, schema)?, contract(right, schema)?);
            let (Domain::Integer(a, b), Domain::Integer(c, d)) = (l.domain, r.domain) else {
                let nonnegative = |domain| {
                    matches!(domain, Domain::Unsigned | Domain::Null)
                        || matches!(domain,Domain::Integer(min,_) if min>=0)
                };
                return (nonnegative(l.domain)
                    && nonnegative(r.domain)
                    && (matches!(l.domain, Domain::Unsigned)
                        || matches!(r.domain, Domain::Unsigned)))
                .then_some(Contract {
                    domain: if comparison(*op) {
                        Domain::Integer(0, 1)
                    } else {
                        Domain::Unsigned
                    },
                    parallel: comparison(*op) && l.parallel && r.parallel,
                });
            };
            if comparison(*op) {
                return Some(Contract {
                    domain: Domain::Integer(0, 1),
                    parallel: l.parallel && r.parallel,
                });
            }
            if matches!(op, BinaryOp::Mod) {
                // |MIN| is 2^63: take the unsigned magnitude before
                // subtracting one, so MAX % MIN remains inside the proof.
                let bound = c
                    .unsigned_abs()
                    .max(d.unsigned_abs())
                    .saturating_sub(1)
                    .min(i64::MAX as u64) as i64;
                return Some(Contract {
                    domain: Domain::Integer(-bound, bound),
                    parallel: l.parallel
                        && r.parallel
                        && (c > 0 || d < 0)
                        && !(a == i64::MIN && c <= -1 && d >= -1),
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
        ExprNode::Function {
            kernel,
            args,
            result_coercion,
            ..
        } => {
            if matches!(kernel, FunctionKernel::DateAdd | FunctionKernel::DateSub) {
                return Some(TemporalShift::bind(node, schema)?.contract());
            }
            if matches!(
                kernel,
                FunctionKernel::LegacyDeterministic("GREATEST" | "LEAST")
            ) {
                if args.len() < 2 || result_coercion.is_some() {
                    return None;
                }
                let mut parallel = true;
                let mut all_null = false;
                for arg in args {
                    if let ExprNode::Column(index) = arg {
                        if schema.fields().get(*index)?.source.flags
                            & (crate::mysql::protocol::ENUM_FLAG | crate::mysql::protocol::SET_FLAG)
                            != 0
                        {
                            return None;
                        }
                    }
                    let child = contract(arg, schema)?;
                    match child.domain {
                        Domain::Integer(..) => {}
                        Domain::Null => all_null = true,
                        _ => return None,
                    }
                    parallel &= child.parallel;
                }
                return Some(Contract {
                    domain: if all_null {
                        Domain::Null
                    } else {
                        Domain::Integer(i64::MIN, i64::MAX)
                    },
                    parallel,
                });
            }
            if let Some(read) = TemporalRead::bind(node, schema) {
                return Some(read.contract());
            }
            if matches!(
                kernel,
                FunctionKernel::LegacyDeterministic("JSON_ARRAY" | "JSON_OBJECT")
            ) {
                Producer::bind(node, schema)?;
                return Some(Contract {
                    domain: Domain::Json,
                    parallel: true,
                });
            }
            if matches!(kernel, FunctionKernel::LegacyDeterministic("JSON_QUOTE")) {
                json::quote_column(node, schema)?;
                return Some(Contract {
                    domain: Domain::Json,
                    parallel: true,
                });
            }
            if matches!(
                kernel,
                FunctionKernel::JsonExtract
                    | FunctionKernel::LegacyDeterministic(
                        "JSON_UNQUOTE"
                            | "JSON_PRETTY"
                            | "JSON_SEARCH"
                            | "JSON_TYPE"
                            | "JSON_LENGTH"
                            | "JSON_VALID"
                            | "JSON_KEYS"
                            | "JSON_CONTAINS_PATH"
                            | "JSON_OVERLAPS"
                            | "JSON_STORAGE_SIZE"
                            | "JSON_STORAGE_FREE"
                            | "JSON_VALUE"
                    )
            ) {
                return Some(JsonRead::bind(node, schema)?.contract());
            }
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
                    (FunctionKernel::Network(function), args) => {
                        function.check_arity(args).ok()?;
                        let child = contract(&args[0], schema)?;
                        if args.len() == 2 && integer(&args[1]).is_none() {
                            return None;
                        }
                        let valid = if matches!(function, NetworkFunction::InetNtoa) {
                            matches!(child.domain, Domain::Integer(..) | Domain::Null)
                        } else {
                            matches!(child.domain, Domain::Text | Domain::Binary | Domain::Null)
                        };
                        if !valid {
                            return None;
                        }
                        let domain = if function.is_predicate() {
                            Domain::Integer(0, 1)
                        } else {
                            match function {
                                NetworkFunction::InetAton => Domain::Integer(0, u32::MAX as i64),
                                NetworkFunction::UuidToBin | NetworkFunction::Inet6Aton => {
                                    Domain::Binary
                                }
                                _ => Domain::Text,
                            }
                        };
                        (domain, child.parallel && function.is_predicate())
                    }
                    (FunctionKernel::LegacyDeterministic("CAST"), [arg, target])
                        if text(target).is_some_and(|s| s.eq_ignore_ascii_case("CHAR")) =>
                    {
                        let child = contract(arg, schema)?;
                        if !matches!(
                            child.domain,
                            Domain::Integer(..) | Domain::Text | Domain::Null
                        ) {
                            return None;
                        }
                        (Domain::Text, child.parallel)
                    }
                    (FunctionKernel::Lpad | FunctionKernel::Rpad, [arg, length, pad])
                        if integer(length).is_some_and(|n| n >= 0) && text(pad).is_some() =>
                    {
                        let child = contract(arg, schema)?;
                        if !matches!(child.domain, Domain::Text | Domain::Null) {
                            return None;
                        }
                        (Domain::Text, child.parallel && (integer(length)? as u64).checked_mul(4).is_some_and(|bytes| bytes <= crate::engine::sysvar::builtin::connection::max_allowed_packet_bytes()))
                    }
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
                        let mut parallel = true;
                        let mut bound = Some(0u64);
                        for arg in args {
                            let child = contract(arg, schema)?;
                            parallel &= child.parallel;
                            bound = bound.and_then(|n| n.checked_add(string_bound(arg, schema)?));
                            match child.domain {
                                Domain::Text | Domain::Integer(..) | Domain::Null => {}
                                Domain::Binary => binary = true,
                                _ => return None,
                            }
                        }
                        parallel &= bound.is_some_and(|bytes| bytes <= crate::engine::sysvar::builtin::connection::max_allowed_packet_bytes());
                        (if binary { Domain::Binary } else { Domain::Text }, parallel)
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
    if let Some(read) = NumericRead::bind(node, batch.schema()) {
        return read.workspace(batch);
    }
    if let Some(read) = FormatRead::bind(node, batch.schema()) {
        return read.workspace(batch);
    }
    if let Some(read) = BooleanRead::bind(node, batch.schema()) {
        return read.workspace(batch);
    }
    if let Some(read) = Conditional::bind(node, batch.schema()) {
        return read.workspace(batch);
    }
    if let Some(read) = CryptoRead::bind(node, batch.schema()) {
        return read.workspace(batch);
    }
    if let Some(read) = UnsignedRead::bind(node, batch.schema()) {
        return read.workspace(batch);
    }
    if let Some(read) = IntegerString::bind(node, batch.schema()) {
        return read.workspace(batch);
    }
    if let Some(read) = RegexpRead::bind(node, batch.schema()) {
        return read.workspace(batch);
    }
    if let Some(read) = StringRead::bind(node, batch.schema()) {
        return read.workspace(batch);
    }
    if let Some(read) = TemporalRead::bind(node, batch.schema()) {
        return read.workspace(batch);
    }
    if let Some(producer) = Producer::bind(node, batch.schema()) {
        return producer.workspace(batch);
    }

    if let Some(column) = json::quote_column(node, batch.schema()) {
        let input = batch.columns().get(column)?;
        let bytes = input
            .memory_bytes()
            .checked_add(batch.num_rows().checked_mul(16)?)?
            .checked_add(8)?;
        return Some((bytes, bytes.checked_mul(2)?.checked_add(256)?));
    }
    if !matches!(node, ExprNode::Column(_)) {
        if let Some(read) = JsonRead::bind(node, batch.schema()) {
            return read.workspace(batch);
        }
    }
    let numeric = batch.num_rows().checked_mul(16)?;
    let offsets = batch.num_rows().checked_add(1)?.checked_mul(8)?;
    match node {
        ExprNode::Column(index) => Some((batch.columns().get(*index)?.memory_bytes(), 0)),
        ExprNode::Const(_) | ExprNode::Placeholder(_) => {
            let value = constant(node)?;
            let bytes = match value.as_ref() {
                Value::Null => 0,
                Value::Int(_) | Value::UInt(_) => numeric,
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
            if matches!(kernel, FunctionKernel::DateAdd | FunctionKernel::DateSub) {
                return TemporalShift::bind(node, batch.schema())?.workspace(batch);
            }
            if matches!(
                kernel,
                FunctionKernel::LegacyDeterministic("GREATEST" | "LEAST")
            ) {
                let mut work = numeric.checked_add(
                    args.len()
                        .checked_mul(std::mem::size_of::<std::borrow::Cow<'_, ColumnVector>>())?,
                )?;
                for arg in args {
                    work = work.checked_add(workspace(arg, batch)?.1)?;
                }
                return Some((numeric, work));
            }
            if temporal_column(*kernel, args).is_some() {
                return Some((numeric, numeric.checked_mul(2)?));
            }
            if matches!(kernel, FunctionKernel::Concat) {
                let mut bytes = offsets;
                let mut work = 0usize;
                for arg in args {
                    let (payload, child) = workspace(arg, batch)?;
                    let payload =
                        if matches!(contract(arg, batch.schema())?.domain, Domain::Integer(..)) {
                            batch.num_rows().checked_mul(20)?.checked_add(offsets)?
                        } else {
                            payload
                        };
                    bytes = bytes.checked_add(payload)?;
                    work = work.checked_add(child)?;
                }
                return Some((bytes, work.checked_add(bytes)?));
            }
            let (input, work) = workspace(args.first()?, batch)?;
            let bytes = match kernel {
                FunctionKernel::Network(function) => match function {
                    NetworkFunction::UuidToBin | NetworkFunction::Inet6Aton => {
                        batch.num_rows().checked_mul(16)?.checked_add(offsets)?
                    }
                    NetworkFunction::BinToUuid => {
                        batch.num_rows().checked_mul(36)?.checked_add(offsets)?
                    }
                    NetworkFunction::InetNtoa | NetworkFunction::Inet6Ntoa => {
                        batch.num_rows().checked_mul(39)?.checked_add(offsets)?
                    }
                    _ => numeric,
                },
                FunctionKernel::LegacyDeterministic("CAST") => {
                    if matches!(
                        contract(&args[0], batch.schema())?.domain,
                        Domain::Integer(..)
                    ) {
                        batch.num_rows().checked_mul(20)?.checked_add(offsets)?
                    } else {
                        input.checked_add(offsets)?
                    }
                }
                FunctionKernel::Lpad | FunctionKernel::Rpad => {
                    let n = usize::try_from(integer(&args[1])?).ok()?;
                    let pad = text(&args[2])?;
                    input
                        .checked_add(
                            batch
                                .num_rows()
                                .checked_mul(n.checked_mul(pad.len().max(4))?)?,
                        )?
                        .checked_add(offsets)?
                }
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

/// Large scan batches need not force composed expressions back to row Values.
/// Retain one bounded result and evaluate only referenced input lanes in small
/// morsels. All allocations are admitted before materializing a morsel; a
/// decline discards every speculative result without publishing diagnostics.
fn evaluate_in_chunks(
    node: &ExprNode,
    batch: &Batch,
    referenced: &[usize],
    payload: usize,
    work: usize,
) -> Option<ColumnVector> {
    let limit = crate::engine::executor::vec::evaluated_batch::byte_target().checked_mul(2)?;
    if payload.checked_mul(2)? > limit || batch.num_rows() <= 1 {
        return None;
    }
    let mut output_charge = batch.memory_charge();
    if !output_charge.add_within(payload.checked_mul(2)?, usize::MAX) {
        return None;
    }
    let count = batch
        .num_rows()
        .checked_mul(limit)?
        .checked_div(work.max(1))?
        .clamp(1, 4096);
    let mut output = ColumnVector::Null { len: 0 };
    for begin in (0..batch.num_rows()).step_by(count) {
        crate::engine::cancel::check_cancelled().ok()?;
        let end = begin.saturating_add(count).min(batch.num_rows());
        let rows = end - begin;
        let input_bytes = referenced.iter().try_fold(0usize, |size, &column| {
            (begin..end).try_fold(size, |size, row| {
                size.checked_add(batch.columns().get(column)?.owned_value_bytes(row))
            })
        })?;
        let input_charge = input_bytes
            .checked_mul(2)?
            .checked_add(
                batch
                    .columns()
                    .len()
                    .checked_mul(std::mem::size_of::<ColumnVector>())?,
            )?
            .checked_add(128)?;
        if input_charge.checked_add(rows.checked_mul(16)?)? > limit {
            return None;
        }
        let mut charge = batch.memory_charge();
        if !charge.add_within(input_charge, usize::MAX) {
            return None;
        }
        let mut index_charge = batch.memory_charge();
        if !index_charge.add_within(rows.checked_mul(16)?, usize::MAX) {
            return None;
        }
        let indices = (begin..end)
            .map(|row| u32::try_from(row).ok())
            .collect::<Option<Vec<_>>>()?;
        let columns = batch
            .columns()
            .iter()
            .enumerate()
            .map(|(index, column)| {
                if referenced.binary_search(&index).is_ok() {
                    column.take_indices_exact(&indices)
                } else {
                    ColumnVector::Null { len: rows }
                }
            })
            .collect();
        let compact = Batch::with_row_count(batch.schema().clone(), columns, rows);
        let actual = compact
            .memory_bytes()
            .checked_add(
                compact
                    .columns()
                    .len()
                    .checked_mul(std::mem::size_of::<ColumnVector>())?,
            )?
            .checked_add(128)?;
        if actual > charge.charged() && !charge.add_within(actual - charge.charged(), usize::MAX) {
            return None;
        }
        let compact = compact.with_memory_charge(charge).ok()?;
        let (_, work) = workspace(node, &compact)?;
        if work > limit {
            return None;
        }
        let mut temporary = compact.memory_charge();
        if !temporary.add_within(work.checked_mul(2)?, usize::MAX) {
            return None;
        }
        let result = evaluate(node, &compact)?;
        // Typed-lane drift must not promote a small byte result into an
        // unbudgeted Generic<Value> buffer. The ordered oracle can handle it.
        if output.len() != 0 && std::mem::discriminant(&output) != std::mem::discriminant(&result) {
            return None;
        }
        if output.len() == 0 {
            output = result;
        } else {
            output.append_rows_from(&result, &(0..rows).collect::<Vec<_>>());
        }
        if output.memory_bytes() > payload.checked_mul(2)? {
            return None;
        }
    }
    Some(output)
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
        ColumnVector::U64 { data, .. } => scatter_fixed!(data, U64),
        ColumnVector::Decimal128 { data, scale, .. } => {
            let data = positions
                .iter()
                .map(|&position| {
                    if position == usize::MAX {
                        0
                    } else {
                        data[position]
                    }
                })
                .collect();
            Some(ColumnVector::Decimal128 { data, scale, nulls })
        }
        ColumnVector::Date32 { data, .. } => scatter_fixed!(data, Date32),
        ColumnVector::Time64 { data, .. } => scatter_fixed!(data, Time64),
        ColumnVector::DateTime64 { data, .. } => scatter_fixed!(data, DateTime64),
        ColumnVector::Timestamp64 { data, .. } => scatter_fixed!(data, Timestamp64),
        ColumnVector::Null { .. } => Some(ColumnVector::Null { len: rows }),
        column @ (ColumnVector::Utf8 { .. }
        | ColumnVector::Bytes { .. }
        | ColumnVector::Json { .. }) => {
            let mut data = Vec::new();
            let mut offsets = Vec::with_capacity(rows + 1);
            offsets.push(0);
            for (row, &position) in positions.iter().enumerate() {
                if nulls.as_ref()?.get(row) {
                    let raw = if let ColumnVector::Json { data, offsets, .. } = &column {
                        data.get(offsets[position] as usize..offsets[position + 1] as usize)?
                    } else {
                        bytes_at(&column, position)?
                    };
                    data.extend_from_slice(raw);
                }
                offsets.push(u32::try_from(data.len()).ok()?);
            }
            Some(if matches!(column, ColumnVector::Bytes { .. }) {
                ColumnVector::Bytes {
                    data,
                    offsets,
                    nulls,
                }
            } else if matches!(column, ColumnVector::Json { .. }) {
                ColumnVector::Json {
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
    if let Some(read) = NumericRead::bind(node, batch.schema()) {
        return read.evaluate(batch);
    }
    if let Some(read) = FormatRead::bind(node, batch.schema()) {
        return read.evaluate(batch);
    }
    if let Some(read) = BooleanRead::bind(node, batch.schema()) {
        return read.evaluate(batch);
    }
    if let Some(read) = Conditional::bind(node, batch.schema()) {
        return read.evaluate(batch);
    }
    if let Some(read) = CryptoRead::bind(node, batch.schema()) {
        return read.evaluate(batch);
    }
    if let Some(read) = UnsignedRead::bind(node, batch.schema()) {
        return read.evaluate(batch);
    }
    if let Some(read) = IntegerString::bind(node, batch.schema()) {
        return read.evaluate(batch);
    }
    if let Some(read) = RegexpRead::bind(node, batch.schema()) {
        return read.evaluate(batch);
    }
    if let Some(read) = StringRead::bind(node, batch.schema()) {
        return read.evaluate(batch);
    }
    if let Some(read) = TemporalRead::bind(node, batch.schema()) {
        return read.evaluate(batch);
    }
    if let Some(producer) = Producer::bind(node, batch.schema()) {
        return producer.evaluate(batch);
    }

    if let Some(column) = json::quote_column(node, batch.schema()) {
        return json::evaluate_quote(batch.columns().get(column)?);
    }
    if !matches!(node, ExprNode::Column(_)) {
        if let Some(read) = JsonRead::bind(node, batch.schema()) {
            return read.evaluate(batch);
        }
    }
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
                Value::UInt(n) => ColumnVector::U64 {
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
                        BinaryOp::Mod => l.checked_rem(r)?,
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
            if matches!(kernel, FunctionKernel::DateAdd | FunctionKernel::DateSub) {
                return TemporalShift::bind(node, batch.schema())?.evaluate(batch);
            }
            if let FunctionKernel::LegacyDeterministic(name @ ("GREATEST" | "LEAST")) = kernel {
                return integer_extrema_column(args, batch, *name == "GREATEST");
            }
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
                FunctionKernel::Network(function) => {
                    let column = operand(&args[0], batch)?;
                    network_column(
                        *function,
                        column.as_ref(),
                        args.get(1).and_then(integer).unwrap_or(0) != 0,
                    )
                }
                FunctionKernel::LegacyDeterministic("CAST") => {
                    let column = operand(&args[0], batch)?;
                    integer_text_column(column.as_ref())
                }
                FunctionKernel::Lpad | FunctionKernel::Rpad => {
                    let column = operand(&args[0], batch)?;
                    pad_column(
                        column.as_ref(),
                        usize::try_from(integer(&args[1])?).ok()?,
                        &text(&args[2])?,
                        matches!(kernel, FunctionKernel::Lpad),
                    )
                }
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

// A signed integer comparison never converts through DOUBLE or text.
// Unsupported runtime lanes decline before any diagnostic is published.
fn integer_extrema_column(
    args: &[ExprNode],
    batch: &Batch,
    greatest: bool,
) -> Option<ColumnVector> {
    let columns = args
        .iter()
        .map(|arg| operand(arg, batch))
        .collect::<Option<Vec<_>>>()?;
    let rows = batch.num_rows();
    let mut nulls = None;
    for column in &columns {
        match column.as_ref() {
            ColumnVector::I64 { nulls: valid, .. } => {
                nulls = crate::engine::executor::vec::expr_batch::merge_nulls(
                    nulls.as_ref(),
                    valid.as_ref(),
                    rows,
                );
            }
            ColumnVector::Null { .. } => nulls = Some(Bitmap::new_all_invalid(rows)),
            _ => return None,
        }
    }
    let mut data = Vec::with_capacity(rows);
    for row in 0..rows {
        if row & 63 == 0 {
            crate::engine::cancel::check_cancelled().ok()?;
        }
        if nulls.as_ref().is_some_and(|valid| !valid.get(row)) {
            data.push(0);
            continue;
        }
        let mut winner = if greatest { i64::MIN } else { i64::MAX };
        for column in &columns {
            let ColumnVector::I64 { data, .. } = column.as_ref() else {
                return None;
            };
            winner = if greatest {
                winner.max(data[row])
            } else {
                winner.min(data[row])
            };
        }
        data.push(winner);
    }
    Some(ColumnVector::I64 { data, nulls })
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
            let begin = data.len();
            for col in &columns {
                if let ColumnVector::I64 { data: values, .. } = col.as_ref() {
                    let mut buffer = itoa::Buffer::new();
                    data.extend_from_slice(buffer.format(values[row]).as_bytes());
                } else {
                    data.extend_from_slice(bytes_at(col, row)?);
                }
                if (data.len() - begin) as u64 > packet {
                    return None;
                }
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

fn integer_text_column(column: &ColumnVector) -> Option<ColumnVector> {
    if matches!(
        column,
        ColumnVector::Utf8 { .. } | ColumnVector::Null { .. }
    ) {
        return Some(column.clone());
    }
    let ColumnVector::I64 { data: values, .. } = column else {
        return None;
    };
    let mut data = Vec::with_capacity(values.len().checked_mul(20)?);
    let mut offsets = Vec::with_capacity(values.len() + 1);
    offsets.push(0);
    for (row, value) in values.iter().enumerate() {
        if row & 63 == 0 {
            crate::engine::cancel::check_cancelled().ok()?;
        }
        if !column.is_null(row) {
            let mut buffer = itoa::Buffer::new();
            data.extend_from_slice(buffer.format(*value).as_bytes());
        }
        offsets.push(u32::try_from(data.len()).ok()?);
    }
    Some(ColumnVector::Utf8 {
        data,
        offsets,
        nulls: validity(column),
    })
}

fn pad_column(column: &ColumnVector, length: usize, pad: &str, left: bool) -> Option<ColumnVector> {
    let mut data = Vec::new();
    let mut offsets = Vec::with_capacity(column.len() + 1);
    offsets.push(0);
    let pad_chars = pad.chars().count();
    let packet = crate::engine::sysvar::builtin::connection::max_allowed_packet_bytes();
    for row in 0..column.len() {
        if row & 63 == 0 {
            crate::engine::cancel::check_cancelled().ok()?;
        }
        if !column.is_null(row) {
            let value = std::str::from_utf8(bytes_at(column, row)?).ok()?;
            let chars = value.chars().count();
            if chars >= length {
                let end = value
                    .char_indices()
                    .nth(length)
                    .map_or(value.len(), |(i, _)| i);
                data.extend_from_slice(&value.as_bytes()[..end]);
            } else if pad_chars != 0 {
                let need = length - chars;
                let cycles = need / pad_chars;
                let tail = pad
                    .char_indices()
                    .nth(need % pad_chars)
                    .map_or(pad.len(), |(i, _)| i);
                let total = cycles
                    .checked_mul(pad.len())?
                    .checked_add(tail)?
                    .checked_add(value.len())?;
                if total as u64 > packet {
                    return None;
                }
                if !left {
                    data.extend_from_slice(value.as_bytes());
                }
                for _ in 0..cycles {
                    data.extend_from_slice(pad.as_bytes());
                }
                data.extend_from_slice(&pad.as_bytes()[..tail]);
                if left {
                    data.extend_from_slice(value.as_bytes());
                }
            }
        }
        offsets.push(u32::try_from(data.len()).ok()?);
    }
    Some(ColumnVector::Utf8 {
        data,
        offsets,
        nulls: validity(column),
    })
}

/// Speculative parsing never publishes diagnostics. Invalid input declines the
/// entire batch so the scalar evaluator reports its original warning/error order.
fn network_column(
    function: NetworkFunction,
    column: &ColumnVector,
    swap: bool,
) -> Option<ColumnVector> {
    let mut numeric = Vec::new();
    let mut data = Vec::new();
    let mut offsets = Vec::with_capacity(column.len() + 1);
    offsets.push(0);
    for row in 0..column.len() {
        if row & 63 == 0 {
            crate::engine::cancel::check_cancelled().ok()?;
        }
        if column.is_null(row) {
            if function.is_predicate() || matches!(function, NetworkFunction::InetAton) {
                numeric.push(0);
            }
            offsets.push(u32::try_from(data.len()).ok()?);
            continue;
        }
        if matches!(function, NetworkFunction::InetNtoa) {
            let n = match column {
                ColumnVector::I64 { data, .. } => u32::try_from(data[row]).ok()?,
                ColumnVector::U64 { data, .. } => u32::try_from(data[row]).ok()?,
                _ => return None,
            };
            network::append_ipv4(n.to_be_bytes(), &mut data);
        } else {
            let raw = bytes_at(column, row)?;
            // These textual grammars accept ASCII only. Invalid UTF-8 cannot
            // become a valid address/UUID after lossy scalar conversion.
            let text = std::str::from_utf8(raw).unwrap_or("");
            match function {
                NetworkFunction::IsUuid => {
                    numeric.push(network::parse_uuid_bytes(&text).is_some() as i64)
                }
                NetworkFunction::IsIpv4 => {
                    numeric.push(network::parse_mysql_strict_ipv4(&text).is_some() as i64)
                }
                NetworkFunction::IsIpv6 => {
                    numeric.push(network::parse_ipv6(&text).is_some() as i64)
                }
                NetworkFunction::IsIpv4Compat | NetworkFunction::IsIpv4Mapped => numeric.push(
                    (matches!(column, ColumnVector::Bytes { .. }) && function.binary_predicate(raw))
                        as i64,
                ),
                NetworkFunction::InetAton => {
                    numeric.push(network::parse_mysql_legacy_ipv4(&text)? as i64)
                }
                NetworkFunction::UuidToBin => {
                    let mut bytes = network::parse_uuid_bytes(&text)?;
                    if swap {
                        network::swap_uuid_time_parts(&mut bytes);
                    }
                    data.extend_from_slice(&bytes);
                }
                NetworkFunction::BinToUuid => {
                    let mut bytes: [u8; 16] = raw.try_into().ok()?;
                    if swap {
                        network::unswap_uuid_time_parts(&mut bytes);
                    }
                    crate::util::uuid::append_ascii(&bytes, &mut data);
                }
                NetworkFunction::Inet6Aton => {
                    if let Some(bytes) = network::parse_mysql_strict_ipv4(&text) {
                        data.extend_from_slice(&bytes);
                    } else {
                        data.extend_from_slice(&network::parse_ipv6(&text)?.octets());
                    }
                }
                NetworkFunction::Inet6Ntoa => {
                    if !matches!(column, ColumnVector::Bytes { .. }) {
                        return None;
                    }
                    match raw.len() {
                        4 => network::append_ipv4(raw.try_into().ok()?, &mut data),
                        16 => network::append_ipv6(raw.try_into().ok()?, &mut data),
                        _ => return None,
                    }
                }
                NetworkFunction::InetNtoa => unreachable!(),
            }
        }
        offsets.push(u32::try_from(data.len()).ok()?);
    }
    let nulls = validity(column);
    Some(if matches!(function, NetworkFunction::InetAton) {
        ColumnVector::U64 {
            data: numeric.into_iter().map(|n| n as u64).collect(),
            nulls,
        }
    } else if function.is_predicate() {
        ColumnVector::I64 {
            data: numeric,
            nulls,
        }
    } else if matches!(
        function,
        NetworkFunction::UuidToBin | NetworkFunction::Inet6Aton
    ) {
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

#[cfg(test)]
mod tests;
