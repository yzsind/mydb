//! Borrowed string kernels, including collation-aware legacy search shapes.
//! Failed speculative evaluation publishes no diagnostics or partial results.
use super::*;

#[derive(Clone, Copy)]
pub(super) enum Text<'a> {
    Literal(&'a str),
    Parameter(usize),
}
impl<'a> Text<'a> {
    fn value(value: &'a Value) -> Option<Self> {
        match value {
            Value::String(s) => Some(Self::Literal(s)),
            Value::SmallString(s) => Some(Self::Literal(std::str::from_utf8(s).ok()?)),
            _ => None,
        }
    }
    fn node(node: &'a ExprNode) -> Option<Self> {
        let value = match node {
            ExprNode::Const(v) => Self::value(v)?,
            ExprNode::Placeholder(i) => Self::Parameter(*i),
            _ => return None,
        };
        value.with(|_| Some(()))?;
        Some(value)
    }
    fn ast(expr: &'a Expr) -> Option<Self> {
        let value = match expr {
            Expr::Literal(Literal::String(s)) => Self::Literal(s),
            Expr::BoundValue(v) => Self::value(v)?,
            Expr::Placeholder(i) => Self::Parameter(*i),
            _ => return None,
        };
        value.with(|_| Some(()))?;
        Some(value)
    }
    fn with<T>(&self, f: impl FnOnce(&str) -> Option<T>) -> Option<T> {
        match self {
            Self::Literal(s) => f(s),
            Self::Parameter(i) => crate::engine::params::with_current_param(*i, |v| match v? {
                Value::String(s) => f(s),
                Value::SmallString(s) => f(std::str::from_utf8(s).ok()?),
                _ => None,
            }),
        }
    }
}
fn extent(value: &Value) -> Option<i64> {
    match value {
        Value::Int(_) | Value::UInt(_) => Some(crate::engine::expr::string_extent_arg(value)),
        _ => None,
    }
}
fn node_extent(node: &ExprNode) -> Option<i64> {
    match node {
        ExprNode::Const(v) => extent(v),
        ExprNode::Placeholder(i) => crate::engine::params::with_current_param(*i, |v| extent(v?)),
        _ => None,
    }
}
fn ast_extent(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(l) => extent(&literal_value(l)),
        Expr::BoundValue(v) => extent(v),
        Expr::Placeholder(i) => crate::engine::params::with_current_param(*i, |v| extent(v?)),
        Expr::Unary {
            op: UnaryOp::Neg,
            expr,
        } => ast_extent(expr)?.checked_neg(),
        _ => None,
    }
}
#[derive(Clone, Copy)]
pub(super) enum Unary<'a> {
    Repeat(usize),
    Substring(Text<'a>, i64),
    Quote,
    Unhex,
}
pub(super) enum StringRead<'a> {
    Weights {
        column: usize,
        collation: u16,
    },
    Compare {
        column: usize,
        value: Text<'a>,
        op: BinaryOp,
        collation: Option<u16>,
    },
    Column {
        column: usize,
        coercibility: Option<i64>,
        binary: bool,
    },
    Search {
        column: usize,
        needle: Text<'a>,
        start: i64,
        collation: u16,
        binary: bool,
    },
    Unary {
        input: &'a ExprNode,
        kind: Unary<'a>,
        binary: bool,
    },
    ConcatWs {
        separator: Text<'a>,
        args: &'a [ExprNode],
    },
}
fn input_domain(node: &ExprNode, schema: &BatchSchema) -> Option<Domain> {
    if let ExprNode::Column(i) = node {
        let field = schema.fields().get(*i)?;
        if field.source.flags
            & (crate::mysql::protocol::ENUM_FLAG | crate::mysql::protocol::SET_FLAG)
            != 0
        {
            return None;
        }
        if field.source.flags & crate::mysql::protocol::BINARY_FLAG != 0
            && field.source.collation_id != Some(crate::charset::BINARY_COLLATION_ID)
        {
            return None;
        }
    }
    let domain = super::contract(node, schema)?.domain;
    matches!(domain, Domain::Text | Domain::Binary | Domain::Null).then_some(domain)
}
impl<'a> StringRead<'a> {
    pub(super) fn bind(node: &'a ExprNode, schema: &BatchSchema) -> Option<Self> {
        if let ExprNode::Binary {
            left,
            op,
            right,
            semantics,
        } = node
        {
            if comparison(*op) && !semantics.comparison.needs_runtime_context() {
                let wrapped = |node: &'a ExprNode| match Self::bind(node, schema)? {
                    Self::Column {
                        column,
                        coercibility: None,
                        binary: false,
                    } => Some(column),
                    _ => None,
                };
                if let Some(column) = wrapped(left) {
                    return Some(Self::Compare {
                        column,
                        value: Text::node(right)?,
                        op: *op,
                        collation: semantics.collation_id,
                    });
                }
                if let Some(column) = wrapped(right) {
                    let op = match op {
                        BinaryOp::Lt => BinaryOp::Gt,
                        BinaryOp::LtEq => BinaryOp::GtEq,
                        BinaryOp::Gt => BinaryOp::Lt,
                        BinaryOp::GtEq => BinaryOp::LtEq,
                        op => *op,
                    };
                    return Some(Self::Compare {
                        column,
                        value: Text::node(left)?,
                        op,
                        collation: semantics.collation_id,
                    });
                }
            }
        }
        if let ExprNode::LegacyEscape {
            expr,
            schema: legacy,
            columns,
            ..
        } = node
        {
            let Expr::Function {
                name,
                args,
                distinct: false,
            } = expr.as_ref()
            else {
                return None;
            };
            if name.eq_ignore_ascii_case("WEIGHT_STRING") && args.len() == 1 {
                let Expr::Identifier(identifier) = &args[0] else {
                    return None;
                };
                let column = legacy.column_index(identifier).ok()?;
                let field = schema.fields().get(column)?;
                if columns.as_slice() != [column]
                    || !matches!(field.column_type, ColumnType::VarString | ColumnType::Blob)
                    || field.source.flags
                        & (crate::mysql::protocol::ENUM_FLAG | crate::mysql::protocol::SET_FLAG)
                        != 0
                {
                    return None;
                }
                let info = crate::engine::expr::resolve_row_expr_collation(&args[0], legacy)?;
                if info.name != "utf8mb4_0900_ai_ci" {
                    return None;
                }
                return Some(Self::Weights {
                    column,
                    collation: info.id,
                });
            }
            if (name.eq_ignore_ascii_case("COERCIBILITY") && args.len() == 1)
                || ((name.eq_ignore_ascii_case("COLLATE")
                    || name.eq_ignore_ascii_case("CONVERT_USING"))
                    && args.len() == 2)
            {
                let (source, coercibility) = if name.eq_ignore_ascii_case("COERCIBILITY") {
                    match &args[0] {
                        Expr::Function {
                            name,
                            args: bound,
                            distinct: false,
                        } if name.eq_ignore_ascii_case("__COLLATED_CONST") && bound.len() == 3 => {
                            let level = match &bound[2] {
                                Expr::Literal(Literal::Int(n)) => *n,
                                Expr::Literal(Literal::UInt(n)) => *n as i64,
                                _ => 2,
                            };
                            (&bound[0], Some(level))
                        }
                        source => (source, Some(2)),
                    }
                } else {
                    (&args[0], None)
                };
                let Expr::Identifier(identifier) = source else {
                    return None;
                };
                let column = legacy.column_index(identifier).ok()?;
                if columns.as_slice() != [column] {
                    return None;
                }
                let field = schema.fields().get(column)?;
                if !matches!(field.column_type, ColumnType::VarString | ColumnType::Blob)
                    || field.source.flags
                        & (crate::mysql::protocol::ENUM_FLAG | crate::mysql::protocol::SET_FLAG)
                        != 0
                    || coercibility.is_none()
                        && crate::engine::expr::resolve_row_expr_collation(source, legacy)?.charset
                            == "binary"
                {
                    return None;
                }
                let binary = if coercibility.is_some() {
                    false
                } else if name.eq_ignore_ascii_case("COLLATE") {
                    Text::ast(&args[1])?.with(|name| {
                        Some(crate::charset::resolve_collation(name).ok()?.charset == "binary")
                    })?
                } else {
                    Text::ast(&args[1])?.with(|name| {
                        let charset = crate::charset::resolve_charset(name).ok()?.name;
                        matches!(charset, "binary" | "utf8mb4").then_some(charset == "binary")
                    })?
                };
                return Some(Self::Column {
                    column,
                    coercibility,
                    binary,
                });
            }
            let instr = name.eq_ignore_ascii_case("INSTR");
            let locate = name.eq_ignore_ascii_case("LOCATE");
            if !(locate && (2..=3).contains(&args.len())
                || (instr || name.eq_ignore_ascii_case("POSITION")) && args.len() == 2)
            {
                return None;
            }
            let (h, n) = if instr {
                (&args[0], &args[1])
            } else {
                (&args[1], &args[0])
            };
            let Expr::Identifier(identifier) = h else {
                return None;
            };
            let column = legacy.column_index(identifier).ok()?;
            if columns.as_slice() != [column] {
                return None;
            }
            let field = schema.fields().get(column)?;
            if !matches!(field.column_type, ColumnType::VarString | ColumnType::Blob)
                || field.source.flags
                    & (crate::mysql::protocol::ENUM_FLAG | crate::mysql::protocol::SET_FLAG)
                    != 0
            {
                return None;
            }
            let info = crate::engine::expr::resolve_row_expr_collation(h, legacy)?;
            return Some(Self::Search {
                column,
                needle: Text::ast(n)?,
                start: if args.len() == 3 {
                    ast_extent(&args[2])?
                } else {
                    1
                },
                collation: info.id,
                binary: info.charset == "binary",
            });
        }
        let ExprNode::Function { kernel, args, .. } = node else {
            return None;
        };
        if let FunctionKernel::LegacyDeterministic(name @ ("COLLATE" | "CONVERT_USING")) = kernel {
            if let [ExprNode::Column(column), target] = args.as_slice() {
                let field = schema.fields().get(*column)?;
                if !matches!(field.column_type, ColumnType::VarString | ColumnType::Blob)
                    || field.source.flags
                        & (crate::mysql::protocol::ENUM_FLAG | crate::mysql::protocol::SET_FLAG)
                        != 0
                    || !matches!(input_domain(&args[0], schema)?, Domain::Text)
                {
                    return None;
                }
                let target = Text::node(target)?;
                let binary = if *name == "COLLATE" {
                    target.with(|name| {
                        Some(crate::charset::resolve_collation(name).ok()?.charset == "binary")
                    })?
                } else {
                    target.with(|name| {
                        let charset = crate::charset::resolve_charset(name).ok()?.name;
                        matches!(charset, "binary" | "utf8mb4").then_some(charset == "binary")
                    })?
                };
                return Some(Self::Column {
                    column: *column,
                    coercibility: None,
                    binary,
                });
            }
        }
        if matches!(kernel, FunctionKernel::ConcatWs) && args.len() >= 2 {
            let separator = Text::node(&args[0])?;
            for arg in &args[1..] {
                if let ExprNode::Column(i) = arg {
                    if schema.fields().get(*i)?.source.flags
                        & (crate::mysql::protocol::ENUM_FLAG | crate::mysql::protocol::SET_FLAG)
                        != 0
                    {
                        return None;
                    }
                }
                if !matches!(
                    super::contract(arg, schema)?.domain,
                    Domain::Text | Domain::Integer(..) | Domain::Unsigned | Domain::Null
                ) {
                    return None;
                }
            }
            return Some(Self::ConcatWs {
                separator,
                args: &args[1..],
            });
        }
        let kind = match kernel {
            FunctionKernel::Repeat if args.len() == 2 => {
                Unary::Repeat(usize::try_from(node_extent(&args[1])?.max(0)).ok()?)
            }
            FunctionKernel::SubstringIndex if args.len() == 3 => {
                Unary::Substring(Text::node(&args[1])?, node_extent(&args[2])?)
            }
            FunctionKernel::LegacyDeterministic("QUOTE") if args.len() == 1 => Unary::Quote,
            FunctionKernel::LegacyDeterministic("UNHEX") if args.len() == 1 => Unary::Unhex,
            _ => return None,
        };
        let input = args.first()?;
        let domain = input_domain(input, schema)?;
        let binary = matches!(domain, Domain::Binary);
        if matches!(kind, Unary::Quote) && binary {
            return None;
        }
        Some(Self::Unary {
            input,
            kind,
            binary,
        })
    }
    pub(super) fn contract(&self) -> Contract {
        Contract {
            domain: match self {
                Self::Weights { .. } => Domain::Binary,
                Self::Compare { .. } => Domain::Integer(0, 1),
                Self::Column {
                    coercibility: Some(level),
                    ..
                } => Domain::Integer(*level, *level),
                Self::Column { binary: true, .. } => Domain::Binary,
                Self::Search { .. } => Domain::Integer(0, u32::MAX as i64),
                Self::ConcatWs { .. } => Domain::Text,
                Self::Unary {
                    kind: Unary::Unhex, ..
                } => Domain::Binary,
                Self::Unary {
                    kind: Unary::Quote, ..
                } => Domain::Text,
                Self::Unary { binary: true, .. } => Domain::Binary,
                _ => Domain::Text,
            },
            parallel: matches!(
                self,
                Self::Search { .. }
                    | Self::Column { .. }
                    | Self::Compare { .. }
                    | Self::Weights { .. }
            ),
        }
    }
    pub(super) fn workspace(&self, batch: &Batch) -> Option<(usize, usize)> {
        let rows = batch.num_rows();
        let offsets = rows.checked_add(1)?.checked_mul(8)?;
        match self {
            Self::Weights { column, .. } => {
                // Input buffers can retain far more capacity than their
                // visible payload. Bound newly produced bytes from data.len(),
                // so harmless storage-builder headroom cannot force a scalar fallback.
                let bytes = match batch.columns().get(*column)? {
                    ColumnVector::Utf8 { data, .. } => {
                        crate::charset::uca9_primary_byte_bound(data)?
                    }
                    ColumnVector::Null { .. } => 0,
                    _ => return None,
                }
                .checked_add(offsets)?;
                Some((bytes, bytes.checked_mul(2)?.checked_add(65536)?))
            }
            Self::Compare { .. } => {
                let bytes = rows.checked_mul(16)?;
                Some((bytes, bytes.checked_mul(2)?.checked_add(512)?))
            }
            Self::Column {
                column,
                coercibility,
                ..
            } => {
                let bytes = if coercibility.is_some() {
                    rows.checked_mul(16)?
                } else {
                    batch.columns().get(*column)?.memory_bytes()
                };
                Some((bytes, bytes.checked_mul(2)?.checked_add(512)?))
            }
            Self::Search {
                column,
                needle,
                binary,
                ..
            } => {
                let column = batch.columns().get(*column)?;
                let largest = match column {
                    ColumnVector::Utf8 { offsets, .. } | ColumnVector::Bytes { offsets, .. } => {
                        offsets
                            .windows(2)
                            .map(|w| (w[1] - w[0]) as usize)
                            .max()
                            .unwrap_or(0)
                    }
                    ColumnVector::Null { .. } => 0,
                    _ => return None,
                };
                let scratch = if *binary {
                    512
                } else {
                    largest
                        .checked_add(needle.with(|s| Some(s.len()))?)?
                        .checked_mul(512)?
                        .checked_add(65536)?
                };
                let payload = rows.checked_mul(16)?;
                Some((payload, payload.checked_mul(2)?.checked_add(scratch)?))
            }
            Self::Unary { input, kind, .. } => {
                let (source, child) = super::workspace(input, batch)?;
                let payload = match kind {
                    Unary::Repeat(n) => source.checked_mul(*n)?.checked_add(offsets)?,
                    Unary::Quote => source
                        .checked_mul(2)?
                        .checked_add(rows.checked_mul(4)?)?
                        .checked_add(offsets)?,
                    _ => source.checked_add(offsets)?,
                };
                Some((
                    payload,
                    child
                        .checked_add(payload.checked_mul(2)?)?
                        .checked_add(512)?,
                ))
            }
            Self::ConcatWs { separator, args } => {
                let mut payload = offsets;
                let mut work = 0usize;
                for arg in *args {
                    let (bytes, child) = super::workspace(arg, batch)?;
                    let bytes = if matches!(
                        super::contract(arg, batch.schema())?.domain,
                        Domain::Integer(..) | Domain::Unsigned
                    ) {
                        rows.checked_mul(20)?.checked_add(offsets)?
                    } else {
                        bytes
                    };
                    payload = payload.checked_add(bytes)?;
                    work = work.checked_add(child)?.checked_add(bytes)?;
                }
                payload = payload.checked_add(
                    separator
                        .with(|s| s.len().checked_mul(args.len().saturating_sub(1)))?
                        .checked_mul(rows)?,
                )?;
                Some((
                    payload,
                    work.checked_add(payload.checked_mul(2)?)?
                        .checked_add(args.len().checked_mul(64)?)?
                        .checked_add(512)?,
                ))
            }
        }
    }
    pub(super) fn evaluate(&self, batch: &Batch) -> Option<ColumnVector> {
        match self {
            Self::Weights { column, collation } => {
                let input = batch.columns().get(*column)?;
                if !matches!(input, ColumnVector::Utf8 { .. } | ColumnVector::Null { .. }) {
                    return None;
                }
                debug_assert_eq!(*collation, crate::charset::UTF8MB4_0900_AI_CI_ID);
                let mut weights =
                    crate::engine::expr::string_search::SearchBuffer::for_primary_weights();
                let mut data = Vec::new();
                let mut offsets = Vec::with_capacity(batch.num_rows() + 1);
                offsets.push(0);
                for row in 0..batch.num_rows() {
                    if row & 63 == 0 {
                        crate::engine::cancel::check_cancelled().ok()?;
                    }
                    if !input.is_null(row) {
                        let text = std::str::from_utf8(bytes_at(input, row)?).ok()?;
                        data.extend_from_slice(weights.primary_weights(text));
                    }
                    offsets.push(u32::try_from(data.len()).ok()?);
                }
                Some(ColumnVector::Bytes {
                    data,
                    offsets,
                    nulls: validity(input),
                })
            }
            Self::Compare {
                column,
                value,
                op,
                collation,
            } => value.with(|rhs| {
                let input = batch.columns().get(*column)?;
                if !matches!(input, ColumnVector::Utf8 { .. } | ColumnVector::Null { .. }) {
                    return None;
                }
                crate::engine::expr::with_compare_collation(*collation, || {
                    let mut data = Vec::with_capacity(batch.num_rows());
                    for row in 0..batch.num_rows() {
                        if row & 63 == 0 {
                            crate::engine::cancel::check_cancelled().ok()?;
                        }
                        if input.is_null(row) {
                            data.push(0);
                            continue;
                        }
                        let lhs = std::str::from_utf8(bytes_at(input, row)?).ok()?;
                        let ordering = crate::engine::expr::compare_borrowed_strings(lhs, rhs)?;
                        use std::cmp::Ordering::{Equal, Greater, Less};
                        let matches = match op {
                            BinaryOp::Eq => ordering == Equal,
                            BinaryOp::NotEq => ordering != Equal,
                            BinaryOp::Lt => ordering == Less,
                            BinaryOp::LtEq => ordering != Greater,
                            BinaryOp::Gt => ordering == Greater,
                            BinaryOp::GtEq => ordering != Less,
                            _ => return None,
                        };
                        data.push(i64::from(matches));
                    }
                    Some(ColumnVector::I64 {
                        data,
                        nulls: validity(input),
                    })
                })
            }),
            Self::Column {
                column,
                coercibility,
                binary,
            } => {
                let input = batch.columns().get(*column)?;
                if !matches!(input, ColumnVector::Utf8 { .. } | ColumnVector::Null { .. }) {
                    return None;
                }
                if coercibility.is_some() {
                    return Some(ColumnVector::I64 {
                        data: vec![(*coercibility)?; batch.num_rows()],
                        nulls: None,
                    });
                }
                match input {
                    ColumnVector::Utf8 {
                        data,
                        offsets,
                        nulls,
                    } if *binary => Some(ColumnVector::Bytes {
                        data: data.clone(),
                        offsets: offsets.clone(),
                        nulls: nulls.clone(),
                    }),
                    _ => Some(input.clone()),
                }
            }
            Self::Search {
                column,
                needle,
                start,
                collation,
                binary,
            } => needle.with(|needle| {
                let input = batch.columns().get(*column)?;
                if !matches!(
                    (*binary, input),
                    (true, ColumnVector::Bytes { .. })
                        | (false, ColumnVector::Utf8 { .. })
                        | (_, ColumnVector::Null { .. })
                ) {
                    return None;
                }
                let mut search = (!*binary).then(|| {
                    crate::engine::expr::string_search::SearchBuffer::new(*collation, needle)
                });
                let mut data = Vec::with_capacity(batch.num_rows());
                for row in 0..batch.num_rows() {
                    if row & 63 == 0 {
                        crate::engine::cancel::check_cancelled().ok()?;
                    }
                    if input.is_null(row) {
                        data.push(0);
                        continue;
                    }
                    let bytes = bytes_at(input, row)?;
                    let found = if *binary {
                        crate::engine::expr::find_position_bytes(bytes, needle.as_bytes(), *start)
                    } else {
                        search
                            .as_mut()?
                            .find(std::str::from_utf8(bytes).ok()?, needle, *start)
                    };
                    data.push(i64::try_from(found).ok()?);
                }
                Some(ColumnVector::I64 {
                    data,
                    nulls: validity(input),
                })
            }),
            Self::ConcatWs { separator, args } => separator.with(|separator| {
                let columns = args
                    .iter()
                    .map(|arg| operand(arg, batch))
                    .collect::<Option<Vec<_>>>()?;
                let mut data = Vec::new();
                let mut offsets = Vec::with_capacity(batch.num_rows() + 1);
                offsets.push(0);
                let packet = crate::engine::sysvar::builtin::connection::max_allowed_packet_bytes();
                for row in 0..batch.num_rows() {
                    if row & 63 == 0 {
                        crate::engine::cancel::check_cancelled().ok()?;
                    }
                    let begin = data.len();
                    let mut first = true;
                    for col in &columns {
                        if col.is_null(row) {
                            continue;
                        }
                        let mut buffer = itoa::Buffer::new();
                        let part = match col.as_ref() {
                            ColumnVector::I64 { data, .. } => buffer.format(data[row]).as_bytes(),
                            ColumnVector::U64 { data, .. } => buffer.format(data[row]).as_bytes(),
                            ColumnVector::Utf8 { .. } => bytes_at(col, row)?,
                            _ => return None,
                        };
                        let length = (data.len() - begin)
                            .checked_add(part.len())?
                            .checked_add(if first { 0 } else { separator.len() })?;
                        if length as u64 > packet {
                            return None;
                        }
                        if !first {
                            data.extend_from_slice(separator.as_bytes());
                        }
                        data.extend_from_slice(part);
                        first = false;
                    }
                    offsets.push(u32::try_from(data.len()).ok()?);
                }
                Some(ColumnVector::Utf8 {
                    data,
                    offsets,
                    nulls: None,
                })
            }),
            Self::Unary {
                input,
                kind,
                binary,
            } => {
                let input = operand(input, batch)?;
                if !matches!(
                    (*binary, input.as_ref()),
                    (true, ColumnVector::Bytes { .. })
                        | (false, ColumnVector::Utf8 { .. })
                        | (_, ColumnVector::Null { .. })
                ) {
                    return None;
                }
                let evaluate = |delimiter: &[u8]| {
                    let mut data = Vec::new();
                    let mut offsets = Vec::with_capacity(batch.num_rows() + 1);
                    offsets.push(0);
                    let packet =
                        crate::engine::sysvar::builtin::connection::max_allowed_packet_bytes();
                    for row in 0..batch.num_rows() {
                        if row & 63 == 0 {
                            crate::engine::cancel::check_cancelled().ok()?;
                        }
                        if input.is_null(row) {
                            if matches!(kind, Unary::Quote) {
                                data.extend_from_slice(b"NULL");
                            }
                            offsets.push(u32::try_from(data.len()).ok()?);
                            continue;
                        }
                        let bytes = bytes_at(&input, row)?;
                        match kind {
                            Unary::Repeat(n) => {
                                let length = bytes.len().checked_mul(*n)?;
                                if length as u64 > packet {
                                    return None;
                                }
                                if length != 0 {
                                    let begin = data.len();
                                    data.extend_from_slice(bytes);
                                    let mut written = bytes.len();
                                    while written < length {
                                        let copy = written.min(length - written);
                                        data.extend_from_within(begin..begin + copy);
                                        written += copy;
                                    }
                                }
                            }
                            Unary::Substring(_, count) => data.extend_from_slice(
                                &bytes[crate::engine::expr::substring_index_range(
                                    bytes, delimiter, *count,
                                )],
                            ),
                            Unary::Quote => {
                                data.push(b'\'');
                                let mut begin = 0;
                                for (i, &b) in bytes.iter().enumerate() {
                                    let escape = match b {
                                        b'\'' => b'\'',
                                        b'\\' => b'\\',
                                        0 => b'0',
                                        26 => b'Z',
                                        _ => continue,
                                    };
                                    data.extend_from_slice(&bytes[begin..i]);
                                    data.push(b'\\');
                                    data.push(escape);
                                    begin = i + 1;
                                }
                                data.extend_from_slice(&bytes[begin..]);
                                data.push(b'\'');
                            }
                            Unary::Unhex => {
                                fn nibble(b: u8) -> Option<u8> {
                                    match b {
                                        b'0'..=b'9' => Some(b - b'0'),
                                        b'a'..=b'f' => Some(b - b'a' + 10),
                                        b'A'..=b'F' => Some(b - b'A' + 10),
                                        _ => None,
                                    }
                                }
                                let mut i = 0;
                                if bytes.len() % 2 == 1 {
                                    data.push(nibble(bytes[0])?);
                                    i = 1;
                                }
                                while i < bytes.len() {
                                    data.push((nibble(bytes[i])? << 4) | nibble(bytes[i + 1])?);
                                    i += 2;
                                }
                            }
                        }
                        offsets.push(u32::try_from(data.len()).ok()?);
                    }
                    let nulls = if matches!(kind, Unary::Quote) {
                        None
                    } else {
                        validity(&input)
                    };
                    Some(if *binary || matches!(kind, Unary::Unhex) {
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
                };
                match kind {
                    Unary::Substring(delimiter, _) => delimiter.with(|d| evaluate(d.as_bytes())),
                    _ => evaluate(&[]),
                }
            }
        }
    }
}
