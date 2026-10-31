//! Typed JSON reads over borrowed binary nodes. Text documents and paths with
//! multiple results retain the ordered scalar parser and autowrap semantics.
use super::*;
use crate::engine::func_cache::{cached_json_path, Governed};
use crate::engine::json::{JsonPath, JsonbRef};
use smallvec::SmallVec;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Output {
    Binary,
    Unquoted,
    Type,
    Length,
    Valid,
    Keys,
    Contains,
    Overlaps,
    StorageSize,
    StorageFree,
    Pretty,
    SearchOne,
    ValueSigned,
    ValueUnsigned,
}

pub(super) struct JsonRead {
    column: usize,
    paths: SmallVec<[Arc<Governed<JsonPath>>; 2]>,
    output: Output,
    probes: SmallVec<[Arc<Governed<JsonPath>>; 2]>,
    all: bool,
    rhs: Option<Vec<u8>>,
    rhs_charge: Option<crate::engine::executor::mem_budget::ScopedCharge>,
    pattern: Option<String>,
    escape: Option<char>,
}

impl JsonRead {
    pub(super) fn bind(node: &ExprNode, schema: &BatchSchema) -> Option<Self> {
        match node {
            ExprNode::Column(column)
                if schema.fields().get(*column)?.column_type == ColumnType::Json =>
            {
                Some(Self {
                    column: *column,
                    paths: SmallVec::new(),
                    output: Output::Binary,
                    probes: SmallVec::new(),
                    all: false,
                    rhs: None,
                    rhs_charge: None,
                    pattern: None,
                    escape: Some('\\'),
                })
            }
            ExprNode::Function { kernel, args, .. } => {
                if matches!(
                    kernel,
                    FunctionKernel::LegacyDeterministic("JSON_CONTAINS_PATH")
                ) && args.len() >= 3
                {
                    let mut input = Self::bind(&args[0], schema)?;
                    if input.output != Output::Binary {
                        return None;
                    }
                    let mode = text(&args[1])?;
                    input.all = if mode.eq_ignore_ascii_case("all") {
                        true
                    } else if mode.eq_ignore_ascii_case("one") {
                        false
                    } else {
                        return None;
                    };
                    for arg in &args[2..] {
                        let path = cached_json_path(&text(arg)?).ok()?;
                        if path.has_wildcard {
                            return None;
                        }
                        input.probes.push(path);
                    }
                    input.output = Output::Contains;
                    return Some(input);
                }
                if matches!(kernel, FunctionKernel::LegacyDeterministic("JSON_OVERLAPS"))
                    && args.len() == 2
                {
                    let mut input = Self::bind(&args[0], schema)?;
                    if input.output != Output::Binary {
                        return None;
                    }
                    let rhs = text(&args[1])?;
                    let bytes = rhs.len().checked_mul(64)?.checked_add(1024)?;
                    let mut charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
                    if !charge.add_within(
                        bytes,
                        crate::engine::executor::vec::evaluated_batch::byte_target(),
                    ) {
                        return None;
                    }
                    input.rhs =
                        Some(crate::engine::json::parse_text(&rhs, 2, "JSON_OVERLAPS").ok()?);
                    input.rhs_charge = Some(charge);
                    input.output = Output::Overlaps;
                    return Some(input);
                }
                if matches!(kernel, FunctionKernel::LegacyDeterministic("JSON_SEARCH"))
                    && (3..=4).contains(&args.len())
                {
                    if !text(&args[1])?.eq_ignore_ascii_case("one") {
                        return None;
                    }
                    let pattern = text(&args[2])?;
                    if pattern.len()
                        > crate::engine::executor::vec::evaluated_batch::byte_target() / 16
                    {
                        return None;
                    }
                    let escape = match args.get(3) {
                        None => Some('\\'),
                        Some(arg) if matches!(constant(arg)?.as_ref(), Value::Null) => Some('\\'),
                        Some(arg) => {
                            let text = text(arg)?;
                            let mut chars = text.chars();
                            let first = chars.next();
                            if chars.next().is_some() {
                                return None;
                            }
                            first
                        }
                    };
                    let mut input = Self::bind(&args[0], schema)?;
                    if input.output != Output::Binary {
                        return None;
                    }
                    let mut charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
                    if !charge.add_within(
                        pattern.len(),
                        crate::engine::executor::vec::evaluated_batch::byte_target(),
                    ) {
                        return None;
                    }
                    input.rhs_charge = Some(charge);
                    input.output = Output::SearchOne;
                    input.pattern = Some(pattern.into_owned());
                    input.escape = escape;
                    return Some(input);
                }
                if matches!(kernel, FunctionKernel::LegacyDeterministic("JSON_VALUE"))
                    && args.len() == 7
                {
                    let returning = text(&args[2])?;
                    if !text(&args[3])?.eq_ignore_ascii_case("NULL")
                        || !text(&args[5])?.eq_ignore_ascii_case("NULL")
                        || !matches!(constant(&args[4])?.as_ref(), Value::Null)
                        || !matches!(constant(&args[6])?.as_ref(), Value::Null)
                    {
                        return None;
                    }
                    let output = match returning.trim().to_ascii_uppercase().as_str() {
                        "UNSIGNED" | "UNSIGNED INTEGER" => Output::ValueUnsigned,
                        "SIGNED" | "SIGNED INTEGER" | "INTEGER" => Output::ValueSigned,
                        _ => return None,
                    };
                    let mut input = Self::bind(&args[0], schema)?;
                    if input.output != Output::Binary {
                        return None;
                    }
                    input.push_path(&args[1])?;
                    input.output = output;
                    return Some(input);
                }
                let output = match kernel {
                    FunctionKernel::JsonExtract if args.len() == 2 => Output::Binary,
                    FunctionKernel::LegacyDeterministic("JSON_UNQUOTE") if args.len() == 1 => {
                        Output::Unquoted
                    }
                    FunctionKernel::LegacyDeterministic("JSON_PRETTY") if args.len() == 1 => {
                        Output::Pretty
                    }
                    FunctionKernel::LegacyDeterministic("JSON_TYPE") if args.len() == 1 => {
                        Output::Type
                    }
                    FunctionKernel::LegacyDeterministic("JSON_LENGTH")
                        if (1..=2).contains(&args.len()) =>
                    {
                        Output::Length
                    }
                    FunctionKernel::LegacyDeterministic("JSON_VALID") if args.len() == 1 => {
                        Output::Valid
                    }
                    FunctionKernel::LegacyDeterministic("JSON_KEYS")
                        if (1..=2).contains(&args.len()) =>
                    {
                        Output::Keys
                    }
                    FunctionKernel::LegacyDeterministic("JSON_STORAGE_SIZE") if args.len() == 1 => {
                        Output::StorageSize
                    }
                    FunctionKernel::LegacyDeterministic("JSON_STORAGE_FREE") if args.len() == 1 => {
                        Output::StorageFree
                    }
                    _ => return None,
                };
                let mut input = Self::bind(&args[0], schema)?;
                if input.output != Output::Binary {
                    return None;
                }
                if let Some(path) = args.get(1) {
                    input.push_path(path)?;
                }
                input.output = output;
                Some(input)
            }
            ExprNode::Binary {
                left,
                op: op @ (BinaryOp::Arrow | BinaryOp::LongArrow),
                right,
                ..
            } => {
                let mut input = Self::bind(left, schema)?;
                if input.output != Output::Binary {
                    return None;
                }
                input.push_path(right)?;
                input.output = if matches!(op, BinaryOp::LongArrow) {
                    Output::Unquoted
                } else {
                    Output::Binary
                };
                Some(input)
            }
            _ => None,
        }
    }

    fn push_path(&mut self, node: &ExprNode) -> Option<()> {
        let path = cached_json_path(&text(node)?).ok()?;
        if path.has_wildcard {
            return None;
        }
        self.paths.push(path);
        Some(())
    }

    pub(super) fn contract(&self) -> Contract {
        let domain = match self.output {
            Output::Binary | Output::Keys | Output::SearchOne => Domain::Json,
            Output::Type | Output::Unquoted | Output::Pretty => Domain::Text,
            Output::Length => Domain::Integer(0, u32::MAX as i64),
            Output::Valid | Output::Contains | Output::Overlaps => Domain::Integer(0, 1),
            Output::StorageFree => Domain::Integer(0, 0),
            Output::StorageSize => Domain::Integer(0, i64::MAX),
            Output::ValueSigned => Domain::Integer(i64::MIN, i64::MAX),
            Output::ValueUnsigned => Domain::Unsigned,
        };
        Contract {
            domain,
            parallel: !matches!(
                self.output,
                Output::StorageSize | Output::ValueSigned | Output::ValueUnsigned
            ),
        }
    }

    fn select<'a>(&self, column: &'a ColumnVector, row: usize) -> Option<Option<JsonbRef<'a>>> {
        if column.is_null(row) {
            return Some(None);
        }
        let ColumnVector::Json { data, offsets, .. } = column else {
            return None;
        };
        let raw = data.get(offsets[row] as usize..offsets[row + 1] as usize)?;
        let mut value = JsonbRef::new(raw).ok()?;
        for path in &self.paths {
            let Some(next) = path.eval_one(value) else {
                return Some(None);
            };
            value = next;
        }
        Some(Some(value))
    }

    pub(super) fn workspace(&self, batch: &Batch) -> Option<(usize, usize)> {
        let rows = batch.num_rows();
        let offsets = rows.checked_add(1)?.checked_mul(8)?;
        let mut bytes = match self.output {
            Output::Length
            | Output::Valid
            | Output::Contains
            | Output::Overlaps
            | Output::StorageFree
            | Output::StorageSize
            | Output::ValueSigned
            | Output::ValueUnsigned => rows.checked_mul(16)?,
            Output::Type => rows.checked_mul(32)?.checked_add(offsets)?,
            _ => offsets,
        };
        if matches!(self.output, Output::Binary | Output::Keys) {
            let column = batch.columns().get(self.column)?;
            bytes = match column {
                ColumnVector::Json { data, .. } => data.len().checked_add(offsets)?,
                ColumnVector::Null { .. } => offsets,
                _ => return None,
            };
        } else if self.output == Output::Unquoted {
            let column = batch.columns().get(self.column)?;
            for row in 0..rows {
                if row & 63 == 0 {
                    crate::engine::cancel::check_cancelled().ok()?;
                }
                if let Some(value) = self.select(column, row)? {
                    let size = if let Some(text) = value.as_str() {
                        text.len()
                    } else if value.as_i64().is_some() {
                        20
                    } else {
                        value.as_bytes().len().checked_mul(6)?.checked_add(64)?
                    };
                    bytes = bytes.checked_add(size)?;
                }
            }
        }
        if self.output == Output::Pretty {
            let column = batch.columns().get(self.column)?;
            let target = crate::engine::executor::vec::evaluated_batch::byte_target();
            if let ColumnVector::Json { offsets, .. } = column {
                if offsets.windows(2).any(|p| (p[1] - p[0]) as usize > target) {
                    return None;
                }
            }
            let mut visited = 0;
            for row in 0..rows {
                if let Some(value) = self.select(column, row)? {
                    bytes = bytes.checked_add(pretty_bound(value, 0, &mut visited)?)?;
                }
            }
        }
        if self.output == Output::SearchOne {
            let column = batch.columns().get(self.column)?;
            let input = match column {
                ColumnVector::Json { data, .. } => data.len(),
                ColumnVector::Null { .. } => 0,
                _ => return None,
            };
            bytes = bytes
                .checked_add(input.checked_mul(6)?)?
                .checked_add(rows.checked_mul(64)?)?;
        }
        let scratch = if matches!(
            self.output,
            Output::Overlaps | Output::StorageSize | Output::SearchOne
        ) {
            let column = batch.columns().get(self.column)?;
            let largest = match column {
                ColumnVector::Json { offsets, .. } => offsets
                    .windows(2)
                    .map(|p| (p[1] - p[0]) as usize)
                    .max()
                    .unwrap_or(0),
                ColumnVector::Null { .. } => 0,
                _ => return None,
            };
            largest
                .checked_mul(if self.output == Output::Overlaps {
                    64
                } else if self.output == Output::SearchOne {
                    32
                } else {
                    2
                })?
                .checked_add(if self.output == Output::SearchOne {
                    self.pattern
                        .as_ref()?
                        .len()
                        .checked_mul(32)?
                        .checked_add(16384)?
                } else {
                    1024
                })?
        } else {
            256
        };
        Some((bytes, bytes.checked_mul(2)?.checked_add(scratch)?))
    }

    pub(super) fn evaluate(&self, batch: &Batch) -> Option<ColumnVector> {
        let column = batch.columns().get(self.column)?;
        let rows = batch.num_rows();
        let mut nulls = Bitmap::new_all_valid(rows);
        let variable = matches!(
            self.output,
            Output::Binary
                | Output::Keys
                | Output::SearchOne
                | Output::Type
                | Output::Unquoted
                | Output::Pretty
        );
        let mut numeric =
            Vec::with_capacity(if !variable && self.output != Output::ValueUnsigned {
                rows
            } else {
                0
            });
        let mut unsigned = Vec::with_capacity(if self.output == Output::ValueUnsigned {
            rows
        } else {
            0
        });
        let mut bytes = Vec::new();
        let mut output = String::new();
        let mut offsets = if variable {
            Vec::with_capacity(rows + 1)
        } else {
            Vec::new()
        };
        if variable {
            offsets.push(0);
        }
        for row in 0..rows {
            if row & 63 == 0 {
                crate::engine::cancel::check_cancelled().ok()?;
            }
            let value = self
                .select(column, row)?
                .filter(|value| self.output != Output::Keys || value.is_object());
            let value = value.filter(|value| {
                !matches!(self.output, Output::ValueSigned | Output::ValueUnsigned)
                    || !value.is_null()
            });
            if let Some(value) = value {
                match self.output {
                    Output::Binary => bytes.extend_from_slice(value.as_bytes()),
                    Output::Unquoted => crate::engine::json::append_unquoted(value, &mut output),
                    Output::Pretty => value.write_pretty(&mut output, 0),
                    Output::SearchOne => match crate::engine::expr::json_search_unscoped_one(
                        value,
                        self.pattern.as_ref()?,
                        self.escape,
                    )
                    .ok()?
                    {
                        Some(path) => crate::engine::json::append_string_node(&path, &mut bytes),
                        None => nulls.set(row, false),
                    },
                    Output::Type => output.push_str(value.type_name()),
                    Output::Length => numeric.push(value.container_len().unwrap_or(1) as i64),
                    Output::Valid => numeric.push(1),
                    Output::Keys => crate::engine::json::append_object_keys(value, &mut bytes)?,
                    Output::Contains => numeric.push(if self.all {
                        self.probes
                            .iter()
                            .all(|path| path.eval_one(value).is_some())
                    } else {
                        self.probes
                            .iter()
                            .any(|path| path.eval_one(value).is_some())
                    } as i64),
                    Output::Overlaps => numeric.push(crate::engine::expr::json_overlaps(
                        // Probe the row with constant keys, preserving the
                        // compiled object's small-RHS lookup strategy.
                        JsonbRef::new(self.rhs.as_ref()?).ok()?,
                        value,
                    ) as i64),
                    Output::StorageSize => numeric.push(
                        crate::engine::json::mysql_binary::encoded_size(value.as_bytes()).ok()?
                            as i64,
                    ),
                    Output::StorageFree => numeric.push(0),
                    Output::ValueSigned | Output::ValueUnsigned => {
                        let v = match value.json_type() {
                            crate::engine::json::JsonType::Integer => Value::Int(value.as_i64()?),
                            crate::engine::json::JsonType::UnsignedInteger => {
                                Value::UInt(u64::try_from(value.exact_number()?.0).ok()?)
                            }
                            crate::engine::json::JsonType::Boolean => {
                                Value::Int(i64::from(value.as_bool()?))
                            }
                            _ => return None,
                        };
                        // JSON_VALUE delegates out-of-range values to its
                        // scalar ON ERROR path instead of CAST's wrap policy.
                        let converted = match (v, self.output == Output::ValueUnsigned) {
                            (Value::Int(n), true) => Value::UInt(u64::try_from(n).ok()?),
                            (Value::UInt(n), false) => Value::Int(i64::try_from(n).ok()?),
                            (value, _) => value,
                        };
                        match converted {
                            Value::UInt(n) => unsigned.push(n),
                            Value::Int(n) => numeric.push(n),
                            _ => return None,
                        }
                    }
                }
            } else {
                nulls.set(row, false);
                if matches!(
                    self.output,
                    Output::Length
                        | Output::Valid
                        | Output::Contains
                        | Output::Overlaps
                        | Output::StorageSize
                        | Output::StorageFree
                        | Output::ValueSigned
                ) {
                    numeric.push(0);
                }
                if self.output == Output::ValueUnsigned {
                    unsigned.push(0);
                }
            }
            if variable {
                offsets.push(
                    u32::try_from(
                        if matches!(
                            self.output,
                            Output::Binary | Output::Keys | Output::SearchOne
                        ) {
                            bytes.len()
                        } else {
                            output.len()
                        },
                    )
                    .ok()?,
                );
            }
        }
        let nulls = (nulls.count_ones() != rows).then_some(nulls);
        Some(match self.output {
            Output::ValueUnsigned => ColumnVector::U64 {
                data: unsigned,
                nulls,
            },
            Output::Binary | Output::Keys | Output::SearchOne => ColumnVector::Json {
                data: bytes,
                offsets,
                nulls,
            },
            Output::Unquoted | Output::Type | Output::Pretty => ColumnVector::Utf8 {
                data: output.into_bytes(),
                offsets,
                nulls,
            },
            Output::Length
            | Output::Valid
            | Output::Contains
            | Output::Overlaps
            | Output::StorageSize
            | Output::StorageFree
            | Output::ValueSigned => ColumnVector::I64 {
                data: numeric,
                nulls,
            },
        })
    }
}

/// Bound indentation and escaped scalar text without formatting a temporary
/// document. Oversized rows retain the original scalar path before this walk.
fn pretty_bound(node: JsonbRef<'_>, level: usize, visited: &mut usize) -> Option<usize> {
    *visited += 1;
    if *visited & 1023 == 0 {
        crate::engine::cancel::check_cancelled().ok()?;
    }
    match node.json_type() {
        crate::engine::json::JsonType::Array | crate::engine::json::JsonType::Object => {
            let count = node.container_len()?;
            if count == 0 {
                return Some(2);
            }
            let mut bytes = 4usize
                .checked_add(level.checked_mul(2)?)?
                .checked_add((count - 1).checked_mul(2)?)?;
            let indent = level.checked_add(1)?.checked_mul(2)?;
            if node.is_array() {
                for child in node.array_iter() {
                    bytes = bytes.checked_add(indent)?.checked_add(pretty_bound(
                        child,
                        level + 1,
                        visited,
                    )?)?;
                }
            } else {
                for (key, child) in node.object_iter() {
                    bytes = bytes
                        .checked_add(indent)?
                        .checked_add(key.len().checked_mul(6)?.checked_add(4)?)?
                        .checked_add(pretty_bound(child, level + 1, visited)?)?;
                }
            }
            Some(bytes)
        }
        crate::engine::json::JsonType::Null => Some(4),
        crate::engine::json::JsonType::Boolean => Some(5),
        crate::engine::json::JsonType::Integer | crate::engine::json::JsonType::UnsignedInteger => {
            Some(20)
        }
        crate::engine::json::JsonType::Double => Some(32),
        crate::engine::json::JsonType::String => {
            node.as_str()?.len().checked_mul(6)?.checked_add(2)
        }
        _ => node.as_bytes().len().checked_mul(6)?.checked_add(64),
    }
}

/// JSON_QUOTE accepts character values; binary, ENUM/SET metadata and other
/// source domains retain their schema-aware scalar conversion/error path.
pub(super) fn quote_column(node: &ExprNode, schema: &BatchSchema) -> Option<usize> {
    let ExprNode::Function {
        kernel: FunctionKernel::LegacyDeterministic("JSON_QUOTE"),
        args,
        ..
    } = node
    else {
        return None;
    };
    let [ExprNode::Column(column)] = args.as_slice() else {
        return None;
    };
    let field = schema.fields().get(*column)?;
    if field.source.flags & (crate::mysql::protocol::ENUM_FLAG | crate::mysql::protocol::SET_FLAG)
        != 0
    {
        return None;
    }
    if !matches!(super::contract(&args[0], schema)?.domain, Domain::Text) {
        return None;
    }
    Some(*column)
}

pub(super) fn evaluate_quote(column: &ColumnVector) -> Option<ColumnVector> {
    if !matches!(
        column,
        ColumnVector::Utf8 { .. } | ColumnVector::Null { .. }
    ) {
        return None;
    }

    let mut data = Vec::new();
    let mut offsets = Vec::with_capacity(column.len() + 1);
    offsets.push(0);
    for row in 0..column.len() {
        if row & 63 == 0 {
            crate::engine::cancel::check_cancelled().ok()?;
        }
        if !column.is_null(row) {
            let text = std::str::from_utf8(super::bytes_at(column, row)?).ok()?;
            crate::engine::json::append_string_node(text, &mut data);
        }
        offsets.push(u32::try_from(data.len()).ok()?);
    }
    Some(ColumnVector::Json {
        data,
        offsets,
        nulls: super::validity(column),
    })
}
