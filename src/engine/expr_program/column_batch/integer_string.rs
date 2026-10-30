//! Integer-driven text/byte producers with shared output buffers.
use super::*;
use string_read::Text;

enum Kind<'a> {
    Char,
    Space,
    Export {
        on: Text<'a>,
        off: Text<'a>,
        separator: Option<Text<'a>>,
        count: usize,
    },
    Make(Vec<Option<Text<'a>>>),
    Elt(Vec<Option<Text<'a>>>),
}
pub(super) struct IntegerString<'a> {
    args: &'a [ExprNode],
    kind: Kind<'a>,
}
impl<'a> IntegerString<'a> {
    pub(super) fn bind(node: &'a ExprNode, schema: &BatchSchema) -> Option<Self> {
        let ExprNode::Function { kernel, args, .. } = node else {
            return None;
        };
        let numeric = |arg| {
            let contract = super::contract(arg, schema)?;
            (contract.parallel
                && matches!(
                    contract.domain,
                    Domain::Integer(..) | Domain::Unsigned | Domain::Null
                ))
            .then_some(())
        };
        let kind = match kernel {
            FunctionKernel::Char if !args.is_empty() => {
                for arg in args {
                    numeric(arg)?;
                }
                Kind::Char
            }
            FunctionKernel::Space if args.len() == 1 => {
                numeric(&args[0])?;
                Kind::Space
            }
            FunctionKernel::LegacyDeterministic("EXPORT_SET") if (3..=5).contains(&args.len()) => {
                numeric(&args[0])?;
                let count = match args.get(4) {
                    None => 64,
                    Some(arg) => match constant(arg)?.as_ref() {
                        Value::Int(n) => (*n as u64).min(64) as usize,
                        Value::UInt(n) => (*n).min(64) as usize,
                        _ => return None,
                    },
                };
                Kind::Export {
                    on: Text::node(&args[1])?,
                    off: Text::node(&args[2])?,
                    separator: match args.get(3) {
                        Some(arg) => Some(Text::node(arg)?),
                        None => None,
                    },
                    count,
                }
            }
            FunctionKernel::MakeSet | FunctionKernel::LegacyDeterministic("ELT")
                if args.len() >= 2 =>
            {
                numeric(&args[0])?;
                let mut items = Vec::new();
                for arg in args
                    .iter()
                    .skip(1)
                    .take(if matches!(kernel, FunctionKernel::MakeSet) {
                        64
                    } else {
                        args.len()
                    })
                {
                    let null = match arg {
                        ExprNode::Const(value) => matches!(value, Value::Null),
                        ExprNode::Placeholder(i) => {
                            crate::engine::params::with_current_param(*i, |value| {
                                value.is_some_and(|value| matches!(value, Value::Null))
                            })
                        }
                        _ => false,
                    };
                    items.push(if null { None } else { Some(Text::node(arg)?) });
                }
                if matches!(kernel, FunctionKernel::MakeSet) {
                    Kind::Make(items)
                } else {
                    Kind::Elt(items)
                }
            }
            _ => return None,
        };
        Some(Self { args, kind })
    }
    pub(super) fn contract(&self) -> Contract {
        Contract {
            domain: if matches!(self.kind, Kind::Char) {
                Domain::Binary
            } else {
                Domain::Text
            },
            parallel: matches!(self.kind, Kind::Char),
        }
    }
    pub(super) fn workspace(&self, batch: &Batch) -> Option<(usize, usize)> {
        let rows = batch.num_rows();
        let width = match &self.kind {
            Kind::Char => self.args.len().checked_mul(4)?,
            Kind::Space => match super::contract(&self.args[0], batch.schema())?.domain {
                Domain::Integer(_, max) if max < 1024 * 1024 => usize::try_from(max.max(0)).ok()?,
                _ => {
                    let ExprNode::Column(i) = self.args[0] else {
                        return None;
                    };
                    match batch.columns().get(i)? {
                        ColumnVector::I64 { data, .. } => {
                            usize::try_from(data.iter().copied().max().unwrap_or(0).max(0)).ok()?
                        }
                        ColumnVector::U64 { data, .. } => {
                            usize::try_from(data.iter().copied().max().unwrap_or(0)).ok()?
                        }
                        ColumnVector::Null { .. } => 0,
                        _ => return None,
                    }
                }
            },
            Kind::Export {
                on,
                off,
                separator,
                count,
            } => {
                let a = on.with(|s| Some(s.len()))?;
                let b = off.with(|s| Some(s.len()))?;
                let sep = match separator {
                    Some(text) => text.with(|s| Some(s.len()))?,
                    None => 1,
                };
                a.max(b)
                    .checked_mul(*count)?
                    .checked_add(sep.checked_mul(count.saturating_sub(1))?)?
            }
            Kind::Make(items) | Kind::Elt(items) => {
                let mut bytes = items.len();
                for item in items.iter().flatten() {
                    bytes = bytes.checked_add(item.with(|s| Some(s.len()))?)?;
                }
                bytes
            }
        };
        let payload = rows.checked_mul(width.checked_add(8)?)?.checked_add(8)?;
        let mut work = payload.checked_mul(2)?.checked_add(512)?;
        for arg in self.args.iter().take(if matches!(self.kind, Kind::Char) {
            self.args.len()
        } else {
            1
        }) {
            work = work.checked_add(super::workspace(arg, batch)?.1)?;
        }
        Some((payload, work))
    }
    pub(super) fn evaluate(&self, batch: &Batch) -> Option<ColumnVector> {
        let numeric_args = if matches!(self.kind, Kind::Char) {
            self.args.len()
        } else {
            1
        };
        let columns = self
            .args
            .iter()
            .take(numeric_args)
            .map(|arg| operand(arg, batch))
            .collect::<Option<Vec<_>>>()?;
        let number = |column: &ColumnVector, row| match column {
            ColumnVector::I64 { data, .. } => Some(data[row] as u64),
            ColumnVector::U64 { data, .. } => Some(data[row]),
            _ => None,
        };
        let on = match &self.kind {
            Kind::Export { on, .. } => Some(on.with(|s| Some(s.as_bytes().to_vec()))?),
            _ => None,
        };
        let off = match &self.kind {
            Kind::Export { off, .. } => Some(off.with(|s| Some(s.as_bytes().to_vec()))?),
            _ => None,
        };
        let separator = match &self.kind {
            Kind::Export {
                separator: Some(text),
                ..
            } => text.with(|s| Some(s.as_bytes().to_vec()))?,
            _ => vec![b','],
        };
        let items = match &self.kind {
            Kind::Make(items) | Kind::Elt(items) => items
                .iter()
                .map(|item| match item {
                    Some(text) => Some(Some(text.with(|s| Some(s.as_bytes().to_vec()))?)),
                    None => Some(None),
                })
                .collect::<Option<Vec<_>>>()?,
            _ => Vec::new(),
        };
        let packet = crate::engine::sysvar::builtin::connection::max_allowed_packet_bytes();
        let mut data = Vec::new();
        let mut elt_validity = if matches!(self.kind, Kind::Elt(_)) {
            Some(Bitmap::new_all_valid(batch.num_rows()))
        } else {
            None
        };
        let mut offsets = Vec::with_capacity(batch.num_rows() + 1);
        offsets.push(0);
        for row in 0..batch.num_rows() {
            if row & 63 == 0 {
                crate::engine::cancel::check_cancelled().ok()?;
            }
            if matches!(self.kind, Kind::Char) {
                for column in &columns {
                    if column.is_null(row) {
                        continue;
                    }
                    let n = match column.as_ref() {
                        ColumnVector::U64 { data, .. } => data[row].min(i64::MAX as u64) as u32,
                        _ => number(column, row)? as u32,
                    };
                    let bytes = n.to_be_bytes();
                    data.extend_from_slice(&bytes[((n.leading_zeros() / 8) as usize).min(3)..]);
                }
            } else if !columns[0].is_null(row) {
                let bits = number(&columns[0], row)?;
                let begin = data.len();
                match &self.kind {
                    Kind::Space => {
                        let n = match columns[0].as_ref() {
                            ColumnVector::I64 { data, .. } => {
                                usize::try_from(data[row].max(0)).ok()?
                            }
                            _ => usize::try_from(bits.min(i64::MAX as u64)).ok()?,
                        };
                        if n as u64 > packet {
                            return None;
                        }
                        data.resize(data.len().checked_add(n)?, b' ');
                    }
                    Kind::Export { count, .. } => {
                        for i in 0..*count {
                            if i > 0 {
                                data.extend_from_slice(&separator);
                            }
                            data.extend_from_slice(if bits & (1u64 << i) != 0 {
                                on.as_ref()?
                            } else {
                                off.as_ref()?
                            });
                        }
                    }
                    Kind::Make(_) => {
                        let mut first = true;
                        for (i, item) in items.iter().enumerate() {
                            if bits & (1u64 << i) == 0 {
                                continue;
                            }
                            if let Some(item) = item {
                                if !first {
                                    data.push(b',');
                                }
                                data.extend_from_slice(item);
                                first = false;
                            }
                        }
                    }
                    Kind::Elt(_) => {
                        if matches!(columns[0].as_ref(), ColumnVector::U64 { .. })
                            && bits > i64::MAX as u64
                        {
                            return None;
                        }
                        let item = bits
                            .checked_sub(1)
                            .and_then(|i| usize::try_from(i).ok())
                            .and_then(|i| items.get(i))
                            .and_then(Option::as_ref);
                        if let Some(item) = item {
                            data.extend_from_slice(item);
                        } else {
                            elt_validity.as_mut()?.set(row, false);
                        }
                    }
                    Kind::Char => unreachable!(),
                }
                if (data.len() - begin) as u64 > packet {
                    return None;
                }
            } else if let Some(validity) = elt_validity.as_mut() {
                validity.set(row, false);
            }
            offsets.push(u32::try_from(data.len()).ok()?);
        }
        let nulls = if matches!(self.kind, Kind::Elt(_)) {
            elt_validity
        } else if matches!(self.kind, Kind::Char) {
            None
        } else {
            validity(&columns[0])
        };
        Some(if matches!(self.kind, Kind::Char) {
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
}
