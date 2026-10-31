//! Borrowed regular-expression reads with one governed cache lookup per batch.
use super::*;
use string_read::Text;

fn ast_integer(expr: &Expr) -> Option<i64> {
    let read = |value: &Value| match value {
        Value::Int(n) => Some(*n),
        Value::UInt(n) => i64::try_from(*n).ok(),
        _ => None,
    };
    match expr {
        Expr::Literal(Literal::Int(n)) => Some(*n),
        Expr::Literal(Literal::UInt(n)) => i64::try_from(*n).ok(),
        Expr::BoundValue(value) => read(value),
        Expr::Placeholder(i) => crate::engine::params::with_current_param(*i, |value| read(value?)),
        Expr::Unary {
            op: UnaryOp::Neg,
            expr,
        } => ast_integer(expr)?.checked_neg(),
        _ => None,
    }
}
fn character_column(schema: &BatchSchema, column: usize) -> Option<()> {
    let field = schema.fields().get(column)?;
    (matches!(field.column_type, ColumnType::VarString | ColumnType::Blob)
        && field.source.flags
            & (crate::mysql::protocol::ENUM_FLAG
                | crate::mysql::protocol::SET_FLAG
                | crate::mysql::protocol::BINARY_FLAG)
            == 0
        && field.source.collation_id != Some(crate::charset::BINARY_COLLATION_ID))
    .then_some(())
}

#[derive(Clone, Copy)]
enum Kind {
    Like,
    Instr,
    Substr,
}
pub(super) struct RegexpRead<'a> {
    column: usize,
    pattern: Text<'a>,
    flags: Option<Text<'a>>,
    collation: Option<u16>,
    kind: Kind,
    pos: usize,
    occurrence: usize,
    return_end: bool,
}
impl<'a> RegexpRead<'a> {
    pub(super) fn bind(node: &'a ExprNode, schema: &BatchSchema) -> Option<Self> {
        // INSTR/SUBSTR remain legacy escapes for strict compilation probes.
        // Recognize only a borrowed column plus scalar constant/parameter args.
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
            let (kind, flags_index) = if name.eq_ignore_ascii_case("REGEXP_INSTR")
                && (2..=6).contains(&args.len())
            {
                (Kind::Instr, 5)
            } else if name.eq_ignore_ascii_case("REGEXP_SUBSTR") && (2..=5).contains(&args.len()) {
                (Kind::Substr, 4)
            } else {
                return None;
            };
            let Expr::Identifier(identifier) = &args[0] else {
                return None;
            };
            let column = legacy.column_index(identifier).ok()?;
            if columns.as_slice() != [column] {
                return None;
            }
            character_column(schema, column)?;
            let pos = match args.get(2) {
                Some(arg) => ast_integer(arg)?,
                None => 1,
            };
            if pos < 1 {
                return None;
            }
            let occurrence = match args.get(3) {
                Some(arg) => ast_integer(arg)?.max(1),
                None => 1,
            };
            let return_opt = if matches!(kind, Kind::Instr) {
                match args.get(4) {
                    Some(arg) => ast_integer(arg)?,
                    None => 0,
                }
            } else {
                0
            };
            if !matches!(return_opt, 0 | 1) {
                return None;
            }
            let flags = match args.get(flags_index) {
                Some(arg) => Some(Text::ast(arg)?),
                None => None,
            };
            let collation = crate::engine::expr::aggregate_expr_collations(
                args.iter().take(2),
                legacy,
                "regexp",
            )
            .ok()?;
            return Some(Self {
                column,
                pattern: Text::ast(&args[1])?,
                flags,
                collation,
                kind,
                pos: usize::try_from(pos).ok()?,
                occurrence: usize::try_from(occurrence).ok()?,
                return_end: return_opt == 1,
            });
        }
        let ExprNode::Function {
            kernel,
            args,
            collation_id,
            ..
        } = node
        else {
            return None;
        };
        let (kind, flags_index) = match kernel {
            FunctionKernel::RegexpLike if (2..=3).contains(&args.len()) => (Kind::Like, 2),
            FunctionKernel::LegacyDeterministic("REGEXP_INSTR")
                if (2..=6).contains(&args.len()) =>
            {
                (Kind::Instr, 5)
            }
            FunctionKernel::LegacyDeterministic("REGEXP_SUBSTR")
                if (2..=5).contains(&args.len()) =>
            {
                (Kind::Substr, 4)
            }
            _ => return None,
        };
        let ExprNode::Column(column) = &args[0] else {
            return None;
        };
        character_column(schema, *column)?;
        let flags = match args.get(flags_index) {
            Some(arg) => Some(Text::node(arg)?),
            None => None,
        };
        let (pos, occurrence, return_end) = if matches!(kind, Kind::Like) {
            (1, 1, false)
        } else {
            let pos = match args.get(2) {
                Some(arg) => integer(arg)?,
                None => 1,
            };
            if pos < 1 {
                return None;
            }
            let occurrence = match args.get(3) {
                Some(arg) => integer(arg)?.max(1),
                None => 1,
            };
            let return_opt = if matches!(kind, Kind::Instr) {
                match args.get(4) {
                    Some(arg) => integer(arg)?,
                    None => 0,
                }
            } else {
                0
            };
            if !matches!(return_opt, 0 | 1) {
                return None;
            }
            (
                usize::try_from(pos).ok()?,
                usize::try_from(occurrence).ok()?,
                return_opt == 1,
            )
        };
        Some(Self {
            column: *column,
            pattern: Text::node(&args[1])?,
            flags,
            collation: *collation_id,
            kind,
            pos,
            occurrence,
            return_end,
        })
    }
    pub(super) fn contract(&self) -> Contract {
        Contract {
            domain: if matches!(self.kind, Kind::Substr) {
                Domain::Text
            } else {
                Domain::Integer(0, i64::MAX)
            },
            parallel: false,
        }
    }
    pub(super) fn workspace(&self, batch: &Batch) -> Option<(usize, usize)> {
        let bytes = batch
            .num_rows()
            .checked_mul(24)?
            .checked_add(8)?
            .checked_add(if matches!(self.kind, Kind::Substr) {
                batch.columns().get(self.column)?.memory_bytes()
            } else {
                0
            })?;
        // The regex cache owns its governor charge independently of this lane.
        Some((bytes, bytes.checked_mul(2)?.checked_add(65536)?))
    }
    pub(super) fn evaluate(&self, batch: &Batch) -> Option<ColumnVector> {
        let input = batch.columns().get(self.column)?;
        if !matches!(input, ColumnVector::Utf8 { .. } | ColumnVector::Null { .. }) {
            return None;
        }
        if (0..batch.num_rows()).all(|row| input.is_null(row)) {
            return Some(ColumnVector::Null {
                len: batch.num_rows(),
            });
        }
        let flags = match self.flags {
            Some(flags) => Some(flags.with(|s| Some(Value::String(s.into())))?),
            None => None,
        };
        let regex = self.pattern.with(|pattern| {
            crate::engine::expr::with_compare_collation(self.collation, || {
                crate::engine::expr::mysql_regex_for_operand(pattern, flags.as_ref(), false).ok()
            })
        })?;
        let mut numbers = Vec::new();
        let mut data = Vec::new();
        let mut offsets = Vec::new();
        let mut nulls = validity(input).unwrap_or_else(|| Bitmap::new_all_valid(batch.num_rows()));
        if matches!(self.kind, Kind::Substr) {
            offsets.reserve(batch.num_rows() + 1);
            offsets.push(0);
        } else {
            numbers.reserve(batch.num_rows());
        }
        for row in 0..batch.num_rows() {
            if row & 63 == 0 {
                crate::engine::cancel::check_cancelled().ok()?;
            }
            let mut number = 0;
            if !input.is_null(row) {
                let text = std::str::from_utf8(bytes_at(input, row)?).ok()?;
                if matches!(self.kind, Kind::Like) {
                    number = i64::from(regex.is_match(text));
                } else {
                    let start = if self.pos == 1 {
                        0
                    } else {
                        crate::engine::expr::regexp_start_byte(text, self.pos).ok()?
                    };
                    let found = if self.occurrence == 1 {
                        regex.find(&text[start..])
                    } else {
                        regex.find_iter(&text[start..]).nth(self.occurrence - 1)
                    };
                    if let Some(found) = found {
                        if matches!(self.kind, Kind::Substr) {
                            data.extend_from_slice(found.as_str().as_bytes());
                        } else {
                            let index = start
                                + if self.return_end {
                                    found.end()
                                } else {
                                    found.start()
                                };
                            number = i64::try_from(text[..index].chars().count())
                                .ok()?
                                .checked_add(1)?;
                        }
                    } else if matches!(self.kind, Kind::Substr) {
                        nulls.set(row, false);
                    }
                }
            }
            if matches!(self.kind, Kind::Substr) {
                offsets.push(u32::try_from(data.len()).ok()?);
            } else {
                numbers.push(number);
            }
        }
        Some(if matches!(self.kind, Kind::Substr) {
            ColumnVector::Utf8 {
                data,
                offsets,
                nulls: Some(nulls),
            }
        } else {
            ColumnVector::I64 {
                data: numbers,
                nulls: Some(nulls),
            }
        })
    }
}
