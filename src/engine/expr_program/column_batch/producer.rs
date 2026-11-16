//! JSON constructors over ordinary integer and character columns. Syntax-
//! sensitive booleans and richer SQL scalar descriptors retain their oracle.
use super::*;
use crate::engine::json::{self, Primitive};
use smallvec::SmallVec;
use std::borrow::Cow;

pub(super) struct Producer<'a> {
    columns: SmallVec<[usize; 4]>,
    keys: Option<SmallVec<[Cow<'a, str>; 4]>>,
}
impl<'a> Producer<'a> {
    pub(super) fn bind(node: &'a ExprNode, schema: &BatchSchema) -> Option<Self> {
        let ExprNode::Function { kernel, args, .. } = node else {
            return None;
        };
        let object = match kernel {
            FunctionKernel::LegacyDeterministic("JSON_ARRAY") => false,
            FunctionKernel::LegacyDeterministic("JSON_OBJECT") if args.len() % 2 == 0 => true,
            _ => return None,
        };
        let column = |node: &ExprNode| -> Option<usize> {
            let ExprNode::Column(index) = node else {
                return None;
            };
            let field = schema.fields().get(*index)?;
            if field.source.flags
                & (crate::mysql::protocol::ENUM_FLAG | crate::mysql::protocol::SET_FLAG)
                != 0
            {
                return None;
            }
            match field.column_type {
                ColumnType::Tiny
                | ColumnType::Short
                | ColumnType::MediumInt
                | ColumnType::Long
                | ColumnType::LongLong => Some(*index),
                ColumnType::VarString | ColumnType::Blob
                    if field.source.flags & crate::mysql::protocol::BINARY_FLAG == 0
                        && field.source.collation_id
                            != Some(crate::charset::BINARY_COLLATION_ID) =>
                {
                    Some(*index)
                }
                _ => None,
            }
        };
        let mut columns = SmallVec::new();
        let keys = if object {
            let mut entries = SmallVec::<[(Cow<'a, str>, usize); 4]>::new();
            for pair in args.chunks_exact(2) {
                let key = text(&pair[0])?;
                let value = column(&pair[1])?;
                if let Some(entry) = entries.iter_mut().find(|(k, _)| k.as_ref() == key.as_ref()) {
                    entry.1 = value;
                } else {
                    entries.push((key, value));
                }
            }
            entries.sort_by(|(a, _), (b, _)| (a.len(), a.as_bytes()).cmp(&(b.len(), b.as_bytes())));
            let mut keys = SmallVec::new();
            for (key, value) in entries {
                keys.push(key);
                columns.push(value);
            }
            Some(keys)
        } else {
            for arg in args {
                columns.push(column(arg)?);
            }
            None
        };
        Some(Self { columns, keys })
    }
    pub(super) fn workspace(&self, batch: &Batch) -> Option<(usize, usize)> {
        let n = batch.num_rows();
        // A container and either count/size varint fit in 21 bytes.
        let mut bytes = n
            .checked_mul(21)?
            .checked_add(n.checked_add(1)?.checked_mul(8)?)?;
        for &column in &self.columns {
            let input = batch.columns().get(column)?;
            let payload = match input {
                ColumnVector::I64 { .. } | ColumnVector::U64 { .. } => n.checked_mul(9)?,
                ColumnVector::Utf8 { data, .. } => data.len().checked_add(n.checked_mul(6)?)?,
                ColumnVector::Null { .. } => n,
                _ => return None,
            };
            bytes = bytes.checked_add(payload)?;
        }
        if let Some(keys) = &self.keys {
            for key in keys {
                bytes = bytes.checked_add(n.checked_mul(key.len().checked_add(10)?)?)?;
            }
        }
        Some((bytes, bytes.checked_mul(2)?.checked_add(256)?))
    }
    fn primitive<'b>(&self, batch: &'b Batch, column: usize, row: usize) -> Option<Primitive<'b>> {
        let column = batch.columns().get(column)?;
        if column.is_null(row) {
            return Some(Primitive::Null);
        }
        match column {
            ColumnVector::I64 { data, .. } => Some(Primitive::Int(data[row])),
            ColumnVector::U64 { data, .. } => Some(Primitive::UInt(data[row])),
            ColumnVector::Utf8 { data, offsets, .. } => Some(Primitive::Text(
                std::str::from_utf8(data.get(offsets[row] as usize..offsets[row + 1] as usize)?)
                    .ok()?,
            )),
            _ => None,
        }
    }
    pub(super) fn evaluate(&self, batch: &Batch) -> Option<ColumnVector> {
        let mut data = Vec::new();
        let mut offsets = Vec::with_capacity(batch.num_rows() + 1);
        offsets.push(0);
        for row in 0..batch.num_rows() {
            if row & 63 == 0 {
                crate::engine::cancel::check_cancelled().ok()?;
            }
            if let Some(keys) = &self.keys {
                let values = self
                    .columns
                    .iter()
                    .zip(keys)
                    .map(|(&column, key)| Some((key.as_ref(), self.primitive(batch, column, row)?)))
                    .collect::<Option<SmallVec<[(&str, Primitive<'_>); 4]>>>()?;
                json::append_primitive_object(&values, &mut data)?;
            } else {
                let values = self
                    .columns
                    .iter()
                    .map(|&column| self.primitive(batch, column, row))
                    .collect::<Option<SmallVec<[Primitive<'_>; 4]>>>()?;
                json::append_primitive_array(&values, &mut data)?;
            }
            offsets.push(u32::try_from(data.len()).ok()?);
        }
        Some(ColumnVector::Json {
            data,
            offsets,
            nulls: None,
        })
    }
}
