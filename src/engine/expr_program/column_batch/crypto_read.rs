//! Governed borrowed AES inputs; no key schedule is retained across batches.
use super::*;
use string_read::Text;

enum Input<'a> {
    Column(usize),
    Child(Box<CryptoRead<'a>>),
}
pub(super) struct CryptoRead<'a> {
    input: Input<'a>,
    key: Text<'a>,
    encrypt: bool,
}
impl<'a> CryptoRead<'a> {
    pub(super) fn bind(node: &'a ExprNode, schema: &BatchSchema) -> Option<Self> {
        let ExprNode::LegacyEscape {
            expr,
            schema: legacy,
            ..
        } = node
        else {
            return None;
        };
        Self::ast(expr, legacy, schema, 0)
    }
    fn ast(
        expr: &'a Expr,
        legacy: &TableSchema,
        schema: &BatchSchema,
        depth: usize,
    ) -> Option<Self> {
        if depth > 8 {
            return None;
        }
        let Expr::Function {
            name,
            args,
            distinct: false,
        } = expr
        else {
            return None;
        };
        if args.len() != 2 {
            return None;
        }
        let encrypt = if name.eq_ignore_ascii_case("AES_ENCRYPT") {
            true
        } else if name.eq_ignore_ascii_case("AES_DECRYPT") {
            false
        } else {
            return None;
        };
        let key = Text::ast(&args[1])?;
        let input = if let Expr::Identifier(name) = &args[0] {
            let column = legacy.column_index(name).ok()?;
            let field = schema.fields().get(column)?;
            if !matches!(field.column_type, ColumnType::VarString | ColumnType::Blob)
                || field.source.flags
                    & (crate::mysql::protocol::ENUM_FLAG | crate::mysql::protocol::SET_FLAG)
                    != 0
            {
                return None;
            }
            let collation = field
                .source
                .collation_id
                .unwrap_or_else(crate::engine::expr::connection_collation_id);
            if !matches!(
                crate::charset::resolve_collation_id(collation)?.charset,
                "binary" | "utf8mb4" | "utf8mb3"
            ) {
                return None;
            }
            Input::Column(column)
        } else {
            Input::Child(Box::new(Self::ast(&args[0], legacy, schema, depth + 1)?))
        };
        Some(Self {
            input,
            key,
            encrypt,
        })
    }
    pub(super) fn contract(&self) -> Contract {
        Contract {
            domain: Domain::Binary,
            parallel: false,
        }
    }
    fn output_bytes(&self, batch: &Batch) -> Option<usize> {
        match &self.input {
            Input::Column(i) => {
                let column = batch.columns().get(*i)?;
                let mut bytes = 0usize;
                for row in 0..batch.num_rows() {
                    if column.is_null(row) {
                        continue;
                    }
                    let length = bytes_at(column, row)?.len();
                    bytes = bytes.checked_add(if self.encrypt {
                        length.checked_div(16)?.checked_add(1)?.checked_mul(16)?
                    } else {
                        length
                    })?;
                }
                Some(bytes)
            }
            Input::Child(child) => child.output_bytes(batch)?.checked_add(if self.encrypt {
                batch.num_rows().checked_mul(16)?
            } else {
                0
            }),
        }
    }
    pub(super) fn workspace(&self, batch: &Batch) -> Option<(usize, usize)> {
        let bytes = self
            .output_bytes(batch)?
            .checked_add(batch.num_rows().checked_add(1)?.checked_mul(8)?)?;
        let child = match &self.input {
            Input::Column(_) => 0,
            Input::Child(child) => child.workspace(batch)?.1,
        };
        Some((
            bytes,
            child
                .checked_add(bytes.checked_mul(4)?)?
                .checked_add(4096)?,
        ))
    }
    pub(super) fn evaluate(&self, batch: &Batch) -> Option<ColumnVector> {
        self.key.with(|key| {
            let cipher = crate::engine::expr::AesEcb::prepare(key.as_bytes(), self.encrypt)?;
            let column = match &self.input {
                Input::Column(i) => std::borrow::Cow::Borrowed(batch.columns().get(*i)?),
                Input::Child(child) => std::borrow::Cow::Owned(child.evaluate(batch)?),
            };
            if !matches!(
                column.as_ref(),
                ColumnVector::Utf8 { .. } | ColumnVector::Bytes { .. } | ColumnVector::Null { .. }
            ) {
                return None;
            }
            let bound = self.output_bytes(batch)?;
            let mut blocks = Vec::<crate::engine::expr::AesBlock>::with_capacity(
                bound.checked_add(15)?.checked_div(16)?,
            );
            let mut ends = Vec::with_capacity(batch.num_rows() + 1);
            ends.push(0usize);
            let mut nulls =
                validity(&column).unwrap_or_else(|| Bitmap::new_all_valid(batch.num_rows()));
            for row in 0..batch.num_rows() {
                if row & 63 == 0 {
                    crate::engine::cancel::check_cancelled().ok()?;
                }
                if !column.is_null(row) {
                    let bytes = bytes_at(&column, row)?;
                    if !self.encrypt && (bytes.is_empty() || bytes.len() % 16 != 0) {
                        nulls.set(row, false);
                    } else {
                        let mut chunks = bytes.chunks_exact(16);
                        for chunk in &mut chunks {
                            blocks.push(crate::engine::expr::AesBlock::clone_from_slice(chunk));
                        }
                        if self.encrypt {
                            let tail = chunks.remainder();
                            let mut block = crate::engine::expr::AesBlock::default();
                            block[..tail.len()].copy_from_slice(tail);
                            block[tail.len()..].fill((16 - tail.len()) as u8);
                            blocks.push(block);
                        }
                    }
                }
                ends.push(blocks.len());
            }
            // ECB has no cross-block state; the backend can process its full
            // parallel block width across row boundaries without changing bytes.
            cipher.crypt_blocks(self.encrypt, &mut blocks);
            let mut data = Vec::with_capacity(bound);
            let mut offsets = Vec::with_capacity(batch.num_rows() + 1);
            offsets.push(0);
            for row in 0..batch.num_rows() {
                if row & 63 == 0 {
                    crate::engine::cancel::check_cancelled().ok()?;
                }
                if nulls.get(row) {
                    let row_blocks = &blocks[ends[row]..ends[row + 1]];
                    if self.encrypt {
                        for block in row_blocks {
                            data.extend_from_slice(block);
                        }
                    } else {
                        let last = row_blocks.last()?;
                        if let Some(length) = crate::engine::expr::pkcs7_unpadded_len(last) {
                            for block in &row_blocks[..row_blocks.len() - 1] {
                                data.extend_from_slice(block);
                            }
                            data.extend_from_slice(&last[..length]);
                        } else {
                            nulls.set(row, false);
                        }
                    }
                }
                offsets.push(u32::try_from(data.len()).ok()?);
            }
            Some(ColumnVector::Bytes {
                data,
                offsets,
                nulls: Some(nulls),
            })
        })
    }
}
