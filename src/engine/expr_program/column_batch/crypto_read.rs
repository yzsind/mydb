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
    pub(super) fn workspace(&self, batch: &Batch) -> Option<(usize, usize)> {
        let (source, child) = match &self.input {
            Input::Column(i) => (batch.columns().get(*i)?.memory_bytes(), 0),
            Input::Child(child) => child.workspace(batch)?,
        };
        let bytes = source
            .checked_add(
                batch
                    .num_rows()
                    .checked_mul(if self.encrypt { 24 } else { 8 })?,
            )?
            .checked_add(8)?;
        Some((
            bytes,
            child
                .checked_add(bytes.checked_mul(2)?)?
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
            let mut data = Vec::new();
            let mut offsets = Vec::with_capacity(batch.num_rows() + 1);
            offsets.push(0);
            let mut nulls =
                validity(&column).unwrap_or_else(|| Bitmap::new_all_valid(batch.num_rows()));
            for row in 0..batch.num_rows() {
                if row & 63 == 0 {
                    crate::engine::cancel::check_cancelled().ok()?;
                }
                if !column.is_null(row) {
                    match cipher.run(self.encrypt, bytes_at(&column, row)?) {
                        Some(bytes) => data.extend_from_slice(&bytes),
                        None => nulls.set(row, false),
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
