//! Immutable row metadata and identifier lookup shared by executor rows.
//!
//! Row values remain owned by `PlanRow`; this module owns only schema-level
//! information so every row produced by one operator can share the same
//! lookup table.

use super::plan::{ColMeta, ColMetaRef, PlanField};

/// Case-insensitive unqualified column lookup.
pub(super) type FieldNameMap<V> = hashbrown::HashMap<String, V, ahash::RandomState>;

pub(super) fn field_lookup_hasher() -> ahash::RandomState {
    static STATE: std::sync::OnceLock<ahash::RandomState> = std::sync::OnceLock::new();
    STATE
        .get_or_init(|| {
            ahash::RandomState::with_seeds(
                0x4D59_4442_4649_454C,
                0x4D59_4442_4C4F_4F4B,
                0x5550_FE73_6E16_2A35,
                0x9E37_79B9_7F4A_7C15,
            )
        })
        .clone()
}

#[derive(Debug, Default)]
pub(super) struct FieldLookup {
    pub(super) by_column: FieldNameMap<smallvec::SmallVec<[usize; 2]>>,
    pub(super) by_qualified: hashbrown::HashMap<(String, String), usize, ahash::RandomState>,
}

#[derive(Debug, Default)]
pub(crate) struct RowSchema {
    metas: std::sync::Arc<[ColMetaRef]>,
    lookup: std::sync::OnceLock<FieldLookup>,
}

/// Compact schema handle carried by every row. `None` means metadata should
/// be derived from the row's fields on first lookup; `Option<Arc<_>>` uses the
/// non-null pointer niche, so this remains pointer-sized without allocating or
/// touching a shared reference count for deferred rows.
#[derive(Debug, Clone, Default)]
pub(super) struct RowSchemaRef(Option<std::sync::Arc<RowSchema>>);

impl RowSchemaRef {
    pub(super) fn deferred() -> Self {
        Self(None)
    }

    #[inline]
    pub(super) fn resolve_index(
        &self,
        fields: &[PlanField],
        qualifier: Option<&str>,
        column: &str,
    ) -> Result<usize, crate::types::QueryError> {
        match &self.0 {
            Some(schema) => schema.resolve_index(fields, qualifier, column),
            None => resolve_linear(fields, qualifier, column),
        }
    }
}

impl From<std::sync::Arc<RowSchema>> for RowSchemaRef {
    fn from(schema: std::sync::Arc<RowSchema>) -> Self {
        Self(Some(schema))
    }
}

pub(super) const LOOKUP_THRESHOLD: usize = 8;

impl RowSchema {
    pub(super) fn empty() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self::default())
    }

    pub(super) fn from_fields(fields: &[PlanField]) -> std::sync::Arc<Self> {
        Self::from_meta_iter(fields.iter().map(|field| field.meta.clone()))
    }

    /// Do `fields` carry exactly this schema's column metadata?
    ///
    /// `ColMeta` is `&'static`, so this is a pointer-equality sweep — cheap
    /// enough to run per row, which lets a batch of homogeneous rows share one
    /// `Arc<RowSchema>` (and therefore one lazily-built `FieldLookup`) instead
    /// of rebuilding both per row.
    pub(super) fn matches_fields(&self, fields: &[PlanField]) -> bool {
        self.metas.len() == fields.len()
            && self
                .metas
                .iter()
                .zip(fields)
                .all(|(meta, field)| meta.ptr_eq(&field.meta))
    }

    pub(super) fn from_metas(metas: &[ColMetaRef]) -> std::sync::Arc<Self> {
        Self::from_meta_iter(metas.iter().cloned())
    }

    fn from_meta_iter(metas: impl ExactSizeIterator<Item = ColMetaRef>) -> std::sync::Arc<Self> {
        if metas.len() == 0 {
            return Self::empty();
        }
        std::sync::Arc::new(Self {
            metas: metas.collect(),
            lookup: std::sync::OnceLock::new(),
        })
    }

    pub(super) fn lookup<'a>(&'a self, fallback_fields: &[PlanField]) -> &'a FieldLookup {
        self.lookup.get_or_init(|| {
            if self.metas.is_empty() {
                FieldLookup::build_from_fields(fallback_fields)
            } else {
                FieldLookup::build_from_metas(self.metas.iter().map(|meta| &**meta))
            }
        })
    }

    /// Resolve one SQL identifier against this schema. Alias shadowing,
    /// qualification and ambiguity are defined here so the linear and hashed
    /// paths cannot drift semantically.
    #[inline]
    pub(super) fn resolve_index(
        &self,
        fields: &[PlanField],
        qualifier: Option<&str>,
        column: &str,
    ) -> Result<usize, crate::types::QueryError> {
        if fields.len() > LOOKUP_THRESHOLD {
            return resolve_hashed(self.lookup(fields), qualifier, column);
        }
        resolve_linear(fields, qualifier, column)
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.metas.len()
    }
}

fn resolve_hashed(
    lookup: &FieldLookup,
    qualifier: Option<&str>,
    column: &str,
) -> Result<usize, crate::types::QueryError> {
    let index = if let Some(qualifier) = qualifier {
        match get_field_qualified_ci(&lookup.by_qualified, qualifier, column).copied() {
            Some(usize::MAX) => return Err(ambiguous_column(column)),
            index => index,
        }
    } else {
        match get_field_ci(&lookup.by_column, column) {
            Some(indices) if indices.len() == 1 => Some(indices[0]),
            Some(_) => return Err(ambiguous_column(column)),
            None => None,
        }
    };
    index.ok_or_else(|| unknown_column(column))
}

fn resolve_linear(
    fields: &[PlanField],
    qualifier: Option<&str>,
    column: &str,
) -> Result<usize, crate::types::QueryError> {
    let mut found = None;
    for (index, field) in fields.iter().enumerate() {
        if !field.column.eq_ignore_ascii_case(column)
            || !qualifier.is_none_or(|value| field_matches_qualifier(field, value))
        {
            continue;
        }
        if qualifier.is_some_and(|value| value.eq_ignore_ascii_case("$group")) {
            found = Some(index);
            continue;
        }
        if found.is_some() {
            return Err(ambiguous_column(column));
        }
        found = Some(index);
    }
    found.ok_or_else(|| unknown_column(column))
}

#[inline]
pub(super) fn field_matches_qualifier(field: &PlanField, qualifier: &str) -> bool {
    match field.alias.as_ref() {
        Some(alias) => alias.eq_ignore_ascii_case(qualifier),
        None => !field.table.is_empty() && field.table.eq_ignore_ascii_case(qualifier),
    }
}

fn ambiguous_column(column: &str) -> crate::types::QueryError {
    crate::storage::error(
        crate::error::catalog::ER_NON_UNIQ_ERROR,
        format!("column '{column}' is ambiguous"),
    )
}

fn unknown_column(column: &str) -> crate::types::QueryError {
    crate::error::messages::bad_field(column, "field list")
}

impl FieldLookup {
    fn build_from_metas<'a, I>(iter: I) -> Self
    where
        I: IntoIterator<Item = &'a ColMeta>,
        I::IntoIter: ExactSizeIterator,
    {
        let iter = iter.into_iter();
        let capacity = iter.len();
        let mut by_column = FieldNameMap::with_capacity_and_hasher(capacity, field_lookup_hasher());
        let mut by_qualified = hashbrown::HashMap::with_capacity_and_hasher(
            capacity.saturating_mul(2),
            field_lookup_hasher(),
        );
        for (index, meta) in iter.enumerate() {
            let column = meta.column.to_ascii_lowercase();
            by_column
                .entry(column.clone())
                .or_insert_with(smallvec::SmallVec::new)
                .push(index);
            let qualifier = match meta.alias.as_deref() {
                Some(alias) => alias,
                None if !meta.table.is_empty() => &meta.table,
                None => continue,
            };
            let is_group = qualifier.eq_ignore_ascii_case("$group");
            by_qualified
                .entry((qualifier.to_ascii_lowercase(), column))
                .and_modify(|prior| {
                    *prior = if is_group { index } else { usize::MAX };
                })
                .or_insert(index);
        }
        Self {
            by_column,
            by_qualified,
        }
    }

    pub(super) fn build(cols: &[ColMeta]) -> Self {
        Self::build_from_metas(cols.iter())
    }

    pub(super) fn build_from_fields(fields: &[PlanField]) -> Self {
        Self::build_from_metas(fields.iter().map(|field| &*field.meta))
    }

    #[inline]
    pub(super) fn get_unqualified(&self, column: &str) -> Option<&smallvec::SmallVec<[usize; 2]>> {
        get_field_ci(&self.by_column, column)
    }
}

#[inline]
fn is_ascii_lower_already(value: &str) -> bool {
    !value.bytes().any(|byte| byte.is_ascii_uppercase())
}

#[inline]
pub(super) fn get_field_ci<'a, V>(map: &'a FieldNameMap<V>, column: &str) -> Option<&'a V> {
    let mut buffer = smallvec::SmallVec::new();
    map.get(lowercase_borrowed(column, &mut buffer))
}

#[inline]
pub(super) fn get_field_qualified_ci<'a, V>(
    map: &'a hashbrown::HashMap<(String, String), V, ahash::RandomState>,
    qualifier: &str,
    column: &str,
) -> Option<&'a V> {
    use core::hash::{BuildHasher, Hash, Hasher};

    let mut qualifier_buffer: smallvec::SmallVec<[u8; 64]> = smallvec::SmallVec::new();
    let mut column_buffer: smallvec::SmallVec<[u8; 64]> = smallvec::SmallVec::new();
    let qualifier = lowercase_borrowed(qualifier, &mut qualifier_buffer);
    let column = lowercase_borrowed(column, &mut column_buffer);

    let mut hasher = map.hasher().build_hasher();
    qualifier.hash(&mut hasher);
    column.hash(&mut hasher);
    map.raw_entry()
        .from_hash(hasher.finish(), |(stored_qualifier, stored_column)| {
            stored_qualifier == qualifier && stored_column == column
        })
        .map(|(_, value)| value)
}

#[inline]
fn lowercase_borrowed<'a>(value: &'a str, buffer: &'a mut smallvec::SmallVec<[u8; 64]>) -> &'a str {
    if is_ascii_lower_already(value) {
        return value;
    }
    buffer.clear();
    buffer.reserve(value.len());
    buffer.extend(value.bytes().map(|byte| byte.to_ascii_lowercase()));
    // ASCII case folding changes only one-byte code points, preserving UTF-8.
    unsafe { std::str::from_utf8_unchecked(buffer) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::executor::select::plan::{intern_col_meta, ColMeta, PlanField};
    use crate::types::{ColumnType, Value};

    fn field(table: &str, alias: Option<&str>, column: &str) -> PlanField {
        PlanField {
            meta: intern_col_meta(ColMeta {
                table: table.to_string(),
                alias: alias.map(str::to_string),
                column: column.to_string(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
                invisible: false,
            }),
            value: Value::Int(1),
        }
    }

    #[test]
    fn case_insensitive_lookup_matches_ascii_oracle_across_scratch_boundaries() {
        let names = [
            String::new(),
            "field".into(),
            "ÅField".into(),
            "åField".into(),
            "分类A".into(),
            "x".repeat(63),
            "x".repeat(64),
            "x".repeat(65),
            format!("{}A", "名".repeat(32)),
            "long".repeat(256),
        ];
        let mut plain = FieldNameMap::with_hasher(field_lookup_hasher());
        let mut qualified = hashbrown::HashMap::with_hasher(field_lookup_hasher());
        for (index, name) in names.iter().enumerate() {
            let key = name.to_ascii_lowercase();
            plain.insert(key.clone(), index);
            qualified.insert(("表alias".to_string(), key), index);
        }
        for name in &names {
            for probe in [
                name.clone(),
                name.to_ascii_lowercase(),
                name.to_ascii_uppercase(),
            ] {
                let expected = plain.get(&probe.to_ascii_lowercase());
                assert_eq!(get_field_ci(&plain, &probe), expected, "{probe}");
                for alias in ["表alias", "表ALIAS"] {
                    assert_eq!(
                        get_field_qualified_ci(&qualified, alias, &probe),
                        expected,
                        "{alias}.{probe}"
                    );
                }
                assert_eq!(get_field_qualified_ci(&qualified, "missing", &probe), None);
            }
        }
        assert_eq!(get_field_ci(&plain, "MISSING"), None);
        // A reused scratch buffer must not leak old bytes into a name or
        // invalidate UTF-8; already-lowercase input still bypasses scratch.
        let mut scratch = smallvec::SmallVec::from_slice(&[0xff; 65]);
        assert_eq!(lowercase_borrowed("ÅNAME", &mut scratch), "Åname");
        let lower = "原样name";
        assert_eq!(
            lowercase_borrowed(lower, &mut scratch).as_ptr(),
            lower.as_ptr()
        );
        assert_eq!(lowercase_borrowed("NEXT", &mut scratch), "next");
    }

    #[test]
    fn deferred_wide_resolution_matches_materialized_lookup_rules() {
        let mut fields = vec![
            field("orders", Some("o"), "ID"),
            field("orders", None, "id"),
            field("orders", None, "ID"),
            field("", None, "id"),
            field("hidden", Some(""), "empty_alias"),
            field("$group", None, "key"),
            field("$group", None, "KEY"),
            field("unicode", None, "分类"),
        ];
        fields.extend((0..32).map(|index| field("other", None, &format!("c{index}"))));
        let lookup = FieldLookup::build_from_fields(&fields);
        let deferred = RowSchemaRef::deferred();
        for qualifier in [
            None,
            Some(""),
            Some("O"),
            Some("ORDERS"),
            Some("hidden"),
            Some("$group"),
            Some("unicode"),
            Some("other"),
            Some("missing"),
        ] {
            for column in ["id", "ID", "empty_alias", "key", "分类", "C31", "missing"] {
                assert_eq!(
                    deferred.resolve_index(&fields, qualifier, column),
                    resolve_hashed(&lookup, qualifier, column),
                    "qualifier={qualifier:?}, column={column}",
                );
            }
        }
    }

    #[test]
    fn schema_lookup_handles_alias_case_and_ambiguity() {
        let fields = vec![field("orders", Some("o"), "ID"), field("items", None, "id")];
        let schema = RowSchema::from_fields(&fields);
        let lookup = schema.lookup(&fields);
        assert_eq!(
            lookup.get_unqualified("Id").map(|ids| ids.as_slice()),
            Some(&[0, 1][..])
        );
        assert_eq!(
            get_field_qualified_ci(&lookup.by_qualified, "O", "id"),
            Some(&0)
        );
        assert_eq!(
            get_field_qualified_ci(&lookup.by_qualified, "orders", "id"),
            None
        );
        assert_eq!(
            get_field_qualified_ci(&lookup.by_qualified, "ITEMS", "ID"),
            Some(&1)
        );
        assert_eq!(schema.len(), 2);
        assert_eq!(schema.resolve_index(&fields, Some("O"), "id"), Ok(0));
        assert_eq!(
            schema
                .resolve_index(&fields, Some("orders"), "id")
                .unwrap_err()
                .code(),
            1054
        );
        assert_eq!(
            schema
                .resolve_index(&fields, None, "id")
                .unwrap_err()
                .code(),
            1052
        );
    }

    #[test]
    fn hashed_and_linear_resolution_have_the_same_rules() {
        let mut fields = vec![field("orders", Some("o"), "id")];
        fields.extend((0..8).map(|index| field("orders", Some("o"), &format!("c{index}"))));
        fields.push(field("items", None, "id"));
        let schema = RowSchema::from_fields(&fields);
        assert_eq!(schema.resolve_index(&fields, Some("O"), "ID"), Ok(0));
        assert_eq!(
            schema
                .resolve_index(&fields, None, "id")
                .unwrap_err()
                .code(),
            1052
        );
        assert_eq!(
            schema
                .resolve_index(&fields, Some("orders"), "id")
                .unwrap_err()
                .code(),
            1054
        );
    }

    #[test]
    fn deferred_schema_is_pointer_sized_and_has_no_cross_row_lookup_state() {
        assert_eq!(
            std::mem::size_of::<RowSchemaRef>(),
            std::mem::size_of::<std::sync::Arc<RowSchema>>()
        );
        let first: Vec<_> = (0..10)
            .map(|index| field("first", None, &format!("a{index}")))
            .collect();
        let second: Vec<_> = (0..10)
            .map(|index| field("second", None, &format!("b{index}")))
            .collect();
        let deferred = RowSchemaRef::deferred();
        assert_eq!(deferred.resolve_index(&first, None, "a9"), Ok(9));
        assert_eq!(deferred.resolve_index(&second, None, "b9"), Ok(9));
        assert_eq!(
            deferred
                .resolve_index(&second, None, "a9")
                .unwrap_err()
                .code(),
            1054
        );
    }
}
