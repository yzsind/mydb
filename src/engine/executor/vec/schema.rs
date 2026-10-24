//! Batch-level schema description.
//!
//! Per-row metadata (table name, alias, column name, type) used to live on
//! every `PlanField`, which meant a wide-table SELECT cloned the same
//! strings once per column per row. `BatchSchema` lifts that metadata to
//! the batch level: every batch references a single `Arc<BatchSchema>` and
//! downstream operators look up column-by-name through the index built
//! here.

#[cfg(test)]
use std::collections::HashMap;
#[cfg(test)]
use std::hash::{Hash, Hasher};

use std::sync::Arc;

use crate::engine::expr_program::InputSchema;
use crate::types::{ColumnSource, ColumnType};

/// One output column described by a `BatchSchema`.
#[derive(Debug, Clone)]
pub struct BatchField {
    pub table: String,
    pub alias: Option<String>,
    pub name: String,
    pub column_type: ColumnType,
    /// Catalog/wire origin metadata. Vector execution must carry this
    /// unchanged: rebuilding it from `ColumnType` loses unsigned, declared
    /// length, collation and org-name information at the protocol boundary.
    pub source: ColumnSource,
}

/// Read-only column metadata for a `Batch`. Cheap to clone via `Arc`.
#[derive(Debug, Default)]
pub struct BatchSchema {
    fields: Vec<BatchField>,
    /// Prebuilt scalar-compiler schema for stable, cached plan templates.
    /// Ordinary one-shot schemas leave this empty and retain the existing
    /// build-on-demand behaviour.  The optional reservation is shared by the
    /// schema and follows every executing `Arc<BatchSchema>` after cache
    /// eviction, so resident derived memory is never refunded early.
    cached_input: Option<Arc<InputSchema>>,
    cache_charge: Option<Arc<crate::engine::executor::mem_budget::ControlledMemoryCharge>>,
    // These lookup tables and the fingerprint back the schema unit-test API;
    // production vector operators address columns by their compiled ordinal
    // and never consult them. Keeping their construction test-only avoids
    // rebuilding hash maps, lowercase Strings and a SipHash digest at every
    // short-lived OLTP Batch boundary.
    #[cfg(test)]
    by_name: HashMap<String, Vec<usize>>,
    #[cfg(test)]
    by_qualified: HashMap<(String, String), usize>,
    #[cfg(test)]
    fingerprint: u64,
}

impl BatchSchema {
    pub fn new(fields: Vec<BatchField>) -> Self {
        let schema = Self {
            fields,
            cached_input: None,
            cache_charge: None,
            #[cfg(test)]
            by_name: HashMap::new(),
            #[cfg(test)]
            by_qualified: HashMap::new(),
            #[cfg(test)]
            fingerprint: 0,
        };
        #[cfg(test)]
        {
            let mut schema = schema;
            schema.rebuild_lookup();
            schema.fingerprint = schema.compute_fingerprint();
            return schema;
        }
        #[cfg(not(test))]
        schema
    }

    pub fn fields(&self) -> &[BatchField] {
        &self.fields
    }

    pub(crate) fn install_cached_input(
        &mut self,
        input: Arc<InputSchema>,
        charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
    ) {
        debug_assert!(self.cached_input.is_none());
        self.cached_input = Some(input);
        self.cache_charge = Some(Arc::new(charge));
    }

    pub(crate) fn cached_input(&self) -> Option<&InputSchema> {
        self.cached_input.as_deref()
    }

    /// Heap capacities owned by the batch metadata itself. The cached input
    /// and cache-key storage are accounted separately by the caller.
    pub(crate) fn heap_capacity_bytes(&self) -> usize {
        self.fields
            .capacity()
            .saturating_mul(std::mem::size_of::<BatchField>())
            .saturating_add(self.fields.iter().fold(0usize, |bytes, field| {
                bytes
                    .saturating_add(field.table.capacity())
                    .saturating_add(field.alias.as_ref().map_or(0, String::capacity))
                    .saturating_add(field.name.capacity())
                    .saturating_add(field.source.schema.capacity())
                    .saturating_add(field.source.table.capacity())
                    .saturating_add(field.source.org_table.capacity())
                    .saturating_add(field.source.org_name.capacity())
            }))
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    #[cfg(test)]
    pub fn fingerprint(&self) -> u64 {
        self.fingerprint
    }

    #[cfg(test)]
    pub fn index_of(&self, qualifier: Option<&str>, column: &str) -> Option<usize> {
        let col_key = column.to_ascii_lowercase();
        if let Some(q) = qualifier {
            return self
                .by_qualified
                .get(&(q.to_ascii_lowercase(), col_key))
                .copied();
        }
        match self.by_name.get(&col_key) {
            Some(v) if v.len() == 1 => Some(v[0]),
            _ => None,
        }
    }

    #[cfg(test)]
    fn rebuild_lookup(&mut self) {
        for (idx, f) in self.fields.iter().enumerate() {
            let key = f.name.to_ascii_lowercase();
            self.by_name.entry(key.clone()).or_default().push(idx);
            if !f.table.is_empty() {
                self.by_qualified
                    .insert((f.table.to_ascii_lowercase(), key.clone()), idx);
            }
            if let Some(alias) = &f.alias {
                self.by_qualified
                    .insert((alias.to_ascii_lowercase(), key), idx);
            }
        }
    }

    #[cfg(test)]
    fn compute_fingerprint(&self) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.fields.len().hash(&mut h);
        for f in &self.fields {
            f.table.to_ascii_lowercase().hash(&mut h);
            f.alias
                .as_ref()
                .map(|s| s.to_ascii_lowercase())
                .hash(&mut h);
            f.name.to_ascii_lowercase().hash(&mut h);
            f.column_type.hash(&mut h);
            f.source.hash(&mut h);
        }
        h.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(t: &str, n: &str, ty: ColumnType) -> BatchField {
        BatchField {
            table: t.to_string(),
            alias: None,
            name: n.to_string(),
            column_type: ty,
            source: ColumnSource::default(),
        }
    }

    #[test]
    fn fingerprint_stable_across_clones() {
        let s = BatchSchema::new(vec![
            field("t", "a", ColumnType::LongLong),
            field("t", "b", ColumnType::VarString),
        ]);
        let f1 = s.fingerprint();
        let s2 = std::sync::Arc::new(s);
        assert_eq!(s2.fingerprint(), f1);
    }

    #[test]
    fn lookup_by_qualifier_and_name() {
        let s = BatchSchema::new(vec![
            field("t", "a", ColumnType::LongLong),
            field("t", "b", ColumnType::VarString),
            field("u", "a", ColumnType::Double),
        ]);
        assert_eq!(s.index_of(Some("t"), "a"), Some(0));
        assert_eq!(s.index_of(Some("u"), "a"), Some(2));
        // ambiguous unqualified `a`
        assert_eq!(s.index_of(None, "a"), None);
        assert_eq!(s.index_of(None, "b"), Some(1));
    }
}
