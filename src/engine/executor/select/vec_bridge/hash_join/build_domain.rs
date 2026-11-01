//! Lossless build-key reduction from a complete preserved SEMI/ANTI input.

use super::*;
use crate::engine::executor::mem_budget::{self, ScopedCharge};
use crate::engine::executor::select::bloom::{ExactIntKeySet, ExactIntKeySetBuilder};

pub(super) enum BuildDomain {
    Integer(Arc<ExactIntKeySet>),
    Encoded {
        keys: hashbrown::HashSet<Vec<u8>, ahash::RandomState>,
        _charge: ScopedCharge,
    },
}

impl BuildDomain {
    /// Allocation refusal disables this optional reduction. The caller still
    /// owns every probe batch and can replay it into the ordinary spill path.
    pub(super) fn from_probe(
        batches: &[Batch],
        key_columns: &[usize],
        null_safe: &[bool],
        collations: &[Option<u16>],
        native_i64: bool,
        rows: usize,
    ) -> Option<Self> {
        if native_i64 {
            let mut builder = ExactIntKeySetBuilder::with_capacity(rows).ok()?;
            for batch in batches {
                let column = &batch.columns()[key_columns[0]];
                if let Some(values) = column.as_i64_slice() {
                    for row in batch.visible_indices() {
                        if !column.is_null(row) {
                            builder.insert(i128::from(values[row]));
                        }
                    }
                }
            }
            return builder
                .finish()
                .ok()
                .map(|keys| Self::Integer(Arc::new(keys)));
        }

        let mut charge = ScopedCharge::new(0);
        let cap = mem_budget::current_budget().map_or(usize::MAX, |budget| budget.limit());
        // Reserve the full table before allocating buckets; encoded strings
        // are charged separately before each distinct key is copied.
        if !charge.add_within(rows.saturating_mul(64), cap) {
            return None;
        }
        let mut keys = hashbrown::HashSet::with_hasher(ahash::RandomState::new());
        keys.try_reserve(rows).ok()?;
        let mut encoded = Vec::with_capacity(64);
        for batch in batches {
            for row in batch.visible_indices() {
                if vec_hash_join::encode_join_key_with_collations(
                    batch,
                    key_columns,
                    null_safe,
                    collations,
                    row,
                    &mut encoded,
                ) && !keys.contains(encoded.as_slice())
                {
                    if !charge.add_within(encoded.len(), cap) {
                        return None;
                    }
                    keys.insert(
                        mem_budget::try_copy_bytes(&encoded, "semi/anti build domain").ok()?,
                    );
                }
            }
        }
        Some(Self::Encoded {
            keys,
            _charge: charge,
        })
    }

    pub(super) fn install_scan_filter(
        &self,
        build: &mut dyn BatchOperator,
        keys: &[usize],
        rejected: &mut Option<Arc<std::sync::OnceLock<Arc<std::sync::atomic::AtomicU64>>>>,
    ) -> bool {
        let Self::Integer(filter) = self else {
            return false;
        };
        let Some(field) = build.schema().fields().get(keys[0]) else {
            return false;
        };
        let qualifier = field.alias.as_deref().unwrap_or(&field.table);
        let qualifier = qualifier
            .rsplit('.')
            .next()
            .unwrap_or(qualifier)
            .to_string();
        let counter = Arc::new(std::sync::OnceLock::new());
        if build.install_runtime_filter(crate::planner::RuntimeFilter::ExactIntKey {
            keys: vec![Expr::CompoundIdentifier(vec![
                qualifier,
                field.name.clone(),
            ])],
            null_safe: vec![false],
            filter: Arc::clone(filter),
            rejected: Arc::clone(&counter),
        }) {
            *rejected = Some(counter);
            true
        } else {
            false
        }
    }
}

/// Keep a local membership check even if a child cannot push the filter into
/// storage. A child accepting a filter is an optimization, never a correctness
/// prerequisite. No residual or preserved-side row is discarded here.
pub(super) struct BuildDomainStream<'a> {
    inner: Box<dyn BatchOperator + 'a>,
    domain: Option<BuildDomain>,
    keys: Vec<usize>,
    null_safe: Vec<bool>,
    collations: Vec<Option<u16>>,
}

impl<'a> BuildDomainStream<'a> {
    pub(super) fn new(
        inner: Box<dyn BatchOperator + 'a>,
        domain: BuildDomain,
        pushed: bool,
        keys: Vec<usize>,
        null_safe: Vec<bool>,
        collations: Vec<Option<u16>>,
    ) -> Self {
        Self {
            inner,
            domain: (!pushed).then_some(domain),
            keys,
            null_safe,
            collations,
        }
    }
}

impl BatchOperator for BuildDomainStream<'_> {
    fn schema(&self) -> &Arc<BatchSchema> {
        self.inner.schema()
    }

    fn properties(&self) -> BatchProperties {
        self.inner.properties()
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        loop {
            crate::engine::cancel::check_cancelled()?;
            let Some(batch) = self.inner.next_batch()? else {
                self.domain = None;
                return Ok(None);
            };
            let mut encoded = Vec::with_capacity(64);
            let mut selected =
                mem_budget::try_vec_with_capacity(batch.visible_rows(), "build domain selection")?;
            for row in batch.visible_indices() {
                let contains = match self.domain.as_ref() {
                    None => true,
                    Some(BuildDomain::Integer(keys)) => {
                        let column = &batch.columns()[self.keys[0]];
                        !column.is_null(row)
                            && column
                                .as_i64_slice()
                                .is_none_or(|values| keys.contains(i128::from(values[row])))
                    }
                    Some(BuildDomain::Encoded { keys, .. }) => {
                        vec_hash_join::encode_join_key_with_collations(
                            &batch,
                            &self.keys,
                            &self.null_safe,
                            &self.collations,
                            row,
                            &mut encoded,
                        ) && keys.contains(encoded.as_slice())
                    }
                };
                if contains {
                    selected.push(row as u32);
                }
            }
            analyze_stats::add_pending_runtime_filter_rejected(
                (batch.visible_rows() - selected.len()) as u64,
            );
            if !selected.is_empty() {
                // A sparse selection still owns the entire scan batch. The
                // ordinary build admission must see only compact live data,
                // otherwise rejected rows alone can force a Grace spill.
                return Ok(Some(Batch::with_row_count(
                    batch.schema().clone(),
                    batch
                        .columns()
                        .iter()
                        .map(|column| column.take_indices(&selected))
                        .collect(),
                    selected.len(),
                )));
            }
        }
    }

    fn install_runtime_filter(&mut self, filter: crate::planner::RuntimeFilter) -> bool {
        self.inner.install_runtime_filter(filter)
    }
}
