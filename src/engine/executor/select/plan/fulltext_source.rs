//! Posting access for current indexes and visible-heap correctness oracles.

use super::*;
use crate::engine::executor::mem_budget::{estimate_row_bytes, ScopedCharge};
use crate::storage::fulltext::{FtPosting, FtPostingBatch, Tokenizer};

/// Query-local ordinals use RowId's ordered representation only inside the
/// heap oracle. They never reach storage and cannot alias partition-local ids.
pub(super) fn heap_document_id(ordinal: usize) -> Result<RowId, QueryError> {
    let page = u32::try_from(ordinal >> 16).map_err(|_| {
        crate::engine::executor::mem_budget::allocation_error("FULLTEXT document ordinal overflow")
    })?;
    Ok(RowId::new(page, ordinal as u16))
}

pub(super) fn heap_document_offset(id: RowId) -> usize {
    ((id.page_id as usize) << 16) | id.slot_id as usize
}

pub(super) fn collect_heap_documents(
    db: &Arc<Database>,
    table: &PlanTable,
    txn: TxnView,
) -> Result<(Vec<Vec<Value>>, ScopedCharge), QueryError> {
    // Corpus statistics and document identities must not depend on a WHERE
    // clause's partition pruning. Residual predicates run after matching.
    let _pruning = match &table.explicit_partition_storage_ids {
        Some(ids) => crate::engine::executor::partition_router::push_pruned_partitions(ids.clone()),
        None => crate::engine::executor::partition_router::suspend_pruned_partitions(),
    };
    let mut rows = Vec::new();
    let mut charge = ScopedCharge::new(0);
    db.scan_rows_ordered_streaming(&table.schema.qualified_name(), txn, |values| {
        crate::engine::cancel::check_cancelled()?;
        heap_document_id(rows.len())?;
        // Include Vec growth headroom before retaining each visible document.
        charge.add_non_spillable(estimate_row_bytes(values).saturating_mul(2))?;
        rows.push(values.to_vec());
        Ok(())
    })?;
    Ok((rows, charge))
}

pub(super) struct FtSearchSource<'a> {
    db: &'a Arc<Database>,
    txn: TxnView,
    table: &'a PlanTable,
    index: &'a IndexSchema,
    heap_rows: Option<&'a [Vec<Value>]>,
    postings: AhHashMap<String, FtPostingBatch>,
    pub(super) corpus: Option<(f64, f64)>,
    pub(super) charge: ScopedCharge,
}

impl<'a> FtSearchSource<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        db: &'a Arc<Database>,
        txn: TxnView,
        table: &'a PlanTable,
        index: &'a IndexSchema,
        heap_rows: Option<&'a [Vec<Value>]>,
        tokenizer: &dyn Tokenizer,
        query: &str,
        mode: crate::parser::FtMode,
    ) -> Result<Self, QueryError> {
        use crate::storage::fulltext::boolean::{parse_boolean_query, Matcher};
        use crate::storage::fulltext::payload::FtPayload;
        let mut source = Self {
            db,
            txn,
            table,
            index,
            heap_rows,
            postings: ah_hash_map(),
            corpus: None,
            charge: ScopedCharge::new(0),
        };
        let Some(rows) = heap_rows else {
            return Ok(source);
        };
        let mut tokens = ah_hash_set();
        let mut prefixes = Vec::new();
        match mode {
            crate::parser::FtMode::NaturalLanguage => tokens.extend(tokenizer.tokenize(query)),
            crate::parser::FtMode::Boolean => {
                for clause in parse_boolean_query(query, tokenizer)? {
                    match clause.matcher {
                        Matcher::Term(terms) | Matcher::Phrase(terms) => tokens.extend(terms),
                        Matcher::Prefix(prefix) => prefixes.push(prefix),
                    }
                }
            }
        }
        let mut sum_len = 0u64;
        for (ordinal, values) in rows.iter().enumerate() {
            crate::engine::cancel::check_cancelled()?;
            let keys = table.schema.fulltext_index_keys(index, values)?;
            let mut counted_length = false;
            for key in keys {
                let mut parts = key.0.into_iter();
                let (Some(KeyPart::String(token)), Some(KeyPart::Bytes(payload))) =
                    (parts.next(), parts.next())
                else {
                    continue;
                };
                if !counted_length {
                    sum_len =
                        sum_len.saturating_add(FtPayload::decode_scoring_only(&payload, crate::storage::fulltext::payload::stored_positions(index))?.0 as u64);
                    counted_length = true;
                }
                if !tokens.contains(&token)
                    && !prefixes.iter().any(|prefix| token.starts_with(prefix))
                {
                    continue;
                }
                // Reserve for postings, position arenas, membership and score
                // maps retained by the runtime, including growth headroom.
                source.charge.add_non_spillable(
                    256 + token.len().saturating_mul(2) + payload.len().saturating_mul(16),
                )?;
                let batch = source.postings.entry(token).or_default();
                let (doc_len, tf, pos_start, pos_len) =
                    FtPayload::decode_into_arena(&payload, &mut batch.positions_arena, crate::storage::fulltext::payload::stored_positions(index))?;
                batch.postings.push(FtPosting {
                    row_id: heap_document_id(ordinal)?,
                    doc_len,
                    tf,
                    pos_start,
                    pos_len,
                });
            }
        }
        let avg = sum_len as f64 / rows.len().max(1) as f64;
        source.corpus = Some((rows.len() as f64, if avg > 0.0 { avg } else { 1.0 }));
        Ok(source)
    }

    pub(super) fn scan_token(
        &self,
        token: &str,
        positions: bool,
    ) -> Result<FtPostingBatch, QueryError> {
        if self.heap_rows.is_some() {
            return Ok(self
                .postings
                .get(token)
                .map(|batch| copy_batch(batch, positions))
                .unwrap_or_default());
        }
        self.db
            .fulltext_scan_token(&self.table.schema, self.index, token, positions, self.txn)
    }

    pub(super) fn scan_ids(&self, token: &str) -> Result<Vec<RowId>, QueryError> {
        if self.heap_rows.is_some() {
            return Ok(self
                .postings
                .get(token)
                .map(|batch| {
                    batch
                        .postings
                        .iter()
                        .map(|posting| posting.row_id)
                        .collect()
                })
                .unwrap_or_default());
        }
        self.db
            .fulltext_scan_token_ids(&self.table.schema, self.index, token, self.txn)
    }

    pub(super) fn scan_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, FtPostingBatch)>, QueryError> {
        if self.heap_rows.is_some() {
            let mut groups: Vec<_> = self
                .postings
                .iter()
                .filter(|(token, _)| token.starts_with(prefix))
                .map(|(token, batch)| (token.clone(), copy_batch(batch, false)))
                .collect();
            groups.sort_unstable_by(|left, right| left.0.cmp(&right.0));
            return Ok(groups);
        }
        self.db
            .fulltext_scan_prefix(&self.table.schema, self.index, prefix, self.txn)
    }

    pub(super) fn contains_phrase(
        &self,
        id: RowId,
        tokenizer: &dyn Tokenizer,
        columns: &[String],
        tokens: &[String],
    ) -> Result<bool, QueryError> {
        if let Some(rows) = self.heap_rows {
            return super::scan::fulltext_row_contains_phrase(
                tokenizer,
                &self.table.schema,
                columns,
                &rows[heap_document_offset(id)],
                tokens,
            );
        }
        match self.db.read_row(&self.table.schema, id, self.txn)? {
            Some(row) => super::scan::fulltext_row_contains_phrase(
                tokenizer,
                &self.table.schema,
                columns,
                &row.values,
                tokens,
            ),
            None => Ok(false),
        }
    }
}

fn copy_batch(batch: &FtPostingBatch, positions: bool) -> FtPostingBatch {
    if positions {
        return batch.clone();
    }
    FtPostingBatch {
        postings: batch
            .postings
            .iter()
            .map(|posting| FtPosting {
                pos_start: 0,
                pos_len: 0,
                ..*posting
            })
            .collect(),
        positions_arena: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};

    #[test]
    fn fulltext_heap_oracle_releases_admission_on_error_and_success() {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let engine = crate::engine::StorageQueryEngine::open(dir.path()).unwrap();
        engine
            .execute("CREATE TABLE budget_ft(id INT PRIMARY KEY,body TEXT,FULLTEXT KEY ft(body))")
            .unwrap();
        engine
            .execute("INSERT INTO budget_ft VALUES(1,'alpha beta gamma')")
            .unwrap();
        let db = engine.database();
        let schema = db.table("budget_ft").unwrap();
        let table = PlanTable::new(schema.id, schema, None);
        let txn = db.begin_txn().unwrap();
        let tiny = Arc::new(MemBudget::new(1));
        with_query_budget(tiny.clone(), || {
            let error = collect_heap_documents(db, &table, txn).err().unwrap();
            assert_eq!(error.code(), 1037);
            assert_eq!(tiny.used(), 0);
        });
        let budget = Arc::new(MemBudget::new(1 << 20));
        with_query_budget(budget.clone(), || {
            let (rows, charge) = collect_heap_documents(db, &table, txn).unwrap();
            assert_eq!(rows.len(), 1);
            assert!(budget.used() > 0);
            drop((rows, charge));
            assert_eq!(budget.used(), 0);
        });
        db.rollback_txn(txn.txn_id).unwrap();
        for ordinal in [0, 65535, 65536, 65537, u32::MAX as usize] {
            assert_eq!(
                heap_document_offset(heap_document_id(ordinal).unwrap()),
                ordinal
            );
        }
    }
}
