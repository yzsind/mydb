//! Full-text (FULLTEXT) index support, syntax-compatible with MySQL 8.0.
//!
//! ## Inverted index storage
//! Reuses the existing B-tree: one row of text emits multiple `(token-key, row_id)` inverted
//! entries (`KeyPart::String(token)`) after tokenization. Querying a token is a range scan
//! over its key, yielding every matching row_id. This choice lets fulltext indexes fully reuse
//! the existing index file format, DML maintenance, online DDL backfill and crash recovery, with little impact on the write path.
//!
//! ## Posting payload
//! An inverted entry's key looks like `[KeyPart::String(token), KeyPart::Bytes(payload)]`; the
//! second segment, encoded by [`payload::FtPayload`], carries per-(token, document) `doc_len` /
//! `tf` / positions. This gives natural-language scoring zero row lookups, phrase verification
//! from persisted positions, and **fully reuses** the existing leaf-page encoding / WAL / `insert_deferred` interfaces (`Bytes` is an existing `KeyPart`).
//!
//! ## Tokenizer consistency
//! Indexing and querying must use the same tokenization rules, or matches are missed. At DDL time
//! the current process's `MYDB_FT_*` settings are snapshotted into the index's
//! [`crate::storage::schema::FulltextMeta`] (ngram_size / min/max token size / stopwords);
//! the query side then uses [`tokenizer::build_tokenizer_from_meta`] to rebuild a same-rules
//! tokenizer from that snapshot — env changes across restarts can no longer mismatch historical indexes. Old indexes deserialize to `None` and use global defaults.
//!
//! ## Scoring
//! Relevance uses BM25 (the `bm25` submodule). `avgdl` is approximated by the candidate-set mean
//! (from the payload's `doc_len`, no row lookup needed); `N` is the catalog's live row count.

pub(crate) mod bm25;
pub(crate) mod boolean;
pub(crate) mod payload;
pub(crate) mod tokenizer;

#[cfg(test)]
pub(crate) use tokenizer::build_tokenizer;
pub(crate) use tokenizer::{build_tokenizer_from_meta, snapshot_tokenizer_config_into, Tokenizer};

use crate::storage::RowId;

/// One decoded inverted-index posting for a `(token, document)` pair.
///
/// `positions` refers into a shared `Vec<u32>` arena owned by the caller —
/// `range` is a slice `arena[range]`. Scan APIs that build a batch of
/// postings for one query share a single arena across all postings, so
/// `df ≈ 22k` postings cost one `Vec<u32>` growth instead of `df` heap
/// allocations. When positions were not stored (payload built with
/// `MYDB_FT_POSITIONS=0`, or the entry is corrupt/truncated), `range` is
/// empty; callers that need positions fall back to re-tokenising the row.
#[derive(Debug, Clone, Copy)]
pub struct FtPosting {
    pub row_id: RowId,
    pub doc_len: u32,
    pub tf: u32,
    pub pos_start: u32,
    pub pos_len: u32,
}

/// A batch of postings for one token (or one prefix expansion) sharing a
/// single positions arena. Ordered as the B-tree returned them (i.e.
/// **payload-bytes order**, not row-id order — sort by `row_id` post-scan
/// when set operations are needed).
#[derive(Debug, Default)]
pub struct FtPostingBatch {
    pub postings: Vec<FtPosting>,
    pub positions_arena: Vec<u32>,
}
