//! Canonical on-disk format versions.
//!
//! Every reader/writer aliases these constants instead of owning a numeric
//! literal. `scripts/render_storage_format_versions.sh` renders the public
//! table from this file and CI rejects documentation drift.

/// v50: LOB descriptors carry a persistent allocation generation. Reusing an
/// overflow page can no longer let stale undo free a newer chain with the same
/// page id (ABA). The descriptor/row, undo, and overflow page layouts changed.
///
/// v49: indexes have stable persisted identities and ordinary table/index
/// files use name-independent object/generation directories. RENAME INDEX is
/// catalog-only; pre-v49 directories are intentionally rejected.
/// v51: AUTO_INCREMENT dynamic metadata moved from per-table `.ai` sidecars
/// into `mysql.innodb_dynamic_metadata`.
/// v52: pending primary-key definitions and primary read-view fences.
/// v53: monotonic per-table physical index identity highwater.
/// v54: primary publication identity for physical-replica recovery.
/// v55: complete FULLTEXT tokenizer configuration in mysql.indexes.
/// v56: online index snapshot coverage in mysql.indexes.
/// v57: snapshot validity of column NOT NULL optimizer facts.
// v58 gives sequence restarts an atomic physical counter identity.
// v59 commits index retirement manifests in the data dictionary with DDL.
// v60 stores sequence recovery states and stable dependencies in catalog tables.
/// v62 removes the retired quantized IVF algorithm and its page format.
/// v63 persists vector WITH configuration independently from index comments.
/// v64: persistent vector manifests, snapshot identities and retirement ledger.
/// v65: encrypted AI Secret, Connection and Model catalog metadata.
pub const CATALOG_VERSION: u32 = 66;
/// v27: adds `AutoIncrementAdvance` and the AUTO_INCREMENT coverage fence in
/// `CheckpointEnd`.
/// v29: CRC covers the WAL frame header and payload.
/// v30: XA PREPARE references durable, checksummed binlog intent chunks.
/// v31: committed DDL generation retirement fences for physical redo.
/// v32: explicit segment replacement horizons and redo-only file compensation.
/// v33: sequence after-states and checkpoint coverage.
/// v34: append-ordered source time on every frame; atomic system file batches.
/// v35: transactional vector-index recovery batches.
/// v36: persistent spatial R-tree page batches.
/// v37: transaction-linked spatial undo and compensation cursors.
pub const WAL_VERSION: u32 = 39;
/// v14: reduced-redo pages require an FPI before later physiological writes.
pub const HEAP_PAGE_VERSION: u16 = 15;
pub const MVCC_ROW_VERSION: u8 = 9;
pub const INDEX_VERSION: u32 = 25;
pub const INDEX_PAGE_VERSION: u16 = 16;
/// Derived primary keys and Zone Map weights are part of physical replication
/// compatibility. Development directories using older keys must be rebuilt.
pub const COLLATION_KEY_VERSION: u32 = 2;
pub const UNDO_BINARY_VERSION: u8 = 14;
pub const UNDO_TABLESPACE_VERSION: u32 = 3;
pub const OVERFLOW_VERSION: u16 = 4;
/// v4 records the committed heap coverage frontier.
/// v7 persists native node membership and trained index definitions.
pub const PAGED_HNSW_INDEX_VERSION: u32 = 7;
pub const PAGED_HNSW_PAGE_VERSION: u16 = 4;
pub const ZONE_MAP_VERSION: u32 = 6;
/// Zone Map v6 uses shared-buffer-pool pages and physical redo. Page v2 adds
/// mutable directory children, mixed record leaves and root revisions. Page
/// v3 adds block-Rice membership and compact relative-key record slots.
pub const PAGED_ZONE_MAP_PAGE_VERSION: u16 = 3;

/// The optional legacy in-memory HNSW snapshot is independent from the
/// default paged `.hvec` format and therefore has its own version.
pub const IN_MEMORY_HNSW_VERSION: u16 = 3;

pub const NATIVE_REPLICATION_METADATA_VERSION: u32 = 3;
pub const COMMIT_CLOCK_VERSION: u32 = 2;
/// Local repository namespaces and bundle/catalog maintenance formats.
pub const BACKUP_REPOSITORY_FORMAT: &str = "MYDB_BACKUP_REPOSITORY/2";
pub const BACKUP_BUNDLE_VERSION: u32 = 3;
pub const BACKUP_CHECKPOINT_VERSION: u32 = 1;
pub const BACKUP_LINEAGE_VERSION: u32 = 1;
pub const NATIVE_IMAGE_VERSION: u32 = 5;
/// The image mode pointer is the single promotion commit point.
pub const IMAGE_MODE_VERSION: u32 = 2;
pub const BINLOG_MANIFEST_VERSION: u32 = 2;
pub const STORAGE_CONTROL_VERSION: u32 = 1;
pub const ROW_CODEC_SPEC_VERSION: u16 = 1;

pub fn native_replication_compatibility() -> String {
    format!(
        "replmeta={NATIVE_REPLICATION_METADATA_VERSION};catalog={CATALOG_VERSION};wal={WAL_VERSION};heap={HEAP_PAGE_VERSION};row={MVCC_ROW_VERSION};index={INDEX_VERSION};index_page={INDEX_PAGE_VERSION};collation={COLLATION_KEY_VERSION};undo={UNDO_BINARY_VERSION};undo_ts={UNDO_TABLESPACE_VERSION};overflow={OVERFLOW_VERSION};hnsw={PAGED_HNSW_INDEX_VERSION}/{PAGED_HNSW_PAGE_VERSION};zone={ZONE_MAP_VERSION}/{PAGED_ZONE_MAP_PAGE_VERSION};clock={COMMIT_CLOCK_VERSION};control={STORAGE_CONTROL_VERSION};owner={PAGE_OWNER_PROOF_VERSION};codec={ROW_CODEC_SPEC_VERSION};spatial={SPATIAL_PAGE_VERSION};vector_state={VECTOR_STATE_VERSION};jsonb={JSONB_VERSION};geometry={GEOMETRY_VALUE_VERSION};native_vector_value={NATIVE_VECTOR_VALUE_VERSION};vector_binlog={VECTOR_BINLOG_VERSION};progress={REPLICA_PROGRESS_VERSION};slot={REPLICATION_SLOT_VERSION};channel={MYSQL_CHANNEL_VERSION};relay={MYSQL_RELAY_VERSION};ledger={COMMIT_LEDGER_VERSION};history={HISTORY_INDEX_VERSION};capture={WAL_RETENTION_VERSION};sysvars={PERSISTED_SYSVARS_VERSION};keyring={AI_KEYRING_VERSION};page=16384;endian=little"
    )
}

pub const SPATIAL_PAGE_VERSION: u16 = 2;

pub const PAGE_OWNER_PROOF_VERSION: u32 = 1;

pub const REPLICA_PROGRESS_VERSION: u16 = 1;
pub const REPLICATION_SLOT_VERSION: u16 = 1;
pub const MYSQL_CHANNEL_VERSION: u16 = 1;
pub const MYSQL_RELAY_VERSION: u16 = 1;
pub const COMMIT_LEDGER_VERSION: u32 = 2;
pub const HISTORY_INDEX_VERSION: u32 = 2;
pub const PERSISTED_SYSVARS_VERSION: u32 = 1;
pub const AI_KEYRING_VERSION: u32 = 1;
pub const WAL_RETENTION_VERSION: u32 = 1;

pub const VECTOR_STATE_VERSION: u16 = 1;
pub const JSONB_VERSION: u16 = 1;
pub const GEOMETRY_VALUE_VERSION: u16 = 1;
pub const NATIVE_VECTOR_VALUE_VERSION: u8 = 1;
pub const VECTOR_BINLOG_VERSION: u16 = 2;
