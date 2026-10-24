//! Shared snapshot of "table statistics for metadata queries".
//!
//! `SHOW TABLE STATUS`, `information_schema.tables`, and
//! `information_schema.partitions` historically computed their numbers
//! independently — `SHOW TABLE STATUS` ran width-based estimators in
//! [`crate::engine::show`] while `information_schema.tables` and the
//! per-partition rows in `information_schema.partitions` hard-coded
//! `AVG_ROW_LENGTH` / `DATA_LENGTH` / `INDEX_LENGTH` to `0`. That meant
//! the same table reported wildly different sizes to two MySQL-shaped
//! queries, and JDBC metadata / ORMs that consult `information_schema`
//! always saw `0`.
//!
//! This module is the single source of truth. The view is **cheap** by
//! construction:
//!
//!  * Row counts come from `TableStats.row_count` plus the in-memory
//!    DML delta — never `Database::table_row_count`, which falls back
//!    to a full heap scan when stats are stale.
//!  * Durable heap/index/LOB allocation comes from warm handle counters or
//!    cold file metadata; memory-only/missing files use catalog width
//!    estimates. No data/index page is pulled into the buffer pool.
//!  * `AUTO_INCREMENT` is the cached next-value, same as
//!    `SHOW CREATE TABLE`.
//!
//! Both `SHOW TABLE STATUS` and `information_schema` build this view
//! once per table per query, so the cost is bounded by the number of
//! catalog entries we already iterate. Per-partition variants reuse
//! the same width estimators but split the row count across partitions
//! when finer per-partition statistics aren't available.

use crate::storage::{DataTypeKind, Database, TableSchema};

/// Snapshot of the statistics columns shared by `SHOW TABLE STATUS`,
/// `information_schema.tables`, and `information_schema.partitions`.
///
/// All sizes are byte counts. `auto_increment` is `None` for tables
/// without an `AUTO_INCREMENT` column.
#[derive(Debug, Clone)]
pub(crate) struct TableStatsView {
    pub rows: u64,
    pub avg_row_length: u64,
    pub data_length: u64,
    /// MySQL semantics: `MAX_DATA_LENGTH` only has meaning for the
    /// MyISAM family. mydb's heaps have no preconfigured upper bound,
    /// so we report `0` (which is what InnoDB does too).
    pub max_data_length: u64,
    pub index_length: u64,
    /// Free bytes the storage engine knows are reclaimable. Filled from
    /// the `.lob` overflow free-list page count (reclaimable by
    /// `OPTIMIZE TABLE` / tail-shrink). Heap/index free space is not
    /// tracked per-table yet, so this reflects LOB free pages only.
    pub data_free: u64,
    pub auto_increment: Option<u64>,
}

impl TableStatsView {
    /// Statistics for the whole table (sum across all partitions when
    /// the table is partitioned).
    pub(crate) fn for_table(db: &Database, table: &TableSchema) -> Self {
        let rows = effective_row_count(db, table);
        let phys = sum_physical_allocation(db, table, &table.storage_ids());
        Self::from_rows(db, table, rows, phys)
    }

    /// Statistics for one physical partition leaf. Prefer persistent
    /// ANALYZE results; pre-ANALYZE tables retain the page-proportional
    /// fallback so metadata reads never trigger a heap scan.
    ///
    /// `partition_storage_id` is the partition's own storage id, used
    /// to fetch its physical page count independently of the parent
    /// table's id (partitions live in their own heap/index files).
    pub(crate) fn for_partition(
        db: &Database,
        table: &TableSchema,
        _partition_count: usize,
        _partition_index: usize,
        partition_storage_id: u64,
    ) -> Self {
        let total_rows = effective_row_count(db, table);
        let mut phys =
            sum_physical_allocation(db, table, std::slice::from_ref(&partition_storage_id));
        if phys.heap_pages == 0 {
            if let Some(stats) = table.stats.partition_stats.get(&partition_storage_id) {
                phys.heap_pages = stats.heap_page_count;
            }
        }
        // G1: apportion the table's total row count across partitions
        // *by physical page count* rather than uniformly. Per-partition
        // rows are still an estimate (mydb tracks stats at the table
        // level only), but scaling by real heap pages tracks skewed
        // RANGE/LIST distributions instead of the "always N/K" fake the
        // uniform split produced. Falls back to uniform split when we
        // have no page-count signal yet (fresh table / memory
        // durability / cold handles).
        let rows = if let Some(stats) = table.stats.partition_stats.get(&partition_storage_id) {
            stats.row_count
        } else if total_rows == 0 {
            0
        } else {
            // Apportion only the population not covered by persistent leaf
            // statistics. This keeps ADD PARTITION estimates consistent with
            // the table total without scanning heaps or assigning old rows to
            // the new leaf. Cumulative rounding preserves the total exactly.
            let all_sids = table.storage_ids();
            let unknown: Vec<_> = all_sids
                .iter()
                .copied()
                .filter(|sid| !table.stats.partition_stats.contains_key(sid))
                .collect();
            let known_rows = all_sids
                .iter()
                .filter_map(|sid| table.stats.partition_stats.get(sid))
                .fold(0u64, |sum, stats| sum.saturating_add(stats.row_count));
            let remaining = total_rows.saturating_sub(known_rows);
            let pages: Vec<_> = unknown.iter().map(|sid| db.heap_page_count(*sid)).collect();
            let total_pages = pages.iter().copied().fold(0u64, u64::saturating_add);
            let position = unknown
                .iter()
                .position(|sid| *sid == partition_storage_id)
                .unwrap_or(0);
            if total_pages > 0 {
                let before = pages[..position]
                    .iter()
                    .copied()
                    .fold(0u64, u64::saturating_add);
                let after = before.saturating_add(pages[position]);
                ((remaining as u128 * after as u128 / total_pages as u128)
                    - (remaining as u128 * before as u128 / total_pages as u128))
                    as u64
            } else {
                let count = unknown.len().max(1) as u64;
                remaining / count + u64::from((position as u64) < remaining % count)
            }
        };
        Self::from_rows(db, table, rows, phys)
    }

    fn from_rows(db: &Database, table: &TableSchema, rows: u64, phys: PhysicalAllocation) -> Self {
        let PhysicalAllocation {
            heap_pages,
            lob_pages,
            lob_free_pages,
            index_bytes,
            index_fallback_bytes_per_row,
        } = phys;
        let page_size = crate::storage::PAGE_SIZE as u64;
        // `heap_pages` is physical for both warm handles and cold durable
        // files. It is zero only when the storage backend has no file to
        // inspect (notably memory durability), where catalog width is the
        // best available estimate. Off-page LOB bytes (`.lob` overflow
        // pages) are always added on
        // top: they are row payload owned by the heap table, not index
        // allocation. A table dominated by large TEXT/BLOB columns must not
        // report only its tiny inline heap footprint.
        let heap_length = if heap_pages > 0 {
            heap_pages.saturating_mul(page_size)
        } else {
            let row_width = estimate_table_row_width(table);
            rows.saturating_mul(row_width)
        };
        let lob_length = lob_pages.saturating_mul(page_size);
        // mydb is a heap-table engine: DATA_LENGTH is the heap plus off-page
        // LOB allocation. PRIMARY is a separate B-tree and therefore belongs
        // to INDEX_LENGTH together with every secondary index.
        let data_length = heap_length.saturating_add(lob_length);
        let avg_row_length = if rows == 0 { 0 } else { data_length / rows };
        let index_length =
            index_bytes.saturating_add(rows.saturating_mul(index_fallback_bytes_per_row));
        // DATA_FREE reflects reclaimable `.lob` free-list pages (heap/index
        // free space isn't tracked per-table yet) — the signal a client
        // uses to decide whether OPTIMIZE TABLE would reclaim space.
        let data_free = lob_free_pages.saturating_mul(page_size);
        let auto_increment = db.auto_increment_next_value(table).ok().flatten();
        Self {
            rows,
            avg_row_length,
            data_length,
            max_data_length: 0,
            index_length,
            data_free,
            auto_increment,
        }
    }
}

/// Physical allocation summed across a table's storage ids (partitions each
/// carry their own heap / index / `.lob` files).
#[derive(Debug, Clone, Copy, Default)]
struct PhysicalAllocation {
    heap_pages: u64,
    /// Physical `.lob` overflow pages (includes the meta page); 0 when
    /// the table has never externalized a value (cold overflow handle).
    lob_pages: u64,
    /// `.lob` free-list pages (reclaimable) — feeds `DATA_FREE`.
    lob_free_pages: u64,
    /// All index files, including PRIMARY: mydb is a heap-table engine.
    index_bytes: u64,
    /// Per-row estimates used only when a physical index does not exist in
    /// the selected durability backend (notably memory durability).
    index_fallback_bytes_per_row: u64,
}

/// Sum heap, index, and `.lob` overflow allocation across the supplied
/// storage ids.
///
/// Warm files are read from their in-memory counters; durable cold files use
/// their on-disk lengths. Consequently these values do not depend on buffer
/// cache residency. A genuinely absent index receives a per-row estimate.
fn sum_physical_allocation(
    db: &Database,
    table: &TableSchema,
    storage_ids: &[u64],
) -> PhysicalAllocation {
    let mut phys = PhysicalAllocation::default();
    for storage_id in storage_ids {
        phys.heap_pages = phys
            .heap_pages
            .saturating_add(db.heap_page_count(*storage_id));
        let (lob_pages, lob_free_pages) = db.lob_page_count(*storage_id);
        phys.lob_pages = phys.lob_pages.saturating_add(lob_pages);
        phys.lob_free_pages = phys.lob_free_pages.saturating_add(lob_free_pages);
    }
    for index in table.iter_public_indexes() {
        let bytes = storage_ids
            .iter()
            .map(|storage_id| db.index_storage_bytes(table, *storage_id, index))
            .fold(0u64, u64::saturating_add);
        if bytes > 0 {
            phys.index_bytes = phys.index_bytes.saturating_add(bytes);
            continue;
        }
        let bytes_per_row =
            (table.index_key_len(index, index.columns.len()) as u64).saturating_add(8);
        phys.index_fallback_bytes_per_row = phys
            .index_fallback_bytes_per_row
            .saturating_add(bytes_per_row);
    }
    phys
}

/// Compute the `CREATE_OPTIONS` string MySQL reports for a table.
///
/// MySQL fills this column with a space-separated list of declared
/// options (`partitioned`, `stats_persistent=1`, ...). Clients (and
/// `mysqldump`) read it to round-trip catalog state. We only emit
/// `partitioned` for now; other options can be plugged in here
/// without changing call sites.
pub(crate) fn table_create_options(table: &TableSchema) -> String {
    let mut opts: Vec<String> = Vec::new();
    for (key, value) in &table.mysql_table_options {
        // MySQL lowercases the key in CREATE_OPTIONS (`max_rows=100`,
        // `row_format=DYNAMIC`); values keep their canonical form.
        opts.push(format!("{}={}", key.to_ascii_lowercase(), value));
    }
    if table.partitioning.is_some() {
        opts.push("partitioned".to_string());
    }
    opts.join(" ")
}

/// `ROW_FORMAT` value MySQL reports in `information_schema.tables` /
/// `SHOW TABLE STATUS`: the declared option in title case, defaulting
/// to `Dynamic` (the InnoDB default) when nothing was declared.
pub(crate) fn table_row_format(table: &TableSchema) -> &'static str {
    match table
        .mysql_table_options
        .iter()
        .find(|(k, _)| k == "ROW_FORMAT")
        .map(|(_, v)| v.as_str())
    {
        Some("FIXED") => "Fixed",
        Some("COMPRESSED") => "Compressed",
        Some("REDUNDANT") => "Redundant",
        Some("COMPACT") => "Compact",
        _ => "Dynamic",
    }
}

/// Row-count estimate that matches the MySQL "approximate" semantics
/// of `information_schema.TABLES.TABLE_ROWS` — fast, never blocks on
/// a heap scan, and always reflects in-flight DML.
///
/// Two pieces:
///  1. `TableStats.row_count` — committed snapshot from the last
///     ANALYZE TABLE (or the running-count maintained on first scan).
///  2. `Database::table_live_row_delta` — committed INSERT minus DELETE
///     rows since ANALYZE. UPDATE is deliberately zero. The separate
///     unsigned modification counter is only an ANALYZE staleness signal
///     and must never be interpreted as table growth.
///
/// We saturate at every step so a buggy or stale counter never wraps
/// the returned value.
fn effective_row_count(db: &Database, table: &TableSchema) -> u64 {
    (table.stats.row_count as i128 + db.table_live_row_delta(table.id) as i128)
        .clamp(0, u64::MAX as i128) as u64
}

/// Estimated per-row byte width.
///
/// 3-byte heap row header + null bitmap + sum of per-column widths.
/// For variable-length / off-page types we use a conservative declared
/// upper bound (e.g. `VarChar(64)` for an unbounded varchar) rather
/// than `0`, because clients use `AVG_ROW_LENGTH` to estimate result
/// set sizes. BLOB / TEXT / JSON / GEOMETRY default to 0 since their
/// bytes live in the LOB file, not the heap row.
pub(crate) fn estimate_table_row_width(table: &TableSchema) -> u64 {
    let visible_columns = table.iter_visible_columns().count() as u64;
    let value_width = table
        .iter_visible_columns()
        .map(|(_, column)| estimate_column_width(&column.data_type))
        .sum::<u64>();
    let null_bitmap = visible_columns.saturating_add(7) / 8;
    3u64.saturating_add(null_bitmap).saturating_add(value_width)
}

pub(crate) fn estimate_column_width(data_type: &DataTypeKind) -> u64 {
    match data_type {
        DataTypeKind::TinyInt | DataTypeKind::Bool | DataTypeKind::Year => 1,
        DataTypeKind::SmallInt => 2,
        DataTypeKind::MediumInt => 3,
        DataTypeKind::Int | DataTypeKind::Float { .. } | DataTypeKind::Date => 4,
        DataTypeKind::BigInt
        | DataTypeKind::Double { .. }
        | DataTypeKind::Time { .. }
        | DataTypeKind::DateTime { .. }
        | DataTypeKind::Timestamp { .. } => 8,
        DataTypeKind::Decimal { precision, .. } => match precision {
            Some(p) if *p <= 9 => 4,
            Some(p) if *p <= 18 => 8,
            _ => 16,
        },
        DataTypeKind::Char { len } => len.unwrap_or(1).min(255) as u64 * 4,
        DataTypeKind::VarChar { len } => len.unwrap_or(64).min(255) as u64,
        DataTypeKind::Binary { len } => len.unwrap_or(1).min(255) as u64,
        DataTypeKind::VarBinary { len } => len.unwrap_or(64).min(255) as u64,
        DataTypeKind::Text { .. }
        | DataTypeKind::Blob { .. }
        | DataTypeKind::Json
        | DataTypeKind::Geometry { .. } => 0,
        DataTypeKind::Vector { dim } => dim.map(|dim| dim as u64 * 4).unwrap_or(0),
        DataTypeKind::Enum { values } => values.iter().map(|v| v.len()).max().unwrap_or(0) as u64,
        DataTypeKind::Set { values } => values.iter().map(|v| v.len() + 1).sum::<usize>() as u64,
        DataTypeKind::Bit { len } => len.unwrap_or(1).div_ceil(8) as u64,
    }
}

/// Approximate per-index `CARDINALITY` when `ANALYZE TABLE` has never
/// been run on this table — used by `information_schema.statistics`
/// so newly-created tables don't report `0` for every index column
/// and mislead query-planner diagnostics on the client side.
///
/// MySQL conventions:
///   * PRIMARY / UNIQUE index prefix that fully identifies a row
///     → `CARDINALITY = row_count` (every key value is distinct).
///   * Any other index → conservative `max(1, rows / 100)`,
///     consistent with InnoDB's "no stats yet" placeholder.
///
/// Returns `None` for an empty table — `0` is the right answer there
/// and callers can pass it through.
pub(crate) fn fallback_index_cardinality(
    rows: u64,
    unique: bool,
    seq_in_index: usize,
    columns_in_index: usize,
) -> u64 {
    if rows == 0 {
        return 0;
    }
    let final_prefix = seq_in_index == columns_in_index;
    if unique && final_prefix {
        rows
    } else {
        (rows / 100).max(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fallback_unique_full_prefix_returns_row_count() {
        // A PRIMARY KEY or UNIQUE index whose prefix length equals the
        // declared index width — each key value identifies one row.
        assert_eq!(fallback_index_cardinality(1000, true, 1, 1), 1000);
        assert_eq!(fallback_index_cardinality(1000, true, 2, 2), 1000);
    }

    #[test]
    fn fallback_unique_partial_prefix_falls_to_low_estimate() {
        // The first column of a 2-column UNIQUE isn't itself unique,
        // so we drop to the conservative non-unique estimate.
        let est = fallback_index_cardinality(10_000, true, 1, 2);
        assert_eq!(est, 100); // 10_000 / 100
    }

    #[test]
    fn fallback_non_unique_floor_is_one() {
        // Even on a tiny table the placeholder cardinality is at
        // least 1 — `0` would be indistinguishable from "no data".
        assert_eq!(fallback_index_cardinality(5, false, 1, 1), 1);
    }

    #[test]
    fn fallback_empty_table_returns_zero() {
        // An empty table genuinely has cardinality 0; we don't fake it.
        assert_eq!(fallback_index_cardinality(0, true, 1, 1), 0);
        assert_eq!(fallback_index_cardinality(0, false, 1, 1), 0);
    }
}
