#[cfg(test)]
use std::collections::HashSet;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};

use serde::{Deserialize, Serialize};

use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};
use crate::engine::executor::memory_footprint::AllocatedBytes;
use crate::types::Value;

use super::naming::current_unix_ms;
use super::schema::{IndexKey, KeyPart};
use super::value::{compare_stat_values, value_to_string};

use std::sync::OnceLock;
use std::{hash::BuildHasherDefault, hash::Hasher};

#[derive(Default)]
struct PrehashedU64Hasher(u64);

impl Hasher for PrehashedU64Hasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let mut hash = 0xcbf2_9ce4_8422_2325u64;
        for byte in bytes {
            hash = (hash ^ u64::from(*byte)).wrapping_mul(0x1000_0000_01b3);
        }
        self.0 = hash;
    }

    #[inline]
    fn write_u64(&mut self, value: u64) {
        self.0 = value;
    }
}

type PrehashedU64Map<V> = hashbrown::HashMap<u64, V, BuildHasherDefault<PrehashedU64Hasher>>;

/// Default per-column TOP-K size. Overridable via `MYDB_ANALYZE_TOPK`.
pub(crate) const TOP_VALUE_LIMIT_DEFAULT: usize = 128;
pub(crate) const ANALYZE_EXACT_ROW_LIMIT: u64 = 4096;
/// Distinct sketch size. NOT a GUC: the sample-merge invariant (HLL-style
/// sketch union across parallel ANALYZE workers) requires every worker to
/// agree on this width, so it stays a fixed compile-time constant.
const ANALYZE_DISTINCT_SKETCH_SIZE: usize = 2048;
/// Exact direct-mapped cache for hashes already observed on the current heap
/// page. Low-NDV columns frequently alternate among a handful of values, so a
/// single `last_hash_page` entry still sent almost every cell through the KMV
/// hash table. A cache hit is an exact `(hash, page)` match and therefore
/// cannot change KMV membership or page-incidence semantics.
const ANALYZE_DISTINCT_RECENT_PAGE_CACHE_SIZE: usize = 32;
const HISTOGRAM_BUCKET_COUNT_DEFAULT: usize = 32;
const ANALYZE_RANDOM_SAMPLE_THRESHOLD_BYTES_DEFAULT: u64 = 64 * 1024 * 1024;
// Tables at or below 64 MiB are analyzed in full. Larger tables sample 512
// heap blocks (8 MiB with the current 16 KiB page size). Zone Map rebuilding
// remains a separate synchronous full-heap pass, so reducing the statistics
// sample changes neither visibility coverage nor data-skipping correctness.
// Both remain
// operator-overridable through MYDB_ANALYZE_SAMPLE_{THRESHOLD_MB,PAGES}.
const ANALYZE_RANDOM_SAMPLE_BLOCKS_DEFAULT: usize = 512;
/// Approximate TOP-K is substantially more expensive than the KMV and
/// min/max summaries because it probes and occasionally maintains a hash map
/// for every non-NULL cell. Keep a deterministic warm-up, then sample one in
/// four rows. Exact small-table statistics continue to use
/// `exact_value_counts` and never depend on this sketch.
const ANALYZE_TOP_CANDIDATE_WARMUP_ROWS: u64 = 1024;
const ANALYZE_TOP_CANDIDATE_SAMPLE_MASK: u64 = 3;
const ANALYZE_HISTOGRAM_SAMPLE_VALUES: usize = 1024;
const ANALYZE_HISTOGRAM_MAX_VALUE_BYTES: u64 = 256;

fn env_usize(key: &str, default: usize, min: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .map(|v| v.max(min))
        .unwrap_or(default)
}

/// Per-column TOP-K size (`MYDB_ANALYZE_TOPK`, >= 1).
pub(crate) fn top_value_limit() -> usize {
    static V: OnceLock<usize> = OnceLock::new();
    *V.get_or_init(|| env_usize("MYDB_ANALYZE_TOPK", TOP_VALUE_LIMIT_DEFAULT, 1))
}

/// Equi-depth histogram bucket count (`MYDB_HISTOGRAM_BUCKETS`, >= 1).
fn histogram_bucket_count() -> usize {
    static V: OnceLock<usize> = OnceLock::new();
    *V.get_or_init(|| env_usize("MYDB_HISTOGRAM_BUCKETS", HISTOGRAM_BUCKET_COUNT_DEFAULT, 1))
}

/// Random-sampling page budget (`MYDB_ANALYZE_SAMPLE_PAGES`, >= 1).
fn analyze_random_sample_blocks() -> usize {
    static V: OnceLock<usize> = OnceLock::new();
    *V.get_or_init(|| {
        env_usize(
            "MYDB_ANALYZE_SAMPLE_PAGES",
            ANALYZE_RANDOM_SAMPLE_BLOCKS_DEFAULT,
            1,
        )
    })
}

/// Byte threshold above which ANALYZE switches to random sampling
/// (`MYDB_ANALYZE_SAMPLE_THRESHOLD_MB`).
fn analyze_random_sample_threshold_bytes() -> u64 {
    static V: OnceLock<u64> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("MYDB_ANALYZE_SAMPLE_THRESHOLD_MB")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .map(|mb| mb.saturating_mul(1024 * 1024))
            .unwrap_or(ANALYZE_RANDOM_SAMPLE_THRESHOLD_BYTES_DEFAULT)
    })
}

/// Derived: TOP-K candidate pool (4× the final TOP-K).
fn analyze_top_value_candidates() -> usize {
    top_value_limit() * 4
}

/// Derived: minimum distinct values before a histogram is built (2× TOP-K).
fn histogram_min_distinct() -> usize {
    // Raising the persisted heavy-hitter budget must not suppress histograms
    // for ordinary medium-NDV columns. Keep the historical 32-value ceiling;
    // a deliberately smaller MYDB_ANALYZE_TOPK may still lower the threshold.
    top_value_limit().min(16) * 2
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStats {
    /// SQL-requested histogram snapshots, independent of automatic CBO statistics.
    #[serde(default)]
    pub explicit_histograms: BTreeMap<String, String>,
    pub row_count: u64,
    /// Actual heap page count observed by the last ANALYZE. 0 = unknown
    /// (pre-existing stats); consumers fall back to the width-based
    /// `estimated_heap_pages`, which can be ~1.7× off for wide rows —
    /// enough to mis-trigger buffer-pool-pressure costing at the
    /// cache-size boundary.
    #[serde(default)]
    pub heap_page_count: u64,
    /// Fraction of heap pages sampled by ANALYZE. `None` denotes a catalog
    /// written before this field existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample_fraction: Option<f64>,
    /// Coarse confidence in `[0,1]`, derived from exact/full-vs-sampled mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f64>,
    pub columns: BTreeMap<String, ColumnStats>,
    pub indexes: BTreeMap<String, IndexStats>,
    /// Persistent statistics for every physical partition leaf, keyed by
    /// storage id. Table-level statistics remain the deterministic merged
    /// view consumed by callers that do not carry a pruning set.
    #[serde(default)]
    pub partition_stats: BTreeMap<u64, PartitionStats>,
    /// Compact conditional domain summaries used by the CBO for star-join
    /// cardinality. Each entry describes the domain of a key column after
    /// equality filters on a low-NDV index prefix have been applied, e.g.
    /// `(d_year = 2000) -> d_date_sk in [..]`.
    #[serde(default)]
    pub conditional_key_domains: Vec<ConditionalKeyDomainStats>,
    /// Phase 3 (full): joint distinct-value counts for correlated column
    /// groups (composite indexes and multi-column foreign keys), collected
    /// at ANALYZE time. Keyed by the canonical (sorted, normalized) column
    /// set so the CBO can look a group up regardless of the order the
    /// columns appear in a predicate or join. Lets AND predicates and
    /// equi-joins on correlated columns use a real joint NDV instead of
    /// multiplying per-column selectivities under the (often wrong)
    /// independence assumption. `serde(default)` keeps older catalogs
    /// loading transparently — they carry no groups until the next ANALYZE.
    #[serde(default)]
    pub multi_column_stats: BTreeMap<String, MultiColumnStats>,
    pub modified_rows_since_analyze: u64,
    pub analyzed_at_unix_ms: Option<u64>,
    /// Sidecar baselines folded by this ANALYZE. Physical replicas replay
    /// the same subtraction when the new DD statistics row arrives.
    #[serde(default)]
    pub last_analyze_live_baseline: i64,
    #[serde(default)]
    pub last_analyze_modification_baseline: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionStats {
    pub row_count: u64,
    pub heap_page_count: u64,
    pub columns: BTreeMap<String, ColumnStats>,
    pub indexes: BTreeMap<String, IndexStats>,
    pub modified_rows_since_analyze: u64,
    pub analyzed_at_unix_ms: Option<u64>,
    #[serde(default)]
    pub check_ok: Option<bool>,
}

impl PartitionStats {
    pub(crate) fn empty() -> Self {
        Self {
            row_count: 0,
            heap_page_count: 0,
            columns: BTreeMap::new(),
            indexes: BTreeMap::new(),
            modified_rows_since_analyze: 0,
            analyzed_at_unix_ms: None,
            check_ok: None,
        }
    }
}

/// Whether persisted frequencies count the population, the page sample, or
/// only the survivors of an approximate candidate sketch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum FrequencySource {
    Exact,
    Sample,
    #[default]
    LowerBound,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FrequencyStats {
    pub source: FrequencySource,
    /// Rows observed before page-sample expansion (including NULLs).
    pub sample_rows: u64,
    /// Every non-NULL value observed by the collector is retained.
    pub complete: bool,
    /// Maximum counter deficit per retained physical value after expansion.
    /// This bounds sketch loss, not the statistical error of page sampling.
    /// None means the collector did not record an error bound.
    pub count_error: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStats {
    pub null_count: u64,
    pub distinct_count: u64,
    pub min: Option<Value>,
    pub max: Option<Value>,
    pub top_values: Vec<ValueFrequency>,
    #[serde(default)]
    pub frequency: FrequencyStats,
    /// Average encoded payload width of non-NULL sampled values. Optional so
    /// catalogs written before CBO v2 remain readable.
    #[serde(default)]
    pub average_width_bytes: Option<f64>,
    /// Phase B #B6: equi-depth histogram covering values that aren't
    /// captured exactly in `top_values`. `serde(default)` lets older
    /// catalog files (no histogram field) deserialize transparently
    /// — they simply have an empty histogram until the next ANALYZE.
    #[serde(default)]
    pub histogram: Vec<HistogramBucket>,
}

/// Equi-depth histogram bucket. Each bucket spans `(prev_upper, upper]`
/// (or `[min, upper]` for the first), and covers approximately
/// `total_rows / N` rows. Cardinality estimation walks the buckets and
/// linearly interpolates within the partial buckets at the range
/// boundaries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistogramBucket {
    pub upper: Value,
    pub cumulative_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValueFrequency {
    pub value: Value,
    pub count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexStats {
    pub prefix_distinct_counts: Vec<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leaf_page_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub internal_page_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tree_height: Option<u64>,
    /// |Pearson r| between index key order and heap page order, sampled
    /// at ANALYZE time. 1.0 = walking the index visits heap pages
    /// sequentially (clustered-like); 0.0 = every fetch is a random
    /// page. `None` on stats persisted before this field existed (or
    /// when the index was empty at ANALYZE time) — costing falls back
    /// to the legacy primary=1.0 / secondary=0.5 defaults.
    #[serde(default)]
    pub correlation: Option<f64>,
    /// Optional FULLTEXT corpus statistics. Older catalogs deserialize with
    /// `None`; costing then uses conservative parser-aware defaults.
    #[serde(default)]
    pub fulltext: Option<FulltextStats>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FulltextStats {
    pub doc_count: u64,
    pub avg_token_count: f64,
    /// Most frequent tokenizer terms and their estimated document
    /// frequencies, sorted descending by DF.
    pub token_df_top: Vec<TokenFrequency>,
    /// Equi-depth upper bounds over retained token document frequencies.
    pub df_histogram: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenFrequency {
    pub token: String,
    pub document_count: u64,
}

/// Joint distinct-value count for a correlated column group. `columns`
/// preserves the group's defined (index / FK) column order for display;
/// lookups go through the canonical sorted key (see
/// [`multi_column_group_key`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiColumnStats {
    pub columns: Vec<String>,
    pub distinct_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConditionalKeyDomainStats {
    pub filter_columns: Vec<String>,
    pub key_columns: Vec<String>,
    pub entries: Vec<ConditionalKeyDomainEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConditionalKeyDomainEntry {
    pub filter_values: Vec<Value>,
    pub row_count: u64,
    pub key_distinct_count: u64,
    pub key_min: Option<Value>,
    pub key_max: Option<Value>,
}

impl TableStats {
    pub(crate) fn empty() -> Self {
        Self {
            explicit_histograms: BTreeMap::new(),
            row_count: 0,
            heap_page_count: 0,
            sample_fraction: None,
            confidence: None,
            columns: BTreeMap::new(),
            indexes: BTreeMap::new(),
            partition_stats: BTreeMap::new(),
            conditional_key_domains: Vec::new(),
            multi_column_stats: BTreeMap::new(),
            modified_rows_since_analyze: 0,
            analyzed_at_unix_ms: Some(current_unix_ms()),
            last_analyze_live_baseline: 0,
            last_analyze_modification_baseline: 0,
        }
    }

    #[cfg(test)]
    /// Joint NDV for the exact set of `columns` (order-independent), if a
    /// multi-column group covering precisely that set was collected at
    /// ANALYZE time. Returns `None` when no such group exists or its count
    /// is zero, so callers fall back to their independence estimate.
    pub fn multi_column_ndv(&self, columns: &[String]) -> Option<u64> {
        if columns.len() < 2 {
            return None;
        }
        let key = multi_column_group_key(columns);
        self.multi_column_stats
            .get(&key)
            .map(|s| s.distinct_count)
            .filter(|&n| n > 0)
    }
}

/// Canonical key for a multi-column group: normalized column names,
/// de-duplicated and sorted, joined by a control character that cannot
/// appear in an identifier. Order-independent so `(a, b)` and `(b, a)`
/// hash to the same group.
pub fn multi_column_group_key(columns: &[String]) -> String {
    let mut names: Vec<String> = columns
        .iter()
        .map(|c| c.trim().to_ascii_lowercase())
        .collect();
    names.sort();
    names.dedup();
    names.join("\u{1}")
}

pub(crate) fn analyze_sample_pages(
    table_id: u64,
    page_count: u64,
    page_size: usize,
) -> Option<Vec<u64>> {
    let sample_blocks = analyze_random_sample_blocks();
    if !analyze_should_sample(
        page_count,
        page_size,
        analyze_random_sample_threshold_bytes(),
        sample_blocks,
    ) {
        return None;
    }

    let mut sample = BTreeSet::new();
    let mut seed = table_id
        ^ page_count.rotate_left(17)
        ^ (sample_blocks as u64).rotate_left(31)
        ^ 0x9e37_79b9_7f4a_7c15;
    while sample.len() < sample_blocks {
        seed = splitmix64(seed);
        sample.insert(seed % page_count);
    }
    Some(sample.into_iter().collect())
}

fn analyze_should_sample(
    page_count: u64,
    page_size: usize,
    threshold_bytes: u64,
    sample_blocks: usize,
) -> bool {
    page_count.saturating_mul(page_size as u64) > threshold_bytes
        && page_count > sample_blocks as u64
}

#[cfg(test)]
mod analyze_policy_tests {
    use super::*;

    #[test]
    fn full_scans_tables_up_to_sixty_four_mibibytes() {
        let pages_at_threshold = (64 * 1024 * 1024 / 16_384) as u64;
        assert!(!analyze_should_sample(
            pages_at_threshold - 1,
            16_384,
            ANALYZE_RANDOM_SAMPLE_THRESHOLD_BYTES_DEFAULT,
            ANALYZE_RANDOM_SAMPLE_BLOCKS_DEFAULT,
        ));
        assert!(!analyze_should_sample(
            pages_at_threshold,
            16_384,
            ANALYZE_RANDOM_SAMPLE_THRESHOLD_BYTES_DEFAULT,
            ANALYZE_RANDOM_SAMPLE_BLOCKS_DEFAULT,
        ));
    }

    #[test]
    fn samples_512_blocks_above_sixty_four_mibibytes() {
        let pages = (64 * 1024 * 1024 / 16_384) as u64 + 1;
        assert!(analyze_should_sample(
            pages,
            16_384,
            ANALYZE_RANDOM_SAMPLE_THRESHOLD_BYTES_DEFAULT,
            ANALYZE_RANDOM_SAMPLE_BLOCKS_DEFAULT,
        ));
        let sampled = analyze_sample_pages(7, pages, 16_384).unwrap();
        assert_eq!(sampled.len(), 512);
    }
}

pub(crate) fn estimate_analyzed_row_count(
    page_count: u64,
    analyzed_pages: &[u64],
    sampled_rows: u64,
) -> u64 {
    if analyzed_pages.is_empty() || analyzed_pages.len() as u64 >= page_count {
        return sampled_rows;
    }
    let estimate =
        (sampled_rows as u128).saturating_mul(page_count as u128) / analyzed_pages.len() as u128;
    estimate.max(sampled_rows as u128).min(u64::MAX as u128) as u64
}

fn scale_sample_count(count: u64, scale: f64, max_count: u64) -> u64 {
    if count == 0 || max_count == 0 {
        return 0;
    }
    if (scale - 1.0).abs() < f64::EPSILON {
        return count.min(max_count);
    }
    ((count as f64 * scale).round() as u64)
        .max(1)
        .min(max_count)
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut mixed = value;
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    mixed ^ (mixed >> 31)
}

pub(crate) struct ColumnStatsBuilder {
    null_count: u64,
    non_null_count: u64,
    total_value_bytes: u64,
    distinct_values: DistinctStatsBuilder<KeyPart>,
    exact_value_counts: BTreeMap<KeyPart, (Value, u64)>,
    /// Bottom-k priority sample. Mergeable across parallel workers and
    /// bounded independently of the table size and value width.
    histogram_samples: BTreeMap<u64, Value>,
    /// Full counts for a bounded small domain, independent of the row limit
    /// and secondary TOP-K sampling. Abandoned on overflow or hash collision.
    complete_counts: Option<PrehashedU64Map<(Value, u64)>>,
    /// Misra-Gries counters are stored relative to this global decrement
    /// baseline. An unseen value with a full candidate set increments the
    /// baseline instead of walking and mutating every candidate. Entries at
    /// or below the baseline are dead and are removed lazily.
    top_candidates: PrehashedU64Map<(Value, u64)>,
    top_candidate_decrements: u64,
    top_candidate_merged_error: u64,
    /// Conservative lower bound for stored candidate counters. It may lag
    /// after a candidate is incremented; that only causes an early cleanup
    /// scan, never changes the resulting counters.
    top_candidate_min_stored: u64,
    /// Non-NULL approximate rows eligible for / actually fed into TOP-K.
    /// These counters make the final frequency scale independent of worker
    /// partitioning and keep the sampling deterministic.
    top_candidate_rows_seen: u64,
    top_candidate_rows_sampled: u64,
    min: Option<Value>,
    max: Option<Value>,
}

pub(crate) struct ConditionalKeyDomainBuilder {
    filter_columns: Vec<String>,
    key_columns: Vec<String>,
    entries: BTreeMap<IndexKey, ConditionalKeyDomainEntryBuilder>,
    overflow: bool,
    unique_key: bool,
    unique_reserved_bytes: usize,
}

struct ConditionalKeyDomainEntryBuilder {
    filter_values: Vec<Value>,
    row_count: u64,
    key_distinct: DistinctStatsBuilder<IndexKey>,
    key_min: Option<Value>,
    key_max: Option<Value>,
    // Includes B-tree nodes, filter/range values and geometric sketch growth.
    // Optional statistics must disappear as a whole if admission fails: a
    // partially populated domain would give the optimizer false selectivity.
    memory: ControlledMemoryCharge,
    payload_bytes: usize,
}

impl ConditionalKeyDomainBuilder {
    pub(crate) const MAX_ENTRIES: usize = 2048;
    // The former minimum reservation for MAX_ENTRIES domains. Uniqueness
    // removes per-domain NDV sketches, so the same byte ceiling can preserve
    // more complete conditional domains without enlarging the memory budget.
    const UNIQUE_DOMAIN_BYTES: usize = Self::MAX_ENTRIES * 2048;
    const UNIQUE_ENTRY_BYTES: usize = 1024;

    pub(crate) fn new(filter_columns: Vec<String>, key_columns: Vec<String>) -> Self {
        Self {
            filter_columns,
            key_columns,
            entries: BTreeMap::new(),
            overflow: false,
            unique_key: false,
            unique_reserved_bytes: 0,
        }
    }

    /// The caller must prove uniqueness of the non-NULL key over its scan.
    /// Then a conditional domain's NDV is its row count, including after
    /// merging disjoint scan workers; no duplicate-detection sketch is needed.
    pub(crate) fn new_unique(filter_columns: Vec<String>, key_columns: Vec<String>) -> Self {
        let mut builder = Self::new(filter_columns, key_columns);
        builder.unique_key = true;
        builder
    }

    fn discard(&mut self) {
        self.entries.clear();
        self.unique_reserved_bytes = 0;
        self.overflow = true;
    }

    pub(crate) fn is_overflowed(&self) -> bool {
        self.overflow
    }

    pub(crate) fn record(
        &mut self,
        filter_values: Vec<Value>,
        key: IndexKey,
        key_value_for_range: Option<Value>,
        precise: bool,
        page_id: u32,
    ) {
        if self.overflow {
            return;
        }
        let filter_key = IndexKey::from(
            filter_values
                .iter()
                .map(KeyPart::from_value)
                .collect::<Vec<_>>(),
        );
        if !self.unique_key
            && !self.entries.contains_key(&filter_key)
            && self.entries.len() >= Self::MAX_ENTRIES
        {
            self.discard();
            return;
        }
        if !self.entries.contains_key(&filter_key) {
            let payload_bytes = filter_values
                .iter()
                .fold(0usize, |bytes, value| {
                    bytes.saturating_add(std::mem::size_of::<Value>() + value.allocated_bytes())
                })
                .saturating_mul(2);
            let bytes = (if self.unique_key {
                Self::UNIQUE_ENTRY_BYTES
            } else {
                2048
            })
            .saturating_add(payload_bytes);
            if self.unique_key
                && self.unique_reserved_bytes.saturating_add(bytes) > Self::UNIQUE_DOMAIN_BYTES
            {
                self.discard();
                return;
            }
            let Some(memory) = ControlledMemoryCharge::try_new_optional(
                ControlledMemoryCategory::StorageScratch,
                bytes,
            ) else {
                self.discard();
                return;
            };
            if self.unique_key {
                self.unique_reserved_bytes += bytes;
            }
            self.entries.insert(
                filter_key.clone(),
                ConditionalKeyDomainEntryBuilder {
                    filter_values,
                    row_count: 0,
                    key_distinct: DistinctStatsBuilder::new(),
                    key_min: None,
                    key_max: None,
                    memory,
                    payload_bytes,
                },
            );
        }
        let entry = self.entries.get_mut(&filter_key).unwrap();
        // 128 bytes per retained hash covers hashbrown's spare buckets,
        // control bytes and BinaryHeap growth, including reallocation overlap.
        let sketch_bytes = if self.unique_key {
            0
        } else {
            (entry.key_distinct.sketch.len() + 1)
                .min(ANALYZE_DISTINCT_SKETCH_SIZE)
                .saturating_mul(128)
        };
        let range_bytes = key_value_for_range
            .as_ref()
            .map_or(0, |v| v.allocated_bytes())
            .saturating_mul(2);
        // Exact keys are optional here too; charge their owned representation
        // before insertion, including B-tree node slack.
        if precise && !self.unique_key {
            entry.payload_bytes = entry.payload_bytes.saturating_add(
                key.0
                    .iter()
                    .map(|part| match part {
                        KeyPart::String(v) | KeyPart::CollatedString { value: v, .. } => {
                            v.capacity()
                        }
                        KeyPart::Bytes(v) => v.capacity(),
                        KeyPart::DecimalWide { value, .. } => (value.bits() as usize).div_ceil(8),
                        _ => 0,
                    })
                    .sum::<usize>()
                    .saturating_add(std::mem::size_of::<IndexKey>() * 2),
            );
        }
        let desired = (if self.unique_key {
            Self::UNIQUE_ENTRY_BYTES
        } else {
            2048usize
        })
        .saturating_add(entry.payload_bytes)
        .saturating_add(sketch_bytes)
        .saturating_add(range_bytes);
        if desired > entry.memory.bytes() {
            let extra = if self.unique_key {
                desired - entry.memory.bytes()
            } else {
                (desired - entry.memory.bytes()).div_ceil(4096) * 4096
            };
            if (self.unique_key
                && self.unique_reserved_bytes.saturating_add(extra) > Self::UNIQUE_DOMAIN_BYTES)
                || !entry.memory.try_grow_optional(extra)
            {
                self.discard();
                return;
            }
            if self.unique_key {
                self.unique_reserved_bytes += extra;
            }
        }
        entry.row_count = entry.row_count.saturating_add(1);
        if !self.unique_key {
            entry.key_distinct.record(&key, precise, page_id);
        }
        if let Some(value) = key_value_for_range {
            if entry.key_min.as_ref().is_none_or(|current| {
                compare_stat_values(&value, current) == std::cmp::Ordering::Less
            }) {
                entry.key_min = Some(value.clone());
            }
            if entry.key_max.as_ref().is_none_or(|current| {
                compare_stat_values(&value, current) == std::cmp::Ordering::Greater
            }) {
                entry.key_max = Some(value);
            }
        }
    }

    pub(crate) fn merge(&mut self, other: Self) {
        if self.overflow || other.overflow || self.unique_key != other.unique_key {
            self.discard();
            return;
        }
        for (filter_key, other_entry) in other.entries {
            if !self.unique_key
                && !self.entries.contains_key(&filter_key)
                && self.entries.len() >= Self::MAX_ENTRIES
            {
                self.discard();
                return;
            }
            match self.entries.entry(filter_key) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    let bytes = other_entry.memory.bytes();
                    if self.unique_key
                        && self.unique_reserved_bytes.saturating_add(bytes)
                            > Self::UNIQUE_DOMAIN_BYTES
                    {
                        self.discard();
                        return;
                    }
                    if self.unique_key {
                        self.unique_reserved_bytes += bytes;
                    }
                    entry.insert(other_entry);
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    let current = entry.get_mut();
                    if self.unique_key {
                        let range_bytes = other_entry
                            .key_min
                            .as_ref()
                            .map_or(0, |v| v.allocated_bytes())
                            .max(
                                other_entry
                                    .key_max
                                    .as_ref()
                                    .map_or(0, |v| v.allocated_bytes()),
                            )
                            .saturating_mul(2);
                        let desired = Self::UNIQUE_ENTRY_BYTES
                            .saturating_add(current.payload_bytes)
                            .saturating_add(range_bytes);
                        let extra = desired.saturating_sub(current.memory.bytes());
                        if self.unique_reserved_bytes.saturating_add(extra)
                            > Self::UNIQUE_DOMAIN_BYTES
                            || !current.memory.try_grow_optional(extra)
                        {
                            self.discard();
                            return;
                        }
                        self.unique_reserved_bytes += extra;
                    } else {
                        if !current.memory.try_grow_optional(other_entry.memory.bytes()) {
                            self.discard();
                            return;
                        }
                        current.payload_bytes = current
                            .payload_bytes
                            .saturating_add(other_entry.payload_bytes);
                        current.key_distinct.merge(other_entry.key_distinct);
                    }
                    current.row_count = current.row_count.saturating_add(other_entry.row_count);
                    if let Some(value) = other_entry.key_min {
                        if current.key_min.as_ref().is_none_or(|candidate| {
                            compare_stat_values(&value, candidate) == std::cmp::Ordering::Less
                        }) {
                            current.key_min = Some(value);
                        }
                    }
                    if let Some(value) = other_entry.key_max {
                        if current.key_max.as_ref().is_none_or(|candidate| {
                            compare_stat_values(&value, candidate) == std::cmp::Ordering::Greater
                        }) {
                            current.key_max = Some(value);
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn finish(
        self,
        precise: bool,
        count_scale: f64,
        row_count: u64,
        page_sample_fraction: f64,
    ) -> Option<ConditionalKeyDomainStats> {
        if self.overflow || self.entries.is_empty() {
            return None;
        }
        let unique_key = self.unique_key;
        let entries = self
            .entries
            .into_values()
            .map(|entry| {
                let scaled_row_count = scale_sample_count(entry.row_count, count_scale, row_count);
                ConditionalKeyDomainEntry {
                    filter_values: entry.filter_values,
                    row_count: scaled_row_count,
                    key_distinct_count: if unique_key {
                        scaled_row_count
                    } else {
                        entry.key_distinct.finish(
                            precise,
                            count_scale,
                            scaled_row_count,
                            page_sample_fraction,
                        )
                    },
                    key_min: entry.key_min,
                    key_max: entry.key_max,
                }
            })
            .filter(|entry| entry.row_count > 0 && entry.key_distinct_count > 0)
            .collect::<Vec<_>>();
        if entries.is_empty() {
            return None;
        }
        Some(ConditionalKeyDomainStats {
            filter_columns: self.filter_columns,
            key_columns: self.key_columns,
            entries,
        })
    }
}

impl ColumnStatsBuilder {
    pub(crate) fn new() -> Self {
        Self {
            null_count: 0,
            non_null_count: 0,
            total_value_bytes: 0,
            distinct_values: DistinctStatsBuilder::new(),
            exact_value_counts: BTreeMap::new(),
            histogram_samples: BTreeMap::new(),
            complete_counts: Some(PrehashedU64Map::default()),
            top_candidates: PrehashedU64Map::with_capacity_and_hasher(
                analyze_top_value_candidates(),
                BuildHasherDefault::default(),
            ),
            top_candidate_decrements: 0,
            top_candidate_merged_error: 0,
            top_candidate_min_stored: u64::MAX,
            top_candidate_rows_seen: 0,
            top_candidate_rows_sampled: 0,
            min: None,
            max: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn record(&mut self, value: &Value, precise: bool, page_id: u32) {
        let key = (!matches!(value, Value::Null)).then(|| KeyPart::from_value(value));
        self.record_with_key(value, key.as_ref(), precise, page_id);
    }

    /// Large-table path. Existing small-domain entries allocate nothing;
    /// only retained extrema, frequencies and candidates own value payloads.
    pub(crate) fn record_approx(&mut self, value: &Value, page_id: u32) {
        if matches!(value, Value::Null) {
            self.null_count += 1;
            return;
        }
        self.non_null_count = self.non_null_count.saturating_add(1);
        self.total_value_bytes = self
            .total_value_bytes
            .saturating_add(estimated_value_width(value));
        let key_hash = stats_hash_value(value);
        if estimated_value_width(value) <= ANALYZE_HISTOGRAM_MAX_VALUE_BYTES {
            self.record_histogram_sample(key_hash, page_id, || value.clone());
        }
        self.distinct_values.record_hash_only(key_hash, page_id);
        self.record_complete_count(key_hash, |stored| stored == value, || value.clone());
        if self.sample_top_candidate() {
            self.record_top_candidate(key_hash, value);
        }
        self.record_bounds(value);
    }

    /// Typed-sink entry used while decoding a heap row directly. The Value
    /// is a stack-only scalar for all fixed-width lanes.
    pub(crate) fn record_approx_typed(&mut self, value: Value, page_id: u32) {
        self.record_approx(&value, page_id);
    }

    /// Borrowed UTF-8 lane for the sink decoder. Only retained extrema or
    /// TOP-K candidates allocate an owned Value.
    pub(crate) fn record_approx_string(&mut self, bytes: &[u8], page_id: u32) {
        self.non_null_count = self.non_null_count.saturating_add(1);
        self.total_value_bytes = self.total_value_bytes.saturating_add(bytes.len() as u64);
        let key_hash = stats_hash_string_bytes(bytes);
        if bytes.len() as u64 <= ANALYZE_HISTOGRAM_MAX_VALUE_BYTES {
            self.record_histogram_sample(key_hash, page_id, || string_value_from_bytes(bytes));
        }
        self.distinct_values.record_hash_only(key_hash, page_id);
        self.record_complete_count(
            key_hash,
            |stored| value_text_bytes(stored) == Some(bytes),
            || string_value_from_bytes(bytes),
        );
        if self.sample_top_candidate() {
            self.record_top_candidate_lazy(key_hash, || string_value_from_bytes(bytes));
        }
        self.record_string_bounds(bytes);
    }

    /// Borrowed binary lane; JSON/geometry/vector callers continue through
    /// `put_value` because their display-order min/max semantics differ.
    pub(crate) fn record_approx_bytes(&mut self, bytes: &[u8], page_id: u32) {
        self.non_null_count = self.non_null_count.saturating_add(1);
        self.total_value_bytes = self.total_value_bytes.saturating_add(bytes.len() as u64);
        let key_hash = stats_hash_bytes(bytes);
        if bytes.len() as u64 <= ANALYZE_HISTOGRAM_MAX_VALUE_BYTES {
            self.record_histogram_sample(key_hash, page_id, || Value::Bytes(bytes.to_vec()));
        }
        self.distinct_values.record_hash_only(key_hash, page_id);
        self.record_complete_count(
            key_hash,
            |stored| matches!(stored, Value::Bytes(s) if s.as_slice() == bytes),
            || Value::Bytes(bytes.to_vec()),
        );
        if self.sample_top_candidate() {
            self.record_top_candidate_lazy(key_hash, || Value::Bytes(bytes.to_vec()));
        }
        self.record_bytes_bounds(bytes);
    }

    pub(crate) fn record_null(&mut self) {
        self.null_count = self.null_count.saturating_add(1);
    }

    /// Record a column value using a canonical key already prepared by the
    /// ANALYZE row layout. This lets column, index-prefix and multi-column
    /// consumers share one Value -> KeyPart conversion.
    pub(crate) fn record_with_key(
        &mut self,
        value: &Value,
        key: Option<&KeyPart>,
        precise: bool,
        page_id: u32,
    ) {
        if matches!(value, Value::Null) {
            self.null_count += 1;
            return;
        }
        let key = key.expect("non-null analyze value must carry a key");
        self.non_null_count = self.non_null_count.saturating_add(1);
        self.total_value_bytes = self
            .total_value_bytes
            .saturating_add(estimated_value_width(value));
        let key_hash = stats_hash_key_part(key);
        if !precise && estimated_value_width(value) <= ANALYZE_HISTOGRAM_MAX_VALUE_BYTES {
            self.record_histogram_sample(key_hash, page_id, || value.clone());
        }
        self.distinct_values
            .record_prehashed(key, key_hash, precise, page_id);
        self.record_complete_count(key_hash, |stored| stored == value, || value.clone());
        if precise {
            self.exact_value_counts
                .entry(key.clone())
                .and_modify(|(_, count)| *count += 1)
                .or_insert_with(|| (value.clone(), 1));
        }
        if !precise && self.sample_top_candidate() {
            self.record_top_candidate(key_hash, value);
        }
        self.record_bounds(value);
    }

    #[inline]
    fn record_histogram_sample(
        &mut self,
        hash: u64,
        page_id: u32,
        make_value: impl FnOnce() -> Value,
    ) {
        let priority =
            splitmix64(hash ^ splitmix64(self.non_null_count ^ ((page_id as u64) << 32)));
        if self.histogram_samples.len() < ANALYZE_HISTOGRAM_SAMPLE_VALUES
            || self
                .histogram_samples
                .last_key_value()
                .is_some_and(|(max, _)| priority < *max)
        {
            self.histogram_samples.insert(priority, make_value());
            if self.histogram_samples.len() > ANALYZE_HISTOGRAM_SAMPLE_VALUES {
                self.histogram_samples.pop_last();
            }
        }
    }

    #[inline]
    fn record_bounds(&mut self, value: &Value) {
        if self.min.as_ref().is_none_or(|current| {
            compare_stat_values_hot(value, current) == std::cmp::Ordering::Less
        }) {
            self.min = Some(value.clone());
        }
        if self.max.as_ref().is_none_or(|current| {
            compare_stat_values_hot(value, current) == std::cmp::Ordering::Greater
        }) {
            self.max = Some(value.clone());
        }
    }

    fn record_string_bounds(&mut self, bytes: &[u8]) {
        let less = self.min.as_ref().is_none_or(|current| {
            value_text_bytes(current)
                .map(|current| bytes < current)
                .unwrap_or_else(|| {
                    compare_stat_values(&string_value_from_bytes(bytes), current)
                        == std::cmp::Ordering::Less
                })
        });
        if less {
            self.min = Some(string_value_from_bytes(bytes));
        }
        let greater = self.max.as_ref().is_none_or(|current| {
            value_text_bytes(current)
                .map(|current| bytes > current)
                .unwrap_or_else(|| {
                    compare_stat_values(&string_value_from_bytes(bytes), current)
                        == std::cmp::Ordering::Greater
                })
        });
        if greater {
            self.max = Some(string_value_from_bytes(bytes));
        }
    }

    fn record_bytes_bounds(&mut self, bytes: &[u8]) {
        let less = self.min.as_ref().is_none_or(|current| match current {
            Value::Bytes(current) => bytes < current.as_slice(),
            _ => {
                compare_stat_values(&Value::Bytes(bytes.to_vec()), current)
                    == std::cmp::Ordering::Less
            }
        });
        if less {
            self.min = Some(Value::Bytes(bytes.to_vec()));
        }
        let greater = self.max.as_ref().is_none_or(|current| match current {
            Value::Bytes(current) => bytes > current.as_slice(),
            _ => {
                compare_stat_values(&Value::Bytes(bytes.to_vec()), current)
                    == std::cmp::Ordering::Greater
            }
        });
        if greater {
            self.max = Some(Value::Bytes(bytes.to_vec()));
        }
    }

    /// Merge another builder's per-page accumulators into self. Used
    /// by the parallel ANALYZE path: each worker fills its own
    /// builder from a disjoint slice of the sampled page list, then
    /// the main thread folds them together before `finish` runs.
    ///
    /// Semantics match what serial `record` would produce *for the
    /// fields the planner actually consumes*:
    ///   * `null_count` and `exact_value_counts` sum on key collisions.
    ///   * `distinct_values` unions both KMV sketches (per-key set
    ///     union → keep smallest K hashes).
    ///   * `top_candidates` sum counts on collision (a partition-
    ///     parallel approximation of Misra-Gries: union locally
    ///     surviving heavy hitters, then trim to the configured
    ///     capacity keeping highest counts).
    ///   * `min` / `max` take the cross-builder extrema.
    pub(crate) fn merge(&mut self, other: ColumnStatsBuilder) {
        match (self.complete_counts.as_mut(), other.complete_counts) {
            (Some(counts), Some(other_counts)) => {
                let mut complete = true;
                for (hash, (value, count)) in other_counts {
                    if let Some((stored, existing)) = counts.get_mut(&hash) {
                        if *stored != value {
                            complete = false;
                            break;
                        }
                        *existing = existing.saturating_add(count);
                    } else if counts.len() < top_value_limit() {
                        counts.insert(hash, (value, count));
                    } else {
                        complete = false;
                        break;
                    }
                }
                if !complete {
                    self.complete_counts = None;
                }
            }
            _ => self.complete_counts = None,
        }
        self.null_count = self.null_count.saturating_add(other.null_count);
        self.non_null_count = self.non_null_count.saturating_add(other.non_null_count);
        self.total_value_bytes = self
            .total_value_bytes
            .saturating_add(other.total_value_bytes);
        self.top_candidate_rows_seen = self
            .top_candidate_rows_seen
            .saturating_add(other.top_candidate_rows_seen);
        self.top_candidate_rows_sampled = self
            .top_candidate_rows_sampled
            .saturating_add(other.top_candidate_rows_sampled);
        self.distinct_values.merge(other.distinct_values);
        for (key, (value, count)) in other.exact_value_counts {
            self.exact_value_counts
                .entry(key)
                .and_modify(|(_, existing)| *existing = existing.saturating_add(count))
                .or_insert((value, count));
        }
        for (priority, value) in other.histogram_samples {
            if self.histogram_samples.len() < ANALYZE_HISTOGRAM_SAMPLE_VALUES
                || self
                    .histogram_samples
                    .last_key_value()
                    .is_some_and(|(max, _)| priority < *max)
            {
                self.histogram_samples.insert(priority, value);
                if self.histogram_samples.len() > ANALYZE_HISTOGRAM_SAMPLE_VALUES {
                    self.histogram_samples.pop_last();
                }
            }
        }
        self.materialize_top_candidate_counts();
        let other_decrements = other.top_candidate_decrements;
        self.top_candidate_merged_error = self
            .top_candidate_merged_error
            .saturating_add(other.top_candidate_merged_error)
            .saturating_add(other_decrements);
        for (key, (value, stored_count)) in other.top_candidates {
            let count = stored_count.saturating_sub(other_decrements);
            if count == 0 {
                continue;
            }
            self.top_candidates
                .entry(key)
                .and_modify(|(_, existing)| *existing = existing.saturating_add(count))
                .or_insert((value, count));
        }
        if self.top_candidates.len() > analyze_top_value_candidates() {
            let mut entries: Vec<(u64, (Value, u64))> = std::mem::take(&mut self.top_candidates)
                .into_iter()
                .collect();
            entries.sort_by(|a, b| b.1 .1.cmp(&a.1 .1));
            self.top_candidate_merged_error = self
                .top_candidate_merged_error
                .saturating_add(entries[analyze_top_value_candidates()].1 .1);
            entries.truncate(analyze_top_value_candidates());
            self.top_candidates = entries.into_iter().collect();
        }
        self.top_candidate_min_stored = self
            .top_candidates
            .values()
            .map(|(_, count)| *count)
            .min()
            .unwrap_or(u64::MAX);
        match (&self.min, other.min) {
            (Some(current), Some(candidate)) => {
                if compare_stat_values(&candidate, current) == std::cmp::Ordering::Less {
                    self.min = Some(candidate);
                }
            }
            (None, candidate @ Some(_)) => self.min = candidate,
            _ => {}
        }
        match (&self.max, other.max) {
            (Some(current), Some(candidate)) => {
                if compare_stat_values(&candidate, current) == std::cmp::Ordering::Greater {
                    self.max = Some(candidate);
                }
            }
            (None, candidate @ Some(_)) => self.max = candidate,
            _ => {}
        }
    }

    #[inline]
    fn record_complete_count(
        &mut self,
        hash: u64,
        equals: impl FnOnce(&Value) -> bool,
        make_value: impl FnOnce() -> Value,
    ) {
        let Some(counts) = self.complete_counts.as_mut() else {
            return;
        };
        if let Some((value, count)) = counts.get_mut(&hash) {
            if equals(value) {
                *count = count.saturating_add(1);
                return;
            }
            // Never label colliding physical values as an exact frequency.
        } else if counts.len() < top_value_limit() {
            counts.insert(hash, (make_value(), 1));
            return;
        }
        self.complete_counts = None;
    }

    fn record_top_candidate(&mut self, key_hash: u64, value: &Value) {
        self.record_top_candidate_lazy(key_hash, || value.clone());
    }

    #[inline]
    fn sample_top_candidate(&mut self) -> bool {
        let ordinal = self.top_candidate_rows_seen;
        self.top_candidate_rows_seen = self.top_candidate_rows_seen.saturating_add(1);
        let sampled = ordinal < ANALYZE_TOP_CANDIDATE_WARMUP_ROWS
            || splitmix64(ordinal) & ANALYZE_TOP_CANDIDATE_SAMPLE_MASK == 0;
        if sampled {
            self.top_candidate_rows_sampled = self.top_candidate_rows_sampled.saturating_add(1);
        }
        sampled
    }

    fn record_top_candidate_lazy(&mut self, key_hash: u64, make_value: impl FnOnce() -> Value) {
        if let Some((_, count)) = self.top_candidates.get_mut(&key_hash) {
            if *count > self.top_candidate_decrements {
                *count = count.saturating_add(1);
                return;
            }
            // This key survived only as a lazily-retained zero counter.
            self.top_candidates.remove(&key_hash);
        }

        self.cleanup_dead_top_candidates();
        if self.top_candidates.len() < analyze_top_value_candidates() {
            let stored_count = self.top_candidate_decrements.saturating_add(1);
            self.top_candidates
                .insert(key_hash, (make_value(), stored_count));
            self.top_candidate_min_stored = self.top_candidate_min_stored.min(stored_count);
            return;
        }

        self.top_candidate_decrements = self.top_candidate_decrements.saturating_add(1);
    }

    fn cleanup_dead_top_candidates(&mut self) {
        if self.top_candidate_min_stored > self.top_candidate_decrements {
            return;
        }
        let baseline = self.top_candidate_decrements;
        self.top_candidates
            .retain(|_, (_, stored_count)| *stored_count > baseline);
        self.top_candidate_min_stored = self
            .top_candidates
            .values()
            .map(|(_, count)| *count)
            .min()
            .unwrap_or(u64::MAX);
    }

    fn materialize_top_candidate_counts(&mut self) {
        let baseline = self.top_candidate_decrements;
        self.top_candidate_merged_error = self.top_candidate_merged_error.saturating_add(baseline);
        self.top_candidates.retain(|_, (_, count)| {
            *count = count.saturating_sub(baseline);
            *count > 0
        });
        self.top_candidate_decrements = 0;
        self.top_candidate_min_stored = self
            .top_candidates
            .values()
            .map(|(_, count)| *count)
            .min()
            .unwrap_or(u64::MAX);
    }

    pub(crate) fn finish(
        mut self,
        precise: bool,
        count_scale: f64,
        row_count: u64,
        page_sample_fraction: f64,
    ) -> ColumnStats {
        if !precise {
            // A serial scan can begin in exact mode and cross the 4096-row
            // limit later. Admit those early rows into the same bounded
            // sample so ordered data does not bias the histogram to its tail.
            for (key, (value, count)) in &self.exact_value_counts {
                if estimated_value_width(value) > ANALYZE_HISTOGRAM_MAX_VALUE_BYTES {
                    continue;
                }
                let hash = stats_hash_key_part(key);
                for ordinal in 0..*count {
                    let priority = splitmix64(hash ^ splitmix64(ordinal));
                    if self.histogram_samples.len() < ANALYZE_HISTOGRAM_SAMPLE_VALUES
                        || self
                            .histogram_samples
                            .last_key_value()
                            .is_some_and(|(max, _)| priority < *max)
                    {
                        self.histogram_samples.insert(priority, value.clone());
                        if self.histogram_samples.len() > ANALYZE_HISTOGRAM_SAMPLE_VALUES {
                            self.histogram_samples.pop_last();
                        }
                    }
                }
            }
        }
        // Use exact frequencies on small tables and a bounded, mergeable
        // row sample on larger tables. TOP-K is deliberately excluded here:
        // heavy hitters alone are not representative range boundaries.
        let mut histogram = if precise && self.exact_value_counts.len() >= histogram_min_distinct()
        {
            build_equi_depth_histogram(
                &self.exact_value_counts,
                histogram_bucket_count(),
                count_scale,
                row_count,
            )
        } else if !precise && self.histogram_samples.len() >= histogram_min_distinct() {
            let mut counts = BTreeMap::<KeyPart, (Value, u64)>::new();
            for value in self.histogram_samples.into_values() {
                let key = KeyPart::from_value(&value);
                counts
                    .entry(key)
                    .and_modify(|(_, count)| *count += 1)
                    .or_insert((value, 1));
            }
            if counts.len() >= histogram_min_distinct() {
                let sample_rows = counts.values().map(|(_, count)| *count).sum::<u64>();
                let estimated_non_null =
                    (self.non_null_count as f64 * count_scale).min(row_count as f64);
                build_equi_depth_histogram(
                    &counts,
                    histogram_bucket_count(),
                    estimated_non_null / sample_rows as f64,
                    row_count,
                )
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };
        if let (Some(max), Some(last)) = (&self.max, histogram.last_mut()) {
            if compare_stat_values(max, &last.upper) == std::cmp::Ordering::Greater {
                last.upper = max.clone();
            }
        }
        let top_candidate_scale = if self.top_candidate_rows_sampled == 0 {
            1.0
        } else {
            self.top_candidate_rows_seen as f64 / self.top_candidate_rows_sampled as f64
        };
        let complete = self.complete_counts.is_some();
        let frequency = FrequencyStats {
            source: if precise || (complete && page_sample_fraction >= 1.0) {
                FrequencySource::Exact
            } else if complete {
                FrequencySource::Sample
            } else {
                FrequencySource::LowerBound
            },
            sample_rows: self.non_null_count.saturating_add(self.null_count),
            complete: complete || (precise && self.exact_value_counts.len() <= top_value_limit()),
            count_error: Some(if complete || precise {
                0
            } else {
                (self
                    .top_candidate_merged_error
                    .saturating_add(self.top_candidate_decrements) as f64
                    * count_scale
                    * top_candidate_scale
                    // A serial scan can outgrow exact mode after its first
                    // 4096 rows. Those counters are absent from the tail
                    // sketch; retain their maximum per-value deficit too.
                    + self.exact_value_counts.values().map(|(_, count)| *count).max().unwrap_or(0) as f64
                        * count_scale)
                    .ceil()
                    .min(row_count as f64) as u64
            }),
        };
        let mut top_values = if let Some(counts) = self.complete_counts {
            counts
                .into_values()
                .map(|(value, count)| ValueFrequency {
                    value,
                    count: scale_sample_count(count, count_scale, row_count),
                })
                .collect::<Vec<_>>()
        } else if precise {
            self.exact_value_counts
                .into_values()
                .map(|(value, count)| ValueFrequency {
                    value,
                    count: scale_sample_count(count, count_scale, row_count),
                })
                .collect::<Vec<_>>()
        } else {
            let baseline = self.top_candidate_decrements;
            self.top_candidates
                .into_values()
                .filter_map(|(value, stored_count)| {
                    let count = stored_count.saturating_sub(baseline);
                    (count > 0).then(|| ValueFrequency {
                        value,
                        count: scale_sample_count(
                            count,
                            count_scale * top_candidate_scale,
                            row_count,
                        ),
                    })
                })
                .collect::<Vec<_>>()
        };
        top_values.sort_by(|left, right| {
            right
                .count
                .cmp(&left.count)
                .then_with(|| value_to_string(&left.value).cmp(&value_to_string(&right.value)))
        });
        top_values.truncate(top_value_limit());
        ColumnStats {
            null_count: scale_sample_count(self.null_count, count_scale, row_count),
            distinct_count: self.distinct_values.finish(
                precise,
                count_scale,
                row_count,
                page_sample_fraction,
            ),
            min: self.min,
            max: self.max,
            top_values,
            frequency,
            average_width_bytes: (self.non_null_count > 0)
                .then_some(self.total_value_bytes as f64 / self.non_null_count as f64),
            histogram,
        }
    }
}

#[inline]
fn compare_stat_values_hot(left: &Value, right: &Value) -> std::cmp::Ordering {
    match (left, right) {
        (Value::Int(left), Value::Int(right)) => left.cmp(right),
        (Value::UInt(left), Value::UInt(right)) => left.cmp(right),
        (Value::Float(left), Value::Float(right)) => left.total_cmp(right),
        (Value::Double(left), Value::Double(right)) => left.total_cmp(right),
        (Value::DecimalCompact(left, left_scale), Value::DecimalCompact(right, right_scale))
            if left_scale == right_scale =>
        {
            left.cmp(right)
        }
        (Value::Date(left), Value::Date(right)) => left.cmp(right),
        (Value::Time(left), Value::Time(right)) => left.cmp(right),
        (Value::DateTime(left), Value::DateTime(right))
        | (Value::Timestamp(left), Value::Timestamp(right)) => left.cmp(right),
        (Value::Year(left), Value::Year(right)) => left.cmp(right),
        (Value::Bytes(left), Value::Bytes(right)) => left.cmp(right),
        (Value::String(left), Value::String(right)) => left.cmp(right),
        (Value::SmallString(left), Value::SmallString(right)) => left.as_ref().cmp(right.as_ref()),
        (Value::String(left), Value::SmallString(right)) => left.as_bytes().cmp(right.as_ref()),
        (Value::SmallString(left), Value::String(right)) => left.as_ref().cmp(right.as_bytes()),
        _ => compare_stat_values(left, right),
    }
}

fn build_equi_depth_histogram(
    exact: &BTreeMap<KeyPart, (Value, u64)>,
    bucket_count: usize,
    count_scale: f64,
    row_count: u64,
) -> Vec<HistogramBucket> {
    if bucket_count == 0 {
        return Vec::new();
    }
    let total: u64 = exact.values().map(|(_, c)| *c).sum();
    if total == 0 {
        return Vec::new();
    }
    // Each bucket aims to hold approximately `total / bucket_count`
    // rows. We walk values in KeyPart order (= index order) and
    // close the current bucket once its local depth reaches the
    // target. `cumulative_count` is the running sum at the bucket's
    // upper bound — that's what the estimator needs to interpolate
    // across overlapping buckets.
    let bucket_depth = total.div_ceil(bucket_count as u64).max(1);

    // Time-aligned mode: when the column is a temporal type (Date /
    // Time / DateTime / Year) AND the `cbo_time_aligned_histogram`
    // GUC is on, bucket closure additionally requires the value to
    // cross a natural calendar boundary (hour / day / week / month /
    // year, picked by span). Bucket upper bounds remain real observed
    // values — only the close *moment* is biased toward boundaries
    // so that `WHERE col = '2026-05-21'` style filters land inside
    // a tight bucket instead of a span-mixing one.
    let alignment = time_alignment_from(exact);

    let mut buckets: Vec<HistogramBucket> = Vec::new();
    let mut accum_total: u64 = 0;
    let mut accum_bucket: u64 = 0;
    let mut last_value: Option<&Value> = None;
    let mut last_close_floor: Option<i64> = None;
    for (key, (value, count)) in exact.iter() {
        accum_total = accum_total.saturating_add(*count);
        accum_bucket = accum_bucket.saturating_add(*count);
        last_value = Some(value);
        if accum_bucket < bucket_depth {
            continue;
        }
        if let Some(align) = alignment {
            // Compute the floor on the natural boundary for the current
            // value; only close when that floor moves past the previous
            // close-floor. This guarantees no two buckets share a
            // boundary slice (e.g. half a day).
            let unit = key_part_time_unit(key);
            if let Some(unit_value) = unit {
                let floor = align.floor(unit_value);
                if last_close_floor.is_some_and(|prev| floor <= prev) {
                    // Same natural slice as previous bucket — defer
                    // closing so this slice is not split across buckets.
                    continue;
                }
                last_close_floor = Some(floor);
            }
        }
        buckets.push(HistogramBucket {
            upper: value.clone(),
            cumulative_count: scale_sample_count(accum_total, count_scale, row_count),
        });
        accum_bucket = 0;
    }
    // Ensure the histogram closes on the maximum value, even when
    // the trailing partial bucket didn't reach `bucket_depth`.
    if let Some(upper) = last_value {
        let needs_final = buckets
            .last()
            .map(|b| compare_stat_values(&b.upper, upper) != std::cmp::Ordering::Equal)
            .unwrap_or(true);
        if needs_final {
            buckets.push(HistogramBucket {
                upper: upper.clone(),
                cumulative_count: scale_sample_count(accum_total, count_scale, row_count),
            });
        }
    }
    buckets
}

/// One i64 unit picked from a time-typed KeyPart so the alignment
/// math doesn't need to dispatch on the type at every value.
/// Returns `None` for non-temporal KeyParts.
fn key_part_time_unit(key: &KeyPart) -> Option<i64> {
    match key {
        KeyPart::Date(days) => Some(*days as i64),
        KeyPart::DateTime(us) => Some(*us),
        KeyPart::Time(us) => Some(*us),
        KeyPart::Year(year_offset) => Some(*year_offset as i64),
        _ => None,
    }
}

/// Inspect the first and last KeyPart in the (already sorted) map to
/// decide whether time alignment kicks in, and which boundary to use.
fn time_alignment_from(exact: &BTreeMap<KeyPart, (Value, u64)>) -> Option<TimeAlignment> {
    if crate::engine::cbo::cbo_consts().time_aligned_histogram() == 0.0 {
        return None;
    }
    let mut iter = exact.iter();
    let (first_key, _) = iter.next()?;
    let last_key = exact.iter().next_back()?.0;
    let first_unit = key_part_time_unit(first_key)?;
    let last_unit = key_part_time_unit(last_key)?;
    // Mixed temporal kinds inside one column would mean the catalog
    // is corrupted; conservatively bail.
    if std::mem::discriminant(first_key) != std::mem::discriminant(last_key) {
        return None;
    }
    let span = last_unit.saturating_sub(first_unit).max(0);
    TimeAlignment::pick(first_key, span)
}

#[derive(Debug, Clone, Copy)]
enum TimeAlignment {
    /// All Date / DateTime / Time / Year bucketing reduces to flooring
    /// a single i64 by `step`. The step is in the KeyPart's natural
    /// unit (days for Date, microseconds for Time / DateTime, years
    /// for Year).
    Step(i64),
}

impl TimeAlignment {
    fn pick(sample: &KeyPart, span: i64) -> Option<Self> {
        const US_PER_MIN: i64 = 60 * 1_000_000;
        const US_PER_HOUR: i64 = 60 * US_PER_MIN;
        const US_PER_DAY: i64 = 24 * US_PER_HOUR;
        const US_PER_WEEK: i64 = 7 * US_PER_DAY;
        const US_PER_MONTH: i64 = 30 * US_PER_DAY;
        let step = match sample {
            KeyPart::Date(_) => {
                // span in days.
                if span <= 60 {
                    1 // daily buckets
                } else if span <= 730 {
                    7 // weekly
                } else {
                    30 // monthly
                }
            }
            KeyPart::DateTime(_) => {
                if span <= US_PER_HOUR {
                    US_PER_MIN
                } else if span <= US_PER_DAY {
                    US_PER_HOUR
                } else if span <= 30 * US_PER_DAY {
                    US_PER_DAY
                } else if span <= 2 * 365 * US_PER_DAY {
                    US_PER_WEEK
                } else {
                    US_PER_MONTH
                }
            }
            KeyPart::Time(_) => {
                // Time is wall-clock duration; span is microseconds.
                if span <= US_PER_HOUR {
                    US_PER_MIN
                } else {
                    US_PER_HOUR
                }
            }
            KeyPart::Year(_) => 1, // 1-year granularity
            _ => return None,
        };
        if step <= 0 {
            None
        } else {
            Some(TimeAlignment::Step(step))
        }
    }

    fn floor(self, value: i64) -> i64 {
        let TimeAlignment::Step(step) = self;
        // Euclidean-style floor so negative values (pre-epoch DATETIME,
        // pre-1970 DATE) align consistently.
        value.div_euclid(step) * step
    }
}

/// Per-sketch-value page incidence: on how many *distinct sampled
/// pages* has this value been seen. Heap scans walk pages in order
/// within a worker, so "new page" is detected by comparing against the
/// last page that recorded this hash; the parallel merge sums counts
/// because workers own disjoint page chunks.
#[derive(Clone, Copy)]
struct PageIncidence {
    pages: u64,
    last_page: u32,
}

pub(crate) struct DistinctStatsBuilder<T> {
    exact_values: BTreeSet<T>,
    sketch: PrehashedU64Map<PageIncidence>,
    max_hashes: BinaryHeap<u64>,
    recent_hash_pages: [Option<(u64, u32)>; ANALYZE_DISTINCT_RECENT_PAGE_CACHE_SIZE],
}

impl<T> DistinctStatsBuilder<T>
where
    T: Clone + Ord + StatsHash,
{
    pub(crate) fn new() -> Self {
        Self {
            exact_values: BTreeSet::new(),
            // Conditional domains can own thousands of mostly tiny sketches per
            // worker. Allocate in proportion to observed NDV, not the KMV
            // ceiling; eager capacity multiplies into gigabytes on parallel
            // ANALYZE before a domain reaches its overflow threshold.
            sketch: PrehashedU64Map::default(),
            max_hashes: BinaryHeap::new(),
            recent_hash_pages: [None; ANALYZE_DISTINCT_RECENT_PAGE_CACHE_SIZE],
        }
    }

    pub(crate) fn record(&mut self, value: &T, precise: bool, page_id: u32) {
        self.record_prehashed(value, value.stats_hash(), precise, page_id);
    }

    /// Record a value whose canonical statistics hash has already been
    /// computed by the caller. ANALYZE uses this when one decoded column
    /// feeds several column/index/group sketches: hashing the same key bytes
    /// again for every consumer was a material part of wide-table ANALYZE
    /// CPU. Exact mode still receives the original value so collision-free
    /// small-table semantics are unchanged.
    pub(crate) fn record_prehashed(&mut self, value: &T, hash: u64, precise: bool, page_id: u32) {
        if precise {
            self.exact_values.insert(value.clone());
        }
        self.record_hash(hash, page_id);
    }

    /// Approximate-only sibling used by precompiled index-prefix layouts.
    /// Large/parallel ANALYZE never retains exact keys, so constructing an
    /// `IndexKey` solely to hash it is pure allocation/copy overhead.
    pub(crate) fn record_hash_only(&mut self, hash: u64, page_id: u32) {
        self.record_hash(hash, page_id);
    }

    /// Merge another partition's KMV sketch + exact-set into self.
    /// Per-key set union preserves distinct-count semantics; the
    /// sketch is then re-truncated to the configured sketch size by
    /// keeping the smallest K hashes (KMV's invariant: the sketch
    /// stores the k smallest hashes seen, regardless of input
    /// order).
    pub(crate) fn merge(&mut self, other: DistinctStatsBuilder<T>) {
        for value in other.exact_values {
            self.exact_values.insert(value);
        }
        for (hash, incidence) in other.sketch {
            if let Some(existing) = self.sketch.get_mut(&hash) {
                // Parallel workers own disjoint page chunks, so the
                // distinct-page counts are additive.
                existing.pages = existing.pages.saturating_add(incidence.pages);
            } else {
                self.insert_hash_incidence(hash, incidence);
            }
        }
    }

    fn record_hash(&mut self, hash: u64, page_id: u32) {
        // Re-observing a hash on the same page cannot change either KMV
        // membership or page incidence. This exact direct-mapped cache also
        // catches alternating low-cardinality values (flags, enum-like
        // dimensions, small integer domains) without touching hashbrown.
        let recent_slot = hash as usize & (ANALYZE_DISTINCT_RECENT_PAGE_CACHE_SIZE - 1);
        if self.recent_hash_pages[recent_slot] == Some((hash, page_id)) {
            return;
        }
        self.recent_hash_pages[recent_slot] = Some((hash, page_id));
        // Once KMV is full, any unseen hash above the retained maximum is
        // guaranteed to be discarded. Check that scalar threshold before a
        // hash-table probe: on unique/high-NDV columns the retained fraction
        // quickly falls below 0.1%, so this removes almost every lookup while
        // preserving exact KMV membership and page-incidence accounting for
        // retained/repeated values.
        if self.sketch.len() >= ANALYZE_DISTINCT_SKETCH_SIZE {
            if let Some(max_hash) = self.max_hashes.peek().copied() {
                if hash > max_hash {
                    return;
                }
            }
        }
        if let Some(incidence) = self.sketch.get_mut(&hash) {
            if incidence.last_page != page_id {
                incidence.pages = incidence.pages.saturating_add(1);
                incidence.last_page = page_id;
            }
            return;
        }
        let incidence = PageIncidence {
            pages: 1,
            last_page: page_id,
        };
        if self.sketch.len() < ANALYZE_DISTINCT_SKETCH_SIZE {
            self.sketch.insert(hash, incidence);
            self.max_hashes.push(hash);
            return;
        }
        let Some(max_hash) = self.max_hashes.peek().copied() else {
            self.sketch.insert(hash, incidence);
            self.max_hashes.push(hash);
            return;
        };
        if hash < max_hash {
            self.max_hashes.pop();
            self.sketch.remove(&max_hash);
            self.sketch.insert(hash, incidence);
            self.max_hashes.push(hash);
        }
    }

    fn insert_hash_incidence(&mut self, hash: u64, incidence: PageIncidence) {
        if self.sketch.len() < ANALYZE_DISTINCT_SKETCH_SIZE {
            self.sketch.insert(hash, incidence);
            self.max_hashes.push(hash);
            return;
        }
        let Some(max_hash) = self.max_hashes.peek().copied() else {
            return;
        };
        if hash < max_hash {
            self.max_hashes.pop();
            self.sketch.remove(&max_hash);
            self.sketch.insert(hash, incidence);
            self.max_hashes.push(hash);
        }
    }

    /// `page_sample_fraction`: fraction of the heap's pages that were
    /// fed into this builder (1.0 when the whole heap was scanned).
    pub(crate) fn finish(
        self,
        precise: bool,
        _count_scale: f64,
        row_count: u64,
        page_sample_fraction: f64,
    ) -> u64 {
        if precise {
            return self.exact_values.len() as u64;
        }
        // KMV estimates the distinct count of the *sampled stream*, not
        // the table: under block sampling a nearly-unique key only ever
        // shows the sample's worth of values, underestimating the table
        // NDV by 1/sample-fraction (TPC-H sf=5 l_orderkey: 28k vs 7.5M).
        // Scale up with a page-level Duj1 estimator [Haas et al. 1995]:
        //
        //   D̂ = d / (1 − (1−q)·f1/n)
        //
        // where q is the page sampling fraction, d the sample-distinct
        // estimate, and f1/n the fraction of (value, page) incidences
        // belonging to values seen on exactly one sampled page. Both are
        // measured on the KMV sketch members — a uniform random subset
        // of the sampled values, so their singleton fraction is an
        // unbiased estimate of the sample-wide one. Page granularity
        // (not row) is what disambiguates a clustered unique key (rows
        // co-located on one page → still a page-singleton → scales to
        // ~row_count) from a scattered low-NDV column (every value hits
        // many sampled pages → f1≈0 → no scale-up).
        let sample_distinct = estimate_distinct_from_kmv(&self.sketch).min(row_count.max(1));
        if page_sample_fraction >= 1.0 || sample_distinct == 0 {
            return sample_distinct;
        }
        let tokens: u64 = self.sketch.values().map(|i| i.pages).sum();
        if tokens == 0 {
            return sample_distinct;
        }
        let singletons = self.sketch.values().filter(|i| i.pages == 1).count() as u64;
        let q = page_sample_fraction.clamp(f64::MIN_POSITIVE, 1.0);
        let denom = 1.0 - (1.0 - q) * (singletons as f64 / tokens as f64);
        let estimate = sample_distinct as f64 / denom.max(q);
        (estimate.round() as u64)
            .max(sample_distinct)
            .min(row_count.max(1))
    }
}

pub(crate) trait StatsHash {
    fn stats_hash(&self) -> u64;
}

impl StatsHash for KeyPart {
    fn stats_hash(&self) -> u64 {
        let mut hasher = StatsHasher::new();
        hash_key_part(&mut hasher, self);
        hasher.finish()
    }
}

impl StatsHash for IndexKey {
    fn stats_hash(&self) -> u64 {
        let mut hasher = StatsHasher::new();
        hasher.write_u8(0x80);
        hasher.write_u64(self.0.len() as u64);
        for part in &self.0 {
            hash_key_part(&mut hasher, part);
        }
        hasher.finish()
    }
}

/// Hash one already-normalized column key using the exact byte stream used
/// by `StatsHash for KeyPart`.
pub(crate) fn stats_hash_key_part(part: &KeyPart) -> u64 {
    part.stats_hash()
}

/// Hash an index key without first allocating an `IndexKey`. `parts` is the
/// requested prefix in index order. The stream intentionally matches
/// `StatsHash for IndexKey` byte-for-byte so KMV membership and persisted
/// estimates remain stable across the fast/legacy paths.
pub(crate) fn stats_hash_index_parts(parts: &[&KeyPart]) -> u64 {
    let mut hasher = StatsHasher::new();
    hasher.write_u8(0x80);
    hasher.write_u64(parts.len() as u64);
    for part in parts {
        hash_key_part(&mut hasher, part);
    }
    hasher.finish()
}

/// Hash a decoded column value as if it had first been converted through
/// `KeyPart::from_value`, but borrow string/binary payloads in place. This is
/// the dominant large-table column-statistics path and avoids one owned
/// string/byte allocation per non-NULL value.
pub(crate) fn stats_hash_value(value: &Value) -> u64 {
    let mut hasher = StatsHasher::new();
    match value {
        Value::Null => hasher.write_u8(0),
        Value::Int(value) => {
            hasher.write_u8(1);
            hasher.write_i64(*value);
        }
        Value::UInt(value) => {
            hasher.write_u8(2);
            hasher.write_u64(*value);
        }
        Value::Float(value) => {
            hasher.write_u8(3);
            hasher.write_bytes(&crate::storage::core::f32_to_order_u32(*value).to_le_bytes());
        }
        Value::Double(value) => {
            hasher.write_u8(6);
            hasher.write_u64(crate::storage::core::f64_to_order_u64(*value));
        }
        Value::DecimalCompact(value, scale) => {
            hasher.write_u8(7);
            hasher.write_bytes(&(*value as i128).to_le_bytes());
            hasher.write_u8(*scale);
        }
        Value::Decimal128(value, scale) => {
            hasher.write_u8(7);
            hasher.write_bytes(&value.to_le_bytes());
            hasher.write_u8(*scale);
        }
        Value::DecimalWide(_) => return KeyPart::from_value(value).stats_hash(),
        // Wide decimal is rare and needs the existing parser to recover the
        // canonical magnitude/scale. It is outside the compact TPC-H/TPC-C
        // hot lane, so keep one authoritative conversion here.
        Value::Decimal(_) => return KeyPart::from_value(value).stats_hash(),
        Value::Bytes(bytes) | Value::Json(bytes) | Value::Geometry(bytes) => {
            hasher.write_u8(4);
            hasher.write_bytes(bytes);
        }
        Value::Vector(bytes) => {
            hasher.write_u8(4);
            hasher.write_bytes(bytes);
        }
        Value::String(text) => {
            hasher.write_u8(5);
            hasher.write_bytes(text.as_bytes());
        }
        Value::SmallString(text) => {
            hasher.write_u8(5);
            hasher.write_bytes(text.as_ref());
        }
        Value::Date(value) => {
            hasher.write_u8(8);
            hasher.write_bytes(&value.to_le_bytes());
        }
        Value::Time(value) => {
            hasher.write_u8(9);
            hasher.write_i64(*value);
        }
        Value::DateTime(value) | Value::Timestamp(value) => {
            hasher.write_u8(10);
            hasher.write_i64(*value);
        }
        Value::Year(value) => {
            hasher.write_u8(11);
            hasher.write_u8(*value);
        }
    }
    hasher.finish()
}

fn stats_hash_string_bytes(bytes: &[u8]) -> u64 {
    let mut hasher = StatsHasher::new();
    hasher.write_u8(5);
    hasher.write_bytes(bytes);
    hasher.finish()
}

fn stats_hash_bytes(bytes: &[u8]) -> u64 {
    let mut hasher = StatsHasher::new();
    hasher.write_u8(4);
    hasher.write_bytes(bytes);
    hasher.finish()
}

fn string_value_from_bytes(bytes: &[u8]) -> Value {
    if bytes.len() <= crate::types::SMALL_STRING_INLINE_CAP {
        Value::SmallString(crate::types::SmallStringBuf::from_slice(bytes))
    } else {
        // Heap string payloads were validated on write. The sink decoder's
        // borrowed lane deliberately avoids paying a second full UTF-8 scan.
        Value::String(unsafe { String::from_utf8_unchecked(bytes.to_vec()) })
    }
}

fn value_text_bytes(value: &Value) -> Option<&[u8]> {
    match value {
        Value::String(text) => Some(text.as_bytes()),
        Value::SmallString(text) => Some(text.as_ref()),
        _ => None,
    }
}

struct StatsHasher {
    value: u64,
}

impl StatsHasher {
    fn new() -> Self {
        Self {
            value: 0xcbf2_9ce4_8422_2325,
        }
    }

    fn write_u8(&mut self, value: u8) {
        self.value ^= value as u64;
        self.value = self.value.wrapping_mul(0x0000_0100_0000_01b3);
    }

    fn write_bytes(&mut self, bytes: &[u8]) {
        self.write_raw_u64(bytes.len() as u64);
        for byte in bytes {
            self.write_u8(*byte);
        }
    }

    fn write_i64(&mut self, value: i64) {
        self.write_bytes(&value.to_le_bytes());
    }

    fn write_u64(&mut self, value: u64) {
        self.write_bytes(&value.to_le_bytes());
    }

    fn write_raw_u64(&mut self, value: u64) {
        for byte in value.to_le_bytes() {
            self.write_u8(byte);
        }
    }

    fn finish(self) -> u64 {
        self.value
    }
}

fn hash_key_part(hasher: &mut StatsHasher, part: &KeyPart) {
    match part {
        KeyPart::Null => hasher.write_u8(0),
        KeyPart::Int(value) => {
            hasher.write_u8(1);
            hasher.write_i64(*value);
        }
        KeyPart::UInt(value) => {
            hasher.write_u8(2);
            hasher.write_u64(*value);
        }
        KeyPart::Float32(value) => {
            hasher.write_u8(3);
            hasher.write_bytes(&value.to_le_bytes());
        }
        KeyPart::Float64(value) => {
            hasher.write_u8(6);
            hasher.write_u64(*value);
        }
        KeyPart::Decimal { value, scale } => {
            hasher.write_u8(7);
            hasher.write_bytes(&value.to_le_bytes());
            hasher.write_u8(*scale);
        }
        KeyPart::DecimalWide { value, scale } => {
            hasher.write_u8(13);
            hasher.write_bytes(&value.to_signed_bytes_le());
            hasher.write_u8(*scale);
        }
        KeyPart::Bytes(value) => {
            hasher.write_u8(4);
            hasher.write_bytes(value);
        }
        KeyPart::String(value) => {
            hasher.write_u8(5);
            hasher.write_bytes(value.as_bytes());
        }
        KeyPart::CollatedString {
            collation_id,
            value,
        } => {
            hasher.write_u8(12);
            hasher.write_bytes(&collation_id.to_le_bytes());
            // Statistics digest: stream the collation key bytes
            // directly into the hasher instead of materialising a
            // throwaway `Vec<u8>` per row.
            crate::charset::with_collation_key_by_id(*collation_id, value, |bytes| {
                hasher.write_bytes(bytes);
            });
        }
        KeyPart::Date(value) => {
            hasher.write_u8(8);
            hasher.write_bytes(&value.to_le_bytes());
        }
        KeyPart::Time(value) => {
            hasher.write_u8(9);
            hasher.write_i64(*value);
        }
        KeyPart::DateTime(value) => {
            hasher.write_u8(10);
            hasher.write_i64(*value);
        }
        KeyPart::Year(value) => {
            hasher.write_u8(11);
            hasher.write_u8(*value);
        }
        KeyPart::Param { .. } => {
            panic!("internal error: KeyPart::Param has no stats hash representation")
        }
    }
}

fn estimate_distinct_from_kmv(sketch: &PrehashedU64Map<PageIncidence>) -> u64 {
    let observed = sketch.len();
    if observed < ANALYZE_DISTINCT_SKETCH_SIZE {
        return observed as u64;
    }
    let Some(max_hash) = sketch.keys().max().copied() else {
        return 0;
    };
    if max_hash == 0 {
        return observed as u64;
    }

    let numerator = ((ANALYZE_DISTINCT_SKETCH_SIZE - 1) as u128) * (u64::MAX as u128);
    let estimate = (numerator / max_hash as u128).max(observed as u128);
    estimate.min(u64::MAX as u128) as u64
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct LegacyColumnStatsBuilder {
    null_count: u64,
    distinct_values: HashSet<KeyPart>,
    value_counts: BTreeMap<KeyPart, (Value, u64)>,
    min: Option<Value>,
    max: Option<Value>,
}

#[cfg(test)]
impl LegacyColumnStatsBuilder {
    pub(crate) fn record(&mut self, value: &Value) {
        if matches!(value, Value::Null) {
            self.null_count += 1;
            return;
        }
        let key = KeyPart::from_value(value);
        self.distinct_values.insert(key.clone());
        self.value_counts
            .entry(key)
            .and_modify(|(_, count)| *count += 1)
            .or_insert_with(|| (value.clone(), 1));
        if self
            .min
            .as_ref()
            .is_none_or(|current| compare_stat_values(value, current) == std::cmp::Ordering::Less)
        {
            self.min = Some(value.clone());
        }
        if self.max.as_ref().is_none_or(|current| {
            compare_stat_values(value, current) == std::cmp::Ordering::Greater
        }) {
            self.max = Some(value.clone());
        }
    }

    pub(crate) fn finish(self) -> ColumnStats {
        let mut top_values = self
            .value_counts
            .into_values()
            .map(|(value, count)| ValueFrequency { value, count })
            .collect::<Vec<_>>();
        top_values.sort_by(|left, right| {
            right
                .count
                .cmp(&left.count)
                .then_with(|| value_to_string(&left.value).cmp(&value_to_string(&right.value)))
        });
        top_values.truncate(top_value_limit());
        ColumnStats {
            null_count: self.null_count,
            distinct_count: self.distinct_values.len() as u64,
            min: self.min,
            max: self.max,
            frequency: FrequencyStats {
                source: FrequencySource::Exact,
                sample_rows: self.null_count + top_values.iter().map(|f| f.count).sum::<u64>(),
                complete: self.distinct_values.len() <= top_value_limit(),
                count_error: Some(0),
            },
            top_values,
            average_width_bytes: None,
            histogram: Vec::new(),
        }
    }
}

fn estimated_value_width(value: &Value) -> u64 {
    match value {
        Value::Null => 0,
        Value::Int(_)
        | Value::UInt(_)
        | Value::Float(_)
        | Value::Double(_)
        | Value::DecimalCompact(..)
        | Value::Date(_)
        | Value::Time(_)
        | Value::DateTime(_)
        | Value::Timestamp(_)
        | Value::Year(_) => 8,
        Value::Decimal128(..) => 16,
        Value::DecimalWide(_) => value.decimal_heap_bytes() as u64,
        Value::Decimal(value) | Value::String(value) => value.len() as u64,
        Value::SmallString(value) => value.len() as u64,
        Value::Bytes(value) | Value::Json(value) | Value::Geometry(value) => value.len() as u64,
        Value::Vector(value) => value.len() as u64 * std::mem::size_of::<f32>() as u64,
    }
}

#[cfg(test)]
mod fast_path_differential_tests {
    use super::*;

    #[test]
    fn borrowed_value_hash_matches_canonical_key_part_hash() {
        let values = [
            Value::Int(-42),
            Value::UInt(42),
            Value::Float(1.25),
            Value::Double(-9.5),
            Value::DecimalCompact(12_345, 2),
            Value::Date(19_000),
            Value::Time(123_456),
            Value::DateTime(987_654),
            Value::Timestamp(456_789),
            Value::Year(124),
            Value::String("Alpha  ".into()),
            Value::SmallString(smallvec::smallvec![b'b', b'e', b't', b'a']),
            Value::Bytes(vec![0, 1, 0xff]),
        ];
        for value in values {
            assert_eq!(
                stats_hash_value(&value),
                stats_hash_key_part(&KeyPart::from_value(&value)),
                "hash mismatch for {value:?}",
            );
        }
    }

    #[test]
    fn borrowed_index_parts_hash_matches_index_key_hash() {
        let parts = [
            KeyPart::Int(-7),
            KeyPart::Decimal {
                value: 12_345,
                scale: 2,
            },
            KeyPart::String("AIR".into()),
        ];
        let refs = parts.iter().collect::<Vec<_>>();
        assert_eq!(
            stats_hash_index_parts(&refs),
            IndexKey::from(parts.to_vec()).stats_hash(),
        );
    }

    fn assert_fast_matches_legacy(values: impl IntoIterator<Item = Value>) {
        let mut fast = ColumnStatsBuilder::new();
        let mut legacy = LegacyColumnStatsBuilder::default();
        let mut rows = 0u64;
        for value in values {
            fast.record_approx(&value, (rows / 32) as u32);
            legacy.record(&value);
            rows += 1;
        }
        let fast = fast.finish(false, 1.0, rows, 1.0);
        let legacy = legacy.finish();
        assert_eq!(fast.null_count, legacy.null_count);
        assert_eq!(fast.distinct_count, legacy.distinct_count);
        assert_eq!(fast.min, legacy.min);
        assert_eq!(fast.max, legacy.max);
        let top = |values: Vec<ValueFrequency>| {
            values
                .into_iter()
                .map(|entry| (entry.value, entry.count))
                .collect::<Vec<_>>()
        };
        assert_eq!(top(fast.top_values), top(legacy.top_values));
    }

    #[test]
    fn low_frequency_large_domain_collection_preserves_counts() {
        assert_fast_matches_legacy((0..40_000).map(|row| {
            if row % 997 == 0 {
                Value::Int(2)
            } else {
                Value::Int(1)
            }
        }));
    }

    #[test]
    fn low_frequency_sample_and_parallel_merge_preserve_full_sample_counts() {
        let mut serial = ColumnStatsBuilder::new();
        let mut workers = (0..4)
            .map(|_| ColumnStatsBuilder::new())
            .collect::<Vec<_>>();
        for row in 0..40_000 {
            let bytes: &[u8] = if row % 997 == 0 { b"rare" } else { b"common" };
            serial.record_approx_string(bytes, row / 100);
            workers[(row % 4) as usize].record_approx_string(bytes, row / 100);
        }
        let mut merged = ColumnStatsBuilder::new();
        for worker in workers {
            merged.merge(worker);
        }
        let full = serial.finish(false, 1.0, 40_000, 1.0);
        let sampled = merged.finish(false, 10.0, 400_000, 0.1);
        assert_eq!(full.frequency.source, FrequencySource::Exact);
        assert_eq!(sampled.frequency.source, FrequencySource::Sample);
        assert!(full.frequency.complete && sampled.frequency.complete);
        assert_eq!(sampled.frequency.sample_rows, 40_000);
        for (a, b) in full.top_values.iter().zip(&sampled.top_values) {
            assert_eq!(a.value, b.value);
            assert_eq!(a.count * 10, b.count);
        }
        assert_eq!(full.top_values[1].count, 41);
        let restored: ColumnStats =
            serde_json::from_str(&serde_json::to_string(&sampled).unwrap()).unwrap();
        assert_eq!(restored.frequency.source, FrequencySource::Sample);
        assert!(restored.frequency.complete);
        assert_eq!(restored.frequency.sample_rows, 40_000);
    }

    #[test]
    fn low_frequency_overflow_and_collision_discard_complete_provenance() {
        let mut merged = ColumnStatsBuilder::new();
        for i in 0..=top_value_limit() {
            let mut worker = ColumnStatsBuilder::new();
            worker.record_approx(&Value::Int(i as i64), 0);
            merged.merge(worker);
        }
        let stats = merged.finish(false, 1.0, (top_value_limit() + 1) as u64, 1.0);
        assert_eq!(stats.frequency.source, FrequencySource::LowerBound);
        assert!(!stats.frequency.complete);
        let mut collision = ColumnStatsBuilder::new();
        collision.record_complete_count(7, |_| true, || Value::Int(1));
        collision.record_complete_count(7, |v| *v == Value::Int(2), || Value::Int(2));
        assert!(collision.complete_counts.is_none());
    }

    #[test]
    fn low_frequency_sketch_error_bounds_survive_parallel_merge() {
        let mut workers = (0..4)
            .map(|_| {
                let mut b = ColumnStatsBuilder::new();
                b.complete_counts = None;
                b
            })
            .collect::<Vec<_>>();
        let mut actual = BTreeMap::<i64, u64>::new();
        for i in 0..20_000 {
            let n = if i % 3 == 0 { 7 } else { i % 1700 };
            *actual.entry(n).or_default() += 1;
            let worker = &mut workers[(i % 4) as usize];
            let value = Value::Int(n);
            worker.record_top_candidate(stats_hash_value(&value), &value);
            worker.non_null_count += 1;
        }
        let mut merged = workers.remove(0);
        for worker in workers {
            merged.merge(worker);
        }
        let stats = merged.finish(false, 1.0, 20_000, 1.0);
        assert_eq!(stats.frequency.source, FrequencySource::LowerBound);
        let error = stats.frequency.count_error.unwrap();
        assert!(error > 0);
        for f in stats.top_values {
            let Value::Int(n) = f.value else {
                panic!("integer candidate");
            };
            assert!(f.count <= actual[&n]);
            assert!(
                actual[&n] <= f.count + error,
                "{n}: {} <= {} + {error}",
                actual[&n],
                f.count
            );
        }
    }

    #[test]
    fn low_frequency_sketch_error_includes_exact_prefix_after_mode_transition() {
        let mut builder = ColumnStatsBuilder::new();
        for row in 0..5000 {
            let value = if (4096..4224).contains(&row) {
                Value::Int(row)
            } else {
                Value::Int(7)
            };
            builder.record(&value, row < 4096, (row / 100) as u32);
        }
        let stats = builder.finish(false, 1.0, 5000, 1.0);
        assert_eq!(stats.frequency.source, FrequencySource::LowerBound);
        let count = stats
            .top_values
            .iter()
            .find(|f| f.value == Value::Int(7))
            .unwrap()
            .count;
        assert_eq!(stats.frequency.count_error, Some(4096));
        assert_eq!(count + stats.frequency.count_error.unwrap(), 5000 - 128);
    }

    #[test]
    fn approximate_histogram_covers_both_sides_of_exact_mode_transition() {
        let mut builder = ColumnStatsBuilder::new();
        for row in 0..8000 {
            builder.record(&Value::Int(row), row < 4096, (row / 100) as u32);
        }
        let stats = builder.finish(false, 1.0, 8000, 1.0);
        assert!(!stats.histogram.is_empty());
        assert!(matches!(stats.histogram.first().unwrap().upper, Value::Int(v) if v < 4096));
        assert_eq!(stats.histogram.last().unwrap().upper, Value::Int(7999));
        assert!(stats
            .histogram
            .windows(2)
            .all(|pair| pair[0].cumulative_count <= pair[1].cumulative_count));
    }

    #[test]
    fn low_frequency_large_typed_domains_match_exact_counts() {
        for (rare, common) in [
            (Value::UInt(2), Value::UInt(1)),
            (Value::DecimalCompact(125, 2), Value::DecimalCompact(250, 2)),
            (Value::Date(19000), Value::Date(19001)),
            (Value::DateTime(1_000_000), Value::DateTime(2_000_000)),
            (Value::Double(1.5), Value::Double(2.5)),
            (Value::Bytes(vec![0, 255]), Value::Bytes(vec![1, 0])),
        ] {
            assert_fast_matches_legacy((0..10_000).map(|i| {
                if i % 701 == 0 {
                    Value::Null
                } else if i % 997 == 0 {
                    rare.clone()
                } else {
                    common.clone()
                }
            }));
        }
    }

    #[test]
    fn typed_approx_accumulator_matches_legacy_for_exact_small_domains() {
        assert_fast_matches_legacy((0..1000).map(|row| {
            if row % 17 == 0 {
                Value::Null
            } else {
                Value::Int((row % 11) - 5)
            }
        }));
        assert_fast_matches_legacy(
            (0..1000).map(|row| Value::DecimalCompact(((row % 9) * 125) as i64, 2)),
        );
        assert_fast_matches_legacy((0..1000).map(|row| Value::Date(19_000 + row % 13)));
        assert_fast_matches_legacy((0..1000).map(|row| Value::String(format!("mode-{}", row % 7))));
    }

    #[test]
    fn sampled_top_candidates_are_deterministic_and_frequency_scaled() {
        fn build() -> ColumnStats {
            let rows = 100_000u64;
            let mut builder = ColumnStatsBuilder::new();
            for row in 0..rows {
                let value = if row % 10 < 7 {
                    Value::Int(7)
                } else {
                    Value::Int((row % 997) as i64 + 100)
                };
                builder.record_approx(&value, (row / 100) as u32);
            }
            builder.finish(false, 1.0, rows, 1.0)
        }

        let first = build();
        let second = build();
        let signature = |stats: &ColumnStats| {
            stats
                .top_values
                .iter()
                .map(|entry| (entry.value.clone(), entry.count))
                .collect::<Vec<_>>()
        };
        assert_eq!(signature(&first), signature(&second));
        let heavy = first
            .top_values
            .iter()
            .find(|entry| entry.value == Value::Int(7))
            .expect("heavy hitter retained after TOP-K row sampling");
        assert!(
            (65_000..=75_000).contains(&heavy.count),
            "scaled heavy-hitter count {} is outside 5 percentage points",
            heavy.count
        );
    }
}

#[cfg(test)]
mod ndv_scaleup_tests {
    use super::*;

    /// Simulate block sampling: `sample_every`-th page of `total_pages`
    /// pages is scanned; `value_of(row)` supplies the column value of
    /// each row; `rows_per_page` rows per page. Returns the NDV
    /// estimate for the supplied population.
    fn run(
        total_pages: u32,
        sample_every: u32,
        rows_per_page: u32,
        total_rows: u64,
        value_of: impl Fn(u64) -> i64,
    ) -> u64 {
        let mut builder: DistinctStatsBuilder<KeyPart> = DistinctStatsBuilder::new();
        for page in (0..total_pages).step_by(sample_every as usize) {
            for slot in 0..rows_per_page {
                let row = page as u64 * rows_per_page as u64 + slot as u64;
                builder.record(&KeyPart::Int(value_of(row)), false, page);
            }
        }
        let q = 1.0 / sample_every as f64;
        builder.finish(false, sample_every as f64, total_rows, q)
    }

    #[test]
    fn clustered_unique_key_scales_to_population() {
        // l_orderkey shape: ~4 consecutive rows share a key, all on the
        // same page. Sample 1/64 pages → the sample only sees 1/64 of
        // the keys, but every key is a page-singleton → Duj1 must scale
        // the estimate back up to ~population NDV (row_count / 4).
        let total_rows = 64_000u64;
        let estimate = run(640, 64, 100, total_rows, |row| (row / 4) as i64);
        let truth = total_rows / 4;
        assert!(
            (truth as f64 * 0.8..=truth as f64 * 1.2).contains(&(estimate as f64)),
            "clustered unique: estimate {estimate} not within 20% of {truth}"
        );
    }

    #[test]
    fn scattered_unique_key_scales_to_population() {
        // o_orderkey shape: every row a fresh key.
        let total_rows = 64_000u64;
        let estimate = run(640, 64, 100, total_rows, |row| row as i64);
        assert!(
            (total_rows as f64 * 0.8..=total_rows as f64 * 1.2).contains(&(estimate as f64)),
            "scattered unique: estimate {estimate} not within 20% of {total_rows}"
        );
    }

    #[test]
    fn scattered_low_cardinality_is_not_inflated() {
        // 100 distinct values striped across every page: each value
        // appears on many sampled pages → f1 ≈ 0 → no scale-up.
        let estimate = run(640, 64, 100, 64_000, |row| (row % 100) as i64);
        assert!(
            (90..=110).contains(&estimate),
            "low-card: estimate {estimate} should stay ≈100"
        );
    }

    #[test]
    fn recent_page_cache_preserves_cross_page_incidence() {
        // Exercise repeated, alternating values that hit the recent-page
        // cache. The first occurrence on every sampled page must still reach
        // the sketch, otherwise all seven values look like page singletons
        // and the block-sampling estimator incorrectly inflates their NDV.
        let mut builder: DistinctStatsBuilder<KeyPart> = DistinctStatsBuilder::new();
        for page in 0..10u32 {
            for slot in 0..700u32 {
                builder.record(&KeyPart::Int((slot % 7) as i64), false, page);
            }
        }
        assert_eq!(builder.finish(false, 10.0, 70_000, 0.1), 7);
    }

    #[test]
    fn full_scan_keeps_sample_estimate() {
        // q = 1.0 → no extrapolation even for page-singleton values.
        let mut builder: DistinctStatsBuilder<KeyPart> = DistinctStatsBuilder::new();
        for row in 0..1000u64 {
            builder.record(&KeyPart::Int(row as i64), false, (row / 10) as u32);
        }
        assert_eq!(builder.finish(false, 1.0, 1000, 1.0), 1000);
    }

    #[test]
    fn parallel_merge_matches_serial() {
        // Two workers over disjoint page chunks must produce the same
        // estimate as one serial pass.
        let value_of = |row: u64| (row / 4) as i64;
        let mut serial: DistinctStatsBuilder<KeyPart> = DistinctStatsBuilder::new();
        let mut left: DistinctStatsBuilder<KeyPart> = DistinctStatsBuilder::new();
        let mut right: DistinctStatsBuilder<KeyPart> = DistinctStatsBuilder::new();
        for page in (0..640u32).step_by(64) {
            for slot in 0..100u32 {
                let row = page as u64 * 100 + slot as u64;
                let key = KeyPart::Int(value_of(row));
                serial.record(&key, false, page);
                if page < 320 {
                    left.record(&key, false, page);
                } else {
                    right.record(&key, false, page);
                }
            }
        }
        left.merge(right);
        assert_eq!(
            serial.finish(false, 64.0, 64_000, 1.0 / 64.0),
            left.finish(false, 64.0, 64_000, 1.0 / 64.0)
        );
    }
}

#[cfg(test)]
mod histogram_tests {
    use super::*;
    use std::sync::Mutex;

    // Acquire when toggling cbo_time_aligned_histogram so tests can
    // run concurrently with the rest of the suite without flapping
    // the global GUC out from under each other.
    static TIME_HIST_GUC_LOCK: Mutex<()> = Mutex::new(());

    fn datetime_us(seconds_since_epoch: i64) -> Value {
        Value::DateTime(seconds_since_epoch.saturating_mul(1_000_000))
    }

    fn date_days(days_since_epoch: i32) -> Value {
        Value::Date(days_since_epoch)
    }

    fn build_map<I: IntoIterator<Item = (Value, u64)>>(iter: I) -> BTreeMap<KeyPart, (Value, u64)> {
        let mut out = BTreeMap::new();
        for (value, count) in iter {
            let key = KeyPart::from_value(&value);
            out.insert(key, (value, count));
        }
        out
    }

    #[test]
    fn integer_column_keeps_pure_equi_depth_shape() {
        // Non-temporal column: behaviour must match the pre-Step-1 algorithm.
        let map = build_map((0..200).map(|i| (Value::Int(i as i64), 1)));
        let buckets = build_equi_depth_histogram(&map, 4, 1.0, 200);
        assert_eq!(buckets.len(), 4);
        // Equi-depth: each bucket's cumulative count grows by ~50.
        for (i, bucket) in buckets.iter().enumerate() {
            assert!(
                bucket.cumulative_count >= (i as u64 + 1) * 50
                    && bucket.cumulative_count <= (i as u64 + 1) * 50 + 1,
                "bucket {i} cumulative_count = {} not near {}",
                bucket.cumulative_count,
                (i as u64 + 1) * 50,
            );
        }
    }

    #[test]
    fn datetime_buckets_close_on_natural_boundaries() {
        let _guard = TIME_HIST_GUC_LOCK.lock().unwrap();
        let prior = crate::engine::cbo::cbo_consts().time_aligned_histogram();
        crate::engine::cbo::set_variable("cbo_time_aligned_histogram", 1.0).unwrap();

        // 10 days × 24 entries (one per hour). Span = 10 days → step = 1 day.
        let one_hour: i64 = 3600;
        let entries: Vec<(Value, u64)> = (0..10 * 24)
            .map(|i| (datetime_us(i as i64 * one_hour), 1))
            .collect();
        let map = build_map(entries);
        let buckets = build_equi_depth_histogram(&map, 4, 1.0, 240);
        crate::engine::cbo::set_variable("cbo_time_aligned_histogram", prior).unwrap();

        // Each bucket upper bound should be exactly on or after a day
        // boundary (its `value mod 86400s == 0` only when the algorithm
        // closed at a precise day tick; we accept any value whose day
        // floor differs from the prior bucket's day floor).
        let mut prev_day: Option<i64> = None;
        for bucket in &buckets {
            if let Value::DateTime(us) = bucket.upper {
                let day = us.div_euclid(86_400_000_000);
                if let Some(prev) = prev_day {
                    assert!(
                        day > prev,
                        "bucket day {day} should be strictly greater than previous {prev}",
                    );
                }
                prev_day = Some(day);
            } else {
                panic!("expected DateTime upper bound, got {:?}", bucket.upper);
            }
        }
        // Last bucket must include the maximum value.
        let last = buckets.last().unwrap();
        if let Value::DateTime(us) = last.upper {
            assert_eq!(us, (10 * 24 - 1) * one_hour * 1_000_000);
        }
    }

    #[test]
    fn date_column_uses_weekly_step_for_year_spans() {
        let _guard = TIME_HIST_GUC_LOCK.lock().unwrap();
        let prior = crate::engine::cbo::cbo_consts().time_aligned_histogram();
        crate::engine::cbo::set_variable("cbo_time_aligned_histogram", 1.0).unwrap();

        // 400 days, one row per day → step = 7 (weekly).
        let map = build_map((0..400).map(|i| (date_days(i as i32), 1)));
        let buckets = build_equi_depth_histogram(&map, 8, 1.0, 400);
        crate::engine::cbo::set_variable("cbo_time_aligned_histogram", prior).unwrap();

        // Each bucket's upper day floor (days / 7) must be strictly
        // increasing — i.e. no two consecutive buckets fall in the same
        // week.
        let mut prev_week: Option<i32> = None;
        for bucket in &buckets {
            if let Value::Date(days) = bucket.upper {
                let week = days.div_euclid(7);
                if let Some(prev) = prev_week {
                    assert!(
                        week > prev,
                        "bucket week {week} not strictly greater than {prev}",
                    );
                }
                prev_week = Some(week);
            }
        }
    }

    #[test]
    fn guc_off_reverts_to_pure_equi_depth() {
        let _guard = TIME_HIST_GUC_LOCK.lock().unwrap();
        let prior = crate::engine::cbo::cbo_consts().time_aligned_histogram();
        crate::engine::cbo::set_variable("cbo_time_aligned_histogram", 0.0).unwrap();

        // Sparse DATETIME values clustered such that the time-aligned
        // path would delay closure. With the GUC off we should get
        // exact bucket_count buckets (or bucket_count+1 with the
        // trailing "ensure final closes on max" guard) again.
        let one_hour: i64 = 3600;
        let map = build_map((0..32).map(|i| (datetime_us(i as i64 * one_hour), 1)));
        let buckets = build_equi_depth_histogram(&map, 4, 1.0, 32);
        crate::engine::cbo::set_variable("cbo_time_aligned_histogram", prior).unwrap();

        // 32 distinct values / 4 buckets = depth 8 → exactly 4 buckets.
        assert_eq!(buckets.len(), 4);
        assert_eq!(buckets[3].cumulative_count, 32);
    }
}

#[cfg(test)]
mod bounded_sketch_tests {
    use super::*;

    #[test]
    fn unique_conditional_domains_preserve_more_values_without_ndv_sketches() {
        let mut left = ConditionalKeyDomainBuilder::new_unique(vec!["f".into()], vec!["id".into()]);
        let mut right =
            ConditionalKeyDomainBuilder::new_unique(vec!["f".into()], vec!["id".into()]);
        for i in 0..3000i64 {
            for (builder, key) in [(&mut left, i), (&mut right, i + 3000)] {
                builder.record(
                    vec![Value::Int(i)],
                    IndexKey::from(vec![KeyPart::Int(key)]),
                    Some(Value::Int(key)),
                    false,
                    0,
                );
            }
        }
        left.merge(right);
        assert!(!left.is_overflowed());
        assert!(left.unique_reserved_bytes <= ConditionalKeyDomainBuilder::UNIQUE_DOMAIN_BYTES);
        assert!(left
            .entries
            .values()
            .all(|entry| entry.key_distinct.sketch.is_empty()
                && entry.key_distinct.max_hashes.is_empty()));
        let result = left.finish(false, 1.0, 6000, 1.0).unwrap();
        assert_eq!(result.entries.len(), 3000);
        for (i, entry) in result.entries.iter().enumerate() {
            assert_eq!(entry.row_count, 2);
            assert_eq!(entry.key_distinct_count, 2);
            assert_eq!(entry.key_min, Some(Value::Int(i as i64)));
            assert_eq!(entry.key_max, Some(Value::Int(i as i64 + 3000)));
        }
    }

    #[test]
    fn unique_conditional_domain_overflow_discards_the_whole_statistic() {
        let before = crate::engine::executor::mem_budget::controlled_memory_snapshot().0;
        let mut builder =
            ConditionalKeyDomainBuilder::new_unique(vec!["f".into()], vec!["id".into()]);
        for i in 0..5000i64 {
            builder.record(
                vec![Value::Int(i)],
                IndexKey::from(vec![KeyPart::Int(i)]),
                Some(Value::Int(i)),
                false,
                0,
            );
        }
        assert!(builder.is_overflowed());
        assert_eq!(builder.unique_reserved_bytes, 0);
        assert!(builder.finish(false, 1.0, 5000, 1.0).is_none());
        assert_eq!(
            crate::engine::executor::mem_budget::controlled_memory_snapshot().0,
            before
        );
    }

    #[test]
    fn sparse_conditional_domains_allocate_for_observed_values() {
        let mut builder =
            ConditionalKeyDomainBuilder::new(vec!["filter".into()], vec!["id".into()]);
        for i in 0..ConditionalKeyDomainBuilder::MAX_ENTRIES {
            builder.record(
                vec![Value::Int(i as i64)],
                IndexKey::from(vec![KeyPart::from_value(&Value::Int(i as i64))]),
                None,
                false,
                0,
            );
        }
        let allocated: usize = builder
            .entries
            .values()
            .map(|entry| {
                entry.key_distinct.sketch.capacity()
                    * (std::mem::size_of::<(u64, PageIncidence)>() + 1)
                    + entry.key_distinct.max_hashes.capacity() * std::mem::size_of::<u64>()
            })
            .sum();
        assert!(
            allocated < 1024 * 1024,
            "sparse sketches allocated {allocated} bytes"
        );
        let stats = builder.finish(false, 1.0, 2048, 1.0).unwrap();
        assert_eq!(stats.entries.len(), 2048);
        assert!(stats
            .entries
            .iter()
            .all(|entry| entry.key_distinct_count == 1));
    }

    #[test]
    fn conditional_domain_pressure_discards_partial_stats_and_releases_memory() {
        use crate::engine::executor::mem_budget::{
            controlled_memory_snapshot, operational_memory_limit_bytes,
        };
        let before = controlled_memory_snapshot().0;
        let mut builder =
            ConditionalKeyDomainBuilder::new(vec!["filter".into()], vec!["id".into()]);
        builder.record(
            vec![Value::Int(1)],
            IndexKey::from(vec![KeyPart::Int(1)]),
            None,
            false,
            0,
        );
        let charged = controlled_memory_snapshot().0;
        assert!(charged > before);
        let filler = ControlledMemoryCharge::try_new_optional(
            ControlledMemoryCategory::SystemMetadata,
            operational_memory_limit_bytes() - charged,
        )
        .unwrap();
        builder.record(
            vec![Value::Int(2)],
            IndexKey::from(vec![KeyPart::Int(2)]),
            None,
            false,
            0,
        );
        assert!(builder.is_overflowed());
        assert!(builder.entries.is_empty());
        assert!(builder.finish(false, 1.0, 2, 1.0).is_none());
        drop(filler);
        assert_eq!(controlled_memory_snapshot().0, before);
    }

    #[test]
    fn lazy_sketch_merge_matches_unsplit_stream() {
        let mut all = DistinctStatsBuilder::<KeyPart>::new();
        let mut left = DistinctStatsBuilder::<KeyPart>::new();
        let mut right = DistinctStatsBuilder::<KeyPart>::new();
        assert_eq!(all.sketch.capacity(), 0);
        assert_eq!(all.max_hashes.capacity(), 0);
        for i in 0..20000 {
            let key = KeyPart::from_value(&Value::Int(i % 7000));
            all.record(&key, false, i as u32 / 100);
            if i < 10000 {
                left.record(&key, false, i as u32 / 100);
            } else {
                right.record(&key, false, i as u32 / 100);
            }
        }
        left.merge(right);
        assert_eq!(left.sketch.len(), ANALYZE_DISTINCT_SKETCH_SIZE);
        assert_eq!(
            left.finish(false, 1.0, 20000, 0.25),
            all.finish(false, 1.0, 20000, 0.25)
        );
    }
}

/// MySQL-compatible histogram JSON; exact decimal endpoints are written as
/// JSON numbers directly to avoid rounding them through an IEEE double.
pub(crate) fn sql_histogram(
    mut values: Vec<Value>,
    column: &super::ColumnSchema,
    bucket_count: usize,
    population: u64,
) -> String {
    use super::DataTypeKind as T;
    let total = values.len();
    values.retain(|v| !matches!(v, Value::Null));
    let nulls = total - values.len();
    let collation = column
        .collation
        .as_deref()
        .unwrap_or(crate::charset::DEFAULT_COLLATION);
    let compare = |a: &Value, b: &Value| match (a, b) {
        (Value::String(_) | Value::SmallString(_), Value::String(_) | Value::SmallString(_)) => {
            crate::charset::compare_strings(
                collation,
                &a.to_display_string(),
                &b.to_display_string(),
            )
        }
        _ => compare_stat_values(a, b),
    };
    values.sort_by(compare);
    let mut groups: Vec<(Value, usize)> = Vec::new();
    for value in values {
        if let Some((previous, count)) = groups
            .last_mut()
            .filter(|(v, _)| compare(v, &value).is_eq())
        {
            let _ = previous;
            *count += 1;
        } else {
            groups.push((value, 1));
        }
    }
    let data_type = match column.data_type {
        T::TinyInt | T::SmallInt | T::MediumInt | T::Int | T::BigInt | T::Bool | T::Bit { .. } => {
            if column.unsigned {
                "uint"
            } else {
                "int"
            }
        }
        T::Float { .. } | T::Double { .. } => "double",
        T::Decimal { .. } => "decimal",
        T::Date => "date",
        T::Time { .. } => "time",
        T::DateTime { .. } | T::Timestamp { .. } => "datetime",
        T::Year => "uint",
        T::Enum { .. } => "enum",
        T::Set { .. } => "set",
        _ => "string",
    };
    let endpoint = |v: &Value| -> String {
        match v {
            Value::Int(_)
            | Value::UInt(_)
            | Value::Float(_)
            | Value::Double(_)
            | Value::Decimal(_)
            | Value::DecimalCompact(..)
            | Value::Decimal128(..)
            | Value::DecimalWide(_)
            | Value::Year(_) => v.to_display_string(),
            Value::Bytes(bytes) => {
                use base64::Engine;
                serde_json::to_string(&format!(
                    "base64:type254:{}",
                    base64::engine::general_purpose::STANDARD.encode(bytes)
                ))
                .unwrap()
            }
            _ => serde_json::to_string(&v.to_display_string()).unwrap(),
        }
    };
    let singleton = groups.len() <= bucket_count;
    let mut buckets = Vec::new();
    let mut cumulative = 0;
    if singleton {
        for (v, count) in &groups {
            cumulative += count;
            buckets.push(format!(
                "[{},{}]",
                endpoint(v),
                cumulative as f64 / total.max(1) as f64
            ));
        }
    } else {
        let non_null = total - nulls;
        let mut lower = 0;
        for (i, (_, count)) in groups.iter().enumerate() {
            cumulative += count;
            if cumulative * bucket_count >= (buckets.len() + 1) * non_null || i + 1 == groups.len()
            {
                buckets.push(format!(
                    "[{},{},{},{}]",
                    endpoint(&groups[lower].0),
                    endpoint(&groups[i].0),
                    cumulative as f64 / total.max(1) as f64,
                    i + 1 - lower
                ));
                lower = i + 1;
            }
        }
    }
    let updated = chrono::Utc::now()
        .format("%Y-%m-%d %H:%M:%S%.6f")
        .to_string();
    format!("{{\"buckets\":[{}],\"data-type\":\"{}\",\"null-values\":{},\"last-updated\":\"{}\",\"sampling-rate\":{},\"histogram-type\":\"{}\",\"number-of-buckets-specified\":{},\"collation-id\":{},\"auto-update\":false}}",
        buckets.join(","), data_type, nulls as f64 / total.max(1) as f64, updated,
        if population == 0 { 1.0 } else { total as f64 / population as f64 },
        if singleton { "singleton" } else { "equi-height" }, bucket_count, crate::charset::collation_id(collation))
}
