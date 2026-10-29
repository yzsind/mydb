// Conservative per-zone min/max metadata.
//
// A zone map is a core query acceleration structure, on par with B-tree
// indexes, and is maintained automatically for every persistent user table.
// Correctness never depends on summary freshness: every mutation only
// widens an interval; missing/corrupt metadata and values that cannot
// be represented are marked unknown.  Consequently a query may read too
// many heap pages, but can never omit a row that MVCC could make visible.

#[cfg(test)]
const ZONE_MAP_DELTA_MAGIC: &[u8; 4] = b"ZMD1";
#[cfg(test)]
const ZONE_MAP_DELTA_V2_MAGIC: &[u8; 4] = b"ZMD2";
#[cfg(test)]
const ZONE_MAP_DELTA_V3_MAGIC: &[u8; 4] = b"ZMD3";
#[cfg(test)]
const ZONE_MAP_DELTA_V4_MAGIC: &[u8; 4] = b"ZMD4";
pub(crate) use crate::storage::format_versions::ZONE_MAP_VERSION;
// Semantic v5 fixes prefix completeness and MVCC membership publication.
// Older summaries may already contain false-negative bounds; invalidate them
// through the schema fingerprint even though sidecar framing is unchanged.
const ZONE_MAP_SEMANTIC_VERSION: u32 = 5;
#[cfg(test)]
const ZONE_MAP_LZ4_MIN_BYTES: usize = 4096;
#[cfg(test)]
const ZONE_MAP_LZ4_SAVINGS_NUMERATOR: usize = 7;
#[cfg(test)]
const ZONE_MAP_LZ4_SAVINGS_DENOMINATOR: usize = 8;
const ZONE_STRING_PREFIX_CHARS: usize = 16;
/// Membership summaries complement min/max on physically unclustered
/// columns.  A zone starts with a tiny exact fingerprint set and promotes to
/// a fixed-size Bloom filter once its local NDV grows beyond the inline cap.
/// Fingerprint collisions and Bloom false positives can only retain extra
/// pages; equal SQL values always produce the same fingerprint.
const ZONE_MEMBERSHIP_EXACT_LIMIT: usize = 32;
const ZONE_MEMBERSHIP_MIN_BLOOM_WORDS: usize = 128; // 1 KiB.
const ZONE_MEMBERSHIP_MAX_BLOOM_WORDS: usize = 1024; // 8 KiB.
const ZONE_MEMBERSHIP_DEFAULT_BLOOM_HASHES: u8 = 5;
// With the maximum 8 KiB provisional bitmap, three probes stay below the 5%
// FPR target at the worst supported ~7 bits/value layout. Using the final
// default of five before NDV is known only adds two probes per row; the final
// layout check below still rejects any provisional bitmap that misses its
// measured target.
const ZONE_MEMBERSHIP_PROVISIONAL_BLOOM_HASHES: u8 = 3;
const ZONE_MEMBERSHIP_MAX_BLOOM_HASHES: u8 = 8;
/// Keep all probes for one value inside one 512-bit block. The former global
/// double-hash pattern touched up to eight unrelated cache lines per cell and
/// dominated synchronous ANALYZE on wide fact tables. Block selection and
/// within-block probes are persisted semantics, hence the v5 sidecar bump.
const ZONE_MEMBERSHIP_BLOOM_BLOCK_WORDS: usize = 8;
// Membership is an additive per-column acceleration layered on exact
// per-column min/max.
// Bound it to the eight highest-benefit non-leading columns so a wide table
// does not pay several Bloom probes for every cell during every synchronous
// ANALYZE. Unselected columns retain exact min/max and therefore remain fully
// correctness-safe; they merely cannot prove an in-range equality absent.
const ZONE_MEMBERSHIP_MAX_COLUMNS: usize = 8;
/// Large DML batches keep cheap scalar summaries synchronous but defer
/// collation-weighted string summaries to maintenance. Marking those columns
/// Unknown is conservative, and avoids making bulk ingest pay one collation
/// transform per string cell on the foreground connection.
const ZONE_MAP_DEFER_EXPENSIVE_BATCH_ROWS: usize = 128;
const ZONE_MAP_DEFER_ALL_BATCH_ROWS: usize = 256;
const ZONE_MAP_MAINTENANCE_QUIET_MS: u64 = 30_000;
static ZONE_MAP_QUIET_MS: AtomicU64 = AtomicU64::new(ZONE_MAP_MAINTENANCE_QUIET_MS);
static ZONE_MAP_RETRY_MS: AtomicU64 = AtomicU64::new(30_000);
/// Bound one targeted maintenance batch. Besides limiting background I/O,
/// this caps the transient `BTreeSet` used to schedule repairs when a trusted
/// sidecar has holes (for example after an older bulk-import path published
/// only the zones it touched).
const ZONE_MAP_MAINTENANCE_MAX_ZONES_PER_PASS: usize = 64;
const ZONE_MAP_MAINTENANCE_PASSES_PER_ROUND: usize = 10;
const ZONE_MAP_MERGE_LATCHES: usize = 64;
#[cfg(test)]
static ZONE_MAP_TEST_MAINTENANCE_QUIET_MS: AtomicU64 =
    AtomicU64::new(ZONE_MAP_MAINTENANCE_QUIET_MS);
static ZONE_MAP_MAINTENANCE: AtomicBool = AtomicBool::new(true);
static ZONE_MAP_REBUILD_RATIO_BITS: AtomicU64 = AtomicU64::new(0.20_f64.to_bits());
static ZONE_MAP_CACHE_HITS: AtomicU64 = AtomicU64::new(0);
static ZONE_MAP_CACHE_MISSES: AtomicU64 = AtomicU64::new(0);
static ZONE_MAP_CACHE_CLOCK: AtomicU64 = AtomicU64::new(1);
static ZONE_MAP_ZONES_SKIPPED: AtomicU64 = AtomicU64::new(0);
static ZONE_MAP_MEMBERSHIP_CHECKS: AtomicU64 = AtomicU64::new(0);
static ZONE_MAP_MEMBERSHIP_REJECTIONS: AtomicU64 = AtomicU64::new(0);
static ZONE_MAP_MEMBERSHIP_SATURATIONS: AtomicU64 = AtomicU64::new(0);
static ZONE_MAP_MEMORY_FALLBACKS: AtomicU64 = AtomicU64::new(0);
static ZONE_MAP_FALLBACK_UNSUPPORTED: AtomicU64 = AtomicU64::new(0);
static ZONE_MAP_FALLBACK_UNTRUSTED: AtomicU64 = AtomicU64::new(0);
static ZONE_MAP_FALLBACK_CORRUPT: AtomicU64 = AtomicU64::new(0);
static ZONE_MAP_FALLBACK_INVALID_RANGE: AtomicU64 = AtomicU64::new(0);
static ZONE_MAP_CANDIDATE_GENERATIONS: AtomicU64 = AtomicU64::new(0);
static ZONE_MAP_MERGES: AtomicU64 = AtomicU64::new(0);
static ZONE_MAP_REBUILDS: AtomicU64 = AtomicU64::new(0);
static ZONE_MAP_REBUILD_SHRINK_ZONES: AtomicU64 = AtomicU64::new(0);
static ZONE_MAP_REBUILD_UNION_ZONES: AtomicU64 = AtomicU64::new(0);
static ZONE_MAP_MAINTENANCE_LAG_MS: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
static ZONE_MAP_DECODE_FAILURES: AtomicU64 = AtomicU64::new(0);
pub(crate) static ZONE_MAP_METADATA_DECODE_NS: AtomicU64 = AtomicU64::new(0);
pub(crate) static ZONE_MAP_RUNTIME_CANDIDATE_NS: AtomicU64 = AtomicU64::new(0);
pub(crate) static ZONE_MAP_RUNTIME_TASKS: AtomicU64 = AtomicU64::new(0);
pub(crate) static ZONE_MAP_RUNTIME_PAGES: AtomicU64 = AtomicU64::new(0);
pub(crate) static ZONE_MAP_RUNTIME_PAGES_SKIPPED: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
thread_local! {
    static ZONE_MAP_COST_ESTIMATE_CALLS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_zone_map_cost_estimate_calls() {
    ZONE_MAP_COST_ESTIMATE_CALLS.with(|calls| calls.set(0));
}

#[cfg(test)]
pub(crate) fn zone_map_cost_estimate_calls() -> u64 {
    ZONE_MAP_COST_ESTIMATE_CALLS.with(std::cell::Cell::get)
}

#[cfg(test)]
static ZONE_MAP_FAILPOINT_STAGE: AtomicU8 = AtomicU8::new(0);
#[cfg(test)]
static ZONE_MAP_REBUILD_PUBLISH_BARRIER: Mutex<Option<Arc<std::sync::Barrier>>> = Mutex::new(None);
#[cfg(test)]
static ZONE_MAP_REBUILD_SCAN_BARRIER: Mutex<Option<Arc<std::sync::Barrier>>> = Mutex::new(None);

fn zone_map_failpoint(stage: u8) -> std::io::Result<()> {
    #[cfg(test)]
    if ZONE_MAP_FAILPOINT_STAGE.load(Ordering::Acquire) == stage {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("zone-map failpoint stage {stage}"),
        ));
    }
    let _ = stage;
    Ok(())
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

fn zone_map_maintenance_quiet_ms() -> u64 {
    #[cfg(test)]
    return ZONE_MAP_TEST_MAINTENANCE_QUIET_MS.load(Ordering::Relaxed);
    #[cfg(not(test))]
    return ZONE_MAP_QUIET_MS.load(Ordering::Relaxed);
}

/// Select stale and missing zones for one bounded maintenance pass.
///
/// A missing entry is not a correctness bug at read time: candidate
/// generation conservatively retains that zone. It is, however, a permanent
/// performance trap if the map as a whole is trusted, because the old
/// maintenance loop considered only entries already present in `zones`.
/// Treat holes below the current physical heap watermark exactly like stale
/// summaries so the targeted rebuild materialises either an exact summary or
/// an explicit Empty zone.
#[cfg(test)]
fn zone_map_maintenance_targets(
    zones: &BTreeMap<u64, ZoneSummary>,
    total_zones: u64,
    rebuild_ratio: f64,
    required_columns: &BTreeSet<u32>,
) -> BTreeSet<u64> {
    zone_map_maintenance_targets_in_range(
        zones,
        0,
        total_zones,
        rebuild_ratio,
        required_columns,
        &BTreeSet::new(),
    )
}

/// `membership_columns` are the columns whose zones carry membership. A
/// ready zone holding values of such a column without a membership record
/// had its filter dropped by foreground DML (see
/// `merge_membership_record_foreground`); rebuilding restores it. Builders
/// only omit membership together with an Unknown column state, which is
/// targeted anyway, so a rebuilt zone never qualifies again by itself.
fn zone_map_maintenance_targets_in_range<Z: AsRef<ZoneSummary>>(
    zones: &BTreeMap<u64, Z>,
    first_zone: u64,
    total_zones: u64,
    rebuild_ratio: f64,
    required_columns: &BTreeSet<u32>,
    membership_columns: &BTreeSet<u32>,
) -> BTreeSet<u64> {
    let mut targets = BTreeSet::new();
    let mut expected_zone = first_zone;
    for (&zone_id, zone) in zones.range(first_zone..total_zones) {
        let zone = zone.as_ref();
        while expected_zone < zone_id && targets.len() < ZONE_MAP_MAINTENANCE_MAX_ZONES_PER_PASS {
            targets.insert(expected_zone);
            expected_zone = expected_zone.saturating_add(1);
        }
        if targets.len() == ZONE_MAP_MAINTENANCE_MAX_ZONES_PER_PASS {
            return targets;
        }
        expected_zone = zone_id.saturating_add(1);
        let invalid_state = matches!(
            zone.state,
            Some(ZoneMapState::Unknown | ZoneMapState::Building)
        );
        let invalid_columns = zone.state != Some(ZoneMapState::Empty)
            && required_columns.iter().any(|column_id| {
                zone.columns.get(column_id).is_none_or(|summary| {
                    matches!(
                        summary.state,
                        ZoneMapState::Unknown | ZoneMapState::Building
                    )
                })
            });
        let dropped_membership = zone.state != Some(ZoneMapState::Empty)
            && membership_columns.iter().any(|column_id| {
                zone.columns.get(column_id).is_some_and(|summary| {
                    summary.state == ZoneMapState::Ready
                        && summary.has_non_null
                        && summary.membership.is_none()
                })
            });
        if invalid_state
            || invalid_columns
            || dropped_membership
            || zone.stale_mutations
                >= 64.max((zone.row_count_estimate as f64 * rebuild_ratio).ceil() as u64)
        {
            targets.insert(zone_id);
        }
        if targets.len() == ZONE_MAP_MAINTENANCE_MAX_ZONES_PER_PASS {
            return targets;
        }
    }
    while expected_zone < total_zones && targets.len() < ZONE_MAP_MAINTENANCE_MAX_ZONES_PER_PASS {
        targets.insert(expected_zone);
        expected_zone = expected_zone.saturating_add(1);
    }
    targets
}

/// Physical-work summary used by the optimizer.  `surviving_pages` and
/// `total_pages` deliberately come from the same heap-page scale; callers
/// must use their ratio when applying the estimate to a statistics-derived
/// scan cost.  Mixing a physical candidate-page count with
/// `estimated_heap_pages(rows, width)` makes wide tables look far more
/// selective than they are.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ZoneMapCostEstimate {
    pub surviving_pages: u64,
    pub total_pages: u64,
    pub ranges: usize,
    /// Number of zone summaries inspected while producing this estimate.
    /// Large maps are sampled, so this is not necessarily the total number
    /// of zones in the table.
    pub zones_examined: u64,
    /// Logical summary record bytes inspected by the bounded sample.
    pub metadata_read_bytes: u64,
    /// Estimated cold physical pages needed by the sampled predicate.
    pub metadata_cache_misses: u64,
    /// Physical storage objects (base table/partitions) considered.
    pub storage_objects: u64,
    pub exact: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ZoneMapFallbackReason {
    Unsupported,
    Untrusted,
    Corrupt,
    MemoryPressure,
    InvalidRange,
}

/// Owned candidate ranges and their temporary-memory reservation. Moving
/// this object to an executor thread, or consuming its iterator, keeps the
/// reservation alive until the last range allocation is released.
#[derive(Default)]
pub struct ZoneMapRanges {
    ranges: smallvec::SmallVec<[(u64, u64); 1]>,
    lease: ZoneScratchLease,
}
impl ZoneMapRanges {
    pub(crate) fn full(page_count: u64) -> Self {
        if page_count == 0 {
            Self::default()
        } else {
            Self {
                ranges: smallvec::smallvec![(0, page_count)],
                lease: ZoneScratchLease::default(),
            }
        }
    }
}
impl std::ops::Deref for ZoneMapRanges {
    type Target = [(u64, u64)];
    fn deref(&self) -> &Self::Target {
        &self.ranges
    }
}
impl std::fmt::Debug for ZoneMapRanges {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.ranges.fmt(f)
    }
}
impl PartialEq for ZoneMapRanges {
    fn eq(&self, other: &Self) -> bool {
        self.ranges == other.ranges
    }
}
impl Eq for ZoneMapRanges {}
impl PartialEq<Vec<(u64, u64)>> for ZoneMapRanges {
    fn eq(&self, other: &Vec<(u64, u64)>) -> bool {
        self.ranges.as_slice() == other.as_slice()
    }
}
pub struct ZoneMapRangeIter {
    ranges: smallvec::IntoIter<[(u64, u64); 1]>,
    _lease: ZoneScratchLease,
}
impl Iterator for ZoneMapRangeIter {
    type Item = (u64, u64);
    fn next(&mut self) -> Option<Self::Item> {
        self.ranges.next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.ranges.size_hint()
    }
}
impl DoubleEndedIterator for ZoneMapRangeIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.ranges.next_back()
    }
}
impl ExactSizeIterator for ZoneMapRangeIter {}
impl std::iter::FusedIterator for ZoneMapRangeIter {}
impl IntoIterator for ZoneMapRanges {
    type Item = (u64, u64);
    type IntoIter = ZoneMapRangeIter;
    fn into_iter(self) -> Self::IntoIter {
        ZoneMapRangeIter {
            ranges: self.ranges.into_iter(),
            _lease: self.lease,
        }
    }
}
impl<'a> IntoIterator for &'a ZoneMapRanges {
    type Item = &'a (u64, u64);
    type IntoIter = std::slice::Iter<'a, (u64, u64)>;
    fn into_iter(self) -> Self::IntoIter {
        self.ranges.iter()
    }
}

enum ZoneMapCandidateResult {
    Ranges(ZoneMapRanges),
    Fallback(ZoneMapFallbackReason),
}

impl ZoneMapCandidateResult {
    fn ranges(self) -> Option<ZoneMapRanges> {
        match self {
            Self::Ranges(ranges) => Some(ranges),
            Self::Fallback(reason) => {
                match reason {
                    ZoneMapFallbackReason::Unsupported => {
                        ZONE_MAP_FALLBACK_UNSUPPORTED.fetch_add(1, Ordering::Relaxed);
                    }
                    ZoneMapFallbackReason::Untrusted => {
                        ZONE_MAP_FALLBACK_UNTRUSTED.fetch_add(1, Ordering::Relaxed);
                    }
                    ZoneMapFallbackReason::Corrupt => {
                        ZONE_MAP_FALLBACK_CORRUPT.fetch_add(1, Ordering::Relaxed);
                    }
                    ZoneMapFallbackReason::MemoryPressure => {
                        ZONE_MAP_MEMORY_FALLBACKS.fetch_add(1, Ordering::Relaxed);
                    }
                    ZoneMapFallbackReason::InvalidRange => {
                        ZONE_MAP_FALLBACK_INVALID_RANGE.fetch_add(1, Ordering::Relaxed);
                    }
                }
                None
            }
        }
    }
}

/// Work that every full Zone Map estimate must perform after metadata has
/// proved usable.  This preview deliberately does not read any zone values:
/// the optimizer can compare its unavoidable predicate-evaluation CPU with
/// an already-valid access path before expanding the expensive candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ZoneMapCostLowerBound {
    pub zones_examined: u64,
    /// The fewest surviving pages the full estimate can report. A sampled
    /// estimate never proves an unsampled stride empty, and an untrusted
    /// partition always survives whole, so even a predicate matching nothing
    /// leaves these pages to scan.
    pub min_surviving_pages: u64,
    /// Physical heap pages the full estimate divides surviving pages by.
    pub total_pages: u64,
}

#[inline]
fn zone_map_sample_geometry(page_count: u64, zone_pages: u64) -> (u64, u64, u64) {
    if page_count == 0 {
        return (0, 1, 0);
    }
    let total_zones = page_count.div_ceil(zone_pages.max(1));
    let sample_cap = if total_zones < 64 {
        total_zones
    } else {
        4096.min(total_zones)
    };
    let step = total_zones.div_ceil(sample_cap).max(1);
    let samples = total_zones.div_ceil(step);
    (total_zones, step, samples)
}

/// The fewest surviving pages `zone_map_cost_estimate_for_storage` reports
/// for a trusted map, whatever the predicate. An exact (unsampled) estimate
/// can prove every zone empty; a sampled one keeps a full stride when no
/// sample survives and scales a single survivor to a `1/samples` share.
fn zone_map_min_sampled_surviving_pages(page_count: u64, zone_pages: u64) -> u64 {
    let (total_zones, step, samples) = zone_map_sample_geometry(page_count, zone_pages);
    if page_count == 0 || (step == 1 && samples == total_zones) {
        return 0;
    }
    let stride = step.saturating_mul(zone_pages.max(1)).min(page_count);
    stride.min(((page_count as f64) / samples.max(1) as f64).ceil() as u64)
}

type ZoneMapsMap = hashbrown::HashMap<u64, Arc<ZoneMap>, ahash::RandomState>;
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ZoneMapState {
    Unknown,
    Ready,
    Building,
    Empty,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) enum ZoneScalar {
    Signed(i64),
    Unsigned(u64),
    Float32(u32),
    Float64(u64),
    Decimal(String),
    Temporal(i64),
    Binary { prefix: Vec<u8>, truncated: bool },
    Text { weight: Vec<u8>, truncated: bool },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum ZoneMembership {
    Exact {
        hashes: Vec<u64>,
        bloom_words: u16,
        bloom_hashes: u8,
    },
    Bloom {
        words: Vec<u64>,
        hashes: u8,
        // Runtime-only acceleration. The sidecar encoding deliberately omits
        // this field; decode/merge calculate it once from the persisted bits.
        #[serde(skip, default)]
        set_bits: u32,
    },
    Saturated,
}

impl ZoneMembership {
    /// Union only adds exact hashes or Bloom bits. Together with the layout,
    /// this token detects a persisted membership change without copying or
    /// comparing a potentially large bitmap on every row mutation.
    fn union_change_token(&self) -> (u8, usize, u32, u8) {
        match self {
            Self::Exact {
                hashes,
                bloom_words,
                bloom_hashes,
            } => (0, hashes.len(), u32::from(*bloom_words), *bloom_hashes),
            Self::Bloom {
                words,
                set_bits,
                hashes,
            } => (1, words.len(), *set_bits, *hashes),
            Self::Saturated => (2, 0, 0, 0),
        }
    }

    fn diagnostic(&self) -> (&'static str, usize, u8, bool) {
        match self {
            Self::Exact {
                hashes,
                bloom_hashes,
                ..
            } => (
                "EXACT",
                hashes.len() * std::mem::size_of::<u64>(),
                *bloom_hashes,
                false,
            ),
            Self::Bloom { words, hashes, .. } => (
                "BLOOM",
                words.len() * std::mem::size_of::<u64>(),
                *hashes,
                false,
            ),
            Self::Saturated => ("SATURATED", 0, 0, true),
        }
    }

    fn saturate(&mut self) {
        if !matches!(self, Self::Saturated) {
            ZONE_MAP_MEMBERSHIP_SATURATIONS.fetch_add(1, Ordering::Relaxed);
        }
        *self = Self::Saturated;
    }
    #[cfg(test)]
    fn empty() -> Self {
        Self::empty_with_layout(ZoneMembershipLayout::default())
    }

    fn empty_with_layout(layout: ZoneMembershipLayout) -> Self {
        Self::Exact {
            hashes: Vec::new(),
            bloom_words: layout.words as u16,
            bloom_hashes: layout.hashes,
        }
    }

    fn set_layout_if_exact(&mut self, layout: ZoneMembershipLayout) {
        if let Self::Exact {
            bloom_words,
            bloom_hashes,
            ..
        } = self
        {
            *bloom_words = layout.words as u16;
            *bloom_hashes = layout.hashes;
        }
    }

    #[inline]
    fn bloom_positions(hash: u64, words: usize, hashes: u8) -> impl Iterator<Item = usize> {
        let h1 = hash;
        let h2 = hash.rotate_left(32) | 1;
        debug_assert!(words.is_power_of_two());
        debug_assert!(words >= ZONE_MEMBERSHIP_BLOOM_BLOCK_WORDS);
        let block_count = words / ZONE_MEMBERSHIP_BLOOM_BLOCK_WORDS;
        debug_assert!(block_count.is_power_of_two());
        let block = hash.rotate_left(17) as usize & (block_count - 1);
        let block_bit_base = block * ZONE_MEMBERSHIP_BLOOM_BLOCK_WORDS * 64;
        let block_bit_mask = ZONE_MEMBERSHIP_BLOOM_BLOCK_WORDS * 64 - 1;
        (0..hashes).map(move |i| {
            block_bit_base
                + (h1.wrapping_add(u64::from(i).wrapping_mul(h2)) as usize & block_bit_mask)
        })
    }

    fn bloom_from_hashes(
        values: impl IntoIterator<Item = u64>,
        word_count: usize,
        hash_count: u8,
    ) -> Vec<u64> {
        let mut words = vec![0u64; word_count];
        for hash in values {
            for bit in Self::bloom_positions(hash, word_count, hash_count) {
                words[bit >> 6] |= 1u64 << (bit & 63);
            }
        }
        words
    }

    fn valid_layout(words: usize, hashes: u8) -> bool {
        words.is_power_of_two()
            && (ZONE_MEMBERSHIP_MIN_BLOOM_WORDS..=ZONE_MEMBERSHIP_MAX_BLOOM_WORDS).contains(&words)
            && (1..=ZONE_MEMBERSHIP_MAX_BLOOM_HASHES).contains(&hashes)
    }

    fn count_set_bits(words: &[u64]) -> u32 {
        words.iter().map(|word| word.count_ones()).sum()
    }

    fn set_bits_saturated(set_bits: u32, word_count: usize) -> bool {
        u64::from(set_bits).saturating_mul(5)
            >= (word_count as u64).saturating_mul(64).saturating_mul(4)
    }

    fn is_saturated(words: &[u64]) -> bool {
        Self::set_bits_saturated(Self::count_set_bits(words), words.len())
    }

    #[inline]
    fn insert_bloom_hash(words: &mut [u64], hashes: u8, set_bits: &mut u32, hash: u64) {
        debug_assert!(words.len().is_power_of_two());
        debug_assert!(words.len() >= ZONE_MEMBERSHIP_BLOOM_BLOCK_WORDS);
        let block_count = words.len() / ZONE_MEMBERSHIP_BLOOM_BLOCK_WORDS;
        let block = hash.rotate_left(17) as usize & (block_count - 1);
        let block_bit_base = block * ZONE_MEMBERSHIP_BLOOM_BLOCK_WORDS * 64;
        let block_bit_mask = ZONE_MEMBERSHIP_BLOOM_BLOCK_WORDS * 64 - 1;
        let step = hash.rotate_left(32) | 1;
        let mut position = hash;
        for _ in 0..hashes {
            let bit = block_bit_base + (position as usize & block_bit_mask);
            let word = &mut words[bit >> 6];
            let mask = 1u64 << (bit & 63);
            if *word & mask == 0 {
                *word |= mask;
                *set_bits = set_bits.saturating_add(1);
            }
            position = position.wrapping_add(step);
        }
    }

    fn false_positive_rate(words: usize, hashes: u8, values: u64) -> f64 {
        if values == 0 {
            return 0.0;
        }
        let bits = (words * 64) as f64;
        let k = f64::from(hashes);
        (1.0 - (-k * values as f64 / bits).exp()).powf(k)
    }

    fn meets_layout(&self, layout: ZoneMembershipLayout, local_ndv: u64) -> bool {
        match self {
            Self::Exact { .. } => true,
            Self::Bloom {
                words,
                hashes,
                set_bits,
            } if Self::valid_layout(words.len(), *hashes) => {
                let actual_set_bits = if *set_bits == 0 {
                    Self::count_set_bits(words)
                } else {
                    *set_bits
                };
                let actual_fpr = (f64::from(actual_set_bits) / (words.len() * 64) as f64)
                    .powi(i32::from(*hashes));
                !Self::set_bits_saturated(actual_set_bits, words.len())
                    && actual_fpr
                        <= Self::false_positive_rate(layout.words, layout.hashes, local_ndv)
                            .max(0.05)
            }
            Self::Bloom { .. } | Self::Saturated => false,
        }
    }

    fn prepare_for_layout(&mut self, layout: ZoneMembershipLayout, local_ndv: u64) -> bool {
        if matches!(self, Self::Exact { .. }) {
            self.set_layout_if_exact(layout);
            return true;
        }
        let Self::Bloom {
            words,
            hashes,
            set_bits,
        } = self
        else {
            return false;
        };
        if words.len() > layout.words && words.len() % layout.words == 0 {
            let bytes = layout.words.saturating_mul(std::mem::size_of::<u64>());
            let Some(_fold_charge) =
                crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                    crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                    bytes,
                )
            else {
                return false;
            };
            let mut folded = Vec::new();
            if folded.try_reserve_exact(layout.words).is_err() {
                return false;
            }
            folded.resize(layout.words, 0u64);
            for (idx, word) in words.iter().copied().enumerate() {
                folded[idx % layout.words] |= word;
            }
            let folded_set_bits = Self::count_set_bits(&folded);
            let folded_fpr =
                (f64::from(folded_set_bits) / (layout.words * 64) as f64).powi(i32::from(*hashes));
            if Self::set_bits_saturated(folded_set_bits, folded.len())
                || folded_fpr
                    > Self::false_positive_rate(layout.words, layout.hashes, local_ndv).max(0.05)
            {
                return false;
            }
            *words = folded;
            *set_bits = folded_set_bits;
        }
        self.meets_layout(layout, local_ndv)
    }

    fn insert_hash(&mut self, hash: u64) {
        match self {
            Self::Exact {
                hashes,
                bloom_words,
                bloom_hashes,
            } => {
                match hashes.binary_search(&hash) {
                    Ok(_) => return,
                    Err(pos) if hashes.len() < ZONE_MEMBERSHIP_EXACT_LIMIT => {
                        hashes.insert(pos, hash);
                        return;
                    }
                    Err(_) => {}
                }
                let word_count = usize::from(*bloom_words);
                let hash_count = *bloom_hashes;
                if !Self::valid_layout(word_count, hash_count) {
                    self.saturate();
                    return;
                }
                let mut bloom =
                    Self::bloom_from_hashes(hashes.iter().copied(), word_count, hash_count);
                let mut set_bits = Self::count_set_bits(&bloom);
                Self::insert_bloom_hash(&mut bloom, hash_count, &mut set_bits, hash);
                *self = if Self::set_bits_saturated(set_bits, bloom.len()) {
                    ZONE_MAP_MEMBERSHIP_SATURATIONS.fetch_add(1, Ordering::Relaxed);
                    Self::Saturated
                } else {
                    Self::Bloom {
                        words: bloom,
                        hashes: hash_count,
                        set_bits,
                    }
                };
            }
            Self::Bloom {
                words,
                hashes,
                set_bits,
            } => {
                if !Self::valid_layout(words.len(), *hashes) {
                    self.saturate();
                    return;
                }
                if *set_bits == 0 {
                    *set_bits = Self::count_set_bits(words);
                }
                Self::insert_bloom_hash(words, *hashes, set_bits, hash);
                if Self::set_bits_saturated(*set_bits, words.len()) {
                    self.saturate();
                }
            }
            Self::Saturated => {}
        }
    }

    fn merge_from(&mut self, incoming: &Self) {
        match incoming {
            Self::Exact { hashes, .. } => {
                for &hash in hashes {
                    self.insert_hash(hash);
                }
            }
            Self::Bloom {
                words: incoming_words,
                hashes: incoming_hashes,
                ..
            } => {
                let compatible = match self {
                    Self::Exact {
                        bloom_words,
                        bloom_hashes,
                        ..
                    } => {
                        usize::from(*bloom_words) == incoming_words.len()
                            && *bloom_hashes == *incoming_hashes
                    }
                    Self::Bloom { words, hashes, .. } => {
                        words.len() == incoming_words.len() && *hashes == *incoming_hashes
                    }
                    Self::Saturated => false,
                };
                if !compatible || !Self::valid_layout(incoming_words.len(), *incoming_hashes) {
                    self.saturate();
                    return;
                }
                if let Self::Exact { hashes, .. } = self {
                    let prior = Self::bloom_from_hashes(
                        hashes.iter().copied(),
                        incoming_words.len(),
                        *incoming_hashes,
                    );
                    *self = Self::Bloom {
                        set_bits: Self::count_set_bits(&prior),
                        words: prior,
                        hashes: *incoming_hashes,
                    };
                }
                let Self::Bloom {
                    words, set_bits, ..
                } = self
                else {
                    unreachable!()
                };
                for (word, incoming) in words.iter_mut().zip(incoming_words) {
                    *word |= *incoming;
                }
                *set_bits = Self::count_set_bits(words);
                if Self::set_bits_saturated(*set_bits, words.len()) {
                    self.saturate();
                }
            }
            Self::Saturated => self.saturate(),
        }
    }

    #[inline]
    #[cfg(test)]
    fn may_contain_hash(&self, hash: u64) -> bool {
        match self {
            Self::Exact { hashes, .. } => hashes.binary_search(&hash).is_ok(),
            Self::Bloom { words, hashes, .. } if Self::valid_layout(words.len(), *hashes) => {
                Self::bloom_positions(hash, words.len(), *hashes)
                    .all(|bit| words[bit >> 6] & (1u64 << (bit & 63)) != 0)
            }
            Self::Bloom { .. } | Self::Saturated => true,
        }
    }

    fn capacity_bytes(&self) -> usize {
        match self {
            Self::Exact { hashes, .. } => {
                hashes.capacity().saturating_mul(std::mem::size_of::<u64>())
            }
            Self::Bloom { words, .. } => {
                words.capacity().saturating_mul(std::mem::size_of::<u64>())
            }
            Self::Saturated => 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ZoneMembershipLayout {
    words: usize,
    hashes: u8,
}

impl Default for ZoneMembershipLayout {
    fn default() -> Self {
        Self {
            words: ZONE_MEMBERSHIP_MIN_BLOOM_WORDS,
            hashes: ZONE_MEMBERSHIP_DEFAULT_BLOOM_HASHES,
        }
    }
}

struct ZoneMembershipHasher(u64);

impl ZoneMembershipHasher {
    #[inline]
    fn new() -> Self {
        Self(0xcbf29ce484222325)
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.0 ^= u64::from(byte);
            self.0 = self.0.wrapping_mul(0x100000001b3);
        }
    }

    #[inline]
    fn write_exact(&mut self, magnitude: i128) {
        self.write(&[0]);
        self.write_magnitude(magnitude);
    }

    /// Preserve the 16-byte little-endian hash stream while folding trailing
    /// zero bytes into one multiplication. Each zero byte in FNV-1a multiplies
    /// by the prime without changing the XOR term.
    #[inline]
    fn write_magnitude(&mut self, magnitude: i128) {
        const PRIME: u64 = 0x100000001b3;
        let bytes = magnitude.to_le_bytes();
        if (0..=u32::MAX as i128).contains(&magnitude) {
            self.write(&bytes[..4]);
            self.0 = self.0.wrapping_mul(PRIME.wrapping_pow(12));
        } else if (0..=u64::MAX as i128).contains(&magnitude) {
            self.write(&bytes[..8]);
            self.0 = self.0.wrapping_mul(PRIME.wrapping_pow(8));
        } else {
            self.write(&bytes);
        }
    }

    /// Hash a compact exact decimal by its canonical numeric identity. Binary
    /// magnitude+scale avoids the repeated u128 division needed to render
    /// millions of decimal strings while preserving 11 == 11.0 == 11.00 and
    /// the cross-domain integer identity used by equality predicates.
    #[inline]
    fn write_decimal_compact(&mut self, mut magnitude: i128, mut scale: u8) {
        // Compact row-codec decimals originate as i64. Keeping their
        // canonicalisation in i64 avoids software i128 division on ARM64;
        // the persistent magnitude/scale byte stream is unchanged.
        if let Ok(mut compact) = i64::try_from(magnitude) {
            while scale > 0 && compact % 10 == 0 {
                compact /= 10;
                scale -= 1;
            }
            magnitude = i128::from(compact);
        } else {
            while scale > 0 && magnitude % 10 == 0 {
                magnitude /= 10;
                scale -= 1;
            }
        }
        if scale == 0 {
            self.write_exact(magnitude);
            return;
        }
        self.write(&[4]);
        self.write_magnitude(magnitude);
        self.write(&[scale]);
    }
}

#[cfg(test)]
mod zone_numeric_hash_tests {
    use super::*;

    #[test]
    fn short_text_prefix_matches_character_oracle_at_unicode_boundaries() {
        for ascii_count in 0..40 {
            for suffix in [
                "",
                "é中文😀",
                "\0ABCDEFGHIJKLMNOPQRSTUVWXYZ",
                "ßééééééééééééééééé",
            ] {
                let value = format!("{}{suffix}", "x".repeat(ascii_count));
                let expected = value
                    .chars()
                    .take(ZONE_STRING_PREFIX_CHARS)
                    .collect::<String>();
                assert_eq!(zone_text_prefix_slice_normalized(&value), expected);
            }
        }
    }

    #[test]
    fn folded_zero_bytes_preserve_every_numeric_hash_byte() {
        let boundaries = [
            i128::MIN,
            -1,
            0,
            1,
            u32::MAX as i128,
            u32::MAX as i128 + 1,
            u64::MAX as i128,
            u64::MAX as i128 + 1,
            i128::MAX,
        ];
        let mut state = 0x91824u128;
        let random = (0..10_000).map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as i128
        });
        for magnitude in boundaries.into_iter().chain(random).chain(0..10_000) {
            for seed in [0, 1, 0xcbf29ce484222325, u64::MAX] {
                let mut expected = ZoneMembershipHasher(seed);
                expected.write(&magnitude.to_le_bytes());
                let mut actual = ZoneMembershipHasher(seed);
                actual.write_magnitude(magnitude);
                assert_eq!(actual.0, expected.0, "magnitude={magnitude}");
            }
            for scale in [0, 1, 2, 9, 18, 38] {
                let mut normalized = magnitude;
                let mut normalized_scale = scale;
                while normalized_scale > 0 && normalized % 10 == 0 {
                    normalized /= 10;
                    normalized_scale -= 1;
                }
                let mut expected = ZoneMembershipHasher::new();
                expected.write(&[if normalized_scale == 0 { 0 } else { 4 }]);
                expected.write(&normalized.to_le_bytes());
                if normalized_scale != 0 {
                    expected.write(&[normalized_scale]);
                }
                let mut actual = ZoneMembershipHasher::new();
                actual.write_decimal_compact(magnitude, scale);
                assert_eq!(actual.0, expected.0, "magnitude={magnitude}, scale={scale}");
            }
        }
    }
}

fn zone_membership_hash(value: &ZoneScalar) -> u64 {
    // Persistent fixed-seed FNV-1a. `DefaultHasher` is deliberately not a
    // storage-format contract and could make a sidecar unusable after a Rust
    // upgrade. Type tags keep equal byte representations of different SQL
    // domains distinct.
    let mut hash = ZoneMembershipHasher::new();
    match value {
        ZoneScalar::Signed(v) => {
            hash.write_exact(i128::from(*v));
        }
        ZoneScalar::Unsigned(v) => {
            hash.write_exact(i128::from(*v));
        }
        ZoneScalar::Float32(v) => {
            hash.write(&[2]);
            let value = f32::from_bits(*v);
            let bits = if value == 0.0 { 0.0f32.to_bits() } else { *v };
            hash.write(&bits.to_le_bytes());
        }
        ZoneScalar::Float64(v) => {
            hash.write(&[3]);
            let value = f64::from_bits(*v);
            let bits = if value == 0.0 { 0.0f64.to_bits() } else { *v };
            hash.write(&bits.to_le_bytes());
        }
        ZoneScalar::Decimal(v) => {
            let canonical =
                crate::util::decimal::canonical_decimal_str(v).unwrap_or_else(|| v.clone());
            if let Some((magnitude, scale)) =
                crate::util::decimal::parse_exact_decimal_str(&canonical)
            {
                hash.write_decimal_compact(magnitude, scale);
            } else {
                // Unparseable metadata is never produced by the current
                // writers, but keep a deterministic domain-separated key for
                // defensive decoding. Predicate compilation will retain the
                // zone when scalar comparison is incompatible.
                hash.write(&[4]);
                hash.write(canonical.as_bytes());
            }
        }
        ZoneScalar::Temporal(v) => {
            hash.write(&[5]);
            hash.write(&v.to_le_bytes());
        }
        ZoneScalar::Binary { prefix, truncated } => {
            hash.write(&[6]);
            hash.write(prefix);
            hash.write(&[u8::from(*truncated)]);
        }
        ZoneScalar::Text { weight, truncated } => {
            hash.write(&[7]);
            hash.write(weight);
            hash.write(&[u8::from(*truncated)]);
        }
    }
    hash.0
}

impl ZoneScalar {
    fn compare(&self, other: &Self) -> Option<std::cmp::Ordering> {
        use ZoneScalar::*;
        Some(match (self, other) {
            (Signed(a), Signed(b)) => a.cmp(b),
            (Unsigned(a), Unsigned(b)) => a.cmp(b),
            (Float32(a), Float32(b)) => f32::from_bits(*a).partial_cmp(&f32::from_bits(*b))?,
            (Float32(a), Float64(b)) => {
                (f32::from_bits(*a) as f64).partial_cmp(&f64::from_bits(*b))?
            }
            (Float64(a), Float32(b)) => {
                f64::from_bits(*a).partial_cmp(&(f32::from_bits(*b) as f64))?
            }
            (Float64(a), Float64(b)) => f64::from_bits(*a).partial_cmp(&f64::from_bits(*b))?,
            (Decimal(a), Decimal(b)) => crate::util::decimal::compare_decimal_str(a, b)?,
            (Temporal(a), Temporal(b)) => a.cmp(b),
            (Binary { prefix: a, .. }, Binary { prefix: b, .. }) => a.cmp(b),
            (Text { weight: a, .. }, Text { weight: b, .. }) => a.cmp(b),
            _ => return None,
        })
    }

    fn was_truncated(&self) -> bool {
        matches!(
            self,
            ZoneScalar::Binary {
                truncated: true,
                ..
            } | ZoneScalar::Text {
                truncated: true,
                ..
            }
        )
    }

    fn display_for_column(&self, column: Option<&ColumnSchema>) -> String {
        if let (Self::Temporal(value), Some(column)) = (self, column) {
            let typed = match column.data_type {
                DataTypeKind::Date => Value::Date(*value as i32),
                DataTypeKind::Time { .. } => Value::Time(*value),
                DataTypeKind::DateTime { .. } | DataTypeKind::Timestamp { .. } => {
                    Value::DateTime(*value)
                }
                DataTypeKind::Year => Value::Year(*value as u8),
                _ => return self.display(),
            };
            return typed.to_display_string();
        }
        self.display()
    }

    fn display(&self) -> String {
        match self {
            ZoneScalar::Signed(v) => v.to_string(),
            ZoneScalar::Unsigned(v) => v.to_string(),
            ZoneScalar::Float32(v) => f32::from_bits(*v).to_string(),
            ZoneScalar::Float64(v) => f64::from_bits(*v).to_string(),
            ZoneScalar::Decimal(value) => value.clone(),
            ZoneScalar::Temporal(v) => v.to_string(),
            ZoneScalar::Binary { prefix, .. } => crate::util::hex::encode_upper(prefix),
            ZoneScalar::Text { weight, .. } => String::from_utf8_lossy(weight).into_owned(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ZoneColumnSummary {
    pub state: ZoneMapState,
    /// Runtime provenance bit. `true` means an Unknown transition is covered
    /// by a committed WAL prefix or trusted sidecar, so later updates can
    /// avoid emitting the same conservative transition again.
    #[serde(skip)]
    durable_unknown: bool,
    pub has_null: bool,
    pub has_non_null: bool,
    pub min: Option<ZoneScalar>,
    pub max: Option<ZoneScalar>,
    pub truncated: bool,
    #[serde(default)]
    pub membership: Option<ZoneMembership>,
}

impl Default for ZoneColumnSummary {
    fn default() -> Self {
        Self {
            state: ZoneMapState::Empty,
            durable_unknown: false,
            has_null: false,
            has_non_null: false,
            min: None,
            max: None,
            truncated: false,
            membership: None,
        }
    }
}

impl ZoneColumnSummary {
    fn mark_unknown(&mut self) {
        self.state = ZoneMapState::Unknown;
        self.durable_unknown = false;
        self.min = None;
        self.max = None;
        self.membership = None;
    }

    fn merge_null(&mut self) {
        self.has_null = true;
        if self.state == ZoneMapState::Empty {
            self.state = ZoneMapState::Ready;
        }
    }

    fn merge_value(&mut self, value: ZoneScalar) {
        self.merge_value_cow(std::borrow::Cow::Owned(value));
    }

    fn merge_value_cow(&mut self, value: std::borrow::Cow<'_, ZoneScalar>) {
        if self.state == ZoneMapState::Unknown {
            return;
        }
        self.state = ZoneMapState::Ready;
        self.has_non_null = true;
        self.truncated |= value.was_truncated();
        match self.min.as_ref().and_then(|old| value.compare(old)) {
            None if self.min.is_some() => return self.mark_unknown(),
            Some(std::cmp::Ordering::Less) | None => self.min = Some(value.as_ref().clone()),
            _ => {}
        }
        match self.max.as_ref().and_then(|old| value.compare(old)) {
            None if self.max.is_some() => self.mark_unknown(),
            Some(std::cmp::Ordering::Greater) | None => self.max = Some(value.into_owned()),
            _ => {}
        }
    }

    /// Observe a complete value, including membership. Endpoint-only merges
    /// must use merge_value after merging the source membership separately.
    #[cfg(test)]
    fn observe_scalar(&mut self, value: ZoneScalar) {
        self.merge_membership_value(&value);
        self.merge_value(value);
    }

    fn merge_summary(&mut self, source: &Self) {
        self.merge_summary_impl(source, true);
    }

    fn merge_summary_impl(&mut self, source: &Self, include_membership: bool) {
        if source.state == ZoneMapState::Unknown {
            let durable = self.durable_unknown || source.durable_unknown;
            self.mark_unknown();
            self.durable_unknown = durable;
            return;
        }
        if self.state == ZoneMapState::Unknown {
            return;
        }
        if source.has_null {
            self.merge_null();
        }
        if let Some(membership) = source.membership.as_ref().filter(|_| include_membership) {
            self.merge_membership(membership);
        } else if source.has_non_null {
            // An incoming envelope without a complete set cannot safely
            // preserve a set that describes only the previous values.
            self.membership = None;
        }
        if let Some(value) = &source.min {
            self.merge_value_cow(std::borrow::Cow::Borrowed(value));
        }
        if let Some(value) = &source.max {
            self.merge_value_cow(std::borrow::Cow::Borrowed(value));
        }
        self.truncated |= source.truncated;
    }

    /// Report changes to the durable summary, not merely an attempted merge.
    /// Bounds and membership are monotone: a value already inside the bounds
    /// cannot move an endpoint, and a union with unchanged cardinality cannot
    /// have changed the set/bitmap. Runtime durable_unknown provenance is not
    /// part of the sidecar payload and must still merge on a no-op.
    #[cfg(test)]
    fn merge_summary_changed(&mut self, source: &Self) -> bool {
        self.merge_summary_changed_impl(source, true)
    }

    fn merge_bounds_changed(&mut self, source: &Self) -> bool {
        debug_assert!(self.membership.is_none());
        self.merge_summary_changed_impl(source, false)
    }

    fn merge_summary_changed_impl(&mut self, source: &Self, include_membership: bool) -> bool {
        let token = |summary: &Self| {
            (
                summary.state,
                summary.has_null,
                summary.has_non_null,
                summary.truncated,
                summary.min.is_some(),
                summary.max.is_some(),
                summary
                    .membership
                    .as_ref()
                    .map(ZoneMembership::union_change_token),
            )
        };
        let before = token(self);
        let bounds_change = self.state != ZoneMapState::Unknown
            && source.state != ZoneMapState::Unknown
            && [source.min.as_ref(), source.max.as_ref()]
                .into_iter()
                .flatten()
                .any(|value| {
                    self.min
                        .as_ref()
                        .and_then(|old| value.compare(old))
                        .is_none_or(|order| order == std::cmp::Ordering::Less)
                        || self
                            .max
                            .as_ref()
                            .and_then(|old| value.compare(old))
                            .is_none_or(|order| order == std::cmp::Ordering::Greater)
                });
        self.merge_summary_impl(source, include_membership);
        bounds_change || before != token(self)
    }

    #[cfg(test)]
    fn enable_membership(&mut self) {
        self.enable_membership_with_layout(ZoneMembershipLayout::default());
    }

    fn enable_membership_with_layout(&mut self, layout: ZoneMembershipLayout) {
        if self.state != ZoneMapState::Unknown && self.membership.is_none() {
            self.membership = Some(ZoneMembership::empty_with_layout(layout));
        }
    }

    fn merge_membership_value(&mut self, value: &ZoneScalar) {
        if let Some(membership) = &mut self.membership {
            membership.insert_hash(zone_membership_hash(value));
        }
    }

    fn merge_membership(&mut self, incoming: &ZoneMembership) {
        if self.state == ZoneMapState::Unknown {
            return;
        }
        match &mut self.membership {
            Some(current) => current.merge_from(incoming),
            // A legacy/unselected summary with existing values cannot be
            // upgraded from a later delta: the incoming set does not cover
            // those old values. Keep membership disabled until a full zone
            // rebuild observes the complete contents.
            None if !self.has_non_null => self.membership = Some(incoming.clone()),
            None => {}
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct ZoneSummary {
    pub state: Option<ZoneMapState>,
    pub columns: BTreeMap<u32, ZoneColumnSummary>,
    pub row_count_estimate: u64,
    pub stale_mutations: u64,
    pub rebuild_scn: u64,
}

impl ZoneSummary {
    /// Union another delta for the same zone into this one, exactly as the
    /// store would observe the two deltas merged one after the other.
    fn absorb(&mut self, incoming: &Self) {
        self.row_count_estimate = self
            .row_count_estimate
            .saturating_add(incoming.row_count_estimate);
        self.stale_mutations = self
            .stale_mutations
            .saturating_add(incoming.stale_mutations);
        self.rebuild_scn = self.rebuild_scn.max(incoming.rebuild_scn);
        if incoming.state == Some(ZoneMapState::Unknown) {
            self.state = Some(ZoneMapState::Unknown);
        }
        for (&column_id, incoming_column) in &incoming.columns {
            self.columns
                .entry(column_id)
                .or_default()
                .merge_summary(incoming_column);
        }
    }
}

// Temporary owned summaries have a separate lease from their source pages.
// Optional reservations deliberately do not latch a terminal query OOM: DML
// can still persist an Unknown root when the derived work cannot be admitted.
#[derive(Debug, Default)]
struct ZoneScratchLease {
    charge: Option<crate::engine::executor::mem_budget::ControlledMemoryCharge>,
}
#[cfg(test)]
static ZONE_MAP_TEST_SCRATCH_LIMIT: AtomicU64 = AtomicU64::new(u64::MAX);

impl ZoneScratchLease {
    fn absorb(&mut self, other: &mut Self) {
        if let Some(mut incoming) = other.charge.take() {
            if let Some(charge) = &mut self.charge {
                charge.absorb(&mut incoming);
            } else {
                self.charge = Some(incoming);
            }
        }
    }

    fn bytes(&self) -> usize {
        self.charge.as_ref().map_or(0, |c| c.bytes())
    }
    fn reserve(&mut self, extra: usize) -> Result<(), QueryError> {
        if extra == 0 {
            return Ok(());
        }
        #[cfg(test)]
        let desired = self.bytes().saturating_add(extra);
        #[cfg(test)]
        if desired as u64 > ZONE_MAP_TEST_SCRATCH_LIMIT.load(Ordering::Relaxed) {
            return Err(Self::denied());
        }
        use crate::engine::executor::mem_budget::{
            ControlledMemoryCategory, ControlledMemoryCharge,
        };
        let admitted = match &mut self.charge {
            Some(charge) => charge.try_grow_optional(extra),
            None => {
                self.charge = ControlledMemoryCharge::try_new_optional(
                    ControlledMemoryCategory::StorageScratch,
                    extra,
                );
                self.charge.is_some()
            }
        };
        if admitted {
            Ok(())
        } else {
            Err(Self::denied())
        }
    }
    fn shrink_to(&mut self, desired: usize) {
        debug_assert!(
            desired <= self.bytes(),
            "zone scratch must be reserved before allocation"
        );
        if let Some(charge) = &mut self.charge {
            charge.shrink(charge.bytes().saturating_sub(desired));
        }
    }
    fn denied() -> QueryError {
        ZONE_MAP_MEMORY_FALLBACKS.fetch_add(1, Ordering::Relaxed);
        error(
            crate::error::catalog::ER_OUTOFMEMORY,
            "zone-map temporary memory admission failed",
        )
    }
}

// std B-tree nodes contain up to 11 keys/values and 12 child links; every
// non-root node has at least 5 keys. This conservative allocator-independent
// bound covers node capacity, rather than charging only live entries.
fn zone_reserve_vec<T>(
    values: &mut Vec<T>,
    lease: &mut ZoneScratchLease,
    additional: usize,
) -> Result<(), QueryError> {
    let needed = values.len().saturating_add(additional);
    if needed <= values.capacity() {
        return Ok(());
    }
    let next = needed
        .checked_next_power_of_two()
        .unwrap_or(usize::MAX)
        .max(4);
    let before = lease.bytes();
    lease.reserve(
        next.saturating_sub(values.capacity())
            .saturating_mul(std::mem::size_of::<T>()),
    )?;
    if values
        .try_reserve_exact(next.saturating_sub(values.len()))
        .is_err()
    {
        lease.shrink_to(before);
        return Err(ZoneScratchLease::denied());
    }
    Ok(())
}
fn zone_tree_bytes<K, V>(entries: usize) -> usize {
    if entries == 0 {
        return 0;
    }
    (128usize
        .saturating_add(
            11usize
                .saturating_mul(std::mem::size_of::<K>().saturating_add(std::mem::size_of::<V>())),
        )
        .saturating_add(12 * std::mem::size_of::<usize>()))
    .saturating_mul(1 + entries / 5)
}
fn zone_scalar_owned_bytes(value: &ZoneScalar) -> usize {
    match value {
        ZoneScalar::Decimal(value) => value.capacity(),
        ZoneScalar::Binary { prefix, .. } => prefix.capacity(),
        ZoneScalar::Text { weight, .. } => weight.capacity(),
        _ => 0,
    }
}
fn zone_value_owned_bytes(value: &Value) -> usize {
    match value {
        Value::String(text) | Value::Decimal(text) => text.capacity(),
        Value::Bytes(bytes) | Value::Json(bytes) | Value::Geometry(bytes) => bytes.capacity(),
        Value::Vector(bytes) => bytes.capacity(),
        _ => value.decimal_heap_bytes(),
    }
}
fn zone_backfill_owned_bytes(column: &ColumnSchema) -> usize {
    column
        .read_backfill_default
        .as_ref()
        .or(column.default.as_ref())
        .map_or_else(
            || {
                if column.nullable || column.default_expr.is_some() {
                    return 0;
                }
                match &column.data_type {
                    DataTypeKind::Enum { values } => values.first().map_or(0, String::len),
                    DataTypeKind::Date
                    | DataTypeKind::Time { .. }
                    | DataTypeKind::DateTime { .. }
                    | DataTypeKind::Timestamp { .. } => 32,
                    _ => 0,
                }
            },
            zone_value_owned_bytes,
        )
}
fn zone_column_owned_bytes(column: &ZoneColumnSummary) -> usize {
    column
        .min
        .as_ref()
        .map_or(0, zone_scalar_owned_bytes)
        .saturating_add(column.max.as_ref().map_or(0, zone_scalar_owned_bytes))
        .saturating_add(
            column
                .membership
                .as_ref()
                .map_or(0, ZoneMembership::capacity_bytes),
        )
}
fn zone_owned_bytes(zone: &ZoneSummary) -> usize {
    zone_tree_bytes::<u32, ZoneColumnSummary>(zone.columns.len()).saturating_add(
        zone.columns
            .values()
            .map(zone_column_owned_bytes)
            .sum::<usize>(),
    )
}
fn zone_membership_merge_allocation(
    current: Option<&ZoneMembership>,
    incoming: Option<&ZoneMembership>,
) -> usize {
    use ZoneMembership::*;
    match (current, incoming) {
        (None, Some(value)) => value.capacity_bytes(),
        (
            Some(Exact {
                hashes,
                bloom_words,
                ..
            }),
            Some(Exact { hashes: added, .. }),
        ) => {
            let count = hashes.len().saturating_add(added.len());
            count
                .min(ZONE_MEMBERSHIP_EXACT_LIMIT)
                .saturating_mul(16)
                .saturating_add(if count > ZONE_MEMBERSHIP_EXACT_LIMIT {
                    usize::from(*bloom_words) * 8
                } else {
                    0
                })
        }
        (Some(Exact { .. }), Some(Bloom { words, .. })) => words.len() * 8,
        _ => 0,
    }
}

#[derive(Default, Serialize)]
#[serde(transparent)]
struct ChargedZoneSummary {
    value: ZoneSummary,
    #[serde(skip)]
    lease: ZoneScratchLease,
}
impl std::ops::Deref for ChargedZoneSummary {
    type Target = ZoneSummary;
    fn deref(&self) -> &ZoneSummary {
        &self.value
    }
}
impl std::ops::DerefMut for ChargedZoneSummary {
    fn deref_mut(&mut self) -> &mut ZoneSummary {
        &mut self.value
    }
}
impl AsRef<ZoneSummary> for ChargedZoneSummary {
    fn as_ref(&self) -> &ZoneSummary {
        &self.value
    }
}
impl AsRef<ZoneSummary> for ZoneSummary {
    fn as_ref(&self) -> &ZoneSummary {
        self
    }
}
impl std::fmt::Debug for ChargedZoneSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.value.fmt(f)
    }
}
impl ChargedZoneSummary {
    fn try_clone_from(value: &ZoneSummary) -> Result<Self, QueryError> {
        let mut lease = ZoneScratchLease::default();
        lease.reserve(zone_owned_bytes(value))?;
        let cloned = value.clone();
        lease.shrink_to(zone_owned_bytes(&cloned));
        Ok(Self {
            value: cloned,
            lease,
        })
    }
    fn column_growth(&self, column: u32) -> usize {
        if self.value.columns.contains_key(&column) {
            0
        } else {
            zone_tree_bytes::<u32, ZoneColumnSummary>(self.value.columns.len() + 1).saturating_sub(
                zone_tree_bytes::<u32, ZoneColumnSummary>(self.value.columns.len()),
            )
        }
    }
    fn begin_column(
        &mut self,
        column: u32,
        temporary: usize,
    ) -> Result<(usize, usize, usize), QueryError> {
        let before = self.lease.bytes();
        let old = self
            .value
            .columns
            .get(&column)
            .map_or(0, zone_column_owned_bytes);
        let growth = self.column_growth(column);
        self.lease.reserve(growth.saturating_add(temporary))?;
        Ok((before, old, growth))
    }
    fn finish_column(&mut self, column: u32, (before, old, growth): (usize, usize, usize)) {
        let now = self
            .value
            .columns
            .get(&column)
            .map_or(0, zone_column_owned_bytes);
        self.lease.shrink_to(
            before
                .saturating_add(growth)
                .saturating_add(now)
                .saturating_sub(old),
        );
    }
}

struct ChargedZoneEntries {
    entries: Vec<(zone_map_page::Key, Vec<u8>)>,
    lease: ZoneScratchLease,
}
impl std::ops::Deref for ChargedZoneEntries {
    type Target = [(zone_map_page::Key, Vec<u8>)];
    fn deref(&self) -> &Self::Target {
        &self.entries
    }
}
impl ChargedZoneEntries {
    fn try_append(&mut self, mut incoming: Self) -> Result<(), Self> {
        let count = self.entries.len().saturating_add(incoming.entries.len());
        let capacity = self.entries.capacity();
        let next_capacity = count.max(capacity.saturating_mul(2));
        let growth = if count > capacity {
            next_capacity.saturating_mul(std::mem::size_of::<(zone_map_page::Key, Vec<u8>)>())
        } else {
            0
        };
        #[cfg(test)]
        if self
            .lease
            .bytes()
            .saturating_add(incoming.lease.bytes())
            .saturating_add(growth) as u64
            > ZONE_MAP_TEST_SCRATCH_LIMIT.load(Ordering::Relaxed)
        {
            let _ = ZoneScratchLease::denied();
            return Err(incoming);
        }
        if self.lease.reserve(growth).is_err() {
            return Err(incoming);
        }
        if count > capacity {
            self.entries
                .reserve_exact(next_capacity - self.entries.len());
        }
        self.entries.append(&mut incoming.entries);
        self.lease.absorb(&mut incoming.lease);
        // Free the incoming slot allocation before releasing its charge.
        incoming.entries = Vec::new();
        let owned = self
            .entries
            .capacity()
            .saturating_mul(std::mem::size_of::<(zone_map_page::Key, Vec<u8>)>())
            .saturating_add(
                self.entries
                    .iter()
                    .map(|(_, value)| value.capacity())
                    .sum::<usize>(),
            );
        self.lease.shrink_to(owned);
        Ok(())
    }

    fn new(columns: usize) -> Result<Self, QueryError> {
        let count = 2usize.saturating_add(columns.saturating_mul(2));
        let mut lease = ZoneScratchLease::default();
        lease
            .reserve(count.saturating_mul(std::mem::size_of::<(zone_map_page::Key, Vec<u8>)>()))?;
        Ok(Self {
            entries: Vec::with_capacity(count),
            lease,
        })
    }
    fn push_encoded(
        &mut self,
        key: zone_map_page::Key,
        peak: usize,
        encode: impl FnOnce() -> Result<Vec<u8>, QueryError>,
    ) -> Result<(), QueryError> {
        debug_assert!(self.entries.len() < self.entries.capacity());
        let before = self.lease.bytes();
        self.lease.reserve(peak)?;
        let bytes = encode()?;
        let allocated = bytes.capacity();
        self.entries.push((key, bytes));
        self.lease.shrink_to(before.saturating_add(allocated));
        Ok(())
    }
    fn push_mutation(&mut self, id: u64, mutation: ZoneMutation) -> Result<(), QueryError> {
        self.push_encoded(ZoneMap::mutation_key(id), 16, || {
            Ok(ZoneMap::mutation_bytes(mutation))
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ZoneMapDeltaBatch {
    pub storage_id: u64,
    pub zone_pages: u32,
    pub schema_fingerprint: u64,
    pub zones: BTreeMap<u64, ZoneSummary>,
}

/// A single-zone widening waiting for its stripe latch holder to publish it.
struct QueuedZoneMerge {
    zone: u64,
    summary: ZoneSummary,
    result: Mutex<Option<Result<(), QueryError>>>,
}

impl QueuedZoneMerge {
    fn complete(&self, result: Result<(), QueryError>) {
        *self.result.lock().unwrap_or_else(|e| e.into_inner()) = Some(result);
    }

    fn take_result(&self) -> Option<Result<(), QueryError>> {
        self.result.lock().unwrap_or_else(|e| e.into_inner()).take()
    }
}

const ZONE_MAP_DML_MAX_ZONES: usize = 64;
#[cfg(test)]
static ZONE_MAP_TEST_DML_MAX_ZONES: AtomicU64 = AtomicU64::new(0);

struct ChargedZoneBatch {
    batch: ZoneMapDeltaBatch,
    lease: ZoneScratchLease,
}
impl std::ops::Deref for ChargedZoneBatch {
    type Target = ZoneMapDeltaBatch;
    fn deref(&self) -> &Self::Target {
        &self.batch
    }
}
impl std::ops::DerefMut for ChargedZoneBatch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.batch
    }
}
impl ChargedZoneBatch {
    fn new(map: &ZoneMap) -> Self {
        Self {
            batch: ZoneMapDeltaBatch {
                storage_id: map.storage_id,
                zone_pages: map.zone_pages,
                schema_fingerprint: map.schema_fingerprint,
                zones: BTreeMap::new(),
            },
            lease: ZoneScratchLease::default(),
        }
    }
    fn ensure_zone(&mut self, id: u64) -> Result<(), QueryError> {
        if !self.batch.zones.contains_key(&id) {
            debug_assert!(self.batch.zones.len() < ZONE_MAP_DML_MAX_ZONES);
            let prior = zone_tree_bytes::<u64, ZoneSummary>(self.batch.zones.len());
            let after = zone_tree_bytes::<u64, ZoneSummary>(self.batch.zones.len() + 1);
            self.lease.reserve(after.saturating_sub(prior))?;
            self.batch.zones.insert(id, ZoneSummary::default());
            #[cfg(test)]
            ZONE_MAP_TEST_DML_MAX_ZONES.fetch_max(self.batch.zones.len() as u64, Ordering::Relaxed);
        }
        Ok(())
    }
    fn begin_column(
        &mut self,
        id: u64,
        column: u32,
        temporary: usize,
    ) -> Result<(usize, usize, usize), QueryError> {
        self.ensure_zone(id)?;
        let zone = &self.batch.zones[&id];
        let before = self.lease.bytes();
        let old = zone.columns.get(&column).map_or(0, zone_column_owned_bytes);
        let growth = if zone.columns.contains_key(&column) {
            0
        } else {
            zone_tree_bytes::<u32, ZoneColumnSummary>(zone.columns.len() + 1).saturating_sub(
                zone_tree_bytes::<u32, ZoneColumnSummary>(zone.columns.len()),
            )
        };
        self.lease.reserve(growth.saturating_add(temporary))?;
        Ok((before, old, growth))
    }
    fn finish_column(
        &mut self,
        id: u64,
        column: u32,
        (before, old, growth): (usize, usize, usize),
    ) {
        let dynamic = self.batch.zones[&id]
            .columns
            .get(&column)
            .map_or(0, zone_column_owned_bytes);
        self.lease.shrink_to(
            before
                .saturating_add(growth)
                .saturating_add(dynamic)
                .saturating_sub(old),
        );
    }
    fn unknown_column(&mut self, id: u64, column: u32) -> Result<(), QueryError> {
        let ticket = self.begin_column(id, column, 0)?;
        self.batch
            .zones
            .get_mut(&id)
            .unwrap()
            .columns
            .entry(column)
            .or_default()
            .mark_unknown();
        self.finish_column(id, column, ticket);
        Ok(())
    }
    fn clone_zone(&mut self, id: u64, source: &ZoneSummary) -> Result<(), QueryError> {
        self.ensure_zone(id)?;
        let before = self.lease.bytes();
        let old = zone_owned_bytes(&self.batch.zones[&id]);
        self.lease.reserve(zone_owned_bytes(source))?;
        let cloned = source.clone();
        let actual = zone_owned_bytes(&cloned);
        self.batch.zones.insert(id, cloned);
        self.lease
            .shrink_to(before.saturating_add(actual).saturating_sub(old));
        Ok(())
    }
    fn aggregate<R: AsRef<[Value]>>(
        &mut self,
        id: u64,
        column_id: u32,
        column: &ColumnSchema,
        rows: &[R],
        index: usize,
        layout: Option<ZoneMembershipLayout>,
    ) -> Result<(), QueryError> {
        self.ensure_zone(id)?;
        let current = self.batch.zones[&id]
            .columns
            .get(&column_id)
            .and_then(|c| c.membership.as_ref());
        let membership_peak = match (layout, current) {
            (Some(layout), None | Some(ZoneMembership::Exact { .. })) => {
                let old = match current {
                    Some(ZoneMembership::Exact { hashes, .. }) => hashes.len(),
                    _ => 0,
                };
                let count = old.saturating_add(rows.len());
                count
                    .min(ZONE_MEMBERSHIP_EXACT_LIMIT)
                    .saturating_mul(16)
                    .saturating_add(if count > ZONE_MEMBERSHIP_EXACT_LIMIT {
                        layout.words.saturating_mul(8)
                    } else {
                        0
                    })
            }
            _ => 0,
        };
        let scalar_peak = match column.data_type {
            DataTypeKind::Char { .. }
            | DataTypeKind::VarChar { .. }
            | DataTypeKind::Enum { .. }
            | DataTypeKind::Set { .. } => PAGE_SIZE * 4,
            DataTypeKind::Decimal { .. } => 1024,
            DataTypeKind::Binary { .. } | DataTypeKind::VarBinary { .. } => 256,
            _ => 128,
        };
        let ticket =
            self.begin_column(id, column_id, scalar_peak.saturating_add(membership_peak))?;
        aggregate_column_values_range(
            self.batch
                .zones
                .get_mut(&id)
                .unwrap()
                .columns
                .entry(column_id)
                .or_default(),
            column,
            rows,
            index,
            layout,
        );
        self.finish_column(id, column_id, ticket);
        Ok(())
    }
    fn flush(&mut self, map: &ZoneMap) -> Result<(), QueryError> {
        map.merge(&self.batch)?;
        self.batch.zones.clear();
        self.lease.shrink_to(0);
        Ok(())
    }
    fn admit_zone(&mut self, map: &ZoneMap, id: u64) -> Result<(), QueryError> {
        if self.batch.zones.len() == ZONE_MAP_DML_MAX_ZONES && !self.batch.zones.contains_key(&id) {
            self.flush(map)?;
        }
        self.ensure_zone(id)
    }
}

/// One bounded rebuild batch captured before its heap scan. Publication
/// revalidates every captured epoch and the snapshot horizon, so a batch may
/// publish while the next batch is already scanning.
struct ZoneRebuildBatch<G> {
    map: Arc<ZoneMap>,
    requested_zones: BTreeSet<u64>,
    progress: Option<(u64, u64)>,
    invalidation_epoch: u64,
    was_trusted: bool,
    neutral_overflow_before: u64,
    epoch_before: BTreeMap<u64, u64>,
    txn: TxnView,
    build_scn: u64,
    _plan_lease: ZoneScratchLease,
    _snapshot_guard: G,
}

enum PreparedZoneRebuild<G> {
    Finished(Option<u64>),
    Batch(ZoneRebuildBatch<G>),
}

struct ScannedZoneRebuild {
    fresh: ZoneMapDeltaBatch,
    deferred_zones: BTreeSet<u64>,
    _scan_charges: Vec<ZoneScratchLease>,
}

struct ZoneMapScanPartial {
    batch: ZoneMapDeltaBatch,
    deferred: Vec<RowId>,
    _owned_charge: ZoneScratchLease,
    _deferred_charge: ZoneScratchLease,
}

impl ZoneMapDeltaBatch {
    #[cfg(test)]
    fn estimated_encoded_bytes(&self) -> usize {
        let mut bytes = 64usize;
        for zone in self.zones.values() {
            bytes = bytes.saturating_add(48 + zone.columns.len() * 32);
            for summary in zone.columns.values() {
                if let Some(membership) = &summary.membership {
                    bytes = bytes.saturating_add(membership.capacity_bytes());
                }
                for scalar in [summary.min.as_ref(), summary.max.as_ref()]
                    .into_iter()
                    .flatten()
                {
                    bytes = bytes.saturating_add(match scalar {
                        ZoneScalar::Decimal(value) => value.len(),
                        ZoneScalar::Binary { prefix, .. } => prefix.len(),
                        ZoneScalar::Text { weight, .. } => weight.len(),
                        _ => 8,
                    });
                }
            }
        }
        bytes
    }

    #[cfg(test)]
    fn merge_from(&mut self, incoming: ZoneMapDeltaBatch) {
        debug_assert_eq!(self.storage_id, incoming.storage_id);
        if self.zone_pages != incoming.zone_pages
            || self.schema_fingerprint != incoming.schema_fingerprint
        {
            // A physical-layout change in the middle of a transaction is not
            // expected. Unknown is the only safe representation if it occurs.
            for (zone_id, incoming_zone) in incoming.zones {
                let zone = self.zones.entry(zone_id).or_default();
                zone.state = Some(ZoneMapState::Unknown);
                zone.row_count_estimate = zone
                    .row_count_estimate
                    .saturating_add(incoming_zone.row_count_estimate);
                zone.stale_mutations = zone
                    .stale_mutations
                    .saturating_add(incoming_zone.stale_mutations);
            }
            return;
        }
        for (zone_id, incoming_zone) in incoming.zones {
            self.zones
                .entry(zone_id)
                .or_default()
                .absorb(&incoming_zone);
        }
    }
}

#[cfg(test)]
fn encode_zone_map_delta(batch: &ZoneMapDeltaBatch) -> Vec<u8> {
    fn put_scalar(out: &mut Vec<u8>, scalar: Option<&ZoneScalar>) {
        match scalar {
            None => out.push(0),
            Some(ZoneScalar::Signed(value)) => {
                out.push(1);
                out.extend_from_slice(&value.to_le_bytes());
            }
            Some(ZoneScalar::Unsigned(value)) => {
                out.push(2);
                out.extend_from_slice(&value.to_le_bytes());
            }
            Some(ZoneScalar::Float32(value)) => {
                out.push(3);
                out.extend_from_slice(&value.to_le_bytes());
            }
            Some(ZoneScalar::Float64(value)) => {
                out.push(4);
                out.extend_from_slice(&value.to_le_bytes());
            }
            Some(ZoneScalar::Decimal(value)) => {
                out.push(5);
                put_bytes(out, value.as_bytes());
            }
            Some(ZoneScalar::Temporal(value)) => {
                out.push(6);
                out.extend_from_slice(&value.to_le_bytes());
            }
            Some(ZoneScalar::Binary { prefix, truncated }) => {
                out.push(7);
                out.push(u8::from(*truncated));
                put_bytes(out, prefix);
            }
            Some(ZoneScalar::Text { weight, truncated }) => {
                out.push(8);
                out.push(u8::from(*truncated));
                put_bytes(out, weight);
            }
        }
    }
    fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
        out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(bytes);
    }
    use zone_state_tag as state;
    let mut out = Vec::with_capacity(batch.estimated_encoded_bytes());
    out.extend_from_slice(ZONE_MAP_DELTA_MAGIC);
    out.extend_from_slice(&batch.storage_id.to_le_bytes());
    out.extend_from_slice(&batch.zone_pages.to_le_bytes());
    out.extend_from_slice(&batch.schema_fingerprint.to_le_bytes());
    out.extend_from_slice(&(batch.zones.len() as u32).to_le_bytes());
    for (zone_id, zone) in &batch.zones {
        out.extend_from_slice(&zone_id.to_le_bytes());
        out.push(zone.state.map(state).unwrap_or(u8::MAX));
        out.extend_from_slice(&zone.row_count_estimate.to_le_bytes());
        out.extend_from_slice(&zone.stale_mutations.to_le_bytes());
        out.extend_from_slice(&zone.rebuild_scn.to_le_bytes());
        out.extend_from_slice(&(zone.columns.len() as u32).to_le_bytes());
        for (column_id, column) in &zone.columns {
            out.extend_from_slice(&column_id.to_le_bytes());
            out.push(state(column.state));
            out.push(
                u8::from(column.has_null)
                    | (u8::from(column.has_non_null) << 1)
                    | (u8::from(column.truncated) << 2),
            );
            put_scalar(&mut out, column.min.as_ref());
            put_scalar(&mut out, column.max.as_ref());
        }
    }
    out
}

#[cfg(test)]
fn decode_zone_map_delta(bytes: &[u8]) -> Option<ZoneMapDeltaBatch> {
    if bytes.starts_with(ZONE_MAP_DELTA_V2_MAGIC)
        || bytes.starts_with(ZONE_MAP_DELTA_V3_MAGIC)
        || bytes.starts_with(ZONE_MAP_DELTA_V4_MAGIC)
    {
        return decode_zone_map_delta_v2(bytes);
    }
    struct Reader<'a> {
        bytes: &'a [u8],
        pos: usize,
    }
    impl<'a> Reader<'a> {
        fn take(&mut self, len: usize) -> Option<&'a [u8]> {
            let end = self.pos.checked_add(len)?;
            let out = self.bytes.get(self.pos..end)?;
            self.pos = end;
            Some(out)
        }
        fn u8(&mut self) -> Option<u8> {
            Some(*self.take(1)?.first()?)
        }
        fn u32(&mut self) -> Option<u32> {
            Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
        }
        fn u64(&mut self) -> Option<u64> {
            Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
        }
        fn i64(&mut self) -> Option<i64> {
            Some(i64::from_le_bytes(self.take(8)?.try_into().ok()?))
        }
        fn blob(&mut self) -> Option<Vec<u8>> {
            let len = self.u32()? as usize;
            Some(self.take(len)?.to_vec())
        }
        fn scalar(&mut self) -> Option<Option<ZoneScalar>> {
            Some(match self.u8()? {
                0 => None,
                1 => Some(ZoneScalar::Signed(self.i64()?)),
                2 => Some(ZoneScalar::Unsigned(self.u64()?)),
                3 => Some(ZoneScalar::Float32(self.u32()?)),
                4 => Some(ZoneScalar::Float64(self.u64()?)),
                5 => Some(ZoneScalar::Decimal(String::from_utf8(self.blob()?).ok()?)),
                6 => Some(ZoneScalar::Temporal(self.i64()?)),
                7 => Some(ZoneScalar::Binary {
                    truncated: self.u8()? != 0,
                    prefix: self.blob()?,
                }),
                8 => Some(ZoneScalar::Text {
                    truncated: self.u8()? != 0,
                    weight: self.blob()?,
                }),
                _ => return None,
            })
        }
    }
    use zone_state_from_tag as state;
    // ZMD1 remains readable for recovery of WAL written before the v2
    // sidecar rollout. The earlier JSON prototype is deliberately dropped.
    if !bytes.starts_with(ZONE_MAP_DELTA_MAGIC) {
        return None;
    }
    let mut reader = Reader {
        bytes,
        pos: ZONE_MAP_DELTA_MAGIC.len(),
    };
    let storage_id = reader.u64()?;
    let zone_pages = reader.u32()?;
    let schema_fingerprint = reader.u64()?;
    let zone_count = reader.u32()? as usize;
    let mut zones = BTreeMap::new();
    for _ in 0..zone_count {
        let zone_id = reader.u64()?;
        let state_tag = reader.u8()?;
        let mut zone = ZoneSummary {
            state: if state_tag == u8::MAX {
                None
            } else {
                Some(state(state_tag)?)
            },
            columns: BTreeMap::new(),
            row_count_estimate: reader.u64()?,
            stale_mutations: reader.u64()?,
            rebuild_scn: reader.u64()?,
        };
        let column_count = reader.u32()? as usize;
        for _ in 0..column_count {
            let column_id = reader.u32()?;
            let column_state = state(reader.u8()?)?;
            let flags = reader.u8()?;
            zone.columns.insert(
                column_id,
                ZoneColumnSummary {
                    state: column_state,
                    durable_unknown: column_state == ZoneMapState::Unknown,
                    has_null: flags & 1 != 0,
                    has_non_null: flags & 2 != 0,
                    truncated: flags & 4 != 0,
                    min: reader.scalar()?,
                    max: reader.scalar()?,
                    membership: None,
                },
            );
        }
        zones.insert(zone_id, zone);
    }
    (reader.pos == bytes.len()).then_some(ZoneMapDeltaBatch {
        storage_id,
        zone_pages,
        schema_fingerprint,
        zones,
    })
}

fn put_var_u64(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn zigzag_i64(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

fn unzigzag_i64(value: u64) -> i64 {
    ((value >> 1) as i64) ^ (-((value & 1) as i64))
}

struct ZoneBytesReader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> ZoneBytesReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn take(&mut self, len: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(len)?;
        let value = self.bytes.get(self.pos..end)?;
        self.pos = end;
        Some(value)
    }

    fn u8(&mut self) -> Option<u8> {
        self.take(1)?.first().copied()
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }

    fn var_u64(&mut self) -> Option<u64> {
        let mut value = 0u64;
        for shift in (0..=63).step_by(7) {
            let byte = self.u8()?;
            if (shift == 63 && byte > 1) || (shift != 0 && byte == 0) {
                return None;
            }
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Some(value);
            }
        }
        None
    }

    fn done(&self) -> bool {
        self.pos == self.bytes.len()
    }
}

fn put_zone_scalar_v2(out: &mut Vec<u8>, scalar: Option<&ZoneScalar>) -> Option<()> {
    match scalar {
        None => out.push(0),
        Some(ZoneScalar::Signed(value)) => {
            out.push(1);
            put_var_u64(out, zigzag_i64(*value));
        }
        Some(ZoneScalar::Unsigned(value)) => {
            out.push(2);
            put_var_u64(out, *value);
        }
        Some(ZoneScalar::Float32(value)) => {
            out.push(3);
            out.extend_from_slice(&value.to_le_bytes());
        }
        Some(ZoneScalar::Float64(value)) => {
            out.push(4);
            out.extend_from_slice(&value.to_le_bytes());
        }
        Some(ZoneScalar::Decimal(value)) => {
            out.push(5);
            let len_pos = out.len();
            out.push(0);
            let start = out.len();
            crate::util::decimal::encode_decimal_bcd(value, out)?;
            let len = out.len() - start;
            if len > u8::MAX as usize {
                return None;
            }
            out[len_pos] = len as u8;
        }
        Some(ZoneScalar::Temporal(value)) => {
            out.push(6);
            put_var_u64(out, zigzag_i64(*value));
        }
        Some(ZoneScalar::Binary { prefix, truncated }) => {
            out.push(7);
            out.push(u8::from(*truncated));
            put_var_u64(out, prefix.len() as u64);
            out.extend_from_slice(prefix);
        }
        Some(ZoneScalar::Text { weight, truncated }) => {
            out.push(8);
            out.push(u8::from(*truncated));
            put_var_u64(out, weight.len() as u64);
            out.extend_from_slice(weight);
        }
    }
    Some(())
}

fn read_zone_scalar_v2(reader: &mut ZoneBytesReader<'_>) -> Option<Option<ZoneScalar>> {
    Some(match reader.u8()? {
        0 => None,
        1 => Some(ZoneScalar::Signed(unzigzag_i64(reader.var_u64()?))),
        2 => Some(ZoneScalar::Unsigned(reader.var_u64()?)),
        3 => Some(ZoneScalar::Float32(reader.u32()?)),
        4 => Some(ZoneScalar::Float64(reader.u64()?)),
        5 => {
            let len = reader.u8()? as usize;
            let bytes = reader.take(len)?;
            let (value, used) = crate::util::decimal::decode_decimal_bcd(bytes)?;
            if used != len {
                return None;
            }
            Some(ZoneScalar::Decimal(value))
        }
        6 => Some(ZoneScalar::Temporal(unzigzag_i64(reader.var_u64()?))),
        7 | 8 => {
            let tag = reader.bytes[reader.pos - 1];
            let truncated = match reader.u8()? {
                0 => false,
                1 => true,
                _ => return None,
            };
            let len = usize::try_from(reader.var_u64()?).ok()?;
            let bytes = reader.take(len)?.to_vec();
            if tag == 7 {
                Some(ZoneScalar::Binary {
                    prefix: bytes,
                    truncated,
                })
            } else {
                Some(ZoneScalar::Text {
                    weight: bytes,
                    truncated,
                })
            }
        }
        _ => return None,
    })
}

#[cfg(test)]
fn put_zone_membership(out: &mut Vec<u8>, membership: Option<&ZoneMembership>) -> Option<()> {
    match membership {
        None => out.push(0),
        Some(ZoneMembership::Exact {
            hashes,
            bloom_words,
            bloom_hashes,
        }) if ZoneMembership::valid_layout(usize::from(*bloom_words), *bloom_hashes) => {
            out.push(1);
            put_var_u64(out, u64::from(*bloom_words));
            out.push(*bloom_hashes);
            put_var_u64(out, hashes.len() as u64);
            for hash in hashes {
                out.extend_from_slice(&hash.to_le_bytes());
            }
        }
        Some(ZoneMembership::Bloom { words, hashes, .. })
            if ZoneMembership::valid_layout(words.len(), *hashes) =>
        {
            out.push(2);
            put_var_u64(out, words.len() as u64);
            out.push(*hashes);
            for word in words {
                out.extend_from_slice(&word.to_le_bytes());
            }
        }
        Some(ZoneMembership::Saturated) => out.push(3),
        Some(ZoneMembership::Exact { .. } | ZoneMembership::Bloom { .. }) => return None,
    }
    Some(())
}

#[cfg(test)]
fn read_zone_membership(
    reader: &mut ZoneBytesReader<'_>,
    self_describing: bool,
) -> Option<Option<ZoneMembership>> {
    Some(match reader.u8()? {
        0 => None,
        1 => {
            let (bloom_words, bloom_hashes) = if self_describing {
                (usize::try_from(reader.var_u64()?).ok()?, reader.u8()?)
            } else {
                (
                    ZONE_MEMBERSHIP_MIN_BLOOM_WORDS,
                    ZONE_MEMBERSHIP_DEFAULT_BLOOM_HASHES,
                )
            };
            if !ZoneMembership::valid_layout(bloom_words, bloom_hashes) {
                return None;
            }
            let len = usize::try_from(reader.var_u64()?).ok()?;
            if len > ZONE_MEMBERSHIP_EXACT_LIMIT {
                return None;
            }
            let mut hashes = Vec::with_capacity(len);
            for _ in 0..len {
                hashes.push(reader.u64()?);
            }
            if hashes.windows(2).any(|pair| pair[0] >= pair[1]) {
                return None;
            }
            Some(ZoneMembership::Exact {
                hashes,
                bloom_words: bloom_words as u16,
                bloom_hashes,
            })
        }
        2 => {
            let (word_count, hashes) = if self_describing {
                (usize::try_from(reader.var_u64()?).ok()?, reader.u8()?)
            } else {
                (
                    ZONE_MEMBERSHIP_MIN_BLOOM_WORDS,
                    ZONE_MEMBERSHIP_DEFAULT_BLOOM_HASHES,
                )
            };
            if !ZoneMembership::valid_layout(word_count, hashes) {
                return None;
            }
            let mut words = Vec::with_capacity(word_count);
            for _ in 0..word_count {
                words.push(reader.u64()?);
            }
            if ZoneMembership::is_saturated(&words) {
                Some(ZoneMembership::Saturated)
            } else {
                Some(ZoneMembership::Bloom {
                    set_bits: ZoneMembership::count_set_bits(&words),
                    words,
                    hashes,
                })
            }
        }
        3 if self_describing => Some(ZoneMembership::Saturated),
        _ => return None,
    })
}

#[cfg(test)]
fn encode_zone_map_delta_v2(batch: &ZoneMapDeltaBatch) -> Option<Vec<u8>> {
    let mut raw = Vec::with_capacity(batch.estimated_encoded_bytes());
    put_var_u64(&mut raw, batch.storage_id);
    put_var_u64(&mut raw, batch.zone_pages as u64);
    raw.extend_from_slice(&batch.schema_fingerprint.to_le_bytes());
    put_var_u64(&mut raw, batch.zones.len() as u64);
    let mut prior_zone = 0u64;
    for (&zone_id, zone) in &batch.zones {
        put_var_u64(&mut raw, zone_id.checked_sub(prior_zone)?);
        prior_zone = zone_id;
        raw.push(zone.state.map_or(0, |state| zone_state_tag(state) + 1));
        put_var_u64(&mut raw, zone.row_count_estimate);
        put_var_u64(&mut raw, zone.stale_mutations);
        put_var_u64(&mut raw, zone.rebuild_scn);
        put_var_u64(&mut raw, zone.columns.len() as u64);
        let mut prior_column = 0u32;
        for (&column_id, column) in &zone.columns {
            put_var_u64(&mut raw, u64::from(column_id.checked_sub(prior_column)?));
            prior_column = column_id;
            raw.push(
                zone_state_tag(column.state)
                    | (u8::from(column.has_null) << 2)
                    | (u8::from(column.has_non_null) << 3)
                    | (u8::from(column.truncated) << 4),
            );
            put_zone_scalar_v2(&mut raw, column.min.as_ref())?;
            put_zone_scalar_v2(&mut raw, column.max.as_ref())?;
            put_zone_membership(&mut raw, column.membership.as_ref())?;
        }
    }
    let compressed =
        (raw.len() >= ZONE_MAP_LZ4_MIN_BYTES).then(|| lz4_flex::compress_prepend_size(&raw));
    let (codec, stored) = match compressed {
        Some(compressed)
            if compressed
                .len()
                .saturating_mul(ZONE_MAP_LZ4_SAVINGS_DENOMINATOR)
                <= raw.len().saturating_mul(ZONE_MAP_LZ4_SAVINGS_NUMERATOR) =>
        {
            (1u8, compressed)
        }
        _ => (0u8, raw.clone()),
    };
    let mut out = Vec::with_capacity(9 + stored.len());
    out.extend_from_slice(ZONE_MAP_DELTA_V4_MAGIC);
    out.push(codec);
    out.extend_from_slice(&(raw.len() as u32).to_le_bytes());
    out.extend_from_slice(&stored);
    Some(out)
}

#[cfg(test)]
fn decode_zone_map_delta_v2(bytes: &[u8]) -> Option<ZoneMapDeltaBatch> {
    if bytes.len() < 9
        || !(bytes.starts_with(ZONE_MAP_DELTA_V2_MAGIC)
            || bytes.starts_with(ZONE_MAP_DELTA_V3_MAGIC)
            || bytes.starts_with(ZONE_MAP_DELTA_V4_MAGIC))
    {
        return None;
    }
    let has_membership =
        bytes.starts_with(ZONE_MAP_DELTA_V3_MAGIC) || bytes.starts_with(ZONE_MAP_DELTA_V4_MAGIC);
    let self_describing_membership = bytes.starts_with(ZONE_MAP_DELTA_V4_MAGIC);
    let codec = bytes[4];
    let raw_len = u32::from_le_bytes(bytes[5..9].try_into().ok()?) as usize;
    let raw = match codec {
        0 => bytes[9..].to_vec(),
        1 => lz4_flex::decompress_size_prepended(&bytes[9..]).ok()?,
        _ => return None,
    };
    if raw.len() != raw_len {
        return None;
    }
    let mut reader = ZoneBytesReader::new(&raw);
    let storage_id = reader.var_u64()?;
    let zone_pages = u32::try_from(reader.var_u64()?).ok()?;
    let schema_fingerprint = reader.u64()?;
    let zone_count = usize::try_from(reader.var_u64()?).ok()?;
    let mut zones = BTreeMap::new();
    let mut prior_zone = 0u64;
    for _ in 0..zone_count {
        let zone_id = prior_zone.checked_add(reader.var_u64()?)?;
        prior_zone = zone_id;
        let state = match reader.u8()? {
            0 => None,
            tag @ 1..=4 => Some(zone_state_from_tag(tag - 1)?),
            _ => return None,
        };
        let row_count_estimate = reader.var_u64()?;
        let stale_mutations = reader.var_u64()?;
        let rebuild_scn = reader.var_u64()?;
        let column_count = usize::try_from(reader.var_u64()?).ok()?;
        let mut columns = BTreeMap::new();
        let mut prior_column = 0u32;
        for _ in 0..column_count {
            let delta = u32::try_from(reader.var_u64()?).ok()?;
            let column_id = prior_column.checked_add(delta)?;
            prior_column = column_id;
            let flags = reader.u8()?;
            if flags & 0xe0 != 0 {
                return None;
            }
            columns.insert(
                column_id,
                ZoneColumnSummary {
                    state: zone_state_from_tag(flags & 3)?,
                    durable_unknown: flags & 3 == zone_state_tag(ZoneMapState::Unknown),
                    has_null: flags & (1 << 2) != 0,
                    has_non_null: flags & (1 << 3) != 0,
                    truncated: flags & (1 << 4) != 0,
                    min: read_zone_scalar_v2(&mut reader)?,
                    max: read_zone_scalar_v2(&mut reader)?,
                    membership: if has_membership {
                        read_zone_membership(&mut reader, self_describing_membership)?
                    } else {
                        None
                    },
                },
            );
        }
        zones.insert(
            zone_id,
            ZoneSummary {
                state,
                columns,
                row_count_estimate,
                stale_mutations,
                rebuild_scn,
            },
        );
    }
    reader.done().then_some(ZoneMapDeltaBatch {
        storage_id,
        zone_pages,
        schema_fingerprint,
        zones,
    })
}

fn zone_state_tag(state: ZoneMapState) -> u8 {
    match state {
        ZoneMapState::Unknown => 0,
        ZoneMapState::Ready => 1,
        ZoneMapState::Building => 2,
        ZoneMapState::Empty => 3,
    }
}

fn zone_state_from_tag(tag: u8) -> Option<ZoneMapState> {
    match tag {
        0 => Some(ZoneMapState::Unknown),
        1 => Some(ZoneMapState::Ready),
        2 => Some(ZoneMapState::Building),
        3 => Some(ZoneMapState::Empty),
        _ => None,
    }
}

/// Compact per-worker-batch zone-map aggregate. Records per-column min/max
/// and null/non-null flags for the entire batch, without per-row
/// intermediate allocations. When the writer thread receives it, it applies
/// the aggregate to every zone the freshly-inserted `row_ids` touch (so
/// per-zone summaries are widened to at least the batch's envelope).
///
/// Zone-precision trade-off: within a single worker batch, all touched
/// zones receive the same min/max — precision degrades linearly with batch
/// size. TPC-H / bulk-load pipelines run `ANALYZE TABLE` afterwards, which
/// invokes `rebuild_zone_map_for_storage` and produces tight per-zone
/// summaries from the heap. Callers that need tight LOAD-time precision
/// without a follow-up ANALYZE can opt out via `ZONE_MAP=OFF` (or set a
/// smaller `PARALLEL_CHUNK_SIZE`). Correctness is unaffected — zone maps
/// only ever conservatively widen.
#[derive(Default)]
pub struct ZoneMapWorkerBatch {
    entries: Vec<ZoneMapWorkerColumn>,
    /// Reusable scratch buffer for text weight computation. Kept on the
    /// batch so `observe_row` does not allocate a fresh `Vec<u8>` per
    /// text column per row — the buffer holds the latest weight while
    /// we compare it against the current running min/max, and only
    /// migrates into the stored `WorkerScalar` when we actually advance.
    text_scratch: Vec<u8>,
    // Declared after the owned buffers so release follows deallocation.
    lease: ZoneScratchLease,
}

/// In-memory running scalar used only by `ZoneMapWorkerBatch`. Compared
/// with `ZoneScalar`, this variant keeps DECIMAL as `(i128, u8)` and
/// text/binary as inline byte buffers so that per-row observe compares
/// without allocating.
#[derive(Debug, Clone)]
enum WorkerScalar {
    Signed(i64),
    Unsigned(u64),
    Float32(u32),
    Float64(u64),
    DecimalCompact { mag: i128, scale: u8 },
    Temporal(i64),
    Bytes { prefix: Vec<u8>, truncated: bool },
    TextWeight { weight: Vec<u8>, truncated: bool },
}

impl WorkerScalar {
    fn owned_bytes(&self) -> usize {
        match self {
            Self::Bytes { prefix, .. } => prefix.capacity(),
            Self::TextWeight { weight, .. } => weight.capacity(),
            _ => 0,
        }
    }

    fn into_zone_scalar(self) -> ZoneScalar {
        match self {
            WorkerScalar::Signed(v) => ZoneScalar::Signed(v),
            WorkerScalar::Unsigned(v) => ZoneScalar::Unsigned(v),
            WorkerScalar::Float32(v) => {
                let value = f32::from_bits(v);
                ZoneScalar::Float32(if value == 0.0 { 0.0f32.to_bits() } else { v })
            }
            WorkerScalar::Float64(v) => {
                let value = f64::from_bits(v);
                ZoneScalar::Float64(if value == 0.0 { 0.0f64.to_bits() } else { v })
            }
            WorkerScalar::DecimalCompact { mag, scale } => {
                let formatted = crate::util::decimal::format_decimal_parts(mag, scale);
                ZoneScalar::Decimal(
                    crate::util::decimal::canonical_decimal_str(&formatted).unwrap_or(formatted),
                )
            }
            WorkerScalar::Temporal(v) => ZoneScalar::Temporal(v),
            WorkerScalar::Bytes { prefix, truncated } => ZoneScalar::Binary { prefix, truncated },
            WorkerScalar::TextWeight { weight, truncated } => {
                ZoneScalar::Text { weight, truncated }
            }
        }
    }

    fn was_truncated(&self) -> bool {
        matches!(
            self,
            WorkerScalar::Bytes {
                truncated: true,
                ..
            } | WorkerScalar::TextWeight {
                truncated: true,
                ..
            }
        )
    }
}

#[inline(always)]
fn zone_membership_hash_worker(value: &WorkerScalar) -> u64 {
    let mut hash = ZoneMembershipHasher::new();
    match value {
        WorkerScalar::Signed(value) => hash.write_exact(i128::from(*value)),
        WorkerScalar::Unsigned(value) => hash.write_exact(i128::from(*value)),
        WorkerScalar::Float32(bits) => {
            hash.write(&[2]);
            let value = f32::from_bits(*bits);
            hash.write(
                &if value == 0.0 {
                    0.0f32.to_bits()
                } else {
                    *bits
                }
                .to_le_bytes(),
            );
        }
        WorkerScalar::Float64(bits) => {
            hash.write(&[3]);
            let value = f64::from_bits(*bits);
            hash.write(
                &if value == 0.0 {
                    0.0f64.to_bits()
                } else {
                    *bits
                }
                .to_le_bytes(),
            );
        }
        WorkerScalar::DecimalCompact { mag, scale } => {
            hash.write_decimal_compact(*mag, *scale);
        }
        WorkerScalar::Temporal(value) => {
            hash.write(&[5]);
            hash.write(&value.to_le_bytes());
        }
        WorkerScalar::Bytes { prefix, truncated } => {
            hash.write(&[6]);
            hash.write(prefix);
            hash.write(&[u8::from(*truncated)]);
        }
        WorkerScalar::TextWeight { weight, truncated } => {
            hash.write(&[7]);
            hash.write(weight);
            hash.write(&[u8::from(*truncated)]);
        }
    }
    hash.0
}

#[inline]
fn zone_membership_hash_borrowed(tag: u8, bytes: &[u8], truncated: bool) -> u64 {
    debug_assert!(matches!(tag, 6 | 7));
    let mut hash = ZoneMembershipHasher::new();
    hash.write(&[tag]);
    hash.write(bytes);
    hash.write(&[u8::from(truncated)]);
    hash.0
}

#[derive(Debug, Clone)]
struct ZoneMapWorkerColumn {
    column_idx: usize,
    column_id: u32,
    collation_id: u16,
    state: ZoneMapState,
    has_null: bool,
    has_non_null: bool,
    truncated: bool,
    min: Option<WorkerScalar>,
    max: Option<WorkerScalar>,
    membership: Option<ZoneMembership>,
}

/// Compare a raw `Value` against a running `WorkerScalar` for the given
/// column. Returns `Ok(Some(Ordering))` if the comparison is defined,
/// `Ok(None)` for representable-but-incompatible types (caller should
/// mark the summary Unknown), and `Err(())` when the value cannot be
/// projected into any zone-map scalar at all (also → Unknown). No
/// allocation happens; text uses an already-computed running weight and
/// a caller-supplied scratch buffer for the incoming value.
#[inline]
fn compare_value_to_worker(
    column: &ColumnSchema,
    collation_id: u16,
    value: &Value,
    stored: &WorkerScalar,
    text_scratch: &mut Vec<u8>,
) -> Result<Option<std::cmp::Ordering>, ()> {
    use DataTypeKind::*;
    match (&column.data_type, value, stored) {
        (
            TinyInt | SmallInt | MediumInt | Int | BigInt | Bool,
            Value::Int(v),
            WorkerScalar::Signed(m),
        ) => Ok(Some(v.cmp(m))),
        (
            TinyInt | SmallInt | MediumInt | Int | BigInt | Bool | Bit { .. },
            Value::UInt(v),
            WorkerScalar::Unsigned(m),
        ) => Ok(Some(v.cmp(m))),
        (Float { .. }, Value::Float(v), WorkerScalar::Float32(m)) if !v.is_nan() => {
            Ok(v.partial_cmp(&f32::from_bits(*m)))
        }
        (Double { .. }, Value::Double(v), WorkerScalar::Float64(m)) if !v.is_nan() => {
            Ok(v.partial_cmp(&f64::from_bits(*m)))
        }
        (
            Decimal { .. },
            Value::DecimalCompact(mag, scale),
            WorkerScalar::DecimalCompact { mag: sm, scale: ss },
        ) if *scale == *ss => Ok(Some((*mag as i128).cmp(sm))),
        (
            Decimal { .. },
            Value::DecimalCompact(mag, scale),
            WorkerScalar::DecimalCompact { .. },
        ) => {
            // Same column should imply same scale; if the row carries a
            // different scale we fall back to unknown to stay conservative.
            let _ = (mag, scale);
            Ok(None)
        }
        (Date, Value::Date(v), WorkerScalar::Temporal(m)) => Ok(Some((*v as i64).cmp(m))),
        (Year, Value::Year(v), WorkerScalar::Temporal(m)) => Ok(Some((*v as i64).cmp(m))),
        (Time { .. }, Value::Time(v), WorkerScalar::Temporal(m))
        | (DateTime { .. }, Value::DateTime(v), WorkerScalar::Temporal(m))
        | (Timestamp { .. }, Value::Timestamp(v), WorkerScalar::Temporal(m)) => Ok(Some(v.cmp(m))),
        (
            Binary { .. } | VarBinary { .. },
            Value::Bytes(bytes),
            WorkerScalar::Bytes { prefix, .. },
        ) => {
            let take = bytes.len().min(ZONE_STRING_PREFIX_CHARS);
            Ok(Some((&bytes[..take]).cmp(&prefix[..])))
        }
        (
            Char { .. } | VarChar { .. } | Enum { .. } | Set { .. },
            Value::String(text),
            WorkerScalar::TextWeight { weight, .. },
        ) => {
            text_scratch.clear();
            crate::charset::collation_key_by_id_into(
                collation_id,
                zone_text_prefix_slice(text, collation_id),
                text_scratch,
            );
            Ok(Some(text_scratch[..].cmp(&weight[..])))
        }
        (
            Char { .. } | VarChar { .. } | Enum { .. } | Set { .. },
            Value::SmallString(bytes),
            WorkerScalar::TextWeight { weight, .. },
        ) => match std::str::from_utf8(bytes.as_slice()) {
            Ok(text) => {
                text_scratch.clear();
                crate::charset::collation_key_by_id_into(
                    collation_id,
                    zone_text_prefix_slice(text, collation_id),
                    text_scratch,
                );
                Ok(Some(text_scratch[..].cmp(&weight[..])))
            }
            Err(_) => Err(()),
        },
        _ => Err(()),
    }
}

/// Fast-path Value → WorkerScalar. Allocates for DECIMAL/text/binary only
/// when the caller actually needs to promote a row to a new min or max.
/// Returns `None` when the (column, value) pair can't be represented — the
/// caller marks the column summary Unknown.
#[inline]
fn worker_scalar_from_value(
    column: &ColumnSchema,
    collation_id: u16,
    value: &Value,
) -> Option<WorkerScalar> {
    use DataTypeKind::*;
    match (&column.data_type, value) {
        (_, Value::Null) => None,
        (TinyInt | SmallInt | MediumInt | Int | BigInt | Bool, Value::Int(v)) => {
            if column.unsigned && *v >= 0 {
                Some(WorkerScalar::Unsigned(*v as u64))
            } else {
                Some(WorkerScalar::Signed(*v))
            }
        }
        (TinyInt | SmallInt | MediumInt | Int | BigInt | Bool | Bit { .. }, Value::UInt(v)) => {
            if column.unsigned || matches!(column.data_type, Bit { .. }) {
                Some(WorkerScalar::Unsigned(*v))
            } else {
                i64::try_from(*v).ok().map(WorkerScalar::Signed)
            }
        }
        (Float { .. }, Value::Float(v)) if !v.is_nan() => {
            Some(WorkerScalar::Float32(if *v == 0.0 {
                0.0f32.to_bits()
            } else {
                v.to_bits()
            }))
        }
        (Double { .. }, Value::Double(v)) if !v.is_nan() => {
            Some(WorkerScalar::Float64(if *v == 0.0 {
                0.0f64.to_bits()
            } else {
                v.to_bits()
            }))
        }
        (Decimal { .. }, Value::DecimalCompact(mag, scale)) => Some(WorkerScalar::DecimalCompact {
            mag: *mag as i128,
            scale: *scale,
        }),
        (Decimal { .. }, Value::Decimal(s)) => {
            // Rare slow path: parse the canonical decimal string into
            // (magnitude, scale) so the running form stays compact.
            let canonical = crate::util::decimal::canonical_decimal_str(s)?;
            let (parsed_mag, parsed_scale) =
                crate::util::decimal::parse_exact_decimal_str(&canonical)?;
            Some(WorkerScalar::DecimalCompact {
                mag: parsed_mag,
                scale: parsed_scale,
            })
        }
        (Date, Value::Date(v)) => Some(WorkerScalar::Temporal(*v as i64)),
        (Time { .. }, Value::Time(v))
        | (DateTime { .. }, Value::DateTime(v))
        | (Timestamp { .. }, Value::Timestamp(v)) => Some(WorkerScalar::Temporal(*v)),
        (Year, Value::Year(v)) => Some(WorkerScalar::Temporal(*v as i64)),
        (Binary { .. } | VarBinary { .. }, Value::Bytes(bytes)) => {
            let truncated = bytes.len() > ZONE_STRING_PREFIX_CHARS;
            Some(WorkerScalar::Bytes {
                prefix: bytes
                    .iter()
                    .take(ZONE_STRING_PREFIX_CHARS)
                    .copied()
                    .collect(),
                truncated,
            })
        }
        (Char { .. } | VarChar { .. } | Enum { .. } | Set { .. }, Value::String(text)) => {
            let prefix = zone_text_prefix_slice(text, collation_id);
            let normalized = if crate::charset::resolve_collation_id(collation_id)
                .is_some_and(|info| info.pad_space)
            {
                crate::util::pad_space::pad_space_trim_end_str(text)
            } else {
                text
            };
            let truncated = prefix.len() < normalized.len();
            let weight = crate::charset::collation_key_by_id(collation_id, prefix);
            Some(WorkerScalar::TextWeight { weight, truncated })
        }
        (Char { .. } | VarChar { .. } | Enum { .. } | Set { .. }, Value::SmallString(bytes)) => {
            let text = std::str::from_utf8(bytes.as_slice()).ok()?;
            let prefix = zone_text_prefix_slice(text, collation_id);
            let normalized = if crate::charset::resolve_collation_id(collation_id)
                .is_some_and(|info| info.pad_space)
            {
                crate::util::pad_space::pad_space_trim_end_str(text)
            } else {
                text
            };
            let truncated = prefix.len() < normalized.len();
            let weight = crate::charset::collation_key_by_id(collation_id, prefix);
            Some(WorkerScalar::TextWeight { weight, truncated })
        }
        _ => None,
    }
}

/// Determine prefix completeness without allocating a scalar or collation
/// weight. Interior values must contribute this bit even if neither endpoint
/// changes; otherwise a singleton prefix can masquerade as one exact value.
fn zone_value_prefix_truncated(value: &Value, collation_id: u16) -> bool {
    let text_truncated = |text: &str| {
        let normalized = if crate::charset::resolve_collation_id(collation_id)
            .is_some_and(|info| info.pad_space)
        {
            crate::util::pad_space::pad_space_trim_end_str(text)
        } else {
            text
        };
        zone_text_prefix_slice_normalized(normalized).len() < normalized.len()
    };
    match value {
        Value::Bytes(bytes) => bytes.len() > ZONE_STRING_PREFIX_CHARS,
        Value::String(text) => text_truncated(text),
        Value::SmallString(bytes) => {
            std::str::from_utf8(bytes.as_slice()).is_ok_and(text_truncated)
        }
        _ => false,
    }
}

impl ZoneMapWorkerBatch {
    pub fn for_table(table: &TableSchema) -> Self {
        if !table.zone_map_enabled() {
            return Self::default();
        }
        let relevant = |column: &&ColumnSchema| {
            !matches!(
                column.data_type,
                DataTypeKind::Text { .. }
                    | DataTypeKind::Blob { .. }
                    | DataTypeKind::Json
                    | DataTypeKind::Geometry { .. }
                    | DataTypeKind::Vector { .. }
            )
        };
        let count = table.columns.iter().filter(relevant).count();
        if count == 0 {
            return Self::default();
        }
        let mut lease = ZoneScratchLease::default();
        let base = count.saturating_mul(std::mem::size_of::<ZoneMapWorkerColumn>());
        // Membership selection owns a relevant-column vector, a sortable
        // candidate vector and bounded output maps, never bitmap-per-column
        // preallocation. Reserve that temporary configuration before calling it.
        let configured = table.stats.analyzed_at_unix_ms.is_some()
            && table.stats.row_count != 0
            && table.stats.heap_page_count != 0;
        let selected = count.min(ZONE_MEMBERSHIP_MAX_COLUMNS);
        let config = if configured {
            table
                .columns
                .len()
                .saturating_mul(std::mem::size_of::<(usize, u32)>())
                .saturating_add(count.saturating_mul(
                    3 * std::mem::size_of::<(usize, u32, u64, u64, ZoneMembershipLayout)>(),
                ))
                .saturating_add(2 * zone_tree_bytes::<u32, (ZoneMembershipLayout, u64)>(selected))
                .saturating_add(zone_tree_bytes::<u32, ZoneMembershipLayout>(selected))
        } else {
            0
        };
        if lease.reserve(base.saturating_add(config)).is_err() {
            return Self::default();
        }
        let memberships = zone_map_membership_columns(table);
        let mut entries = Vec::with_capacity(count);
        for (column_idx, column) in table.columns.iter().enumerate() {
            if !relevant(&column) {
                continue;
            }
            entries.push(ZoneMapWorkerColumn {
                column_idx,
                column_id: column.column_id,
                collation_id: crate::charset::collation_id(
                    column.collation.as_deref().unwrap_or("utf8mb4_0900_ai_ci"),
                ),
                state: ZoneMapState::Empty,
                has_null: false,
                has_non_null: false,
                truncated: false,
                min: None,
                max: None,
                membership: memberships
                    .get(&column.column_id)
                    .copied()
                    .map(ZoneMembership::empty_with_layout),
            });
        }
        drop(memberships);
        lease.shrink_to(entries.capacity() * std::mem::size_of::<ZoneMapWorkerColumn>());
        Self {
            entries,
            text_scratch: Vec::new(),
            lease,
        }
    }

    pub fn is_active(&self) -> bool {
        !self.entries.is_empty()
    }

    fn owned_bytes(&self) -> usize {
        self.entries
            .capacity()
            .saturating_mul(std::mem::size_of::<ZoneMapWorkerColumn>())
            .saturating_add(self.text_scratch.capacity())
            .saturating_add(
                self.entries
                    .iter()
                    .map(ZoneMapWorkerColumn::owned_bytes)
                    .sum::<usize>(),
            )
    }

    fn disable(&mut self) {
        // Inactive means the writer must revoke the persisted root. Never
        // leave a partially observed aggregate looking like Empty or Ready.
        self.entries = Vec::new();
        self.text_scratch = Vec::new();
        self.lease = ZoneScratchLease::default();
    }

    pub fn observe_row(&mut self, table: &TableSchema, values: &[Value]) {
        for index in 0..self.entries.len() {
            let entry = &self.entries[index];
            if entry.state == ZoneMapState::Unknown {
                continue;
            }
            let Some(column) = table.columns.get(entry.column_idx) else {
                self.disable();
                return;
            };
            if column.column_id != entry.column_id {
                self.disable();
                return;
            }
            let before = self.lease.bytes();
            let old_column = entry.owned_bytes();
            let old_scratch = self.text_scratch.capacity();
            let value = values.get(entry.column_idx);
            let peak = entry.observation_peak(value, old_scratch);
            if self.lease.reserve(peak).is_err() {
                self.disable();
                return;
            }
            let hash = if entry
                .membership
                .as_ref()
                .is_some_and(|membership| !matches!(membership, ZoneMembership::Saturated))
            {
                value
                    .filter(|value| !matches!(value, Value::Null))
                    .filter(|value| !matches!(value, Value::Decimal(text) if text.len() > 256))
                    .and_then(|value| worker_scalar_from_value(column, entry.collation_id, value))
                    .map(|scalar| zone_membership_hash_worker(&scalar))
            } else {
                None
            };
            let membership_peak = entry.membership_growth(hash);
            if self.lease.reserve(membership_peak).is_err() {
                self.disable();
                return;
            }
            self.entries[index].observe_value(table, values, &mut self.text_scratch, hash);
            let desired = before
                .saturating_sub(old_column)
                .saturating_sub(old_scratch)
                .saturating_add(self.entries[index].owned_bytes())
                .saturating_add(self.text_scratch.capacity());
            if desired > self.lease.bytes() {
                // A future codec expansion must not leave unaccounted state.
                let _ = ZoneScratchLease::denied();
                self.disable();
                return;
            }
            if desired < self.lease.bytes() {
                self.lease.shrink_to(desired);
            }
        }
    }
}

impl Clone for ZoneMapWorkerBatch {
    fn clone(&self) -> Self {
        let mut lease = ZoneScratchLease::default();
        if lease.reserve(self.owned_bytes()).is_err() {
            return Self::default();
        }
        let copy = Self {
            entries: self.entries.clone(),
            text_scratch: self.text_scratch.clone(),
            lease,
        };
        let actual = copy.owned_bytes();
        let mut copy = copy;
        copy.lease.shrink_to(actual);
        copy
    }
}

impl std::fmt::Debug for ZoneMapWorkerBatch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ZoneMapWorkerBatch")
            .field("entries", &self.entries)
            .field("text_scratch_capacity", &self.text_scratch.capacity())
            .field("charged_bytes", &self.lease.bytes())
            .finish()
    }
}

impl ZoneMapWorkerColumn {
    fn owned_bytes(&self) -> usize {
        self.min
            .as_ref()
            .map_or(0, WorkerScalar::owned_bytes)
            .saturating_add(self.max.as_ref().map_or(0, WorkerScalar::owned_bytes))
            .saturating_add(
                self.membership
                    .as_ref()
                    .map_or(0, ZoneMembership::capacity_bytes),
            )
    }

    fn membership_growth(&self, hash: Option<u64>) -> usize {
        let Some(hash) = hash else {
            return 0;
        };
        match &self.membership {
            Some(ZoneMembership::Exact {
                hashes,
                bloom_words,
                ..
            }) if (hashes.len() >= ZONE_MEMBERSHIP_EXACT_LIMIT
                || hashes.len() == hashes.capacity())
                && hashes.binary_search(&hash).is_err() =>
            {
                if hashes.len() >= ZONE_MEMBERSHIP_EXACT_LIMIT {
                    usize::from(*bloom_words) * 8
                } else if hashes.len() == hashes.capacity() {
                    hashes.capacity().saturating_mul(2).max(4).saturating_mul(8)
                } else {
                    0
                }
            }
            _ => 0,
        }
    }

    fn observation_peak(&self, value: Option<&Value>, scratch_capacity: usize) -> usize {
        if value.is_none_or(|value| matches!(value, Value::Null)) {
            return 0;
        }
        let text_peak = |text: &str| {
            let prefix = zone_text_prefix_slice(text, self.collation_id);
            // ASCII weights are a prefix copy. General Unicode normalization
            // has bounded expansion for a 16-character prefix; keep one
            // column's temporary headroom, not a schema-wide bitmap reserve.
            let weight = if prefix.is_ascii() {
                prefix.len()
            } else {
                PAGE_SIZE
            };
            let scratch = if weight > scratch_capacity {
                weight.max(scratch_capacity.saturating_mul(2)).max(8)
            } else {
                0
            };
            weight.saturating_mul(3).saturating_add(scratch)
        };
        let scalar = match value.expect("nonnull observation") {
            Value::String(text) => text_peak(text),
            Value::SmallString(bytes) => std::str::from_utf8(bytes.as_slice()).map_or(0, text_peak),
            Value::Bytes(bytes) => bytes.len().min(ZONE_STRING_PREFIX_CHARS).saturating_mul(3),
            Value::Decimal(text) if text.len() <= 256 => {
                text.len().saturating_mul(4).saturating_add(64)
            }
            _ => 0,
        };
        scalar
    }

    fn observe_value(
        &mut self,
        table: &TableSchema,
        values: &[Value],
        text_scratch: &mut Vec<u8>,
        membership_hash: Option<u64>,
    ) {
        let entry = self;
        if entry.state == ZoneMapState::Unknown {
            return;
        }
        let column = &table.columns[entry.column_idx];
        let value = match values.get(entry.column_idx) {
            Some(v) => v,
            None => {
                entry.state = ZoneMapState::Unknown;
                entry.min = None;
                entry.max = None;
                entry.membership = None;
                return;
            }
        };
        if matches!(value, Value::Decimal(text) if text.len() > 256) {
            entry.state = ZoneMapState::Unknown;
            entry.min = None;
            entry.max = None;
            entry.membership = None;
            return;
        }
        if matches!(value, Value::Null) {
            entry.has_null = true;
            if entry.state == ZoneMapState::Empty {
                entry.state = ZoneMapState::Ready;
            }
            return;
        }
        if !entry.truncated {
            entry.truncated = zone_value_prefix_truncated(value, entry.collation_id);
        }
        if let Some(membership) = &mut entry.membership {
            if !matches!(membership, ZoneMembership::Saturated) {
                let Some(hash) = membership_hash else {
                    entry.state = ZoneMapState::Unknown;
                    entry.min = None;
                    entry.max = None;
                    entry.membership = None;
                    return;
                };
                membership.insert_hash(hash);
            }
        }
        // Cheap compare: if we already have a running min AND max,
        // and the incoming value is inside `[min, max]`, we skip
        // allocation for this row entirely — the common case for
        // stable/near-stable distributions.
        let advance_min: Option<bool>;
        let advance_max: Option<bool>;
        match (entry.min.as_ref(), entry.max.as_ref()) {
            (Some(cur_min), Some(cur_max)) => {
                match compare_value_to_worker(
                    column,
                    entry.collation_id,
                    value,
                    cur_min,
                    text_scratch,
                ) {
                    Ok(Some(ord)) => {
                        advance_min = Some(ord == std::cmp::Ordering::Less);
                    }
                    Ok(None) => {
                        entry.state = ZoneMapState::Unknown;
                        entry.min = None;
                        entry.max = None;
                        entry.membership = None;
                        return;
                    }
                    Err(()) => {
                        entry.state = ZoneMapState::Unknown;
                        entry.min = None;
                        entry.max = None;
                        entry.membership = None;
                        return;
                    }
                }
                match compare_value_to_worker(
                    column,
                    entry.collation_id,
                    value,
                    cur_max,
                    text_scratch,
                ) {
                    Ok(Some(ord)) => {
                        advance_max = Some(ord == std::cmp::Ordering::Greater);
                    }
                    Ok(None) => {
                        entry.state = ZoneMapState::Unknown;
                        entry.min = None;
                        entry.max = None;
                        entry.membership = None;
                        return;
                    }
                    Err(()) => {
                        entry.state = ZoneMapState::Unknown;
                        entry.min = None;
                        entry.max = None;
                        entry.membership = None;
                        return;
                    }
                }
                entry.has_non_null = true;
                entry.state = ZoneMapState::Ready;
                if !(advance_min.unwrap_or(false) || advance_max.unwrap_or(false)) {
                    // Fast path: neither endpoint moves. No allocation
                    // and no promotion — exit early.
                    return;
                }
            }
            _ => {
                advance_min = Some(true);
                advance_max = Some(true);
            }
        }
        // Slow path: allocate a fresh WorkerScalar so we can install
        // it on the min and/or max slot.
        let Some(new_scalar) = worker_scalar_from_value(column, entry.collation_id, value) else {
            entry.state = ZoneMapState::Unknown;
            entry.min = None;
            entry.max = None;
            entry.membership = None;
            return;
        };
        entry.has_non_null = true;
        entry.state = ZoneMapState::Ready;
        entry.truncated |= new_scalar.was_truncated();
        match (advance_min.unwrap_or(false), advance_max.unwrap_or(false)) {
            (true, true) => {
                entry.max = Some(new_scalar.clone());
                entry.min = Some(new_scalar);
            }
            (true, false) => entry.min = Some(new_scalar),
            (false, true) => entry.max = Some(new_scalar),
            (false, false) => {}
        }
    }
}
#[cfg(test)]
mod zone_map_worker_budget_tests {
    use super::*;

    #[test]
    fn zone_entry_batch_transfer_releases_capacity_and_recovers_on_denial() {
        let baseline = scratch_bytes();
        let mut left = ChargedZoneEntries::new(0).unwrap();
        left.push_mutation(
            1,
            ZoneMutation {
                epoch: 1,
                last_ms: 0,
            },
        )
        .unwrap();
        let mut right = ChargedZoneEntries::new(0).unwrap();
        right
            .push_mutation(
                2,
                ZoneMutation {
                    epoch: 2,
                    last_ms: 0,
                },
            )
            .unwrap();
        let limit = Limit::set(left.lease.bytes() as u64);
        let right = match left.try_append(right) {
            Err(right) => right,
            Ok(()) => panic!("small lease must use the streaming fallback"),
        };
        assert_eq!(left.len(), 1);
        assert_eq!(right.len(), 1);
        drop(limit);
        assert!(left.try_append(right).is_ok());
        assert_eq!(left.len(), 2);
        assert_eq!(left[0].0.zone, 1);
        assert_eq!(left[1].0.zone, 2);
        assert_eq!(scratch_bytes(), baseline + left.lease.bytes());
        drop(left);
        assert_eq!(scratch_bytes(), baseline);
    }
    struct Limit(u64);
    impl Limit {
        fn set(bytes: u64) -> Self {
            Self(ZONE_MAP_TEST_SCRATCH_LIMIT.swap(bytes, Ordering::SeqCst))
        }
    }
    impl Drop for Limit {
        fn drop(&mut self) {
            ZONE_MAP_TEST_SCRATCH_LIMIT.store(self.0, Ordering::SeqCst);
        }
    }
    fn scratch_bytes() -> usize {
        crate::engine::executor::mem_budget::controlled_memory_snapshot().2[1]
    }
    fn fixture(
        columns: &str,
    ) -> (
        tempfile::TempDir,
        crate::engine::StorageQueryEngine,
        Arc<TableSchema>,
    ) {
        fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let mut config = StorageConfig::from_env();
        config.buffer_pool_pages = 64;
        let engine =
            crate::engine::StorageQueryEngine::open_with_storage_config_without_background_workers(
                dir.path(),
                config,
            )
            .unwrap();
        engine
            .execute(&format!(
                "CREATE TABLE worker_budget ({columns}) ZONE_MAP=ON"
            ))
            .unwrap();
        let table = engine.db_for_tests().table("test.worker_budget").unwrap();
        (dir, engine, table)
    }

    #[test]
    fn zone_map_worker_budget_refusal_disables_aggregate_and_persists_invalidation() {
        let (_dir, engine, table) = fixture("v INT");
        engine
            .execute("INSERT INTO worker_budget VALUES (1)")
            .unwrap();
        let db = engine.db_for_tests();
        let map = db.zone_map_for(table.id, &table).unwrap();
        let baseline = scratch_bytes();
        let limit = Limit::set(0);
        let mut worker = ZoneMapWorkerBatch::for_table(&table);
        worker.observe_row(&table, &[Value::Int(9000)]);
        assert!(!worker.is_active());
        assert_eq!(worker.lease.bytes(), 0);
        assert_eq!(scratch_bytes(), baseline);
        drop(limit);
        db.apply_worker_batch(
            0,
            table.id,
            &[RowId {
                page_id: 0,
                slot_id: 0,
            }],
            &worker,
        )
        .unwrap();
        assert!(
            !map.store.trusted(),
            "inactive worker must persist Unknown, never Empty"
        );
        db.checkpoint().unwrap();
        db.forget_zone_map(table.id).unwrap();
        assert!(!db.zone_map_for(table.id, &table).unwrap().store.trusted());
    }

    #[test]
    fn zone_map_worker_membership_growth_is_charged_and_refusal_releases_everything() {
        let (_dir, _engine, table) = fixture("v INT");
        let baseline = scratch_bytes();
        let mut worker = ZoneMapWorkerBatch::for_table(&table);
        worker.entries[0].membership = Some(ZoneMembership::empty_with_layout(
            ZoneMembershipLayout::default(),
        ));
        for value in 0..ZONE_MEMBERSHIP_EXACT_LIMIT {
            worker.observe_row(&table, &[Value::Int(value as i64)]);
        }
        assert!(worker.is_active());
        assert_eq!(worker.lease.bytes(), worker.owned_bytes());
        assert_eq!(scratch_bytes(), baseline + worker.lease.bytes());
        let limit = Limit::set(worker.lease.bytes() as u64);
        worker.observe_row(&table, &[Value::Int(0)]);
        assert!(
            worker.is_active(),
            "duplicate membership needs no bitmap reserve"
        );
        worker.observe_row(
            &table,
            &[Value::Int(ZONE_MEMBERSHIP_EXACT_LIMIT as i64 + 1)],
        );
        assert!(
            !worker.is_active(),
            "denied exact-to-Bloom growth revokes the aggregate"
        );
        assert_eq!(worker.lease.bytes(), 0);
        assert_eq!(scratch_bytes(), baseline);
        drop(limit);
    }

    #[test]
    fn zone_map_worker_large_values_are_borrowed_and_clone_has_independent_charge() {
        let (_dir, _engine, table) =
            fixture("txt VARCHAR(4096), bytes VARBINARY(4096), amount DECIMAL(38,0)");
        let values = vec![
            Value::String("A".repeat(8 * 1024 * 1024)),
            Value::Bytes(vec![7; 8 * 1024 * 1024]),
            Value::Decimal("0".repeat(1024 * 1024)),
        ];
        let baseline = scratch_bytes();
        let limit = Limit::set(8 * 1024);
        let mut worker = ZoneMapWorkerBatch::for_table(&table);
        worker.observe_row(&table, &values);
        assert!(worker.is_active());
        assert!(worker.entries[0].truncated && worker.entries[1].truncated);
        assert_eq!(worker.entries[2].state, ZoneMapState::Unknown);
        assert_eq!(worker.lease.bytes(), worker.owned_bytes());
        assert!(
            worker.lease.bytes() < 4096,
            "only prefixes and column configuration are retained"
        );
        assert_eq!(scratch_bytes(), baseline + worker.lease.bytes());
        let copied = worker.clone();
        assert!(copied.is_active());
        assert_eq!(copied.lease.bytes(), copied.owned_bytes());
        assert_eq!(
            scratch_bytes(),
            baseline + worker.lease.bytes() + copied.lease.bytes()
        );
        drop(copied);
        drop(worker);
        assert_eq!(scratch_bytes(), baseline);
        drop(limit);
        assert_eq!(
            match &values[0] {
                Value::String(value) => value.len(),
                _ => 0,
            },
            8 * 1024 * 1024
        );
    }
    #[test]
    fn zone_map_scanner_text_cache_and_shared_scratch_keep_independent_leases() {
        let baseline = scratch_bytes();
        let info =
            crate::charset::resolve_collation_id(crate::charset::UTF8MB4_0900_AI_CI_ID).unwrap();
        let mut scratch = Vec::new();
        let mut text_lease = ZoneScratchLease::default();
        let mut state = WorkerColumnState::default();
        state.enable_membership_with_layout(ZoneMembershipLayout::default());
        for id in 0..16 {
            state.observe_text_bytes_for_info(
                info,
                format!("key-{id:02}").as_bytes(),
                &mut scratch,
                &mut text_lease,
            );
            assert_eq!(state.state, ZoneMapState::Ready);
            assert_eq!(state.lease.bytes(), state.owned_bytes());
        }
        assert_eq!(state.state, ZoneMapState::Ready);
        assert!(state.text_weight_cache.spilled());
        assert_eq!(state.lease.bytes(), state.owned_bytes());
        assert_eq!(text_lease.bytes(), scratch.capacity());
        assert_eq!(
            scratch_bytes(),
            baseline + state.lease.bytes() + text_lease.bytes()
        );
        drop(state);
        assert_eq!(
            scratch_bytes(),
            baseline + text_lease.bytes(),
            "shared scratch outlives a completed zone"
        );
        let mut next = WorkerColumnState::default();
        let large = vec![b'z'; 8 * 1024 * 1024];
        next.observe_text_bytes_for_info(info, &large, &mut scratch, &mut text_lease);
        assert_eq!(next.state, ZoneMapState::Ready);
        assert!(next.truncated);
        assert!(next.lease.bytes() < 1024);
        assert_eq!(next.lease.bytes(), next.owned_bytes());
        drop(next);
        drop(scratch);
        drop(text_lease);
        assert_eq!(scratch_bytes(), baseline);
    }

    #[test]
    fn zone_map_scanner_interior_text_needs_no_allocation_admission() {
        let baseline = scratch_bytes();
        let info =
            crate::charset::resolve_collation_id(crate::charset::UTF8MB4_0900_AI_CI_ID).unwrap();
        let mut scratch = Vec::with_capacity(PAGE_SIZE);
        let mut text_lease = ZoneScratchLease::default();
        text_lease.reserve(scratch.capacity()).unwrap();
        let mut state = WorkerColumnState::default();
        state.text_weight_cache_saturated = true;
        for text in ["a".repeat(128), "z".repeat(128)] {
            state.observe_text_bytes_for_info(info, text.as_bytes(), &mut scratch, &mut text_lease);
        }
        let bounds = |state: &WorkerColumnState| {
            [&state.min, &state.max].map(|bound| match bound {
                Some(WorkerScalar::TextWeight { weight, truncated }) => {
                    (weight.clone(), *truncated)
                }
                _ => panic!("expected text bounds"),
            })
        };
        let expected = bounds(&state);
        let charged = state.lease.bytes();
        let limit = Limit::set(charged as u64);
        for text in ["middle".repeat(32), "é".repeat(128), "MIDDLE".repeat(32)] {
            state.observe_text_bytes_for_info(info, text.as_bytes(), &mut scratch, &mut text_lease);
            assert_eq!(state.state, ZoneMapState::Ready);
            assert_eq!(bounds(&state), expected);
            assert_eq!(state.lease.bytes(), charged);
            assert_eq!(state.lease.bytes(), state.owned_bytes());
        }
        drop(limit);
        drop(state);
        drop(scratch);
        drop(text_lease);
        assert_eq!(scratch_bytes(), baseline);
    }

    #[test]
    fn zone_map_scanner_denied_membership_growth_stays_unknown() {
        let baseline = scratch_bytes();
        let mut state = WorkerColumnState::default();
        state.enable_membership_with_layout(ZoneMembershipLayout::default());
        for value in 0..ZONE_MEMBERSHIP_EXACT_LIMIT {
            state.observe_worker_scalar(WorkerScalar::Signed(value as i64));
        }
        let limit = Limit::set(state.lease.bytes() as u64);
        state.observe_worker_scalar(WorkerScalar::Signed(0));
        assert_eq!(state.state, ZoneMapState::Ready);
        state.observe_worker_scalar(WorkerScalar::Signed(ZONE_MEMBERSHIP_EXACT_LIMIT as i64 + 1));
        assert_eq!(
            state.state,
            ZoneMapState::Unknown,
            "caller must not overwrite refusal with Ready"
        );
        assert!(state.membership.is_none() && state.min.is_none() && state.max.is_none());
        assert_eq!(state.lease.bytes(), 0);
        assert_eq!(scratch_bytes(), baseline);
        drop(limit);
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct ZoneMutation {
    epoch: u64,
    last_ms: u64,
}

#[derive(Clone, Copy, Default)]
struct UnknownColumnProof {
    zone: u64,
    columns: u64,
}

// A bounded runtime frontier for mutations that leave durable proofs intact.
// Unused count credits are already included in durable counters; losing an
// entry only increases maintenance estimates. Evicting an entry invalidates
// builders spanning that eviction, without starving unrelated cold zones.
struct NeutralMutations {
    zones: crate::storage::txn::FastHashMap<u64, NeutralZone>,
    /// First-noted order of `zones`, oldest first, for FIFO eviction.
    order: std::collections::VecDeque<u64>,
    /// Charge for the reserved capacity of `zones` and `order`.
    zone_charge: Option<crate::engine::executor::mem_budget::ControlledMemoryCharge>,
    overflow_epoch: u64,
    // Negative pruning evidence only, never decoded bounds or Bloom data.
    // Slots are tagged, bounded, and invalidated before any shrinking rebuild.
    unknown_proofs: Vec<UnknownColumnProof>,
    unknown_proof_charge: Option<crate::engine::executor::mem_budget::ControlledMemoryCharge>,
}

#[derive(Clone, Copy)]
struct NeutralZone {
    mutation: ZoneMutation,
    unused_rows: u64,
    unused_stale: u64,
}

/// Zones per table whose mutation provenance and count credits stay
/// runtime-only. Random point DML spreads over every zone of a table, so the
/// bound must cover tables of several GiB (one zone per MiB by default):
/// with too few entries a zone loses its credits before reusing them and
/// every DELETE/UPDATE rewrites the zone counters, i.e. one logged summary
/// page per row. Memory grows only with zones actually mutated.
const NEUTRAL_MUTATION_ZONES: usize = 4096;
/// Charged bytes per reserved entry: key, value, control byte, order slot.
const NEUTRAL_ZONE_BYTES: usize =
    2 * std::mem::size_of::<u64>() + std::mem::size_of::<NeutralZone>() + 1;

impl Default for NeutralMutations {
    fn default() -> Self {
        Self {
            zones: Default::default(),
            order: std::collections::VecDeque::new(),
            zone_charge: None,
            overflow_epoch: 0,
            unknown_proofs: Vec::new(),
            unknown_proof_charge: None,
        }
    }
}
impl NeutralMutations {
    #[cfg(test)]
    fn stale_credit(&self, id: u64) -> u64 {
        self.credits(id).1
    }
    fn credits(&self, id: u64) -> (u64, u64) {
        self.zones
            .get(&id)
            .map_or((0, 0), |zone| (zone.unused_rows, zone.unused_stale))
    }
    fn set_credits(&mut self, id: u64, rows: u64, stale: u64) {
        if let Some(zone) = self.zones.get_mut(&id) {
            zone.unused_rows = rows;
            zone.unused_stale = stale;
        }
    }
    fn clear_credits(&mut self) {
        for zone in self.zones.values_mut() {
            zone.unused_rows = 0;
            zone.unused_stale = 0;
        }
    }
    fn get(&self, id: u64) -> Option<ZoneMutation> {
        self.zones.get(&id).map(|zone| zone.mutation)
    }
    fn note(&mut self, id: u64, mutation: ZoneMutation) {
        if let Some(zone) = self.zones.get_mut(&id) {
            zone.mutation = mutation;
            return;
        }
        if self.zones.len() >= NEUTRAL_MUTATION_ZONES || !self.reserve_zone() {
            // Evicting, like failing to record, drops runtime provenance:
            // builders spanning it revalidate through the overflow epoch.
            self.overflow_epoch = self.overflow_epoch.wrapping_add(1);
            let Some(oldest) = self.order.pop_front() else {
                return;
            };
            self.zones.remove(&oldest);
        }
        self.zones.insert(
            id,
            NeutralZone {
                mutation,
                unused_rows: 0,
                unused_stale: 0,
            },
        );
        self.order.push_back(id);
    }
    /// Make room for one more zone, charging the grown capacity first.
    fn reserve_zone(&mut self) -> bool {
        if self.zones.len() < self.zones.capacity() && self.order.len() < self.order.capacity() {
            return true;
        }
        let capacity = (self.zones.len() * 2).clamp(16, NEUTRAL_MUTATION_ZONES);
        let bytes = capacity * NEUTRAL_ZONE_BYTES;
        let admitted = match &mut self.zone_charge {
            Some(charge) => {
                charge.bytes() >= bytes || charge.try_grow_optional(bytes - charge.bytes())
            }
            None => {
                self.zone_charge =
                    crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                        crate::engine::executor::mem_budget::ControlledMemoryCategory::DerivedCache,
                        bytes,
                    );
                self.zone_charge.is_some()
            }
        };
        if admitted {
            self.zones.reserve(capacity - self.zones.len());
            self.order.reserve(capacity - self.order.len());
        }
        admitted
    }
    fn memory_bytes(&self) -> usize {
        self.zone_charge.as_ref().map_or(0, |charge| charge.bytes())
            + self.unknown_proofs.capacity() * std::mem::size_of::<UnknownColumnProof>()
    }
}

pub(crate) struct ZoneMap {
    storage_id: u64,
    zone_pages: u32,
    schema_fingerprint: u64,
    schema_object_id: AtomicU64,
    schema_version: AtomicU64,
    columns: RwLock<Arc<[u32]>>,
    membership_columns: RwLock<Arc<[(u32, ZoneMembershipLayout)]>>,
    data_gate: RwLock<()>,
    // Widening reads the current zone before publishing its union. Serialize
    // that read/modify/write only against updates to the same zone; rebuilds,
    // invalidation and retirement retain the exclusive data gate.
    merge_latches: [Mutex<()>; ZONE_MAP_MERGE_LATCHES],
    // Single-zone widenings that found their stripe latch busy. The next
    // latch holder publishes all of them as one union (see `merge`).
    merge_queues: [Mutex<Vec<Arc<QueuedZoneMerge>>>; ZONE_MAP_MERGE_LATCHES],
    retired: AtomicBool,
    store: Arc<zone_map_page::mutable::Store>,
    missing_is_empty: AtomicBool,
    generation: AtomicU64,
    mutation_epoch: AtomicU64,
    // Runtime provenance only; summaries remain exclusively in shared pages.
    neutral_mutations: Mutex<NeutralMutations>,
    invalidation_epoch: AtomicU64,
    last_mutation_ms: AtomicU64,
    maintenance_cursor: AtomicU64,
    // Presence-only prefix proof for an Unknown-hole root. Both fields are
    // read/updated under data_gate; a reset invalidates the proof by epoch.
    maintenance_coverage_cursor: AtomicU64,
    maintenance_coverage_epoch: AtomicU64,
    #[cfg(test)]
    coverage_publish_barrier: Mutex<Option<Arc<std::sync::Barrier>>>,
    maintenance_retry_ms: AtomicU64,
    last_persisted_lsn: AtomicU64,
    file_generation: AtomicU64,
    last_error: Mutex<Option<String>>,
    rebuild_error: Mutex<Option<String>>,
    dirty: AtomicBool,
    rebuild_state: AtomicU8,
    rebuild_zones_total: AtomicU64,
    rebuild_zones_done: AtomicU64,
    rebuild_started_ms: AtomicU64,
    rebuild_finished_ms: AtomicU64,
    trusted_base: AtomicBool,
    cache_admitted: AtomicBool,
    last_access: AtomicU64,
    path: Mutex<Option<PathBuf>>,
    derived_charge: Mutex<Option<crate::engine::executor::mem_budget::ControlledMemoryCharge>>,
    memory_recounts: AtomicU64,
}

impl std::fmt::Debug for ZoneMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZoneMap")
            .field("storage_id", &self.storage_id)
            .field("zone_pages", &self.zone_pages)
            .field("schema_fingerprint", &self.schema_fingerprint)
            .finish_non_exhaustive()
    }
}

/// Snapshot and publication guard for an explicit ANALYZE whose statistics
/// workers share page ownership and decoding with the zone image builder.
struct AnalyzeZoneBuildContext {
    storage_id: u64,
    map: Arc<ZoneMap>,
    txn: TxnView,
    build_scn: u64,
}

impl ZoneMap {
    fn new(
        storage_id: u64,
        zone_pages: u32,
        schema_fingerprint: u64,
        store: Arc<zone_map_page::mutable::Store>,
        missing_is_empty: bool,
    ) -> Self {
        let initial_epoch = store.revision();
        let trusted_base = store
            .root_snapshot()
            .is_some_and(|root| root.missing_is_empty || root.records != 0);
        let map = Self {
            storage_id,
            zone_pages,
            schema_fingerprint,
            store,
            schema_object_id: AtomicU64::new(0),
            schema_version: AtomicU64::new(0),
            columns: RwLock::new(Arc::from([])),
            membership_columns: RwLock::new(Arc::from([])),
            data_gate: RwLock::new(()),
            merge_latches: std::array::from_fn(|_| Mutex::new(())),
            merge_queues: std::array::from_fn(|_| Mutex::new(Vec::new())),
            retired: AtomicBool::new(false),
            missing_is_empty: AtomicBool::new(missing_is_empty),
            generation: AtomicU64::new(1),
            mutation_epoch: AtomicU64::new(initial_epoch),
            neutral_mutations: Mutex::new(NeutralMutations::default()),
            invalidation_epoch: AtomicU64::new(0),
            last_mutation_ms: AtomicU64::new(0),
            maintenance_cursor: AtomicU64::new(0),
            maintenance_coverage_cursor: AtomicU64::new(0),
            maintenance_coverage_epoch: AtomicU64::new(0),
            #[cfg(test)]
            coverage_publish_barrier: Mutex::new(None),
            maintenance_retry_ms: AtomicU64::new(0),
            last_persisted_lsn: AtomicU64::new(0),
            file_generation: AtomicU64::new(1),
            last_error: Mutex::new(None),
            rebuild_error: Mutex::new(None),
            dirty: AtomicBool::new(false),
            rebuild_state: AtomicU8::new(0),
            rebuild_zones_total: AtomicU64::new(0),
            rebuild_zones_done: AtomicU64::new(0),
            rebuild_started_ms: AtomicU64::new(0),
            rebuild_finished_ms: AtomicU64::new(0),
            trusted_base: AtomicBool::new(trusted_base),
            cache_admitted: AtomicBool::new(true),
            last_access: AtomicU64::new(ZONE_MAP_CACHE_CLOCK.fetch_add(1, Ordering::Relaxed)),
            path: Mutex::new(None),
            derived_charge: Mutex::new(None),
            memory_recounts: AtomicU64::new(0),
        };
        map.sync_derived_charge();
        map
    }

    fn touch(&self) {
        self.last_access.store(
            ZONE_MAP_CACHE_CLOCK.fetch_add(1, Ordering::Relaxed),
            Ordering::Relaxed,
        );
    }

    fn bind_table_schema(&self, table: &TableSchema) {
        let object_id = if table.object_id == 0 {
            table.id
        } else {
            table.object_id
        };
        if self.matches_bound_table_schema(table) {
            return;
        }
        if let Ok(mut neutral) = self.neutral_mutations.lock() {
            neutral.unknown_proofs.fill(UnknownColumnProof::default());
        }
        let columns: Arc<[u32]> = zone_map_relevant_columns(table)
            .into_iter()
            .map(|(_, id)| id)
            .collect::<Vec<_>>()
            .into();
        let membership: Arc<[(u32, ZoneMembershipLayout)]> = zone_map_membership_columns(table)
            .into_iter()
            .collect::<Vec<_>>()
            .into();
        if let Ok(mut slot) = self.columns.write() {
            *slot = columns;
        }
        if let Ok(mut slot) = self.membership_columns.write() {
            *slot = membership;
        }
        self.schema_object_id.store(object_id, Ordering::Release);
        self.schema_version
            .store(table.schema_version, Ordering::Release);
        self.sync_derived_charge();
    }

    fn cached_membership_columns(&self) -> Arc<[(u32, ZoneMembershipLayout)]> {
        self.membership_columns
            .read()
            .map(|columns| Arc::clone(&columns))
            .unwrap_or_else(|_| Arc::from([]))
    }

    fn matches_bound_table_schema(&self, table: &TableSchema) -> bool {
        let object_id = if table.object_id == 0 {
            table.id
        } else {
            table.object_id
        };
        self.schema_object_id.load(Ordering::Acquire) == object_id
            && self.schema_version.load(Ordering::Acquire) == table.schema_version
    }

    /// Unbound construction helpers may populate a map before a catalog is
    /// available. This stores only column identities, never zone contents.
    fn register_unbound_columns(&self, zones: &BTreeMap<u64, ZoneSummary>) {
        if self.schema_object_id.load(Ordering::Acquire) != 0 {
            return;
        }
        let changed = if let Ok(mut columns) = self.columns.write() {
            let mut ids: BTreeSet<_> = columns.iter().copied().collect();
            for zone in zones.values() {
                ids.extend(zone.columns.keys().copied());
            }
            if ids.len() != columns.len() {
                *columns = ids.into_iter().collect::<Vec<_>>().into();
                true
            } else {
                false
            }
        } else {
            false
        };
        if changed {
            self.sync_derived_charge();
        }
    }

    /// Callers hold data_gate while reading multiple records. The returned
    /// object contains a single zone and is never retained by this handle.
    fn cost_probe_scope(&self) -> zone_map_page::CostProbeScope {
        self.store.cost_probe_scope()
    }

    #[cfg(test)]
    fn may_match_zone(
        &self,
        id: u64,
        program: &ZonePredicateProgram,
        safe_read_scn: u64,
    ) -> Result<bool, QueryError> {
        let mut matched = true;
        self.visit_matches(
            std::iter::once(id),
            program,
            safe_read_scn,
            |_, value, _| {
                matched = value;
                Ok(())
            },
        )?;
        Ok(matched)
    }

    fn visit_matches(
        &self,
        zones: impl IntoIterator<Item = u64>,
        program: &ZonePredicateProgram,
        safe_read_scn: u64,
        mut consume: impl FnMut(u64, bool, u64) -> Result<(), QueryError>,
    ) -> Result<(), QueryError> {
        if self.retired.load(Ordering::Acquire) {
            return Err(Self::access_error("retired zone map"));
        }
        let mut consumer_failed = false;
        self.store
            .visit_matches(zones, program, safe_read_scn, |zone, matched, bytes| {
                consume(zone, matched, bytes).inspect_err(|_| consumer_failed = true)
            })
            .inspect_err(|err| {
                // Cancellation/output allocation is not evidence of corrupt metadata.
                if !consumer_failed {
                    self.note_store_failure(err, true);
                }
            })
    }

    fn read_zone(
        &self,
        id: u64,
        requested: Option<&BTreeSet<u32>>,
    ) -> Result<Option<ChargedZoneSummary>, QueryError> {
        match self.read_zone_inner(id, requested, true) {
            Ok(zone) => Ok(zone),
            Err(err) => {
                self.note_store_failure(&err, true);
                Err(err)
            }
        }
    }

    fn read_zone_inner(
        &self,
        id: u64,
        requested: Option<&BTreeSet<u32>>,
        include_membership: bool,
    ) -> Result<Option<ChargedZoneSummary>, QueryError> {
        use zone_map_page::{summary, Kind};
        if self.retired.load(Ordering::Acquire) {
            return Err(Self::access_error("retired zone map"));
        }
        let Some(value) = self
            .store
            .get(summary::key(0, Kind::ZoneMeta, id), |bytes| {
                bytes.map(summary::decode_meta).transpose()
            })?
        else {
            return Ok(None);
        };
        let mut value = value;
        self.apply_runtime_counts(id, &mut value)?;
        let mut zone = ChargedZoneSummary {
            value,
            lease: ZoneScratchLease::default(),
        };
        let all_columns = self
            .columns
            .read()
            .map_err(|_| Self::access_error("zone-map schema lock poisoned"))?
            .clone();
        let mut read_column = |column| -> Result<(), QueryError> {
            let before = zone.lease.bytes();
            let growth = zone.column_growth(column);
            let decoded = self
                .store
                .get(summary::key(column, Kind::Bounds, id), |bytes| {
                    bytes
                        .map(|bytes| {
                            zone.lease.reserve(
                                growth
                                    .saturating_add(bytes.len().saturating_mul(8))
                                    .saturating_add(512),
                            )?;
                            summary::decode_bounds(bytes)
                        })
                        .transpose()
                })?;
            if let Some(mut bounds) = decoded {
                if include_membership {
                    bounds.membership =
                        self.store
                            .get(summary::key(column, Kind::Membership, id), |bytes| {
                                bytes
                                    .map(|bytes| {
                                        let owned = summary::membership_owned_bytes(bytes)?;
                                        zone.lease
                                            .reserve(owned.saturating_mul(2).saturating_add(64))?;
                                        summary::decode_membership(bytes)
                                    })
                                    .transpose()
                                    .map(Option::flatten)
                            })?;
                }
                zone.value.columns.insert(column, bounds);
                zone.finish_column(column, (before, 0, growth));
            }
            Ok(())
        };
        if let Some(columns) = requested {
            for &column in columns {
                read_column(column)?;
            }
        } else {
            for &column in all_columns.iter() {
                read_column(column)?;
            }
        }
        Ok(Some(zone))
    }

    fn access_error(message: &str) -> QueryError {
        error(crate::error::catalog::ER_GET_ERRNO, message)
    }

    fn note_store_failure(&self, err: &QueryError, reading: bool) {
        if self.retired.load(Ordering::Acquire) {
            return;
        }
        // A failed reservation before changing a trusted root is ordinary
        // cache pressure. A revoked root or corrupt read requires repair.
        if !self.store.trusted()
            || (reading && err.code() != crate::error::catalog::ER_OUTOFMEMORY.code())
        {
            self.trusted_base.store(false, Ordering::Release);
            self.invalidation_epoch.fetch_add(1, Ordering::AcqRel);
        }
        self.record_write_error(err);
    }

    fn store_apply(&self, entries: &[(zone_map_page::Key, Vec<u8>)]) -> Result<(), QueryError> {
        self.store
            .apply(entries)
            .inspect_err(|err| self.note_store_failure(err, false))
    }

    fn mutation_key(id: u64) -> zone_map_page::Key {
        zone_map_page::Key {
            column: 0,
            kind: zone_map_page::Kind::ZoneMeta as u8,
            zone: id,
        }
    }

    fn mutation_bytes(mutation: ZoneMutation) -> Vec<u8> {
        let mut bytes = vec![0; 16];
        write_u64(&mut bytes, 0, mutation.epoch);
        write_u64(&mut bytes, 8, mutation.last_ms);
        bytes
    }

    fn zone_mutation(&self, id: u64) -> Result<Option<ZoneMutation>, QueryError> {
        let durable = self
            .store
            .get(Self::mutation_key(id), |bytes| {
                bytes
                    .map(|bytes| {
                        if bytes.len() != 16 {
                            return Err(Self::access_error("corrupt zone mutation record"));
                        }
                        Ok(ZoneMutation {
                            epoch: read_u64(bytes, 0),
                            last_ms: read_u64(bytes, 8),
                        })
                    })
                    .transpose()
            })
            .inspect_err(|err| self.note_store_failure(err, true))?;
        let neutral = self
            .neutral_mutations
            .lock()
            .map_err(|_| Self::access_error("neutral mutation lock poisoned"))?
            .get(id);
        Ok(match (durable, neutral) {
            (Some(a), Some(b)) => Some(ZoneMutation {
                epoch: a.epoch.max(b.epoch),
                last_ms: a.last_ms.max(b.last_ms),
            }),
            (a, b) => a.or(b),
        })
    }

    fn next_zone_mutation_epoch(&self, id: u64, baseline: u64) -> Result<u64, QueryError> {
        let persisted = self.zone_mutation(id)?.map_or(Ok(1), |prior| {
            prior
                .epoch
                .checked_add(1)
                .ok_or_else(|| Self::access_error("zone mutation epoch overflow"))
        })?;
        Ok(baseline.max(persisted))
    }

    #[cfg(test)]
    fn set_zone_mutation(&self, id: u64, mutation: ZoneMutation) -> Result<(), QueryError> {
        if self.retired.load(Ordering::Acquire) {
            return Err(Self::access_error("retired zone map"));
        }
        let mut entries = ChargedZoneEntries::new(0)?;
        entries.push_mutation(id, mutation)?;
        self.store_apply(&entries)
    }

    fn encoded_zone(&self, id: u64, zone: &ZoneSummary) -> Result<ChargedZoneEntries, QueryError> {
        use zone_map_page::{summary, Kind};
        let mut entries = ChargedZoneEntries::new(zone.columns.len())?;
        // Missing membership has the same conservative read semantics as None.
        // Compare canonical bytes without decoding an existing Bloom allocation.
        entries.lease.reserve(8)?;
        let no_membership = summary::encode_membership(None)?;
        entries.push_encoded(summary::key(0, Kind::ZoneMeta, id), 32, || {
            Ok(summary::encode_meta(zone).to_vec())
        })?;
        for (&column, bounds) in &zone.columns {
            let scalar_bytes = bounds
                .min
                .as_ref()
                .map_or(0, zone_scalar_owned_bytes)
                .saturating_add(bounds.max.as_ref().map_or(0, zone_scalar_owned_bytes));
            entries.push_encoded(
                summary::key(column, Kind::Bounds, id),
                64usize.saturating_add(scalar_bytes).saturating_mul(4),
                || summary::encode_bounds(bounds),
            )?;
            let membership_key = summary::key(column, Kind::Membership, id);
            if bounds.membership.is_none()
                && !self.store.get(membership_key, |current| {
                    Ok(current.is_some_and(|bytes| bytes != no_membership.as_slice()))
                })?
            {
                continue;
            }
            // An existing Some (or malformed record) must be explicitly replaced;
            // skipping it would leave an obsolete filter after rebuild or repair.
            let membership_bytes = bounds
                .membership
                .as_ref()
                .map_or(0, ZoneMembership::capacity_bytes);
            entries.push_encoded(
                membership_key,
                256usize.saturating_add(membership_bytes.saturating_mul(4)),
                || summary::encode_membership(bounds.membership.as_ref()),
            )?;
        }
        Ok(entries)
    }

    fn publish_replacement_entries(&self, entries: &ChargedZoneEntries) -> Result<(), QueryError> {
        {
            let mut neutral = self
                .neutral_mutations
                .lock()
                .map_err(|_| Self::access_error("neutral mutation lock poisoned"))?;
            for (key, _) in entries.iter() {
                if key.column == 0
                    && key.kind == zone_map_page::Kind::Meta as u8
                    && !neutral.unknown_proofs.is_empty()
                {
                    let index = key.zone as usize & (neutral.unknown_proofs.len() - 1);
                    if neutral.unknown_proofs[index].zone == key.zone {
                        neutral.unknown_proofs[index].columns = 0;
                    }
                }
            }
        }
        self.store_apply(entries)?;
        let mut neutral = self
            .neutral_mutations
            .lock()
            .map_err(|_| Self::access_error("neutral mutation lock poisoned"))?;
        for (key, _) in entries.iter() {
            if key.column == 0 && key.kind == zone_map_page::Kind::Meta as u8 {
                neutral.set_credits(key.zone, 0, 0);
            }
        }
        Ok(())
    }

    /// A safe rebuild replaces exactly these zones. The caller owns data_gate.
    fn replace_zones(&self, zones: &BTreeMap<u64, ZoneSummary>) -> Result<(), QueryError> {
        if self.retired.load(Ordering::Acquire) {
            return Err(Self::access_error("retired zone map"));
        }
        self.register_unbound_columns(zones);
        let columns = self
            .columns
            .read()
            .map_err(|_| Self::access_error("zone-map schema lock poisoned"))?
            .clone();
        // Column-major keys from multiple zones can share one leaf update.
        // Keep staging bounded and release it before retrying an admission
        // failure, preserving the original one-zone progress path.
        const BATCH_TARGET_BYTES: usize = 4 * 1024 * 1024;
        // Membership encoding dominates publication CPU and needs only shared
        // store reads. Encode a bounded window on scoped workers, then admit
        // the results serially in zone order. Any failed parallel encode
        // discards the rest of its window and resumes the serial path, so the
        // out-of-memory retry below behaves exactly as before.
        const ENCODE_WINDOW: usize = 64;
        const MIN_ZONES_PER_ENCODER: usize = 8;
        let encode = |id: u64, source: &ZoneSummary| {
            if columns
                .iter()
                .all(|column| source.columns.contains_key(column))
            {
                return self.encoded_zone(id, source);
            }
            let mut replacement = ChargedZoneSummary::try_clone_from(source)?;
            for &column in columns.iter() {
                if !replacement.columns.contains_key(&column) {
                    let ticket = replacement.begin_column(column, 0)?;
                    let mut value = ZoneColumnSummary::default();
                    if replacement.state != Some(ZoneMapState::Empty) {
                        value.mark_unknown();
                    }
                    replacement.columns.insert(column, value);
                    replacement.finish_column(column, ticket);
                }
            }
            self.encoded_zone(id, &replacement)
        };
        let zones = zones
            .iter()
            .map(|(id, zone)| (*id, zone))
            .collect::<Vec<_>>();
        let encoders = (crate::planner::default_parallel_degree() as usize)
            .min(ENCODE_WINDOW.min(zones.len()) / MIN_ZONES_PER_ENCODER)
            .max(1);
        let mut pending: Option<ChargedZoneEntries> = None;
        for window in zones.chunks(ENCODE_WINDOW) {
            let mut encoded_window = if encoders > 1 {
                let part = window.len().div_ceil(encoders);
                std::thread::scope(|scope| {
                    let handles = window
                        .chunks(part)
                        .map(|zones| {
                            let encode = &encode;
                            scope.spawn(move || {
                                zones
                                    .iter()
                                    .map(|(id, source)| encode(*id, source))
                                    .collect::<Vec<_>>()
                            })
                        })
                        .collect::<Vec<_>>();
                    handles
                        .into_iter()
                        .map(|handle| {
                            handle
                                .join()
                                .map_err(|_| Self::access_error("zone-map encoder panicked"))
                        })
                        .collect::<Result<Vec<_>, QueryError>>()
                })?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>()
                .into_iter()
            } else {
                Vec::new().into_iter()
            };
            for &(id, source) in window {
                let encoded = match encoded_window.next() {
                    Some(Ok(encoded)) => Ok(encoded),
                    _ => {
                        encoded_window = Vec::new().into_iter();
                        encode(id, source)
                    }
                };
                let encoded = match encoded {
                    Err(err)
                        if err.is(crate::error::catalog::ER_OUTOFMEMORY) && pending.is_some() =>
                    {
                        let prior = pending.take().expect("pending entries were checked");
                        self.publish_replacement_entries(&prior)?;
                        drop(prior);
                        encode(id, source)?
                    }
                    result => result?,
                };
                pending = Some(match pending.take() {
                    None => encoded,
                    Some(mut prior) => {
                        if prior.lease.bytes().saturating_add(encoded.lease.bytes())
                            > BATCH_TARGET_BYTES
                        {
                            self.publish_replacement_entries(&prior)?;
                            encoded
                        } else {
                            match prior.try_append(encoded) {
                                Ok(()) => prior,
                                Err(encoded) => {
                                    self.publish_replacement_entries(&prior)?;
                                    encoded
                                }
                            }
                        }
                    }
                });
            }
        }
        if let Some(entries) = pending {
            self.publish_replacement_entries(&entries)?;
        }
        self.dirty.store(true, Ordering::Release);
        Ok(())
    }

    fn reset_unknown(&self) -> Result<(), QueryError> {
        if self.retired.load(Ordering::Acquire) {
            return Err(Self::access_error("retired zone map"));
        }
        self.invalidation_epoch.fetch_add(1, Ordering::AcqRel);
        self.missing_is_empty.store(false, Ordering::Release);
        self.store
            .reset_unknown()
            .inspect_err(|err| self.note_store_failure(err, false))?;
        let mut neutral = self
            .neutral_mutations
            .lock()
            .map_err(|_| Self::access_error("neutral mutation lock poisoned"))?;
        neutral.clear_credits();
        neutral.unknown_proofs.fill(UnknownColumnProof::default());
        drop(neutral);
        self.trusted_base.store(false, Ordering::Release);
        self.dirty.store(true, Ordering::Release);
        Ok(())
    }

    fn merge(&self, batch: &ZoneMapDeltaBatch) -> Result<(), QueryError> {
        if batch.storage_id != self.storage_id
            || batch.zone_pages != self.zone_pages
            || batch.schema_fingerprint != self.schema_fingerprint
        {
            return Err(Self::access_error(
                "zone-map merge identity or schema mismatch",
            ));
        }
        if batch.zones.is_empty() {
            return Ok(());
        }
        if batch.zones.len() == 1 {
            let (&zone, summary) = batch.zones.iter().next().expect("one zone");
            return self.merge_single_zone(batch, zone, summary);
        }
        let mut latch_scratch = ZoneScratchLease::default();
        let _zones = self.lock_merge_zones(batch.zones.keys().copied(), &mut latch_scratch)?;
        self.merge_latched(batch)
    }

    /// Concurrent widenings of one zone stripe are combined. A caller that
    /// finds the stripe latch busy queues a copy of its delta and waits for
    /// the latch; whoever holds it publishes the union of every queued delta
    /// in one store update. Each caller still returns only after a latch
    /// holder has published its delta, so DML observes its widening before
    /// commit exactly as before, but the hot tail zone of an append-heavy
    /// table no longer pays one page publication and WAL append per row while
    /// every other inserter waits on the same latch.
    fn merge_single_zone(
        &self,
        batch: &ZoneMapDeltaBatch,
        zone: u64,
        summary: &ZoneSummary,
    ) -> Result<(), QueryError> {
        let stripe = zone as usize % ZONE_MAP_MERGE_LATCHES;
        let latch = &self.merge_latches[stripe];
        let (_latch, queued_self) = match latch.try_lock() {
            Ok(guard) => (guard, false),
            Err(std::sync::TryLockError::WouldBlock) => {
                let queued = Arc::new(QueuedZoneMerge {
                    zone,
                    summary: summary.clone(),
                    result: Mutex::new(None),
                });
                self.lock_merge_queue(stripe)?.push(Arc::clone(&queued));
                let guard = latch
                    .lock()
                    .map_err(|_| Self::access_error("zone-map merge lock poisoned"))?;
                // Only latch holders drain the queue, and they complete every
                // drained entry before releasing the latch.
                if let Some(result) = queued.take_result() {
                    return result;
                }
                (guard, true)
            }
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(Self::access_error("zone-map merge lock poisoned"));
            }
        };
        let queued = std::mem::take(&mut *self.lock_merge_queue(stripe)?);
        if queued.is_empty() {
            return self.merge_latched(batch);
        }
        let mut combined = ZoneMapDeltaBatch {
            storage_id: batch.storage_id,
            zone_pages: batch.zone_pages,
            schema_fingerprint: batch.schema_fingerprint,
            zones: BTreeMap::new(),
        };
        if !queued_self {
            combined.zones.entry(zone).or_default().absorb(summary);
        }
        for entry in &queued {
            combined
                .zones
                .entry(entry.zone)
                .or_default()
                .absorb(&entry.summary);
        }
        let result = self.merge_latched(&combined);
        for entry in &queued {
            entry.complete(result.clone());
        }
        result
    }

    fn lock_merge_queue(
        &self,
        stripe: usize,
    ) -> Result<std::sync::MutexGuard<'_, Vec<Arc<QueuedZoneMerge>>>, QueryError> {
        self.merge_queues[stripe]
            .lock()
            .map_err(|_| Self::access_error("zone-map merge queue poisoned"))
    }

    /// Caller holds the merge latches of every zone in `batch`.
    fn merge_latched(&self, batch: &ZoneMapDeltaBatch) -> Result<(), QueryError> {
        let result = loop {
            let gate = self
                .data_gate
                .read()
                .map_err(|_| Self::access_error("zone-map data lock poisoned"))?;
            if self.retired.load(Ordering::Acquire) {
                return Err(Self::access_error("retired zone map"));
            }
            if self.store.trusted() {
                // Every intermediate leaf is a conservative widening. The
                // store serializes physical publication, and our zone latches
                // prevent a second merger from overwriting this zone's union.
                break self.merge_locked(batch);
            }
            drop(gate);
            let _exclusive = self
                .data_gate
                .write()
                .map_err(|_| Self::access_error("zone-map data lock poisoned"))?;
            if !self.store.trusted() {
                self.reset_unknown()?;
            }
        };
        if let Err(err) = result {
            // A reset revokes all zones, so wait for other widening batches
            // and candidate readers before replacing their shared root.
            let _exclusive = self
                .data_gate
                .write()
                .map_err(|_| Self::access_error("zone-map data lock poisoned"))?;
            // Optional metadata must not strand ordinary DML after a corrupt
            // leaf or failed admission. First make an empty/Unknown baseline
            // visible and recoverable; if that cannot be logged, fail DML.
            self.note_store_failure(&err, false);
            self.reset_unknown()?;
            self.record_write_error(&err);
            return Ok(());
        }
        Ok(())
    }

    fn lock_merge_zones<'a>(
        &'a self,
        zones: impl Iterator<Item = u64>,
        scratch: &mut ZoneScratchLease,
    ) -> Result<smallvec::SmallVec<[std::sync::MutexGuard<'a, ()>; 8]>, QueryError> {
        let mask = zones.fold(0u64, |mask, id| {
            mask | (1u64 << (id % ZONE_MAP_MERGE_LATCHES as u64))
        });
        let count = mask.count_ones() as usize;
        if count > 8 {
            scratch.reserve(
                count.next_power_of_two() * std::mem::size_of::<std::sync::MutexGuard<'_, ()>>(),
            )?;
        }
        let mut guards = smallvec::SmallVec::new();
        // Stripe order, rather than zone order, closes cycles for batches
        // containing different zone ids that collide on the same stripes.
        let mut remaining = mask;
        while remaining != 0 {
            let index = remaining.trailing_zeros() as usize;
            remaining &= remaining - 1;
            guards.push(
                self.merge_latches[index]
                    .lock()
                    .map_err(|_| Self::access_error("zone-map merge lock poisoned"))?,
            );
        }
        Ok(guards)
    }

    fn merge_locked(&self, batch: &ZoneMapDeltaBatch) -> Result<(), QueryError> {
        self.register_unbound_columns(&batch.zones);
        let epoch = self.mutation_epoch.fetch_add(1, Ordering::AcqRel) + 1;
        let now = crate::util::coarse_clock::coarse_unix_ms();
        for (&id, incoming) in &batch.zones {
            if self.retired.load(Ordering::Acquire) {
                return Err(Self::access_error("retired zone map"));
            }
            let prior = self.store.get(
                zone_map_page::summary::key(0, zone_map_page::Kind::ZoneMeta, id),
                |bytes| bytes.map(zone_map_page::summary::decode_meta).transpose(),
            )?;
            let mut entries = ChargedZoneEntries::new(incoming.columns.len())?;
            let is_missing = prior.is_none();
            let mut target = prior.unwrap_or_default();
            self.apply_runtime_counts(id, &mut target)?;
            if is_missing && !self.missing_is_empty.load(Ordering::Acquire) {
                target.state = Some(ZoneMapState::Unknown);
            }
            let mut summary_changed = is_missing
                || incoming.rebuild_scn > target.rebuild_scn
                || (incoming.state == Some(ZoneMapState::Unknown)
                    && target.state != Some(ZoneMapState::Unknown));
            target.row_count_estimate = target
                .row_count_estimate
                .saturating_add(incoming.row_count_estimate);
            target.stale_mutations = target
                .stale_mutations
                .saturating_add(incoming.stale_mutations);
            target.rebuild_scn = target.rebuild_scn.max(incoming.rebuild_scn);
            if incoming.state == Some(ZoneMapState::Unknown) {
                target.state = Some(ZoneMapState::Unknown);
            }
            for (&column, source) in &incoming.columns {
                let scalar_bytes = source
                    .min
                    .as_ref()
                    .map_or(0, zone_scalar_owned_bytes)
                    .saturating_add(source.max.as_ref().map_or(0, zone_scalar_owned_bytes));
                let peak = scalar_bytes.saturating_mul(4).saturating_add(64);
                // Only this column's old bounds are needed. Decode and merge
                // them on the stack instead of materializing a temporary zone
                // B-tree and a second tree of requested column ids per batch.
                let mut bounds_scratch = ZoneScratchLease::default();
                bounds_scratch.reserve(peak)?;
                let prior = self.store.get(
                    zone_map_page::summary::key(column, zone_map_page::Kind::Bounds, id),
                    |bytes| {
                        bytes
                            .map(|bytes| {
                                bounds_scratch.reserve(
                                    peak.saturating_add(bytes.len().saturating_mul(8))
                                        .saturating_add(512),
                                )?;
                                zone_map_page::summary::decode_bounds(bytes)
                            })
                            .transpose()
                    },
                )?;
                let column_missing = prior.is_none();
                let mut destination = prior.unwrap_or_else(|| {
                    let mut value = ZoneColumnSummary::default();
                    // Existing zone + missing column has an incomplete base.
                    if !is_missing || !self.missing_is_empty.load(Ordering::Acquire) {
                        value.mark_unknown();
                    }
                    value
                });
                let mut membership_base = ZoneColumnSummary {
                    state: destination.state,
                    has_non_null: destination.has_non_null,
                    ..Default::default()
                };
                // Membership is merged directly from its paged record below.
                // Borrow scalar endpoints; unchanged string/decimal bounds
                // should not allocate copies just to compare them again.
                let bounds_changed = destination.merge_bounds_changed(source) || column_missing;
                if bounds_changed {
                    let owned = zone_column_owned_bytes(&destination);
                    entries.push_encoded(
                        zone_map_page::summary::key(column, zone_map_page::Kind::Bounds, id),
                        64usize.saturating_add(owned).saturating_mul(4),
                        || zone_map_page::summary::encode_bounds(&destination),
                    )?;
                    summary_changed = true;
                }
                if destination.state == ZoneMapState::Unknown {
                    membership_base.state = ZoneMapState::Unknown;
                }
                let membership_key =
                    zone_map_page::summary::key(column, zone_map_page::Kind::Membership, id);
                let before = entries.lease.bytes();
                entries
                    .lease
                    .reserve(256 + ZONE_MEMBERSHIP_MAX_BLOOM_WORDS * 8 * 6)?;
                let merged = self.store.get(membership_key, |bytes| {
                    zone_map_page::summary::merge_membership_record_foreground(
                        bytes,
                        &membership_base,
                        source,
                    )
                })?;
                if let Some(bytes) = merged {
                    let allocated = bytes.capacity();
                    entries.entries.push((membership_key, bytes));
                    entries.lease.shrink_to(before + allocated);
                    summary_changed = true;
                } else {
                    entries.lease.shrink_to(before);
                }
            }
            // A persisted zone frontier may outlive this handle. Never
            // reuse that frontier after restart or cache eviction.
            let epoch = self.next_zone_mutation_epoch(id, epoch)?;
            self.mutation_epoch.fetch_max(epoch, Ordering::AcqRel);
            let mutation = ZoneMutation {
                epoch,
                last_ms: now,
            };
            let neutral_update = !summary_changed
                && (incoming.stale_mutations != 0 || incoming.row_count_estimate != 0);
            let (row_credit, stale_credit) = self
                .neutral_mutations
                .lock()
                .map_err(|_| Self::access_error("neutral mutation lock poisoned"))?
                .credits(id);
            if neutral_update
                && row_credit >= incoming.row_count_estimate
                && stale_credit >= incoming.stale_mutations
            {
                let mut neutral = self
                    .neutral_mutations
                    .lock()
                    .map_err(|_| Self::access_error("neutral mutation lock poisoned"))?;
                neutral.note(id, mutation);
                neutral.set_credits(
                    id,
                    row_credit - incoming.row_count_estimate,
                    stale_credit - incoming.stale_mutations,
                );
                continue;
            }
            // Pre-account bounded count increases for proof-neutral mutations.
            // Runtime credits recover the exact counter. Losing a handle or
            // slot only raises the maintenance estimate; recovery never loses
            // mutations or underestimates rows, and pruning evidence is unchanged.
            // Preserve the other counter's credits when only one is recharged;
            // alternating inserts and deletes must not cancel each other's batch.
            let next_credit = if neutral_update {
                (if incoming.stale_mutations != 0 {
                    63
                } else {
                    stale_credit
                })
                .min(u64::MAX - target.stale_mutations)
            } else {
                0
            };
            target.stale_mutations += next_credit;
            let next_row_credit = if neutral_update {
                (if incoming.row_count_estimate != 0 {
                    63
                } else {
                    row_credit
                })
                .min(u64::MAX - target.row_count_estimate)
            } else {
                0
            };
            target.row_count_estimate += next_row_credit;
            entries.push_encoded(
                zone_map_page::summary::key(0, zone_map_page::Kind::ZoneMeta, id),
                32,
                || Ok(zone_map_page::summary::encode_meta(&target).to_vec()),
            )?;
            entries.push_mutation(id, mutation)?;
            // DML unions never make pruning narrower. In-place leaf updates
            // may be replayed independently; a commit follows all of them in
            // WAL order. Shrinking rebuilds still use ordinary store_apply.
            self.store
                .apply_widening(&entries)
                .inspect_err(|err| self.note_store_failure(err, false))?;
            let mut neutral = self
                .neutral_mutations
                .lock()
                .map_err(|_| Self::access_error("neutral mutation lock poisoned"))?;
            if next_credit != 0 || next_row_credit != 0 {
                neutral.note(id, mutation);
            }
            neutral.set_credits(id, next_row_credit, next_credit);
        }
        self.last_mutation_ms.store(now, Ordering::Release);
        self.dirty.store(true, Ordering::Release);
        Ok(())
    }

    fn unknown_column_mask(columns: &[u32]) -> Option<u64> {
        columns
            .iter()
            .try_fold(0, |mask, &column| Some(mask | 1u64.checked_shl(column)?))
            .filter(|mask| *mask != 0)
    }

    /// Caller holds data_gate. Only durable Unknown observations are cached;
    /// a hit permits skipping redundant widening, not pruning any heap data.
    fn cached_unknown_columns(&self, id: u64, columns: &[u32]) -> bool {
        let Some(mask) = Self::unknown_column_mask(columns) else {
            return false;
        };
        if self.retired.load(Ordering::Acquire) {
            return false;
        }
        let hit = self.neutral_mutations.lock().ok().is_some_and(|neutral| {
            let proofs = &neutral.unknown_proofs;
            !proofs.is_empty() && {
                let proof = proofs[id as usize & (proofs.len() - 1)];
                proof.zone == id && proof.columns & mask == mask
            }
        });
        hit && self.store.trusted()
    }

    fn remember_unknown_columns(&self, id: u64, columns: &[u32]) {
        let Some(mask) = Self::unknown_column_mask(columns) else {
            return;
        };
        let Ok(mut neutral) = self.neutral_mutations.lock() else {
            return;
        };
        if neutral.unknown_proofs.is_empty() {
            let records = self
                .store
                .root_snapshot()
                .map_or(64, |root| root.records.min(4096) as usize);
            let capacity = records.max(64).next_power_of_two().min(4096);
            let bytes = capacity * std::mem::size_of::<UnknownColumnProof>();
            use crate::engine::executor::mem_budget::{
                ControlledMemoryCategory, ControlledMemoryCharge,
            };
            let Some(charge) = ControlledMemoryCharge::try_new_optional(
                ControlledMemoryCategory::DerivedCache,
                bytes,
            ) else {
                return;
            };
            neutral.unknown_proofs = vec![UnknownColumnProof::default(); capacity];
            neutral.unknown_proof_charge = Some(charge);
        }
        let index = id as usize & (neutral.unknown_proofs.len() - 1);
        let proof = &mut neutral.unknown_proofs[index];
        if proof.zone != id {
            *proof = UnknownColumnProof {
                zone: id,
                columns: 0,
            };
        }
        proof.columns |= mask;
    }

    /// Inspect update provenance without materializing column maps or Bloom
    /// bitmaps. Callers hold data_gate across this read and their decision.
    fn apply_runtime_counts(&self, id: u64, value: &mut ZoneSummary) -> Result<(), QueryError> {
        let neutral = self
            .neutral_mutations
            .lock()
            .map_err(|_| Self::access_error("neutral mutation lock poisoned"))?;
        let (rows, stale) = neutral.credits(id);
        value.row_count_estimate = value.row_count_estimate.saturating_sub(rows);
        value.stale_mutations = value.stale_mutations.saturating_sub(stale);
        Ok(())
    }

    fn zone_update_status(
        &self,
        id: u64,
        columns: &[u32],
    ) -> Result<Option<(u64, u64, bool)>, QueryError> {
        use zone_map_page::{summary, Kind};
        if self.retired.load(Ordering::Acquire) {
            return Err(Self::access_error("retired zone map"));
        }
        let mut scratch = ZoneScratchLease::default();
        scratch.reserve(PAGE_SIZE * 2)?;
        let result = (|| {
            let Some(mut meta) = self
                .store
                .get(summary::key(0, Kind::ZoneMeta, id), |bytes| {
                    bytes.map(summary::decode_meta).transpose()
                })?
            else {
                return Ok(None);
            };
            self.apply_runtime_counts(id, &mut meta)?;
            let mut all_unknown = true;
            for &column in columns {
                let unknown = self
                    .store
                    .get(summary::key(column, Kind::Bounds, id), |bytes| {
                        bytes.map(summary::decode_bounds).transpose().map(|value| {
                            value.is_some_and(|value| value.state == ZoneMapState::Unknown)
                        })
                    })?;
                if !unknown {
                    all_unknown = false;
                    break;
                }
            }
            Ok(Some((
                meta.row_count_estimate,
                meta.stale_mutations,
                all_unknown,
            )))
        })();
        result.inspect_err(|err| self.note_store_failure(err, true))
    }

    #[cfg(test)]
    fn note_summary_neutral_mutations(&self, zone_ids: &[u64], column_ids: &[u32]) -> bool {
        if zone_ids.is_empty() {
            return true;
        }
        let Ok(_gate) = self.data_gate.write() else {
            return false;
        };
        for &id in zone_ids {
            if self.cached_unknown_columns(id, column_ids) {
                continue;
            }
            if !matches!(
                self.zone_update_status(id, column_ids),
                Ok(Some((_, _, true)))
            ) {
                return false;
            }
        }
        if self
            .note_summary_neutral_mutations_locked(zone_ids)
            .is_err()
        {
            return false;
        }
        for &id in zone_ids {
            self.remember_unknown_columns(id, column_ids);
        }
        true
    }

    /// Caller excludes rebuilds through data_gate and serializes each zone's
    /// writers through its merge latch (or the exclusive data gate). Publish
    /// the mutation fence before releasing either guard.
    fn note_summary_neutral_mutations_locked(&self, zone_ids: &[u64]) -> Result<(), QueryError> {
        // The Unknown proof was already redo-logged before the DML commit
        // prefix. Persisting identical proof/epochs for every following row
        // only rewrites whole pages. No live builder survives a retired handle
        // or restart, so a runtime fence is enough for these neutral changes.
        let epoch = self.mutation_epoch.fetch_add(1, Ordering::AcqRel) + 1;
        let now = crate::util::coarse_clock::coarse_unix_ms();
        for &id in zone_ids {
            let epoch = self.next_zone_mutation_epoch(id, epoch)?;
            self.mutation_epoch.fetch_max(epoch, Ordering::AcqRel);
            let mut neutral = self
                .neutral_mutations
                .lock()
                .map_err(|_| Self::access_error("neutral mutation lock poisoned"))?;
            neutral.note(
                id,
                ZoneMutation {
                    epoch,
                    last_ms: now,
                },
            );
        }
        self.last_mutation_ms.store(now, Ordering::Release);
        Ok(())
    }

    fn estimated_base_memory_bytes(&self) -> usize {
        self.memory_recounts.fetch_add(1, Ordering::Relaxed);
        let mut bytes = std::mem::size_of::<Self>()
            + std::mem::size_of::<zone_map_page::mutable::Store>()
            + std::mem::size_of::<BufferPageFileKey>()
            + 4 * std::mem::size_of::<usize>();
        bytes += self
            .columns
            .read()
            .map_or(0, |columns| columns.len() * std::mem::size_of::<u32>());
        bytes += self.membership_columns.read().map_or(0, |columns| {
            columns.len() * std::mem::size_of::<(u32, ZoneMembershipLayout)>()
        });
        bytes += self
            .path
            .lock()
            .ok()
            .and_then(|path| path.as_ref().map(|path| path.as_os_str().len()))
            .unwrap_or(0);
        for slot in [&self.last_error, &self.rebuild_error] {
            bytes += slot
                .lock()
                .ok()
                .and_then(|value| value.as_ref().map(String::capacity))
                .unwrap_or(0);
        }
        bytes
    }

    fn estimated_memory_bytes(&self) -> usize {
        self.estimated_base_memory_bytes()
            + self
                .neutral_mutations
                .lock()
                .map_or(0, |neutral| neutral.memory_bytes())
    }

    fn charged_memory_bytes(&self) -> usize {
        let base = self
            .derived_charge
            .lock()
            .ok()
            .and_then(|slot| slot.as_ref().map(|charge| charge.bytes()))
            .unwrap_or(0);
        base + self.neutral_mutations.lock().map_or(0, |neutral| {
            [&neutral.unknown_proof_charge, &neutral.zone_charge]
                .into_iter()
                .flatten()
                .map(|charge| charge.bytes())
                .sum::<usize>()
        })
    }

    fn sync_derived_charge_to(&self, desired: usize) -> bool {
        use crate::engine::executor::mem_budget::{
            ControlledMemoryCategory, ControlledMemoryCharge,
        };
        let Ok(mut slot) = self.derived_charge.lock() else {
            self.cache_admitted.store(false, Ordering::Release);
            return false;
        };
        let admitted = match slot.as_mut() {
            Some(charge) if charge.bytes() < desired => {
                charge.try_grow_optional(desired - charge.bytes())
            }
            Some(charge) => {
                charge.shrink(charge.bytes().saturating_sub(desired));
                true
            }
            None => match ControlledMemoryCharge::try_new_optional(
                ControlledMemoryCategory::DerivedCache,
                desired,
            ) {
                Some(charge) => {
                    *slot = Some(charge);
                    true
                }
                None => false,
            },
        };
        if !admitted {
            ZONE_MAP_MEMORY_FALLBACKS.fetch_add(1, Ordering::Relaxed);
        }
        self.cache_admitted.store(admitted, Ordering::Release);
        admitted
    }

    fn sync_derived_charge(&self) -> bool {
        // The optional hint allocation owns a separate charge, so concurrent
        // base recounts cannot shrink away memory reserved by a new cache.
        self.sync_derived_charge_to(self.estimated_base_memory_bytes())
    }

    fn record_write_error(&self, err: &QueryError) {
        if let Ok(mut slot) = self.last_error.lock() {
            *slot = Some(err.message().to_string());
        }
        self.sync_derived_charge();
    }

    #[cfg(test)]
    fn write_to_file(&self, lsn: u64) -> Result<(), QueryError> {
        self.store.flush()?;
        self.last_persisted_lsn.store(lsn, Ordering::Release);
        self.dirty.store(false, Ordering::Release);
        if let Ok(mut slot) = self.last_error.lock() {
            *slot = None;
        }
        Ok(())
    }

    fn storage_snapshot(&self) -> zone_map_page::mutable::StorageSnapshot {
        let mut snapshot = self.store.storage_snapshot();
        snapshot.charged_bytes = snapshot
            .charged_bytes
            .saturating_add(self.charged_memory_bytes() as u64);
        snapshot
    }

    #[cfg(test)]
    fn snapshot_for_tests(&self) -> BTreeMap<u64, ZoneSummary> {
        use zone_map_page::{Key, Kind};
        let mut ids = Vec::new();
        let mut columns = BTreeSet::new();
        self.store
            .visit_range(
                Key {
                    column: 0,
                    kind: 0,
                    zone: 0,
                },
                Key {
                    column: u32::MAX,
                    kind: u8::MAX,
                    zone: u64::MAX,
                },
                |key, _| {
                    if key.column == 0 && key.kind == Kind::Meta as u8 {
                        ids.push(key.zone);
                    }
                    if key.kind == Kind::Bounds as u8 {
                        columns.insert(key.column);
                    }
                    Ok(true)
                },
            )
            .expect("test zone records");
        ids.into_iter()
            .map(|id| {
                (
                    id,
                    self.read_zone(id, Some(&columns))
                        .expect("test zone read")
                        .expect("test zone metadata exists")
                        .value,
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod paged_zone_map_handle_tests {
    use super::*;

    #[test]
    fn batched_zone_replacement_matches_single_zone_updates() {
        let (_left_dir, left) = map();
        let (_right_dir, right) = map();
        let ids = (0..80u64).chain([u64::from(u32::MAX) + 1, u64::MAX]);
        let mut zones = BTreeMap::new();
        for id in ids {
            let mut zone = batch(id, id as i64).zones.remove(&id).unwrap();
            let mut membership = ZoneMembership::empty_with_layout(ZoneMembershipLayout::default());
            membership.insert_hash(zone_membership_hash(&ZoneScalar::Signed(id as i64)));
            zone.columns.get_mut(&1).unwrap().membership = Some(membership);
            zones.insert(id, zone);
        }
        for round in 0..2 {
            if round == 1 {
                for zone in zones.values_mut() {
                    zone.columns.get_mut(&1).unwrap().membership = None;
                }
            }
            {
                let _gate = left.data_gate.write().unwrap();
                left.replace_zones(&zones).unwrap();
            }
            {
                let _gate = right.data_gate.write().unwrap();
                for (&id, zone) in &zones {
                    right
                        .replace_zones(&BTreeMap::from([(id, zone.clone())]))
                        .unwrap();
                }
            }
            assert_eq!(
                serde_json::to_value(left.snapshot_for_tests()).unwrap(),
                serde_json::to_value(right.snapshot_for_tests()).unwrap(),
            );
        }
    }

    fn map() -> (tempfile::TempDir, ZoneMap) {
        fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool = BufferPool::new(
            16,
            Arc::clone(&log),
            DurabilityMode::Memory,
            Duration::from_secs(5),
        );
        let file = zone_map_page::PageFile::new(
            dir.path().join("map.zm"),
            17,
            1,
            pool,
            log,
            DurabilityMode::Memory,
        )
        .unwrap();
        let store = Arc::new(zone_map_page::mutable::Store::create(file, 123, 8).unwrap());
        store.set_missing_is_empty(true).unwrap();
        (dir, ZoneMap::new(17, 8, 123, store, true))
    }

    fn batch(zone: u64, value: i64) -> ZoneMapDeltaBatch {
        let mut column = ZoneColumnSummary::default();
        column.merge_value(ZoneScalar::Signed(value));
        ZoneMapDeltaBatch {
            storage_id: 17,
            zone_pages: 8,
            schema_fingerprint: 123,
            zones: BTreeMap::from([(
                zone,
                ZoneSummary {
                    columns: BTreeMap::from([(1, column)]),
                    row_count_estimate: 1,
                    ..ZoneSummary::default()
                },
            )]),
        }
    }

    #[test]
    fn zone_map_paged_reset_does_not_promote_partial_dml_to_ready() {
        let (_dir, map) = map();
        map.merge(&batch(0, 10)).unwrap();
        assert_eq!(
            map.read_zone(0, None).unwrap().unwrap().columns[&1].state,
            ZoneMapState::Ready
        );
        {
            let _gate = map.data_gate.write().unwrap();
            map.reset_unknown().unwrap();
        }
        map.merge(&batch(0, 99)).unwrap();
        let partial = map.read_zone(0, None).unwrap().unwrap();
        assert_eq!(partial.state, Some(ZoneMapState::Unknown));
        assert_eq!(partial.columns[&1].state, ZoneMapState::Unknown);
        {
            let _gate = map.data_gate.write().unwrap();
            map.replace_zones(&batch(0, 10).zones).unwrap();
        }
        let repaired = map.read_zone(0, None).unwrap().unwrap();
        assert_ne!(repaired.state, Some(ZoneMapState::Unknown));
        assert_eq!(repaired.columns[&1].min, Some(ZoneScalar::Signed(10)));
    }

    #[test]
    fn zone_map_paged_handle_memory_does_not_scale_with_zone_count() {
        let (_dir, map) = map();
        map.merge(&batch(0, 10)).unwrap();
        let memory = map.estimated_memory_bytes();
        for zone in 1..65 {
            map.merge(&batch(zone, zone as i64)).unwrap();
        }
        assert_eq!(map.estimated_memory_bytes(), memory);
        assert_eq!(map.snapshot_for_tests().len(), 65);
    }

    #[test]
    fn zone_map_paged_mutation_frontier_cannot_repeat_after_handle_recreation() {
        let (_dir, map) = map();
        map.merge(&batch(0, 10)).unwrap();
        let before = map.zone_mutation(0).unwrap().unwrap().epoch;
        let reopened = ZoneMap::new(17, 8, 123, Arc::clone(&map.store), true);
        reopened.mutation_epoch.store(0, Ordering::Release);
        reopened.merge(&batch(0, 20)).unwrap();
        assert!(reopened.zone_mutation(0).unwrap().unwrap().epoch > before);
    }
}

/// Return a `&str` slice of the input text that (a) strips trailing spaces
/// only for a PAD SPACE collation and (b) is truncated to at most
/// `ZONE_STRING_PREFIX_CHARS` code
/// points. Allocation-free — text_scalar / the WorkerScalar fast path use
/// this in place of `chars().take(N).collect::<String>()`.
fn zone_text_prefix_slice(text: &str, collation_id: u16) -> &str {
    let normalized =
        if crate::charset::resolve_collation_id(collation_id).is_some_and(|info| info.pad_space) {
            crate::util::pad_space::pad_space_trim_end_str(text)
        } else {
            text
        };
    zone_text_prefix_slice_normalized(normalized)
}

#[inline]
fn zone_text_prefix_slice_normalized(normalized: &str) -> &str {
    // Byte length bounds code-point length, including non-ASCII short text.
    if normalized.len() <= ZONE_STRING_PREFIX_CHARS {
        return normalized;
    }
    let ascii_end = ZONE_STRING_PREFIX_CHARS;
    if normalized.as_bytes()[..ascii_end].is_ascii() {
        return &normalized[..ascii_end];
    }
    let mut end = normalized.len();
    let mut count = 0;
    for (idx, _) in normalized.char_indices() {
        if count == ZONE_STRING_PREFIX_CHARS {
            end = idx;
            break;
        }
        count += 1;
    }
    &normalized[..end]
}

/// The scanner owns this scratch lease separately from any one column, so
/// a completed zone cannot release the capacity that the next zone reuses.
fn write_zone_text_weight(
    collation: &'static crate::charset::CollationInfo,
    prefix: &str,
    scratch: &mut Vec<u8>,
    lease: &mut ZoneScratchLease,
) -> bool {
    let bound = if prefix.is_ascii() {
        prefix.len()
    } else {
        PAGE_SIZE
    };
    if lease.bytes() < scratch.capacity()
        && lease.reserve(scratch.capacity() - lease.bytes()).is_err()
    {
        return false;
    }
    if bound > scratch.capacity() {
        // Include allocator growth's old/new overlap, but only for this one
        // prefix. Large input strings are never copied into the sketch.
        if lease.reserve(bound.saturating_mul(2).max(16)).is_err() {
            return false;
        }
    }
    crate::charset::collation_key_for_info_into(collation, prefix, scratch);
    if scratch.capacity() > lease.bytes() {
        *scratch = Vec::new();
        lease.shrink_to(0);
        let _ = ZoneScratchLease::denied();
        return false;
    }
    if scratch.capacity() < lease.bytes() {
        lease.shrink_to(scratch.capacity());
    }
    true
}

// Cache hits need the raw prefix and its two membership hashes. Endpoint
// weights already live in min/max; retaining another weight Vec wastes space.
#[derive(Debug, Clone, Copy)]
struct CachedZoneTextPrefix {
    raw: [u8; 32],
    len: u8,
    observed_truncations: u8,
    hash: u64,
    truncated_hash: u64,
}

/// Per-column running state shared between the LOAD path aggregator and
/// the ANALYZE rebuild aggregator. Structurally similar to
/// `ZoneMapWorkerColumn` but without the column_idx/column_id book-keeping
/// (the caller already knows which slot maps to which column) so it can
/// be embedded in a Vec parallel to `relevant_columns`.

#[derive(Debug)]
struct WorkerColumnState {
    state: ZoneMapState,
    has_null: bool,
    has_non_null: bool,
    truncated: bool,
    min: Option<WorkerScalar>,
    max: Option<WorkerScalar>,
    membership: Option<ZoneMembership>,
    // Idempotent membership updates repeat heavily for low-NDV values. This
    // fixed per-zone cache avoids the exact-set search / Bloom probes without
    // changing which hashes are represented in the persisted summary.
    recent_membership_hashes: [u64; 64],
    recent_membership_valid: u64,
    // Zero caches membership hashes; nonzero domains cache fully observed
    // scalar bits (including their type and decimal scale). Reuse the same
    // bounded storage so duplicate numerics can bypass hashing as well.
    recent_membership_domain: u16,
    // Per-zone dictionary for short textual values. Fact-table dimensions
    // such as status/mode/instruction repeat heavily; recomputing their full
    // Unicode collation weight millions of times dominated fused ANALYZE.
    // The cache is strictly bounded and keyed by exact raw bytes, so it is an
    // optimization only and cannot alter collation semantics.
    text_weight_cache: smallvec::SmallVec<[CachedZoneTextPrefix; 2]>,
    text_weight_cache_saturated: bool,
    text_weight_cache_hint: [u8; 32],
    temporary_bytes: usize,
    lease: ZoneScratchLease,
}

impl Default for WorkerColumnState {
    fn default() -> Self {
        Self {
            state: ZoneMapState::Empty,
            has_null: false,
            has_non_null: false,
            truncated: false,
            min: None,
            max: None,
            membership: None,
            recent_membership_hashes: [0; 64],
            recent_membership_valid: 0,
            recent_membership_domain: 0,
            text_weight_cache: smallvec::SmallVec::new(),
            text_weight_cache_saturated: false,
            text_weight_cache_hint: [0; 32],
            temporary_bytes: 0,
            lease: ZoneScratchLease::default(),
        }
    }
}

impl WorkerColumnState {
    fn owned_bytes(&self) -> usize {
        self.min
            .as_ref()
            .map_or(0, WorkerScalar::owned_bytes)
            .saturating_add(self.max.as_ref().map_or(0, WorkerScalar::owned_bytes))
            .saturating_add(
                self.membership
                    .as_ref()
                    .map_or(0, ZoneMembership::capacity_bytes),
            )
            .saturating_add(if self.text_weight_cache.spilled() {
                self.text_weight_cache.capacity() * std::mem::size_of::<CachedZoneTextPrefix>()
            } else {
                0
            })
    }
    fn begin_temporary(&mut self, bytes: usize) -> bool {
        if self.lease.reserve(bytes).is_err() {
            self.mark_unknown();
            return false;
        }
        self.temporary_bytes = self.temporary_bytes.saturating_add(bytes);
        true
    }
    fn finish_temporary(&mut self, previous: usize) {
        self.temporary_bytes = previous;
        let desired = self.owned_bytes().saturating_add(previous);
        if desired > self.lease.bytes() {
            let _ = ZoneScratchLease::denied();
            self.mark_unknown();
        } else if desired < self.lease.bytes() {
            self.lease.shrink_to(desired);
        }
    }

    #[inline]
    fn enable_membership_with_layout(&mut self, layout: ZoneMembershipLayout) {
        if self.membership.is_none() {
            // Values observed before membership was enabled must be inserted.
            self.recent_membership_valid = 0;
        }
        self.membership
            .get_or_insert_with(|| ZoneMembership::empty_with_layout(layout));
    }

    #[inline]
    fn mark_unknown(&mut self) {
        self.state = ZoneMapState::Unknown;
        self.min = None;
        self.max = None;
        self.membership = None;
        self.text_weight_cache = smallvec::SmallVec::new();
        self.text_weight_cache_saturated = true;
        self.recent_membership_valid = 0;
        self.lease.shrink_to(self.temporary_bytes);
    }

    #[inline]
    fn insert_membership_hash(&mut self, hash: u64) -> bool {
        if self.recent_membership_domain != 0 {
            self.recent_membership_domain = 0;
            self.recent_membership_valid = 0;
        }
        let slot = hash as usize & (self.recent_membership_hashes.len() - 1);
        let valid_bit = 1u64 << slot;
        if self.recent_membership_valid & valid_bit != 0
            && self.recent_membership_hashes[slot] == hash
        {
            return true;
        }
        if !self.insert_membership_hash_uncached(hash) {
            return false;
        }
        self.recent_membership_hashes[slot] = hash;
        self.recent_membership_valid |= valid_bit;
        true
    }

    #[inline]
    fn insert_membership_hash_uncached(&mut self, hash: u64) -> bool {
        // Only search for admission when insertion could allocate. With spare
        // capacity, insert_hash already performs the required duplicate check.
        let peak = match &self.membership {
            Some(ZoneMembership::Exact {
                hashes,
                bloom_words,
                ..
            }) if (hashes.len() >= ZONE_MEMBERSHIP_EXACT_LIMIT
                || hashes.len() == hashes.capacity())
                && hashes.binary_search(&hash).is_err() =>
            {
                if hashes.len() >= ZONE_MEMBERSHIP_EXACT_LIMIT {
                    usize::from(*bloom_words) * 8
                } else if hashes.len() == hashes.capacity() {
                    hashes.capacity().saturating_mul(2).max(4) * 8
                } else {
                    0
                }
            }
            _ => 0,
        };
        let previous = self.temporary_bytes;
        let capacity_before = self
            .membership
            .as_ref()
            .map_or(0, ZoneMembership::capacity_bytes);
        if peak != 0 && !self.begin_temporary(peak) {
            return false;
        }
        if let Some(membership) = &mut self.membership {
            membership.insert_hash(hash);
        }
        if peak != 0
            || self
                .membership
                .as_ref()
                .map_or(0, ZoneMembership::capacity_bytes)
                != capacity_before
        {
            self.finish_temporary(previous);
        }
        self.state != ZoneMapState::Unknown
    }

    #[inline]
    fn observe_null(&mut self) {
        if self.state == ZoneMapState::Unknown {
            return;
        }
        self.has_null = true;
        if self.state == ZoneMapState::Empty {
            self.state = ZoneMapState::Ready;
        }
    }

    #[inline(always)]
    fn observe_worker_scalar(&mut self, incoming: WorkerScalar) {
        if self.state == ZoneMapState::Unknown {
            return;
        }
        // Primitive endpoints are inline. Membership growth performs its own
        // admission, and an incompatible endpoint clears its lease in
        // mark_unknown; neither needs a second full ownership walk per value.
        if incoming.owned_bytes() == 0 {
            self.observe_worker_scalar_inner(incoming);
            return;
        }
        let previous = self.temporary_bytes;
        if !self.begin_temporary(incoming.owned_bytes().saturating_mul(2)) {
            return;
        }
        self.observe_worker_scalar_inner(incoming);
        self.finish_temporary(previous);
    }
    #[inline(always)]
    fn observe_worker_scalar_inner(&mut self, incoming: WorkerScalar) {
        if self.state == ZoneMapState::Unknown {
            return;
        }
        let cache_key = match &incoming {
            WorkerScalar::Signed(v) => Some((1, *v as u64)),
            WorkerScalar::Unsigned(v) => Some((2, *v)),
            WorkerScalar::Float32(v) if !f32::from_bits(*v).is_nan() => Some((3, u64::from(*v))),
            WorkerScalar::Float64(v) if !f64::from_bits(*v).is_nan() => Some((4, *v)),
            WorkerScalar::Temporal(v) => Some((5, *v as u64)),
            WorkerScalar::DecimalCompact { mag, scale } => i64::try_from(*mag)
                .ok()
                .map(|v| (256 + u16::from(*scale), v as u64)),
            _ => None,
        };
        let cache_entry = cache_key.map(|(domain, bits)| {
            if self.recent_membership_domain != domain {
                self.recent_membership_domain = domain;
                self.recent_membership_valid = 0;
            }
            // Mix before indexing: fixed-scale decimals and floating-point
            // bit patterns commonly share their low bits. Equality still
            // checks the complete bits, never just this slot.
            let slot = (bits.wrapping_mul(0x9e3779b97f4a7c15) >> 58) as usize;
            (slot, bits)
        });
        if let Some((slot, bits)) = cache_entry {
            if self.recent_membership_valid & (1u64 << slot) != 0
                && self.recent_membership_hashes[slot] == bits
            {
                return;
            }
        }
        if self
            .membership
            .as_ref()
            .is_some_and(|membership| !matches!(membership, ZoneMembership::Saturated))
        {
            let hash = zone_membership_hash_worker(&incoming);
            let inserted = if cache_entry.is_some() {
                self.insert_membership_hash_uncached(hash)
            } else {
                self.insert_membership_hash(hash)
            };
            if !inserted {
                return;
            }
        }
        let compare = |stored: &WorkerScalar| -> Option<std::cmp::Ordering> {
            match (&incoming, stored) {
                (WorkerScalar::Signed(left), WorkerScalar::Signed(right)) => Some(left.cmp(right)),
                (WorkerScalar::Unsigned(left), WorkerScalar::Unsigned(right)) => {
                    Some(left.cmp(right))
                }
                (WorkerScalar::Float32(left), WorkerScalar::Float32(right)) => {
                    f32::from_bits(*left).partial_cmp(&f32::from_bits(*right))
                }
                (WorkerScalar::Float64(left), WorkerScalar::Float64(right)) => {
                    f64::from_bits(*left).partial_cmp(&f64::from_bits(*right))
                }
                (
                    WorkerScalar::DecimalCompact {
                        mag: left,
                        scale: left_scale,
                    },
                    WorkerScalar::DecimalCompact {
                        mag: right,
                        scale: right_scale,
                    },
                ) if left_scale == right_scale => Some(left.cmp(right)),
                (WorkerScalar::Temporal(left), WorkerScalar::Temporal(right)) => {
                    Some(left.cmp(right))
                }
                _ => None,
            }
        };
        let (advance_min, advance_max) = match (self.min.as_ref(), self.max.as_ref()) {
            (Some(min), Some(max)) => {
                let (Some(min_cmp), Some(max_cmp)) = (compare(min), compare(max)) else {
                    self.mark_unknown();
                    return;
                };
                (
                    min_cmp == std::cmp::Ordering::Less,
                    max_cmp == std::cmp::Ordering::Greater,
                )
            }
            _ => (true, true),
        };
        self.has_non_null = true;
        self.state = ZoneMapState::Ready;
        match (advance_min, advance_max) {
            (true, true) => {
                self.max = Some(incoming.clone());
                self.min = Some(incoming);
            }
            (true, false) => self.min = Some(incoming),
            (false, true) => self.max = Some(incoming),
            (false, false) => {}
        }
        if let Some((slot, bits)) = cache_entry {
            self.recent_membership_hashes[slot] = bits;
            self.recent_membership_valid |= 1u64 << slot;
        }
    }

    /// Borrowed-text sibling of `observe`.  The ANALYZE payload sink already
    /// owns a validated UTF-8 slice inside the pinned heap page; wrapping it
    /// in `Value::String` would allocate once per row solely for zone-map
    /// comparison.  Keep the exact CHAR/VARCHAR collation and PAD SPACE
    /// semantics while retaining bytes only when an endpoint advances.
    #[inline]
    fn observe_text_bytes(
        &mut self,
        collation_id: u16,
        bytes: &[u8],
        text_scratch: &mut Vec<u8>,
        text_lease: &mut ZoneScratchLease,
    ) {
        let info = crate::charset::resolve_collation_id(collation_id).unwrap_or_else(|| {
            crate::charset::resolve_collation_id(crate::charset::UTF8MB4_0900_AI_CI_ID)
                .expect("default collation id must resolve")
        });
        self.observe_text_bytes_for_info(info, bytes, text_scratch, text_lease);
    }

    #[inline]
    fn observe_text_bytes_for_info(
        &mut self,
        collation: &'static crate::charset::CollationInfo,
        bytes: &[u8],
        text_scratch: &mut Vec<u8>,
        text_lease: &mut ZoneScratchLease,
    ) {
        if self.state == ZoneMapState::Unknown {
            return;
        }
        // A complete short NO PAD value is already its normalized prefix.
        // Confirm exact bytes and prior observation before skipping all work.
        if !collation.pad_space
            && !self.text_weight_cache_saturated
            && bytes.len() <= ZONE_STRING_PREFIX_CHARS
        {
            let slot = (usize::from(bytes.first().copied().unwrap_or(0)) ^ bytes.len()) & 31;
            if let Some(index) = usize::from(self.text_weight_cache_hint[slot]).checked_sub(1) {
                if self.text_weight_cache.get(index).is_some_and(|cached| {
                    usize::from(cached.len) == bytes.len()
                        && cached.observed_truncations & 1 != 0
                        && cached.raw[..bytes.len()] == *bytes
                }) {
                    return;
                }
            }
        }
        let previous = self.temporary_bytes;
        if self.observe_text_bytes_for_info_inner(collation, bytes, text_scratch, text_lease) {
            self.finish_temporary(previous);
        }
    }
    fn observe_text_bytes_for_info_inner(
        &mut self,
        collation: &'static crate::charset::CollationInfo,
        bytes: &[u8],
        text_scratch: &mut Vec<u8>,
        text_lease: &mut ZoneScratchLease,
    ) -> bool {
        if self.state == ZoneMapState::Unknown {
            return true;
        }
        // Stored string lanes were UTF-8 validated before row encoding and
        // protected by the heap-page checksum. The typed decoder has the same
        // contract as its `Value::String` sibling, so revalidating every
        // VARCHAR for both statistics and zone-map consumers is redundant.
        let text = unsafe { std::str::from_utf8_unchecked(bytes) };
        let normalized = if collation.pad_space {
            crate::util::pad_space::pad_space_trim_end_str(text)
        } else {
            text
        };
        let prefix = zone_text_prefix_slice_normalized(normalized);
        let truncated = prefix.len() < normalized.len();
        let prefix_bytes = prefix.as_bytes();
        let cacheable = !self.text_weight_cache_saturated && prefix_bytes.len() <= 32;
        let hint_slot =
            (usize::from(prefix_bytes.first().copied().unwrap_or(0)) ^ prefix_bytes.len()) & 31;
        let cached = cacheable
            .then(|| {
                let matches = |cached: &CachedZoneTextPrefix| {
                    usize::from(cached.len) == prefix_bytes.len()
                        && cached.raw[..prefix_bytes.len()] == prefix_bytes[..]
                };
                // A hint is only an index, never evidence of equality. Verify the
                // bytes and retain the bounded dictionary search on collisions.
                let hint = usize::from(self.text_weight_cache_hint[hint_slot]).checked_sub(1);
                hint.filter(|&index| self.text_weight_cache.get(index).is_some_and(matches))
                    .or_else(|| self.text_weight_cache.iter().position(matches))
            })
            .flatten();
        if let Some(cached) = cached {
            self.text_weight_cache_hint[hint_slot] = (cached + 1) as u8;
            let observed_bit = 1 << u8::from(truncated);
            if self.text_weight_cache[cached].observed_truncations & observed_bit != 0 {
                return false;
            }
            // The identical zone prefix has already been fully observed, so
            // its weight cannot extend either endpoint. Preserve the distinct
            // truncated/non-truncated membership hashes, then skip the weight
            // copy and both endpoint comparisons.
            if self
                .membership
                .as_ref()
                .is_some_and(|membership| !matches!(membership, ZoneMembership::Saturated))
            {
                let hash = if truncated {
                    self.text_weight_cache[cached].truncated_hash
                } else {
                    self.text_weight_cache[cached].hash
                };
                if !self.insert_membership_hash(hash) {
                    return true;
                }
            }
            self.text_weight_cache[cached].observed_truncations |= observed_bit;
            self.has_non_null = true;
            self.state = ZoneMapState::Ready;
            self.truncated |= truncated;
            return false;
        }
        if !write_zone_text_weight(collation, prefix, text_scratch, text_lease) {
            self.mark_unknown();
            return true;
        }
        let incoming = text_scratch.as_slice();
        let compare = |stored: &WorkerScalar| match stored {
            WorkerScalar::TextWeight { weight, .. } => Some(incoming.cmp(weight.as_slice())),
            _ => None,
        };
        let (advance_min, advance_max) = match (self.min.as_ref(), self.max.as_ref()) {
            (Some(cur_min), Some(cur_max)) => {
                let (Some(min_cmp), Some(max_cmp)) = (compare(cur_min), compare(cur_max)) else {
                    self.mark_unknown();
                    return true;
                };
                (
                    min_cmp == std::cmp::Ordering::Less,
                    max_cmp == std::cmp::Ordering::Greater,
                )
            }
            _ => (true, true),
        };
        let spill = if cacheable
            && self.text_weight_cache.len() < 16
            && self.text_weight_cache.len() == self.text_weight_cache.capacity()
        {
            self.text_weight_cache.capacity().saturating_mul(2).max(1)
                * std::mem::size_of::<CachedZoneTextPrefix>()
        } else {
            0
        };
        // Only endpoint/cache copies allocate. An interior value still updates
        // membership, whose independent reservation covers exact/Bloom growth.
        let copies = usize::from(advance_min) + usize::from(advance_max);
        if !self.begin_temporary(
            text_scratch
                .len()
                .saturating_mul(copies)
                .saturating_add(spill),
        ) {
            return true;
        }
        if self
            .membership
            .as_ref()
            .is_some_and(|membership| !matches!(membership, ZoneMembership::Saturated))
        {
            if !self.insert_membership_hash(zone_membership_hash_borrowed(7, incoming, truncated)) {
                return true;
            }
        }
        // Reconcile membership before consuming the outer reservation for
        // cache storage; nested accounting must not count that storage twice.
        if cacheable && self.text_weight_cache.len() < 16 {
            let mut raw = [0u8; 32];
            raw[..prefix_bytes.len()].copy_from_slice(prefix_bytes);
            self.text_weight_cache_hint[hint_slot] = (self.text_weight_cache.len() + 1) as u8;
            self.text_weight_cache.push(CachedZoneTextPrefix {
                raw,
                len: prefix_bytes.len() as u8,
                observed_truncations: 1 << u8::from(truncated),
                hash: zone_membership_hash_borrowed(7, text_scratch, false),
                truncated_hash: zone_membership_hash_borrowed(7, text_scratch, true),
            });
        } else if cacheable {
            // High-cardinality text (for example lineitem.comment) gets
            // no benefit from a tiny dictionary. Stop probing after the
            // first bounded miss window instead of scanning 16 entries
            // for every subsequent row in the zone.
            self.text_weight_cache.clear();
            self.text_weight_cache_saturated = true;
        }
        self.has_non_null = true;
        self.state = ZoneMapState::Ready;
        self.truncated |= truncated;
        if advance_min {
            self.min = Some(WorkerScalar::TextWeight {
                weight: incoming.to_vec(),
                truncated,
            });
        }
        if advance_max {
            self.max = Some(WorkerScalar::TextWeight {
                weight: incoming.to_vec(),
                truncated,
            });
        }
        true
    }

    #[inline]
    fn observe_binary_bytes(&mut self, bytes: &[u8]) {
        if self.state == ZoneMapState::Unknown {
            return;
        }
        let previous = self.temporary_bytes;
        if !self.begin_temporary(bytes.len().min(ZONE_STRING_PREFIX_CHARS) * 2) {
            return;
        }
        self.observe_binary_bytes_inner(bytes);
        self.finish_temporary(previous);
    }
    fn observe_binary_bytes_inner(&mut self, bytes: &[u8]) {
        if self.state == ZoneMapState::Unknown {
            return;
        }
        let prefix_len = bytes.len().min(ZONE_STRING_PREFIX_CHARS);
        let incoming = &bytes[..prefix_len];
        let truncated = prefix_len < bytes.len();
        if self
            .membership
            .as_ref()
            .is_some_and(|membership| !matches!(membership, ZoneMembership::Saturated))
        {
            if !self.insert_membership_hash(zone_membership_hash_borrowed(6, incoming, truncated)) {
                return;
            }
        }
        let compare = |stored: &WorkerScalar| match stored {
            WorkerScalar::Bytes { prefix, .. } => Some(incoming.cmp(prefix.as_slice())),
            _ => None,
        };
        let (advance_min, advance_max) = match (self.min.as_ref(), self.max.as_ref()) {
            (Some(cur_min), Some(cur_max)) => {
                let (Some(min_cmp), Some(max_cmp)) = (compare(cur_min), compare(cur_max)) else {
                    self.mark_unknown();
                    return;
                };
                (
                    min_cmp == std::cmp::Ordering::Less,
                    max_cmp == std::cmp::Ordering::Greater,
                )
            }
            _ => (true, true),
        };
        self.has_non_null = true;
        self.state = ZoneMapState::Ready;
        self.truncated |= truncated;
        if advance_min {
            self.min = Some(WorkerScalar::Bytes {
                prefix: incoming.to_vec(),
                truncated,
            });
        }
        if advance_max {
            self.max = Some(WorkerScalar::Bytes {
                prefix: incoming.to_vec(),
                truncated,
            });
        }
    }

    /// Same cheap-then-promote pattern as `ZoneMapWorkerBatch::observe_row`,
    /// factored out so both the LOAD and REBUILD paths avoid duplicating
    /// the WorkerScalar comparison logic.
    #[inline]
    fn observe(
        &mut self,
        column: &ColumnSchema,
        collation_id: u16,
        value: &Value,
        text_scratch: &mut Vec<u8>,
        text_lease: &mut ZoneScratchLease,
    ) {
        if self.state == ZoneMapState::Unknown {
            return;
        }
        let temporary = match value {
            Value::Decimal(text) if text.len() > 256 => {
                self.mark_unknown();
                return;
            }
            Value::Decimal(text) => text.len().saturating_mul(4).saturating_add(64),
            _ => 0,
        };
        let previous = self.temporary_bytes;
        if !self.begin_temporary(temporary) {
            return;
        }
        self.observe_inner(column, collation_id, value, text_scratch, text_lease);
        self.finish_temporary(previous);
    }
    fn observe_inner(
        &mut self,
        column: &ColumnSchema,
        collation_id: u16,
        value: &Value,
        text_scratch: &mut Vec<u8>,
        text_lease: &mut ZoneScratchLease,
    ) {
        if self.state == ZoneMapState::Unknown {
            return;
        }
        if matches!(value, Value::Null) {
            self.has_null = true;
            if self.state == ZoneMapState::Empty {
                self.state = ZoneMapState::Ready;
            }
            return;
        }

        // Text is the expensive bulk-load case. Materialise the incoming
        // collation weight exactly once, compare that one scratch value with
        // both endpoints, and allocate only when the envelope advances.
        // The previous generic path generated the weight twice per row; the
        // older INSERT path allocated a String + Vec for every row.
        if matches!(
            column.data_type,
            DataTypeKind::Char { .. }
                | DataTypeKind::VarChar { .. }
                | DataTypeKind::Enum { .. }
                | DataTypeKind::Set { .. }
        ) {
            let bytes = match value {
                Value::String(text) => Some(text.as_bytes()),
                Value::SmallString(bytes) => Some(bytes.as_slice()),
                _ => None,
            };
            let Some(bytes) = bytes else {
                self.mark_unknown();
                return;
            };
            self.observe_text_bytes(collation_id, bytes, text_scratch, text_lease);
            return;
        }
        if matches!(
            column.data_type,
            DataTypeKind::Binary { .. } | DataTypeKind::VarBinary { .. }
        ) {
            if let Value::Bytes(bytes) = value {
                self.observe_binary_bytes(bytes);
            } else {
                self.mark_unknown();
            }
            return;
        }
        if self
            .membership
            .as_ref()
            .is_some_and(|membership| !matches!(membership, ZoneMembership::Saturated))
        {
            // Keep the per-row representation compact. In particular,
            // DecimalCompact used to take the expensive route through
            // format -> canonicalize -> parse merely to produce the same
            // persistent membership hash. The worker hasher implements the
            // identical canonical numeric identity without allocating.
            let Some(scalar) = worker_scalar_from_value(column, collation_id, value) else {
                self.mark_unknown();
                return;
            };
            let hash = zone_membership_hash_worker(&scalar);
            if !self.insert_membership_hash(hash) {
                return;
            }
        }
        let advance_min: bool;
        let advance_max: bool;
        match (self.min.as_ref(), self.max.as_ref()) {
            (Some(cur_min), Some(cur_max)) => {
                let min_cmp = match compare_value_to_worker(
                    column,
                    collation_id,
                    value,
                    cur_min,
                    text_scratch,
                ) {
                    Ok(Some(ord)) => ord,
                    Ok(None) | Err(()) => {
                        self.mark_unknown();
                        return;
                    }
                };
                let max_cmp = match compare_value_to_worker(
                    column,
                    collation_id,
                    value,
                    cur_max,
                    text_scratch,
                ) {
                    Ok(Some(ord)) => ord,
                    Ok(None) | Err(()) => {
                        self.mark_unknown();
                        return;
                    }
                };
                self.has_non_null = true;
                self.state = ZoneMapState::Ready;
                advance_min = min_cmp == std::cmp::Ordering::Less;
                advance_max = max_cmp == std::cmp::Ordering::Greater;
                if !advance_min && !advance_max {
                    return;
                }
            }
            _ => {
                advance_min = true;
                advance_max = true;
            }
        }
        let Some(new_scalar) = worker_scalar_from_value(column, collation_id, value) else {
            self.mark_unknown();
            return;
        };
        self.has_non_null = true;
        self.state = ZoneMapState::Ready;
        self.truncated |= new_scalar.was_truncated();
        match (advance_min, advance_max) {
            (true, true) => {
                self.max = Some(new_scalar.clone());
                self.min = Some(new_scalar);
            }
            (true, false) => self.min = Some(new_scalar),
            (false, true) => self.max = Some(new_scalar),
            (false, false) => {}
        }
    }
}

/// Typed row-codec sink for a zone-map rebuild. Fixed-width values flow
/// straight from the heap payload into `WorkerScalar`; no per-row
/// `Vec<Value>` or DECIMAL string is materialised on the common path.
struct ZoneRebuildDecodeSink<'a> {
    table: &'a TableSchema,
    column_indices: &'a [usize],
    collation_ids: &'a [u16],
    collations: &'a [&'static crate::charset::CollationInfo],
    running: &'a mut [WorkerColumnState],
    text_scratch: &'a mut Vec<u8>,
    text_lease: &'a mut ZoneScratchLease,
}

impl ZoneRebuildDecodeSink<'_> {
    #[inline(always)]
    fn observe_worker(&mut self, col: usize, scalar: WorkerScalar) {
        self.running[col].observe_worker_scalar(scalar);
    }

    #[inline]
    fn observe_value(&mut self, col: usize, mut value: Value) {
        let column_idx = self.column_indices[col];
        normalize_char_value_for_read(&mut value, &self.table.columns[column_idx].data_type);
        self.running[col].observe(
            &self.table.columns[column_idx],
            self.collation_ids[col],
            &value,
            self.text_scratch,
            self.text_lease,
        );
    }
}

impl RowDecodeSink for ZoneRebuildDecodeSink<'_> {
    #[inline]
    fn put_null(&mut self, col: usize) {
        self.running[col].observe_null();
    }

    #[inline]
    fn put_i64(&mut self, col: usize, value: i64) {
        self.observe_worker(col, WorkerScalar::Signed(value));
    }

    #[inline]
    fn put_u64(&mut self, col: usize, value: u64) {
        self.observe_worker(col, WorkerScalar::Unsigned(value));
    }

    #[inline]
    fn put_f32(&mut self, col: usize, value: f32) {
        if value.is_nan() {
            self.running[col].mark_unknown();
        } else {
            self.observe_worker(
                col,
                WorkerScalar::Float32(if value == 0.0 {
                    0.0f32.to_bits()
                } else {
                    value.to_bits()
                }),
            );
        }
    }

    #[inline]
    fn put_f64(&mut self, col: usize, value: f64) {
        if value.is_nan() {
            self.running[col].mark_unknown();
        } else {
            self.observe_worker(
                col,
                WorkerScalar::Float64(if value == 0.0 {
                    0.0f64.to_bits()
                } else {
                    value.to_bits()
                }),
            );
        }
    }

    #[inline]
    fn put_decimal_compact(&mut self, col: usize, magnitude: i64, scale: u8) {
        self.observe_worker(
            col,
            WorkerScalar::DecimalCompact {
                mag: i128::from(magnitude),
                scale,
            },
        );
    }

    #[inline]
    fn put_decimal_wide(&mut self, col: usize, magnitude: i128, scale: u8) {
        self.observe_worker(
            col,
            WorkerScalar::DecimalCompact {
                mag: magnitude,
                scale,
            },
        );
    }

    #[inline]
    fn put_date(&mut self, col: usize, days: i32) {
        self.observe_worker(col, WorkerScalar::Temporal(i64::from(days)));
    }

    #[inline]
    fn put_year(&mut self, col: usize, year: u8) {
        self.observe_worker(col, WorkerScalar::Temporal(i64::from(year)));
    }

    #[inline]
    fn put_time(&mut self, col: usize, micros: i64) {
        self.observe_worker(col, WorkerScalar::Temporal(micros));
    }

    #[inline]
    fn put_datetime(&mut self, col: usize, micros: i64) {
        self.observe_worker(col, WorkerScalar::Temporal(micros));
    }

    #[inline]
    fn put_bytes(&mut self, col: usize, bytes: &[u8]) {
        self.running[col].observe_binary_bytes(bytes);
    }

    #[inline]
    fn put_string(&mut self, col: usize, bytes: &[u8]) {
        let column_idx = self.column_indices[col];
        let bytes = if matches!(
            self.table.columns[column_idx].data_type,
            DataTypeKind::Char { .. }
        ) {
            &bytes[..crate::util::pad_space::trim_trailing_spaces_len(bytes)]
        } else {
            bytes
        };
        self.running[col].observe_text_bytes_for_info(
            self.collations[col],
            bytes,
            self.text_scratch,
            self.text_lease,
        );
    }

    fn put_json(&mut self, col: usize, _bytes: &[u8]) {
        self.running[col].mark_unknown();
    }

    fn put_geometry(&mut self, col: usize, _bytes: &[u8]) {
        self.running[col].mark_unknown();
    }

    fn put_vector(&mut self, col: usize, _bytes: &[u8]) {
        self.running[col].mark_unknown();
    }

    #[inline]
    fn put_value(&mut self, col: usize, value: Value) {
        self.observe_value(col, value);
    }
}

fn worker_state_into_summary(state: WorkerColumnState) -> ZoneColumnSummary {
    ZoneColumnSummary {
        state: state.state,
        durable_unknown: false,
        has_null: state.has_null,
        has_non_null: state.has_non_null,
        truncated: state.truncated,
        min: state.min.map(|s| s.into_zone_scalar()),
        max: state.max.map(|s| s.into_zone_scalar()),
        membership: state.membership,
    }
}

/// Use the same configured parallelism as the statistics scan preceding an
/// explicit ANALYZE. A fixed maintenance default otherwise leaves the full
/// heap pass under-parallelized even when statistics workers scale up.
/// The maintenance-specific override remains available; both paths are
/// capped by the number of zone-aligned chunks in the bounded scan batch.
fn rebuild_parallel_degree(page_count: u64, zone_pages: u64) -> u32 {
    let configured = crate::util::env::var("MYDB_ZONE_MAP_REBUILD_THREADS")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|d| *d >= 1)
        .unwrap_or_else(crate::planner::default_parallel_degree);
    rebuild_parallel_degree_with_threads(page_count, zone_pages, configured)
}

fn rebuild_parallel_degree_with_threads(page_count: u64, zone_pages: u64, configured: u32) -> u32 {
    const REBUILD_MIN_PAGES: u64 = 256;
    if page_count < REBUILD_MIN_PAGES || zone_pages == 0 {
        return 1;
    }
    if configured <= 1 {
        return 1;
    }
    let zones = page_count.div_ceil(zone_pages);
    let max_useful = zones.max(1) as u32;
    configured.min(max_useful)
}

/// Fold `incoming` into `target` using the same monotonic widen-only rules
/// `merge_value`/`merge_null`/`mark_unknown` would apply if run per row.
/// Used by the streaming rebuild to combine a zone's freshly-aggregated
/// summary with an existing entry (e.g. deferred-row fallback afterwards).
fn merge_column_summary_into(target: &mut ZoneColumnSummary, incoming: ZoneColumnSummary) {
    target.merge_summary(&incoming);
}

/// Enumerate the column indices and column_ids that participate in zone
/// map min/max tracking. Skipping Text/Blob/JSON/Geometry/Vector is the same
/// contract every per-row path uses; pre-computing lets the hot bulk-load
/// aggregation avoid `matches!` and `column.data_type` branch cost inside
/// its tight loops. Column order is preserved so downstream layers keep
/// deterministic column_id iteration.
fn zone_map_relevant_columns(table: &TableSchema) -> Vec<(usize, u32)> {
    let mut out = Vec::with_capacity(table.columns.len());
    for (idx, column) in table.columns.iter().enumerate() {
        if matches!(
            column.data_type,
            DataTypeKind::Text { .. }
                | DataTypeKind::Blob { .. }
                | DataTypeKind::Json
                | DataTypeKind::Geometry { .. }
                | DataTypeKind::Vector { .. }
        ) {
            continue;
        }
        out.push((idx, column.column_id));
    }
    out
}

/// Select a deterministic set of non-leading-index columns for value
/// membership. Membership is optional metadata: without fresh ANALYZE NDV
/// and physical page counts we keep min/max only. Every column with a known
/// non-zero NDV remains eligible, including low-NDV columns whose membership
/// can prove that an in-range value is absent from a zone.
fn zone_map_membership_requirements(
    table: &TableSchema,
) -> BTreeMap<u32, (ZoneMembershipLayout, u64)> {
    if table.stats.analyzed_at_unix_ms.is_none()
        || table.stats.row_count == 0
        || table.stats.heap_page_count == 0
    {
        return BTreeMap::new();
    }
    let zone_count = table
        .stats
        .heap_page_count
        .div_ceil(u64::from(table.zone_pages()).max(1));
    let rows_per_zone = table.stats.row_count.div_ceil(zone_count.max(1)).max(1);
    let mut candidates = zone_map_relevant_columns(table)
        .into_iter()
        .filter(|(idx, _)| {
            table
                .columns
                .get(*idx)
                .is_some_and(|column| table.index_on_column_prefix(&column.name).is_none())
        })
        .filter_map(|(idx, column_id)| {
            let ndv = table
                .columns
                .get(idx)
                .and_then(|column| table.stats.columns.get(&normalize_name(&column.name)))
                .map_or(0, |stats| stats.distinct_count);
            if ndv == 0 {
                return None;
            }
            let local_ndv = ndv.min(rows_per_zone).max(1);
            // About 6.24 bits/value is sufficient for a 5% target FPR at the
            // optimal k. Round the resulting bytes up to a 1..8 KiB power of
            // two so the persisted layout is stable and allocator-friendly.
            let target_bits = local_ndv.saturating_mul(7);
            let target_bytes = target_bits.div_ceil(8).max(1_024);
            let bytes = target_bytes.next_power_of_two().clamp(1_024, 8 * 1_024) as usize;
            let words = bytes / std::mem::size_of::<u64>();
            let bits_per_value = (words * 64) as f64 / local_ndv as f64;
            let hashes = (bits_per_value * std::f64::consts::LN_2)
                .round()
                .clamp(1.0, f64::from(ZONE_MEMBERSHIP_MAX_BLOOM_HASHES))
                as u8;
            Some((
                idx,
                column_id,
                ndv,
                local_ndv,
                ZoneMembershipLayout { words, hashes },
            ))
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        right
            .2
            .cmp(&left.2)
            .then_with(|| left.4.words.cmp(&right.4.words))
            .then_with(|| left.0.cmp(&right.0))
    });
    candidates
        .into_iter()
        .take(ZONE_MEMBERSHIP_MAX_COLUMNS)
        .map(|(_, column_id, _, local_ndv, layout)| (column_id, (layout, local_ndv)))
        .collect()
}

fn zone_map_membership_columns(table: &TableSchema) -> BTreeMap<u32, ZoneMembershipLayout> {
    zone_map_membership_requirements(table)
        .into_iter()
        .map(|(column_id, (layout, _))| (column_id, layout))
        .collect()
}

#[inline]
fn cached_membership_layout(
    columns: &[(u32, ZoneMembershipLayout)],
    column_id: u32,
) -> Option<ZoneMembershipLayout> {
    columns
        .binary_search_by_key(&column_id, |(candidate, _)| *candidate)
        .ok()
        .map(|position| columns[position].1)
}

/// During the first ANALYZE there are no trustworthy NDV statistics yet, but
/// the fused full-heap pass is the only opportunity to collect membership
/// without a second scan. Track a bounded provisional set using the largest
/// legal layout; once statistics are finalized, the caller drops columns
/// whose measured benefit is below the threshold and retunes exact sets.
fn zone_map_provisional_membership_columns(
    table: &TableSchema,
) -> BTreeMap<u32, ZoneMembershipLayout> {
    let selected = zone_map_membership_columns(table);
    let has_reliable_stats = table.stats.row_count > 0
        && table.stats.heap_page_count > 0
        && !table.stats.columns.is_empty();
    if !selected.is_empty() || has_reliable_stats {
        return selected;
    }
    zone_map_relevant_columns(table)
        .into_iter()
        .filter(|(idx, _)| {
            table
                .columns
                .get(*idx)
                .is_some_and(|column| table.index_on_column_prefix(&column.name).is_none())
        })
        .take(ZONE_MEMBERSHIP_MAX_COLUMNS)
        .map(|(_, column_id)| {
            (
                column_id,
                ZoneMembershipLayout {
                    words: ZONE_MEMBERSHIP_MAX_BLOOM_WORDS,
                    hashes: ZONE_MEMBERSHIP_PROVISIONAL_BLOOM_HASHES,
                },
            )
        })
        .collect()
}

/// Aggregate a run of rows sharing a single zone into one column summary.
/// The hot bulk-load path calls this once per (zone, column) instead of the
/// old per-row/per-column `entry(...)` traffic. We track running min/max as
/// indices into the row slice so we clone the ZoneScalar at most twice
/// (once for min, once for max) irrespective of run length. `merge_value`'s
/// per-row clone would otherwise happen on every strictly-decreasing/
/// strictly-increasing row.
fn aggregate_column_values_range<R: AsRef<[Value]>>(
    summary: &mut ZoneColumnSummary,
    column: &ColumnSchema,
    rows: &[R],
    col_idx: usize,
    membership_layout: Option<ZoneMembershipLayout>,
) {
    if summary.state == ZoneMapState::Unknown {
        return;
    }
    if let Some(layout) = membership_layout {
        summary.enable_membership_with_layout(layout);
        for row in rows {
            match row.as_ref().get(col_idx) {
                Some(Value::Null) => {}
                Some(value) => match zone_scalar(column, value) {
                    Some(scalar) => summary.merge_membership_value(&scalar),
                    None => {
                        summary.mark_unknown();
                        return;
                    }
                },
                None => {
                    summary.mark_unknown();
                    return;
                }
            }
        }
    }
    if matches!(
        column.data_type,
        DataTypeKind::TinyInt
            | DataTypeKind::SmallInt
            | DataTypeKind::MediumInt
            | DataTypeKind::Int
            | DataTypeKind::BigInt
            | DataTypeKind::Bool
    ) {
        if column.unsigned {
            let mut min = None::<u64>;
            let mut max = None::<u64>;
            let mut has_null = false;
            for row in rows {
                match row.as_ref().get(col_idx) {
                    Some(Value::UInt(value)) => {
                        min = Some(min.map_or(*value, |current| current.min(*value)));
                        max = Some(max.map_or(*value, |current| current.max(*value)));
                    }
                    Some(Value::Null) => has_null = true,
                    _ => return summary.mark_unknown(),
                }
            }
            if has_null {
                summary.merge_null();
            }
            if let Some(value) = min {
                summary.merge_value(ZoneScalar::Unsigned(value));
            }
            if let Some(value) = max {
                summary.merge_value(ZoneScalar::Unsigned(value));
            }
        } else {
            let mut min = None::<i64>;
            let mut max = None::<i64>;
            let mut has_null = false;
            for row in rows {
                match row.as_ref().get(col_idx) {
                    Some(Value::Int(value)) => {
                        min = Some(min.map_or(*value, |current| current.min(*value)));
                        max = Some(max.map_or(*value, |current| current.max(*value)));
                    }
                    Some(Value::Null) => has_null = true,
                    _ => return summary.mark_unknown(),
                }
            }
            if has_null {
                summary.merge_null();
            }
            if let Some(value) = min {
                summary.merge_value(ZoneScalar::Signed(value));
            }
            if let Some(value) = max {
                summary.merge_value(ZoneScalar::Signed(value));
            }
        }
        return;
    }
    let collation_id =
        crate::charset::collation_id(column.collation.as_deref().unwrap_or("utf8mb4_0900_ai_ci"));
    let mut state = WorkerColumnState::default();
    let mut text_scratch = Vec::new();
    let mut text_lease = ZoneScratchLease::default();
    for row in rows {
        match row.as_ref().get(col_idx) {
            Some(value) => state.observe(
                column,
                collation_id,
                value,
                &mut text_scratch,
                &mut text_lease,
            ),
            None => {
                state.mark_unknown();
                break;
            }
        }
    }
    let membership = summary.membership.take();
    merge_column_summary_into(summary, worker_state_into_summary(state));
    if summary.state != ZoneMapState::Unknown {
        summary.membership = membership;
    }
}

fn zone_schema_fingerprint(table: &TableSchema) -> u64 {
    // Stable, length-delimited FNV-1a encoding.  This fingerprint is part of
    // the sidecar trust boundary, so it must not depend on Rust's randomized
    // `Hash` implementation or on `Debug` formatting.
    let mut fingerprint = 0xcbf29ce484222325u64;
    let mut write = |bytes: &[u8]| {
        for &byte in bytes {
            fingerprint ^= u64::from(byte);
            fingerprint = fingerprint.wrapping_mul(0x100000001b3);
        }
    };
    write(&ZONE_MAP_SEMANTIC_VERSION.to_le_bytes());
    write(&table.zone_pages().to_le_bytes());
    write(&(table.columns.len() as u64).to_le_bytes());
    for column in &table.columns {
        write(&column.column_id.to_le_bytes());
        write(&[u8::from(column.unsigned)]);
        let encoded_type = serde_json::to_vec(&column.data_type).unwrap_or_default();
        write(&(encoded_type.len() as u64).to_le_bytes());
        write(&encoded_type);
        let collation_id = crate::charset::collation_id(
            column.collation.as_deref().unwrap_or("utf8mb4_0900_ai_ci"),
        );
        write(&collation_id.to_le_bytes());
        let pad_space =
            crate::charset::resolve_collation_id(collation_id).is_some_and(|info| info.pad_space);
        write(&[u8::from(pad_space)]);
    }
    fingerprint
}

fn zone_query_scalar(column: &ColumnSchema, value: &Value) -> Option<ZoneScalar> {
    // FLOAT comparisons execute in f64 for mixed numeric operands. Never
    // round the query to f32: doing so loses strict inequality boundary rows.
    if matches!(
        column.data_type,
        DataTypeKind::Float { .. } | DataTypeKind::Double { .. }
    ) && matches!(
        value,
        Value::Int(_)
            | Value::UInt(_)
            | Value::Float(_)
            | Value::Double(_)
            | Value::Decimal(_)
            | Value::DecimalCompact(..)
            | Value::Decimal128(..)
            | Value::DecimalWide(..)
    ) {
        let number = crate::engine::expr::numeric(value).ok()?;
        if number.is_nan() {
            return None;
        }
        if matches!(column.data_type, DataTypeKind::Float { .. })
            && (number as f32) as f64 == number
        {
            return Some(ZoneScalar::Float32(
                (if number == 0.0 { 0.0 } else { number as f32 }).to_bits(),
            ));
        }
        return Some(ZoneScalar::Float64(
            (if number == 0.0 { 0.0 } else { number }).to_bits(),
        ));
    }
    if matches!(column.data_type, DataTypeKind::Year) {
        let year = match value {
            Value::Int(n) => *n,
            Value::UInt(n) => i64::try_from(*n).ok()?,
            _ => return zone_scalar_physical(column, value),
        };
        return match year {
            0 => Some(ZoneScalar::Temporal(0)),
            1901..=2155 => Some(ZoneScalar::Temporal(year - 1900)),
            _ => None,
        };
    }
    // Integral compact dates use the same parser and session timezone as the
    // row evaluator. Fractional numerics retain fallback (fsp rounding is
    // expression-context dependent).
    if matches!(
        column.data_type,
        DataTypeKind::DateTime { .. } | DataTypeKind::Timestamp { .. }
    ) && matches!(value, Value::Int(_) | Value::UInt(_))
    {
        let text = Value::String(value.to_display_string());
        let coerced =
            crate::engine::expr::coerce_comparison_constant_for_type(&text, &column.data_type)?;
        return zone_scalar_physical(column, &coerced);
    }
    zone_scalar(column, value)
}

fn zone_scalar(column: &ColumnSchema, value: &Value) -> Option<ZoneScalar> {
    zone_scalar_physical(column, value).or_else(|| {
        let coerced =
            crate::engine::expr::coerce_comparison_constant_for_type(value, &column.data_type)?;
        (coerced != *value)
            .then(|| zone_scalar_physical(column, &coerced))
            .flatten()
    })
}

fn zone_scalar_physical(column: &ColumnSchema, value: &Value) -> Option<ZoneScalar> {
    use DataTypeKind::*;
    match (&column.data_type, value) {
        (_, Value::Null) => None,
        (TinyInt | SmallInt | MediumInt | Int | BigInt | Bool, Value::Int(v)) => {
            if column.unsigned && *v >= 0 {
                Some(ZoneScalar::Unsigned(*v as u64))
            } else {
                Some(ZoneScalar::Signed(*v))
            }
        }
        (TinyInt | SmallInt | MediumInt | Int | BigInt | Bool | Bit { .. }, Value::UInt(v)) => {
            if column.unsigned || matches!(column.data_type, Bit { .. }) {
                Some(ZoneScalar::Unsigned(*v))
            } else {
                i64::try_from(*v).ok().map(ZoneScalar::Signed)
            }
        }
        (Float { .. }, Value::Float(v)) if !v.is_nan() => Some(ZoneScalar::Float32(if *v == 0.0 {
            0.0f32.to_bits()
        } else {
            v.to_bits()
        })),
        (Double { .. }, Value::Double(v)) if !v.is_nan() => {
            Some(ZoneScalar::Float64(if *v == 0.0 {
                0.0f64.to_bits()
            } else {
                v.to_bits()
            }))
        }
        (Decimal { .. }, Value::DecimalCompact(mag, scale)) => Some(ZoneScalar::Decimal(
            crate::util::decimal::canonical_decimal_str(
                &crate::util::decimal::format_decimal_parts(*mag as i128, *scale),
            )?,
        )),
        (Decimal { .. }, Value::Decimal(s)) => {
            crate::util::decimal::canonical_decimal_str(s).map(ZoneScalar::Decimal)
        }
        // MySQL compares an integer literal to a DECIMAL column in exact
        // decimal arithmetic.  Integer tokens are represented as Int/UInt in
        // the parser, so requiring Value::Decimal here incorrectly rejected
        // safe predicates such as `decimal_col = 18` during zone-bound
        // compilation.  Preserve the exact integer text; decimal comparison
        // canonicalizes scale on both the query bound and stored summaries.
        (Decimal { .. }, Value::Int(v)) => Some(ZoneScalar::Decimal(v.to_string())),
        (Decimal { .. }, Value::UInt(v)) => Some(ZoneScalar::Decimal(v.to_string())),
        (Date, Value::Date(v)) => Some(ZoneScalar::Temporal(*v as i64)),
        (Time { .. }, Value::Time(v))
        | (DateTime { .. }, Value::DateTime(v))
        | (Timestamp { .. }, Value::Timestamp(v)) => Some(ZoneScalar::Temporal(*v)),
        (Year, Value::Year(v)) => Some(ZoneScalar::Temporal(*v as i64)),
        (Binary { .. } | VarBinary { .. }, Value::Bytes(bytes)) => {
            let truncated = bytes.len() > ZONE_STRING_PREFIX_CHARS;
            Some(ZoneScalar::Binary {
                prefix: bytes
                    .iter()
                    .take(ZONE_STRING_PREFIX_CHARS)
                    .copied()
                    .collect(),
                truncated,
            })
        }
        (Char { .. } | VarChar { .. } | Enum { .. } | Set { .. }, Value::String(text)) => {
            text_scalar(column, text)
        }
        (Char { .. } | VarChar { .. } | Enum { .. } | Set { .. }, Value::SmallString(bytes)) => {
            std::str::from_utf8(bytes.as_slice())
                .ok()
                .and_then(|s| text_scalar(column, s))
        }
        _ => None,
    }
}

fn text_scalar(column: &ColumnSchema, text: &str) -> Option<ZoneScalar> {
    // Zone summaries are populated with `collation_key_by_id`; query bounds
    // must use the identical representation. `WEIGHT_STRING()`'s SQL result
    // encoder is intentionally presentation-oriented and is not byte-for-byte
    // interchangeable with the internal collation key. Mixing the two makes
    // a valid equality/IN bound appear outside [min,max], causing a false
    // negative before the row evaluator sees the record.
    let collation_id =
        crate::charset::collation_id(column.collation.as_deref().unwrap_or("utf8mb4_0900_ai_ci"));
    let normalized =
        if crate::charset::resolve_collation_id(collation_id).is_some_and(|info| info.pad_space) {
            crate::util::pad_space::pad_space_trim_end_str(text)
        } else {
            text
        };
    let prefix = zone_text_prefix_slice(text, collation_id);
    let truncated = prefix.len() < normalized.len();
    let weight = crate::charset::collation_key_by_id(collation_id, prefix);
    Some(ZoneScalar::Text { weight, truncated })
}

fn literal_value(expr: &Expr) -> Option<Value> {
    match expr {
        Expr::BoundValue(value) => Some(value.clone()),
        Expr::Placeholder(index) => crate::engine::params::current_param(*index),
        Expr::Literal(Literal::Null) => Some(Value::Null),
        Expr::Literal(Literal::Bool(v)) => Some(Value::Int(i64::from(*v))),
        Expr::Literal(Literal::Int(v)) => Some(Value::Int(*v)),
        Expr::Literal(Literal::UInt(v)) => Some(Value::UInt(*v)),
        Expr::Literal(Literal::Float(v)) => Some(Value::Double(*v)),
        Expr::Literal(Literal::Decimal(v)) => Some(Value::Decimal(v.clone())),
        Expr::Literal(Literal::String(v)) => Some(Value::String(v.clone())),
        Expr::Literal(Literal::HexBytes(v) | Literal::BitBytes(v)) => Some(Value::Bytes(v.clone())),
        // Unary negative numerics and other deterministic constant-foldable
        // expressions (for example DATE arithmetic) are valid zone bounds.
        // Identifiers/non-constants simply fail folding and remain Unknown.
        _ => crate::engine::expr::eval_const(expr).ok(),
    }
}

fn expr_column<'a>(table: &'a TableSchema, expr: &Expr) -> Option<&'a ColumnSchema> {
    let name = match expr {
        Expr::Identifier(name) => name.as_str(),
        Expr::CompoundIdentifier(parts) => parts.last()?.as_str(),
        _ => return None,
    };
    table
        .columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(name))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ZoneMatchDecision {
    /// The metadata proves that no row in the zone can satisfy the predicate.
    ProvenNoMatch,
    /// Valid, compatible bounds overlap the predicate.
    MayMatch,
    /// Metadata or SQL-domain compatibility is insufficient to decide.
    Unknown,
}

fn comparison_decision(
    summary: &ZoneColumnSummary,
    op: crate::parser::BinaryOp,
    query: &ZoneScalar,
) -> ZoneMatchDecision {
    use crate::parser::BinaryOp;
    // Unknown means that the envelope was deliberately not computed (for
    // example on the foreground path of a large bulk insert).  It can never
    // prove exclusion, so retain the zone until maintenance rebuilds an exact
    // Ready summary.  Treating Unknown like Empty here causes false negatives.
    if summary.state == ZoneMapState::Unknown {
        return ZoneMatchDecision::Unknown;
    }
    if summary.state != ZoneMapState::Ready {
        return if summary.state == ZoneMapState::Empty {
            ZoneMatchDecision::ProvenNoMatch
        } else {
            ZoneMatchDecision::Unknown
        };
    }
    if !summary.has_non_null {
        return ZoneMatchDecision::ProvenNoMatch;
    }
    // A prefix envelope is not a proof that all values equal the query.
    // `truncated` also covers interior values whose equal prefix did not
    // replace either bound during merging.
    if op == BinaryOp::NotEq && (summary.truncated || query.was_truncated()) {
        return ZoneMatchDecision::Unknown;
    }
    let (Some(min), Some(max)) = (&summary.min, &summary.max) else {
        return ZoneMatchDecision::Unknown;
    };
    let Some(q_min) = query.compare(min) else {
        return ZoneMatchDecision::Unknown;
    };
    let Some(q_max) = query.compare(max) else {
        return ZoneMatchDecision::Unknown;
    };
    // Prefix equality is never proof of exclusion: a retained 16-character
    // boundary may stand for any longer value with that prefix.
    let prefix_boundary = ((summary.truncated || min.was_truncated() || query.was_truncated())
        && q_min == std::cmp::Ordering::Equal)
        || ((summary.truncated || max.was_truncated() || query.was_truncated())
            && q_max == std::cmp::Ordering::Equal);
    if prefix_boundary {
        return ZoneMatchDecision::Unknown;
    }
    let may_match = match op {
        BinaryOp::Eq | BinaryOp::NullSafeEq => {
            !(q_min == std::cmp::Ordering::Less || q_max == std::cmp::Ordering::Greater)
        }
        BinaryOp::NotEq => q_min != std::cmp::Ordering::Equal || q_max != std::cmp::Ordering::Equal,
        BinaryOp::Lt => q_min == std::cmp::Ordering::Greater,
        BinaryOp::LtEq => q_min != std::cmp::Ordering::Less,
        BinaryOp::Gt => q_max == std::cmp::Ordering::Less,
        BinaryOp::GtEq => q_max != std::cmp::Ordering::Greater,
        _ => return ZoneMatchDecision::Unknown,
    };
    if may_match {
        ZoneMatchDecision::MayMatch
    } else {
        ZoneMatchDecision::ProvenNoMatch
    }
}

#[inline]
fn comparison_may_match(
    summary: &ZoneColumnSummary,
    op: crate::parser::BinaryOp,
    query: &ZoneScalar,
) -> bool {
    comparison_decision(summary, op, query) != ZoneMatchDecision::ProvenNoMatch
}

#[cfg(test)]
#[inline]
fn equality_may_match(summary: &ZoneColumnSummary, query: &ZoneScalar) -> bool {
    equality_may_match_with(summary, query, |query| {
        summary
            .membership
            .as_ref()
            .map(|membership| membership.may_contain_hash(zone_membership_hash(query)))
    })
}

/// Share SQL-domain and prefix safety checks between resident summaries and
/// borrowed paged membership. The probe is lazy: an incompatible domain must
/// never reject a zone, nor read an otherwise unnecessary membership page.
#[inline]
fn equality_may_match_with(
    summary: &ZoneColumnSummary,
    query: &ZoneScalar,
    probe: impl FnOnce(&ZoneScalar) -> Option<bool>,
) -> bool {
    use crate::parser::BinaryOp;
    match comparison_decision(summary, BinaryOp::Eq, query) {
        ZoneMatchDecision::ProvenNoMatch => return false,
        // Membership fingerprints are meaningful only after min/max proves
        // that the query scalar is in the same SQL comparison domain.
        ZoneMatchDecision::Unknown => return true,
        ZoneMatchDecision::MayMatch => {}
    }
    // Mixed FLOAT/f64 bounds have different fingerprint encodings. Range
    // comparison remains exact but membership cannot reject across domains.
    if summary
        .min
        .as_ref()
        .is_some_and(|min| std::mem::discriminant(min) != std::mem::discriminant(query))
    {
        return true;
    }
    let Some(may_match) = probe(query) else {
        return true;
    };
    ZONE_MAP_MEMBERSHIP_CHECKS.fetch_add(1, Ordering::Relaxed);
    if !may_match {
        ZONE_MAP_MEMBERSHIP_REJECTIONS.fetch_add(1, Ordering::Relaxed);
    }
    may_match
}

fn zone_like_scalar(column: &ColumnSchema, text: &str) -> Option<ZoneScalar> {
    if matches!(
        column.data_type,
        DataTypeKind::Binary { .. } | DataTypeKind::VarBinary { .. }
    ) {
        zone_scalar(column, &Value::Bytes(text.as_bytes().to_vec()))
    } else {
        zone_scalar(column, &Value::String(text.to_string()))
    }
}

fn like_prefix_zone_bounds(
    column: &ColumnSchema,
    prefix: &str,
) -> Option<(ZoneScalar, Option<ZoneScalar>)> {
    if matches!(
        column.data_type,
        DataTypeKind::Binary { .. } | DataTypeKind::VarBinary { .. }
    ) {
        let bytes = prefix.as_bytes();
        let bounded = &bytes[..bytes.len().min(ZONE_STRING_PREFIX_CHARS)];
        let lower = ZoneScalar::Binary {
            prefix: bounded.to_vec(),
            truncated: bytes.len() > bounded.len(),
        };
        let mut upper_bytes = bounded.to_vec();
        let upper = loop {
            let Some(last) = upper_bytes.last_mut() else {
                break None;
            };
            if *last != u8::MAX {
                *last += 1;
                break Some(ZoneScalar::Binary {
                    prefix: upper_bytes,
                    truncated: false,
                });
            }
            upper_bytes.pop();
        };
        return Some((lower, upper));
    }

    let bounded: String = prefix.chars().take(ZONE_STRING_PREFIX_CHARS).collect();
    let lower = zone_like_scalar(column, &bounded)?;
    let upper = crate::engine::expr::like_prefix_successor_for_collation(
        &bounded,
        column.collation.as_deref(),
    )
    .and_then(|value| zone_scalar(column, &value));
    Some((lower, upper))
}

#[derive(Debug, Clone)]
enum ZonePredicateProgram {
    Never,
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
    Compare {
        column_id: u32,
        op: crate::parser::BinaryOp,
        query: ZoneScalar,
    },
    NullSafeNull {
        column_id: u32,
    },
    IsNull {
        column_id: u32,
        negated: bool,
    },
    Between {
        column_id: u32,
        low: ZoneScalar,
        high: ZoneScalar,
    },
    InList {
        column_id: u32,
        values: Vec<ZoneScalar>,
    },
    LikePrefix {
        column_id: u32,
        lower: ZoneScalar,
        upper: Option<ZoneScalar>,
    },
}

impl ZonePredicateProgram {
    #[cfg(test)]
    fn may_match(&self, zone: &ZoneSummary) -> bool {
        if matches!(self, Self::Never) {
            return false;
        }
        if zone.state == Some(ZoneMapState::Unknown) {
            return true;
        }
        self.may_match_by(&mut |predicate, column| {
            zone.columns.get(&column).is_none_or(|summary| {
                predicate.column_may_match(summary, |query| {
                    summary
                        .membership
                        .as_ref()
                        .map(|membership| membership.may_contain_hash(zone_membership_hash(query)))
                })
            })
        })
    }

    fn may_match_by(&self, lookup: &mut impl FnMut(&Self, u32) -> bool) -> bool {
        match self {
            Self::Never => false,
            Self::And(left, right) => left.may_match_by(lookup) && right.may_match_by(lookup),
            Self::Or(left, right) => left.may_match_by(lookup) || right.may_match_by(lookup),
            Self::Compare { column_id, .. }
            | Self::NullSafeNull { column_id }
            | Self::IsNull { column_id, .. }
            | Self::Between { column_id, .. }
            | Self::InList { column_id, .. }
            | Self::LikePrefix { column_id, .. } => lookup(self, *column_id),
        }
    }

    fn column_may_match(
        &self,
        summary: &ZoneColumnSummary,
        mut probe: impl FnMut(&ZoneScalar) -> Option<bool>,
    ) -> bool {
        use crate::parser::BinaryOp;
        match self {
            Self::Compare { op, query, .. } => {
                if matches!(op, BinaryOp::Eq | BinaryOp::NullSafeEq) {
                    equality_may_match_with(summary, query, probe)
                } else {
                    comparison_may_match(summary, *op, query)
                }
            }
            Self::NullSafeNull { .. } => summary.state != ZoneMapState::Ready || summary.has_null,
            Self::IsNull { negated, .. } => {
                summary.state != ZoneMapState::Ready
                    || if *negated {
                        summary.has_non_null
                    } else {
                        summary.has_null
                    }
            }
            Self::Between { low, high, .. } => {
                comparison_may_match(summary, BinaryOp::GtEq, low)
                    && comparison_may_match(summary, BinaryOp::LtEq, high)
            }
            Self::InList { values, .. } => values
                .iter()
                .any(|value| equality_may_match_with(summary, value, &mut probe)),
            Self::LikePrefix { lower, upper, .. } => {
                comparison_may_match(summary, BinaryOp::GtEq, lower)
                    && upper
                        .as_ref()
                        .is_none_or(|upper| comparison_may_match(summary, BinaryOp::Lt, upper))
            }
            // Only leaves reach this method. Conservatively retain a zone if
            // an invalid caller supplies a composite predicate.
            Self::Never => false,
            Self::And(..) | Self::Or(..) => true,
        }
    }
}

// Map finite ordinal domains back into the existing text envelope. This keeps
// sidecars and membership hashes in one domain. Bound expansion so a wide SET
// cannot turn planning into an exponential operation.
fn compile_zone_member_comparison(
    table: &TableSchema,
    name: &str,
    args: &[Expr],
) -> Option<ZonePredicateProgram> {
    if args.len() < 4 {
        return None;
    }
    let column = expr_column(table, &args[0])?;
    let is_set = matches!(column.data_type, DataTypeKind::Set { .. });
    if !(is_set && matches!(name, "__SET_CMP" | "__SET_CMP_BINARY")
        || matches!(column.data_type, DataTypeKind::Enum { .. }) && name == "__ENUM_CMP")
    {
        return None;
    }
    let constants = args[1..]
        .iter()
        .map(literal_value)
        .collect::<Option<Vec<_>>>()?;
    let members = &constants[2..];
    let declared = match &column.data_type {
        DataTypeKind::Enum { values } | DataTypeKind::Set { values } => values,
        _ => return None,
    };
    if members.len() != declared.len()
        || !members
            .iter()
            .zip(declared)
            .all(|(a, b)| a.to_display_string() == *b)
    {
        return None;
    }
    let count = if is_set {
        1usize.checked_shl(members.len().try_into().ok()?)?
    } else {
        members.len() + 1
    };
    if count > 1024 {
        return None;
    }
    let mut values = Vec::new();
    let mut call_values = Vec::with_capacity(constants.len() + 1);
    call_values.push(Value::Null);
    call_values.extend(constants.iter().cloned());
    for ordinal in 0..count {
        let text = if is_set {
            members
                .iter()
                .enumerate()
                .filter(|(i, _)| ordinal & (1usize << i) != 0)
                .map(|(_, v)| v.to_display_string())
                .collect::<Vec<_>>()
                .join(",")
        } else if ordinal == 0 {
            String::new()
        } else {
            members[ordinal - 1].to_display_string()
        };
        let stored = if !is_set && ordinal > 0 && text.is_empty() {
            Value::SmallString(crate::types::SmallStringBuf::new())
        } else {
            Value::String(text.clone())
        };
        call_values[0] = stored;
        if matches!(
            crate::engine::expr::eval_enum_set_comparison_values(name, &call_values).ok()?,
            Value::Int(1)
        ) {
            values.push(text_scalar(column, &text)?);
        }
    }
    if constants[0] == Value::Null && constants[1].to_display_string() == "<=>" {
        return Some(ZonePredicateProgram::NullSafeNull {
            column_id: column.column_id,
        });
    }
    Some(if values.is_empty() {
        ZonePredicateProgram::Never
    } else {
        ZonePredicateProgram::InList {
            column_id: column.column_id,
            values,
        }
    })
}

fn compile_zone_predicate(table: &TableSchema, expr: &Expr) -> Option<ZonePredicateProgram> {
    use crate::parser::BinaryOp;
    match expr {
        Expr::Function { name, args, .. }
            if matches!(
                name.as_str(),
                "__ENUM_CMP" | "__SET_CMP" | "__SET_CMP_BINARY"
            ) =>
        {
            compile_zone_member_comparison(table, name, args)
        }
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => match (
            compile_zone_predicate(table, left),
            compile_zone_predicate(table, right),
        ) {
            (Some(left), Some(right)) => {
                Some(ZonePredicateProgram::And(Box::new(left), Box::new(right)))
            }
            (Some(program), None) | (None, Some(program)) => Some(program),
            (None, None) => None,
        },
        Expr::Binary {
            left,
            op: BinaryOp::Or,
            right,
        } => Some(ZonePredicateProgram::Or(
            Box::new(compile_zone_predicate(table, left)?),
            Box::new(compile_zone_predicate(table, right)?),
        )),
        Expr::Binary { left, op, right }
            if matches!(
                op,
                BinaryOp::Eq
                    | BinaryOp::NotEq
                    | BinaryOp::NullSafeEq
                    | BinaryOp::Lt
                    | BinaryOp::LtEq
                    | BinaryOp::Gt
                    | BinaryOp::GtEq
            ) =>
        {
            let (column, value, op, constant) = if let (Some(column), Some(value)) =
                (expr_column(table, left), literal_value(right))
            {
                (column, value, *op, right.as_ref())
            } else if let (Some(column), Some(value)) =
                (expr_column(table, right), literal_value(left))
            {
                let flipped = match op {
                    BinaryOp::Lt => BinaryOp::Gt,
                    BinaryOp::LtEq => BinaryOp::GtEq,
                    BinaryOp::Gt => BinaryOp::Lt,
                    BinaryOp::GtEq => BinaryOp::LtEq,
                    other => *other,
                };
                (column, value, flipped, left.as_ref())
            } else {
                return None;
            };
            if matches!(
                column.data_type,
                DataTypeKind::DateTime { .. } | DataTypeKind::Timestamp { .. }
            ) && matches!(value, Value::Int(_) | Value::UInt(_))
                && !matches!(
                    constant,
                    Expr::Literal(_) | Expr::BoundValue(_) | Expr::Placeholder(_)
                )
            {
                // Computed numerics compare to YYYYMMDDHHMMSS numerically;
                // only literal/bound numerics use compact-date conversion.
                return None;
            }
            if matches!(value, Value::Null) {
                return Some(if op == BinaryOp::NullSafeEq {
                    ZonePredicateProgram::NullSafeNull {
                        column_id: column.column_id,
                    }
                } else {
                    ZonePredicateProgram::Never
                });
            }
            Some(ZonePredicateProgram::Compare {
                column_id: column.column_id,
                op,
                // Inequality exclusion requires exact equality, so do not
                // round a constant into the stored column domain (e.g. a
                // DOUBLE literal into FLOAT). Unsupported coercions retain
                // the ordinary scan and SQL evaluator.
                query: if op == BinaryOp::NotEq
                    && !matches!(
                        column.data_type,
                        DataTypeKind::Float { .. } | DataTypeKind::Double { .. }
                    ) {
                    zone_scalar_physical(column, &value)?
                } else {
                    zone_query_scalar(column, &value)?
                },
            })
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            let column = expr_column(table, inner)?;
            Some(ZonePredicateProgram::IsNull {
                column_id: column.column_id,
                negated: matches!(expr, Expr::IsNotNull(_)),
            })
        }
        Expr::Between {
            expr,
            low,
            high,
            negated: false,
        } => {
            let column = expr_column(table, expr)?;
            Some(ZonePredicateProgram::Between {
                column_id: column.column_id,
                low: zone_query_scalar(column, &literal_value(low)?)?,
                high: zone_query_scalar(column, &literal_value(high)?)?,
            })
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            let column = expr_column(table, expr)?;
            let mut values = Vec::with_capacity(list.len());
            for item in list {
                let value = literal_value(item)?;
                if !matches!(value, Value::Null) {
                    values.push(zone_query_scalar(column, &value)?);
                }
            }
            Some(if values.is_empty() {
                ZonePredicateProgram::Never
            } else {
                ZonePredicateProgram::InList {
                    column_id: column.column_id,
                    values,
                }
            })
        }
        Expr::Like {
            expr,
            pattern,
            negated: false,
            escape_char,
        } => {
            let column = expr_column(table, expr)?;
            let value = literal_value(pattern)?;
            let pattern = match value {
                Value::String(s) | Value::Decimal(s) => s,
                Value::Bytes(bytes) => String::from_utf8(bytes).ok()?,
                other => other.to_display_string(),
            };
            match crate::engine::expr::analyze_like_sargable_pattern(&pattern, *escape_char)? {
                crate::engine::expr::LikeSargablePattern::Literal(literal) => {
                    Some(ZonePredicateProgram::Compare {
                        column_id: column.column_id,
                        op: BinaryOp::Eq,
                        query: zone_like_scalar(column, &literal)?,
                    })
                }
                crate::engine::expr::LikeSargablePattern::Prefix(prefix) => {
                    let (lower, upper) = like_prefix_zone_bounds(column, &prefix)?;
                    Some(ZonePredicateProgram::LikePrefix {
                        column_id: column.column_id,
                        lower,
                        upper,
                    })
                }
            }
        }
        _ => None,
    }
}

impl Database {
    /// Keep the storage's loader latch until the physical record has landed.
    /// Otherwise another query could reopen the old ready root between cache
    /// retirement and redo and retain that stale root for future pruning.
    fn retire_replayed_zone_map(
        &self,
        relative: &str,
    ) -> Result<Option<std::sync::MutexGuard<'_, Option<(u64, u64)>>>, QueryError> {
        let Some((_, info)) = crate::storage::naming::parse_relative_object_path(relative) else {
            return Ok(None);
        };
        if info.ext != "zm" {
            return Ok(None);
        }
        let storage_id = info.table_id;
        let mut load = self.zone_map_load_locks
            [storage_id as usize % self.zone_map_load_locks.len()]
        .lock()
        .map_err(|_| ZoneMap::access_error("zone-map loader lock poisoned"))?;
        // Incoming physical redo can install a ready root even while local
        // pruning is OFF. The loader latch spans the complete replay.
        *load = None;
        let map = self
            .zone_maps
            .write()
            .map_err(|_| ZoneMap::access_error("zone-map cache lock poisoned"))?
            .remove(&storage_id);
        if let Some(map) = map {
            // Publish retirement before waiting: an in-progress candidate
            // builder observes it at its final check and returns fallback.
            map.retired.store(true, Ordering::Release);
            let _data = map
                .data_gate
                .write()
                .map_err(|_| ZoneMap::access_error("zone-map data lock poisoned"))?;
            // Never flush the old map. Incoming physical records own both
            // the durable bytes and the root's revision/publication state.
        }
        Ok(Some(load))
    }
}
impl Database {
    /// Summary invalidation is physical redo, appended before the caller can
    /// commit its heap mutation. This path also runs while pruning is OFF.
    fn invalidate_unavailable_zone_map(
        &self,
        txn_id: u64,
        storage_id: u64,
        table: &TableSchema,
        empty: bool,
    ) -> Result<(), QueryError> {
        if empty || !table.zone_map_enabled() || is_temp_table_id(storage_id) {
            return Ok(());
        }
        self.queue_zone_map_invalidation(txn_id, storage_id, false)
    }

    fn queue_zone_map_invalidation(
        &self,
        _txn_id: u64,
        storage_id: u64,
        empty: bool,
    ) -> Result<(), QueryError> {
        if empty || is_temp_table_id(storage_id) {
            return Ok(());
        }
        let load_latch =
            &self.zone_map_load_locks[storage_id as usize % self.zone_map_load_locks.len()];
        // A failed speculative loader must not prevent durable revocation.
        // A poisoned shard's cached result is never accepted.
        let mut load = load_latch.lock().unwrap_or_else(|err| err.into_inner());
        // The caller holds zone_map_gate, so an OFF interval cannot end or
        // admit a local rebuild during this check. Remember only successful
        // revocations, with bounded storage and no per-row path/stat work.
        let disabled = !self.zone_map_enabled();
        let epoch = self.zone_map_retirement_epoch.load(Ordering::Acquire);
        if disabled && !load_latch.is_poisoned() && *load == Some((storage_id, epoch)) {
            return Ok(());
        }
        let Some(table) = self.table_by_id(storage_id) else {
            return Ok(());
        };
        let cached = self
            .zone_maps
            .read()
            .ok()
            .and_then(|maps| maps.get(&storage_id).cloned());
        let map = if let Some(map) = cached {
            map
        } else {
            if !self
                .zone_map_file_path(storage_id)
                .try_exists()
                .map_err(io_err)?
            {
                if disabled {
                    *load = Some((storage_id, epoch));
                }
                return Ok(());
            }
            self.open_paged_zone_map(storage_id, &table, false)?
        };
        let _data = map.data_gate.write().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "zone map gate poisoned",
            )
        })?;
        map.trusted_base.store(false, Ordering::Release);
        map.invalidation_epoch.fetch_add(1, Ordering::AcqRel);
        map.store.invalidate()?;
        map.mutation_epoch
            .fetch_max(map.store.revision(), Ordering::AcqRel);
        if disabled {
            *load = Some((storage_id, epoch));
        }
        load_latch.clear_poison();
        Ok(())
    }

    pub(crate) fn zone_map_backup_capture_guard(
        &self,
    ) -> Result<parking_lot::RwLockWriteGuard<'_, ()>, QueryError> {
        let guard = self.zone_map_gate.write();
        // A rebuild can finish after the initial checkpoint but before this
        // gate. Ordinary writeback can persist its ready root before a leaf;
        // flush the frozen image before any backup copies those bytes.
        // This is a data flush, not a recursive checkpoint/gate acquisition.
        self.flush_dirty_data()?;
        Ok(guard)
    }

    /// Distribute a compact worker-side aggregate over the zones the
    /// freshly-inserted rows actually landed in. Used by the parallel LOAD
    /// DATA path: the worker maintains a single [`ZoneMapWorkerBatch`]
    /// while it decodes/encodes the batch (no per-row projection
    /// allocation), and this method hands the widened envelope to every
    /// touched zone. Per-zone precision within a batch is intentionally
    /// coarse — `ANALYZE TABLE` (which the bulk-load pipeline runs) then
    /// rebuilds tight per-zone summaries by scanning the heap.
    pub(crate) fn apply_worker_batch(
        &self,
        txn_id: u64,
        storage_id: u64,
        row_ids: &[RowId],
        worker_batch: &ZoneMapWorkerBatch,
    ) -> Result<(), QueryError> {
        let _zone_operation = self.zone_map_gate.read_recursive();
        if !self.zone_map_enabled() {
            return self.queue_zone_map_invalidation(txn_id, storage_id, row_ids.is_empty());
        }
        if row_ids.is_empty() {
            return Ok(());
        }
        if !worker_batch.is_active() {
            return self.queue_zone_map_invalidation(txn_id, storage_id, false);
        }
        let Some(table) = self.table_by_id(storage_id) else {
            return Ok(());
        };
        let Some(map) = self.zone_map_for(storage_id, &table) else {
            return self.invalidate_unavailable_zone_map(
                txn_id,
                storage_id,
                &table,
                row_ids.is_empty(),
            );
        };
        let result = (|| -> Result<(), QueryError> {
            let zone_pages = u64::from(map.zone_pages);
            let mut counts = BTreeMap::<u64, u64>::new();
            let mut counts_lease = ZoneScratchLease::default();
            let flush = |counts: &mut BTreeMap<u64, u64>,
                         counts_lease: &mut ZoneScratchLease|
             -> Result<(), QueryError> {
                let mut batch = ChargedZoneBatch::new(&map);
                for (&id, &count) in counts.iter() {
                    batch.ensure_zone(id)?;
                    batch.zones.get_mut(&id).unwrap().row_count_estimate = count;
                    for entry in &worker_batch.entries {
                        if entry.state == ZoneMapState::Unknown
                            || (entry.state == ZoneMapState::Empty
                                && entry.min.is_none()
                                && entry.max.is_none()
                                && !entry.has_null)
                        {
                            batch.unknown_column(id, entry.column_id)?;
                            continue;
                        }
                        let scalar_bytes = [entry.min.as_ref(), entry.max.as_ref()]
                            .into_iter()
                            .flatten()
                            .map(|value| match value {
                                WorkerScalar::Bytes { prefix, .. } => prefix.capacity(),
                                WorkerScalar::TextWeight { weight, .. } => weight.capacity(),
                                _ => 0,
                            })
                            .sum::<usize>();
                        let peak = scalar_bytes
                            .saturating_mul(4)
                            .saturating_add(1024)
                            .saturating_add(
                                entry
                                    .membership
                                    .as_ref()
                                    .map_or(0, ZoneMembership::capacity_bytes),
                            );
                        let ticket = batch.begin_column(id, entry.column_id, peak)?;
                        let summary = batch
                            .zones
                            .get_mut(&id)
                            .unwrap()
                            .columns
                            .entry(entry.column_id)
                            .or_default();
                        if entry.has_null {
                            summary.merge_null();
                        }
                        if let Some(membership) = &entry.membership {
                            summary.merge_membership(membership);
                        }
                        if let Some(min) = &entry.min {
                            summary.merge_value(min.clone().into_zone_scalar());
                        }
                        if let Some(max) = &entry.max {
                            summary.merge_value(max.clone().into_zone_scalar());
                        }
                        summary.truncated |= entry.truncated;
                        batch.finish_column(id, entry.column_id, ticket);
                    }
                }
                batch.flush(&map)?;
                counts.clear();
                counts_lease.shrink_to(0);
                Ok(())
            };
            for row_id in row_ids {
                let id = u64::from(row_id.page_id) / zone_pages;
                if counts.len() == ZONE_MAP_DML_MAX_ZONES && !counts.contains_key(&id) {
                    flush(&mut counts, &mut counts_lease)?;
                }
                if !counts.contains_key(&id) {
                    counts_lease.reserve(
                        zone_tree_bytes::<u64, u64>(counts.len() + 1)
                            .saturating_sub(zone_tree_bytes::<u64, u64>(counts.len())),
                    )?;
                }
                let count = counts.entry(id).or_default();
                *count = count.saturating_add(1);
            }
            flush(&mut counts, &mut counts_lease)
        })();
        self.finish_zone_map_dml_work(txn_id, storage_id, &map, result)
    }

    pub fn zone_map_enabled(&self) -> bool {
        self.zone_map_enabled.load(Ordering::Acquire)
    }

    /// Cover a complete unit of derived work, including publication. OFF
    /// bypasses optional metadata work; DML still queues validity records.
    pub(crate) fn zone_map_operation(&self) -> Option<parking_lot::RwLockReadGuard<'_, ()>> {
        if !self.zone_map_enabled() {
            return None;
        }
        let guard = self.zone_map_gate.read_recursive();
        self.zone_map_enabled().then_some(guard)
    }

    fn initialize_zone_map_enabled(&self) -> Result<(), QueryError> {
        let raw = self
            .persisted_variables()
            .get("mydb_zone_map_enabled")
            .map(|entry| entry.value)
            .or_else(|| crate::startup_config::var("MYDB_ZONE_MAP_ENABLED").ok());
        let enabled = match raw.as_deref().map(str::trim) {
            None => true,
            Some(value)
                if ["1", "on", "true", "yes"]
                    .iter()
                    .any(|v| value.eq_ignore_ascii_case(v)) =>
            {
                true
            }
            Some(value)
                if ["0", "off", "false", "no"]
                    .iter()
                    .any(|v| value.eq_ignore_ascii_case(v)) =>
            {
                false
            }
            Some(_) => {
                return Err(error(
                    crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
                    "MYDB_ZONE_MAP_ENABLED / mydb_zone_map_enabled requires a boolean value",
                ))
            }
        };
        self.set_zone_map_enabled(enabled)
    }

    /// Disable optional pruning without discarding unchanged summaries.
    /// DML while disabled still durably revokes the affected roots.
    pub fn set_zone_map_enabled(&self, enabled: bool) -> Result<(), QueryError> {
        let _gate = self.zone_map_gate.write();
        self.zone_map_retirement_epoch
            .fetch_add(1, Ordering::AcqRel);
        self.zone_map_enabled.store(enabled, Ordering::Release);
        if let Some(cache) = crate::engine::cache::global_plan_cache() {
            cache.clear();
        }
        Ok(())
    }

    pub fn zone_map_maintenance(&self) -> bool {
        ZONE_MAP_MAINTENANCE.load(Ordering::Relaxed)
    }
    pub fn set_zone_map_maintenance(&self, enabled: bool) {
        ZONE_MAP_MAINTENANCE.store(enabled, Ordering::Relaxed);
    }
    #[cfg(test)]
    pub(crate) fn set_zone_map_maintenance_quiet_ms_for_tests(&self, value: u64) {
        ZONE_MAP_TEST_MAINTENANCE_QUIET_MS.store(value, Ordering::Relaxed);
    }
    #[cfg(test)]
    pub(crate) fn set_zone_map_rebuild_publish_barrier_for_tests(
        &self,
        barrier: Option<Arc<std::sync::Barrier>>,
    ) {
        if let Ok(mut slot) = ZONE_MAP_REBUILD_PUBLISH_BARRIER.lock() {
            *slot = barrier;
        }
    }
    pub fn zone_map_maintenance_quiet_ms(&self) -> u64 {
        zone_map_maintenance_quiet_ms()
    }
    pub fn set_zone_map_maintenance_quiet_ms(&self, value: u64) {
        ZONE_MAP_QUIET_MS.store(value, Ordering::Relaxed);
        #[cfg(test)]
        ZONE_MAP_TEST_MAINTENANCE_QUIET_MS.store(value, Ordering::Relaxed);
    }
    pub fn zone_map_maintenance_retry_ms(&self) -> u64 {
        ZONE_MAP_RETRY_MS.load(Ordering::Relaxed)
    }
    pub fn set_zone_map_maintenance_retry_ms(&self, value: u64) {
        ZONE_MAP_RETRY_MS.store(value, Ordering::Relaxed);
    }
    pub fn zone_map_rebuild_ratio(&self) -> f64 {
        f64::from_bits(ZONE_MAP_REBUILD_RATIO_BITS.load(Ordering::Relaxed))
    }
    pub fn set_zone_map_rebuild_ratio(&self, ratio: f64) -> bool {
        if !ratio.is_finite() || !(0.0..=1.0).contains(&ratio) {
            return false;
        }
        ZONE_MAP_REBUILD_RATIO_BITS.store(ratio.to_bits(), Ordering::Relaxed);
        true
    }
    pub(crate) fn forget_zone_map(&self, storage_id: u64) -> Result<(), QueryError> {
        // DDL can rename/exchange physical summaries. Advance before retiring
        // the handle so concurrent checks cannot republish a current token.
        self.zone_map_retirement_epoch
            .fetch_add(1, Ordering::AcqRel);
        let map = self
            .zone_maps
            .read()
            .ok()
            .and_then(|maps| maps.get(&storage_id).cloned());
        if let Some(map) = map {
            let _data = map.data_gate.write().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "zone map gate poisoned",
                )
            })?;
            // Removing a dirty handle must not expose its older disk image to
            // an immediate reload or drop the only checkpoint rec_lsn owner.
            map.store.flush()?;
            map.retired.store(true, Ordering::Release);
            map.store.discard()?;
            if let Ok(mut maps) = self.zone_maps.write() {
                if maps
                    .get(&storage_id)
                    .is_some_and(|current| Arc::ptr_eq(current, &map))
                {
                    maps.remove(&storage_id);
                }
            }
        }
        Ok(())
    }

    pub(crate) fn flush_for_rename_zone_map(&self, storage_id: u64) -> Result<(), QueryError> {
        self.forget_zone_map(storage_id)
    }

    fn open_paged_zone_map(
        &self,
        storage_id: u64,
        table: &TableSchema,
        known_empty: bool,
    ) -> Result<Arc<ZoneMap>, QueryError> {
        use zone_map_page::{mutable::Store, PageFile};
        #[cfg(test)]
        if zone_map_failpoint(7).is_err() {
            return Err(error(
                crate::error::catalog::ER_OUTOFMEMORY,
                "zone-map handle admission rejected",
            ));
        }
        let path = self.zone_map_file_path(storage_id);
        let fingerprint = zone_schema_fingerprint(table);
        let file = || {
            PageFile::new(
                path.clone(),
                storage_id,
                self.storage_generation(storage_id),
                Arc::clone(&self.buffer_pool),
                Arc::clone(&self.log),
                self.config.durability,
            )
        };
        let store = match Store::open_existing(file()?, fingerprint, table.zone_pages()) {
            Ok(store) => store,
            Err(err)
                if self.is_snapshot_read_only()
                    || self.physical_replica_mode()
                    || err.is(crate::error::catalog::ER_OUTOFMEMORY) =>
            {
                return Err(err)
            }
            Err(_) => {
                // Derived metadata has no authority over rows. Recreate an
                // explicitly Unknown baseline using WAL, never infer that a
                // populated heap is empty from a missing summary file.
                let store = Store::recover_unknown(file()?, fingerprint, table.zone_pages())?;
                if known_empty {
                    store.set_missing_is_empty(true)?;
                }
                store
            }
        };
        let trusted = store.trusted()
            && store
                .root_snapshot()
                .is_some_and(|root| root.missing_is_empty || root.records != 0);
        let missing_empty = store.missing_is_empty();
        let map = Arc::new(ZoneMap::new(
            storage_id,
            table.zone_pages(),
            fingerprint,
            Arc::new(store),
            missing_empty,
        ));
        map.trusted_base.store(trusted, Ordering::Release);
        map.file_generation
            .store(self.storage_generation(storage_id), Ordering::Release);
        *map.path.lock().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "zone map path lock poisoned",
            )
        })? = Some(path);
        map.bind_table_schema(table);
        Ok(map)
    }

    fn zone_map_for(&self, storage_id: u64, table: &TableSchema) -> Option<Arc<ZoneMap>> {
        let _operation = self.zone_map_operation()?;
        let descriptor = if table.columns.is_empty() {
            Some(self.table_by_id(table.id)?)
        } else {
            None
        };
        let table = descriptor.as_deref().unwrap_or(table);
        if table.columns.is_empty() || !table.zone_map_enabled() || is_temp_table_id(storage_id) {
            return None;
        }
        if let Some(map) = self.zone_maps.read().ok()?.get(&storage_id).cloned() {
            if !map.retired.load(Ordering::Acquire)
                && map.zone_pages == table.zone_pages()
                && map.matches_bound_table_schema(table)
            {
                ZONE_MAP_CACHE_HITS.fetch_add(1, Ordering::Relaxed);
                map.touch();
                if !map.cache_admitted.load(Ordering::Acquire) {
                    map.sync_derived_charge();
                }
                return Some(map);
            }
        }
        let _load = self.zone_map_load_locks[storage_id as usize % self.zone_map_load_locks.len()]
            .lock()
            .ok()?;
        if let Some(map) = self.zone_maps.read().ok()?.get(&storage_id).cloned() {
            if !map.retired.load(Ordering::Acquire)
                && map.zone_pages == table.zone_pages()
                && map.schema_fingerprint == zone_schema_fingerprint(table)
            {
                map.bind_table_schema(table);
                map.touch();
                if !map.cache_admitted.load(Ordering::Acquire) {
                    map.sync_derived_charge();
                }
                return Some(map);
            }
        }
        self.forget_zone_map(storage_id).ok()?;
        ZONE_MAP_CACHE_MISSES.fetch_add(1, Ordering::Relaxed);
        let empty = self
            .with_heap_read(storage_id, |heap| Ok(heap.page_count_relaxed() == 0))
            .unwrap_or(false);
        let map = self.open_paged_zone_map(storage_id, table, empty).ok()?;
        self.zone_maps
            .write()
            .ok()?
            .insert(storage_id, Arc::clone(&map));
        Some(map)
    }

    /// Only optional lightweight handles are reclaimed here. Their pages,
    /// including dirty pages, remain owned and charged by the shared pool.
    pub(crate) fn reclaim_zone_maps_for_memory_target(&self, target: usize) {
        if crate::engine::executor::mem_budget::controlled_memory_snapshot().0 <= target {
            return;
        }
        if let Ok(mut maps) = self.zone_maps.try_write() {
            maps.retain(|_, map| Arc::strong_count(map) != 1);
        }
    }

    pub(crate) fn invalidate_zone_maps_for_table(
        &self,
        table: &TableSchema,
    ) -> Result<(), QueryError> {
        let _operation = self.zone_map_gate.read_recursive();
        for id in table.storage_ids() {
            self.queue_zone_map_invalidation(0, id, false)?;
            self.forget_zone_map(id)?;
        }
        Ok(())
    }

    pub(crate) fn initialize_empty_zone_maps_for_table(
        &self,
        table: &TableSchema,
    ) -> Result<(), QueryError> {
        // Creating the initial root also publishes physical metadata WAL;
        // DDL's transaction floor does not replace the metadata checkpoint gate.
        let _operation = self.zone_map_gate.read_recursive();
        if !self.zone_map_enabled() || !table.zone_map_enabled() {
            return Ok(());
        }
        for id in table.storage_ids() {
            if is_temp_table_id(id) {
                continue;
            }
            self.forget_zone_map(id)?;
            let map = self.open_paged_zone_map(id, table, true)?;
            self.zone_maps
                .write()
                .map_err(|_| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "zone map cache poisoned",
                    )
                })?
                .insert(id, map);
        }
        Ok(())
    }

    fn finish_zone_map_dml_work(
        &self,
        txn_id: u64,
        storage_id: u64,
        map: &ZoneMap,
        result: Result<(), QueryError>,
    ) -> Result<(), QueryError> {
        if let Err(err) = result {
            map.record_write_error(&err);
            // The heap batch already exists. Discard optional scratch first,
            // then durably revoke the root; inability to revoke remains an
            // explicit DML error rather than exposing uncovered rows.
            self.queue_zone_map_invalidation(txn_id, storage_id, false)?;
        }
        Ok(())
    }

    fn flush_zone_map_update_batch(
        map: &ZoneMap,
        batch: &mut ChargedZoneBatch,
        neutral: &mut BTreeSet<u64>,
        lease: &mut ZoneScratchLease,
    ) -> Result<(), QueryError> {
        // Neutral zones already published their fences while checking their
        // proof. Only owned widening deltas remain to be published here.
        batch.flush(map)?;
        neutral.clear();
        lease.shrink_to(0);
        Ok(())
    }

    pub(crate) fn merge_zone_map_rows(
        &self,
        txn_id: u64,
        storage_id: u64,
        row_ids: &[RowId],
        rows: &[Vec<Value>],
    ) -> Result<(), QueryError> {
        let _zone_operation = self.zone_map_gate.read_recursive();
        if !self.zone_map_enabled() {
            return self.queue_zone_map_invalidation(txn_id, storage_id, row_ids.is_empty());
        }
        if row_ids.is_empty() {
            return Ok(());
        }
        let Some(table) = self.table_by_id(storage_id) else {
            return Ok(());
        };
        let Some(map) = self.zone_map_for(storage_id, &table) else {
            return self.invalidate_unavailable_zone_map(
                txn_id,
                storage_id,
                &table,
                row_ids.is_empty(),
            );
        };
        // Defer policy is based on the original input, not each bounded
        // zone group; batching must not accidentally enable expensive work.
        let defer_expensive = rows.len() >= ZONE_MAP_DEFER_EXPENSIVE_BATCH_ROWS;
        let defer_all = rows.len() >= ZONE_MAP_DEFER_ALL_BATCH_ROWS;
        let result = (|| -> Result<(), QueryError> {
            let mut columns_lease = ZoneScratchLease::default();
            let relevant_columns = if defer_all {
                Vec::new()
            } else {
                columns_lease.reserve(
                    table
                        .columns
                        .len()
                        .saturating_mul(2 * std::mem::size_of::<(usize, u32)>()),
                )?;
                zone_map_relevant_columns(&table)
            };
            let membership_columns = map.cached_membership_columns();
            let mut batch = ChargedZoneBatch::new(&map);
            let mut i = 0;
            while i < row_ids.len() {
                let id = row_ids[i].page_id as u64 / u64::from(map.zone_pages);
                let start = i;
                i += 1;
                while i < row_ids.len()
                    && row_ids[i].page_id as u64 / u64::from(map.zone_pages) == id
                {
                    i += 1;
                }
                batch.admit_zone(&map, id)?;
                let count = (i - start) as u64;
                let zone = batch.zones.get_mut(&id).unwrap();
                zone.row_count_estimate = zone.row_count_estimate.saturating_add(count);
                if defer_all {
                    zone.state = Some(ZoneMapState::Unknown);
                    zone.stale_mutations = zone.stale_mutations.saturating_add(count);
                    continue;
                }
                if defer_expensive {
                    zone.stale_mutations = zone.stale_mutations.saturating_add(count);
                }
                for &(index, column_id) in &relevant_columns {
                    let column = &table.columns[index];
                    if defer_expensive
                        && matches!(
                            column.data_type,
                            DataTypeKind::Char { .. }
                                | DataTypeKind::VarChar { .. }
                                | DataTypeKind::Enum { .. }
                                | DataTypeKind::Set { .. }
                        )
                    {
                        batch.unknown_column(id, column_id)?;
                    } else {
                        batch.aggregate(
                            id,
                            column_id,
                            column,
                            &rows[start..i],
                            index,
                            cached_membership_layout(&membership_columns, column_id),
                        )?;
                    }
                }
            }
            batch.flush(&map)
        })();
        self.finish_zone_map_dml_work(txn_id, storage_id, &map, result)
    }

    /// UPDATE keeps the old envelope and merges only new values for columns
    /// that actually changed. It does not increase the live-row estimate;
    /// instead it records one stale mutation so maintenance can eventually
    /// reclaim obsolete historical extremes after old snapshots drain.
    pub(crate) fn merge_zone_map_updated_rows<R: AsRef<[Value]>>(
        &self,
        txn_id: u64,
        storage_id: u64,
        row_ids: &[RowId],
        rows: &[R],
        changed_columns: Option<&[usize]>,
    ) -> Result<(), QueryError> {
        let _zone_operation = self.zone_map_gate.read_recursive();
        if !self.zone_map_enabled() {
            return self.queue_zone_map_invalidation(txn_id, storage_id, row_ids.is_empty());
        }
        if row_ids.is_empty() {
            return Ok(());
        }
        let Some(table) = self.table_by_id(storage_id) else {
            return Ok(());
        };
        let Some(map) = self.zone_map_for(storage_id, &table) else {
            return self.invalidate_unavailable_zone_map(
                txn_id,
                storage_id,
                &table,
                row_ids.is_empty(),
            );
        };
        let result = (|| -> Result<(), QueryError> {
            let zone_pages = map.zone_pages as u64;
            let membership_columns = map.cached_membership_columns();
            let rebuild_ratio = self.zone_map_rebuild_ratio();
            let selected_count =
                changed_columns.map_or(table.columns.len(), |columns| columns.len());
            let mut selected_lease = ZoneScratchLease::default();
            selected_lease.reserve(selected_count.saturating_mul(64))?;
            let touch_columns: smallvec::SmallVec<[(usize, u32); 8]> =
                if let Some(cols) = changed_columns {
                    cols.iter()
                        .filter_map(|&idx| {
                            table.columns.get(idx).and_then(|column| {
                                if matches!(
                                    column.data_type,
                                    DataTypeKind::Text { .. }
                                        | DataTypeKind::Blob { .. }
                                        | DataTypeKind::Json
                                        | DataTypeKind::Geometry { .. }
                                        | DataTypeKind::Vector { .. }
                                ) {
                                    None
                                } else {
                                    Some((idx, column.column_id))
                                }
                            })
                        })
                        .collect()
                } else {
                    zone_map_relevant_columns(&table).into_iter().collect()
                };
            if touch_columns.is_empty() {
                return Ok(());
            }
            let column_ids = touch_columns
                .iter()
                .map(|(_, id)| *id)
                .collect::<smallvec::SmallVec<[u32; 8]>>();
            let mut batch = ChargedZoneBatch::new(&map);
            let mut neutral = BTreeSet::new();
            let mut neutral_lease = ZoneScratchLease::default();
            let mut i = 0;
            while i < row_ids.len() {
                let id = row_ids[i].page_id as u64 / zone_pages;
                let start = i;
                i += 1;
                while i < row_ids.len() && row_ids[i].page_id as u64 / zone_pages == id {
                    i += 1;
                }
                if batch.zones.len() + neutral.len() >= ZONE_MAP_DML_MAX_ZONES
                    && !batch.zones.contains_key(&id)
                    && !neutral.contains(&id)
                {
                    Self::flush_zone_map_update_batch(
                        &map,
                        &mut batch,
                        &mut neutral,
                        &mut neutral_lease,
                    )?;
                }
                // Every heap update preceded this pass. A neutral zone has
                // already published its fence under the write gate; repeated
                // runs need neither another decode nor another redo record.
                if neutral.contains(&id) {
                    continue;
                }
                let count = (i - start) as u64;
                let (invalidate, already_unknown) = {
                    let _zone = map.merge_latches[id as usize % ZONE_MAP_MERGE_LATCHES]
                        .lock()
                        .map_err(|_| ZoneMap::access_error("zone-map merge lock poisoned"))?;
                    let _gate = map
                        .data_gate
                        .read()
                        .map_err(|_| ZoneMap::access_error("zone map gate poisoned"))?;
                    if map.cached_unknown_columns(id, &column_ids) {
                        map.note_summary_neutral_mutations_locked(&[id])?;
                        (false, true)
                    } else {
                        let status = map.zone_update_status(id, &column_ids)?;
                        if status.is_some_and(|(_, _, unknown)| unknown) {
                            map.note_summary_neutral_mutations_locked(&[id])?;
                            map.remember_unknown_columns(id, &column_ids);
                        }
                        status
                            .map(|(row_count, stale, unknown)| {
                                let threshold =
                                    64.max((row_count as f64 * rebuild_ratio).ceil() as u64);
                                (stale.saturating_add(count) >= threshold, unknown)
                            })
                            .unwrap_or((false, false))
                    }
                };
                if already_unknown {
                    if !neutral.contains(&id) {
                        neutral_lease.reserve(
                            zone_tree_bytes::<u64, ()>(neutral.len() + 1)
                                .saturating_sub(zone_tree_bytes::<u64, ()>(neutral.len())),
                        )?;
                        neutral.insert(id);
                        #[cfg(test)]
                        ZONE_MAP_TEST_DML_MAX_ZONES.fetch_max(
                            (batch.zones.len()
                                + neutral
                                    .iter()
                                    .filter(|id| !batch.zones.contains_key(id))
                                    .count()) as u64,
                            Ordering::Relaxed,
                        );
                    }
                    continue;
                }
                batch.ensure_zone(id)?;
                let zone = batch.zones.get_mut(&id).unwrap();
                zone.stale_mutations = zone.stale_mutations.saturating_add(count);
                for &(index, column_id) in &touch_columns {
                    if invalidate {
                        batch.unknown_column(id, column_id)?;
                    } else {
                        batch.aggregate(
                            id,
                            column_id,
                            &table.columns[index],
                            &rows[start..i],
                            index,
                            cached_membership_layout(&membership_columns, column_id),
                        )?;
                    }
                }
            }
            Self::flush_zone_map_update_batch(&map, &mut batch, &mut neutral, &mut neutral_lease)
        })();
        self.finish_zone_map_dml_work(txn_id, storage_id, &map, result)
    }

    pub(crate) fn note_zone_map_deletes(
        &self,
        txn_id: u64,
        storage_id: u64,
        row_ids: &[RowId],
    ) -> Result<(), QueryError> {
        let _zone_operation = self.zone_map_gate.read_recursive();
        if !self.zone_map_enabled() {
            return self.queue_zone_map_invalidation(txn_id, storage_id, row_ids.is_empty());
        }
        let Some(table) = self.table_by_id(storage_id) else {
            return Ok(());
        };
        let Some(map) = self.zone_map_for(storage_id, &table) else {
            return self.invalidate_unavailable_zone_map(
                txn_id,
                storage_id,
                &table,
                row_ids.is_empty(),
            );
        };
        let result = (|| -> Result<(), QueryError> {
            let mut batch = ChargedZoneBatch::new(&map);
            for row_id in row_ids {
                let id = u64::from(row_id.page_id) / u64::from(map.zone_pages);
                batch.admit_zone(&map, id)?;
                let zone = batch.zones.get_mut(&id).unwrap();
                zone.stale_mutations = zone.stale_mutations.saturating_add(1);
            }
            batch.flush(&map)
        })();
        self.finish_zone_map_dml_work(txn_id, storage_id, &map, result)
    }

    fn replay_zone_map_wal(
        &self,
        records: &(impl crate::wal::WalRecordSource + ?Sized),
    ) -> Result<(), QueryError> {
        for index in 0..records.len() {
            if let Some(bytes) = records.zone_delta(index)? {
                self.apply_zone_map_wal(&bytes)?;
            }
        }
        Ok(())
    }

    pub(crate) fn apply_zone_map_wal(&self, _bytes: &[u8]) -> Result<(), QueryError> {
        Err(error(crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "legacy logical zone-map WAL is not supported by paged summaries; use a new data directory"))
    }

    /// Build adjacent heap page ranges that cannot be disproven by the zone
    /// summaries. `None` means the predicate/table is not eligible and the
    /// caller should use its ordinary sequential scan unchanged.
    pub fn zone_map_candidate_ranges(
        &self,
        table: &TableSchema,
        filter: &Expr,
    ) -> Option<ZoneMapRanges> {
        self.zone_map_candidate_ranges_inner(table, filter, false)
            .ranges()
    }

    /// Execute an already-selected ZoneMapScan. Planner/session switch checks
    /// happened before the physical operator was created and must not be
    /// repeated on a worker thread with a different TLS context.
    pub fn zone_map_candidate_ranges_for_plan(
        &self,
        table: &TableSchema,
        filter: &Expr,
    ) -> Option<ZoneMapRanges> {
        self.zone_map_candidate_ranges_inner(table, filter, true)
            .ranges()
    }

    /// Candidate page ranges for one physical heap of a logical table.
    /// Partitioned execution calls this once per selected `storage_id`.
    pub fn zone_map_candidate_ranges_for_storage_for_plan(
        &self,
        table: &TableSchema,
        storage_id: u64,
        filter: &Expr,
    ) -> Option<ZoneMapRanges> {
        self.zone_map_candidate_ranges_for_storage_inner(table, storage_id, filter, true)
            .ranges()
    }

    fn zone_map_candidate_ranges_inner(
        &self,
        table: &TableSchema,
        filter: &Expr,
        force: bool,
    ) -> ZoneMapCandidateResult {
        if table.is_partitioned() {
            return ZoneMapCandidateResult::Fallback(ZoneMapFallbackReason::Unsupported);
        }
        self.zone_map_candidate_ranges_for_storage_inner(table, table.id, filter, force)
    }

    fn zone_map_candidate_ranges_for_storage_inner(
        &self,
        table: &TableSchema,
        storage_id: u64,
        filter: &Expr,
        force: bool,
    ) -> ZoneMapCandidateResult {
        let Some(_zone_operation) = self.zone_map_operation() else {
            return ZoneMapCandidateResult::Fallback(ZoneMapFallbackReason::Unsupported);
        };
        let runtime_started = force.then(std::time::Instant::now);
        if (!force && !crate::storage::schema::zone_map_optimizer_enabled())
            || !table.zone_map_enabled()
            || !table.storage_ids().contains(&storage_id)
        {
            return ZoneMapCandidateResult::Fallback(ZoneMapFallbackReason::Unsupported);
        }
        let Some(program) = compile_zone_predicate(table, filter) else {
            return ZoneMapCandidateResult::Fallback(ZoneMapFallbackReason::Unsupported);
        };
        if matches!(program, ZonePredicateProgram::Never) {
            return ZoneMapCandidateResult::Ranges(ZoneMapRanges::default());
        }
        let Some(map) = self.zone_map_for(storage_id, table) else {
            return ZoneMapCandidateResult::Fallback(ZoneMapFallbackReason::Untrusted);
        };
        let Ok(_data) = map.data_gate.read() else {
            return ZoneMapCandidateResult::Fallback(ZoneMapFallbackReason::Corrupt);
        };
        if map.retired.load(Ordering::Acquire) || !map.trusted_base.load(Ordering::Acquire) {
            return ZoneMapCandidateResult::Fallback(if map.charged_memory_bytes() == 0 {
                ZoneMapFallbackReason::MemoryPressure
            } else {
                ZoneMapFallbackReason::Untrusted
            });
        }
        let Ok(page_count) = self.heap_page_count_for_storage(storage_id) else {
            return ZoneMapCandidateResult::Fallback(ZoneMapFallbackReason::InvalidRange);
        };
        if page_count == 0 {
            if let Some(started) = runtime_started {
                ZONE_MAP_RUNTIME_CANDIDATE_NS.fetch_add(
                    started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                    Ordering::Relaxed,
                );
            }
            ZONE_MAP_CANDIDATE_GENERATIONS.fetch_add(1, Ordering::Relaxed);
            return ZoneMapCandidateResult::Ranges(ZoneMapRanges::default());
        }
        if !map.trusted_base.load(Ordering::Acquire) || !map.cache_admitted.load(Ordering::Acquire)
        {
            return ZoneMapCandidateResult::Fallback(ZoneMapFallbackReason::MemoryPressure);
        }
        use crate::engine::executor::mem_budget::{
            ControlledMemoryCategory, ControlledMemoryCharge,
        };
        // The typed reader charges bounded decode scratch and probes
        // membership directly in a pinned page. Charge only output growth.
        let Some(mut scratch) =
            ControlledMemoryCharge::try_new_optional(ControlledMemoryCategory::StorageScratch, 0)
        else {
            return ZoneMapCandidateResult::Fallback(ZoneMapFallbackReason::MemoryPressure);
        };
        let total_zones = page_count.div_ceil(map.zone_pages as u64);
        ZONE_MAP_CACHE_HITS.fetch_add(1, Ordering::Relaxed);
        // SQL RR guards remain registered for the transaction's lifetime;
        // statement and historical guards share this atomic horizon sample.
        // Using the oldest registered view is conservative for newer readers.
        let (oldest_reader, visible_scn) = self.snapshot_visibility_horizon();
        let safe_read_scn = oldest_reader.unwrap_or(visible_scn).min(visible_scn);
        let mut ranges = Vec::new();
        let read_result =
            map.visit_matches(
                0..total_zones,
                &program,
                safe_read_scn,
                |zone_id, may_match, _| {
                    if zone_id % 64 == 0 {
                        crate::engine::cancel::check_cancelled()?;
                    }
                    if !may_match {
                        return Ok(());
                    }
                    let lo = zone_id * map.zone_pages as u64;
                    let hi = ((zone_id + 1) * map.zone_pages as u64).min(page_count);
                    if let Some((_, prior_hi)) = ranges.last_mut() {
                        if *prior_hi == lo {
                            *prior_hi = hi;
                            return Ok(());
                        }
                    }
                    if ranges.len() == ranges.capacity() {
                        let additional = ranges.capacity().max(4);
                        if !scratch.try_grow_optional(
                            additional.saturating_mul(std::mem::size_of::<(u64, u64)>()),
                        ) || ranges.try_reserve_exact(additional).is_err()
                        {
                            return Err(ZoneScratchLease::denied());
                        }
                    }
                    ranges.push((lo, hi));
                    Ok(())
                },
            );
        if let Err(err) = read_result {
            return ZoneMapCandidateResult::Fallback(
                if err.code() == crate::error::catalog::ER_OUTOFMEMORY.code() {
                    ZoneMapFallbackReason::MemoryPressure
                } else {
                    ZoneMapFallbackReason::Corrupt
                },
            );
        }
        if map.retired.load(Ordering::Acquire) {
            return ZoneMapCandidateResult::Fallback(ZoneMapFallbackReason::Untrusted);
        }
        let surviving_zones = ranges
            .iter()
            .map(|(lo, hi)| {
                if hi <= lo {
                    0
                } else {
                    (hi - 1) / map.zone_pages as u64 - lo / map.zone_pages as u64 + 1
                }
            })
            .sum::<u64>();
        ZONE_MAP_ZONES_SKIPPED.fetch_add(
            total_zones.saturating_sub(surviving_zones),
            Ordering::Relaxed,
        );
        if let Some(started) = runtime_started {
            let scheduled_pages = ranges
                .iter()
                .map(|(lo, hi)| hi.saturating_sub(*lo))
                .sum::<u64>();
            ZONE_MAP_RUNTIME_CANDIDATE_NS.fetch_add(
                started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                Ordering::Relaxed,
            );
            ZONE_MAP_RUNTIME_TASKS.fetch_add(ranges.len() as u64, Ordering::Relaxed);
            ZONE_MAP_RUNTIME_PAGES.fetch_add(scheduled_pages, Ordering::Relaxed);
            ZONE_MAP_RUNTIME_PAGES_SKIPPED.fetch_add(
                page_count.saturating_sub(scheduled_pages),
                Ordering::Relaxed,
            );
        }
        ZONE_MAP_CANDIDATE_GENERATIONS.fetch_add(1, Ordering::Relaxed);
        ZoneMapCandidateResult::Ranges(ZoneMapRanges {
            ranges: smallvec::SmallVec::from_vec(ranges),
            lease: ZoneScratchLease {
                charge: Some(scratch),
            },
        })
    }

    pub fn zone_map_cost_estimate_forced(
        &self,
        table: &TableSchema,
        filter: &Expr,
    ) -> Option<(u64, usize)> {
        self.zone_map_cost_details_forced(table, filter)
            .map(|estimate| (estimate.surviving_pages, estimate.ranges))
    }

    pub(crate) fn zone_map_cost_details(
        &self,
        table: &TableSchema,
        filter: &Expr,
    ) -> Option<ZoneMapCostEstimate> {
        self.zone_map_cost_estimate_inner(table, filter, false)
    }

    pub(crate) fn zone_map_cost_details_forced(
        &self,
        table: &TableSchema,
        filter: &Expr,
    ) -> Option<ZoneMapCostEstimate> {
        self.zone_map_cost_estimate_inner(table, filter, true)
    }

    /// Return the unavoidable sampling work for an ordinary Zone Map cost
    /// estimate without evaluating a predicate against any zone scalar.
    /// Complexity is O(number of storage objects), independent of table size.
    /// `None` also lets the planner reject unsupported predicates or missing /
    /// untrusted metadata without entering the full estimator.
    pub(crate) fn zone_map_cost_lower_bound(
        &self,
        table: &TableSchema,
        filter: &Expr,
    ) -> Option<ZoneMapCostLowerBound> {
        let Some(_zone_operation) = self.zone_map_operation() else {
            return None;
        };
        if !crate::storage::schema::zone_map_optimizer_enabled() || !table.zone_map_enabled() {
            return None;
        }
        compile_zone_predicate(table, filter)?;
        let mut zones_examined = 0u64;
        let mut min_surviving_pages = 0u64;
        let mut total_pages = 0u64;
        let mut saw_trusted_map = false;
        for storage_id in table.storage_ids() {
            let page_count = self.heap_page_count_for_storage(storage_id).ok()?;
            total_pages = total_pages.saturating_add(page_count);
            let map = self.zone_map_for(storage_id, table)?;
            if !map.trusted_base.load(Ordering::Acquire) {
                min_surviving_pages = min_surviving_pages.saturating_add(page_count);
                continue;
            }
            saw_trusted_map = true;
            let zone_pages = map.zone_pages as u64;
            zones_examined =
                zones_examined.saturating_add(zone_map_sample_geometry(page_count, zone_pages).2);
            min_surviving_pages = min_surviving_pages
                .saturating_add(zone_map_min_sampled_surviving_pages(page_count, zone_pages));
        }
        saw_trusted_map.then_some(ZoneMapCostLowerBound {
            zones_examined,
            min_surviving_pages,
            total_pages,
        })
    }

    fn zone_map_cost_estimate_inner(
        &self,
        table: &TableSchema,
        filter: &Expr,
        force: bool,
    ) -> Option<ZoneMapCostEstimate> {
        let Some(_zone_operation) = self.zone_map_operation() else {
            return None;
        };
        #[cfg(test)]
        ZONE_MAP_COST_ESTIMATE_CALLS.with(|calls| calls.set(calls.get().saturating_add(1)));
        if (!force && !crate::storage::schema::zone_map_optimizer_enabled())
            || !table.zone_map_enabled()
        {
            return None;
        }
        let program = compile_zone_predicate(table, filter)?;
        let mut total_pages = 0u64;
        let mut surviving_pages = 0u64;
        let mut total_ranges = 0usize;
        let mut zones_examined = 0u64;
        let mut metadata_read_bytes = 0u64;
        let mut metadata_cache_misses = 0u64;
        let mut storage_objects = 0u64;
        let mut saw_trusted_map = false;
        let mut exact = true;
        for storage_id in table.storage_ids() {
            let page_count = self.heap_page_count_for_storage(storage_id).ok()?;
            total_pages = total_pages.saturating_add(page_count);
            let Some(estimate) =
                self.zone_map_cost_estimate_for_storage(table, storage_id, &program, page_count)
            else {
                // A missing/untrusted partition is conservatively costed as
                // a full contiguous scan; trusted siblings can still prune.
                surviving_pages = surviving_pages.saturating_add(page_count);
                total_ranges = total_ranges.saturating_add(usize::from(page_count > 0));
                continue;
            };
            saw_trusted_map = true;
            exact &= estimate.exact;
            storage_objects = storage_objects.saturating_add(1);
            surviving_pages = surviving_pages.saturating_add(estimate.surviving_pages);
            total_ranges = total_ranges.saturating_add(estimate.ranges);
            zones_examined = zones_examined.saturating_add(estimate.zones_examined);
            metadata_read_bytes = metadata_read_bytes.saturating_add(estimate.metadata_read_bytes);
            metadata_cache_misses =
                metadata_cache_misses.saturating_add(estimate.metadata_cache_misses);
        }
        saw_trusted_map.then_some(ZoneMapCostEstimate {
            surviving_pages,
            total_pages,
            ranges: total_ranges,
            zones_examined,
            metadata_read_bytes,
            metadata_cache_misses,
            storage_objects,
            exact,
        })
    }

    fn zone_map_cost_estimate_for_storage(
        &self,
        table: &TableSchema,
        storage_id: u64,
        program: &ZonePredicateProgram,
        page_count: u64,
    ) -> Option<ZoneMapCostEstimate> {
        let map = self.zone_map_for(storage_id, table)?;
        let _data = map.data_gate.read().ok()?;
        if map.retired.load(Ordering::Acquire)
            || !map.trusted_base.load(Ordering::Acquire)
            || !map.cache_admitted.load(Ordering::Acquire)
        {
            return None;
        }
        if page_count == 0 {
            return Some(ZoneMapCostEstimate {
                surviving_pages: 0,
                total_pages: 0,
                ranges: 0,
                zones_examined: 0,
                metadata_read_bytes: 0,
                metadata_cache_misses: 0,
                storage_objects: 1,
                exact: true,
            });
        }
        let (total_zones, step, expected_samples) =
            zone_map_sample_geometry(page_count, map.zone_pages as u64);
        // Observe only pages actually visited by the bounded sample. This
        // avoids scanning the buffer pool and naturally distinguishes warm,
        // cold and repeatedly evicted pages without a per-page tracking set.
        let cost_probe = map.cost_probe_scope();
        let mut metadata_read_bytes = 0u64;
        let (oldest_reader, visible_scn) = self.snapshot_visibility_horizon();
        let safe_read_scn = oldest_reader.unwrap_or(visible_scn).min(visible_scn);
        let mut sampled = 0u64;
        let mut surviving = 0u64;
        let mut exact_pages = 0u64;
        let mut transitions = 0usize;
        let mut prior = false;
        map.visit_matches(
            (0..total_zones).step_by(step as usize),
            program,
            safe_read_scn,
            |zone_id, may_match, inspected_bytes| {
                if sampled % 64 == 0 {
                    crate::engine::cancel::check_cancelled()?;
                }
                metadata_read_bytes = metadata_read_bytes.saturating_add(inspected_bytes);
                sampled += 1;
                surviving += u64::from(may_match);
                if may_match {
                    exact_pages += (page_count - zone_id * u64::from(map.zone_pages))
                        .min(u64::from(map.zone_pages));
                }
                if may_match && !prior {
                    transitions += 1;
                }
                prior = may_match;
                Ok(())
            },
        )
        .ok()?;
        if map.retired.load(Ordering::Acquire) {
            return None;
        }
        debug_assert_eq!(sampled, expected_samples);
        let exact = step == 1 && sampled == total_zones;
        let ratio = surviving as f64 / sampled.max(1) as f64;
        let surviving_pages = if exact {
            exact_pages
        } else if surviving == 0 {
            // A sampled miss is not proof that every unsampled zone misses.
            // Keep at least one full sampling stride in the estimate.
            step.saturating_mul(u64::from(map.zone_pages))
                .min(page_count)
        } else {
            ((page_count as f64) * ratio).ceil() as u64
        };
        let ranges = ((transitions as f64) * total_zones as f64 / sampled.max(1) as f64)
            .ceil()
            .max(f64::from(surviving_pages > 0)) as usize;
        let metadata_cache_misses = cost_probe.misses();
        Some(ZoneMapCostEstimate {
            surviving_pages: surviving_pages.min(page_count),
            total_pages: page_count,
            ranges,
            zones_examined: sampled,
            metadata_read_bytes,
            metadata_cache_misses,
            storage_objects: 1,
            exact,
        })
    }

    #[cfg(test)]
    pub fn zone_map_diagnostic_rows(&self) -> Vec<Vec<Value>> {
        self.zone_map_diagnostic_rows_filtered(None, None, None, None)
    }

    pub(crate) fn zone_map_diagnostic_rows_filtered(
        &self,
        table_schema: Option<&str>,
        table_name: Option<&str>,
        storage_filter: Option<u64>,
        column_filter: Option<&str>,
    ) -> Vec<Vec<Value>> {
        let Some(_zone_operation) = self.zone_map_operation() else {
            return Vec::new();
        };
        let tables = self
            .catalog
            .read()
            .map(|c| {
                c.tables
                    .values()
                    .filter(|table| {
                        table_schema
                            .is_none_or(|schema| table.database.eq_ignore_ascii_case(schema))
                            && table_name.is_none_or(|name| table.name.eq_ignore_ascii_case(name))
                    })
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let mut rows = Vec::new();
        for header in tables {
            let table = if header.columns.is_empty() {
                self.table_by_key(&table_key(&header.database, &header.name))
                    .unwrap_or(header)
            } else {
                header
            };
            for storage_id in table.storage_ids() {
                if storage_filter.is_some_and(|filter| filter != storage_id) {
                    continue;
                }
                let Some(map) = self.zone_map_for(storage_id, &table) else {
                    continue;
                };
                let partition_name = table.partitioning.as_ref().and_then(|info| {
                    info.partitions.iter().find_map(|partition| {
                        if partition.subpartitions.is_empty() && partition.storage_id == storage_id
                        {
                            Some(partition.name.clone())
                        } else {
                            partition
                                .subpartitions
                                .iter()
                                .find(|sub| sub.storage_id == storage_id)
                                .map(|sub| sub.name.clone())
                        }
                    })
                });
                let column_count = table
                    .columns
                    .iter()
                    .filter(|column| {
                        column_filter.is_none_or(|name| column.name.eq_ignore_ascii_case(name))
                    })
                    .count();
                let mut columns_lease = ZoneScratchLease::default();
                if let Err(err) = columns_lease.reserve(zone_tree_bytes::<u32, ()>(column_count)) {
                    map.record_write_error(&err);
                    continue;
                }
                let diagnostic_columns = table
                    .columns
                    .iter()
                    .filter(|column| {
                        column_filter.is_none_or(|name| column.name.eq_ignore_ascii_case(name))
                    })
                    .map(|column| column.column_id)
                    .collect::<BTreeSet<_>>();
                if diagnostic_columns.is_empty() {
                    continue;
                }
                let Ok(page_count) = self.heap_page_count_for_storage(storage_id) else {
                    continue;
                };
                let total_zones = page_count.div_ceil(u64::from(map.zone_pages));
                for zone_id in 0..total_zones {
                    // Keep only one zone's selected columns decoded. The
                    // result rows are owned by the caller; no diagnostic scan
                    // populates a separate persistent summary cache.
                    let Ok(_gate) = map.data_gate.read() else {
                        continue;
                    };
                    let zone = map
                        .read_zone(zone_id, Some(&diagnostic_columns))
                        .ok()
                        .flatten()
                        .unwrap_or_else(|| ChargedZoneSummary {
                            value: ZoneSummary {
                                state: Some(ZoneMapState::Unknown),
                                ..ZoneSummary::default()
                            },
                            lease: ZoneScratchLease::default(),
                        });
                    let unknown = ZoneColumnSummary {
                        state: ZoneMapState::Unknown,
                        ..ZoneColumnSummary::default()
                    };
                    for column_id in &diagnostic_columns {
                        let summary = if zone.state == Some(ZoneMapState::Unknown) {
                            &unknown
                        } else {
                            zone.columns.get(column_id).unwrap_or(&unknown)
                        };
                        let column = table.columns.iter().find(|c| c.column_id == *column_id);
                        let column_name = table
                            .columns
                            .iter()
                            .find(|c| c.column_id == *column_id)
                            .map(|c| c.name.clone())
                            .unwrap_or_else(|| format!("#{column_id}"));
                        let (membership_kind, membership_bytes, membership_hashes, saturated) =
                            summary
                                .membership
                                .as_ref()
                                .map(ZoneMembership::diagnostic)
                                .unwrap_or(("NONE", 0, 0, false));
                        rows.push(vec![
                            Value::String(table.database.clone()),
                            Value::String(table.name.clone()),
                            partition_name
                                .as_ref()
                                .map(|name| Value::String(name.clone()))
                                .unwrap_or(Value::Null),
                            Value::UInt(storage_id),
                            Value::UInt(zone_id),
                            Value::UInt(zone_id * map.zone_pages as u64),
                            Value::UInt((zone_id + 1) * map.zone_pages as u64),
                            Value::String(column_name),
                            summary
                                .min
                                .as_ref()
                                .map(|v| Value::String(v.display_for_column(column)))
                                .unwrap_or(Value::Null),
                            summary
                                .max
                                .as_ref()
                                .map(|v| Value::String(v.display_for_column(column)))
                                .unwrap_or(Value::Null),
                            Value::Int(i64::from(summary.has_null)),
                            Value::Int(i64::from(summary.has_non_null)),
                            Value::Int(i64::from(summary.truncated)),
                            Value::String(format!("{:?}", summary.state)),
                            Value::UInt(map.generation.load(Ordering::Acquire)),
                            Value::UInt(zone.rebuild_scn),
                            Value::UInt(zone.stale_mutations),
                            Value::String(membership_kind.into()),
                            Value::UInt(membership_bytes as u64),
                            Value::UInt(u64::from(membership_hashes)),
                            Value::UInt(1),
                            Value::UInt(ZONE_MAP_SEMANTIC_VERSION as u64),
                            Value::Int(i64::from(saturated)),
                        ]);
                    }
                }
            }
        }
        rows
    }

    pub fn zone_map_storage_rows(&self) -> Vec<Vec<Value>> {
        let Some(_zone_operation) = self.zone_map_operation() else {
            return Vec::new();
        };
        let tables = self
            .catalog
            .read()
            .map(|catalog| catalog.tables.values().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        let mut rows = Vec::new();
        for table in tables {
            for storage_id in table.storage_ids() {
                let Some(map) = self.zone_map_for(storage_id, &table) else {
                    continue;
                };
                let partition_name = table.partitioning.as_ref().and_then(|info| {
                    info.partitions.iter().find_map(|partition| {
                        if partition.subpartitions.is_empty() && partition.storage_id == storage_id
                        {
                            Some(partition.name.clone())
                        } else {
                            partition
                                .subpartitions
                                .iter()
                                .find(|sub| sub.storage_id == storage_id)
                                .map(|sub| sub.name.clone())
                        }
                    })
                });
                // Metadata-only snapshot: showing storage state must not
                // read every zone, bounds or membership page into the pool.
                let snapshot = map.storage_snapshot();
                let file_bytes = snapshot.file_bytes;
                let live_bytes = snapshot.live_bytes;
                let last_error = map
                    .rebuild_error
                    .lock()
                    .ok()
                    .and_then(|value| value.clone())
                    .or_else(|| map.last_error.lock().ok().and_then(|value| value.clone()));
                let trusted = map.trusted_base.load(Ordering::Acquire);
                let state = if !trusted {
                    "UNKNOWN"
                } else if last_error.is_some() {
                    "DEGRADED"
                } else if snapshot.pending {
                    "PENDING"
                } else {
                    "READY"
                };
                rows.push(vec![
                    Value::String(table.database.clone()),
                    Value::String(table.name.clone()),
                    partition_name.map(Value::String).unwrap_or(Value::Null),
                    Value::UInt(storage_id),
                    Value::UInt(ZONE_MAP_VERSION as u64),
                    Value::UInt(map.generation.load(Ordering::Acquire)),
                    Value::UInt(map.last_persisted_lsn.load(Ordering::Acquire)),
                    Value::UInt(file_bytes),
                    Value::UInt(live_bytes),
                    Value::UInt(file_bytes.saturating_sub(live_bytes)),
                    Value::UInt(snapshot.frame_count),
                    Value::UInt(snapshot.resident_bytes),
                    Value::UInt(snapshot.dirty_bytes),
                    if file_bytes == 0 {
                        Value::Null
                    } else {
                        Value::Double(1.0)
                    },
                    Value::String(state.into()),
                    last_error.map(Value::String).unwrap_or(Value::Null),
                    Value::String(
                        match map.rebuild_state.load(Ordering::Acquire) {
                            1 => "RUNNING",
                            2 => "COMPLETE",
                            3 => "FAILED",
                            _ => "IDLE",
                        }
                        .into(),
                    ),
                    Value::UInt(map.rebuild_zones_total.load(Ordering::Acquire)),
                    Value::UInt(map.rebuild_zones_done.load(Ordering::Acquire)),
                    Value::UInt(map.rebuild_started_ms.load(Ordering::Acquire)),
                    Value::UInt(map.rebuild_finished_ms.load(Ordering::Acquire)),
                    Value::UInt(snapshot.charged_bytes),
                    Value::UInt(ZONE_MAP_MEMORY_FALLBACKS.load(Ordering::Relaxed)),
                ]);
            }
        }
        rows
    }

    fn begin_analyze_zone_build(
        &self,
        _table: &TableSchema,
    ) -> Result<Option<AnalyzeZoneBuildContext>, QueryError> {
        // Statistics workers previously retained every zone and deferred row
        // until publication. The paged builder scans bounded ranges after
        // statistics publication so its working set is independent of table size.
        Ok(None)
    }

    fn cancel_analyze_zone_build(&self, context: AnalyzeZoneBuildContext) {
        context.map.rebuild_state.store(3, Ordering::Release);
        context
            .map
            .rebuild_finished_ms
            .store(now_unix_ms(), Ordering::Release);
        let _ = self.commit_txn(context.txn.txn_id);
    }

    fn publish_analyze_zone_build(
        &self,
        table: &TableSchema,
        context: AnalyzeZoneBuildContext,
        partial: AnalyzeZonePartial,
    ) -> Result<(), QueryError> {
        self.cancel_analyze_zone_build(context);
        drop(partial);
        self.rebuild_zone_map_for_table(table)
    }

    /// Populate missing/building summaries from a normal MVCC scan.  A fresh
    /// generation may replace the old envelope only when the commit frontier,
    /// mutation epoch, active writers, and historical-snapshot frontier prove
    /// that the scan covered every value that can still become visible.
    /// Otherwise the fresh image is unioned into the live map.
    pub(crate) fn rebuild_zone_map_for_table(&self, table: &TableSchema) -> Result<(), QueryError> {
        if self.physical_replica_mode() {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "physical replicas cannot rebuild zone-map pages locally",
            ));
        }
        let Some(_zone_operation) = self.zone_map_operation() else {
            return Ok(());
        };
        if !table.zone_map_enabled() {
            return Ok(());
        }
        for storage_id in table.storage_ids() {
            self.rebuild_zone_map_for_storage(table, storage_id, &|| self.physical_replica_mode())?;
        }
        Ok(())
    }

    /// Scan one page range of the given storage and build a per-zone
    /// aggregate. Returns the fresh delta and the deferred (invisible) row
    /// ids that need a second-pass resolution. Extracted so the outer
    /// rebuild can parallelise across zone-aligned page ranges.
    fn rebuild_zone_map_scan_range<F>(
        &self,
        table: &TableSchema,
        storage_id: u64,
        map: &ZoneMap,
        txn: TxnView,
        build_scn: u64,
        relevant_columns: &[(usize, u32)],
        projected_indices: &[usize],
        page_lo: u64,
        page_hi: u64,
        should_cancel: &F,
    ) -> Result<Option<ZoneMapScanPartial>, QueryError>
    where
        F: Fn() -> bool + Sync + ?Sized,
    {
        let zone_pages = map.zone_pages as u64;
        let mut fresh = ChargedZoneBatch::new(map);
        let mut config_lease = ZoneScratchLease::default();
        config_lease.reserve(
            relevant_columns
                .len()
                .saturating_mul(
                    std::mem::size_of::<WorkerColumnState>()
                        + std::mem::size_of::<u16>()
                        + std::mem::size_of::<&crate::charset::CollationInfo>()
                        + 4 * std::mem::size_of::<Value>(),
                )
                .saturating_add(table.columns.len().saturating_mul(128))
                .saturating_add(zone_tree_bytes::<u32, ZoneMembershipLayout>(
                    ZONE_MEMBERSHIP_MAX_COLUMNS,
                )),
        )?;
        let mut current_zone: Option<u64> = None;
        let mut zone_row_count: u64 = 0;
        // WorkerScalar-based running summaries: same shape as the LOAD
        // path aggregator. Comparisons stay in compact (i128, u8) space
        // for DECIMAL and reuse a shared `text_scratch` for text weights,
        // so most rows never allocate anything even during a 6 M-row
        // lineitem rebuild.
        let membership_columns = zone_map_membership_columns(table);
        let mut zone_running: Vec<WorkerColumnState> = relevant_columns
            .iter()
            .map(|(_, column_id)| {
                let mut state = WorkerColumnState::default();
                if let Some(layout) = membership_columns.get(column_id).copied() {
                    state.enable_membership_with_layout(layout);
                }
                state
            })
            .collect();
        let collation_ids: Vec<u16> = relevant_columns
            .iter()
            .map(|(idx, _)| {
                crate::charset::collation_id(
                    table.columns[*idx]
                        .collation
                        .as_deref()
                        .unwrap_or("utf8mb4_0900_ai_ci"),
                )
            })
            .collect();
        let collations: Vec<&'static crate::charset::CollationInfo> = collation_ids
            .iter()
            .map(|id| {
                crate::charset::resolve_collation_id(*id).expect("schema collation id must resolve")
            })
            .collect();
        let mut text_lease = ZoneScratchLease::default();
        let mut text_scratch: Vec<u8> = Vec::new();
        let mut vis_cache = ScanVisibilityCache::new();
        let mut deferred_lease = ZoneScratchLease::default();
        let mut deferred: Vec<RowId> = Vec::new();
        let mut markers_lease = ZoneScratchLease::default();
        let mut deferred_markers = BTreeMap::new();
        let mut visit_err: Option<QueryError> = None;
        let plan = ProjectedReadPlan::new(projected_indices, table);
        let mut row_values_lease = ZoneScratchLease::default();
        let mut value_scratch: Vec<Value> = Vec::with_capacity(projected_indices.len());
        let mut backfill_scratch: Vec<Value> = Vec::new();
        let mut cancelled = false;
        let mut cancel_checked_page = None;
        let flush_zone = |zone_id: u64,
                          zone_row_count: u64,
                          zone_running: &mut Vec<WorkerColumnState>,
                          fresh: &mut ChargedZoneBatch,
                          relevant_columns: &[(usize, u32)]|
         -> Result<(), QueryError> {
            fresh.ensure_zone(zone_id)?;
            let entry = fresh.zones.get_mut(&zone_id).unwrap();
            entry.row_count_estimate = entry.row_count_estimate.saturating_add(zone_row_count);
            entry.rebuild_scn = build_scn;
            for (i, (_, column_id)) in relevant_columns.iter().enumerate() {
                let state = &zone_running[i];
                let scalar_bytes = state
                    .min
                    .as_ref()
                    .map_or(0, WorkerScalar::owned_bytes)
                    .saturating_add(state.max.as_ref().map_or(0, WorkerScalar::owned_bytes));
                let old_membership = fresh.zones[&zone_id]
                    .columns
                    .get(column_id)
                    .and_then(|column| column.membership.as_ref());
                let peak = scalar_bytes
                    .saturating_mul(5)
                    .saturating_add(1024)
                    .saturating_add(
                        state
                            .membership
                            .as_ref()
                            .map_or(0, ZoneMembership::capacity_bytes),
                    )
                    .saturating_add(zone_membership_merge_allocation(
                        old_membership,
                        state.membership.as_ref(),
                    ));
                let ticket = fresh.begin_column(zone_id, *column_id, peak)?;
                // The destination lease exists before taking the worker's
                // buffers, so dropping its lease cannot leave them uncharged.
                let summary = worker_state_into_summary(std::mem::take(&mut zone_running[i]));
                if let Some(layout) = membership_columns.get(column_id).copied() {
                    zone_running[i].enable_membership_with_layout(layout);
                }
                match fresh
                    .zones
                    .get_mut(&zone_id)
                    .unwrap()
                    .columns
                    .entry(*column_id)
                {
                    std::collections::btree_map::Entry::Vacant(slot) => {
                        slot.insert(summary);
                    }
                    std::collections::btree_map::Entry::Occupied(mut slot) => {
                        merge_column_summary_into(slot.get_mut(), summary)
                    }
                }
                fresh.finish_column(zone_id, *column_id, ticket);
            }
            Ok(())
        };
        for batch_lo in (page_lo..page_hi).step_by(ANALYZE_SCAN_BATCH_PAGES as usize) {
            if should_cancel() {
                return Ok(None);
            }
            self.with_heap_read(storage_id, |heap| {
                heap.scan_mvcc_payloads_direct_range_until(
                    projected_indices,
                    batch_lo,
                    (batch_lo + ANALYZE_SCAN_BATCH_PAGES).min(page_hi),
                    |row| {
                        if cancel_checked_page != Some(row.row_id.page_id) {
                            cancel_checked_page = Some(row.row_id.page_id);
                            if should_cancel() {
                                cancelled = true;
                                return Ok(true);
                            }
                        }
                        if visit_err.is_some() {
                            return Ok(true);
                        }
                        let visible = match self.row_version_visible_cached(
                            &mut vis_cache,
                            row.header,
                            txn,
                        ) {
                            Ok(v) => v,
                            Err(e) => {
                                visit_err = Some(e);
                                return Ok(true);
                            }
                        };
                        if !visible {
                            zone_reserve_vec(&mut deferred, &mut deferred_lease, 1)?;
                            deferred.push(row.row_id);
                            return Ok(false);
                        }
                        if row.header.is_deleted() {
                            return Ok(false);
                        }
                        let zone_id = row.row_id.page_id as u64 / zone_pages;
                        if Some(zone_id) != current_zone {
                            if let Some(prev_zone) = current_zone {
                                flush_zone(
                                    prev_zone,
                                    zone_row_count,
                                    &mut zone_running,
                                    &mut fresh,
                                    relevant_columns,
                                )?;
                            }
                            current_zone = Some(zone_id);
                            zone_row_count = 0;
                        }
                        zone_row_count = zone_row_count.saturating_add(1);
                        let fast = {
                            let mut sink = ZoneRebuildDecodeSink {
                                table,
                                column_indices: projected_indices,
                                collation_ids: &collation_ids,
                                collations: &collations,
                                running: &mut zone_running,
                                text_scratch: &mut text_scratch,
                                text_lease: &mut text_lease,
                            };
                            decode_row_into_sink_fast(
                                &row.payload[row.body_offset..],
                                row.codec,
                                row.layout_id,
                                row.plan,
                                &mut sink,
                                row.has_external,
                            )?
                        };
                        if !fast {
                            value_scratch.clear();
                            backfill_scratch.clear();
                            row_values_lease.shrink_to(0);
                            let mut count_pos = 0usize;
                            let stored_count =
                                read_varint_at(&row.payload[row.body_offset..], &mut count_pos)?
                                    as usize;
                            let defaults = projected_indices
                                .iter()
                                .filter_map(|&index| table.columns.get(index))
                                .filter(|column| column.physical_pos as usize >= stored_count)
                                .map(zone_backfill_owned_bytes)
                                .fold(0usize, usize::saturating_add);
                            row_values_lease.reserve(
                                row.payload
                                    .len()
                                    .saturating_mul(8)
                                    .saturating_add(defaults.saturating_mul(4)),
                            )?;
                            decode_row_schema_columns_into_with_plan_ext_for_layout(
                                &row.payload[row.body_offset..],
                                row.codec,
                                row.layout_id,
                                row.plan,
                                &mut value_scratch,
                                row.has_external,
                            )?;
                            let values_to_visit: &[Value] =
                                if plan.needs_adjustment(&value_scratch, stored_count) {
                                    backfill_scratch.clear();
                                    backfill_scratch.extend_from_slice(&value_scratch);
                                    plan.apply(
                                        &mut backfill_scratch,
                                        projected_indices,
                                        stored_count,
                                        table,
                                    );
                                    &backfill_scratch
                                } else {
                                    &value_scratch
                                };
                            for (i, (col_idx, _)) in relevant_columns.iter().enumerate() {
                                let Some(value) = values_to_visit.get(i) else {
                                    zone_running[i].mark_unknown();
                                    continue;
                                };
                                zone_running[i].observe(
                                    &table.columns[*col_idx],
                                    collation_ids[i],
                                    value,
                                    &mut text_scratch,
                                    &mut text_lease,
                                );
                            }
                        }
                        if !fast {
                            let retained = value_scratch
                                .iter()
                                .chain(&backfill_scratch)
                                .map(zone_value_owned_bytes)
                                .fold(0usize, usize::saturating_add);
                            row_values_lease.shrink_to(retained);
                        }
                        Ok(false)
                    },
                )
            })?;
            if cancelled || visit_err.is_some() {
                break;
            }
            if let Some(zone_id) = current_zone.take() {
                flush_zone(
                    zone_id,
                    zone_row_count,
                    &mut zone_running,
                    &mut fresh,
                    relevant_columns,
                )?;
                zone_row_count = 0;
            }
            // Resolve invisible versions after releasing the heap guard, one
            // heap batch at a time. Retain only one marker per affected zone
            // for the publication fence, never every invisible row in a zone.
            for row_id in deferred.drain(..) {
                if should_cancel() {
                    return Ok(None);
                }
                let zone_id = u64::from(row_id.page_id) / zone_pages;
                if !deferred_markers.contains_key(&zone_id) {
                    markers_lease.reserve(
                        zone_tree_bytes::<u64, RowId>(deferred_markers.len() + 1)
                            .saturating_sub(zone_tree_bytes::<u64, RowId>(deferred_markers.len())),
                    )?;
                    deferred_markers.insert(zone_id, row_id);
                }
                if let Some(values) =
                    self.read_row_columns_at(storage_id, table, row_id, txn, projected_indices)?
                {
                    fresh.ensure_zone(zone_id)?;
                    let zone = fresh.zones.get_mut(&zone_id).unwrap();
                    zone.row_count_estimate = zone.row_count_estimate.saturating_add(1);
                    zone.rebuild_scn = build_scn;
                    for (slot, (column_index, column_id)) in relevant_columns.iter().enumerate() {
                        fresh.aggregate(
                            zone_id,
                            *column_id,
                            &table.columns[*column_index],
                            std::slice::from_ref(&values),
                            slot,
                            membership_columns.get(column_id).copied(),
                        )?;
                    }
                }
            }
        }
        if let Some(e) = visit_err {
            return Err(e);
        }
        if cancelled {
            return Ok(None);
        }
        if let Some(zone_id) = current_zone.take() {
            flush_zone(
                zone_id,
                zone_row_count,
                &mut zone_running,
                &mut fresh,
                relevant_columns,
            )?;
        }
        markers_lease.reserve(
            deferred_markers
                .len()
                .saturating_mul(std::mem::size_of::<RowId>()),
        )?;
        let markers = deferred_markers.into_values().collect::<Vec<_>>();
        markers_lease.shrink_to(
            markers
                .capacity()
                .saturating_mul(std::mem::size_of::<RowId>()),
        );
        let ChargedZoneBatch { batch, lease } = fresh;
        Ok(Some(ZoneMapScanPartial {
            batch,
            deferred: markers,
            _owned_charge: lease,
            _deferred_charge: markers_lease,
        }))
    }

    fn rebuild_zone_map_for_storage<F>(
        &self,
        table: &TableSchema,
        storage_id: u64,
        should_cancel: &F,
    ) -> Result<bool, QueryError>
    where
        F: Fn() -> bool + Sync + ?Sized,
    {
        let Some(map) = self.zone_map_for(storage_id, table) else {
            return Ok(false);
        };
        self.rebuild_zone_map_incremental(table, storage_id, &map, should_cancel)
    }

    /// Explicit full rebuild, streamed through bounded batches. Cancellation
    /// leaves completed records available to the separate background repair
    /// path; background passes never restart this full heap scan.
    fn rebuild_zone_map_incremental<F>(
        &self,
        table: &TableSchema,
        storage_id: u64,
        map: &Arc<ZoneMap>,
        should_cancel: &F,
    ) -> Result<bool, QueryError>
    where
        F: Fn() -> bool + Sync + ?Sized,
    {
        if should_cancel() {
            return Ok(false);
        }
        // Missing records in a freshly reset tree mean Unknown. Publishing
        // that conservative base avoids a table-sized set of explicit Unknown
        // records and lets each successfully rebuilt range survive cancellation.
        {
            let _gate = map.data_gate.write().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "zone map lock poisoned",
                )
            })?;
            map.reset_unknown()?;
            map.generation.fetch_add(1, Ordering::AcqRel);
            map.mutation_epoch.fetch_add(1, Ordering::AcqRel);
            map.trusted_base.store(true, Ordering::Release);
        }
        // The watermark follows reset: no previously committed new-zone
        // metadata can be erased after choosing the set to rebuild.
        let build_frontier = self.visible_scn();
        let total = self
            .heap_page_count_for_storage(storage_id)?
            .div_ceil(u64::from(map.zone_pages));
        map.rebuild_started_ms
            .store(now_unix_ms(), Ordering::Release);
        map.rebuild_finished_ms.store(0, Ordering::Release);
        map.rebuild_state.store(1, Ordering::Release);
        map.rebuild_zones_total.store(total, Ordering::Release);
        map.rebuild_zones_done.store(0, Ordering::Release);
        let invalidation_epoch = map.invalidation_epoch.load(Ordering::Acquire);
        // Each batch scans in parallel but publishes serially under the data
        // gate. Scan batch N+1 while batch N publishes: N+1 captured its own
        // epochs and registered its snapshot before scanning, and publishing
        // N touches only N's zones, so per-batch validation is unchanged.
        let reclaim_database = crate::engine::executor::mem_budget::current_reclaim_database();
        let pipelined = std::thread::scope(|scope| -> Result<Option<bool>, QueryError> {
            let mut fully_replaced = true;
            type Scanned<G> = (
                ZoneRebuildBatch<G>,
                Result<Option<ScannedZoneRebuild>, QueryError>,
            );
            let mut in_flight: Option<(u64, u64, std::thread::ScopedJoinHandle<'_, Scanned<_>>)> =
                None;
            let ranges = (0..total)
                .step_by(64)
                .map(|first| Some((first, first.saturating_add(64).min(total))))
                .chain(std::iter::once(None));
            for range in ranges {
                let mut started = None;
                if let Some((first, end)) = range {
                    if should_cancel() {
                        return Ok(None);
                    }
                    let requested = (first..end).collect::<BTreeSet<_>>();
                    match self.prepare_zone_rebuild_batch(
                        table,
                        storage_id,
                        &requested,
                        should_cancel,
                        Some((first, total)),
                    )? {
                        PreparedZoneRebuild::Finished(None) => return Ok(None),
                        PreparedZoneRebuild::Finished(Some(replaced)) => {
                            fully_replaced &= replaced == end - first;
                        }
                        PreparedZoneRebuild::Batch(batch) => {
                            let reclaim_database = reclaim_database.as_ref();
                            let handle = scope.spawn(move || {
                                let _reclaim = reclaim_database.map(
                                    crate::engine::executor::mem_budget::ReclaimDatabaseScope::enter,
                                );
                                let scanned = self.scan_zone_rebuild_batch(
                                    table,
                                    storage_id,
                                    &batch,
                                    should_cancel,
                                );
                                (batch, scanned)
                            });
                            started = Some((first, end, handle));
                        }
                    }
                }
                if let Some((first, end, handle)) = in_flight.take() {
                    let (batch, scanned) = handle.join().map_err(|_| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "zone map rebuild scan panicked",
                        )
                    })?;
                    let Some(replaced) =
                        self.publish_zone_rebuild_batch(table, batch, scanned, should_cancel)?
                    else {
                        return Ok(None);
                    };
                    fully_replaced &= replaced == end - first;
                    map.maintenance_cursor
                        .store(end % total.max(1), Ordering::Release);
                }
                in_flight = started;
            }
            Ok(Some(fully_replaced))
        })?;
        let Some(fully_replaced) = pipelined else {
            map.rebuild_state.store(0, Ordering::Release);
            map.rebuild_finished_ms
                .store(now_unix_ms(), Ordering::Release);
            return Ok(false);
        };
        let final_epoch = map.mutation_epoch.load(Ordering::Acquire);
        let final_total = self
            .heap_page_count_for_storage(storage_id)?
            .div_ceil(u64::from(map.zone_pages));
        {
            let _gate = map.data_gate.write().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "zone map lock poisoned",
                )
            })?;
            // Only a complete, stable heap coverage can make absent records
            // mean Empty again. Partial/cancelled builds retain Unknown holes.
            if fully_replaced
                && !map.retired.load(Ordering::Acquire)
                && !should_cancel()
                && self.active_write_txns_atomic.load(Ordering::Acquire) == 0
                && self.visible_scn() == build_frontier
                && self
                    .oldest_active_snapshot_scn()
                    .is_none_or(|oldest| oldest >= build_frontier)
                && map.mutation_epoch.load(Ordering::Acquire) == final_epoch
                && map.invalidation_epoch.load(Ordering::Acquire) == invalidation_epoch
                && final_total == total
            {
                map.store.set_missing_is_empty(true)?;
                map.missing_is_empty.store(true, Ordering::Release);
            }
        }
        map.rebuild_state.store(2, Ordering::Release);
        map.rebuild_finished_ms
            .store(now_unix_ms(), Ordering::Release);
        if let Some(cache) = crate::engine::cache::global_plan_cache() {
            cache.invalidate_table(table.id);
        }
        Ok(true)
    }

    /// Rebuild only stale zones. The snapshot can publish a shrinking zone
    /// when no mutation touched that zone since the scan began and no older
    /// snapshot can still observe values outside the new envelope. Unrelated
    /// DML is allowed to proceed; a racing target zone is merged
    /// conservatively so correctness never depends on maintenance timing.
    fn rebuild_zone_map_for_storage_zones<F>(
        &self,
        table: &TableSchema,
        storage_id: u64,
        requested_zones: &BTreeSet<u64>,
        should_cancel: &F,
        progress: Option<(u64, u64)>,
    ) -> Result<Option<u64>, QueryError>
    where
        F: Fn() -> bool + Sync + ?Sized,
    {
        let batch = match self.prepare_zone_rebuild_batch(
            table,
            storage_id,
            requested_zones,
            should_cancel,
            progress,
        )? {
            PreparedZoneRebuild::Finished(result) => return Ok(result),
            PreparedZoneRebuild::Batch(batch) => batch,
        };
        let scanned = self.scan_zone_rebuild_batch(table, storage_id, &batch, should_cancel);
        self.publish_zone_rebuild_batch(table, batch, scanned, should_cancel)
    }

    /// Capture the per-zone mutation epochs and register the snapshot that
    /// one rebuild batch scans. Nothing is published here.
    fn prepare_zone_rebuild_batch<F>(
        &self,
        table: &TableSchema,
        storage_id: u64,
        requested_zones: &BTreeSet<u64>,
        should_cancel: &F,
        progress: Option<(u64, u64)>,
    ) -> Result<PreparedZoneRebuild<impl Drop + '_>, QueryError>
    where
        F: Fn() -> bool + Sync + ?Sized,
    {
        if requested_zones.is_empty() {
            return Ok(PreparedZoneRebuild::Finished(Some(0)));
        }
        if should_cancel() {
            return Ok(PreparedZoneRebuild::Finished(None));
        }
        let Some(map) = self.zone_map_for(storage_id, table) else {
            return Ok(PreparedZoneRebuild::Finished(Some(0)));
        };
        let invalidation_epoch = map.invalidation_epoch.load(Ordering::Acquire);
        let was_trusted = map.trusted_base.load(Ordering::Acquire);
        // Capture before taking the snapshot/page watermark. Otherwise a
        // committed insertion in that gap can be absent from both the scan
        // and the epoch comparison.
        let mut plan_lease = ZoneScratchLease::default();
        let count = requested_zones.len();
        plan_lease.reserve(
            zone_tree_bytes::<u64, u64>(count)
                .saturating_add(zone_tree_bytes::<u64, ()>(count).saturating_mul(2))
                .saturating_add(zone_tree_bytes::<u64, ZoneSummary>(count))
                .saturating_add(count.saturating_mul(4 * std::mem::size_of::<(u64, u64)>())),
        )?;
        let capture_gate = map
            .data_gate
            .read()
            .map_err(|_| ZoneMap::access_error("zone-map data lock poisoned"))?;
        let neutral_overflow_before = map
            .neutral_mutations
            .lock()
            .map_err(|_| ZoneMap::access_error("neutral mutation lock poisoned"))?
            .overflow_epoch;
        let epoch_before = requested_zones
            .iter()
            .map(|id| {
                Ok((
                    *id,
                    map.zone_mutation(*id)?.map_or(0, |mutation| mutation.epoch),
                ))
            })
            .collect::<Result<BTreeMap<_, _>, QueryError>>()?;
        drop(capture_gate);
        // A read-only transaction slot alone does not pin the purge horizon.
        // Keep one registered view across all parallel scans and publication.
        let (txn, _snapshot_guard) = self.borrowed_read_only_statement_view();
        let build_scn = txn.snapshot_scn;
        #[cfg(test)]
        if let Some(barrier) = ZONE_MAP_REBUILD_SCAN_BARRIER.lock().unwrap().clone() {
            barrier.wait();
            barrier.wait();
        }
        map.rebuild_state.store(1, Ordering::Release);
        if progress.is_none() {
            map.rebuild_started_ms
                .store(now_unix_ms(), Ordering::Release);
        }
        map.rebuild_finished_ms.store(0, Ordering::Release);
        map.rebuild_zones_total.store(
            progress.map_or(requested_zones.len() as u64, |(_, total)| total),
            Ordering::Release,
        );
        map.rebuild_zones_done
            .store(progress.map_or(0, |(first, _)| first), Ordering::Release);
        Ok(PreparedZoneRebuild::Batch(ZoneRebuildBatch {
            map,
            requested_zones: requested_zones.clone(),
            progress,
            invalidation_epoch,
            was_trusted,
            neutral_overflow_before,
            epoch_before,
            txn,
            build_scn,
            _plan_lease: plan_lease,
            _snapshot_guard,
        }))
    }

    /// Parallel heap scan of one prepared batch. It reads only heap pages and
    /// the batch snapshot, so it may overlap the previous batch's publication.
    fn scan_zone_rebuild_batch<F, G>(
        &self,
        table: &TableSchema,
        storage_id: u64,
        batch: &ZoneRebuildBatch<G>,
        should_cancel: &F,
    ) -> Result<Option<ScannedZoneRebuild>, QueryError>
    where
        F: Fn() -> bool + Sync + ?Sized,
    {
        let map = &batch.map;
        let txn = batch.txn;
        let build_scn = batch.build_scn;
        let requested_zones = &batch.requested_zones;
        let relevant_columns = zone_map_relevant_columns(table);
        let projected_indices = relevant_columns
            .iter()
            .map(|(index, _)| *index)
            .collect::<Vec<_>>();
        let page_count = self.with_heap_read(storage_id, |heap| heap.page_count())?;
        let zone_pages = map.zone_pages as u64;
        // Leases outlive merged buffers, including on cancellation/error.
        let mut _scan_charges = Vec::new();
        let mut fresh = ZoneMapDeltaBatch {
            storage_id,
            zone_pages: map.zone_pages,
            schema_fingerprint: map.schema_fingerprint,
            zones: BTreeMap::new(),
        };
        let mut deferred = Vec::new();

        // Coalesce adjacent target zones into bounded heap scans.
        let mut ranges = Vec::new();
        let mut iter = requested_zones.iter().copied().peekable();
        while let Some(first) = iter.next() {
            let mut last = first;
            while iter
                .peek()
                .is_some_and(|next| *next == last.saturating_add(1))
            {
                last = iter.next().unwrap_or(last);
            }
            ranges.push((first, last));
        }
        for (first, last) in ranges {
            let page_lo = first.saturating_mul(zone_pages).min(page_count);
            let page_hi = last
                .saturating_add(1)
                .saturating_mul(zone_pages)
                .min(page_count);
            if page_lo < page_hi {
                let degree = rebuild_parallel_degree(page_hi - page_lo, zone_pages) as u64;
                let step = (page_hi - page_lo)
                    .div_ceil(degree)
                    .div_ceil(zone_pages)
                    .saturating_mul(zone_pages);
                let reclaim_database =
                    crate::engine::executor::mem_budget::current_reclaim_database();
                let scan = |lo, hi| {
                    let _reclaim = reclaim_database
                        .as_ref()
                        .map(crate::engine::executor::mem_budget::ReclaimDatabaseScope::enter);
                    self.rebuild_zone_map_scan_range(
                        table,
                        storage_id,
                        &map,
                        txn,
                        build_scn,
                        &relevant_columns,
                        &projected_indices,
                        lo,
                        hi,
                        should_cancel,
                    )
                };
                let partials = if degree == 1 {
                    vec![scan(page_lo, page_hi)]
                } else {
                    std::thread::scope(|scope| {
                        let mut handles = Vec::new();
                        for lo in (page_lo..page_hi).step_by(step as usize) {
                            let scan = &scan;
                            handles.push(scope.spawn(move || scan(lo, (lo + step).min(page_hi))));
                        }
                        handles
                            .into_iter()
                            .map(|handle| {
                                handle.join().unwrap_or_else(|_| {
                                    Err(error(
                                        crate::error::catalog::ER_GET_ERRNO,
                                        "zone map rebuild worker panicked",
                                    ))
                                })
                            })
                            .collect()
                    })
                };
                for partial in partials {
                    let Some(ZoneMapScanPartial {
                        batch,
                        deferred: mut batch_deferred,
                        _owned_charge,
                        _deferred_charge,
                    }) = partial?
                    else {
                        return Ok(None);
                    };
                    _scan_charges.push(_owned_charge);
                    _scan_charges.push(_deferred_charge);
                    fresh.zones.extend(batch.zones);
                    deferred.append(&mut batch_deferred);
                }
            }
        }

        let deferred_zones = deferred
            .into_iter()
            .map(|row_id| u64::from(row_id.page_id) / zone_pages)
            .collect::<BTreeSet<_>>();
        Ok(Some(ScannedZoneRebuild {
            fresh,
            deferred_zones,
            _scan_charges,
        }))
    }

    /// Validate the captured epochs and publish one scanned batch under the
    /// zone-map data gate.
    fn publish_zone_rebuild_batch<F, G>(
        &self,
        table: &TableSchema,
        batch: ZoneRebuildBatch<G>,
        scanned: Result<Option<ScannedZoneRebuild>, QueryError>,
        should_cancel: &F,
    ) -> Result<Option<u64>, QueryError>
    where
        F: Fn() -> bool + Sync + ?Sized,
    {
        let ZoneRebuildBatch {
            map,
            requested_zones,
            progress,
            invalidation_epoch,
            was_trusted,
            neutral_overflow_before,
            epoch_before,
            build_scn,
            _snapshot_guard,
            ..
        } = batch;
        let requested_zones = &requested_zones;
        let result = (|| {
            let Some(ScannedZoneRebuild {
                mut fresh,
                deferred_zones,
                _scan_charges,
            }) = scanned?
            else {
                return Ok(None);
            };
            if should_cancel() {
                return Ok(None);
            }

            #[cfg(test)]
            if let Some(barrier) = ZONE_MAP_REBUILD_PUBLISH_BARRIER
                .lock()
                .ok()
                .and_then(|barrier| barrier.clone())
            {
                // First rendezvous exposes the precise scan→publish window;
                // the second lets the test release maintenance after DML has
                // advanced the target zone epoch.
                barrier.wait();
                barrier.wait();
            }

            let snapshots_safe = self
                .oldest_active_snapshot_scn()
                .is_none_or(|oldest| oldest >= build_scn);
            // One critical section covers validation, replacement, dirty
            // frame publication and frontier update, in DML's lock order.
            let gate = map.data_gate.write().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "zone map lock poisoned",
                )
            })?;
            let mut replaceable = BTreeSet::new();
            let mut fallback = ChargedZoneBatch::new(&map);
            let neutral_slots_stable = map
                .neutral_mutations
                .lock()
                .map_err(|_| ZoneMap::access_error("neutral mutation lock poisoned"))?
                .overflow_epoch
                == neutral_overflow_before;
            for zone_id in requested_zones {
                let stable = neutral_slots_stable
                    && epoch_before.get(zone_id).copied().unwrap_or(0)
                        == map
                            .zone_mutation(*zone_id)?
                            .map_or(0, |mutation| mutation.epoch);
                if snapshots_safe
                    && stable
                    && map.invalidation_epoch.load(Ordering::Acquire) == invalidation_epoch
                    && !deferred_zones.contains(zone_id)
                {
                    replaceable.insert(*zone_id);
                } else if let Some(zone) = fresh.zones.get(zone_id) {
                    fallback.clone_zone(*zone_id, zone)?;
                }
            }

            let replaced = replaceable.len() as u64;
            ZONE_MAP_REBUILD_SHRINK_ZONES.fetch_add(replaced, Ordering::Relaxed);
            ZONE_MAP_REBUILD_UNION_ZONES.fetch_add(fallback.zones.len() as u64, Ordering::Relaxed);
            if !replaceable.is_empty() {
                let next_epoch = map.mutation_epoch.fetch_add(1, Ordering::AcqRel) + 1;
                let mut replacements = BTreeMap::new();
                for zone_id in &replaceable {
                    let mut replacement =
                        fresh.zones.remove(zone_id).unwrap_or_else(|| ZoneSummary {
                            state: Some(ZoneMapState::Empty),
                            rebuild_scn: build_scn,
                            ..ZoneSummary::default()
                        });
                    replacement.stale_mutations = 0;
                    replacement.rebuild_scn = build_scn;
                    replacements.insert(*zone_id, replacement);
                }
                map.replace_zones(&replacements)?;
                let mut mutations = ChargedZoneEntries::new(replaceable.len())?;
                for zone_id in &replaceable {
                    let zone_epoch = map.next_zone_mutation_epoch(*zone_id, next_epoch)?;
                    map.mutation_epoch.fetch_max(zone_epoch, Ordering::AcqRel);
                    mutations.push_mutation(
                        *zone_id,
                        ZoneMutation {
                            epoch: zone_epoch,
                            last_ms: 0,
                        },
                    )?;
                }
                if map.retired.load(Ordering::Acquire) {
                    return Err(ZoneMap::access_error("retired zone map"));
                }
                map.store_apply(&mutations)?;
            }
            drop(gate);
            if !fallback.zones.is_empty() {
                map.merge(&fallback)?;
            }
            map.rebuild_zones_done.store(
                progress.map_or(0, |(first, _)| first) + requested_zones.len() as u64,
                Ordering::Release,
            );
            Ok(Some(replaced))
        })();

        if matches!(result, Ok(Some(_))) {
            map.sync_derived_charge();
            let _gate = map.data_gate.write().unwrap_or_else(|e| e.into_inner());
            if map.invalidation_epoch.load(Ordering::Acquire) == invalidation_epoch {
                map.trusted_base.store(was_trusted, Ordering::Release);
            }
        }

        if result
            .as_ref()
            .is_ok_and(|count| count.is_some_and(|count| count > 0))
        {
            ZONE_MAP_REBUILDS.fetch_add(1, Ordering::Relaxed);
            if let Some(cache) = crate::engine::cache::global_plan_cache() {
                cache.invalidate_table(table.id);
            }
        }
        map.rebuild_state.store(
            match result {
                Ok(Some(_))
                    if progress.is_some_and(|(first, total)| {
                        first + (requested_zones.len() as u64) < total
                    }) =>
                {
                    1
                }
                Ok(Some(_)) => 2,
                Ok(None) => 0,
                Err(_) => 3,
            },
            Ordering::Release,
        );
        map.rebuild_finished_ms
            .store(now_unix_ms(), Ordering::Release);
        result
    }

    /// Check at most one metadata batch of the contiguous covered prefix.
    /// Presence is sufficient: Unknown records still refuse pruning, while
    /// Ready/Empty records were published through the ordinary MVCC fence.
    /// DML only inserts or widens metadata; only reset can erase this proof.
    fn advance_zone_map_coverage<F>(
        &self,
        storage_id: u64,
        map: &ZoneMap,
        total_zones: u64,
        should_cancel: &F,
    ) -> Result<bool, QueryError>
    where
        F: Fn() -> bool + ?Sized,
    {
        let (epoch, mutation_epoch, progressed, covered) = {
            let _gate = map.data_gate.write().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "zone map lock poisoned",
                )
            })?;
            if map.retired.load(Ordering::Acquire)
                || map.missing_is_empty.load(Ordering::Acquire)
                || !map.trusted_base.load(Ordering::Acquire)
                || !map.store.trusted()
            {
                return Ok(false);
            }
            let epoch = map.invalidation_epoch.load(Ordering::Acquire);
            if map.maintenance_coverage_epoch.load(Ordering::Relaxed) != epoch {
                map.maintenance_coverage_cursor.store(0, Ordering::Relaxed);
                map.maintenance_coverage_epoch
                    .store(epoch, Ordering::Relaxed);
            }
            let first = map
                .maintenance_coverage_cursor
                .load(Ordering::Relaxed)
                .min(total_zones);
            let end = first
                .saturating_add(ZONE_MAP_MAINTENANCE_MAX_ZONES_PER_PASS as u64)
                .min(total_zones);
            let mut covered = first;
            let meta_only = BTreeSet::new();
            while covered < end {
                if should_cancel() {
                    break;
                }
                if map.read_zone(covered, Some(&meta_only))?.is_none() {
                    break;
                }
                covered += 1;
            }
            map.maintenance_coverage_cursor
                .store(covered, Ordering::Relaxed);
            map.rebuild_zones_total
                .store(total_zones, Ordering::Release);
            map.rebuild_zones_done.store(covered, Ordering::Release);
            (
                epoch,
                map.mutation_epoch.load(Ordering::Acquire),
                covered != first,
                covered,
            )
        };
        if covered < total_zones || should_cancel() {
            return Ok(progressed);
        }
        // Never take a heap latch while holding the summary data gate. DML
        // orders its heap write before summary publication.
        let final_total = self
            .heap_page_count_for_storage(storage_id)?
            .div_ceil(u64::from(map.zone_pages));
        #[cfg(test)]
        if let Some(barrier) = map.coverage_publish_barrier.lock().unwrap().clone() {
            barrier.wait();
            barrier.wait();
        }
        let _gate = map.data_gate.write().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "zone map lock poisoned",
            )
        })?;
        if final_total == total_zones
            && !map.retired.load(Ordering::Acquire)
            && self.active_write_txns_atomic.load(Ordering::Acquire) == 0
            && map.invalidation_epoch.load(Ordering::Acquire) == epoch
            && map.mutation_epoch.load(Ordering::Acquire) == mutation_epoch
            && map.maintenance_coverage_epoch.load(Ordering::Relaxed) == epoch
            && map.maintenance_coverage_cursor.load(Ordering::Relaxed) >= final_total
            && map.trusted_base.load(Ordering::Acquire)
            && map.store.trusted()
            && !map.missing_is_empty.load(Ordering::Acquire)
            && !should_cancel()
        {
            // Concurrent new zones cannot become visible without their own
            // summary widening/invalidation. All zones at this watermark
            // already have explicit metadata, so only absent future zones
            // gain the Empty default; no existing Unknown becomes Ready.
            map.store.set_missing_is_empty(true)?;
            map.missing_is_empty.store(true, Ordering::Release);
            map.rebuild_state.store(2, Ordering::Release);
            map.rebuild_finished_ms
                .store(now_unix_ms(), Ordering::Release);
            return Ok(true);
        }
        Ok(progressed)
    }

    fn maintain_zone_map_storage<F>(
        &self,
        table: &TableSchema,
        storage_id: u64,
        map: &Arc<ZoneMap>,
        ratio: f64,
        now_ms: u64,
        should_stop: &F,
    ) -> Result<bool, QueryError>
    where
        F: Fn() -> bool + Sync + ?Sized,
    {
        if should_stop() {
            return Ok(false);
        }
        {
            let _gate = map.data_gate.write().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "zone map lock poisoned",
                )
            })?;
            if !map.trusted_base.load(Ordering::Acquire) || !map.store.trusted() {
                map.reset_unknown()?;
                map.generation.fetch_add(1, Ordering::AcqRel);
                map.mutation_epoch.fetch_add(1, Ordering::AcqRel);
                map.maintenance_cursor.store(0, Ordering::Relaxed);
                map.trusted_base.store(true, Ordering::Release);
                map.rebuild_started_ms
                    .store(now_unix_ms(), Ordering::Release);
            }
        }
        // Capture after initialization: reset cannot erase data from a zone
        // that was omitted because its heap pages were allocated meanwhile.
        let total_zones = self
            .heap_page_count_for_storage(storage_id)?
            .div_ceil(u64::from(map.zone_pages));
        let completing_base = !map.missing_is_empty.load(Ordering::Acquire);
        if completing_base {
            map.rebuild_zones_total
                .store(total_zones, Ordering::Release);
            map.rebuild_state.store(1, Ordering::Release);
        }
        if should_stop() {
            map.rebuild_state.store(0, Ordering::Release);
            return Ok(false);
        }
        if total_zones == 0 {
            return self.advance_zone_map_coverage(storage_id, map, 0, should_stop);
        }
        let first = map
            .maintenance_cursor
            .load(Ordering::Relaxed)
            .min(total_zones - 1);
        let end = first
            .saturating_add(ZONE_MAP_MAINTENANCE_MAX_ZONES_PER_PASS as u64)
            .min(total_zones);
        let mut planning_lease = ZoneScratchLease::default();
        planning_lease.reserve(
            table
                .columns
                .len()
                .saturating_mul(2 * std::mem::size_of::<(usize, u32)>())
                .saturating_add(zone_tree_bytes::<u32, ()>(table.columns.len())),
        )?;
        let required_columns = zone_map_relevant_columns(table)
            .into_iter()
            .map(|(_, id)| id)
            .collect::<BTreeSet<_>>();
        planning_lease.shrink_to(zone_tree_bytes::<u32, ()>(required_columns.len()));
        let cached_membership = map.cached_membership_columns();
        planning_lease.reserve(zone_tree_bytes::<u32, ()>(cached_membership.len()))?;
        let membership_columns = cached_membership
            .iter()
            .map(|(column_id, _)| *column_id)
            .filter(|column_id| required_columns.contains(column_id))
            .collect::<BTreeSet<_>>();
        let mut sampled_lease = ZoneScratchLease::default();
        let mut sampled = BTreeMap::new();
        let mut eligible_lease = ZoneScratchLease::default();
        let mut eligible = BTreeSet::new();
        for id in first..end {
            if should_stop() {
                map.rebuild_state.store(0, Ordering::Release);
                return Ok(false);
            }
            let _gate = map.data_gate.read().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "zone map lock poisoned",
                )
            })?;
            if map.zone_mutation(id)?.is_none_or(|mutation| {
                mutation.last_ms == 0
                    || now_ms.saturating_sub(mutation.last_ms) >= zone_map_maintenance_quiet_ms()
            }) {
                eligible_lease.reserve(
                    zone_tree_bytes::<u64, ()>(eligible.len() + 1)
                        .saturating_sub(zone_tree_bytes::<u64, ()>(eligible.len())),
                )?;
                eligible.insert(id);
                if let Some(zone) = map.read_zone(id, Some(&required_columns))? {
                    sampled_lease.reserve(
                        zone_tree_bytes::<u64, ChargedZoneSummary>(sampled.len() + 1)
                            .saturating_sub(zone_tree_bytes::<u64, ChargedZoneSummary>(
                                sampled.len(),
                            )),
                    )?;
                    sampled.insert(id, zone);
                }
            }
        }
        let mut targets = zone_map_maintenance_targets_in_range(
            &sampled,
            first,
            end,
            ratio,
            &required_columns,
            &membership_columns,
        );
        targets.retain(|id| eligible.contains(id));
        let repaired = if targets.is_empty() {
            false
        } else {
            ZONE_MAP_MAINTENANCE_LAG_MS.store(
                now_ms.saturating_sub(map.last_mutation_ms.load(Ordering::Acquire)),
                Ordering::Relaxed,
            );
            let result = self.rebuild_zone_map_for_storage_zones(
                table,
                storage_id,
                &targets,
                should_stop,
                None,
            )?;
            if result.is_none() {
                map.rebuild_state.store(0, Ordering::Release);
                return Ok(false);
            }
            true
        };
        // Rotate even when this entire window is hot. A hot early zone must
        // not keep later cold zones from receiving their bounded repair turn.
        map.maintenance_cursor
            .store(if end == total_zones { 0 } else { end }, Ordering::Relaxed);
        let coverage = self.advance_zone_map_coverage(storage_id, map, total_zones, should_stop)?;
        if !map.missing_is_empty.load(Ordering::Acquire) {
            map.rebuild_state
                .store(if should_stop() { 0 } else { 1 }, Ordering::Release);
        }
        Ok(repaired || coverage)
    }

    #[cfg(test)]
    pub fn maintain_zone_maps(&self) -> Result<u64, QueryError> {
        self.maintain_zone_maps_until(|| false)
    }

    /// Visit every storage once per pass so a large table cannot consume all
    /// ten batches before other tables get a turn. Each pass releases its
    /// scratch memory and operation guard before the next one starts.
    pub(crate) fn maintain_zone_maps_round_until<F>(
        &self,
        should_cancel: F,
    ) -> Result<u64, QueryError>
    where
        F: Fn() -> bool + Sync,
    {
        let mut rebuilt = 0;
        let mut first_error = None;
        for _ in 0..ZONE_MAP_MAINTENANCE_PASSES_PER_ROUND {
            if should_cancel() {
                break;
            }
            match self.maintain_zone_maps_until(&should_cancel) {
                Ok(count) => rebuilt += count,
                Err(err) => {
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
            }
            // Zero repairs can still advance past a Ready or hot prefix.
            // Do not stop here: later batches may contain cold Unknown zones.
        }
        first_error.map_or(Ok(rebuilt), Err)
    }

    /// Run one online maintenance pass, abandoning derived rebuild work as
    /// soon as `should_cancel` becomes true. Cancellation is not an error:
    /// the unpublished scratch generation is discarded and the map remains
    /// eligible for a later retry.
    pub(crate) fn maintain_zone_maps_until<F>(&self, should_cancel: F) -> Result<u64, QueryError>
    where
        F: Fn() -> bool + Sync,
    {
        if self.physical_replica_mode() {
            return Ok(0);
        }
        let Some(_zone_operation) = self.zone_map_operation() else {
            return Ok(0);
        };
        if !self.zone_map_maintenance() {
            return Ok(0);
        }
        // A bulk LOAD deliberately leaves zones Unknown until its generation
        // is complete. Starting a background rebuild against that moving
        // generation wastes I/O/CPU, oversubscribes the configured LOAD
        // degree, and can make shutdown wait for a scan whose result cannot
        // be published. Treat an active LOAD exactly like cancellation; the
        // next maintenance pass will see the committed generation.
        let should_stop = || {
            should_cancel()
                || self.physical_replica_mode()
                || !self.zone_map_maintenance()
                || self.foreground_bulk_ddl_is_active()
                || self.analyze_scan_is_active()
                || self.dml_stats.load_data_active.load(Ordering::Relaxed) != 0
        };
        if should_stop() {
            return Ok(0);
        }
        let ratio = self.zone_map_rebuild_ratio();
        let tables = self
            .catalog_read()?
            .tables
            .values()
            .filter(|table| table.zone_map_enabled())
            .cloned()
            .collect::<Vec<_>>();
        let mut rebuilt = 0u64;
        let mut first_error = None;
        for header in tables {
            let table = self.table(&header.qualified_name())?;
            for storage_id in table.storage_ids() {
                if should_stop() {
                    return Ok(rebuilt);
                }
                let Some(map) = self.zone_map_for(storage_id, &table) else {
                    continue;
                };
                let now_ms = now_unix_ms();
                if now_ms < map.maintenance_retry_ms.load(Ordering::Acquire) {
                    continue;
                }
                let outcome = self.maintain_zone_map_storage(
                    &table,
                    storage_id,
                    &map,
                    ratio,
                    now_ms,
                    &should_stop,
                );
                match outcome {
                    Ok(true) => {
                        rebuilt += 1;
                        *map.rebuild_error.lock().unwrap_or_else(|e| e.into_inner()) = None;
                        map.maintenance_retry_ms.store(0, Ordering::Release);
                    }
                    Ok(false) => {}
                    Err(err) => {
                        *map.rebuild_error.lock().unwrap_or_else(|e| e.into_inner()) =
                            Some(err.message().to_string());
                        map.record_write_error(&err);
                        map.rebuild_state.store(3, Ordering::Release);
                        map.rebuild_finished_ms
                            .store(now_unix_ms(), Ordering::Release);
                        map.maintenance_retry_ms.store(
                            now_unix_ms().saturating_add(self.zone_map_maintenance_retry_ms()),
                            Ordering::Release,
                        );
                        if first_error.is_none() {
                            first_error = Some(err);
                        }
                    }
                }
            }
        }
        // Direct callers still receive failure; the background loop can retry
        // while every other table has already received its maintenance turn.
        match first_error {
            Some(err) => Err(err),
            None => Ok(rebuilt),
        }
    }

    pub fn zone_map_status_rows(&self) -> Vec<(&'static str, String)> {
        // Read only resident-page counters and light handle metadata. SHOW
        // must not walk the paged directory or load cold zone summaries.
        let resident_pages = self.buffer_pool.kind_used_pages[4].load(Ordering::Relaxed);
        let (handles, handle_bytes, handle_charged_bytes) = self
            .zone_maps
            .read()
            .map(|maps| {
                maps.values().fold(
                    (maps.len(), 0usize, 0usize),
                    |(count, bytes, charged), map| {
                        (
                            count,
                            bytes.saturating_add(map.estimated_memory_bytes()),
                            charged.saturating_add(map.charged_memory_bytes()),
                        )
                    },
                )
            })
            .unwrap_or_default();
        vec![
            (
                "zone_map_enabled",
                if self.zone_map_enabled() { "ON" } else { "OFF" }.into(),
            ),
            ("zone_map_resident_pages", resident_pages.to_string()),
            (
                "zone_map_resident_bytes",
                resident_pages.saturating_mul(PAGE_SIZE as u64).to_string(),
            ),
            ("zone_map_handles", handles.to_string()),
            ("zone_map_handle_bytes", handle_bytes.to_string()),
            (
                "zone_map_handle_charged_bytes",
                handle_charged_bytes.to_string(),
            ),
            (
                "zone_map_handle_hits",
                ZONE_MAP_CACHE_HITS.load(Ordering::Relaxed).to_string(),
            ),
            (
                "zone_map_handle_misses",
                ZONE_MAP_CACHE_MISSES.load(Ordering::Relaxed).to_string(),
            ),
            (
                "zone_map_zones_skipped",
                ZONE_MAP_ZONES_SKIPPED.load(Ordering::Relaxed).to_string(),
            ),
            (
                "zone_map_membership_checks",
                ZONE_MAP_MEMBERSHIP_CHECKS
                    .load(Ordering::Relaxed)
                    .to_string(),
            ),
            (
                "zone_map_membership_rejections",
                ZONE_MAP_MEMBERSHIP_REJECTIONS
                    .load(Ordering::Relaxed)
                    .to_string(),
            ),
            (
                "zone_map_membership_saturations",
                ZONE_MAP_MEMBERSHIP_SATURATIONS
                    .load(Ordering::Relaxed)
                    .to_string(),
            ),
            (
                "zone_map_memory_fallbacks",
                ZONE_MAP_MEMORY_FALLBACKS
                    .load(Ordering::Relaxed)
                    .to_string(),
            ),
            (
                "zone_map_fallback_unsupported",
                ZONE_MAP_FALLBACK_UNSUPPORTED
                    .load(Ordering::Relaxed)
                    .to_string(),
            ),
            (
                "zone_map_fallback_untrusted",
                ZONE_MAP_FALLBACK_UNTRUSTED
                    .load(Ordering::Relaxed)
                    .to_string(),
            ),
            (
                "zone_map_fallback_corrupt",
                ZONE_MAP_FALLBACK_CORRUPT
                    .load(Ordering::Relaxed)
                    .to_string(),
            ),
            (
                "zone_map_fallback_invalid_range",
                ZONE_MAP_FALLBACK_INVALID_RANGE
                    .load(Ordering::Relaxed)
                    .to_string(),
            ),
            (
                "zone_map_candidate_generations",
                ZONE_MAP_CANDIDATE_GENERATIONS
                    .load(Ordering::Relaxed)
                    .to_string(),
            ),
            (
                "zone_map_merges",
                ZONE_MAP_MERGES.load(Ordering::Relaxed).to_string(),
            ),
            (
                "zone_map_rebuilds",
                ZONE_MAP_REBUILDS.load(Ordering::Relaxed).to_string(),
            ),
            (
                "zone_map_rebuild_shrink_zones",
                ZONE_MAP_REBUILD_SHRINK_ZONES
                    .load(Ordering::Relaxed)
                    .to_string(),
            ),
            (
                "zone_map_rebuild_union_zones",
                ZONE_MAP_REBUILD_UNION_ZONES
                    .load(Ordering::Relaxed)
                    .to_string(),
            ),
            (
                "zone_map_maintenance_lag_ms",
                ZONE_MAP_MAINTENANCE_LAG_MS
                    .load(Ordering::Relaxed)
                    .to_string(),
            ),
        ]
    }
}

#[cfg(test)]
mod zone_map_tests {
    use super::*;
    use crate::parser::BinaryOp;

    #[test]
    fn sampled_cost_floor_never_exceeds_estimator_outcomes() {
        // Exact estimates can prove every zone empty.
        assert_eq!(zone_map_min_sampled_surviving_pages(0, 64), 0);
        assert_eq!(zone_map_min_sampled_surviving_pages(64 * 4096, 64), 0);
        for (page_count, zone_pages) in [
            (64 * 4097, 64),
            (64 * 8982 + 17, 64),
            (1_000_003, 1),
            (u32::MAX as u64, 64),
        ] {
            let (total_zones, step, samples) = zone_map_sample_geometry(page_count, zone_pages);
            assert!(step > 1 || samples < total_zones);
            let floor = zone_map_min_sampled_surviving_pages(page_count, zone_pages);
            assert!(floor > 0);
            // The estimator's two sampled outcomes: no survivor keeps a full
            // stride; any survivor scales by at least 1/samples.
            let no_survivor = step.saturating_mul(zone_pages).min(page_count);
            let one_survivor = ((page_count as f64) / samples as f64).ceil() as u64;
            assert!(floor <= no_survivor && floor <= one_survivor);
        }
    }

    struct PagedTestMap {
        map: ZoneMap,
        pool: Arc<BufferPool>,
        log: Arc<parking_lot::Mutex<LogManager>>,
        path: PathBuf,
        _dir: tempfile::TempDir,
    }

    impl std::ops::Deref for PagedTestMap {
        type Target = ZoneMap;
        fn deref(&self) -> &Self::Target {
            &self.map
        }
    }

    fn paged_test_map(storage_id: u64, zone_pages: u32, fingerprint: u64) -> PagedTestMap {
        fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool = BufferPool::new(
            16,
            Arc::clone(&log),
            DurabilityMode::Strict,
            Duration::from_secs(5),
        );
        let path =
            crate::storage::naming::table_object_path(dir.path(), "test", "zone", storage_id, "zm");
        let file = zone_map_page::PageFile::new(
            path.clone(),
            storage_id,
            1,
            Arc::clone(&pool),
            Arc::clone(&log),
            DurabilityMode::Strict,
        )
        .unwrap();
        let store =
            Arc::new(zone_map_page::mutable::Store::create(file, fingerprint, zone_pages).unwrap());
        store.set_missing_is_empty(true).unwrap();
        let map = ZoneMap::new(storage_id, zone_pages, fingerprint, store, true);
        PagedTestMap {
            map,
            pool,
            log,
            path,
            _dir: dir,
        }
    }

    impl PagedTestMap {
        fn reopen(&self) -> ZoneMap {
            self.map.write_to_file(0).unwrap();
            self.pool.flush_and_evict_all().unwrap();
            let file = zone_map_page::PageFile::new(
                self.path.clone(),
                self.storage_id,
                1,
                Arc::clone(&self.pool),
                Arc::clone(&self.log),
                DurabilityMode::Strict,
            )
            .unwrap();
            let store = Arc::new(
                zone_map_page::mutable::Store::open_existing(
                    file,
                    self.schema_fingerprint,
                    self.zone_pages,
                )
                .unwrap(),
            );
            let reopened = ZoneMap::new(
                self.storage_id,
                self.zone_pages,
                self.schema_fingerprint,
                store,
                true,
            );
            *reopened.columns.write().unwrap() = self.columns.read().unwrap().clone();
            reopened
        }
    }

    fn paged_delta(value: i64) -> ZoneMapDeltaBatch {
        let mut summary = ZoneColumnSummary::default();
        summary.enable_membership();
        summary.merge_value(ZoneScalar::Signed(value));
        ZoneMapDeltaBatch {
            storage_id: 1,
            zone_pages: 64,
            schema_fingerprint: 99,
            zones: BTreeMap::from([(
                0,
                ZoneSummary {
                    columns: BTreeMap::from([(7, summary)]),
                    ..ZoneSummary::default()
                },
            )]),
        }
    }

    #[test]
    fn zone_map_widening_does_not_wait_for_a_concurrent_reader() {
        let map = paged_test_map(1, 64, 99);
        let gate = map.data_gate.read().unwrap();
        let (send, recv) = std::sync::mpsc::channel();
        let completed = std::thread::scope(|scope| {
            scope.spawn(|| send.send(map.merge(&paged_delta(1))).unwrap());
            let completed = recv.recv_timeout(Duration::from_millis(250));
            drop(gate);
            if completed.is_err() {
                recv.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
            }
            completed
        });
        completed
            .expect("conservative widening must coexist with a reader")
            .unwrap();
    }

    #[test]
    fn zone_map_merge_latches_exclude_same_zone_but_allow_other_zones() {
        let map = paged_test_map(1, 64, 99);
        let zone = map.merge_latches[0].lock().unwrap();
        let (send, recv) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            scope.spawn(|| send.send(map.merge(&paged_delta(1))).unwrap());
            let mut other = paged_delta(2);
            let value = other.zones.remove(&0).unwrap();
            other.zones.insert(1, value);
            map.merge(&other).unwrap();
            assert!(recv.recv_timeout(Duration::from_millis(50)).is_err());
            drop(zone);
            recv.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
        });
        assert!(map.read_zone(0, None).unwrap().is_some());
        assert!(map.read_zone(1, None).unwrap().is_some());
    }

    #[test]
    fn zone_map_concurrent_same_zone_unions_preserve_bounds_membership_and_counts() {
        // 32 values stay within the exact fingerprint set, which foreground
        // DML maintains synchronously.
        let map = paged_test_map(1, 64, 99);
        std::thread::scope(|scope| {
            for worker in 0..4 {
                let map = &map;
                scope.spawn(move || {
                    for value in worker * 8..(worker + 1) * 8 {
                        let mut batch = paged_delta(value);
                        let zone = batch.zones.get_mut(&0).unwrap();
                        zone.row_count_estimate = 1;
                        zone.columns
                            .get_mut(&7)
                            .unwrap()
                            .merge_membership_value(&ZoneScalar::Signed(value));
                        map.merge(&batch).unwrap();
                    }
                });
            }
        });
        for map in [&map.map, &map.reopen()] {
            let zone = map.read_zone(0, None).unwrap().unwrap();
            assert_eq!(zone.row_count_estimate, 32);
            let column = &zone.columns[&7];
            assert_eq!(column.min, Some(ZoneScalar::Signed(0)));
            assert_eq!(column.max, Some(ZoneScalar::Signed(31)));
            for value in 0..32 {
                assert!(column
                    .membership
                    .as_ref()
                    .unwrap()
                    .may_contain_hash(zone_membership_hash(&ZoneScalar::Signed(value))));
            }
        }
    }

    #[test]
    fn zone_map_foreground_dml_drops_growing_filter_and_schedules_rebuild() {
        let map = paged_test_map(1, 64, 99);
        for value in 0..64 {
            let mut batch = paged_delta(value);
            let zone = batch.zones.get_mut(&0).unwrap();
            zone.row_count_estimate = 1;
            zone.columns
                .get_mut(&7)
                .unwrap()
                .merge_membership_value(&ZoneScalar::Signed(value));
            map.merge(&batch).unwrap();
            let zone = map.read_zone(0, None).unwrap().unwrap();
            let column = &zone.columns[&7];
            if value < 32 {
                assert!(column
                    .membership
                    .as_ref()
                    .is_some_and(|membership| membership
                        .may_contain_hash(zone_membership_hash(&ZoneScalar::Signed(value)))));
            } else {
                // Promotion to a Bloom filter is left to maintenance.
                assert!(column.membership.is_none(), "value {value}");
            }
        }
        for map in [&map.map, &map.reopen()] {
            let zone = map.read_zone(0, None).unwrap().unwrap();
            assert_eq!(zone.row_count_estimate, 64);
            let column = &zone.columns[&7];
            // Bounds keep pruning exactly; only the membership proof is gone.
            assert_eq!(column.state, ZoneMapState::Ready);
            assert_eq!(column.min, Some(ZoneScalar::Signed(0)));
            assert_eq!(column.max, Some(ZoneScalar::Signed(63)));
            assert!(column.membership.is_none());
            let zones = BTreeMap::from([(0, zone)]);
            let required = BTreeSet::from([7]);
            assert!(zone_map_maintenance_targets_in_range(
                &zones,
                0,
                1,
                1.0,
                &required,
                &BTreeSet::new()
            )
            .is_empty());
            assert!(
                zone_map_maintenance_targets_in_range(&zones, 0, 1, 1.0, &required, &required)
                    .contains(&0)
            );
        }
    }

    #[test]
    fn zone_map_queued_same_zone_widenings_publish_as_one_union() {
        let map = paged_test_map(1, 64, 99);
        let latch = map.merge_latches[0].lock().unwrap();
        let (send, recv) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            for value in 0..4 {
                let (map, send) = (&map, send.clone());
                scope.spawn(move || {
                    let mut batch = paged_delta(value);
                    let zone = batch.zones.get_mut(&0).unwrap();
                    zone.row_count_estimate = 1;
                    zone.columns
                        .get_mut(&7)
                        .unwrap()
                        .merge_membership_value(&ZoneScalar::Signed(value));
                    send.send(map.merge(&batch)).unwrap();
                });
            }
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while map.merge_queues[0].lock().unwrap().len() < 4 {
                assert!(std::time::Instant::now() < deadline);
                std::thread::yield_now();
            }
            assert!(recv.recv_timeout(Duration::from_millis(50)).is_err());
            drop(latch);
            for _ in 0..4 {
                recv.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
            }
        });
        assert!(map.merge_queues[0].lock().unwrap().is_empty());
        for map in [&map.map, &map.reopen()] {
            let zone = map.read_zone(0, None).unwrap().unwrap();
            assert_eq!(zone.row_count_estimate, 4);
            let column = &zone.columns[&7];
            assert_eq!(column.min, Some(ZoneScalar::Signed(0)));
            assert_eq!(column.max, Some(ZoneScalar::Signed(3)));
            for value in 0..4 {
                assert!(column
                    .membership
                    .as_ref()
                    .unwrap()
                    .may_contain_hash(zone_membership_hash(&ZoneScalar::Signed(value))));
            }
        }
    }

    struct ZoneMapFailpointGuard;

    impl ZoneMapFailpointGuard {
        fn install(stage: u8) -> Self {
            ZONE_MAP_FAILPOINT_STAGE.store(stage, Ordering::Release);
            Self
        }
    }

    impl Drop for ZoneMapFailpointGuard {
        fn drop(&mut self) {
            ZONE_MAP_FAILPOINT_STAGE.store(0, Ordering::Release);
        }
    }

    fn count_zone_map_payloads(payload: &WalPayload) -> usize {
        match payload {
            WalPayload::ZoneMapMergeBatch { .. } => 1,
            WalPayload::TxnBatch { records } => records.iter().map(count_zone_map_payloads).sum(),
            _ => 0,
        }
    }

    fn collect_zone_prepare_order(payload: &WalPayload, out: &mut Vec<&'static str>) {
        match payload {
            WalPayload::FullPageImage { path, .. }
            | WalPayload::FullPageImageCompressed { path, .. }
            | WalPayload::PageDelta { path, .. }
                if path.ends_with(".zm") =>
            {
                out.push("zone")
            }
            WalPayload::PrepareTxn { .. } => out.push("prepare"),
            WalPayload::TxnBatch { records } => {
                for payload in records {
                    collect_zone_prepare_order(payload, out);
                }
            }
            _ => {}
        }
    }

    fn zone_map_test_column() -> ColumnDef {
        ColumnDef {
            name: "id".into(),
            data_type: DataType::Int,
            unsigned: false,
            zerofill: false,
            invisible: false,
            nullable: false,
            primary_key: true,
            unique: false,
            auto_increment: false,
            default: None,
            on_update: None,
            checks: Vec::new(),
            generated: None,
            charset: None,
            collation: None,
        }
    }

    #[test]
    fn zone_map_audit_float_boundaries_match_row_comparison() {
        use crate::parser::BinaryOp;
        let dir = tempfile::Builder::new()
            .prefix("zone-map-")
            .tempdir_in("/tmp/mydb")
            .unwrap();
        let engine =
            crate::engine::StorageQueryEngine::open_with_storage_config_without_background_workers(
                dir.path(),
                StorageConfig::from_env(),
            )
            .unwrap();
        engine
            .execute("CREATE TABLE t (f FLOAT, d DOUBLE) ZONE_SIZE=64K")
            .unwrap();
        let table = engine
            .db_for_tests()
            .snapshot_table_schema("test.t")
            .unwrap();
        for stored in [
            0.0f32,
            -0.0,
            5.25,
            -5.25,
            f32::MIN_POSITIVE,
            f32::MAX,
            f32::INFINITY,
            f32::NEG_INFINITY,
        ] {
            let mut summary = ZoneColumnSummary::default();
            summary.merge_value(
                zone_scalar_physical(&table.columns[0], &Value::Float(stored)).unwrap(),
            );
            for query in [
                Value::Double(stored as f64),
                Value::Double(stored as f64 + 0.0000001),
                Value::Double(stored as f64 - 0.0000001),
                Value::Int(1),
                Value::UInt(u64::MAX),
                Value::Decimal("5.2499999".into()),
            ] {
                let scalar = zone_query_scalar(&table.columns[0], &query).unwrap();
                let cmp =
                    crate::engine::expr::compare_values(&Value::Float(stored), &query).unwrap();
                for op in [
                    BinaryOp::Eq,
                    BinaryOp::NotEq,
                    BinaryOp::Lt,
                    BinaryOp::LtEq,
                    BinaryOp::Gt,
                    BinaryOp::GtEq,
                ] {
                    let matches = match op {
                        BinaryOp::Eq => cmp.is_eq(),
                        BinaryOp::NotEq => !cmp.is_eq(),
                        BinaryOp::Lt => cmp.is_lt(),
                        BinaryOp::LtEq => !cmp.is_gt(),
                        BinaryOp::Gt => cmp.is_gt(),
                        BinaryOp::GtEq => !cmp.is_lt(),
                        _ => unreachable!(),
                    };
                    assert_eq!(
                        comparison_may_match(&summary, op, &scalar),
                        matches,
                        "{stored:?} {op:?} {query:?}"
                    );
                }
            }
        }
        assert!(zone_query_scalar(&table.columns[0], &Value::Double(f64::NAN)).is_none());
    }

    #[test]
    fn zone_map_paged_revocation_survives_checkpoint_and_restart() {
        let (dir, engine) = switch_engine();
        engine.execute("CREATE TABLE zm (v INT)").unwrap();
        engine.execute("INSERT INTO zm VALUES (1)").unwrap();
        engine.execute("ANALYZE TABLE zm").unwrap();
        let db = engine.db_for_tests();
        db.set_zone_map_enabled(false).unwrap();
        engine.execute("UPDATE zm SET v=1000").unwrap();
        db.checkpoint().unwrap();
        drop(engine);
        let engine =
            crate::engine::StorageQueryEngine::open_with_storage_config_without_background_workers(
                dir.path(),
                StorageConfig::from_env(),
            )
            .unwrap();
        let crate::engine::QueryResponse::ResultSet(result) = engine
            .execute("SELECT /*+ ZONE_MAP(zm) */ v FROM zm WHERE v=1000")
            .unwrap()
        else {
            panic!("result")
        };
        assert_eq!(result.rows, vec![vec![Value::Int(1000)]]);
    }

    #[test]
    fn zone_map_paged_repeated_revocation_and_repair_is_conservative() {
        let map = paged_test_map(1, 64, 99);
        map.merge(&paged_delta(42)).unwrap();
        map.store.invalidate().unwrap();
        map.store.invalidate().unwrap();
        assert!(map.read_zone(0, None).is_err());
        {
            let _gate = map.data_gate.write().unwrap();
            map.reset_unknown().unwrap();
        }
        assert!(map.read_zone(0, None).unwrap().is_none());
        // A conservative delta cannot turn a missing unknown base into Ready.
        map.merge(&paged_delta(1000)).unwrap();
        assert_eq!(
            map.read_zone(0, Some(&BTreeSet::from([7])))
                .unwrap()
                .unwrap()
                .state,
            Some(ZoneMapState::Unknown)
        );
        {
            let _gate = map.data_gate.write().unwrap();
            map.replace_zones(&paged_delta(1000).zones).unwrap();
        }
        assert_eq!(
            map.reopen()
                .read_zone(0, Some(&BTreeSet::from([7])))
                .unwrap()
                .unwrap()
                .columns[&7]
                .min,
            Some(ZoneScalar::Signed(1000))
        );
    }

    #[test]
    fn zone_map_paged_malformed_legacy_redo_never_restores_trust() {
        let (_dir, engine) = switch_engine();
        engine.execute("CREATE TABLE zm (v INT)").unwrap();
        engine.execute("INSERT INTO zm VALUES (1)").unwrap();
        engine.execute("ANALYZE TABLE zm").unwrap();
        let db = engine.db_for_tests();
        let table = db.table("test.zm").unwrap();
        assert!(db.apply_zone_map_wal(b"unknown-header").is_err());
        let predicate = Expr::Binary {
            left: Box::new(Expr::Identifier("v".into())),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Int(1))),
        };
        assert!(db
            .zone_map_candidate_ranges(&table, &predicate)
            .is_none_or(|ranges| !ranges.is_empty()));
    }

    #[test]
    fn zone_map_paged_maintenance_resets_row_estimates() {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine =
            crate::engine::StorageQueryEngine::open_with_storage_config_without_background_workers(
                dir.path(),
                StorageConfig::from_env(),
            )
            .unwrap();
        engine
            .execute("CREATE TABLE t (v INT) ZONE_SIZE=64K")
            .unwrap();
        engine
            .execute("INSERT INTO t SELECT n FROM generate_series(1,500) AS gs(n)")
            .unwrap();
        engine.execute("ANALYZE TABLE t").unwrap();
        engine.execute("DELETE FROM t WHERE v>50").unwrap();
        let db = engine.db_for_tests();
        let table = db.snapshot_table_schema("test.t").unwrap();
        let map = db.zone_map_for(table.id, &table).unwrap();
        db.set_zone_map_maintenance_quiet_ms_for_tests(0);
        let outcome = db.maintain_zone_maps();
        db.set_zone_map_maintenance_quiet_ms_for_tests(30_000);
        outcome.unwrap();
        assert_eq!(
            map.snapshot_for_tests()
                .values()
                .map(|zone| zone.row_count_estimate)
                .sum::<u64>(),
            50
        );
    }

    #[test]
    fn zone_map_paged_fresh_wide_unselected_membership_stays_absent() {
        use zone_map_page::{summary, Kind};
        let map = paged_test_map(1, 64, 99);
        let mut zone = ZoneSummary {
            state: Some(ZoneMapState::Ready),
            row_count_estimate: 1,
            ..Default::default()
        };
        for column in 0..128 {
            let mut bounds = ZoneColumnSummary::default();
            bounds.observe_scalar(ZoneScalar::Signed(column as i64));
            zone.columns.insert(column, bounds);
        }
        let zones = BTreeMap::from([(0, zone)]);
        {
            let _gate = map.data_gate.write().unwrap();
            map.replace_zones(&zones).unwrap();
            // Repeated repair of unselected columns must not create None records.
            map.replace_zones(&zones).unwrap();
        }
        let reopened = map.reopen();
        for column in 0..128 {
            assert!(reopened
                .store
                .get(summary::key(column, Kind::Bounds, 0), |bytes| Ok(
                    bytes.is_some()
                ))
                .unwrap());
            assert!(reopened
                .store
                .get(summary::key(column, Kind::Membership, 0), |bytes| Ok(
                    bytes.is_none()
                ))
                .unwrap());
        }
        assert_eq!(
            reopened.read_zone(0, None).unwrap().unwrap().columns.len(),
            128
        );
    }

    #[test]
    fn zone_map_paged_membership_clear_reset_and_repair_keep_new_values() {
        use zone_map_page::{summary, Kind};
        let map = paged_test_map(1, 64, 99);
        let make_zone = |values: &[i64], membership: bool| {
            let mut bounds = ZoneColumnSummary::default();
            if membership {
                bounds.enable_membership();
            }
            for &value in values {
                bounds.observe_scalar(ZoneScalar::Signed(value));
            }
            ZoneSummary {
                state: Some(ZoneMapState::Ready),
                row_count_estimate: values.len() as u64,
                columns: BTreeMap::from([(7, bounds)]),
                ..Default::default()
            }
        };
        let predicate = |value| ZonePredicateProgram::Compare {
            column_id: 7,
            op: BinaryOp::Eq,
            query: ZoneScalar::Signed(value),
        };
        let key = summary::key(7, Kind::Membership, 0);
        {
            let _gate = map.data_gate.write().unwrap();
            map.replace_zones(&BTreeMap::from([(0, make_zone(&[10, 30], true))]))
                .unwrap();
            assert!(!map.may_match_zone(0, &predicate(20), u64::MAX).unwrap());
            let cleared = make_zone(&[10, 20, 30], false);
            map.replace_zones(&BTreeMap::from([(0, cleared.clone())]))
                .unwrap();
            assert!(map.may_match_zone(0, &predicate(20), u64::MAX).unwrap());
            assert!(map
                .store
                .get(key, |bytes| {
                    let bytes = bytes.expect("old filter requires an explicit clear");
                    Ok(summary::decode_membership(bytes)?.is_none())
                })
                .unwrap());
            assert!(!map
                .encoded_zone(0, &cleared)
                .unwrap()
                .iter()
                .any(|(entry, _)| *entry == key));
        }
        assert!(map
            .reopen()
            .may_match_zone(0, &predicate(20), u64::MAX)
            .unwrap());
        {
            let _gate = map.data_gate.write().unwrap();
            map.replace_zones(&BTreeMap::from([(0, make_zone(&[100, 200, 300], true))]))
                .unwrap();
            assert!(map.may_match_zone(0, &predicate(200), u64::MAX).unwrap());
            assert!(!map.may_match_zone(0, &predicate(150), u64::MAX).unwrap());
            map.reset_unknown().unwrap();
            map.replace_zones(&BTreeMap::from([(0, make_zone(&[100, 150, 300], false))]))
                .unwrap();
            assert!(map.may_match_zone(0, &predicate(150), u64::MAX).unwrap());
            assert!(map.store.get(key, |bytes| Ok(bytes.is_none())).unwrap());
        }
        let reopened = map.reopen();
        assert!(reopened
            .may_match_zone(0, &predicate(150), u64::MAX)
            .unwrap());
        assert_eq!(
            reopened.read_zone(0, None).unwrap().unwrap().state,
            Some(ZoneMapState::Ready)
        );
    }

    #[test]
    fn zone_map_paged_neutral_merge_does_not_allocate_or_log() {
        let map = paged_test_map(1, 64, 99);
        map.merge(&paged_delta(42)).unwrap();
        map.write_to_file(0).unwrap();
        let size = fs::metadata(&map.path).unwrap().len();
        let before = map.log.lock().last_lsn();
        // Re-applying identical encoded records changes neither summary nor
        // mutation metadata, unlike another foreground DML merge.
        let zone = map
            .read_zone(0, Some(&BTreeSet::from([7])))
            .unwrap()
            .unwrap();
        map.store
            .apply(&map.encoded_zone(0, &zone).unwrap())
            .unwrap();
        map.write_to_file(0).unwrap();
        assert_eq!(fs::metadata(&map.path).unwrap().len(), size);
        assert_eq!(map.log.lock().last_lsn(), before);
    }

    #[test]
    fn rebuild_parallel_degree_inherits_process_setting_unless_overridden() {
        crate::util::env::with_vars(
            &[
                ("MYDB_PARALLEL_DEGREE", Some("8")),
                ("MYDB_ZONE_MAP_REBUILD_THREADS", None),
            ],
            || {
                assert_eq!(rebuild_parallel_degree(1024, 64), 8);
                crate::util::env::with_vars(
                    &[("MYDB_ZONE_MAP_REBUILD_THREADS", Some("2"))],
                    || assert_eq!(rebuild_parallel_degree(1024, 64), 2),
                );
                crate::util::env::with_vars(&[("MYDB_PARALLEL_DEGREE", Some("1"))], || {
                    assert_eq!(rebuild_parallel_degree(1024, 64), 1)
                });
            },
        );
    }

    #[test]
    fn rebuild_parallel_degree_respects_configured_workers_and_zone_boundaries() {
        assert_eq!(rebuild_parallel_degree_with_threads(1024, 64, 4), 4);
        assert_eq!(rebuild_parallel_degree_with_threads(1024, 64, 8), 8);
        assert_eq!(rebuild_parallel_degree_with_threads(1024, 64, 1), 1);
        assert_eq!(rebuild_parallel_degree_with_threads(256, 64, 8), 4);
        assert_eq!(rebuild_parallel_degree_with_threads(1024, 0, 8), 1);
        assert_eq!(rebuild_parallel_degree_with_threads(255, 64, 4), 1);
        assert_eq!(rebuild_parallel_degree_with_threads(256, 256, 4), 1);
    }

    #[test]
    fn zone_map_paged_cache_uses_buffer_pool_quota_and_rejects_removed_settings() {
        let (_dir, engine) = switch_engine();
        engine
            .execute("SET GLOBAL mydb_buffer_pool_quota_zone_map_pct=15")
            .unwrap();
        assert_eq!(engine.db_for_tests().buffer_pool_quota_zone_map_pct(), 15);
        for setting in [
            "mydb_zone_map_cache_mb",
            "mydb_zone_map_compact_ratio",
            "mydb_zone_map_max_frame_versions",
        ] {
            assert!(
                engine.execute(&format!("SET GLOBAL {setting}=10")).is_err(),
                "removed {setting} must not silently succeed"
            );
        }
    }

    #[test]
    fn maintenance_targets_holes_in_a_trusted_zone_map() {
        let ready = |value: i64| {
            let mut summary = ZoneColumnSummary::default();
            summary.merge_value(ZoneScalar::Signed(value));
            let mut zone = ZoneSummary::default();
            zone.state = Some(ZoneMapState::Ready);
            zone.row_count_estimate = 100;
            zone.columns.insert(1, summary);
            zone
        };
        let zones = BTreeMap::from([(0, ready(10)), (2, ready(20)), (5, ready(30))]);

        let required = BTreeSet::from([1]);
        let targets = zone_map_maintenance_targets(&zones, 6, 0.20, &required);

        assert_eq!(targets, BTreeSet::from([1, 3, 4]));
        assert!(zone_map_maintenance_targets(&zones, 3, 0.20, &required).contains(&1));
        assert!(zone_map_maintenance_targets(&zones, 1, 0.20, &required).is_empty());

        let missing_column = zone_map_maintenance_targets(
            &BTreeMap::from([(0, ready(40))]),
            1,
            0.20,
            &BTreeSet::from([1, 2]),
        );
        assert_eq!(missing_column, BTreeSet::from([0]));

        let empty = ZoneSummary {
            state: Some(ZoneMapState::Empty),
            ..ZoneSummary::default()
        };
        assert!(zone_map_maintenance_targets(
            &BTreeMap::from([(0, empty)]),
            1,
            0.20,
            &BTreeSet::from([1, 2]),
        )
        .is_empty());
    }

    #[test]
    fn maintenance_target_batch_is_bounded() {
        let targets = zone_map_maintenance_targets(
            &BTreeMap::new(),
            ZONE_MAP_MAINTENANCE_MAX_ZONES_PER_PASS as u64 + 10,
            0.20,
            &BTreeSet::new(),
        );
        assert_eq!(targets.len(), ZONE_MAP_MAINTENANCE_MAX_ZONES_PER_PASS);
        assert_eq!(targets.first(), Some(&0));
        assert_eq!(
            targets.last(),
            Some(&(ZONE_MAP_MAINTENANCE_MAX_ZONES_PER_PASS as u64 - 1))
        );
    }

    #[test]
    fn unknown_column_summary_never_prunes() {
        let mut summary = ZoneColumnSummary::default();
        summary.mark_unknown();
        let query = ZoneScalar::Signed(42);

        for op in [
            crate::parser::BinaryOp::Eq,
            crate::parser::BinaryOp::Lt,
            crate::parser::BinaryOp::LtEq,
            crate::parser::BinaryOp::Gt,
            crate::parser::BinaryOp::GtEq,
        ] {
            assert!(comparison_may_match(&summary, op, &query));
        }
    }

    #[test]
    fn truncated_prefix_is_conservative_at_boundary() {
        let column = ColumnSchema {
            not_null_from_scn: 0,
            name: "s".into(),
            data_type: DataTypeKind::VarChar { len: Some(255) },
            nullable: true,
            unsigned: false,
            zerofill: false,
            invisible: false,
            auto_increment: false,
            default: None,
            read_backfill_default: None,
            default_expr: None,
            on_update_expr: None,
            column_id: 1,
            state: ColumnState::Public,
            physical_pos: 0,
            logical_pos: 0,
            ordinal_position: 1,
            charset: Some("utf8mb4".into()),
            collation: Some("utf8mb4_0900_ai_ci".into()),
            comment: String::new(),
            generated_expr: None,
            generated_stored: false,
            generated_column_names: Vec::new(),
            generated_expr_ast: None,
            generated_program: None,
        };
        let scalar = zone_scalar(&column, &Value::String("abcdefghijklmnopq".into())).unwrap();
        assert!(scalar.was_truncated());
        let sixteen = "数".repeat(16);
        let seventeen = "数".repeat(17);
        assert!(!zone_scalar(&column, &Value::String(sixteen.clone()))
            .unwrap()
            .was_truncated());
        assert!(zone_scalar(&column, &Value::String(seventeen))
            .unwrap()
            .was_truncated());
        let padded = zone_scalar(&column, &Value::String(format!("{sixteen}   "))).unwrap();
        let plain = zone_scalar(&column, &Value::String(sixteen)).unwrap();
        assert_eq!(padded.compare(&plain), Some(std::cmp::Ordering::Equal));

        let float_column = ColumnSchema {
            data_type: DataTypeKind::Double {
                precision: None,
                scale: None,
            },
            ..column.clone()
        };
        assert!(zone_scalar(&float_column, &Value::Double(f64::NAN)).is_none());

        let decimal_column = ColumnSchema {
            data_type: DataTypeKind::Decimal {
                precision: Some(65),
                scale: Some(30),
            },
            ..column
        };
        let left = zone_scalar(
            &decimal_column,
            &Value::Decimal(
                "99999999999999999999999999999999999.000000000000000000000000000001".into(),
            ),
        )
        .unwrap();
        let right = zone_scalar(
            &decimal_column,
            &Value::Decimal(
                "99999999999999999999999999999999999.000000000000000000000000000002".into(),
            ),
        )
        .unwrap();
        assert_eq!(left.compare(&right), Some(std::cmp::Ordering::Less));
    }

    #[test]
    fn zone_map_paged_concurrent_eviction_and_persistence_preserve_widening() {
        let map = paged_test_map(1, 64, 99);
        map.merge(&paged_delta(0)).unwrap();
        let published = AtomicU64::new(0);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                for value in 1..=100 {
                    map.merge(&paged_delta(value)).unwrap();
                    published.store(value as u64, Ordering::Release);
                }
            });
            scope.spawn(|| {
                for _ in 0..20 {
                    map.pool.flush_and_evict_all().unwrap();
                }
            });
            scope.spawn(|| {
                for _ in 0..100 {
                    let committed = published.load(Ordering::Acquire) as i64;
                    let _gate = map.data_gate.read().unwrap();
                    let zone = map
                        .read_zone(0, Some(&BTreeSet::from([7])))
                        .unwrap()
                        .unwrap();
                    let Some(ZoneScalar::Signed(maximum)) = zone.columns[&7].max else {
                        panic!("bounds missing")
                    };
                    assert!(maximum >= committed, "eviction lost completed widening");
                }
            });
        });
        assert_eq!(
            map.reopen().snapshot_for_tests()[&0].columns[&7].max,
            Some(ZoneScalar::Signed(100))
        );
    }

    #[test]
    fn zone_map_paged_hot_lookup_respects_column_projection() {
        let map = paged_test_map(1, 64, 99);
        let mut delta = paged_delta(42);
        let prototype = delta.zones[&0].columns[&7].clone();
        for column in 0..128 {
            delta
                .zones
                .get_mut(&0)
                .unwrap()
                .columns
                .insert(column, prototype.clone());
        }
        map.merge(&delta).unwrap();
        for _ in 0..3 {
            let zone = map
                .read_zone(0, Some(&BTreeSet::from([7])))
                .unwrap()
                .unwrap();
            assert_eq!(zone.columns.len(), 1);
            assert_eq!(zone.columns[&7].min, Some(ZoneScalar::Signed(42)));
        }
    }

    #[test]
    fn zone_map_paged_cold_lookup_survives_pool_eviction() {
        let map = paged_test_map(1, 64, 99);
        map.merge(&paged_delta(42)).unwrap();
        map.pool.flush_and_evict_all().unwrap();
        let gate = map.data_gate.read().unwrap();
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| map.pool.flush_and_evict_all());
            worker.join().unwrap().unwrap();
            assert_eq!(
                map.read_zone(0, Some(&BTreeSet::from([7])))
                    .unwrap()
                    .unwrap()
                    .columns[&7]
                    .min,
                Some(ZoneScalar::Signed(42))
            );
        });
        drop(gate);
    }

    #[test]
    fn zone_map_paged_crc_round_trip_and_corruption_rejection() {
        let map = paged_test_map(1, 64, 99);
        map.merge(&paged_delta(10)).unwrap();
        map.merge(&paged_delta(20)).unwrap();
        assert_eq!(
            map.reopen().snapshot_for_tests()[&0].columns[&7].max,
            Some(ZoneScalar::Signed(20))
        );
        map.pool.flush_and_evict_all().unwrap();
        let mut bytes = fs::read(&map.path).unwrap();
        bytes[PAGE_SIZE + 100] ^= 0x5a;
        fs::write(&map.path, bytes).unwrap();
        assert!(map.read_zone(0, Some(&BTreeSet::from([7]))).is_err());
    }

    #[test]
    fn zone_map_paged_rejects_legacy_sidecar_magic() {
        let map = paged_test_map(1, 64, 99);
        map.pool.flush_and_evict_all().unwrap();
        let mut bytes = fs::read(&map.path).unwrap();
        bytes[..4].copy_from_slice(b"ZMAP");
        fs::write(&map.path, bytes).unwrap();
        let file = zone_map_page::PageFile::new(
            map.path.clone(),
            1,
            1,
            Arc::clone(&map.pool),
            Arc::clone(&map.log),
            DurabilityMode::Strict,
        )
        .unwrap();
        assert!(zone_map_page::mutable::Store::open_existing(file, 99, 64).is_err());
    }

    #[test]
    fn zone_map_paged_wide_table_loads_only_requested_columns() {
        let map = paged_test_map(1, 64, 99);
        let mut batch = paged_delta(0);
        for id in 0..64 {
            let mut zone = ZoneSummary::default();
            for column in 0..128 {
                let mut summary = ZoneColumnSummary::default();
                summary.merge_value(ZoneScalar::Signed(id as i64 + column as i64));
                zone.columns.insert(column, summary);
            }
            batch.zones.insert(id, zone);
        }
        map.merge(&batch).unwrap();
        map.pool.flush_and_evict_all().unwrap();
        let selected = map
            .read_zone(63, Some(&BTreeSet::from([7])))
            .unwrap()
            .unwrap();
        assert_eq!(selected.columns.len(), 1);
        assert_eq!(selected.columns[&7].min, Some(ZoneScalar::Signed(70)));
        assert!(map.storage_snapshot().resident_bytes <= 16 * PAGE_SIZE as u64);
    }

    #[test]
    fn zone_map_paged_type_and_space_matrix() {
        let mut report = Vec::new();
        for columns in [1u32, 32, 128] {
            let mut zones = BTreeMap::new();
            // Keep every type/width/page-size combination; sixteen zones
            // exercise multi-page encoding and cold reopen without turning
            // this correctness matrix into a strict-fsync throughput test.
            for zone_id in 0..16u64 {
                let mut zone = ZoneSummary {
                    row_count_estimate: 256,
                    rebuild_scn: 100,
                    ..ZoneSummary::default()
                };
                for column_id in 1..=columns {
                    let mut summary = ZoneColumnSummary::default();
                    match column_id % 6 {
                        0 => {
                            summary.merge_value(ZoneScalar::Signed(zone_id as i64 * 10));
                            summary.merge_value(ZoneScalar::Signed(zone_id as i64 * 10 + 9));
                        }
                        1 => {
                            summary.merge_value(ZoneScalar::Unsigned(zone_id * 100));
                            summary.merge_value(ZoneScalar::Unsigned(zone_id * 100 + 99));
                        }
                        2 => {
                            summary.merge_value(ZoneScalar::Decimal(format!(
                                "{}.{:02}",
                                zone_id, column_id
                            )));
                            summary.merge_value(ZoneScalar::Decimal(format!(
                                "{}.{:02}",
                                zone_id + 1,
                                column_id
                            )));
                        }
                        3 => {
                            summary.merge_value(ZoneScalar::Text {
                                weight: format!("prefix-{zone_id:04}").into_bytes(),
                                truncated: false,
                            });
                            summary.merge_value(ZoneScalar::Text {
                                weight: format!("prefix-{zone_id:04}-z").into_bytes(),
                                truncated: false,
                            });
                        }
                        4 => {
                            summary.merge_value(ZoneScalar::Binary {
                                prefix: vec![column_id as u8, zone_id as u8],
                                truncated: false,
                            });
                            summary.merge_value(ZoneScalar::Binary {
                                prefix: vec![column_id as u8, zone_id as u8, 0xff],
                                truncated: false,
                            });
                        }
                        _ => {
                            summary.merge_value(ZoneScalar::Float64((zone_id as f64).to_bits()));
                            summary
                                .merge_value(ZoneScalar::Float64((zone_id as f64 + 0.5).to_bits()));
                        }
                    }
                    zone.columns.insert(column_id, summary);
                }
                zones.insert(zone_id, zone);
            }
            let json_bytes = serde_json::to_vec(&zones).unwrap().len() as u64;
            let batch = ZoneMapDeltaBatch {
                storage_id: columns as u64,
                zone_pages: 8,
                schema_fingerprint: 0x1234,
                zones: zones.clone(),
            };
            let zmd1 = encode_zone_map_delta(&batch).len() as u64;
            let zmd2 = encode_zone_map_delta_v2(&batch).unwrap().len() as u64;
            for zone_pages in [8u32, 128, 8192] {
                let map = paged_test_map(columns as u64, zone_pages, 0x1234);
                let path = map.path.clone();
                map.merge(&ZoneMapDeltaBatch {
                    storage_id: columns as u64,
                    zone_pages,
                    schema_fingerprint: 0x1234,
                    zones: zones.clone(),
                })
                .unwrap();
                map.write_to_file(1).unwrap();
                let sidecar = fs::metadata(path).unwrap().len();
                report.push((columns, zone_pages, json_bytes, sidecar, zmd1, zmd2));
                let reloaded = map.reopen();
                let actual = reloaded.snapshot_for_tests();
                assert_eq!(actual.len(), zones.len());
                for (id, expected) in &zones {
                    let got = &actual[id];
                    assert_eq!(
                        (
                            got.state,
                            got.row_count_estimate,
                            got.stale_mutations,
                            got.rebuild_scn
                        ),
                        (
                            expected.state,
                            expected.row_count_estimate,
                            expected.stale_mutations,
                            expected.rebuild_scn
                        ),
                        "zone {id}"
                    );
                    assert_eq!(got.columns.len(), expected.columns.len());
                    for (column, wanted) in &expected.columns {
                        let got = &got.columns[column];
                        assert_eq!(
                            (got.state, got.has_null, got.has_non_null, got.truncated),
                            (
                                wanted.state,
                                wanted.has_null,
                                wanted.has_non_null,
                                wanted.truncated
                            ),
                            "zone {id} column {column}"
                        );
                        for (left, right) in [(&got.min, &wanted.min), (&got.max, &wanted.max)] {
                            match (left, right) {
                                (Some(left), Some(right)) => assert_eq!(
                                    left.compare(right),
                                    Some(std::cmp::Ordering::Equal),
                                    "zone {id} column {column}: {left:?} / {right:?}"
                                ),
                                (None, None) => {}
                                _ => panic!("endpoint presence differs: zone {id} column {column}"),
                            }
                        }
                        assert_eq!(got.membership, wanted.membership);
                    }
                }
            }
            if columns == 128 {
                assert!(
                    zmd2.saturating_mul(100) <= zmd1.saturating_mul(60),
                    "wide WAL target missed: ZMD2={zmd2} ZMD1={zmd1}"
                );
            }
        }
        eprintln!("zone-map-paged storage / legacy codec byte matrix: {report:?}");
    }

    #[test]
    fn zone_map_paged_small_update_preserves_unrelated_zone() {
        let map = paged_test_map(1, 64, 99);
        let mut batch = paged_delta(1);
        batch
            .zones
            .insert(1024, paged_delta(100).zones.remove(&0).unwrap());
        map.merge(&batch).unwrap();
        map.write_to_file(0).unwrap();
        let before = map.read_zone(1024, None).unwrap().unwrap();
        let size = fs::metadata(&map.path).unwrap().len();
        map.merge(&paged_delta(2)).unwrap();
        let after = map.reopen();
        assert_eq!(
            serde_json::to_vec(
                &after
                    .read_zone(1024, Some(&BTreeSet::from([7])))
                    .unwrap()
                    .unwrap()
            )
            .unwrap(),
            serde_json::to_vec(&before).unwrap()
        );
        assert_eq!(
            fs::metadata(&map.path).unwrap().len(),
            size,
            "non-splitting update must reuse pages"
        );
    }

    #[test]
    fn zone_map_paged_revoked_root_cannot_reopen_after_flush() {
        let map = paged_test_map(1, 64, 99);
        map.merge(&paged_delta(10)).unwrap();
        map.store.invalidate().unwrap();
        map.pool.flush_and_evict_all().unwrap();
        let file = zone_map_page::PageFile::new(
            map.path.clone(),
            1,
            1,
            Arc::clone(&map.pool),
            Arc::clone(&map.log),
            DurabilityMode::Strict,
        )
        .unwrap();
        let reopened = zone_map_page::mutable::Store::open_existing(file, 99, 64).unwrap();
        assert!(!reopened.trusted());
        assert!(reopened
            .get(
                zone_map_page::summary::key(7, zone_map_page::Kind::Bounds, 0),
                |_| Ok(())
            )
            .is_err());
        assert!(map.read_zone(0, None).is_err());
    }

    #[test]
    fn checkpoint_end_crash_windows_recover_heap_and_zone_map_conservatively() {
        for stage in [5u8, 6] {
            let dir = tempfile::Builder::new()
                .prefix("zone-map-")
                .tempdir_in("/tmp/mydb")
                .unwrap();
            {
                let db = Database::open(dir.path()).unwrap();
                db.create_table("t", vec![zone_map_test_column()], vec!["id".into()], vec![])
                    .unwrap();
                let txn = db.begin_dml_txn().unwrap();
                db.insert_rows(txn, "test.t", vec![vec![Value::Int(42)]])
                    .unwrap();
                db.commit_txn(txn.txn_id).unwrap();
                let _failpoint = ZoneMapFailpointGuard::install(stage);
                assert!(
                    db.checkpoint().is_err(),
                    "stage {stage} must interrupt checkpoint"
                );
            }

            let db = Database::open(dir.path()).unwrap();
            let txn = db.begin_read_only_txn().unwrap();
            let rows = db.scan_rows("test.t", txn).unwrap();
            assert_eq!(rows.len(), 1, "stage {stage} lost heap data");
            assert_eq!(rows[0].values[0], Value::Int(42));
            db.commit_txn(txn.txn_id).unwrap();
            let table = db.snapshot_table_schema("test.t").unwrap();
            let predicate = Expr::Binary {
                left: Box::new(Expr::Identifier("id".into())),
                op: crate::parser::BinaryOp::Eq,
                right: Box::new(Expr::Literal(Literal::Int(42))),
            };
            let ranges = db.zone_map_candidate_ranges(&table, &predicate);
            assert!(
                ranges.is_none() || ranges.is_some_and(|ranges| !ranges.is_empty()),
                "stage {stage} produced a false-negative empty candidate set"
            );
        }
    }

    #[test]
    fn zone_map_paged_transaction_rollback_keeps_conservative_durable_bounds() {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let db = Database::open(dir.path()).unwrap();
        db.create_table("t", vec![zone_map_test_column()], vec!["id".into()], vec![])
            .unwrap();
        let txn = db.begin_dml_txn().unwrap();
        db.insert_rows(
            txn,
            "test.t",
            vec![vec![Value::Int(1)], vec![Value::Int(2)]],
        )
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        let aborted = db.begin_dml_txn().unwrap();
        db.insert_rows(aborted, "test.t", vec![vec![Value::Int(99999)]])
            .unwrap();
        db.rollback_txn(aborted.txn_id).unwrap();
        db.checkpoint().unwrap();
        db.log.lock().flush().unwrap();
        assert_eq!(
            LogManager::scan(dir.path())
                .unwrap()
                .records
                .iter()
                .map(|record| count_zone_map_payloads(&record.payload))
                .sum::<usize>(),
            0,
            "new format must not write logical ZoneMapMergeBatch records"
        );
        drop(db);
        let db = Database::open(dir.path()).unwrap();
        let txn = db.begin_read_only_txn().unwrap();
        let mut actual = db
            .scan_rows("test.t", txn)
            .unwrap()
            .into_iter()
            .map(|row| row.values)
            .collect::<Vec<_>>();
        actual.sort_by_key(|row| row[0].to_display_string());
        assert_eq!(actual, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
        db.commit_txn(txn.txn_id).unwrap();
        let table = db.table("test.t").unwrap();
        let predicate = Expr::Binary {
            left: Box::new(Expr::Identifier("id".into())),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Int(2))),
        };
        assert!(db
            .zone_map_candidate_ranges(&table, &predicate)
            .is_none_or(|ranges| !ranges.is_empty()));
    }

    #[test]
    fn concurrent_cold_zone_map_loads_publish_one_shared_map() {
        let dir = tempfile::Builder::new()
            .prefix("zone-map-")
            .tempdir_in("/tmp/mydb")
            .unwrap();
        let db = Database::open(dir.path()).unwrap();
        db.create_table("t", vec![zone_map_test_column()], vec!["id".into()], vec![])
            .unwrap();
        let txn = db.begin_dml_txn().unwrap();
        db.insert_rows(txn, "test.t", vec![vec![Value::Int(7)]])
            .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        db.checkpoint().unwrap();
        let table = db.table("test.t").unwrap();
        db.forget_zone_map(table.id).unwrap();
        let barrier = std::sync::Barrier::new(16);
        let maps = std::thread::scope(|scope| {
            let handles = (0..16)
                .map(|_| {
                    scope.spawn(|| {
                        barrier.wait();
                        db.zone_map_for(table.id, &table).unwrap()
                    })
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(|h| h.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert!(maps.iter().all(|map| Arc::ptr_eq(map, &maps[0])));
        assert!(Arc::ptr_eq(
            &db.zone_map_for(table.id, &table).unwrap(),
            &maps[0]
        ));
    }

    #[test]
    fn zone_map_checkpoint_catalog_autoincrement_precedes_metadata_gate() {
        let (_dir, engine) = switch_engine();
        engine
            .execute("CREATE TABLE zm_checkpoint_ai (id BIGINT AUTO_INCREMENT PRIMARY KEY)")
            .unwrap();
        engine
            .execute("INSERT INTO zm_checkpoint_ai VALUES (NULL)")
            .unwrap();
        engine.db_for_tests().checkpoint().unwrap();
        // The second checkpoint replaces an existing DD metadata row through
        // delete_row_at, which recursively enters the Zone Map read gate.
        engine
            .execute("INSERT INTO zm_checkpoint_ai VALUES (NULL)")
            .unwrap();
        engine.db_for_tests().checkpoint().unwrap();
    }

    #[test]
    fn zone_map_checkpoint_waits_for_metadata_publication_before_dirty_snapshot() {
        use std::sync::mpsc;
        use std::time::Duration;

        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let db = Arc::new(Database::open(dir.path()).unwrap());
        db.create_table("t", vec![zone_map_test_column()], vec!["id".into()], vec![])
            .unwrap();
        let txn = db.begin_dml_txn().unwrap();
        db.insert_rows(txn, "test.t", vec![vec![Value::Int(1)]])
            .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        db.analyze_table("test.t").unwrap();
        db.checkpoint().unwrap();
        let table = db.table("test.t").unwrap();
        let map = db.zone_map_for(table.id, &table).unwrap();
        // Prepare real dirty metadata, then model a page publisher admitted
        // before checkpoint. Its WAL records have no transaction floor pin.
        {
            let _data = map.data_gate.write().unwrap();
            map.store.invalidate().unwrap();
        }
        let publisher = db.buffer_pool.page_wal_publish_gate.read();
        let before = db.buffer_pool.last_checkpoint_lsn();
        let (done_tx, done_rx) = mpsc::channel();
        let checkpointer_db = db.clone();
        let checkpointer = std::thread::spawn(move || {
            let result = checkpointer_db.checkpoint();
            done_tx.send(result).unwrap();
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while db.checkpoint_running.try_lock().is_ok() {
            assert!(std::time::Instant::now() < deadline);
            std::thread::yield_now();
        }
        assert!(matches!(
            done_rx.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        assert_eq!(
            db.buffer_pool.last_checkpoint_lsn(),
            before,
            "redo floor must wait for publication"
        );
        // Checkpoint must include the dirty root after this publisher drains.
        // Do not enter another page write under the held reader: this gate is
        // deliberately nonrecursive so a queued checkpoint cannot starve.
        drop(publisher);
        done_rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap();
        checkpointer.join().unwrap();
        drop(map);
        drop(db);
        let db = Database::open(dir.path()).unwrap();
        let table = db.table("test.t").unwrap();
        let map = db.zone_map_for(table.id, &table).unwrap();
        assert!(!map.trusted_base.load(Ordering::Acquire));
    }

    #[test]
    fn zone_map_checkpoint_admits_publication_during_durable_sync() {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let db = Arc::new(Database::open(dir.path()).unwrap());
        db.create_table("t", vec![zone_map_test_column()], vec!["id".into()], vec![])
            .unwrap();
        let txn = db.begin_dml_txn().unwrap();
        db.insert_rows(txn, "test.t", vec![vec![Value::Int(1)]])
            .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        // The hook runs after the data-file sync, before CheckpointEnd. Only
        // the dirty snapshot needs the gate; publishers must not wait for the
        // rest of the checkpoint's I/O.
        let probe = Arc::clone(&db);
        let (admitted_tx, admitted_rx) = std::sync::mpsc::channel();
        SEQUENCE_CHECKPOINT_TEST_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                let admitted = probe.buffer_pool.page_wal_publish_gate.try_read().is_some();
                admitted_tx.send(admitted).unwrap();
            }));
        });
        db.checkpoint().unwrap();
        assert!(admitted_rx.try_recv().unwrap());
    }

    #[test]
    fn zone_map_checkpoint_does_not_wait_for_long_summary_operation() {
        use std::sync::mpsc;
        use std::time::Duration;

        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let db = Arc::new(Database::open(dir.path()).unwrap());
        // ANALYZE and maintenance retain this reader while scanning heaps.
        // Only their short page-publication windows may delay checkpoint.
        let operation = db.zone_map_gate.read_recursive();
        let (done_tx, done_rx) = mpsc::channel();
        let checkpointer_db = db.clone();
        let checkpointer = std::thread::spawn(move || {
            done_tx.send(checkpointer_db.checkpoint()).unwrap();
        });
        let result = done_rx.recv_timeout(Duration::from_secs(10));
        // Release even on failure so the diagnostic assertion cannot leave
        // an accidentally blocked checkpoint thread behind.
        drop(operation);
        checkpointer.join().unwrap();
        result
            .expect("a long Zone Map reader must not block checkpoint")
            .unwrap();
    }

    #[test]
    fn zone_map_paged_checkpoint_preserves_evicted_pages_and_durable_root() {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let db = Database::open(dir.path()).unwrap();
        db.create_table("t", vec![zone_map_test_column()], vec!["id".into()], vec![])
            .unwrap();
        let txn = db.begin_dml_txn().unwrap();
        db.insert_rows(txn, "test.t", vec![vec![Value::Int(0)]])
            .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        db.analyze_table("test.t").unwrap();
        db.checkpoint().unwrap();
        db.buffer_pool.flush_and_evict_all().unwrap();
        db.checkpoint().unwrap();
        let table = db.table("test.t").unwrap();
        let predicate = Expr::Binary {
            left: Box::new(Expr::Identifier("id".into())),
            op: BinaryOp::NotEq,
            right: Box::new(Expr::Literal(Literal::Int(0))),
        };
        assert!(db
            .zone_map_candidate_ranges(&table, &predicate)
            .unwrap()
            .is_empty());
        drop(db);
        let db = Database::open(dir.path()).unwrap();
        let table = db.table("test.t").unwrap();
        assert!(db
            .zone_map_candidate_ranges(&table, &predicate)
            .unwrap()
            .is_empty());
        let txn = db.begin_dml_txn().unwrap();
        db.insert_rows(txn, "test.t", vec![vec![Value::Int(1)]])
            .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        db.checkpoint().unwrap();
        db.buffer_pool.flush_and_evict_all().unwrap();
        assert!(db
            .zone_map_candidate_ranges(&table, &predicate)
            .is_none_or(|ranges| !ranges.is_empty()));
        db.invalidate_zone_maps_for_table(&table).unwrap();
        db.checkpoint().unwrap();
        assert!(db
            .zone_map_candidate_ranges(&table, &predicate)
            .is_none_or(|ranges| !ranges.is_empty()));
        db.rebuild_zone_map_for_table(&table).unwrap();
        db.checkpoint().unwrap();
        let path = db.zone_map_file_path(table.id);
        db.drop_table("test.t").unwrap();
        db.checkpoint().unwrap();
        assert!(!path.exists(), "DROP must retire paged sidecar");
    }

    #[test]
    fn missing_zone_map_on_populated_heap_does_not_scan_rows_to_test_emptiness() {
        let dir = tempfile::Builder::new()
            .prefix("zone-map-")
            .tempdir_in("/tmp/mydb")
            .unwrap();
        let db = Database::open(dir.path()).unwrap();
        db.create_table("t", vec![zone_map_test_column()], vec!["id".into()], vec![])
            .unwrap();
        let txn = db.begin_dml_txn().unwrap();
        db.insert_rows(
            txn,
            "test.t",
            (0..10000).map(|id| vec![Value::Int(id)]).collect(),
        )
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();

        // Model memory-pressure eviction before a checkpoint has produced a
        // trusted sidecar.  The heap is cold so an accidental exact row-count
        // fallback is observable as buffer-pool read I/O.
        db.flush_data_cache().unwrap();
        let table = db.snapshot_table_schema("test.t").unwrap();
        db.forget_zone_map(table.id).unwrap();
        fs::remove_file(db.zone_map_file_path(table.id)).unwrap();
        let reads_before = db.storage_status().buffer_pool_read_ios;

        let map = db.zone_map_for(table.id, &table).unwrap();

        let reads_after = db.storage_status().buffer_pool_read_ios;
        assert!(
            reads_after.saturating_sub(reads_before) <= 4,
            "missing summary lookup must not scan the populated heap"
        );
        assert!(!map.trusted_base.load(Ordering::Acquire));
    }

    #[test]
    fn xa_prepare_persists_zone_map_delta_before_prepare_record() {
        let dir = tempfile::Builder::new()
            .prefix("zone-map-")
            .tempdir_in("/tmp/mydb")
            .unwrap();
        let db = Database::open(dir.path()).unwrap();
        db.create_table("t", vec![zone_map_test_column()], vec!["id".into()], vec![])
            .unwrap();
        let txn = db.begin_dml_txn().unwrap();
        db.insert_rows(txn, "test.t", vec![vec![Value::Int(7)]])
            .unwrap();
        let xid = crate::xa::Xid::new(1, b"zone-map".to_vec(), b"prepare".to_vec()).unwrap();
        db.prepare_xa(txn.txn_id, 0, xid.clone()).unwrap();
        db.log.lock().flush().unwrap();
        let mut order = Vec::new();
        for record in LogManager::scan(dir.path()).unwrap().records {
            collect_zone_prepare_order(&record.payload, &mut order);
        }
        let zone = order.iter().position(|kind| *kind == "zone").unwrap();
        let prepare = order.iter().position(|kind| *kind == "prepare").unwrap();
        assert!(
            zone < prepare,
            "Zone Map redo must precede XA PREPARE: {order:?}"
        );
        db.rollback_prepared(&xid).unwrap();
    }

    #[test]
    fn first_trusted_conservative_rebuild_invalidates_cached_seq_scan() {
        use crate::engine::cache::{PlanCache, StatementKey};
        use crate::planner::{PhysicalPlan, PlanTable};
        use smallvec::SmallVec;

        let dir = tempfile::Builder::new()
            .prefix("zone-map-")
            .tempdir_in("/tmp/mydb")
            .unwrap();
        let db = Database::open(dir.path()).unwrap();
        db.create_table("t", vec![zone_map_test_column()], vec!["id".into()], vec![])
            .unwrap();
        let write = db.begin_dml_txn().unwrap();
        db.insert_rows(write, "test.t", vec![vec![Value::Int(7)]])
            .unwrap();
        db.commit_txn(write.txn_id).unwrap();

        let table = db.snapshot_table_schema("test.t").unwrap();
        let map = db.zone_map_for(table.id, &table).unwrap();
        map.trusted_base.store(false, Ordering::Release);

        let cache = Arc::new(PlanCache::new(8));
        crate::engine::cache::register_global_plan(&cache);
        let physical = Arc::new(PhysicalPlan::SeqScan {
            table: PlanTable::new(table.id, Arc::clone(&table), None),
            filter: None,
            estimated_rows: 1.0,
            access_rows: 1.0,
            cost: crate::engine::cbo::Cost::ZERO,
            possible_keys: Vec::new(),
            runtime_filters: Vec::new(),
            hint_note: None,
            projected_columns: None,
        });
        let sql = "SELECT * FROM t";
        let crate::parser::Statement::Select(select) = crate::parser::parse_statement(sql).unwrap()
        else {
            unreachable!()
        };
        let key = StatementKey {
            sql: Arc::from(sql),
            database: Arc::from("test"),
            semantics: Default::default(),
        };
        cache.insert(
            key,
            Arc::new(select),
            physical,
            SmallVec::from_vec(vec![(
                Arc::<str>::from("test.t"),
                table.id,
                table.schema_version,
            )]),
            SmallVec::new(),
        );
        assert_eq!(cache.stats().size, 1);

        // Keep an older snapshot open so the rebuild must conservatively
        // union rather than publish a shrinking generation. This was the
        // missing invalidation branch that left the cached SeqScan resident.
        let older_snapshot = db.begin_read_only_txn().unwrap();
        db.rebuild_zone_map_for_table(&table).unwrap();
        db.commit_txn(older_snapshot.txn_id).unwrap();

        assert!(map.trusted_base.load(Ordering::Acquire));
        assert_eq!(
            cache.stats().size,
            0,
            "false -> true trust publication must evict the stale SeqScan"
        );
    }

    #[test]
    fn zone_membership_exact_promotes_to_bloom_without_false_negatives() {
        let values = (0..256).map(ZoneScalar::Signed).collect::<Vec<_>>();
        let mut membership = ZoneMembership::empty();
        for value in &values {
            membership.insert_hash(zone_membership_hash(value));
        }
        assert!(matches!(membership, ZoneMembership::Bloom { .. }));
        assert!(values
            .iter()
            .all(|value| membership.may_contain_hash(zone_membership_hash(value))));
    }

    #[test]
    fn worker_membership_hash_matches_persistent_scalar_without_allocating() {
        let values = vec![
            WorkerScalar::Signed(-17),
            WorkerScalar::Unsigned(17),
            WorkerScalar::Float32((-0.0f32).to_bits()),
            WorkerScalar::Float64(42.5f64.to_bits()),
            WorkerScalar::DecimalCompact {
                mag: -1_234_000,
                scale: 5,
            },
            WorkerScalar::DecimalCompact { mag: 7, scale: 4 },
            WorkerScalar::DecimalCompact { mag: 0, scale: 2 },
            WorkerScalar::Temporal(19_358),
            WorkerScalar::Bytes {
                prefix: vec![0, 1, 0xff],
                truncated: true,
            },
            WorkerScalar::TextWeight {
                weight: b"AIR".to_vec(),
                truncated: false,
            },
        ];
        for value in values {
            assert_eq!(
                zone_membership_hash_worker(&value),
                zone_membership_hash(&value.clone().into_zone_scalar())
            );
        }
        assert_eq!(
            zone_membership_hash_borrowed(6, &[0, 1, 0xff], true),
            zone_membership_hash(&ZoneScalar::Binary {
                prefix: vec![0, 1, 0xff],
                truncated: true,
            })
        );
        assert_eq!(
            zone_membership_hash_borrowed(7, b"AIR", false),
            zone_membership_hash(&ZoneScalar::Text {
                weight: b"AIR".to_vec(),
                truncated: false,
            })
        );
    }

    #[test]
    fn zone_membership_set_bits_tracks_duplicates_merge_and_decode() {
        let layout = ZoneMembershipLayout {
            words: ZONE_MEMBERSHIP_MAX_BLOOM_WORDS,
            hashes: ZONE_MEMBERSHIP_DEFAULT_BLOOM_HASHES,
        };
        let values = (0..512).map(ZoneScalar::Signed).collect::<Vec<_>>();
        let mut membership = ZoneMembership::empty_with_layout(layout);
        for value in &values {
            let hash = zone_membership_hash(value);
            membership.insert_hash(hash);
            membership.insert_hash(hash);
            if let ZoneMembership::Bloom {
                words, set_bits, ..
            } = &membership
            {
                assert_eq!(*set_bits, ZoneMembership::count_set_bits(words));
            }
        }

        let mut incoming = ZoneMembership::empty_with_layout(layout);
        for value in 512..768 {
            incoming.insert_hash(zone_membership_hash(&ZoneScalar::Signed(value)));
        }
        membership.merge_from(&incoming);
        let ZoneMembership::Bloom {
            words, set_bits, ..
        } = &membership
        else {
            panic!("expected bloom membership");
        };
        assert_eq!(*set_bits, ZoneMembership::count_set_bits(words));
        assert!((0..768)
            .all(|value| membership
                .may_contain_hash(zone_membership_hash(&ZoneScalar::Signed(value)))));

        let mut encoded = Vec::new();
        put_zone_membership(&mut encoded, Some(&membership)).unwrap();
        let mut reader = ZoneBytesReader::new(&encoded);
        let decoded = read_zone_membership(&mut reader, true).unwrap().unwrap();
        let ZoneMembership::Bloom {
            words, set_bits, ..
        } = &decoded
        else {
            panic!("expected decoded bloom membership");
        };
        assert_eq!(*set_bits, ZoneMembership::count_set_bits(words));
        assert!((0..768).all(
            |value| decoded.may_contain_hash(zone_membership_hash(&ZoneScalar::Signed(value)))
        ));
    }

    #[test]
    fn bloom_recurrence_matches_persisted_double_hash_positions() {
        for words in [
            ZONE_MEMBERSHIP_MIN_BLOOM_WORDS,
            ZONE_MEMBERSHIP_MAX_BLOOM_WORDS,
        ] {
            for hashes in 1..=ZONE_MEMBERSHIP_MAX_BLOOM_HASHES {
                let mut actual = vec![0u64; words];
                let mut set_bits = 0;
                for hash in [0, 1, u64::MAX, 0x1234_5678_9abc_def0] {
                    ZoneMembership::insert_bloom_hash(&mut actual, hashes, &mut set_bits, hash);
                }
                let mut expected = vec![0u64; words];
                for hash in [0, 1, u64::MAX, 0x1234_5678_9abc_def0] {
                    for bit in ZoneMembership::bloom_positions(hash, words, hashes) {
                        expected[bit >> 6] |= 1u64 << (bit & 63);
                    }
                }
                assert_eq!(actual, expected);
                assert_eq!(set_bits, ZoneMembership::count_set_bits(&expected));
            }
        }
    }

    #[test]
    fn worker_membership_duplicate_cache_preserves_exact_summary() {
        let layout = ZoneMembershipLayout::default();
        let hashes = [11, 7, 11, 19, 7, 31, 19, 11];
        let mut cached = WorkerColumnState::default();
        cached.enable_membership_with_layout(layout);
        let mut expected = ZoneMembership::empty_with_layout(layout);
        for hash in hashes {
            cached.insert_membership_hash(hash);
            expected.insert_hash(hash);
        }
        assert_eq!(cached.membership, Some(expected));
    }

    #[test]
    fn worker_scalar_cache_preserves_summary_across_types_and_collisions() {
        let mut cases = vec![
            vec![
                WorkerScalar::Signed(i64::MIN),
                WorkerScalar::Signed(i64::MAX),
            ],
            vec![WorkerScalar::Unsigned(0), WorkerScalar::Unsigned(u64::MAX)],
            vec![
                WorkerScalar::Temporal(i64::MIN),
                WorkerScalar::Temporal(i64::MAX),
            ],
            vec![
                WorkerScalar::DecimalCompact {
                    mag: i128::MIN,
                    scale: 38,
                },
                WorkerScalar::DecimalCompact {
                    mag: i128::MAX,
                    scale: 38,
                },
            ],
            vec![
                WorkerScalar::Float32(0),
                WorkerScalar::Float32((-0.0f32).to_bits()),
                WorkerScalar::Float32(f32::INFINITY.to_bits()),
                WorkerScalar::Float32(f32::NEG_INFINITY.to_bits()),
            ],
            vec![
                WorkerScalar::Float64(0),
                WorkerScalar::Float64((-0.0f64).to_bits()),
                WorkerScalar::Float64(f64::INFINITY.to_bits()),
                WorkerScalar::Float64(f64::NEG_INFINITY.to_bits()),
            ],
            vec![WorkerScalar::Float64(f64::NAN.to_bits()); 3],
            vec![WorkerScalar::Float32(f32::NAN.to_bits()); 3],
            vec![
                WorkerScalar::Signed(1),
                WorkerScalar::Unsigned(1),
                WorkerScalar::Signed(1),
            ],
            vec![
                WorkerScalar::DecimalCompact { mag: 100, scale: 2 },
                WorkerScalar::DecimalCompact { mag: 100, scale: 3 },
                WorkerScalar::DecimalCompact { mag: 100, scale: 2 },
            ],
        ];
        for kind in 0..6 {
            cases.push(
                (-200..200)
                    .flat_map(|v| [v, 0, v, v % 11])
                    .map(|v| match kind {
                        0 => WorkerScalar::Signed(v),
                        1 => WorkerScalar::Unsigned(v as u64),
                        2 => WorkerScalar::Temporal(v),
                        3 => WorkerScalar::DecimalCompact {
                            mag: i128::from(v) * 100,
                            scale: 2,
                        },
                        4 => WorkerScalar::Float32((v as f32 / 10.0).to_bits()),
                        _ => WorkerScalar::Float64((v as f64 / 10.0).to_bits()),
                    })
                    .collect(),
            );
        }
        for values in cases {
            for membership in [false, true] {
                let mut cached = WorkerColumnState::default();
                let mut uncached = WorkerColumnState::default();
                if membership {
                    cached.enable_membership_with_layout(ZoneMembershipLayout::default());
                    uncached.enable_membership_with_layout(ZoneMembershipLayout::default());
                }
                for value in values.iter().cycle().take(values.len() * 2) {
                    cached.observe_worker_scalar(value.clone());
                    uncached.recent_membership_valid = 0;
                    uncached.observe_worker_scalar(value.clone());
                    assert_eq!(cached.state, uncached.state, "{value:?}");
                    assert_eq!(cached.membership, uncached.membership, "{value:?}");
                    assert_eq!(cached.has_non_null, uncached.has_non_null);
                    assert_eq!(format!("{:?}", cached.min), format!("{:?}", uncached.min));
                    assert_eq!(format!("{:?}", cached.max), format!("{:?}", uncached.max));
                }
            }
        }
    }

    #[test]
    fn worker_scalar_cache_is_invalidated_when_membership_is_enabled() {
        let mut state = WorkerColumnState::default();
        state.observe_worker_scalar(WorkerScalar::Signed(42));
        state.enable_membership_with_layout(ZoneMembershipLayout::default());
        state.observe_worker_scalar(WorkerScalar::Signed(42));
        let mut expected = ZoneMembership::empty_with_layout(ZoneMembershipLayout::default());
        expected.insert_hash(zone_membership_hash_worker(&WorkerScalar::Signed(42)));
        assert_eq!(state.membership, Some(expected));
    }

    #[test]
    fn worker_membership_growth_and_cache_collisions_preserve_summary() {
        let layout = ZoneMembershipLayout::default();
        let mut cached = WorkerColumnState::default();
        cached.enable_membership_with_layout(layout);
        let mut expected = ZoneMembership::empty_with_layout(layout);
        // Collide in the direct-mapped recent-value cache, with duplicates
        // both before and after exact-set growth and the Bloom transition.
        for i in 0..ZONE_MEMBERSHIP_EXACT_LIMIT * 3 {
            for hash in [(i as u64) << 6, 0, (i as u64) << 6] {
                assert!(cached.insert_membership_hash(hash));
                expected.insert_hash(hash);
                assert_eq!(cached.membership.as_ref(), Some(&expected));
            }
        }
    }

    #[test]
    fn worker_text_cache_hit_preserves_summary_and_membership() {
        let values = [
            "",
            "A",
            "a",
            "A",
            "a", // same hint slot, different raw bytes
            "é",
            "É",
            "é",
            "中文😀",
            "中文😀",
            "x\0",
            "x\0",
            "abcdefghijklmnopq",
            "abcdefghijklmnop",
            "abcdefghijklmnopq",
            "abcdefghijklmnop",
            "repeated ",
            "repeated",
            "repeated ",
        ];
        for collation in crate::charset::collations() {
            let mut cached = WorkerColumnState::default();
            cached.enable_membership_with_layout(ZoneMembershipLayout::default());
            let mut uncached = WorkerColumnState::default();
            uncached.enable_membership_with_layout(ZoneMembershipLayout::default());
            uncached.text_weight_cache_saturated = true;
            let mut cached_scratch = Vec::new();
            let mut uncached_scratch = Vec::new();
            let mut cached_lease = ZoneScratchLease::default();
            let mut uncached_lease = ZoneScratchLease::default();
            let saturation = (0..32).map(|i| format!("distinct-{i}"));
            for value in values.iter().map(|v| v.to_string()).chain(saturation) {
                cached.observe_text_bytes_for_info(
                    collation,
                    value.as_bytes(),
                    &mut cached_scratch,
                    &mut cached_lease,
                );
                uncached.observe_text_bytes_for_info(
                    collation,
                    value.as_bytes(),
                    &mut uncached_scratch,
                    &mut uncached_lease,
                );
                assert_eq!(
                    cached.state, uncached.state,
                    "{}: {value:?}",
                    collation.name
                );
                assert_eq!(cached.has_non_null, uncached.has_non_null);
                assert_eq!(cached.truncated, uncached.truncated);
                assert_eq!(cached.membership, uncached.membership);
                assert_eq!(format!("{:?}", cached.min), format!("{:?}", uncached.min));
                assert_eq!(format!("{:?}", cached.max), format!("{:?}", uncached.max));
            }
        }
    }

    #[test]
    fn provisional_membership_accepts_good_final_layout_and_rejects_saturation() {
        let provisional = ZoneMembershipLayout {
            words: ZONE_MEMBERSHIP_MAX_BLOOM_WORDS,
            hashes: ZONE_MEMBERSHIP_PROVISIONAL_BLOOM_HASHES,
        };
        let final_layout = ZoneMembershipLayout {
            words: ZONE_MEMBERSHIP_MIN_BLOOM_WORDS,
            hashes: ZONE_MEMBERSHIP_DEFAULT_BLOOM_HASHES,
        };
        let mut membership = ZoneMembership::empty_with_layout(provisional);
        for value in 0..256 {
            membership.insert_hash(zone_membership_hash(&ZoneScalar::Signed(value)));
        }
        assert!(membership.prepare_for_layout(final_layout, 256));
        let ZoneMembership::Bloom { words, .. } = &membership else {
            panic!("expected compacted bloom");
        };
        assert_eq!(words.len(), final_layout.words);
        assert!((0..256)
            .all(|value| membership
                .may_contain_hash(zone_membership_hash(&ZoneScalar::Signed(value)))));

        let mut saturated = ZoneMembership::empty_with_layout(final_layout);
        for value in 0..20_000 {
            saturated.insert_hash(zone_membership_hash(&ZoneScalar::Signed(value)));
            if matches!(saturated, ZoneMembership::Saturated) {
                break;
            }
        }
        assert!(!saturated.meets_layout(final_layout, 20_000));
    }

    #[test]
    fn explicit_analyze_publishes_one_zone_generation_per_scan() {
        let dir = tempfile::Builder::new()
            .prefix("zone-map-")
            .tempdir_in("/tmp/mydb")
            .unwrap();
        let db = Database::open(dir.path()).unwrap();
        db.create_table(
            "t",
            vec![
                zone_map_test_column(),
                ColumnDef {
                    name: "v".into(),
                    data_type: crate::parser::DataType::Int,
                    unsigned: false,
                    zerofill: false,
                    invisible: false,
                    nullable: false,
                    primary_key: false,
                    unique: false,
                    auto_increment: false,
                    default: None,
                    on_update: None,
                    checks: Vec::new(),
                    generated: None,
                    charset: None,
                    collation: None,
                },
            ],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
        let txn = db.begin_dml_txn().unwrap();
        db.insert_rows(
            txn,
            "test.t",
            (0..25_000)
                .map(|value| vec![Value::Int(value), Value::Int(value * 17)])
                .collect(),
        )
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();

        let table = db.snapshot_table_schema("test.t").unwrap();
        let map = db.zone_map_for(table.id, &table).unwrap();
        let before = map.generation.load(Ordering::Acquire);
        db.analyze_table("test.t").unwrap();
        let after_first = map.generation.load(Ordering::Acquire);
        assert_eq!(after_first, before + 1, "first ANALYZE rebuilt twice");
        let analyzed = db.snapshot_table_schema("test.t").unwrap();
        assert!(
            zone_map_membership_columns(&analyzed)
                .values()
                .any(|layout| layout.words > ZONE_MEMBERSHIP_MIN_BLOOM_WORDS),
            "fixture must require a non-minimal final membership layout"
        );

        db.analyze_table("test.t").unwrap();
        assert_eq!(
            map.generation.load(Ordering::Acquire),
            after_first + 1,
            "repeat ANALYZE rebuilt twice"
        );
    }

    #[test]
    fn analyze_keeps_membership_for_low_ndv_non_index_column() {
        let dir = tempfile::Builder::new()
            .prefix("zone-map-")
            .tempdir_in("/tmp/mydb")
            .unwrap();
        let db = Database::open(dir.path()).unwrap();
        db.create_table(
            "t_low_ndv",
            vec![
                zone_map_test_column(),
                ColumnDef {
                    name: "v".into(),
                    data_type: crate::parser::DataType::Int,
                    unsigned: false,
                    zerofill: false,
                    invisible: false,
                    nullable: false,
                    primary_key: false,
                    unique: false,
                    auto_increment: false,
                    default: None,
                    on_update: None,
                    checks: Vec::new(),
                    generated: None,
                    charset: None,
                    collation: None,
                },
            ],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
        let txn = db.begin_dml_txn().unwrap();
        db.insert_rows(
            txn,
            "test.t_low_ndv",
            (0..1_000)
                .map(|id| vec![Value::Int(id), Value::Int((id % 50) * 2)])
                .collect(),
        )
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();

        db.analyze_table("test.t_low_ndv").unwrap();
        let analyzed = db.snapshot_table_schema("test.t_low_ndv").unwrap();
        let column = analyzed
            .columns
            .iter()
            .find(|column| column.name == "v")
            .unwrap();
        let ndv = analyzed.stats.columns["v"].distinct_count;
        let zone_count = analyzed
            .stats
            .heap_page_count
            .div_ceil(u64::from(analyzed.zone_pages()).max(1));
        let rows_per_zone = analyzed.stats.row_count.div_ceil(zone_count.max(1));
        assert_eq!(ndv, 50);
        assert!(
            rows_per_zone.saturating_mul(1_000) > ndv.saturating_mul(2_995),
            "fixture must exercise a column rejected by the former 5% threshold"
        );

        let selected = zone_map_membership_requirements(&analyzed);
        assert!(
            selected.contains_key(&column.column_id),
            "low-NDV non-index column must remain eligible for membership"
        );
        let map = db.zone_map_for(analyzed.id, &analyzed).unwrap();
        assert!(
            map.snapshot_for_tests().values().any(|zone| zone
                .columns
                .get(&column.column_id)
                .is_some_and(|summary| summary.membership.is_some())),
            "ANALYZE must persist membership for the selected low-NDV column"
        );
    }

    #[test]
    fn zone_membership_saturation_releases_bloom_and_stops_rejecting() {
        let mut membership = ZoneMembership::empty_with_layout(ZoneMembershipLayout {
            words: ZONE_MEMBERSHIP_MIN_BLOOM_WORDS,
            hashes: ZONE_MEMBERSHIP_MAX_BLOOM_HASHES,
        });
        for value in 0..20_000 {
            membership.insert_hash(zone_membership_hash(&ZoneScalar::Signed(value)));
            if matches!(membership, ZoneMembership::Saturated) {
                break;
            }
        }
        assert!(matches!(membership, ZoneMembership::Saturated));
        assert_eq!(membership.capacity_bytes(), 0);
        assert!(membership.may_contain_hash(zone_membership_hash(&ZoneScalar::Signed(i64::MAX))));
    }

    #[test]
    fn zone_summary_change_tracking_covers_persisted_types_and_memberships() {
        let mut summaries = vec![ZoneColumnSummary::default()];
        for value in [
            ZoneScalar::Signed(-4),
            ZoneScalar::Signed(99),
            ZoneScalar::Unsigned(u64::MAX),
            ZoneScalar::Float32(1.25f32.to_bits()),
            ZoneScalar::Float64((-4.5f64).to_bits()),
            ZoneScalar::Decimal("123.45".into()),
            ZoneScalar::Temporal(123456),
            ZoneScalar::Binary {
                prefix: vec![0, 255],
                truncated: false,
            },
            ZoneScalar::Binary {
                prefix: vec![0, 255],
                truncated: true,
            },
            ZoneScalar::Text {
                weight: b"abc".to_vec(),
                truncated: false,
            },
            ZoneScalar::Text {
                weight: b"xyz".to_vec(),
                truncated: true,
            },
        ] {
            let mut summary = ZoneColumnSummary::default();
            summary.enable_membership();
            summary.observe_scalar(value);
            summaries.push(summary);
        }
        let mut null = ZoneColumnSummary::default();
        null.merge_null();
        summaries.push(null);
        let mut unknown = ZoneColumnSummary::default();
        unknown.mark_unknown();
        summaries.push(unknown);
        for count in [32, 128, 256] {
            let mut summary = ZoneColumnSummary::default();
            summary.enable_membership();
            for value in 0..count {
                summary.observe_scalar(ZoneScalar::Signed(value));
            }
            summaries.push(summary);
        }
        let mut saturated = summaries.last().unwrap().clone();
        saturated.membership = Some(ZoneMembership::Saturated);
        summaries.push(saturated);
        let encoded = |column: &ZoneColumnSummary| {
            (
                zone_map_page::summary::encode_bounds(column).unwrap(),
                zone_map_page::summary::encode_membership(column.membership.as_ref()).unwrap(),
            )
        };
        for initial in &summaries {
            for source in &summaries {
                let mut merged = initial.clone();
                let before = encoded(&merged);
                let changed = merged.merge_summary_changed(source);
                let mut bounds = initial.clone();
                bounds.membership = None;
                let mut expected = bounds.clone();
                let mut bounds_source = source.clone();
                bounds_source.membership = None;
                expected.merge_summary(&bounds_source);
                let changed_bounds = bounds.merge_bounds_changed(source);
                assert_eq!(encoded(&bounds), encoded(&expected));
                assert_eq!(bounds.durable_unknown, expected.durable_unknown);
                assert!(changed_bounds || encoded(&bounds).0 == before.0);
                assert!(
                    changed || before == encoded(&merged),
                    "changed persisted summary was not dirtied: {initial:?} + {source:?}"
                );
            }
        }
    }

    #[test]
    fn zone_map_paged_flush_without_changes_writes_no_summary_redo() {
        let map = paged_test_map(1, 64, 99);
        map.merge(&paged_delta(10)).unwrap();
        map.write_to_file(0).unwrap();
        let lsn = map.log.lock().last_lsn();
        let bytes = fs::read(&map.path).unwrap();
        map.write_to_file(0).unwrap();
        assert_eq!(map.log.lock().last_lsn(), lsn);
        assert_eq!(fs::read(&map.path).unwrap(), bytes);
    }

    #[test]
    fn zone_map_paged_repeated_updates_do_not_append_file_versions() {
        let map = paged_test_map(1, 64, 99);
        map.merge(&paged_delta(0)).unwrap();
        map.write_to_file(0).unwrap();
        let size = fs::metadata(&map.path).unwrap().len();
        for value in 1..=100 {
            map.merge(&paged_delta(value)).unwrap();
        }
        map.write_to_file(0).unwrap();
        assert_eq!(fs::metadata(&map.path).unwrap().len(), size);
        assert_eq!(
            map.reopen()
                .read_zone(0, Some(&BTreeSet::from([7])))
                .unwrap()
                .unwrap()
                .columns[&7]
                .max,
            Some(ZoneScalar::Signed(100))
        );
    }

    #[test]
    fn zone_map_paged_handle_charge_does_not_scale_with_zones() {
        let map = paged_test_map(1, 64, 99);
        map.merge(&paged_delta(0)).unwrap();
        let before = map.charged_memory_bytes();
        for id in 1..256 {
            let mut delta = paged_delta(id as i64);
            let zone = delta.zones.remove(&0).unwrap();
            delta.zones.insert(id, zone);
            map.merge(&delta).unwrap();
        }
        assert_eq!(map.charged_memory_bytes(), before);
        assert!(map.storage_snapshot().resident_bytes <= 16 * PAGE_SIZE as u64);
    }

    #[test]
    fn zone_map_paged_eviction_releases_resident_bytes() {
        let map = paged_test_map(1, 64, 99);
        map.merge(&paged_delta(42)).unwrap();
        assert!(map.storage_snapshot().resident_bytes > 0);
        map.pool.flush_and_evict_all().unwrap();
        assert_eq!(map.storage_snapshot().resident_bytes, 0);
        assert_eq!(
            map.read_zone(0, Some(&BTreeSet::from([7])))
                .unwrap()
                .unwrap()
                .columns[&7]
                .min,
            Some(ZoneScalar::Signed(42))
        );
    }

    #[test]
    fn zone_not_equal_only_excludes_exact_singleton_bounds() {
        use crate::parser::BinaryOp::NotEq;
        let mut summary = ZoneColumnSummary::default();
        summary.merge_value(ZoneScalar::Signed(7));
        summary.has_null = true;
        assert_eq!(
            comparison_decision(&summary, NotEq, &ZoneScalar::Signed(7)),
            ZoneMatchDecision::ProvenNoMatch
        );
        assert!(comparison_may_match(
            &summary,
            NotEq,
            &ZoneScalar::Signed(8)
        ));
        summary.merge_value(ZoneScalar::Signed(9));
        assert!(comparison_may_match(
            &summary,
            NotEq,
            &ZoneScalar::Signed(7)
        ));
        summary.state = ZoneMapState::Unknown;
        assert!(comparison_may_match(
            &summary,
            NotEq,
            &ZoneScalar::Signed(7)
        ));

        let mut prefix = ZoneColumnSummary::default();
        prefix.merge_value(ZoneScalar::Text {
            weight: vec![1, 2],
            truncated: true,
        });
        assert!(comparison_may_match(
            &prefix,
            NotEq,
            &ZoneScalar::Text {
                weight: vec![1, 2],
                truncated: false,
            }
        ));
        assert!(comparison_may_match(&prefix, NotEq, &ZoneScalar::Signed(7)));

        // Equal prefixes can leave untruncated min/max objects in place.
        let mut interior = ZoneColumnSummary::default();
        interior.merge_value(ZoneScalar::Text {
            weight: vec![1],
            truncated: false,
        });
        interior.merge_value(ZoneScalar::Text {
            weight: vec![1],
            truncated: true,
        });
        assert!(comparison_may_match(
            &interior,
            NotEq,
            &ZoneScalar::Text {
                weight: vec![1],
                truncated: false,
            }
        ));
        for scalar in [
            ZoneScalar::Unsigned(u64::MAX),
            ZoneScalar::Decimal("7.00".into()),
            ZoneScalar::Float64((-0.0f64).to_bits()),
        ] {
            let mut single = ZoneColumnSummary::default();
            single.merge_value(scalar.clone());
            assert!(!comparison_may_match(&single, NotEq, &scalar));
        }
        let mut nulls = ZoneColumnSummary::default();
        nulls.merge_null();
        assert!(!comparison_may_match(&nulls, NotEq, &ZoneScalar::Signed(7)));
    }

    #[test]
    fn zone_membership_fingerprint_uses_sql_exact_numeric_equality() {
        let signed = zone_membership_hash(&ZoneScalar::Signed(11));
        let unsigned = zone_membership_hash(&ZoneScalar::Unsigned(11));
        let decimal_integer = zone_membership_hash(&ZoneScalar::Decimal("11".into()));
        let decimal_scaled = zone_membership_hash(&ZoneScalar::Decimal("11.00".into()));
        let worker_decimal_integer =
            zone_membership_hash_worker(&WorkerScalar::DecimalCompact { mag: 11, scale: 0 });
        let worker_decimal_scaled = zone_membership_hash_worker(&WorkerScalar::DecimalCompact {
            mag: 1_100,
            scale: 2,
        });
        assert_eq!(signed, unsigned);
        assert_eq!(signed, decimal_integer);
        assert_eq!(signed, decimal_scaled);
        assert_eq!(signed, worker_decimal_integer);
        assert_eq!(signed, worker_decimal_scaled);
        assert_ne!(
            signed,
            zone_membership_hash(&ZoneScalar::Decimal("11.01".into()))
        );
        assert_eq!(
            zone_membership_hash(&ZoneScalar::Decimal("11.01".into())),
            zone_membership_hash_worker(&WorkerScalar::DecimalCompact {
                mag: 1_101,
                scale: 2,
            })
        );
        assert_eq!(
            zone_membership_hash(&ZoneScalar::Decimal("-0.0100".into())),
            zone_membership_hash_worker(&WorkerScalar::DecimalCompact {
                mag: -100,
                scale: 4,
            })
        );
    }

    #[test]
    fn zone_float_zero_has_one_ordering_and_membership_identity() {
        let positive = ZoneScalar::Float64(0.0f64.to_bits());
        let negative = ZoneScalar::Float64((-0.0f64).to_bits());
        assert_eq!(positive.compare(&negative), Some(std::cmp::Ordering::Equal));
        assert_eq!(
            zone_membership_hash(&positive),
            zone_membership_hash(&negative)
        );
    }

    #[test]
    fn incompatible_membership_domain_is_unknown_not_a_rejection() {
        let mut summary = ZoneColumnSummary::default();
        summary.enable_membership();
        summary.merge_membership_value(&ZoneScalar::Signed(11));
        summary.merge_value(ZoneScalar::Signed(11));
        assert_eq!(
            comparison_decision(
                &summary,
                crate::parser::BinaryOp::Eq,
                &ZoneScalar::Decimal("11".into())
            ),
            ZoneMatchDecision::Unknown
        );
        assert!(equality_may_match(
            &summary,
            &ZoneScalar::Decimal("11".into())
        ));
    }

    #[test]
    fn zone_text_prefix_obeys_collation_pad_attribute() {
        let no_pad = crate::charset::collation_id("utf8mb4_0900_ai_ci");
        let pad_space = crate::charset::collation_id("utf8mb4_general_ci");
        assert_eq!(zone_text_prefix_slice("value ", no_pad), "value ");
        assert_eq!(zone_text_prefix_slice("value ", pad_space), "value");
    }

    #[test]
    fn zone_membership_prunes_scattered_equality_inside_min_max() {
        let mut summary = ZoneColumnSummary::default();
        summary.enable_membership();
        for value in [1, 100, 1_000, 10_000] {
            let scalar = ZoneScalar::Signed(value);
            summary.merge_membership_value(&scalar);
            summary.merge_value(scalar);
        }
        assert!(comparison_may_match(
            &summary,
            crate::parser::BinaryOp::Eq,
            &ZoneScalar::Signed(500)
        ));
        assert!(!equality_may_match(&summary, &ZoneScalar::Signed(500)));
        assert!(equality_may_match(&summary, &ZoneScalar::Signed(1_000)));
    }

    #[test]
    fn zone_membership_round_trips_through_wal_v3() {
        let mut column = ZoneColumnSummary::default();
        column.enable_membership();
        for value in 0..64 {
            let scalar = ZoneScalar::Signed(value);
            column.merge_membership_value(&scalar);
            column.merge_value(scalar);
        }
        let expected = column.membership.clone();
        let mut zone = ZoneSummary::default();
        zone.columns.insert(7, column);
        let batch = ZoneMapDeltaBatch {
            storage_id: 9,
            zone_pages: 64,
            schema_fingerprint: 77,
            zones: BTreeMap::from([(3, zone)]),
        };
        let encoded = encode_zone_map_delta_v2(&batch).unwrap();
        assert_eq!(&encoded[..4], ZONE_MAP_DELTA_V4_MAGIC);
        let decoded = decode_zone_map_delta(&encoded).unwrap();
        assert_eq!(decoded.zones[&3].columns[&7].membership, expected);
    }

    #[test]
    fn membership_delta_does_not_unsafely_upgrade_legacy_summary() {
        let mut legacy = ZoneColumnSummary::default();
        legacy.merge_value(ZoneScalar::Signed(7));
        let mut delta = ZoneMembership::empty();
        delta.insert_hash(zone_membership_hash(&ZoneScalar::Signed(9)));
        legacy.merge_membership(&delta);
        legacy.merge_value(ZoneScalar::Signed(9));
        assert!(
            legacy.membership.is_none(),
            "a partial delta cannot describe values that predate membership tracking"
        );
        assert!(equality_may_match(&legacy, &ZoneScalar::Signed(7)));
    }
    #[test]
    fn zone_map_audit_unknown_null_and_prefix_operator_matrix() {
        use crate::parser::BinaryOp::*;
        for state in [ZoneMapState::Unknown, ZoneMapState::Building] {
            let zone = ZoneSummary {
                columns: BTreeMap::from([(
                    1,
                    ZoneColumnSummary {
                        state,
                        ..Default::default()
                    },
                )]),
                ..Default::default()
            };
            assert!(ZonePredicateProgram::NullSafeNull { column_id: 1 }.may_match(&zone));
        }
        let values = [
            b"abcdefghijklmno".as_slice(),
            b"abcdefghijklmnop",
            b"abcdefghijklmnopq",
            b"abcdefghijklmnopr",
            b"z",
        ];
        let scalar = |bytes: &[u8]| ZoneScalar::Binary {
            prefix: bytes[..bytes.len().min(16)].to_vec(),
            truncated: bytes.len() > 16,
        };
        for a in values {
            for b in values {
                for q in values {
                    let mut summary = ZoneColumnSummary::default();
                    summary.merge_value(scalar(a));
                    summary.merge_value(scalar(b));
                    for op in [Eq, NotEq, Lt, LtEq, Gt, GtEq] {
                        let oracle = |v: &[u8]| match op {
                            Eq => v == q,
                            NotEq => v != q,
                            Lt => v < q,
                            LtEq => v <= q,
                            Gt => v > q,
                            GtEq => v >= q,
                            _ => unreachable!(),
                        };
                        if oracle(a) || oracle(b) {
                            assert!(
                                comparison_may_match(&summary, op, &scalar(q)),
                                "{a:?} {b:?} {op:?} {q:?}"
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn zone_map_audit_truncation_survives_all_merge_and_wal_paths() {
        let mut summary = ZoneColumnSummary::default();
        summary.merge_value(ZoneScalar::Binary {
            prefix: b"abcdefghijklmnop".to_vec(),
            truncated: false,
        });
        summary.merge_value(ZoneScalar::Binary {
            prefix: b"abcdefghijklmnop".to_vec(),
            truncated: true,
        });
        assert!(summary.truncated);
        assert!(!summary.min.as_ref().unwrap().was_truncated());
        let batch = ZoneMapDeltaBatch {
            storage_id: 1,
            zone_pages: 64,
            schema_fingerprint: 7,
            zones: BTreeMap::from([(
                0,
                ZoneSummary {
                    columns: BTreeMap::from([(1, summary)]),
                    ..Default::default()
                },
            )]),
        };
        let mut merged = ZoneMapDeltaBatch {
            zones: BTreeMap::new(),
            ..batch.clone()
        };
        merged.merge_from(batch);
        let decoded =
            decode_zone_map_delta_v2(&encode_zone_map_delta_v2(&merged).unwrap()).unwrap();
        let map = paged_test_map(1, 64, 7);
        map.merge(&decoded).unwrap();
        assert!(map.snapshot_for_tests()[&0].columns[&1].truncated);
    }

    #[test]
    fn zone_map_rebuild_snapshot_survives_concurrent_update_and_undo_purge() {
        let (_dir, engine) = switch_engine();
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
            .unwrap();
        engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        engine.execute("ANALYZE TABLE t").unwrap();
        let db = engine.db_for_tests();
        let table = db.snapshot_table_schema("test.t").unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        *ZONE_MAP_REBUILD_SCAN_BARRIER.lock().unwrap() = Some(Arc::clone(&barrier));
        let outcome = std::thread::scope(|scope| {
            let rebuild = scope.spawn(|| {
                db.rebuild_zone_map_for_storage_zones(
                    &table,
                    table.id,
                    &BTreeSet::from([0]),
                    &|| false,
                    None,
                )
            });
            barrier.wait();
            let update = engine.execute("UPDATE t SET v=20 WHERE id=1");
            db.purge_obsolete_undo();
            barrier.wait();
            update.unwrap();
            rebuild.join().unwrap()
        });
        *ZONE_MAP_REBUILD_SCAN_BARRIER.lock().unwrap() = None;
        assert!(
            outcome.is_ok(),
            "rebuild lost its active snapshot: {outcome:?}"
        );
        assert_eq!(db.active_snapshot_count(), 0);
        let response = engine
            .execute("SELECT v FROM t WHERE v=20")
            .unwrap()
            .materialize()
            .unwrap();
        let rows = match response {
            crate::engine::api::QueryResponse::ResultSet(result) => result.rows,
            crate::engine::api::QueryResponse::SharedResultSet(result) => result.rows,
            _ => panic!("expected query rows"),
        };
        assert_eq!(rows, vec![vec![Value::Int(20)]]);
    }

    #[test]
    fn zone_map_audit_analyze_deferred_value_populates_membership() {
        let (_dir, engine) = switch_engine();
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
            .unwrap();
        engine
            .execute("INSERT INTO t VALUES (0,0),(1,50),(2,100)")
            .unwrap();
        engine.execute("ANALYZE TABLE t").unwrap();
        let db = engine.db_for_tests();
        let txn = db.begin_read_only_txn().unwrap();
        let _snapshot_guard = db.register_statement_snapshot(txn.snapshot_scn);
        engine.execute("UPDATE t SET v=51 WHERE id=1").unwrap();
        let table = db.snapshot_table_schema("test.t").unwrap();
        let map = db.zone_map_for(table.id, &table).unwrap();
        let relevant = zone_map_relevant_columns(&table);
        let projected = relevant.iter().map(|(index, _)| *index).collect::<Vec<_>>();
        let pages = db.heap_page_count_for_storage(table.id).unwrap();
        let partial = db
            .rebuild_zone_map_scan_range(
                &table,
                table.id,
                &map,
                txn,
                txn.snapshot_scn,
                &relevant,
                &projected,
                0,
                pages,
                &|| false,
            )
            .unwrap()
            .unwrap();
        assert!(
            !partial.deferred.is_empty(),
            "fixture must resolve an undo version"
        );
        let column = table.columns[1].column_id;
        let summary = &partial.batch.zones.values().next().unwrap().columns[&column];
        assert!(
            summary.membership.is_some(),
            "must actually exercise membership"
        );
        assert!(equality_may_match(summary, &ZoneScalar::Signed(50)));
        db.commit_txn(txn.txn_id).unwrap();
    }

    #[test]
    fn zone_map_paged_sampled_zone_probe_does_not_materialize_other_zones() {
        let map = paged_test_map(1, 64, 99);
        for id in [0, 1024, 1_000_000] {
            let mut delta = paged_delta(id as i64);
            let zone = delta.zones.remove(&0).unwrap();
            delta.zones.insert(id, zone);
            map.merge(&delta).unwrap();
        }
        let zone = map
            .read_zone(1_000_000, Some(&BTreeSet::from([7])))
            .unwrap()
            .unwrap();
        assert_eq!(zone.columns[&7].min, Some(ZoneScalar::Signed(1_000_000)));
        assert!(map
            .read_zone(500_000, Some(&BTreeSet::from([7])))
            .unwrap()
            .is_none());
    }

    #[test]
    fn zone_map_paged_incremental_rebuild_persists_ranges_and_releases_scratch() {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let config = StorageConfig {
            buffer_pool_pages: 128,
            ..StorageConfig::from_env()
        };
        let engine =
            crate::engine::StorageQueryEngine::open_with_storage_config_without_background_workers(
                dir.path(),
                config.clone(),
            )
            .unwrap();
        engine
            .execute("CREATE TABLE t (id INT, v INT, pad VARCHAR(800)) ZONE_SIZE=64K")
            .unwrap();
        engine
            .execute(
                "INSERT INTO t SELECT n,n,REPEAT('x',700) FROM generate_series(1,10000) AS gs(n)",
            )
            .unwrap();
        let db = engine.db_for_tests();
        let table = db.snapshot_table_schema("test.t").unwrap();
        let map = db.zone_map_for(table.id, &table).unwrap();
        map.trusted_base.store(false, Ordering::Release);
        assert!(db
            .rebuild_zone_map_incremental(&table, table.id, &map, &|| false)
            .unwrap());
        let total = map.rebuild_zones_total.load(Ordering::Acquire);
        assert!(total > 64, "fixture must span multiple repair batches");
        assert_eq!(map.rebuild_zones_done.load(Ordering::Acquire), total);
        assert_eq!(map.rebuild_state.load(Ordering::Acquire), 2);
        let column = table.columns[1].column_id;
        for id in 0..total {
            assert_eq!(
                map.read_zone(id, Some(&BTreeSet::from([column])))
                    .unwrap()
                    .unwrap()
                    .columns[&column]
                    .state,
                ZoneMapState::Ready
            );
        }
        db.checkpoint().unwrap();
        db.buffer_pool.flush_and_evict_all().unwrap();
        assert_eq!(map.storage_snapshot().resident_bytes, 0);
        drop(map);
        drop(engine);
        let engine =
            crate::engine::StorageQueryEngine::open_with_storage_config_without_background_workers(
                dir.path(),
                config,
            )
            .unwrap();
        let db = engine.db_for_tests();
        let table = db.snapshot_table_schema("test.t").unwrap();
        let map = db.zone_map_for(table.id, &table).unwrap();
        assert!(map.trusted_base.load(Ordering::Acquire));
        for id in 0..total {
            assert_eq!(
                map.read_zone(id, Some(&BTreeSet::from([column])))
                    .unwrap()
                    .unwrap()
                    .columns[&column]
                    .state,
                ZoneMapState::Ready
            );
        }
    }

    #[test]
    fn analyze_zone_rebuild_releases_its_statistics_snapshot() {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine =
            crate::engine::StorageQueryEngine::open_with_storage_config_without_background_workers(
                dir.path(),
                StorageConfig::from_env(),
            )
            .unwrap();
        engine
            .execute("CREATE TABLE t (id INT, v INT, pad VARCHAR(800)) ZONE_SIZE=64K")
            .unwrap();
        engine.execute("CREATE TABLE unrelated (id INT)").unwrap();
        engine
            .execute(
                "INSERT INTO t SELECT n,n,REPEAT('x',700) FROM generate_series(1,10000) AS gs(n)",
            )
            .unwrap();
        let db = engine.db_for_tests();
        let hook = db.install_analyze_test_hook();
        let worker = Arc::clone(db);
        let analyze = std::thread::spawn(move || worker.analyze_table("test.t"));
        let reached = hook.wait_until_reached(std::time::Duration::from_secs(10));
        // Advance the database frontier after ANALYZE pins its statistics
        // snapshot, without changing any row in the table being analyzed.
        let commit = engine.execute("INSERT INTO unrelated VALUES (1)");
        hook.release();
        let result = analyze.join().unwrap();
        assert!(reached, "ANALYZE did not reach the snapshot hook");
        commit.unwrap();
        result.unwrap();

        let table = db.table("test.t").unwrap();
        let map = db.zone_map_for(table.id, &table).unwrap();
        let zones = map.snapshot_for_tests();
        assert!(zones.len() > 64, "exercise multiple rebuild batches");
        assert_eq!(
            zones.len() as u64,
            db.heap_page_count_for_storage(table.id)
                .unwrap()
                .div_ceil(u64::from(map.zone_pages))
        );
        for (id, zone) in &zones {
            assert_ne!(zone.state, Some(ZoneMapState::Unknown), "zone {id}");
            assert_eq!(
                zone.columns[&table.columns[1].column_id].state,
                ZoneMapState::Ready,
                "zone {id}"
            );
        }
    }

    #[test]
    fn incremental_zone_rebuild_keeps_unknown_for_older_snapshot() {
        let dir = tempfile::Builder::new()
            .prefix("zone-map-")
            .tempdir_in("/tmp/mydb")
            .unwrap();
        let engine =
            crate::engine::StorageQueryEngine::open_with_storage_config_without_background_workers(
                dir.path(),
                StorageConfig::from_env(),
            )
            .unwrap();
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
            .unwrap();
        engine.execute("INSERT INTO t VALUES (1,10)").unwrap();
        let db = engine.db_for_tests();
        let old = db.begin_read_only_txn().unwrap();
        let _snapshot_guard = db.register_statement_snapshot(old.snapshot_scn);
        engine.execute("UPDATE t SET v=20 WHERE id=1").unwrap();
        let table = db.snapshot_table_schema("test.t").unwrap();
        let map = db.zone_map_for(table.id, &table).unwrap();
        map.trusted_base.store(false, Ordering::Release);
        assert!(db
            .rebuild_zone_map_incremental(&table, table.id, &map, &|| false)
            .unwrap());
        assert_eq!(
            map.snapshot_for_tests()[&0].state,
            Some(ZoneMapState::Unknown)
        );
        db.commit_txn(old.txn_id).unwrap();
    }

    #[test]
    fn zone_frame_admission_failure_does_not_publish_corrupt_or_missing_base() {
        let dir = tempfile::Builder::new()
            .prefix("zone-map-")
            .tempdir_in("/tmp/mydb")
            .unwrap();
        let db = Database::open(dir.path()).unwrap();
        db.create_table("t", vec![zone_map_test_column()], vec!["id".into()], vec![])
            .unwrap();
        let txn = db.begin_dml_txn().unwrap();
        db.insert_rows(txn, "test.t", vec![vec![Value::Int(42)]])
            .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        let table = db.table("t").unwrap();
        db.rebuild_zone_map_for_table(&table).unwrap();
        db.checkpoint().unwrap();
        db.forget_zone_map(table.id).unwrap();
        let corrupt = ZONE_MAP_DECODE_FAILURES.load(Ordering::Relaxed);
        {
            let _failure = ZoneMapFailpointGuard::install(7);
            assert!(db.zone_map_for(table.id, &table).is_none());
            assert!(!db.zone_maps.read().unwrap().contains_key(&table.id));
            assert_eq!(ZONE_MAP_DECODE_FAILURES.load(Ordering::Relaxed), corrupt);
        }
        let reloaded = db.zone_map_for(table.id, &table).unwrap();
        assert!(reloaded.trusted_base.load(Ordering::Acquire));
        assert_eq!(reloaded.rebuild_started_ms.load(Ordering::Acquire), 0);
    }

    #[test]
    fn zone_cache_admission_failure_preserves_base_validity() {
        let map = paged_test_map(1, 64, 7);
        assert!(!map.sync_derived_charge_to(usize::MAX));
        assert!(map.trusted_base.load(Ordering::Acquire));
        assert!(!map.cache_admitted.load(Ordering::Acquire));
        assert!(map.sync_derived_charge());
        assert!(map.cache_admitted.load(Ordering::Acquire));
        // Conversely, memory becoming available cannot validate a missing
        // or corrupt base: only a validated load/rebuild may do that.
        map.trusted_base.store(false, Ordering::Release);
        assert!(!map.sync_derived_charge_to(usize::MAX));
        assert!(map.sync_derived_charge());
        assert!(!map.trusted_base.load(Ordering::Acquire));
    }

    #[test]
    fn zone_rebuild_error_survives_successful_sidecar_write() {
        let map = paged_test_map(1, 64, 7);

        *map.rebuild_error.lock().unwrap() = Some("scan failed".to_string());
        map.dirty.store(true, Ordering::Release);
        map.write_to_file(0).unwrap();
        assert_eq!(
            map.rebuild_error.lock().unwrap().as_deref(),
            Some("scan failed")
        );
    }

    #[test]
    fn zone_map_audit_hot_zone_does_not_starve_cold_zone() {
        let dir = tempfile::Builder::new()
            .prefix("zone-map-")
            .tempdir_in("/tmp/mydb")
            .unwrap();
        let engine =
            crate::engine::StorageQueryEngine::open_with_storage_config_without_background_workers(
                dir.path(),
                StorageConfig::from_env(),
            )
            .unwrap();
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, pad VARCHAR(800)) ZONE_SIZE=64K")
            .unwrap();
        engine
            .execute(
                "INSERT INTO t SELECT n,n,REPEAT('x',700) FROM generate_series(1,600) AS gs(n)",
            )
            .unwrap();
        engine.execute("ANALYZE TABLE t").unwrap();
        let db = engine.db_for_tests();
        let table = db.snapshot_table_schema("test.t").unwrap();
        let map = db.zone_map_for(table.id, &table).unwrap();
        let column = table.columns[1].column_id;
        {
            let _gate = map.data_gate.write().unwrap();
            let mut zones = map.snapshot_for_tests();
            assert!(zones.len() > 1);
            for (&id, zone) in &mut zones {
                zone.columns.get_mut(&column).unwrap().mark_unknown();
                map.set_zone_mutation(id, ZoneMutation::default()).unwrap();
            }
            map.replace_zones(&zones).unwrap();
            map.set_zone_mutation(
                0,
                ZoneMutation {
                    epoch: 1,
                    last_ms: now_unix_ms(),
                },
            )
            .unwrap();
            map.last_mutation_ms.store(now_unix_ms(), Ordering::Release);
        }
        assert_eq!(db.maintain_zone_maps().unwrap(), 1);
        let zones = map.snapshot_for_tests();
        assert_eq!(zones[&0].columns[&column].state, ZoneMapState::Unknown);
        assert_eq!(zones[&1].columns[&column].state, ZoneMapState::Ready);
    }

    #[test]
    fn zone_map_audit_maintenance_failure_does_not_skip_next_table() {
        let dir = tempfile::Builder::new()
            .prefix("zone-map-")
            .tempdir_in("/tmp/mydb")
            .unwrap();
        let engine =
            crate::engine::StorageQueryEngine::open_with_storage_config_without_background_workers(
                dir.path(),
                StorageConfig::from_env(),
            )
            .unwrap();
        for name in ["a", "b"] {
            engine
                .execute(&format!("CREATE TABLE {name} (id INT) ZONE_SIZE=64K"))
                .unwrap();
            engine
                .execute(&format!("INSERT INTO {name} VALUES (1)"))
                .unwrap();
            engine.execute(&format!("ANALYZE TABLE {name}")).unwrap();
        }
        let db = engine.db_for_tests();
        let tables = db
            .catalog_read()
            .unwrap()
            .tables
            .values()
            .filter(|table| table.zone_map_enabled())
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(tables.len(), 2);
        let broken = db.zone_map_for(tables[0].id, &tables[0]).unwrap();
        let healthy = db.zone_map_for(tables[1].id, &tables[1]).unwrap();
        {
            let _gate = healthy.data_gate.write().unwrap();
            let mut zones = healthy.snapshot_for_tests();
            zones.get_mut(&0).unwrap().state = Some(ZoneMapState::Unknown);
            healthy.replace_zones(&zones).unwrap();
            healthy
                .set_zone_mutation(0, ZoneMutation::default())
                .unwrap();
        }
        {
            let _gate = broken.data_gate.write().unwrap();
            broken
                .set_zone_mutation(0, ZoneMutation::default())
                .unwrap();
        }
        let poisoned = Arc::clone(&broken);
        assert!(std::thread::spawn(move || {
            let _guard = poisoned.data_gate.write().unwrap();
            panic!("injected local map failure");
        })
        .join()
        .is_err());
        assert!(db.maintain_zone_maps().is_err());
        assert!(broken.last_error.lock().unwrap().is_some());
        assert!(broken.maintenance_retry_ms.load(Ordering::Acquire) > now_unix_ms());
        assert_ne!(
            healthy.snapshot_for_tests()[&0].state,
            Some(ZoneMapState::Unknown)
        );
        assert_eq!(healthy.rebuild_state.load(Ordering::Acquire), 2);
    }

    #[test]
    fn zone_map_unknown_proof_reuses_validation_and_rebuild_invalidates_it() {
        let map = paged_test_map(1, 64, 7);
        let mut unknown = ZoneColumnSummary::default();
        unknown.mark_unknown();
        let mut zones = BTreeMap::from([(
            0,
            ZoneSummary {
                row_count_estimate: 100,
                columns: BTreeMap::from([(7, unknown.clone())]),
                ..Default::default()
            },
        )]);
        map.replace_zones(&zones).unwrap();
        let before = map.pool.buffer_stats().hits;
        assert!(map.note_summary_neutral_mutations(&[0], &[7]));
        let first = map.pool.buffer_stats().hits - before;
        let before = map.pool.buffer_stats().hits;
        assert!(map.note_summary_neutral_mutations(&[0], &[7]));
        let second = map.pool.buffer_stats().hits - before;
        assert!(
            second < first,
            "unchanged Unknown proof was decoded again: {first}/{second}"
        );
        assert!(map.sync_derived_charge());
        assert_eq!(map.charged_memory_bytes(), map.estimated_memory_bytes());
        let mut ready = ZoneColumnSummary::default();
        ready.observe_scalar(ZoneScalar::Signed(10));
        zones.get_mut(&0).unwrap().columns.insert(7, ready);
        map.replace_zones(&zones).unwrap();
        assert!(!map.note_summary_neutral_mutations(&[0], &[7]));
        // Large column ids retain the ordinary validated fallback.
        zones.get_mut(&0).unwrap().columns.insert(65, unknown);
        map.replace_zones(&zones).unwrap();
        assert!(map.note_summary_neutral_mutations(&[0], &[65]));
        assert!(!map.note_summary_neutral_mutations(&[0], &[7, 65]));
        let reopened = map.reopen();
        assert!(!reopened.note_summary_neutral_mutations(&[0], &[7]));
    }

    #[test]
    fn zone_map_unknown_proof_tags_prevent_slot_aliases() {
        let map = paged_test_map(1, 64, 7);
        let mut unknown = ZoneColumnSummary::default();
        unknown.mark_unknown();
        let zones = [0, 4096]
            .into_iter()
            .map(|id| {
                (
                    id,
                    ZoneSummary {
                        columns: BTreeMap::from([(7, unknown.clone())]),
                        ..Default::default()
                    },
                )
            })
            .collect();
        map.replace_zones(&zones).unwrap();
        assert!(map.note_summary_neutral_mutations(&[0], &[7]));
        assert!(map.cached_unknown_columns(0, &[7]));
        assert!(!map.cached_unknown_columns(0, &[7, 8]));
        assert!(map.note_summary_neutral_mutations(&[4096], &[7]));
        assert!(map.cached_unknown_columns(4096, &[7]));
        assert!(!map.cached_unknown_columns(0, &[7]));
        assert!(!map.cached_unknown_columns(u64::MAX, &[7]));
        map.reset_unknown().unwrap();
        assert!(!map.cached_unknown_columns(4096, &[7]));
        assert!(map.sync_derived_charge());
        assert_eq!(map.charged_memory_bytes(), map.estimated_memory_bytes());
    }

    #[test]
    fn zone_map_neutral_stale_credit_preserves_counts_and_recovery_upper_bound() {
        let map = paged_test_map(1, 64, 7);
        let mut column = ZoneColumnSummary::default();
        column.enable_membership();
        column.observe_scalar(ZoneScalar::Signed(10));
        let zones = BTreeMap::from([(
            0,
            ZoneSummary {
                row_count_estimate: 1000,
                columns: BTreeMap::from([(1, column.clone())]),
                ..Default::default()
            },
        )]);
        map.replace_zones(&zones).unwrap();
        let mut delta = ZoneMapDeltaBatch {
            storage_id: 1,
            zone_pages: 64,
            schema_fingerprint: 7,
            zones: BTreeMap::from([(
                0,
                ZoneSummary {
                    stale_mutations: 1,
                    columns: BTreeMap::from([(1, column)]),
                    ..Default::default()
                },
            )]),
        };
        let initial = map.log.lock().last_lsn();
        map.merge(&delta).unwrap();
        let charged = map.log.lock().last_lsn();
        assert!(charged > initial, "first recharge is included in WAL cost");
        for stale in 2..=64 {
            map.merge(&delta).unwrap();
            assert_eq!(
                map.read_zone(0, None).unwrap().unwrap().stale_mutations,
                stale
            );
            assert_eq!(map.zone_update_status(0, &[1]).unwrap().unwrap().1, stale);
        }
        assert_eq!(map.log.lock().last_lsn(), charged);
        map.merge(&delta).unwrap();
        assert!(map.log.lock().last_lsn() > charged);
        assert_eq!(map.read_zone(0, None).unwrap().unwrap().stale_mutations, 65);
        let reopened = map.reopen();
        assert_eq!(
            reopened
                .read_zone(0, None)
                .unwrap()
                .unwrap()
                .stale_mutations,
            128
        );
        drop(reopened);
        // A changed proof must consume the exact runtime count and retain
        // the full root revocation/publication protocol.
        delta
            .zones
            .get_mut(&0)
            .unwrap()
            .columns
            .get_mut(&1)
            .unwrap()
            .observe_scalar(ZoneScalar::Signed(20));
        let before = map.log.lock().last_lsn();
        map.merge(&delta).unwrap();
        assert!(map.log.lock().last_lsn() > before);
        assert_eq!(map.read_zone(0, None).unwrap().unwrap().stale_mutations, 66);
        assert_eq!(map.neutral_mutations.lock().unwrap().stale_credit(0), 0);
        assert_eq!(
            map.reopen()
                .read_zone(0, None)
                .unwrap()
                .unwrap()
                .stale_mutations,
            66
        );
        map.replace_zones(&zones).unwrap();
        assert_eq!(map.read_zone(0, None).unwrap().unwrap().stale_mutations, 0);
        delta.zones.get_mut(&0).unwrap().row_count_estimate = 1;
        map.merge(&delta).unwrap();
        assert_eq!(
            map.read_zone(0, None).unwrap().unwrap().row_count_estimate,
            1001
        );

        map.replace_zones(&zones).unwrap();
        delta.zones.get_mut(&0).unwrap().row_count_estimate = 0;
        delta.zones.get_mut(&0).unwrap().columns = zones[&0].columns.clone();
        map.merge(&delta).unwrap();
        assert_eq!(map.read_zone(0, None).unwrap().unwrap().stale_mutations, 1);
        assert!(zone_map_maintenance_targets(
            &map.snapshot_for_tests(),
            1,
            0.0,
            &BTreeSet::from([1])
        )
        .is_empty());
        let overflow = map.neutral_mutations.lock().unwrap().overflow_epoch;
        for id in 1..=NEUTRAL_MUTATION_ZONES as u64 {
            map.neutral_mutations.lock().unwrap().note(
                id,
                ZoneMutation {
                    epoch: 100,
                    last_ms: 1,
                },
            );
        }
        assert!(map.neutral_mutations.lock().unwrap().overflow_epoch > overflow);
        assert_eq!(map.read_zone(0, None).unwrap().unwrap().stale_mutations, 64);
        assert!(zone_map_maintenance_targets(
            &map.snapshot_for_tests(),
            1,
            0.0,
            &BTreeSet::from([1])
        )
        .contains(&0));
        map.replace_zones(&zones).unwrap();
        map.merge(&delta).unwrap();
        assert_eq!(map.neutral_mutations.lock().unwrap().stale_credit(0), 63);
        map.replace_zones(&zones).unwrap();
        assert_eq!(map.neutral_mutations.lock().unwrap().stale_credit(0), 0);
        assert_eq!(map.read_zone(0, None).unwrap().unwrap().stale_mutations, 0);
    }

    #[test]
    fn zone_map_covered_inserts_amortize_count_redo_without_losing_counts() {
        let map = paged_test_map(1, 64, 7);
        let mut column = ZoneColumnSummary::default();
        column.enable_membership();
        column.observe_scalar(ZoneScalar::Signed(10));
        let zones = BTreeMap::from([(
            0,
            ZoneSummary {
                row_count_estimate: 1000,
                columns: BTreeMap::from([(1, column.clone())]),
                ..Default::default()
            },
        )]);
        map.replace_zones(&zones).unwrap();
        let mut delta = ZoneMapDeltaBatch {
            storage_id: 1,
            zone_pages: 64,
            schema_fingerprint: 7,
            zones: BTreeMap::from([(
                0,
                ZoneSummary {
                    row_count_estimate: 1,
                    stale_mutations: 1,
                    columns: BTreeMap::from([(1, column)]),
                    ..Default::default()
                },
            )]),
        };
        map.merge(&delta).unwrap();
        let charged = map.log.lock().last_lsn();
        for count in 2..=64 {
            map.merge(&delta).unwrap();
            let zone = map.read_zone(0, None).unwrap().unwrap();
            assert_eq!(zone.row_count_estimate, 1000 + count);
            assert_eq!(zone.stale_mutations, count);
            assert_eq!(
                map.zone_update_status(0, &[1]).unwrap().unwrap().0,
                1000 + count
            );
        }
        assert_eq!(
            map.log.lock().last_lsn(),
            charged,
            "covered insert counts must not rewrite summary pages"
        );
        map.merge(&delta).unwrap();
        assert!(map.log.lock().last_lsn() > charged);
        let reopened = map.reopen();
        let zone = reopened.read_zone(0, None).unwrap().unwrap();
        assert_eq!(zone.row_count_estimate, 1128);
        assert_eq!(zone.stale_mutations, 128);
        drop(reopened);
        delta
            .zones
            .get_mut(&0)
            .unwrap()
            .columns
            .get_mut(&1)
            .unwrap()
            .observe_scalar(ZoneScalar::Signed(20));
        map.merge(&delta).unwrap();
        let reopened = map.reopen();
        let zone = reopened.read_zone(0, None).unwrap().unwrap();
        assert_eq!(zone.row_count_estimate, 1066);
        assert_eq!(zone.stale_mutations, 66);
        assert_eq!(zone.columns[&1].max, Some(ZoneScalar::Signed(20)));
        drop(reopened);
        map.replace_zones(&zones).unwrap();
        assert_eq!(
            map.read_zone(0, None).unwrap().unwrap().row_count_estimate,
            1000
        );
        // Recharging one dimension must retain the other dimension's credit.
        delta.zones.get_mut(&0).unwrap().columns = zones[&0].columns.clone();
        delta.zones.get_mut(&0).unwrap().stale_mutations = 0;
        map.merge(&delta).unwrap();
        delta.zones.get_mut(&0).unwrap().row_count_estimate = 0;
        delta.zones.get_mut(&0).unwrap().stale_mutations = 1;
        map.merge(&delta).unwrap();
        let charged = map.log.lock().last_lsn();
        for _ in 2..=64 {
            delta.zones.get_mut(&0).unwrap().row_count_estimate = 1;
            delta.zones.get_mut(&0).unwrap().stale_mutations = 0;
            map.merge(&delta).unwrap();
            delta.zones.get_mut(&0).unwrap().row_count_estimate = 0;
            delta.zones.get_mut(&0).unwrap().stale_mutations = 1;
            map.merge(&delta).unwrap();
        }
        assert_eq!(map.log.lock().last_lsn(), charged);
        let zone = map.read_zone(0, None).unwrap().unwrap();
        assert_eq!(zone.row_count_estimate, 1064);
        assert_eq!(zone.stale_mutations, 64);
        delta.zones.get_mut(&0).unwrap().row_count_estimate = 1;
        delta.zones.get_mut(&0).unwrap().stale_mutations = 0;
        map.merge(&delta).unwrap();
        for id in 1..=NEUTRAL_MUTATION_ZONES as u64 {
            map.neutral_mutations.lock().unwrap().note(
                id,
                ZoneMutation {
                    epoch: 1000,
                    last_ms: 1,
                },
            );
        }
        assert_eq!(
            map.read_zone(0, None).unwrap().unwrap().row_count_estimate,
            1128
        );
        let mut saturated = zones.clone();
        saturated.get_mut(&0).unwrap().row_count_estimate = u64::MAX - 1;
        saturated.get_mut(&0).unwrap().stale_mutations = u64::MAX - 1;
        map.replace_zones(&saturated).unwrap();
        delta.zones.get_mut(&0).unwrap().stale_mutations = 1;
        for _ in 0..2 {
            map.merge(&delta).unwrap();
            let zone = map.read_zone(0, None).unwrap().unwrap();
            assert_eq!(zone.row_count_estimate, u64::MAX);
            assert_eq!(zone.stale_mutations, u64::MAX);
        }
    }

    #[test]
    fn zone_map_audit_neutral_update_rechecks_rebuilt_summary() {
        let map = paged_test_map(1, 64, 7);
        let mut zones = BTreeMap::from([(
            0,
            ZoneSummary {
                columns: BTreeMap::from([(1, ZoneColumnSummary::default())]),
                ..Default::default()
            },
        )]);
        {
            let _gate = map.data_gate.write().unwrap();
            map.replace_zones(&zones).unwrap();
        }
        assert!(!map.note_summary_neutral_mutations(&[0], &[1]));
        zones
            .get_mut(&0)
            .unwrap()
            .columns
            .get_mut(&1)
            .unwrap()
            .mark_unknown();
        {
            let _gate = map.data_gate.write().unwrap();
            map.replace_zones(&zones).unwrap();
        }
        let runtime_epoch = map.mutation_epoch.load(Ordering::Acquire);
        let before = map.log.lock().last_lsn();
        for _ in 0..100 {
            assert!(map.note_summary_neutral_mutations(&[0], &[1]));
        }
        assert_eq!(
            map.log.lock().last_lsn(),
            before,
            "persistent Unknown needs no repeated page redo"
        );
        assert_eq!(
            map.mutation_epoch.load(Ordering::Acquire),
            runtime_epoch + 100
        );
        assert_eq!(
            map.neutral_mutations.lock().unwrap().get(0).unwrap().epoch,
            runtime_epoch + 100
        );
        let reopened = map.reopen();
        assert!(reopened.neutral_mutations.lock().unwrap().get(0).is_none());
        assert_eq!(
            reopened.read_zone(0, None).unwrap().unwrap().columns[&1].state,
            ZoneMapState::Unknown
        );
        assert!(reopened.note_summary_neutral_mutations(&[0], &[1]));
        assert_eq!(map.log.lock().last_lsn(), before);
        reopened
            .set_zone_mutation(
                0,
                ZoneMutation {
                    epoch: 1_000_000,
                    last_ms: 0,
                },
            )
            .unwrap();
        let before = map.log.lock().last_lsn();
        assert!(reopened.note_summary_neutral_mutations(&[0], &[1]));
        assert!(
            reopened.zone_mutation(0).unwrap().unwrap().epoch > 1_000_000,
            "a reopened handle must advance beyond persisted zone epochs"
        );
        assert_eq!(map.log.lock().last_lsn(), before);
    }
    #[test]
    fn zone_map_neutral_runtime_frontier_is_bounded_and_fences_eviction() {
        let mut frontier = NeutralMutations::default();
        let capacity = NEUTRAL_MUTATION_ZONES as u64;
        for id in 0..capacity {
            frontier.note(
                id,
                ZoneMutation {
                    epoch: 10,
                    last_ms: 1,
                },
            );
        }
        for epoch in 11..100 {
            frontier.note(
                0,
                ZoneMutation {
                    epoch,
                    last_ms: epoch,
                },
            );
        }
        assert_eq!(
            frontier.overflow_epoch, 0,
            "one hot zone must not invalidate cold-zone builders"
        );
        assert_eq!(frontier.get(1).unwrap().epoch, 10);
        frontier.note(
            capacity,
            ZoneMutation {
                epoch: 100,
                last_ms: 100,
            },
        );
        assert_eq!(frontier.overflow_epoch, 1);
        assert!(frontier.get(0).is_none());
        frontier.note(
            0,
            ZoneMutation {
                epoch: 101,
                last_ms: 101,
            },
        );
        assert_eq!(
            frontier.overflow_epoch, 2,
            "eviction/reinsertion must not hide mutations through ABA"
        );
        assert_eq!(frontier.zones.len(), NEUTRAL_MUTATION_ZONES);
        assert_eq!(frontier.order.len(), NEUTRAL_MUTATION_ZONES);
    }

    #[test]
    #[ignore = "manual metadata directory benchmark"]
    fn zone_map_paged_directory_benchmark() {
        let map = paged_test_map(1, 64, 99);
        for id in 0..4096 {
            let mut delta = paged_delta(id as i64);
            let zone = delta.zones.remove(&0).unwrap();
            delta.zones.insert(id, zone);
            map.merge(&delta).unwrap();
        }
        let started = std::time::Instant::now();
        for id in (0..4096).step_by(31) {
            let zone = map
                .read_zone(id, Some(&BTreeSet::from([7])))
                .unwrap()
                .unwrap();
            assert_eq!(zone.columns[&7].min, Some(ZoneScalar::Signed(id as i64)));
        }
        eprintln!("paged sampled probes: {:?}", started.elapsed());
    }
    fn switch_engine() -> (tempfile::TempDir, crate::engine::StorageQueryEngine) {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::Builder::new()
            .prefix("zone-storage-")
            .tempdir_in("/tmp/mydb")
            .unwrap();
        let mut config = StorageConfig::from_env();
        config.buffer_pool_pages = 256;
        let engine =
            crate::engine::StorageQueryEngine::open_with_storage_config_without_background_workers(
                dir.path(),
                config,
            )
            .unwrap();
        (dir, engine)
    }

    #[test]
    fn zone_map_global_switch_revokes_only_modified_paged_roots() {
        let (_dir, engine) = switch_engine();
        for name in ["zm", "untouched"] {
            engine
                .execute(&format!("CREATE TABLE {name} (id INT PRIMARY KEY, v INT)"))
                .unwrap();
            engine
                .execute(&format!("INSERT INTO {name} VALUES (1,1),(2,2)"))
                .unwrap();
            engine.execute(&format!("ANALYZE TABLE {name}")).unwrap();
        }
        let db = engine.db_for_tests();
        db.checkpoint().unwrap();
        let untouched = db.zone_map_file_path(db.table("test.untouched").unwrap().id);
        let before = fs::read(&untouched).unwrap();
        db.set_zone_map_enabled(false).unwrap();
        engine.execute("INSERT INTO zm VALUES (3,300)").unwrap();
        engine.execute("UPDATE zm SET v=-100 WHERE id=1").unwrap();
        engine.execute("DELETE FROM zm WHERE id=2").unwrap();
        engine.execute("ANALYZE TABLE zm").unwrap();
        assert_eq!(db.maintain_zone_maps().unwrap(), 0);
        db.checkpoint().unwrap();
        assert_eq!(
            fs::read(&untouched).unwrap(),
            before,
            "unchanged roots need no invalidation"
        );
        let table = db.table("test.zm").unwrap();
        let filter = Expr::Binary {
            left: Box::new(Expr::Identifier("v".into())),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Int(-100))),
        };
        assert!(db
            .zone_map_candidate_ranges_for_plan(&table, &filter)
            .is_none());
        assert!(db.zone_map_cost_estimate_forced(&table, &filter).is_none());
        db.set_zone_map_enabled(true).unwrap();
        assert!(db
            .zone_map_candidate_ranges_for_plan(&table, &filter)
            .is_none_or(|ranges| !ranges.is_empty()));
        let crate::engine::QueryResponse::ResultSet(result) = engine
            .execute("SELECT /*+ ZONE_MAP(zm) */ id,v FROM zm WHERE v=-100")
            .unwrap()
        else {
            panic!("result")
        };
        assert_eq!(result.rows, vec![vec![Value::Int(1), Value::Int(-100)]]);
        engine.execute("ANALYZE TABLE zm").unwrap();
        assert!(db
            .zone_map_candidate_ranges_for_plan(&table, &filter)
            .is_some());
    }

    #[test]
    fn zone_map_global_switch_unmodified_roots_remain_usable() {
        let (_dir, engine) = switch_engine();
        engine.execute("CREATE TABLE zm (v INT)").unwrap();
        engine.execute("INSERT INTO zm VALUES (1)").unwrap();
        engine.execute("ANALYZE TABLE zm").unwrap();
        let db = engine.db_for_tests();
        db.checkpoint().unwrap();
        let table = db.table("test.zm").unwrap();
        let before = fs::read(db.zone_map_file_path(table.id)).unwrap();
        db.set_zone_map_enabled(false).unwrap();
        db.set_zone_map_enabled(true).unwrap();
        db.checkpoint().unwrap();
        assert_eq!(fs::read(db.zone_map_file_path(table.id)).unwrap(), before);
        let predicate = Expr::Binary {
            left: Box::new(Expr::Identifier("v".into())),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Int(999))),
        };
        assert!(db
            .zone_map_candidate_ranges(&table, &predicate)
            .unwrap()
            .is_empty());
    }

    struct ZoneScratchLimitGuard(u64);
    impl ZoneScratchLimitGuard {
        fn install(limit: u64) -> Self {
            Self(ZONE_MAP_TEST_SCRATCH_LIMIT.swap(limit, Ordering::AcqRel))
        }
    }
    impl Drop for ZoneScratchLimitGuard {
        fn drop(&mut self) {
            ZONE_MAP_TEST_SCRATCH_LIMIT.store(self.0, Ordering::Release);
        }
    }

    #[test]
    fn zone_map_candidate_lease_follows_iterator_and_worker_thread() {
        use crate::engine::executor::mem_budget::controlled_memory_snapshot;
        let baseline = controlled_memory_snapshot().2[1];
        let mut lease = ZoneScratchLease::default();
        let mut values = Vec::new();
        zone_reserve_vec(&mut values, &mut lease, 3).unwrap();
        values.extend([(0, 1), (2, 3), (4, 5)]);
        let bytes = lease.bytes();
        let ranges = ZoneMapRanges {
            ranges: smallvec::SmallVec::from_vec(values),
            lease,
        };
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let worker_barrier = Arc::clone(&barrier);
        let worker = std::thread::spawn(move || {
            let mut iter = ranges.into_iter();
            assert_eq!(iter.next(), Some((0, 1)));
            worker_barrier.wait();
            worker_barrier.wait();
            assert_eq!(iter.next_back(), Some((4, 5)));
            // Early cancellation drops the unconsumed allocation and lease.
        });
        barrier.wait();
        let retained = controlled_memory_snapshot().2[1];
        barrier.wait();
        worker.join().unwrap();
        assert_eq!(retained, baseline + bytes);
        assert_eq!(controlled_memory_snapshot().2[1], baseline);
        let inline = ZoneMapRanges::full(10);
        assert_eq!(&*inline, &[(0, 10)]);
        assert_eq!(controlled_memory_snapshot().2[1], baseline);
    }

    #[test]
    fn zone_map_owned_summary_lease_survives_return_and_clone_is_independent() {
        use crate::engine::executor::mem_budget::controlled_memory_snapshot;
        let map = paged_test_map(1, 64, 99);
        // This test compares exact process-wide scratch charges. A cleaner
        // completing a page write between snapshots can release its unrelated
        // 32 KiB workspace and invalidate that comparison.
        map.pool.shutdown_flusher();
        let mut delta = paged_delta(10);
        let column = delta
            .zones
            .get_mut(&0)
            .unwrap()
            .columns
            .get_mut(&7)
            .unwrap();
        *column = ZoneColumnSummary::default();
        column.merge_value(ZoneScalar::Text {
            weight: vec![b'a'; 2048],
            truncated: true,
        });
        map.merge(&delta).unwrap();
        let before = controlled_memory_snapshot().2[1];
        let owned = map
            .read_zone(0, Some(&BTreeSet::from([7])))
            .unwrap()
            .unwrap();
        let held = owned.lease.bytes();
        assert!(held >= 4096);
        assert_eq!(
            controlled_memory_snapshot().2[1],
            before + held,
            "returning the object must retain its scratch lease"
        );
        let cloned = ChargedZoneSummary::try_clone_from(&owned).unwrap();
        assert_eq!(
            controlled_memory_snapshot().2[1],
            before + held + cloned.lease.bytes()
        );
        drop(cloned);
        assert_eq!(controlled_memory_snapshot().2[1], before + held);
        {
            let _limit = ZoneScratchLimitGuard::install(0);
            assert!(ChargedZoneSummary::try_clone_from(&owned).is_err());
            assert!(map.read_zone(0, Some(&BTreeSet::from([7]))).is_err());
            assert!(
                map.trusted_base.load(Ordering::Acquire),
                "optional read admission must not corrupt a trusted root"
            );
            assert_eq!(controlled_memory_snapshot().2[1], before + held);
        }
        let entries = map.encoded_zone(0, &owned).unwrap();
        assert!(controlled_memory_snapshot().2[1] >= before + held + entries.lease.bytes());
        drop(entries);
        drop(owned);
        assert_eq!(controlled_memory_snapshot().2[1], before);
        let mut rejected = ZoneScratchLease::default();
        assert!(
            rejected.reserve(usize::MAX).is_err(),
            "real governor must reject an impossible optional reservation"
        );
        assert_eq!(controlled_memory_snapshot().2[1], before);
    }

    #[test]
    fn zone_map_dml_scratch_rejection_durably_invalidates_without_losing_rows() {
        let (dir, engine) = switch_engine();
        engine
            .execute("CREATE TABLE zm (id INT PRIMARY KEY,v INT)")
            .unwrap();
        engine.execute("INSERT INTO zm VALUES (1,1)").unwrap();
        engine.execute("ANALYZE TABLE zm").unwrap();
        {
            let _limit = ZoneScratchLimitGuard::install(0);
            engine.execute("INSERT INTO zm VALUES (2,999)").unwrap();
            engine.execute("UPDATE zm SET v=-999 WHERE id=1").unwrap();
        }
        engine.db_for_tests().checkpoint().unwrap();
        drop(engine);
        let engine =
            crate::engine::StorageQueryEngine::open_with_storage_config_without_background_workers(
                dir.path(),
                StorageConfig::from_env(),
            )
            .unwrap();
        for hint in ["ZONE_MAP", "NO_ZONE_MAP"] {
            let crate::engine::QueryResponse::ResultSet(result) = engine
                .execute(&format!(
                    "SELECT /*+ {hint}(zm) */ id,v FROM zm WHERE v IN(-999,999) ORDER BY id"
                ))
                .unwrap()
            else {
                panic!("result")
            };
            assert_eq!(
                result.rows,
                vec![
                    vec![Value::Int(1), Value::Int(-999)],
                    vec![Value::Int(2), Value::Int(999)]
                ]
            );
        }
        engine.execute("ANALYZE TABLE zm").unwrap();
        let table = engine.db_for_tests().table("test.zm").unwrap();
        assert!(
            !engine
                .db_for_tests()
                .zone_map_for(table.id, &table)
                .unwrap()
                .storage_snapshot()
                .pending
        );
    }

    #[test]
    fn zone_map_dml_unordered_batches_bound_distinct_zones_and_coalesce_neutral_runs() {
        let (dir, engine) = switch_engine();
        engine
            .execute("CREATE TABLE zm (id INT PRIMARY KEY,v INT,pad VARCHAR(800)) ZONE_SIZE=64K")
            .unwrap();
        engine
            .execute(
                "INSERT INTO zm SELECT n,n,REPEAT('x',700) FROM generate_series(1,8000) AS gs(n)",
            )
            .unwrap();
        let db = engine.db_for_tests();
        let table = db.table("test.zm").unwrap();
        let map = db.zone_map_for(table.id, &table).unwrap();
        let read = db.begin_read_only_txn().unwrap();
        let mut samples = BTreeMap::new();
        for row in db.scan_rows("test.zm", read).unwrap() {
            samples
                .entry(u64::from(row.row_id.page_id) / u64::from(map.zone_pages))
                .or_insert((row.row_id, row.values));
        }
        db.commit_txn(read.txn_id).unwrap();
        assert!(samples.len() > ZONE_MAP_DML_MAX_ZONES);
        let mut permutation = samples.keys().copied().collect::<Vec<_>>();
        permutation.sort_by_key(|id| id.wrapping_mul(0x9e3779b97f4a7c15));
        let mut ids = Vec::new();
        let mut rows = Vec::new();
        for _ in 0..4 {
            for id in &permutation {
                ids.push(samples[id].0);
                rows.push(samples[id].1.clone());
            }
        }
        assert!(rows.len() >= ZONE_MAP_DEFER_ALL_BATCH_ROWS);
        let txn = db.begin_dml_txn().unwrap();
        ZONE_MAP_TEST_DML_MAX_ZONES.store(0, Ordering::Release);
        // Re-observe real physical rows to isolate the derived batching layer;
        // widening metadata must never change their stored values.
        db.merge_zone_map_rows(txn.txn_id, table.id, &ids, &rows)
            .unwrap();
        assert_eq!(ZONE_MAP_TEST_DML_MAX_ZONES.load(Ordering::Acquire), 64);
        for id in samples.keys() {
            assert_eq!(
                map.read_zone(*id, Some(&BTreeSet::new()))
                    .unwrap()
                    .unwrap()
                    .state,
                Some(ZoneMapState::Unknown),
                "whole-input defer policy must survive 64-zone flushing"
            );
        }
        let column = table.columns[1].column_id;
        let selected = permutation.iter().take(32).copied().collect::<Vec<_>>();
        let mut seeded = ZoneMapDeltaBatch {
            storage_id: table.id,
            zone_pages: map.zone_pages,
            schema_fingerprint: map.schema_fingerprint,
            zones: BTreeMap::new(),
        };
        for &id in &selected {
            let mut unknown = ZoneColumnSummary::default();
            unknown.mark_unknown();
            seeded.zones.insert(
                id,
                ZoneSummary {
                    columns: BTreeMap::from([(column, unknown)]),
                    ..ZoneSummary::default()
                },
            );
        }
        map.merge(&seeded).unwrap();
        ids.clear();
        rows.clear();
        for _ in 0..1000 {
            for id in &selected {
                ids.push(samples[id].0);
                rows.push(samples[id].1[..2].to_vec());
            }
        }
        db.log.lock().flush().unwrap();
        let before = db.log.lock().last_lsn();
        ZONE_MAP_TEST_DML_MAX_ZONES.store(0, Ordering::Release);
        db.merge_zone_map_updated_rows(txn.txn_id, table.id, &ids, &rows, Some(&[1]))
            .unwrap();
        db.log.lock().flush().unwrap();
        let records = LogManager::scan(dir.path())
            .unwrap()
            .records
            .iter()
            .filter(|record| record.lsn > before)
            .count();
        assert!(records<=selected.len()*8,"neutral repeated runs should log per distinct zone, got {records} records for {} zones",selected.len());
        assert!(ZONE_MAP_TEST_DML_MAX_ZONES.load(Ordering::Acquire) <= 64);
        db.rollback_txn(txn.txn_id).unwrap();
        let query = |hint| {
            engine
                .execute(&format!(
                    "SELECT /*+ {hint}(zm) */ id,v FROM zm WHERE id IN(1,4000,8000) ORDER BY id"
                ))
                .unwrap()
        };
        let crate::engine::QueryResponse::ResultSet(actual) = query("ZONE_MAP") else {
            panic!("result")
        };
        let crate::engine::QueryResponse::ResultSet(expected) = query("NO_ZONE_MAP") else {
            panic!("result")
        };
        assert_eq!(actual.rows, expected.rows);
        assert_eq!(actual.rows.len(), 3);
    }

    #[test]
    fn zone_map_background_round_continues_past_ready_prefix_with_bounded_batches() {
        let (_dir, engine) = switch_engine();
        // One wide inline row per page reaches the real 64/640/768-zone
        // boundaries without a 100,000-row insertion workload or LOB chains.
        engine
            .execute("CREATE TABLE zm (id INT, v INT, a VARCHAR(3000), b VARCHAR(3000), c VARCHAR(3000)) ZONE_SIZE=64K")
            .unwrap();
        engine
            .execute(
                "INSERT INTO zm SELECT n,n,REPEAT('x',3000),REPEAT('x',3000),REPEAT('x',3000) FROM generate_series(1,3328) AS gs(n)",
            )
            .unwrap();
        let db = engine.db_for_tests();
        let table = db.table("test.zm").unwrap();
        let map = db.zone_map_for(table.id, &table).unwrap();
        {
            let _gate = map.data_gate.write().unwrap();
            map.reset_unknown().unwrap();
            map.trusted_base.store(true, Ordering::Release);
        }
        let total = db
            .heap_page_count_for_storage(table.id)
            .unwrap()
            .div_ceil(u64::from(map.zone_pages));
        assert!(total > 768, "fixture must span more than one round");
        assert_eq!(db.maintain_zone_maps().unwrap(), 1);
        assert_eq!(map.snapshot_for_tests().len(), 64);
        let first_epoch = map.zone_mutation(0).unwrap().unwrap().epoch;
        // Revisit the completed prefix, as after a restart. This first pass
        // makes no repairs; the round must still reach the missing suffix.
        map.maintenance_cursor.store(0, Ordering::Release);
        assert_eq!(db.maintain_zone_maps_round_until(|| false).unwrap(), 9);
        assert_eq!(map.snapshot_for_tests().len(), 640);
        assert_eq!(map.maintenance_cursor.load(Ordering::Acquire), 640);
        assert_eq!(map.zone_mutation(0).unwrap().unwrap().epoch, first_epoch);
        // Cancel between batches, after two more have been published. No
        // third batch may be scheduled and the completed prefix survives.
        assert_eq!(
            db.maintain_zone_maps_round_until(|| {
                map.maintenance_cursor.load(Ordering::Acquire) >= 768
            })
            .unwrap(),
            2
        );
        assert_eq!(map.snapshot_for_tests().len(), 768);
        assert_eq!(map.maintenance_cursor.load(Ordering::Acquire), 768);
        assert!(!map.missing_is_empty.load(Ordering::Acquire));
    }

    #[test]
    fn zone_map_background_resume_is_bounded_and_reuses_completed_ranges() {
        let (_dir, engine) = switch_engine();
        engine
            .execute("CREATE TABLE zm (id INT, v INT, pad VARCHAR(800)) ZONE_SIZE=64K")
            .unwrap();
        engine
            .execute(
                "INSERT INTO zm SELECT n,n,REPEAT('x',700) FROM generate_series(1,12000) AS gs(n)",
            )
            .unwrap();
        let db = engine.db_for_tests();
        // Retained neutral DML provenance can leave a boundary zone hot after
        // reset. Control the quiet period so batch/resume assertions do not
        // depend on ingestion batch boundaries.
        struct QuietReset(u64);
        impl Drop for QuietReset {
            fn drop(&mut self) {
                ZONE_MAP_TEST_MAINTENANCE_QUIET_MS.store(self.0, Ordering::Release);
            }
        }
        let _quiet = QuietReset(ZONE_MAP_TEST_MAINTENANCE_QUIET_MS.swap(0, Ordering::AcqRel));
        let table = db.table("test.zm").unwrap();
        let map = db.zone_map_for(table.id, &table).unwrap();
        {
            let _gate = map.data_gate.write().unwrap();
            map.reset_unknown().unwrap();
            map.trusted_base.store(true, Ordering::Release);
        }
        let total = db
            .heap_page_count_for_storage(table.id)
            .unwrap()
            .div_ceil(u64::from(map.zone_pages));
        assert!(
            total > 64,
            "fixture must require multiple maintenance turns"
        );
        assert_eq!(db.maintain_zone_maps().unwrap(), 1);
        assert_eq!(map.snapshot_for_tests().len(), 64);
        assert_eq!(map.maintenance_coverage_cursor.load(Ordering::Acquire), 64);
        assert!(!map.missing_is_empty.load(Ordering::Acquire));
        let first_epoch = map.zone_mutation(0).unwrap().unwrap().epoch;
        let first_rows = serde_json::to_value(map.read_zone(0, None).unwrap()).unwrap();
        assert_eq!(db.maintain_zone_maps_until(|| true).unwrap(), 0);
        assert_eq!(map.zone_mutation(0).unwrap().unwrap().epoch, first_epoch);
        for _ in 0..total.div_ceil(64) + 2 {
            if map.missing_is_empty.load(Ordering::Acquire) {
                break;
            }
            assert_eq!(db.maintain_zone_maps().unwrap(), 1);
        }
        assert!(map.missing_is_empty.load(Ordering::Acquire));
        assert!(!map.storage_snapshot().pending);
        assert_eq!(map.snapshot_for_tests().len() as u64, total);
        assert_eq!(
            map.zone_mutation(0).unwrap().unwrap().epoch,
            first_epoch,
            "resume must not rebuild the already complete first batch"
        );
        assert_eq!(
            serde_json::to_value(map.read_zone(0, None).unwrap()).unwrap(),
            first_rows
        );
    }

    #[test]
    fn zone_map_background_hot_writer_does_not_block_cold_unknown_ranges() {
        let (_dir, engine) = switch_engine();
        engine
            .execute("CREATE TABLE zm (id INT PRIMARY KEY, v INT, pad VARCHAR(800)) ZONE_SIZE=64K")
            .unwrap();
        engine
            .execute(
                "INSERT INTO zm SELECT n,n,REPEAT('x',700) FROM generate_series(1,10000) AS gs(n)",
            )
            .unwrap();
        let db = engine.db_for_tests();
        let table = db.table("test.zm").unwrap();
        let map = db.zone_map_for(table.id, &table).unwrap();
        {
            let _gate = map.data_gate.write().unwrap();
            map.reset_unknown().unwrap();
            map.trusted_base.store(true, Ordering::Release);
        }
        let total = db
            .heap_page_count_for_storage(table.id)
            .unwrap()
            .div_ceil(u64::from(map.zone_pages));
        assert!(total > 64);
        let (work_tx, work_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            let engine_ref = &engine;
            scope.spawn(move || {
                while work_rx.recv().is_ok() {
                    done_tx
                        .send(engine_ref.execute("UPDATE zm SET v=v+1 WHERE id=1"))
                        .unwrap();
                }
            });
            for _ in 0..total.div_ceil(64) + 2 {
                work_tx.send(()).unwrap();
                done_rx.recv().unwrap().unwrap();
                // The latest write keeps zone 0 inside the ordinary quiet
                // window, but unrelated cold windows must still progress.
                db.maintain_zone_maps().unwrap();
            }
            drop(work_tx);
        });
        let column = table.columns[1].column_id;
        assert_eq!(
            map.read_zone(1, Some(&BTreeSet::from([column])))
                .unwrap()
                .unwrap()
                .columns[&column]
                .state,
            ZoneMapState::Ready
        );
        assert_eq!(
            map.read_zone(total - 1, Some(&BTreeSet::from([column])))
                .unwrap()
                .unwrap()
                .columns[&column]
                .state,
            ZoneMapState::Ready
        );
        let hot = map
            .read_zone(0, Some(&BTreeSet::from([column])))
            .unwrap()
            .unwrap();
        assert!(
            hot.state == Some(ZoneMapState::Unknown)
                || hot.columns[&column].state == ZoneMapState::Unknown
        );
        assert!(
            map.missing_is_empty.load(Ordering::Acquire),
            "metadata coverage can finish while a hot zone remains conservatively Unknown"
        );
        let query = |hint| {
            engine
                .execute(&format!(
                    "SELECT /*+ {hint}(zm) */ id,v FROM zm WHERE id=1 OR v>=9990 ORDER BY id"
                ))
                .unwrap()
        };
        let crate::engine::QueryResponse::ResultSet(actual) = query("ZONE_MAP") else {
            panic!("result")
        };
        let crate::engine::QueryResponse::ResultSet(expected) = query("NO_ZONE_MAP") else {
            panic!("result")
        };
        assert_eq!(actual.rows, expected.rows);
    }

    #[test]
    fn zone_map_background_coverage_does_not_publish_retired_handle() {
        let (_dir, engine) = switch_engine();
        engine.execute("CREATE TABLE zm (v INT)").unwrap();
        engine.execute("INSERT INTO zm VALUES (1)").unwrap();
        engine.execute("ANALYZE TABLE zm").unwrap();
        let db = engine.db_for_tests();
        let table = db.table("test.zm").unwrap();
        let map = db.zone_map_for(table.id, &table).unwrap();
        {
            let _gate = map.data_gate.write().unwrap();
            map.store.set_missing_is_empty(false).unwrap();
            map.missing_is_empty.store(false, Ordering::Release);
        }
        let total = db
            .heap_page_count_for_storage(table.id)
            .unwrap()
            .div_ceil(u64::from(map.zone_pages));
        let barrier = Arc::new(std::sync::Barrier::new(2));
        *map.coverage_publish_barrier.lock().unwrap() = Some(Arc::clone(&barrier));
        std::thread::scope(|scope| {
            let worker =
                scope.spawn(|| db.advance_zone_map_coverage(table.id, &map, total, &|| false));
            barrier.wait();
            let retired = db.forget_zone_map(table.id);
            let lsn = db.log.lock().last_lsn();
            barrier.wait();
            worker.join().unwrap().unwrap();
            retired.unwrap();
            assert_eq!(
                db.log.lock().last_lsn(),
                lsn,
                "a retired map must not publish another root FPI"
            );
        });
        assert!(map.retired.load(Ordering::Acquire));
        assert!(!map.missing_is_empty.load(Ordering::Acquire));
    }

    #[test]
    fn zone_map_background_coverage_rechecks_new_zone_and_reset_epoch() {
        let (_dir, engine) = switch_engine();
        engine
            .execute("CREATE TABLE zm (v INT,pad VARCHAR(800)) ZONE_SIZE=64K")
            .unwrap();
        engine.execute("INSERT INTO zm VALUES (1,'base')").unwrap();
        engine.execute("ANALYZE TABLE zm").unwrap();
        let db = engine.db_for_tests();
        let table = db.table("test.zm").unwrap();
        let map = db.zone_map_for(table.id, &table).unwrap();
        {
            let _gate = map.data_gate.write().unwrap();
            map.store.set_missing_is_empty(false).unwrap();
            map.missing_is_empty.store(false, Ordering::Release);
        }
        let total = db
            .heap_page_count_for_storage(table.id)
            .unwrap()
            .div_ceil(u64::from(map.zone_pages));
        let barrier = Arc::new(std::sync::Barrier::new(2));
        *map.coverage_publish_barrier.lock().unwrap() = Some(Arc::clone(&barrier));
        std::thread::scope(|scope| {
            let worker =
                scope.spawn(|| db.advance_zone_map_coverage(table.id, &map, total, &|| false));
            barrier.wait();
            let inserted=engine.execute("INSERT INTO zm SELECT 10000+n,REPEAT('x',700) FROM generate_series(1,256) AS gs(n)");
            barrier.wait();
            worker.join().unwrap().unwrap();
            inserted.unwrap();
        });
        *map.coverage_publish_barrier.lock().unwrap() = None;
        assert!(
            !map.missing_is_empty.load(Ordering::Acquire),
            "a writer racing the heap watermark must defer Empty-default publication"
        );
        let new_total = db
            .heap_page_count_for_storage(table.id)
            .unwrap()
            .div_ceil(u64::from(map.zone_pages));
        assert!(new_total > total);
        let crate::engine::QueryResponse::ResultSet(result) = engine
            .execute("SELECT /*+ ZONE_MAP(zm) */ COUNT(*) FROM zm WHERE v>10000")
            .unwrap()
        else {
            panic!("result")
        };
        assert_eq!(result.rows[0][0].to_display_string(), "256");
        db.advance_zone_map_coverage(table.id, &map, new_total, &|| false)
            .unwrap();
        assert!(map.missing_is_empty.load(Ordering::Acquire));
        {
            let _gate = map.data_gate.write().unwrap();
            map.reset_unknown().unwrap();
            map.trusted_base.store(true, Ordering::Release);
        }
        db.advance_zone_map_coverage(table.id, &map, new_total, &|| false)
            .unwrap();
        assert_eq!(
            map.maintenance_coverage_cursor.load(Ordering::Acquire),
            0,
            "reset invalidates the old prefix proof"
        );
        assert!(!map.missing_is_empty.load(Ordering::Acquire));
    }

    #[test]
    fn zone_map_paged_repeated_analyze_reuses_file_high_water() {
        let (_dir, engine) = switch_engine();
        engine.execute("CREATE TABLE zm (v INT)").unwrap();
        engine.execute("INSERT INTO zm VALUES (1),(2),(3)").unwrap();
        engine.execute("ANALYZE TABLE zm").unwrap();
        let db = engine.db_for_tests();
        db.checkpoint().unwrap();
        let table = db.table("test.zm").unwrap();
        let path = db.zone_map_file_path(table.id);
        assert!(
            !db.zone_map_for(table.id, &table)
                .unwrap()
                .storage_snapshot()
                .pending,
            "uncontended full ANALYZE must finish Ready"
        );
        let high_water = fs::metadata(&path).unwrap().len();
        for _ in 0..5 {
            engine.execute("ANALYZE TABLE zm").unwrap();
            db.checkpoint().unwrap();
            assert!(
                !db.zone_map_for(table.id, &table)
                    .unwrap()
                    .storage_snapshot()
                    .pending
            );
            assert_eq!(
                fs::metadata(&path).unwrap().len(),
                high_water,
                "same-size rebuild must not append another complete tree"
            );
        }
    }

    #[test]
    fn zone_map_paged_repair_epoch_never_regresses_after_reopen() {
        let (dir, engine) = switch_engine();
        engine.execute("CREATE TABLE zm (v INT)").unwrap();
        engine.execute("INSERT INTO zm VALUES (1)").unwrap();
        engine.execute("ANALYZE TABLE zm").unwrap();
        {
            let db = engine.db_for_tests();
            let table = db.table("test.zm").unwrap();
            let map = db.zone_map_for(table.id, &table).unwrap();
            let _gate = map.data_gate.write().unwrap();
            map.set_zone_mutation(
                0,
                ZoneMutation {
                    epoch: 1_000_000,
                    last_ms: 0,
                },
            )
            .unwrap();
        }
        engine.db_for_tests().checkpoint().unwrap();
        drop(engine);
        let db = Database::open(dir.path()).unwrap();
        let table = db.table("test.zm").unwrap();
        assert_eq!(
            db.rebuild_zone_map_for_storage_zones(
                &table,
                table.id,
                &BTreeSet::from([0]),
                &|| false,
                None
            )
            .unwrap(),
            Some(1)
        );
        let map = db.zone_map_for(table.id, &table).unwrap();
        assert!(map.zone_mutation(0).unwrap().unwrap().epoch > 1_000_000);
    }

    #[test]
    fn zone_map_role_switch_waits_for_admitted_rebuild_or_reader() {
        let (_dir, engine) = switch_engine();
        let db = engine.db_for_tests();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            let operation = db.zone_map_operation().unwrap();
            scope.spawn(|| {
                started_tx.send(()).unwrap();
                db.set_physical_replica_mode(true).unwrap();
                done_tx.send(()).unwrap();
            });
            started_rx.recv().unwrap();
            assert!(done_rx.recv_timeout(Duration::from_millis(30)).is_err());
            drop(operation);
            done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        });
        assert!(db.physical_replica_mode());
        db.set_physical_replica_mode(false).unwrap();
    }

    #[test]
    fn zone_map_physical_replica_does_not_build_or_modify_summary_pages() {
        let (_dir, engine) = switch_engine();
        engine.execute("CREATE TABLE zm (v INT)").unwrap();
        engine.execute("INSERT INTO zm VALUES (1)").unwrap();
        engine.execute("ANALYZE TABLE zm").unwrap();
        let db = engine.db_for_tests();
        db.checkpoint().unwrap();
        let table = db.table("test.zm").unwrap();
        let path = db.zone_map_file_path(table.id);
        let before = fs::read(&path).unwrap();
        let lsn = db.log.lock().last_lsn();
        db.set_physical_replica_mode(true).unwrap();
        assert_eq!(db.maintain_zone_maps().unwrap(), 0);
        assert!(db.rebuild_zone_map_for_table(&table).is_err());
        assert_eq!(db.log.lock().last_lsn(), lsn);
        assert_eq!(fs::read(&path).unwrap(), before);
        db.set_physical_replica_mode(false).unwrap();
    }

    #[test]
    fn zone_map_disabled_reuses_durable_revocation_until_reenabled() {
        let (_dir, engine) = switch_engine();
        engine.execute("CREATE TABLE zm (v INT)").unwrap();
        engine.execute("INSERT INTO zm VALUES (1)").unwrap();
        engine.execute("ANALYZE TABLE zm").unwrap();
        let db = engine.db_for_tests();
        let table = db.table("test.zm").unwrap();
        db.set_zone_map_enabled(false).unwrap();
        db.queue_zone_map_invalidation(0, table.id, false).unwrap();
        let revoked_lsn = db.log.lock().last_lsn();
        for _ in 0..16 {
            db.queue_zone_map_invalidation(0, table.id, false).unwrap();
        }
        assert_eq!(db.log.lock().last_lsn(), revoked_lsn);
        db.set_zone_map_enabled(true).unwrap();
        engine.execute("ANALYZE TABLE zm").unwrap();
        let rebuilt_lsn = db.log.lock().last_lsn();
        db.set_zone_map_enabled(false).unwrap();
        db.queue_zone_map_invalidation(0, table.id, false).unwrap();
        assert!(db.log.lock().last_lsn() > rebuilt_lsn);
    }

    #[test]
    fn zone_map_disabled_revocation_retries_failed_load_and_concurrent_writers() {
        let (_dir, engine) = switch_engine();
        engine.execute("CREATE TABLE zm (v INT)").unwrap();
        engine.execute("INSERT INTO zm VALUES (1)").unwrap();
        engine.execute("ANALYZE TABLE zm").unwrap();
        let db = engine.db_for_tests();
        let table = db.table("test.zm").unwrap();
        db.checkpoint().unwrap();
        db.forget_zone_map(table.id).unwrap();
        db.set_zone_map_enabled(false).unwrap();
        {
            let _failure = ZoneMapFailpointGuard::install(7);
            assert!(db.queue_zone_map_invalidation(0, table.id, false).is_err());
        }
        let before = db.log.lock().last_lsn();
        let barrier = std::sync::Barrier::new(8);
        let storage_id = table.id;
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    db.queue_zone_map_invalidation(0, storage_id, false)
                        .unwrap();
                });
            }
        });
        assert_eq!(db.log.lock().last_lsn(), before + 1);
        db.set_zone_map_enabled(true).unwrap();
        assert!(!db.zone_map_for(table.id, &table).unwrap().store.trusted());
    }

    #[test]
    fn zone_map_disabled_revocation_forgets_ddl_and_replayed_roots() {
        let (_dir, engine) = switch_engine();
        engine.execute("CREATE TABLE zm (v INT)").unwrap();
        engine.execute("INSERT INTO zm VALUES (1)").unwrap();
        engine.execute("ANALYZE TABLE zm").unwrap();
        let db = engine.db_for_tests();
        let table = db.table("test.zm").unwrap();
        db.checkpoint().unwrap();
        db.set_zone_map_enabled(false).unwrap();
        db.queue_zone_map_invalidation(0, table.id, false).unwrap();
        let first = db.log.lock().last_lsn();
        db.forget_zone_map(table.id).unwrap();
        db.queue_zone_map_invalidation(0, table.id, false).unwrap();
        assert!(db.log.lock().last_lsn() > first);
        let second = db.log.lock().last_lsn();
        let relative = db.relative_path(&db.zone_map_file_path(table.id));
        drop(db.retire_replayed_zone_map(&relative).unwrap());
        db.queue_zone_map_invalidation(0, table.id, false).unwrap();
        assert!(db.log.lock().last_lsn() > second);
    }

    #[test]
    fn zone_map_disabled_missing_summary_survives_reenable_and_restart() {
        use crate::engine::{QueryResponse, StorageQueryEngine};
        let (dir, engine) = switch_engine();
        let db = engine.db_for_tests();
        db.set_zone_map_enabled(false).unwrap();
        engine
            .execute("CREATE TABLE zm (id INT PRIMARY KEY, v INT)")
            .unwrap();
        engine.execute("INSERT INTO zm VALUES (1,1)").unwrap();
        engine.execute("UPDATE zm SET v=1000 WHERE id=1").unwrap();
        let table = db.table("test.zm").unwrap();
        assert!(!db.zone_map_file_path(table.id).exists());
        db.set_zone_map_enabled(true).unwrap();
        engine.execute("ANALYZE TABLE zm").unwrap();
        db.set_zone_map_enabled(false).unwrap();
        engine.execute("UPDATE zm SET v=2000 WHERE id=1").unwrap();
        engine.execute("INSERT INTO zm VALUES (2,2000)").unwrap();
        db.checkpoint().unwrap();
        drop(engine);
        let reopened = StorageQueryEngine::open_with_storage_config_without_background_workers(
            dir.path(),
            StorageConfig::from_env(),
        )
        .unwrap();
        let response = reopened
            .execute("SELECT /*+ ZONE_MAP(zm) */ id FROM zm WHERE v=2000 ORDER BY id")
            .unwrap();
        let rows = match response {
            QueryResponse::ResultSet(result) => result.rows,
            QueryResponse::SharedResultSet(result) => result.rows,
            other => panic!("unexpected response: {other:?}"),
        };
        assert_eq!(rows, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
    }

    #[test]
    fn zone_map_global_switch_waits_for_admitted_work() {
        let (_dir, engine) = switch_engine();
        let db = engine.db_for_tests();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            let operation = db.zone_map_operation().unwrap();
            scope.spawn(|| {
                started_tx.send(()).unwrap();
                db.set_zone_map_enabled(false).unwrap();
                done_tx.send(()).unwrap();
            });
            started_rx.recv().unwrap();
            assert!(done_rx.recv_timeout(Duration::from_millis(30)).is_err());
            drop(operation);
            done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        });
        assert!(!db.zone_map_enabled());
    }

    #[test]
    fn zone_map_audit_unavailable_map_revokes_and_logs_all_write_paths() {
        for operation in 0..4 {
            let (_dir, engine) = switch_engine();
            engine.execute("CREATE TABLE zm (v INT)").unwrap();
            engine.execute("INSERT INTO zm VALUES (1)").unwrap();
            engine.execute("ANALYZE TABLE zm").unwrap();
            let db = engine.db_for_tests();
            db.checkpoint().unwrap();
            let table = db.table("test.zm").unwrap();
            db.zone_maps.write().unwrap().remove(&table.id);
            let latch = &db.zone_map_load_locks[table.id as usize % db.zone_map_load_locks.len()];
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _lock = latch.lock().unwrap();
                panic!("injected transient zone-map load failure");
            }));
            assert!(db.zone_map_for(table.id, &table).is_none());
            let row_ids = [RowId {
                page_id: 0,
                slot_id: 0,
            }];
            let rows = [vec![Value::Int(1000)]];
            let outcome = match operation {
                0 => db.merge_zone_map_rows(4242, table.id, &row_ids, &rows),
                1 => db.merge_zone_map_updated_rows(4242, table.id, &row_ids, &rows, None),
                2 => db.note_zone_map_deletes(4242, table.id, &row_ids),
                _ => {
                    let mut batch = ZoneMapWorkerBatch::for_table(&table);
                    batch.observe_row(&table, &rows[0]);
                    db.apply_worker_batch(4242, table.id, &row_ids, &batch)
                }
            };
            latch.clear_poison();
            if outcome.is_err() {
                // A rejected write is the required result when the poisoned
                // loader prevents a durable invalidation; it must not claim
                // success with an unrecorded summary gap.
                continue;
            }
            assert!(
                db.zone_map_for(table.id, &table)
                    .is_none_or(|map| !map.trusted_base.load(Ordering::Acquire)),
                "successful write path {operation} lost invalidation"
            );
            db.checkpoint().unwrap();
            db.zone_maps.write().unwrap().remove(&table.id);
            assert!(!db
                .zone_map_for(table.id, &table)
                .unwrap()
                .trusted_base
                .load(Ordering::Acquire));
        }
    }

    #[test]
    fn zone_map_audit_catalog_option_case() {
        let (_dir, engine) = switch_engine();
        engine.execute("CREATE TABLE zm (v INT)").unwrap();
        let mut table = (*engine.db_for_tests().table("test.zm").unwrap()).clone();
        for value in ["off", "Off", "OFF"] {
            table.mysql_table_options = vec![("zone_map".into(), value.into())];
            assert!(!table.zone_map_enabled(), "{value}");
        }
    }

    #[test]
    fn zone_map_audit_failed_load_dml_remains_visible_after_reopen() {
        use crate::engine::{QueryResponse, StorageQueryEngine};
        let check = |engine: &StorageQueryEngine| {
            let response = engine
                .execute("SELECT /*+ ZONE_MAP(zm) */ v FROM zm WHERE v=1000")
                .unwrap();
            let rows = match response {
                QueryResponse::ResultSet(result) => result.rows,
                QueryResponse::SharedResultSet(result) => result.rows,
                other => panic!("unexpected response: {other:?}"),
            };
            assert_eq!(rows, vec![vec![Value::Int(1000)]]);
        };
        for sql in ["INSERT INTO zm VALUES (1000)", "UPDATE zm SET v=1000"] {
            let (dir, engine) = switch_engine();
            engine
                .execute("CREATE TABLE zm (v INT) ZONE_SIZE=64K")
                .unwrap();
            engine.execute("INSERT INTO zm VALUES (1)").unwrap();
            engine.execute("ANALYZE TABLE zm").unwrap();
            let db = engine.db_for_tests();
            db.checkpoint().unwrap();
            let table = db.table("test.zm").unwrap();
            db.zone_maps.write().unwrap().remove(&table.id);
            let latch = &db.zone_map_load_locks[table.id as usize % db.zone_map_load_locks.len()];
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _lock = latch.lock().unwrap();
                panic!("injected zone-map load failure during DML");
            }));
            engine.execute(sql).unwrap();
            latch.clear_poison();
            check(&engine);
            db.checkpoint().unwrap();
            drop(engine);
            let reopened = StorageQueryEngine::open_with_storage_config_without_background_workers(
                dir.path(),
                StorageConfig::from_env(),
            )
            .unwrap();
            check(&reopened);
        }
    }
}
