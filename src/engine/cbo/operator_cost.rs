//! Per-operator cost formulas (Phase 3).
//!
//! Each function in this module takes a [`CboContext`], the relevant
//! row counts / widths / index metadata, and returns a fully-populated
//! [`Cost`]. The diagnostic fields (io_seq_pages, io_rnd_pages,
//! cpu_units, mem_bytes) are filled in so EXPLAIN FORMAT=JSON can
//! surface a meaningful breakdown.
//!
//! Phase 3.1 ships the leaf-scan family: `seq_scan`, `index_lookup`,
//! `index_range_scan`. Phase 3.2/3.3 follow with joins, aggregates,
//! sorts, and pipeline-aware compositions.
//!
//! ## Conventions
//!
//! - All inputs are non-negative `f64` (callers should clamp upstream).
//! - `total` and `startup` are returned in cost-units relative to
//!   `cbo_seq_page_cost = 1.0`.
//! - The `width` field is *output* tuple width in bytes; callers use
//!   it to size downstream operator memory.
//! - Empty inputs (rows == 0) still incur a small minimum cost so the
//!   optimizer doesn't pick a "free" plan over a trivially better one.

use super::cache_model::{hit_rate, page_io_cost, AccessKind};
use super::consts::consts as cbo_consts;
use super::cost::Cost;
use super::CboContext;
use super::PAGE_SIZE_BYTES;

/// Output-tuple width (bytes) at or below which a scan pays only the flat
/// `cpu_tuple_cost` to decode each row. The flat constant already covers
/// tuple iteration plus decoding a reference-width tuple; only the bytes
/// *beyond* this reference incur the per-byte `cpu_byte_decode_cost`
/// surcharge. Chosen as a small handful of narrow columns so that point /
/// covering-key tuples are unaffected and only genuinely wide rows are
/// surcharged.
pub const DECODE_REF_WIDTH: f64 = 32.0;

/// CPU work of one heap-page probe relative to iterating one already-local
/// tuple.  A non-covering index range must hash the page id, find and pin the
/// buffer frame, locate the slot, inspect the MVCC header and only then decode
/// the projected columns.  Treating a resident probe as only `mem_page_cost`
/// made broad uncorrelated ranges look almost free once the table fit in the
/// buffer pool, even though none of that CPU/pointer-chasing work disappears.
///
/// Correlated ranges are charged once per visited heap page and scattered
/// ranges once per chunk-local distinct page through `logical_heap_probes`, so
/// this does not turn a clustered primary-key walk into one probe per row.
const HEAP_PROBE_CPU_TUPLES: f64 = 40.0;

/// Per-row CPU to iterate and deserialize one output tuple `output_width`
/// bytes wide. Shared by [`seq_scan`] and [`index_fast_scan`] so a heap
/// scan and a covering-index scan that emit the *same* projected columns
/// pay the *same* per-row CPU — leaving page count as the tie-breaker,
/// which is the whole point of an index-only scan on a wide table with a
/// narrow covering key. Wider tuples (more / bigger projected columns)
/// cost proportionally more to decode; narrow tuples stay at the flat
/// `cpu_tuple_cost`. Set `cbo_cpu_byte_decode_cost = 0` to recover a flat
/// (width-blind) per-row decode cost.
pub(crate) fn per_row_decode_cost(output_width: f64) -> f64 {
    let consts = cbo_consts();
    let extra = (output_width - DECODE_REF_WIDTH).max(0.0) * consts.cpu_byte_decode_cost();
    consts.cpu_tuple_cost() + extra
}

/// Cost of a sequential heap scan that reads `total_pages` pages,
/// produces `output_rows` after applying a filter, and emits tuples
/// `output_width` bytes wide.
///
/// `qual_cost_per_row` covers the per-row cost of evaluating the
/// WHERE predicate, in cost-units (typically `n_operators *
/// cbo_cpu_operator_cost`). Pass 0.0 if there is no filter.
pub fn seq_scan(
    ctx: &CboContext,
    total_pages: f64,
    rows_scanned: f64,
    _output_rows: f64,
    output_width: f64,
    qual_cost_per_row: f64,
) -> Cost {
    let io = page_io_cost(
        AccessKind::Sequential,
        total_pages,
        ctx.effective_cache_pages as f64,
        total_pages,
    );
    let cpu = rows_scanned * (per_row_decode_cost(output_width) + qual_cost_per_row);
    Cost {
        startup: 0.0,
        total: io + cpu,
        io_seq_pages: total_pages,
        io_rnd_pages: 0.0,
        cpu_units: cpu,
        mem_bytes: 0.0,
        spill_bytes: 0.0,
    }
}

#[cfg(test)]
/// Cost of a parallel sequential scan with `degree` workers. Models:
///   - per-worker IO/CPU = serial / degree (workers cover disjoint
///     page ranges, so sequential prefetch and CPU split cleanly)
///   - thread-spawn / merge overhead = degree * `cbo_parallel_setup_cost`
///     applied to startup *and* total
///
/// `degree >= 1`. When `degree == 1` this collapses to plain
/// `seq_scan` plus a one-worker setup tax. CBO compares this against
/// the serial cost and only picks parallel when the savings beat the
/// overhead by a configurable hysteresis margin (see Phase 2 caller).
pub fn parallel_seq_scan(
    ctx: &CboContext,
    total_pages: f64,
    rows_scanned: f64,
    output_rows: f64,
    output_width: f64,
    qual_cost_per_row: f64,
    degree: u32,
) -> Cost {
    let serial = seq_scan(
        ctx,
        total_pages,
        rows_scanned,
        output_rows,
        output_width,
        qual_cost_per_row,
    );
    let p = degree.max(1) as f64;
    let setup = p * ctx.model.parallel_setup_cost;
    Cost {
        startup: setup,
        total: serial.total / p + setup,
        // Diagnostics are total work/resource counters, not critical-path
        // latency. Workers still read and decode the same population.
        io_seq_pages: serial.io_seq_pages,
        io_rnd_pages: serial.io_rnd_pages,
        cpu_units: serial.cpu_units + setup,
        mem_bytes: serial.mem_bytes,
        spill_bytes: serial.spill_bytes,
    }
}

#[cfg(test)]
/// Cost of a `Exchange` boundary on top of a parallel input. Adds a
/// merge term that's either O(rows) for `Unordered` (concat) or
/// O(rows * log2(degree)) for `Sorted` (k-way merge).
pub fn exchange(input: &Cost, degree: u32, rows: f64, sorted: bool) -> Cost {
    let consts = cbo_consts();
    let p = degree.max(1) as f64;
    let merge = if sorted {
        rows * p.log2().max(0.0) * consts.cpu_tuple_cost()
    } else {
        rows * consts.cpu_tuple_cost() * 0.1
    };
    Cost {
        startup: input.startup,
        total: input.total + merge,
        io_seq_pages: input.io_seq_pages,
        io_rnd_pages: input.io_rnd_pages,
        cpu_units: input.cpu_units + merge,
        mem_bytes: input.mem_bytes,
        spill_bytes: input.spill_bytes,
    }
}

/// Cost of a B-tree point lookup followed by `matched_rows` heap
/// fetches. `index_height` is the number of internal pages traversed
/// (typically 2-4 for indices up to a few hundred million rows). The
/// table's total page count `table_pages` feeds the cache hit rate
/// model — random-access pages compete for the buffer pool with the
/// rest of the table.
pub fn index_lookup(
    ctx: &CboContext,
    index_height: f64,
    matched_rows: f64,
    table_pages: f64,
    output_width: f64,
) -> Cost {
    let consts = cbo_consts();
    let cache = ctx.effective_cache_pages as f64;
    // The descent reads `height` index pages randomly; for typical
    // indices these are hot in cache.
    let index_working_set = index_height.max(1.0);
    let descent_io = page_io_cost(AccessKind::Random, index_height, cache, index_working_set);
    // `table_pages == 0` is the explicit covering-index signal used by the
    // planner: the index supplies every requested column, so there is no heap
    // fetch. Otherwise estimate distinct heap pages rather than one I/O per
    // match, capped by the heap population.
    let heap_pages = if table_pages <= 0.0 || matched_rows <= 0.0 {
        0.0
    } else {
        let pages = table_pages.max(1.0);
        pages * (1.0 - (1.0 - 1.0 / pages).powf(matched_rows.max(0.0)))
    };
    let heap_io = page_io_cost(AccessKind::Random, heap_pages, cache, table_pages);
    let cpu_index = index_height * consts.cpu_index_tuple_cost();
    let cpu_tuples =
        matched_rows * (consts.cpu_index_tuple_cost() + per_row_decode_cost(output_width));
    let startup = descent_io + cpu_index;
    let total = startup + heap_io + cpu_tuples;
    Cost {
        startup,
        total,
        io_seq_pages: 0.0,
        io_rnd_pages: index_height + heap_pages,
        cpu_units: cpu_index + cpu_tuples,
        mem_bytes: 0.0,
        spill_bytes: 0.0,
    }
}

/// Cost of a B-tree range scan returning `matched_rows`. Index leaf
/// pages are walked sequentially via the next-leaf pointer; heap
/// fetches are random unless the index is *correlated* with physical
/// row order (clustering factor ≈ 1.0).
///
/// `index_height` is the descent depth, `leaf_pages` is the number of
/// leaf pages spanning the range, and `correlation` ∈ [0, 1] is the
/// estimated index-vs-heap correlation. A primary-key range has
/// correlation ≈ 1; a random secondary index near 0.
#[derive(Clone, Copy, Debug)]
pub struct IndexRangeScanInput {
    pub index_height: f64,
    pub leaf_pages: f64,
    pub matched_rows: f64,
    /// ANALYZE row count for the heap. This, together with `table_pages`,
    /// defines physical row density; projected tuple width must never be
    /// used as a proxy for heap occupancy.
    pub table_rows: f64,
    pub table_pages: f64,
    pub output_width: f64,
    pub correlation: f64,
    /// Executor-side chunk-local RID ordering. `None` is the normal key-order
    /// path. Supplying a chunk size may only remove duplicate page probes
    /// within each chunk; it is deliberately not modeled as a global MRR.
    pub mrr_chunk_rows: Option<f64>,
}

#[derive(Clone, Copy, Debug)]
pub struct IndexRangeCostEstimate {
    pub cost: Cost,
    pub logical_heap_probes: f64,
    pub distinct_heap_pages: f64,
    pub miss_heap_pages: f64,
    pub hit_heap_probes: f64,
    pub rows_per_heap_page: f64,
}

fn distinct_pages(population_pages: f64, probes: f64) -> f64 {
    if population_pages <= 0.0 || probes <= 0.0 {
        return 0.0;
    }
    let pages = population_pages.max(1.0);
    pages * (1.0 - (1.0 - 1.0 / pages).powf(probes.max(0.0)))
}

fn chunk_local_page_probes(table_pages: f64, matched_rows: f64, chunk_rows: f64) -> f64 {
    if matched_rows <= 0.0 || table_pages <= 0.0 {
        return 0.0;
    }
    let chunk = chunk_rows.max(1.0);
    let full_chunks = (matched_rows / chunk).floor();
    let tail = matched_rows - full_chunks * chunk;
    full_chunks * distinct_pages(table_pages, chunk) + distinct_pages(table_pages, tail)
}

pub fn estimate_index_range_scan(
    ctx: &CboContext,
    input: IndexRangeScanInput,
) -> IndexRangeCostEstimate {
    let IndexRangeScanInput {
        index_height,
        leaf_pages,
        matched_rows,
        table_rows,
        table_pages,
        output_width,
        correlation,
        mrr_chunk_rows,
    } = input;
    let model = ctx.model;
    let cache = ctx.effective_cache_pages as f64;
    let index_working_set = (index_height + leaf_pages).max(1.0);
    let descent_io = page_io_cost(AccessKind::Random, index_height, cache, index_working_set);
    let leaf_io = page_io_cost(AccessKind::Sequential, leaf_pages, cache, index_working_set);
    // Keep logical probes separate from physical page population. The
    // executor normally fetches one RID at a time in index-key order, so an
    // uncorrelated range performs one logical heap probe per candidate even
    // when many candidates eventually map to the same physical page. The
    // buffer pool may turn revisits into cheap hits, but it does not erase
    // latching, lookup and decode work.
    let corr = correlation.clamp(0.0, 1.0);
    let rows_per_page = if table_pages > 0.0 && table_rows > 0.0 {
        (table_rows / table_pages).max(1.0)
    } else {
        1.0
    };
    let correlated_pages = (matched_rows / rows_per_page)
        .ceil()
        .min(table_pages.max(0.0));
    let scattered_distinct = distinct_pages(table_pages, matched_rows);
    let scattered_logical = mrr_chunk_rows
        .map(|chunk| chunk_local_page_probes(table_pages, matched_rows, chunk))
        .unwrap_or(matched_rows.max(0.0));

    let correlated_logical = correlated_pages * corr;
    let random_logical = scattered_logical * (1.0 - corr);
    let correlated_distinct = correlated_pages * corr;
    let random_distinct = scattered_distinct * (1.0 - corr);
    let residency = hit_rate(AccessKind::Random, cache, table_pages);
    let miss_fraction = 1.0 - residency;
    // MRR visits heap pages in page order inside every canonical window. A
    // page evicted between windows may be read again, so physical misses are
    // bounded by the sum of window-local distinct pages (`*_logical`), not by
    // the global distinct-page population. This keeps the cost faithful while
    // remaining independent of the executor's live work-area grant.
    let correlated_misses = correlated_logical * miss_fraction;
    let random_misses = random_logical * miss_fraction;
    let logical_heap_probes = correlated_logical + random_logical;
    let miss_heap_pages = correlated_misses + random_misses;
    let hit_heap_probes = (logical_heap_probes - miss_heap_pages).max(0.0);
    let heap_io = correlated_misses * model.seq_page_cost
        + random_misses * model.seq_page_cost
        + hit_heap_probes * model.mem_page_cost;
    let heap_probe_cpu = logical_heap_probes * model.cpu_tuple_cost * HEAP_PROBE_CPU_TUPLES;
    let cpu_index = (index_height + leaf_pages) * model.cpu_index_tuple_cost;
    let cpu_tuples =
        matched_rows * (model.cpu_index_tuple_cost + per_row_decode_cost(output_width));
    let startup = descent_io + index_height * model.cpu_index_tuple_cost;
    let total = startup
        + leaf_io
        + heap_io
        + heap_probe_cpu
        + cpu_tuples
        + (cpu_index - index_height * model.cpu_index_tuple_cost);
    let cost = Cost {
        startup,
        total: total.max(startup),
        // Diagnostic page counters represent distinct physical pages. The
        // logical probe count is exposed separately in optimizer trace.
        io_seq_pages: leaf_pages + correlated_distinct,
        io_rnd_pages: index_height + random_distinct,
        cpu_units: cpu_index + heap_probe_cpu + cpu_tuples,
        mem_bytes: 0.0,
        spill_bytes: 0.0,
    };
    IndexRangeCostEstimate {
        cost,
        logical_heap_probes,
        distinct_heap_pages: correlated_distinct + random_distinct,
        miss_heap_pages,
        hit_heap_probes,
        rows_per_heap_page: rows_per_page,
    }
}

pub fn index_range_scan(
    ctx: &CboContext,
    index_height: f64,
    leaf_pages: f64,
    matched_rows: f64,
    table_rows: f64,
    table_pages: f64,
    output_width: f64,
    correlation: f64,
) -> Cost {
    estimate_index_range_scan(
        ctx,
        IndexRangeScanInput {
            index_height,
            leaf_pages,
            matched_rows,
            table_rows,
            table_pages,
            output_width,
            correlation,
            mrr_chunk_rows: None,
        },
    )
    .cost
}

/// Historical per-probe approximation used only by the small-batch INLJ
/// estimator. An INLJ probe is not executed by `IndexRangeScan`; keeping its
/// separately calibrated distinct-page formula here prevents changes to the
/// real range-scan model from silently rewriting join enumeration.
pub(crate) fn calibrated_inlj_range_probe(
    ctx: &CboContext,
    index_height: f64,
    leaf_pages: f64,
    matched_rows: f64,
    table_pages: f64,
    output_width: f64,
    correlation: f64,
) -> Cost {
    let consts = cbo_consts();
    let cache = ctx.effective_cache_pages as f64;
    let index_working_set = (index_height + leaf_pages).max(1.0);
    let descent_io = page_io_cost(AccessKind::Random, index_height, cache, index_working_set);
    let leaf_io = page_io_cost(AccessKind::Sequential, leaf_pages, cache, index_working_set);
    let corr = correlation.clamp(0.0, 1.0);
    let rows_per_page = ((PAGE_SIZE_BYTES as f64) / output_width.max(8.0)).max(1.0);
    let correlated_pages = (matched_rows / rows_per_page)
        .ceil()
        .min(table_pages.max(0.0));
    let scattered_pages = distinct_pages(table_pages, matched_rows);
    let heap_seq = correlated_pages * corr;
    let heap_rnd = scattered_pages * (1.0 - corr);
    let heap_io = page_io_cost(AccessKind::Sequential, heap_seq, cache, table_pages)
        + page_io_cost(AccessKind::Random, heap_rnd, cache, table_pages);
    let cpu_index = (index_height + leaf_pages) * consts.cpu_index_tuple_cost();
    let cpu_tuples = matched_rows * (consts.cpu_index_tuple_cost() + consts.cpu_tuple_cost());
    let startup = descent_io + index_height * consts.cpu_index_tuple_cost();
    let total = startup
        + leaf_io
        + heap_io
        + cpu_tuples
        + (cpu_index - index_height * consts.cpu_index_tuple_cost());
    Cost {
        startup,
        total: total.max(startup),
        io_seq_pages: leaf_pages + heap_seq,
        io_rnd_pages: index_height + heap_rnd,
        cpu_units: cpu_index + cpu_tuples,
        mem_bytes: 0.0,
        spill_bytes: 0.0,
    }
}

/// Cost of the inner leg of a *batched* index nested-loop join over
/// `matched_rows` candidate entries.
///
/// The executor (`try_execute_batched_index_nested_loop_join`) does
/// NOT perform per-outer-row B-tree descents with immediate heap
/// fetches. It walks the index once for the de-duplicated key batch,
/// accumulates all candidate row_ids, runs `sort_unstable() + dedup()`
/// (CPU charged separately by the planner), and then fetches heap rows
/// in ascending row_id order — each distinct heap page is touched once,
/// in the same order a sequential scan would visit it.
///
/// Heap I/O is therefore bounded by the number of *distinct pages*
/// hit, not by `matched_rows`:
///  - correlated rows (PK-shaped index, heap order ≈ key order)
///    cluster: ≈ `matched_rows / rows_per_page` contiguous pages,
///    priced sequentially;
///  - uncorrelated rows spread uniformly: Yao's approximation
///    `pages × (1 − (1 − 1/pages)^m)` distinct pages, priced randomly
///    but capped at one full sequential sweep of the heap — sorted
///    ascending page visits can never cost more than scanning every
///    heap page once.
///
/// Earlier revisions modeled this leg as `index_range_scan` with
/// correlation 0.5, charging `matched_rows × (1 − corr)` *independent
/// random page reads* at near-miss hit rates. On a large FK-shaped
/// probe that term alone was ~10× the executor's real work (TPC-H
/// sf=1 part⋈lineitem: INLJ costed 1.05M vs hash join 231k while
/// running 3× faster), inverting INLJ/HJ plan choice.
#[derive(Clone, Copy, Debug)]
pub struct BatchedIndexLookupInput {
    pub index_height: f64,
    pub leaf_pages: f64,
    pub matched_rows: f64,
    pub probe_keys: f64,
    pub table_pages: f64,
    pub output_width: f64,
    pub correlation: f64,
    /// Stable pages already owned by the outer subtree. This is derived from
    /// plan costs/statistics, never from live buffer-pool residency.
    pub competing_pages: f64,
}

#[derive(Clone, Copy, Debug)]
pub struct BatchedIndexLookupEstimate {
    pub cost: Cost,
    pub distinct_heap_pages: f64,
    /// Sum of distinct heap pages visited by every fixed-size probe chunk.
    /// Unlike `distinct_heap_pages`, the same physical page is counted again
    /// when a later chunk revisits it.
    pub logical_heap_page_visits: f64,
    pub working_set_pages: f64,
    pub probe_batches: f64,
}

pub fn estimate_batched_index_lookup(
    ctx: &CboContext,
    input: BatchedIndexLookupInput,
) -> BatchedIndexLookupEstimate {
    let BatchedIndexLookupInput {
        index_height,
        leaf_pages,
        matched_rows,
        probe_keys,
        table_pages,
        output_width,
        correlation,
        competing_pages,
    } = input;
    let consts = cbo_consts();
    let cache = ctx.effective_cache_pages as f64;
    let pages = table_pages.max(1.0);
    let working_set_pages =
        (pages + index_height.max(0.0) + leaf_pages.max(0.0) + competing_pages.max(0.0)).max(1.0);
    let index_working_set = (index_height + leaf_pages).max(1.0);
    let descent_io = page_io_cost(AccessKind::Random, index_height, cache, index_working_set);
    let leaf_io = page_io_cost(AccessKind::Sequential, leaf_pages, cache, working_set_pages);
    let m = matched_rows.max(0.0);
    let corr = correlation.clamp(0.0, 1.0);
    let rows_per_page = ((PAGE_SIZE_BYTES as f64) / output_width.max(8.0)).max(1.0);
    // Correlated (PK-shaped) probes cluster each key's matches on one
    // page, but the *keys themselves* are spread over the key space —
    // `m / rows_per_page` only holds when the probe keys are dense
    // (range-shaped). A selective-but-spread FK probe (TPC-H Q3:
    // ~740k of 7.5M orderkeys) touches ≈ one page per key cluster;
    // Yao over `probe_keys` is the floor (sf=5: ~250k pages vs the
    // packed formula's ~4k — a 60× geometry underestimate that made
    // a pool-thrashing INLJ look 3× cheaper than the hash join).
    let keys = probe_keys.max(0.0).min(m.max(1.0));
    let packed_pages = (m / rows_per_page).ceil();
    let key_cluster_pages = pages * (1.0 - (1.0 - 1.0 / pages).powf(keys));
    // Physical page coverage is a property of rows/keys/statistics, not
    // cache residency. Keep it separate from the miss-weighted page work
    // below so outer working-set pressure changes cost but not geometry.
    let physical_clustered_pages = packed_pages.max(key_cluster_pages).min(pages);
    let yao_pages = pages * (1.0 - (1.0 - 1.0 / pages).powf(m));
    let scattered_pages = yao_pages.min(pages);
    // The stream executor sorts/deduplicates RIDs independently for every
    // outer-key chunk. Estimate page visits per chunk rather than globally:
    // a page evicted (or merely revisited) between chunks pays the lookup
    // and buffer work again. The global page population below remains useful
    // for diagnostics, but cannot cap repeated chunk-local sweeps.
    let chunk_keys = crate::engine::cbo::PLANNING_INLJ_CHUNK_ROWS as f64;
    let probe_batches = (keys / chunk_keys).ceil().max(1.0);
    let matches_per_key = if keys > 0.0 { m / keys } else { 0.0 };
    let full_batches = (keys / chunk_keys).floor();
    let tail_keys = keys - full_batches * chunk_keys;
    let chunk_clustered_pages = |batch_keys: f64| {
        if batch_keys <= 0.0 {
            return 0.0;
        }
        let batch_matches = (batch_keys * matches_per_key).min(m);
        let packed = (batch_matches / rows_per_page).ceil();
        packed.max(distinct_pages(pages, batch_keys)).min(pages)
    };
    let chunk_scattered_pages = |batch_keys: f64| {
        if batch_keys <= 0.0 {
            return 0.0;
        }
        distinct_pages(pages, (batch_keys * matches_per_key).min(m))
    };
    let clustered_logical =
        full_batches * chunk_clustered_pages(chunk_keys) + chunk_clustered_pages(tail_keys);
    let scattered_logical =
        full_batches * chunk_scattered_pages(chunk_keys) + chunk_scattered_pages(tail_keys);
    // Both legs visit page-sorted RIDs within a chunk. Correlation controls
    // how much of that work is densely clustered versus scattered, while
    // cache pricing remains based on the stable planning baseline.
    let heap_seq = corr * clustered_logical;
    let heap_scatter = (1.0 - corr) * scattered_logical;
    let heap_io = page_io_cost(
        AccessKind::OrderedNoPrefetch,
        heap_seq,
        cache,
        working_set_pages,
    ) + page_io_cost(
        AccessKind::OrderedNoPrefetch,
        heap_scatter,
        cache,
        working_set_pages,
    );
    // Once the combined lookup/outer working set exceeds the stable planning
    // cache, every chunk-local visit pays the full buffer lookup/pin, MVCC
    // header and slot-routing path amid eviction pressure. Preserve the
    // established light resident-table calibration for cache-sized joins;
    // this large-working-set gate is stable schema/statistics input, never
    // live residency.
    let heap_probe_cpu_tuples = if working_set_pages > cache {
        HEAP_PROBE_CPU_TUPLES
    } else {
        1.0
    };
    let heap_page_cpu = (heap_seq + heap_scatter) * consts.cpu_tuple_cost() * heap_probe_cpu_tuples;
    // Bulk routing still compares every distinct probe key against the
    // B-tree's routing/leaf key space. This CPU exists even when all pages
    // are resident and is therefore separate from page I/O.
    let key_routing_cpu = keys * leaf_pages.max(2.0).log2() * consts.cpu_index_tuple_cost();
    let cpu_index = (index_height + leaf_pages) * consts.cpu_index_tuple_cost() + key_routing_cpu;
    let cpu_tuples = m * (consts.cpu_index_tuple_cost() + consts.cpu_tuple_cost());
    let startup = descent_io + index_height * consts.cpu_index_tuple_cost();
    let total = startup
        + leaf_io
        + heap_io
        + heap_page_cpu
        + cpu_tuples
        + (cpu_index - index_height * consts.cpu_index_tuple_cost());
    let cost = Cost {
        startup,
        total: total.max(startup),
        io_seq_pages: leaf_pages + heap_seq + heap_scatter,
        io_rnd_pages: index_height,
        cpu_units: cpu_index + cpu_tuples + heap_page_cpu,
        mem_bytes: 0.0,
        spill_bytes: 0.0,
    };
    BatchedIndexLookupEstimate {
        cost,
        distinct_heap_pages: (corr * physical_clustered_pages + (1.0 - corr) * scattered_pages)
            .min(pages),
        logical_heap_page_visits: heap_seq + heap_scatter,
        working_set_pages,
        probe_batches,
    }
}

#[cfg(test)]
pub fn batched_index_lookup(
    ctx: &CboContext,
    index_height: f64,
    leaf_pages: f64,
    matched_rows: f64,
    probe_keys: f64,
    table_pages: f64,
    output_width: f64,
    correlation: f64,
) -> Cost {
    estimate_batched_index_lookup(
        ctx,
        BatchedIndexLookupInput {
            index_height,
            leaf_pages,
            matched_rows,
            probe_keys,
            table_pages,
            output_width,
            correlation,
            competing_pages: 0.0,
        },
    )
    .cost
}

/// Cost of an index-only ("fast") scan: descend the B-tree, walk the
/// leaf chain across `leaf_pages`, and emit `matched_rows` from the
/// index entries directly without touching the heap. Used for covering
/// scans where every required column lives in the index (key columns
/// or INCLUDE'd columns), so heap fetches would be pure waste.
///
/// Compared with `index_range_scan(..., correlation = 1.0)` this drops
/// the residual `correlated_pages` heap I/O term entirely, making fast
/// scan strictly cheaper for the same `(height, leaf_pages, rows)`.
///
/// Per-row CPU is `cpu_tuple_cost` — symmetric with `seq_scan` — because
/// the leaf entry *is* the emitted tuple (no heap fetch follows). So when
/// a covering index and a seq scan return the same rows, the cheaper one
/// is whichever touches fewer pages, which is the whole point of an
/// index-only scan on a wide table with a narrow covering key.
pub fn index_fast_scan(
    ctx: &CboContext,
    index_height: f64,
    leaf_pages: f64,
    matched_rows: f64,
    output_width: f64,
) -> Cost {
    let consts = cbo_consts();
    let cache = ctx.effective_cache_pages as f64;
    // Use leaf_pages as the working-set proxy: descent + leaf traversal
    // compete only with the index's own pages, never with heap.
    let working_set = leaf_pages.max(1.0);
    let descent_io = page_io_cost(AccessKind::Random, index_height, cache, working_set);
    let leaf_io = page_io_cost(AccessKind::Sequential, leaf_pages, cache, working_set);
    let cpu_index = (index_height + leaf_pages) * consts.cpu_index_tuple_cost();
    // A covering fast scan emits one output tuple per leaf entry and never
    // touches the heap, so the leaf entry *is* the result tuple. Charge the
    // same per-row decode cost a seq scan pays (`per_row_decode_cost`) instead
    // of `cpu_index_tuple_cost + cpu_tuple_cost`: the old sum double-counted
    // the entry as both an index tuple *and* a separate heap tuple, making a
    // covering scan strictly *more* expensive per row than a seq scan even
    // though both decode the same projected columns. With per-row CPU now
    // symmetric and width-aware (here `output_width` is the covered-column
    // width), the leaf-vs-heap page-count difference — the real, physical
    // advantage of an index-only scan — decides the comparison.
    let cpu_tuples = matched_rows * per_row_decode_cost(output_width);
    let startup = descent_io + index_height * consts.cpu_index_tuple_cost();
    let total =
        startup + leaf_io + cpu_tuples + (cpu_index - index_height * consts.cpu_index_tuple_cost());
    Cost {
        startup,
        total,
        io_seq_pages: leaf_pages,
        io_rnd_pages: index_height,
        cpu_units: cpu_index + cpu_tuples,
        mem_bytes: 0.0,
        spill_bytes: 0.0,
    }
}

/// Approximate the height of a B-tree storing `entries` rows. mydb's
/// btree fanout is on the order of a few hundred per internal page
/// (depends on key width), so we use 256 as a conservative default.
/// Phase 6 calibration can tune this via `cbo_btree_fanout` if needed.
pub fn estimated_btree_height(entries: f64) -> f64 {
    if entries <= 1.0 {
        return 1.0;
    }
    let fanout = 256.0_f64;
    (entries.log(fanout)).ceil().max(1.0)
}

/// Cost of a hash join. The build side feeds a hash table, the probe
/// side streams through it. Returns a [`Cost`] whose `startup` reflects
/// the build phase (must complete before probing emits any rows).
///
/// Inputs:
/// - `build_cost`/`probe_cost`: child plan costs (already include their
///   own IO + CPU)
/// - `build_rows` / `probe_rows`: post-filter row counts hitting each
///   side of the join
/// - `build_width` / `probe_width`: tuple widths in bytes
/// - `output_rows` / `output_width`: post-join cardinality and width
/// - `match_rate`: fraction of probe rows that find a match
///   (`output_rows / probe_rows` clamped to `[0, 1]`)
/// - `bloom_filter_applicable`: when true, applies the bloom-prefilter
///   savings from the executor's runtime filter
pub fn hash_join(
    ctx: &CboContext,
    build_cost: &Cost,
    probe_cost: &Cost,
    build_rows: f64,
    probe_rows: f64,
    build_width: f64,
    output_rows: f64,
    output_width: f64,
    match_rate: f64,
    bloom_filter_applicable: bool,
) -> Cost {
    let consts = cbo_consts();
    // Include key/control/next-array state instead of treating a HashMap as a
    // fixed 1.5x row-payload multiplier. Probe width is derived conservatively
    // from the joined output; it affects spill I/O only, never plan memory.
    let build_mem_bytes = build_rows * (build_width + 64.0) * 1.25;
    let probe_width = (output_width - build_width).max(8.0);
    // The vector Grace path builds a bounded Bloom filter before partitioning
    // the probe side and rejects definite misses before they are serialized.
    // Costing the complete probe relation as spill input made a selective
    // fact-table hash join look orders of magnitude more expensive than its
    // actual I/O.  Keep a conservative 12.5% false-positive allowance (the
    // executor uses at least 10 bits/key while the filter is below its cap),
    // and always retain the estimated matching rows.
    const CONSERVATIVE_BLOOM_FALSE_POSITIVE_RATE: f64 = 0.125;
    let probe_spill_pass_rate = if bloom_filter_applicable {
        let matches = match_rate.clamp(0.0, 1.0);
        matches + (1.0 - matches) * CONSERVATIVE_BLOOM_FALSE_POSITIVE_RATE
    } else {
        1.0
    };
    let spill = super::spill_model::hash_join_spill_cost_with_probe(
        build_mem_bytes,
        probe_rows * probe_width * probe_spill_pass_rate,
        ctx.planning_mem_budget_bytes as f64,
    );
    let cpu_build = build_rows * (consts.hash_build_cost() + consts.cpu_tuple_cost());
    let cpu_probe = probe_rows
        * (consts.hash_probe_cost() + match_rate.clamp(0.0, 1.0) * consts.cpu_operator_cost());
    let cpu_output = output_rows.max(0.0) * per_row_decode_cost(output_width);
    let bloom_savings = if bloom_filter_applicable {
        // Bloom prefilter discards (1 - match_rate) of probe rows
        // before they hit the hash table. Phase 6 calibration may
        // refine this 0.7 efficiency factor.
        probe_rows * (1.0 - match_rate.clamp(0.0, 1.0)) * consts.cpu_operator_cost() * 0.7
    } else {
        0.0
    };
    let startup = build_cost.total + cpu_build + spill;
    let total = startup + probe_cost.total + cpu_probe + cpu_output - bloom_savings;
    Cost {
        startup,
        total: total.max(startup),
        io_seq_pages: build_cost.io_seq_pages + probe_cost.io_seq_pages,
        io_rnd_pages: build_cost.io_rnd_pages + probe_cost.io_rnd_pages,
        cpu_units: build_cost.cpu_units + probe_cost.cpu_units + cpu_build + cpu_probe + cpu_output
            - bloom_savings,
        mem_bytes: build_cost
            .mem_bytes
            .max(probe_cost.mem_bytes)
            .max(build_mem_bytes.min(ctx.planning_mem_budget_bytes as f64)),
        spill_bytes: if spill > 0.0 { build_mem_bytes } else { 0.0 },
    }
}

pub(crate) struct ReverseSemiAntiHashJoinInput {
    pub build_rows: f64,
    pub probe_rows: f64,
    pub probe_width: f64,
    pub key_width: f64,
    pub matched_build_rows: f64,
    pub output_rows: f64,
}

/// The reverse SEMI/ANTI kernel retains probe batches plus candidate/matched
/// key sets, then streams the nominal build. It never retains build payload.
/// Keep full child scan costs (including local predicates) and charge a key
/// lookup for every surviving build row: runtime-filter pushdown is optional.
/// If this state does not fit the fixed planning grant, decline the estimate
/// so the caller prices the ordinary Grace-capable fallback instead.
pub(crate) fn reverse_semi_anti_hash_join(
    ctx: &CboContext,
    build_cost: &Cost,
    probe_cost: &Cost,
    input: ReverseSemiAntiHashJoinInput,
) -> Option<Cost> {
    let ReverseSemiAntiHashJoinInput {
        build_rows,
        probe_rows,
        probe_width,
        key_width,
        matched_build_rows,
        output_rows,
    } = input;
    // The generic executor reserves 160 bytes/probe row for two capacity-
    // sized sets. Include both encoded-key buffers and retained probe data.
    // This also conservatively covers the native integer lane's exact set
    // and matched set without assuming a key type or live memory grant.
    let state_bytes =
        probe_rows.max(0.0) * (probe_width.max(0.0) + 160.0 + 2.0 * key_width.max(0.0));
    if state_bytes > ctx.planning_mem_budget_bytes as f64 {
        return None;
    }
    let consts = cbo_consts();
    let cpu_candidates = probe_rows.max(0.0) * (consts.hash_build_cost() + consts.cpu_tuple_cost());
    let cpu_lookup = build_rows.max(0.0) * consts.hash_probe_cost();
    let cpu_matches = matched_build_rows.clamp(0.0, build_rows.max(0.0)) * consts.hash_build_cost();
    let cpu_output = probe_rows.max(0.0) * consts.cpu_operator_cost()
        + output_rows.max(0.0) * per_row_decode_cost(probe_width);
    let cpu = cpu_candidates + cpu_lookup + cpu_matches + cpu_output;
    let mut cost = build_cost.add(probe_cost);
    cost.total += cpu;
    // Both inputs are consumed before the first selected probe batch emits.
    cost.startup = cost.total;
    cost.cpu_units += cpu;
    cost.mem_bytes = cost.mem_bytes.max(state_bytes);
    Some(cost)
}

/// Cost of an index nested-loop join: for each outer row, perform an
/// inner index lookup. `per_lookup` is the cost of one inner probe
/// (typically computed by [`index_lookup`] with `matched_rows` set to
/// the per-outer match count).
pub fn index_nested_loop_join(
    outer_cost: &Cost,
    per_lookup: &Cost,
    outer_rows: f64,
    output_rows: f64,
    output_width: f64,
) -> Cost {
    let consts = cbo_consts();
    let cpu = outer_rows * consts.cpu_tuple_cost()
        + output_rows.max(0.0) * per_row_decode_cost(output_width);
    let inner_total = outer_rows * per_lookup.total;
    let startup = outer_cost.startup + per_lookup.startup;
    let total = outer_cost.total + inner_total + cpu;
    Cost {
        startup,
        total: total.max(startup),
        io_seq_pages: outer_cost.io_seq_pages,
        io_rnd_pages: outer_cost.io_rnd_pages + outer_rows * per_lookup.io_rnd_pages,
        cpu_units: outer_cost.cpu_units + cpu + outer_rows * per_lookup.cpu_units,
        mem_bytes: outer_cost.mem_bytes.max(per_lookup.mem_bytes),
        spill_bytes: outer_cost.spill_bytes + outer_rows * per_lookup.spill_bytes,
    }
}

/// Cost of a batched index nested-loop join. The outer side is still
/// scanned once, but inner probes are de-duplicated and executed as a
/// single batched index/heap access rather than `outer_rows` independent
/// B-tree descents.
///
/// The per-row bookkeeping mirrors the executor's batched-INLJ
/// pipeline, which is hash-shaped on both legs:
/// - every outer row encodes an `IndexKey` (heap-allocated key parts)
///   and inserts into the `key_to_left` hash map — at least the work
///   of a hash-join build row (`hash_build_cost`);
/// - every matched index entry pays a prefix-key probe of that map
///   plus the joined output-row materialisation — at least the work
///   of a hash-join probe row plus a tuple copy
///   (`hash_probe_cost + cpu_tuple_cost`).
///
/// Earlier revisions charged only `outer_rows × (cpu_tuple +
/// cpu_operator)` here, which priced the whole batch bookkeeping at
/// roughly one comparison per outer row. That made a large batched
/// INLJ look ~2× cheaper than the equivalent hash join even though
/// the executor does strictly more per-row work (hash insert + hash
/// probe + row materialisation vs. one hash probe), inverting plan
/// choice on large FK-shaped joins.
pub fn batched_index_nested_loop_join(
    outer_cost: &Cost,
    batched_lookup: &Cost,
    outer_rows: f64,
    matched_rows: f64,
    output_rows: f64,
    output_width: f64,
) -> Cost {
    let consts = cbo_consts();
    let cpu = outer_rows * (consts.cpu_tuple_cost() + consts.hash_build_cost())
        + matched_rows.max(0.0) * consts.hash_probe_cost()
        + output_rows.max(0.0) * per_row_decode_cost(output_width);
    let startup = outer_cost.startup + batched_lookup.startup;
    let total = outer_cost.total + batched_lookup.total + cpu;
    Cost {
        startup,
        total: total.max(startup),
        io_seq_pages: outer_cost.io_seq_pages + batched_lookup.io_seq_pages,
        io_rnd_pages: outer_cost.io_rnd_pages + batched_lookup.io_rnd_pages,
        cpu_units: outer_cost.cpu_units + cpu + batched_lookup.cpu_units,
        mem_bytes: outer_cost.mem_bytes.max(batched_lookup.mem_bytes),
        spill_bytes: outer_cost.spill_bytes + batched_lookup.spill_bytes,
    }
}

/// Cost of a plain nested-loop join (no hash, no inner index). Each
/// outer row scans the entire inner side. Inner cost is paid once if
/// the inner is cached (block NL); otherwise `outer_rows` times. We
/// model the cached-block variant, which is what the executor uses.
pub fn nested_loop_join(
    outer_cost: &Cost,
    inner_cost: &Cost,
    outer_rows: f64,
    inner_rows: f64,
    output_rows: f64,
    output_width: f64,
    qual_cost_per_row: f64,
) -> Cost {
    let consts = cbo_consts();
    let cpu = outer_rows * inner_rows * (consts.cpu_operator_cost() + qual_cost_per_row)
        + output_rows.max(0.0) * per_row_decode_cost(output_width);
    let startup = outer_cost.startup + inner_cost.total;
    let total = outer_cost.total + inner_cost.total + cpu;
    Cost {
        startup,
        total: total.max(startup),
        io_seq_pages: outer_cost.io_seq_pages + inner_cost.io_seq_pages,
        io_rnd_pages: outer_cost.io_rnd_pages + inner_cost.io_rnd_pages,
        cpu_units: outer_cost.cpu_units + inner_cost.cpu_units + cpu,
        mem_bytes: inner_cost.mem_bytes.max(outer_cost.mem_bytes),
        spill_bytes: outer_cost.spill_bytes + inner_cost.spill_bytes,
    }
}

/// Cost of an in-memory or external sort over `input_rows` of width
/// `width`. When the input fits in `mem_budget_bytes`, sort is purely
/// CPU `O(n log n)`; otherwise external-sort spill cost kicks in.
///
/// Sort is fully pipeline-breaking: `startup == total - emit_cpu`,
/// because nothing can leave the operator until the entire input is
/// consumed.
pub fn sort(ctx: &CboContext, input_cost: &Cost, input_rows: f64, width: f64) -> Cost {
    let consts = cbo_consts();
    let n = input_rows.max(0.0);
    if n == 0.0 {
        return *input_cost;
    }
    let log = n.max(2.0).log2();
    let cpu = n * log * consts.sort_cost_factor() * consts.cpu_operator_cost();
    let input_bytes = n * width;
    let spill =
        super::spill_model::sort_spill_cost(input_bytes, ctx.planning_mem_budget_bytes as f64);
    let emit_cpu = n * consts.cpu_tuple_cost();
    let startup = input_cost.total + cpu + spill;
    let total = startup + emit_cpu;
    Cost {
        startup,
        total,
        io_seq_pages: input_cost.io_seq_pages
            + (spill / consts.spill_page_cost().max(0.001) / 2.0).max(0.0),
        io_rnd_pages: input_cost.io_rnd_pages,
        cpu_units: input_cost.cpu_units + cpu + emit_cpu,
        mem_bytes: input_cost
            .mem_bytes
            .max(input_bytes.min(ctx.planning_mem_budget_bytes as f64)),
        spill_bytes: input_cost.spill_bytes + if spill > 0.0 { input_bytes } else { 0.0 },
    }
}

/// Cost of incremental sort when the input is already ordered by a
/// non-empty ORDER BY prefix. Without group cardinality stats for the
/// prefix, model it as a full sort with a CPU and spill discount
/// proportional to the presorted-key fraction.
pub fn incremental_sort(
    ctx: &CboContext,
    input_cost: &Cost,
    input_rows: f64,
    width: f64,
    presorted_prefix: usize,
    total_keys: usize,
) -> Cost {
    let full = sort(ctx, input_cost, input_rows, width);
    let total_keys = total_keys.max(1) as f64;
    let prefix = (presorted_prefix as f64).clamp(0.0, total_keys);
    let discount = (1.0 - 0.6 * (prefix / total_keys)).clamp(0.25, 1.0);
    let input_total = input_cost.total;
    let adjusted_extra = (full.total - input_total).max(0.0) * discount;
    let adjusted_startup_extra = (full.startup - input_total).max(0.0) * discount;
    Cost {
        startup: input_total + adjusted_startup_extra,
        total: input_total + adjusted_extra,
        io_seq_pages: input_cost.io_seq_pages
            + (full.io_seq_pages - input_cost.io_seq_pages).max(0.0) * discount,
        io_rnd_pages: full.io_rnd_pages,
        cpu_units: input_cost.cpu_units
            + (full.cpu_units - input_cost.cpu_units).max(0.0) * discount,
        mem_bytes: full.mem_bytes,
        spill_bytes: input_cost.spill_bytes
            + (full.spill_bytes - input_cost.spill_bytes).max(0.0) * discount,
    }
}

/// Cost of a TopK heap of capacity `k` over `input_rows`. Memory is
/// fixed at `k × width`, CPU is `n log k`.
pub fn top_k(input_cost: &Cost, input_rows: f64, k: f64, width: f64) -> Cost {
    let consts = cbo_consts();
    let n = input_rows.max(1.0);
    let k = k.max(1.0);
    let log_k = k.max(2.0).log2();
    let cpu = n * log_k * consts.cpu_operator_cost();
    let mem = k * width;
    let startup = input_cost.total + cpu;
    let emit_cpu = k.min(n) * consts.cpu_tuple_cost();
    Cost {
        startup,
        total: startup + emit_cpu,
        io_seq_pages: input_cost.io_seq_pages,
        io_rnd_pages: input_cost.io_rnd_pages,
        cpu_units: input_cost.cpu_units + cpu + emit_cpu,
        mem_bytes: input_cost.mem_bytes.max(mem),
        spill_bytes: input_cost.spill_bytes,
    }
}

/// Cost of a deep-page ORDER BY window. The executor evaluates each
/// sort key once, uses quickselect to isolate `[offset, offset+limit)`,
/// then sorts only the final page.
pub fn top_window(
    input_cost: &Cost,
    input_rows: f64,
    limit: f64,
    offset: f64,
    width: f64,
    order_keys: usize,
) -> Cost {
    let consts = cbo_consts();
    let n = input_rows.max(1.0);
    let limit = limit.max(0.0);
    let offset = offset.max(0.0);
    let page_rows = if offset >= n {
        0.0
    } else {
        limit.min(n - offset)
    };
    let key_eval = n * (order_keys.max(1) as f64) * consts.cpu_operator_cost();
    let partition = n * 2.0 * consts.cpu_operator_cost();
    let page_sort = if page_rows > 1.0 {
        page_rows * page_rows.log2() * consts.cpu_operator_cost()
    } else {
        0.0
    };
    let cpu = key_eval + partition + page_sort;
    let startup = input_cost.total + cpu;
    let emit_cpu = page_rows * consts.cpu_tuple_cost();
    Cost {
        startup,
        total: startup + emit_cpu,
        io_seq_pages: input_cost.io_seq_pages,
        io_rnd_pages: input_cost.io_rnd_pages,
        cpu_units: input_cost.cpu_units + cpu + emit_cpu,
        mem_bytes: input_cost.mem_bytes.max(n * width),
        spill_bytes: input_cost.spill_bytes,
    }
}

/// Per-group state size of a numeric/scalar accumulator. The values
/// mirror the actual heap footprint of the accumulator types in
/// `src/engine/executor/select/aggregate.rs`:
///
/// - `CountAcc`         — single `u64`                 ≈  8 B
/// - `SumAcc`/`MinMaxAcc` over numerics — `NumericAggState` enum ≈ 24 B
/// - `AvgAcc`            — sum (`NumericAggState`) + count (`u64`) ≈ 32 B
/// - `VarianceAcc`       — Welford triplet (count, mean, M2) ≈ 32 B
/// - `BitAcc`            — accumulator + presence flag ≈ 16 B
/// - `GroupConcatAcc`    — owned `String` buffer; conservative avg 256 B
/// - `JsonArrayAggAcc`/`JsonObjectAggAcc` — `Vec<Value>`/`HashMap` ~256 B
/// - `CountDistinctAcc`/`DistinctReducer` — `HashSet<ValueKey>`, where
///   each entry is 24 B header + payload; the per-row cost is amortised
///   by the planner's `input_rows / group_count` estimate elsewhere.
pub fn accumulator_state_bytes_by_name(name: &str, distinct: bool) -> f64 {
    let upper = name.to_ascii_uppercase();
    let base = match upper.as_str() {
        "COUNT" => 8.0,
        "SUM" | "MIN" | "MAX" | "BIT_AND" | "BIT_OR" | "BIT_XOR" => 24.0,
        "AVG" => 32.0,
        "STD" | "STDDEV" | "STDDEV_POP" | "STDDEV_SAMP" | "VARIANCE" | "VAR_POP" | "VAR_SAMP" => {
            32.0
        }
        "GROUP_CONCAT" | "JSON_ARRAYAGG" | "JSON_OBJECTAGG" => 256.0,
        _ => 24.0,
    };
    if distinct {
        // A DISTINCT aggregate carries a per-group HashSet keyed by
        // ValueKey. The set itself is ~64 B of bucket-array overhead
        // even before any rows land in it. Per-entry overhead is
        // priced in by the planner via its (input_rows / group_count)
        // multiplier when it has one available.
        base + 64.0
    } else {
        base
    }
}

/// Sum of [`accumulator_state_bytes_by_name`] across a slot list, plus
/// a fixed 32 B header for the per-group `Vec<Box<dyn Accumulator>>`
/// pointer table. Used to refine [`hash_aggregate`]'s `state_bytes`
/// estimate so DISTINCT-/GROUP_CONCAT-heavy queries are not priced as
/// cheaply as plain SUM-over-int queries.
pub fn aggregate_state_bytes_from_specs(specs: &[(String, bool)]) -> f64 {
    let mut total = 32.0;
    for (name, distinct) in specs {
        total += accumulator_state_bytes_by_name(name, *distinct);
    }
    total
}

/// Cost of a hash-based group-by aggregation. Builds an in-memory
/// state-per-group hash; spills if state outgrows the budget.
///
/// `state_bytes_per_group` is optional; when `None`, falls back to the
/// pre-Phase-4 estimate (`output_width * 1.5 + 64`). Pass `Some(..)`
/// computed via [`aggregate_state_bytes_from_specs`] when the planner
/// knows the aggregate list, so DISTINCT / GROUP_CONCAT plans get a
/// realistic memory footprint and the CBO can route them toward a
/// streaming alternative when sort + ordered aggregation would be
/// cheaper.
pub fn hash_aggregate(
    ctx: &CboContext,
    input_cost: &Cost,
    input_rows: f64,
    group_count: f64,
    output_width: f64,
) -> Cost {
    hash_aggregate_with_state(ctx, input_cost, input_rows, group_count, output_width, None)
}

pub fn hash_aggregate_with_state(
    ctx: &CboContext,
    input_cost: &Cost,
    input_rows: f64,
    group_count: f64,
    output_width: f64,
    state_bytes_per_group: Option<f64>,
) -> Cost {
    let consts = cbo_consts();
    let per_group_bytes = state_bytes_per_group.unwrap_or_else(|| output_width * 1.5 + 64.0);
    let state_bytes = group_count * per_group_bytes;
    let spill =
        super::spill_model::hash_agg_spill_cost(state_bytes, ctx.planning_mem_budget_bytes as f64);
    let cpu = input_rows * (consts.hash_build_cost() + consts.cpu_tuple_cost());
    let emit_cpu = group_count * consts.cpu_tuple_cost();
    let startup = input_cost.total + cpu + spill;
    let total = startup + emit_cpu;
    Cost {
        startup,
        total,
        io_seq_pages: input_cost.io_seq_pages,
        io_rnd_pages: input_cost.io_rnd_pages,
        cpu_units: input_cost.cpu_units + cpu + emit_cpu,
        mem_bytes: input_cost
            .mem_bytes
            .max(state_bytes.min(ctx.planning_mem_budget_bytes as f64)),
        spill_bytes: input_cost.spill_bytes + if spill > 0.0 { state_bytes } else { 0.0 },
    }
}

/// Cost of a streaming (sorted-input) aggregate. No hash table — just
/// constant memory and a single CPU pass.
pub fn streaming_aggregate(
    input_cost: &Cost,
    input_rows: f64,
    group_count: f64,
    output_width: f64,
) -> Cost {
    let consts = cbo_consts();
    let cpu = input_rows * consts.cpu_tuple_cost();
    let emit = group_count * consts.cpu_tuple_cost();
    Cost {
        startup: input_cost.startup,
        total: input_cost.total + cpu + emit,
        io_seq_pages: input_cost.io_seq_pages,
        io_rnd_pages: input_cost.io_rnd_pages,
        cpu_units: input_cost.cpu_units + cpu + emit,
        mem_bytes: input_cost.mem_bytes.max(output_width + 64.0),
        spill_bytes: input_cost.spill_bytes,
    }
}

/// Cost of `DISTINCT` via HashSet. Behaves like a hash aggregate with
/// no per-group state (just keys).
pub fn hash_distinct(
    ctx: &CboContext,
    input_cost: &Cost,
    input_rows: f64,
    distinct_rows: f64,
    width: f64,
) -> Cost {
    hash_aggregate(ctx, input_cost, input_rows, distinct_rows, width)
}

/// Cost of `DISTINCT` via streaming (sorted) input. O(n) compare against
/// last-emitted, O(1) memory.
pub fn streaming_distinct(
    input_cost: &Cost,
    input_rows: f64,
    distinct_rows: f64,
    width: f64,
) -> Cost {
    streaming_aggregate(input_cost, input_rows, distinct_rows, width)
}

#[cfg(test)]
/// Cost of a window aggregate. The frame shape determines per-row work:
/// - `O(1)` per row for cumulative frames (UNBOUNDED PRECEDING + CURRENT
///   ROW),
/// - `O(frame_size)` per row for sliding RANGE / ROWS frames.
///
/// `partition_count` × `avg_partition_size = input_rows`. We model the
/// sort prerequisite separately (it's wrapped by the planner's normal
/// Sort node when needed).
pub fn window_agg(
    input_cost: &Cost,
    input_rows: f64,
    avg_frame_size: f64,
    output_width: f64,
) -> Cost {
    let consts = cbo_consts();
    let n = input_rows.max(1.0);
    let frame = avg_frame_size.max(1.0);
    let cpu = n * frame * consts.cpu_operator_cost();
    let emit = n * consts.cpu_tuple_cost();
    Cost {
        startup: input_cost.total,
        total: input_cost.total + cpu + emit,
        io_seq_pages: input_cost.io_seq_pages,
        io_rnd_pages: input_cost.io_rnd_pages,
        cpu_units: input_cost.cpu_units + cpu + emit,
        mem_bytes: input_cost.mem_bytes.max(n * output_width.max(1.0)),
        spill_bytes: input_cost.spill_bytes,
    }
}

/// Cost of a Limit/Offset node. The child can stop early once
/// `offset + limit` rows have been seen, so total cost is bounded.
pub fn limit(input_cost: &Cost, input_rows: f64, limit_rows: f64, offset_rows: f64) -> Cost {
    let consts = cbo_consts();
    let available = (input_rows - offset_rows).max(0.0);
    let output_rows = limit_rows.max(0.0).min(available);
    let total_consumed = (offset_rows.max(0.0) + output_rows).min(input_rows.max(0.0));
    let stop_fraction = if input_rows > 0.0 {
        (total_consumed / input_rows).clamp(0.0, 1.0)
    } else {
        1.0
    };
    let downstream_total =
        input_cost.startup + (input_cost.total - input_cost.startup) * stop_fraction;
    let emit = output_rows * consts.cpu_tuple_cost();
    Cost {
        startup: input_cost.startup,
        total: downstream_total + emit,
        io_seq_pages: input_cost.io_seq_pages * stop_fraction,
        io_rnd_pages: input_cost.io_rnd_pages * stop_fraction,
        cpu_units: input_cost.cpu_units * stop_fraction + emit,
        mem_bytes: input_cost.mem_bytes,
        spill_bytes: input_cost.spill_bytes * stop_fraction,
    }
}

/// Cost of a Projection node — pure CPU per row.
pub fn projection(input_cost: &Cost, output_rows: f64, output_width: f64) -> Cost {
    let cpu = output_rows * per_row_decode_cost(output_width);
    Cost {
        startup: input_cost.startup,
        total: input_cost.total + cpu,
        io_seq_pages: input_cost.io_seq_pages,
        io_rnd_pages: input_cost.io_rnd_pages,
        cpu_units: input_cost.cpu_units + cpu,
        mem_bytes: input_cost.mem_bytes,
        spill_bytes: input_cost.spill_bytes,
    }
}

/// Approximate average tuple width (bytes) for a table schema. Sums
/// the per-column declared byte length and adds a 16-byte header
/// allowance. Variable-length columns (`VARCHAR`, `TEXT`) report their
/// max length, which over-estimates fixed-width work but keeps the
/// optimizer conservative — Phase 6 calibration tunes this if needed.
pub fn estimated_row_width(schema: &crate::storage::TableSchema) -> f64 {
    let header = 16.0;
    let body: f64 = schema
        .columns
        .iter()
        .map(|column| {
            schema
                .stats
                .columns
                .get(&column.name.to_ascii_lowercase())
                .and_then(|stats| stats.average_width_bytes)
                .unwrap_or(column.data_type.key_byte_length() as f64)
            // The tuple header allowance already includes the null
            // bitmap. Charging another bit per nullable column here
            // double-counts storage and makes ANALYZE/non-ANALYZE
            // widths disagree for otherwise identical schemas.
        })
        .sum();
    header + body
}

/// Approximate the heap page count holding `rows` of `width` bytes.
/// Inflates by 1.25x to account for tuple headers, alignment slack,
/// and free-space reserved for updates.
pub fn estimated_heap_pages(rows: f64, width: f64) -> f64 {
    if rows <= 0.0 {
        return 0.0;
    }
    let usable_per_page = (PAGE_SIZE_BYTES as f64) * 0.85;
    let bytes = rows * width.max(8.0) * 1.25;
    (bytes / usable_per_page).ceil().max(1.0)
}

/// Approximate the leaf page count for `rows` entries in a B-tree
/// over `index`. Older code unconditionally assumed "100 entries per
/// leaf" — for a narrow secondary index (e.g. a single `INT` key) that
/// under-estimates leaf fanout by 4-6×, making `index_fast_scan` look
/// almost as expensive as a full heap scan and erasing its real I/O
/// advantage.
///
/// The estimate uses the actual sum of declared key-byte lengths
/// across the index's effective columns (declared key + INCLUDE
/// columns), plus a per-entry overhead for the RID and varint length
/// prefixes, then assumes ~70 % page utilization (typical for B-trees
/// after ordinary inserts/splits).
///
/// Result is clamped to ≥ 1.
pub fn estimated_index_leaf_pages(
    schema: &crate::storage::TableSchema,
    index: &crate::storage::IndexSchema,
    rows: f64,
) -> f64 {
    if rows <= 1.0 {
        return 1.0;
    }
    // RowId (page_id u64 + slot u16 + generation u16 + table_id u64
    // discriminator) plus a few bytes of length-prefix and tag
    // overhead per encoded `IndexKey`/`KeyPart`. Conservative-but-tight.
    const ENTRY_OVERHEAD_BYTES: f64 = 28.0;
    let key_bytes: usize = index
        .effective_columns()
        .iter()
        .map(|col_name| {
            schema
                .columns
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(col_name))
                .map(|c| c.data_type.key_byte_length() + if c.nullable { 1 } else { 0 })
                .unwrap_or(8)
        })
        .sum();
    let entry_size = (key_bytes as f64) + ENTRY_OVERHEAD_BYTES;
    let usable_per_page = (PAGE_SIZE_BYTES as f64) * 0.70;
    let entries_per_leaf = (usable_per_page / entry_size.max(1.0)).floor().max(1.0);
    (rows / entries_per_leaf).ceil().max(1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> CboContext {
        CboContext::defaults_for_test()
    }

    #[test]
    fn seq_scan_zero_pages_is_zero_io() {
        let c = seq_scan(&ctx(), 0.0, 0.0, 0.0, 16.0, 0.0);
        assert_eq!(c.io_seq_pages, 0.0);
        assert_eq!(c.total, 0.0);
    }

    #[test]
    fn seq_scan_grows_with_pages() {
        let small = seq_scan(&ctx(), 10.0, 1000.0, 1000.0, 16.0, 0.0);
        let big = seq_scan(&ctx(), 1000.0, 100_000.0, 100_000.0, 16.0, 0.0);
        assert!(big.total > small.total);
    }

    #[test]
    fn seq_scan_qual_adds_cpu_only() {
        let no_qual = seq_scan(&ctx(), 100.0, 10_000.0, 10_000.0, 16.0, 0.0);
        let with_qual = seq_scan(&ctx(), 100.0, 10_000.0, 10_000.0, 16.0, 0.001);
        assert!(with_qual.total > no_qual.total);
        // IO unchanged.
        assert_eq!(with_qual.io_seq_pages, no_qual.io_seq_pages);
    }

    #[test]
    fn index_lookup_startup_is_descent_only() {
        let c = index_lookup(&ctx(), 3.0, 1.0, 1000.0, 16.0);
        // Descent dominates startup; heap fetch is in the diff.
        assert!(c.startup > 0.0);
        assert!(c.total > c.startup);
    }

    #[test]
    fn index_lookup_more_matches_more_cost() {
        let one = index_lookup(&ctx(), 3.0, 1.0, 1000.0, 16.0);
        let many = index_lookup(&ctx(), 3.0, 100.0, 1000.0, 16.0);
        assert!(many.total > one.total);
        assert_eq!(many.startup, one.startup); // descent same
    }

    #[test]
    fn covering_index_lookup_never_charges_heap_io() {
        let c = index_lookup(&ctx(), 3.0, 100.0, 0.0, 16.0);
        assert_eq!(c.io_rnd_pages, 3.0);
    }

    #[test]
    fn index_lookup_heap_pages_are_capped_by_table_pages() {
        let c = index_lookup(&ctx(), 3.0, 10_000.0, 100.0, 16.0);
        assert!(c.io_rnd_pages <= 103.0, "pages={}", c.io_rnd_pages);
    }

    #[test]
    fn correlated_range_cheaper_than_uncorrelated() {
        let corr = index_range_scan(&ctx(), 3.0, 5.0, 1000.0, 100_000.0, 1000.0, 16.0, 1.0);
        let uncorr = index_range_scan(&ctx(), 3.0, 5.0, 1000.0, 100_000.0, 1000.0, 16.0, 0.0);
        assert!(
            uncorr.total > corr.total,
            "uncorr={} corr={}",
            uncorr.total,
            corr.total
        );
    }

    #[test]
    fn resident_noncovering_range_still_charges_logical_heap_probe_cpu() {
        let c = CboContext::new(1_000_000, 256 * 1024 * 1024);
        let estimate = estimate_index_range_scan(
            &c,
            IndexRangeScanInput {
                index_height: 3.0,
                leaf_pages: 500.0,
                matched_rows: 200_000.0,
                table_rows: 1_000_000.0,
                table_pages: 10_000.0,
                output_width: 32.0,
                correlation: 0.0,
                mrr_chunk_rows: Some(2048.0),
            },
        );
        let probe_cpu_floor =
            estimate.logical_heap_probes * c.model.cpu_tuple_cost * HEAP_PROBE_CPU_TUPLES;
        assert!(estimate.hit_heap_probes > 0.0);
        assert!(
            estimate.cost.cpu_units >= probe_cpu_floor,
            "cpu={} probe_floor={probe_cpu_floor}",
            estimate.cost.cpu_units
        );
    }

    #[test]
    fn q6_large_uncorrelated_range_loses_to_projected_parallel_seq_scan() {
        let c = CboContext::new(131_072, 256 * 1024 * 1024);
        let table_rows = 6_001_215.0;
        let heap_pages = 55_000.0;
        let candidates = 826_271.0;
        let projection_width = 48.0;
        let qual = 0.02;
        let seq = parallel_seq_scan(
            &c,
            heap_pages,
            table_rows,
            114_160.0,
            projection_width,
            qual,
            2,
        );
        let mut index = index_range_scan(
            &c,
            3.0,
            1_500.0,
            candidates,
            table_rows,
            heap_pages,
            projection_width,
            0.02,
        );
        let residual_cpu = candidates * qual;
        index.total += residual_cpu;
        index.cpu_units += residual_cpu;
        assert!(
            seq.total < index.total,
            "Q6 projected parallel seq={} index={}",
            seq.total,
            index.total
        );
    }

    #[test]
    fn q14_small_date_range_still_prefers_index() {
        let c = CboContext::new(131_072, 256 * 1024 * 1024);
        let table_rows = 6_001_215.0;
        let heap_pages = 55_000.0;
        let candidates = 69_000.0;
        let qual = 0.01;
        let seq = parallel_seq_scan(&c, heap_pages, table_rows, candidates, 48.0, qual, 2);
        let mut index = index_range_scan(
            &c, 3.0, 150.0, candidates, table_rows, heap_pages, 48.0, 0.02,
        );
        let residual_cpu = candidates * qual;
        index.total += residual_cpu;
        index.cpu_units += residual_cpu;
        assert!(
            index.total < seq.total,
            "Q14 index={} projected parallel seq={}",
            index.total,
            seq.total
        );
    }

    #[test]
    fn range_heap_density_is_independent_of_projection_width() {
        let c = ctx();
        let input = IndexRangeScanInput {
            index_height: 3.0,
            leaf_pages: 20.0,
            matched_rows: 20_000.0,
            table_rows: 1_000_000.0,
            table_pages: 10_000.0,
            output_width: 32.0,
            correlation: 0.8,
            mrr_chunk_rows: None,
        };
        let narrow = estimate_index_range_scan(&c, input);
        let wide = estimate_index_range_scan(
            &c,
            IndexRangeScanInput {
                output_width: 512.0,
                ..input
            },
        );
        assert_eq!(narrow.rows_per_heap_page, wide.rows_per_heap_page);
        assert_eq!(narrow.logical_heap_probes, wide.logical_heap_probes);
        assert_eq!(narrow.distinct_heap_pages, wide.distinct_heap_pages);
    }

    #[test]
    fn mrr_discount_is_chunk_local_and_default_has_one_probe_per_candidate() {
        let c = ctx();
        let input = IndexRangeScanInput {
            index_height: 3.0,
            leaf_pages: 100.0,
            matched_rows: 100_000.0,
            table_rows: 1_000_000.0,
            table_pages: 10_000.0,
            output_width: 32.0,
            correlation: 0.0,
            mrr_chunk_rows: None,
        };
        let default = estimate_index_range_scan(&c, input);
        let chunked = estimate_index_range_scan(
            &c,
            IndexRangeScanInput {
                mrr_chunk_rows: Some(4096.0),
                ..input
            },
        );
        let impossible_global_mrr = distinct_pages(input.table_pages, input.matched_rows);
        assert_eq!(default.logical_heap_probes, input.matched_rows);
        assert!(chunked.logical_heap_probes < default.logical_heap_probes);
        assert!(
            chunked.logical_heap_probes > impossible_global_mrr * 2.0,
            "chunk-local={} global={}",
            chunked.logical_heap_probes,
            impossible_global_mrr
        );
        assert!(default.cost.is_valid() && chunked.cost.is_valid());
    }

    #[test]
    fn range_cost_is_monotone_in_candidates_and_cache_never_creates_nan() {
        for cache in [0_u64, 100, 10_000, 1_000_000] {
            let c = CboContext::new(cache, 256 * 1024 * 1024);
            let mut previous = 0.0;
            for rows in [1.0, 100.0, 10_000.0, 100_000.0] {
                let estimate = estimate_index_range_scan(
                    &c,
                    IndexRangeScanInput {
                        index_height: 3.0,
                        leaf_pages: (rows / 500.0_f64).ceil().max(1.0),
                        matched_rows: rows,
                        table_rows: 1_000_000.0,
                        table_pages: 10_000.0,
                        output_width: 64.0,
                        correlation: 0.3,
                        mrr_chunk_rows: None,
                    },
                );
                assert!(estimate.cost.is_valid());
                assert!(estimate.cost.total >= previous);
                previous = estimate.cost.total;
            }
        }
    }

    #[test]
    fn covering_fast_scan_beats_seq_when_fewer_pages() {
        // date_dim sf=1 shape: 73k rows, a wide 28-column heap (~2000 pages)
        // vs a narrow PRIMARY(d_date_sk) covering index (~410 leaf pages).
        // Both scans return the same rows and decode the same projected key,
        // so whichever touches fewer pages must win. Before the per-row CPU
        // fix the index-only scan lost: it was charged cpu_index_tuple_cost +
        // cpu_tuple_cost per row while the seq scan paid only cpu_tuple_cost,
        // and that ~0.005/row penalty outweighed its page-count advantage once
        // the warm cache discounted both scans' IO.
        let rows = 73_049.0;
        let heap_pages = 2_000.0;
        let leaf_pages = 410.0;
        let height = 3.0;
        let seq = seq_scan(&ctx(), heap_pages, rows, rows, 8.0, 0.0);
        let idx = index_fast_scan(&ctx(), height, leaf_pages, rows, 8.0);
        assert!(
            idx.total < seq.total,
            "covering index-only ({}) should beat seq scan ({}) when it reads far fewer pages",
            idx.total,
            seq.total
        );
    }

    #[test]
    fn covering_fast_scan_loses_when_not_smaller() {
        // Guard against over-correction: when the covering index is no smaller
        // than the heap (same page count), its extra B-tree descent makes it
        // cost a touch more than a plain seq scan. Symmetric per-row CPU must
        // not turn index-only into a universal win.
        let rows = 50_000.0;
        let pages = 800.0;
        let height = 3.0;
        let seq = seq_scan(&ctx(), pages, rows, rows, 8.0, 0.0);
        let idx = index_fast_scan(&ctx(), height, pages, rows, 8.0);
        assert!(
            idx.total >= seq.total,
            "with equal page counts seq ({}) should not lose to index-only ({})",
            seq.total,
            idx.total
        );
    }

    #[test]
    fn seq_scan_wide_rows_cost_more_cpu() {
        // Width-aware decode: a scan emitting wide tuples pays more per-row
        // CPU than one emitting narrow tuples, even over the same pages.
        let narrow = seq_scan(&ctx(), 100.0, 50_000.0, 50_000.0, 16.0, 0.0);
        let wide = seq_scan(&ctx(), 100.0, 50_000.0, 50_000.0, 400.0, 0.0);
        assert!(
            wide.total > narrow.total,
            "wide={} narrow={}",
            wide.total,
            narrow.total
        );
        // IO is identical (same page count); only the decode CPU differs.
        assert_eq!(wide.io_seq_pages, narrow.io_seq_pages);
    }

    #[test]
    fn per_row_decode_symmetric_across_scan_kinds() {
        // A heap scan and a covering index scan emitting the same-width tuples
        // pay the same per-row decode CPU. With equal page counts the index
        // scan only adds its B-tree descent, so it is never cheaper per row —
        // and on a wide table where the covering index has far fewer pages it
        // wins on IO instead. (See covering_fast_scan_beats_seq_when_fewer_pages.)
        let width = 200.0;
        let rows = 30_000.0;
        let seq = seq_scan(&ctx(), 60.0, rows, rows, width, 0.0);
        let idx = index_fast_scan(&ctx(), 3.0, 60.0, rows, width);
        assert!(
            idx.total >= seq.total,
            "idx={} seq={}",
            idx.total,
            seq.total
        );
        assert!(
            (idx.total - seq.total) / seq.total < 0.05,
            "per-row decode should dominate and match: idx={} seq={}",
            idx.total,
            seq.total
        );
    }

    #[test]
    fn btree_height_grows_logarithmically() {
        assert_eq!(estimated_btree_height(1.0), 1.0);
        let h_small = estimated_btree_height(1000.0);
        let h_huge = estimated_btree_height(1_000_000_000.0);
        assert!(h_huge > h_small);
        assert!(h_huge < 10.0, "got {h_huge}"); // log_256(1B) ≈ 4
    }

    #[test]
    fn heap_pages_for_empty_table() {
        assert_eq!(estimated_heap_pages(0.0, 16.0), 0.0);
    }

    #[test]
    fn heap_pages_grow_with_row_count() {
        let small = estimated_heap_pages(100.0, 100.0);
        let big = estimated_heap_pages(100_000.0, 100.0);
        assert!(big > small);
    }

    fn small_cost(rows: f64) -> Cost {
        Cost {
            startup: 0.0,
            total: rows * 0.01,
            ..Cost::ZERO
        }
    }

    #[test]
    fn hash_join_startup_includes_build_only() {
        let build = small_cost(100.0);
        let probe = small_cost(1000.0);
        let c = hash_join(
            &ctx(),
            &build,
            &probe,
            100.0,
            1000.0,
            16.0,
            100.0,
            32.0,
            0.1,
            false,
        );
        // Startup must cover build.total + cpu_build, and stay below total.
        assert!(c.startup >= build.total);
        assert!(c.total >= c.startup);
    }

    #[test]
    fn hash_join_bloom_savings_lower_total() {
        let build = small_cost(100.0);
        let probe = small_cost(10_000.0);
        let no_bloom = hash_join(
            &ctx(),
            &build,
            &probe,
            100.0,
            10_000.0,
            16.0,
            100.0,
            32.0,
            0.01,
            false,
        );
        let with_bloom = hash_join(
            &ctx(),
            &build,
            &probe,
            100.0,
            10_000.0,
            16.0,
            100.0,
            32.0,
            0.01,
            true,
        );
        assert!(with_bloom.total <= no_bloom.total);
    }

    #[test]
    fn hash_join_bloom_reduces_grace_probe_spill() {
        let mut tight = ctx();
        tight.planning_mem_budget_bytes = 1024 * 1024;
        let build = small_cost(100_000.0);
        let probe = small_cost(10_000_000.0);
        let no_bloom = hash_join(
            &tight,
            &build,
            &probe,
            100_000.0,
            10_000_000.0,
            32.0,
            100_000.0,
            64.0,
            0.01,
            false,
        );
        let with_bloom = hash_join(
            &tight,
            &build,
            &probe,
            100_000.0,
            10_000_000.0,
            32.0,
            100_000.0,
            64.0,
            0.01,
            true,
        );
        assert!(no_bloom.spill_bytes > 0.0);
        assert!(with_bloom.spill_bytes > 0.0);
        assert!(
            with_bloom.total < no_bloom.total * 0.5,
            "Bloom-filtered Grace probe was not repriced: bloom={} plain={}",
            with_bloom.total,
            no_bloom.total
        );
    }

    #[test]
    fn hash_join_spill_only_when_overflowing_budget() {
        let mut tight = ctx();
        tight.planning_mem_budget_bytes = 1024; // 1 KiB only
        let build = small_cost(10_000.0);
        let probe = small_cost(1_000.0);
        let c = hash_join(
            &tight, &build, &probe, 10_000.0, 1_000.0, 64.0, 1.0, 64.0, 0.1, false,
        );
        assert!(c.spill_bytes > 0.0);
    }

    #[test]
    fn nested_loop_join_cost_grows_with_outer() {
        let outer_small = small_cost(10.0);
        let outer_big = small_cost(1000.0);
        let inner = small_cost(100.0);
        let c1 = nested_loop_join(&outer_small, &inner, 10.0, 100.0, 100.0, 32.0, 0.0);
        let c2 = nested_loop_join(&outer_big, &inner, 1000.0, 100.0, 1000.0, 32.0, 0.0);
        assert!(c2.total > c1.total);
    }

    #[test]
    fn batched_index_nlj_charges_per_matched_row() {
        let outer = small_cost(100_000.0);
        let lookup = Cost {
            total: 50.0,
            ..Cost::ZERO
        };
        let few = batched_index_nested_loop_join(&outer, &lookup, 100_000.0, 1.0, 1.0, 32.0);
        let many =
            batched_index_nested_loop_join(&outer, &lookup, 100_000.0, 100_000.0, 100_000.0, 32.0);
        assert!(many.total > few.total);
        // Per-row bookkeeping is hash-shaped on both legs: an outer
        // (build) row and a matched (probe+materialise) row must each
        // cost at least as much as the corresponding hash-join row.
        let consts = cbo_consts();
        let min_expected = 100_000.0 * consts.hash_build_cost()
            + 100_000.0 * (consts.hash_probe_cost() + consts.cpu_tuple_cost());
        assert!(
            many.total - outer.total >= min_expected,
            "batched INLJ bookkeeping under-priced: {} < {}",
            many.total - outer.total,
            min_expected
        );
    }

    #[test]
    fn batched_index_lookup_monotone_in_matched_rows() {
        let c = ctx();
        let small = batched_index_lookup(&c, 3.0, 10.0, 1_000.0, 250.0, 50_000.0, 100.0, 0.5);
        let big =
            batched_index_lookup(&c, 3.0, 1_000.0, 500_000.0, 125_000.0, 50_000.0, 100.0, 0.5);
        assert!(big.total > small.total);
    }

    #[test]
    fn batched_index_lookup_reprices_each_probe_chunk() {
        // RIDs are sorted only inside each streamed probe chunk. Doubling
        // the key count therefore adds both CPU and another set of page
        // visits; a global one-sweep cap would incorrectly erase that work.
        let c = ctx();
        let table_pages = 50_000.0;
        let at_cover = batched_index_lookup(
            &c,
            3.0,
            1_000.0,
            5_000_000.0,
            1_250_000.0,
            table_pages,
            100.0,
            0.0,
        );
        let past_cover = batched_index_lookup(
            &c,
            3.0,
            1_000.0,
            10_000_000.0,
            2_500_000.0,
            table_pages,
            100.0,
            0.0,
        );
        assert!(
            past_cover.total > at_cover.total * 1.5,
            "additional probe chunks were globally capped: first={} second={}",
            at_cover.total,
            past_cover.total
        );
    }

    #[test]
    fn batched_index_lookup_cost_grows_with_distinct_probe_keys() {
        let c = ctx();
        let few = batched_index_lookup(&c, 3.0, 900.0, 500_000.0, 1_000.0, 80_000.0, 96.0, 0.2);
        let many = batched_index_lookup(&c, 3.0, 900.0, 500_000.0, 100_000.0, 80_000.0, 96.0, 0.2);
        assert!(many.total > few.total);
    }

    #[test]
    fn canonical_range_mrr_window_reuses_more_pages_than_smaller_lookup_windows() {
        let c = ctx();
        // The heap dwarfs the fixed planning cache. INLJ uses 16K-key probe
        // batches while a non-covering range scan is costed with the canonical
        // 131K-RID MRR window, so the latter reuses more pages per pass. The
        // range path can issue prefetch-friendly runs, while sparse INLJ
        // chunks retain point-read cost even after sorting their RIDs; pin
        // both the physical reuse and the resulting cost ordering.
        let batched = estimate_batched_index_lookup(
            &c,
            BatchedIndexLookupInput {
                index_height: 3.0,
                leaf_pages: 840.0,
                matched_rows: 300_000.0,
                probe_keys: 75_000.0,
                table_pages: 217_000.0,
                output_width: 400.0,
                correlation: 0.5,
                competing_pages: 0.0,
            },
        );
        let range_mrr = estimate_index_range_scan(
            &c,
            IndexRangeScanInput {
                index_height: 3.0,
                leaf_pages: 840.0,
                matched_rows: 300_000.0,
                table_rows: 7_500_000.0,
                table_pages: 217_000.0,
                output_width: 400.0,
                correlation: 0.5,
                mrr_chunk_rows: Some(super::super::PLANNING_MRR_WINDOW_ROWS),
            },
        );
        assert!(range_mrr.logical_heap_probes < batched.logical_heap_page_visits);
        assert!(range_mrr.cost.total < batched.cost.total);
    }

    #[test]
    fn batched_index_lookup_pays_misses_when_table_dwarfs_pool() {
        // TPC-H Q3 sf=5 shape: ~740k probe keys, ~1.5M matched rows,
        // 270k-page heap vs a 65k-page pool. The probes sweep nearly
        // every heap page without prefetch, so the lookup leg must
        // price close to (not under a third of) a cold sequential
        // sweep — otherwise INLJ beats hash join on cost while running
        // 2.6× slower cold. With the pool covering the whole table the
        // same probe must stay cheap (sf=1 calibration preserved).
        let mut cold = ctx();
        cold.effective_cache_pages = 65_536;
        let table_pages = 270_000.0;
        let lookup = batched_index_lookup(
            &cold,
            3.0,
            4_000.0,
            1_500_000.0,
            740_000.0,
            table_pages,
            40.0,
            1.0,
        );
        let full_sweep = page_io_cost(
            AccessKind::OrderedNoPrefetch,
            table_pages,
            cold.effective_cache_pages as f64,
            table_pages,
        );
        assert!(
            lookup.total * 3.0 > full_sweep,
            "cold batched lookup under-priced: lookup={} sweep={}",
            lookup.total,
            full_sweep
        );

        let mut warm = ctx();
        warm.effective_cache_pages = 400_000;
        let warm_lookup = batched_index_lookup(
            &warm,
            3.0,
            4_000.0,
            1_500_000.0,
            740_000.0,
            table_pages,
            40.0,
            1.0,
        );
        assert!(
            warm_lookup.total * 2.0 < lookup.total,
            "warm batched lookup should remain materially cheaper: warm={} cold={}",
            warm_lookup.total,
            lookup.total
        );
    }

    #[test]
    fn batched_index_lookup_charges_full_heap_probe_cpu() {
        let c = ctx();
        let estimate = estimate_batched_index_lookup(
            &c,
            BatchedIndexLookupInput {
                index_height: 3.0,
                leaf_pages: 4_000.0,
                matched_rows: 2_000_000.0,
                probe_keys: 500_000.0,
                table_pages: 250_000.0,
                output_width: 96.0,
                correlation: 0.0,
                competing_pages: 100_000.0,
            },
        );
        let heap_cpu_floor =
            estimate.logical_heap_page_visits * c.model.cpu_tuple_cost * HEAP_PROBE_CPU_TUPLES;
        assert!(
            estimate.cost.cpu_units >= heap_cpu_floor,
            "cpu={} heap_probe_floor={heap_cpu_floor}",
            estimate.cost.cpu_units
        );
    }

    #[test]
    fn batched_index_lookup_correlated_not_pricier_than_scattered() {
        let c = ctx();
        let clustered =
            batched_index_lookup(&c, 3.0, 840.0, 300_000.0, 75_000.0, 217_000.0, 400.0, 1.0);
        let scattered =
            batched_index_lookup(&c, 3.0, 840.0, 300_000.0, 75_000.0, 217_000.0, 400.0, 0.0);
        assert!(clustered.total <= scattered.total);
    }

    #[test]
    fn batched_index_lookup_reports_stable_page_geometry_and_outer_pressure() {
        let c = ctx();
        let base = BatchedIndexLookupInput {
            index_height: 3.0,
            leaf_pages: 1_200.0,
            matched_rows: 280_000.0,
            probe_keys: 57_000.0,
            table_pages: 54_000.0,
            output_width: 48.0,
            correlation: 0.5,
            competing_pages: 0.0,
        };
        let isolated = estimate_batched_index_lookup(&c, base);
        let pressured = estimate_batched_index_lookup(
            &c,
            BatchedIndexLookupInput {
                competing_pages: 30_000.0,
                ..base
            },
        );
        assert!(isolated.distinct_heap_pages > 0.0);
        assert!(isolated.distinct_heap_pages <= base.table_pages);
        assert!(
            isolated.logical_heap_page_visits >= isolated.distinct_heap_pages,
            "chunk-local page visits must include the global page population"
        );
        assert_eq!(isolated.distinct_heap_pages, pressured.distinct_heap_pages);
        assert_eq!(
            isolated.logical_heap_page_visits,
            pressured.logical_heap_page_visits
        );
        assert!(pressured.working_set_pages > isolated.working_set_pages);
        assert!(pressured.cost.total >= isolated.cost.total);
        assert!(pressured.probe_batches >= 4.0);
    }

    #[test]
    fn index_nlj_scales_with_outer_rows() {
        let outer = small_cost(50.0);
        let per_lookup = Cost {
            total: 1.5,
            ..Cost::ZERO
        };
        let small = index_nested_loop_join(&outer, &per_lookup, 5.0, 5.0, 32.0);
        let big = index_nested_loop_join(&outer, &per_lookup, 500.0, 500.0, 32.0);
        assert!(big.total > small.total);
    }

    #[test]
    fn sort_cost_grows_with_n_log_n() {
        let input = small_cost(1000.0);
        let small = sort(&ctx(), &input, 100.0, 16.0);
        let big = sort(&ctx(), &input, 100_000.0, 16.0);
        assert!(big.total > small.total * 100.0); // n log n grows faster than linear
    }

    #[test]
    fn topk_cheaper_than_full_sort_for_large_n() {
        let input = small_cost(1_000_000.0);
        let s = sort(&ctx(), &input, 1_000_000.0, 16.0);
        let t = top_k(&input, 1_000_000.0, 100.0, 16.0);
        assert!(t.total < s.total);
    }

    #[test]
    fn hash_aggregate_emits_group_count_rows() {
        let input = small_cost(10_000.0);
        let c = hash_aggregate(&ctx(), &input, 10_000.0, 50.0, 32.0);
        assert!(c.total >= input.total);
    }

    #[test]
    fn streaming_aggregate_uses_constant_memory() {
        let input = small_cost(10_000.0);
        let c = streaming_aggregate(&input, 10_000.0, 50.0, 32.0);
        // Memory bounded — single group accumulator, not 50.
        assert!(c.mem_bytes < 1024.0);
    }

    #[test]
    fn limit_clips_downstream_cost() {
        let input = Cost {
            startup: 0.0,
            total: 100.0,
            ..Cost::ZERO
        };
        let c = limit(&input, 10_000.0, 10.0, 0.0);
        // 10/10000 = 0.1% of input cost.
        assert!(c.total < input.total / 10.0);
    }

    #[test]
    fn limit_cost_is_bounded_by_input() {
        let input = small_cost(10_000.0);
        let c = limit(&input, 10_000.0, 50.0, 0.0);
        assert!(c.total <= input.total);
    }

    #[test]
    fn projection_passes_through_io() {
        let input = Cost {
            total: 10.0,
            io_seq_pages: 5.0,
            io_rnd_pages: 3.0,
            cpu_units: 2.0,
            ..Cost::ZERO
        };
        let c = projection(&input, 100.0, 16.0);
        assert_eq!(c.io_seq_pages, 5.0);
        assert_eq!(c.io_rnd_pages, 3.0);
        assert!(c.cpu_units > input.cpu_units); // adds projection CPU
    }

    #[test]
    fn window_agg_scales_with_frame_size() {
        let input = small_cost(1000.0);
        let cumulative = window_agg(&input, 1000.0, 1.0, 16.0);
        let sliding = window_agg(&input, 1000.0, 100.0, 16.0);
        assert!(sliding.total > cumulative.total);
    }

    #[test]
    fn estimated_row_width_includes_columns() {
        use crate::storage::DataTypeKind;
        let mut schema = super::super::test_support::table_schema(&[
            ("a", DataTypeKind::Int),
            ("b", DataTypeKind::BigInt),
        ]);
        schema.database = "t".to_string();
        // Header (16) + Int (4) + BigInt (8) = 28.
        assert_eq!(estimated_row_width(&schema), 28.0);
    }
}
