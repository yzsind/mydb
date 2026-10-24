//! HNSW audit regression tests (2026-09).
//!
//! Each test isolates one defect found in the 2026-09 source audit of
//! `engine/vector`. They are named `audit_bugN_*` and mirror the numbered
//! findings in `doc_old/hnsw-bugs-audit.md`. Env-var toggles are safe here:
//! `MYDB_HNSW_QUANTIZE` is read per-call under `cfg(test)` and cargo-nextest
//! runs every test in its own process.

use super::distance::Metric;
use super::hnsw::{HnswIndex, DEFAULT_EF_CONSTRUCTION, DEFAULT_M};
use crate::storage::RowId;

fn rid(n: u32) -> RowId {
    RowId {
        page_id: n,
        slot_id: (n % 7) as u16,
    }
}

/// Deterministic pseudo-random vectors in [-1, 1].
fn gen_vectors(n: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut out = Vec::with_capacity(n);
    let mut state = seed;
    for _ in 0..n {
        let mut v = Vec::with_capacity(dim);
        for _ in 0..dim {
            state = state
                .wrapping_mul(0x5851_F42D_4C95_7F2D)
                .wrapping_add(0x1405_7B7E_F767_814F);
            let u = (state >> 11) as f64 / (1u64 << 53) as f64;
            v.push((u as f32) * 2.0 - 1.0);
        }
        out.push(v);
    }
    out
}

fn set_quant(on: bool) {
    // SAFETY: process-wide env write; nextest gives every test its own process.
    unsafe {
        if on {
            std::env::set_var("MYDB_HNSW_QUANTIZE", "1");
        } else {
            std::env::set_var("MYDB_HNSW_QUANTIZE", "0");
        }
    }
}

fn brute_topk(vectors: &[Vec<f32>], query: &[f32], k: usize) -> Vec<usize> {
    let mut scored: Vec<(f32, usize)> = vectors
        .iter()
        .enumerate()
        .map(|(i, v)| (super::distance::ordering_distance(query, v, Metric::L2), i))
        .collect();
    scored.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    scored.into_iter().take(k).map(|(_, i)| i).collect()
}

/// Regression for BUG-1: quantized ties must not discard an exact top-k
/// member inside the ef pool. Five decoys precede the true nearest neighbor
/// in SQ8 order; exact navigation and SQ8 full-pool re-ranking agree.
#[test]
fn audit_bug1_search_rerank_truncates_exact_topk() {
    let dim = 8usize;
    let n_random = 90usize;
    let n_decoy = 5usize;
    let n = n_random + n_decoy + 1;

    let mut vectors = gen_vectors(n_random, dim, 0xDEB1);
    // Decoys: radius 0.002 → every coordinate quantizes to code 0 (the arena
    // spans [-1,1], so one code step ≈ 0.0079), SSD=0 — but exact distance
    // 4e-6 ≫ the target's 1e-6. Ids 90..94.
    for i in 0..n_decoy {
        let mut v = vec![0.0f32; dim];
        v[i % dim] = 0.002;
        if i >= dim {
            v[(i * 3) % dim] = 0.002;
        }
        vectors.push(v);
    }
    // Target: the true nearest neighbour, inserted last (id 95).
    let mut target = vec![0.0f32; dim];
    target[0] = 0.001;
    vectors.push(target);
    let query = vec![0.0f32; dim];

    let truth = brute_topk(&vectors, &query, n_decoy + 1);
    assert_eq!(
        truth.first(),
        Some(&95),
        "construction: id 95 is the true NN"
    );

    let build = || {
        let index = HnswIndex::new(Metric::L2, DEFAULT_M, DEFAULT_EF_CONSTRUCTION);
        for (i, v) in vectors.iter().enumerate() {
            index.insert(rid(i as u32), v.clone()).unwrap();
        }
        index
    };

    // Control: exact navigation re-ranks by exact distance throughout.
    set_quant(false);
    let exact_top1 = build().search(&query, 1, n);
    assert_eq!(
        exact_top1.first().map(|(r, _)| r.page_id),
        Some(95),
        "control: exact navigation must return the true NN"
    );

    // SQ8 navigation with k=1: the SSD=0 ties sort by id, so the target sits
    // behind the five decoys; re-ranking must still recover it.
    set_quant(true);
    let sq8_top1 = build().search(&query, 1, n);
    assert_eq!(
        sq8_top1.first().map(|(r, _)| r.page_id),
        Some(95),
        "SQ8 must re-rank the full candidate pool"
    );

    // Same graph, same effort, k = n: re-ranking the FULL pool recovers the
    // target — it was inside the explored pool and was lost to the first-k
    // truncation, not to navigation.
    let sq8_full = build()
        .search(&query, n, n)
        .into_iter()
        .map(|(r, _)| r.page_id)
        .collect::<Vec<_>>();
    assert!(
        sq8_full.contains(&95),
        "bug1: true NN missing even from the full re-ranked pool"
    );
}

/// Regression for BUG-2: an in-place UPDATE immediately supersedes old SQ8
/// codes, including while an immutable arena is still published.
#[test]
fn audit_bug2_inplace_update_returns_updated_neighbor() {
    let dim = 4usize;
    let n_background = 95usize;
    let n = n_background + 1;

    // Background in [950, 1050]; the target starts at 1000 (code ≈ 104, far
    // from the query's clamped -127 codes, so its stale SQ8 distance is large).
    let mut vectors = gen_vectors(n_background, dim, 0x5EA4);
    for v in vectors.iter_mut() {
        for x in v.iter_mut() {
            *x = *x * 50.0 + 1000.0;
        }
    }
    let initial_target = vec![1000.0f32; dim];
    vectors.push(initial_target);

    let index = HnswIndex::new(Metric::L2, DEFAULT_M, DEFAULT_EF_CONSTRUCTION);
    for (i, v) in vectors.iter().enumerate() {
        index.insert(rid(i as u32), v.clone()).unwrap();
    }
    let query = vec![0.0f32; dim];
    // Warm the SQ8 arena over the pre-update vectors.
    set_quant(true);
    let _ = index.search(&query, 5, n);

    // Drift-adaptive in-place update: move the row onto the query. The ratio
    // is caller-supplied, so 1e9 forces the in-place path deterministically.
    let updated = vec![0.0f32; dim];
    assert!(
        index.try_update_in_place(rid(n_background as u32), &updated, 1e9),
        "construction: the update must take the in-place path"
    );

    // The updated row is now the true nearest neighbour.
    let mut with_update = vectors.clone();
    with_update[n_background] = updated.clone();
    let truth = brute_topk(&with_update, &query, 1);
    assert_eq!(truth, vec![n_background]);

    // The updated row must surface without restarting or compacting.
    let sq8_top5 = index
        .search(&query, 5, n)
        .into_iter()
        .map(|(r, _)| r.page_id as usize)
        .collect::<Vec<_>>();
    assert!(
        sq8_top5.contains(&n_background),
        "updated row missing: {n_background} still surfaced in {sq8_top5:?}"
    );

    // Exact navigation (no arena) surfaces it — the graph edges are intact;
    // only the stale quantized code hid the row.
    set_quant(false);
    let exact_top5 = index
        .search(&query, 5, n)
        .into_iter()
        .map(|(r, _)| r.page_id as usize)
        .collect::<Vec<_>>();
    assert!(
        exact_top5.contains(&n_background),
        "control: exact navigation must surface the updated row, got {exact_top5:?}"
    );
}
