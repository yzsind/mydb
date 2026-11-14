//! Shared native IVF training parameters, sampling and numeric k-means.
use super::centroids::{assignment_threads, PreparedCentroids, RowRef, EXACT};
use super::distance::{self, Metric};

/// IVF training parameters. Query effort is resolved from live session, hint
/// and per-index metadata by `session::effective_nprobe`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct IvfParams {
    /// Number of inverted lists (k-means centroids). `0` = auto (`√N`, clamped).
    pub nlist: usize,
    /// k-means iterations at build/retrain.
    pub kmeans_iters: usize,
    /// Training sample size multiplier: sample `min(N, mult·nlist)` rows.
    pub train_sample_mult: usize,
}

impl Default for IvfParams {
    fn default() -> Self {
        IvfParams {
            nlist: 0,
            kmeans_iters: 15,
            train_sample_mult: 256,
        }
    }
}

/// Index of the nearest centroid to `v` under `metric`.
pub(super) fn nearest_centroid(
    centroids: &[f32],
    nlist: usize,
    dim: usize,
    v: &[f32],
    metric: Metric,
) -> usize {
    let mut best = 0usize;
    let mut best_d = f64::INFINITY;
    let mut start = 0;
    if metric == Metric::L2 && dim >= 32 {
        while start + 4 <= nlist {
            let scores = distance::l2_sq_batch4(v, &centroids[start * dim..(start + 4) * dim], dim);
            for (offset, d) in scores.into_iter().enumerate() {
                let d = if d.is_normal() {
                    d as f64
                } else {
                    let c = start + offset;
                    distance::ordering_distance(v, &centroids[c * dim..(c + 1) * dim], Metric::L2)
                };
                if d < best_d {
                    best_d = d;
                    best = start + offset;
                }
            }
            start += 4;
        }
    }
    for c in start..nlist {
        let cs = &centroids[c * dim..c * dim + dim];
        let d = distance::ordering_distance(v, cs, metric);
        if d < best_d {
            best_d = d;
            best = c;
        }
    }
    best
}

/// Evenly-strided indices `0..n`, at most `target` of them (used to sample the
/// training set without allocating a shuffle; `Math.random` is unavailable in
/// this environment and a deterministic stride keeps builds reproducible).
pub(super) fn strided_sample(n: usize, target: usize) -> Vec<usize> {
    if target >= n {
        return (0..n).collect();
    }
    let step = (n as f64 / target as f64).max(1.0);
    let mut out = Vec::with_capacity(target);
    let mut acc = 0.0f64;
    while (acc as usize) < n && out.len() < target {
        out.push(acc as usize);
        acc += step;
    }
    out
}

/// Deterministic pseudo-random sample of `target` distinct indices from `0..n`,
/// for training the coarse quantizer. Unlike [`strided_sample`], the picks are
/// decorrelated from heap insertion order, so a table physically sorted by some
/// vector dimension does not yield a systematically biased training set (which
/// would place centroids poorly and depress recall). Deterministic (an LCG
/// seeded from `n`, no `Math.random`) so builds stay reproducible and the paged
/// backend remains a bit-for-bit differential oracle of the memory backend.
pub(crate) fn training_sample(n: usize, target: usize) -> Vec<usize> {
    if target >= n {
        return (0..n).collect();
    }
    // SplitMix64-style LCG seeded from `n`; reject-sample distinct indices into
    // a set. `target < n` here, and training targets are modest (≈256·√N), so
    // the birthday-collision reject loop terminates quickly.
    let mut state = (n as u64)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(1);
    let mut next = || {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    };
    let mut seen = std::collections::HashSet::with_capacity(target);
    let mut out = Vec::with_capacity(target);
    // Bound the loop defensively so a pathological seed can't spin; fall back to
    // filling any shortfall with a strided pass.
    let mut guard = target.saturating_mul(8).max(64);
    while out.len() < target && guard > 0 {
        guard -= 1;
        let i = (next() % n as u64) as usize;
        if seen.insert(i) {
            out.push(i);
        }
    }
    if out.len() < target {
        for i in strided_sample(n, target) {
            if out.len() >= target {
                break;
            }
            if seen.insert(i) {
                out.push(i);
            }
        }
    }
    out
}

/// Lloyd's k-means. `samples` are the (already normalized, for Cosine) training
/// vectors; returns `k·dim` row-major centroids. Empty clusters are reseeded to
/// a strided sample so `nlist` lists always exist. For Cosine, centroids are
/// re-normalized each iteration (spherical k-means).
///
/// The assign step ranks centroids with the blocked kernel of
/// [`PreparedCentroids`]; rows (or centroid sets) outside its f32 key domain
/// use the exact [`nearest_centroid`]. Both steps fan out over cores and are
/// deterministic: the update sums each cluster's members in sample order, the
/// same order as a serial pass, so `MYDB_IVF_BUILD_THREADS=1` reproduces the
/// parallel result bit for bit.
pub(super) fn kmeans<V: AsRef<[f32]> + Sync>(
    samples: &[V],
    k: usize,
    dim: usize,
    iters: usize,
    cosine: bool,
) -> Vec<f32> {
    let threads = assignment_threads(samples.len(), k, dim);
    kmeans_with_threads(samples, k, dim, iters, cosine, threads)
}

fn kmeans_with_threads<V: AsRef<[f32]> + Sync>(
    samples: &[V],
    k: usize,
    dim: usize,
    iters: usize,
    cosine: bool,
    threads: usize,
) -> Vec<f32> {
    let n = samples.len();
    let k = k.clamp(1, n.max(1));
    let mut centroids = vec![0f32; k * dim];
    // Init: strided distinct samples.
    let init = strided_sample(n, k);
    for (c, &si) in init.iter().enumerate() {
        centroids[c * dim..c * dim + dim].copy_from_slice(samples[si].as_ref());
    }
    if n == 0 {
        return centroids;
    }
    let metric = if cosine { Metric::Cosine } else { Metric::L2 };
    // Use an invalid sentinel so the first assignment pass always counts as a
    // change.  Initialising to cluster 0 can otherwise terminate after one
    // update when all initial samples happen to prefer cluster 0, even though
    // the empty-cluster reseed has just changed the centroids.
    let mut assign = vec![u32::MAX; n];
    let mut next = vec![0u32; n];
    let timing = std::env::var_os("MYDB_HNSW_BUILD_TIMING").is_some();
    let (mut assign_time, mut update_time, mut rounds) =
        (std::time::Duration::ZERO, std::time::Duration::ZERO, 0usize);
    let rows: Option<Vec<RowRef<'_>>> = samples
        .iter()
        .map(|s| (s.as_ref().len() == dim).then(|| RowRef::from_f32(s.as_ref())))
        .collect();
    for _ in 0..iters.max(1) {
        rounds += 1;
        let started = std::time::Instant::now();
        // Assign.
        let prepared = rows.as_ref().and_then(|rows| {
            PreparedCentroids::new(centroids.clone(), dim, metric).zip(Some(rows))
        });
        if let Some((prepared, rows)) = prepared {
            prepared.assign_parallel(rows, &mut next, threads);
            for (i, slot) in next.iter_mut().enumerate() {
                if *slot == EXACT {
                    *slot =
                        nearest_centroid(&centroids, k, dim, samples[i].as_ref(), metric) as u32;
                }
            }
        } else {
            kmeans_assign_exact(samples, &centroids, k, dim, metric, &mut next, threads);
        }
        let changed = next != assign;
        std::mem::swap(&mut assign, &mut next);
        let assigned = std::time::Instant::now();
        assign_time += assigned - started;
        // Update: mean of assigned samples.
        kmeans_update(samples, &assign, &mut centroids, k, dim, cosine, threads);
        update_time += assigned.elapsed();
        if !changed {
            break;
        }
    }
    if timing {
        eprintln!(
            "[ivf] kmeans n={n} k={k} dim={dim} threads={threads} rounds={rounds} assign_ms={} update_ms={}",
            assign_time.as_millis(),
            update_time.as_millis()
        );
    }
    centroids
}

/// Exact assign step (the oracle for exceptional domains): each sample to its
/// nearest centroid by [`nearest_centroid`], over disjoint index ranges.
fn kmeans_assign_exact<V: AsRef<[f32]> + Sync>(
    samples: &[V],
    centroids: &[f32],
    k: usize,
    dim: usize,
    metric: Metric,
    assign: &mut [u32],
    threads: usize,
) {
    let chunk = samples.len().div_ceil(threads.max(1)).max(1);
    crate::util::thread_spawn::scope(|scope| {
        for (index, slice) in assign.chunks_mut(chunk).enumerate() {
            let start = index * chunk;
            scope.spawn(move || {
                for (j, a) in slice.iter_mut().enumerate() {
                    *a = nearest_centroid(centroids, k, dim, samples[start + j].as_ref(), metric)
                        as u32;
                }
            });
        }
    });
}

/// Update step: every centroid becomes the f64 mean of its members, summed in
/// sample order (identical to one serial pass over the samples). Empty
/// clusters are reseeded; Cosine centroids are re-normalized.
fn kmeans_update<V: AsRef<[f32]> + Sync>(
    samples: &[V],
    assign: &[u32],
    centroids: &mut [f32],
    k: usize,
    dim: usize,
    cosine: bool,
    threads: usize,
) {
    let n = samples.len();
    // Counting sort of sample indices by cluster keeps sample order per cluster.
    let mut offsets = vec![0usize; k + 1];
    for &c in assign {
        offsets[c as usize + 1] += 1;
    }
    for c in 0..k {
        offsets[c + 1] += offsets[c];
    }
    let mut cursor = offsets.clone();
    let mut members = vec![0u32; n];
    for (i, &c) in assign.iter().enumerate() {
        members[cursor[c as usize]] = i as u32;
        cursor[c as usize] += 1;
    }
    let (offsets, members) = (&offsets, &members);
    let update = |first: usize, out: &mut [f32]| {
        let mut sums = vec![0f64; dim];
        for (local, center) in out.chunks_exact_mut(dim).enumerate() {
            let c = first + local;
            let cluster = &members[offsets[c]..offsets[c + 1]];
            if cluster.is_empty() {
                // Empty cluster: reseed to a strided sample to keep nlist cells.
                let si = (c * 2654435761usize) % n;
                center.copy_from_slice(samples[si].as_ref());
                continue;
            }
            sums.fill(0.0);
            for &i in cluster {
                for (a, &x) in sums.iter_mut().zip(samples[i as usize].as_ref().iter()) {
                    *a += x as f64;
                }
            }
            let inv = 1.0 / cluster.len() as f64;
            for (x, &sum) in center.iter_mut().zip(&sums) {
                *x = (sum * inv) as f32;
            }
            if cosine {
                let norm: f32 = center.iter().map(|x| x * x).sum::<f32>().sqrt();
                if norm > 0.0 {
                    for x in center.iter_mut() {
                        *x /= norm;
                    }
                }
            }
        }
    };
    let per_thread = k.div_ceil(threads.max(1)).max(1);
    if threads <= 1 || k < 2 {
        return update(0, centroids);
    }
    crate::util::thread_spawn::scope(|scope| {
        for (index, out) in centroids.chunks_mut(per_thread * dim).enumerate() {
            let update = &update;
            scope.spawn(move || update(index * per_thread, out));
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::RowId;
    #[test]
    fn nearest_centroid_batch_matches_single_row_oracle() {
        for dim in [1, 31, 32, 65, 768] {
            let data = rows(20, dim, 9123);
            for nlist in [1, 3, 4, 5, 17] {
                let centroids: Vec<f32> = data[..nlist]
                    .iter()
                    .flat_map(|(_, v)| v.iter().copied())
                    .collect();
                for metric in [Metric::L2, Metric::Cosine, Metric::Dot] {
                    for (_, query) in &data {
                        let mut best = 0;
                        let mut score = f64::INFINITY;
                        for c in 0..nlist {
                            let d = distance::ordering_distance(
                                query,
                                &centroids[c * dim..(c + 1) * dim],
                                metric,
                            );
                            if d < score {
                                best = c;
                                score = d;
                            }
                        }
                        assert_eq!(
                            nearest_centroid(&centroids, nlist, dim, query, metric),
                            best
                        );
                    }
                }
            }
            // Large BFLOAT16 exponents overflow the float32 batch accumulator;
            // tiny ones underflow. Both must retain the wide scalar ordering.
            for scale in [1e-30f32, 1e30f32] {
                let query = vec![scale; dim];
                let centroids: Vec<f32> = [4.0, 3.0, 1.5, -2.0, 2.0]
                    .into_iter()
                    .flat_map(|x| vec![x * scale; dim])
                    .collect();
                assert_eq!(nearest_centroid(&centroids, 5, dim, &query, Metric::L2), 2);
            }
            let tied = vec![0.0; 5 * dim];
            assert_eq!(nearest_centroid(&tied, 5, dim, &data[0].1, Metric::L2), 0);
        }
    }

    #[test]
    fn kmeans_is_thread_count_independent_and_converges() {
        for (dim, cosine) in [(768, true), (65, false), (3, true)] {
            let data: Vec<Vec<f32>> = rows(3000, dim, 41)
                .into_iter()
                .map(|(_, mut v)| {
                    if cosine {
                        distance::normalize_in_place(&mut v);
                    }
                    v
                })
                .collect();
            let serial = kmeans_with_threads(&data, 24, dim, 6, cosine, 1);
            for threads in [2, 5, 16] {
                let parallel = kmeans_with_threads(&data, 24, dim, 6, cosine, threads);
                assert_eq!(
                    serial.iter().map(|x| x.to_bits()).collect::<Vec<_>>(),
                    parallel.iter().map(|x| x.to_bits()).collect::<Vec<_>>(),
                    "dim={dim} threads={threads}"
                );
            }
            // Lloyd iterations never increase the inertia.
            let metric = if cosine { Metric::Cosine } else { Metric::L2 };
            let inertia = |centers: &[f32]| -> f64 {
                data.iter()
                    .map(|v| {
                        let c = nearest_centroid(centers, 24, dim, v, metric);
                        distance::ordering_distance(v, &centers[c * dim..(c + 1) * dim], metric)
                    })
                    .sum()
            };
            let first = kmeans_with_threads(&data, 24, dim, 1, cosine, 4);
            assert!(
                inertia(&serial) <= inertia(&first) * (1.0 + 1e-6),
                "dim={dim}"
            );
        }
        // Exceptional magnitudes take the exact assign path and stay finite.
        let huge: Vec<Vec<f32>> = (0..64)
            .map(|i| vec![(i % 4) as f32 * 1e20 + 1e19; 8])
            .collect();
        let centers = kmeans_with_threads(&huge, 4, 8, 5, false, 3);
        assert!(centers.iter().all(|x| x.is_finite()));
    }

    #[test]
    fn training_sample_is_distinct_and_decorrelated() {
        // C4: distinct indices, correct count, and — unlike the systematic
        // stride — not the arithmetic sequence `0, step, 2·step, …` (so a
        // heap physically ordered by a vector dimension is not sampled with a
        // fixed period). Deterministic across calls (reproducible builds).
        let n = 10_000;
        let target = 500;
        let s1 = training_sample(n, target);
        assert_eq!(s1.len(), target, "must return exactly `target` indices");
        let uniq: std::collections::HashSet<usize> = s1.iter().copied().collect();
        assert_eq!(uniq.len(), target, "indices must be distinct");
        assert!(s1.iter().all(|&i| i < n), "indices in range");
        // Reproducible.
        assert_eq!(s1, training_sample(n, target), "must be deterministic");
        // Not the strided arithmetic sequence.
        let strided = strided_sample(n, target);
        assert_ne!(s1, strided, "should not equal the systematic stride");
        // target >= n degenerates to the full set (same as strided).
        assert_eq!(training_sample(50, 100), (0..50).collect::<Vec<_>>());
    }

    fn rows(n: usize, dim: usize, seed: u64) -> Vec<(RowId, Vec<f32>)> {
        // Deterministic pseudo-random clustered vectors (no Math.random here).
        // 16 well-separated random cluster centers in R^dim + per-dim noise, so
        // clusters are not collinear (a realistic ANN workload).
        let mut s = seed;
        let mut next = || {
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((s >> 33) as f32 / (1u64 << 31) as f32) - 1.0
        };
        let n_clusters = 16;
        let centers: Vec<Vec<f32>> = (0..n_clusters)
            .map(|_| (0..dim).map(|_| next() * 5.0).collect())
            .collect();
        (0..n)
            .map(|i| {
                let c = &centers[i % n_clusters];
                let v: Vec<f32> = c.iter().map(|&x| x + next() * 0.3).collect();
                (RowId::new((i + 1) as u32, 0), v)
            })
            .collect()
    }
}
