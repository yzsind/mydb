//! Differential: IVF navigator graph quality — serial vs multi-threaded
//! `link_all`. The paged IVF navigator (`ivf_paged_nav`) builds an M=8 HNSW
//! over the table and *replaces* the geometric IVF list scan whenever its
//! candidate pool is non-empty. Server-side differentials (doc_old/ivf-sq8-bugs-audit.md,
//! bug 8) show the navigator returning single-digit-percent top-10 overlap with
//! exact brute force while the plain IVF list scan scores ~90% on the same
//! workload — and the graph quality flips nondeterministically across
//! restarts. These tests pin the build-side half of that contract: parallel
//! `link_all` must produce a graph whose navigation recall matches the serial
//! build (the oracle).

#[cfg(test)]
mod nav_recall_tests {
    use crate::engine::vector::distance::Metric;
    use crate::engine::vector::hnsw::HnswIndex;
    use crate::storage::RowId;

    fn gen(n: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
        // Clustered vectors: 16 separated centers + noise (realistic ANN shape).
        let mut s = seed;
        let mut next = || {
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((s >> 33) as f32 / (1u64 << 31) as f32) - 1.0
        };
        let centers: Vec<Vec<f32>> = (0..16)
            .map(|_| (0..dim).map(|_| next() * 5.0).collect())
            .collect();
        (0..n)
            .map(|i| centers[i % 16].iter().map(|&x| x + next() * 0.3).collect())
            .collect()
    }

    fn build(n: usize, dim: usize, threads: usize) -> HnswIndex {
        let vecs = gen(n, dim, 0x5EED);
        let g = HnswIndex::new(Metric::L2, 8, 64); // the navigator's M / efC
        for (i, v) in vecs.iter().enumerate() {
            g.append_node(
                RowId {
                    page_id: i as u32,
                    slot_id: 0,
                },
                v.clone(),
            )
            .unwrap();
        }
        g.link_all(threads);
        g
    }

    fn recall(g: &HnswIndex, vecs: &[Vec<f32>], queries: usize, k: usize, ef: usize) -> f64 {
        let mut hits = 0usize;
        let mut total = 0usize;
        for qi in (0..vecs.len()).step_by(vecs.len() / queries) {
            let q = &vecs[qi];
            let got: Vec<u32> = g
                .search(q, k, ef)
                .into_iter()
                .map(|(r, _)| r.page_id)
                .collect();
            let mut truth: Vec<(f32, u32)> = vecs
                .iter()
                .enumerate()
                .map(|(i, v)| (crate::engine::vector::distance::l2_sq(q, v), i as u32))
                .collect();
            truth.sort_by(|a, b| a.0.total_cmp(&b.0));
            let top: Vec<u32> = truth.into_iter().take(k).map(|(_, i)| i).collect();
            hits += top.iter().filter(|t| got.contains(t)).count();
            total += k;
        }
        hits as f64 / total as f64
    }

    /// The navigator's operating point: M=8, ef = max(k·4, 2·nprobe+16) = 40
    /// for the default k=10 / nprobe=8 query. Whatever the thread count, the
    /// published graph must navigate — a near-random candidate pool is the
    /// server-side "navigator ≈ 3% overlap" symptom seen in the SQL audit.
    #[test]
    fn nav_graph_parallel_link_recall_matches_serial() {
        let n = 2_048;
        let dim = 48;
        let vecs = gen(n, dim, 0x5EED);
        let serial = build(n, dim, 1);
        let par = build(n, dim, 4);
        let r_serial = recall(&serial, &vecs, 20, 10, 40);
        let r_par = recall(&par, &vecs, 20, 10, 40);
        eprintln!("nav recall@10 ef=40: serial={r_serial:.2} parallel={r_par:.2}");
        assert!(
            r_serial > 0.7,
            "serial navigator graph should navigate: recall={r_serial:.2}"
        );
        assert!(
            r_par > 0.7,
            "parallel navigator graph collapsed (serial={r_serial:.2}, \
             parallel={r_par:.2}) — audit bug 8 root cause"
        );
    }

    /// Query-shape differential for the server symptom: queries drawn from the
    /// *data* distribution vs out-of-distribution probes. A healthy graph must
    /// stay within a reasonable recall band of the serial oracle for both.
    #[test]
    fn nav_graph_recall_is_stable_across_builds() {
        let n = 2_048;
        let dim = 48;
        // Two independent parallel builds must both navigate (the server
        // flips between "finds the exact row" and "misses it" per restart).
        for build_no in 0..2 {
            let par = build(n, dim, 4 + build_no);
            let vecs = gen(n, dim, 0x5EED);
            let r = recall(&par, &vecs, 20, 10, 40);
            eprintln!("build#{build_no} parallel recall@10 ef=40: {r:.2}");
            assert!(
                r > 0.6,
                "build#{build_no} navigator recall collapsed: {r:.2}"
            );
        }
    }

    /// Full-scale differential mirroring the server-side navigator build
    /// (100k rows × 256-d, M=8, efC=64, multi-threaded link_all, queried at
    /// the navigator's operating point ef=40). Run explicitly:
    /// `cargo nextest run -E 'test(nav_scale)' --run-ignored only` (slow).
    #[test]
    #[ignore]
    fn nav_scale_differential_100k_x_256d() {
        let n = 100_000;
        let dim = 256;
        let mut s = 42u64;
        let mut next = || {
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((s >> 33) as f32 / (1u64 << 31) as f32) - 1.0
        };
        let centers: Vec<Vec<f32>> = (0..32)
            .map(|_| (0..dim).map(|_| next() * 5.0).collect())
            .collect();
        let vecs: Vec<Vec<f32>> = (0..n)
            .map(|i| centers[i % 32].iter().map(|&x| x + next() * 0.3).collect())
            .collect();
        let make = |threads: usize| {
            let g = HnswIndex::new(Metric::L2, 8, 64);
            for (i, v) in vecs.iter().enumerate() {
                g.append_node(
                    RowId {
                        page_id: i as u32,
                        slot_id: 0,
                    },
                    v.clone(),
                )
                .unwrap();
            }
            g.link_all(threads);
            g
        };
        let threads = std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(1)
            .clamp(1, 16);
        let t0 = std::time::Instant::now();
        let par = make(threads);
        eprintln!("nav parallel build (threads={threads}): {:?}", t0.elapsed());
        let t1 = std::time::Instant::now();
        let serial = make(1);
        eprintln!("nav serial build: {:?}", t1.elapsed());
        let r_par = recall(&par, &vecs, 10, 10, 40);
        let r_serial = recall(&serial, &vecs, 10, 10, 40);
        eprintln!("nav recall@10 ef=40 @100kx256d: parallel={r_par:.2} serial={r_serial:.2}");
        // The server wraps the navigator in ReadOnlyHnswIndex (frozen
        // adjacency + vectors) and searches through that wrapper — replicate
        // the exact production call path.
        let frozen = crate::engine::vector::hnsw::ReadOnlyHnswIndex::new(make(threads));
        let mut frozen_hits = 0usize;
        let mut total = 0usize;
        for qi in (0..vecs.len()).step_by(vecs.len() / 10) {
            let q = &vecs[qi];
            let got: Vec<u32> = frozen
                .search(q, 10, 40)
                .into_iter()
                .map(|(r, _)| r.page_id)
                .collect();
            let mut truth: Vec<(f32, u32)> = vecs
                .iter()
                .enumerate()
                .map(|(i, v)| (crate::engine::vector::distance::l2_sq(q, v), i as u32))
                .collect();
            truth.sort_by(|a, b| a.0.total_cmp(&b.0));
            let top: Vec<u32> = truth.into_iter().take(10).map(|(_, i)| i).collect();
            frozen_hits += top.iter().filter(|t| got.contains(t)).count();
            total += 10;
        }
        let r_frozen = frozen_hits as f64 / total as f64;
        eprintln!(
            "nav recall@10 ef=40 @100kx256d: parallel={r_par:.2} serial={r_serial:.2} frozen={r_frozen:.2}"
        );
        assert!(
            r_par >= r_serial - 0.2,
            "parallel navigator graph far worse than serial oracle \
             (parallel={r_par:.2}, serial={r_serial:.2})"
        );
        assert!(
            r_frozen >= 0.5,
            "frozen (ReadOnly) navigator path collapsed: {r_frozen:.2} \
             — matches the server-side ivf_paged_nav recall collapse"
        );
    }
}
