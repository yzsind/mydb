//! IVF_SQ8 bug-audit regression tests (2026-09-25 audit, doc_old/ivf-sq8-bugs-audit.md).
//! The paged-backend audit cases live in `storage/core/ivf_page.rs`
//! (`audit_bug1`/`audit_bug3`/`audit_bug5`) next to the store tests they need;
//! this module holds the memory-backend case.

use super::ivf::{IvfParams, IvfSq8Index};
use std::sync::Arc;

fn gen(n: usize, dim: usize, seed: u64) -> Vec<(crate::storage::RowId, Vec<f32>)> {
    let mut s = seed;
    let mut next = || {
        s = s
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((s >> 33) as f32 / (1u64 << 31) as f32) - 1.0
    };
    (0..n)
        .map(|i| {
            (
                crate::storage::RowId {
                    page_id: i as u32,
                    slot_id: 0,
                },
                (0..dim).map(|_| next()).collect(),
            )
        })
        .collect()
}

/// B7 regression: a real compaction must preserve visible candidates while
/// searches run concurrently. Deterministic lock/progress and concurrent-DML
/// publication tests live next to MemoryIvf in ivf.rs.
#[test]
fn audit_bug7_memory_compact_preserves_concurrent_search_results() {
    let rows = gen(40_000, 128, 0xBEEF);
    let ivf = IvfSq8Index::new(
        super::distance::Metric::L2,
        IvfParams {
            nlist: 16,
            ..Default::default()
        },
    );
    ivf.build_from_rows(&rows).expect("build");
    for i in (0..rows.len()).step_by(2) {
        ivf.mark_deleted_by_row_id(rows[i].0);
    }
    assert_eq!(ivf.stale_count(), 20_000);

    // The original audit accidentally compacted twice without recreating
    // tombstones: its second compact was a no-op. Exercise a real rebuild.
    let idx = Arc::new(ivf);
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let searcher_idx = Arc::clone(&idx);
    let ready = barrier.clone();
    let searcher = std::thread::spawn(move || {
        ready.wait();
        for _ in 0..32 {
            let got = searcher_idx
                .try_search_result(&vec![0.25; 128], 10, 8)
                .unwrap();
            assert_eq!(got.candidates.len(), 10);
            assert!(got.candidates.iter().all(|(rid, _)| rid.page_id % 2 == 1));
        }
    });
    barrier.wait();
    assert_eq!(idx.compact(), 20_000);
    searcher.join().unwrap();
    assert_eq!(idx.stale_count(), 0);
    assert_eq!(idx.compact(), 0);
}
