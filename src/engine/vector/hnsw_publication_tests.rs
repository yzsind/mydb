//! Deterministic publication races against the real insert/search entry points.

use super::*;
use crate::engine::vector::{codec::from_f32, cursor::VectorSearchCursor, graph::VectorGraph};
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::time::Duration;

fn row(id: u32) -> RowId {
    RowId {
        page_id: id,
        slot_id: 0,
    }
}

fn vector(id: u32) -> Vec<f32> {
    vec![id as f32 + 1.0, 1.0]
}

fn memory(graph: &VectorGraph) -> &HnswIndex {
    let VectorGraph::Memory(graph) = graph else {
        unreachable!()
    };
    graph
}

fn graph(metric: Metric, nodes: u32) -> Arc<VectorGraph> {
    let graph = HnswIndex::new(metric, 4, 16).without_navigation_quantization();
    // Unlinked but initialized nodes also belong to exhaustive searches.
    for id in 0..nodes {
        graph.append_node(row(id), vector(id)).unwrap();
    }
    if nodes != 0 {
        graph.link_node(0);
    }
    Arc::new(VectorGraph::Memory(graph))
}

fn with_paused_insert(graph: &Arc<VectorGraph>, id: u32, check: impl FnOnce(&HnswIndex)) {
    let (claimed_tx, claimed_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let writer_graph = Arc::clone(graph);
    let writer = crate::util::thread_spawn::spawn(move || {
        NODE_RESERVED_TEST_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |claimed| {
                claimed_tx.send(claimed).unwrap();
                release_rx.recv_timeout(Duration::from_secs(30)).unwrap();
            }));
        });
        writer_graph.insert(row(id), from_f32(&vector(id))).unwrap();
    });
    // Always release and join the writer, including on a reader assertion.
    let result = catch_unwind(AssertUnwindSafe(|| {
        assert_eq!(
            claimed_rx.recv_timeout(Duration::from_secs(30)).unwrap(),
            id
        );
        check(memory(graph));
    }));
    let released = release_tx.send(());
    let joined = writer.join();
    if let Err(panic) = result {
        resume_unwind(panic);
    }
    released.unwrap();
    joined.unwrap();
}

fn assert_rows(mut got: Vec<(RowId, f64)>, expected: impl IntoIterator<Item = u32>) {
    got.sort_unstable_by_key(|(row, _)| *row);
    let ids: Vec<_> = got.into_iter().map(|(row, _)| row.page_id).collect();
    let mut expected: Vec<_> = expected.into_iter().collect();
    expected.sort_unstable();
    assert_eq!(ids, expected);
}

#[test]
fn exhaustive_search_skips_reserved_slots_and_keeps_published_tail() {
    for metric in [Metric::L2, Metric::Cosine, Metric::Dot] {
        for prefix in [0, 1] {
            let graph = graph(metric, prefix);
            with_paused_insert(&graph, prefix, |index| {
                assert_eq!(index.nodes.len(), prefix as usize + 1);
                assert_eq!(index.nodes.published_len(), prefix as usize);
                // Complete a later insert while the earlier slot is a hole.
                graph
                    .insert(row(prefix + 1), from_f32(&vector(prefix + 1)))
                    .unwrap();
                let got = index.search(&vector(prefix + 1), 16, usize::MAX);
                assert_eq!(got.first().unwrap().0, row(prefix + 1));
                assert_rows(got, (0..prefix).chain([prefix + 1]));
            });
            assert_rows(
                memory(&graph).search(&vector(prefix + 1), 16, usize::MAX),
                0..prefix + 2,
            );
        }
    }
}

#[test]
fn exhaustive_cursor_requests_heap_completion_for_publication_holes() {
    for metric in [Metric::L2, Metric::Cosine, Metric::Dot] {
        for prefix in [0, 1] {
            let graph = graph(metric, prefix);
            let query = from_f32(&vector(prefix + 1));
            with_paused_insert(&graph, prefix, |_| {
                graph.insert(row(prefix + 1), query.clone()).unwrap();
                let mut cursor =
                    VectorSearchCursor::new(Arc::clone(&graph), &query, 0, None).unwrap();
                assert!(cursor.next_batch(usize::MAX, 16).unwrap().is_none());
            });
            let mut cursor = VectorSearchCursor::new(Arc::clone(&graph), &query, 0, None).unwrap();
            assert_rows(
                cursor.next_batch(usize::MAX, 16).unwrap().unwrap(),
                0..prefix + 2,
            );
            assert!(cursor.exhausted());
        }
    }
}

#[test]
fn exhaustive_search_handles_reserved_first_slot_of_unallocated_bucket() {
    let count = SLAB_BASE as u32;
    let graph = graph(Metric::L2, count);
    with_paused_insert(&graph, count, |index| {
        assert!(index.nodes.dir[1].load(Ordering::Acquire).is_null());
        assert_rows(index.search(&vector(0), usize::MAX, usize::MAX), 0..count);
        graph
            .insert(row(count + 1), from_f32(&vector(count + 1)))
            .unwrap();
        assert_rows(
            index.search(&vector(0), usize::MAX, usize::MAX),
            (0..count).chain([count + 1]),
        );
        index.mark_deleted_by_row_id(row(count + 1));
        assert_rows(index.search(&vector(0), usize::MAX, usize::MAX), 0..count);
    });
    assert_rows(
        memory(&graph).search(&vector(0), usize::MAX, usize::MAX),
        0..count + 1,
    );
}

#[test]
fn exhaustive_cursor_detects_publication_hole_between_work_slices() {
    let count = 2050;
    let graph = graph(Metric::L2, count);
    let query = from_f32(&vector(0));
    let mut cursor = VectorSearchCursor::new(Arc::clone(&graph), &query, 0, None).unwrap();
    let index = memory(&graph);
    assert!(index.advance_cursor(&mut cursor, usize::MAX).unwrap());
    assert_eq!(cursor.scan_position, 1024);
    assert!(cursor.resume_work);
    with_paused_insert(&graph, count, |index| {
        assert!(!index.advance_cursor(&mut cursor, usize::MAX).unwrap());
        assert!(!cursor.exhausted());
    });
    let mut fresh = VectorSearchCursor::new(Arc::clone(&graph), &query, 0, None).unwrap();
    assert_rows(
        fresh
            .next_batch(usize::MAX, count as usize + 1)
            .unwrap()
            .unwrap(),
        0..count + 1,
    );
    assert!(fresh.exhausted());
}
