//! Exhaustive-effort searches must cover nodes outside the greedy entry's reach.
use super::*;

#[test]
fn hnsw_exhaustive_effort_matches_oracle_after_greedy_descent_and_deletes() {
    for metric in [Metric::L2, Metric::Cosine, Metric::Dot] {
        let graph = HnswIndex::new(metric, 4, 64);
        let rows = (1..=40u64)
            .map(|id| {
                let vector = (0..3)
                    .map(|d| ((id * (d + 3) + d * 7) % 23 + 1) as f32)
                    .collect::<Vec<_>>();
                let row = RowId {
                    page_id: id as u32,
                    slot_id: 0,
                };
                graph.append_node(row, vector.clone()).unwrap();
                (row, vector)
            })
            .collect::<Vec<_>>();
        graph.link_all(1);
        for deleted in [false, true] {
            if deleted {
                for (row, _) in &rows {
                    if row.page_id % 7 == 0 {
                        graph.mark_deleted_by_row_id(*row);
                    }
                }
            }
            for query in [[1.25, 2.25, 3.25], [4.25, 5.25, 6.25]] {
                let mut expected = rows
                    .iter()
                    .filter(|(row, _)| !deleted || row.page_id % 7 != 0)
                    .map(|(row, v)| {
                        let dot = v
                            .iter()
                            .zip(query)
                            .map(|(a, b)| *a as f64 * b as f64)
                            .sum::<f64>();
                        let score = match metric {
                            Metric::L2 => v
                                .iter()
                                .zip(query)
                                .map(|(a, b)| (*a as f64 - b as f64).powi(2))
                                .sum::<f64>(),
                            Metric::Dot => -dot,
                            Metric::Cosine => {
                                1.0 - dot
                                    / (v.iter().map(|a| (*a as f64).powi(2)).sum::<f64>().sqrt()
                                        * query
                                            .iter()
                                            .map(|b| (*b as f64).powi(2))
                                            .sum::<f64>()
                                            .sqrt())
                            }
                            _ => unreachable!(),
                        };
                        (*row, score)
                    })
                    .collect::<Vec<_>>();
                expected.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
                for k in [1, 13, 32, 40, 64] {
                    let actual = graph.search(&query, k, 64);
                    let expected = &expected[..k.min(expected.len())];
                    assert_eq!(actual.len(), expected.len(), "{metric:?}/{deleted}/{k}");
                    for (actual, expected) in actual.iter().zip(expected) {
                        assert_eq!(actual.0, expected.0, "{metric:?}/{deleted}/{k}");
                        assert!(
                            (actual.1 - expected.1).abs() < 1e-6,
                            "{actual:?}/{expected:?}"
                        );
                    }
                }
            }
        }
    }
}
