//! Pure HNSW decisions shared by resident and paged storage.

/// Deterministic SplitMix64 draw; storage backends supply the same ordinal,
/// level multiplier and maximum layer so rebuilding preserves graph shape.
#[inline]
pub(super) fn random_level(ordinal: u64, ml: f64, max_layer: usize) -> usize {
    let mut state = ordinal
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(0xD1B5_4A32_D192_ED03);
    state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    let u = ((z >> 11) as f64 / (1u64 << 53) as f64).max(1e-12);
    ((-u.ln() * ml).floor() as usize).min(max_layer)
}

pub(super) fn quant_enabled_uncached() -> bool {
    match std::env::var("MYDB_HNSW_QUANTIZE") {
        Ok(v) => !matches!(v.as_str(), "0" | "off" | "OFF" | "false" | "FALSE"),
        Err(_) => true,
    }
}

/// Heap order shared by both backends; the identifier keeps its native width.
#[derive(Clone, Copy, PartialEq, Debug)]
pub(crate) struct Scored<Id> {
    pub(crate) dist: f32,
    pub(crate) id: Id,
}
impl<Id: Eq> Eq for Scored<Id> {}
impl<Id: Ord> Ord for Scored<Id> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.dist
            .total_cmp(&other.dist)
            .then_with(|| self.id.cmp(&other.id))
    }
}
impl<Id: Ord> PartialOrd for Scored<Id> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Algorithm 4 preserves candidate order and stops at the same first rejection.
/// Storage access stays in a statically dispatched backend closure.
#[inline]
pub(super) fn select_diverse<Id: Copy, E>(
    candidates: &[Scored<Id>],
    maximum: usize,
    mut distance: impl FnMut(Id, Id) -> Result<f32, E>,
) -> Result<Vec<Scored<Id>>, E> {
    select_diverse_by(
        candidates.iter().copied(),
        maximum,
        |candidate, previous| Ok(distance(candidate.id, previous.id)? < candidate.dist),
    )
}

/// Apply the same ordered selection to pre-resolved candidate positions.
/// The predicate reports redundancy, preserving strict comparisons (including
/// unordered distances) and allowing backends to retain their cached lookups.
#[inline]
pub(super) fn select_diverse_by<C: Copy, E>(
    candidates: impl IntoIterator<Item = C>,
    maximum: usize,
    mut redundant: impl FnMut(C, C) -> Result<bool, E>,
) -> Result<Vec<C>, E> {
    let mut selected = Vec::with_capacity(maximum);
    for candidate in candidates {
        if selected.len() >= maximum {
            break;
        }
        let mut keep = true;
        for previous in &selected {
            if redundant(candidate, *previous)? {
                keep = false;
                break;
            }
        }
        if keep {
            selected.push(candidate);
        }
    }
    Ok(selected)
}

#[inline]
pub(super) fn permits_in_place_update(drift_sq: f32, nearest_sq: f32, ratio_sq: f32) -> bool {
    !(!nearest_sq.is_finite() || drift_sq > ratio_sq * nearest_sq)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diversity_stops_at_capacity_and_first_redundant_neighbor() {
        let mut visited = Vec::new();
        let selected = select_diverse_by(0..6, 3, |candidate, previous| {
            visited.push((candidate, previous));
            Ok::<_, ()>(candidate == 2)
        })
        .unwrap();
        assert_eq!(selected, [0, 1, 3]);
        assert_eq!(visited, [(1, 0), (2, 0), (3, 0), (3, 1)]);
        let empty = select_diverse_by(0..6, 0, |_, _| Err::<bool, _>("must not run"));
        assert_eq!(empty, Ok(Vec::new()));
        let failed = select_diverse_by(0..6, 3, |_, _| Err::<bool, _>("read failed"));
        assert_eq!(failed, Err("read failed"));
    }

    #[test]
    fn diversity_keeps_equal_and_unordered_distances() {
        for distance in [0.0, -0.0, 1.0, f32::INFINITY, f32::NEG_INFINITY, f32::NAN] {
            let candidates = [
                Scored { id: 0, dist: 0.0 },
                Scored {
                    id: 1,
                    dist: distance,
                },
            ];
            for pair_distance in [distance, f32::NAN] {
                let selected =
                    select_diverse(&candidates, 2, |_, _| Ok::<_, ()>(pair_distance)).unwrap();
                assert_eq!(selected.iter().map(|s| s.id).collect::<Vec<_>>(), [0, 1]);
            }
        }
    }

    #[test]
    fn heap_ties_and_diversity_match_across_identifier_widths() {
        let narrow = [
            Scored {
                dist: 4.0,
                id: 20u32,
            },
            Scored { dist: 1.0, id: 10 },
            Scored { dist: 1.0, id: 9 },
        ];
        let mut wide = narrow.map(|s| Scored {
            dist: s.dist,
            id: u64::from(s.id),
        });
        wide.sort_unstable();
        assert_eq!(wide.map(|s| s.id), [9, 10, 20]);
        let candidates = [
            Scored { dist: 1.0, id: 10 },
            Scored { dist: 4.0, id: 20 },
            Scored { dist: 8.0, id: 30 },
        ];
        let selected = select_diverse(&candidates, 3, |a, b| {
            Ok::<_, ()>(if a == 20 && b == 10 { 0.5 } else { 10.0 })
        })
        .unwrap();
        assert_eq!(
            selected.into_iter().map(|s| s.id).collect::<Vec<_>>(),
            [10, 30]
        );
        let result = select_diverse(&candidates, 3, |_, _| Err::<f32, _>("page read failed"));
        assert_eq!(result.unwrap_err(), "page read failed");
    }

    #[test]
    fn movement_requires_a_finite_local_scale_and_keeps_the_equal_boundary() {
        assert!(permits_in_place_update(4.0, 16.0, 0.25));
        assert!(!permits_in_place_update(4.1, 16.0, 0.25));
        assert!(!permits_in_place_update(0.0, f32::INFINITY, 1.0));
    }
}
