// Transaction IDs in a retained WAL usually occupy a compact monotonic range.
// Use a governed bitmap there, with a hash fallback for sparse/large IDs.
#[derive(Clone)]
enum RecoveryTxnSet {
    Dense {
        base: u64,
        words: Vec<u64>,
        count: usize,
    },
    Sparse(hashbrown::HashSet<u64, ahash::RandomState>),
}
impl RecoveryTxnSet {
    fn for_scan(scan: &ReplayScan) -> Self {
        if let Some((low, high)) = scan.records.bootstrap().and_then(|b| b.txn_bounds) {
            if let Some(span) = high.checked_sub(low).and_then(|n| n.checked_add(1)) {
                // Five sets consume at most five bytes per retained record,
                // included in recovery's navigation reservation.
                if span <= scan.records.len().saturating_mul(8) as u64 && span <= 128 * 1024 * 1024
                {
                    return Self::Dense {
                        base: low,
                        words: vec![0; (span as usize).div_ceil(64)],
                        count: 0,
                    };
                }
            }
        }
        Self::Sparse(Default::default())
    }
    fn sparse(&self) -> bool {
        matches!(self, Self::Sparse(_))
    }
    fn contains(&self, id: &u64) -> bool {
        match self {
            Self::Dense { base, words, .. } => id.checked_sub(*base).is_some_and(|n| {
                words
                    .get((n / 64) as usize)
                    .is_some_and(|word| word & (1 << (n % 64)) != 0)
            }),
            Self::Sparse(set) => set.contains(id),
        }
    }
    fn insert(&mut self, id: u64) -> bool {
        if let Self::Dense { base, words, count } = self {
            if let Some(n) = id
                .checked_sub(*base)
                .filter(|n| *n < words.len() as u64 * 64)
            {
                let mask = 1 << (n % 64);
                let word = &mut words[(n / 64) as usize];
                let new = *word & mask == 0;
                *word |= mask;
                *count += usize::from(new);
                return new;
            }
            let mut set = hashbrown::HashSet::default();
            set.extend(self.iter());
            *self = Self::Sparse(set);
        }
        let Self::Sparse(set) = self else {
            unreachable!()
        };
        set.insert(id)
    }
    fn remove(&mut self, id: &u64) -> bool {
        match self {
            Self::Dense { base, words, count } => {
                let Some(n) = id
                    .checked_sub(*base)
                    .filter(|n| *n < words.len() as u64 * 64)
                else {
                    return false;
                };
                let mask = 1 << (n % 64);
                let word = &mut words[(n / 64) as usize];
                let old = *word & mask != 0;
                *word &= !mask;
                *count -= usize::from(old);
                old
            }
            Self::Sparse(set) => set.remove(id),
        }
    }
    fn len(&self) -> usize {
        match self {
            Self::Dense { count, .. } => *count,
            Self::Sparse(set) => set.len(),
        }
    }
    fn iter(&self) -> Box<dyn Iterator<Item = u64> + '_> {
        match self {
            Self::Sparse(set) => Box::new(set.iter().copied()),
            Self::Dense { base, words, .. } => Box::new(
                words
                    .iter()
                    .copied()
                    .enumerate()
                    .flat_map(move |(i, mut word)| {
                        std::iter::from_fn(move || {
                            if word == 0 {
                                return None;
                            }
                            let bit = word.trailing_zeros();
                            word &= word - 1;
                            Some(*base + i as u64 * 64 + bit as u64)
                        })
                    }),
            ),
        }
    }
    fn extend(&mut self, ids: impl IntoIterator<Item = u64>) {
        for id in ids {
            self.insert(id);
        }
    }
}
impl FromIterator<u64> for RecoveryTxnSet {
    fn from_iter<T: IntoIterator<Item = u64>>(ids: T) -> Self {
        Self::Sparse(ids.into_iter().collect())
    }
}

#[cfg(test)]
mod recovery_txn_set_tests {
    use super::*;
    #[test]
    fn dense_recovery_membership_matches_sparse_and_preserves_extreme_ids() {
        let mut dense = RecoveryTxnSet::Dense {
            base: 100,
            words: vec![0; 3],
            count: 0,
        };
        let mut oracle = HashSet::new();
        for id in [100, 101, 163, 164, 291, 100, 0, u64::MAX] {
            assert_eq!(dense.insert(id), oracle.insert(id));
            assert_eq!(dense.len(), oracle.len());
            assert!(dense.contains(&id));
        }
        for id in [101, 101, 0, u64::MAX, 300] {
            assert_eq!(dense.remove(&id), oracle.remove(&id));
            assert_eq!(dense.len(), oracle.len());
        }
        assert_eq!(dense.iter().collect::<HashSet<_>>(), oracle);
        assert_eq!(dense.clone().iter().collect::<HashSet<_>>(), oracle);
    }
}
