//! Exact terminal DISTINCT union. Existing scan workers have already joined.
//! Fingerprints are computed once; merge workers borrow canonical keys and
//! own disjoint hash shards, without copying keys or locking a shared set.

use std::hash::Hash;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::engine::executor::mem_budget::{MemBudget, OwnedMemCharge};
use crate::types::QueryError;

fn join_all<T>(
    handles: Vec<std::thread::ScopedJoinHandle<'_, Result<T, QueryError>>>,
    abort: &AtomicBool,
) -> Result<Vec<T>, QueryError> {
    let mut output = Vec::with_capacity(handles.len());
    let mut failure = None;
    for handle in handles {
        let result = handle.join().unwrap_or_else(|_| {
            Err(crate::error::catalog::ER_GET_ERRNO
                .error("parallel DISTINCT merge worker panicked"))
        });
        match result {
            Ok(value) => output.push(value),
            Err(error) => {
                abort.store(true, Ordering::Relaxed);
                if failure.is_none() {
                    failure = Some(error);
                }
            }
        }
    }
    match failure {
        Some(error) => Err(error),
        None => Ok(output),
    }
}

pub(crate) fn count_union<K: Eq + Hash + Sync>(
    sources: &[&hashbrown::HashSet<K, ahash::RandomState>],
    budget: &Arc<MemBudget>,
) -> Result<Option<usize>, QueryError> {
    let total = sources.iter().map(|source| source.len()).sum::<usize>();
    if sources.len() < 2 || sources.len() > 16 || total < 128 * 1024 {
        return Ok(None);
    }
    let workers = 1usize << sources.len().ilog2();
    // Each source reference/fingerprint uses 16 bytes. A shard's hashbrown
    // allocation has <16/7 buckets per requested entry, each holding an
    // 8-byte reference and one control byte. 40 bytes per input key plus
    // fixed per-worker metadata covers both simultaneously, including full
    // capacities and control-group padding. No key bytes are copied.
    let cancel = crate::engine::cancel::CancellationSnapshot::capture();
    cancel.check()?;
    let bytes = total
        .saturating_mul(40)
        .saturating_add(sources.len() * 4096);
    let Ok(_charge) = OwnedMemCharge::new(budget.clone(), bytes) else {
        return Ok(None);
    };
    let builder = ahash::RandomState::new();
    let abort = AtomicBool::new(false);
    let batches = std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(sources.len());
        for source in sources.iter().copied() {
            let builder = &builder;
            let abort = &abort;
            let cancel = cancel.clone();
            handles.push(scope.spawn(move || {
                let _cancel = cancel.enter();
                let mut keys = Vec::with_capacity(source.len());
                let mut counts = [0usize; 16];
                for (index, key) in source.iter().enumerate() {
                    if index % 8192 == 0 {
                        crate::engine::cancel::check_cancelled()?;
                        if abort.load(Ordering::Relaxed) {
                            return Err(crate::error::catalog::ER_QUERY_INTERRUPTED
                                .error("parallel DISTINCT merge aborted"));
                        }
                    }
                    let hash = builder.hash_one(key);
                    counts[hash as usize & (workers - 1)] += 1;
                    keys.push((hash, key));
                }
                Ok((keys, counts))
            }));
        }
        join_all(handles, &abort)
    })?;
    let counts = std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(workers);
        for worker in 0..workers {
            let batches = &batches;
            let builder = builder.clone();
            let abort = &abort;
            let cancel = cancel.clone();
            handles.push(scope.spawn(move || {
                let _cancel = cancel.enter();
                let capacity = batches
                    .iter()
                    .map(|(_, counts)| counts[worker])
                    .sum::<usize>();
                let mut unique = hashbrown::HashMap::with_capacity_and_hasher(capacity, builder);
                for (keys, _) in batches {
                    for (index, &(hash, key)) in keys.iter().enumerate() {
                        if index % 8192 == 0 {
                            crate::engine::cancel::check_cancelled()?;
                            if abort.load(Ordering::Relaxed) {
                                return Err(crate::error::catalog::ER_QUERY_INTERRUPTED
                                    .error("parallel DISTINCT merge aborted"));
                            }
                        }
                        if hash as usize & (workers - 1) != worker {
                            continue;
                        }
                        if let hashbrown::hash_map::RawEntryMut::Vacant(entry) = unique
                            .raw_entry_mut()
                            .from_hash(hash, |stored| *stored == key)
                        {
                            entry.insert_hashed_nocheck(hash, key, ());
                        }
                    }
                }
                Ok(unique.len())
            }));
        }
        join_all(handles, &abort)
    })?;
    Ok(Some(counts.into_iter().sum()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parallel_distinct_union_is_exact_and_skips_without_budget() {
        let sources: Vec<hashbrown::HashSet<u64, ahash::RandomState>> = (0..4)
            .map(|worker| {
                (0..40_000)
                    .map(|key| {
                        if key < 20_000 {
                            key
                        } else {
                            key + worker * 40_000
                        }
                    })
                    .collect()
            })
            .collect();
        let refs = sources.iter().collect::<Vec<_>>();
        let budget = Arc::new(MemBudget::new(32 * 1024 * 1024));
        assert_eq!(count_union(&refs, &budget).unwrap(), Some(100_000));
        assert_eq!(budget.used(), 0);
        assert_eq!(
            count_union(&refs, &Arc::new(MemBudget::new(1024))).unwrap(),
            None
        );
        assert!(sources.iter().all(|source| source.len() == 40_000));
        let cancel = crate::engine::cancel::CancellationSnapshot::capture();
        cancel.cancel_local();
        let _scope = cancel.enter();
        assert_eq!(count_union(&refs, &budget).unwrap_err().code(), 1317);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn parallel_distinct_worker_panic_joins_all_workers_and_releases_reservation() {
        struct Key(u64, Arc<AtomicBool>);
        impl PartialEq for Key {
            fn eq(&self, other: &Self) -> bool {
                self.0 == other.0
            }
        }
        impl Eq for Key {}
        impl Hash for Key {
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                assert!(!self.1.load(Ordering::Relaxed), "injected hash failure");
                self.0.hash(state);
            }
        }
        let fail = Arc::new(AtomicBool::new(false));
        let source: hashbrown::HashSet<Key, ahash::RandomState> =
            (0..70_000).map(|key| Key(key, fail.clone())).collect();
        fail.store(true, Ordering::Relaxed);
        let budget = Arc::new(MemBudget::new(32 * 1024 * 1024));
        assert!(count_union(&[&source, &source], &budget).is_err());
        assert_eq!(budget.used(), 0);
        assert_eq!(source.len(), 70_000);
    }
}
