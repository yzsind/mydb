pub type AhHashMap<K, V> = hashbrown::HashMap<K, V, ahash::RandomState>;
pub type AhHashSet<T> = hashbrown::HashSet<T, ahash::RandomState>;

#[inline]
pub fn ah_hash_map<K, V>() -> AhHashMap<K, V> {
    hashbrown::HashMap::with_hasher(ahash::RandomState::new())
}

#[inline]
pub fn ah_hash_map_with_capacity<K, V>(capacity: usize) -> AhHashMap<K, V> {
    hashbrown::HashMap::with_capacity_and_hasher(capacity, ahash::RandomState::new())
}

#[inline]
pub fn ah_hash_set<T>() -> AhHashSet<T> {
    hashbrown::HashSet::with_hasher(ahash::RandomState::new())
}

#[inline]
pub fn ah_hash_set_with_capacity<T>(capacity: usize) -> AhHashSet<T> {
    hashbrown::HashSet::with_capacity_and_hasher(capacity, ahash::RandomState::new())
}

/// Stable FNV-1a identity hash for lock instrumentation across snapshots.
#[inline]
pub(crate) fn fnv1a_64(s: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in s.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}
