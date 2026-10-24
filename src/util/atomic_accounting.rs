//! Atomic byte accounting shared by memory and spill quotas.
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

macro_rules! accounting {
    ($sub:ident, $add:ident, $atomic:ty, $value:ty) => {
        /// Release at most the current balance and return the actual release.
        #[inline]
        pub(crate) fn $sub(counter: &$atomic, delta: $value) -> $value {
            let mut current = counter.load(Ordering::Acquire);
            loop {
                let actual = delta.min(current);
                match counter.compare_exchange_weak(
                    current,
                    current - actual,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return actual,
                    Err(observed) => current = observed,
                }
            }
        }

        /// Reserve bytes only if the addition is representable and within the limit.
        #[inline]
        pub(crate) fn $add(counter: &$atomic, delta: $value, limit: $value) -> Option<$value> {
            let mut current = counter.load(Ordering::Acquire);
            loop {
                let next = current.checked_add(delta)?;
                if next > limit {
                    return None;
                }
                match counter.compare_exchange_weak(
                    current,
                    next,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return Some(next),
                    Err(observed) => current = observed,
                }
            }
        }
    };
}

accounting!(saturating_sub_usize, try_add_usize, AtomicUsize, usize);
accounting!(saturating_sub_u64, try_add_u64, AtomicU64, u64);
