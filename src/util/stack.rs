//! Bounded recursive SQL walkers enter large frames only with sufficient stack.
//! Extra segments stay on the same thread (preserving session TLS) and are
//! charged by capacity for their entire lifetime, including error unwinding.

use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};
use crate::types::QueryError;

const RED_ZONE: usize = 512 * 1024;
const SEGMENT_BYTES: usize = 8 * 1024 * 1024;

#[inline]
pub(crate) fn with_stack<T>(
    category: ControlledMemoryCategory,
    f: impl FnOnce() -> T,
) -> Result<T, QueryError> {
    if stacker::remaining_stack().is_some_and(|bytes| bytes >= RED_ZONE) {
        return Ok(f());
    }
    with_reserved_stack(category, f)
}

#[cold]
#[inline(never)]
pub(crate) fn with_reserved_stack<T>(
    category: ControlledMemoryCategory,
    f: impl FnOnce() -> T,
) -> Result<T, QueryError> {
    with_reserved_stack_bytes(category, SEGMENT_BYTES, f)
}

/// Run `f` on a fresh segment of at least `bytes`, for a recursive pass whose
/// depth is bounded and known up front but cannot check the stack per level.
#[cold]
#[inline(never)]
pub(crate) fn with_reserved_stack_bytes<T>(
    category: ControlledMemoryCategory,
    bytes: usize,
    f: impl FnOnce() -> T,
) -> Result<T, QueryError> {
    let bytes = bytes.max(SEGMENT_BYTES);
    let _charge = ControlledMemoryCharge::try_new(category, bytes)?;
    Ok(stacker::grow(bytes, f))
}

/// Infallible variant for recursive walkers that cannot report an error.
/// The segment is charged to `category` when admission succeeds; otherwise it
/// is still allocated, because exhausting the thread stack aborts the process.
#[inline]
pub(crate) fn ensure_stack<T>(category: ControlledMemoryCategory, f: impl FnOnce() -> T) -> T {
    if stacker::remaining_stack().is_some_and(|bytes| bytes >= RED_ZONE) {
        return f();
    }
    grow_best_effort(category, f)
}

#[cold]
#[inline(never)]
fn grow_best_effort<T>(category: ControlledMemoryCategory, f: impl FnOnce() -> T) -> T {
    let _charge = ControlledMemoryCharge::try_new(category, SEGMENT_BYTES).ok();
    stacker::grow(SEGMENT_BYTES, f)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::executor::mem_budget::controlled_memory_snapshot;

    #[test]
    fn temporary_stack_charge_is_released_after_callback_error() {
        let before = controlled_memory_snapshot().2[12];
        let result: Result<(), &str> =
            with_reserved_stack(ControlledMemoryCategory::ParserTransient, || {
                assert_eq!(controlled_memory_snapshot().2[12], before + SEGMENT_BYTES);
                Err("callback failed")
            })
            .unwrap();
        assert_eq!(result, Err("callback failed"));
        assert_eq!(controlled_memory_snapshot().2[12], before);
    }
}
