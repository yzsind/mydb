//! Thread-local execution parameter context for late `?` binding.
//!
//! Phase 2 of the SQL caching design: `Expr::Placeholder(idx)` is no longer
//! eagerly substituted with `Expr::BoundValue(value)` during prepared
//! statement EXECUTE. The placeholder is preserved in the cached AST/plan
//! and looked up here at evaluation time. This is what lets a single cached
//! plan be reused across many parameter sets (true plan-cache reuse for
//! prepared statements, and the substrate for COM_QUERY auto-parameterization).
//!
//! Phase 2.6 storage layout:
//!   * a `Cell<*const Value>` + `Cell<usize>` pair for fast read access
//!     (one TLS load + one Cell::get + a pointer add per `current_param`)
//!   * the owning `Vec<Value>` (when produced by `with_params`) is held by
//!     the RAII guard on the stack, so its backing storage outlives the
//!     pointer cell that references it.
//!
//! The hot read path (`current_param`) does **no** RefCell borrow check, no
//! `Option::as_ref` plumbing, and no `Vec::get` bounds check beyond a
//! single `idx < len` comparison. Combined with `with_params_slice` (below)
//! this lets streaming cursors reinstall their captured parameter slice
//! without cloning the `Vec` on every `next_row` call.

use std::cell::Cell;

use crate::types::Value;

/// Owned parameter binding captured for execution on another thread.
///
/// Literal provenance is part of the binding context: auto-parameterized
/// COM_QUERY placeholders must retain literal diagnostics, while genuine
/// prepared-statement parameters use execution-time conversion rules.
#[derive(Clone)]
pub struct ParamContextSnapshot {
    params: Vec<Value>,
    auto_parameterized: bool,
}

impl ParamContextSnapshot {
    #[inline]
    pub fn run<R>(&self, f: impl FnOnce() -> R) -> R {
        with_params_slice_and_provenance(&self.params, self.auto_parameterized, f)
    }
}

/// `(ptr, len)` describing the active parameter slice. The pair is held
/// in a single `Cell` so reading both fields is one TLS access plus one
/// `Cell::get`. `ptr.is_null()` ⇔ no active context.
#[derive(Clone, Copy)]
struct ParamSliceRef {
    ptr: *const Value,
    len: usize,
}

impl ParamSliceRef {
    const EMPTY: ParamSliceRef = ParamSliceRef {
        ptr: std::ptr::null(),
        len: 0,
    };
}

thread_local! {
    static PARAMS: Cell<ParamSliceRef> = const { Cell::new(ParamSliceRef::EMPTY) };
    /// Bumped on every install/restore of `PARAMS`, so derived per-binding
    /// facts can be cached without trusting a possibly reused slice address.
    static PARAMS_GENERATION: Cell<u64> = const { Cell::new(0) };
    // Distinguishes literals extracted from COM_QUERY text from genuine SQL /
    // wire prepared-statement parameters. Resolve-time diagnostics must still
    // inspect the former as source literals, while the latter are converted at
    // EXECUTE time.
    static AUTO_PARAMETERIZED: Cell<bool> = const { Cell::new(false) };
    /// Set while a SELECT runs as an INSERT…SELECT *source*. It flips the
    /// planner's "active params force an eager (fully-materialised) response"
    /// rule off for that source only: the INSERT chunk loader consumes rows
    /// incrementally and the streaming scan worker snapshots + re-installs the
    /// bound params on its own thread (see `seq_scan_stream`), so a param-
    /// carrying source can stream instead of buffering the whole result. Scoped
    /// by [`StreamingParamSourceGuard`]; top-level (client-facing) SELECTs are
    /// unaffected.
    static STREAMING_PARAM_SOURCE: Cell<bool> = const { Cell::new(false) };
}

/// RAII guard enabling param-carrying INSERT…SELECT sources to stream (see
/// [`STREAMING_PARAM_SOURCE`]). Restores the previous value on drop, so nesting
/// and re-entrancy are safe.
pub struct StreamingParamSourceGuard(bool);

impl StreamingParamSourceGuard {
    pub fn enter() -> Self {
        Self(STREAMING_PARAM_SOURCE.with(|c| c.replace(true)))
    }
}

impl Drop for StreamingParamSourceGuard {
    fn drop(&mut self) {
        STREAMING_PARAM_SOURCE.with(|c| c.set(self.0));
    }
}

/// Whether the current SELECT is an INSERT…SELECT source that opted into
/// streaming despite active params.
pub fn streaming_param_source_active() -> bool {
    STREAMING_PARAM_SOURCE.with(|c| c.get())
}

/// Look up the parameter at `idx`. Returns `None` if there is no active
/// parameter context or the index is out of range — callers should map
/// `None` to `ER_NO_DATA_FOR_BIND_PARAMETER (1210)`.
#[inline]
pub fn current_param(idx: usize) -> Option<Value> {
    with_current_param(idx, |value| value.cloned())
}

#[inline]
pub(crate) fn with_current_param<R>(idx: usize, f: impl FnOnce(Option<&Value>) -> R) -> R {
    let slice = PARAMS.with(|c| c.get());
    if slice.ptr.is_null() || idx >= slice.len {
        return f(None);
    }
    // SAFETY: the binding guard owns the slice throughout this callback;
    // nested installations restore their predecessor before freeing storage.
    f(Some(unsafe { &*slice.ptr.add(idx) }))
}

#[inline]
pub(crate) fn has_active_param(idx: usize) -> bool {
    PARAMS.with(|cell| {
        let slice = cell.get();
        !slice.ptr.is_null() && idx < slice.len
    })
}

/// Whether any parameter context is currently active. Useful for executor
/// fast paths that want to skip placeholder-safe planning when no `?` are
/// in flight.
#[allow(dead_code)]
pub fn has_active_params() -> bool {
    !PARAMS.with(|c| c.get()).ptr.is_null()
}

/// Whether the active binding originated from text auto-parameterization.
#[inline]
pub fn auto_parameterized_active() -> bool {
    AUTO_PARAMETERIZED.with(Cell::get)
}

/// Look up the parameter at `idx` as a row count (LIMIT / OFFSET use).
/// Returns `None` when there is no context, the index is out of range,
/// or the value is not a non-negative integer — callers fall back to
/// their planning-time baked count.
#[inline]
pub fn current_param_usize(idx: usize) -> Option<usize> {
    match current_param(idx)? {
        Value::Int(n) if n >= 0 => usize::try_from(n).ok(),
        Value::UInt(n) => usize::try_from(n).ok(),
        _ => None,
    }
}

/// Capture a clone of the active parameter context, suitable for handing
/// to a streaming result cursor (or any other state that outlives the
/// initial `with_params` scope). The returned `Vec` is reinstalled later
/// via `with_params_slice`, which avoids cloning it back into thread-local
/// storage on every reentry.
pub fn snapshot_current_params() -> Option<Vec<Value>> {
    let slice = PARAMS.with(|c| c.get());
    if slice.ptr.is_null() {
        return None;
    }
    // SAFETY: same justification as `current_param`. The slice is alive
    // for the duration of this call.
    Some(unsafe { std::slice::from_raw_parts(slice.ptr, slice.len) }.to_vec())
}

/// Borrow the active parameter slice without cloning it.  The slice is valid
/// only for the duration of `f`; callers must not let references escape.
///
/// Expression binding uses this to derive a compact parameter-type signature
/// for its process-global program cache.  Keeping the operation here avoids
/// turning every cache probe into a `Vec<Value>` clone.
pub(crate) fn with_current_params<R>(f: impl FnOnce(Option<&[Value]>, bool) -> R) -> R {
    let slice = PARAMS.with(|cell| cell.get());
    let auto_parameterized = auto_parameterized_active();
    if slice.ptr.is_null() {
        return f(None, auto_parameterized);
    }
    // SAFETY: identical lifetime argument to `current_param`: the installed
    // owner/borrow remains live throughout this synchronous callback.
    let values = unsafe { std::slice::from_raw_parts(slice.ptr, slice.len) };
    f(Some(values), auto_parameterized)
}

/// Capture parameters together with their literal/prepared provenance for a
/// worker thread. Consumers should reinstall it with
/// [`ParamContextSnapshot::run`], not just copy the value slice.
pub fn snapshot_current_context() -> Option<ParamContextSnapshot> {
    snapshot_current_params().map(|params| ParamContextSnapshot {
        params,
        auto_parameterized: auto_parameterized_active(),
    })
}

#[inline]
fn set_params(install: ParamSliceRef) {
    PARAMS.with(|c| c.set(install));
    PARAMS_GENERATION.with(|generation| generation.set(generation.get().wrapping_add(1)));
}

/// Identity of the current parameter installation on this thread.
pub(crate) fn current_params_generation() -> u64 {
    PARAMS_GENERATION.with(Cell::get)
}

/// Run `f` with `params` installed as the current binding context. The
/// `Vec` is owned by the returned guard, so the slice it backs lives at
/// least as long as `f`. Restores the previous binding (which may have
/// been a slice borrowed by `with_params_slice` or another owned `Vec`)
/// when the guard drops.
#[inline]
pub fn with_params<R>(params: Vec<Value>, f: impl FnOnce() -> R) -> R {
    let previous_auto = AUTO_PARAMETERIZED.with(|flag| flag.replace(false));
    let _auto_guard = AutoParamGuard(previous_auto);
    install_owned_params(params, f)
}

#[inline]
fn install_owned_params<R>(params: Vec<Value>, f: impl FnOnce() -> R) -> R {
    let prev = PARAMS.with(|c| c.get());
    let _guard = OwnedGuard { params, prev };
    let install = ParamSliceRef {
        ptr: _guard.params.as_ptr(),
        len: _guard.params.len(),
    };
    set_params(install);
    f()
}

/// Install parameters extracted from literals in a COM_QUERY statement.
/// These share the hot placeholder lookup with prepared statements but keep
/// their literal provenance for resolve-time validation.
#[inline]
pub fn with_auto_params<R>(params: Vec<Value>, f: impl FnOnce() -> R) -> R {
    let previous = AUTO_PARAMETERIZED.with(|flag| flag.replace(true));
    let _guard = AutoParamGuard(previous);
    install_owned_params(params, f)
}

struct AutoParamGuard(bool);

impl Drop for AutoParamGuard {
    fn drop(&mut self) {
        AUTO_PARAMETERIZED.with(|flag| flag.set(self.0));
    }
}

/// Run `f` with the slice borrowed without copying or owning it. The
/// caller must keep `params` alive across the call (Rust's borrow checker
/// enforces this through the function signature). Useful for hot paths
/// that already hold the parameter buffer — e.g. a streaming result
/// cursor that captured params at construction time and re-enters the
/// scope on every `next_row` call.
#[inline]
pub fn with_params_slice<R>(params: &[Value], f: impl FnOnce() -> R) -> R {
    install_params_slice(params, f)
}

#[inline]
fn with_params_slice_and_provenance<R>(
    params: &[Value],
    auto_parameterized: bool,
    f: impl FnOnce() -> R,
) -> R {
    let previous_auto = AUTO_PARAMETERIZED.with(|flag| flag.replace(auto_parameterized));
    let _auto_guard = AutoParamGuard(previous_auto);
    install_params_slice(params, f)
}

#[inline]
fn install_params_slice<R>(params: &[Value], f: impl FnOnce() -> R) -> R {
    let prev = PARAMS.with(|c| c.get());
    let install = ParamSliceRef {
        ptr: params.as_ptr(),
        len: params.len(),
    };
    set_params(install);
    let _guard = SliceGuard { prev };
    f()
}

struct OwnedGuard {
    /// Held to keep the backing storage alive. Drops *after* the inner
    /// `Drop::drop` body finishes, by Rust drop ordering, so by the time
    /// the `Vec` is dropped the thread-local pointer no longer aliases
    /// it.
    params: Vec<Value>,
    prev: ParamSliceRef,
}

impl Drop for OwnedGuard {
    fn drop(&mut self) {
        set_params(self.prev);
        // `self.params` is dropped after this returns — by then the
        // pointer cell already references the previous binding (or
        // null), so the `Vec`'s deallocation cannot create a dangling
        // pointer.
    }
}

struct SliceGuard {
    prev: ParamSliceRef,
}

impl Drop for SliceGuard {
    fn drop(&mut self) {
        set_params(self.prev);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_params_returns_none() {
        assert!(current_param(0).is_none());
        assert!(!has_active_params());
    }

    #[test]
    fn with_params_then_lookup_and_restore() {
        let result = with_params(vec![Value::Int(7), Value::String("x".into())], || {
            assert!(has_active_params());
            assert_eq!(current_param(0), Some(Value::Int(7)));
            assert_eq!(current_param(1), Some(Value::String("x".into())));
            assert!(current_param(2).is_none());
            42
        });
        assert_eq!(result, 42);
        assert!(!has_active_params(), "guard restores prior empty state");
    }

    #[test]
    fn auto_parameterized_scope_tracks_literal_provenance() {
        assert!(!auto_parameterized_active());
        with_auto_params(vec![Value::String("literal".into())], || {
            assert!(auto_parameterized_active());
            assert_eq!(current_param(0), Some(Value::String("literal".into())));
            with_params(vec![Value::Int(7)], || {
                assert!(!auto_parameterized_active());
            });
            assert!(auto_parameterized_active());
        });
        assert!(!auto_parameterized_active());
    }

    #[test]
    fn nested_with_params_restores_outer_on_drop() {
        with_params(vec![Value::Int(1)], || {
            assert_eq!(current_param(0), Some(Value::Int(1)));
            with_params(vec![Value::Int(99), Value::Int(100)], || {
                assert_eq!(current_param(0), Some(Value::Int(99)));
                assert_eq!(current_param(1), Some(Value::Int(100)));
            });
            assert_eq!(
                current_param(0),
                Some(Value::Int(1)),
                "outer params restored after inner scope"
            );
            assert!(current_param(1).is_none());
        });
    }

    #[test]
    fn with_params_slice_does_not_copy() {
        let buffer = vec![Value::Int(11), Value::String("y".into())];
        let observed = with_params_slice(&buffer, || {
            (current_param(0).unwrap(), current_param(1).unwrap())
        });
        assert_eq!(observed.0, Value::Int(11));
        assert_eq!(observed.1, Value::String("y".into()));
        assert!(!has_active_params(), "slice guard restored prior state");
    }

    #[test]
    fn snapshot_then_reinstall_with_slice_round_trips() {
        with_params(vec![Value::Int(5), Value::Int(6)], || {
            let snap = snapshot_current_params().expect("active context");
            // Pretend we're a cursor: re-enter via slice on each row.
            for _ in 0..3 {
                with_params_slice(&snap, || {
                    assert_eq!(current_param(0), Some(Value::Int(5)));
                    assert_eq!(current_param(1), Some(Value::Int(6)));
                });
            }
        });
    }

    #[test]
    fn context_snapshot_reinstalls_auto_parameterized_provenance() {
        with_auto_params(vec![Value::Int(5)], || {
            let snapshot = snapshot_current_context().expect("active context");
            std::thread::spawn(move || {
                assert!(!auto_parameterized_active());
                snapshot.run(|| {
                    assert!(auto_parameterized_active());
                    assert_eq!(current_param(0), Some(Value::Int(5)));
                });
                assert!(!auto_parameterized_active());
            })
            .join()
            .unwrap();
        });
    }

    #[test]
    fn nested_slice_restores_outer_owned() {
        with_params(vec![Value::Int(1)], || {
            let inner = vec![Value::Int(99)];
            with_params_slice(&inner, || {
                assert_eq!(current_param(0), Some(Value::Int(99)));
            });
            assert_eq!(
                current_param(0),
                Some(Value::Int(1)),
                "owned outer params restored after slice scope"
            );
        });
    }
}

/// Validate bound LIMIT operands before optimization can erase a LIMIT node.
/// Unbound parameters are legal during PREPARE metadata planning.
pub(crate) fn validate_limit(
    value: Option<&crate::parser::LimitValue>,
) -> Result<(), crate::types::QueryError> {
    use crate::parser::LimitValue;
    let Some(LimitValue::Placeholder(index) | LimitValue::OffsetCommaPlaceholder(index)) = value
    else {
        return Ok(());
    };
    let Some(value) = current_param(*index) else {
        return Ok(());
    };
    match value {
        Value::Int(n) if n < 0 => Err(crate::error::catalog::ER_DATA_OUT_OF_RANGE
            .error("unsigned integer value is out of range in LIMIT/OFFSET")),
        Value::Int(_) | Value::UInt(_) => Ok(()),
        _ => Err(crate::error::catalog::ER_WRONG_ARGUMENTS
            .error("LIMIT/OFFSET parameter must be a non-negative integer")),
    }
}

pub(crate) fn snapshot_selected_context(
    indices: &std::collections::BTreeSet<usize>,
) -> Result<Option<ParamContextSnapshot>, crate::types::QueryError> {
    let Some(last) = indices.last() else {
        return Ok(None);
    };
    if indices.iter().any(|i| !has_active_param(*i)) {
        return Err(crate::error::catalog::ER_WRONG_ARGUMENTS.error("parameter is not bound"));
    }
    let mut params = vec![
        Value::Null;
        last.checked_add(1).ok_or_else(|| {
            crate::error::catalog::ER_WRONG_ARGUMENTS.error("parameter index overflow")
        })?
    ];
    for index in indices {
        params[*index] = current_param(*index).ok_or_else(|| {
            crate::error::catalog::ER_WRONG_ARGUMENTS.error("parameter is not bound")
        })?;
    }
    Ok(Some(ParamContextSnapshot {
        params,
        auto_parameterized: auto_parameterized_active(),
    }))
}
