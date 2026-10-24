//! Runtime configuration for the vectorized executor.

use std::sync::OnceLock;

/// Three-valued `MYDB_VECTORIZED` flag.
///
/// - `Off` — keep the same `BatchOperator` graph and select scalar expression
///   and aggregate kernels. This is the deterministic correctness oracle; it
///   never restarts a query through a second physical executor.
/// - `On` — select typed/vector kernels wherever the batch compiler supports
///   them; unsupported expressions use their scalar lane in the current batch.
/// - `Auto` (default) — uses the same semantics as `On`; operator-local cost
///   decisions may adjust batch size and prefetch, never plan topology.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VecMode {
    Off,
    On,
    Auto,
}

impl Default for VecMode {
    fn default() -> Self {
        VecMode::Auto
    }
}

pub fn current_mode() -> VecMode {
    static MODE: OnceLock<VecMode> = OnceLock::new();
    *MODE.get_or_init(|| match crate::util::env::var("MYDB_VECTORIZED") {
        Ok(v) => match v.to_ascii_lowercase().as_str() {
            "on" | "1" | "true" => VecMode::On,
            "auto" => VecMode::Auto,
            "off" | "0" | "false" => VecMode::Off,
            // Unknown value — fall through to the default so a typo
            // doesn't silently route the entire workload to one path.
            _ => VecMode::default(),
        },
        Err(_) => VecMode::default(),
    })
}

thread_local! {
    static VEC_MODE_OVERRIDE: std::cell::Cell<Option<VecMode>> = std::cell::Cell::new(None);
}

pub(crate) struct VecModeScope {
    previous: Option<VecMode>,
}

impl Drop for VecModeScope {
    fn drop(&mut self) {
        VEC_MODE_OVERRIDE.with(|cell| cell.set(self.previous));
    }
}

/// Install the statement-captured kernel mode on the current worker. Unlike
/// process environment lookup, this follows the execution context across
/// thread boundaries and remains stable for the whole query.
pub(crate) fn enter_mode(mode: VecMode) -> VecModeScope {
    let previous = VEC_MODE_OVERRIDE.with(|cell| cell.replace(Some(mode)));
    VecModeScope { previous }
}

pub fn effective_mode() -> VecMode {
    VEC_MODE_OVERRIDE.with(|c| c.get().unwrap_or_else(current_mode))
}

/// Override the vectorized mode for the duration of a closure. Test-only.
/// Implemented with the same scope inherited by execution workers.
#[cfg(test)]
pub fn with_vec_mode<R>(mode: VecMode, f: impl FnOnce() -> R) -> R {
    let _scope = enter_mode(mode);
    f()
}
