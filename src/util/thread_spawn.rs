//! Native thread creation preserves the server CPU mask when called by a
//! connection reactor. Other callers retain their own scheduling policy.
#[allow(unused_imports)]
pub use std::thread::*;

pub struct Builder(std::thread::Builder);

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}

impl Builder {
    pub fn new() -> Self {
        Self(std::thread::Builder::new())
    }
    pub fn name(self, name: String) -> Self {
        Self(self.0.name(name))
    }
    pub fn stack_size(self, size: usize) -> Self {
        Self(self.0.stack_size(size))
    }
    pub fn spawn<F, T>(self, f: F) -> std::io::Result<JoinHandle<T>>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let _affinity = super::connection_dispatch::unpin_for_spawn()?;
        self.0.spawn(f)
    }
    pub fn spawn_scoped<'scope, 'env, F, T>(
        self,
        scope: &'scope Scope<'scope, 'env>,
        f: F,
    ) -> std::io::Result<ScopedJoinHandle<'scope, T>>
    where
        F: FnOnce() -> T + Send + 'scope,
        T: Send + 'scope,
    {
        let _affinity = super::connection_dispatch::unpin_for_spawn()?;
        self.0.spawn_scoped(scope, f)
    }
}

pub fn spawn<F, T>(f: F) -> JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    Builder::new()
        .spawn(f)
        .expect("cannot spawn database worker thread")
}

pub fn scope<'env, F, T>(f: F) -> T
where
    F: for<'scope> FnOnce(&'scope Scope<'scope, 'env>) -> T,
{
    let _affinity = super::connection_dispatch::unpin_for_spawn()
        .expect("cannot restore CPU affinity for scoped database workers");
    std::thread::scope(f)
}

/// Reactor CPU placement must not shrink lazily initialized compute pools.
/// Unowned callers retain std's thread-specific affinity/quota behavior.
pub fn available_parallelism() -> std::io::Result<std::num::NonZeroUsize> {
    if let Some(parallelism) = super::connection_dispatch::reactor_server_parallelism() {
        Ok(parallelism)
    } else {
        std::thread::available_parallelism()
    }
}
