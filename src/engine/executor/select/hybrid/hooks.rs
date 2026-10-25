//! Per-query fault/concurrency hooks, excluded from production builds.
use super::*;

#[cfg(test)]
pub(in crate::engine::executor) type BranchHook = Arc<dyn Fn(usize) + Send + Sync>;
#[cfg(test)]
thread_local! {static BRANCH_HOOK:std::cell::RefCell<Option<BranchHook>>=const{std::cell::RefCell::new(None)};}
#[cfg(test)]
pub(in crate::engine::executor) fn branch_hook() -> Option<BranchHook> {
    BRANCH_HOOK.with(|slot| slot.borrow().clone())
}
#[cfg(test)]
pub(in crate::engine::executor) struct HookScope(Option<BranchHook>);
#[cfg(test)]
impl Drop for HookScope {
    fn drop(&mut self) {
        BRANCH_HOOK.with(|slot| *slot.borrow_mut() = self.0.take());
    }
}
#[cfg(test)]
pub(in crate::engine::executor) fn enter_branch_hook(hook: Option<BranchHook>) -> HookScope {
    HookScope(BRANCH_HOOK.with(|slot| slot.replace(hook)))
}
