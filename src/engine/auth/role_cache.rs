//! Bounded thread-local role closures. Keys retain only weak catalog roots;
//! cached closures own their governor charge and cannot keep old catalogs alive.
use super::*;
use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};
use std::cell::RefCell;
use std::sync::Weak;
struct Closure {
    roles: BTreeSet<AccountKey>,
    _charge: ControlledMemoryCharge,
}
struct Entry {
    graph: Weak<()>,
    accounts: Weak<imbl::OrdMap<String, UserAccount>>,
    policy: u64,
    principal: AccountKey,
    seed: AuthCow<BTreeSet<AccountKey>>,
    closure: Arc<Closure>,
}
thread_local! { static CACHE: RefCell<VecDeque<Entry>> = const { RefCell::new(VecDeque::new()) }; }
pub(super) fn with_roles<R>(
    auth: &AuthCatalog,
    ctx: &AuthorizationContext,
    f: impl FnOnce(&BTreeSet<AccountKey>) -> R,
) -> R {
    if ctx.active_roles.is_empty() {
        return f(&BTreeSet::new());
    }
    let policy = ROLE_POLICY_GENERATION.load(Ordering::Acquire);
    let hit = CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let index = cache.iter().position(|entry| {
            entry.policy == policy
                && auth.role_edges.same_storage(&entry.graph)
                && auth.accounts.same_storage(&entry.accounts)
                && entry.principal == ctx.authenticated_as
                && entry.seed == ctx.active_roles
        })?;
        let entry = cache.remove(index).unwrap();
        let result = entry.closure.clone();
        cache.push_front(entry);
        Some(result)
    });
    if let Some(hit) = hit {
        return f(&hit.roles);
    }
    let roles = auth.compute_effective_roles(ctx);
    let bytes = 4 * std::mem::size_of::<Entry>()
        + std::mem::size_of::<Closure>()
        + roles
            .iter()
            .map(|r| 128 + r.user.capacity() + r.host.capacity())
            .sum::<usize>()
        + ctx.authenticated_as.user.capacity()
        + ctx.authenticated_as.host.capacity();
    let Some(charge) =
        ControlledMemoryCharge::try_new_optional(ControlledMemoryCategory::SystemMetadata, bytes)
    else {
        return f(&roles);
    };
    let closure = Arc::new(Closure {
        roles,
        _charge: charge,
    });
    CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if cache.len() == 64 {
            cache.pop_back();
        }
        cache.push_front(Entry {
            graph: auth.role_edges.weak(),
            accounts: auth.accounts.weak(),
            policy,
            principal: ctx.authenticated_as.clone(),
            seed: ctx.active_roles.clone(),
            closure: closure.clone(),
        });
    });
    f(&closure.roles)
}
