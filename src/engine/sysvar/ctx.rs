//! Read / write contexts threaded through every system variable's
//! backend. Kept narrow so backends can ignore the bits they don't need.

use crate::engine::api::SessionContext;
use crate::storage::Database;

use super::meta::Scope;

/// Context for *reading* a system variable. `session` is optional because
/// some call sites (e.g. the stub engine's `SHOW VARIABLES` path before a
/// `Database` is attached) don't have one. `db` is optional for the same
/// reason — pre-connection lookups happen on the stub engine which has
/// no storage handle.
pub struct ReadCtx<'a> {
    pub session: Option<&'a SessionContext>,
    pub db: Option<&'a Database>,
    /// Whether the caller is asking for the GLOBAL or SESSION value. For
    /// `Both`-scope vars this picks the right storage; for `Session`/
    /// `Global`-only vars the backend ignores this.
    pub scope: Scope,
}

impl<'a> ReadCtx<'a> {
    pub fn new(
        session: Option<&'a SessionContext>,
        db: Option<&'a Database>,
        scope: Scope,
    ) -> Self {
        Self { session, db, scope }
    }

    #[cfg(test)]
    pub fn global(db: Option<&'a Database>) -> Self {
        Self {
            session: None,
            db,
            scope: Scope::Global,
        }
    }

    pub fn session(session: &'a SessionContext, db: Option<&'a Database>) -> Self {
        Self {
            session: Some(session),
            db,
            scope: Scope::Session,
        }
    }
}

/// Context for *writing* a system variable. `session` is always present
/// because:
///   * Session-scope writes flow into it directly.
///   * Global writes use the authenticated session for privilege checks and
///     persistence metadata; they must not change its session-scoped value.
pub struct WriteCtx<'a> {
    pub session: &'a mut SessionContext,
    pub db: Option<&'a Database>,
    /// Effective scope after `SetScope::resolve_for`: Session or Global.
    /// `Both` is not used here.
    pub scope: Scope,
}

impl<'a> WriteCtx<'a> {
    pub fn new(session: &'a mut SessionContext, db: Option<&'a Database>, scope: Scope) -> Self {
        debug_assert!(scope != Scope::Both, "WriteCtx scope must be resolved");
        Self { session, db, scope }
    }

    pub fn is_global(&self) -> bool {
        self.scope == Scope::Global
    }
}
