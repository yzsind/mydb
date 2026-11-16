//! Sampled native probe points for transaction lock tenure. They reuse the
//! existing lock-diagnostics switch and retain no production event buffer.

#[cfg(not(test))]
use std::sync::OnceLock;

pub(crate) const GRANT: u64 = 1;
pub(crate) const REENTER: u64 = 2;
pub(crate) const RELEASE: u64 = 3;
pub(crate) const WAIT: u64 = 4;
pub(crate) const ELIGIBLE: u64 = 5;
pub(crate) const CANCEL_WAIT: u64 = 6;
pub(crate) const UNIQUE_WAIT: u64 = 7;
pub(crate) const UNIQUE_WAIT_END: u64 = 8;
pub(crate) const UNIQUE_CHECK_PASSED: u64 = 9;
pub(crate) const STATEMENT_BEGIN: u64 = 10;
pub(crate) const STATEMENT_END: u64 = 11;
pub(crate) const READ_BEGIN: u64 = 12;
pub(crate) const READ_END: u64 = 13;
pub(crate) const ATTACH: u64 = 14;
pub(crate) const UNIQUE_BLOCKER: u64 = 15;
pub(crate) const UNIQUE_OWNER_RELEASED: u64 = 16;
pub(crate) const COMMIT_BEGIN: u64 = 20;
pub(crate) const COMMIT_END: u64 = 21;

#[inline]
pub(crate) fn sampled(txn: u64) -> bool {
    txn != 0 && txn.wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 60 == 0
}

#[inline]
fn enabled() -> bool {
    #[cfg(test)]
    return crate::util::env::var_os("MYDB_LOCK_DIAGNOSTICS").is_some();
    #[cfg(not(test))]
    {
        static ENABLED: OnceLock<bool> = OnceLock::new();
        *ENABLED.get_or_init(|| crate::util::env::var_os("MYDB_LOCK_DIAGNOSTICS").is_some())
    }
}

#[inline]
pub(crate) fn tracing(txn: u64) -> bool {
    sampled(txn) && enabled()
}

/// A stable calling convention for native probes, not a SQL/protocol entry.
/// Row events carry table/row/mode; statement and socket events carry the
/// connection id in `object`. A zero scope is resolved through that connection.
#[no_mangle]
#[inline(never)]
pub(crate) extern "C" fn mydb_lock_diagnostic_event(
    kind: u64,
    scope: u64,
    txn: u64,
    object: u64,
    row: u64,
    detail: u64,
) {
    std::hint::black_box((kind, scope, txn, object, row, detail));
    #[cfg(test)]
    EVENTS.with(|events| {
        events
            .borrow_mut()
            .push((kind, scope, txn, object, row, detail))
    });
}

#[inline]
pub(crate) fn event(kind: u64, scope: u64, txn: u64, object: u64, row: u64, detail: u64) {
    if tracing(txn) {
        mydb_lock_diagnostic_event(kind, scope, txn, object, row, detail);
    }
}

pub(crate) fn attach(txn: u64, connection: u64) {
    if enabled() {
        mydb_lock_diagnostic_event(ATTACH, 0, txn, connection, 0, 0);
    }
}

pub(crate) fn unique_blocker(
    txn: u64,
    owner: u64,
    table: u64,
    index: &str,
    key: &impl std::hash::Hash,
) {
    unique_event(UNIQUE_BLOCKER, txn, owner, table, index, key);
}

pub(crate) fn unique_owner_released(
    txn: u64,
    owner: u64,
    table: u64,
    index: &str,
    key: &impl std::hash::Hash,
) {
    unique_event(UNIQUE_OWNER_RELEASED, txn, owner, table, index, key);
}

pub(crate) fn unique_check_passed(txn: u64, table: u64, index: &str, key: &impl std::hash::Hash) {
    unique_event(UNIQUE_CHECK_PASSED, txn, 0, table, index, key);
}

fn unique_event(
    kind: u64,
    txn: u64,
    owner: u64,
    table: u64,
    index: &str,
    key: &impl std::hash::Hash,
) {
    if tracing(txn) {
        use std::hash::{Hash, Hasher};
        let mut hash = std::collections::hash_map::DefaultHasher::new();
        let index = crate::storage::naming::normalize_name_cow(index);
        (index.as_ref(), key).hash(&mut hash);
        mydb_lock_diagnostic_event(kind, 0, txn, table, owner, hash.finish());
    }
}

pub(crate) fn statement_begin(scope: u64, txn: u64, connection: u64, sql: &str) {
    if (txn == 0 || sampled(txn)) && enabled() {
        let word = sql.split_ascii_whitespace().next().unwrap_or("");
        let class = ["SELECT", "INSERT", "UPDATE", "DELETE", "COMMIT", "ROLLBACK"]
            .iter()
            .position(|candidate| word.eq_ignore_ascii_case(candidate))
            .map_or(0, |index| index as u64 + 1);
        mydb_lock_diagnostic_event(STATEMENT_BEGIN, scope, txn, connection, 0, class);
    }
}

pub(crate) fn statement_end(scope: u64, txn: u64, connection: u64) {
    if (txn == 0 || sampled(txn)) && enabled() {
        mydb_lock_diagnostic_event(STATEMENT_END, scope, txn, connection, 0, 0);
    }
}

pub(crate) struct Span {
    end: u64,
    scope: u64,
    txn: u64,
    object: u64,
}

impl Span {
    #[inline]
    pub(crate) fn enter(begin: u64, end: u64, scope: u64, txn: u64, object: u64) -> Option<Self> {
        if !sampled(txn) || !enabled() {
            return None;
        }
        mydb_lock_diagnostic_event(begin, scope, txn, object, 0, 0);
        Some(Self {
            end,
            scope,
            txn,
            object,
        })
    }
}

impl Drop for Span {
    fn drop(&mut self) {
        mydb_lock_diagnostic_event(self.end, self.scope, self.txn, self.object, 0, 0);
    }
}

#[cfg(test)]
thread_local! {
    static EVENTS: std::cell::RefCell<Vec<(u64, u64, u64, u64, u64, u64)>> = const { std::cell::RefCell::new(Vec::new()) };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unique_events_join_normalized_keys_and_include_uncontended_checks() {
        let _enabled = crate::util::env::scoped_vars(&[("MYDB_LOCK_DIAGNOSTICS", Some("1"))]);
        let txn = (1..1024).find(|txn| sampled(*txn)).unwrap();
        EVENTS.with(|events| events.borrow_mut().clear());
        unique_blocker(txn, 42, 9, "PRIMARY", &vec![1u8, 2]);
        unique_owner_released(txn, 42, 9, "primary", &vec![1u8, 2]);
        unique_check_passed(txn, 9, "Primary", &vec![1u8, 2]);
        EVENTS.with(|events| {
            let events = events.borrow();
            assert_eq!(events.len(), 3);
            assert_eq!(events[0].0, UNIQUE_BLOCKER);
            assert_eq!(events[1].0, UNIQUE_OWNER_RELEASED);
            assert_eq!(events[2].0, UNIQUE_CHECK_PASSED);
            assert_eq!(events[0].5, events[1].5);
            assert_eq!(events[0].5, events[2].5);
            assert_eq!(events[2].4, 0);
        });
    }

    #[test]
    fn diagnostic_span_closes_on_unwind_and_disabled_path_records_nothing() {
        let txn = (1..1024).find(|txn| sampled(*txn)).unwrap();
        let _disabled = crate::util::env::scoped_vars(&[("MYDB_LOCK_DIAGNOSTICS", None)]);
        assert!(Span::enter(READ_BEGIN, READ_END, 0, txn, 7).is_none());
        assert!(EVENTS.with(|events| events.borrow().is_empty()));
        let _enabled = crate::util::env::scoped_vars(&[("MYDB_LOCK_DIAGNOSTICS", Some("1"))]);
        let result = std::panic::catch_unwind(|| {
            let _span = Span::enter(READ_BEGIN, READ_END, 0, txn, 7).unwrap();
            panic!("cancel probe scope");
        });
        assert!(result.is_err());
        EVENTS.with(|events| {
            let events = events.borrow();
            assert_eq!(events.len(), 2);
            assert_eq!(events[0], (READ_BEGIN, 0, txn, 7, 0, 0));
            assert_eq!(events[1], (READ_END, 0, txn, 7, 0, 0));
        });
    }
}
