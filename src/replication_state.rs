// Shared declarations for the two alternative replication backends.
// Included in their module so public paths, generic scope functions and TLS
// keep their original identity without an additional runtime abstraction.

static REPLICA_READ_ONLY: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
// STRICT by default; crash recovery retains its separate idempotent override.
static SLAVE_EXEC_MODE: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);

thread_local! {
    static REPLICATION_APPLIER: std::cell::Cell<bool> = std::cell::Cell::new(false);
}

pub fn replica_read_only() -> bool {
    REPLICA_READ_ONLY.load(std::sync::atomic::Ordering::Acquire)
}

pub fn set_replica_read_only(enabled: bool) {
    REPLICA_READ_ONLY.store(enabled, std::sync::atomic::Ordering::Release);
}

pub fn slave_exec_mode() -> &'static str {
    if SLAVE_EXEC_MODE.load(std::sync::atomic::Ordering::Relaxed) == 1 {
        "IDEMPOTENT"
    } else {
        "STRICT"
    }
}

pub fn set_slave_exec_mode(idempotent: bool) {
    SLAVE_EXEC_MODE.store(u8::from(idempotent), std::sync::atomic::Ordering::Relaxed);
}

pub(crate) fn replication_applier_active() -> bool {
    REPLICATION_APPLIER.with(std::cell::Cell::get)
}

pub(crate) fn is_replica_write_statement(statement: &Statement) -> bool {
    crate::statement_effects::is_replica_forbidden(statement)
}

pub fn is_replication_admin_sql(sql: &str) -> bool {
    // Head guard — only these five keywords ever start a replication
    // admin statement, so reject the rest without allocating an uppercase
    // copy of the full SQL payload.
    let (head_buf, head_len) = crate::parser::extract_first_keyword(sql);
    if !matches!(
        &head_buf[..head_len],
        b"CHANGE" | b"START" | b"STOP" | b"RESET" | b"SHOW"
    ) {
        return false;
    }
    if &head_buf[..head_len] == b"SHOW" {
        return crate::replication_sql::ReplicationShow::classify(sql).is_some();
    }
    let upper = sql.trim().trim_end_matches(';').trim().to_ascii_uppercase();
    upper.starts_with("CHANGE REPLICATION SOURCE TO")
        || upper.starts_with("CHANGE MASTER TO")
        || upper.starts_with("START REPLICA")
        || upper.starts_with("START SLAVE")
        || upper.starts_with("STOP REPLICA")
        || upper.starts_with("STOP SLAVE")
        || upper.starts_with("RESET REPLICA")
        || upper.starts_with("RESET SLAVE")
}

pub(crate) fn with_replication_applier_pub<T>(
    f: impl FnOnce() -> Result<T, QueryError>,
) -> Result<T, QueryError> {
    crate::util::scoped_cell::with_value(&REPLICATION_APPLIER, true, f)
}

#[cfg(feature = "mysql-replica")]
use self::with_replication_applier_pub as with_replication_applier;

#[cfg(test)]
mod scope_tests {
    use super::*;

    #[test]
    fn replication_applier_scope_restores_after_panic() {
        assert!(!replication_applier_active());
        let result = std::panic::catch_unwind(|| {
            let _: Result<(), QueryError> = with_replication_applier_pub(|| {
                assert!(replication_applier_active());
                panic!("scope unwind");
            });
        });
        assert!(result.is_err());
        let leaked = replication_applier_active();
        REPLICATION_APPLIER.with(|flag| flag.set(false));
        assert!(!leaked, "applier privilege must not survive unwinding");
    }

    #[cfg(feature = "mysql-replica")]
    #[test]
    fn recovery_scope_preserves_outer_override_and_restores_after_panic() {
        assert!(!RECOVERY_IDEMPOTENT.with(Cell::get));
        let retained = with_recovery_idempotent(|| {
            with_recovery_idempotent(|| assert!(RECOVERY_IDEMPOTENT.with(Cell::get)));
            RECOVERY_IDEMPOTENT.with(Cell::get)
        });
        assert!(!RECOVERY_IDEMPOTENT.with(Cell::get));
        assert!(
            retained,
            "inner recovery scope must preserve the outer override"
        );
        let result =
            std::panic::catch_unwind(|| with_recovery_idempotent(|| panic!("recovery unwind")));
        assert!(result.is_err());
        let leaked = RECOVERY_IDEMPOTENT.with(Cell::get);
        RECOVERY_IDEMPOTENT.with(|flag| flag.set(false));
        assert!(!leaked);
    }
}
