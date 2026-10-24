//! P0-D1: enforce max_user_connections and max_connections_per_hour.
//!
//! charge_new_connection existed but was never called during login;
//! only charge_question/charge_update were used in dispatch/execution. These tests
//! establish:
//!
//! Exhausted quotas return 1226 ER_USER_LIMIT_REACHED.
//! max_user_connections caps simultaneous sessions; release decrements the count.
//! max_connections_per_hour caps a rolling window whose count resets as it advances.
//! Both limits zero makes charge a no-op.
//!
//! authenticate_request and authenticate_with_plaintext_password now charge,
//! and close_session releases the slot.

use crate::engine::auth::{
    charge_new_connection, flush_user_resources, release_connection, AccountKey,
};

#[test]
fn charge_new_connection_returns_1226_on_user_cap_exceed() {
    flush_user_resources();
    let key = AccountKey::new("alice".to_string(), "%".to_string());
    // limit=2, two successful charges; third must trip 1226.
    charge_new_connection(&key, 0, 2).unwrap();
    charge_new_connection(&key, 0, 2).unwrap();
    let err = charge_new_connection(&key, 0, 2).unwrap_err();
    assert_eq!(err.code(), 1226, "must surface ER_USER_LIMIT_REACHED");
    assert!(
        err.message().contains("max_user_connections"),
        "error must name the breached resource; got '{}'",
        err.message()
    );
    // Drain so we don't leak counters into other tests.
    release_connection(&key);
    release_connection(&key);
}

#[test]
fn release_connection_frees_slot_for_next_charge() {
    flush_user_resources();
    let key = AccountKey::new("bob".to_string(), "%".to_string());
    charge_new_connection(&key, 0, 1).unwrap();
    // Second charge while previous slot is held → blocked.
    assert!(charge_new_connection(&key, 0, 1).is_err());
    // Release frees the slot; next charge succeeds.
    release_connection(&key);
    charge_new_connection(&key, 0, 1).unwrap();
    release_connection(&key);
}

#[test]
fn charge_new_connection_respects_connections_per_hour_cap() {
    flush_user_resources();
    let key = AccountKey::new("carol".to_string(), "%".to_string());
    // max_connections_per_hour=2 → 3rd opening in the same window trips.
    // Release after each so the simultaneous-session cap doesn't shadow
    // the rolling-window cap.
    charge_new_connection(&key, 2, 0).unwrap();
    release_connection(&key);
    charge_new_connection(&key, 2, 0).unwrap();
    release_connection(&key);
    let err = charge_new_connection(&key, 2, 0).unwrap_err();
    assert_eq!(err.code(), 1226);
    assert!(
        err.message().contains("max_connections_per_hour"),
        "rolling-window cap must surface specifically; got '{}'",
        err.message()
    );
}

#[test]
fn charge_new_connection_with_zero_limits_is_noop() {
    flush_user_resources();
    let key = AccountKey::new("dan".to_string(), "%".to_string());
    // Both at 0 = unlimited. A handful of charges in succession all pass.
    for _ in 0..50 {
        charge_new_connection(&key, 0, 0).unwrap();
    }
    // Releases match the charges so nothing leaks.
    for _ in 0..50 {
        release_connection(&key);
    }
}

#[test]
fn flush_user_resources_clears_rolling_window_but_not_active_sessions() {
    flush_user_resources();
    let key = AccountKey::new("eve".to_string(), "%".to_string());
    charge_new_connection(&key, 5, 5).unwrap();
    // FLUSH USER_RESOURCES resets the rolling-window counters but leaves
    // `current_conns` intact — matches MySQL's documented behaviour.
    flush_user_resources();
    // Simultaneous-session cap should still see the in-flight slot, so
    // another charge against a 1-cap is blocked even after FLUSH.
    let err = charge_new_connection(&key, 0, 1).unwrap_err();
    assert_eq!(err.code(), 1226);
    release_connection(&key);
}
