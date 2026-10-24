use super::*;
use crate::engine::status_counters::SessionCounter;

thread_local! {
    static AFTER_RETIRED: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

pub(super) fn after_retired_snapshot() {
    if let Some(hook) = AFTER_RETIRED.with(|hook| hook.borrow_mut().take()) {
        hook();
    }
}

type Snapshot = fn(&SessionRegistry) -> Vec<StatusGroupSnapshot>;
const SNAPSHOTS: [Snapshot; 3] = [
    SessionRegistry::account_status_snapshots,
    SessionRegistry::host_status_snapshots,
    SessionRegistry::user_status_snapshots,
];

#[test]
fn group_status_never_loses_a_connection_between_retired_and_live_snapshots() {
    for snapshot in SNAPSHOTS {
        for truncate in [false, true] {
            let registry = SessionRegistry::new();
            let handle =
                registry.register(7, "reader".into(), "127.0.0.1:1234".into(), "test".into());
            handle.status.add(SessionCounter::Questions, 11);
            let interleaved = registry.clone();
            AFTER_RETIRED.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    // Force the lifecycle transition in the snapshot gap if
                    // the reader leaves one. A consistent reader holds the
                    // registry lock, so the transition must occur afterward.
                    let can_write = interleaved.sessions.try_write().is_some();
                    if can_write {
                        if truncate {
                            interleaved.truncate_thread_status();
                        } else {
                            interleaved.unregister(7);
                        }
                    }
                }));
            });
            let rows = snapshot(&registry);
            assert_eq!(
                rows.len(),
                1,
                "a lifecycle transition lost the entire group"
            );
            assert_eq!(rows[0].status.value(SessionCounter::Questions), 11);
            if truncate {
                registry.truncate_thread_status();
            } else {
                registry.unregister(7);
            }
            let rows = snapshot(&registry);
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].status.value(SessionCounter::Questions), 11);
        }
    }
}

#[test]
fn group_status_preserves_dimensions_normalization_and_reset_semantics() {
    let registry = SessionRegistry::new();
    for (id, user, host, questions) in [
        (1, "alice", "localhost:1234", 4),
        (2, "alice", "localhost:4321", 7),
        (3, "alice", "[::1]:3306", 11),
        (4, "bob", "localhost:8888", 13),
        (5, "background", "127.0.0.1:9000", 100),
    ] {
        let handle = registry.register(id, user.into(), host.into(), "test".into());
        handle.status.add(SessionCounter::Questions, questions);
        if id == 5 {
            handle.entry.lock().kind = ProcessKind::Background;
        }
    }
    registry.unregister(2);
    let simplify = |rows: Vec<StatusGroupSnapshot>| {
        rows.into_iter()
            .map(|row| {
                (
                    row.user,
                    row.host,
                    row.status.value(SessionCounter::Questions),
                )
            })
            .collect::<Vec<_>>()
    };
    let accounts = simplify(registry.account_status_snapshots());
    assert_eq!(
        accounts,
        vec![
            (Some("alice".into()), Some("::1".into()), 11),
            (Some("alice".into()), Some("localhost".into()), 11),
            (Some("bob".into()), Some("localhost".into()), 13),
        ]
    );
    let hosts = simplify(registry.host_status_snapshots());
    assert_eq!(
        hosts,
        vec![
            (None, Some("::1".into()), 11),
            (None, Some("localhost".into()), 24)
        ]
    );
    let users = simplify(registry.user_status_snapshots());
    assert_eq!(
        users,
        vec![
            (Some("alice".into()), None, 22),
            (Some("bob".into()), None, 13)
        ]
    );
    assert_eq!(registry.thread_status_snapshots().len(), 3);
    registry.truncate_thread_status();
    assert!(registry
        .thread_status_snapshots()
        .iter()
        .all(|r| r.status.value(SessionCounter::Questions) == 0));
    assert_eq!(simplify(registry.account_status_snapshots()), accounts);
    assert_eq!(simplify(registry.host_status_snapshots()), hosts);
    assert_eq!(simplify(registry.user_status_snapshots()), users);
    registry.reset_account_status();
    assert!(registry
        .account_status_snapshots()
        .iter()
        .all(|r| r.status.value(SessionCounter::Questions) == 0));
    assert_eq!(simplify(registry.host_status_snapshots()), hosts);
    assert_eq!(simplify(registry.user_status_snapshots()), users);
    registry.reset_host_status();
    registry.reset_user_status();
    for snapshot in SNAPSHOTS {
        assert!(snapshot(&registry)
            .iter()
            .all(|r| r.status.value(SessionCounter::Questions) == 0));
    }
}

#[test]
fn group_status_saturates_and_keeps_empty_identity_keys() {
    let registry = SessionRegistry::new();
    for (id, count) in [(1, u64::MAX), (2, 7)] {
        let handle = registry.register(id, "".into(), "".into(), "test".into());
        handle.status.add(SessionCounter::BytesSent, count);
        handle.status.add(SessionCounter::RowsRead, count);
        if id == 1 {
            registry.unregister(id);
        }
    }
    for (index, snapshot) in SNAPSHOTS.into_iter().enumerate() {
        let rows = snapshot(&registry);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].user.as_deref(),
            if index == 1 { None } else { Some("") }
        );
        assert_eq!(
            rows[0].host.as_deref(),
            if index == 2 { None } else { Some("") }
        );
        assert_eq!(rows[0].status.value(SessionCounter::BytesSent), u64::MAX);
        assert_eq!(rows[0].status.value(SessionCounter::RowsRead), u64::MAX);
    }
}

#[test]
fn concurrent_retirement_and_thread_truncation_preserve_group_totals() {
    let registry = SessionRegistry::new();
    for id in 1..=32 {
        let handle = registry.register(
            id,
            format!("user_{}", id % 3),
            format!("127.0.0.{}:{}", id % 4, 3000 + id),
            "test".into(),
        );
        handle.status.add(SessionCounter::Questions, id as u64);
    }
    let barrier = std::sync::Barrier::new(4);
    std::thread::scope(|scope| {
        for snapshot in SNAPSHOTS {
            let registry = &registry;
            let barrier = &barrier;
            scope.spawn(move || {
                barrier.wait();
                for _ in 0..100 {
                    let total: u64 = snapshot(registry)
                        .iter()
                        .map(|row| row.status.value(SessionCounter::Questions))
                        .sum();
                    assert_eq!(total, 528);
                    std::thread::yield_now();
                }
            });
        }
        barrier.wait();
        for id in 1..=32 {
            if id % 4 == 0 {
                registry.truncate_thread_status();
            }
            registry.unregister(id);
            std::thread::yield_now();
        }
    });
    for snapshot in SNAPSHOTS {
        let total: u64 = snapshot(&registry)
            .iter()
            .map(|row| row.status.value(SessionCounter::Questions))
            .sum();
        assert_eq!(total, 528);
    }
}
