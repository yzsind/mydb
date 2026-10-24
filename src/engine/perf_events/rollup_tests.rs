use super::*;

type Rows = fn() -> Vec<Vec<Value>>;

fn dimensions() -> [(&'static str, Rows, usize); 6] {
    [
        (
            "events_statements_summary_by_user_by_event_name",
            statement_summary_by_user_rows,
            2,
        ),
        (
            "events_statements_summary_by_host_by_event_name",
            statement_summary_by_host_rows,
            2,
        ),
        (
            "events_statements_summary_by_account_by_event_name",
            statement_summary_by_account_rows,
            3,
        ),
        (
            "events_waits_summary_by_user_by_event_name",
            wait_summary_by_user_rows,
            2,
        ),
        (
            "events_waits_summary_by_host_by_event_name",
            wait_summary_by_host_rows,
            2,
        ),
        (
            "events_waits_summary_by_account_by_event_name",
            wait_summary_by_account_rows,
            3,
        ),
    ]
}

fn record_sample(session: &SessionContext, sql: &str) {
    record_statement(
        session,
        sql,
        Duration::from_micros(10),
        QueryLogMetadata {
            lock_wait_count: 1,
            lock_wait_ms: 1.0,
            ..Default::default()
        },
        &Ok(QueryResponse::Ok),
    );
}

fn sessions() -> Vec<SessionContext> {
    [("alice", "host-a"), ("alice", "host-b"), ("bob", "host-a")]
        .into_iter()
        .enumerate()
        .map(|(index, (user, host))| {
            let mut session = SessionContext::default();
            session.connection_id = 7_810_001 + index as u32;
            session.user = user.into();
            session.host = host.into();
            for _ in 0..=index {
                record_sample(&session, "SELECT 1");
            }
            record_sample(&session, "UPDATE t SET a=1");
            session
        })
        .collect()
}

#[test]
fn account_keys_and_counts_survive_partial_disconnect_and_reconnect() {
    reset();
    let sessions = sessions();
    let select = "statement/sql/select";
    let users = statement_by_user_snapshot();
    assert_eq!(users[&("alice".into(), select)].metrics.count_star, 3);
    assert_eq!(users[&("bob".into(), select)].metrics.count_star, 3);
    let hosts = statement_by_host_snapshot();
    assert_eq!(hosts[&("host-a".into(), select)].metrics.count_star, 4);
    assert_eq!(hosts[&("host-b".into(), select)].metrics.count_star, 2);
    let accounts = statement_by_account_snapshot();
    assert_eq!(
        accounts[&("alice".into(), "host-a".into(), select)]
            .metrics
            .count_star,
        1
    );
    assert_eq!(
        accounts[&("alice".into(), "host-b".into(), select)]
            .metrics
            .count_star,
        2
    );
    let waits = wait_by_user_snapshot();
    assert_eq!(waits[&("alice".into(), WAIT_EVENT_NAME)].count_star, 5);
    assert_eq!(waits[&("bob".into(), WAIT_EVENT_NAME)].count_star, 4);
    let dimensions = dimensions();
    let before: Vec<_> = dimensions.iter().map(|(_, rows, _)| rows()).collect();
    for session in &sessions {
        unregister_thread(session.connection_id as u64);
        for (index, (name, rows, _)) in dimensions.iter().enumerate() {
            assert_eq!(rows(), before[index], "{name} after disconnect");
        }
    }
    let mut reconnected = sessions[0].clone();
    reconnected.user = "用户甲".into();
    reconnected.host = "host-c".into();
    record_sample(&reconnected, "SELECT 1");
    let users = statement_by_user_snapshot();
    assert_eq!(users[&("alice".into(), select)].metrics.count_star, 3);
    assert_eq!(users[&("用户甲".into(), select)].metrics.count_star, 1);
    assert_eq!(
        wait_by_account_snapshot()[&("用户甲".into(), "host-c".into(), WAIT_EVENT_NAME)].count_star,
        1
    );
    unregister_thread(reconnected.connection_id as u64);
}

#[test]
fn account_reset_epochs_stay_independent_through_thread_reset_and_retirement() {
    reset();
    let sessions = sessions();
    let dimensions = dimensions();
    // Retired and live records must obey the same reset epoch.
    unregister_thread(sessions[2].connection_id as u64);
    let before: Vec<_> = dimensions.iter().map(|(_, rows, _)| rows()).collect();
    for table in [
        "events_statements_summary_by_thread_by_event_name",
        "events_waits_summary_by_thread_by_event_name",
    ] {
        assert!(truncate(table));
        for (index, (_, rows, _)) in dimensions.iter().enumerate() {
            assert_eq!(rows(), before[index]);
        }
    }
    for (target, (table, _, count_column)) in dimensions.iter().enumerate() {
        let before: Vec<_> = dimensions.iter().map(|(_, rows, _)| rows()).collect();
        assert!(truncate(table));
        for (index, (name, rows, _)) in dimensions.iter().enumerate() {
            if index == target {
                assert!(rows().is_empty(), "{name}");
            } else {
                assert_eq!(
                    rows(),
                    before[index],
                    "{name} changed while resetting {table}"
                );
            }
        }
        record_sample(&sessions[0], "SELECT 1");
        let refreshed = dimensions[target].1();
        assert_eq!(refreshed.len(), 1);
        assert_eq!(refreshed[0][*count_column], u(1));
    }
    let before: Vec<_> = dimensions.iter().map(|(_, rows, _)| rows()).collect();
    for session in &sessions[..2] {
        unregister_thread(session.connection_id as u64);
    }
    for (index, (name, rows, _)) in dimensions.iter().enumerate() {
        assert_eq!(
            rows(),
            before[index],
            "{name} after retiring separate epochs"
        );
    }
}

#[test]
fn account_snapshot_keeps_counts_when_a_connection_retires_between_shards() {
    use std::sync::mpsc;
    reset();
    let mut target = SessionContext::default();
    target.connection_id = PERF_EVENT_SHARDS as u32 * 1234;
    target.user = "snapshot-target".into();
    let mut witness = target.clone();
    witness.connection_id += 1;
    witness.user = "snapshot-witness".into();
    record_sample(&target, "SELECT 1");
    record_sample(&witness, "SELECT 1");
    let target_id = u64::from(target.connection_id);
    let witness_id = u64::from(witness.connection_id);
    assert_eq!(target_id as usize & (PERF_EVENT_SHARDS - 1), 0);
    assert_eq!(witness_id as usize & (PERF_EVENT_SHARDS - 1), 1);
    let (start_tx, start_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        start_rx.recv().unwrap();
        unregister_thread(target_id);
        done_tx.send(()).unwrap();
    });
    let snapshot = account_summary_snapshot::<_, _, 0>(
        |state| {
            if state.thread_accounts.contains_key(&target_id) {
                start_tx.send(()).unwrap();
            }
            if state.thread_accounts.contains_key(&witness_id) {
                // The previous shard's read guard must already be released.
                // Force retirement to finish before the rest of the snapshot.
                done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            }
            &state.statement_by_user
        },
        |state| &state.statement_by_thread,
        |user, _, event| (user.to_owned(), event),
    );
    worker.join().unwrap();
    assert_eq!(
        snapshot
            .get(&(target.user, "statement/sql/select"))
            .map(|aggregate| aggregate.metrics.count_star)
            .unwrap_or(0),
        1,
        "moving a completed event from live to retired storage must not lose it"
    );
    unregister_thread(witness_id);
}
