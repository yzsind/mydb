use super::*;
#[test]
fn error_admission_preserves_dimension_priority_and_updates_full_maps() {
    let session = SessionContext::default();
    let err = crate::error::catalog::ER_BAD_FIELD_ERROR.error("first");
    let second = crate::error::catalog::ER_PARSE_ERROR.error("second");
    for limit in 0..=10 {
        let mut state = ErrorSummary::default();
        state.max_keys = limit;
        let first = ErrorSample {
            error: &err,
            raised: true,
            handled: false,
            now: "first timestamp",
        };
        state.record(&session, &first);
        let admitted = limit.min(5);
        assert_eq!(state.total_keys, admitted);
        assert_eq!(
            [
                state.global.len(),
                state.by_account.len(),
                state.by_host.len(),
                state.by_thread.len(),
                state.by_user.len(),
            ],
            std::array::from_fn::<_, 5, _>(|index| usize::from(index < admitted))
        );
        state.record(
            &session,
            &ErrorSample {
                handled: true,
                now: "later timestamp",
                ..first
            },
        );
        for stat in state
            .global
            .values()
            .chain(state.by_account.values())
            .chain(state.by_host.values())
            .chain(state.by_thread.values())
            .chain(state.by_user.values())
        {
            assert_eq!((stat.raised, stat.handled), (2, 1));
            assert_eq!(stat.first_seen, "first timestamp");
            assert_eq!(stat.last_seen, "later timestamp");
        }
        assert_eq!(state.total_keys, admitted);
        state.record(
            &session,
            &ErrorSample {
                error: &second,
                ..first
            },
        );
        assert_eq!(state.total_keys, limit);
        assert_eq!(
            state.total_keys,
            state.global.len()
                + state.by_account.len()
                + state.by_host.len()
                + state.by_thread.len()
                + state.by_user.len()
        );
        assert_eq!(state.global.contains_key(&second.code()), limit > 5);
        assert_eq!(
            state.by_account.contains_key(&(
                session.user.clone(),
                session.host.clone(),
                second.code()
            )),
            limit > 6
        );
        assert_eq!(
            state
                .by_host
                .contains_key(&(session.host.clone(), second.code())),
            limit > 7
        );
        assert_eq!(
            state
                .by_thread
                .contains_key(&(u64::from(session.connection_id), second.code())),
            limit > 8
        );
        assert_eq!(
            state
                .by_user
                .contains_key(&(session.user.clone(), second.code())),
            limit > 9
        );
    }
}

#[test]
fn error_counters_saturate_and_keep_original_metadata() {
    let error = crate::error::catalog::ER_BAD_FIELD_ERROR.error("column");
    let first = ErrorSample {
        error: &error,
        raised: true,
        handled: false,
        now: "2026-10-09 01:00:00.000001",
    };
    let mut stat = ErrorStat::new(&first);
    stat.raised = u64::MAX;
    stat.handled = u64::MAX - 1;
    stat.record(&ErrorSample {
        handled: true,
        now: "2026-10-09 01:00:00.000002",
        ..first
    });
    stat.record(&ErrorSample {
        handled: true,
        now: "2026-10-09 01:00:00.000003",
        ..first
    });
    assert_eq!((stat.raised, stat.handled), (u64::MAX, u64::MAX));
    assert_eq!(stat.first_seen, first.now);
    assert_eq!(stat.last_seen, "2026-10-09 01:00:00.000003");
    assert_eq!(stat.code, error.code());
    assert_eq!(stat.sql_state, error.sql_state().as_str());
}

#[test]
fn custom_error_rows_share_exact_suffix_across_all_dimensions() {
    reset();
    let mut session = SessionContext::default();
    session.connection_id = 12345;
    session.user = "用户甲".into();
    session.host = "::1".into();
    let err = QueryError::user_defined(60_000, "45001", "custom condition").unwrap();
    record_raised(&session, &err);
    record_handled(&session, &err);
    let global = global_rows_filtered(Some(60_000));
    assert_eq!(global.len(), 1);
    assert_eq!(global[0].len(), 7);
    assert_eq!(
        &global[0][..5],
        &[
            Value::Int(60_000),
            s("MYDB_ER_USER_DEFINED_CONDITION"),
            s("45001"),
            Value::UInt(2),
            Value::UInt(1)
        ]
    );
    for (rows, prefix) in [
        (account_rows(), vec![s(&session.user), s(&session.host)]),
        (host_rows(), vec![s(&session.host)]),
        (
            thread_rows(),
            vec![Value::UInt(u64::from(session.connection_id))],
        ),
        (user_rows(), vec![s(&session.user)]),
    ] {
        assert_eq!(rows.len(), 1);
        assert_eq!(&rows[0][..prefix.len()], prefix.as_slice());
        assert_eq!(&rows[0][prefix.len()..], global[0].as_slice());
    }
    unregister_thread(u64::from(session.connection_id));
    assert!(thread_rows().is_empty());
    assert_eq!(summary().read().total_keys, 4);
    assert_eq!(global_rows_filtered(Some(60_000)), global);
}

#[test]
fn pfs_audit_error_truncate_preserves_other_dimensions_and_capacity() {
    reset();
    let session = SessionContext::default();
    let err = crate::error::catalog::ER_BAD_FIELD_ERROR.error("Unknown column");
    record_raised(&session, &err);
    let global = global_rows_filtered(Some(1054));
    let users = user_rows();
    truncate("events_errors_summary_by_thread_by_error");
    assert!(thread_rows().is_empty());
    assert_eq!(global_rows_filtered(Some(1054)), global);
    assert_eq!(user_rows(), users);
    assert_eq!(summary().read().total_keys, 4);
    record_raised(&session, &err);
    assert_eq!(summary().read().total_keys, 5);

    truncate("events_errors_summary_global_by_error");
    assert!(account_rows().is_empty());
    assert!(host_rows().is_empty());
    assert!(thread_rows().is_empty());
    assert!(user_rows().is_empty());
    let global = global_rows_filtered(Some(1054));
    assert_eq!(global.len(), 1);
    assert_eq!(
        &global[0][3..],
        &[Value::UInt(0), Value::UInt(0), Value::Null, Value::Null]
    );
    assert_eq!(summary().read().total_keys, 0);

    record_raised(&session, &err);
    assert_eq!(summary().read().total_keys, 5);
    assert_eq!(global_rows_filtered(Some(1054))[0][3], Value::UInt(1));
    assert_eq!(thread_rows()[0][4], Value::UInt(1));
}

#[test]
fn disconnect_only_removes_matching_error_keys_and_releases_their_capacity() {
    reset();
    let err = crate::error::catalog::ER_BAD_FIELD_ERROR.error("column");
    let sample = ErrorSample {
        error: &err,
        raised: true,
        handled: false,
        now: "timestamp",
    };
    {
        let mut state = summary().write();
        state.global.insert(err.code(), ErrorStat::new(&sample));
        for thread in [0, 1, 7, u64::MAX] {
            for code in [60000, 60001, u16::MAX] {
                let error = QueryError::user_defined(code, "45001", "custom").unwrap();
                state.by_thread.insert(
                    (thread, code),
                    ErrorStat::new(&ErrorSample {
                        error: &error,
                        ..sample
                    }),
                );
            }
        }
        state.total_keys = 13;
    }
    for (target, remaining) in [(1, 10), (u64::MAX, 7), (99, 7), (0, 4), (7, 1)] {
        unregister_thread(target);
        let state = summary().read();
        assert_eq!(state.total_keys, remaining);
        assert_eq!(state.total_keys, state.by_thread.len() + state.global.len());
        assert!(state.by_thread.keys().all(|(thread, _)| *thread != target));
        assert_eq!(state.global.get(&err.code()).unwrap().raised, 1);
    }
}

#[test]
fn thread_key_removal_matches_filtering_across_sparse_and_dense_ranges() {
    for size in [0, 1, 64, 65, 128, 256] {
        for target in [1, u64::MAX] {
            for owned in [0, 1, 16, 17, size / 2, size] {
                let mut entries: BTreeMap<_, _> = (0..size)
                    .map(|index| {
                        let thread = if index < owned {
                            target
                        } else if index % 2 == 0 {
                            0
                        } else {
                            2
                        };
                        let code = if index == 0 { u16::MAX } else { index as u16 };
                        ((thread, code), index)
                    })
                    .collect();
                let before = entries.len();
                let mut expected = entries.clone();
                expected.retain(|(thread, _), _| *thread != target);
                let removed = remove_thread_entries(&mut entries, target);
                assert_eq!(
                    entries, expected,
                    "size={size}, target={target}, owned={owned}"
                );
                assert_eq!(removed, before - expected.len());
            }
        }
    }
}
