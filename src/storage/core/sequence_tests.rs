use super::*;
use crate::storage::sequence::{BoundOpt, SeqOptions};

fn open_sequence_database(path: &Path) -> Database {
    let mut config = StorageConfig::from_env();
    config.durability = DurabilityMode::Strict;
    config.buffer_pool_pages = 256;
    config.buffer_pool_io_threads = 1;
    config.group_commit_window = Duration::ZERO;
    config.checkpoint_wal_bytes = u64::MAX;
    config.checkpoint_interval = Duration::from_secs(3600);
    config.startup_validation = StartupValidationMode::Off;
    Database::open_with_config(path, config).unwrap()
}

fn sequence_definition(db: &Database) -> Arc<SequenceSchema> {
    db.resolve_sequence("test", "catalog_sequence").unwrap()
}

#[test]
fn sequence_checkpoint_keeps_reservations_published_after_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Arc::new(open_sequence_database(dir.path()));
        db.create_sequence(
            "test",
            "catalog_sequence",
            false,
            false,
            &SeqOptions::default(),
        )
        .unwrap();
        let def = sequence_definition(&db);
        db.sequence_alloc(&def, 1).unwrap();
        let writer = db.clone();
        SEQUENCE_CHECKPOINT_TEST_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                writer.sequence_setval(&def, 500, false).unwrap()
            }));
        });
        db.checkpoint().unwrap();
        assert!(LogManager::scan(dir.path())
            .unwrap()
            .records
            .iter()
            .any(|record| matches!(
                record.payload,
                WalPayload::SequenceState {
                    resume_value: 500,
                    ..
                }
            )));
    }
    let db = open_sequence_database(dir.path());
    assert_eq!(
        db.sequence_alloc(&sequence_definition(&db), 1)
            .unwrap()
            .start,
        500
    );
}

#[test]
fn sequence_retirement_proves_dropped_wal_and_is_reclaimed_after_coverage() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_sequence_database(dir.path());
        db.create_sequence(
            "test",
            "catalog_sequence",
            false,
            false,
            &SeqOptions::default(),
        )
        .unwrap();
        let def = sequence_definition(&db);
        db.sequence_alloc(&def, 1).unwrap();
        db.drop_sequences("test", &["catalog_sequence".into()], false)
            .unwrap();
        assert_eq!(
            db.scan_dd_table(crate::storage::data_dictionary::DD_SEQUENCE_RETIREMENTS)
                .unwrap()
                .len(),
            1
        );
    }
    let db = open_sequence_database(dir.path());
    assert!(db.resolve_sequence("test", "catalog_sequence").is_err());
    db.checkpoint().unwrap();
    db.checkpoint().unwrap();
    assert!(db
        .scan_dd_table(crate::storage::data_dictionary::DD_SEQUENCE_RETIREMENTS)
        .unwrap()
        .is_empty());
    assert!(db
        .apply_sequence_value(SequenceValue {
            sequence_id: 999,
            counter_id: 999,
            resume_value: 1,
            is_called: true,
            state_lsn: 1
        })
        .is_err());
}

#[test]
fn sequence_catalog_checkpoint_roundtrip_without_sidecar() {
    let dir = tempfile::tempdir().unwrap();
    let max;
    {
        let db = open_sequence_database(dir.path());
        db.create_sequence(
            "test",
            "catalog_sequence",
            false,
            false,
            &SeqOptions::default(),
        )
        .unwrap();
        let def = sequence_definition(&db);
        let initial = db.read_sequence_value(def.counter_id).unwrap().unwrap();
        assert_eq!(
            (initial.resume_value, initial.is_called, initial.state_lsn),
            (1, false, 0)
        );
        for value in 1..=100 {
            assert_eq!(db.sequence_alloc(&def, 1).unwrap().start, value);
        }
        let scan = LogManager::scan(dir.path()).unwrap();
        let reservations: Vec<_> = scan
            .records
            .iter()
            .filter(|r| matches!(r.payload, WalPayload::SequenceState { .. }))
            .collect();
        assert_eq!(reservations.len(), 4);
        assert!(!scan.records.iter().any(
            |r| matches!(&r.payload, WalPayload::FileCreate { path, .. } if path.ends_with(".seq"))
        ));
        max = db.sequences.get(def.id).unwrap().logged.lock().resume_value;
        assert_eq!(max, 132);
        db.checkpoint().unwrap();
        assert_eq!(
            db.read_sequence_value(def.counter_id)
                .unwrap()
                .unwrap()
                .resume_value,
            max
        );
        assert!(db.sequence_dirty.lock().entries.is_empty());
        // A removed dirty item must become eligible for the next epoch.
        for _ in 0..40 {
            db.sequence_alloc(&def, 1).unwrap();
        }
        assert!(!db.sequence_dirty.lock().entries.is_empty());
        db.checkpoint().unwrap();
        assert!(db.sequence_dirty.lock().entries.is_empty());
    }
    let db = open_sequence_database(dir.path());
    assert!(
        db.sequence_alloc(&sequence_definition(&db), 1)
            .unwrap()
            .start
            > 140
    );
}

#[test]
fn sequence_catalog_recovery_uses_lsn_for_descending_setval() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_sequence_database(dir.path());
        let options = SeqOptions {
            increment: Some(-3),
            min_value: BoundOpt::Value(-10000),
            ..Default::default()
        };
        db.create_sequence("test", "catalog_sequence", false, false, &options)
            .unwrap();
        let def = sequence_definition(&db);
        assert_eq!(db.sequence_alloc(&def, 1).unwrap().start, -1);
        db.sequence_setval(&def, -2000, true).unwrap();
        db.checkpoint().unwrap();
        db.sequence_setval(&def, -5, false).unwrap();
    }
    let db = open_sequence_database(dir.path());
    assert_eq!(
        db.sequence_alloc(&sequence_definition(&db), 1)
            .unwrap()
            .start,
        -5
    );
}

#[test]
fn sequence_catalog_restart_is_atomic_and_rename_preserves_state() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_sequence_database(dir.path());
    db.create_sequence(
        "test",
        "catalog_sequence",
        false,
        false,
        &SeqOptions::default(),
    )
    .unwrap();
    let old = sequence_definition(&db);
    db.sequence_alloc(&old, 10).unwrap();
    db.fail_next_catalog_commit_for_test();
    assert!(db
        .alter_sequence(
            "test",
            "catalog_sequence",
            false,
            &SeqOptions::default(),
            Some(Some(900)),
            None
        )
        .is_err());
    assert_eq!(sequence_definition(&db).counter_id, old.counter_id);
    assert_eq!(db.sequence_alloc(&old, 1).unwrap().start, 11);
    db.alter_sequence(
        "test",
        "catalog_sequence",
        false,
        &SeqOptions::default(),
        Some(Some(900)),
        None,
    )
    .unwrap();
    let new = sequence_definition(&db);
    assert_ne!(new.counter_id, old.counter_id);
    assert_eq!(new.id, old.id);
    assert!(db.read_sequence_value(old.counter_id).unwrap().is_none());
    assert_eq!(db.sequence_alloc(&new, 1).unwrap().start, 900);
    db.alter_sequence(
        "test",
        "catalog_sequence",
        false,
        &SeqOptions::default(),
        None,
        Some("renamed_sequence"),
    )
    .unwrap();
    let renamed = db.resolve_sequence("test", "renamed_sequence").unwrap();
    assert_eq!(renamed.counter_id, new.counter_id);
    assert_eq!(db.sequence_alloc(&renamed, 1).unwrap().start, 901);
}

#[test]
fn sequence_catalog_missing_state_is_corruption_not_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_sequence_database(dir.path());
    db.create_sequence(
        "test",
        "catalog_sequence",
        false,
        false,
        &SeqOptions::default(),
    )
    .unwrap();
    let def = sequence_definition(&db);
    let mut txn = CatalogTxn::new(&db);
    txn.replace(
        crate::storage::data_dictionary::DD_SEQUENCE_VALUES,
        CatalogRowSelector::PrimaryValues(vec![Value::UInt(def.counter_id)]),
        Vec::new(),
    );
    txn.commit().unwrap();
    db.sequences.remove(def.id);
    let err = db.sequence_alloc(&def, 1).unwrap_err();
    assert!(err.message().contains("missing state"));
}

#[test]
fn sequence_replica_tracker_bounds_hot_generation_and_keeps_uncovered_tail() {
    let mut tracker = ReplicaRestartpointTracker::new(false);
    for lsn in 1..10000 {
        tracker.register_sequence(7, lsn).unwrap();
    }
    assert_eq!(tracker.sequence_lsns.len(), 1);
    assert_eq!(tracker.sequence_floor(), Some(1));
    tracker.cover_sequence_before(5000);
    assert_eq!(tracker.sequence_floor(), Some(5000));
    tracker.cover_sequence_before(10000);
    assert_eq!(tracker.sequence_floor(), None);
}

#[test]
fn sequence_online_apply_reorders_values_by_lsn_and_promotes_without_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_sequence_database(dir.path());
    db.create_sequence(
        "test",
        "catalog_sequence",
        false,
        false,
        &SeqOptions::default(),
    )
    .unwrap();
    let def = sequence_definition(&db);
    let mut records = Vec::new();
    for (value, called) in [(1000, true), (7, false)] {
        let payload = WalPayload::SequenceState {
            sequence_id: def.id,
            counter_id: def.counter_id,
            resume_value: value,
            is_called: called,
            logged_unix_us: 1,
        };
        let lsn = db.append_wal(0, payload.clone()).unwrap();
        records.push(WalRecord {
            source_time_us: 0,
            lsn,
            txn_id: 0,
            prev_txn_lsn: 0,
            payload,
        });
    }
    db.set_physical_replica_mode(true).unwrap();
    for record in &records {
        db.apply_wal_record(record).unwrap();
    }
    db.prepare_sequence_promotion().unwrap();
    db.set_physical_replica_mode(false).unwrap();
    assert_eq!(db.sequence_alloc(&def, 1).unwrap().start, 7);
}

#[test]
fn sequence_concurrent_allocations_are_disjoint_during_checkpoints() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_sequence_database(dir.path());
    db.create_sequence(
        "test",
        "catalog_sequence",
        false,
        false,
        &SeqOptions::default(),
    )
    .unwrap();
    let def = sequence_definition(&db);
    let all = std::thread::scope(|scope| {
        let cp = scope.spawn(|| {
            for _ in 0..8 {
                db.checkpoint().unwrap();
            }
        });
        let handles: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    (0..100)
                        .map(|_| db.sequence_alloc(&def, 7).unwrap())
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let all = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect::<Vec<_>>();
        cp.join().unwrap();
        all
    });
    let values: HashSet<_> = all
        .iter()
        .flat_map(|a| (0..a.count).map(|n| a.start + n * a.increment))
        .collect();
    assert_eq!(values.len(), 8 * 100 * 7);
    db.checkpoint().unwrap();
    let persisted = db.read_sequence_value(def.counter_id).unwrap().unwrap();
    assert!(persisted.resume_value >= *values.iter().max().unwrap());
}

#[test]
fn sequence_catalog_codec_rejects_invalid_state() {
    let value = SequenceValue {
        sequence_id: 3,
        counter_id: 7,
        resume_value: i64::MIN,
        is_called: false,
        state_lsn: 99,
    };
    assert_eq!(SequenceValue::from_row(&value.to_row()).unwrap(), value);
    for (index, bad) in [
        (0, Value::UInt(0)),
        (1, Value::Null),
        (2, Value::UInt(u64::MAX)),
        (3, Value::UInt(2)),
        (4, Value::Int(-1)),
    ] {
        let mut row = value.to_row();
        row[index] = bad;
        assert!(SequenceValue::from_row(&row).is_err());
    }
}
