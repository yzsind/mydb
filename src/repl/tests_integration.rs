//! End-to-end integration tests for the mydb-native physical
//! replication stack: spin up a master service + a replica receiver in
//! the same process, exchange WAL records, and verify on-disk WAL
//! convergence + slot/progress accounting.

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex as PlMutex;
use tempfile::TempDir;
use tokio::sync::Notify;

use crate::repl::config::ReplConfig;
use crate::repl::master::{self, MasterService, MasterServiceInit, SlotRegistry};
use crate::repl::replica::{Receiver, ReceiverEvent, ReplicaProgress};
use crate::repl::role::generate_uuid_bytes;
use crate::wal::{LogManager, WalHeapInsert, WalPayload};

struct Master {
    log: Arc<crate::wal::LogMutex>,
    service: MasterService,
    _dir: TempDir,
}

/// Physical-replication tests need both nodes to start at WAL LSN 1. Most
/// engine tests can clone nextest's clean catalog seed, but a seeded checkpoint
/// intentionally has a later WAL origin and is therefore not a valid replica
/// fixture.
fn fresh_database_tempdir() -> TempDir {
    let dir = TempDir::new().expect("tempdir");
    std::fs::write(dir.path().join(".mydb-test-fresh-data-dir"), []).unwrap();
    dir
}

async fn initialize_replica_fixture(
    master: std::net::SocketAddr,
    dir: &std::path::Path,
) -> ReplicaProgress {
    let mut role = crate::repl::role::RoleState::new_standalone();
    role.role = crate::repl::role::Role::Replica;
    role.master_endpoint = Some(master.to_string());
    crate::repl::role::save(dir, &role).unwrap();
    crate::repl::base_backup::perform_base_backup_into_with_config(
        &master.ip().to_string(),
        master.port(),
        &role.node_uuid,
        dir,
        ReplConfig::test_defaults(),
    )
    .await
    .unwrap();
    crate::repl::replica::progress::load_progress(dir)
        .unwrap()
        .unwrap()
}

async fn build_master() -> Master {
    let dir = TempDir::new().unwrap();
    let log = Arc::new(parking_lot::Mutex::new(
        LogManager::open(dir.path()).unwrap(),
    ));
    log.lock().set_max_segment_bytes(8192);
    let slots = Arc::new(PlMutex::new(SlotRegistry::open(dir.path()).unwrap()));
    let service = master::start_master_service(MasterServiceInit {
        bind_host: "127.0.0.1".into(),
        bind_port: 0,
        config: ReplConfig {
            heartbeat: Duration::from_millis(10),
            ..ReplConfig::test_defaults()
        },
        master_uuid: generate_uuid_bytes(),
        epoch: 1,
        server_version: "test-master".into(),
        log: Arc::clone(&log),
        slots,
        db: None,
        data_dir: None,
        role_state: None,
    })
    .await
    .unwrap();
    Master {
        log,
        service,
        _dir: dir,
    }
}

fn append_record(log: &Arc<crate::wal::LogMutex>, txn_id: u64, marker: u8) -> u64 {
    let mut g = log.lock();
    g.append(
        txn_id,
        WalPayload::HeapInsertBatch {
            path: "tables/1.heap".into(),
            inserts: vec![WalHeapInsert {
                page_id: 1,
                slot_id: 0,
                payload: vec![marker; 128],
                undo_lsn: None,
            }],
        },
    )
    .unwrap()
}

fn append_commit(log: &Arc<crate::wal::LogMutex>, txn_id: u64, commit_scn: u64) -> u64 {
    let mut g = log.lock();
    g.append(
        txn_id,
        WalPayload::CommitTxn {
            commit_scn,
            commit_unix_us: commit_scn,
            binlog: None,
        },
    )
    .unwrap()
}

#[tokio::test]
async fn audit_fragmented_ack_survives_heartbeat_ticks() {
    use crate::repl::protocol::{self, Handshake, Message, ReplicaAck};
    use tokio::io::AsyncWriteExt;
    let master = build_master().await;
    let mut socket = tokio::net::TcpStream::connect(master.service.bind_addr())
        .await
        .unwrap();
    let hs = Handshake {
        auth_token: ReplConfig::test_defaults().auth_token,
        protocol_version: protocol::PROTOCOL_VERSION,
        replica_uuid: generate_uuid_bytes(),
        requested_start_lsn: 1,
        last_known_master_uuid: [0; 16],
        last_known_epoch: 0,
        cluster_uuid: [0; 16],
        last_known_timeline_uuid: [0; 16],
        client_version: "audit".into(),
        request_base_backup: false,
        max_chunk_bytes: 4096,
        format_compatibility: crate::storage::format_versions::native_replication_compatibility(),
    };
    protocol::write_frame_async(&mut socket, &Message::Handshake(hs))
        .await
        .unwrap();
    let Message::HandshakeAck(ack) = protocol::read_frame_async(&mut socket).await.unwrap() else {
        panic!()
    };
    assert!(ack.accepted, "{}", ack.reason);
    let mut bytes = Vec::new();
    protocol::write_frame(
        &mut bytes,
        &Message::ReplicaAck(ReplicaAck {
            received_lsn: 0,
            durable_lsn: 0,
            applied_lsn: 0,
            time_ms: 0,
        }),
    )
    .unwrap();
    socket.write_all(&bytes[..13]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    socket.write_all(&bytes[13..]).await.unwrap();
    // A new WAL write must still arrive after the fragmented ACK completes.
    let lsn = append_record(&master.log, 1, 0xAB);
    master.log.lock().flush().unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match protocol::read_frame_async(&mut socket).await.unwrap() {
                Message::WalChunk(chunk) if chunk.end_lsn >= lsn => break,
                Message::Heartbeat(_) => {}
                other => panic!("unexpected {other:?}"),
            }
        }
    })
    .await
    .unwrap();
    drop(socket);
    master.service.shutdown().await;
}

#[tokio::test]
async fn replica_receives_master_wal_and_persists_progress() {
    let master = build_master().await;
    let bind_addr = master.service.bind_addr();

    // Master pre-existing WAL.
    let l1 = append_record(&master.log, 1, 0xAA);
    let l2 = append_record(&master.log, 1, 0xBB);
    let last_lsn = append_commit(&master.log, 1, 100);
    let _ = (l1, l2);
    master.log.lock().flush().unwrap();

    // Replica setup
    let replica_dir = TempDir::new().unwrap();
    let replica_log = Arc::new(parking_lot::Mutex::new(
        LogManager::open(replica_dir.path()).unwrap(),
    ));
    replica_log.lock().set_max_segment_bytes(8192);

    let receiver = Receiver {
        master_host: bind_addr.ip().to_string(),
        master_port: bind_addr.port(),
        replica_uuid: crate::repl::role::generate_uuid_string(),
        config: ReplConfig {
            heartbeat: Duration::from_millis(10),
            reconnect_backoff: Duration::from_millis(10),
            ..ReplConfig::test_defaults()
        },
        data_dir: replica_dir.path().to_path_buf(),
        log: Arc::clone(&replica_log),
        db: None,
        initial_progress: ReplicaProgress::default(),
        shutdown_flag: None,
    };
    let shutdown = Arc::new(Notify::new());
    let shutdown_clone = Arc::clone(&shutdown);
    let join = tokio::spawn(async move {
        let _ = receiver.run_once(shutdown_clone, |_event| {}).await;
    });

    // Wait until replica's local WAL last_lsn catches up.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        let next = replica_log.lock().next_lsn();
        if next > last_lsn {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let next = replica_log.lock().next_lsn();
    assert!(
        next > last_lsn,
        "replica next_lsn={next}, want > {last_lsn}"
    );

    // Progress file must reflect the catch-up. The durable watermark
    // is group-fsync paced, so poll briefly instead of reading once.
    let progress_path = crate::repl::replica::progress::progress_path(replica_dir.path());
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    let mut progress = ReplicaProgress::default();
    while std::time::Instant::now() < deadline {
        if let Ok(bytes) = std::fs::read(&progress_path) {
            if let Ok(p) = serde_json::from_slice::<ReplicaProgress>(&bytes) {
                progress = p;
                if progress.durable_applied_lsn >= last_lsn {
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        progress.durable_applied_lsn >= last_lsn,
        "progress.durable_applied_lsn={}, want >= {}",
        progress.durable_applied_lsn,
        last_lsn
    );

    shutdown.notify_waiters();
    let _ = tokio::time::timeout(Duration::from_secs(2), join).await;
    master.service.shutdown().await;
}

#[tokio::test]
async fn live_appends_after_replica_connects_also_propagate() {
    let master = build_master().await;
    let bind_addr = master.service.bind_addr();
    let pre = append_record(&master.log, 1, 0x01);

    let replica_dir = TempDir::new().unwrap();
    let replica_log = Arc::new(parking_lot::Mutex::new(
        LogManager::open(replica_dir.path()).unwrap(),
    ));
    replica_log.lock().set_max_segment_bytes(8192);
    let receiver = Receiver {
        master_host: bind_addr.ip().to_string(),
        master_port: bind_addr.port(),
        replica_uuid: crate::repl::role::generate_uuid_string(),
        config: ReplConfig {
            heartbeat: Duration::from_millis(10),
            reconnect_backoff: Duration::from_millis(10),
            ..ReplConfig::test_defaults()
        },
        data_dir: replica_dir.path().to_path_buf(),
        log: Arc::clone(&replica_log),
        db: None,
        initial_progress: ReplicaProgress::default(),
        shutdown_flag: None,
    };
    let shutdown = Arc::new(Notify::new());
    let shutdown_clone = Arc::clone(&shutdown);
    let join = tokio::spawn(async move {
        let _ = receiver.run_once(shutdown_clone, |_| {}).await;
    });

    // After connection is set up, append more on master.
    tokio::time::sleep(Duration::from_millis(150)).await;
    let post1 = append_record(&master.log, 2, 0x02);
    let post2 = append_record(&master.log, 2, 0x03);
    let post3 = append_commit(&master.log, 2, 200);
    master.log.lock().flush().unwrap();
    let target = post3;

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        let next = replica_log.lock().next_lsn();
        if next > target {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let next = replica_log.lock().next_lsn();
    assert!(
        next > target,
        "replica next_lsn={next}, want > {target} (live appends {post1}/{post2}/{post3} after pre={pre})"
    );

    shutdown.notify_waiters();
    let _ = tokio::time::timeout(Duration::from_secs(2), join).await;
    master.service.shutdown().await;
}

#[tokio::test]
async fn replica_reconnect_resumes_from_persisted_progress() {
    let master = build_master().await;
    let bind_addr = master.service.bind_addr();
    let l1 = append_record(&master.log, 1, 0x11);
    let l2 = append_record(&master.log, 1, 0x22);
    let commit1 = append_commit(&master.log, 1, 50);
    master.log.lock().flush().unwrap();
    let _ = (l1, l2);

    let replica_dir = TempDir::new().unwrap();
    let replica_uuid = crate::repl::role::generate_uuid_string();

    // First connection: catch up to commit1.
    {
        let replica_log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(replica_dir.path()).unwrap(),
        ));
        let receiver = Receiver {
            master_host: bind_addr.ip().to_string(),
            master_port: bind_addr.port(),
            replica_uuid: replica_uuid.clone(),
            config: ReplConfig {
                heartbeat: Duration::from_millis(10),
                reconnect_backoff: Duration::from_millis(10),
                ..ReplConfig::test_defaults()
            },
            data_dir: replica_dir.path().to_path_buf(),
            log: Arc::clone(&replica_log),
            db: None,
            initial_progress: ReplicaProgress::default(),
            shutdown_flag: None,
        };
        let shutdown = Arc::new(Notify::new());
        let sd = Arc::clone(&shutdown);
        let join = tokio::spawn(async move {
            let _ = receiver.run_once(sd, |_| {}).await;
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while std::time::Instant::now() < deadline {
            let next = replica_log.lock().next_lsn();
            if next > commit1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        shutdown.notify_waiters();
        let _ = tokio::time::timeout(Duration::from_secs(2), join).await;
    }

    // Master appends more while replica is offline.
    let l3 = append_record(&master.log, 2, 0x33);
    let l4 = append_record(&master.log, 2, 0x44);
    let commit2 = append_commit(&master.log, 2, 75);
    let _ = (l3, l4);
    master.log.lock().flush().unwrap();

    // Second connection: must resume from persisted progress and pick up only the new records.
    let progress: ReplicaProgress = serde_json::from_slice(
        &std::fs::read(crate::repl::replica::progress::progress_path(
            replica_dir.path(),
        ))
        .expect("progress present"),
    )
    .unwrap();
    assert!(
        progress.durable_applied_lsn >= commit1,
        "first round didn't catch up: {:?}",
        progress
    );

    let replica_log = Arc::new(parking_lot::Mutex::new(
        LogManager::open(replica_dir.path()).unwrap(),
    ));
    let receiver = Receiver {
        master_host: bind_addr.ip().to_string(),
        master_port: bind_addr.port(),
        replica_uuid,
        config: ReplConfig {
            heartbeat: Duration::from_millis(10),
            reconnect_backoff: Duration::from_millis(10),
            ..ReplConfig::test_defaults()
        },
        data_dir: replica_dir.path().to_path_buf(),
        log: Arc::clone(&replica_log),
        db: None,
        initial_progress: progress,
        shutdown_flag: None,
    };
    let shutdown = Arc::new(Notify::new());
    let sd = Arc::clone(&shutdown);
    let join = tokio::spawn(async move {
        let _ = receiver.run_once(sd, |_| {}).await;
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        let next = replica_log.lock().next_lsn();
        if next > commit2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let next = replica_log.lock().next_lsn();
    assert!(
        next > commit2,
        "after reconnect, replica next_lsn={next}, want > {commit2}"
    );

    shutdown.notify_waiters();
    let _ = tokio::time::timeout(Duration::from_secs(2), join).await;
    master.service.shutdown().await;
}

#[tokio::test]
async fn three_replicas_concurrent_catch_up_to_same_master() {
    let master = build_master().await;
    let bind_addr = master.service.bind_addr();
    let mut last_lsn = 0;
    for i in 0..6u8 {
        let _ = append_record(&master.log, 1, i);
        last_lsn = append_commit(&master.log, 1, 100 + i as u64);
    }
    master.log.lock().flush().unwrap();

    let mut replica_logs = Vec::new();
    let mut shutdowns = Vec::new();
    let mut joins = Vec::new();
    let mut _replica_dirs = Vec::new();
    for _ in 0..3 {
        let dir = TempDir::new().unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        log.lock().set_max_segment_bytes(8192);
        replica_logs.push(Arc::clone(&log));
        let receiver = Receiver {
            master_host: bind_addr.ip().to_string(),
            master_port: bind_addr.port(),
            replica_uuid: crate::repl::role::generate_uuid_string(),
            config: ReplConfig {
                heartbeat: Duration::from_millis(10),
                reconnect_backoff: Duration::from_millis(10),
                ..ReplConfig::test_defaults()
            },
            data_dir: dir.path().to_path_buf(),
            log: Arc::clone(&log),
            db: None,
            initial_progress: ReplicaProgress::default(),
            shutdown_flag: None,
        };
        let shutdown = Arc::new(Notify::new());
        let sd = Arc::clone(&shutdown);
        shutdowns.push(shutdown);
        let join = tokio::spawn(async move { receiver.run_once(sd, |_| {}).await });
        joins.push(join);
        _replica_dirs.push(dir);
    }

    // This asserts convergence, not a five-second service-level objective.
    // Concurrent fsync-heavy tests can delay all three initial handshakes.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut all_caught_up = false;
    while std::time::Instant::now() < deadline {
        for join in &mut joins {
            if join.is_finished() {
                panic!("replica receiver stopped before catch-up: {:?}", join.await);
            }
        }
        let caught_up = replica_logs.iter().all(|l| l.lock().next_lsn() > last_lsn);
        if caught_up {
            all_caught_up = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let nexts: Vec<u64> = replica_logs.iter().map(|l| l.lock().next_lsn()).collect();
    assert!(
        all_caught_up,
        "not all replicas caught up to {last_lsn}: nexts={nexts:?}"
    );

    let slot_snapshot = master.service.slots.lock().snapshot();
    let active = slot_snapshot
        .iter()
        .filter(|s| s.status == crate::repl::master::SlotStatus::Active)
        .count();
    assert_eq!(active, 3, "expected 3 active slots, got {active}");

    for s in shutdowns {
        s.notify_waiters();
    }
    for j in joins {
        let _ = tokio::time::timeout(Duration::from_secs(2), j).await;
    }
    master.service.shutdown().await;
}

#[tokio::test]
async fn handshake_below_retention_fires_resync_event() {
    let master = build_master().await;
    let bind_addr = master.service.bind_addr();
    // Build some WAL then compact, leaving min_available > 1.
    let _ = append_record(&master.log, 1, 0x99);
    let _ = append_record(&master.log, 1, 0x98);
    let _ = append_commit(&master.log, 1, 50);
    {
        let mut g = master.log.lock();
        let next = g.next_lsn();
        g.compact_to_checkpoint(next, 100, 200).unwrap();
    }

    let replica_dir = TempDir::new().unwrap();
    let replica_log = Arc::new(parking_lot::Mutex::new(
        LogManager::open(replica_dir.path()).unwrap(),
    ));
    let mut initial = ReplicaProgress::default();
    // Pretend we'd already applied through LSN 0 — request LSN 1, which is
    // below master's min_available after compaction.
    initial.durable_applied_lsn = 0;

    let receiver = Receiver {
        master_host: bind_addr.ip().to_string(),
        master_port: bind_addr.port(),
        replica_uuid: crate::repl::role::generate_uuid_string(),
        config: ReplConfig::test_defaults(),
        data_dir: replica_dir.path().to_path_buf(),
        log: Arc::clone(&replica_log),
        db: None,
        initial_progress: initial,
        shutdown_flag: None,
    };

    let shutdown = Arc::new(Notify::new());
    let sd = Arc::clone(&shutdown);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let join = tokio::spawn(async move {
        let _ = receiver
            .run_once(sd, move |ev| {
                let _ = tx.send(ev);
            })
            .await;
    });

    let mut saw_resync = false;
    let mut saw_connected = false;
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while std::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
            Ok(Some(ReceiverEvent::ResyncRequired { .. })) => {
                saw_resync = true;
                break;
            }
            Ok(Some(ReceiverEvent::Connected { .. })) => saw_connected = true,
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(_) => continue,
        }
    }
    assert!(
        saw_resync || !saw_connected,
        "expected a Resync (or at least no Connected event) when below retention"
    );

    shutdown.notify_waiters();
    let _ = tokio::time::timeout(Duration::from_secs(2), join).await;
    master.service.shutdown().await;
}

// ============================================================
// Online apply tests (Item 1): master writes through real SQL,
// replica receiver+applier project the WAL into a live Database,
// SELECT on the replica returns the row without restart.
// ============================================================

#[cfg(test)]
mod online_apply {
    use super::*;
    use crate::engine::executor::StorageQueryEngine;
    use crate::engine::{QueryLogConfig, QueryResponse, SessionRegistry};
    use crate::repl::master::{self, MasterServiceInit, SlotRegistry};
    use crate::repl::replica::Receiver;
    use crate::repl::role::generate_uuid_bytes;
    use crate::types::Value;

    /// Bring up `StorageQueryEngine` + a `MasterService` bound to the
    /// engine's underlying WAL log. Returns the engine, its data dir
    /// guard, and the service.
    async fn build_master_engine() -> (Arc<StorageQueryEngine>, TempDir, MasterService) {
        let dir = fresh_database_tempdir();
        let engine = Arc::new(
            StorageQueryEngine::open_full(
                dir.path().to_path_buf(),
                QueryLogConfig::default(),
                Arc::new(SessionRegistry::new()),
            )
            .expect("open master engine"),
        );
        let db = engine.database_for_replication().clone();
        let log = db.wal_log();
        let slots = Arc::new(PlMutex::new(SlotRegistry::open(dir.path()).unwrap()));
        let service = master::start_master_service(MasterServiceInit {
            bind_host: "127.0.0.1".into(),
            bind_port: 0,
            config: ReplConfig {
                heartbeat: Duration::from_millis(10),
                ..ReplConfig::test_defaults()
            },
            master_uuid: generate_uuid_bytes(),
            epoch: 1,
            server_version: "test-master".into(),
            log,
            slots,
            db: Some(Arc::clone(&db)),
            data_dir: Some(dir.path().to_path_buf()),
            role_state: None,
        })
        .await
        .unwrap();
        (engine, dir, service)
    }

    /// Bring up a replica `StorageQueryEngine` and start a
    /// `Receiver::run_once` task connected to the given master bind
    /// address. Returns the replica engine, its data dir, the shutdown
    /// handle, and the receiver task join handle.
    async fn spawn_replica(
        master_addr: std::net::SocketAddr,
    ) -> (
        Arc<StorageQueryEngine>,
        TempDir,
        Arc<Notify>,
        tokio::task::JoinHandle<()>,
    ) {
        let dir = TempDir::new().unwrap();
        let replica_uuid = crate::repl::role::generate_uuid_string();
        let mut role = crate::repl::role::RoleState::new_standalone();
        role.role = crate::repl::role::Role::Replica;
        role.master_endpoint = Some(master_addr.to_string());
        crate::repl::role::save(dir.path(), &role).unwrap();
        crate::repl::base_backup::perform_base_backup_into_with_config(
            &master_addr.ip().to_string(),
            master_addr.port(),
            &replica_uuid,
            dir.path(),
            ReplConfig::test_defaults(),
        )
        .await
        .unwrap();
        let progress = crate::repl::replica::progress::load_progress(dir.path())
            .unwrap()
            .unwrap();
        let engine = Arc::new(
            StorageQueryEngine::open_full(
                dir.path().to_path_buf(),
                QueryLogConfig::default(),
                Arc::new(SessionRegistry::new()),
            )
            .expect("open replica engine"),
        );
        let db = engine.database_for_replication().clone();
        let log = db.wal_log();
        let receiver = Receiver {
            master_host: master_addr.ip().to_string(),
            master_port: master_addr.port(),
            replica_uuid,
            config: ReplConfig {
                heartbeat: Duration::from_millis(10),
                reconnect_backoff: Duration::from_millis(10),
                ..ReplConfig::test_defaults()
            },
            data_dir: dir.path().to_path_buf(),
            log,
            db: Some(Arc::clone(&db)),
            initial_progress: progress,
            shutdown_flag: None,
        };
        let shutdown = Arc::new(Notify::new());
        let sd = Arc::clone(&shutdown);
        let join = tokio::spawn(async move {
            if let Err(err) = receiver.run_once(sd, |_| {}).await {
                eprintln!("[test] replica run_once failed: {err:?}");
            }
        });
        (engine, dir, shutdown, join)
    }

    /// Poll `engine.execute(sql)` until at least `min_rows` rows come
    /// back (or the deadline elapses). `engine.execute` runs
    /// synchronously, so we wrap it in `spawn_blocking` to avoid
    /// stalling the runtime under the receiver loop.
    async fn poll_select_rows(
        engine: &Arc<StorageQueryEngine>,
        sql: &str,
        min_rows: usize,
        deadline_ms: u64,
    ) -> Vec<Vec<Value>> {
        let deadline = std::time::Instant::now() + Duration::from_millis(deadline_ms);
        loop {
            let engine_clone = Arc::clone(engine);
            let sql_owned = sql.to_string();
            let outcome = tokio::task::spawn_blocking(move || engine_clone.execute(&sql_owned))
                .await
                .unwrap();
            if let Ok(QueryResponse::ResultSet(rs)) = outcome {
                if rs.rows.len() >= min_rows {
                    return rs.rows;
                }
            }
            if std::time::Instant::now() > deadline {
                let engine_final = Arc::clone(engine);
                let sql_final = sql.to_string();
                let outcome = tokio::task::spawn_blocking(move || engine_final.execute(&sql_final))
                    .await
                    .unwrap();
                return match outcome {
                    Ok(QueryResponse::ResultSet(rs)) => rs.rows,
                    _ => Vec::new(),
                };
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
    }

    async fn poll_select_exact(
        engine: &Arc<StorageQueryEngine>,
        sql: &str,
        expected: &[Vec<Value>],
        deadline_ms: u64,
    ) -> Vec<Vec<Value>> {
        let deadline = std::time::Instant::now() + Duration::from_millis(deadline_ms);
        loop {
            let engine_clone = Arc::clone(engine);
            let sql_owned = sql.to_string();
            let outcome = tokio::task::spawn_blocking(move || engine_clone.execute(&sql_owned))
                .await
                .unwrap();
            if let Ok(QueryResponse::ResultSet(rs)) = outcome {
                if rs.rows == expected {
                    return rs.rows;
                }
                if std::time::Instant::now() > deadline {
                    return rs.rows;
                }
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
    }

    #[tokio::test]
    async fn master_insert_visible_to_replica_select_without_restart() {
        let (master_engine, _master_dir, master_service) = build_master_engine().await;
        let bind_addr = master_service.bind_addr();
        let (replica_engine, _replica_dir, shutdown, join) = spawn_replica(bind_addr).await;
        // DDL + INSERT through the master engine. Each runs in its own
        // implicit txn → produces BeginTxn / Heap* / CommitTxn records.
        let me = Arc::clone(&master_engine);
        tokio::task::spawn_blocking(move || {
            me.execute("CREATE TABLE t (a INT)").expect("create table");
            me.execute("INSERT INTO t VALUES (1), (2), (3)")
                .expect("insert rows");
        })
        .await
        .unwrap();
        let rows = poll_select_rows(&replica_engine, "SELECT a FROM t ORDER BY a", 3, 5_000).await;
        assert_eq!(
            rows.len(),
            3,
            "replica did not see 3 rows after online apply: {:?}",
            rows
        );
        let metadata = poll_select_rows(
            &replica_engine,
            "SELECT TABLE_ROWS FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA='test' AND TABLE_NAME='t'",
            1,
            5_000,
        )
        .await;
        assert_eq!(metadata, vec![vec![Value::Int(3)]]);

        let me = Arc::clone(&master_engine);
        tokio::task::spawn_blocking(move || {
            me.execute("UPDATE t SET a=20 WHERE a=2")
                .expect("update row");
            me.execute("DELETE FROM t WHERE a=1").expect("delete row");
        })
        .await
        .unwrap();
        let expected_rows = vec![vec![Value::Int(3)], vec![Value::Int(20)]];
        let rows = poll_select_exact(
            &replica_engine,
            "SELECT a FROM t ORDER BY a",
            &expected_rows,
            5_000,
        )
        .await;
        assert_eq!(rows, expected_rows);
        let table_rows = poll_select_rows(
            &replica_engine,
            "SELECT TABLE_ROWS FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA='test' AND TABLE_NAME='t'",
            1,
            5_000,
        )
        .await;
        assert_eq!(table_rows, vec![vec![Value::Int(2)]]);
        let metadata = poll_select_rows(
            &replica_engine,
            "SELECT TABLE_ROWS,MODIFIED_ROWS FROM information_schema.MYDB_STATS_HEALTH \
             WHERE TABLE_SCHEMA='test' AND TABLE_NAME='t'",
            1,
            5_000,
        )
        .await;
        assert_eq!(metadata, vec![vec![Value::Int(0), Value::Int(5)]]);

        shutdown.notify_waiters();
        let _ = tokio::time::timeout(Duration::from_secs(2), join).await;
        master_service.shutdown().await;
    }

    #[tokio::test]
    async fn hybrid_rrf_replica_reads_online_fulltext_vector_updates() {
        let (master, _master_dir, service) = build_master_engine().await;
        let (replica, _replica_dir, shutdown, join) = spawn_replica(service.bind_addr()).await;
        let writer = Arc::clone(&master);
        tokio::task::spawn_blocking(move || {
            writer.execute("CREATE TABLE hybrid_docs(id INT PRIMARY KEY,body TEXT,v VECTOR(2) NOT NULL,FULLTEXT KEY ft(body))").unwrap();
            writer.execute("INSERT INTO hybrid_docs VALUES(1,'database guide','[1,0]'),(2,'database database','[0,1]')").unwrap();
            writer.execute("CREATE INDEX hv ON hybrid_docs(v) USING HNSW").unwrap();
        }).await.unwrap();
        let sql = "SELECT id,match_count FROM ((SELECT id FROM hybrid_docs WHERE MATCH(body) AGAINST('database') ORDER BY MATCH(body) AGAINST('database') DESC,id LIMIT 2) RRF (SELECT /*+ HNSW_EF(32) */ id FROM hybrid_docs ORDER BY L2_DISTANCE(v,TO_VECTOR('[1,0]')),id LIMIT 2)) h ORDER BY id";
        let expected = vec![
            vec![Value::Int(1), Value::UInt(2)],
            vec![Value::Int(2), Value::UInt(2)],
        ];
        assert_eq!(
            poll_select_exact(&replica, sql, &expected, 10_000).await,
            expected
        );
        let writer = Arc::clone(&master);
        tokio::task::spawn_blocking(move || {
            writer
                .execute("DELETE FROM hybrid_docs WHERE id=1")
                .unwrap();
            writer
                .execute("UPDATE hybrid_docs SET body='unrelated text',v='[2,2]' WHERE id=2")
                .unwrap();
        })
        .await
        .unwrap();
        let expected = vec![vec![Value::Int(2), Value::UInt(1)]];
        assert_eq!(
            poll_select_exact(&replica, sql, &expected, 10_000).await,
            expected
        );
        shutdown.notify_one();
        join.await.unwrap();
        service.shutdown().await;
    }

    #[tokio::test]
    async fn online_primary_key_publication_materializes_before_replica_reads() {
        let (source, _source_dir, service) = build_master_engine().await;
        let (replica, _replica_dir, shutdown, join) = spawn_replica(service.bind_addr()).await;
        let writer = source.clone();
        tokio::task::spawn_blocking(move || {
            writer
                .execute("CREATE TABLE pk_live(id INT NULL, v INT)")
                .unwrap();
            writer
                .execute("INSERT INTO pk_live VALUES(1,10),(2,20)")
                .unwrap();
        })
        .await
        .unwrap();
        let initial = vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(20)],
        ];
        assert_eq!(
            poll_select_exact(
                &replica,
                "SELECT id,v FROM pk_live ORDER BY id",
                &initial,
                5000
            )
            .await,
            initial
        );
        let hook = source
            .database_for_replication()
            .install_online_index_validation_test_hook();
        let worker = source.clone();
        let ddl = tokio::task::spawn_blocking(move || {
            let mut session = crate::engine::SessionContext::default();
            session.connection_id = 7801;
            worker.execute_with_session(
                &mut session,
                "ALTER TABLE pk_live ADD PRIMARY KEY(id), ADD INDEX v_idx(v), LOCK=NONE",
            )
        });
        let wait_hook = hook.clone();
        let reached = tokio::task::spawn_blocking(move || {
            wait_hook.wait_until_reached(Duration::from_secs(5))
        })
        .await
        .unwrap();
        let writer = source.clone();
        let writes = tokio::task::spawn_blocking(move || {
            let mut session = crate::engine::SessionContext::default();
            session.connection_id = 7802;
            for sql in [
                "INSERT INTO pk_live VALUES(3,30)",
                "UPDATE pk_live SET id=4,v=11 WHERE id=1",
                "DELETE FROM pk_live WHERE id=2",
            ] {
                writer.execute_with_session(&mut session, sql)?;
            }
            Ok::<_, crate::types::QueryError>(())
        })
        .await
        .unwrap();
        hook.release();
        let result = ddl.await.unwrap();
        assert!(reached, "source did not reach validation: {result:?}");
        writes.unwrap();
        result.unwrap();
        let published = poll_select_rows(&replica,
            "SELECT INDEX_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA='test' AND TABLE_NAME='pk_live' AND INDEX_NAME='PRIMARY'",
            1, 5000).await;
        assert_eq!(published, vec![vec![Value::String("PRIMARY".into())]]);
        let expected = vec![
            vec![Value::Int(3), Value::Int(30)],
            vec![Value::Int(4), Value::Int(11)],
        ];
        let rows = poll_select_exact(
            &replica,
            "SELECT id,v FROM pk_live FORCE INDEX(PRIMARY) ORDER BY id",
            &expected,
            5000,
        )
        .await;
        assert_eq!(rows, expected);
        let schema = replica
            .database_for_replication()
            .table("test.pk_live")
            .unwrap();
        assert_eq!(schema.primary_key, vec!["id"]);
        assert!(!schema.columns[0].nullable);
        shutdown.notify_waiters();
        let _ = tokio::time::timeout(Duration::from_secs(2), join).await;
        service.shutdown().await;
    }

    #[tokio::test]
    async fn physical_replica_index_retirement_reuses_names_without_local_wal() {
        let (source, _source_dir, service) = build_master_engine().await;
        let (replica, _replica_dir, shutdown, join) = spawn_replica(service.bind_addr()).await;
        let writer = source.clone();
        tokio::task::spawn_blocking(move || {
            writer
                .execute("CREATE TABLE retired(id INT PRIMARY KEY,v INT,KEY vi(v))")
                .unwrap();
            writer
                .execute("INSERT INTO retired VALUES(1,10),(2,20)")
                .unwrap();
        })
        .await
        .unwrap();
        let expected = vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(20)],
        ];
        assert_eq!(
            poll_select_exact(
                &replica,
                "SELECT id,v FROM retired FORCE INDEX(vi) WHERE v>=0 ORDER BY id",
                &expected,
                5000
            )
            .await,
            expected
        );
        let writer = source.clone();
        tokio::task::spawn_blocking(move || {
            writer
                .database_for_replication()
                .fail_next_ddl_cleanup_for_test();
            writer.execute("DROP INDEX vi ON retired").unwrap();
        })
        .await
        .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while replica
            .database_for_replication()
            .table("test.retired")
            .unwrap()
            .indexes
            .iter()
            .any(|index| index.name == "vi")
        {
            assert!(
                std::time::Instant::now() < deadline,
                "replica did not publish index drop"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let db = replica.database_for_replication().clone();
        let before = db.wal_log().lock().last_lsn();
        let source_log = source.database_for_replication().wal_log();
        tokio::task::spawn_blocking(move || {
            assert_eq!(db.catalog_retirement_count_for_test(), 1);
            assert_eq!(db.maintain_catalog_retirements_until(|| false).unwrap(), 1);
            let after = db.wal_log().lock().last_lsn();
            // The receiver remains live: legitimate source frames can arrive
            // during cleanup. Compare their bytes instead of treating every
            // concurrent tail advance as a locally generated record.
            if after > before {
                let mut received = crate::wal::WalReader::open(&db.wal_log(), before + 1).unwrap();
                let mut source = crate::wal::WalReader::open(&source_log, before + 1).unwrap();
                while let Some(chunk) = received.next_chunk(after, 1 << 20).unwrap() {
                    let expected = source
                        .next_chunk(chunk.end_lsn, chunk.bytes.len())
                        .unwrap()
                        .unwrap();
                    assert_eq!(
                        chunk.bytes, expected.bytes,
                        "replica cleanup generated divergent local WAL"
                    );
                }
            }
            assert_eq!(
                db.catalog_retirement_count_for_test(),
                1,
                "replica removed source-owned DD authority"
            );
        })
        .await
        .unwrap();
        let writer = source.clone();
        tokio::task::spawn_blocking(move || {
            writer.execute("CREATE INDEX vi ON retired(v)").unwrap();
            writer
                .execute("UPDATE retired SET v=11 WHERE id=1")
                .unwrap();
            writer
                .database_for_replication()
                .maintain_catalog_retirements_until(|| false)
                .unwrap();
        })
        .await
        .unwrap();
        let expected = vec![
            vec![Value::Int(1), Value::Int(11)],
            vec![Value::Int(2), Value::Int(20)],
        ];
        assert_eq!(
            poll_select_exact(
                &replica,
                "SELECT id,v FROM retired FORCE INDEX(vi) WHERE v>=0 ORDER BY id",
                &expected,
                5000
            )
            .await,
            expected
        );
        shutdown.notify_waiters();
        let _ = tokio::time::timeout(Duration::from_secs(2), join).await;
        service.shutdown().await;
    }

    #[tokio::test]
    async fn physical_replica_preserves_timestamp_9999_and_indexes() {
        let (source, _source_dir, service) = build_master_engine().await;
        let (replica, _replica_dir, shutdown, join) = spawn_replica(service.bind_addr()).await;
        let engine = Arc::clone(&source);
        tokio::task::spawn_blocking(move || {
            let mut session = crate::engine::SessionContext::default();
            engine
                .execute_with_session(&mut session, "SET time_zone='+00:00'")
                .unwrap();
            engine
                .execute_with_session(
                    &mut session,
                    "CREATE TABLE future_phys(id INT PRIMARY KEY, ts TIMESTAMP(6), KEY idx_ts(ts))",
                )
                .unwrap();
            engine
                .execute_with_session(
                    &mut session,
                    "INSERT INTO future_phys VALUES(1,'9999-12-31 23:59:59.999999'),\
                 (2,'2099-12-31 23:59:59.123456')",
                )
                .unwrap();
        })
        .await
        .unwrap();
        let rows = poll_select_rows(
            &replica,
            "SELECT id,UNIX_TIMESTAMP(ts) FROM future_phys FORCE INDEX(idx_ts) ORDER BY id",
            2,
            5_000,
        )
        .await;
        assert_eq!(rows[0][0], Value::Int(1));
        assert_eq!(
            crate::storage::value_to_string(&rows[0][1]),
            "253402300799.999999"
        );
        assert_eq!(
            crate::storage::value_to_string(&rows[1][1]),
            "4102444799.123456"
        );
        let engine = Arc::clone(&source);
        tokio::task::spawn_blocking(move || {
            let mut session = crate::engine::SessionContext::default();
            engine
                .execute_with_session(&mut session, "SET time_zone='+00:00'")
                .unwrap();
            engine
                .execute_with_session(
                    &mut session,
                    "UPDATE future_phys SET ts='9999-01-01 00:00:00.123456' WHERE id=2",
                )
                .unwrap();
            engine
                .execute_with_session(&mut session, "DELETE FROM future_phys WHERE id=1")
                .unwrap();
        })
        .await
        .unwrap();
        let rows = poll_select_rows(
            &replica,
            "SELECT id,UNIX_TIMESTAMP(ts) FROM future_phys \
             WHERE id=2 AND UNIX_TIMESTAMP(ts)=253370764800.123456",
            1,
            10_000,
        )
        .await;
        assert_eq!(
            rows,
            vec![vec![
                Value::Int(2),
                crate::engine::expr::eval_unix_timestamp_value(&Value::Timestamp(
                    crate::storage::parse_datetime_to_micros("9999-01-01 00:00:00.123456").unwrap()
                ))
                .unwrap()
            ]]
        );
        assert_eq!(
            poll_select_rows(
                &replica,
                "SELECT COUNT(*) FROM future_phys HAVING COUNT(*)=1",
                1,
                10_000
            )
            .await,
            vec![vec![Value::Int(1)]]
        );
        shutdown.notify_waiters();
        let _ = tokio::time::timeout(Duration::from_secs(2), join).await;
        service.shutdown().await;
    }

    #[tokio::test]
    async fn physical_replica_applies_zone_map_wal_before_online_pruning() {
        let (master_engine, _master_dir, master_service) = build_master_engine().await;
        let bind_addr = master_service.bind_addr();
        let (replica_engine, _replica_dir, shutdown, join) = spawn_replica(bind_addr).await;
        replica_engine
            .database_for_replication()
            .set_physical_replica_mode(true)
            .unwrap();
        let me = Arc::clone(&master_engine);
        tokio::task::spawn_blocking(move || {
            me.execute(
                "CREATE TABLE zm_phys (id INT PRIMARY KEY, pad VARCHAR(700)) \
                 ZONE_MAP=ON ZONE_SIZE=64K",
            )
            .expect("create zone-map table");
            me.execute(&format!(
                "INSERT INTO zm_phys VALUES (1,'base'),(9000,'{}')",
                "x".repeat(600)
            ))
            .expect("insert replicated extreme");
        })
        .await
        .unwrap();

        let rows = poll_select_rows(
            &replica_engine,
            "SELECT /*+ ZONE_MAP(zm_phys) */ id FROM zm_phys WHERE id=9000",
            1,
            5_000,
        )
        .await;
        assert_eq!(rows, vec![vec![Value::Int(9000)]]);
        let re = Arc::clone(&replica_engine);
        tokio::task::spawn_blocking(move || {
            let zone = re
                .execute("SELECT /*+ ZONE_MAP(zm_phys) */ id FROM zm_phys WHERE id=9000")
                .unwrap();
            let oracle = re
                .execute("SELECT /*+ NO_ZONE_MAP(zm_phys) */ id FROM zm_phys WHERE id=9000")
                .unwrap();
            let QueryResponse::ResultSet(zone) = zone else {
                panic!("expected Zone Map result set")
            };
            let QueryResponse::ResultSet(oracle) = oracle else {
                panic!("expected SeqScan result set")
            };
            assert_eq!(zone.rows, oracle.rows);
        })
        .await
        .unwrap();

        // Publish the baseline on the source: physical replicas must use
        // source pages and cannot allocate a conflicting local summary tree.
        let me = Arc::clone(&master_engine);
        let rebuilt_lsn = tokio::task::spawn_blocking(move || {
            let db = me.database_for_replication();
            let table = db.table("test.zm_phys").unwrap();
            db.rebuild_zone_map_for_table(&table).unwrap();
            let log = db.wal_log();
            let mut log = log.lock();
            log.flush().unwrap();
            log.next_lsn().saturating_sub(1)
        })
        .await
        .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while replica_engine
            .database_for_replication()
            .replica_applied_lsn()
            < rebuilt_lsn
        {
            assert!(
                tokio::time::Instant::now() < deadline,
                "replica did not apply summary baseline"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // Disabling source maintenance must revoke its replicated bounds
        // even though the replica's own master switch stays ON.
        let re = Arc::clone(&replica_engine);
        tokio::task::spawn_blocking(move || {
            let db = re.database_for_replication();
            let table = db.table("test.zm_phys").unwrap();
            let rejected = db.rebuild_zone_map_for_table(&table).unwrap_err();
            assert_eq!(
                rejected.code(),
                crate::error::catalog::ER_NOT_SUPPORTED_YET.code()
            );
            assert!(rejected.message().contains("physical replicas"));
            let filter = crate::parser::Expr::Binary {
                left: Box::new(crate::parser::Expr::Identifier("id".into())),
                op: crate::parser::BinaryOp::Eq,
                right: Box::new(crate::parser::Expr::Literal(crate::parser::Literal::Int(
                    99999,
                ))),
            };
            assert_eq!(
                db.zone_map_candidate_ranges_for_plan(&table, &filter),
                Some(crate::storage::ZoneMapRanges::default())
            );
            assert!(db.zone_map_enabled());
        })
        .await
        .unwrap();
        let me = Arc::clone(&master_engine);
        tokio::task::spawn_blocking(move || {
            me.execute("SET GLOBAL mydb_zone_map_enabled=OFF").unwrap();
            me.execute("INSERT INTO zm_phys VALUES (99999,'disabled source')")
                .unwrap();
        })
        .await
        .unwrap();
        let rows = poll_select_rows(
            &replica_engine,
            "SELECT /*+ ZONE_MAP(zm_phys) */ id FROM zm_phys WHERE id=99999",
            1,
            5_000,
        )
        .await;
        assert_eq!(rows, vec![vec![Value::Int(99999)]]);
        assert!(replica_engine.database_for_replication().zone_map_enabled());

        shutdown.notify_waiters();
        let _ = tokio::time::timeout(Duration::from_secs(2), join).await;
        master_service.shutdown().await;
    }

    #[tokio::test]
    async fn physical_replica_slot_reclaim_waits_for_old_snapshot_and_retries() {
        let (master_engine, _master_dir, master_service) = build_master_engine().await;
        let replica_dir = TempDir::new().unwrap();
        let initial_progress =
            initialize_replica_fixture(master_service.bind_addr(), replica_dir.path()).await;
        let replica_engine = Arc::new(
            StorageQueryEngine::open_full(
                replica_dir.path().to_path_buf(),
                QueryLogConfig::default(),
                Arc::new(SessionRegistry::new()),
            )
            .unwrap(),
        );
        let replica_db = Arc::clone(replica_engine.database_for_replication());
        replica_db.set_physical_replica_mode(true).unwrap();
        let status = Arc::new(PlMutex::new(String::new()));
        let progress = Arc::new(PlMutex::new(initial_progress.clone()));
        let mut role = crate::repl::role::RoleState::new_standalone();
        role.role = crate::repl::role::Role::Replica;
        let mut lifecycle = crate::repl::lifecycle::ReceiverLifecycle::new();
        lifecycle
            .start(
                tokio::runtime::Handle::current(),
                Arc::clone(&replica_db),
                replica_db.wal_log(),
                replica_dir.path().to_path_buf(),
                master_service.bind_addr().to_string(),
                role.node_uuid.clone(),
                ReplConfig {
                    heartbeat: Duration::from_millis(10),
                    reconnect_backoff: Duration::from_millis(10),
                    ..ReplConfig::test_defaults()
                },
                initial_progress,
                Arc::clone(&status),
                Arc::clone(&progress),
                Arc::new(std::sync::Mutex::new(role)),
            )
            .unwrap();
        let source = Arc::clone(&master_engine);
        let initial_lsn = tokio::task::spawn_blocking(move || {
            source
                .execute("SET GLOBAL mydb_undo_retention_ms=0")
                .unwrap();
            source
                .execute("CREATE TABLE replica_rr_purge (id INT) ZONE_MAP=OFF")
                .unwrap();
            source
                .execute("INSERT INTO replica_rr_purge VALUES (1),(9000)")
                .unwrap();
            let log = source.database_for_replication().wal_log();
            let mut log = log.lock();
            log.flush().unwrap();
            log.next_lsn().saturating_sub(1)
        })
        .await
        .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while replica_db.replica_applied_lsn() < initial_lsn {
            assert!(
                tokio::time::Instant::now() < deadline,
                "initial catch-up: {}",
                status.lock()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let reader = Arc::new(PlMutex::new(crate::engine::SessionContext::default()));
        let session = Arc::clone(&reader);
        let replica = Arc::clone(&replica_engine);
        tokio::task::spawn_blocking(move || {
            let mut session = session.lock();
            replica
                .execute_with_session(
                    &mut session,
                    "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
                )
                .unwrap();
            replica
                .execute_with_session(
                    &mut session,
                    "START TRANSACTION READ ONLY, WITH CONSISTENT SNAPSHOT",
                )
                .unwrap();
            let QueryResponse::ResultSet(rows) = replica
                .execute_with_session(&mut session, "SELECT id FROM replica_rr_purge ORDER BY id")
                .unwrap()
            else {
                panic!("rows");
            };
            assert_eq!(rows.rows, vec![vec![Value::Int(1)], vec![Value::Int(9000)]]);
        })
        .await
        .unwrap();
        let source = Arc::clone(&master_engine);
        tokio::task::spawn_blocking(move || {
            source
                .execute("DELETE FROM replica_rr_purge WHERE id=9000")
                .unwrap();
            let db = source.database_for_replication();
            db.purge_obsolete_undo();
            let log = db.wal_log();
            let mut log = log.lock();
            log.flush().unwrap();
        })
        .await
        .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let conflict_status = loop {
            let current_status = status.lock().clone();
            if current_status.contains("replay blocked by active snapshot") {
                break current_status;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "expected snapshot conflict: {}",
                current_status
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        // Earlier independent SlotReclaim records may replay successfully.
        // Assert against the exact record rejected by the receiver instead
        // of scanning live WAL and selecting an unrelated first reclamation.
        let reclaimed_lsn = conflict_status
            .split_once("slot reclaim at LSN ")
            .and_then(|(_, tail)| tail.split_whitespace().next())
            .and_then(|lsn| lsn.parse::<u64>().ok())
            .expect("receiver reported a SlotReclaim snapshot conflict");
        assert!(
            replica_db.replica_applied_lsn() < reclaimed_lsn,
            "conflicting reclaim must not advance the applied watermark"
        );
        assert!(
            lifecycle.is_running(),
            "snapshot conflict must remain retryable"
        );
        let session = Arc::clone(&reader);
        let replica = Arc::clone(&replica_engine);
        tokio::task::spawn_blocking(move || {
            let mut session = session.lock();
            let QueryResponse::ResultSet(rows) = replica
                .execute_with_session(&mut session, "SELECT id FROM replica_rr_purge ORDER BY id")
                .unwrap()
            else {
                panic!("rows");
            };
            assert_eq!(
                rows.rows,
                vec![vec![Value::Int(1)], vec![Value::Int(9000)]],
                "replay conflict must preserve the old snapshot's physical row and undo anchor"
            );
            replica
                .execute_with_session(&mut session, "ROLLBACK")
                .unwrap();
        })
        .await
        .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while replica_db.replica_applied_lsn() < reclaimed_lsn {
            assert!(
                tokio::time::Instant::now() < deadline,
                "automatic retry failed: {}",
                status.lock()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let replica = Arc::clone(&replica_engine);
        tokio::task::spawn_blocking(move || {
            let QueryResponse::ResultSet(rows) = replica
                .execute("SELECT id FROM replica_rr_purge ORDER BY id")
                .unwrap()
            else {
                panic!("rows");
            };
            assert_eq!(rows.rows, vec![vec![Value::Int(1)]]);
        })
        .await
        .unwrap();
        lifecycle.stop_and_wait().await;
        master_service.shutdown().await;
    }

    #[tokio::test]
    async fn physical_replica_page_delta_stream_preserves_zone_map_results() {
        let (master_engine, master_dir, master_service) = build_master_engine().await;
        let (replica_engine, _replica_dir, shutdown, join) =
            spawn_replica(master_service.bind_addr()).await;
        replica_engine
            .database_for_replication()
            .set_physical_replica_mode(true)
            .unwrap();
        let source = Arc::clone(&master_engine);
        let source_path = master_dir.path().to_path_buf();
        let target_lsn = tokio::task::spawn_blocking(move || {
            source.execute("CREATE TABLE zm_delta_stream (id INT PRIMARY KEY, pad VARCHAR(128)) ZONE_MAP=ON ZONE_SIZE=64K").unwrap();
            let values = (0..256).map(|id| format!("({id},'value-{id:08}-tail')")).collect::<Vec<_>>().join(",");
            source.execute(&format!("INSERT INTO zm_delta_stream VALUES {values}")).unwrap();
            let db = source.database_for_replication();
            db.rebuild_zone_map_for_table(&db.table("test.zm_delta_stream").unwrap()).unwrap();
            let start = db.wal_log().lock().next_lsn();
            for update in 0..8 {
                source.execute(&format!("UPDATE zm_delta_stream SET pad='changed-{update}' WHERE id=4")).unwrap();
            }
            let log = db.wal_log();
            let end = {
                let mut log = log.lock();
                log.flush().unwrap();
                log.next_lsn() - 1
            };
            assert!(LogManager::scan(&source_path).unwrap().records.iter().any(|record|
                record.lsn >= start && matches!(&record.payload, WalPayload::PageDelta { path, .. } if path.ends_with(".zm"))),
                "the actual SQL update stream must exercise page delta transport");
            end
        }).await.unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while replica_engine
            .database_for_replication()
            .replica_applied_lsn()
            < target_lsn
        {
            assert!(
                tokio::time::Instant::now() < deadline,
                "delta stream did not catch up"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let replica = Arc::clone(&replica_engine);
        tokio::task::spawn_blocking(move || {
            let QueryResponse::ResultSet(pruned) = replica.execute(
                "SELECT /*+ ZONE_MAP(zm_delta_stream) */ id FROM zm_delta_stream WHERE pad='changed-7'").unwrap() else { panic!("result set") };
            let QueryResponse::ResultSet(oracle) = replica.execute(
                "SELECT /*+ NO_ZONE_MAP(zm_delta_stream) */ id FROM zm_delta_stream WHERE pad='changed-7'").unwrap() else { panic!("result set") };
            assert_eq!(pruned.rows, vec![vec![Value::Int(4)]]);
            assert_eq!(pruned.rows, oracle.rows);
        }).await.unwrap();
        shutdown.notify_waiters();
        let _ = tokio::time::timeout(Duration::from_secs(2), join).await;
        master_service.shutdown().await;
    }

    #[tokio::test]
    async fn physical_replica_zone_map_respects_old_repeatable_read_snapshot() {
        let (master_engine, _master_dir, master_service) = build_master_engine().await;
        let (replica_engine, _replica_dir, shutdown, join) =
            spawn_replica(master_service.bind_addr()).await;
        replica_engine
            .database_for_replication()
            .set_physical_replica_mode(true)
            .unwrap();
        let source = Arc::clone(&master_engine);
        let initial_lsn = tokio::task::spawn_blocking(move || {
            source
                .execute("SET GLOBAL mydb_undo_retention_ms=600000")
                .unwrap();
            source
                .execute(
                    "CREATE TABLE zm_old_rr (id INT, pad VARCHAR(700)) ZONE_MAP=ON ZONE_SIZE=64K",
                )
                .unwrap();
            source
                .execute("INSERT INTO zm_old_rr VALUES (1,'keep'),(9000,'old snapshot')")
                .unwrap();
            let db = source.database_for_replication();
            db.rebuild_zone_map_for_table(&db.table("test.zm_old_rr").unwrap())
                .unwrap();
            let log = db.wal_log();
            let mut log = log.lock();
            log.flush().unwrap();
            log.next_lsn().saturating_sub(1)
        })
        .await
        .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while replica_engine
            .database_for_replication()
            .replica_applied_lsn()
            < initial_lsn
        {
            assert!(
                tokio::time::Instant::now() < deadline,
                "initial replica catch-up timed out"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let reader = Arc::new(PlMutex::new(crate::engine::SessionContext::default()));
        let session = Arc::clone(&reader);
        let replica = Arc::clone(&replica_engine);
        tokio::task::spawn_blocking(move || {
            let mut session = session.lock();
            session.connection_id = 900;
            replica
                .execute_with_session(
                    &mut session,
                    "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
                )
                .unwrap();
            assert_eq!(
                session.isolation,
                crate::storage::IsolationLevel::RepeatableRead
            );
            replica
                .execute_with_session(
                    &mut session,
                    "START TRANSACTION READ ONLY, WITH CONSISTENT SNAPSHOT",
                )
                .unwrap();
            let QueryResponse::ResultSet(rows) = replica
                .execute_with_session(
                    &mut session,
                    "SELECT /*+ NO_ZONE_MAP(zm_old_rr) */ id FROM zm_old_rr WHERE id=9000",
                )
                .unwrap()
            else {
                panic!("expected rows");
            };
            assert_eq!(rows.rows, vec![vec![Value::Int(9000)]]);
            assert!(session.txn_snapshot_scn.is_some());
            assert!(session.txn_snapshot_guard.is_some());
        })
        .await
        .unwrap();
        let source = Arc::clone(&master_engine);
        let rebuilt_lsn = tokio::task::spawn_blocking(move || {
            source
                .execute("DELETE FROM zm_old_rr WHERE id=9000")
                .unwrap();
            let db = source.database_for_replication();
            db.rebuild_zone_map_for_table(&db.table("test.zm_old_rr").unwrap())
                .unwrap();
            let log = db.wal_log();
            let mut log = log.lock();
            log.flush().unwrap();
            log.next_lsn().saturating_sub(1)
        })
        .await
        .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while replica_engine
            .database_for_replication()
            .replica_applied_lsn()
            < rebuilt_lsn
        {
            assert!(
                tokio::time::Instant::now() < deadline,
                "rebuilt replica catch-up timed out"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let replica = Arc::clone(&replica_engine);
        tokio::task::spawn_blocking(move || {
            let mut reader = reader.lock();
            let db = replica.database_for_replication();
            let table = db.table("test.zm_old_rr").unwrap();
            let filter = crate::parser::Expr::Binary {
                left: Box::new(crate::parser::Expr::Identifier("id".into())),
                op: crate::parser::BinaryOp::Eq,
                right: Box::new(crate::parser::Expr::Literal(crate::parser::Literal::Int(9000))),
            };
            let candidates = db.zone_map_candidate_ranges_for_plan(&table, &filter);
            assert!(candidates.as_ref().is_none_or(|ranges| !ranges.is_empty()),
                "a rebuilt source summary cannot exclude a zone for an older replica snapshot: {candidates:?}");
            let QueryResponse::ResultSet(oracle) = replica.execute_with_session(&mut reader,
                "SELECT /*+ NO_ZONE_MAP(zm_old_rr) */ id FROM zm_old_rr WHERE id=9000").unwrap() else { panic!("expected rows"); };
            assert_eq!(oracle.rows, vec![vec![Value::Int(9000)]], "old row remains visible to the replica snapshot");
            let QueryResponse::ResultSet(pruned) = replica.execute_with_session(&mut reader,
                "SELECT /*+ ZONE_MAP(zm_old_rr) */ id FROM zm_old_rr WHERE id=9000").unwrap() else { panic!("expected rows"); };
            assert_eq!(pruned.rows, oracle.rows, "a source rebuild must not prune older replica snapshots");
            replica.execute_with_session(&mut reader, "ROLLBACK").unwrap();
        }).await.unwrap();
        shutdown.notify_waiters();
        let _ = tokio::time::timeout(Duration::from_secs(2), join).await;
        master_service.shutdown().await;
    }

    #[tokio::test]
    async fn live_master_writes_after_replica_connects_become_visible() {
        let (master_engine, _master_dir, master_service) = build_master_engine().await;
        let bind_addr = master_service.bind_addr();
        // Pre-DDL so the schema exists on both sides before replica
        // attaches; this tests live-stream apply (not catch-up).
        let me = Arc::clone(&master_engine);
        tokio::task::spawn_blocking(move || {
            me.execute("CREATE TABLE t2 (a INT)").expect("create table");
        })
        .await
        .unwrap();

        let (replica_engine, _replica_dir, shutdown, join) = spawn_replica(bind_addr).await;
        // Wait for the replica's initial catch-up of the CREATE TABLE.
        tokio::time::sleep(Duration::from_millis(200)).await;

        let me2 = Arc::clone(&master_engine);
        tokio::task::spawn_blocking(move || {
            me2.execute("INSERT INTO t2 VALUES (42)")
                .expect("post-connect insert");
        })
        .await
        .unwrap();

        let rows = poll_select_rows(&replica_engine, "SELECT a FROM t2", 1, 5_000).await;
        assert_eq!(rows.len(), 1, "expected 1 row on replica: {:?}", rows);
        match &rows[0][0] {
            Value::Int(42) => {}
            Value::UInt(42) => {}
            other => panic!("expected 42, got {:?}", other),
        }

        shutdown.notify_waiters();
        let _ = tokio::time::timeout(Duration::from_secs(2), join).await;
        master_service.shutdown().await;
    }
}

// ============================================================
// Receiver lifecycle tests (Item 2): START/STOP MYDB REPLICATION
// drives a real receiver task against the captured tokio runtime.
// ============================================================

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    use crate::engine::executor::StorageQueryEngine;
    use crate::engine::{QueryLogConfig, QueryResponse, SessionRegistry};
    use crate::repl::config::ReplConfig;
    use crate::repl::master::{self, MasterServiceInit, SlotRegistry};
    use crate::repl::role::{generate_uuid_bytes, generate_uuid_string};
    use crate::repl::{MydbReplManager, ReceiverLifecycle, Role};
    use crate::types::Value;
    use std::sync::atomic::{AtomicBool, AtomicU64};
    use std::sync::Mutex as StdMutex;

    /// Build a manager that drives a real receiver task without
    /// touching `OnceLock`-backed `GLOBAL`. The manager's role state
    /// is forced to Replica with `master_endpoint` pointing at
    /// `master_addr`.
    fn build_manager(
        master_addr: std::net::SocketAddr,
        data_dir: std::path::PathBuf,
    ) -> Arc<MydbReplManager> {
        if crate::repl::replica::progress::load_progress(&data_dir)
            .unwrap()
            .is_none()
        {
            std::fs::write(data_dir.join(".mydb-test-fresh-data-dir"), []).unwrap();
        }
        let engine = Arc::new(
            StorageQueryEngine::open_full(
                data_dir.clone(),
                QueryLogConfig::default(),
                Arc::new(SessionRegistry::new()),
            )
            .expect("open replica engine"),
        );
        let slots = Arc::new(PlMutex::new(SlotRegistry::open(&data_dir).unwrap()));
        let state = crate::repl::role::RoleState {
            metadata_version: crate::repl::role::REPLICATION_METADATA_VERSION,
            role: Role::Replica,
            node_uuid: generate_uuid_string(),
            cluster_uuid: String::new(),
            timeline_uuid: String::new(),
            parent_timeline_uuid: None,
            fork_lsn: 0,
            generation: 1,
            write_fence: None,
            promotion_pending: false,
            promotion_target_lsn: None,
            master_endpoint: Some(format!("{}:{}", master_addr.ip(), master_addr.port())),
            epoch: 1,
            promoted_at_ms: 0,
            demoted_at_ms: 0,
        };
        let state = crate::repl::role::load(&data_dir).unwrap().unwrap_or(state);
        crate::repl::role::save(&data_dir, &state).unwrap();
        Arc::new(MydbReplManager {
            data_dir,
            engine,
            role_state: Arc::new(StdMutex::new(state)),
            config: ReplConfig {
                heartbeat: Duration::from_millis(10),
                reconnect_backoff: Duration::from_millis(10),
                ..ReplConfig::test_defaults()
            },
            slots,
            transition: PlMutex::new(()),
            semi_sync_acked_commits: AtomicU64::new(0),
            semi_sync_timeout_commits: AtomicU64::new(0),
            semi_sync_degraded_commits: AtomicU64::new(0),
            master_writes_frozen: AtomicBool::new(false),
            backup_capture_frozen: AtomicBool::new(false),
            runtime_handle: PlMutex::new(None),
            receiver_lifecycle: Arc::new(PlMutex::new(ReceiverLifecycle::new())),
            master_service: Arc::new(PlMutex::new(None)),
            receiver_status: Arc::new(PlMutex::new(String::new())),
            receiver_progress: Arc::new(PlMutex::new(ReplicaProgress::default())),
        })
    }

    #[tokio::test]
    async fn release_audit_busy_source_backup_does_not_hold_capture_freeze() {
        let dir = TempDir::new().unwrap();
        let manager = build_manager("127.0.0.1:1".parse().unwrap(), dir.path().to_path_buf());
        let engine = manager.engine.clone();
        let mut session = crate::engine::SessionContext::default();
        engine
            .execute("CREATE TABLE busy_capture (id INT PRIMARY KEY)")
            .unwrap();
        engine.execute_with_session(&mut session, "BEGIN").unwrap();
        engine
            .execute_with_session(&mut session, "INSERT INTO busy_capture VALUES (1)")
            .unwrap();
        let db = engine.database_for_replication();
        let mut writer = tokio::io::BufWriter::new(tokio::io::sink());
        let started = std::time::Instant::now();
        let result =
            crate::repl::base_backup::stream_base_backup(&mut writer, db, dir.path()).await;
        assert!(result.is_err());
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(!db.capture.frozen.load(std::sync::atomic::Ordering::Acquire));
        assert!(!db.capture.active.load(std::sync::atomic::Ordering::Acquire));
        engine
            .execute_with_session(&mut session, "ROLLBACK")
            .unwrap();
    }

    #[test]
    fn release_audit_concurrent_admin_transition_is_rejected() {
        let dir = TempDir::new().unwrap();
        let manager = build_manager("127.0.0.1:1".parse().unwrap(), dir.path().to_path_buf());
        let _transition = manager.transition.lock();
        let err = manager.handle_reset_replication().unwrap_err();
        assert_eq!(
            err.code(),
            crate::error::catalog::MYDB_ER_ROLE_CHANGE_BLOCKED.code()
        );
    }

    #[tokio::test]
    async fn release_audit_unseeded_replica_reports_source_configuration() {
        let dir = TempDir::new().unwrap();
        let mut role = crate::repl::role::RoleState::new_standalone();
        role.role = Role::Replica;
        crate::repl::role::save(dir.path(), &role).unwrap();
        let err = crate::repl::maybe_bootstrap_replica(dir.path())
            .await
            .unwrap_err();
        assert!(err.message().contains("MYDB_REPL_MASTER_HOST"));
        assert!(crate::repl::role::load(dir.path()).unwrap().is_some());
    }

    #[tokio::test]
    async fn release_audit_repeated_apply_failure_stops_without_reseed_marker() {
        let (source, _source_dir, service) = build_master_with_engine().await;
        let dir = TempDir::new().unwrap();
        let manager = build_manager(service.bind_addr(), dir.path().to_path_buf());
        manager.install_runtime_handle_for_tests(tokio::runtime::Handle::current());
        let db = manager.engine.database_for_replication();
        let baseline = db.wal_log().lock().last_lsn();
        db.set_physical_replica_mode(true).unwrap();
        std::fs::create_dir(dir.path().join("unwritable-redo-target")).unwrap();
        {
            let log = source.database_for_replication().wal_log();
            let mut log = log.lock();
            log.append_reserved(
                baseline + 1,
                0,
                crate::wal::WalPayload::SlotReclaim {
                    path: "unwritable-redo-target".into(),
                    page_id: 0,
                    slot_id: 0,
                },
            )
            .unwrap();
            log.flush().unwrap();
        }
        crate::wal::LogManager::sync_durable_to(
            &source.database_for_replication().wal_log(),
            baseline + 1,
        )
        .unwrap();
        let progress = ReplicaProgress {
            received_lsn: baseline,
            durable_lsn: baseline,
            applied_lsn: baseline,
            durable_applied_lsn: baseline,
            ..ReplicaProgress::default()
        };
        crate::repl::replica::progress::save_progress(dir.path(), &progress).unwrap();
        manager.start_receiver_if_needed().unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while manager.is_receiver_running() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            !manager.is_receiver_running(),
            "{}",
            manager.receiver_status.lock()
        );
        assert!(manager
            .receiver_status
            .lock()
            .contains("repeated apply failure"));
        assert!(crate::repl::replica::progress::resync_marker_reason(dir.path()).is_none());
        service.shutdown().await;
    }

    #[test]
    fn release_audit_idempotent_promote_reports_listener_bind_failure() {
        let dir = TempDir::new().unwrap();
        let mut manager = build_manager("127.0.0.1:1".parse().unwrap(), dir.path().to_path_buf());
        let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        Arc::get_mut(&mut manager).unwrap().config.bind_port =
            occupied.local_addr().unwrap().port();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        manager.install_runtime_handle_for_tests(runtime.handle().clone());
        manager.set_role(Role::Master, None).unwrap();
        let err = manager
            .handle_promote(
                "PROMOTE TO MASTER",
                &crate::engine::SessionContext::default(),
            )
            .unwrap_err();
        assert!(err.message().contains("bind"));
    }

    #[test]
    fn release_audit_pending_promotion_blocks_reset_and_repoint() {
        let dir = TempDir::new().unwrap();
        let manager = build_manager("127.0.0.1:1".parse().unwrap(), dir.path().to_path_buf());
        let runtime = tokio::runtime::Runtime::new().unwrap();
        manager.install_runtime_handle_for_tests(runtime.handle().clone());
        {
            let mut state = manager.role_state.lock().unwrap();
            state.promotion_pending = true;
            state.promotion_target_lsn = Some(10);
            crate::repl::role::save(dir.path(), &state).unwrap();
        }
        assert!(manager.handle_reset_replication().is_err());
        assert!(manager
            .handle_change_master("CHANGE MYDB MASTER TO HOST='127.0.0.1', PORT=3371")
            .is_err());
        assert_eq!(manager.role(), Role::Replica);
    }

    #[test]
    fn release_audit_unconfigured_replica_reopens_existing_catalog() {
        let dir = TempDir::new().unwrap();
        let manager = build_manager("127.0.0.1:1".parse().unwrap(), dir.path().to_path_buf());
        manager
            .engine
            .database_for_replication()
            .checkpoint()
            .unwrap();
        {
            let mut state = manager.role_state.lock().unwrap();
            state.master_endpoint = None;
            crate::repl::role::save(dir.path(), &state).unwrap();
        }
        drop(manager);
        StorageQueryEngine::open_full(
            dir.path().to_path_buf(),
            QueryLogConfig::default(),
            Arc::new(SessionRegistry::new()),
        )
        .expect("unconfigured replica must remain administrable");
    }

    #[test]
    fn audit_reset_then_change_master_restores_replica_role() {
        let dir = TempDir::new().unwrap();
        let manager = build_manager("127.0.0.1:1".parse().unwrap(), dir.path().to_path_buf());
        let runtime = tokio::runtime::Runtime::new().unwrap();
        manager.install_runtime_handle_for_tests(runtime.handle().clone());
        manager.handle_reset_replication().unwrap();
        manager
            .handle_change_master("CHANGE MYDB MASTER TO HOST='127.0.0.1', PORT=3371")
            .unwrap();
        assert_eq!(manager.role(), Role::Replica);
        assert!(manager
            .engine
            .database_for_replication()
            .physical_replica_mode());
        assert!(crate::repl::replica::progress::resync_marker_reason(dir.path()).is_some());
        assert_eq!(
            crate::repl::role::load(dir.path()).unwrap().unwrap().role,
            Role::Replica
        );
    }

    #[test]
    fn audit_resume_admin_writes_does_not_release_backup_freeze() {
        let dir = TempDir::new().unwrap();
        let manager = build_manager("127.0.0.1:1".parse().unwrap(), dir.path().to_path_buf());
        manager.role_state.lock().unwrap().role = Role::Master;
        manager
            .master_writes_frozen
            .store(true, std::sync::atomic::Ordering::Release);
        manager
            .backup_capture_frozen
            .store(true, std::sync::atomic::Ordering::Release);
        manager.handle_resume_master_writes().unwrap();
        assert!(manager
            .backup_capture_frozen
            .load(std::sync::atomic::Ordering::Acquire));
        assert!(!manager
            .master_writes_frozen
            .load(std::sync::atomic::Ordering::Acquire));
    }

    #[test]
    fn audit_slot_status_exposes_invalidated_and_abandoned_counts() {
        let dir = TempDir::new().unwrap();
        let manager = build_manager("127.0.0.1:1".parse().unwrap(), dir.path().to_path_buf());
        {
            let mut slots = manager.slots.lock();
            slots.create_or_get("active", 1);
            slots.create_or_get("invalidated", 1);
            slots.mark_invalidated("invalidated");
            slots.create_or_get("abandoned", 1);
            slots.mark_abandoned("abandoned");
        }
        let crate::engine::QueryResponse::ResultSet(result) = manager.show_master_status().unwrap()
        else {
            panic!()
        };
        for name in [
            "Active_Slot_Count",
            "Invalidated_Slot_Count",
            "Abandoned_Slot_Count",
        ] {
            let i = result
                .columns
                .iter()
                .position(|col| col.name == name)
                .unwrap();
            assert_eq!(result.rows[0][i], crate::types::Value::UInt(1));
        }
        let crate::engine::QueryResponse::ResultSet(result) = manager.show_replicas().unwrap()
        else {
            panic!()
        };
        assert!(result
            .columns
            .iter()
            .any(|col| col.name == "Replica_Acked_LSN"));
    }

    #[test]
    fn audit_force_resync_rejects_unavailable_restart_before_marking_data() {
        let dir = TempDir::new().unwrap();
        let manager = build_manager("127.0.0.1:1".parse().unwrap(), dir.path().to_path_buf());
        // Unit-test processes do not cache server launch args.
        assert!(!crate::mysql::shutdown::restart_supported());
        let err = manager
            .handle_force_resync(&crate::engine::SessionContext::default())
            .unwrap_err();
        assert_eq!(
            err.code(),
            crate::error::catalog::ER_RESTART_SERVER_FAILED.code()
        );
        assert!(crate::repl::replica::progress::resync_marker_reason(dir.path()).is_none());
        assert!(!crate::mysql::shutdown::is_shutting_down());
    }

    #[test]
    fn audit_reset_clears_physical_replica_mode() {
        let dir = TempDir::new().unwrap();
        let manager = build_manager("127.0.0.1:1".parse().unwrap(), dir.path().to_path_buf());
        manager
            .engine
            .database_for_replication()
            .set_physical_replica_mode(true)
            .unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        manager.install_runtime_handle_for_tests(runtime.handle().clone());
        manager.handle_reset_replication().unwrap();
        assert_eq!(manager.role(), Role::Standalone);
        assert!(!manager
            .engine
            .database_for_replication()
            .physical_replica_mode());
        assert_eq!(
            crate::repl::role::load(dir.path()).unwrap().unwrap().role,
            Role::Standalone
        );
    }

    async fn build_master_with_engine() -> (Arc<StorageQueryEngine>, TempDir, MasterService) {
        let dir = fresh_database_tempdir();
        let engine = Arc::new(
            StorageQueryEngine::open_full(
                dir.path().to_path_buf(),
                QueryLogConfig::default(),
                Arc::new(SessionRegistry::new()),
            )
            .expect("open master engine"),
        );
        let db = engine.database_for_replication().clone();
        let log = db.wal_log();
        let slots = Arc::new(PlMutex::new(SlotRegistry::open(dir.path()).unwrap()));
        let service = master::start_master_service(MasterServiceInit {
            bind_host: "127.0.0.1".into(),
            bind_port: 0,
            config: ReplConfig {
                heartbeat: Duration::from_millis(10),
                ..ReplConfig::test_defaults()
            },
            master_uuid: generate_uuid_bytes(),
            epoch: 1,
            server_version: "test-master".into(),
            log,
            slots,
            db: Some(Arc::clone(&db)),
            data_dir: Some(dir.path().to_path_buf()),
            role_state: None,
        })
        .await
        .unwrap();
        (engine, dir, service)
    }

    async fn select_count(engine: &Arc<StorageQueryEngine>, sql: &str) -> Vec<Vec<Value>> {
        let engine_clone = Arc::clone(engine);
        let sql_owned = sql.to_string();
        let outcome = tokio::task::spawn_blocking(move || engine_clone.execute(&sql_owned))
            .await
            .unwrap();
        match outcome {
            Ok(QueryResponse::ResultSet(rs)) => rs.rows,
            _ => Vec::new(),
        }
    }

    async fn wait_for_rows(
        engine: &Arc<StorageQueryEngine>,
        sql: &str,
        target: usize,
        deadline_ms: u64,
    ) -> usize {
        let deadline = std::time::Instant::now() + Duration::from_millis(deadline_ms);
        loop {
            let rows = select_count(engine, sql).await;
            if rows.len() >= target {
                return rows.len();
            }
            if std::time::Instant::now() > deadline {
                return rows.len();
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
    }

    #[tokio::test]
    async fn start_then_stop_then_start_replication_catches_up() {
        let (master_engine, _master_dir, master_service) = build_master_with_engine().await;
        let bind_addr = master_service.bind_addr();
        let replica_dir = TempDir::new().unwrap();
        initialize_replica_fixture(bind_addr, replica_dir.path()).await;
        let manager = build_manager(bind_addr, replica_dir.path().to_path_buf());
        manager.install_runtime_handle_for_tests(tokio::runtime::Handle::current());
        let replica_engine = Arc::clone(manager.engine());

        // Master sends 1 batch.
        let me = Arc::clone(&master_engine);
        tokio::task::spawn_blocking(move || {
            me.execute("CREATE TABLE t3 (a INT)").expect("ddl");
            me.execute("INSERT INTO t3 VALUES (1), (2)").expect("ins1");
        })
        .await
        .unwrap();

        // 1. START → catches up.
        manager.handle_start_replication().expect("start");
        assert!(manager.is_receiver_running());
        let n = wait_for_rows(&replica_engine, "SELECT a FROM t3", 2, 5_000).await;
        assert_eq!(n, 2, "after first START, replica should see 2 rows");

        // 2. STOP → master keeps writing, replica's LSN should not move.
        // Use the deterministic stop_and_wait so the receiver task has
        // definitely drained before we let the master write more
        // (otherwise an in-flight WalChunk could still land on the
        // replica during the gap, making the assertion flaky).
        manager.stop_receiver_and_wait().await;
        assert!(!manager.is_receiver_running());

        let progress_after_stop = crate::repl::replica::load_progress(replica_dir.path())
            .unwrap()
            .unwrap_or_default();

        let me2 = Arc::clone(&master_engine);
        tokio::task::spawn_blocking(move || {
            me2.execute("INSERT INTO t3 VALUES (3), (4), (5)")
                .expect("ins2");
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let n_during_stop = select_count(&replica_engine, "SELECT a FROM t3")
            .await
            .len();
        assert_eq!(
            n_during_stop, 2,
            "replica saw new rows while STOP MYDB REPLICATION was in effect"
        );

        // 3. START again → catches up to the 3 extra rows.
        manager.handle_start_replication().expect("start again");
        assert!(manager.is_receiver_running());
        let n_after_restart = wait_for_rows(&replica_engine, "SELECT a FROM t3", 5, 5_000).await;
        assert_eq!(
            n_after_restart, 5,
            "after second START, replica should see all 5 rows"
        );
        // Persisted progress must have advanced past the first stop.
        // The durable watermark is group-fsync paced; poll briefly.
        let mut progress_after_restart = ReplicaProgress::default();
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while std::time::Instant::now() < deadline {
            progress_after_restart = crate::repl::replica::load_progress(replica_dir.path())
                .unwrap()
                .unwrap_or_default();
            if progress_after_restart.durable_applied_lsn > progress_after_stop.durable_applied_lsn
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            progress_after_restart.durable_applied_lsn > progress_after_stop.durable_applied_lsn,
            "replica progress must advance after second START (was {}, now {})",
            progress_after_stop.durable_applied_lsn,
            progress_after_restart.durable_applied_lsn
        );

        manager.stop_receiver_and_wait().await;
        master_service.shutdown().await;
    }

    #[tokio::test]
    async fn start_replication_is_idempotent() {
        let (_, _master_dir, master_service) = build_master_with_engine().await;
        let bind_addr = master_service.bind_addr();
        let replica_dir = TempDir::new().unwrap();
        let manager = build_manager(bind_addr, replica_dir.path().to_path_buf());
        manager.install_runtime_handle_for_tests(tokio::runtime::Handle::current());

        manager.handle_start_replication().expect("first start");
        assert!(manager.is_receiver_running());
        manager
            .handle_start_replication()
            .expect("second start no-op");
        assert!(manager.is_receiver_running());

        manager.stop_receiver_and_wait().await;
        master_service.shutdown().await;
    }

    #[tokio::test]
    async fn stop_replication_when_not_running_is_noop() {
        // No live master required — STOP just unwinds receiver state.
        let replica_dir = TempDir::new().unwrap();
        let manager = build_manager(
            // Bogus address; we never start the receiver.
            "127.0.0.1:1".parse().unwrap(),
            replica_dir.path().to_path_buf(),
        );
        manager.install_runtime_handle_for_tests(tokio::runtime::Handle::current());

        assert!(!manager.is_receiver_running());
        manager.handle_stop_replication().expect("stop noop");
    }
}

// ============================================================
// Streaming base backup tests (Item 3).
// ============================================================

#[cfg(test)]
mod base_backup_tests {
    use super::*;
    use crate::engine::executor::StorageQueryEngine;
    use crate::engine::{QueryLogConfig, QueryResponse, SessionRegistry};
    use crate::repl::base_backup::perform_base_backup_into_with_config;
    use crate::repl::master::{self, MasterServiceInit, SlotRegistry};
    use crate::repl::role::{generate_uuid_bytes, generate_uuid_string};
    use crate::types::Value;

    /// Bring up a master with a full `Database` bound to the
    /// `MasterServiceInit` so base backup is actually supported.
    async fn build_master_with_base_backup() -> (Arc<StorageQueryEngine>, TempDir, MasterService) {
        let dir = fresh_database_tempdir();
        let engine = Arc::new(
            StorageQueryEngine::open_full(
                dir.path().to_path_buf(),
                QueryLogConfig::default(),
                Arc::new(SessionRegistry::new()),
            )
            .expect("open master engine"),
        );
        let db = engine.database_for_replication().clone();
        let log = db.wal_log();
        let slots = Arc::new(PlMutex::new(SlotRegistry::open(dir.path()).unwrap()));
        let service = master::start_master_service(MasterServiceInit {
            bind_host: "127.0.0.1".into(),
            bind_port: 0,
            config: ReplConfig {
                heartbeat: Duration::from_millis(10),
                ..ReplConfig::test_defaults()
            },
            master_uuid: generate_uuid_bytes(),
            epoch: 1,
            server_version: "test-master".into(),
            log,
            slots,
            db: Some(Arc::clone(&db)),
            data_dir: Some(dir.path().to_path_buf()),
            role_state: None,
        })
        .await
        .unwrap();
        (engine, dir, service)
    }

    #[tokio::test]
    async fn audit_base_backup_permit_covers_blocked_transfer() {
        use crate::repl::protocol::{self, Handshake, Message};
        let (_engine, dir, service) = build_master_with_base_backup().await;
        // A sparse, non-engine file makes the sender block on socket writes
        // without requiring a large SQL fixture or timing a checkpoint.
        let padding = dir.path().join("schemas/backup-padding");
        std::fs::File::create(padding)
            .unwrap()
            .set_len(64 * 1024 * 1024)
            .unwrap();
        let handshake = || {
            Message::Handshake(Handshake {
                auth_token: ReplConfig::test_defaults().auth_token,
                protocol_version: protocol::PROTOCOL_VERSION,
                replica_uuid: generate_uuid_bytes(),
                requested_start_lsn: 0,
                last_known_master_uuid: [0; 16],
                last_known_epoch: 0,
                cluster_uuid: [0; 16],
                last_known_timeline_uuid: [0; 16],
                client_version: "audit".into(),
                request_base_backup: true,
                max_chunk_bytes: 4096,
                format_compatibility:
                    crate::storage::format_versions::native_replication_compatibility(),
            })
        };
        let mut first = tokio::net::TcpStream::connect(service.bind_addr())
            .await
            .unwrap();
        protocol::write_frame_async(&mut first, &handshake())
            .await
            .unwrap();
        let Message::HandshakeAck(ack) = protocol::read_frame_async(&mut first).await.unwrap()
        else {
            panic!()
        };
        assert!(ack.accepted);
        assert!(matches!(
            protocol::read_frame_async(&mut first).await.unwrap(),
            Message::BaseBackupStart(_)
        ));
        let mut second = tokio::net::TcpStream::connect(service.bind_addr())
            .await
            .unwrap();
        protocol::write_frame_async(&mut second, &handshake())
            .await
            .unwrap();
        let Message::HandshakeAck(ack) = protocol::read_frame_async(&mut second).await.unwrap()
        else {
            panic!()
        };
        assert!(!ack.accepted);
        assert!(ack.reason.contains("concurrency limit"));
        drop(first);
        drop(second);
        // Cancellation releases the permit: a later backup can start.
        let mut accepted = false;
        for _ in 0..100 {
            let mut next = tokio::net::TcpStream::connect(service.bind_addr())
                .await
                .unwrap();
            protocol::write_frame_async(&mut next, &handshake())
                .await
                .unwrap();
            if let Message::HandshakeAck(ack) = protocol::read_frame_async(&mut next).await.unwrap()
            {
                if ack.accepted {
                    accepted = true;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(accepted, "failed backup leaked its permit");
        service.shutdown().await;
    }

    #[tokio::test]
    async fn release_audit_base_backup_stream_uses_frozen_files() {
        use crate::repl::protocol::{self, Handshake, Message};
        let (_engine, dir, service) = build_master_with_base_backup().await;
        let relative = "schemas/mutable-backup-fixture";
        let original = vec![0x35; 8 * 1024 * 1024];
        // Keep the writer open: Windows directory enumeration may report
        // a stale length until close, while the file handle sees every byte.
        let mut live_writer = std::fs::File::create(dir.path().join(relative)).unwrap();
        std::io::Write::write_all(&mut live_writer, &original).unwrap();
        let mut stream = tokio::net::TcpStream::connect(service.bind_addr())
            .await
            .unwrap();
        let handshake = Message::Handshake(Handshake {
            auth_token: ReplConfig::test_defaults().auth_token,
            protocol_version: protocol::PROTOCOL_VERSION,
            replica_uuid: generate_uuid_bytes(),
            requested_start_lsn: 0,
            last_known_master_uuid: [0; 16],
            last_known_epoch: 0,
            cluster_uuid: [0; 16],
            last_known_timeline_uuid: [0; 16],
            client_version: "release-audit".into(),
            request_base_backup: true,
            max_chunk_bytes: 4096,
            format_compatibility: crate::storage::format_versions::native_replication_compatibility(
            ),
        });
        protocol::write_frame_async(&mut stream, &handshake)
            .await
            .unwrap();
        assert!(
            matches!(protocol::read_frame_async(&mut stream).await.unwrap(), Message::HandshakeAck(ack) if ack.accepted)
        );
        assert!(matches!(
            protocol::read_frame_async(&mut stream).await.unwrap(),
            Message::BaseBackupStart(_)
        ));
        // A stalled network receiver leaves most of this file unread by the
        // sender. Mutate its existing inode, just like an in-place page flush.
        std::fs::write(dir.path().join(relative), vec![0x99; original.len()]).unwrap();
        let mut captured = Vec::new();
        loop {
            match protocol::read_frame_async(&mut stream).await.unwrap() {
                Message::BaseBackupChunk(chunk) if chunk.path == relative => {
                    captured.extend_from_slice(&chunk.bytes)
                }
                Message::BaseBackupEnd(_) => break,
                Message::BaseBackupChunk(_) => {}
                other => panic!("unexpected backup frame: {other:?}"),
            }
        }
        assert_eq!(captured, original);
        drop(stream);
        service.shutdown().await;
    }

    #[tokio::test]
    async fn streaming_base_backup_seeds_fresh_replica_with_master_data() {
        let (master_engine, _master_dir, master_service) = build_master_with_base_backup().await;
        let bind_addr = master_service.bind_addr();
        // Master populates a real table with > 1 page of rows so we
        // exercise multi-chunk streaming (rows are small but >1MB
        // total is unrealistic for a tight test, so we instead
        // verify the row-count round-trips and read back the data).
        let me = Arc::clone(&master_engine);
        tokio::task::spawn_blocking(move || {
            me.execute("CREATE TABLE bb_vec (id INT PRIMARY KEY, v VECTOR(3) NOT NULL)")
                .unwrap();
            me.execute("INSERT INTO bb_vec VALUES (1,'[10,0,0]'),(2,'[20,0,0]'),(3,'[30,0,0]')")
                .unwrap();
            me.execute("CREATE INDEX ivf_v ON bb_vec(v) USING IVF WITH (nlist=2)")
                .unwrap();
            me.execute("CREATE TABLE bb (k INT PRIMARY KEY, v VARCHAR(64))")
                .expect("create table");
            me.execute(
                "INSERT INTO bb (k, v) \
                 SELECT n, CONCAT('row-', n) FROM generate_series(0, 99) AS gs(n)",
            )
            .expect("insert");
            me.execute(
                "CREATE TABLE bb_part (k INT PRIMARY KEY, v VARCHAR(64)) \
                 PARTITION BY HASH(k) PARTITIONS 2",
            )
            .expect("create partitioned table");
            me.execute(
                "INSERT INTO bb_part VALUES \
                 (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')",
            )
            .expect("insert partition baseline");
        })
        .await
        .unwrap();

        let replica_dir = TempDir::new().unwrap();
        // Fresh replica: empty data dir. Driving
        // `perform_base_backup_into` should pull a complete file
        // snapshot from the master.
        let replica_uuid = generate_uuid_string();
        let backup_lsn = perform_base_backup_into_with_config(
            &bind_addr.ip().to_string(),
            bind_addr.port(),
            &replica_uuid,
            replica_dir.path(),
            ReplConfig::test_defaults(),
        )
        .await
        .expect("perform_base_backup_into");
        assert!(backup_lsn > 0, "backup_lsn should reflect master state");

        // The replica's data dir should now contain `schemas/test/`,
        // and a `system/replication/replica_progress.json`.
        assert!(
            crate::storage::naming::schemas_root(replica_dir.path()).exists(),
            "schemas/"
        );
        assert!(
            crate::storage::naming::system_replication_dir(replica_dir.path())
                .join("replica_progress.json")
                .exists(),
            "progress.json"
        );

        let mut replica_role = crate::repl::role::RoleState::new_standalone();
        replica_role.demote_to_replica(format!("{}:{}", bind_addr.ip(), bind_addr.port()));
        crate::repl::role::save(replica_dir.path(), &replica_role).unwrap();

        // Open a StorageQueryEngine on the bootstrapped data dir.
        // It runs recovery, loads the catalog, and reflects the
        // master's snapshot.
        let replica_dir_path = replica_dir.path().to_path_buf();
        let replica_engine = tokio::task::spawn_blocking(move || {
            StorageQueryEngine::open_full(
                replica_dir_path,
                QueryLogConfig::default(),
                Arc::new(SessionRegistry::new()),
            )
            .expect("open replica engine after base backup")
        })
        .await
        .unwrap();
        let replica_engine = Arc::new(replica_engine);
        let vector_sql =
            "SELECT /*+ IVF_NPROBE(2) */ id FROM bb_vec ORDER BY DISTANCE(v,'[0,0,0]') LIMIT 1";
        let re = Arc::clone(&replica_engine);
        let initial = tokio::task::spawn_blocking(move || re.execute(vector_sql))
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(initial, QueryResponse::ResultSet(ref rs) if rs.rows[0][0] == Value::Int(1))
        );

        let re = Arc::clone(&replica_engine);
        let rows = tokio::task::spawn_blocking(move || re.execute("SELECT k FROM bb ORDER BY k"))
            .await
            .unwrap()
            .expect("select on replica");
        match rows {
            QueryResponse::ResultSet(rs) => {
                assert_eq!(
                    rs.rows.len(),
                    100,
                    "replica after base backup should have all 100 master rows"
                );
            }
            other => panic!("unexpected: {:?}", other),
        }

        // Derived B-tree files are intentionally excluded from the streamed
        // snapshot.  Startup validation must rebuild every physical
        // partition leaf, not a non-existent logical-table index.  The heap
        // scan above can mask this bug, while FORCE INDEX / aggregate reads
        // expose it immediately.
        let re = Arc::clone(&replica_engine);
        let partition_aggregate = tokio::task::spawn_blocking(move || {
            re.execute(
                "SELECT COUNT(*), SUM(k), MIN(k), MAX(k) \
                 FROM bb_part FORCE INDEX(PRIMARY)",
            )
        })
        .await
        .unwrap()
        .expect("partition index aggregate on replica");
        match partition_aggregate {
            QueryResponse::ResultSet(rs) => {
                assert_eq!(rs.rows.len(), 1);
                assert_eq!(rs.rows[0][0], Value::Int(4));
                assert_eq!(crate::storage::value_to_string(&rs.rows[0][1]), "10");
                assert_eq!(rs.rows[0][2], Value::Int(1));
                assert_eq!(rs.rows[0][3], Value::Int(4));
            }
            other => panic!("unexpected partition aggregate: {:?}", other),
        }

        // Regression: a real process opens the backup, serves reads, and only
        // then starts receiving new source transactions. The old backup omitted
        // the segmented undo baseline; the first live UndoRecord referenced a
        // source offset absent locally, apply failed, and reconnect incorrectly
        // promoted the durable WAL tail to "applied". Keep streaming after the
        // initial read and require online visibility without a restart.
        let replica_db = replica_engine.database_for_replication().clone();
        let replica_log = replica_db.wal_log();
        let progress = crate::repl::replica::load_progress(replica_dir.path())
            .unwrap()
            .expect("base backup progress");
        let receiver = Receiver {
            master_host: bind_addr.ip().to_string(),
            master_port: bind_addr.port(),
            replica_uuid,
            config: ReplConfig {
                heartbeat: Duration::from_millis(10),
                ..ReplConfig::test_defaults()
            },
            data_dir: replica_dir.path().to_path_buf(),
            log: Arc::clone(&replica_log),
            db: Some(replica_db),
            initial_progress: progress,
            shutdown_flag: None,
        };
        let shutdown = Arc::new(Notify::new());
        let receiver_shutdown = Arc::clone(&shutdown);
        let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
        let (result_tx, mut result_rx) = tokio::sync::mpsc::unbounded_channel();
        let receiver_join = tokio::spawn(async move {
            let result = receiver
                .run_once(receiver_shutdown, move |event| {
                    let _ = event_tx.send(event);
                })
                .await;
            let message = result.as_ref().err().map(|err| err.message().to_string());
            let _ = result_tx.send(message);
            result
        });
        let connected = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if matches!(event_rx.recv().await, Some(ReceiverEvent::Connected { .. })) {
                    break;
                }
            }
        })
        .await;
        assert!(connected.is_ok(), "steady-state receiver did not connect");
        let me = Arc::clone(&master_engine);
        tokio::task::spawn_blocking(move || {
            me.execute("INSERT INTO bb_vec VALUES (4,'[0,0,0]')")
                .unwrap();
            me.execute("INSERT INTO bb (k, v) VALUES (100, 'after-backup')")
                .expect("post-backup insert");
        })
        .await
        .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut visible = false;
        while std::time::Instant::now() < deadline {
            let re = Arc::clone(&replica_engine);
            let count = tokio::task::spawn_blocking(move || {
                match re.execute("SELECT COUNT(*) FROM bb").unwrap() {
                    QueryResponse::ResultSet(rs) => match rs.rows[0][0] {
                        Value::Int(v) => v,
                        Value::UInt(v) => v as i64,
                        _ => 0,
                    },
                    _ => 0,
                }
            })
            .await
            .unwrap();
            if count == 101 {
                visible = true;
                break;
            }
            if let Ok(Some(ReceiverEvent::Disconnected { reason })) =
                tokio::time::timeout(Duration::from_millis(1), event_rx.recv()).await
            {
                panic!("receiver disconnected after base backup: {reason}");
            }
            if let Ok(message) = result_rx.try_recv() {
                panic!("receiver exited after base backup: {message:?}");
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        if !visible {
            let progress = crate::repl::replica::load_progress(replica_dir.path())
                .unwrap()
                .unwrap_or_default();
            let (next, durable) = {
                let log = replica_log.lock();
                (log.next_lsn(), log.durable_lsn())
            };
            let re = Arc::clone(&replica_engine);
            let rows = tokio::task::spawn_blocking(move || re.execute("SELECT k, v FROM bb"))
                .await
                .unwrap();
            panic!(
                "post-backup row was not visible without restart: progress={progress:?} local_next={next} local_durable={durable} rows={rows:?}"
            );
        }
        // The heap commit above is later than the vector delta. A warmed
        // replica must now see the new exact match through its IVF path.
        let re = Arc::clone(&replica_engine);
        let result = tokio::task::spawn_blocking(move || re.execute(vector_sql))
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(result, QueryResponse::ResultSet(ref rs) if rs.rows[0][0] == Value::Int(4)),
            "{result:?}"
        );
        shutdown.notify_waiters();
        let receiver_result = tokio::time::timeout(Duration::from_secs(2), receiver_join)
            .await
            .expect("receiver shutdown")
            .expect("receiver join");
        receiver_result.expect("receiver apply after base backup");

        master_service.shutdown().await;
    }

    #[tokio::test]
    async fn base_backup_handshake_rejected_when_master_lacks_db_handle() {
        // Master started WITHOUT a `db`/`data_dir` (the legacy
        // WAL-streaming-only path). A base backup request should
        // be refused with handshake.accepted=false rather than
        // hanging or panicking.
        let dir = TempDir::new().unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let slots = Arc::new(PlMutex::new(SlotRegistry::open(dir.path()).unwrap()));
        let service = master::start_master_service(MasterServiceInit {
            bind_host: "127.0.0.1".into(),
            bind_port: 0,
            config: ReplConfig {
                heartbeat: Duration::from_millis(10),
                ..ReplConfig::test_defaults()
            },
            master_uuid: generate_uuid_bytes(),
            epoch: 1,
            server_version: "test-master".into(),
            log,
            slots,
            db: None,
            data_dir: None,
            role_state: None,
        })
        .await
        .unwrap();
        let bind_addr = service.bind_addr();

        let replica_dir = TempDir::new().unwrap();
        let err = perform_base_backup_into_with_config(
            &bind_addr.ip().to_string(),
            bind_addr.port(),
            &generate_uuid_string(),
            replica_dir.path(),
            ReplConfig::test_defaults(),
        )
        .await
        .expect_err("base backup must fail when master has no db");
        assert_eq!(err.code(), 1235);
        service.shutdown().await;
    }
}
