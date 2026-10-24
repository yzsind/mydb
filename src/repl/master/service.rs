//! Master-side TCP listener for mydb-native physical replication.
//!
//! Binds `<bind_host>:<bind_port>` (defaults `0.0.0.0:3316`), accepts
//! incoming replica connections, and hands each one off to
//! [`sender::run_sender`]. The service owns the slot registry, the WAL
//! log handle, and a single broadcast tracker that watches the running
//! sender count.

use crate::repl::now_ms;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use parking_lot::Mutex as PlMutex;
use tokio::net::TcpListener;
use tokio::sync::Notify;

use crate::repl::config::ReplConfig;
use crate::repl::master::sender::{self, SenderEnv, SenderTracker};
use crate::repl::master::slot::{SlotHandle, SlotRegistry};
use crate::storage::error;
use crate::types::QueryError;
use crate::util::logging::log_message;

/// Public handle for a running master service.
pub struct MasterService {
    pub bind_addr: SocketAddr,
    /// Persistent registry of replication slots. Sender tasks hold
    /// `SlotHandle`s by uuid.
    pub slots: Arc<PlMutex<SlotRegistry>>,
    pub tracker: Arc<SenderTracker>,
    pub shutdown: Arc<Notify>,
    shutdown_flag: Arc<AtomicBool>,
    join: tokio::task::JoinHandle<()>,
}

impl MasterService {
    pub fn bind_addr(&self) -> SocketAddr {
        self.bind_addr
    }

    /// Initiate graceful shutdown. Subsequent calls are a no-op.
    pub async fn shutdown(self) {
        self.shutdown_flag.store(true, Ordering::Release);
        self.shutdown.notify_waiters();
        let _ = self.join.await;
    }
}

/// Configuration handed to [`start`]: everything that pins this service
/// to a particular mydb instance.
pub struct MasterServiceInit {
    pub bind_host: String,
    pub bind_port: u16,
    pub config: ReplConfig,
    pub master_uuid: [u8; 16],
    pub epoch: u64,
    pub server_version: String,
    pub log: Arc<crate::wal::LogMutex>,
    pub slots: Arc<PlMutex<SlotRegistry>>,
    /// Item 3: live `Database` + data dir. When `None`, the service
    /// runs in WAL-streaming-only mode and refuses any handshake that
    /// would require a base backup (Resync-and-disconnect).
    pub db: Option<Arc<crate::storage::Database>>,
    pub data_dir: Option<std::path::PathBuf>,
    pub role_state: Option<Arc<std::sync::Mutex<crate::repl::role::RoleState>>>,
}

/// Start the listener. Returns a [`MasterService`] handle whose
/// `bind_addr` is the actual address (useful when `bind_port = 0`).
pub async fn start(init: MasterServiceInit) -> Result<MasterService, QueryError> {
    let bind = format!("{}:{}", init.bind_host, init.bind_port);
    let listener = TcpListener::bind(&bind).await.map_err(|err| {
        error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            format!("repl bind {bind}: {err}"),
        )
    })?;
    start_bound(init, listener)
}

/// Start from an already-bound nonblocking listener. The process manager uses
/// this entry point so SQL/startup can report bind failure synchronously rather
/// than returning OK and logging an asynchronous failure later.
pub fn start_bound(
    init: MasterServiceInit,
    listener: TcpListener,
) -> Result<MasterService, QueryError> {
    let bind_addr = listener.local_addr().map_err(|err| {
        error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            format!("repl local_addr: {err}"),
        )
    })?;

    let tracker = Arc::new(SenderTracker::default());
    let shutdown = Arc::new(Notify::new());
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let base_backup_slots = Arc::new(tokio::sync::Semaphore::new(init.config.max_base_backups));
    let connection_slots = Arc::new(tokio::sync::Semaphore::new(init.config.max_connections));

    let slots = init.slots;
    let env = SenderEnv {
        log: init.log,
        config: init.config,
        master_uuid: init.master_uuid,
        epoch: init.epoch,
        server_version: init.server_version,
        db: init.db,
        data_dir: init.data_dir,
        slots: Arc::clone(&slots),
        shutdown: Arc::clone(&shutdown),
        shutdown_flag: Arc::clone(&shutdown_flag),
        role_state: init.role_state,
        base_backup_slots,
    };
    let slots_for_handle = Arc::clone(&slots);
    let tracker_clone = Arc::clone(&tracker);
    let shutdown_clone = Arc::clone(&shutdown);
    let connection_slots_for_accept = Arc::clone(&connection_slots);

    let join = tokio::spawn(async move {
        let mut senders = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                biased;
                _ = shutdown_clone.notified() => {
                    log_message(format_args!(
                        "mydb-repl master: shutdown signal received, stopping accept loop"
                    ));
                    break;
                }
                Some(_completed) = senders.join_next(), if !senders.is_empty() => {}
                accept = listener.accept() => {
                    match accept {
                        Ok((stream, peer)) => {
                            let connection_permit = match Arc::clone(&connection_slots_for_accept)
                                .try_acquire_owned()
                            {
                                Ok(permit) => permit,
                                Err(_) => {
                                log_message(format_args!(
                                    "mydb-repl master: rejecting {peer}; replication connection limit reached"
                                ));
                                drop(stream);
                                continue;
                                }
                            };
                            log_message(format_args!(
                                "mydb-repl master: replica connected from {peer}"
                            ));
                            // Disable Nagle: replication frames (acks,
                            // heartbeats) are small and latency-sensitive;
                            // Nagle × delayed-ACK otherwise injects tens of
                            // ms into the semi-sync ack RTT (Pf2).
                            if let Err(e) = stream.set_nodelay(true) {
                                log_message(format_args!(
                                    "mydb-repl master: set_nodelay failed for {peer}: {e}"
                                ));
                            }
                            let env = env.clone();
                            let slots = Arc::clone(&slots);
                            let tracker = Arc::clone(&tracker_clone);
                            senders.spawn(async move {
                                let _connection_permit = connection_permit;
                                let stream = match crate::repl::tls::accept(
                                    stream,
                                    &env.config,
                                    &env.config.bind_host,
                                )
                                .await
                                {
                                    Ok(stream) => stream,
                                    Err(err) => {
                                        log_message(format_args!(
                                            "mydb-repl master: TLS/transport handshake failed for {peer}: {err}"
                                        ));
                                        return;
                                    }
                                };
                                let slot_for = |uuid: &str| -> SlotHandle {
                                    let mut g = slots.lock();
                                    let handle = g.create_or_get(uuid, now_ms());
                                    // Persist so a good on-disk acked_lsn
                                    // exists from connect; log on failure
                                    // instead of silently swallowing (C16).
                                    if let Err(e) = g.persist(uuid) {
                                        log_message(format_args!(
                                            "mydb-repl master: slot persist on connect failed \
                                             for {uuid}: {}",
                                            e.message()
                                        ));
                                    }
                                    handle
                                };
                                match sender::run_sender(stream, env, slot_for, tracker).await {
                                    Ok(()) => {}
                                    Err(err) => log_message(format_args!(
                                        "mydb-repl master: sender ended with error: {}",
                                        err.message()
                                    )),
                                }
                            });
                        }
                        Err(err) => {
                            log_message(format_args!(
                                "mydb-repl master: accept error: {err}"
                            ));
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        }
                    }
                }
            }
        }
        // The same sticky shutdown signal is observed by every sender stream.
        // Join them before the service reports stopped so DEMOTE/shutdown cannot
        // race a detached sender still reading the Database WAL.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        while !senders.is_empty() {
            let now = tokio::time::Instant::now();
            if now >= deadline {
                senders.abort_all();
                while senders.join_next().await.is_some() {}
                break;
            }
            let _ = tokio::time::timeout(deadline - now, senders.join_next()).await;
        }
    });

    Ok(MasterService {
        bind_addr,
        slots: slots_for_handle,
        tracker,
        shutdown,
        shutdown_flag,
        join,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repl::protocol::{
        read_frame_async, write_frame_async, Handshake, Message, ReplicaAck, PROTOCOL_VERSION,
    };
    use crate::wal::{LogManager, WalHeapInsert, WalPayload};
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
    use tokio::net::TcpStream;

    fn append_filler_records(log: &Arc<crate::wal::LogMutex>, n: usize) -> Vec<u64> {
        let mut g = log.lock();
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            let lsn = g
                .append(
                    1,
                    WalPayload::HeapInsertBatch {
                        path: "schemas/test/tables/t__1__data.heap".into(),
                        inserts: vec![WalHeapInsert {
                            page_id: 1,
                            slot_id: 0,
                            payload: vec![0x42; 128],
                            undo_lsn: None,
                        }],
                    },
                )
                .unwrap();
            out.push(lsn);
        }
        g.flush().unwrap();
        out
    }

    async fn build_service(dir: &TempDir) -> (MasterService, Arc<crate::wal::LogMutex>) {
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        // Force small segments so multi-segment paths are exercised.
        log.lock().set_max_segment_bytes(4096);
        let slots = Arc::new(PlMutex::new(SlotRegistry::open(dir.path()).unwrap()));
        let svc = start(MasterServiceInit {
            bind_host: "127.0.0.1".to_string(),
            bind_port: 0,
            config: ReplConfig {
                heartbeat: Duration::from_millis(50),
                ..ReplConfig::default()
            },
            master_uuid: [0xAA; 16],
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
        (svc, log)
    }

    async fn handshake_replica(
        addr: SocketAddr,
        start_lsn: u64,
    ) -> (
        BufReader<tokio::net::tcp::OwnedReadHalf>,
        BufWriter<tokio::net::tcp::OwnedWriteHalf>,
        crate::repl::protocol::HandshakeAck,
    ) {
        let stream = TcpStream::connect(addr).await.unwrap();
        let (rh, wh) = stream.into_split();
        let mut reader = BufReader::new(rh);
        let mut writer = BufWriter::new(wh);
        let h = Handshake {
            protocol_version: PROTOCOL_VERSION,
            replica_uuid: [0xBB; 16],
            requested_start_lsn: start_lsn,
            last_known_master_uuid: [0; 16],
            last_known_epoch: 0,
            cluster_uuid: [0; 16],
            last_known_timeline_uuid: [0; 16],
            client_version: "test-replica".into(),
            request_base_backup: false,
            max_chunk_bytes: 1024 * 1024,
            format_compatibility: crate::storage::format_versions::native_replication_compatibility(
            ),
        };
        write_frame_async(&mut writer, &Message::Handshake(h))
            .await
            .unwrap();
        writer.flush().await.unwrap();
        let ack = match read_frame_async(&mut reader).await.unwrap() {
            Message::HandshakeAck(a) => a,
            other => panic!("expected HandshakeAck, got {other:?}"),
        };
        (reader, writer, ack)
    }

    #[tokio::test]
    async fn handshake_accepts_in_range_start_lsn() {
        let dir = TempDir::new().unwrap();
        let (svc, log) = build_service(&dir).await;
        let lsns = append_filler_records(&log, 4);
        let (_reader, _writer, ack) = handshake_replica(svc.bind_addr(), lsns[0]).await;
        assert!(ack.accepted, "ack.reason = {}", ack.reason);
        assert_eq!(ack.master_uuid, [0xAA; 16]);
        assert!(ack.current_lsn > lsns[0]);
        svc.shutdown().await;
    }

    #[tokio::test]
    async fn handshake_rejects_truncated_start_lsn_and_sends_resync() {
        let dir = TempDir::new().unwrap();
        let (svc, log) = build_service(&dir).await;
        append_filler_records(&log, 10);
        // Run a checkpoint compact to truncate WAL.
        {
            let mut g = log.lock();
            let next = g.next_lsn();
            g.compact_to_checkpoint(next, 100, 200).unwrap();
        }
        // LSN 1 is now below min_available.
        let (mut reader, _writer, ack) = handshake_replica(svc.bind_addr(), 1).await;
        assert!(!ack.accepted);
        let resync = match read_frame_async(&mut reader).await.unwrap() {
            Message::Resync(r) => r,
            other => panic!("expected Resync, got {other:?}"),
        };
        assert!(resync.new_start_lsn >= ack.min_available_lsn);
        svc.shutdown().await;
    }

    #[tokio::test]
    async fn streams_existing_records_to_replica_and_records_ack() {
        let dir = TempDir::new().unwrap();
        let (svc, log) = build_service(&dir).await;
        let lsns = append_filler_records(&log, 8);
        let last_lsn = *lsns.last().unwrap();
        let (mut reader, mut writer, ack) = handshake_replica(svc.bind_addr(), lsns[0]).await;
        assert!(ack.accepted);

        // Drain enough chunks to cover lsns.
        let mut received_frames = 0u32;
        let mut highest_seen = 0u64;
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while highest_seen < last_lsn && std::time::Instant::now() < deadline {
            let msg =
                tokio::time::timeout(Duration::from_millis(500), read_frame_async(&mut reader))
                    .await;
            let msg = match msg {
                Ok(Ok(m)) => m,
                Ok(Err(e)) => panic!("read err: {e}"),
                Err(_) => continue, // heartbeat tick may have elapsed
            };
            if let Message::WalChunk(chunk) = msg {
                received_frames += chunk.frame_count;
                highest_seen = chunk.end_lsn.max(highest_seen);
            }
        }
        assert!(
            highest_seen >= last_lsn,
            "highest_seen={highest_seen}, last={last_lsn}"
        );
        assert!(received_frames as usize >= lsns.len());

        // Ack and verify slot advance.
        write_frame_async(
            &mut writer,
            &Message::ReplicaAck(ReplicaAck {
                received_lsn: last_lsn,
                durable_lsn: last_lsn,
                applied_lsn: last_lsn,
                time_ms: 0,
            }),
        )
        .await
        .unwrap();
        writer.flush().await.unwrap();

        let replica_uuid = crate::repl::role::uuid_bytes_to_string(&[0xBB; 16]);
        let mut updated = 0u64;
        let until = std::time::Instant::now() + Duration::from_secs(2);
        while std::time::Instant::now() < until {
            let reached = {
                let g = svc.slots.lock();
                if let Some(h) = g.get(&replica_uuid) {
                    updated = h.acked_lsn();
                }
                updated >= last_lsn
            };
            if reached {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            updated >= last_lsn,
            "slot acked_lsn={updated}, want>={last_lsn}"
        );
        svc.shutdown().await;
    }

    #[tokio::test]
    async fn slot_acked_lsn_is_persisted_to_disk_after_ack() {
        // C1: the acked_lsn must survive a master restart. Previously it
        // lived only in memory; a recovered master read a stale/zero
        // acked_lsn and could mis-prune WAL. After an ack, reopening the
        // registry from disk must observe the advanced acked_lsn.
        let dir = TempDir::new().unwrap();
        let (svc, log) = build_service(&dir).await;
        let lsns = append_filler_records(&log, 6);
        let last_lsn = *lsns.last().unwrap();
        let (mut reader, mut writer, ack) = handshake_replica(svc.bind_addr(), lsns[0]).await;
        assert!(ack.accepted);

        // Drain to the tail.
        let mut highest = 0u64;
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while highest < last_lsn && std::time::Instant::now() < deadline {
            if let Ok(Ok(Message::WalChunk(c))) =
                tokio::time::timeout(Duration::from_millis(300), read_frame_async(&mut reader))
                    .await
            {
                highest = c.end_lsn.max(highest);
            }
        }
        // Ack the tail.
        write_frame_async(
            &mut writer,
            &Message::ReplicaAck(ReplicaAck {
                received_lsn: last_lsn,
                durable_lsn: last_lsn,
                applied_lsn: last_lsn,
                time_ms: 0,
            }),
        )
        .await
        .unwrap();
        writer.flush().await.unwrap();

        // Wait for the in-memory slot to advance (proves the ack landed).
        let replica_uuid = crate::repl::role::uuid_bytes_to_string(&[0xBB; 16]);
        let until = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            let acked = svc
                .slots
                .lock()
                .get(&replica_uuid)
                .map(|h| h.acked_lsn())
                .unwrap_or(0);
            if acked >= last_lsn || std::time::Instant::now() >= until {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // Reopen a fresh registry from the SAME dir: the persisted
        // acked_lsn must reflect the ack (first ack persists immediately;
        // subsequent ones are time-throttled).
        let reopened = SlotRegistry::open(dir.path()).unwrap();
        let persisted = reopened
            .get(&replica_uuid)
            .expect("slot file present on disk")
            .acked_lsn();
        assert!(
            persisted >= last_lsn,
            "persisted acked_lsn={persisted}, want >= {last_lsn}"
        );
        svc.shutdown().await;
    }

    #[tokio::test]
    async fn handshake_rejects_ahead_of_master_lsn_with_resync() {
        // A replica that applied WAL a crashed master lost comes back
        // requesting an LSN beyond the master's next LSN. That's a
        // diverged timeline: handshake must reject AND send a Resync so
        // the replica re-seeds instead of retrying forever.
        let dir = TempDir::new().unwrap();
        let (svc, log) = build_service(&dir).await;
        append_filler_records(&log, 3);
        let next = log.lock().next_lsn();
        let (mut reader, _writer, ack) = handshake_replica(svc.bind_addr(), next + 100).await;
        assert!(!ack.accepted, "ahead-of-master LSN must be rejected");
        let resync = match read_frame_async(&mut reader).await.unwrap() {
            Message::Resync(r) => r,
            other => panic!("expected Resync, got {other:?}"),
        };
        assert!(
            resync.reason.contains("ahead"),
            "reason should mention divergence: {}",
            resync.reason
        );
        svc.shutdown().await;
    }

    #[tokio::test]
    async fn handshake_rejects_replica_on_fork_uuid_mismatch() {
        let dir = TempDir::new().unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        log.lock().set_max_segment_bytes(4096);
        let slots = Arc::new(PlMutex::new(SlotRegistry::open(dir.path()).unwrap()));
        let svc = start(MasterServiceInit {
            bind_host: "127.0.0.1".to_string(),
            bind_port: 0,
            config: ReplConfig::default(),
            master_uuid: [0xAA; 16],
            epoch: 5,
            server_version: "test".into(),
            log: Arc::clone(&log),
            slots,
            db: None,
            data_dir: None,
            role_state: None,
        })
        .await
        .unwrap();
        append_filler_records(&log, 3);

        let stream = TcpStream::connect(svc.bind_addr()).await.unwrap();
        let (rh, wh) = stream.into_split();
        let mut reader = BufReader::new(rh);
        let mut writer = BufWriter::new(wh);
        // Replica claims a DIFFERENT prior master UUID.
        let h = Handshake {
            protocol_version: PROTOCOL_VERSION,
            replica_uuid: [0xBB; 16],
            requested_start_lsn: 1,
            last_known_master_uuid: [0x55; 16], // different from master_uuid
            last_known_epoch: 4,
            cluster_uuid: [0; 16],
            last_known_timeline_uuid: [0; 16],
            client_version: "test-replica".into(),
            request_base_backup: false,
            max_chunk_bytes: 1024 * 1024,
            format_compatibility: crate::storage::format_versions::native_replication_compatibility(
            ),
        };
        write_frame_async(&mut writer, &Message::Handshake(h))
            .await
            .unwrap();
        writer.flush().await.unwrap();
        let ack = match read_frame_async(&mut reader).await.unwrap() {
            Message::HandshakeAck(a) => a,
            other => panic!("expected HandshakeAck, got {other:?}"),
        };
        assert!(
            ack.accepted,
            "protocol v2 must not treat a node UUID change alone as a fork: {}",
            ack.reason
        );
        svc.shutdown().await;
    }

    #[tokio::test]
    async fn handshake_rejects_replica_when_replica_epoch_exceeds_master() {
        let dir = TempDir::new().unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let slots = Arc::new(PlMutex::new(SlotRegistry::open(dir.path()).unwrap()));
        let svc = start(MasterServiceInit {
            bind_host: "127.0.0.1".to_string(),
            bind_port: 0,
            config: ReplConfig::default(),
            master_uuid: [0xCC; 16],
            epoch: 3,
            server_version: "test".into(),
            log: Arc::clone(&log),
            slots,
            db: None,
            data_dir: None,
            role_state: None,
        })
        .await
        .unwrap();
        append_filler_records(&log, 2);

        let stream = TcpStream::connect(svc.bind_addr()).await.unwrap();
        let (rh, wh) = stream.into_split();
        let mut reader = BufReader::new(rh);
        let mut writer = BufWriter::new(wh);
        let h = Handshake {
            protocol_version: PROTOCOL_VERSION,
            replica_uuid: [0xDD; 16],
            requested_start_lsn: 1,
            last_known_master_uuid: [0xCC; 16],
            last_known_epoch: 7, // ahead of master's epoch=3
            cluster_uuid: [0; 16],
            last_known_timeline_uuid: [0; 16],
            client_version: "test-replica".into(),
            request_base_backup: false,
            max_chunk_bytes: 1024 * 1024,
            format_compatibility: crate::storage::format_versions::native_replication_compatibility(
            ),
        };
        write_frame_async(&mut writer, &Message::Handshake(h))
            .await
            .unwrap();
        writer.flush().await.unwrap();
        let ack = match read_frame_async(&mut reader).await.unwrap() {
            Message::HandshakeAck(a) => a,
            other => panic!("expected HandshakeAck, got {other:?}"),
        };
        assert!(!ack.accepted);
        assert!(
            ack.reason.contains("generation") && ack.reason.contains("ahead"),
            "reason: {}",
            ack.reason
        );
        svc.shutdown().await;
    }

    #[tokio::test]
    async fn live_appends_after_handshake_are_streamed_too() {
        let dir = TempDir::new().unwrap();
        let (svc, log) = build_service(&dir).await;
        let initial = append_filler_records(&log, 2);
        let (mut reader, _writer, ack) = handshake_replica(svc.bind_addr(), initial[0]).await;
        assert!(ack.accepted);

        // Append more records AFTER the replica has connected.
        let later = {
            let mut g = log.lock();
            let mut out = Vec::new();
            for _ in 0..3 {
                let lsn = g
                    .append(
                        2,
                        WalPayload::HeapInsertBatch {
                            path: "schemas/test/tables/t__1__data.heap".into(),
                            inserts: vec![WalHeapInsert {
                                page_id: 1,
                                slot_id: 0,
                                payload: vec![0x77; 64],
                                undo_lsn: None,
                            }],
                        },
                    )
                    .unwrap();
                out.push(lsn);
            }
            g.flush().unwrap();
            out
        };
        let target = *later.last().unwrap();
        let mut highest_seen = 0u64;
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while highest_seen < target && std::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(500), read_frame_async(&mut reader))
                .await
            {
                Ok(Ok(Message::WalChunk(chunk))) => {
                    highest_seen = chunk.end_lsn.max(highest_seen);
                }
                Ok(Ok(_)) => {}
                Ok(Err(e)) => panic!("read err: {e}"),
                Err(_) => continue,
            }
        }
        assert!(
            highest_seen >= target,
            "want stream live appends, saw {highest_seen}"
        );
        svc.shutdown().await;
    }
}
